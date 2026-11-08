// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// B+tree skip-list implementation.

#include "crowdb-tree/memtable/skip_list.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <new>
#include <random>

namespace crowdb::tree
{

namespace
{
// Branching probability for skip-list height (p=0.25, RocksDB/LevelDB default).
constexpr uint32_t kBranching = 4; // 1/4 probability of height increase
} // namespace

// --- Node allocation ---

Node *ConcurrentSkipList::alloc_node(uint32_t height, Slice key, const std::shared_ptr<AllocationBudget> &budget)
{
    const auto      sz = Node::alloc_size(height, key.size());
    AllocationLease credit(budget, sz, AllocationKind::kMetadata);
    auto           *n    = construct_node(::operator new(sz), height, key);
    n->allocation_credit = std::move(credit);
    n->destroy           = [](EpochManager::Deferred *entry) noexcept {
        auto *node   = static_cast<Node *>(entry);
        auto  credit = std::move(node->allocation_credit);
        destroy_node(node);
        ::operator delete(static_cast<void *>(node));
    };
    return n;
}

Node *ConcurrentSkipList::construct_node(void *storage, uint32_t height, Slice key) noexcept
{
    auto *n     = new (storage) Node{};
    n->height_  = height;
    n->key_len_ = static_cast<uint32_t>(key.size());
    n->destroy  = [](EpochManager::Deferred *entry) noexcept {
        auto *node       = static_cast<Node *>(entry);
        auto  page_block = std::move(node->shared_allocation);
        destroy_node(node);
    };
    // Construct the tower (next_[0..height-1]) with placement new.
    for (uint32_t i = 0; i < height; ++i) {
        new (n->next_ptr(i)) std::atomic<Node *>(nullptr);
    }
    // Copy the key inline.
    if (!key.empty()) {
        std::memcpy(const_cast<char *>(n->key_data()), key.data(), key.size());
    }
    return n;
}

void ConcurrentSkipList::destroy_node(Node *n) noexcept
{
    for (uint32_t i = 0; i < n->height_; ++i) {
        n->next_ptr(i)->~atomic();
    }
    n->~Node();
}

void ConcurrentSkipList::free_node(void *p)
{
    auto *node = static_cast<Node *>(p);
    node->destroy(node);
}

// --- ConcurrentSkipList ---

ConcurrentSkipList::ConcurrentSkipList(EpochManager *epoch, std::shared_ptr<AllocationBudget> budget)
    : epoch_credit_(budget, epoch == nullptr ? sizeof(EpochManager) : 0, AllocationKind::kMetadata),
      owned_epoch_(epoch == nullptr ? std::make_unique<EpochManager>(budget, true) : nullptr),
      epoch_(epoch == nullptr ? owned_epoch_.get() : epoch),
      budget_(std::move(budget))
{
    head_ = alloc_node(kMaxHeight, Slice(), budget_);
}

bool ConcurrentSkipList::retire_step() noexcept
{
    for (size_t step = 0; step < 64; ++step) {
        if (retiring_version_ != nullptr) {
            auto *version     = retiring_version_;
            retiring_version_ = version->next();
            epoch_->defer(version);
            continue;
        }
        auto *node = head_->next(0);
        if (node == nullptr) {
            return true;
        }
        head_->set_next(0, node->next(0));
        retiring_version_ = CellVersion::pointer(node->versions_.load(std::memory_order_relaxed));
        epoch_->defer(node);
    }
    return retiring_version_ == nullptr && head_->next(0) == nullptr;
}

ConcurrentSkipList::~ConcurrentSkipList()
{
    while (!retire_step()) {
    }
    free_node(head_);
}

uint32_t ConcurrentSkipList::random_height()
{
    static thread_local std::minstd_rand rng{std::random_device{}()};
    uint32_t                             h = 1;
    while (h < kMaxHeight && (rng() % kBranching) == 0) {
        ++h;
    }
    return h;
}

Node *ConcurrentSkipList::find_ge(Slice key, Node **prev, Node **successors) const
{
    Node *x         = head_;
    Node *candidate = nullptr;
    for (int h = static_cast<int>(kMaxHeight) - 1; h >= 0; --h) {
        Node *next = x->next(h);
        while (next != nullptr && next->key_slice().compare(key) < 0) {
            x    = next;
            next = x->next(h);
        }
        candidate = next;
        if (prev != nullptr) {
            prev[h] = x;
        }
        if (successors != nullptr) {
            successors[h] = next;
        }
    }
    return candidate;
}

void ConcurrentSkipList::link_upper(Node *node)
{
    std::array<Node *, kMaxHeight> prev{};
    std::array<Node *, kMaxHeight> next{};
    for (uint32_t level = 1; level < node->height_; ++level) {
        do {
            find_ge(node->key_slice(), prev.data(), next.data());
            node->set_next(level, next[level]);
        } while (!prev[level]->next_ptr(level)->compare_exchange_strong(next[level], node, std::memory_order_acq_rel));
    }
    uint32_t height = max_height_.load(std::memory_order_relaxed);
    while (height < node->height_ && !max_height_.compare_exchange_weak(height, node->height_)) {
    }
}

const CellVersion *ConcurrentSkipList::find(Slice key) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h); // acquire
        while (next != nullptr && next->key_slice().compare(key) < 0) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *cand = x->next(0); // acquire
    if (cand != nullptr && cand->key_slice().compare(key) == 0) {
        return CellVersion::pointer(cand->versions_.load(std::memory_order_acquire))->value_owner.get();
    }
    return nullptr;
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor(Slice start_after) const
{
    if (start_after.empty()) {
        // First live node.
        Node *n = head_->next(0);
        return Cursor(n);
    }
    // Find first node with key > start_after.
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr && next->key_slice().compare(start_after) <= 0) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *n = x->next(0);
    return Cursor(n);
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor_from(Slice start_key, bool inclusive) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr &&
               (inclusive ? next->key_slice().compare(start_key) < 0 : next->key_slice().compare(start_key) <= 0)) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    Node *n = x->next(0);
    return Cursor(n);
}

ConcurrentSkipList::Cursor ConcurrentSkipList::cursor_reverse(Slice start_key, bool has_start_bound,
                                                              bool inclusive) const
{
    Node *x = head_;
    int   h = static_cast<int>(max_height_.load(std::memory_order_acquire)) - 1;
    while (h >= 0) {
        Node *next = x->next(h);
        while (next != nullptr && (!has_start_bound || (inclusive ? next->key_slice().compare(start_key) <= 0
                                                                  : next->key_slice().compare(start_key) < 0))) {
            x    = next;
            next = x->next(h);
        }
        --h;
    }
    return Cursor(x == head_ ? nullptr : x);
}

void ConcurrentSkipList::Cursor::select()
{
    candidate_ = nullptr;
    while (cur_ != nullptr) {
        if (upper_.has_value() && cur_->key_slice() >= *upper_) {
            cur_ = nullptr;
            return;
        }
        auto *versions = CellVersion::pointer(cur_->versions_.load(std::memory_order_acquire));
        candidate_     = versions->version_at(frontier_);
        if (candidate_ != nullptr && (floor_ == 0 || candidate_->slot > floor_)) {
            return;
        }
        cur_ = cur_->next(0);
    }
    candidate_ = nullptr;
}

void ConcurrentSkipList::Cursor::advance()
{
    if (cur_ != nullptr) {
        cur_ = cur_->next(0);
    }
    select();
}

ConcurrentSkipList::Cursor ConcurrentSkipList::prefix_cursor(uint64_t frontier, uint64_t floor) const
{
    return Cursor(head_->next(0), frontier, floor);
}

ConcurrentSkipList::Cursor ConcurrentSkipList::range_cursor(std::optional<Slice> lower, std::optional<Slice> upper,
                                                            uint64_t frontier, uint64_t floor) const
{
    if (floor > frontier || (lower.has_value() && upper.has_value() && *lower >= *upper)) {
        return {};
    }
    return Cursor(lower.has_value() ? find_ge(*lower, nullptr) : head_->next(0), frontier, floor, upper);
}

} // namespace crowdb::tree
