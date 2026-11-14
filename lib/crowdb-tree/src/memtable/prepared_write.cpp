// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/skip_list.h"

#include <array>
#include <cassert>

namespace crowdb::tree
{
void ConcurrentSkipList::PreparedWrite::NodeDeleter::operator()(Node *node) const noexcept
{
    ConcurrentSkipList::free_node(node);
}

ConcurrentSkipList::PreparedWrite ConcurrentSkipList::prepare_write(Slice key, buffer &&cell, uint8_t flags,
                                                                    AllocationLease payload, uint64_t bound,
                                                                    const std::shared_ptr<AllocationBudget> &budget)
{
#ifdef CROWDB_TREE_TEST_UTIL
    if (test_hook_ != nullptr) {
        test_hook_(test_context_, PausePoint::kBeforePrepare, key);
    }
#endif
    auto result             = prepare_cell(key, std::move(cell), 0, flags, std::move(payload), bound, budget);
    result.initialize_slot_ = true;
    return result;
}

ConcurrentSkipList::PreparedWrite ConcurrentSkipList::prepare_key(Slice key, uint64_t bound,
                                                                  const std::shared_ptr<AllocationBudget> &budget)
{
    PreparedWrite result;
    result.guard_ = epoch_->enter(budget);
    result.bound_ = bound;
#ifdef CROWDB_TREE_TEST_UTIL
    if (test_hook_ != nullptr) {
        test_hook_(test_context_, PausePoint::kBeforeSearch, key);
    }
#endif
    auto *existing = find_ge(key, nullptr);
    if (existing != nullptr && existing->key_slice() == key) {
        result.existing_ = existing;
    }
    return result;
}

ConcurrentSkipList::PreparedWrite ConcurrentSkipList::prepare_cell(Slice key, buffer &&cell, uint64_t slot,
                                                                   uint8_t flags, AllocationLease payload,
                                                                   uint64_t                                 bound,
                                                                   const std::shared_ptr<AllocationBudget> &budget)
{
    auto        result  = prepare_key(key, bound, budget);
    const auto  height  = result.existing_ == nullptr ? random_height() : 0;
    const auto  bytes   = height == 0 ? 0 : Node::alloc_size(height, key.size());
    void       *storage = nullptr;
    const auto &counter = epoch_->memtable_allocation();
    result.version_ =
        CellVersion::create(std::move(cell), slot, flags, std::move(payload), budget, counter, bytes, &storage);
    if (storage != nullptr) {
        result.candidate_.reset(construct_node(storage, height, key));
        result.candidate_->shared_allocation = result.version_->value_owner;
        result.candidate_->allocation.set(counter, bytes);
    }
    return result;
}

ConcurrentSkipList::PreparedWrite ConcurrentSkipList::prepare_reference(Slice                                    key,
                                                                        std::shared_ptr<const CellVersion>       cell,
                                                                        uint64_t                                 bound,
                                                                        const std::shared_ptr<AllocationBudget> &budget)
{
    assert(cell);
    auto result = prepare_key(key, bound, budget);
    if (result.existing_ == nullptr) {
        result.candidate_.reset(alloc_node(random_height(), key, budget));
        result.candidate_->allocation.set(epoch_->memtable_allocation(),
                                          Node::alloc_size(result.candidate_->height_, key.size()));
    }
    result.version_ = CellVersion::reference(std::move(cell), budget, epoch_->memtable_allocation());
    return result;
}

bool ConcurrentSkipList::commit_write(PreparedWrite write, uint64_t slot, MutationStats *stats) noexcept
{
    assert(write.version_ && (write.existing_ != nullptr || write.candidate_));
    if (epoch_->source_sealed()) {
        return false;
    }
    if (write.initialize_slot_) {
        // The cell is private until the parent CAS. A split tail publishes its
        // reference only after the parent has set the slot and encoded it.
        auto &cell = write.version_->cell;
        if (cell.ownership() != buffer::mode::kExternal) {
            for (size_t index = 0; index < sizeof(slot); ++index) {
                cell.data()[index] = static_cast<uint8_t>(slot >> (8 * index));
            }
        }
    }
    else {
        assert(write.version_->value_owner->slot == slot);
    }
    write.version_->slot = slot;
    if (write.existing_ != nullptr) {
        return insert_version(*write.existing_, write.version_, write.bound_, stats);
    }

    const auto                     key = write.key();
    std::array<Node *, kMaxHeight> previous{};
    std::array<Node *, kMaxHeight> next{};
    write.candidate_->pruning_bound_.store(write.bound_, std::memory_order_relaxed);
    write.candidate_->versions_.store(reinterpret_cast<uintptr_t>(write.version_.get()), std::memory_order_relaxed);
    write.version_->next_bits.store(slot <= write.bound_ ? CellVersion::kEnd : 0, std::memory_order_relaxed);
    for (;;) {
        find_ge(key, previous.data(), next.data());
        if (next[0] != nullptr && next[0]->key_slice() == key) {
            return insert_version(*next[0], write.version_, write.bound_, stats);
        }
        write.candidate_->set_next(0, next[0]);
        if (previous[0]->next_ptr(0)->compare_exchange_strong(next[0], write.candidate_.get(),
                                                              std::memory_order_acq_rel)) {
            auto *published = write.candidate_.release();
            auto *version   = write.version_.release();
#ifdef CROWDB_TREE_TEST_UTIL
            if (test_hook_ != nullptr) {
                test_hook_(test_context_, PausePoint::kBeforeInsertAccounting, key);
            }
#endif
            count_.fetch_add(1, std::memory_order_relaxed);
            account_node(*published, *version);
#ifdef CROWDB_TREE_TEST_UTIL
            if (test_hook_ != nullptr) {
                test_hook_(test_context_, PausePoint::kAfterLevelZero, key);
            }
#endif
            link_upper(published);
            return true;
        }
    }
}
} // namespace crowdb::tree
