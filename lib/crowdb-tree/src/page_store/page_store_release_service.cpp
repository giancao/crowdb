// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/page_store/page_store_release_service.h"

#include "btree/allocation_bound.h"

#include <cassert>
#include <utility>

namespace crowdb::tree
{
Status PageStoreReleaseService::create(const std::shared_ptr<AllocationBudget> &budget, size_t capacity,
                                       std::shared_ptr<const void> wake_owner, Wake wake, void *context,
                                       std::shared_ptr<PageStoreReleaseService> *out)
try {
    if (!budget || capacity == 0 || capacity >= kClosed || out == nullptr || *out || (wake != nullptr && !wake_owner)) {
        return Status::invalid_argument();
    }
    auto service = std::allocate_shared<PageStoreReleaseService>(MetadataAllocator<PageStoreReleaseService>(budget));
    service->budget_     = budget;
    service->wake_owner_ = std::move(wake_owner);
    service->wake_       = wake;
    service->context_    = context;
    service->capacity_   = capacity;
    *out                 = std::move(service);
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status PageStoreReleaseService::bind_wake(std::shared_ptr<const void> owner, Wake wake, void *context)
{
    if (!owner || wake == nullptr || wake_ != nullptr || (state_.load(std::memory_order_acquire) & kClosed) != 0) {
        return Status::invalid_argument();
    }
    wake_owner_ = std::move(owner);
    context_    = context;
    wake_       = wake;
    wake_(context_);
    return {};
}

PageStoreReleaseService::~PageStoreReleaseService()
{
    assert((state_.load(std::memory_order_relaxed) & ~kClosed) == 0);
    assert(incoming_.load(std::memory_order_relaxed) == nullptr && active_ == nullptr && deferred_ == nullptr);
}

size_t PageStoreReleaseService::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<PageStoreReleaseService>();
}

Status PageStoreReleaseService::register_node(Node &node)
{
    if (node.dispose == nullptr || node.service || node.next != nullptr || node.queued) {
        return Status::invalid_argument();
    }
    auto keep  = shared_from_this();
    auto state = state_.load(std::memory_order_acquire);
    do {
        if ((state & kClosed) != 0) {
            return Status::unavailable();
        }
        if (state >= capacity_) {
            return Status::resource_exhausted();
        }
    } while (!state_.compare_exchange_weak(state, state + 1, std::memory_order_acq_rel));
    node.service = std::move(keep);
    return {};
}

void PageStoreReleaseService::enqueue(Node &node) noexcept
{
    assert(node.service && !node.queued);
    node.queued = true;
    node.service->pending_.fetch_add(1, std::memory_order_relaxed);
    resume(node);
}

void PageStoreReleaseService::resume(Node &node) noexcept
{
    assert(node.service && node.queued);
    auto  keep = node.service;
    auto *head = keep->incoming_.load(std::memory_order_relaxed);
    do {
        node.next = head;
    } while (!keep->incoming_.compare_exchange_weak(head, &node, std::memory_order_release, std::memory_order_relaxed));
    keep->wake();
}

PageStoreReleaseService::Step PageStoreReleaseService::pump(size_t limit, common::DiskIOUring *reactor) noexcept
{
    if (consumer_.test_and_set(std::memory_order_acquire)) {
        return {.more = true};
    }
    auto keep_service = weak_from_this().lock();
    assert(keep_service);
    Step result;
    if (active_ == nullptr) {
        if (prefer_deferred_ && deferred_ != nullptr) {
            active_          = std::exchange(deferred_, nullptr);
            prefer_deferred_ = false;
        }
        else {
            active_          = incoming_.exchange(nullptr, std::memory_order_acq_rel);
            prefer_deferred_ = true;
            if (active_ == nullptr) {
                active_          = std::exchange(deferred_, nullptr);
                prefer_deferred_ = false;
            }
        }
    }
    for (; result.visited < limit && active_ != nullptr; ++result.visited) {
        auto *node = active_;
        active_    = node->next;
        // The method retains the service through actual free and publication.
        const auto disposal = node->dispose(node, reactor);
        if (disposal == Disposal::kDone) {
            pending_.fetch_sub(1, std::memory_order_relaxed);
            state_.fetch_sub(1, std::memory_order_release);
            ++result.completed;
        }
        else if (disposal == Disposal::kMore) {
            node->next = deferred_;
            deferred_  = node;
        }
    }
    result.more = active_ != nullptr || deferred_ != nullptr || incoming_.load(std::memory_order_acquire) != nullptr;
    consumer_.clear(std::memory_order_release);
    return result;
}

void PageStoreReleaseService::stop_admission() noexcept
{
    state_.fetch_or(kClosed, std::memory_order_acq_rel);
    wake();
}

PageStoreReleaseService::Snapshot PageStoreReleaseService::snapshot() const noexcept
{
    const auto state = state_.load(std::memory_order_acquire);
    return {.registered = state & ~kClosed,
            .pending    = pending_.load(std::memory_order_acquire),
            .closed     = (state & kClosed) != 0};
}

bool PageStoreReleaseService::drained() const noexcept
{
    return state_.load(std::memory_order_acquire) == kClosed;
}

void PageStoreReleaseService::wake() const noexcept
{
    if (wake_ != nullptr) {
        wake_(context_);
    }
}
} // namespace crowdb::tree
