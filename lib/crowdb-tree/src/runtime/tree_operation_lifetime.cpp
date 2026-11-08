// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_operation_lifetime.h"

#include "btree/allocation_bound.h"

#include <cassert>

namespace crowdb::tree
{
struct TreeOperationLifetime::State
{
    static constexpr uint64_t kClosed = uint64_t{1} << 63;
    static constexpr uint64_t kNotify = uint64_t{1} << 62;
    static constexpr uint64_t kCount  = ~(kClosed | kNotify);

    explicit State(std::shared_ptr<const void> owner) : host_owner(std::move(owner))
    {
    }

    void release() noexcept
    {
        const auto previous = active.fetch_sub(1, std::memory_order_acq_rel);
        if ((previous & kCount) == 1) {
            active.notify_all();
            if ((previous & kNotify) != 0) {
                complete_drain();
            }
        }
    }

    void arm_drain(uint64_t flags, AsyncCompletion complete) noexcept
    {
        on_drained          = std::move(complete);
        const auto previous = active.fetch_or(flags | kNotify, std::memory_order_acq_rel);
        assert((previous & kNotify) == 0);
        if ((previous & kCount) == 0) {
            complete_drain();
        }
    }

    void complete_drain() noexcept
    {
        auto value = active.load(std::memory_order_acquire);
        while ((value & (kCount | kNotify)) == kNotify) {
            if (active.compare_exchange_weak(value, value & ~kNotify, std::memory_order_acq_rel)) {
                auto done = std::move(on_drained);
                done.complete(Status::Ok());
                return;
            }
        }
    }

    std::shared_ptr<const void> host_owner;
    AsyncCompletion             on_drained;
    std::atomic<uint64_t>       active{0};
    std::atomic<uint64_t>       denied{0};
};

TreeOperationLifetime::TreeOperationLifetime(const std::shared_ptr<AllocationBudget> &metadata,
                                             std::shared_ptr<const void>              host_owner)
{
    state_ = detail::reserved_shared<State>(metadata, std::move(host_owner));
}

TreeOperationLifetime::~TreeOperationLifetime()
{
    close();
}

Status TreeOperationLifetime::try_acquire(const std::shared_ptr<AllocationBudget> &metadata,
                                          std::shared_ptr<Ticket>                 *out)
{
    return acquire(metadata, out);
}

Status TreeOperationLifetime::acquire(const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<Ticket> *out)
{
    if (out == nullptr || *out) {
        return Status::invalid_argument("operation ticket output must be empty");
    }
    auto active = state_->active.load(std::memory_order_acquire);
    for (;;) {
        if ((active & State::kClosed) != 0) {
            return Status::unavailable("Tree operation admission closed");
        }
        if (active == State::kCount) {
            state_->denied.fetch_add(1, std::memory_order_relaxed);
            return Status::resource_exhausted("Tree operation tracking counter overflow");
        }
        if (state_->active.compare_exchange_weak(active, active + 1, std::memory_order_acq_rel)) {
            break;
        }
    }
    bool constructed = false;
    try {
        *out = detail::reserved_construct<Ticket>(metadata, [&](Ticket *storage) {
            ::new (static_cast<void *>(storage)) Ticket(state_);
            constructed = true;
        });
    }
    catch (const std::bad_alloc &) {
        // shared_ptr destroys an already constructed Ticket if allocating its
        // control block fails; only a failed object allocation needs rollback.
        if (!constructed) {
            state_->release();
        }
        state_->denied.fetch_add(1, std::memory_order_relaxed);
        return Status::resource_exhausted("Tree operation ticket allocation failed");
    }
    return Status::Ok();
}

TreeOperationLifetime::Ticket::~Ticket()
{
    state_->release();
}

std::shared_ptr<AsyncPageStore> TreeOperationLifetime::Ticket::page_store(AsyncPageStore *store)
{
    return store == nullptr ? nullptr : std::shared_ptr<AsyncPageStore>(shared_from_this(), store);
}

size_t TreeOperationLifetime::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<State>();
}

size_t TreeOperationLifetime::ticket_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<Ticket>();
}

size_t TreeOperationLifetime::outstanding() const noexcept
{
    return state_->active.load(std::memory_order_acquire) & State::kCount;
}

ResourceUsage TreeOperationLifetime::resource_usage() const noexcept
{
    return {.limit = 0, .used = outstanding(), .denied_tree = state_->denied.load(std::memory_order_relaxed)};
}

bool TreeOperationLifetime::closed() const noexcept
{
    return (state_->active.load(std::memory_order_acquire) & State::kClosed) != 0;
}

void TreeOperationLifetime::close() noexcept
{
    state_->active.fetch_or(State::kClosed, std::memory_order_acq_rel);
}

void TreeOperationLifetime::close_async(AsyncCompletion complete) noexcept
{
    auto state = state_;
    state->arm_drain(State::kClosed, std::move(complete));
}

void TreeOperationLifetime::drain_async(AsyncCompletion complete) noexcept
{
    auto state = state_;
    state->arm_drain(0, std::move(complete));
}

void TreeOperationLifetime::close_and_drain() noexcept
{
    close();
    drain_accepted();
}

void TreeOperationLifetime::drain_accepted() noexcept
{
    auto active = state_->active.load(std::memory_order_acquire);
    while ((active & State::kCount) != 0) {
        state_->active.wait(active, std::memory_order_acquire);
        active = state_->active.load(std::memory_order_acquire);
    }
}
} // namespace crowdb::tree
