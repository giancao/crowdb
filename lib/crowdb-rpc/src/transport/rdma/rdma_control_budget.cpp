// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_control_stream.h"

#include <atomic>
#include <new>

namespace crowdb::rpc
{
struct ControlStreamBudgetState
{
    uint64_t                                  limit;
    std::function<void()>                     wake;
    std::shared_ptr<ControlStreamBudgetState> totals;
    std::atomic<uint64_t>                     used{0};
    std::atomic<uint64_t>                     peak{0};
    std::atomic<uint64_t>                     stalls{0};

    void update_peak(uint64_t current)
    {
        uint64_t previous = peak.load(std::memory_order_relaxed);
        while (previous < current && !peak.compare_exchange_weak(previous, current, std::memory_order_relaxed)) {
        }
    }

    void release(uint64_t bytes)
    {
        used.fetch_sub(bytes, std::memory_order_acq_rel);
        if (totals)
            totals->used.fetch_sub(bytes, std::memory_order_relaxed);
        if (wake)
            wake();
    }
};

ControlStreamBudget::ControlStreamBudget(uint64_t limit, std::function<void()> wake, const ControlStreamBudget *totals)
    : state_(std::make_shared<ControlStreamBudgetState>())
{
    state_->limit = limit;
    state_->wake  = std::move(wake);
    if (totals != nullptr)
        state_->totals = totals->state_;
}

ControlStreamLease::ControlStreamLease(std::shared_ptr<ControlStreamBudgetState> state, uint64_t bytes)
    : state_(std::move(state)),
      bytes_(bytes)
{
}

ControlStreamLease::~ControlStreamLease()
{
    state_->release(bytes_);
}

std::shared_ptr<ControlStreamLease> ControlStreamBudget::try_reserve(uint64_t bytes)
{
    uint64_t used = state_->used.load(std::memory_order_relaxed);
    do {
        if (bytes == 0 || bytes > state_->limit || used > state_->limit - bytes) {
            state_->stalls.fetch_add(1, std::memory_order_relaxed);
            if (state_->totals)
                state_->totals->stalls.fetch_add(1, std::memory_order_relaxed);
            return {};
        }
    } while (!state_->used.compare_exchange_weak(used, used + bytes, std::memory_order_acq_rel));
    state_->update_peak(used + bytes);
    if (state_->totals) {
        auto &totals = *state_->totals;
        totals.update_peak(totals.used.fetch_add(bytes, std::memory_order_relaxed) + bytes);
    }
    // Build the lease before publishing its shared_ptr. If its control-block
    // allocation fails, shared_ptr deletes the lease and returns admission.
    ControlStreamLease *lease = nullptr;
    try {
        lease = new ControlStreamLease(state_, bytes);
    }
    catch (const std::bad_alloc &) {
        state_->release(bytes);
        return {};
    }
    try {
        return std::shared_ptr<ControlStreamLease>(lease);
    }
    catch (const std::bad_alloc &) {
        return {};
    }
}

uint64_t ControlStreamBudget::used() const
{
    return state_->used.load(std::memory_order_acquire);
}

uint64_t ControlStreamBudget::high_water() const
{
    return state_->peak.load(std::memory_order_relaxed);
}

uint64_t ControlStreamBudget::stalls() const
{
    return state_->stalls.load(std::memory_order_relaxed);
}

uint64_t ControlStreamBudget::limit() const
{
    return state_->limit;
}
} // namespace crowdb::rpc
