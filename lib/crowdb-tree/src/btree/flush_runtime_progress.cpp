// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "flush_runtime_internal.h"

#include <cassert>

namespace crowdb::tree::detail
{
FlushRuntime::ProgressState::~ProgressState()
{
    assert(!active.load(std::memory_order_acquire));
}

void FlushRuntime::ProgressState::schedule(const std::shared_ptr<const void> &owner) noexcept
{
    // The single owner reuses its operation after the previous task exits.
    runtime->lifecycle->jobs.fetch_add(1, std::memory_order_acq_rel);
    work.fetch_add(1, std::memory_order_acq_rel);
    [[maybe_unused]] const auto previous = lane->jobs_.fetch_add(1, std::memory_order_acq_rel);
    assert((previous & Lane::kClosing) == 0 && "progress must join before closing its lane");
    auto *job = ::new (static_cast<void *>(operation_storage.data()))
        State::Job(runtime, lane, task, context, weak_from_this().lock(), owner);
    runtime->submit(*job);
}

void FlushRuntime::ProgressState::request(const std::shared_ptr<const void> &owner) noexcept
{
    assert(owner);
    auto value = work.load(std::memory_order_acquire);
    do {
        if ((value & (State::kClosed | kStopArmed)) != 0) {
            return;
        }
    } while (!work.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    requested.store(true, std::memory_order_release);
    if (!active.exchange(true, std::memory_order_acq_rel)) {
        schedule(owner);
    }
    finish_work();
}

void FlushRuntime::ProgressState::complete(const std::shared_ptr<const void> &owner) noexcept
{
    // Pair with request's exchange: either this RMW acquires the preceding
    // requested publication, or the producer observes idle and schedules it.
    // A release store followed by an independent load can lose both wakes.
    active.exchange(false, std::memory_order_acq_rel);
    if (requested.load(std::memory_order_acquire) && !active.exchange(true, std::memory_order_acq_rel)) {
        schedule(owner);
    }
}

void FlushRuntime::ProgressState::finish_work() noexcept
{
    const auto previous = work.fetch_sub(1, std::memory_order_acq_rel);
    if ((previous & kWork) == 1) {
        work.notify_all();
        if ((previous & kCloseArmed) != 0) {
            finish_close(previous - 1);
        }
    }
}

void FlushRuntime::ProgressState::drain() noexcept
{
    auto value = work.load(std::memory_order_acquire);
    while ((value & kWork) != 0) {
        work.wait(value, std::memory_order_acquire);
        value = work.load(std::memory_order_acquire);
    }
}

void FlushRuntime::ProgressState::close_async(AsyncCompletion complete) noexcept
{
    on_drained = std::move(complete);
    arm_close(0);
}

void FlushRuntime::ProgressState::stop_async(AsyncCompletion complete) noexcept
{
    on_drained = std::move(complete);
    arm_close(kStopArmed);
}

void FlushRuntime::ProgressState::arm_close(uint64_t flags) noexcept
{
    const auto armed    = kCloseArmed | flags;
    const auto previous = work.fetch_or(armed, std::memory_order_acq_rel);
    if ((previous & kWork) == 0) {
        finish_close(previous | armed);
    }
}

void FlushRuntime::ProgressState::finish_close(uint64_t idle) noexcept
{
    assert((idle & kWork) == 0);
    if ((idle & State::kClosed) != 0 ||
        !work.compare_exchange_strong(idle, idle | State::kClosed, std::memory_order_acq_rel)) {
        return;
    }
    auto done = std::move(on_drained);
    done.complete(Status::Ok());
}

void FlushRuntime::ProgressState::close_and_drain() noexcept
{
    work.fetch_or(State::kClosed, std::memory_order_acq_rel);
    drain();
}

FlushRuntime::Progress::Progress(std::shared_ptr<ProgressState> state) : state_(std::move(state))
{
}

void FlushRuntime::Progress::request(std::shared_ptr<const void> owner) noexcept
{
    state_->request(owner);
}

void FlushRuntime::Progress::drain() noexcept
{
    state_->drain();
}

void FlushRuntime::Progress::close_async(AsyncCompletion complete) noexcept
{
    auto state = state_;
    state->close_async(std::move(complete));
}

void FlushRuntime::Progress::stop_async(AsyncCompletion complete) noexcept
{
    auto state = state_;
    state->stop_async(std::move(complete));
}

void FlushRuntime::Progress::close_and_drain() noexcept
{
    state_->close_and_drain();
}

size_t FlushRuntime::progress_allocation_bound() noexcept
{
    return shared_allocation_bound<ProgressState>() + shared_allocation_bound<Progress>();
}

Status FlushRuntime::reserve_progress(std::shared_ptr<Lane> lane, Task task, void *context,
                                      std::shared_ptr<AllocationBudget> budget, std::shared_ptr<Progress> *out)
{
    if (!lane || task == nullptr || out == nullptr) {
        return Status::invalid_argument("runtime progress requires lane, task and output");
    }
    if ((lane->jobs_.load(std::memory_order_acquire) & Lane::kClosing) != 0) {
        return Status::unavailable("flush runtime lane closed");
    }
    auto value = state_->lifecycle->jobs.load(std::memory_order_acquire);
    do {
        if ((value & State::kClosed) != 0) {
            return Status::unavailable("flush runtime closed");
        }
    } while (!state_->lifecycle->jobs.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    Status status = Status::Ok();
    try {
        auto progress = std::allocate_shared<ProgressState>(MetadataAllocator<ProgressState>(budget), state_.get(),
                                                            std::move(lane), task, context);
        *out          = std::allocate_shared<Progress>(MetadataAllocator<Progress>(budget), progress);
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    state_->finish();
    return status;
}
} // namespace crowdb::tree::detail
