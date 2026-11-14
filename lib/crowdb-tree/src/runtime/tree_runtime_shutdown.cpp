// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_dispatcher.h"
#include "tree_runtime_internal.h"

namespace crowdb::tree
{
bool TreeRuntime::State::start_binding() noexcept
{
    auto value = bindings.load(std::memory_order_acquire);
    do {
        if ((value & kClosed) != 0) {
            return false;
        }
    } while (!bindings.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    return true;
}

void TreeRuntime::State::finish_binding() noexcept
{
    if (bindings.fetch_sub(1, std::memory_order_acq_rel) == (kClosed | kDrainArmed | 1U)) {
        drain_views();
    }
}

Status TreeRuntime::drain_async(AsyncCompletion complete) noexcept
{
    if (!complete) {
        return Status::invalid_argument();
    }
    auto state    = state_;
    auto expected = State::Shutdown::kOpen;
    if (!state->shutdown.compare_exchange_strong(expected, State::Shutdown::kDraining, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    stop_admission();
    state->on_shutdown = std::move(complete);
    if ((state->bindings.fetch_or(State::kDrainArmed, std::memory_order_acq_rel) & State::kBindings) == 0) {
        state->drain_views();
    }
    return Status::Ok();
}

void TreeRuntime::State::drain_views() noexcept
{
    view_cpu_progress->close_async({.context = this, .complete_fn = &State::view_cpu_drained});
}

void TreeRuntime::State::view_cpu_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.view_backend_progress->close_async({.context = &self, .complete_fn = &State::view_backend_drained});
}

void TreeRuntime::State::view_backend_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.retirement->close_async({.context = &self, .complete_fn = &State::retirement_drained});
}

void TreeRuntime::State::retirement_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.cpu->drain_async({.context = &self, .complete_fn = &State::cpu_drained});
}

void TreeRuntime::State::cpu_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.backend->drain_async({.context = &self, .complete_fn = &State::backend_drained});
}

void TreeRuntime::State::backend_drained(void *context, Status status) noexcept
{
    auto &self = *static_cast<State *>(context);
    auto  done = std::move(self.on_shutdown);
    self.shutdown.store(Shutdown::kDrained, std::memory_order_release);
    done.complete(std::move(status));
}

Status TreeRuntime::stop_workers_async(AsyncCompletion complete) noexcept
{
    if (!complete || detail::FlushRuntime::on_worker_thread()) {
        return Status::invalid_argument();
    }
    auto state    = state_;
    auto expected = State::Shutdown::kDrained;
    if (!state->shutdown.compare_exchange_strong(expected, State::Shutdown::kStopping, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    state->on_shutdown = std::move(complete);
    state->running_pools.store(2, std::memory_order_relaxed);
    state->cpu->stop_workers({.context = state.get(), .complete_fn = &State::workers_exited});
    state->backend->stop_workers({.context = state.get(), .complete_fn = &State::workers_exited});
    return Status::Ok();
}

void TreeRuntime::State::workers_exited(void *context, Status status) noexcept
{
    auto &self = *static_cast<State *>(context);
    if (self.running_pools.fetch_sub(1, std::memory_order_acq_rel) == 1) {
        auto done = std::move(self.on_shutdown);
        self.shutdown.store(Shutdown::kExited, std::memory_order_release);
        done.complete(std::move(status));
    }
}
} // namespace crowdb::tree
