// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "bootstrap_budget.h"
#include "btree/allocation_reservation.h"
#include "btree/flush_runtime.h"
#include "btree/retirement_queue.h"
#include "crowdb-tree/tree_runtime.h"

namespace crowdb::tree
{
class ApplyDispatcher;

[[nodiscard]] detail::ResourceBudget::Limits resource_limits(TreeResourceLimits limits) noexcept;

struct TreeRuntime::State
{
    static constexpr uint64_t kClosed     = uint64_t{1} << 63;
    static constexpr uint64_t kDrainArmed = uint64_t{1} << 62;
    static constexpr uint64_t kBindings   = ~(kClosed | kDrainArmed);
    enum class Shutdown : uint8_t { kOpen, kDraining, kDrained, kStopping, kExited };

    State(TreeRuntimeOptions options, std::shared_ptr<detail::BootstrapBudget> credit);
    ~State();
    [[nodiscard]] bool start_binding() noexcept;
    void               finish_binding() noexcept;
    void               drain_views() noexcept;
    static void        view_cpu_drained(void *context, Status status) noexcept;
    static void        view_backend_drained(void *context, Status status) noexcept;
    static void        retirement_drained(void *context, Status status) noexcept;
    static void        cpu_drained(void *context, Status status) noexcept;
    static void        backend_drained(void *context, Status status) noexcept;
    static void        workers_exited(void *context, Status status) noexcept;

    TreeRuntimeOptions                       options;
    std::shared_ptr<detail::BootstrapBudget> lifecycle;
    detail::ResourceBudget                   budget;
    std::shared_ptr<detail::FlushRuntime>    cpu;
    std::shared_ptr<detail::FlushRuntime>    backend;
    std::shared_ptr<detail::RetirementQueue> retirement;
    std::shared_ptr<ApplyDispatcher>         view_cpu_progress;
    std::shared_ptr<ApplyDispatcher>         view_backend_progress;
    std::atomic<uint64_t>                    bindings{0};
    std::atomic<Shutdown>                    shutdown{Shutdown::kOpen};
    std::atomic<unsigned>                    running_pools{0};
    AsyncCompletion                          on_shutdown;
};

struct TreeRuntimeAccess
{
    static std::shared_ptr<AllocationBudget> lifecycle(const TreeRuntime &runtime) noexcept
    {
        return runtime.state_->lifecycle;
    }
};
} // namespace crowdb::tree
