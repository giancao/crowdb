// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "flush_runtime.h"
#include "runtime/pool_storage.h"

#include <array>
#include <chrono>
#include <exec/static_thread_pool.hpp>
#include <vector>

namespace crowdb::tree::detail
{
inline uint64_t flush_runtime_now_ns() noexcept
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

struct FlushRuntime::State
{
    static constexpr uint64_t kClosed = uint64_t{1} << 63;
    static constexpr uint64_t kDrain  = uint64_t{1} << 62;
    static constexpr uint64_t kJobs   = ~(kClosed | kDrain);

    struct Activity
    {
        std::atomic<uint64_t> jobs{0};
        AsyncCompletion       on_drained;

        void complete() noexcept
        {
            auto done = std::move(on_drained);
            done.complete(Status::Ok());
        }
    };

    struct Job;

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        Job *job;
        void set_value() const noexcept;
    };

    struct Job
    {
        using Operation = stdexec::connect_result_t<
            decltype(stdexec::schedule(std::declval<exec::static_thread_pool &>().get_scheduler())), Receiver>;

        Job(State *owner, std::shared_ptr<Lane> tree_lane, Task work, void *argument,
            std::shared_ptr<ProgressState> progress_owner = {},
            std::shared_ptr<const void>    argument_owner = {}) noexcept
            : state(owner),
              lane(std::move(tree_lane)),
              task(work),
              context(argument),
              progress(std::move(progress_owner)),
              context_owner(std::move(argument_owner)),
              queued_at(flush_runtime_now_ns()),
              operation(stdexec::connect(stdexec::schedule(state->scheduler()), Receiver{this}))
        {
        }

        State                         *state;
        std::shared_ptr<Lane>          lane;
        Task                           task;
        void                          *context;
        std::shared_ptr<ProgressState> progress;
        std::shared_ptr<const void>    context_owner;
        uint64_t                       queued_at;
        Operation                      operation;
    };

    State(size_t workers, size_t target, const std::shared_ptr<AllocationBudget> &budget)
        : pool_storage(budget),
          schedulers(MetadataAllocator<exec::static_thread_pool::scheduler>(budget)),
          worker_count(workers),
          queue_target(target),
          lifecycle(std::allocate_shared<Activity>(MetadataAllocator<Activity>(budget))),
          pool(static_cast<uint32_t>(workers), {}, exec::no_numa_policy{}, &pool_storage, pool_storage.thread_factory())
    {
        // Register on the bootstrap thread once. A cached target scheduler
        // submits through its existing shared MPSC remote queue, without
        // creating a queue for each new host or callback thread.
        schedulers.reserve(workers);
        for (size_t index = 0; index < workers; ++index) {
            schedulers.push_back(pool.get_scheduler_on_thread(index));
        }
    }

    [[nodiscard]] const exec::static_thread_pool::scheduler &scheduler() noexcept
    {
        return schedulers[next_worker.fetch_add(1, std::memory_order_relaxed) % schedulers.size()];
    }

    static void release_job(Job *job) noexcept
    {
        const bool             embedded = job->progress != nullptr;
        MetadataAllocator<Job> allocator(embedded ? nullptr : job->lane->allocations_);
        std::destroy_at(job);
        if (!embedded) {
            allocator.deallocate(job, 1);
        }
    }

    void submit(Job &job) noexcept
    {
        job.lane->queued_.fetch_add(1, std::memory_order_relaxed);
        queued.fetch_add(1, std::memory_order_relaxed);
        submitted.fetch_add(1, std::memory_order_relaxed);
        stdexec::start(job.operation);
    }

    void finish() const noexcept
    {
        // Zero lets the host start teardown. Retain the notification word
        // until notification returns; pool teardown joins this receiver.
        auto       completion = lifecycle;
        const auto previous   = completion->jobs.fetch_sub(1, std::memory_order_acq_rel);
        if ((previous & kJobs) == 1) {
            completion->jobs.notify_all();
            if ((previous & kDrain) != 0) {
                completion->complete();
            }
        }
    }

    PoolStorage pool_storage;
    std::vector<exec::static_thread_pool::scheduler, MetadataAllocator<exec::static_thread_pool::scheduler>> schedulers;
    const size_t              worker_count;
    const size_t              queue_target;
    std::atomic<size_t>       next_worker{0};
    std::shared_ptr<Activity> lifecycle;
    std::atomic<size_t>       queued{0};
    std::atomic<uint64_t>     inflight{0};
    std::atomic<uint64_t>     submitted{0};
    std::atomic<uint64_t>     completed{0};
    std::atomic<uint64_t>     rejected{0};
    std::atomic<uint64_t>     queue_ns{0};
    std::atomic<uint64_t>     execute_ns{0};
    exec::static_thread_pool  pool;
};

struct FlushRuntime::ProgressState : std::enable_shared_from_this<ProgressState>
{
    static constexpr uint64_t kCloseArmed = uint64_t{1} << 61;
    static constexpr uint64_t kStopArmed  = uint64_t{1} << 62;
    static constexpr uint64_t kWork       = ~(State::kClosed | kStopArmed | kCloseArmed);

    ProgressState(State *runtime, std::shared_ptr<Lane> tree_lane, Task work, void *argument)
        : runtime(runtime),
          lane(std::move(tree_lane)),
          task(work),
          context(argument)
    {
    }

    ~ProgressState();
    void request(const std::shared_ptr<const void> &owner) noexcept;
    void schedule(const std::shared_ptr<const void> &owner) noexcept;
    void complete(const std::shared_ptr<const void> &owner) noexcept;
    void finish_work() noexcept;
    void drain() noexcept;
    void close_async(AsyncCompletion complete) noexcept;
    void stop_async(AsyncCompletion complete) noexcept;
    void arm_close(uint64_t flags) noexcept;
    void finish_close(uint64_t idle) noexcept;
    void close_and_drain() noexcept;

    State                *runtime;
    std::shared_ptr<Lane> lane;
    Task                  task;
    void                 *context;
    std::atomic<bool>     active{false}, requested{false};
    // Producers and scheduled jobs both retain the registration until its
    // final idle transition. A producer may schedule after the last job ends.
    std::atomic<uint64_t> work{0};
    AsyncCompletion       on_drained;
    alignas(State::Job) std::array<std::byte, sizeof(State::Job)> operation_storage;
};

} // namespace crowdb::tree::detail
