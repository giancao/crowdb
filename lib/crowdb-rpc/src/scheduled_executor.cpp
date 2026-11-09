// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/scheduled_executor.h"

#include <algorithm>
#include <limits>
#include <vector>

namespace crowdb::rpc
{

ScheduledExecutor::TaskId ScheduledExecutor::schedule(Task task, uint32_t delay_ms, Task cancelled)
{
    if (!task) {
        return 0;
    }
    TaskId        id = next_id_.fetch_add(1, std::memory_order_relaxed);
    ScheduledTask st;
    st.deadline  = Clock::now() + std::chrono::milliseconds(delay_ms);
    st.callback  = std::move(task);
    st.cancelled = std::move(cancelled);
    {
        std::scoped_lock lock(mu_);
        if (closed_ || id == 0) {
            return 0;
        }
        tasks_.emplace(id, std::move(st));
    }
    return id;
}

bool ScheduledExecutor::cancel(TaskId id)
{
    if (id == 0) {
        return false;
    }
    decltype(tasks_)::node_type removed;
    {
        std::scoped_lock lock(mu_);
        removed = tasks_.extract(id);
    }
    if (removed.empty()) {
        return false;
    }
    if (removed.mapped().cancelled) {
        removed.mapped().cancelled();
    }
    return true;
}

int ScheduledExecutor::run_due_tasks(const std::function<void(int)> &rearm)
{
    std::vector<decltype(tasks_)::node_type> to_run;
    Clock::time_point                        now = Clock::now();

    {
        std::scoped_lock lock(mu_);
        to_run.reserve(tasks_.size());
        for (auto it = tasks_.begin(); it != tasks_.end();) {
            if (it->second.deadline <= now) {
                // Grow the vector before extracting so allocation failure
                // leaves the accepted timer in the task map.
                to_run.emplace_back();
                to_run.back() = tasks_.extract(it++);
            }
            else {
                ++it;
            }
        }
    }

    // Run callbacks outside the lock (callbacks may schedule new tasks).
    for (auto &task : to_run) {
        try {
            task.mapped().callback();
        }
        catch (...) {
            // One receiver must not strand other accepted timers.
        }
    }
    std::scoped_lock lock(mu_);
    auto             next_deadline = Clock::time_point::max();
    for (const auto &[id, task] : tasks_) {
        (void)id;
        next_deadline = std::min(next_deadline, task.deadline);
    }
    int next = -1;
    if (next_deadline != Clock::time_point::max()) {
        const auto ms = std::chrono::ceil<std::chrono::milliseconds>(next_deadline - Clock::now()).count();
        next          = static_cast<int>(std::clamp<int64_t>(ms, 1, std::numeric_limits<int>::max()));
    }
    // Serialize kernel rearm with task-map mutations. This hook only sets
    // the engine timer; user callbacks above never execute under the lock.
    if (rearm) {
        rearm(next);
    }
    return next;
}

void ScheduledExecutor::close()
{
    decltype(tasks_) cancelled;
    {
        std::scoped_lock lock(mu_);
        closed_ = true;
        tasks_.swap(cancelled);
    }
    for (auto &[id, task] : cancelled) {
        (void)id;
        if (task.cancelled) {
            try {
                task.cancelled();
            }
            catch (...) {
            }
        }
    }
}

size_t ScheduledExecutor::pending_count()
{
    std::scoped_lock lock(mu_);
    return tasks_.size();
}

} // namespace crowdb::rpc
