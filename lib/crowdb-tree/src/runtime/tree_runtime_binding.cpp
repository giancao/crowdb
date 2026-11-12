// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_runtime_binding.h"

#include "binding_metrics.h"
#include "btree/allocation_account.h"
#include "btree/tree_metrics.h"
#include "tree_runtime_internal.h"

namespace crowdb::tree
{
TreeRuntimeBinding::TreeRuntimeBinding(std::shared_ptr<TreeRuntime> runtime, TreeBindingOptions options,
                                       std::shared_ptr<detail::ResourceBudget::Account> account,
                                       std::shared_ptr<detail::AllocationReservation>   lifecycle,
                                       std::shared_ptr<detail::FlushRuntime::Lane>      lane,
                                       std::shared_ptr<detail::FlushRuntime::Lane>      backend_lane)
    : owner_(std::move(runtime)),
      options_(options),
      account_(std::move(account)),
      lifecycle_(std::move(lifecycle)),
      work_metrics_(detail::binding_metrics<TreeWorkMetrics>(lifecycle_, owner_->state_->lifecycle)),
      resource_metrics_(detail::binding_metrics<TreeResourceMetrics>(lifecycle_, owner_->state_->lifecycle)),
      cpu_(owner_->state_, owner_->state_->cpu.get()),
      lane_(std::move(lane)),
      backend_cpu_(owner_->state_, owner_->state_->backend.get()),
      backend_lane_(std::move(backend_lane))
{
    account_->set_work_metrics(work_metrics_);
    account_->set_resource_metrics(resource_metrics_);
}

const std::shared_ptr<ApplyDispatcher> &TreeRuntimeBinding::view_cpu_progress() const noexcept
{
    return owner_->state_->view_cpu_progress;
}

const std::shared_ptr<ApplyDispatcher> &TreeRuntimeBinding::view_backend_progress() const noexcept
{
    return owner_->state_->view_backend_progress;
}

void TreeRuntimeBinding::drain() const noexcept
{
    lane_->drain();
    backend_lane_->drain();
    account_->drain_retirements();
}

ResourceUsageSnapshot TreeRuntimeBinding::resource_usage() const noexcept
{
    auto result = account_->resource_usage();
    result[6]   = {.limit = lane_->queue_target(), .used = lane_->queued()};
    result[7]   = {.limit = backend_lane_->queue_target(), .used = backend_lane_->queued()};
    return result;
}

TreeRuntimeBinding::~TreeRuntimeBinding()
{
    backend_progress_.reset();
    lifecycle_->seal();
    owner_->state_->finish_binding();
}

Status TreeRuntimeBinding::create(std::shared_ptr<TreeRuntime> runtime, TreeBindingOptions options,
                                  std::shared_ptr<TreeRuntimeBinding> *out)
try {
    if (!runtime || out == nullptr) {
        return Status::invalid_argument();
    }
    auto status = runtime->validate_binding(options);
    if (!status.ok()) {
        return status;
    }
    auto &state = *runtime->state_;
    if (!state.start_binding()) {
        return Status::unavailable();
    }

    struct PendingBinding
    {
        TreeRuntime::State *state;

        ~PendingBinding()
        {
            if (state != nullptr) {
                state->finish_binding();
            }
        }
    } pending{&state};

    auto account = state.budget.make_account(resource_limits(options.limits), state.lifecycle);
    auto credit  = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, options.lifecycle_bytes);
    if (!credit) {
        return runtime->stopped() ? Status::unavailable() : Status::resource_exhausted();
    }
    std::shared_ptr<detail::AllocationAccount> allocations;
    status = detail::AllocationAccount::create_ongoing(account, detail::BudgetClass::kRouting, &allocations);
    if (!status.ok()) {
        return status;
    }
    auto lifecycle = detail::AllocationReservation::create(std::move(*credit));
    auto lane = detail::reserved_shared<detail::FlushRuntime::Lane>(lifecycle, options.cpu_queue_limit, allocations);
    auto backend_lane = detail::reserved_shared<detail::FlushRuntime::Lane>(lifecycle, options.backend_queue_limit,
                                                                            std::move(allocations));
    auto result       = detail::reserved_construct<TreeRuntimeBinding>(lifecycle, [&](TreeRuntimeBinding *value) {
        ::new (static_cast<void *>(value))
            TreeRuntimeBinding(runtime, options, account, lifecycle, std::move(lane), std::move(backend_lane));
        pending.state = nullptr;
    });
    status =
        ApplyDispatcher::create(result->backend_cpu_, result->backend_lane_, 1, lifecycle, &result->backend_progress_);
    if (!status.ok()) {
        return status;
    }
    if (runtime->stopped()) {
        return Status::unavailable();
    }
    *out = std::move(result);
    return {};
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (const std::invalid_argument &) {
    return Status::invalid_argument();
}
catch (...) {
    return Status::internal_error();
}
} // namespace crowdb::tree
