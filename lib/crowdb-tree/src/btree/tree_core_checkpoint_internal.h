// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/apply_dispatcher.h"
#include "btree/apply_progress.h"
#include "btree/apply_waiters.h"
#include "btree/tree_core.h"
#include "page_store/local/block_account.h"
#include "snapshot/checkpoint_operation.h"

namespace crowdb::tree::checkpoint_detail
{
struct Credits
{
    PageLoadBudget                                 budget;
    std::shared_ptr<detail::AllocationReservation> snapshot;
    ~Credits();
};

struct Preparation
{
    bool                                           handoff_blocked = false;
    TreeCore                                      *core            = nullptr;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AllocationBudget>              control;
    std::shared_ptr<AllocationBudget>              finish;
    std::shared_ptr<AllocationBudget>              work_allocations;
    CheckpointOptions                              options;
    PageMaintenanceOperationOptions                structure_options;
    Status structure(std::shared_ptr<const TreeRoot> view, uint64_t pid, std::shared_ptr<PageSplitOperation> *out);
    std::shared_ptr<ApplyDispatcher> cpu_progress;
    std::shared_ptr<ApplyWaiters>    waiters;
    ~Preparation();
    Status work(const TreeRoot &view, const CapturedWritePlan *plan, CheckpointWork *out);
};

CheckpointOptions defaults(const TreeCore &core);
void              schedule_free_blocks(TreeCore &core, std::shared_ptr<TreeOperationLifetime::Ticket> ticket) noexcept;
} // namespace crowdb::tree::checkpoint_detail
