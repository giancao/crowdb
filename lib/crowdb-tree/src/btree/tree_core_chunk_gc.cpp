// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"
#include "backend/chunk/chunk_collection_operation.h"
#include "btree/allocation_account.h"
#include "btree/tree_core_internal.h"

namespace crowdb::tree
{
Status TreeCore::prepare_chunk_collection(std::shared_ptr<ChunkCollectionOperation> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable();
    }
    auto backend = std::dynamic_pointer_cast<ChunkBackendState>(chunk_backend());
    if (!backend) {
        return Status::not_supported();
    }
    std::shared_ptr<detail::AllocationAccount> allocations;
    auto                                       status =
        detail::AllocationAccount::create_ongoing(binding()->account(), detail::BudgetClass::kReclaim, &allocations);
    if (!status.ok()) {
        return status;
    }
    PageLoadBudget budget;
    budget.bytes        = std::move(allocations);
    budget.cpu_progress = binding()->backend_progress();

    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    status = operation_lifetime().acquire(budget.bytes, &ticket);
    if (!status.ok()) {
        return status;
    }
    status = ChunkCollectionOperation::create(backend, ticket->backend(backend->store.get()), binding()->cpu(),
                                              binding()->lane(), budget, out);
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
