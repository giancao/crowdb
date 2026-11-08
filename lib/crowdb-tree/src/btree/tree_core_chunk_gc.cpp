// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/tree_core_internal.h"
#include "page_store/chunk/chunk_collection_operation.h"
#include "page_store/chunk/page_chunk_store_state.h"

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
    auto page_store_state = std::dynamic_pointer_cast<PageChunkStoreState>(page_chunk_store());
    if (!page_store_state) {
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
    status = ChunkCollectionOperation::create(page_store_state, ticket->page_store(page_store_state->store.get()),
                                              binding()->cpu(), binding()->lane(), budget, out);
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
