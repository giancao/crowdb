// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_internal.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
Status TreeCore::capture_read_only_view(std::shared_ptr<PublishedReadViewContext> *out)
{
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree snapshot admission closed");
    }
    return PublishedReadViewContext::capture_admitted(shared_from_this(), out);
}

Status TreeCore::capture_read_only_view(std::shared_ptr<const TreeRoot>            view,
                                        std::shared_ptr<PublishedReadViewContext> *out)
{
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree snapshot admission closed");
    }
    if (!view || view->generation != capture()->generation) {
        return Status::unavailable("fixed snapshot belongs to an old Tree generation");
    }
    return PublishedReadViewContext::capture_admitted(shared_from_this(), out, std::move(view));
}

Status TreeCore::acquire_snapshot_window(const std::shared_ptr<AllocationBudget>        &metadata,
                                         std::shared_ptr<TreeOperationLifetime::Ticket> *ticket,
                                         AsyncPageStore                                **page_store)
{
    if (page_store == nullptr) {
        return Status::invalid_argument("snapshot read window requires page_store output");
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree snapshot admission closed");
    }
    auto status = state_->lifetime->acquire(metadata, ticket);
    if (status.ok()) {
        *page_store = state_->page_store.get();
    }
    return status;
}
} // namespace crowdb::tree
