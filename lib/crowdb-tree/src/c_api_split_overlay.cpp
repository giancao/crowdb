// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/prepared_split_overlay.h"
#include "btree/retirement_queue.h"
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/split_overlay_c_api.h"

struct ct_prepared_split_overlay : crowdb::tree::detail::RetirementNode
{
    crowdb::tree::AllocationLease                       allocation;
    std::shared_ptr<crowdb::tree::PreparedSplitOverlay> owner;
    void (*released)(void *) = nullptr;
    void *release_context    = nullptr;
};

namespace
{
using namespace crowdb::tree;

ct_status code(const crowdb::tree::Status &status) noexcept
{
    return static_cast<ct_status>(status.code());
}

Status split_arm_inputs(ct_tree *source, ct_tree *destination, const ct_split_overlay_limits_v1 *limits,
                        TreeCore *&parent, TreeCore *&child, std::shared_ptr<detail::AllocationReservation> &metadata)
{
    auto *parent_tree = c_api_tree(source);
    auto *child_tree  = c_api_tree(destination);
    if (parent_tree == nullptr || child_tree == nullptr || limits == nullptr || limits->version != 1 ||
        limits->struct_size != sizeof(*limits) || limits->max_records > SIZE_MAX || limits->max_bytes > SIZE_MAX) {
        return Status::invalid_argument();
    }
    parent = TreeCoreAccess::get(*parent_tree);
    child  = TreeCoreAccess::get(*child_tree);
    if (parent == nullptr || child == nullptr) {
        return Status::invalid_argument();
    }
    auto reserved = detail::ResourceBudget::try_reserve(
        child->binding()->account(), detail::BudgetClass::kRouting,
        sizeof(ct_prepared_split_overlay) + detail::shared_allocation_bound<detail::AllocationReservation>());
    if (!reserved) {
        return Status::resource_exhausted();
    }
    metadata = detail::AllocationReservation::create(std::move(*reserved));
    return Status::Ok();
}

std::unique_ptr<ct_prepared_split_overlay> split_handle(const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    AllocationLease charge(metadata, sizeof(ct_prepared_split_overlay), AllocationKind::kMetadata);
    auto            result    = std::make_unique<ct_prepared_split_overlay>();
    result->allocation        = std::move(charge);
    result->retirement_budget = metadata;
    metadata->seal();
    return result;
}
} // namespace

extern "C" ct_status ct_arm_split_overlay(ct_tree *source, ct_tree *destination,
                                          const ct_split_overlay_limits_v1 *limits, ct_prepared_split_overlay **out,
                                          uint64_t *start_frontier)
try {
    if (out == nullptr || *out != nullptr || start_frontier == nullptr) {
        return code(Status::invalid_argument());
    }
    TreeCore                                      *parent = nullptr;
    TreeCore                                      *child  = nullptr;
    std::shared_ptr<detail::AllocationReservation> metadata;
    auto status = split_arm_inputs(source, destination, limits, parent, child, metadata);
    if (!status.ok()) {
        return code(status);
    }
    auto result = split_handle(metadata);
    status      = PreparedSplitOverlay::arm(
        *parent, *child,
        {.max_records = static_cast<size_t>(limits->max_records), .max_bytes = static_cast<size_t>(limits->max_bytes)},
        &result->owner, start_frontier);
    if (!status.ok()) {
        return code(status);
    }
    *out = result.release();
    return code(Status::Ok());
}
catch (const std::bad_alloc &) {
    return code(crowdb::tree::Status::resource_exhausted());
}
catch (...) {
    return code(crowdb::tree::Status::internal_error());
}

extern "C" ct_status ct_arm_split_overlay_async(ct_tree *source, ct_tree *destination,
                                                const ct_split_overlay_limits_v1 *limits,
                                                ct_split_overlay_armed complete, void *ctx)
try {
    if (complete == nullptr) {
        return code(Status::invalid_argument());
    }
    TreeCore                                      *parent = nullptr;
    TreeCore                                      *child  = nullptr;
    std::shared_ptr<detail::AllocationReservation> metadata;
    auto status = split_arm_inputs(source, destination, limits, parent, child, metadata);
    if (!status.ok()) {
        return code(status);
    }
    return code(PreparedSplitOverlay::arm_async(
        *parent, *child,
        {.max_records = static_cast<size_t>(limits->max_records), .max_bytes = static_cast<size_t>(limits->max_bytes)},
        [complete, ctx, metadata = std::move(metadata)](Status status, std::shared_ptr<PreparedSplitOverlay> owner,
                                                        uint64_t start) {
            if (!status.ok()) {
                complete(ctx, code(status), nullptr, 0);
                return;
            }
            std::unique_ptr<ct_prepared_split_overlay> result;
            try {
                result = split_handle(metadata);
            }
            catch (const std::bad_alloc &) {
                owner.reset();
                complete(ctx, code(Status::resource_exhausted()), nullptr, 0);
                return;
            }
            result->owner = std::move(owner);
            complete(ctx, code(Status::Ok()), result.release(), start);
        }));
}
catch (const std::bad_alloc &) {
    return code(Status::resource_exhausted());
}
catch (...) {
    return code(Status::internal_error());
}

extern "C" ct_status ct_prepared_split_overlay_prepare(ct_prepared_split_overlay *prepared, uint64_t replayed_prefix)
try {
    return code(prepared != nullptr && prepared->owner ? prepared->owner->prepare(replayed_prefix)
                                                       : crowdb::tree::Status::invalid_argument());
}
catch (const std::bad_alloc &) {
    return code(crowdb::tree::Status::resource_exhausted());
}
catch (...) {
    return code(crowdb::tree::Status::internal_error());
}

extern "C" ct_status ct_prepared_split_overlay_prepare_async(ct_prepared_split_overlay *prepared,
                                                             uint64_t                   replayed_prefix,
                                                             ct_split_overlay_prepared complete, void *ctx)
{
    if (prepared == nullptr || !prepared->owner || complete == nullptr) {
        return code(Status::invalid_argument());
    }

    struct Reply
    {
        ct_split_overlay_prepared complete;
        void                     *ctx;
    };

    try {
        auto reply = std::make_shared<Reply>(complete, ctx);
        auto owner = prepared->owner;
        return code(owner->prepare_async(replayed_prefix, {.context = reply.get(),
                                                           .complete_fn =
                                                               [](void *p, Status status) {
                                                                   const auto &reply = *static_cast<Reply *>(p);
                                                                   reply.complete(reply.ctx, code(status));
                                                               },
                                                           .owner = reply}));
    }
    catch (const std::bad_alloc &) {
        return code(Status::resource_exhausted());
    }
}

extern "C" ct_status ct_prepared_split_overlay_try_install(ct_prepared_split_overlay *prepared, uint64_t frontier,
                                                           ct_installed_split_overlay_v1 *out)
{
    using namespace crowdb::tree;
    if (prepared == nullptr || !prepared->owner || out == nullptr || out->version != 1 ||
        out->struct_size != sizeof(*out)) {
        return code(Status::invalid_argument());
    }
    InstalledSplitOverlay installed;
    auto                  status = prepared->owner->try_install(frontier, &installed);
    if (status.ok()) {
        out->generation = installed.generation;
        out->frontier   = installed.frontier;
    }
    return code(status);
}

extern "C" int ct_prepared_split_overlay_committed_frontier(const ct_prepared_split_overlay *prepared, uint64_t *out)
{
    if (prepared != nullptr && prepared->owner && out != nullptr) {
        if (auto frontier = prepared->owner->committed_frontier()) {
            *out = *frontier;
            return 1;
        }
    }
    return 0;
}

extern "C" void ct_prepared_split_overlay_destroy(ct_prepared_split_overlay *prepared)
{
    delete prepared;
}

extern "C" ct_status ct_prepared_split_overlay_release_async(ct_prepared_split_overlay *prepared,
                                                             void (*complete)(void *), void *ctx)
{
    if (prepared == nullptr || !prepared->retirement_budget) {
        return code(Status::invalid_argument());
    }
    auto queue = prepared->retirement_budget->retirement_queue();
    if (!queue) {
        return code(Status::not_supported());
    }
    prepared->released        = complete;
    prepared->release_context = ctx;
    prepared->dispose         = [](detail::RetirementNode *node) noexcept {
        auto *handle = static_cast<ct_prepared_split_overlay *>(node);
        auto  owner  = std::move(handle->owner);
        owner->notify_released(handle->released, handle->release_context);
        delete handle;
        return true;
    };
    queue->retire(prepared);
    return 0;
}
