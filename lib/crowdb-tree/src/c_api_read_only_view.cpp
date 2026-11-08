// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/runtime_range_rebuild.h"
#include "btree/tree_core_access.h"
#include "c_api_future_internal.h"
#include "c_api_internal.h"
#include "c_api_owned_budget.h"
#include "c_api_read_only_view_internal.h"
#include "crowdb-common/log.h"

using namespace crowdb::tree;

namespace
{
ct_range_rebuild_stats rebuild_stats(const RangeRebuildStats &value) noexcept
{
    return {.entries_examined = value.entries_examined,
            .entries_emitted  = value.entries_emitted,
            .entries_filtered = value.entries_filtered,
            .pages_reused     = value.pages_reused,
            .pages_rebuilt    = value.pages_rebuilt,
            .subtrees_skipped = value.subtrees_skipped};
}

struct ViewRebuild : std::enable_shared_from_this<ViewRebuild>
{
    using Complete = void (*)(void *, ct_status, ct_range_rebuild_stats);

    ViewRebuild(std::shared_ptr<TreeCore> core, Complete complete, void *context)
        : core(std::move(core)),
          complete(complete),
          context(context)
    {
    }

    Status start(const std::shared_ptr<PublishedReadViewContext> &view)
    {
        if (!view->detached()) {
            return Status::not_supported("asynchronous range rebuild requires owned source page_block");
        }
        return copy_read_only_view_range_async(
            view, *core, core->config().key_range,
            [owner = shared_from_this()](Status status, RuntimeRangeImport &staging) {
                owner->copied(std::move(status), staging);
            });
    }

    void source_flushed(TreeCore &source, uint64_t frontier, Status status) noexcept
    try {
        if (status.ok()) {
            std::shared_ptr<PublishedReadViewContext> view;
            status = source.capture_read_only_view(&view);
            if (status.ok()) {
                status = view->view()->coverage >= frontier
                           ? start(view)
                           : Status::unavailable("source frontier changed during range rebuild");
            }
        }
        if (!status.ok()) {
            finish(std::move(status));
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    void copied(Status status, RuntimeRangeImport &staging) noexcept
    try {
        if (status.ok()) {
            stats       = rebuild_stats(staging.stats);
            auto credit = std::move(staging.credit);
            status      = core->install_readable_async(
                std::move(staging.catalog), credit,
                {.context = this, .complete_fn = &ViewRebuild::installed, .owner = weak_from_this().lock()});
        }
        if (!status.ok()) {
            finish(std::move(status));
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    static void installed(void *context, Status status) noexcept
    {
        auto &self = *static_cast<ViewRebuild *>(context);
        try {
            if (status.ok()) {
                self.core->checkpoint_async([owner = self.weak_from_this().lock()](Status result, uint64_t) {
                    owner->finish(std::move(result));
                });
                return;
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        self.finish(std::move(status));
    }

    void finish(Status status) noexcept
    {
        complete(context, static_cast<ct_status>(status.code()), status.ok() ? stats : ct_range_rebuild_stats{});
    }

    std::shared_ptr<TreeCore> core;
    Complete                  complete = nullptr;
    void                     *context  = nullptr;
    ct_range_rebuild_stats    stats{};
};
} // namespace

ct_status ct_read_only_view_capture(ct_tree *tree, uint64_t expected_sequence, uint64_t expected_coverage,
                                    ct_read_only_view **out)
try {
    if (tree == nullptr || out == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    *out         = nullptr;
    auto *facade = c_api_tree(tree);
    auto *core   = TreeCoreAccess::get(*facade);
    if (core == nullptr) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    const auto matches = [&] {
        return expected_sequence == 0 ||
               (facade->durable_sequence() == expected_sequence && facade->durable_coverage() == expected_coverage);
    };
    if (!matches()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    std::shared_ptr<PublishedReadViewContext> context;
    auto                                      status = core->capture_read_only_view(&context);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    if (!matches() || context->view()->coverage != expected_coverage || !context->detached()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    *out = new ct_read_only_view(std::move(context));
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

void ct_read_only_view_free(ct_read_only_view *view)
{
    delete view;
}

ct_status ct_read_only_view_close_async(ct_read_only_view *view, void (*complete)(void *), void *context)
{
    if (view == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto owner = view->context;
    return static_cast<ct_status>(owner->close_async(complete, context).code());
}

void c_api_complete_view_read(const std::shared_ptr<ct_future_impl> &value, const Status &status,
                              ct_read_only_view_read_fn complete, void *context)
{
    auto       code   = static_cast<ct_status>(status.code());
    ct_future *future = nullptr;
    if (status.ok()) {
        value->done.store(true, std::memory_order_release);
        try {
            future = reinterpret_cast<ct_future *>(make_ct_future_handle(value));
        }
        catch (const std::bad_alloc &) {
            code = static_cast<ct_status>(Code::kResourceExhausted);
        }
    }
    complete(context, code, future);
}

ct_status ct_read_only_view_get_async(ct_read_only_view *view, const uint8_t *key, size_t size,
                                      ct_read_only_view_read_fn complete, void *context)
try {
    if (view == nullptr || complete == nullptr || (key == nullptr && size != 0)) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    if (view->context->closed()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    const auto owner       = view->context;
    auto       value       = make_ct_future(owner->binding()->lifecycle_budget());
    value->result_controls = c_api_result_controls(owner->binding()->account());
    auto status =
        owner->get_async(Slice(key, size), [value = std::move(value), complete, context](Status result, GetView view) {
            value->get_result = std::move(view);
            c_api_complete_view_read(value, result, complete, context);
        });
    return static_cast<ct_status>(status.code());
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_read_only_view_rebuild(ct_read_only_view *view, ct_tree *destination, ct_range_rebuild_stats *stats)
try {
    if (view == nullptr || destination == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    if (view->context->closed()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    auto *tree = c_api_tree(destination);
    auto *core = TreeCoreAccess::get(*tree);
    if (core == nullptr) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    RuntimeRangeImport staging;
    const char        *phase  = "range copy";
    auto               status = copy_read_only_view_range(view->context, *core, core->config().key_range, staging);
    if (status.ok()) {
        phase  = "range installation";
        status = core->install_readable(std::move(staging.catalog), staging.credit);
    }
    if (status.ok()) {
        phase  = "flush with persistent";
        status = tree->flush_with_persistent();
    }
    if (status.ok() && stats != nullptr) {
        *stats = rebuild_stats(staging.stats);
    }
    if (!status.ok()) {
        CRB_LOG_WARN("read-only view {} failed: {}", phase, status.to_string());
    }
    return static_cast<ct_status>(status.code());
}

catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (const std::exception &error) {
    CRB_LOG_ERROR("read-only view range rebuild threw: {}", error.what());
    return static_cast<ct_status>(Code::kInternal);
}
catch (...) {
    CRB_LOG_ERROR("read-only view range rebuild threw an unknown exception");
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_read_only_view_rebuild_async(ct_read_only_view *view, ct_tree *destination, ViewRebuild::Complete complete,
                                          void *context)
try {
    if (view == nullptr || destination == nullptr || complete == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto *facade = c_api_tree(destination);
    auto *core   = facade == nullptr ? nullptr : TreeCoreAccess::get(*facade);
    if (core == nullptr) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    auto rebuild = detail::reserved_shared<ViewRebuild>(core->binding()->lifecycle_budget(), core->shared_from_this(),
                                                        complete, context);
    return static_cast<ct_status>(rebuild->start(view->context).code());
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_status ct_rebuild_bounded_range_async(ct_tree *source, ct_tree *destination, ViewRebuild::Complete complete,
                                         void *context)
try {
    if (source == nullptr || destination == nullptr || source == destination || complete == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto *source_tree      = c_api_tree(source);
    auto *destination_tree = c_api_tree(destination);
    auto *parent           = source_tree == nullptr ? nullptr : TreeCoreAccess::get(*source_tree);
    auto *child            = destination_tree == nullptr ? nullptr : TreeCoreAccess::get(*destination_tree);
    if (parent == nullptr || child == nullptr || !child->config().key_range.is_bounded()) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    const auto                                frontier = parent->contiguous_slot();
    std::shared_ptr<PublishedReadViewContext> view;
    auto                                      status = parent->capture_read_only_view(&view);
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    if (!view->detached()) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
    auto rebuild = detail::reserved_shared<ViewRebuild>(child->binding()->lifecycle_budget(), child->shared_from_this(),
                                                        complete, context);
    if (view->view()->coverage >= frontier) {
        return static_cast<ct_status>(rebuild->start(view).code());
    }
    view.reset();
    parent->flush_async([parent = parent->shared_from_this(), rebuild = std::move(rebuild), frontier](
                            Status status) noexcept { rebuild->source_flushed(*parent, frontier, std::move(status)); });
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
