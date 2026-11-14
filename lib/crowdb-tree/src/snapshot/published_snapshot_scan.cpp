// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_scan_internal.h"
#include "host_completion.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
namespace
{
Status prepare_view_scan(const PublishedReadViewContext &view, CoreScanOptions options,
                         std::shared_ptr<core_read_detail::CoreScan> *out, std::shared_ptr<TreeCore> *host_core)
{
    if (options.strict_byte_budget && options.byte_budget == 0) {
        return Status::invalid_argument("read-only view scan requires a byte budget");
    }
    auto limits = view.read_limits();
    auto status = core_read_detail::scan_limits(options, &limits);
    if (!status.ok()) {
        return status;
    }
    const auto root = view.view()->mapping.get(view.view()->root_page_id);
    if (!root || root->level >= limits.max_route_depth) {
        return Status::corruption("read-only view scan has no bounded root");
    }
    limits.max_route_depth = root->level + 1ULL;
    SnapshotReadWindow window;
    status = view.prepare_window(&window);
    if (!status.ok()) {
        return status;
    }
    if (host_core != nullptr) {
        *host_core = std::move(window.host_core);
    }
    limits.metadata = window.budget.bytes;
    core_read_detail::ReadPreparation ready;
    ready.credit       = std::move(window.budget.bytes);
    ready.ticket       = std::move(window.ticket);
    ready.binding      = view.binding();
    ready.view         = view.view();
    ready.loader       = std::move(window.loader);
    ready.limits       = std::move(limits);
    auto        range  = core_read_detail::scan_range(options, ready.credit);
    const auto &domain = view.range();
    if (domain.lower && (!range.lower || *domain.lower > *range.lower)) {
        range.lower = domain.lower;
    }
    if (domain.upper && (!range.upper || *domain.upper < *range.upper)) {
        range.upper = domain.upper;
    }
    status = core_read_detail::prepare_scan(options, std::move(ready), std::move(range),
                                            view.load_limits().max_value_bytes, out);
    if (status.ok()) {
        (*out)->lane = view.lane();
    }
    return status;
}
} // namespace

Status PublishedReadViewContext::scan_async(CoreScanOptions options, ScanComplete complete) const
try {
    if (!complete) {
        return Status::invalid_argument("read-only view scan needs a completion");
    }
    if (!detached() || closed()) {
        return Status::unavailable("read-only view is closed or borrows its backend");
    }
    std::shared_ptr<core_read_detail::CoreScan> operation;
    auto                                        status = prepare_view_scan(*this, options, &operation, nullptr);
    if (status.ok()) {
        operation->complete = std::move(complete);
        operation->owner    = operation;
        operation->schedule();
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("read-only view scan allocation failed");
}

Status PublishedReadViewContext::scan(CoreScanOptions options, ScanPackedBuf *out, size_t *count, bool *truncated) const
try {
    if (out == nullptr || count == nullptr || truncated == nullptr) {
        return Status::invalid_argument("read-only view scan needs output");
    }
    *out       = {};
    *count     = 0;
    *truncated = false;
    std::shared_ptr<TreeCore>                   host_core;
    std::shared_ptr<core_read_detail::CoreScan> operation;
    auto                                        status = prepare_view_scan(*this, options, &operation, &host_core);
    if (!status.ok()) {
        return status;
    }
    detail::HostCompletion done;
    operation->complete = [&](Status outcome, ScanPackedBuf packed, size_t entries, bool more) {
        status = std::move(outcome);
        if (status.ok()) {
            *out       = std::move(packed);
            *count     = entries;
            *truncated = more;
        }
        done.signal();
    };
    operation->owner = operation;
    core_read_detail::CoreScan::step(operation.get());
    done.wait();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("read-only view scan allocation failed");
}
} // namespace crowdb::tree
