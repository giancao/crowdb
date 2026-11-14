// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "c_api_future_internal.h"
#include "c_api_internal.h"
#include "c_api_owned_budget.h"
#include "c_api_read_only_view_internal.h"

using namespace crowdb::tree;

ct_status ct_read_only_view_scan_async(ct_read_only_view *view, const ct_scan_options_v2 *options,
                                       ct_read_only_view_read_fn complete, void *context)
try {
    if (view == nullptr || complete == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    CoreScanOptions scan;
    if (const auto status = c_api_scan_options(options, &scan); status != 0) {
        return status;
    }
    const auto owner = view->context;
    if (owner->closed()) {
        return static_cast<ct_status>(Code::kUnavailable);
    }
    auto value             = make_ct_future(owner->binding()->lifecycle_budget());
    value->kind            = ct_future_impl::Kind::kScan;
    value->result_controls = c_api_result_controls(owner->binding()->account());
    auto status = owner->scan_async(scan, [value = std::move(value), complete,
                                           context](Status result, ScanPackedBuf packed, size_t count, bool truncated) {
        value->scan_count     = count;
        value->scan_packed    = std::move(packed);
        value->scan_truncated = truncated;
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
