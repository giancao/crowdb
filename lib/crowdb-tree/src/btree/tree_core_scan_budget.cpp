// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_scan_internal.h"

#include <algorithm>

namespace crowdb::tree::core_read_detail
{
PageKeyRange scan_range(CoreScanOptions options, const std::shared_ptr<AllocationBudget> &credit)
{
    PageKeyRange range;
    if (!options.prefix.empty()) {
        range.lower = OwnedKey(options.prefix, credit);
        KeyArenaStorage upper(options.prefix.data(), options.prefix.size(), detail::ReservationAllocator<char>(credit));
        while (!upper.empty() && static_cast<uint8_t>(upper.back()) == 255) {
            upper.pop_back();
        }
        if (!upper.empty()) {
            upper.back() = static_cast<char>(static_cast<uint8_t>(upper.back()) + 1);
            range.upper  = OwnedKey(Slice(upper.data(), upper.size()), credit);
        }
    }
    if (!options.begin.empty() && (!range.lower || options.begin > Slice(*range.lower))) {
        range.lower = OwnedKey(options.begin, credit);
    }
    if (!options.end.empty() && (!range.upper || options.end < Slice(*range.upper))) {
        range.upper = OwnedKey(options.end, credit);
    }
    return range;
}

Status scan_limits(CoreScanOptions options, PublishedReadLimits *limits)
{
    for (const auto key : {options.prefix, options.start, options.end, options.begin}) {
        if (key.size() > limits->max_key_bytes) {
            return Status::invalid_argument("scan boundary exceeds the key limit");
        }
    }
    const size_t count        = options.limit == 0 ? SIZE_MAX : options.limit;
    const size_t bytes        = options.byte_budget == 0 ? SIZE_MAX : options.byte_budget;
    const auto   window_bytes = std::min(bytes, limits->max_result_bytes);
    // Keys are unique: all except the one possible empty key cost at least one
    // byte. A tiny byte window cannot contain more than bytes + 1 results;
    // the oversized-first exception contributes only one result.
    const auto byte_count    = detail::AllocationBound(window_bytes).add(1).value().value_or(SIZE_MAX);
    limits->max_entries      = std::max<size_t>(1, std::min({count, limits->max_entries, byte_count}));
    limits->max_result_bytes = window_bytes;
    return Status::Ok();
}

Status prepare_scan(CoreScanOptions options, ReadPreparation ready, PageKeyRange range, size_t value_limit,
                    std::shared_ptr<CoreScan> *out)
{
    const size_t bytes            = options.byte_budget == 0 ? SIZE_MAX : options.byte_budget;
    auto         scan             = detail::reserved_shared<CoreScan>(ready.credit);
    scan->prepared                = std::move(ready);
    auto                &prepared = scan->prepared;
    PublishedScanOptions scan_options{.start_inclusive       = options.start_inclusive,
                                      .keys_only             = options.keys_only,
                                      .include_tombstones    = options.include_tombstones,
                                      .allow_oversized_first = !options.strict_byte_budget,
                                      .byte_budget           = bytes};
    if (options.has_start_bound || !options.start.empty()) {
        scan_options.start = OwnedKey(options.start, prepared.credit);
    }
    auto status = PublishedScan::create(prepared.view, prepared.loader, std::move(range), options.reverse,
                                        prepared.limits, &scan->read, std::move(scan_options));
    if (!status.ok()) {
        return status;
    }
    scan->limit       = options.limit == 0 ? SIZE_MAX : options.limit;
    scan->byte_limit  = bytes;
    scan->value_limit = value_limit;
    scan->deadline_ms = options.deadline_ms;
    scan->strict      = options.strict_byte_budget;
    scan->packed      = ScanPackedBuf(prepared.credit);
    *out              = std::move(scan);
    return Status::Ok();
}

Status prepare_scan(TreeCore &core, CoreScanOptions options, std::shared_ptr<CoreScan> *out)
{
    auto limits = core.read_limits();
    auto status = scan_limits(options, &limits);
    if (!status.ok()) {
        return status;
    }
    ReadPreparation ready;
    status = prepare(core, core.capture(), limits, &ready);
    if (!status.ok()) {
        return status;
    }
    auto        range  = scan_range(options, ready.credit);
    const auto &domain = core.config().key_range;
    if (domain.start() && (!range.lower || Slice(*domain.start()) > Slice(*range.lower))) {
        range.lower = OwnedKey(Slice(*domain.start()), ready.credit);
    }
    if (domain.end() && (!range.upper || Slice(*domain.end()) < Slice(*range.upper))) {
        range.upper = OwnedKey(Slice(*domain.end()), ready.credit);
    }
    status = prepare_scan(options, std::move(ready), std::move(range), core.load_limits().max_value_bytes, out);
    if (status.ok()) {
        (*out)->lane     = core.binding()->lane();
        (*out)->progress = core.cpu_progress();
    }
    return status;
}
} // namespace crowdb::tree::core_read_detail
