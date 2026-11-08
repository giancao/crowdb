// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/stored_overflow.h"

namespace crowdb::tree
{
Status capture_stored_overflow(const LeafFrameView &leaf, size_t max_references,
                               const std::shared_ptr<AllocationBudget>         &metadata,
                               std::shared_ptr<const StoredOverflowReferences> *out)
{
    size_t references = 0;
    for (uint32_t i = 0; i < leaf.count(); ++i) {
        const Slice    raw = leaf.cell(i);
        const CellView cell(raw);
        if (!cell.valid()) {
            return Status::corruption();
        }
        if (cell.is_overflow()) {
            if (raw.size() != kOverflowCellSize || cell.overflow_head() == kInvalidPageId || cell.overflow_len() == 0) {
                return Status::corruption();
            }
            if (references == max_references) {
                return Status::resource_exhausted();
            }
            ++references;
        }
    }
    auto result = detail::reserved_shared<StoredOverflowReferences>(
        metadata, detail::ReservationAllocator<StoredOverflowReference>(metadata));
    result->reserve(references);
    for (uint32_t i = 0; i < leaf.count(); ++i) {
        const CellView cell(leaf.cell(i));
        if (cell.is_overflow()) {
            result->push_back({.head = cell.overflow_head(), .bytes = cell.overflow_len()});
        }
    }
    *out = std::move(result);
    return Status::Ok();
}
} // namespace crowdb::tree
