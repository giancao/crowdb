// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/page_writeback.h"

namespace crowdb::tree
{
struct StoredOverflowReference
{
    uint64_t head  = kInvalidPageId;
    uint64_t bytes = 0;
};

using StoredOverflowReferences = detail::ReservedVector<StoredOverflowReference>;

struct ReadablePageFrame
{
    uint64_t                            incoming_references = 0;
    std::weak_ptr<const PageBlockRef>   resident_source     = std::shared_ptr<const PageBlockRef>{};
    uint64_t                            page_id             = kInvalidPageId;
    page_type                           type                = page_type::kLeafBase;
    uint32_t                            frame_bytes         = 0;
    uint32_t                            logical_bytes       = 0;
    uint64_t                            next_overflow       = kInvalidPageId;
    uint32_t                            overflow_bytes      = 0;
    std::shared_ptr<const PageBlockRef> page_block;
    // Every encoded leaf cell with an overflow reference.
    // An empty owned list proves no stored overflow references; null is unknown.
    std::shared_ptr<const StoredOverflowReferences> stored_overflow = nullptr;
    std::optional<FinalPageSummary>                 summary         = std::nullopt;
};

using DurableCapturedFrame = ReadablePageFrame;

} // namespace crowdb::tree
