// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_geometry.h"

#include "btree/allocation_bound.h"
#include "btree/page_maintenance.h"
#include "crowdb-tree/maptable/compressor.h"
#include "crowdb-tree/maptable/frame_page.h"

#include <algorithm>
#include <bit>
#include <limits>

namespace crowdb::tree
{
namespace
{
constexpr size_t kOverhead = kFrameHeaderSize + kFrameTrailerSize;
}

size_t leaf_record_capacity(uint32_t frame_bytes) noexcept
{
    const size_t bytes = std::min<size_t>(frame_bytes, kMaxPhysicalPageBytes);
    return bytes > kOverhead ? bytes - kOverhead : 0;
}

size_t page_frame_bytes(const Config &config) noexcept
{
    return config.frame_bytes > kDurableBlobHeader ? config.frame_bytes - kDurableBlobHeader : 0;
}

size_t physical_frame_capacity(const Config &config) noexcept
{
    const auto key      = config.max_key_size != 0 ? config.max_key_size : config.frame_bytes / 2;
    const auto value    = config.max_inline_value != 0 ? config.max_inline_value : config.frame_bytes / 4;
    const auto required = detail::AllocationBound(kOverhead + kLeafSlotSize + kCellHeaderSize)
                              .add(key)
                              .add(std::max(value, size_t{kOverflowCellSize}))
                              .value();
    // An oversized page keeps the same rule: header plus frame is a power of two.
    if (!required || *required >= kMaxPhysicalPageBytes - kDurableBlobHeader) {
        return kMaxPhysicalPageBytes - kDurableBlobHeader;
    }
    const auto frame = page_frame_bytes(config);
    return *required > frame ? std::bit_ceil(*required + kDurableBlobHeader) - kDurableBlobHeader : frame;
}

Status configure_leaf_geometry(const Config &config, PageMaintenanceLimits *pages)
{
    const size_t capacity = leaf_record_capacity(static_cast<uint32_t>(page_frame_bytes(config)));
    if (pages == nullptr || capacity == 0 || config.leaf_split_bytes == 0 ||
        config.leaf_split_bytes > std::numeric_limits<size_t>::max() - kOverhead ||
        config.leaf_merge_bytes > std::numeric_limits<size_t>::max() - kOverhead) {
        return Status::invalid_argument("leaf record thresholds exceed bounded frame geometry");
    }
    const size_t split         = std::min(config.leaf_split_bytes, capacity);
    const size_t merge         = split - std::max<size_t>(1, split / 4);
    pages->primary_page_bytes  = capacity + kOverhead;
    pages->target_page_bytes   = split + kOverhead;
    pages->merge_target_bytes  = merge + kOverhead;
    pages->merge_trigger_bytes = std::min(config.leaf_merge_bytes, merge) + kOverhead;
    return Status::Ok();
}
} // namespace crowdb::tree
