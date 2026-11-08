// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/index_summary.h"
#include "crowdb-tree/maptable/frame_page.h"

namespace crowdb::tree::snapshot
{
inline constexpr size_t kIndexSummaryRecordBytes = 32;

inline void encode_index_summary(uint8_t *out, uint32_t pid, const IndexSummary &summary)
{
    frame_put_u32(out, 0, pid);
    frame_put_u32(out, 4, summary.exact ? 1 : 0);
    frame_put_u64(out, 8, summary.exact ? summary.live_kv : 0);
    frame_put_u64(out, 16, summary.exact ? summary.live_key_bytes : 0);
    frame_put_u64(out, 24, summary.exact ? summary.live_value_bytes : 0);
}

inline bool decode_index_summary(const uint8_t *in, uint32_t *pid, IndexSummary *out)
{
    const auto flags  = frame_u32(in, 4);
    const auto kv     = frame_u64(in, 8);
    const auto keys   = frame_u64(in, 16);
    const auto values = frame_u64(in, 24);
    if (flags > 1 || (flags == 0 && (kv != 0 || keys != 0 || values != 0)) || keys > UINT64_MAX - values) {
        return false;
    }
    *pid = frame_u32(in, 0);
    *out = {.live_kv = kv, .live_key_bytes = keys, .live_value_bytes = values, .exact = flags != 0};
    return true;
}
} // namespace crowdb::tree::snapshot
