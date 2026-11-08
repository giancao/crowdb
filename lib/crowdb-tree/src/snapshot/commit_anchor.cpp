// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "snapshot/commit_anchor.h"

#include "crowdb-common/crc32c.h"
#include "snapshot/index_summary_codec.h"
#include "snapshot/metadata_codec.h"

#include <algorithm>

namespace crowdb::tree::snapshot
{
namespace
{
uint32_t get_u32(const uint8_t *p)
{
    uint32_t v = 0;
    for (int i = 0; i < 4; ++i) {
        v |= static_cast<uint32_t>(p[i]) << (8 * i);
    }
    return v;
}

uint64_t get_u64(const uint8_t *p)
{
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (8 * i);
    }
    return v;
}

} // namespace

void encode_anchor(const CommitAnchor &anchor, uint64_t slot_bytes, std::vector<uint8_t> *out)
{
    out->resize(slot_bytes);
    (void)encode_anchor_into(anchor, *out);
}

bool decode_anchor(std::span<const uint8_t> input, CommitAnchor *out)
{
    if (input.size() < kAnchorChecksumOffset + sizeof(uint32_t) || out == nullptr) {
        return false;
    }
    const auto *buf = input.data();
    if (get_u32(buf) != kAnchorMagic || get_u32(buf + 4) != kFormatVersion) {
        return false;
    }
    const auto stored_crc = get_u32(buf + kAnchorChecksumOffset);
    if (crowdb::common::crc32c(buf, kAnchorChecksumOffset) != stored_crc) {
        return false;
    }
    IndexSummary root_summary;
    uint32_t     has_root_summary;
    if (!decode_index_summary(buf + 92, &has_root_summary, &root_summary) || has_root_summary > 1 ||
        (has_root_summary == 0 && root_summary.exact)) {
        return false;
    }
    auto *a              = out;
    a->magic             = get_u32(buf);
    a->format_version    = get_u32(buf + 4);
    a->snapshot_seq      = get_u64(buf + 8);
    a->root_page_id      = get_u64(buf + 16);
    a->last_applied_slot = get_u64(buf + 24);
    a->next_page_id      = get_u64(buf + 32);
    a->segment_slots     = get_u32(buf + 40);
    a->segdir_addr       = get_u64(buf + 44);
    a->segdir_len        = get_u32(buf + 52);
    a->segdir_crc        = get_u32(buf + 56);
    a->leaf_count        = get_u64(buf + 60);
    a->inner_count       = get_u64(buf + 68);
    a->summaries_addr    = get_u64(buf + 76);
    a->summaries_len     = get_u32(buf + 84);
    a->summaries_crc     = get_u32(buf + 88);
    a->root_summary      = has_root_summary != 0 ? std::optional(root_summary) : std::nullopt;
    return true;
}

Status read_valid_anchors(const PageStore &store, uint32_t iu, std::vector<CommitAnchor> *out)
{
    const uint64_t slot_bytes = superblock_slot_bytes(iu);
    out->clear();
    for (uint64_t slot : {static_cast<uint64_t>(0), slot_bytes}) {
        if (slot + slot_bytes > store.size()) {
            continue;
        }
        std::vector<uint8_t> buf(slot_bytes);
        Status               status = store.read_at(slot, buf.data(), buf.size());
        if (status.code() == Code::kNotFound) {
            // A growing block file may not have written the other A/B slot yet.
            continue;
        }
        if (!status.ok()) {
            return status;
        }
        CommitAnchor a;
        if (!decode_anchor(buf, &a)) {
            continue;
        }
        out->push_back(a);
    }
    std::ranges::sort(*out, {}, &CommitAnchor::snapshot_seq);
    return Status::Ok();
}

} // namespace crowdb::tree::snapshot
