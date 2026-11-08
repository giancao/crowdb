// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"
#include "snapshot/chunk_root_commit.h"

namespace crowdb::tree
{
bool ChunkRootCommit::unchanged(std::span<const DurableCapturedFrame *const> frames,
                                const ChunkMappingSegmentRef                &index) const
{
    if (!baseline_ || frames.size() != index.frame_count || frames.front()->page_id != index.first_page ||
        frames.back()->page_id != index.last_page) {
        return false;
    }
    for (const auto *frame : frames) {
        const auto old = baseline_->frames.find(frame->page_id);
        if (old == baseline_->frames.end() || !captured_write_detail::same_durable_frame(old->second, *frame)) {
            return false;
        }
        const auto &a = old->second.stored_overflow;
        const auto &b = frame->stored_overflow;
        if (a == b) {
            continue;
        }
        if (!a || !b || a->size() != b->size() ||
            !std::equal(a->begin(), a->end(), b->begin(),
                        [](const auto &x, const auto &y) { return x.head == y.head && x.bytes == y.bytes; })) {
            return false;
        }
    }
    return true;
}

Status ChunkRootCommit::reuse_index(const ChunkMappingSegmentRef          &index,
                                    std::shared_ptr<const ChunkPageBlock> *page_block)
{
    detail::ChunkStageId pin;
    detail::ChunkId      unused;
    auto                 status = ids_->next(PagePurpose::kPageIndex, &pin, &unused);
    return status.ok() ? pin_chunk_page_block(domain_, index.pack, pin, metadata_, page_block) : status;
}

Status ChunkRootCommit::write_index(std::span<const DurableCapturedFrame *const> frames,
                                    std::shared_ptr<const ChunkPageBlock>       *page_block)
{
    ChunkMetadataBytes bytes{detail::ReservationAllocator<uint8_t>(window_)};
    auto               status = encode_chunk_mapping_segment(root_.identity, root_.indexes.size(), frames, &bytes);
    if (!status.ok()) {
        return status;
    }
    detail::ChunkStageId id;
    detail::ChunkId      chunk;
    status = ids_->next(PagePurpose::kPageIndex, &id, &chunk);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<ChunkPageBlock> written;
    status = prepare_chunk_page_block(domain_, id, chunk, static_cast<uint32_t>(bytes.size()), PagePurpose::kPageIndex,
                                      metadata_, &written);
    if (status.ok()) {
        status = write_chunk_page_block(*written, bytes, window_);
    }
    if (status.ok()) {
        *page_block = std::move(written);
    }
    return status;
}
} // namespace crowdb::tree
