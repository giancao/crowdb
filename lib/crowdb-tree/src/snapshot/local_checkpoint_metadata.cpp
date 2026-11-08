// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "snapshot/index_summary_codec.h"
#include "snapshot/local_checkpoint_internal.h"

#include <algorithm>

namespace crowdb::tree
{
Status LocalCheckpointOperation::State::pack_frame(const DurableCapturedFrame &frame, uint64_t *word)
{
    const auto &domain     = registry->ledger_->limits();
    const auto &page_block = frame.page_block;
    if (!page_block || page_block->storage != PageBlockStorage::kDurableBlob || !page_block->reloadable() ||
        page_block->page_store_id != domain.page_store_id || page_block->generation != domain.generation ||
        page_block->address < domain.region_begin || page_block->address % iu != 0 || page_block->length % iu != 0 ||
        page_block->address > UINT64_MAX - page_block->length || page_block->address / iu > slot_word::kMaxIuIndex ||
        page_block->length / iu > slot_word::kMaxIuCount || frame.logical_bytes == 0 ||
        frame.logical_bytes > page_block->length || frame.page_id >= image->pages->next_page_id) {
        return Status::corruption();
    }
    if (frame.type == page_type::kLeafBase) {
        ++image->anchor.leaf_count;
    }
    else if (frame.type == page_type::kInnerBase) {
        ++image->anchor.inner_count;
    }
    else if (frame.type != page_type::kOverflowFrame || frame.page_id == image->pages->root_page_id) {
        return Status::corruption();
    }
    *word = slot_word::pack_unloaded(page_block->address / iu, static_cast<uint32_t>(page_block->length / iu));
    return Status::Ok();
}

void LocalCheckpointOperation::State::segment()
{
    if (position == image->pages->frames.end()) {
        finish_segments();
        return;
    }
    if (directory.size() == limits.max_segments) {
        finish(Status::resource_exhausted());
        return;
    }
    const auto index = position->first / kMappingSegmentSize;
    if (index >= kMaxMappingSegments) {
        finish(Status::invalid_argument());
        return;
    }
    if (previous && image->pages->frames.same_segment(previous->pages->frames, index)) {
        const auto &segments = previous->metadata->segments;
        const auto  found =
            std::lower_bound(segments.begin(), segments.end(), index,
                             [](const auto &segment, uint64_t number) { return segment.first.seg_idx < number; });
        if (found != segments.end() && found->first.seg_idx == index &&
            !limits.exclusion.overlaps(found->second.address, found->second.length)) {
            const auto [leaves, inners] = image->pages->frames.primary_counts(index);
            image->anchor.leaf_count += leaves;
            image->anchor.inner_count += inners;
            directory.push_back(found->first);
            image->metadata->segments.push_back(*found);
            position = image->pages->frames.lower_bound((index + 1) * kMappingSegmentSize);
            stage    = Stage::Segment;
            wake();
            return;
        }
    }
    words.fill(0);
    auto     old  = previous ? previous->pages->frames.lower_bound(index * kMappingSegmentSize)
                             : CapturedDurablePages::Frames::const_iterator{};
    bool     same = previous != nullptr;
    uint32_t live = 0;
    while (position != image->pages->frames.end() && position->first / kMappingSegmentSize == index) {
        const auto &[pid, frame] = *position;
        auto status =
            pid == frame.page_id ? pack_frame(frame, &words[pid % kMappingSegmentSize]) : Status::corruption();
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (same) {
            same = old != previous->pages->frames.end() && old->first == pid && old->second.page_block &&
                   words[pid % kMappingSegmentSize] ==
                       slot_word::pack_unloaded(old->second.page_block->address / iu,
                                                static_cast<uint32_t>(old->second.page_block->length / iu));
            if (same) {
                ++old;
            }
        }
        ++live;
        ++position;
    }
    if (same && (old == previous->pages->frames.end() || old->first / kMappingSegmentSize != index)) {
        const auto &segments = previous->metadata->segments;
        const auto  found =
            std::lower_bound(segments.begin(), segments.end(), index,
                             [](const auto &segment, uint64_t pid) { return segment.first.seg_idx < pid; });
        if (found != segments.end() && found->first.seg_idx == index &&
            !limits.exclusion.overlaps(found->second.address, found->second.length)) {
            directory.push_back(found->first);
            image->metadata->segments.push_back(*found);
            stage = Stage::Segment;
            wake();
            return;
        }
    }
    reused_segments = false;
    const MappingSegmentImageHeader header{.seg_idx    = static_cast<uint32_t>(index),
                                           .generation = image->anchor.snapshot_seq,
                                           .slot_count = kMappingSegmentSize,
                                           .live_count = live};
    segment_entry = {.seg_idx    = header.seg_idx,
                     .generation = header.generation,
                     .image_len  = static_cast<uint32_t>(segment_image_encoded_size(header.slot_count))};
    auto status   = snapshot::encode_segment_into(header, words, window->bytes, &segment_entry.image_crc);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    std::ranges::fill(window->bytes.subspan(segment_entry.image_len, segment_bytes - segment_entry.image_len),
                      uint8_t{0});
    reserve(segment_bytes, Stage::SegmentReserved);
}

void LocalCheckpointOperation::State::finish_segments()
{
    image->anchor.root_summary = image->pages->root_summary;
    const bool reuse_summaries = previous && image->pages->index_summaries.same(previous->pages->index_summaries) &&
                                 !limits.exclusion.overlaps(previous->summaries.address, previous->summaries.length);
    if (reuse_summaries) {
        image->summaries             = previous->summaries;
        image->anchor.summaries_addr = previous->anchor.summaries_addr;
        image->anchor.summaries_len  = previous->anchor.summaries_len;
        image->anchor.summaries_crc  = previous->anchor.summaries_crc;
    }
    if (previous && reused_segments && directory.size() == previous->metadata->segments.size() &&
        snapshot::DirectoryEncoder(directory).logical_bytes() == previous->anchor.segdir_len &&
        !limits.exclusion.overlaps(previous->anchor.segdir_addr, round_up_to_iu(previous->anchor.segdir_len, iu))) {
        const auto &pages = *image->pages;
        const auto &old   = *previous->pages;
        unchanged         = reuse_summaries && image->anchor.root_summary == previous->anchor.root_summary &&
                            !(limits.retire_previous_anchor && registry->needs_anchor_retirement()) &&
                            pages.coverage == old.coverage && pages.generation == old.generation &&
                            pages.root_page_id == old.root_page_id && pages.next_page_id == old.next_page_id &&
                            image->anchor.leaf_count == previous->anchor.leaf_count &&
                            image->anchor.inner_count == previous->anchor.inner_count;
        if (unchanged) {
            finish(Status::Ok());
            return;
        }
        image->metadata           = previous->metadata;
        image->anchor.segdir_addr = previous->anchor.segdir_addr;
        image->anchor.segdir_len  = previous->anchor.segdir_len;
        image->anchor.segdir_crc  = previous->anchor.segdir_crc;
        prepare_summaries();
        return;
    }
    encoder.emplace(directory);
    image->anchor.segdir_len = encoder->logical_bytes();
    reserve(round_up_to_iu(encoder->logical_bytes(), iu), Stage::DirectoryReserved);
}

void LocalCheckpointOperation::State::prepare_summaries()
{
    const auto count = image->pages->index_summaries.size();
    if (count == 0 || image->summaries.owner) {
        io(true, 0, 0, Stage::BeforeAnchor);
        return;
    }
    if (count > UINT32_MAX / snapshot::kIndexSummaryRecordBytes) {
        finish(Status::resource_exhausted("index summary length exceeds the anchor field"));
        return;
    }
    image->anchor.summaries_len = static_cast<uint32_t>(count * snapshot::kIndexSummaryRecordBytes);
    reserve(round_up_to_iu(image->anchor.summaries_len, iu), Stage::SummariesReserved);
}

void LocalCheckpointOperation::State::summaries_window()
{
    struct Output
    {
        std::span<uint8_t> bytes;
        size_t             written = 0;
    } output{.bytes = window->bytes};

    image->pages->index_summaries.visit_from(
        summary_pid, &output, +[](void *raw, uint32_t pid, IndexSummary summary) {
            auto &out = *static_cast<Output *>(raw);
            if (out.bytes.size() - out.written < snapshot::kIndexSummaryRecordBytes) {
                return false;
            }
            snapshot::encode_index_summary(out.bytes.data() + out.written, pid, summary);
            out.written += snapshot::kIndexSummaryRecordBytes;
            return true;
        });
    if (output.written == 0) {
        finish(Status::internal_error("missing index summaries"));
        return;
    }
    image->anchor.summaries_crc =
        common::crc32c_update(image->anchor.summaries_crc, output.bytes.data(), output.written);
    const auto bytes = round_up_to_iu(output.written, iu);
    std::ranges::fill(output.bytes.subspan(output.written, bytes - output.written), uint8_t{0});
    const auto address = image->anchor.summaries_addr + summary_offset;
    summary_offset += output.written;
    io(false, address, bytes, Stage::SummariesWritten);
}

void LocalCheckpointOperation::State::directory_window()
{
    if (!encoder) {
        finish(Status::internal_error());
        return;
    }
    size_t written = 0;
    auto   status  = encoder->next(window->bytes, &written);
    if (!status.ok() || written == 0) {
        finish(status.ok() ? Status::corruption() : std::move(status));
        return;
    }
    const auto address = directory_address + directory_offset;
    const auto bytes   = round_up_to_iu(written, iu);
    std::ranges::fill(window->bytes.subspan(written, bytes - written), uint8_t{0});
    directory_offset += bytes;
    io(false, address, bytes, Stage::DirectoryWritten);
}

void LocalCheckpointOperation::State::anchor()
{
    auto status = snapshot::encode_anchor_into(image->anchor, window->bytes);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    const auto slot_bytes = snapshot::superblock_slot_bytes(iu);
    const auto address    = image->anchor.snapshot_seq % 2 == 0 ? slot_bytes : 0;
    io(false, address, slot_bytes, Stage::AnchorWritten, true);
}

void LocalCheckpointOperation::State::reserve(uint64_t length, Stage next)
{
    stage   = next;
    waiting = true;
    event_ready.store(false, std::memory_order_release);
    auto status =
        registry->ledger_->reserve(length, budget.bytes, [self = shared_from_this()](ExtentLedgerResult result) {
            self->reserved = std::move(result.extent);
            self->event(std::move(result.status));
        });
    if (!status.ok()) {
        event(std::move(status));
    }
}
} // namespace crowdb::tree
