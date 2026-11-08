// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/maptable/mapping_slot.h"
#include "snapshot/index_summary_codec.h"
#include "snapshot/local_recovery_internal.h"

#include <algorithm>

namespace crowdb::tree
{
void LocalRecoveryOperation::State::next_anchor()
{
    if (slot == anchors.size()) {
        stage   = Stage::Activated;
        waiting = true;
        event_ready.store(false, std::memory_order_release);
        auto status = ledger->activate(budget.bytes, [self = shared_from_this()](ExtentLedgerResult result) {
            self->event(std::move(result.status));
        });
        if (!status.ok()) {
            event(std::move(status));
        }
        return;
    }
    const auto bytes = snapshot::superblock_slot_bytes(domain.iu_size);
    if ((slot + 1) * bytes > readable_bytes) {
        ++slot;
        wake();
        return;
    }
    read(slot * bytes, bytes, Stage::AnchorRead);
}

void LocalRecoveryOperation::State::anchor_read()
{
    const auto bytes = window->bytes.first(read_bytes);
    if (event_status.code() != Code::kNotFound && bytes.size() >= 2 * sizeof(uint32_t) &&
        frame_u32(bytes.data(), 0) == snapshot::kAnchorMagic &&
        frame_u32(bytes.data(), sizeof(uint32_t)) != snapshot::kFormatVersion) {
        finish(Status::not_supported("unsupported local anchor format"));
        return;
    }
    snapshot::CommitAnchor anchor;
    if (event_status.code() == Code::kNotFound || !snapshot::decode_anchor(bytes, &anchor)) {
        ++slot;
        stage = Stage::NextAnchor;
        wake();
        return;
    }
    if (anchor.snapshot_seq == 0 || (anchor.snapshot_seq % 2 == 0 ? 1U : 0U) != slot ||
        anchor.segment_slots != kMappingSegmentSize ||
        anchor.next_page_id > kMappingSegmentSize * kMaxMappingSegments || anchor.segdir_len < 20 ||
        (anchor.segdir_len - 20) % 32 != 0 || (anchor.segdir_len - 20) / 32 > limits.max_segments) {
        finish(Status::corruption());
        return;
    }
    image                = detail::reserved_shared<ProtectedCheckpoint>(budget.bytes, budget.bytes);
    image->anchor        = anchor;
    pages                = detail::reserved_shared<CapturedDurablePages>(budget.bytes, budget.bytes);
    pages->root_page_id  = anchor.root_page_id;
    pages->next_page_id  = anchor.next_page_id;
    pages->coverage      = anchor.last_applied_slot;
    pages->root_summary  = anchor.root_summary;
    pages->generation    = anchor.snapshot_seq;
    pages->page_store_id = domain.page_store_id;
    directory_offset     = 0;
    segment_index        = 0;
    summaries_offset     = 0;
    summaries_crc        = 0;
    if (anchor.summaries_len == 0) {
        if (anchor.summaries_addr != 0 || anchor.summaries_crc != 0) {
            finish(Status::corruption("unexpected empty index summary reference"));
            return;
        }
        protect_directory();
        return;
    }
    if (anchor.summaries_len % snapshot::kIndexSummaryRecordBytes != 0 ||
        anchor.summaries_len / snapshot::kIndexSummaryRecordBytes >= anchor.inner_count ||
        anchor.inner_count > limits.max_frames) {
        finish(Status::corruption("invalid index summary length"));
        return;
    }
    protect(anchor.summaries_addr, round_up_to_iu(anchor.summaries_len, domain.iu_size), Stage::SummariesProtected);
}

void LocalRecoveryOperation::State::protect_directory()
{
    const auto &anchor = image->anchor;
    protect(anchor.segdir_addr, round_up_to_iu(anchor.segdir_len, domain.iu_size), Stage::DirectoryProtected);
}

void LocalRecoveryOperation::State::read_summaries()
{
    const auto &anchor = image->anchor;
    const auto  bytes =
        std::min<uint64_t>(metadata_window, round_up_to_iu(anchor.summaries_len, domain.iu_size) - summaries_offset);
    read(anchor.summaries_addr + summaries_offset, bytes, Stage::SummariesRead);
}

void LocalRecoveryOperation::State::summaries_read()
{
    const auto &anchor  = image->anchor;
    const auto  logical = std::min<uint64_t>(read_bytes, anchor.summaries_len - summaries_offset);
    summaries_crc       = common::crc32c_update(summaries_crc, window->bytes.data(), logical);
    for (size_t offset = 0; offset < logical; offset += snapshot::kIndexSummaryRecordBytes) {
        uint32_t     pid;
        IndexSummary summary;
        if (!snapshot::decode_index_summary(window->bytes.data() + offset, &pid, &summary) ||
            pid >= anchor.next_page_id || pid == anchor.root_page_id ||
            (!summaries.empty() && pid <= summaries.back().pid)) {
            finish(Status::corruption("invalid index summary record"));
            return;
        }
        summaries.push_back({.pid = pid, .summary = summary});
    }
    summaries_offset += logical;
    if (summaries_offset != anchor.summaries_len) {
        read_summaries();
        return;
    }
    if (summaries_crc != anchor.summaries_crc) {
        finish(Status::corruption("invalid index summary checksum"));
        return;
    }
    pages->index_summaries = IndexSummaries{}.merge(summaries, budget.bytes);
    summaries.clear();
    protect_directory();
}

void LocalRecoveryOperation::State::read_directory()
{
    const auto &anchor = image->anchor;
    const auto  bytes =
        std::min<uint64_t>(metadata_window, round_up_to_iu(anchor.segdir_len, domain.iu_size) - directory_offset);
    read(anchor.segdir_addr + directory_offset, bytes, Stage::DirectoryRead);
}

void LocalRecoveryOperation::State::directory_read()
{
    const auto &anchor = image->anchor;
    if (directory_offset == 0) {
        uint32_t count  = 0;
        auto     status = snapshot::directory_entry_count(window->bytes.first(read_bytes), anchor.segdir_len, &count);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        directory.resize(count);
        decoder.emplace(directory, anchor.segdir_len);
    }
    if (!decoder) {
        finish(Status::internal_error());
        return;
    }
    const auto logical = std::min<size_t>(read_bytes, anchor.segdir_len - directory_offset);
    auto       status  = decoder->next(window->bytes.first(logical));
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    directory_offset += read_bytes;
    if (!decoder->complete()) {
        read_directory();
        return;
    }
    if (!decoder->finish().ok() || decoder->crc() != anchor.segdir_crc) {
        finish(Status::corruption());
        return;
    }
    stage = Stage::Segment;
    wake();
}

void LocalRecoveryOperation::State::segment()
{
    if (segment_index == directory.size()) {
        frame_position = pages->frames.begin();
        stage          = Stage::NextFrame;
        wake();
        return;
    }
    const auto &entry = directory[segment_index];
    if (entry.seg_idx >= kMaxMappingSegments ||
        (segment_index > 0 && directory[segment_index - 1].seg_idx >= entry.seg_idx) ||
        entry.image_len != segment_image_encoded_size(kMappingSegmentSize)) {
        finish(Status::corruption());
        return;
    }
    protect(entry.image_addr, round_up_to_iu(entry.image_len, domain.iu_size), Stage::SegmentProtected);
}

void LocalRecoveryOperation::State::segment_read()
{
    MappingSegmentImageHeader header;
    auto                      status = snapshot::decode_segment_into(window->bytes.first(read_bytes), &header, words);
    const auto               &entry  = directory[segment_index];
    if (!status.ok() || header.seg_idx != entry.seg_idx || header.generation != entry.generation ||
        header.slot_count != kMappingSegmentSize || header.body_crc != entry.image_crc) {
        finish(Status::corruption());
        return;
    }
    word_index = 0;
    stage      = Stage::PageExtent;
    wake();
}

void LocalRecoveryOperation::State::page_extent()
{
    while (word_index < words.size() && slot_word::is_empty(words[word_index])) {
        ++word_index;
    }
    if (word_index == words.size()) {
        ++segment_index;
        stage = Stage::Segment;
        wake();
        return;
    }
    const auto word = words[word_index];
    if (!slot_word::is_unloaded(word)) {
        finish(Status::not_supported());
        return;
    }
    const auto pid = (uint64_t{directory[segment_index].seg_idx} * kMappingSegmentSize) + word_index;
    if (pid >= pages->next_page_id || pages->frames.size() == limits.max_frames) {
        finish(pid >= pages->next_page_id ? Status::corruption() : Status::resource_exhausted());
        return;
    }
    const auto iu_index = slot_word::unloaded_iu_index(word);
    const auto iu_count = slot_word::unloaded_iu_count(word);
    if (iu_index > UINT64_MAX / domain.iu_size) {
        finish(Status::corruption());
        return;
    }
    const auto address = iu_index * domain.iu_size;
    const auto bytes   = uint64_t{iu_count} * domain.iu_size;
    if (bytes > limits.max_blob_bytes) {
        finish(Status::resource_exhausted());
        return;
    }
    protect(address, bytes, Stage::PageProtected);
}

void LocalRecoveryOperation::State::page_protected()
{
    auto page_block              = detail::reserved_shared<PageBlockRef>(budget.bytes);
    page_block->page_store_id    = recovered.page_store_id;
    page_block->generation       = recovered.generation;
    page_block->content_id       = recovered.identity;
    page_block->address          = recovered.address;
    page_block->length           = recovered.length;
    page_block->durability       = PageBlockDurability::kCheckpointCommitted;
    page_block->owner            = recovered.owner;
    page_block->physical_owner   = recovered.owner;
    page_block->physical_metrics = recovered.physical_metrics.lock();
    const auto pid               = (uint64_t{directory[segment_index].seg_idx} * kMappingSegmentSize) + word_index;
    pages->frames.emplace(pid, DurableCapturedFrame{.page_id = pid, .page_block = std::move(page_block)});
    ++word_index;
    stage = Stage::PageExtent;
    wake();
}
} // namespace crowdb::tree
