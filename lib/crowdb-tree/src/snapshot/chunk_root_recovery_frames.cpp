// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_codec_internal.h"
#include "snapshot/chunk_root_recovery.h"
#include "snapshot/index_summary_codec.h"
#include "snapshot/stored_overflow.h"

namespace crowdb::tree
{
Status ChunkRootRecovery::finish_indexes()
{
    if (pages_->frames.size() != root_.frame_count || references_ != root_.stored_references) {
        return Status::corruption();
    }
    leaf_             = pages_->frames.begin();
    dependency_phase_ = true;
    return Status::Ok();
}

Status ChunkRootRecovery::index_loaded()
{
    auto status = decode_chunk_mapping_segment_header(index_bytes_, root_.identity, index_, &left_, &offset_);
    if (!status.ok()) {
        return status;
    }
    if (left_ != root_.indexes[index_].frame_count) {
        return Status::corruption();
    }
    return Status::Ok();
}

Status ChunkRootRecovery::summaries_loaded()
{
    detail::ReservedVector<IndexSummaries::Change> changes{
        detail::ReservationAllocator<IndexSummaries::Change>(window_)};
    changes.reserve(index_bytes_.size() / snapshot::kIndexSummaryRecordBytes);
    for (size_t offset = 0; offset < index_bytes_.size(); offset += snapshot::kIndexSummaryRecordBytes) {
        uint32_t     pid;
        IndexSummary summary;
        if (!snapshot::decode_index_summary(index_bytes_.data() + offset, &pid, &summary) ||
            pid >= root_.identity.next_page_id || pid == root_.identity.root_page_id ||
            (!changes.empty() && changes.back().pid >= pid)) {
            return Status::corruption("invalid Chunk index summary record");
        }
        const auto page = pages_->frames.find(pid);
        if (page == pages_->frames.end() || page->second.type != page_type::kInnerBase) {
            return Status::corruption("Chunk summary does not reference an index");
        }
        changes.push_back({.pid = pid, .summary = summary});
    }
    pages_->index_summaries = IndexSummaries{}.merge(changes, metadata_);
    index_bytes_            = ChunkMetadataBytes(index_bytes_.get_allocator());
    summaries_ready_        = true;
    return Status::Ok();
}

Status ChunkRootRecovery::prepare_frame(ChunkPageEntry *out, size_t *offset, uint64_t left,
                                        std::optional<uint64_t> previous) const
{
    auto status = decode_chunk_page_entry(index_bytes_, offset, out);
    if (!status.ok()) {
        return status;
    }
    const auto &record   = *out;
    const auto &expected = root_.indexes[index_];
    if ((previous && record.page_id <= *previous) || record.page_id >= root_.identity.next_page_id ||
        (left == expected.frame_count && record.page_id != expected.first_page) ||
        (left == 1 && (record.page_id != expected.last_page || *offset != index_bytes_.size() - 4))) {
        return Status::corruption();
    }
    if (record.pack.length > limits_.max_blob_bytes || record.frame_bytes > limits_.max_frame_bytes) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

Status ChunkRootRecovery::frame_loaded(const ChunkPageEntry &record, std::shared_ptr<const ChunkPageBlock> pack,
                                       std::span<const uint8_t> blob)
{
    auto page_block              = detail::reserved_shared<PageBlockRef>(metadata_);
    page_block->page_store_id    = domain_->page_store_id;
    page_block->length           = record.pack.length;
    page_block->generation       = domain_->epoch;
    page_block->checksum         = record.pack.checksum;
    page_block->durability       = PageBlockDurability::kCheckpointCommitted;
    page_block->storage          = PageBlockStorage::kChunkPack;
    page_block->owner            = pack;
    page_block->physical_owner   = pack;
    page_block->physical_metrics = pack->physical_metrics;
    page_block->chunk            = std::move(pack);
    DurableCapturedFrame descriptor{.page_id         = record.page_id,
                                    .type            = record.type,
                                    .frame_bytes     = record.frame_bytes,
                                    .logical_bytes   = record.logical_bytes,
                                    .next_overflow   = record.next_overflow,
                                    .overflow_bytes  = record.overflow_bytes,
                                    .page_block      = std::move(page_block),
                                    .stored_overflow = {},
                                    .summary         = {}};
    auto                 status = validate_frame(descriptor, record.stored_reference_words, blob);
    if (!status.ok()) {
        return status;
    }
    pages_->frames.emplace(record.page_id, std::move(descriptor));
    max_frame_bytes_ = std::max<size_t>(max_frame_bytes_, record.frame_bytes);
    max_blob_bytes_  = std::max<size_t>(max_blob_bytes_, record.pack.length);
    last_pid_        = record.page_id;
    have_pid_        = true;
    if (--left_ == 0) {
        ++index_;
    }
    return Status::Ok();
}

Status ChunkRootRecovery::validate_frame(DurableCapturedFrame &descriptor, std::span<const uint8_t> words,
                                         std::span<const uint8_t> blob)
{
    if (durable_blob_raw_len(blob.data(), blob.size()) != descriptor.frame_bytes ||
        durable_blob_logical_len(blob.data(), blob.size()) != descriptor.logical_bytes) {
        return Status::corruption();
    }
    raw_.resize(descriptor.frame_bytes);
    auto status = decode_durable_page(blob.data(), blob.size(), raw_.data(), raw_.size());
    if (!status.ok()) {
        return status;
    }
    if (!frame_validate(raw_.data(), raw_.size()) || frame_u64(raw_.data(), fh::kSelfpage_id) != descriptor.page_id ||
        frame_page_type(raw_.data()) != descriptor.type) {
        return Status::corruption();
    }
    if (descriptor.type == page_type::kOverflowFrame) {
        const OverflowFrameView overflow(raw_.data(), raw_.size());
        return overflow.chunk_len() == descriptor.overflow_bytes && overflow.next_page_id() == descriptor.next_overflow
                 ? Status::Ok()
                 : Status::corruption();
    }
    if (descriptor.type == page_type::kInnerBase) {
        if (root_.root_summary) {
            const auto summary = frame_summary(raw_.data());
            descriptor.summary = FinalPageSummary{.live_counts_known = summary.exact,
                                                  .live_keys         = summary.live_kv,
                                                  .live_key_bytes    = summary.live_key_bytes,
                                                  .live_value_bytes  = summary.live_value_bytes};
        }
        return Status::Ok();
    }
    const LeafFrameView leaf(raw_.data(), raw_.size());
    size_t              stored = 0;
    for (uint32_t i = 0; i < leaf.count(); ++i) {
        const auto     raw = leaf.cell(i);
        const CellView cell(raw);
        if (!cell.valid() || cell.slot() > pages_->coverage ||
            (cell.flags() != 0 && cell.flags() != kFlagTombstone && cell.flags() != kFlagOverflow) ||
            (cell.is_tombstone() && raw.size() != kCellHeaderSize)) {
            return Status::corruption();
        }
        stored += cell.is_overflow() ? 1 : 0;
    }
    if (stored != words.size() / 16 || stored > root_.stored_references - references_) {
        return Status::corruption();
    }
    status = capture_stored_overflow(leaf, kChunkPageEntryReferenceLimit, metadata_, &descriptor.stored_overflow);
    if (!status.ok()) {
        return status;
    }
    const auto &refs = *descriptor.stored_overflow;
    if (refs.size() != words.size() / 16 || refs.size() > root_.stored_references - references_) {
        return Status::corruption();
    }
    for (size_t i = 0; i < refs.size(); ++i) {
        const auto encoded = words.subspan(i * 16, 16);
        if (refs[i].head != chunk_codec::word(encoded, 0) || refs[i].bytes != chunk_codec::word(encoded, 1)) {
            return Status::corruption();
        }
    }
    references_ += refs.size();
    return Status::Ok();
}
} // namespace crowdb::tree
