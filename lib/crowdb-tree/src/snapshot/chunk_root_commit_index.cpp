// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_store/chunk/chunk_frame.h"
#include "snapshot/chunk_root_commit.h"
#include "snapshot/index_summary_codec.h"

namespace crowdb::tree
{
Status ChunkRootCommit::index()
{
    if (page_ == pages_->frames.end()) {
        page_  = pages_->frames.begin();
        stage_ = Stage::Summaries;
        return Status::Ok();
    }
    std::array<const DurableCapturedFrame *, kChunkMappingSegmentEntryLimit> frames{};
    const ChunkMappingSegmentRef                                            *previous = nullptr;
    if (previous_) {
        while (previous_index_ < previous_->indexes.size() &&
               previous_->indexes[previous_index_].last_page < page_->first) {
            ++previous_index_;
        }
        if (previous_index_ < previous_->indexes.size()) {
            previous = &previous_->indexes[previous_index_];
        }
    }
    size_t   count = 0;
    auto     next  = page_;
    uint64_t last  = UINT64_MAX;
    if (previous != nullptr) {
        last = page_->first < previous->first_page ? previous->first_page - 1 : previous->last_page;
    }
    while (next != pages_->frames.end() && count < frames.size() && next->first <= last) {
        if (next->first != next->second.page_id) {
            return fail(Status::corruption("Chunk checkpoint page identity mismatch"));
        }
        frames[count++] = &next->second;
        ++next;
    }
    std::shared_ptr<const ChunkPageBlock> page_block;
    const auto                            selected = std::span(frames).first(count);
    auto status = previous != nullptr && unchanged(selected, *previous) ? reuse_index(*previous, &page_block)
                                                                        : write_index(selected, &page_block);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    for (size_t i = 0; i < count; ++i) {
        references_.insert(frames[i]->page_block->chunk->pack.stage);
        root_.stored_references += frames[i]->stored_overflow ? frames[i]->stored_overflow->size() : 0;
    }
    if (root_.stored_references > detail::kMaxCandidateReferences) {
        return fail(Status::resource_exhausted());
    }
    references_.insert(page_block->pack.stage);
    root_.indexes.push_back({.first_page  = frames[0]->page_id,
                             .last_page   = frames[count - 1]->page_id,
                             .frame_count = count,
                             .pack        = page_block->pack});
    index_pins_.push_back(std::move(page_block));
    page_ = next;
    return Status::Ok();
}

Status ChunkRootCommit::prepare_summaries()
{
    if (pages_->index_summaries.size() == 0) {
        return Status::Ok();
    }
    if (previous_ && previous_->summaries && pages_->index_summaries.same(baseline_->index_summaries)) {
        root_.summaries = previous_->summaries;
        return Status::Ok();
    }
    const auto count = pages_->index_summaries.size();
    if (count > detail::kMaxChunkPackBytes / snapshot::kIndexSummaryRecordBytes) {
        return Status::resource_exhausted("index summaries exceed a Chunk pack");
    }
    const auto           length = count * snapshot::kIndexSummaryRecordBytes;
    detail::ChunkStageId stage;
    detail::ChunkId      chunk;
    auto                 status = ids_->next(PagePurpose::kPageIndex, &stage, &chunk);
    if (status.ok()) {
        status = prepare_chunk_page_block(domain_, stage, chunk, static_cast<uint32_t>(length), PagePurpose::kPageIndex,
                                          metadata_, &new_summary_pack_);
    }
    if (!status.ok()) {
        return status;
    }
    summary_bytes_.resize(length);
    uint32_t first  = 0;
    auto    *output = summary_bytes_.data();
    pages_->index_summaries.visit_from(
        first, static_cast<void *>(&output), +[](void *raw, uint32_t pid, IndexSummary summary) {
            auto &position = *static_cast<uint8_t **>(raw);
            snapshot::encode_index_summary(position, pid, summary);
            position += snapshot::kIndexSummaryRecordBytes;
            return true;
        });
    return Status::Ok();
}

Status ChunkRootCommit::install_summaries()
{
    if (summary_pack_) {
        root_.summaries = summary_pack_->pack;
        references_.insert(summary_pack_->pack.stage);
        index_pins_.push_back(std::move(summary_pack_));
    }
    summary_bytes_ = decltype(summary_bytes_)(summary_bytes_.get_allocator());
    stage_         = Stage::Dependencies;
    return Status::Ok();
}

Status ChunkRootCommit::checksum()
{
    std::array<detail::ChunkStageId, detail::kMaxStageScan> window{};
    size_t                                                  count = 0;
    while (reference_ != references_.end() && count < window.size()) {
        window[count++] = *reference_++;
    }
    checksum_ = detail::chunk_reference_checksum(checksum_, std::span(window).first(count));
    if (reference_ != references_.end()) {
        return Status::Ok();
    }
    auto status = encode_chunk_root(root_, &manifest_);
    if (!status.ok()) {
        return fail(std::move(status));
    }
    reference_ = references_.begin();
    stage_     = Stage::Begin;
    return Status::Ok();
}

Status ChunkRootCommit::begin()
{
    const detail::ChunkManifestCandidate candidate{.id                  = candidate_,
                                                   .expected_generation = root_.identity.root_generation - 1,
                                                   .reference_count     = references_.size(),
                                                   .reference_checksum  = checksum_,
                                                   .manifest            = manifest_};
    auto                                *catalog = domain_->catalog->protection();
    auto                                 status  = catalog->check_read_owner();
    if (!status.ok()) {
        return fail(std::move(status));
    }
    candidate_started_ = true;
    stage_             = Stage::QueryBegin;
    status             = catalog->begin_candidate(domain_->tree, candidate);
    if (!status.ok()) {
        status_ = std::move(status);
        return Status::Ok();
    }
    stage_ = references_.empty() ? Stage::Commit : Stage::Prepare;
    return Status::Ok();
}

Status ChunkRootCommit::prepare()
{
    pending_count_ = 0;
    auto next      = reference_;
    while (next != references_.end() && pending_count_ < pending_.size()) {
        pending_[pending_count_++] = *next++;
    }
    stage_      = Stage::QueryPrepare;
    auto status = domain_->catalog->protection()->prepare_references(domain_->tree, candidate_, prepared_,
                                                                     std::span(pending_).first(pending_count_));
    if (!status.ok()) {
        status_ = std::move(status);
        return Status::Ok();
    }
    accepted_window();
    return Status::Ok();
}

void ChunkRootCommit::accepted_window()
{
    for (size_t i = 0; i < pending_count_; ++i) {
        ++reference_;
    }
    ++prepared_;
    pending_count_ = 0;
    status_        = Status::Ok();
    stage_         = reference_ == references_.end() ? Stage::Commit : Stage::Prepare;
}
} // namespace crowdb::tree
