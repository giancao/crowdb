// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_plan.h"

#include "btree/allocation_bound.h"
#include "btree/overflow_page_block.h"
#include "crowdb-tree/maptable/mapping_geometry.h"

#include <algorithm>

namespace crowdb::tree
{
CapturedLayoutPlanner::CapturedLayoutPlanner(uint64_t next_page_id, uint64_t page_store_id, CapturedWriteLimits limits,
                                             size_t max_segments, std::shared_ptr<const CapturedDurablePages> inherited,
                                             std::shared_ptr<AllocationBudget> metadata)
    : next_(next_page_id),
      backend_(page_store_id),
      limits_(std::move(limits)),
      max_segments_(max_segments),
      inherited_(std::move(inherited)),
      segments_(std::less<>{}, detail::ReservationAllocator<uint64_t>(metadata)),
      rebase_segments_(std::less<>{}, detail::ReservationAllocator<uint64_t>(metadata))
{
}

std::optional<size_t> CapturedLayoutPlanner::allocation_bound(uint64_t next_page_id, size_t max_segments) noexcept
{
    const auto segments = (next_page_id + kMappingSegmentSize - 1) / kMappingSegmentSize;
    const auto rebase_segments =
        (next_page_id / MappingTable::kFanout) + static_cast<uint64_t>(next_page_id % MappingTable::kFanout != 0);
    const auto max_rebase_segments =
        detail::AllocationBound().add_product(max_segments, kMappingSegmentSize / MappingTable::kFanout).value();
    return detail::AllocationBound(detail::shared_allocation_bound<CapturedLayoutPlanner>())
        .add_product(std::min<uint64_t>(segments, max_segments), sizeof(uint64_t) + (4 * sizeof(void *)))
        .add(max_rebase_segments ? detail::AllocationBound()
                                       .add_product(std::min<uint64_t>(rebase_segments, *max_rebase_segments),
                                                    sizeof(uint64_t) + (4 * sizeof(void *)))
                                       .value()
                                 : std::nullopt)
        .value();
}

Status CapturedLayoutPlanner::segment(uint64_t pid)
{
    if (pid >= next_) {
        return Status::corruption("captured PID exceeds the planned namespace");
    }
    const auto number = pid / kMappingSegmentSize;
    if (!segments_.contains(number)) {
        if (segments_.size() == max_segments_) {
            return Status::resource_exhausted("checkpoint segment limit");
        }
        segments_.insert(number);
    }
    return Status::Ok();
}

Status CapturedLayoutPlanner::accept(CapturedFrameResult::Plan page)
{
    if (page_) {
        return Status::invalid_argument("previous layout page is incomplete");
    }
    if (!detail::AllocationBound(plan_.primary_frames)
             .add(1)
             .add(plan_.new_overflow_frames)
             .add(page.summary.new_overflow_frames)
             .value() ||
        !detail::AllocationBound(plan_.reference_heads).add(page.reused_overflow.size()).value()) {
        return Status::resource_exhausted("checkpoint layout count overflow");
    }
    auto status = segment(page.page_id);
    if (!status.ok()) {
        return status;
    }
    ++plan_.primary_frames;
    // Relocation can replace overflow dependencies while keeping the primary
    // bytes. Such receipts still require a rebase and keep the full allowance.
    if (page.reused_primary && limits_.frames.exclusion.length == 0) {
        ++plan_.reused_primary_frames;
    }
    else {
        rebase_segments_.insert(page.page_id / MappingTable::kFanout);
    }
    plan_.new_overflow_frames += page.summary.new_overflow_frames;
    plan_.reference_heads += page.reused_overflow.size();
    reference_ = 0;
    pid_       = kInvalidPageId;
    page_      = std::move(page);
    return Status::Ok();
}

Status CapturedLayoutPlanner::step()
{
    if (!page_) {
        return Status::invalid_argument();
    }
    if (reference_ == page_->reused_overflow.size()) {
        page_.reset();
        return Status::Ok();
    }
    if (plan_.reused_dependency_entries == SIZE_MAX) {
        return Status::resource_exhausted("checkpoint dependency count overflow");
    }
    const auto &reference = page_->reused_overflow[reference_];
    if (pid_ == kInvalidPageId) {
        pid_         = reference.head;
        remaining_   = reference.bytes;
        chain_steps_ = 0;
    }
    size_t count = 0;
    if (reference.page_block) {
        count = reference.page_block->pages.size();
    }
    else if (inherited_) {
        count = inherited_->frames.size();
    }
    if (count != 0 && ++chain_steps_ > count) {
        return Status::corruption("captured overflow dependency cycle");
    }
    OverflowPageBlock old;
    if (reference.page_block) {
        const auto found = reference.page_block->pages.find(pid_);
        if (found == reference.page_block->pages.end()) {
            return Status::corruption("missing captured overflow");
        }
        old = found->second;
    }
    else {
        if (!inherited_) {
            return Status::unavailable("captured overflow has no protected descriptor");
        }
        const auto found = inherited_->frames.find(pid_);
        if (found == inherited_->frames.end() || found->second.type != page_type::kOverflowFrame) {
            return Status::corruption("missing captured overflow");
        }
        const auto &frame = found->second;
        old               = {.page_block    = frame.page_block,
                             .next          = frame.next_overflow,
                             .bytes         = frame.overflow_bytes,
                             .frame_bytes   = frame.frame_bytes,
                             .logical_bytes = frame.logical_bytes};
    }
    const bool resident = old.page_block && old.page_block->storage == PageBlockStorage::kResidentFrame;
    if (!old.page_block || !old.page_block->readable() || (!resident && old.page_block->page_store_id != backend_) ||
        (resident && (old.page_block->resident_frame.size() != old.frame_bytes ||
                      old.frame_bytes > limits_.write.max_frame_bytes)) ||
        old.bytes == 0 || old.bytes > remaining_ || (old.next == kInvalidPageId) != (old.bytes == remaining_)) {
        return Status::corruption("captured overflow topology or owner differs from the cell");
    }
    auto status = segment(pid_);
    if (!status.ok()) {
        return status;
    }
    ++plan_.reused_dependency_entries;
    plan_.resident_dependency_entries += static_cast<size_t>(resident);
    plan_.relocated_dependency_entries += static_cast<size_t>(limits_.frames.exclusion.overlaps(*old.page_block));
    remaining_ -= old.bytes;
    pid_ = old.next;
    if (remaining_ == 0) {
        ++reference_;
    }
    return Status::Ok();
}

Status CapturedLayoutPlanner::finish()
{
    if (pending()) {
        return Status::invalid_argument();
    }
    if (plan_.new_overflow_frames > MappingTable::kPageLimit - next_) {
        return Status::resource_exhausted("checkpoint overflow namespace exhausted");
    }
    plan_.segments       = segments_.size();
    plan_.rebase_batches = rebase_segments_.size();
    if (plan_.new_overflow_frames != 0) {
        const auto first = next_ / kMappingSegmentSize;
        const auto last  = (next_ + plan_.new_overflow_frames - 1) / kMappingSegmentSize;
        plan_.segments += last - first + 1 - static_cast<size_t>(segments_.contains(first));
    }
    if (plan_.segments > max_segments_) {
        return Status::resource_exhausted("checkpoint segment limit");
    }
    const auto frames = detail::AllocationBound(plan_.primary_frames)
                            .add(plan_.new_overflow_frames)
                            .add(plan_.reused_dependency_entries)
                            .value();
    const auto heads  = detail::AllocationBound(plan_.reference_heads).add(plan_.new_overflow_frames).value();
    if (!frames || !heads) {
        return Status::resource_exhausted("checkpoint layout count overflow");
    }
    plan_.frame_count_upper = *frames;
    // Each newly spilled value needs at most one stored reference per emitted
    // overflow frame. Reused chains may share PIDs, so descriptor counts above
    // are conservative; physical dependency ownership still counts each use.
    plan_.reference_heads = *heads;
    return Status::Ok();
}
} // namespace crowdb::tree
