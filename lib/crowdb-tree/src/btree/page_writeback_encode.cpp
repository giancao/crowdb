// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_store/chunk/chunk_page_block.h"
#include "page_store/page_block_io.h"
#include "page_writeback_internal.h"

#include <algorithm>

namespace crowdb::tree
{
void PageWriteOperation::State::prepare()
{
    const auto *frame = input.frame.data();
    if (!frame_validate(frame, input.info.bytes) || frame_page_type(frame) != input.info.type ||
        frame_u64(frame, fh::kSelfpage_id) != input.info.page_id) {
        finish(Status::corruption("write input does not match its immutable frame identity"));
        return;
    }
    bool needs_dependencies = false;
    if (input.info.type == page_type::kLeafBase) {
        LeafFrameView leaf(frame, input.info.bytes);
        for (size_t i = 0; i < leaf.count(); ++i) {
            needs_dependencies = needs_dependencies || CellView(leaf.cell(i)).is_overflow();
        }
    }
    else if (input.info.type == page_type::kOverflowFrame) {
        needs_dependencies = OverflowFrameView(frame, input.info.bytes).next_page_id() != kInvalidPageId;
    }
    else if (input.info.type != page_type::kInnerBase) {
        finish(Status::invalid_argument("only final frame kinds may be written"));
        return;
    }
    if (needs_dependencies && !input.reload_dependencies) {
        finish(Status::invalid_argument("overflow write requires captured extent dependencies"));
        return;
    }
    if (input.raw_blob.empty()) {
        auto status =
            encode_durable_page_into(input.frame, limits.compression, aligned, scratch, &report.logical_bytes);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        report.physical_bytes = limits.padded_blob_bytes(report.logical_bytes - kDurableBlobHeader);
        auto padding          = aligned.subspan(report.logical_bytes, report.physical_bytes - report.logical_bytes);
        std::fill(padding.begin(), padding.end(), uint8_t{0});
    }
    else {
        report.logical_bytes  = static_cast<uint32_t>(kDurableBlobHeader + input.frame.size());
        report.physical_bytes = input.raw_blob.size();
    }
    try {
        reserve(report.physical_bytes, [self = shared_from_this()](Status status, ReservedWriteExtent extent) {
            self->reserved(std::move(status), std::move(extent));
        });
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::io_error());
    }
    if ((reservation_events.fetch_or(2, std::memory_order_acq_rel) & 1U) != 0) {
        queue_write(true);
    }
}

void PageWriteOperation::State::reserved(Status status, ReservedWriteExtent extent) noexcept
{
    if (reservation_claimed.exchange(true, std::memory_order_acq_rel) || finished.load(std::memory_order_acquire)) {
        return;
    }
    reservation_status = std::move(status);
    reserved_extent    = std::move(extent);
    if ((reservation_events.fetch_or(1, std::memory_order_acq_rel) & 2U) != 0) {
        queue_write();
    }
}

void PageWriteOperation::State::queue_write(bool on_worker) noexcept
{
    if (finished.load(std::memory_order_acquire)) {
        reserved_extent.owner.reset();
        return;
    }
    if (on_worker) {
        write(this);
        return;
    }
    Status status;
    try {
        status = runtime->try_submit(lane, &State::write, this);
    }
    catch (...) {
        status = Status::resource_exhausted();
    }
    if (!status.ok()) {
        reserved_extent.owner.reset();
        finish(std::move(status));
    }
}

void PageWriteOperation::State::write(void *context) noexcept
{
    auto self = static_cast<State *>(context)->pending;
    try {
        self->submit_write();
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted());
    }
    catch (...) {
        self->finish(Status::io_error());
    }
}

void PageWriteOperation::State::submit_write()
{
    auto extent = std::move(reserved_extent);
    if (!reservation_status.ok()) {
        finish(std::move(reservation_status));
        return;
    }
    if (!extent.owner || extent.length != report.physical_bytes || extent.address % limits.iu_size != 0 ||
        extent.address > UINT64_MAX - extent.length) {
        finish(Status::invalid_argument("backend allocation must own an aligned nonwrapping write extent"));
        return;
    }
    auto chunk = extent.chunk.lock();
    if (static_cast<bool>(chunk) != limits.chunk_backed ||
        (chunk && (extent.address != 0 || !chunk_page_block_matches(*chunk, extent.page_store_id, extent.length)))) {
        finish(Status::invalid_argument("backend reservation differs from its admitted storage capability"));
        return;
    }
    protection->extent           = std::move(extent.owner);
    page_block->physical_owner   = protection->extent;
    page_block->physical_metrics = extent.physical_metrics.lock();
    page_block->page_store_id    = extent.page_store_id;
    page_block->address          = extent.address;
    page_block->length           = extent.length;
    page_block->content_id       = extent.content_id;
    page_block->generation       = extent.generation;
    if (chunk) {
        page_block->storage = PageBlockStorage::kChunkPack;
        page_block->chunk   = chunk;
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    auto                           self = shared_from_this();
    const std::span<const uint8_t> bytes =
        input.raw_blob.empty() ? std::span<const uint8_t>(aligned).first(report.physical_bytes) : input.raw_blob;
    io_resources.begin(input.raw_blob.empty() ? blob.capacity() : bytes.size());
    io_submitted = true;
    if (chunk) {
        submit_chunk_write(*store, std::move(chunk), bytes, chunk_credit,
                           {.context     = this,
                            .complete_fn = &State::written,
                            .owner       = std::move(self),
                            .allocations = budget.backend,
                            .cause       = budget.cause});
        return;
    }
    store->submit_typed_write(PagePurpose::kBtreePage, page_block->address, bytes.data(), bytes.size(),
                              {.context     = this,
                               .complete_fn = &State::written,
                               .owner       = std::move(self),
                               .allocations = budget.backend,
                               .cause       = budget.cause});
}
} // namespace crowdb::tree
