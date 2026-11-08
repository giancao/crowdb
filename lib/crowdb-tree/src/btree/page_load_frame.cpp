// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/maptable/compressor.h"
#include "page_load_internal.h"

namespace crowdb::tree
{
namespace
{
template <typename Bytes>
Status decode_frame(std::span<const uint8_t> blob, const PageBlockRef &page_block, const PageLoadLimits &limits,
                    Bytes &scratch, std::span<const uint8_t> *out)
{
    if (!page_block.readable()) {
        return Status::corruption("page_block has neither an extent nor an owned resident frame");
    }
    const bool   resident = page_block.storage == PageBlockStorage::kResidentFrame;
    const size_t length = resident ? page_block.resident_frame.size() : durable_blob_raw_len(blob.data(), blob.size());
    if (length <= kFrameHeaderSize + kFrameTrailerSize || length > limits.max_frame_bytes || length > UINT32_MAX) {
        return Status::corruption("page_block frame exceeds declared bounds");
    }
    if (resident) {
        *out = page_block.resident_frame;
        return Status::Ok();
    }
    if (blob.front() == static_cast<uint8_t>(compress_algo::kNone)) {
        return view_raw_durable_page(blob, static_cast<uint32_t>(length), out);
    }
    scratch.resize(length);
    auto status = decode_durable_page(blob.data(), blob.size(), scratch.data(), static_cast<uint32_t>(length));
    if (status.ok()) {
        record_work(scratch.get_allocator().owner(), TreeWorkCounter::MaterializeCopyBytes, length);
        *out = scratch;
    }
    return status;
}
} // namespace

Status decode_page_block_frame(std::span<const uint8_t> blob, const PageBlockRef &page_block,
                               const PageLoadLimits &limits, LoadBytes &scratch, std::span<const uint8_t> *out)
{
    return decode_frame(blob, page_block, limits, scratch, out);
}

Status decode_page_block_frame(std::span<const uint8_t> blob, const PageBlockRef &page_block,
                               const PageLoadLimits &limits, FrameStore::Bytes &scratch, std::span<const uint8_t> *out)
{
    return decode_frame(blob, page_block, limits, scratch, out);
}
} // namespace crowdb::tree
