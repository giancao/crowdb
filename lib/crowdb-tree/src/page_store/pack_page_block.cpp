// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "chunk/chunk_frame.h"
#include "chunk/chunk_page_block.h"
#include "chunk/chunk_physical_registry.h"

namespace crowdb::tree
{
bool PageChunkStoreDomain::supported() const noexcept
{
    return page_store_id != 0 && tree != 0 && epoch != 0 && mirrors != 0 && mirrors <= detail::kMaxMirrorCopies &&
           max_chunk_bytes != 0 && max_chunk_bytes <= detail::kMaxChunkPackBytes && catalog &&
           catalog->stage_schema_supported() && catalog->protection() != nullptr && catalog->pin_release_queue() &&
           transport && transport->fixed_staging_supported();
}

bool chunk_page_block_matches(const ChunkPageBlock &page_block, uint64_t domain, uint64_t length) noexcept
{
    const auto &pack = page_block.pack;
    return page_block.domain && page_block.domain->supported() && page_block.domain->page_store_id == domain &&
           page_block.pin && pack.stage.owner_epoch != 0 &&
           (pack.stage.operation_high != 0 || pack.stage.operation_low != 0) && !pack.chunk.empty() && length != 0 &&
           length == pack.length && length <= detail::kMaxChunkPackBytes && detail::valid_page_purpose(pack.purpose) &&
           detail::chunk_purpose(pack.chunk) == pack.purpose &&
           pack.offset <= UINT64_MAX - detail::chunk_framed_length(pack.length);
}

std::array<uint64_t, 8> chunk_page_block_identity(const ChunkPageBlock &page_block) noexcept
{
    const auto &pack = page_block.pack;
    return {pack.stage.owner_epoch,
            pack.stage.operation_high,
            pack.stage.operation_low,
            pack.chunk.high,
            pack.chunk.low,
            pack.offset,
            pack.length,
            (uint64_t{pack.checksum} << 32U) | static_cast<uint32_t>(pack.purpose)};
}

bool same_chunk_page_block(const std::shared_ptr<const ChunkPageBlock> &left,
                           const std::shared_ptr<const ChunkPageBlock> &right) noexcept
{
    if (!left || !right) {
        return left == right;
    }
    return chunk_page_block_identity(*left) == chunk_page_block_identity(*right) &&
           left->pack.length == right->pack.length && left->pack.checksum == right->pack.checksum &&
           left->pack.purpose == right->pack.purpose && left->domain && right->domain &&
           left->domain->page_store_id == right->domain->page_store_id;
}

size_t chunk_page_block_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<ChunkPageBlock>() + detail::ChunkPinReleaseQueue::pin_allocation_bound() +
           ChunkPhysicalRegistry::pack_allocation_bound();
}

} // namespace crowdb::tree
