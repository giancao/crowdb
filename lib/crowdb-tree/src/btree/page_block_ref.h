// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <span>

namespace crowdb::tree
{
class PageBlockMetricExtent;
struct OverflowPageBlockSet;
struct ChunkPageBlock;
[[nodiscard]] bool chunk_page_block_matches(const ChunkPageBlock &, uint64_t domain, uint64_t length) noexcept;
[[nodiscard]] bool same_chunk_page_block(const std::shared_ptr<const ChunkPageBlock> &,
                                         const std::shared_ptr<const ChunkPageBlock> &) noexcept;
[[nodiscard]] std::array<uint64_t, 8> chunk_page_block_identity(const ChunkPageBlock &) noexcept;
enum class PageBlockStorage : uint8_t { kDurableBlob, kResidentFrame, kChunkPack };
enum class PageBlockDurability : uint8_t { kReloadable, kCheckpointCommitted, kResident };

struct PageBlockCapabilities
{
    bool independent_reload = false;
    bool staged_lifetime    = false;

    [[nodiscard]] bool can_stage() const
    {
        return independent_reload && staged_lifetime;
    }
};

// A page block reference: location, identity and owners for readable page frames.
// A page segment groups frames; current Local ranges and Chunk BtreePage packs
// encode singleton page segments. Resident frames retain their allocation owner.
// Page block size is independent of the page disk store's device-block reclamation unit.
// Mapping segments store PID-to-page-segment-location entries separately from the bytes.
// The page store owner protects every extent/overflow object reachable from this page.
// Only a completed checkpoint changes the durable frontier.
struct PageBlockRef
{
    bool retire_step() noexcept
    {
        return true;
    }

    uint64_t                    page_store_id = 0;
    uint64_t                    address       = 0;
    uint64_t                    length        = 0;
    uint64_t                    content_id    = 0;
    uint64_t                    generation    = 0;
    uint64_t                    checksum      = 0;
    uint32_t                    encoding      = 0;
    PageBlockDurability         durability    = PageBlockDurability::kReloadable;
    std::shared_ptr<const void> owner;
    // Exact allocation protection, without transitive page dependencies. GC
    // can rebind a retained frame to a new dependency graph without retaining
    // the old graph. It is never sufficient as a complete primary owner.
    std::shared_ptr<const void>            physical_owner;
    std::shared_ptr<PageBlockMetricExtent> physical_metrics = nullptr;
    // Exact immutable physical dependencies of this primary frame. Keeping a
    // leaf readable must not pin unrelated pages from its checkpoint.
    std::shared_ptr<const OverflowPageBlockSet> overflow;

    // An imported raw frame has an allocation owner, not a file extent. It
    // never qualifies for checkpoint reuse or page store garbage collection.
    PageBlockStorage                      storage = PageBlockStorage::kDurableBlob;
    std::span<const uint8_t>              resident_frame;
    std::shared_ptr<const ChunkPageBlock> chunk;

    [[nodiscard]] bool reloadable() const
    {
        return length != 0 && owner != nullptr &&
               (storage == PageBlockStorage::kDurableBlob ||
                (storage == PageBlockStorage::kChunkPack && address == 0 && chunk &&
                 chunk_page_block_matches(*chunk, page_store_id, length)));
    }

    [[nodiscard]] bool readable() const
    {
        return reloadable() || (storage == PageBlockStorage::kResidentFrame && owner != nullptr &&
                                !resident_frame.empty() && length == 0 && address == 0 && page_store_id == 0);
    }
};

// A nonwrapping physical interval selected for relocation. A zero length
// leaves ordinary clean-primary and overflow reuse unchanged.
struct PageBlockExclusion
{
    uint64_t address = 0;
    uint64_t length  = 0;

    [[nodiscard]] bool overlaps(uint64_t begin, uint64_t bytes) const noexcept
    {
        return length != 0 && bytes != 0 && (begin >= address ? begin - address < length : address - begin < bytes);
    }

    [[nodiscard]] bool overlaps(const PageBlockRef &page_block) const noexcept
    {
        return page_block.storage == PageBlockStorage::kDurableBlob && page_block.reloadable() &&
               overlaps(page_block.address, page_block.length);
    }
};
} // namespace crowdb::tree
