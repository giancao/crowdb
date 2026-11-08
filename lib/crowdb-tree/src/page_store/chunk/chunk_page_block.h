// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "chunk_fixed_pack.h"
#include "chunk_pin_release.h"

namespace crowdb::tree
{
class PageBlockMetricExtent;
class ScheduledPageStore;
class ChunkPhysicalRegistry;
struct ChunkPhysicalPack;

// A stable storage domain, independent of a Tree generation or CPU executor.
// Every descriptor owns a paid pin; the host release service outlives them.
struct PageChunkStoreDomain
{
    uint64_t                                page_store_id   = 0;
    uint64_t                                tree            = 0;
    uint64_t                                epoch           = 0;
    uint32_t                                mirrors         = 0;
    uint64_t                                max_chunk_bytes = 256 * 1024 * 1024;
    std::shared_ptr<detail::RootCatalog>    catalog;
    std::shared_ptr<detail::ChunkTransport> transport;
    std::shared_ptr<ChunkPhysicalRegistry>  physical_registry;

    [[nodiscard]] bool supported() const noexcept;
};

// Filled privately by the writer before its completion publishes a const view.
// BtreePage packs locate page blocks holding individual page frames; a page
// segment groups page frames. Mapping segments use PageIndex packs; other PageIndex metadata
// retains its own format and ownership rules.
// Shared page descriptors use this one pack owner, including its physical-byte
// observation; no per-page copy of whole-pack storage accounting is needed.
struct ChunkPageBlock
{
    using Read  = void (*)(ScheduledPageStore &, std::shared_ptr<const ChunkPageBlock>, std::span<uint8_t>,
                           std::shared_ptr<AllocationBudget>, AsyncCompletion);
    using Write = void (*)(ScheduledPageStore &, std::shared_ptr<ChunkPageBlock>, std::span<const uint8_t>,
                           AsyncCompletion);
    Read                                        read  = nullptr;
    Write                                       write = nullptr;
    detail::ChunkFixedPackRef                   pack;
    std::shared_ptr<const PageChunkStoreDomain> domain;
    std::shared_ptr<detail::ChunkPinOwner>      pin;
    std::shared_ptr<AllocationBudget>           physical_budget;
    std::shared_ptr<ChunkPhysicalPack>          physical;
    std::shared_ptr<PageBlockMetricExtent>      physical_metrics;
};

[[nodiscard]] bool chunk_page_block_matches(const ChunkPageBlock &page_block, uint64_t domain,
                                            uint64_t length) noexcept;
[[nodiscard]] bool same_chunk_page_block(const std::shared_ptr<const ChunkPageBlock> &left,
                                         const std::shared_ptr<const ChunkPageBlock> &right) noexcept;
[[nodiscard]] std::array<uint64_t, 8> chunk_page_block_identity(const ChunkPageBlock &page_block) noexcept;
[[nodiscard]] size_t                  chunk_page_block_allocation_bound() noexcept;
[[nodiscard]] size_t                  chunk_pin_validation_bound() noexcept;

// Preparation spends no metadata I/O. Arm before the first mutation so an
// unknown StageReserved result still receives an exact, durable final release.
Status prepare_chunk_page_block(const std::shared_ptr<const PageChunkStoreDomain> &domain, detail::ChunkStageId stage,
                                detail::ChunkId chunk, uint32_t length, PagePurpose purpose,
                                const std::shared_ptr<AllocationBudget> &metadata,
                                std::shared_ptr<ChunkPageBlock>         *out);
Status pin_chunk_page_block(const std::shared_ptr<const PageChunkStoreDomain> &domain, detail::ChunkFixedPackRef pack,
                            detail::ChunkStageId pin, const std::shared_ptr<AllocationBudget> &metadata,
                            std::shared_ptr<const ChunkPageBlock> *out);

void   initialize_chunk_page_block_io(ChunkPageBlock &page_block) noexcept;
Status validate_chunk_pin_stage(const PageChunkStoreDomain &domain, const detail::ChunkFixedPackRef &pack,
                                const std::optional<detail::ChunkStageRecord> &record);
void   submit_pin_chunk_page_block(const std::shared_ptr<const PageChunkStoreDomain> &domain,
                                   detail::ChunkFixedPackRef pack, detail::ChunkStageId pin,
                                   const std::shared_ptr<AllocationBudget> &metadata,
                                   std::shared_ptr<const ChunkPageBlock> *out, AsyncCompletion complete,
                                   std::optional<detail::ChunkStageRecord> *verified_stage = nullptr);

// Finite blocking page store steps; the caller must dispatch them on its bounded
// backend lane. Exact identities are never translated to synthetic file
// offsets.
Status read_chunk_page_block(const ChunkPageBlock &page_block, std::span<uint8_t> output,
                             const std::shared_ptr<AllocationBudget> &budget);
Status write_chunk_page_block(ChunkPageBlock &page_block, std::span<const uint8_t> input,
                              const std::shared_ptr<AllocationBudget> &budget);
} // namespace crowdb::tree
