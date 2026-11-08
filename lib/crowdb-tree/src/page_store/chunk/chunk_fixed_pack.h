// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_reservation.h"
#include "chunk_root_catalog.h"

#include <span>

namespace crowdb::tree
{
class ChunkPhysicalRegistry;
class ScheduledPageStore;
struct ChunkPhysicalPack;
} // namespace crowdb::tree

namespace crowdb::tree::detail
{
// Immutable identity of a typed pack. BtreePage identifies individual frames
// in page segment storage; mapping segments and other metadata use PageIndex.
// Readers never consult the active manifest. Stage/manifest ownership is held
// separately from the byte window; multiple packs may share one physical allocation.
struct ChunkFixedPackRef
{
    ChunkStageId stage;
    ChunkId      chunk;
    uint64_t     offset   = 0;
    uint32_t     length   = 0;
    uint32_t     checksum = 0;
    PagePurpose  purpose  = PagePurpose::kInvalid;
};

struct ChunkPackWindow
{
    explicit ChunkPackWindow(const std::shared_ptr<AllocationBudget> &budget)
        : bytes(std::vector<uint8_t, BudgetAllocator<uint8_t, AllocationBudget, AllocationKind::kIo>>::allocator_type(
              budget))
    {
    }

    std::vector<uint8_t, BudgetAllocator<uint8_t, AllocationBudget, AllocationKind::kIo>> bytes;
};

// Fixed pack framing and its durable staging protocol. Async submissions retain
// inputs through their one terminal completion; mirrors join before publication.
class ChunkFixedPackStore
{
  public:
    struct Write
    {
        std::span<const uint8_t> input;
        ChunkFixedPackRef       *out;
    };

    ChunkFixedPackStore(uint64_t tree, uint64_t epoch, uint32_t mirrors, std::shared_ptr<RootCatalog> catalog,
                        std::shared_ptr<ChunkTransport>        transport,
                        std::shared_ptr<ChunkPhysicalRegistry> registry = {});
    [[nodiscard]] bool                         supported() const noexcept;
    [[nodiscard]] static std::optional<size_t> write_allocation_bound(size_t bytes) noexcept;
    [[nodiscard]] static std::optional<size_t> read_allocation_bound(size_t bytes) noexcept;
    [[nodiscard]] static std::optional<size_t> read_into_allocation_bound(size_t bytes) noexcept;
    Status write(ChunkStageId operation, ChunkId fixed, PagePurpose purpose, std::span<const uint8_t> input,
                 const std::shared_ptr<AllocationBudget> &budget, ChunkFixedPackRef *out,
                 std::shared_ptr<ChunkPhysicalPack>      *physical        = nullptr,
                 const std::shared_ptr<AllocationBudget> &physical_budget = {});
    Status write_many(ChunkStageId operation, ChunkId fixed, PagePurpose purpose, std::span<const Write> inputs,
                      const std::shared_ptr<AllocationBudget> &budget,
                      std::shared_ptr<ChunkPhysicalPack>      *physical        = nullptr,
                      const std::shared_ptr<AllocationBudget> &physical_budget = {});
    void   submit_write_many(ChunkStageId operation, ChunkId fixed, PagePurpose purpose, std::span<const Write> inputs,
                             const std::shared_ptr<AllocationBudget> &budget, AsyncCompletion completion,
                             std::shared_ptr<ChunkPhysicalPack>      *physical        = nullptr,
                             const std::shared_ptr<AllocationBudget> &physical_budget = {});
    void   submit_read_into(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                            std::span<uint8_t> output, AsyncCompletion completion,
                            std::shared_ptr<ScheduledPageStore> cpu = {}) const;
    Status read(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                std::shared_ptr<const ChunkPackWindow> *out) const;
    [[nodiscard]] Status read_into(const ChunkFixedPackRef &ref, const std::shared_ptr<AllocationBudget> &budget,
                                   std::span<uint8_t> output) const;

  private:
    struct AsyncWrite;
    struct AsyncRead;
    uint64_t                               tree_;
    uint64_t                               epoch_;
    uint32_t                               mirrors_;
    std::shared_ptr<RootCatalog>           catalog_;
    std::shared_ptr<ChunkTransport>        transport_;
    std::shared_ptr<ChunkPhysicalRegistry> registry_;
};

Status encode_fixed_pack(ChunkId id, PagePurpose purpose, std::span<const uint8_t> input, std::span<uint8_t> framed);
Status decode_fixed_pack(const ChunkFixedPackRef &ref, std::span<const uint8_t> framed, std::span<uint8_t> output);
} // namespace crowdb::tree::detail
