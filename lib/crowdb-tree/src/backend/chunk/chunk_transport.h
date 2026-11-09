// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_io_context.h"
#include "crowdb-common/atomic_shared_ptr.h"
#include "crowdb-tree/backend/page_purpose.h"
#include "crowdb-tree/status.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

namespace crowdb::tree
{
class AllocationBudget;
class ChunkPhysicalRegistry;
} // namespace crowdb::tree

namespace crowdb::tree::detail
{

using common::AtomicSharedPtr;

inline constexpr uint32_t kMaxMirrorCopies = 5;

struct ChunkId
{
    uint64_t high = 0;
    uint64_t low  = 0;

    constexpr ChunkId() = default;

    constexpr ChunkId(uint64_t compact) : low(compact)
    {
    }

    constexpr ChunkId(uint64_t high_bits, uint64_t low_bits) : high(high_bits), low(low_bits)
    {
    }

    constexpr bool operator==(const ChunkId /*unused*/ &) const = default;

    [[nodiscard]] constexpr bool empty() const
    {
        return high == 0 && low == 0;
    }
};

// One immutable physical allocation, including every rounded mirror segment.
struct ChunkAllocationIdentity
{
    ChunkId  chunk;
    uint64_t owner_epoch    = 0;
    uint64_t created_ms     = 0;
    uint64_t physical_bytes = 0;
};

struct ChunkLayout
{
    ChunkId  chunk_id;
    uint64_t logical_capacity   = 0;
    uint64_t acknowledged_bytes = 0;
    bool     sealed             = false;
};

struct ChunkTransportCompletion
{
    void (*complete_fn)(void *context, Status status) = nullptr;
    bool (*stop_requested_fn)(void *context)          = nullptr;
    void                             *context         = nullptr;
    std::shared_ptr<const void>       owner           = nullptr;
    std::shared_ptr<AllocationBudget> allocations     = nullptr;

    void complete(Status status) const
    {
        if (complete_fn != nullptr) {
            complete_fn(context, std::move(status));
        }
    }

    [[nodiscard]] bool stop_requested() const
    {
        return stop_requested_fn != nullptr && stop_requested_fn(context);
    }
};

struct ChunkRegistryPool;

class ChunkTransport
{
  public:
    virtual ~ChunkTransport();
    // Registry identity is stable across writer incarnations for this
    // transport, retirement domain and logical Tree ID. Submission errors do
    // not call complete. The accepted request retains the output owner.
    Status submit_physical_registry(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                                    std::shared_ptr<ChunkPhysicalRegistry> *out,
                                    ChunkTransportCompletion                complete) noexcept;
    // Synchronous compatibility entry for an external host.
    Status physical_registry(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                             std::shared_ptr<ChunkPhysicalRegistry> *out);

    // Fixed identity is durably protected before this call. An unknown result
    // never authorizes generating another identity or releasing that protection.
    [[nodiscard]] virtual bool fixed_staging_supported() const noexcept
    {
        return false;
    }

    virtual Status allocate_fixed_chunk(ChunkId /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/,
                                        PagePurpose /*unused*/)
    {
        return Status::not_supported();
    }

    virtual void submit_allocate_fixed_chunk(ChunkId id, uint64_t capacity, uint64_t epoch, PagePurpose purpose,
                                             ChunkTransportCompletion completion)
    {
        completion.complete(allocate_fixed_chunk(id, capacity, epoch, purpose));
    }

    [[nodiscard]] virtual Status verify_fixed_read_fence(ChunkId /*unused*/, uint32_t /*unused*/,
                                                         ChunkIoContext /*unused*/) const
    {
        return Status::not_supported();
    }

    virtual Status write_fixed_mirror(ChunkId /*unused*/, uint32_t /*unused*/, uint64_t /*unused*/,
                                      const uint8_t * /*unused*/, size_t /*unused*/, ChunkIoContext /*unused*/)
    {
        return Status::not_supported();
    }

    virtual void submit_write_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data,
                                           size_t length, ChunkIoContext context, ChunkTransportCompletion completion)
    {
        completion.complete(write_fixed_mirror(id, mirror, offset, data, length, context));
    }

    virtual Status read_fixed_mirror(ChunkId /*unused*/, uint32_t /*unused*/, uint64_t /*unused*/, uint8_t * /*unused*/,
                                     size_t /*unused*/, ChunkIoContext /*unused*/) const
    {
        return Status::not_supported();
    }

    virtual void submit_verify_fixed_read_fence(ChunkId id, uint32_t mirrors, ChunkIoContext context,
                                                ChunkTransportCompletion completion) const
    {
        completion.complete(verify_fixed_read_fence(id, mirrors, context));
    }

    virtual void submit_read_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length,
                                          ChunkIoContext context, ChunkTransportCompletion completion) const
    {
        completion.complete(read_fixed_mirror(id, mirror, offset, data, length, context));
    }

    // Called only after the creator's fixed request ages are excluded. A
    // successful barrier waits for all already accepted ordered disk writes.
    [[nodiscard]] virtual Status drain_fixed_chunk(ChunkId /*unused*/) const
    {
        return Status::not_supported();
    }

    // Always query authority, not a cached route/layout. A different physical
    // owner cannot be reclaimed using an old stage's logical reference.
    virtual Status inspect_fixed_allocation(ChunkId /*unused*/, uint64_t /*unused*/,
                                            ChunkAllocationIdentity * /*unused*/) const
    {
        return Status::not_supported();
    }

    // Success means ChunkDB has completed physical deletion. Lost replies retain
    // protection and may retry the same immutable identity.
    virtual Status delete_chunk(ChunkId /*unused*/)
    {
        return Status::not_supported();
    }

    virtual void submit_drain_fixed_chunk(ChunkId id, ChunkTransportCompletion completion) const
    {
        completion.complete(drain_fixed_chunk(id));
    }

    virtual void submit_inspect_fixed_allocation(ChunkId id, uint64_t epoch, ChunkAllocationIdentity *out,
                                                 ChunkTransportCompletion completion) const
    {
        completion.complete(inspect_fixed_allocation(id, epoch, out));
    }

    virtual void submit_delete_chunk(ChunkId id, ChunkTransportCompletion completion)
    {
        completion.complete(delete_chunk(id));
    }

    virtual void submit_advance_write(ChunkId id, uint64_t expected, uint64_t acknowledged,
                                      ChunkTransportCompletion completion)
    {
        completion.complete(advance_write(id, expected, acknowledged));
    }

    virtual void submit_seal_chunk(ChunkId id, uint64_t epoch, uint64_t acknowledged,
                                   ChunkTransportCompletion completion)
    {
        completion.complete(seal_chunk(id, epoch, acknowledged));
    }

    virtual Status advance_write(ChunkId /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
    {
        return Status::not_supported();
    }

    virtual Status query_chunk(ChunkId /*unused*/, ChunkLayout * /*unused*/) const
    {
        return Status::not_supported();
    }

    virtual void submit_query_chunk(ChunkId id, ChunkLayout *out, ChunkTransportCompletion completion) const
    {
        completion.complete(query_chunk(id, out));
    }

    virtual Status seal_chunk(ChunkId /*unused*/, uint64_t /*unused*/, uint64_t /*unused*/)
    {
        return Status::not_supported();
    }

  private:
    std::atomic<ChunkRegistryPool *> registry_pool_{nullptr};
};

// Lock-free immutable-snapshot transport for tests and embedded use. It models
// the same allocation, mirror write, acknowledged-cursor, and seal
// boundaries as the production RPC transport.
class MemoryChunkTransport final : public ChunkTransport
{
  public:
    Status allocate_mirror_chunk(uint64_t logical_capacity, uint64_t owner_epoch, ChunkId *chunk_id,
                                 PagePurpose purpose);
    Status write_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, const uint8_t *data, size_t length);
    Status advance_write(ChunkId chunk_id, uint64_t expected_bytes, uint64_t acknowledged_bytes) override;
    Status read_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset, uint8_t *data, size_t length) const;
    Status query_chunk(ChunkId chunk_id, ChunkLayout *layout) const override;
    Status seal_chunk(ChunkId chunk_id, uint64_t owner_epoch, uint64_t acknowledged_bytes) override;

    void corrupt_mirror(ChunkId chunk_id, uint32_t mirror_index, uint64_t offset);

    void inject_unavailable(bool unavailable)
    {
        unavailable_.store(unavailable, std::memory_order_release);
    }

  private:
    struct Chunk
    {
        ChunkLayout                                        layout;
        uint64_t                                           owner_epoch = 0;
        std::array<std::vector<uint8_t>, kMaxMirrorCopies> mirrors;
    };

    using Chunks = std::vector<Chunk>;

    template <typename Mutation> Status mutate(ChunkId chunk_id, Mutation mutation);

    std::atomic<uint64_t>         next_chunk_id_{1};
    AtomicSharedPtr<const Chunks> chunks_;
    std::atomic<bool>             unavailable_{false};
};

} // namespace crowdb::tree::detail
