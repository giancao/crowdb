// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_transport.h"
#include "crowdb-tree/c_api.h"

#include <memory>
#include <optional>

namespace crowdb::tree::detail
{

// Direct ChunkDB/DiskIO FlatBuffers transport over the callback C++ RPC slab.
// Wire types and RPC implementation headers remain confined to its translation
// unit so neither the public tree ABI nor local page store objects depend on them.
class RpcChunkTransport final : public ChunkTransport
{
  public:
    explicit RpcChunkTransport(const ct_chunk_rpc_transport_options &options);
    ~RpcChunkTransport() override;

    void submit_allocate_fixed_chunk(ChunkId id, uint64_t capacity, uint64_t epoch, PagePurpose purpose,
                                     ChunkTransportCompletion completion) override;
    void submit_write_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length,
                                   ChunkIoContext context, ChunkTransportCompletion completion) override;

    [[nodiscard]] bool fixed_staging_supported() const noexcept override
    {
        return true;
    }

    void submit_query_chunk(ChunkId id, ChunkLayout *out, ChunkTransportCompletion completion) const override;

    void submit_verify_fixed_read_fence(ChunkId id, uint32_t mirrors, ChunkIoContext context,
                                        ChunkTransportCompletion completion) const override;
    void submit_read_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data, size_t length,
                                  ChunkIoContext context, ChunkTransportCompletion completion) const override;
    void submit_drain_fixed_chunk(ChunkId id, ChunkTransportCompletion completion) const override;
    void submit_inspect_fixed_allocation(ChunkId id, uint64_t epoch, ChunkAllocationIdentity *out,
                                         ChunkTransportCompletion completion) const override;
    void submit_delete_chunk(ChunkId id, ChunkTransportCompletion completion) override;
    void submit_advance_write(ChunkId id, uint64_t expected, uint64_t acknowledged,
                              ChunkTransportCompletion completion) override;
    void submit_seal_chunk(ChunkId id, uint64_t epoch, uint64_t acknowledged,
                           ChunkTransportCompletion completion) override;

    [[nodiscard]] bool valid() const;

  private:
    struct Impl;
    std::shared_ptr<Impl> impl_;
};

} // namespace crowdb::tree::detail
