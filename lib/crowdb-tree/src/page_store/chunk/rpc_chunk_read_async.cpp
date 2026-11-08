// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
struct RpcChunkTransport::Impl::AsyncRead : std::enable_shared_from_this<AsyncRead>
{
    std::shared_ptr<const Impl> owner;
    RemoteChunk                 chunk;
    uint32_t                    mirror   = 0;
    uint64_t                    offset   = 0;
    uint8_t                    *data     = nullptr;
    size_t                      length   = 0;
    size_t                      consumed = 0;
    ChunkIoContext              context;
    ChunkTransportCompletion    completion;

    void finish(Status status)
    {
        auto done = std::exchange(completion, {});
        done.complete(std::move(status));
    }

    void loaded(Status status, RemoteChunk remote) noexcept
    try {
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        chunk = std::move(remote);
        if (offset > chunk.layout.acknowledged_bytes || length > chunk.layout.acknowledged_bytes - offset) {
            finish(Status::unavailable("tree chunk RPC read exceeds acknowledged cursor"));
            return;
        }
        next();
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    void received(Status status, RpcResult result, size_t bytes) noexcept
    try {
        if (status.ok()) {
            const auto *response = verified_response<crowdb::diskio::proto::FBDiskReadResponse>(result.control);
            status               = response == nullptr ? Status::corruption("DiskIO read response is malformed")
                                                       : diskio_status(response->ret_code());
            if (status.ok() && !response->request_age_checked()) {
                status = Status::not_supported("fixed pack read requires request-age fencing");
            }
        }
        if (status.ok() && (result.data == nullptr || crowdb_rpc_buffer_len(result.data) != bytes)) {
            status = Status::corruption("DiskIO read response length differs");
        }
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        std::memcpy(data + consumed, crowdb_rpc_buffer_data(result.data), bytes);
        consumed += bytes;
        next();
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    void next() noexcept
    try {
        if (completion.stop_requested()) {
            finish(Status::unavailable("chunk RPC read stopped before submission"));
            return;
        }
        if (consumed == length) {
            finish(Status::Ok());
            return;
        }
        const uint64_t cursor = offset + consumed;
        const auto     strip  = std::find_if(chunk.strips.begin(), chunk.strips.end(), [cursor](const Strip &item) {
            return cursor >= item.chunk_offset && cursor - item.chunk_offset < item.capacity;
        });
        if (strip == chunk.strips.end() || mirror >= strip->mirrors.size()) {
            finish(Status::corruption("tree chunk RPC read has no matching mirror range"));
            return;
        }
        const size_t bytes   = std::min<uint64_t>(length - consumed, strip->capacity - (cursor - strip->chunk_offset));
        const auto  &segment = strip->mirrors[mirror];
        RpcChunkRouteLease route;
        auto               status = owner->resolve_route(segment, &route);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        const uint64_t unit_bytes  = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t zone_offset = (segment.unit_offset * unit_bytes) + (cursor - strip->chunk_offset);
        const auto     request_id  = owner->next_request_id();
        const FBInt128 disk(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        builder.Finish(crowdb::diskio::proto::CreateFBDiskReadRequest(
            builder, request_id, monotonic_nanos(), &disk, segment.zone_index, zone_offset,
            static_cast<uint32_t>(bytes), 0, context.created_ms, false));
        submit_rpc(std::move(route), request_id, crowdb::rpc::proto::FBMsgType_EDiskReadRequest,
                   {builder.GetBufferPointer(), builder.GetSize()}, {}, completion.allocations,
                   [self = shared_from_this(), bytes](Status status, RpcResult result) {
                       self->received(std::move(status), std::move(result), bytes);
                   });
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }
};

void RpcChunkTransport::submit_read_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, uint8_t *data,
                                                 size_t length, ChunkIoContext context,
                                                 ChunkTransportCompletion completion) const
try {
    if (context.created_ms == 0 || (data == nullptr && length != 0) || length > UINT64_MAX - offset) {
        completion.complete(Status::invalid_argument());
        return;
    }
    auto state        = reserved_shared<Impl::AsyncRead>(completion.allocations);
    state->owner      = impl_;
    state->mirror     = mirror;
    state->offset     = offset;
    state->data       = data;
    state->length     = length;
    state->context    = context;
    state->completion = completion;
    Impl::RemoteChunk remote;
    if (impl_->cached_valid(id, &remote)) {
        state->loaded(Status::Ok(), std::move(remote));
    }
    else {
        impl_->submit_query_remote(id, completion.allocations, [state](Status status, Impl::RemoteChunk remote) {
            state->loaded(std::move(status), std::move(remote));
        });
    }
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}

} // namespace crowdb::tree::detail
