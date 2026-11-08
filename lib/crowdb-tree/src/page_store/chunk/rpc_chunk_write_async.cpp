// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-common/log.h"
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
struct RpcChunkTransport::Impl::AsyncWrite : std::enable_shared_from_this<AsyncWrite>
{
    AsyncWrite(Impl &value, RemoteChunk remote, uint32_t mirror, uint64_t begin, const uint8_t *input, size_t bytes,
               ChunkTransportCompletion complete, ChunkIoContext fixed)
        : owner(value.shared_from_this()),
          chunk(std::move(remote)),
          mirror_index(mirror),
          offset(begin),
          data(input),
          length(bytes),
          completion(std::move(complete)),
          context(fixed)
    {
    }

    std::shared_ptr<Impl>    owner;
    RemoteChunk              chunk;
    uint32_t                 mirror_index;
    uint64_t                 offset;
    const uint8_t           *data;
    size_t                   length;
    size_t                   consumed = 0;
    ChunkTransportCompletion completion;
    ChunkIoContext           context;
    std::atomic<bool>        terminal{false};

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        auto done = std::move(completion);
        try {
            done.complete(std::move(status));
        }
        catch (...) {
            CRB_LOG_ERROR("Chunk write receiver threw after terminal");
        }
    }

    void written(Status status, RpcResult result) noexcept
    try {
        if (status.ok()) {
            const auto *response = verified_response<crowdb::diskio::proto::FBDiskWriteResponse>(result.control);
            status               = response == nullptr ? Status::corruption("DiskIO write response is malformed")
                                                       : diskio_status(response->ret_code());
            if (status.ok() && !response->sync_completed()) {
                status = Status::not_supported("fixed pack write requires synchronized completion");
            }
        }
        if (!status.ok()) {
            finish(std::move(status));
        }
        else {
            submit_part();
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    void submit_part() noexcept
    try {
        if (completion.stop_requested()) {
            finish(Status::unavailable("chunk RPC write stopped before transport submission"));
            return;
        }
        if (consumed == length) {
            finish(Status::Ok());
            return;
        }
        const uint64_t cursor = offset + consumed;
        const auto     strip  = std::find_if(chunk.strips.begin(), chunk.strips.end(), [cursor](const Strip &item) {
            return cursor >= item.chunk_offset && cursor < item.chunk_offset + item.capacity;
        });
        if (strip == chunk.strips.end()) {
            finish(Status::resource_exhausted("tree chunk RPC write exceeds allocated strips"));
            return;
        }
        if (mirror_index >= strip->mirrors.size()) {
            finish(Status::invalid_argument("tree chunk mirror index exceeds layout"));
            return;
        }
        const size_t bytes   = std::min<uint64_t>(length - consumed, strip->chunk_offset + strip->capacity - cursor);
        const auto  &segment = strip->mirrors[mirror_index];
        RpcChunkRouteLease route;
        auto               status = owner->resolve_route(segment, &route);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        const uint64_t                 unit_bytes   = static_cast<uint64_t>(strip->unit_kb) * 1024;
        const uint64_t                 segment_base = segment.unit_offset * unit_bytes;
        const uint64_t                 zone_offset  = segment_base + (cursor - strip->chunk_offset);
        const uint64_t                 request_id   = owner->next_request_id();
        const FBInt128                 disk_id(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        builder.Finish(crowdb::diskio::proto::CreateFBDiskWriteRequest(
            builder, request_id, monotonic_nanos(), &disk_id, segment.zone_index, zone_offset,
            static_cast<uint32_t>(bytes), segment_base, context.created_ms, true));
        const auto input = std::span(data + consumed, bytes);
        consumed += bytes;
        submit_rpc(std::move(route), request_id, crowdb::rpc::proto::FBMsgType_EDiskWriteRequest,
                   {builder.GetBufferPointer(), builder.GetSize()}, input, completion.allocations,
                   [self = shared_from_this()](Status status, RpcResult result) {
                       self->written(std::move(status), std::move(result));
                   });
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted("chunk RPC write allocation failed"));
    }
    catch (...) {
        finish(Status::internal_error("chunk RPC write submission failed"));
    }
};

void RpcChunkTransport::Impl::submit_write(RemoteChunk chunk, uint32_t mirror_index, uint64_t offset,
                                           const uint8_t *data, size_t length, ChunkTransportCompletion completion,
                                           ChunkIoContext context)
try {
    const auto budget = completion.allocations;
    auto       state  = reserved_shared<AsyncWrite>(budget, *this, std::move(chunk), mirror_index, offset, data, length,
                                                    completion, context);
    state->submit_part();
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted("chunk RPC write state allocation failed"));
}

void RpcChunkTransport::submit_write_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data,
                                                  size_t length, ChunkIoContext context,
                                                  ChunkTransportCompletion completion)
{
    if (context.created_ms == 0 || (data == nullptr && length != 0) || offset > UINT64_MAX - length) {
        completion.complete(Status::invalid_argument("tree chunk RPC mirror write arguments are invalid"));
        return;
    }
    Impl::RemoteChunk chunk;
    if (!impl_->cached(id, &chunk)) {
        completion.complete(Status::unavailable("tree chunk write has no local liveness authority"));
        return;
    }
    if (chunk.self_fence_deadline_ms != 0 && monotonic_millis() >= chunk.self_fence_deadline_ms) {
        completion.complete(Status::unavailable("tree chunk liveness authority has self-fenced"));
        return;
    }
    impl_->submit_write(std::move(chunk), mirror, offset, data, length, std::move(completion), context);
}
} // namespace crowdb::tree::detail
