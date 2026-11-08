// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
struct RpcChunkTransport::Impl::AsyncFence : std::enable_shared_from_this<AsyncFence>
{
    std::shared_ptr<const Impl> owner;
    ChunkTransportCompletion    completion;
    ChunkIoContext              context;
    uint32_t                    mirrors = 0;
    bool                        drain   = false;
    std::atomic<size_t>         remaining{1};
    std::atomic<bool>           failed{false};
    Status                      failure;

    void completed(Status status)
    {
        if (!status.ok() && !failed.exchange(true, std::memory_order_acq_rel)) {
            failure = std::move(status);
        }
        if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            auto done = std::exchange(completion, {});
            done.complete(std::move(failure));
        }
    }

    void submit_segment(const Segment &segment, uint32_t unit_kb)
    {
        if (completion.stop_requested()) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            completed(Status::unavailable("chunk barrier stopped before submission"));
            return;
        }
        RpcChunkRouteLease route;
        auto               status = owner->resolve_route(segment, &route);
        if (!status.ok()) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            completed(std::move(status));
            return;
        }
        const uint64_t unit_bytes = static_cast<uint64_t>(unit_kb) * 1024;
        if (unit_bytes == 0 || segment.unit_offset > UINT64_MAX / unit_bytes) {
            remaining.fetch_add(1, std::memory_order_relaxed);
            completed(Status::corruption("Chunk barrier physical offset overflow"));
            return;
        }
        const auto                     request_id = owner->next_request_id();
        const FBInt128                 disk(segment.disk_high, segment.disk_low);
        flatbuffers::FlatBufferBuilder builder;
        uint16_t                       message_type;
        if (drain) {
            const auto offset = segment.unit_offset * unit_bytes;
            builder.Finish(crowdb::diskio::proto::CreateFBDiskWriteRequest(builder, request_id, monotonic_nanos(),
                                                                           &disk, segment.zone_index, offset, 0, offset,
                                                                           wall_time_ms(), true));
            message_type = crowdb::rpc::proto::FBMsgType_EDiskWriteRequest;
        }
        else {
            builder.Finish(crowdb::diskio::proto::CreateFBDiskReadRequest(
                builder, request_id, monotonic_nanos(), &disk, segment.zone_index, 0, 0, 0, context.created_ms, true));
            message_type = crowdb::rpc::proto::FBMsgType_EDiskReadRequest;
        }
        RpcComplete ready = [self = shared_from_this()](Status status, RpcResult result) {
            try {
                if (status.ok() && self->drain) {
                    const auto *reply = verified_response<crowdb::diskio::proto::FBDiskWriteResponse>(result.control);
                    status            = reply == nullptr ? Status::corruption("DiskIO drain response is malformed")
                                                         : diskio_status(reply->ret_code());
                    if (status.ok() && !reply->sync_completed()) {
                        status = Status::not_supported("DiskIO cannot confirm ordered drain");
                    }
                }
                else if (status.ok()) {
                    const auto *reply = verified_response<crowdb::diskio::proto::FBDiskReadResponse>(result.control);
                    status            = reply == nullptr ? Status::corruption("DiskIO fence response is malformed")
                                                         : diskio_status(reply->ret_code());
                    if (status.ok() && !reply->request_age_checked()) {
                        status = Status::not_supported("DiskIO cannot fence fixed-pack reads");
                    }
                }
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted();
            }
            catch (...) {
                status = Status::internal_error();
            }
            self->completed(std::move(status));
        };
        remaining.fetch_add(1, std::memory_order_relaxed);
        try {
            submit_rpc(std::move(route), request_id, message_type, {builder.GetBufferPointer(), builder.GetSize()}, {},
                       completion.allocations, std::move(ready));
        }
        catch (const std::bad_alloc &) {
            completed(Status::resource_exhausted());
        }
        catch (...) {
            completed(Status::internal_error());
        }
    }

    void loaded(Status status, RemoteChunk remote) noexcept
    {
        try {
            if (status.ok()) {
                for (const auto &strip : remote.strips) {
                    const auto count = drain ? strip.mirrors.size() : mirrors;
                    if (count > strip.mirrors.size()) {
                        status = Status::invalid_argument("Chunk fence mirror count exceeds layout");
                        break;
                    }
                    for (size_t index = 0; index < count; ++index) {
                        submit_segment(strip.mirrors[index], strip.unit_kb);
                    }
                }
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        completed(std::move(status));
    }
};

void RpcChunkTransport::submit_verify_fixed_read_fence(ChunkId id, uint32_t mirrors, ChunkIoContext context,
                                                       ChunkTransportCompletion completion) const
try {
    if (context.created_ms == 0 || mirrors == 0 || mirrors > kMaxMirrorCopies) {
        completion.complete(Status::invalid_argument());
        return;
    }
    auto state        = reserved_shared<Impl::AsyncFence>(completion.allocations);
    state->owner      = impl_;
    state->completion = completion;
    state->context    = context;
    state->mirrors    = mirrors;
    Impl::RemoteChunk remote;
    if (impl_->cached_valid(id, &remote)) {
        state->loaded(Status::Ok(), std::move(remote));
    }
    else {
        impl_->submit_query_remote(
            id, completion.allocations,
            [state](Status status, Impl::RemoteChunk remote) { state->loaded(std::move(status), std::move(remote)); },
            [completion] { return completion.stop_requested(); });
    }
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}

void RpcChunkTransport::submit_drain_fixed_chunk(ChunkId id, ChunkTransportCompletion completion) const
try {
    using namespace crowdb::chunkdb::proto;
    if (!valid() || id.empty() || !valid_page_purpose(chunk_purpose(id))) {
        completion.complete(Status::invalid_argument());
        return;
    }
    auto state                                = reserved_shared<Impl::AsyncFence>(completion.allocations);
    state->owner                              = impl_;
    state->completion                         = completion;
    state->drain                              = true;
    const auto                     request_id = impl_->next_request_id();
    const FBInt128                 wire_id(id.high, id.low);
    flatbuffers::FlatBufferBuilder query;
    query.Finish(CreateFBQueryChunkRequest(query, request_id, monotonic_nanos(), &wire_id));
    impl_->submit_chunkdb<FBQueryChunkResponse>(
        id, request_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, {query.GetBufferPointer(), query.GetSize()},
        completion.allocations,
        [state, id](Status status, RpcResult result) {
            Impl::RemoteChunk remote;
            try {
                if (status.ok()) {
                    const auto *response = verified_response<FBQueryChunkResponse>(result.control);
                    status = response == nullptr ? Status::corruption("ChunkDB drain query is malformed")
                                                 : chunkdb_status(response->ret_code(), response->error_msg());
                    if (status.ok()) {
                        const auto *chunk = response->chunk();
                        if (chunk == nullptr || chunk->id() == nullptr || chunk->id()->high() != id.high ||
                            chunk->id()->low() != id.low) {
                            status = Status::corruption("ChunkDB drain identity differs");
                        }
                        else if (chunk->capacity() != 0 || (chunk->strips() != nullptr && !chunk->strips()->empty())) {
                            status = Impl::parse_chunk(chunk, &remote);
                        }
                    }
                    else if (status.code() == Code::kNotFound) {
                        status = Status::Ok();
                    }
                }
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted();
            }
            catch (...) {
                status = Status::internal_error();
            }
            state->loaded(std::move(status), std::move(remote));
        },
        true, [completion] { return completion.stop_requested(); });
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
