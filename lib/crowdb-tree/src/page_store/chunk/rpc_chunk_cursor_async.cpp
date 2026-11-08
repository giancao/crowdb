// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
void RpcChunkTransport::submit_advance_write(ChunkId id, uint64_t expected, uint64_t acknowledged,
                                             ChunkTransportCompletion completion)
try {
    Impl::RemoteChunk chunk;
    if (!impl_->cached(id, &chunk) ||
        (chunk.self_fence_deadline_ms != 0 && monotonic_millis() >= chunk.self_fence_deadline_ms)) {
        completion.complete(Status::unavailable("tree chunk advance has no local liveness authority"));
        return;
    }
    auto advance = [owner = impl_, acknowledged, completion](Status status, Impl::RemoteChunk remote) {
        try {
            if (!status.ok()) {
                completion.complete(std::move(status));
                return;
            }
            using namespace crowdb::chunkdb::proto;
            const auto                     request_id = owner->next_request_id();
            const FBInt128                 wire_id(remote.layout.chunk_id.high, remote.layout.chunk_id.low);
            flatbuffers::FlatBufferBuilder builder;
            builder.Finish(CreateFBAdvanceChunkWriteRequest(
                builder, request_id, monotonic_nanos(), &wire_id, remote.owner_epoch, remote.modify_ts, acknowledged,
                std::numeric_limits<uint32_t>::max(), owner->options.writer_lease_ms));
            owner->submit_chunkdb<FBAdvanceChunkWriteResponse>(
                remote.layout.chunk_id, request_id, crowdb::rpc::proto::FBMsgType_EAdvanceChunkWriteRequest,
                {builder.GetBufferPointer(), builder.GetSize()}, completion.allocations,
                [owner, remote, acknowledged, completion](Status status, RpcResult result) {
                    try {
                        if (status.ok()) {
                            const auto *response = verified_response<FBAdvanceChunkWriteResponse>(result.control);
                            status = response == nullptr ? Status::corruption("ChunkDB advance response is malformed")
                                                         : chunkdb_status(response->ret_code(), response->error_msg());
                            Impl::RemoteChunk updated;
                            if (status.ok()) {
                                status = Impl::parse_chunk(response->chunk(), &updated);
                            }
                            if (status.ok() && (updated.layout.chunk_id != remote.layout.chunk_id ||
                                                updated.owner_epoch != remote.owner_epoch ||
                                                updated.layout.acknowledged_bytes != acknowledged)) {
                                status = Status::corruption("ChunkDB advance returned another boundary");
                            }
                            if (status.ok()) {
                                updated.self_fence_deadline_ms = remote.self_fence_deadline_ms;
                                owner->cache(std::move(updated));
                            }
                        }
                        completion.complete(std::move(status));
                    }
                    catch (const std::bad_alloc &) {
                        completion.complete(Status::resource_exhausted());
                    }
                    catch (...) {
                        completion.complete(Status::internal_error());
                    }
                },
                false, [completion] { return completion.stop_requested(); });
        }
        catch (const std::bad_alloc &) {
            completion.complete(Status::resource_exhausted());
        }
        catch (...) {
            completion.complete(Status::internal_error());
        }
    };
    if (chunk.layout.acknowledged_bytes == expected) {
        advance(Status::Ok(), std::move(chunk));
    }
    else {
        impl_->submit_query_remote(
            id, completion.allocations,
            [advance, deadline = chunk.self_fence_deadline_ms](Status status, Impl::RemoteChunk remote) {
                remote.self_fence_deadline_ms = deadline;
                advance(std::move(status), std::move(remote));
            },
            [completion] { return completion.stop_requested(); });
    }
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}

void RpcChunkTransport::submit_seal_chunk(ChunkId id, uint64_t epoch, uint64_t acknowledged,
                                          ChunkTransportCompletion completion)
try {
    impl_->submit_query_remote(
        id, completion.allocations,
        [owner = impl_, id, epoch, acknowledged, completion](Status status, Impl::RemoteChunk remote) {
            try {
                if (status.ok() && (remote.owner_epoch != epoch || remote.layout.acknowledged_bytes != acknowledged ||
                                    acknowledged > remote.layout.logical_capacity)) {
                    status = Status::unavailable("tree chunk seal is fenced by owner epoch or cursor");
                }
                if (!status.ok()) {
                    completion.complete(std::move(status));
                    return;
                }
                using namespace crowdb::chunkdb::proto;
                const auto                     request_id = owner->next_request_id();
                const FBInt128                 wire_id(id.high, id.low);
                flatbuffers::FlatBufferBuilder builder;
                builder.Finish(
                    CreateFBSealChunkRequest(builder, request_id, monotonic_nanos(), &wire_id, acknowledged));
                owner->submit_chunkdb<FBSealChunkResponse>(
                    id, request_id, crowdb::rpc::proto::FBMsgType_ESealChunkRequest,
                    {builder.GetBufferPointer(), builder.GetSize()}, completion.allocations,
                    [owner, id, epoch, acknowledged, completion](Status status, RpcResult result) {
                        try {
                            if (status.ok()) {
                                const auto *response = verified_response<FBSealChunkResponse>(result.control);
                                status               = response == nullptr
                                                         ? Status::corruption("ChunkDB seal response is malformed")
                                                         : chunkdb_status(response->ret_code(), response->error_msg());
                                Impl::RemoteChunk updated;
                                if (status.ok()) {
                                    status = Impl::parse_chunk(response->chunk(), &updated);
                                }
                                if (status.ok() &&
                                    (updated.layout.chunk_id != id || updated.owner_epoch != epoch ||
                                     updated.layout.acknowledged_bytes != acknowledged || !updated.layout.sealed)) {
                                    status = Status::corruption(
                                        "ChunkDB seal response does not match the exact sealed boundary");
                                }
                                if (status.ok()) {
                                    updated.valid_until_ms = UINT64_MAX;
                                    owner->cache(std::move(updated));
                                }
                            }
                            completion.complete(std::move(status));
                        }
                        catch (const std::bad_alloc &) {
                            completion.complete(Status::resource_exhausted());
                        }
                        catch (...) {
                            completion.complete(Status::internal_error());
                        }
                    },
                    false, [completion] { return completion.stop_requested(); });
            }
            catch (const std::bad_alloc &) {
                completion.complete(Status::resource_exhausted());
            }
            catch (...) {
                completion.complete(Status::internal_error());
            }
        },
        [completion] { return completion.stop_requested(); });
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
