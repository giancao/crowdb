// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
void RpcChunkTransport::Impl::submit_query_remote(ChunkId chunk, const std::shared_ptr<AllocationBudget> &metadata,
                                                  RemoteComplete complete, std::function<bool()> stopped) const
{
    const uint64_t                 request_id = next_request_id();
    const FBInt128                 id(chunk.high, chunk.low);
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(crowdb::chunkdb::proto::CreateFBQueryChunkRequest(builder, request_id, monotonic_nanos(), &id));
    submit_chunkdb<crowdb::chunkdb::proto::FBQueryChunkResponse>(
        chunk, request_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest,
        {builder.GetBufferPointer(), builder.GetSize()}, metadata,
        [owner = shared_from_this(), chunk, complete](Status status, RpcResult result) {
            RemoteChunk remote;
            try {
                if (status.ok()) {
                    const auto *response =
                        verified_response<crowdb::chunkdb::proto::FBQueryChunkResponse>(result.control);
                    if (response == nullptr) {
                        status = Status::corruption("ChunkDB query response is malformed");
                    }
                    else {
                        status = chunkdb_status(response->ret_code(), response->error_msg());
                        if (status.ok()) {
                            status = parse_chunk(response->chunk(), &remote);
                        }
                        if (status.ok() && remote.layout.chunk_id != chunk) {
                            status = Status::corruption("ChunkDB query returned another chunk");
                        }
                        if (status.ok()) {
                            const auto now        = monotonic_millis();
                            remote.valid_until_ms = response->layout_validity_ms() > UINT64_MAX - now
                                                      ? UINT64_MAX
                                                      : now + response->layout_validity_ms();
                            owner->cache(remote);
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
            complete(std::move(status), std::move(remote));
        },
        true, std::move(stopped));
}

void RpcChunkTransport::submit_query_chunk(ChunkId id, ChunkLayout *out, ChunkTransportCompletion completion) const
try {
    if (out == nullptr) {
        completion.complete(Status::invalid_argument());
        return;
    }
    impl_->submit_query_remote(
        id, completion.allocations,
        [completion, out](Status status, Impl::RemoteChunk chunk) {
            if (status.ok()) {
                *out = chunk.layout;
            }
            completion.complete(std::move(status));
        },
        [stop = completion.stop_requested_fn, context = completion.context] {
            return stop != nullptr && stop(context);
        });
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
