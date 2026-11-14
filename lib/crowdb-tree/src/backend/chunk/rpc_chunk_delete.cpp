// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
namespace
{
Status deleted(ChunkId id, const RpcResult &result)
{
    using namespace crowdb::chunkdb::proto;
    const auto *response = verified_response<FBDeleteChunkResponse>(result.control);
    if (response == nullptr) {
        return Status::corruption("ChunkDB deletion response is malformed");
    }
    auto status = chunkdb_status(response->ret_code(), response->error_msg());
    if (status.code() == Code::kNotFound) {
        // The caller has already fenced late allocation and durable Deleting
        // forbids new pins. An exact absent identity has no physical strips.
        return Status::Ok();
    }
    if (!status.ok()) {
        return status;
    }
    const auto *chunk = response->chunk();
    if (chunk == nullptr || chunk->id() == nullptr || chunk->id()->high() != id.high || chunk->id()->low() != id.low ||
        chunk->state() != FBChunkState_Deleted || chunk->capacity() != 0 ||
        (chunk->strips() != nullptr && !chunk->strips()->empty())) {
        return Status::corruption("ChunkDB deletion has not released every physical strip");
    }
    return Status::Ok();
}
} // namespace

void RpcChunkTransport::submit_delete_chunk(ChunkId id, ChunkTransportCompletion completion)
try {
    using namespace crowdb::chunkdb::proto;
    if (!valid() || id.empty() || !valid_page_purpose(chunk_purpose(id))) {
        completion.complete(Status::invalid_argument());
        return;
    }
    const auto                     request_id = impl_->next_request_id();
    const FBInt128                 wire_id(id.high, id.low);
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(CreateFBDeleteChunkRequest(builder, request_id, monotonic_nanos(), &wire_id, true));
    impl_->submit_chunkdb<FBDeleteChunkResponse>(
        id, request_id, crowdb::rpc::proto::FBMsgType_EDeleteChunkRequest,
        {builder.GetBufferPointer(), builder.GetSize()}, completion.allocations,
        [owner = impl_, id, completion](Status status, RpcResult result) {
            try {
                if (status.ok()) {
                    status = deleted(id, result);
                }
                if (status.ok()) {
                    owner->forget(id);
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
} // namespace crowdb::tree::detail
