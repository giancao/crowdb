// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
namespace
{
Status decode_allocation(const crowdb::chunkdb::proto::FBQueryChunkResponse *response, ChunkId id, uint64_t epoch,
                         ChunkAllocationIdentity *out)
{
    using namespace crowdb::chunkdb::proto;
    if (response == nullptr) {
        return Status::corruption("Chunk allocation query is malformed");
    }
    auto status = chunkdb_status(response->ret_code(), response->error_msg());
    if (!status.ok()) {
        return status;
    }
    const auto *chunk = response->chunk();
    if (chunk == nullptr || chunk->id() == nullptr || chunk->id()->high() != id.high || chunk->id()->low() != id.low) {
        return Status::corruption("Chunk allocation identity differs");
    }
    if (chunk->state() == FBChunkState_Deleted && chunk->capacity() == 0 &&
        (chunk->strips() == nullptr || chunk->strips()->empty())) {
        return Status::not_found();
    }
    if (chunk->writer_epoch() != epoch) {
        return Status::unavailable("retired stage names another physical owner");
    }
    if (chunk->strips() == nullptr || chunk->strips()->empty()) {
        return Status::corruption("Chunk allocation has no physical segments");
    }
    uint64_t bytes = 0;
    for (const auto *strip : *chunk->strips()) {
        const auto *mirror = strip == nullptr ? nullptr : strip->strip_body_as_FBMirrorStrip();
        if (strip == nullptr || strip->strip_type() != FBStripType_Mirror || strip->unit_kb() == 0 ||
            mirror == nullptr || mirror->segments() == nullptr || mirror->segments()->empty()) {
            return Status::corruption("Chunk allocation is not a complete mirror layout");
        }
        const uint64_t unit = static_cast<uint64_t>(strip->unit_kb()) * 1024;
        for (const auto *segment : *mirror->segments()) {
            if (segment == nullptr || segment->unit_count() == 0 || segment->owner_chunk().high() != id.high ||
                segment->owner_chunk().low() != id.low || segment->unit_count() > (UINT64_MAX - bytes) / unit) {
                return Status::corruption("Chunk physical segment identity or capacity differs");
            }
            bytes += segment->unit_count() * unit;
        }
    }
    *out = {.chunk = id, .owner_epoch = epoch, .created_ms = chunk->create_ts_ms(), .physical_bytes = bytes};
    return Status::Ok();
}
} // namespace

void RpcChunkTransport::submit_inspect_fixed_allocation(ChunkId id, uint64_t epoch, ChunkAllocationIdentity *out,
                                                        ChunkTransportCompletion completion) const
try {
    using namespace crowdb::chunkdb::proto;
    if (!valid() || id.empty() || epoch == 0 || out == nullptr) {
        completion.complete(Status::invalid_argument());
        return;
    }
    const auto                     request_id = impl_->next_request_id();
    const FBInt128                 wire_id(id.high, id.low);
    flatbuffers::FlatBufferBuilder query;
    query.Finish(CreateFBQueryChunkRequest(query, request_id, monotonic_nanos(), &wire_id));
    impl_->submit_chunkdb<FBQueryChunkResponse>(
        id, request_id, crowdb::rpc::proto::FBMsgType_EQueryChunkRequest, {query.GetBufferPointer(), query.GetSize()},
        completion.allocations,
        [id, epoch, out, completion](Status status, RpcResult result) {
            try {
                if (status.ok()) {
                    status = decode_allocation(verified_response<FBQueryChunkResponse>(result.control), id, epoch, out);
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
        true, [completion] { return completion.stop_requested(); });
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
