// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
void RpcChunkTransport::submit_allocate_fixed_chunk(ChunkId id, uint64_t capacity, uint64_t epoch, PagePurpose purpose,
                                                    ChunkTransportCompletion completion)
try {
    if (!valid() || id.empty() || capacity == 0 || capacity > UINT32_MAX || !valid_page_purpose(purpose) ||
        chunk_purpose(id) != purpose) {
        completion.complete(Status::invalid_argument("invalid fixed Chunk allocation"));
        return;
    }
    const auto geometry =
        rpc_chunk_allocation(capacity, impl_->options.strip_capacity_bytes, impl_->options.mirror_copies);
    const auto                     request_id = impl_->next_request_id();
    const FBInt128                 fixed(id.high, id.low);
    flatbuffers::FlatBufferBuilder builder;
    builder.Finish(crowdb::chunkdb::proto::CreateFBAllocateChunkRequest(
        builder, request_id, monotonic_nanos(), &fixed, geometry.granularity_kb, geometry.strip_count,
        FBStripType_Mirror, 0, 0, impl_->options.mirror_copies == 0 ? 2 : impl_->options.mirror_copies,
        static_cast<crowdb::chunkdb::proto::FBChunkType>(purpose), epoch, impl_->options.writer_lease_ms));
    impl_->submit_chunkdb<crowdb::chunkdb::proto::FBAllocateChunkResponse>(
        id, request_id, crowdb::rpc::proto::FBMsgType_EAllocateChunkRequest,
        {builder.GetBufferPointer(), builder.GetSize()}, completion.allocations,
        [owner = impl_, id, capacity, epoch, purpose, completion](Status status, RpcResult result) {
            try {
                if (status.ok()) {
                    const auto *response =
                        verified_response<crowdb::chunkdb::proto::FBAllocateChunkResponse>(result.control);
                    if (response == nullptr) {
                        status = Status::corruption("ChunkDB allocation response is malformed");
                    }
                    else {
                        status = chunkdb_status(response->ret_code(), response->error_msg());
                        Impl::RemoteChunk remote;
                        if (status.ok()) {
                            status = Impl::parse_chunk(response->chunk(), &remote);
                        }
                        if (status.ok() &&
                            (remote.layout.chunk_id != id || remote.owner_epoch != epoch ||
                             remote.layout.logical_capacity < capacity || chunk_purpose(id) != purpose)) {
                            status = Status::corruption("ChunkDB allocation metadata mismatch");
                        }
                        if (status.ok()) {
                            remote.valid_until_ms         = UINT64_MAX;
                            remote.self_fence_deadline_ms = monotonic_millis() + kLivenessSelfFenceMs;
                            owner->cache(std::move(remote));
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
            completion.complete(std::move(status));
        },
        false,
        [stop = completion.stop_requested_fn, context = completion.context] {
            return stop != nullptr && stop(context);
        });
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
