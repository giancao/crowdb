// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
crowdb_rpc_server_t RpcChunkTransport::Impl::timer_server(ChunkId chunk) const noexcept
{
    auto state = timer_route_state.load(std::memory_order_acquire);
    if (state == 0 && timer_route_state.compare_exchange_strong(state, 1, std::memory_order_acq_rel)) {
        try {
            RpcChunkRouteLease selected;
            auto               status = chunkdb_routes.resolve(chunk, false, &selected);
            if (status.ok() && selected.route.server != nullptr) {
                timer_route.emplace(std::move(selected));
            }
            else {
                // This retains a local event-loop owner only. RPC requests
                // themselves always resolve the original chunk's authority.
                RpcChunkRouteLease bootstrap;
                status = chunkdb_routes.resolve({}, false, &bootstrap);
                if (status.ok() && bootstrap.route.server != nullptr) {
                    timer_route.emplace(std::move(bootstrap));
                }
            }
        }
        catch (...) {
            CRB_LOG_DEBUG("Chunk liveness could not resolve a timer owner");
        }
        timer_route_state.store(timer_route ? 2 : 0, std::memory_order_release);
        state = timer_route ? 2 : 0;
    }
    if (state == 2 && timer_route) {
        return reinterpret_cast<crowdb_rpc_server_t>(timer_route->route.server);
    }
    return nullptr;
}

void RpcChunkTransport::Impl::start_liveness(ChunkId chunk) const noexcept
{
    if (liveness_stopped.load(std::memory_order_acquire) || liveness_timer.load(std::memory_order_acquire) != 0) {
        return;
    }
    auto *const server   = timer_server(chunk);
    uint64_t    expected = 0;
    if (server == nullptr || !liveness_timer.compare_exchange_strong(expected, UINT64_MAX, std::memory_order_acq_rel)) {
        return;
    }
    auto owner = weak_from_this().lock();
    if (owner == nullptr) {
        liveness_timer.store(0, std::memory_order_release);
        return;
    }
    auto *context = new (std::nothrow) std::shared_ptr<const Impl>(std::move(owner));
    if (context == nullptr) {
        liveness_timer.store(0, std::memory_order_release);
        return;
    }
    const auto id = crowdb_rpc_server_schedule(server, 60'000, &Impl::liveness_ready, context);
    if (id == 0) {
        delete context;
        liveness_timer.store(0, std::memory_order_release);
        return;
    }
    expected = UINT64_MAX;
    static_cast<void>(liveness_timer.compare_exchange_strong(expected, id, std::memory_order_acq_rel));
    if (liveness_stopped.load(std::memory_order_acquire)) {
        static_cast<void>(crowdb_rpc_server_cancel_scheduled(server, id));
    }
}

void RpcChunkTransport::Impl::stop_liveness() noexcept
{
    liveness_stopped.store(true, std::memory_order_release);
    const auto id = liveness_timer.load(std::memory_order_acquire);
    if (id != 0 && id != UINT64_MAX && timer_route) {
        static_cast<void>(
            crowdb_rpc_server_cancel_scheduled(reinterpret_cast<crowdb_rpc_server_t>(timer_route->route.server), id));
    }
}

void RpcChunkTransport::Impl::liveness_ready(void *context, int fired) noexcept
{
    std::unique_ptr<std::shared_ptr<const Impl>> retained(static_cast<std::shared_ptr<const Impl> *>(context));
    const auto                                  &owner = *retained;
    owner->liveness_timer.store(0, std::memory_order_release);
    if (fired != 0 && !owner->liveness_stopped.load(std::memory_order_acquire)) {
        try {
            owner->renew_due_liveness();
        }
        catch (...) {
            CRB_LOG_ERROR("Chunk liveness renewal could not be submitted; existing fence deadlines remain active");
        }
        owner->start_liveness();
    }
}

void RpcChunkTransport::Impl::renew_due_liveness() const
{
    const auto current = chunks.load(std::memory_order_acquire);
    if (current == nullptr) {
        return;
    }
    const uint64_t now = monotonic_millis();
    for (const auto &chunk : *current) {
        if (chunk.layout.sealed || chunk.self_fence_deadline_ms == 0 ||
            now + (kLivenessSelfFenceMs - kLivenessRenewalMs) < chunk.self_fence_deadline_ms) {
            continue;
        }
        const uint64_t                 request_id = next_request_id();
        const FBInt128                 id(chunk.layout.chunk_id.high, chunk.layout.chunk_id.low);
        flatbuffers::FlatBufferBuilder builder;
        builder.Finish(crowdb::chunkdb::proto::CreateFBAdvanceChunkWriteRequest(
            builder, request_id, monotonic_nanos(), &id, chunk.owner_epoch, chunk.modify_ts,
            chunk.layout.acknowledged_bytes, std::numeric_limits<uint32_t>::max(), options.writer_lease_ms));
        submit_chunkdb<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(
            chunk.layout.chunk_id, request_id, crowdb::rpc::proto::FBMsgType_EAdvanceChunkWriteRequest,
            {builder.GetBufferPointer(), builder.GetSize()}, {},
            [owner = shared_from_this(), chunk](Status status, RpcResult result) {
                try {
                    const auto *response =
                        status.ok()
                            ? verified_response<crowdb::chunkdb::proto::FBAdvanceChunkWriteResponse>(result.control)
                            : nullptr;
                    RemoteChunk renewed;
                    if (response != nullptr && chunkdb_status(response->ret_code(), response->error_msg()).ok() &&
                        parse_chunk(response->chunk(), &renewed).ok()) {
                        renewed.self_fence_deadline_ms = monotonic_millis() + kLivenessSelfFenceMs;
                        owner->cache(std::move(renewed));
                    }
                    else {
                        owner->fence_liveness(chunk);
                    }
                }
                catch (...) {
                    CRB_LOG_ERROR(
                        "Chunk liveness response could not update its cache; existing fence deadline remains active");
                }
            });
    }
}
} // namespace crowdb::tree::detail
