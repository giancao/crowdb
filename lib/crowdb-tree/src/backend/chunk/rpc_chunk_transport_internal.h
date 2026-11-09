// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "chunk_c_api_internal.h"
#include "chunk_purpose.h"
#include "chunkdb_generated.h"
#include "crowdb-rpc/c_api.h"
#include "diskio_generated.h"
#include "msg_type_generated.h"
#include "rpc_chunk_allocation.h"
#include "rpc_chunk_result.h"
#include "rpc_chunk_route.h"
#include "rpc_chunk_transport.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <string>
#include <utility>
#include <vector>

namespace crowdb::tree::detail
{
namespace rpc_chunk_detail
{

using crowdb::chunkdb::proto::FBChunk;
using crowdb::chunkdb::proto::FBChunkdbRetCode;
using crowdb::chunkdb::proto::FBChunkdbRetCode_Success;
using crowdb::chunkdb::proto::FBChunkState_Sealed;
using crowdb::chunkdb::proto::FBStripType_Mirror;
using crowdb::rpc::proto::FBInt128;

inline uint64_t monotonic_nanos()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

inline uint64_t wall_time_ms()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count());
}

inline uint64_t monotonic_millis()
{
    return monotonic_nanos() / 1'000'000;
}

inline constexpr uint64_t kLivenessRenewalMs   = 12U * 60U * 1000U;
inline constexpr uint64_t kLivenessSelfFenceMs = 13U * 60U * 1000U;

template <typename Response> const Response *verified_response(crowdb_rpc_buffer_t buffer)
{
    if (buffer == nullptr) {
        return nullptr;
    }
    const auto           *bytes  = crowdb_rpc_buffer_data(buffer);
    const auto            length = crowdb_rpc_buffer_len(buffer);
    flatbuffers::Verifier verifier(bytes, length);
    return verifier.VerifyBuffer<Response>(nullptr) ? flatbuffers::GetRoot<Response>(bytes) : nullptr;
}

inline Status chunkdb_status(FBChunkdbRetCode code, const flatbuffers::String *error)
{
    using namespace crowdb::chunkdb::proto;
    const std::string message = error == nullptr || error->empty() ? "tree chunk request failed" : error->str();
    switch (code) {
    case FBChunkdbRetCode_Success:
        return Status::Ok();
    case crowdb::chunkdb::proto::FBChunkdbRetCode_NotFound:
        return Status::not_found(message);
    case FBChunkdbRetCode_InvalidArgument:
    case FBChunkdbRetCode_FailedPrecondition:
    case FBChunkdbRetCode_StripIndexOutOfRange:
        return Status::invalid_argument("ChunkDB rejected the tree chunk request: " + message);
    case FBChunkdbRetCode_Unavailable:
    case FBChunkdbRetCode_NotMyRange:
    case FBChunkdbRetCode_Aborted:
        return Status::unavailable("ChunkDB could not complete the tree chunk request: " + message);
    default:
        return Status::internal_error("ChunkDB returned an unexpected tree chunk result: " + message);
    }
}

inline Status diskio_status(crowdb::diskio::proto::FBDiskIoRetCode code)
{
    using namespace crowdb::diskio::proto;
    switch (code) {
    case FBDiskIoRetCode_Success:
        return Status::Ok();
    case FBDiskIoRetCode_InvalidAlignment:
        return Status::invalid_argument("DiskIO rejected tree page alignment");
    case FBDiskIoRetCode_ConnectionError:
    case FBDiskIoRetCode_DiskNotExist:
    case FBDiskIoRetCode_ZoneNotExist:
    case FBDiskIoRetCode_OldRequest:
        return Status::unavailable("DiskIO tree page target is unavailable");
    default:
        return Status::internal_error("DiskIO tree page operation failed");
    }
}

} // namespace rpc_chunk_detail

using namespace rpc_chunk_detail;

struct RpcChunkTransport::Impl : std::enable_shared_from_this<Impl>
{
    struct AsyncWrite;
    struct AsyncRead;
    struct AsyncFence;
    struct ChunkdbCall;

    struct Segment
    {
        uint64_t disk_high   = 0;
        uint64_t disk_low    = 0;
        uint64_t unit_offset = 0;
        uint32_t zone_index  = 0;
    };

    struct Strip
    {
        uint64_t             chunk_offset = 0;
        uint64_t             capacity     = 0;
        uint32_t             unit_kb      = 0;
        std::vector<Segment> mirrors;
    };

    struct RemoteChunk
    {
        ChunkLayout        layout;
        uint64_t           owner_epoch            = 0;
        uint64_t           modify_ts              = 0;
        uint64_t           valid_until_ms         = 0;
        uint64_t           self_fence_deadline_ms = 0;
        std::vector<Strip> strips;
    };

    using RemoteChunks   = std::vector<RemoteChunk>;
    using RemoteComplete = std::function<void(Status, RemoteChunk)>;

    explicit Impl(const ct_chunk_rpc_transport_options &configured)
        : chunkdb_routes(configured),
          disk_resolver(configured.disk_resolver),
          options(configured)
    {
        if (configured.disk_routes != nullptr) {
            disk_routes.assign(configured.disk_routes, configured.disk_routes + configured.disk_route_count);
        }
        options.disk_routes                = nullptr;
        options.disk_route_count           = 0;
        const uint32_t completion_capacity = configured.completion_capacity == 0 ? 256 : configured.completion_capacity;
        const uint64_t timeout_ms          = configured.rpc_timeout_ms == 0 ? 30'000 : configured.rpc_timeout_ms;
        if (configured.chunkdb.client != nullptr) {
            crowdb_rpc_client_set_completion_pool_size(reinterpret_cast<crowdb_rpc_client_t>(configured.chunkdb.client),
                                                       completion_capacity);
            crowdb_rpc_client_start_reaper(reinterpret_cast<crowdb_rpc_client_t>(configured.chunkdb.client),
                                           timeout_ms * 1'000'000, 100'000'000);
        }
        for (const auto &disk : disk_routes) {
            if (disk.route.client != nullptr) {
                crowdb_rpc_client_set_completion_pool_size(reinterpret_cast<crowdb_rpc_client_t>(disk.route.client),
                                                           completion_capacity);
                crowdb_rpc_client_start_reaper(reinterpret_cast<crowdb_rpc_client_t>(disk.route.client),
                                               timeout_ms * 1'000'000, 100'000'000);
            }
        }
    }

    [[nodiscard]] bool valid() const
    {
        return options.mirror_copies <= kMaxMirrorCopies && options.strip_capacity_bytes % 1024 == 0 &&
               chunkdb_routes.valid() && (disk_resolver.valid() || !disk_routes.empty());
    }

    [[nodiscard]] uint64_t next_request_id() const
    {
        return request_ids.fetch_add(1, std::memory_order_relaxed);
    }

    Status resolve_route(const Segment &segment, RpcChunkRouteLease *lease) const
    {
        if (disk_resolver.valid()) {
            return disk_resolver.resolve(ChunkId(segment.disk_high, segment.disk_low), false, lease);
        }
        auto found = std::find_if(disk_routes.begin(), disk_routes.end(), [&segment](const auto &item) {
            return item.disk_id_high == segment.disk_high && item.disk_id_low == segment.disk_low;
        });
        if (found == disk_routes.end() || found->route.client == nullptr || found->route.server == nullptr ||
            found->route.connection == nullptr) {
            return Status::unavailable("DiskIO route is unavailable");
        }
        lease->route = found->route;
        return Status::Ok();
    }

    static Status parse_chunk(const FBChunk *chunk, RemoteChunk *out)
    {
        if (chunk == nullptr || chunk->id() == nullptr) {
            return Status::corruption("ChunkDB returned malformed tree chunk metadata");
        }
        // ChunkDB keeps a Deleted tombstone after releasing the physical
        // strips. Treat that state as an unavailable tree root instead of
        // misclassifying the intentionally empty strips vector as malformed
        // metadata. The owner can then repair or retire the assignment.
        if (chunk->state() == crowdb::chunkdb::proto::FBChunkState_Deleted) {
            return Status::unavailable("tree chunk is deleted");
        }
        RemoteChunk parsed;
        parsed.layout = {
            .chunk_id           = ChunkId(chunk->id()->high(), chunk->id()->low()),
            .logical_capacity   = static_cast<uint64_t>(chunk->capacity()) * 1024,
            .acknowledged_bytes = chunk->acknowledged_cursor(),
            .sealed             = chunk->state() == FBChunkState_Sealed,
        };
        const auto purpose = chunk_purpose(parsed.layout.chunk_id);
        if (!valid_page_purpose(purpose) ||
            static_cast<uint8_t>(chunk->chunk_type()) != static_cast<uint8_t>(purpose)) {
            return Status::corruption("ChunkDB tree chunk purpose mismatch");
        }
        parsed.owner_epoch = chunk->writer_epoch();
        parsed.modify_ts   = chunk->modify_ts();
        if (chunk->strips() == nullptr || chunk->strips()->empty()) {
            if (!parsed.layout.sealed || parsed.layout.logical_capacity != 0 || parsed.layout.acknowledged_bytes != 0) {
                return Status::corruption("ChunkDB tree chunk has no valid physical layout");
            }
            *out = std::move(parsed);
            return Status::Ok();
        }
        for (const auto *wire_strip : *chunk->strips()) {
            const auto *mirror = wire_strip == nullptr ? nullptr : wire_strip->strip_body_as_FBMirrorStrip();
            if (wire_strip == nullptr || wire_strip->strip_type() != FBStripType_Mirror || mirror == nullptr ||
                mirror->segments() == nullptr || mirror->segments()->empty() || wire_strip->unit_kb() == 0) {
                return Status::corruption("ChunkDB returned a non-mirror tree chunk layout");
            }
            Strip strip{.chunk_offset = static_cast<uint64_t>(wire_strip->chunk_offset()) * 1024,
                        .capacity     = static_cast<uint64_t>(wire_strip->capacity()) * 1024,
                        .unit_kb      = wire_strip->unit_kb(),
                        .mirrors      = {}};
            strip.mirrors.reserve(mirror->segments()->size());
            for (size_t index = 0; index < mirror->segments()->size(); ++index) {
                const auto *segment = mirror->segments()->Get(index);
                strip.mirrors.push_back({
                    .disk_high   = segment->disk_id().high(),
                    .disk_low    = segment->disk_id().low(),
                    .unit_offset = segment->unit_offset(),
                    .zone_index  = segment->zone_index(),
                });
            }
            parsed.strips.push_back(strip);
        }
        *out = std::move(parsed);
        return Status::Ok();
    }

    void cache(RemoteChunk chunk) const
    {
        auto current = chunks.load(std::memory_order_acquire);
        for (;;) {
            auto next =
                current == nullptr ? std::make_shared<RemoteChunks>() : std::make_shared<RemoteChunks>(*current);
            auto found =
                std::find_if(next->begin(), next->end(), [id = chunk.layout.chunk_id](const RemoteChunk &item) {
                    return item.layout.chunk_id == id;
                });
            if (found == next->end()) {
                next->push_back(chunk);
            }
            else {
                if (chunk.modify_ts < found->modify_ts) {
                    return;
                }
                if (chunk.self_fence_deadline_ms == 0) {
                    chunk.self_fence_deadline_ms = found->self_fence_deadline_ms;
                }
                *found = chunk;
            }
            if (chunks.compare_exchange_weak(current, next, std::memory_order_release, std::memory_order_acquire)) {
                if (chunk.self_fence_deadline_ms != 0 && !chunk.layout.sealed) {
                    start_liveness(chunk.layout.chunk_id);
                }
                return;
            }
        }
    }

    void forget(ChunkId id) const
    {
        auto current = chunks.load(std::memory_order_acquire);
        while (current != nullptr) {
            auto next = std::make_shared<RemoteChunks>(*current);
            std::erase_if(*next, [id](const RemoteChunk &entry) { return entry.layout.chunk_id == id; });
            if (chunks.compare_exchange_weak(current, next, std::memory_order_release, std::memory_order_acquire)) {
                return;
            }
        }
    }

    [[nodiscard]] bool cached(ChunkId chunk_id, RemoteChunk *out) const
    {
        auto current = chunks.load(std::memory_order_acquire);
        if (current == nullptr) {
            return false;
        }
        auto found = std::find_if(current->begin(), current->end(),
                                  [chunk_id](const RemoteChunk &item) { return item.layout.chunk_id == chunk_id; });
        if (found == current->end()) {
            return false;
        }
        *out = *found;
        return true;
    }

    [[nodiscard]] bool cached_valid(ChunkId chunk_id, RemoteChunk *out) const
    {
        return cached(chunk_id, out) && monotonic_millis() < out->valid_until_ms;
    }

    void fence_liveness(RemoteChunk chunk) const
    {
        chunk.self_fence_deadline_ms = monotonic_millis();
        cache(std::move(chunk));
    }

    void                start_liveness(ChunkId chunk = {}) const noexcept;
    void                stop_liveness() noexcept;
    void                renew_due_liveness() const;
    crowdb_rpc_server_t timer_server(ChunkId chunk) const noexcept;
    static void         liveness_ready(void *context, int fired) noexcept;

    void submit_chunkdb(ChunkId chunk, uint64_t request_id, uint16_t message_type, std::span<const uint8_t> control,
                        const std::shared_ptr<AllocationBudget> &metadata, RpcComplete complete, bool read_only,
                        bool (*wrong_owner)(const RpcResult &), std::function<bool()> stopped) const;

    template <typename Response>
    void submit_chunkdb(ChunkId chunk, uint64_t request_id, uint16_t message_type, std::span<const uint8_t> control,
                        const std::shared_ptr<AllocationBudget> &metadata, RpcComplete complete, bool read_only = false,
                        std::function<bool()> stopped = {}) const
    {
        submit_chunkdb(
            chunk, request_id, message_type, control, metadata, std::move(complete), read_only,
            +[](const RpcResult &result) {
                const auto *response = verified_response<Response>(result.control);
                return response != nullptr &&
                       response->ret_code() == crowdb::chunkdb::proto::FBChunkdbRetCode_NotMyRange;
            },
            std::move(stopped));
    }

    void submit_write(RemoteChunk chunk, uint32_t mirror_index, uint64_t offset, const uint8_t *data, size_t length,
                      ChunkTransportCompletion completion, ChunkIoContext context);
    void submit_query_remote(ChunkId chunk, const std::shared_ptr<AllocationBudget> &metadata, RemoteComplete complete,
                             std::function<bool()> stopped = {}) const;

    RpcChunkRouteResolver                       chunkdb_routes;
    RpcChunkRouteResolver                       disk_resolver;
    ct_chunk_rpc_transport_options              options;
    std::vector<ct_chunk_rpc_disk_route>        disk_routes;
    inline static std::atomic<uint64_t>         request_ids{1};
    mutable AtomicSharedPtr<const RemoteChunks> chunks;
    mutable std::atomic<bool>                   liveness_stopped{false};
    mutable std::atomic<uint8_t>                timer_route_state{0};
    mutable std::optional<RpcChunkRouteLease>   timer_route;
    mutable std::atomic<uint64_t>               liveness_timer{0};
};

} // namespace crowdb::tree::detail
