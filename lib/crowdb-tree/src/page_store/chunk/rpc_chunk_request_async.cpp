// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-common/log.h"
#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{
struct RpcChunkTransport::Impl::ChunkdbCall : std::enable_shared_from_this<ChunkdbCall>
{
    struct Retry
    {
        std::shared_ptr<Retry>       callback_owner;
        std::shared_ptr<ChunkdbCall> call;

        static void ready(void *context, int fired) noexcept
        {
            auto &retry    = *static_cast<Retry *>(context);
            auto  retained = std::move(retry.callback_owner);
            if (fired != 0) {
                retry.call->send();
            }
            else {
                retry.call->finish(Status::unavailable(), {});
            }
        }
    };

    ChunkdbCall(std::shared_ptr<const Impl> transport, std::shared_ptr<AllocationBudget> budget)
        : owner(std::move(transport)),
          metadata(std::move(budget)),
          control(ReservationAllocator<uint8_t>(metadata))
    {
    }

    std::shared_ptr<const Impl>       owner;
    std::shared_ptr<AllocationBudget> metadata;
    ReservedVector<uint8_t>           control;
    ChunkId                           chunk;
    uint64_t                          request_id   = 0;
    uint16_t                          message_type = 0;
    uint32_t                          attempt      = 0;
    bool                              read_only    = false;
    bool (*wrong_owner)(const RpcResult &)         = nullptr;
    std::function<bool()> stopped;
    RpcComplete           complete;
    bool                  terminal = false;

    void finish(Status status, RpcResult result) noexcept
    {
        if (terminal) {
            return;
        }
        terminal  = true;
        auto done = std::move(complete);
        try {
            done(std::move(status), std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("ChunkDB receiver threw after terminal");
        }
    }

    void retry(Status status) noexcept
    try {
        RpcChunkRouteLease refresh;
        static_cast<void>(owner->chunkdb_routes.resolve(chunk, true, &refresh));
        if (++attempt == 3) {
            finish(std::move(status), {});
            return;
        }
        if (stopped && stopped()) {
            finish(Status::unavailable(), {});
            return;
        }
        auto *const server = owner->timer_server(chunk);
        if (server == nullptr) {
            finish(Status::unavailable("Chunk RPC timer owner is unavailable"), {});
            return;
        }
        auto timer            = reserved_shared<Retry>(metadata);
        timer->call           = shared_from_this();
        timer->callback_owner = timer;
        const auto id         = crowdb_rpc_server_schedule(server, 50 * attempt, &Retry::ready, timer.get());
        if (id == 0) {
            timer->callback_owner.reset();
            finish(Status::unavailable("Chunk RPC retry timer was rejected"), {});
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted(), {});
    }
    catch (...) {
        finish(Status::internal_error(), {});
    }

    void received(Status status, RpcResult result) noexcept
    {
        if (!status.ok()) {
            // A submitted mutation may already have committed. Refresh the
            // route hint but never blindly submit that mutation again.
            if (!read_only) {
                RpcChunkRouteLease refresh;
                try {
                    static_cast<void>(owner->chunkdb_routes.resolve(chunk, true, &refresh));
                }
                catch (...) {
                    CRB_LOG_DEBUG("Chunk RPC could not refresh the route after a failed mutation");
                }
                finish(std::move(status), std::move(result));
            }
            else {
                retry(std::move(status));
            }
        }
        else if (wrong_owner(result)) {
            retry(Status::unavailable());
        }
        else {
            finish(Status::Ok(), std::move(result));
        }
    }

    void send() noexcept
    try {
        if (stopped && stopped()) {
            finish(Status::unavailable(), {});
            return;
        }
        RpcChunkRouteLease route;
        auto               status = owner->chunkdb_routes.resolve(chunk, false, &route);
        if (!status.ok()) {
            retry(std::move(status));
            return;
        }
        submit_rpc(std::move(route), request_id, message_type, control, {}, metadata,
                   [self = shared_from_this()](Status status, RpcResult result) {
                       self->received(std::move(status), std::move(result));
                   });
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted(), {});
    }
    catch (...) {
        finish(Status::internal_error(), {});
    }
};

void RpcChunkTransport::Impl::submit_chunkdb(ChunkId chunk, uint64_t request_id, uint16_t message_type,
                                             std::span<const uint8_t>                 control,
                                             const std::shared_ptr<AllocationBudget> &metadata, RpcComplete complete,
                                             bool                  read_only, bool (*wrong_owner)(const RpcResult &),
                                             std::function<bool()> stopped) const
{
    std::shared_ptr<ChunkdbCall> call;
    try {
        call = reserved_shared<ChunkdbCall>(metadata, shared_from_this(), metadata);
        call->control.assign(control.begin(), control.end());
    }
    catch (const std::bad_alloc &) {
        complete(Status::resource_exhausted(), {});
        return;
    }
    call->chunk        = chunk;
    call->request_id   = request_id;
    call->message_type = message_type;
    call->read_only    = read_only;
    call->wrong_owner  = wrong_owner;
    call->stopped      = std::move(stopped);
    call->complete     = std::move(complete);
    call->send();
}
} // namespace crowdb::tree::detail
