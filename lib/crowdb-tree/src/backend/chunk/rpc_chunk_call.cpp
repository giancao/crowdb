// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "crowdb-common/log.h"
#include "rpc_chunk_result.h"
#include "rpc_chunk_route.h"

namespace crowdb::tree::detail
{
namespace
{
struct AsyncRpcCall
{
    std::shared_ptr<AsyncRpcCall> callback_owner;
    RpcComplete                   complete;
    RpcChunkRouteLease            route;
    RpcResult                     result;
    Status                        submission_status;
    std::atomic<unsigned>         events{0};
    std::atomic<bool>             reported{false};

    explicit AsyncRpcCall(RpcChunkRouteLease lease) : route(std::move(lease))
    {
    }

    static void completed(uint64_t /*request_id*/, crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                          crowdb_rpc_status status, void *context) noexcept
    {
        auto &call          = *static_cast<AsyncRpcCall *>(context);
        auto  retained      = std::move(call.callback_owner);
        call.result.status  = status;
        call.result.control = control;
        call.result.data    = data;
        call.events.fetch_or(1, std::memory_order_acq_rel);
        call.publish();
    }

    void submitted(crowdb_rpc_status status) noexcept
    {
        if (status != CROWDB_RPC_OK) {
            // RPC rejection leaves context ownership with the caller.
            callback_owner.reset();
            submission_status =
                status == CROWDB_RPC_ERR_SEND_QUEUE ? Status::resource_exhausted() : Status::unavailable();
        }
        events.fetch_or(2, std::memory_order_acq_rel);
        publish();
    }

    void publish() noexcept
    {
        const auto observed = events.load(std::memory_order_acquire);
        if ((observed & 2U) == 0 || (submission_status.ok() && (observed & 1U) == 0) ||
            reported.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        auto done   = std::move(complete);
        auto status = submission_status.without_message();
        if (status.ok() && result.status != CROWDB_RPC_OK) {
            status = Status::unavailable();
        }
        try {
            done(std::move(status), std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("Chunk RPC receiver threw after terminal");
        }
    }
};
} // namespace

void submit_rpc(RpcChunkRouteLease route, uint64_t request_id, uint16_t message_type, std::span<const uint8_t> control,
                std::span<const uint8_t> data, const std::shared_ptr<AllocationBudget> &metadata, RpcComplete complete)
{
    if (!complete) {
        return;
    }
    const auto &address = route.route;
    if (address.client == nullptr || address.server == nullptr || address.connection == nullptr ||
        control.size() > UINT32_MAX || data.size() > UINT32_MAX) {
        complete(Status::invalid_argument("invalid Chunk RPC route or payload"), {});
        return;
    }
    std::shared_ptr<AsyncRpcCall> call;
    try {
        call = reserved_shared<AsyncRpcCall>(metadata, std::move(route));
    }
    catch (const std::bad_alloc &) {
        complete(Status::resource_exhausted(), {});
        return;
    }
    auto *control_buffer = crowdb_rpc_buffer_create(control.data(), static_cast<uint32_t>(control.size()));
    auto *data_buffer =
        data.empty() ? nullptr : crowdb_rpc_buffer_create(data.data(), static_cast<uint32_t>(data.size()));
    if (control_buffer == nullptr || (!data.empty() && data_buffer == nullptr)) {
        if (control_buffer != nullptr) {
            crowdb_rpc_buffer_release(control_buffer);
        }
        if (data_buffer != nullptr) {
            crowdb_rpc_buffer_release(data_buffer);
        }
        complete(Status::resource_exhausted(), {});
        return;
    }
    call->complete              = std::move(complete);
    call->callback_owner        = call;
    const auto &submitted_route = call->route.route;
    const auto  status =
        crowdb_rpc_client_send(reinterpret_cast<crowdb_rpc_client_t>(submitted_route.client),
                               reinterpret_cast<crowdb_rpc_server_t>(submitted_route.server),
                               reinterpret_cast<crowdb_rpc_conn_t>(submitted_route.connection), request_id,
                               control_buffer, data_buffer, message_type, &AsyncRpcCall::completed, call.get());
    call->submitted(status);
}
} // namespace crowdb::tree::detail
