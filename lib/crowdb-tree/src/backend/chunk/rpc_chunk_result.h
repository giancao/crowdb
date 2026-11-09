// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/c_api.h"
#include "crowdb-tree/status.h"

#include <functional>
#include <memory>
#include <span>
#include <utility>

namespace crowdb::tree
{
class AllocationBudget;
}

namespace crowdb::tree::detail
{

struct RpcResult
{
    crowdb_rpc_status   status  = CROWDB_RPC_ERR_CONN_ERROR;
    crowdb_rpc_buffer_t control = nullptr;
    crowdb_rpc_buffer_t data    = nullptr;

    RpcResult()                             = default;
    RpcResult(const RpcResult &)            = delete;
    RpcResult &operator=(const RpcResult &) = delete;

    RpcResult(RpcResult &&other) noexcept
        : status(other.status),
          control(std::exchange(other.control, nullptr)),
          data(std::exchange(other.data, nullptr))
    {
    }

    RpcResult &operator=(RpcResult &&other) noexcept
    {
        if (this != &other) {
            release();
            status  = other.status;
            control = std::exchange(other.control, nullptr);
            data    = std::exchange(other.data, nullptr);
        }
        return *this;
    }

    ~RpcResult()
    {
        release();
    }

    void release()
    {
        if (control != nullptr) {
            crowdb_rpc_buffer_release(control);
            control = nullptr;
        }
        if (data != nullptr) {
            crowdb_rpc_buffer_release(data);
            data = nullptr;
        }
    }
};

struct RpcChunkRouteLease;
using RpcComplete = std::function<void(Status, RpcResult)>;
void submit_rpc(RpcChunkRouteLease route, uint64_t request_id, uint16_t message_type, std::span<const uint8_t> control,
                std::span<const uint8_t> data, const std::shared_ptr<AllocationBudget> &metadata, RpcComplete complete);

} // namespace crowdb::tree::detail
