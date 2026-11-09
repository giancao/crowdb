// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/transport/rdma/rdma_transport.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <chrono>
#    include <optional>

namespace crowdb::rpc
{
// Owns one private CM channel until the last setup event has been acknowledged.
// The caller keeps the transport alive and unregisters the fd before dropping it.
class RdmaTransport::ActiveConnect
{
  public:
    enum class Progress : uint8_t { kRead, kRetry, kEstablished, kFailed };

    ~ActiveConnect();
    ActiveConnect(const ActiveConnect &)            = delete;
    ActiveConnect &operator=(const ActiveConnect &) = delete;

    [[nodiscard]] int fd() const noexcept
    {
        return channel_->fd;
    }

    [[nodiscard]] auto deadline() const noexcept
    {
        return deadline_;
    }

    [[nodiscard]] bool waiting_for_context() const noexcept
    {
        return phase_ == Phase::kPrepare;
    }

    Progress  advance(bool wait_for_context = false);
    CmPublish try_publish(std::shared_ptr<Connection> &connection);

    [[nodiscard]] bool requires_qp_retirement() const noexcept
    {
        return id_ != nullptr && connection_ != nullptr;
    }

    bool try_retire();

  private:
    friend class RdmaTransport;
    enum class Phase : uint8_t { kAddress, kRoute, kPrepare, kConnect, kEstablished, kFailed };

    ActiveConnect(RdmaTransport &transport, std::string address, const RdmaEndpointCapability *expected_peer);
    bool     start(int port);
    Progress submit_connect(bool wait_for_context);
    bool     prepare_established(const RdmaCmOffer &peer);
    Progress fail() noexcept;

    RdmaTransport                        &transport_;
    std::string                           address_;
    std::optional<RdmaEndpointCapability> expected_peer_;
    RdmaEndpointCapability                local_;
    rdma_event_channel                   *channel_ = nullptr;
    rdma_cm_id                           *id_      = nullptr;
    std::shared_ptr<Connection>           connection_;
    std::shared_ptr<ConnectionPools>      pools_;
    std::chrono::steady_clock::time_point deadline_;
    Phase                                 phase_ = Phase::kAddress;
};
} // namespace crowdb::rpc

#endif
