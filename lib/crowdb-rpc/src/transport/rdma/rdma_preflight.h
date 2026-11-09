// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-rpc/framing.h"
#include "crowdb-rpc/transport/rdma/rdma_capability.h"

#include <array>
#include <chrono>
#include <memory>

namespace crowdb::rpc
{
struct RdmaPreflight
{
    enum class Progress : uint8_t { kRead, kWrite, kComplete, kFailed };

    RdmaPreflight()                                         = default;
    RdmaPreflight(const RdmaPreflight &)                    = delete;
    RdmaPreflight         &operator=(const RdmaPreflight &) = delete;
    int                    fd                               = -1;
    RdmaEndpointCapability peer;
    ~RdmaPreflight();
    int release_fd();

    static std::unique_ptr<RdmaPreflight> start(const std::string &address, int port,
                                                const RdmaEndpointCapability &local, const std::string &source_address,
                                                uint32_t timeout_ms = 2000);
    [[nodiscard]] Progress                interest() const noexcept;

    [[nodiscard]] auto deadline() const noexcept
    {
        return deadline_;
    }

    // Call after the current fd interest becomes ready. Any further socket
    // operations are nonblocking, including transitions between write and read.
    Progress advance();

    static std::unique_ptr<RdmaPreflight> connect(const std::string &address, int port,
                                                  const RdmaEndpointCapability &local,
                                                  const std::string &source_address, uint32_t timeout_ms = 2000);

  private:
    enum class Phase : uint8_t { kConnect, kSend, kHeader, kBody, kComplete, kFailed };

    Progress transfer();
    Progress fail() noexcept;

    std::chrono::steady_clock::time_point                  deadline_;
    std::array<uint8_t, HEADER_SIZE + RDMA_HANDSHAKE_SIZE> bytes_{};
    size_t                                                 offset_ = 0;
    size_t                                                 length_ = bytes_.size();
    Phase                                                  phase_  = Phase::kConnect;
};
} // namespace crowdb::rpc
