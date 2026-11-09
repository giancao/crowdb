// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_preflight.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <utility>

namespace crowdb::rpc
{
namespace
{
using Clock = std::chrono::steady_clock;

bool wait_for(int fd, short events, Clock::time_point deadline)
{
    for (;;) {
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - Clock::now()).count();
        if (remaining <= 0) {
            return false;
        }
        pollfd    descriptor{.fd = fd, .events = events, .revents = 0};
        const int result = ::poll(&descriptor, 1, static_cast<int>(std::min<int64_t>(remaining, INT32_MAX)));
        if (result > 0) {
            return (descriptor.revents & events) != 0;
        }
        if (result == 0 || errno != EINTR) {
            return false;
        }
    }
}

} // namespace

RdmaPreflight::~RdmaPreflight()
{
    if (fd >= 0) {
        ::close(fd);
    }
}

int RdmaPreflight::release_fd()
{
    return phase_ == Phase::kComplete ? std::exchange(fd, -1) : -1;
}

std::unique_ptr<RdmaPreflight> RdmaPreflight::start(const std::string &address, int port,
                                                    const RdmaEndpointCapability &local,
                                                    const std::string &source_address, uint32_t timeout_ms)
{
    if (port <= 0 || port > 65535 || timeout_ms == 0) {
        return nullptr;
    }
    auto connection = std::make_unique<RdmaPreflight>();
    connection->fd  = ::socket(AF_INET, SOCK_STREAM, 0);
    if (connection->fd < 0 || ::fcntl(connection->fd, F_SETFL, O_NONBLOCK) != 0) {
        return nullptr;
    }
#ifdef SO_NOSIGPIPE
    int nosigpipe = 1;
    ::setsockopt(connection->fd, SOL_SOCKET, SO_NOSIGPIPE, &nosigpipe, sizeof(nosigpipe));
#endif
    sockaddr_in peer{};
    peer.sin_family = AF_INET;
    peer.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, address.c_str(), &peer.sin_addr) != 1) {
        return nullptr;
    }
    if (!source_address.empty()) {
        sockaddr_in source{};
        source.sin_family = AF_INET;
        if (::inet_pton(AF_INET, source_address.c_str(), &source.sin_addr) != 1 ||
            ::bind(connection->fd, reinterpret_cast<sockaddr *>(&source), sizeof(source)) != 0) {
            return nullptr;
        }
    }
    connection->deadline_ = Clock::now() + std::chrono::milliseconds(timeout_ms);
    if (::connect(connection->fd, reinterpret_cast<sockaddr *>(&peer), sizeof(peer)) != 0 && errno != EINPROGRESS) {
        return nullptr;
    }

    Header header;
    header.msg_type = RDMA_HANDSHAKE_MSG_TYPE;
    header.msg_size = RDMA_HANDSHAKE_SIZE;
    header.flags    = FLAG_ONE_WAY;
    serialize_header(connection->bytes_.data(), header);
    const auto capability = serialize_rdma_handshake(local);
    std::copy(capability.begin(), capability.end(), connection->bytes_.begin() + HEADER_SIZE);
    return connection;
}

RdmaPreflight::Progress RdmaPreflight::interest() const noexcept
{
    switch (phase_) {
    case Phase::kConnect:
    case Phase::kSend:
        return Progress::kWrite;
    case Phase::kHeader:
    case Phase::kBody:
        return Progress::kRead;
    case Phase::kComplete:
        return Progress::kComplete;
    case Phase::kFailed:
        return Progress::kFailed;
    }
    return Progress::kFailed;
}

RdmaPreflight::Progress RdmaPreflight::fail() noexcept
{
    phase_ = Phase::kFailed;
    return Progress::kFailed;
}

RdmaPreflight::Progress RdmaPreflight::transfer()
{
    const bool writing = phase_ == Phase::kSend;
    int        flags   = 0;
#ifdef MSG_NOSIGNAL
    flags = MSG_NOSIGNAL;
#endif
    while (offset_ < length_) {
        auto       bytes = std::span(bytes_).subspan(offset_, length_ - offset_);
        const auto count =
            writing ? ::send(fd, bytes.data(), bytes.size(), flags) : ::recv(fd, bytes.data(), bytes.size(), 0);
        if (count > 0) {
            offset_ += static_cast<size_t>(count);
            continue;
        }
        if (count == 0) {
            return fail();
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return interest();
        }
        if (errno != EINTR || Clock::now() >= deadline_) {
            return fail();
        }
    }
    offset_ = 0;
    return Progress::kComplete;
}

RdmaPreflight::Progress RdmaPreflight::advance()
{
    auto next = interest();
    if (next == Progress::kComplete || next == Progress::kFailed) {
        return next;
    }
    if (Clock::now() >= deadline_) {
        return fail();
    }
    if (phase_ == Phase::kConnect) {
        int       error = 0;
        socklen_t size  = sizeof(error);
        if (::getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &size) != 0 || error != 0) {
            return fail();
        }
        phase_ = Phase::kSend;
    }
    for (;;) {
        next = transfer();
        if (next != Progress::kComplete) {
            return next;
        }
        if (phase_ == Phase::kSend) {
            phase_  = Phase::kHeader;
            length_ = HEADER_SIZE;
        }
        else if (phase_ == Phase::kHeader) {
            const auto header = parse_header(bytes_.data());
            if (header.magic != MAGIC || header.msg_type != RDMA_HANDSHAKE_MSG_TYPE ||
                header.msg_offset != HEADER_SIZE || header.data_size != 0 ||
                header.msg_size < RDMA_LEGACY_HANDSHAKE_SIZE || header.msg_size > RDMA_HANDSHAKE_SIZE) {
                return fail();
            }
            phase_  = Phase::kBody;
            length_ = header.msg_size;
        }
        else {
            if (!parse_rdma_handshake(std::span(bytes_).first(length_), &peer)) {
                return fail();
            }
            phase_ = Phase::kComplete;
            return Progress::kComplete;
        }
    }
}

std::unique_ptr<RdmaPreflight> RdmaPreflight::connect(const std::string &address, int port,
                                                      const RdmaEndpointCapability &local,
                                                      const std::string &source_address, uint32_t timeout_ms)
{
    auto connection = start(address, port, local, source_address, timeout_ms);
    if (connection == nullptr) {
        return nullptr;
    }
    auto next = connection->interest();
    while (next == Progress::kRead || next == Progress::kWrite) {
        if (!wait_for(connection->fd, next == Progress::kRead ? POLLIN : POLLOUT, connection->deadline_)) {
            return nullptr;
        }
        next = connection->advance();
    }
    return next == Progress::kComplete ? std::move(connection) : nullptr;
}
} // namespace crowdb::rpc
