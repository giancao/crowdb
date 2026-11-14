// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "rdma/rdma_active.h"
#include "rdma/rdma_preflight.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace crowdb::rpc
{
#ifdef CROWDB_RPC_HAVE_RDMA
class SocketTransport::ConnectAttempt
{
  public:
    ConnectAttempt(SocketTransport &transport, std::string address, int port)
        : transport_(transport),
          address_(std::move(address)),
          port_(port)
    {
        auto &rdma = *transport_.rdma_transport_;
        rdma.stats().preflight_attempts.fetch_add(1, std::memory_order_relaxed);
        preflight_ = RdmaPreflight::start(address_, port_, transport_.local_rdma_capability(), rdma.source_address());
        if (preflight_ == nullptr) {
            fail_io();
        }
    }

    [[nodiscard]] bool waiting() const noexcept
    {
        return phase_ == Phase::kPreflight || phase_ == Phase::kCm;
    }

    [[nodiscard]] bool waiting_for_context() const noexcept
    {
        return phase_ == Phase::kCm && active_->waiting_for_context();
    }

    [[nodiscard]] pollfd descriptor() const noexcept
    {
        if (phase_ == Phase::kCm) {
            return {.fd = active_->fd(), .events = POLLIN, .revents = 0};
        }
        const short events = preflight_->interest() == RdmaPreflight::Progress::kWrite ? POLLOUT : POLLIN;
        return {.fd = preflight_->fd, .events = events, .revents = 0};
    }

    [[nodiscard]] auto deadline() const noexcept
    {
        return phase_ == Phase::kCm ? active_->deadline() : preflight_->deadline();
    }

    void advance(bool wait_for_context = false)
    {
        if (phase_ == Phase::kCm) {
            using Progress  = RdmaTransport::ActiveConnect::Progress;
            const auto next = active_->advance(wait_for_context);
            if (next != Progress::kRead && next != Progress::kRetry) {
                phase_ = next == Progress::kEstablished ? Phase::kRdmaReady : Phase::kFinish;
            }
            return;
        }

        using Progress  = RdmaPreflight::Progress;
        const auto next = preflight_->advance();
        if (next == Progress::kRead || next == Progress::kWrite) {
            return;
        }
        auto &rdma = *transport_.rdma_transport_;
        if (next == Progress::kFailed ||
            !validate_rdma_capability(preflight_->peer, RDMA_PROTOCOL_VERSION, preflight_->peer.generation, {}) ||
            preflight_->peer.wire_layout != rdma.config().wire_layout) {
            fail_io();
            return;
        }

        const auto &peer = preflight_->peer;
        active_ = rdma.start_connect(peer.cm_address == "0.0.0.0" ? address_ : peer.cm_address, peer.port, &peer);
        phase_  = active_ == nullptr ? Phase::kFinish : Phase::kCm;
    }

    void fail_io() noexcept
    {
        if (phase_ == Phase::kPreflight) {
            transport_.rdma_transport_->stats().preflight_failures.fetch_add(1, std::memory_order_relaxed);
        }
        phase_ = Phase::kFinish;
    }

    bool try_finish(std::shared_ptr<Connection> &connection)
    {
        if (phase_ == Phase::kRdmaReady) {
            const auto published = active_->try_publish(connection);
            if (published == RdmaTransport::CmPublish::kBusy) {
                return false;
            }
            if (published == RdmaTransport::CmPublish::kPublished) {
                for (const auto &engine : transport_.engines_) {
                    transport_.rdma_transport_->register_events(engine.get());
                }
                transport_.attach_rdma_connection(connection);
                return true;
            }
        }

        // Leave failed CM setup owned by this attempt until retirement.
        if (!transport_.allow_tcp_fallback_) {
            CRB_LOG_WARN("RDMA preflight/setup failed; TCP fallback disabled addr={} port={}", address_, port_);
            return true;
        }
        transport_.rdma_transport_->stats().fallback_count.fetch_add(1, std::memory_order_relaxed);
        const int fd = preflight_ == nullptr ? -1 : preflight_->release_fd();
        if (fd < 0) {
            preflight_.reset();
            connection = transport_.connect_tcp(address_, port_);
            return true;
        }
        transport_.prepare_tcp_socket(fd);
        connection = transport_.create_connection(fd, address_ + ":" + std::to_string(port_));
        connection->set_peer_capability(preflight_->peer);
        return true;
    }

    [[nodiscard]] bool requires_retirement() const noexcept
    {
        return active_ != nullptr && active_->requires_qp_retirement();
    }

    bool try_retire()
    {
        return active_ == nullptr || active_->try_retire();
    }

  private:
    enum class Phase : uint8_t { kPreflight, kCm, kRdmaReady, kFinish };

    SocketTransport                              &transport_;
    std::string                                   address_;
    int                                           port_;
    std::unique_ptr<RdmaPreflight>                preflight_;
    std::unique_ptr<RdmaTransport::ActiveConnect> active_;
    Phase                                         phase_ = Phase::kPreflight;
};

std::unique_ptr<SocketTransport::ConnectAttempt> SocketTransport::start_rdma_connect(const std::string &addr, int port)
{
    if ((rdma_explicit_configuration_ && !rdma_enabled_) || rdma_transport_ == nullptr || !rdma_transport_->ready()) {
        return nullptr;
    }
    return std::make_unique<ConnectAttempt>(*this, addr, port);
}
#endif

std::shared_ptr<Connection> SocketTransport::connect(const std::string &addr, int port)
{
    if (rdma_explicit_configuration_ && !rdma_enabled_) {
        return connect_tcp(addr, port);
    }
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr && rdma_transport_->ready()) {
        ConnectAttempt attempt(*this, addr, port);
        while (attempt.waiting()) {
            const auto remaining =
                std::chrono::ceil<std::chrono::milliseconds>(attempt.deadline() - std::chrono::steady_clock::now());
            if (remaining.count() <= 0) {
                attempt.fail_io();
                break;
            }
            auto       descriptor = attempt.descriptor();
            const auto ready      = ::poll(&descriptor, 1, static_cast<int>(remaining.count()));
            if (ready < 0 && errno == EINTR) {
                continue;
            }
            if (ready <= 0 || (descriptor.revents & descriptor.events) == 0) {
                attempt.fail_io();
                break;
            }
            attempt.advance(true);
        }
        std::shared_ptr<Connection> connection;
        while (!attempt.try_finish(connection)) {
            std::this_thread::yield();
        }
        return connection;
    }
#endif
    if (!allow_tcp_fallback_) {
        CRB_LOG_WARN("RDMA connection unavailable; TCP fallback disabled addr={} port={}", addr, port);
        return nullptr;
    }
#ifdef CROWDB_RPC_HAVE_RDMA
    if (rdma_transport_ != nullptr) {
        rdma_transport_->stats().fallback_count.fetch_add(1, std::memory_order_relaxed);
    }
#endif
    return connect_tcp(addr, port);
}

void SocketTransport::prepare_tcp_socket(int fd) const
{
    int nodelay = tcp_nodelay_ ? 1 : 0;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &nodelay, sizeof(nodelay));
#ifdef __linux__
    if (quickack_) {
        int quickack = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_QUICKACK, &quickack, sizeof(quickack));
    }
#endif
}

std::shared_ptr<Connection> SocketTransport::connect_tcp(const std::string &addr, int port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        CRB_LOG_WARN("connect: socket() failed addr={} port={} errno={} ({})", addr, port, errno, std::strerror(errno));
        return nullptr;
    }

    struct sockaddr_in sa{};
    sa.sin_family = AF_INET;
    sa.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) <= 0) {
        CRB_LOG_WARN("connect: inet_pton failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    const int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
        CRB_LOG_WARN("connect: nonblocking setup failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    // A missing peer must not block the caller's executor while TCP retries
    // its handshake. Queued writes wait for writability in the worker;
    // handshake failures use the same error/close path as established peers.
    if (::connect(fd, reinterpret_cast<struct sockaddr *>(&sa), sizeof(sa)) < 0 && errno != EINPROGRESS) {
        CRB_LOG_WARN("connect: connect() failed addr={} port={} errno={} ({})", addr, port, errno,
                     std::strerror(errno));
        ::close(fd);
        return nullptr;
    }

    prepare_tcp_socket(fd);

    auto conn = create_connection(fd, addr + ":" + std::to_string(port));
    send_rdma_handshake(conn);
    CRB_LOG_INFO("rpc transport: connection initiated -> {}:{} conn_id={}", addr, port,
                 static_cast<long long>(conn->id()));
    return conn;
}

} // namespace crowdb::rpc

struct crowdb_rpc_connect_s
{
#ifdef CROWDB_RPC_HAVE_RDMA
    std::unique_ptr<crowdb::rpc::SocketTransport::ConnectAttempt> attempt;
    std::unique_ptr<crowdb_rpc_conn_s>                            completion;
#endif
};

crowdb_rpc_connect_t crowdb_rpc_connect_begin(crowdb_rpc_server_t server, const char *addr, int port,
                                              crowdb_rpc_conn_t *out_conn)
{
    if (out_conn == nullptr) {
        return nullptr;
    }
    *out_conn = nullptr;
    if (server == nullptr || addr == nullptr) {
        return nullptr;
    }
    try {
#ifdef CROWDB_RPC_HAVE_RDMA
        auto attempt = server->server->transport()->start_rdma_connect(addr, port);
        if (attempt != nullptr) {
            auto completion = std::make_unique<crowdb_rpc_conn_s>();
            return new crowdb_rpc_connect_s{.attempt = std::move(attempt), .completion = std::move(completion)};
        }
#endif
        *out_conn = crowdb_rpc_connect(server, addr, port);
    }
    catch (...) {
        return nullptr;
    }
    return nullptr;
}

int crowdb_rpc_connect_step(crowdb_rpc_connect_t attempt, int event, int *fd, uint32_t *timeout_ms,
                            crowdb_rpc_conn_t *out_conn)
{
    if (out_conn == nullptr || fd == nullptr || timeout_ms == nullptr) {
        return CROWDB_RPC_CONNECT_FAILED;
    }
    *out_conn   = nullptr;
    *fd         = -1;
    *timeout_ms = 0;
#ifdef CROWDB_RPC_HAVE_RDMA
    if (attempt == nullptr || attempt->completion == nullptr || event < -1 || event > 1) {
        return CROWDB_RPC_CONNECT_FAILED;
    }
    try {
        auto &setup = *attempt->attempt;
        if (setup.waiting_for_context()) {
            setup.advance();
        }
        else if (setup.waiting()) {
            if (event < 0) {
                setup.fail_io();
            }
            else if (event > 0) {
                setup.advance();
            }
        }
        if (setup.waiting_for_context()) {
            return CROWDB_RPC_CONNECT_RETRY;
        }
        if (setup.waiting()) {
            const auto remaining =
                std::chrono::ceil<std::chrono::milliseconds>(setup.deadline() - std::chrono::steady_clock::now());
            const auto descriptor = setup.descriptor();
            *fd                   = descriptor.fd;
            *timeout_ms           = static_cast<uint32_t>(std::max<int64_t>(0, remaining.count()));
            return descriptor.events == POLLIN ? CROWDB_RPC_CONNECT_READ : CROWDB_RPC_CONNECT_WRITE;
        }
        if (!setup.try_finish(attempt->completion->conn)) {
            return CROWDB_RPC_CONNECT_RETRY;
        }
        if (attempt->completion->conn != nullptr) {
            *out_conn = attempt->completion.release();
            return CROWDB_RPC_CONNECT_DONE;
        }
    }
    catch (...) {
        crowdb_rpc_conn_close(attempt->completion.get());
    }
    attempt->completion.reset();
#else
    (void)attempt;
    (void)event;
#endif
    return CROWDB_RPC_CONNECT_FAILED;
}

bool crowdb_rpc_connect_requires_retirement(crowdb_rpc_connect_t attempt)
{
#ifdef CROWDB_RPC_HAVE_RDMA
    return attempt != nullptr && attempt->attempt->requires_retirement();
#else
    (void)attempt;
    return false;
#endif
}

int crowdb_rpc_connect_retire_step(crowdb_rpc_connect_t attempt)
{
#ifdef CROWDB_RPC_HAVE_RDMA
    try {
        return attempt == nullptr || attempt->attempt->try_retire() ? 1 : 0;
    }
    catch (...) {
        return -1;
    }
#else
    (void)attempt;
    return 1;
#endif
}

void crowdb_rpc_connect_destroy(crowdb_rpc_connect_t attempt)
{
    delete attempt;
}
