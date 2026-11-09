// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rdma_active.h"

#include "crowdb-common/log.h"
#include "rdma_channel.h"
#include "rdma_cq.h"
#include "rdma_offer.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <arpa/inet.h>
#    include <poll.h>

#    include <algorithm>
#    include <cerrno>

namespace crowdb::rpc
{

namespace
{
using Clock = std::chrono::steady_clock;

bool submit_address(rdma_cm_id *id, const std::string &addr, int port, const std::string &source_address_text)
{
    sockaddr_in destination{};
    destination.sin_family = AF_INET;
    destination.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &destination.sin_addr) != 1) {
        CRB_LOG_WARN("rdma connect: invalid destination {}", addr);
        return false;
    }
    sockaddr_in source{};
    if (!source_address_text.empty()) {
        source.sin_family = AF_INET;
        if (::inet_pton(AF_INET, source_address_text.c_str(), &source.sin_addr) != 1) {
            CRB_LOG_WARN("rdma connect: invalid source address {}", source_address_text);
            return false;
        }
        if (rdma_bind_addr(id, reinterpret_cast<sockaddr *>(&source)) != 0) {
            CRB_LOG_WARN("rdma connect: bind source failed source={} errno={}", source_address_text, errno);
            return false;
        }
    }
    // Passing a source address again can fail after rdma_bind_addr.
    if (rdma_resolve_addr(id, nullptr, reinterpret_cast<sockaddr *>(&destination), 2000) != 0) {
        CRB_LOG_WARN("rdma connect: resolve_addr failed addr={} port={} errno={}", addr, port, errno);
        return false;
    }
    return true;
}
} // namespace

bool RdmaTransport::create_active_connection(rdma_cm_id *id, const std::string &addr,
                                             std::shared_ptr<Connection>      &connection,
                                             std::shared_ptr<ConnectionPools> &pools,
                                             const RdmaEndpointCapability     *expected_peer)
{
    ibv_device_attr device_attributes{};
    if (ibv_query_device(id->verbs, &device_attributes) != 0) {
        return false;
    }
    const uint64_t required_capacity = static_cast<uint64_t>(config_.sq_depth) + config_.posted_receive_target;
    CqReservation  reservation(required_capacity > UINT32_MAX ? nullptr
                                                              : reserve_cq(static_cast<uint32_t>(required_capacity)),
                               static_cast<uint32_t>(required_capacity));
    if (reservation.queue == nullptr) {
        return false;
    }
    ibv_qp_init_attr qp_attr{};
    qp_attr.send_cq             = reservation.queue->cq;
    qp_attr.recv_cq             = reservation.queue->cq;
    qp_attr.qp_type             = IBV_QPT_RC;
    qp_attr.cap.max_send_wr     = std::min(config_.sq_depth, static_cast<uint32_t>(device_attributes.max_qp_wr));
    qp_attr.cap.max_recv_wr     = std::min(config_.rq_depth, static_cast<uint32_t>(device_attributes.max_qp_wr));
    qp_attr.cap.max_send_sge    = std::min(config_.qp_max_send_sge, static_cast<uint32_t>(device_attributes.max_sge));
    qp_attr.cap.max_recv_sge    = std::min(config_.qp_max_recv_sge, static_cast<uint32_t>(device_attributes.max_sge));
    qp_attr.cap.max_inline_data = 0;
    qp_attr.sq_sig_all          = 0;
    qp_attr.qp_context          = id;

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {
        return false;
    }
    if (config_.wire_layout == RdmaWireLayout::ControlData &&
        (qp_attr.cap.max_send_sge < 2 || qp_attr.cap.max_recv_sge < 2)) {
        rdma_destroy_qp(id);
        return false;
    }
    pools = std::make_shared<ConnectionPools>(config_.send_queue_capacity,
                                              config_.receive_batch_size > 1 ? config_.posted_receive_target : 0);
    pools->effective_capacity = qp_attr.cap;
    pools->cq_index           = reservation.queue->index;
    pools->reserved_capacity  = reservation.capacity;
    connection                = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), addr, application_pool_);
    set_connection_pools(connection.get(), pools);
    reservation.committed = true;
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    install_connection_notifiers(connection, pools->cq_index);
    // Preflight already binds the direct receive class. Prepost its negotiated
    // slots before CM connect so an immediate peer response finds RQ credits.
    if (config_.wire_layout == RdmaWireLayout::Contiguous || expected_peer != nullptr) {
        if (config_.wire_layout == RdmaWireLayout::ControlData) {
            RdmaCmOffer peer{.receive_frame_limit       = static_cast<uint16_t>(expected_peer->receive_frame_limit),
                             .receive_slot_capacity     = expected_peer->receive_slot_capacity,
                             .wire_layout               = expected_peer->wire_layout,
                             .generation                = expected_peer->generation,
                             .max_control_message_bytes = expected_peer->max_control_message_bytes};
            configure_peer(*pools, peer);
        }
        else {
            pools->effective_data_capacity = config_.data_slot_capacity();
        }
        if (!create_connection_pools(*pools) || !prime_receive(connection)) {
            // Some receives may already be posted. Keep their QP and CQ
            // reservation in the attempt until it retires.
            return false;
        }
    }
    return true;
}

RdmaTransport::ActiveConnect::ActiveConnect(RdmaTransport &transport, std::string address,
                                            const RdmaEndpointCapability *expected_peer)
    : transport_(transport),
      address_(std::move(address))
{
    if (expected_peer != nullptr) {
        expected_peer_ = *expected_peer;
    }
}

RdmaTransport::ActiveConnect::~ActiveConnect()
{
    while (!try_retire()) {
        std::this_thread::yield();
    }
}

bool RdmaTransport::ActiveConnect::try_retire()
{
    if (id_ != nullptr) {
        if (connection_ != nullptr) {
            if (!transport_.try_destroy_connection_qp(connection_.get(), id_)) {
                return false;
            }
        }
        else if (id_->qp != nullptr) {
            rdma_destroy_qp(id_);
        }
        rdma_destroy_id(id_);
        id_ = nullptr;
    }
    if (channel_ != nullptr) {
        rdma_destroy_event_channel(channel_);
        channel_ = nullptr;
    }
    return true;
}

bool RdmaTransport::ActiveConnect::start(int port)
{
    // Existing connections use the shared CM channel. Active setup owns a
    // private channel so its setup events cannot be consumed by another owner.
    channel_ = rdma_create_event_channel();
    if (channel_ == nullptr) {
        return false;
    }
    if (!rdma_detail::make_nonblocking(channel_->fd)) {
        return false;
    }
    if (rdma_create_id(channel_, &id_, &transport_, RDMA_PS_TCP) != 0) {
        return false;
    }
    if (!submit_address(id_, address_, port, transport_.config_.source_address)) {
        return false;
    }
    deadline_ = Clock::now() + std::chrono::milliseconds(3000);
    return true;
}

RdmaTransport::ActiveConnect::Progress RdmaTransport::ActiveConnect::submit_connect(bool wait_for_context)
{
    const auto context = transport_.adopt_cm_context(id_->verbs, wait_for_context);
    if (context == CmContext::kBusy) {
        return Progress::kRetry;
    }
    const auto *expected = expected_peer_ ? &*expected_peer_ : nullptr;
    if (context != CmContext::kReady ||
        !transport_.create_active_connection(id_, address_, connection_, pools_, expected)) {
        return fail();
    }

    rdma_conn_param params{};
    local_                     = transport_.local_capability();
    const auto offer           = serialize_rdma_offer(local_, expected);
    params.private_data        = offer.data();
    params.private_data_len    = static_cast<uint8_t>(offer.size());
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = transport_.config_.retry_count;
    params.rnr_retry_count     = transport_.config_.rnr_retry_count;
    if (rdma_connect(id_, &params) != 0) {
        CRB_LOG_WARN("rdma connect: rdma_connect failed addr={} errno={}", address_, errno);
        return fail();
    }
    phase_    = Phase::kConnect;
    deadline_ = Clock::now() + std::chrono::milliseconds(5000);
    return Progress::kRead;
}

bool RdmaTransport::ActiveConnect::prepare_established(const RdmaCmOffer &peer)
{
    if (pools_->recv == nullptr) {
        transport_.configure_peer(*pools_, peer);
        if (!transport_.create_connection_pools(*pools_) || !transport_.prime_receive(connection_)) {
            return false;
        }
    }
    else if (transport_.config_.wire_layout == RdmaWireLayout::Contiguous) {
        transport_.configure_peer(*pools_, peer);
    }
    pools_->qp_number = id_->qp->qp_num;
    ibv_qp_attr receive_attributes{};
    receive_attributes.min_rnr_timer = transport_.config_.min_rnr_timer;
    return ibv_modify_qp(id_->qp, &receive_attributes, IBV_QP_MIN_RNR_TIMER) == 0;
}

RdmaTransport::ActiveConnect::Progress RdmaTransport::ActiveConnect::fail() noexcept
{
    phase_ = Phase::kFailed;
    return Progress::kFailed;
}

RdmaTransport::ActiveConnect::Progress RdmaTransport::ActiveConnect::advance(bool wait_for_context)
{
    if (phase_ == Phase::kEstablished) {
        return Progress::kEstablished;
    }
    if (phase_ == Phase::kPrepare) {
        return submit_connect(wait_for_context);
    }
    if (phase_ == Phase::kFailed || Clock::now() >= deadline_) {
        return fail();
    }

    rdma_cm_event *raw = nullptr;
    if (rdma_get_cm_event(channel_, &raw) != 0) {
        return errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR ? Progress::kRead : fail();
    }
    std::unique_ptr<rdma_cm_event, decltype(&rdma_ack_cm_event)> event(raw, &rdma_ack_cm_event);
    if (!event) {
        return fail();
    }

    auto expected_event = RDMA_CM_EVENT_ESTABLISHED;
    if (phase_ == Phase::kAddress) {
        expected_event = RDMA_CM_EVENT_ADDR_RESOLVED;
    }
    else if (phase_ == Phase::kRoute) {
        expected_event = RDMA_CM_EVENT_ROUTE_RESOLVED;
    }
    if (event->id != id_ || event->event != expected_event || event->status != 0) {
        CRB_LOG_WARN("rdma connect: CM event failed addr={} expected={} event={} status={}", address_,
                     static_cast<int>(expected_event), static_cast<int>(event->event), event->status);
        return fail();
    }
    if (phase_ == Phase::kAddress) {
        event.reset();
        if (rdma_resolve_route(id_, 2000) != 0) {
            CRB_LOG_WARN("rdma connect: resolve_route failed addr={} errno={}", address_, errno);
            return fail();
        }
        phase_    = Phase::kRoute;
        deadline_ = Clock::now() + std::chrono::milliseconds(3000);
        return Progress::kRead;
    }
    if (phase_ == Phase::kRoute) {
        event.reset();
        phase_ = Phase::kPrepare;
        return submit_connect(wait_for_context);
    }

    RdmaCmOffer peer;
    const auto *expected = expected_peer_ ? &*expected_peer_ : nullptr;
    const auto &reply    = event->param.conn;
    const bool  valid =
        reply.private_data != nullptr &&
        parse_rdma_offer(std::span(static_cast<const uint8_t *>(reply.private_data), reply.private_data_len), &peer) &&
        validate_rdma_offer(peer, local_, expected, true);
    event.reset();
    if (!valid || !prepare_established(peer)) {
        return fail();
    }
    phase_ = Phase::kEstablished;
    return Progress::kEstablished;
}

RdmaTransport::CmPublish RdmaTransport::ActiveConnect::try_publish(std::shared_ptr<Connection> &connection)
{
    if (phase_ != Phase::kEstablished || id_ == nullptr) {
        return CmPublish::kFailed;
    }
    const auto result = transport_.try_publish_cm_connection(id_, connection_, pools_);
    if (result == CmPublish::kPublished) {
        id_        = nullptr;
        connection = std::move(connection_);
    }
    return result;
}

std::unique_ptr<RdmaTransport::ActiveConnect> RdmaTransport::start_connect(const std::string &addr, int port,
                                                                           const RdmaEndpointCapability *expected_peer)
{
    if (!ready() || addr.empty() || port <= 0 || port > 65535 ||
        (config_.require_tcp_preflight && expected_peer == nullptr) ||
        (expected_peer != nullptr &&
         (expected_peer->wire_layout != config_.wire_layout ||
          !validate_rdma_capability(*expected_peer, RDMA_PROTOCOL_VERSION, expected_peer->generation, "")))) {
        CRB_LOG_WARN("rdma connect: transport unavailable addr={} port={}", addr, port);
        return nullptr;
    }
    stats_.cm_attempts.fetch_add(1, std::memory_order_relaxed);
    auto attempt = std::unique_ptr<ActiveConnect>(new ActiveConnect(*this, addr, expected_peer));
    return attempt->start(port) ? std::move(attempt) : nullptr;
}

std::shared_ptr<Connection> RdmaTransport::connect(const std::string &addr, int port,
                                                   const RdmaEndpointCapability *expected_peer)
{
    auto attempt = start_connect(addr, port, expected_peer);
    if (!attempt) {
        return nullptr;
    }
    using Progress = ActiveConnect::Progress;
    for (;;) {
        const auto remaining = std::chrono::ceil<std::chrono::milliseconds>(attempt->deadline() - Clock::now()).count();
        if (remaining <= 0) {
            return nullptr;
        }
        pollfd     descriptor{.fd = attempt->fd(), .events = POLLIN, .revents = 0};
        const auto ready = ::poll(&descriptor, 1, static_cast<int>(remaining));
        if (ready < 0 && errno == EINTR) {
            continue;
        }
        if (ready <= 0 || (descriptor.revents & POLLIN) == 0) {
            return nullptr;
        }
        switch (attempt->advance(true)) {
        case Progress::kRead:
        case Progress::kRetry:
            break;
        case Progress::kEstablished: {
            std::shared_ptr<Connection> connection;
            while (attempt->try_publish(connection) == CmPublish::kBusy) {
                std::this_thread::yield();
            }
            if (connection != nullptr) {
                register_events(event_engine_);
            }
            return connection;
        }
        case Progress::kFailed:
            return nullptr;
        }
    }
}

} // namespace crowdb::rpc

#endif // CROWDB_RPC_HAVE_RDMA
