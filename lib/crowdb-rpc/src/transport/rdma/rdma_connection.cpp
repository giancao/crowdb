// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-common/log.h"
#include "crowdb-rpc/transport/rdma/rdma_transport.h"
#include "crowdb-rpc/transport/socket_transport.h"
#include "rdma_channel.h"
#include "rdma_cq.h"
#include "rdma_offer.h"

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <arpa/inet.h>

#    include <algorithm>
#    include <chrono>
#    include <new>
#    include <utility>

namespace crowdb::rpc
{

bool RdmaTransport::listen(const std::string &addr, int port)
{
    if (addr.empty() || port < 0 || port > 65535 || listen_id_ != nullptr) {

        CRB_LOG_WARN("rdma listen: transport unavailable pd={} addr={} port={} existing_listener={}",
                     static_cast<void *>(pd_), addr, port, listen_id_ != nullptr);
        return false;
    }
    if (cm_channel_ == nullptr) {
        auto *channel = rdma_create_event_channel();
        if (channel != nullptr && !rdma_detail::make_nonblocking(channel->fd)) {
            rdma_destroy_event_channel(channel);
            return false;
        }
        cm_channel_ = channel;
    }
    if (cm_channel_ == nullptr || rdma_create_id(cm_channel_, &listen_id_, this, RDMA_PS_TCP) != 0) {
        CRB_LOG_WARN("rdma listen: create CM id failed addr={} port={} errno={}", addr, port, errno);
        return false;
    }
    sockaddr_in local{};
    local.sin_family = AF_INET;
    local.sin_port   = htons(static_cast<uint16_t>(port));
    if (::inet_pton(AF_INET, addr.c_str(), &local.sin_addr) != 1) {
        CRB_LOG_WARN("rdma listen: invalid address {}", addr);
        rdma_destroy_id(listen_id_);
        listen_id_ = nullptr;
        return false;
    }
    if (rdma_bind_addr(listen_id_, reinterpret_cast<sockaddr *>(&local)) != 0 || rdma_listen(listen_id_, 16) != 0) {
        CRB_LOG_WARN("rdma listen: bind/listen failed addr={} port={} errno={}", addr, port, errno);
        rdma_destroy_id(listen_id_);
        listen_id_ = nullptr;
        return false;
    }
    local_capability_.cm_address = addr;
    local_capability_.port       = ntohs(rdma_get_src_port(listen_id_));
    listen_active_.store(true, std::memory_order_release);
    return true;
}

bool RdmaTransport::advance_cm_retirement()
{
    auto &pending = cm_retirement_;
    if (pending.event != nullptr) {
        std::unique_lock lock(cm_mu_, std::try_to_lock);
        if (!lock.owns_lock()) {
            return false;
        }
        const auto it = cm_connections_.find(pending.event->id);
        if (it != cm_connections_.end()) {
            pending.id         = it->first;
            pending.connection = it->second;
            if (pending.event->event == RDMA_CM_EVENT_DISCONNECTED ||
                pending.event->event == RDMA_CM_EVENT_CONNECT_ERROR) {
                stats_.cm_disconnects.fetch_add(1, std::memory_order_relaxed);
            }
        }
        lock.unlock();
        rdma_ack_cm_event(std::exchange(pending.event, nullptr));
        if (pending.connection == nullptr) {
            return true;
        }
    }
    auto *connection = pending.connection.get();
    if (!pending.cq_drained) {
        const auto *pools = pools_for(connection);
        if (drain_queue(pools == nullptr ? 0 : pools->cq_index, config_.cq_depth) < 0) {
            return false;
        }
        pending.cq_drained = true;
    }
    if (!pending.qp_retired) {
        pending.qp_retired = try_destroy_connection_qp(connection, pending.id);
        if (!pending.qp_retired) {
            return false;
        }
    }
    std::unique_lock lock(cm_mu_, std::try_to_lock);
    if (!lock.owns_lock()) {
        return false;
    }
    cm_connections_.erase(pending.id);
    cm_pools_.erase(pending.id);
    std::erase(cm_ids_, pending.id);
    lock.unlock();
    rdma_destroy_id(pending.id);
    pending = {};
    return true;
}

struct RdmaTransport::PassiveAccept
{
    enum class Phase { Preparing, Awaiting, Publishing, Retiring };

    rdma_cm_id                           *id = nullptr;
    RdmaCmOffer                           peer;
    std::shared_ptr<Connection>           connection;
    std::shared_ptr<ConnectionPools>      pools;
    Phase                                 phase = Phase::Preparing;
    std::chrono::steady_clock::time_point deadline;
    uint64_t                              deadline_timer  = 0;
    bool                                  retry_scheduled = false;
};

std::shared_ptr<Connection> RdmaTransport::accept()
{
    if (cm_polling_.test_and_set(std::memory_order_acquire)) {
        return nullptr;
    }
    auto release = [](std::atomic_flag *flag) { flag->clear(std::memory_order_release); };
    const std::unique_ptr<std::atomic_flag, decltype(release)> owner(&cm_polling_, release);
    return accept_cm_events();
}

std::shared_ptr<Connection> RdmaTransport::accept_cm_events()
{
    using Phase = PassiveAccept::Phase;
    if (cm_channel_ == nullptr) {
        return nullptr;
    }
    for (;;) {
        if (cm_retirement_.pending() && !advance_cm_retirement()) {
            return nullptr;
        }
        rdma_cm_event *event = nullptr;
        if (rdma_get_cm_event(cm_channel_, &event) != 0) {
            break;
        }
        const auto type = event->event;
        if (type == RDMA_CM_EVENT_CONNECT_REQUEST && listen_id_ != nullptr) {
            start_passive_accept(event);
            continue;
        }
        const auto found = passive_accepts_.find(event->id);
        if (found != passive_accepts_.end()) {
            auto      attempt = found->second;
            const int status  = event->status;
            rdma_ack_cm_event(event);
            if (attempt->phase != Phase::Retiring) {
                if (attempt->phase == Phase::Awaiting && type == RDMA_CM_EVENT_ESTABLISHED && status == 0 &&
                    std::chrono::steady_clock::now() < attempt->deadline) {
                    ibv_qp_attr receive_attributes{};
                    receive_attributes.min_rnr_timer = config_.min_rnr_timer;
                    attempt->phase = ibv_modify_qp(attempt->id->qp, &receive_attributes, IBV_QP_MIN_RNR_TIMER) == 0
                                       ? Phase::Publishing
                                       : Phase::Retiring;
                }
                else {
                    CRB_LOG_WARN("rdma accept: ESTABLISHED missing event={} status={}", static_cast<int>(type), status);
                    attempt->phase = Phase::Retiring;
                }
                if (attempt->deadline_timer != 0) {
                    socket_transport_->cancel_scheduled(std::exchange(attempt->deadline_timer, 0));
                }
            }
            if (auto connection = advance_passive_accept(attempt)) {
                return connection;
            }
            continue;
        }
        if (event->id != nullptr && event->id != listen_id_ &&
            (type == RDMA_CM_EVENT_DISCONNECTED || type == RDMA_CM_EVENT_CONNECT_ERROR ||
             type == RDMA_CM_EVENT_DEVICE_REMOVAL || type == RDMA_CM_EVENT_ADDR_CHANGE)) {
            cm_retirement_.event = event;
        }
        else {
            rdma_ack_cm_event(event);
        }
    }

    // Diagnostic callers drive the same state machine directly without a socket timer.
    if (socket_transport_ == nullptr) {
        for (auto it = passive_accepts_.begin(); it != passive_accepts_.end();) {
            auto attempt = (it++)->second;
            if (auto connection = advance_passive_accept(attempt)) {
                return connection;
            }
        }
    }
    return nullptr;
}

void RdmaTransport::start_passive_accept(rdma_cm_event *event)
{
    auto *id = event->id;
    if (passive_accepts_.contains(id)) {
        rdma_ack_cm_event(event);
        return;
    }
    std::unique_ptr<rdma_cm_event, decltype(&rdma_ack_cm_event)> ack(event, &rdma_ack_cm_event);
    std::shared_ptr<PassiveAccept>                               attempt;
    try {
        RdmaCmOffer peer;
        if (event->param.conn.private_data != nullptr &&
            parse_rdma_offer(std::span(static_cast<const uint8_t *>(event->param.conn.private_data),
                                       event->param.conn.private_data_len),
                             &peer) &&
            validate_rdma_offer(peer, local_capability(), nullptr, config_.require_tcp_preflight)) {
            auto owner  = std::make_shared<PassiveAccept>();
            owner->id   = id;
            owner->peer = peer;
            passive_accepts_.emplace(id, owner);
            attempt = std::move(owner);
        }
    }
    catch (const std::bad_alloc &) {
        CRB_LOG_ERROR("rdma accept: could not retain connection setup");
    }
    // Release private_data and acknowledge the event before handing the ID
    // back to the provider or accepting the request.
    rdma_ack_cm_event(ack.release());
    if (attempt == nullptr) {
        rdma_reject(id, nullptr, 0);
        rdma_destroy_id(id);
        return;
    }
    (void)advance_passive_accept(attempt);
}

bool RdmaTransport::prepare_passive_accept(PassiveAccept &attempt)
{
    auto          *id                = attempt.id;
    const auto    &peer_offer        = attempt.peer;
    const uint64_t required_capacity = static_cast<uint64_t>(config_.sq_depth) + config_.posted_receive_target;
    CqReservation  reservation(required_capacity > UINT32_MAX ? nullptr
                                                              : reserve_cq(static_cast<uint32_t>(required_capacity)),
                               static_cast<uint32_t>(required_capacity));
    if (reservation.queue == nullptr) {
        return false;
    }
    ibv_qp_init_attr qp_attr{
        .qp_context = id,
        .send_cq    = reservation.queue->cq,
        .recv_cq    = reservation.queue->cq,
        .srq        = nullptr,
        .cap        = {.max_send_wr     = config_.sq_depth,
                       .max_recv_wr     = config_.rq_depth,
                       .max_send_sge    = config_.qp_max_send_sge,
                       .max_recv_sge    = config_.qp_max_recv_sge,
                       .max_inline_data = 0},
        .qp_type    = IBV_QPT_RC,
        .sq_sig_all = 0
    };

    if (rdma_create_qp(id, pd_, &qp_attr) != 0) {
        return false;
    }
    auto pools = std::make_shared<ConnectionPools>(config_.send_queue_capacity,
                                                   config_.receive_batch_size > 1 ? config_.posted_receive_target : 0);
    configure_peer(*pools, peer_offer);
    pools->qp_number          = id->qp->qp_num;
    pools->effective_capacity = qp_attr.cap;
    pools->cq_index           = reservation.queue->index;
    pools->reserved_capacity  = reservation.capacity;
    if ((config_.wire_layout == RdmaWireLayout::ControlData &&
         (qp_attr.cap.max_send_sge < 2 || qp_attr.cap.max_recv_sge < 2)) ||
        !create_connection_pools(*pools)) {
        return false;
    }
    auto connection = std::make_shared<Connection>(reinterpret_cast<int64_t>(id), "rdma-peer", application_pool_);
    set_connection_pools(connection.get(), pools);
    reservation.committed = true;
    attempt.connection    = connection;
    attempt.pools         = pools;
    connection->set_path(ConnectionPath::Rdma);
    connection->transport_handle = reinterpret_cast<uint64_t>(id->qp);
    install_connection_notifiers(connection, pools->cq_index);
    if (!prime_receive(connection)) {
        return false;
    }

    rdma_conn_param params{};
    const auto      reply      = serialize_rdma_reply(local_capability(), peer_offer);
    params.private_data        = reply.data();
    params.private_data_len    = static_cast<uint8_t>(reply.size());
    params.initiator_depth     = 1;
    params.responder_resources = 1;
    params.retry_count         = config_.retry_count;
    params.rnr_retry_count     = config_.rnr_retry_count;
    const int accept_rc        = rdma_accept(id, &params);

    if (accept_rc != 0) {
        CRB_LOG_WARN("rdma accept: rdma_accept failed errno={}", errno);
        return false;
    }
    return true;
}

std::shared_ptr<Connection> RdmaTransport::advance_passive_accept(const std::shared_ptr<PassiveAccept> &attempt)
{
    using Phase = PassiveAccept::Phase;
    if (attempt->phase == Phase::Preparing) {
        try {
            const auto context = adopt_cm_context(attempt->id->verbs, false);
            if (context == CmContext::kBusy && retry_passive_accept(attempt)) {
                return nullptr;
            }
            if (context == CmContext::kReady && prepare_passive_accept(*attempt)) {
                attempt->phase    = Phase::Awaiting;
                attempt->deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(5000);
                if (socket_transport_ == nullptr) {
                    return nullptr;
                }
                attempt->deadline_timer = schedule_passive_accept(attempt, 5000, true);
                if (attempt->deadline_timer != 0) {
                    return nullptr;
                }
            }
        }
        catch (const std::bad_alloc &) {
            CRB_LOG_ERROR("rdma accept: could not prepare connection");
        }
        attempt->phase = Phase::Retiring;
    }
    if (attempt->phase == Phase::Awaiting) {
        if (std::chrono::steady_clock::now() < attempt->deadline) {
            return nullptr;
        }
        CRB_LOG_WARN("rdma accept: timed out waiting for ESTABLISHED");
        attempt->phase = Phase::Retiring;
    }
    if (attempt->phase == Phase::Publishing) {
        const auto published = try_publish_cm_connection(attempt->id, attempt->connection, attempt->pools);
        if (published == CmPublish::kPublished) {
            passive_accepts_.erase(std::exchange(attempt->id, nullptr));
            return attempt->connection;
        }
        if (published == CmPublish::kBusy && retry_passive_accept(attempt)) {
            return nullptr;
        }
        attempt->phase = Phase::Retiring;
    }
    if (try_retire_passive_accept(*attempt)) {
        passive_accepts_.erase(std::exchange(attempt->id, nullptr));
    }
    else {
        (void)retry_passive_accept(attempt);
    }
    return nullptr;
}

bool RdmaTransport::try_retire_passive_accept(PassiveAccept &attempt)
{
    if (attempt.connection != nullptr) {
        if (!try_destroy_connection_qp(attempt.connection.get(), attempt.id)) {
            return false;
        }
    }
    else if (attempt.id->qp != nullptr) {
        rdma_destroy_qp(attempt.id);
    }
    rdma_destroy_id(attempt.id);
    return true;
}

void RdmaTransport::retire_passive_accepts_at_shutdown()
{
    for (const auto &[id, attempt] : passive_accepts_) {
        (void)id;
        while (!try_retire_passive_accept(*attempt)) {
            std::this_thread::yield();
        }
        attempt->id = nullptr;
    }
    passive_accepts_.clear();
}

bool RdmaTransport::retry_passive_accept(const std::shared_ptr<PassiveAccept> &attempt)
{
    if (socket_transport_ == nullptr || attempt->retry_scheduled) {
        return true;
    }
    attempt->retry_scheduled = schedule_passive_accept(attempt, 1, false) != 0;
    return attempt->retry_scheduled;
}

uint64_t RdmaTransport::schedule_passive_accept(std::weak_ptr<PassiveAccept> attempt, uint32_t delay_ms, bool deadline)
{
    try {
        return socket_transport_->schedule([this, attempt, deadline] { on_passive_accept_timer(attempt, deadline); },
                                           delay_ms);
    }
    catch (const std::bad_alloc &) {
        CRB_LOG_ERROR("rdma accept: could not schedule connection progress");
        return 0;
    }
}

void RdmaTransport::on_passive_accept_timer(const std::weak_ptr<PassiveAccept> &weak, bool deadline)
{
    const auto attempt = weak.lock();
    if (attempt == nullptr) {
        return;
    }
    if (cm_polling_.test_and_set(std::memory_order_acquire)) {
        (void)schedule_passive_accept(weak, 1, deadline);
        return;
    }
    auto release = [](std::atomic_flag *flag) { flag->clear(std::memory_order_release); };
    const std::unique_ptr<std::atomic_flag, decltype(release)> owner(&cm_polling_, release);
    if (attempt->id == nullptr) {
        return;
    }
    if (deadline) {
        if (attempt->phase != PassiveAccept::Phase::Awaiting) {
            return;
        }
        attempt->deadline_timer = 0;
    }
    else {
        attempt->retry_scheduled = false;
    }
    if (auto connection = advance_passive_accept(attempt); connection != nullptr && connection_handler_) {
        connection_handler_(connection);
    }
}

} // namespace crowdb::rpc

#endif
