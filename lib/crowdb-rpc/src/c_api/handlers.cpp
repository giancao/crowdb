// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/server/message.h"

#include <memory>

static std::shared_ptr<void> own_handler_context(void *user_data, crowdb_rpc_handler_release_fn release)
{
    return {user_data, [release](void *context) {
                if (release != nullptr) {
                    release(context);
                }
            }};
}

// ── Custom handler dispatch ──────────

void crowdb_rpc_server_register_handler(crowdb_rpc_server_t server, uint16_t msg_type, crowdb_rpc_handler_fn callback,
                                        void *user_data, crowdb_rpc_handler_release_fn release)
{
    try {
        auto owner = own_handler_context(user_data, release);
        if (server == nullptr || callback == nullptr) {
            return;
        }
        server->server->register_handler(
            msg_type, [callback, owner = std::move(owner)](crowdb::rpc::Frame *req, crowdb::rpc::Connection *conn) {
                crowdb::rpc::invoke_c_handler(callback, owner.get(), req, conn);
                return nullptr; // async — callback submits response later.
            });
    }
    catch (...) {
    }
}

crowdb_rpc_status crowdb_rpc_server_submit_response(crowdb_rpc_server_t server, void *conn_handle,
                                                    const uint8_t *control, uint32_t control_len, const uint8_t *data,
                                                    uint32_t data_len, uint16_t msg_type, uint64_t request_id)
{
    try {
        if (server == nullptr || conn_handle == nullptr) {
            return CROWDB_RPC_ERR_INVALID_ARG;
        }

        auto *conn = static_cast<crowdb::rpc::Connection *>(conn_handle);
        auto *pool = server->server->pool();

        // Allocate response buffers from the pool and copy data.
        crowdb::rpc::Buffer *resp_ctrl = nullptr;
        if (control != nullptr && control_len > 0) {
            resp_ctrl = pool->alloc(control_len);
            if (resp_ctrl == nullptr) {
                return CROWDB_RPC_ERR_SEND_QUEUE;
            }
            resp_ctrl->write(control, control_len);
        }

        crowdb::rpc::Buffer *resp_data = nullptr;
        if (data != nullptr && data_len > 0) {
            resp_data = pool->alloc(data_len);
            if (resp_data == nullptr) {
                if (resp_ctrl != nullptr) {
                    resp_ctrl->release();
                }
                return CROWDB_RPC_ERR_SEND_QUEUE;
            }
            resp_data->write(data, data_len);
        }

        auto *frame = crowdb::rpc::build_out_frame(request_id, msg_type, resp_ctrl, resp_data);
        if (!server->server->transport()->submit(conn, frame)) {
            if (frame->control != nullptr) {
                frame->control->release();
            }
            frame->release_data();
            delete frame;
            return CROWDB_RPC_ERR_SEND_QUEUE;
        }
        return CROWDB_RPC_OK;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

static crowdb::rpc::Buffer *take_response_buffer(crowdb_rpc_buffer_t handle)
{
    if (handle == nullptr || handle->buf == nullptr)
        return nullptr;

    auto *buffer = handle->buf;
    buffer->control_allocator.destroy(handle);
    return buffer;
}

static crowdb_rpc_status submit_response_buffers(crowdb_rpc_server_t server, crowdb::rpc::Connection *connection,
                                                 crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                                 uint16_t msg_type, uint64_t request_id,
                                                 crowdb::rpc::Frame *request = nullptr)
{
    const auto release_buffer = [](crowdb::rpc::Buffer *buffer) { buffer->release(); };
    try {
        std::unique_ptr<crowdb::rpc::Buffer, decltype(release_buffer)> response_control(take_response_buffer(control),
                                                                                        release_buffer);
        std::unique_ptr<crowdb::rpc::Buffer, decltype(release_buffer)> response_data(take_response_buffer(data),
                                                                                     release_buffer);
        if (server == nullptr || connection == nullptr)
            return CROWDB_RPC_ERR_INVALID_ARG;

        std::unique_ptr<crowdb::rpc::OutFrame> response(
            crowdb::rpc::build_out_frame(request_id, msg_type, response_control.get(), response_data.get()));
        if (request != nullptr) {
            response->request_cache           = std::move(request->control_owner);
            response->tcp_request_cache_bytes = request->tcp_request_cache_bytes;
        }
        if (!server->server->transport()->submit(connection, response.get()))
            return CROWDB_RPC_ERR_SEND_QUEUE;
        if (request != nullptr)
            request->tcp_request_cache_bytes = 0;
        (void)response.release();
        (void)response_control.release();
        (void)response_data.release();
        return CROWDB_RPC_OK;
    }
    catch (...) {
        return CROWDB_RPC_ERR_CONN_ERROR;
    }
}

crowdb_rpc_status crowdb_rpc_server_submit_response_buffer(crowdb_rpc_server_t server, void *conn_handle,
                                                           crowdb_rpc_buffer_t control, crowdb_rpc_buffer_t data,
                                                           uint16_t msg_type, uint64_t request_id)
{
    return submit_response_buffers(server, static_cast<crowdb::rpc::Connection *>(conn_handle), control, data, msg_type,
                                   request_id);
}

crowdb_rpc_status crowdb_rpc_server_reply(crowdb_rpc_server_t server, void *frame_handle, crowdb_rpc_buffer_t control,
                                          crowdb_rpc_buffer_t data, uint16_t msg_type)
{
    auto      *request    = static_cast<crowdb::rpc::Frame *>(frame_handle);
    auto      *connection = request == nullptr ? nullptr : request->connection_owner.get();
    const auto request_id = request == nullptr ? 0 : request->request_id;
    return submit_response_buffers(server, connection, control, data, msg_type, request_id, request);
}

void crowdb_rpc_client_register_handler(crowdb_rpc_client_t client, uint16_t msg_type, crowdb_rpc_handler_fn callback,
                                        void *user_data, crowdb_rpc_handler_release_fn release)
{
    try {
        auto owner = own_handler_context(user_data, release);
        if (client == nullptr || callback == nullptr) {
            return;
        }
        client->client->register_handler(msg_type, callback, std::move(owner));
    }
    catch (...) {
    }
}

void crowdb_rpc_client_clear_handlers(crowdb_rpc_client_t client)
{
    try {
        if (client == nullptr) {
            return;
        }
        client->client->clear_handlers();
    }
    catch (...) {
    }
}

void crowdb_rpc_client_set_transport(crowdb_rpc_client_t client, crowdb_rpc_server_t server)
{
    try {
        if (client == nullptr || server == nullptr) {
            return;
        }
        client->client->set_transport(server->server->transport());
    }
    catch (...) {
    }
}

// ── Server-side request-response correlation ──────────────

void crowdb_rpc_server_set_request_client(crowdb_rpc_server_t server, crowdb_rpc_client_t client)
{
    try {
        if (server == nullptr || client == nullptr) {
            return;
        }
        server->server->set_request_client(client->client);
    }
    catch (...) {
    }
}

void crowdb_rpc_server_route_connection(crowdb_rpc_server_t server, crowdb_rpc_conn_t conn)
{
    try {
        if (server != nullptr && conn != nullptr && conn->conn != nullptr) {
            server->server->route_connection(*conn->conn);
        }
    }
    catch (...) {
    }
}
