// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/framing.h"

namespace crowdb::rpc
{
namespace
{
struct ControlOwner
{
    std::shared_ptr<void> admission;
    ControlBytes          bytes;
};

crowdb_rpc_buffer_t wrap_control(Frame &frame, bool take = false, bool keep_admission = false)
{
    if (frame.control.empty())
        return nullptr;
    if (!take && frame.control_owner == nullptr)
        return crowdb_rpc_buffer_create(frame.control.data(), static_cast<uint32_t>(frame.control.size()));
    auto owner  = std::make_unique<ControlOwner>();
    auto handle = crowdb_rpc_buffer_create_external(
        frame.control.data(), static_cast<uint32_t>(frame.control.size()),
        [](void *context) { delete static_cast<ControlOwner *>(context); }, owner.get());
    if (handle != nullptr) {
        owner->bytes = std::move(frame.control);
        if (!keep_admission)
            owner->admission = std::move(frame.control_owner);
        if ((take || keep_admission) && owner->admission == nullptr && owner->bytes.capacity() <= UINT32_MAX) {
            handle->buf->allocation_capacity = static_cast<uint32_t>(owner->bytes.capacity());
        }
        (void)owner.release();
    }
    return handle;
}

crowdb_rpc_buffer_t wrap_data(Buffer *buffer, uint64_t request_id)
{
    if (buffer == nullptr || buffer->len == 0)
        return nullptr;
    buffer->set_metadata(request_id, buffer->stream_id, buffer->sequence, buffer->logical_offset,
                         buffer->registration_id);
    return wrap_owned_buffer(buffer->ref_clone());
}
} // namespace

void frame_to_c_handles(Frame *frame, crowdb_rpc_buffer_t *out_ctrl, crowdb_rpc_buffer_t *out_data)
{
    *out_ctrl = nullptr;
    *out_data = nullptr;
    std::unique_ptr<Frame> incoming(frame);
    if (incoming == nullptr)
        return;
    *out_ctrl = wrap_control(*incoming, false, incoming->release_admission_on_delivery);
    *out_data = wrap_data(incoming->data_buf, incoming->request_id);
}

void invoke_c_handler(crowdb_rpc_handler_fn callback, void *user_data, Frame *request, Connection *conn)
{
    if (callback == nullptr || request == nullptr) {
        delete request;
        return;
    }
    request->connection_owner = conn == nullptr ? nullptr : conn->weak_from_this().lock();
    if (request->connection_owner != nullptr && conn->path() == ConnectionPath::Tcp) {
        request->tcp_request_cache_bytes = static_cast<uint64_t>(request->header.msg_size) + request->header.data_size;
        if (request->tcp_request_cache_bytes != 0)
            conn->add_request_cache_bytes(request->tcp_request_cache_bytes);
    }
    uint64_t       req_id      = request->request_id;
    uint64_t       create_nano = request->rpc_create_nano;
    uint16_t       msg_type    = request->header.msg_type;
    const uint8_t *ctrl_ptr    = request->control.empty() ? nullptr : request->control.data();
    auto           ctrl_len    = static_cast<uint32_t>(request->control.size());
    const uint8_t *data_ptr    = nullptr;
    uint32_t       data_len    = 0;
    if (request->data_buf != nullptr && request->data_buf->len > 0) {
        data_ptr = request->data_buf->payload_data();
        data_len = request->data_buf->len;
    }
    void *conn_handle  = static_cast<void *>(conn);
    void *frame_handle = static_cast<void *>(request);

    // Transfer frame ownership to the callback. The callback must call
    // crowdb_rpc_frame_release(frame_handle) when done.
    callback(req_id, create_nano, msg_type, ctrl_ptr, ctrl_len, data_ptr, data_len, conn_handle, frame_handle,
             user_data);
}

} // namespace crowdb::rpc

extern "C" void crowdb_rpc_frame_release(void *frame_handle)
{
    delete static_cast<crowdb::rpc::Frame *>(frame_handle);
}

extern "C" crowdb_rpc_buffer_t crowdb_rpc_frame_take_control(void *frame_handle)
{
    try {
        if (frame_handle != nullptr) {
            return crowdb::rpc::wrap_control(*static_cast<crowdb::rpc::Frame *>(frame_handle), true);
        }
    }
    catch (...) {
    }
    return nullptr;
}

extern "C" crowdb_rpc_buffer_t crowdb_rpc_frame_take_request_control(void *frame_handle)
{
    try {
        if (frame_handle != nullptr) {
            return crowdb::rpc::wrap_control(*static_cast<crowdb::rpc::Frame *>(frame_handle), true, true);
        }
    }
    catch (...) {
    }
    return nullptr;
}

extern "C" void crowdb_rpc_frame_close_connection(void *frame_handle)
{
    try {
        if (frame_handle != nullptr) {
            auto connection = static_cast<crowdb::rpc::Frame *>(frame_handle)->connection_owner;
            if (connection != nullptr) {
                connection->close();
            }
        }
    }
    catch (...) {
    }
}

extern "C" crowdb_rpc_conn_t crowdb_rpc_frame_retain_connection(void *frame_handle)
{
    auto *frame = static_cast<crowdb::rpc::Frame *>(frame_handle);
    if (frame == nullptr || frame->connection_owner == nullptr)
        return nullptr;
    try {
        return new crowdb_rpc_conn_s{frame->connection_owner};
    }
    catch (...) {
        return nullptr;
    }
}
