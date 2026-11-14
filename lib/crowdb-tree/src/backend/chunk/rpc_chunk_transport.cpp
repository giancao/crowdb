// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "rpc_chunk_transport_internal.h"

namespace crowdb::tree::detail
{

RpcChunkTransport::RpcChunkTransport(const ct_chunk_rpc_transport_options &options)
    : impl_(std::make_shared<Impl>(options))
{
    impl_->start_liveness();
}

RpcChunkTransport::~RpcChunkTransport()
{
    impl_->stop_liveness();
}

bool RpcChunkTransport::valid() const
{
    return impl_->valid();
}

} // namespace crowdb::tree::detail

ct_status ct_rpc_chunk_transport_open(const ct_chunk_rpc_transport_options *options, ct_chunk_transport **out)
{
    if (options == nullptr || out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto transport = std::make_shared<crowdb::tree::detail::RpcChunkTransport>(*options);
    if (!transport->valid()) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle       = std::make_unique<ct_chunk_transport>();
    handle->transport = std::move(transport);
    *out              = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

void ct_chunk_transport_free(ct_chunk_transport *transport)
{
    delete transport;
}
