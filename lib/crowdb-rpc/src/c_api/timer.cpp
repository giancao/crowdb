// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-rpc/c_api_internal.h"

extern "C" uint64_t crowdb_rpc_server_schedule(crowdb_rpc_server_t server, uint32_t delay_ms,
                                               crowdb_rpc_on_timer complete, void *context)
{
    if (server == nullptr || complete == nullptr) {
        return 0;
    }
    try {
        return server->server->transport()->schedule([complete, context] { complete(context, 1); }, delay_ms,
                                                     [complete, context] { complete(context, 0); });
    }
    catch (...) {
        return 0;
    }
}

extern "C" bool crowdb_rpc_server_cancel_scheduled(crowdb_rpc_server_t server, uint64_t id)
{
    if (server == nullptr) {
        return false;
    }
    try {
        return server->server->transport()->cancel_scheduled(id);
    }
    catch (...) {
        return false;
    }
}
