// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/chunk_runtime_c_api.h"

int main()
{
    auto *volatile runtime_constructor = &ct_chunk_runtime_page_store_open;
    auto *volatile rpc_constructor     = &ct_rpc_chunk_transport_open;
    return runtime_constructor != nullptr && rpc_constructor != nullptr ? 0 : 1;
}
