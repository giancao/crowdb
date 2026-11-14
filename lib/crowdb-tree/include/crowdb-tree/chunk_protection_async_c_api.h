// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/chunk_protection_c_api.h"
#include "crowdb-tree/chunk_stage_async_c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

struct ct_root_catalog_protection_async_callbacks
{
    uint32_t version;
    uint32_t struct_bytes;
    ct_status (*writer_quiesced)(void *, uint64_t, uint64_t, ct_catalog_status_complete, void *);
    ct_status (*begin)(void *, uint64_t, ct_chunk_stage_id, uint64_t, uint64_t, uint32_t, const uint8_t *, size_t,
                       ct_catalog_status_complete, void *);
    ct_status (*load)(void *, uint64_t, ct_chunk_stage_id, ct_chunk_candidate_status *, ct_catalog_status_complete,
                      void *);
    ct_status (*list)(void *, uint64_t, const ct_chunk_stage_id *, uint32_t, ct_chunk_candidate_status *, size_t *,
                      ct_catalog_status_complete, void *);
    ct_status (*prepare)(void *, uint64_t, ct_chunk_stage_id, uint64_t, const ct_chunk_stage_id *, size_t,
                         ct_catalog_status_complete, void *);
    ct_status (*finish)(void *, uint64_t, ct_chunk_stage_id, uint32_t, ct_catalog_status_complete, void *);
    ct_status (*release)(void *, uint64_t, ct_chunk_stage_id, uint64_t, ct_catalog_status_complete, void *);
    ct_status (*pin)(void *, uint64_t, ct_chunk_stage_id, ct_chunk_stage_id, uint8_t, ct_catalog_status_complete,
                     void *);
    ct_status (*prune)(void *, uint64_t, ct_chunk_stage_id, const ct_chunk_stage_id *, uint32_t,
                       ct_chunk_pin_progress *, ct_catalog_status_complete, void *);
    ct_status (*root)(void *, uint64_t, uint64_t, ct_catalog_blob_complete, void *);
};

// Bind once during host initialization. Output buffers and input ranges stay
// owned by the native operation until its accepted terminal callback.
ct_status ct_callback_root_catalog_set_protection_async_v1(ct_root_catalog *,
                                                           const ct_root_catalog_protection_async_callbacks *);

#ifdef __cplusplus
}
#endif
