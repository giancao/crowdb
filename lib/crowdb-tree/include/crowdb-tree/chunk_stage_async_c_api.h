// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/chunk_stage_c_api.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*ct_catalog_status_complete)(void *, ct_status);
// Blob ownership follows ct_root_catalog_callbacks::free_blob, including errors.
typedef void (*ct_catalog_blob_complete)(void *, ct_status, const uint8_t *, size_t);

struct ct_root_catalog_staging_async_callbacks
{
    uint32_t version;
    uint32_t struct_bytes;
    ct_status (*load)(void *, uint64_t, ct_chunk_stage_id, ct_catalog_blob_complete, void *);
    ct_status (*compare)(void *, uint64_t, uint64_t, const uint8_t *, size_t, const uint8_t *, size_t,
                         ct_catalog_status_complete, void *);
    ct_status (*list)(void *, uint64_t, const ct_chunk_stage_id *, uint32_t, ct_catalog_blob_complete, void *);
};

// Initialization only, before exposing this catalog to a Tree. OK transfers
// completion context to each submitted callback; rejection never calls it.
ct_status ct_callback_root_catalog_set_staging_async_v1(ct_root_catalog *,
                                                        const ct_root_catalog_staging_async_callbacks *);
ct_status ct_root_catalog_stage_compare_async_v1(ct_root_catalog *, uint64_t, uint64_t, const uint8_t *, size_t,
                                                 const uint8_t *, size_t, ct_catalog_status_complete, void *);

#ifdef __cplusplus
}
#endif
