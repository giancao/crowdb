// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_root_catalog.h"

namespace crowdb::tree::detail::test_protection
{
struct TestProtection
{
    int                    drops      = 0;
    int                    free_blobs = 0;
    int                    pins       = 0;
    ct_status              pin_error  = 0;
    uint32_t               action     = 0;
    bool                   corrupt    = false;
    ct_status              reader     = -8;
    std::array<uint8_t, 3> root{1, 2, 3};

    static ct_status load_root(void *p, int32_t /*kind*/, uint64_t /*tree*/, uint64_t /*object*/, const uint8_t **out,
                               size_t *length)
    {
        auto *self = static_cast<TestProtection *>(p);
        *out       = self->root.data();
        *length    = self->root.size();
        return 0;
    }

    static ct_status load(void *p, uint64_t /*tree*/, ct_chunk_stage_id id, ct_chunk_candidate_status *out)
    {
        auto *self = static_cast<TestProtection *>(p);
        *out       = {.id                  = id,
                      .generation          = 1,
                      .reference_count     = 1,
                      .prepared_references = 1,
                      .prepared_windows    = 1,
                      .released_windows    = 0,
                      .state               = self->corrupt ? 99U : 2U};
        return 0;
    }

    static ct_status list(void *p, uint64_t tree, const ct_chunk_stage_id * /*after*/, uint32_t /*limit*/,
                          ct_chunk_candidate_status *out, size_t *count)
    {
        *count = 1;
        return load(p, tree, {.owner_epoch = 1, .operation_high = 2, .operation_low = 3}, out);
    }

    ct_root_catalog_callbacks base()
    {
        return {.load      = &load_root,
                .free_blob = +[](void *p, const uint8_t *, size_t) { ++static_cast<TestProtection *>(p)->free_blobs; },
                .reclaim_before   = nullptr,
                .pin_generation   = nullptr,
                .unpin_generation = nullptr,
                .drop_context     = +[](void *p) { ++static_cast<TestProtection *>(p)->drops; }};
    }

    static ct_root_catalog_staging_callbacks stages()
    {
        return {.version      = 1,
                .struct_bytes = sizeof(ct_root_catalog_staging_callbacks),
                .capabilities = CT_CHUNK_STAGE_SCHEMA_V1,
                .load =
                    +[](void *, uint64_t, ct_chunk_stage_id, const uint8_t **, size_t *) -> ct_status { return -1; },
                .compare = +[](void *, uint64_t, uint64_t, const uint8_t *, size_t, const uint8_t *,
                               size_t) -> ct_status { return 0; },
                .list    = +[](void *, uint64_t, const ct_chunk_stage_id *, uint32_t, const uint8_t **,
                               size_t *) -> ct_status { return -5; }};
    }

    static ct_root_catalog_protection_callbacks protection()
    {
        return {.version         = 1,
                .struct_bytes    = sizeof(ct_root_catalog_protection_callbacks),
                .capabilities    = CT_CHUNK_PROTECTION_SCHEMA_V1,
                .check_reader    = +[](void *p) -> ct_status { return static_cast<TestProtection *>(p)->reader; },
                .writer_quiesced = +[](void *, uint64_t, uint64_t) -> ct_status { return 0; },
                .begin   = +[](void *, uint64_t, ct_chunk_stage_id, uint64_t, uint64_t, uint32_t, const uint8_t *,
                               size_t) -> ct_status { return 0; },
                .load    = &load,
                .list    = &list,
                .prepare = +[](void *, uint64_t, ct_chunk_stage_id, uint64_t, const ct_chunk_stage_id *,
                               size_t) -> ct_status { return 0; },
                .finish  = +[](void *p, uint64_t, ct_chunk_stage_id, uint32_t action) -> ct_status {
                    static_cast<TestProtection *>(p)->action = action;
                    return 0;
                },
                .release = +[](void *, uint64_t, ct_chunk_stage_id, uint64_t) -> ct_status { return 0; },
                .pin     = +[](void *p, uint64_t, ct_chunk_stage_id, ct_chunk_stage_id, uint8_t acquire) -> ct_status {
                    if (static_cast<TestProtection *>(p)->pin_error != 0) {
                        return static_cast<TestProtection *>(p)->pin_error;
                    }
                    static_cast<TestProtection *>(p)->pins += acquire ? 1 : -1;
                    return 0;
                },
                .prune = +[](void *, uint64_t, ct_chunk_stage_id, const ct_chunk_stage_id *, uint32_t,
                             ct_chunk_pin_progress *out) -> ct_status {
                    *out = {
                        .after     = {.owner_epoch = 1, .operation_high = 2, .operation_low = 3},
                        .examined  = 1,
                        .released  = 1,
                        .has_after = 1,
                        .more      = 0
                    };
                    return 0;
                }};
    }
};

} // namespace crowdb::tree::detail::test_protection
