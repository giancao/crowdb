// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/chunk/chunk_fixed_pack.h"

namespace crowdb::tree::detail
{
class TestStageCatalog : public RootCatalog
{
  public:
    std::optional<ChunkStageRecord> record;

    bool stage_schema_supported() const noexcept override
    {
        return true;
    }

    Status load_stage(uint64_t /*unused*/, ChunkStageId /*unused*/, std::optional<ChunkStageRecord> *out) const override
    {
        *out = record;
        return Status::Ok();
    }

    Status compare_stage(uint64_t /*unused*/, uint64_t /*unused*/, const std::optional<ChunkStageRecord> &expected,
                         const std::optional<ChunkStageRecord> &next) override
    {
        auto status = validate_stage_transition(expected, next);
        if (!status.ok()) {
            return status;
        }
        if (record == next) {
            return Status::Ok();
        }
        if (record != expected) {
            return Status::unavailable();
        }
        record = next;
        return Status::Ok();
    }
};

} // namespace crowdb::tree::detail
