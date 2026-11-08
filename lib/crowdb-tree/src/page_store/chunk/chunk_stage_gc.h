// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_root_catalog.h"

namespace crowdb::tree
{
class ChunkPhysicalRegistry;
}

namespace crowdb::tree::detail
{
struct ChunkStageGcResult
{
    Status status;
    bool   deletion_acknowledged = false;
    bool   stage_removed         = false;
};

// Retirement is a separate ownership decision. This finite page store step only
// consumes a durably retired identity; time, absence, or a write error alone
// never authorize reclamation.
ChunkStageGcResult reclaim_retired_stage(RootCatalog &catalog, ChunkTransport &transport, uint64_t tree, uint64_t epoch,
                                         ChunkStageId id, const std::shared_ptr<AllocationBudget> &budget = {},
                                         const std::shared_ptr<ChunkPhysicalRegistry> &registry = {});

void submit_reclaim_retired_stage(std::shared_ptr<RootCatalog> catalog, std::shared_ptr<ChunkTransport> transport,
                                  uint64_t tree, uint64_t epoch, ChunkStageId id,
                                  std::shared_ptr<AllocationBudget>      budget,
                                  std::shared_ptr<ChunkPhysicalRegistry> registry, ChunkStageGcResult *out,
                                  AsyncCompletion complete);
} // namespace crowdb::tree::detail
