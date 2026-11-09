// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "local_block_gc.h"

namespace crowdb::tree
{
struct LocalSparseBlock
{
    Status          status;
    ProtectedExtent exclusion;
    uint64_t        next_block = 0;
};

// Inspect at most eight block positions and fence the first existing block
// whose free fraction exceeds the selected threshold. The exclusion does not
// replace readable leases and must survive both anchor rotations and deletion.
void select_sparse_local_block_async(BlockPageStore &store, std::shared_ptr<ExtentLedger> ledger, uint64_t first_block,
                                     const std::shared_ptr<AllocationBudget> &metadata, double free_threshold,
                                     const std::shared_ptr<AllocationBudget> &backend,
                                     std::function<void(LocalSparseBlock)>    complete);
} // namespace crowdb::tree
