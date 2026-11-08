// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/page_store/page_disk_store.h"
#include "extent_ledger.h"

namespace crowdb::tree
{
class ApplyDispatcher;

struct LocalBlockGcLimits
{
    size_t   max_blocks        = 64;
    uint64_t max_deleted_bytes = 64 * 1024 * 1024;
};

struct LocalBlockGcResult
{
    Status   status;
    uint64_t next_block       = 0;
    size_t   inspected        = 0;
    size_t   protected_blocks = 0;
    size_t   deleted_blocks   = 0;
    uint64_t deleted_bytes    = 0;
    bool     complete         = false;
};

// One synchronous host-side pass. Every deletion holds an exclusive ledger
// lease, covering retained anchors, readers, staged frames and pending writes.
// The caller pre-admits metadata for the bounded pass. Partial progress remains
// valid on failure; resumption starts at next_block without skipping a failure.
[[nodiscard]] std::optional<size_t> local_block_gc_metadata_bound(LocalBlockGcLimits limits) noexcept;
[[nodiscard]] size_t                local_block_gc_operation_bound() noexcept;
// Memory page block pages are reclaimable without enabling legacy block compaction.
[[nodiscard]] uint64_t local_block_gc_unit(const PageStore &store) noexcept;
LocalBlockGcResult     collect_free_local_blocks(PageStore &store, ExtentLedger &ledger, uint64_t first_block,
                                                 LocalBlockGcLimits                       limits,
                                                 const std::shared_ptr<AllocationBudget> &metadata,
                                                 const std::shared_ptr<AllocationBudget> &backend = {});

// Accepted page mem store cleanup resumes directly from ledger completion.
// The completion owner keeps store and ledger alive until terminal delivery.
void collect_free_memory_blocks(PageMemStore &store, std::shared_ptr<ExtentLedger> ledger, uint64_t first_block,
                                LocalBlockGcLimits limits, const std::shared_ptr<AllocationBudget> &metadata,
                                detail::InlineFunction<void(LocalBlockGcResult)> complete);
void collect_free_file_blocks(PageDiskStore &store, std::shared_ptr<ExtentLedger> ledger, uint64_t first_block,
                              LocalBlockGcLimits limits, const std::shared_ptr<AllocationBudget> &metadata,
                              const std::shared_ptr<AllocationBudget> &backend, AsyncPageStore *asynchronous,
                              const std::shared_ptr<ApplyDispatcher>          &legacy_progress,
                              detail::InlineFunction<void(LocalBlockGcResult)> complete);
} // namespace crowdb::tree
