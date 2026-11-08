// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "local_sparse_gc.h"

#include <algorithm>
#include <cmath>

namespace crowdb::tree
{
namespace
{
struct SparseSelection : std::enable_shared_from_this<SparseSelection>
{
    PageDiskStore                        *store = nullptr;
    std::shared_ptr<ExtentLedger>         ledger;
    std::shared_ptr<AllocationBudget>     metadata;
    std::shared_ptr<AllocationBudget>     backend;
    std::function<void(LocalSparseBlock)> complete;
    LocalSparseBlock                      result;
    uint64_t                              bytes     = 0;
    uint64_t                              end       = 0;
    uint64_t                              next      = 0;
    size_t                                visits    = 0;
    double                                threshold = 0;

    void finish(Status status) noexcept
    {
        result.status = std::move(status);
        auto done     = std::move(complete);
        try {
            done(std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("sparse selection receiver threw after terminal");
        }
    }

    void advance() noexcept
    {
        try {
            while (next < end && visits < 8) {
                const auto index = next++;
                ++visits;
                result.next_block = next < end ? next : 0;
                uint64_t offset   = 0;
                if (!store->file_for_offset(index * bytes, &offset, backend)) {
                    continue;
                }
                auto status = ledger->exclude_allocations(
                    index * bytes, bytes, metadata,
                    [self = shared_from_this()](ExtentLedgerResult fence) { self->selected(std::move(fence)); });
                if (!status.ok()) {
                    finish(std::move(status));
                }
                return;
            }
            finish(Status::not_found());
        }
        catch (const std::bad_alloc &) {
            finish(Status::resource_exhausted());
        }
        catch (...) {
            finish(Status::internal_error());
        }
    }

    void selected(ExtentLedgerResult fence) noexcept
    {
        if (!fence.status.ok()) {
            finish(std::move(fence.status));
            return;
        }
        if (fence.occupied_bytes != 0 && static_cast<long double>(bytes - fence.occupied_bytes) / bytes > threshold) {
            result.exclusion = std::move(fence.extent);
            finish(Status::Ok());
            return;
        }
        fence.extent = {};
        advance();
    }
};
} // namespace

void select_sparse_local_block_async(PageDiskStore &store, std::shared_ptr<ExtentLedger> ledger, uint64_t first_block,
                                     const std::shared_ptr<AllocationBudget> &metadata, double free_threshold,
                                     const std::shared_ptr<AllocationBudget> &backend,
                                     std::function<void(LocalSparseBlock)>    complete)
{
    if (!complete) {
        return;
    }
    const auto bytes = store.block_size();
    if (!ledger || free_threshold <= 0.0 || free_threshold >= 1.0 || std::isnan(free_threshold)) {
        complete({.status = Status::invalid_argument(), .exclusion = {}});
        return;
    }
    if (bytes == 0 || bytes > 64 * 1024 * 1024 || bytes % ledger->limits().iu_size != 0) {
        complete({.status = Status::not_supported(), .exclusion = {}});
        return;
    }
    std::shared_ptr<SparseSelection> operation;
    try {
        operation = detail::reserved_shared<SparseSelection>(metadata);
    }
    catch (const std::bad_alloc &) {
        complete({.status = Status::resource_exhausted(), .exclusion = {}});
        return;
    }
    const auto begin     = ledger->limits().region_begin;
    operation->store     = &store;
    operation->bytes     = bytes;
    operation->end       = std::min(ledger->stats().address_high_water / bytes, uint64_t{UINT32_MAX} + 1);
    operation->next      = std::max(first_block, (begin / bytes) + static_cast<uint64_t>(begin % bytes != 0));
    operation->ledger    = std::move(ledger);
    operation->metadata  = metadata;
    operation->backend   = backend;
    operation->threshold = free_threshold;
    operation->complete  = std::move(complete);
    operation->advance();
}
} // namespace crowdb::tree
