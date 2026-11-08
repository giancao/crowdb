// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/apply_dispatcher.h"
#include "btree/tree_work_counters.h"
#include "crowdb-common/log.h"
#include "local_block_gc.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
using Complete = detail::InlineFunction<void(LocalBlockGcResult)>;

struct LocalBlockGc : std::enable_shared_from_this<LocalBlockGc>
{
    PageMemStore  *memory = nullptr;
    PageDiskStore *file   = nullptr;
#ifdef CROWDB_HAVE_LIBURING
    AsyncPageDiskStore *asynchronous = nullptr;
#endif
    std::shared_ptr<ExtentLedger>     ledger;
    std::shared_ptr<AllocationBudget> metadata;
    std::shared_ptr<AllocationBudget> backend;
    std::shared_ptr<ApplyDispatcher>  legacy_progress;
    ApplyDispatcher::Node             legacy_node{&delete_legacy, this};
    ProtectedExtent                   lease;
    Complete                          complete;
    LocalBlockGcLimits                limits;
    LocalBlockGcResult                result;
    uint64_t                          bytes = 0;
    uint64_t                          end   = 0;

    void finish(Status status) noexcept
    {
        result.status = std::move(status);
        try {
            complete(std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("local block GC receiver threw after terminal");
        }
    }

    void next() noexcept
    {
        if (result.next_block >= end || result.inspected >= limits.max_blocks ||
            bytes > limits.max_deleted_bytes - result.deleted_bytes) {
            result.complete = result.next_block >= end;
            finish(Status::Ok());
            return;
        }
        try {
            const auto status = ledger->claim_free(
                result.next_block * bytes, bytes, metadata,
                [self = shared_from_this()](ExtentLedgerResult claimed) { self->claimed(std::move(claimed)); });
            if (!status.ok()) {
                finish(status);
            }
        }
        catch (const std::bad_alloc &) {
            finish(Status::resource_exhausted());
        }
        catch (...) {
            finish(Status::internal_error());
        }
    }

    void claimed(ExtentLedgerResult claimed) noexcept
    {
        ++result.inspected;
        if (claimed.status.code() == Code::kNotFound) {
            ++result.protected_blocks;
            advance(false);
            return;
        }
        if (!claimed.status.ok()) {
            finish(std::move(claimed.status));
            return;
        }
        lease = std::move(claimed.extent);
        if (memory != nullptr) {
            advance(memory->release_page_block_page(result.next_block));
            return;
        }
#ifdef CROWDB_HAVE_LIBURING
        if (asynchronous != nullptr) {
            asynchronous->submit_delete_block(static_cast<uint32_t>(result.next_block),
                                              {.context = this,
                                               .complete_fn =
                                                   [](void *context, Status status) noexcept {
                                                       static_cast<LocalBlockGc *>(context)->deleted(std::move(status));
                                                   },
                                               .owner       = weak_from_this().lock(),
                                               .allocations = backend});
            return;
        }
#endif
        legacy_progress->request(legacy_node, weak_from_this().lock());
    }

    static void delete_legacy(void *context) noexcept
    {
        auto &self = *static_cast<LocalBlockGc *>(context);
        try {
            uint64_t offset = 0;
            if (!self.file->file_for_offset(self.result.next_block * self.bytes, &offset, self.backend)) {
                self.advance(false);
                return;
            }
            self.deleted(self.file->delete_block(static_cast<uint32_t>(self.result.next_block), self.backend));
        }
        catch (const std::bad_alloc &) {
            self.deleted(Status::resource_exhausted());
        }
        catch (...) {
            self.deleted(Status::io_error());
        }
    }

    void deleted(Status status) noexcept
    {
        if (!status.ok() && status.code() != Code::kNotFound) {
            lease = {};
            finish(std::move(status));
            return;
        }
        advance(status.ok());
    }

    void advance(bool deleted) noexcept
    {
        if (deleted) {
            ++result.deleted_blocks;
            result.deleted_bytes += bytes;
            record_work(metadata, TreeWorkCounter::PageBlockDeletedBytes, bytes, TreeWorkCause::GarbageCollection);
        }
        ++result.next_block;
        // The exclusive range lease survives physical deletion. Releasing it
        // queues onto the same ledger; another claim cannot race page reuse.
        lease = {};
        next();
    }
};

std::shared_ptr<LocalBlockGc> prepare_gc(std::shared_ptr<ExtentLedger> ledger, uint64_t first_block, uint64_t bytes,
                                         uint64_t last, LocalBlockGcLimits limits,
                                         const std::shared_ptr<AllocationBudget> &metadata, Complete &complete)
{
    if (!complete) {
        return {};
    }
    if (!ledger || limits.max_blocks == 0 || bytes == 0 || limits.max_deleted_bytes == 0 ||
        bytes % ledger->limits().iu_size != 0) {
        complete({.status = Status::invalid_argument("invalid local GC geometry or limits")});
        return {};
    }
    if (limits.max_deleted_bytes < bytes) {
        complete({.status = Status::resource_exhausted("GC deletion budget cannot hold one block")});
        return {};
    }
    const auto begin = ledger->limits().region_begin;
    const auto first = std::max(first_block, (begin / bytes) + static_cast<uint64_t>(begin % bytes != 0));
    std::shared_ptr<LocalBlockGc> operation;
    try {
        operation = detail::reserved_shared<LocalBlockGc>(metadata);
    }
    catch (const std::bad_alloc &) {
        complete({.status = Status::resource_exhausted("local block GC allocation failed"), .next_block = first});
        return {};
    }
    operation->bytes             = bytes;
    operation->end               = std::min(ledger->stats().address_high_water / bytes, last);
    operation->ledger            = std::move(ledger);
    operation->metadata          = metadata;
    operation->limits            = limits;
    operation->result.next_block = first;
    operation->complete          = std::move(complete);
    return operation;
}
} // namespace

size_t local_block_gc_operation_bound() noexcept
{
    return detail::shared_allocation_bound<LocalBlockGc>();
}

void collect_free_memory_blocks(PageMemStore &store, std::shared_ptr<ExtentLedger> ledger, uint64_t first_block,
                                LocalBlockGcLimits limits, const std::shared_ptr<AllocationBudget> &metadata,
                                Complete complete)
{
    if (auto operation = prepare_gc(std::move(ledger), first_block, PageMemStore::kPageBlockPageBytes, UINT64_MAX,
                                    limits, metadata, complete)) {
        operation->memory = &store;
        operation->next();
    }
}

void collect_free_file_blocks(PageDiskStore &store, std::shared_ptr<ExtentLedger> ledger, uint64_t first_block,
                              LocalBlockGcLimits limits, const std::shared_ptr<AllocationBudget> &metadata,
                              const std::shared_ptr<AllocationBudget> &backend, AsyncPageStore *asynchronous,
                              const std::shared_ptr<ApplyDispatcher> &legacy_progress, Complete complete)
{
    if (auto operation = prepare_gc(std::move(ledger), first_block, store.block_size(), uint64_t{UINT32_MAX} + 1,
                                    limits, metadata, complete)) {
        operation->file    = &store;
        operation->backend = backend;
#ifdef CROWDB_HAVE_LIBURING
        operation->asynchronous = dynamic_cast<AsyncPageDiskStore *>(asynchronous);
        if (operation->asynchronous != nullptr) {
            operation->next();
            return;
        }
#else
        (void)asynchronous;
#endif
        if (!legacy_progress) {
            operation->finish(Status::not_supported());
            return;
        }
        operation->legacy_progress = legacy_progress;
        operation->next();
    }
}
} // namespace crowdb::tree
