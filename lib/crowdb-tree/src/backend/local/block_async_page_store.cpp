// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Local asynchronous block-backend implementation.

#include "backend_io_allocation.h"
#include "block_account.h"
#include "block_async_io.h"
#include "block_directory.h"
#include "btree/allocation_bound.h"

#include <linux/stat.h>
#include <sys/stat.h>

#include <atomic>
#include <cerrno>
#include <utility>
#include <vector>

namespace crowdb::tree
{

// ── BlockAsyncPageStore ───────────────────────────────────────────

BlockAsyncPageStore::BlockAsyncPageStore(BlockPageStore *store, ::crowdb::common::DiskIOUring *uring)
    : store_(store),
      uring_(uring)
{
}

uint64_t BlockAsyncPageStore::submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete)
{
    detail::submit_block_io(*store_, *uring_, addr, buf, nullptr, len, std::move(on_complete));
    return 0;
}

uint64_t BlockAsyncPageStore::submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete)
{
    detail::submit_block_io(*store_, *uring_, addr, nullptr, buf, len, std::move(on_complete));
    return 0;
}

void BlockAsyncPageStore::submit_delete_block(uint32_t block_idx, AsyncCompletion on_complete)
{
    store_->submit_delete_block(block_idx, *uring_, std::move(on_complete));
}

namespace
{
struct SizeProbe
{
    struct Stat
    {
        std::shared_ptr<SizeProbe>  probe;
        std::shared_ptr<FileMedium> file;
        uint64_t                    start;
        struct statx                info{};
    };

    SizeProbe(uint64_t *size, AsyncCompletion complete) : size(size), complete(std::move(complete))
    {
    }

    static void completed(void *context, int result) noexcept
    {
        auto &stat  = *static_cast<Stat *>(context);
        auto &probe = *stat.probe;
        if (result >= 0) {
            if ((stat.info.stx_mask & (STATX_TYPE | STATX_SIZE)) != (STATX_TYPE | STATX_SIZE) ||
                !S_ISREG(stat.info.stx_mode)) {
                result = -EINVAL;
            }
            else if (stat.info.stx_size > UINT64_MAX - stat.start) {
                result = -EOVERFLOW;
            }
            else {
                const auto end     = stat.start + static_cast<uint64_t>(stat.info.stx_size);
                auto       current = probe.end.load(std::memory_order_relaxed);
                while (current < end && !probe.end.compare_exchange_weak(current, end, std::memory_order_relaxed)) {
                }
            }
        }
        probe.finish(result);
    }

    void finish(int result) noexcept
    {
        if (result < 0) {
            int expected = 0;
            error.compare_exchange_strong(expected, result, std::memory_order_relaxed);
        }
        if (remaining.fetch_sub(1, std::memory_order_acq_rel) != 1) {
            return;
        }
        const auto failure = error.load(std::memory_order_relaxed);
        auto       done    = std::move(complete);
        if (failure == 0) {
            *size = end.load(std::memory_order_relaxed);
            done.complete(Status::Ok());
        }
        else if (failure == -ENOMEM) {
            done.complete(Status::resource_exhausted());
        }
        else if (failure == -EOVERFLOW) {
            done.complete(Status::corruption());
        }
        else {
            done.complete(Status::io_error());
        }
    }

    uint64_t             *size;
    AsyncCompletion       complete;
    std::atomic<size_t>   remaining{1}; // keep submission itself in the join
    std::atomic<uint64_t> end{0};
    std::atomic<int>      error{0};
};

struct FsyncState : std::enable_shared_from_this<FsyncState>
{
    explicit FsyncState(const std::shared_ptr<AllocationBudget> &budget)
        : targets(MetadataAllocator<BlockSyncTarget>(budget))
    {
    }

    BlockPageStore::BlockSyncTargets targets;
    size_t                           idx = 0;
    AsyncCompletion                  callback;
    ::crowdb::common::DiskIOUring   *uring = nullptr;

    static void completed(void *context, int result) noexcept
    {
        static_cast<FsyncState *>(context)->next(result);
    }

    void next(int result) noexcept
    {
        if (result >= 0 && idx != 0) {
            const auto &target = targets[idx - 1];
            target.medium->acknowledge_sync(target.generation);
        }
        if (result < 0 || idx == targets.size()) {
            auto status = result < 0 ? Status::io_error() : Status::Ok();
            if (result == -ENOMEM) {
                status = Status::resource_exhausted();
            }
            callback.complete(std::move(status));
            return;
        }
        auto self = weak_from_this().lock();
        if (!self) {
            callback.complete(Status::internal_error("fsync continuation lost its owner"));
            return;
        }
        const int fd = targets[idx++].medium->fd();
        uring->submit_fsync_owned(fd, false,
                                  {.context    = this,
                                   .callback   = &completed,
                                   .owner      = std::move(self),
                                   .allocation = detail::io_allocation(callback.allocations)});
    }
};
} // namespace

namespace detail
{
std::optional<size_t> block_sync_allocation_bound(size_t extents) noexcept
{
    return AllocationBound(shared_allocation_bound<FsyncState>())
        .add_product(extents, sizeof(BlockSyncTarget))
        .add_product(2, ::crowdb::common::DiskIOUring::owned_submission_allocation_bytes())
        .value();
}
} // namespace detail

Status BlockAsyncPageStore::submit_size(uint64_t *size, AsyncCompletion on_complete)
try {
    if (size == nullptr || !on_complete) {
        return Status::invalid_argument();
    }
    if (!store_->blocks_ || uring_ == nullptr) {
        return Status::not_supported();
    }
    const auto budget = detail::block_allocation_budget(*store_, on_complete.allocations);
    if (!budget) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    const auto probe =
        std::allocate_shared<SizeProbe>(MetadataAllocator<SizeProbe>(budget), size, std::move(on_complete));
    int failure = 0;
    try {
        uint64_t                    cursor = 0;
        uint32_t                    index  = 0;
        std::shared_ptr<FileMedium> file;
        while (store_->blocks_->next(&cursor, &index, &file, budget)) {
            if (index > UINT64_MAX / store_->block_size_) {
                failure = -EOVERFLOW;
                break;
            }
            auto stat = std::allocate_shared<SizeProbe::Stat>(MetadataAllocator<SizeProbe::Stat>(budget), probe,
                                                              std::move(file), index * store_->block_size_);
            probe->remaining.fetch_add(1, std::memory_order_relaxed);
            uring_->submit_stat_owned(stat->file->fd(), &stat->info,
                                      {.context    = stat.get(),
                                       .callback   = &SizeProbe::completed,
                                       .owner      = stat,
                                       .allocation = detail::io_allocation(budget)});
        }
    }
    catch (const std::bad_alloc &) {
        failure = -ENOMEM;
    }
    probe->finish(failure);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status BlockAsyncPageStore::submit_fsync(AsyncCompletion on_complete)
try {
    auto budget = detail::block_allocation_budget(*store_, on_complete.allocations);
    if (!budget) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    auto targets = store_->sync_targets(budget);
    if (targets.empty()) {
        on_complete.complete(Status::Ok());
        return Status::Ok();
    }
    auto state                  = std::allocate_shared<FsyncState>(MetadataAllocator<FsyncState>(budget), budget);
    state->targets              = std::move(targets);
    state->callback             = std::move(on_complete);
    state->callback.allocations = std::move(budget);
    state->uring                = uring_;
    state->next(0);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

void BlockAsyncPageStore::cancel(uint64_t /*op_id*/)
{
    // No-op — per-op cancel is removed. Use DiskIOUring::cancel_fd for
    // fd-level cancellation.
}

} // namespace crowdb::tree
