// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend_metadata.h"
#include "block_account.h"
#include "block_directory.h"
#include "btree/allocation_account.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/backend_release_service.h"
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#    include "host_completion.h"
#endif

#include <dirent.h>

#include <cerrno>
#include <cstdio>
#include <cstring>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
// Parse a block filename like "1-3.blk-0007" → (store_id=1, group_id=3, idx=7).
// Returns false if the filename doesn't match the pattern.
bool parse_block_filename(const std::string &name, uint32_t &out_store, uint32_t &out_part, uint32_t &out_idx)
{
    auto dot = name.find('.');
    if (dot == std::string::npos) {
        return false;
    }
    if (name.substr(dot + 1, 4) != "blk-") {
        return false;
    }
    auto dash = name.find('-');
    if (dash == std::string::npos || dash >= dot) {
        return false;
    }
    try {
        out_store = static_cast<uint32_t>(std::stoul(name.substr(0, dash)));
        out_part  = static_cast<uint32_t>(std::stoul(name.substr(dash + 1, dot - dash - 1)));
        out_idx   = static_cast<uint32_t>(std::stoul(name.substr(dot + 5)));
    }
    catch (const std::invalid_argument &) {
        return false;
    }
    catch (const std::out_of_range &) {
        return false;
    }
    return true;
}

template <class Visit> Status visit_files(const std::string &dir, Visit visit)
{
    auto *directory = ::opendir(dir.c_str());
    if (directory == nullptr) {
        return Status::io_error(std::string("opendir: ") + std::strerror(errno));
    }

    struct CloseDirectory
    {
        DIR *value;

        ~CloseDirectory()
        {
            ::closedir(value);
        }
    } close{directory};

    for (;;) {
        errno       = 0;
        auto *entry = ::readdir(directory);
        if (entry == nullptr) {
            return errno == 0 ? Status::Ok() : Status::io_error(std::string("readdir: ") + std::strerror(errno));
        }
        auto status = visit(entry->d_name);
        if (!status.ok()) {
            return status;
        }
    }
}
} // namespace

Status BlockPageStore::prepare_blocks(const std::string &dir, uint32_t store_id, uint32_t group_id, uint64_t block_size,
                                      uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
try {
    const auto unit = iu_size == 0 ? 4096 : iu_size;
    if (out == nullptr || block_size == 0 || block_size % unit != 0) {
        return Status::invalid_argument("open_blocks: invalid block geometry");
    }
    std::shared_ptr<detail::AllocationAccount> allocations;
    auto                                       status = detail::AllocationAccount::create_unbound(&allocations);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<BackendReleaseService> release;
    status = BackendReleaseService::create(allocations, SIZE_MAX / 2, {}, nullptr, nullptr, &release);
    if (!status.ok()) {
        return status;
    }
    detail::BackendObjectStorage storage(sizeof(BlockPageStore), alignof(BlockPageStore), allocations);
    auto                        *value = ::new (storage.get())
        BlockPageStore(allocations, dir, store_id, group_id, block_size, unit, std::move(release));
    (void)storage.release();
    out->reset(value);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status BlockPageStore::open_blocks(const std::string &dir, uint32_t store_id, uint32_t group_id, uint64_t block_size,
                                   uint32_t iu_size, std::unique_ptr<BlockPageStore> *out)
try {
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    std::unique_ptr<BlockPageStore> store;
    auto                            status = prepare_blocks(dir, store_id, group_id, block_size, iu_size, &store);
    if (!status.ok()) {
        return status;
    }
    status = visit_files(dir, [&](const char *name) {
        uint32_t found_store = 0;
        uint32_t found_group = 0;
        uint32_t index       = 0;
        if (!parse_block_filename(name, found_store, found_group, index) || found_store != store_id ||
            found_group != group_id) {
            return Status::Ok();
        }
        std::shared_ptr<FileMedium> file;
        return store->blocks_->ensure(index, dir + "/" + name, store->iu_size_ > 1, &file);
    });
    if (!status.ok()) {
        return status;
    }
    if (store->num_extents() == 0) {
        std::shared_ptr<FileMedium> file;
        status = store->blocks_->ensure(0, store->block_path(0).c_str(), store->iu_size_ > 1, &file);
        if (!status.ok()) {
            return status;
        }
    }
    *out = std::move(store);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

#ifdef CROWDB_HAVE_LIBURING
Status BlockPageStore::submit_open_blocks(std::span<const std::string> names, common::DiskIOUring &uring,
                                          AsyncCompletion done)
try {
    if (!blocks_ || !done) {
        return Status::invalid_argument();
    }
    if (!uring.valid()) {
        return Status::not_supported();
    }
    auto budget = detail::block_allocation_budget(*this, done.allocations);
    if (!budget) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }

    struct OpenFiles
    {
        explicit OpenFiles(AsyncCompletion done) : done(std::move(done))
        {
        }

        void finish(Status status) noexcept
        {
            auto expected = Code::kOk;
            if (!status.ok() && failure.compare_exchange_strong(expected, status.code(), std::memory_order_relaxed)) {
                result = std::move(status);
            }
            if (remaining.fetch_sub(1, std::memory_order_acq_rel) == 1) {
                auto completion = std::move(done);
                completion.complete(std::move(result));
            }
        }

        AsyncCompletion     done;
        std::atomic<size_t> remaining{1}; // join also covers enumeration of the supplied names
        std::atomic<Code>   failure{Code::kOk};
        Status              result;
    };

    auto open   = std::allocate_shared<OpenFiles>(MetadataAllocator<OpenFiles>(budget), std::move(done));
    auto submit = [&](uint32_t index, std::string_view name) {
        open->remaining.fetch_add(1, std::memory_order_relaxed);
        blocks_->submit_ensure(index, *this, uring,
                               {.context = open.get(),
                                .complete_fn =
                                    [](void *context, Status status) noexcept {
                                        static_cast<OpenFiles *>(context)->finish(std::move(status));
                                    },
                                .owner       = open,
                                .allocations = budget},
                               name);
    };
    try {
        bool found = false;
        for (const auto &name : names) {
            uint32_t found_store = 0;
            uint32_t found_group = 0;
            uint32_t index       = 0;
            if (!parse_block_filename(name, found_store, found_group, index) || found_store != store_id_ ||
                found_group != group_id_) {
                continue;
            }
            if (name.find('/') != std::string::npos || name.find('\0') != std::string::npos) {
                open->finish(Status::invalid_argument());
                return Status::Ok();
            }
            found = true;
            submit(index, name);
        }
        if (!found) {
            submit(0, {});
        }
    }
    catch (const std::bad_alloc &) {
        open->finish(Status::resource_exhausted());
        return Status::Ok();
    }
    open->finish(Status::Ok());
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status BlockPageStore::open_blocks(const std::string &dir, uint32_t store_id, uint32_t group_id, uint64_t block_size,
                                   uint32_t iu_size, common::DiskIOUring &uring, std::unique_ptr<BlockPageStore> *out)
try {
    if (out == nullptr) {
        return Status::invalid_argument();
    }
    if (!uring.valid()) {
        return Status::not_supported();
    }
    std::unique_ptr<BlockPageStore> store;
    auto                            status = prepare_blocks(dir, store_id, group_id, block_size, iu_size, &store);
    if (!status.ok()) {
        return status;
    }
    std::vector<std::string> names;
    status = visit_files(dir, [&](const char *name) {
        names.emplace_back(name);
        return Status::Ok();
    });
    if (!status.ok()) {
        return status;
    }

    struct Result
    {
        detail::HostCompletion ready;
        Status                 status;
    } result;

    status = store->submit_open_blocks(names, uring,
                                       {.context = &result, .complete_fn = [](void *context, Status status) noexcept {
                                            auto &result  = *static_cast<Result *>(context);
                                            result.status = std::move(status);
                                            result.ready.signal();
                                        }});
    if (!status.ok()) {
        return status;
    }
    result.ready.wait();
    if (result.status.ok()) {
        *out = std::move(store);
    }
    return std::move(result.status);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
#endif

BlockPageStore::BlockPath BlockPageStore::block_path(uint32_t                                 index,
                                                     const std::shared_ptr<AllocationBudget> &budget) const
{
    char name[64];
    std::snprintf(name, sizeof(name), "%u-%u.blk-%04u", store_id_, group_id_, index);
    BlockPath path(dir_, MetadataAllocator<char>(budget ? budget : allocations_));
    path += '/';
    path += name;
    return path;
}

Status BlockPageStore::delete_block(uint32_t block_idx)
{
    return delete_block(block_idx, {});
}

Status BlockPageStore::delete_block(uint32_t block_idx, const std::shared_ptr<AllocationBudget> &budget)
try {
    if (!detail::valid_block_budget(*this, budget)) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    if (!blocks_) {
        return Status::invalid_argument("delete_block requires an array of blocks");
    }
    return blocks_->erase(block_idx, block_path(block_idx, budget).c_str(), budget);
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend metadata allocation unavailable");
}

#ifdef CROWDB_HAVE_LIBURING
void BlockPageStore::submit_ensure_block(uint32_t block_idx, common::DiskIOUring &uring, AsyncCompletion done)
{
    done.allocations = detail::block_allocation_budget(*this, done.allocations);
    if (!done.allocations || !blocks_) {
        done.complete(Status::invalid_argument());
        return;
    }
    blocks_->submit_ensure(block_idx, *this, uring, std::move(done));
}

void BlockPageStore::submit_delete_block(uint32_t block_idx, common::DiskIOUring &uring, AsyncCompletion done)
{
    done.allocations = detail::block_allocation_budget(*this, done.allocations);
    if (!done.allocations || !blocks_) {
        done.complete(Status::invalid_argument());
        return;
    }
    blocks_->submit_erase(block_idx, *this, uring, std::move(done));
}
#endif

size_t BlockPageStore::num_extents() const
{
    return blocks_ ? blocks_->count() : 0;
}

Status BlockPageStore::ensure_extents(uint64_t off, size_t len, const std::shared_ptr<AllocationBudget> &budget)
try {
    if (!detail::valid_block_budget(*this, budget)) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    if (len == 0) {
        return Status::Ok();
    }
    if (off > UINT64_MAX - len || (blocks_ && (off + len - 1) / block_size_ > UINT32_MAX)) {
        return Status::invalid_argument("BlockPageStore: byte range overflow");
    }
    if (!blocks_) {
        if (auto *file = dynamic_cast<FileMedium *>(medium_.get())) {
            file->mark_dirty();
        }
        return Status::Ok();
    }
    const auto last = (off + len - 1) / block_size_;
    for (auto index = off / block_size_; index <= last; ++index) {
        std::shared_ptr<FileMedium> file;
        auto                        status =
            blocks_->ensure(static_cast<uint32_t>(index), block_path(static_cast<uint32_t>(index), budget).c_str(),
                            iu_size_ > 1, &file, budget);
        if (!status.ok()) {
            return status;
        }
        file->mark_dirty();
    }
    return Status::Ok();
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend metadata allocation unavailable");
}

int BlockPageStore::fd_for_offset(uint64_t off, uint64_t *out_local) const
{
    auto file = file_for_offset(off, out_local);
    return file ? file->fd() : -1;
}

std::shared_ptr<FileMedium> BlockPageStore::file_for_offset(uint64_t off, uint64_t *out_local,
                                                            const std::shared_ptr<AllocationBudget> &budget) const
{
    if (!detail::valid_block_budget(*this, budget)) {
        return {};
    }
    if (!blocks_) {
        *out_local = off;
        return std::dynamic_pointer_cast<FileMedium>(medium_);
    }
    if (off / block_size_ > UINT32_MAX) {
        return {};
    }
    *out_local = off % block_size_;
    return blocks_->get(static_cast<uint32_t>(off / block_size_), budget);
}

Status BlockPageStore::sync()
{
    return sync({});
}

Status BlockPageStore::sync(const std::shared_ptr<AllocationBudget> &budget)
{
    if (!detail::valid_block_budget(*this, budget)) {
        return Status::invalid_argument("backend allocation budget belongs to another store");
    }
    if (sync_mode_ == SyncMode::kSkip) {
        return Status::Ok();
    }
    if (!blocks_) {
        return medium_->fsync();
    }
    uint64_t                    cursor = 0;
    uint32_t                    index  = 0;
    std::shared_ptr<FileMedium> file;
    while (blocks_->next(&cursor, &index, &file, budget)) {
        if (file->dirty()) {
            auto status = file->fsync();
            if (!status.ok()) {
                return status;
            }
        }
    }
    return Status::Ok();
}

uint64_t BlockPageStore::size() const
{
    return size({});
}

uint64_t BlockPageStore::size(const std::shared_ptr<AllocationBudget> &budget) const
{
    if (!detail::valid_block_budget(*this, budget)) {
        throw std::invalid_argument("backend allocation budget belongs to another store");
    }
    if (!blocks_) {
        if (is_block_device_ && capacity_ > 0) {
            return capacity_;
        }
        return medium_ ? medium_->size() : 0;
    }
    uint64_t                    total  = 0;
    uint64_t                    cursor = 0;
    uint32_t                    index  = 0;
    std::shared_ptr<FileMedium> file;
    while (blocks_->next(&cursor, &index, &file, budget)) {
        total = std::max(total, (static_cast<uint64_t>(index) * block_size_) + file->size());
    }
    return total;
}

std::vector<BlockSyncTarget> BlockPageStore::sync_targets() const
{
    std::vector<BlockSyncTarget> targets;
    if (sync_mode_ == SyncMode::kSkip) {
        return targets;
    }
    if (!blocks_) {
        auto file = std::dynamic_pointer_cast<FileMedium>(medium_);
        if (file && file->dirty()) {
            targets.push_back({.medium = file, .generation = file->dirty_generation()});
        }
        return targets;
    }
    uint64_t                    cursor = 0;
    uint32_t                    index  = 0;
    std::shared_ptr<FileMedium> file;
    while (blocks_->next(&cursor, &index, &file)) {
        if (file->dirty()) {
            targets.push_back({.medium = file, .generation = file->dirty_generation()});
        }
    }
    return targets;
}

BlockPageStore::BlockSyncTargets BlockPageStore::sync_targets(const std::shared_ptr<AllocationBudget> &budget) const
{
    auto allocations = detail::block_allocation_budget(*this, budget);
    if (!allocations) {
        throw std::invalid_argument("backend allocation budget belongs to another store");
    }
    BlockSyncTargets targets{MetadataAllocator<BlockSyncTarget>(allocations)};
    if (sync_mode_ == SyncMode::kSkip) {
        return targets;
    }
    targets.reserve(blocks_ ? blocks_->count() : 1);
    if (!blocks_) {
        auto file = std::dynamic_pointer_cast<FileMedium>(medium_);
        if (file && file->dirty()) {
            targets.push_back({.medium = file, .generation = file->dirty_generation()});
        }
        return targets;
    }
    uint64_t                    cursor = 0;
    uint32_t                    index  = 0;
    std::shared_ptr<FileMedium> file;
    while (blocks_->next(&cursor, &index, &file, allocations)) {
        if (file->dirty()) {
            if (targets.size() == targets.capacity()) {
                throw std::bad_alloc();
            }
            targets.push_back({.medium = file, .generation = file->dirty_generation()});
        }
    }
    return targets;
}

std::vector<int> BlockPageStore::dirty_fds() const
{
    std::vector<int> fds;
    for (const auto &target : sync_targets()) {
        fds.push_back(target.medium->fd());
    }
    return fds;
}

std::vector<int> BlockPageStore::all_extent_fds() const
{
    std::vector<int> fds;
    if (!blocks_) {
        if (auto *file = dynamic_cast<FileMedium *>(medium_.get())) {
            fds.push_back(file->fd());
        }
        return fds;
    }
    uint64_t                    cursor = 0;
    uint32_t                    index  = 0;
    std::shared_ptr<FileMedium> file;
    while (blocks_->next(&cursor, &index, &file)) {
        fds.push_back(file->fd());
    }
    return fds;
}

} // namespace crowdb::tree
