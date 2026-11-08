// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "block_directory.h"

#include "backend_io_allocation.h"
#include "btree/allocation_bound.h"

#include <unistd.h>
#ifdef CROWDB_HAVE_LIBURING
#    include <fcntl.h>
#endif

#include <cassert>
#include <cerrno>
#include <cstring>

namespace crowdb::tree
{
namespace
{
template <class T> T *create_metadata(const std::shared_ptr<AllocationBudget> &budget)
{
    MetadataAllocator<T> allocator(budget);
    auto                *result = allocator.allocate(1);
    std::construct_at(result, budget);
    return result;
}

template <class T> void destroy_metadata(T *value) noexcept
{
    if (value == nullptr) {
        return;
    }
    MetadataAllocator<T> allocator(value->budget);
    std::destroy_at(value);
    allocator.deallocate(value, 1);
}
} // namespace

BlockDirectory::Entry::Entry(std::shared_ptr<AllocationBudget> owner) : budget(std::move(owner))
{
    destroy = [](EpochManager::Deferred *entry) noexcept { destroy_metadata(static_cast<Entry *>(entry)); };
}

BlockDirectory::BlockDirectory(std::shared_ptr<AllocationBudget>        budget,
                               std::shared_ptr<PageStoreReleaseService> release)
    : budget_(std::move(budget)),
      epoch_(budget_)
{
    if (release) {
        retirement_.directory = this;
        retirement_.dispose   = [](PageStoreReleaseService::Node *base, common::DiskIOUring *) noexcept {
            auto *directory = static_cast<Retirement *>(base)->directory;
            if (!directory->retire_step()) {
                return PageStoreReleaseService::Disposal::kMore;
            }
            MetadataAllocator<BlockDirectory> allocator(directory->budget_);
            std::destroy_at(directory);
            allocator.deallocate(directory, 1);
            return PageStoreReleaseService::Disposal::kDone;
        };
        if (!release->register_node(retirement_).ok()) {
            throw std::bad_alloc();
        }
    }
}

void BlockDirectoryDeleter::operator()(BlockDirectory *directory) const noexcept
{
    if (directory != nullptr) {
        directory->release_owned();
    }
}

void BlockDirectory::release_owned() noexcept
{
    if (retirement_.service) {
        PageStoreReleaseService::enqueue(retirement_);
    }
    else {
        MetadataAllocator<BlockDirectory> allocator(budget_);
        std::destroy_at(this);
        allocator.deallocate(this, 1);
    }
}

BlockDirectory::~BlockDirectory()
{
    if (!retiring_) {
        (void)epoch_.try_reclaim();
        destroy_node(root_, 24);
    }
}

void BlockDirectory::destroy_node(Node &node, unsigned shift)
{
    for (auto &slot : node.children) {
        void *child = slot.load(std::memory_order_relaxed);
        if (child == nullptr) {
            continue;
        }
        if (shift == 8) {
            auto *segment = static_cast<Segment *>(child);
            for (auto &entry : segment->slots) {
                destroy_metadata(live_entry(entry.load(std::memory_order_relaxed)));
            }
            destroy_metadata(segment);
        }
        else {
            auto *directory = static_cast<Node *>(child);
            destroy_node(*directory, shift - 8);
            destroy_metadata(directory);
        }
    }
}

BlockDirectory::Segment *BlockDirectory::segment(uint32_t index, bool create,
                                                 const std::shared_ptr<AllocationBudget> &budget)
{
    auto *node = &root_;
    for (unsigned shift = 24; shift >= 8; shift -= 8) {
        auto &slot  = node->children[(index >> shift) & 255];
        auto *child = slot.load(std::memory_order_acquire);
        if (child == nullptr && create) {
            auto *candidate = shift == 8 ? static_cast<void *>(create_metadata<Segment>(budget))
                                         : static_cast<void *>(create_metadata<Node>(budget));
            if (!slot.compare_exchange_strong(child, candidate, std::memory_order_release, std::memory_order_acquire)) {
                if (shift == 8) {
                    destroy_metadata(static_cast<Segment *>(candidate));
                }
                else {
                    destroy_metadata(static_cast<Node *>(candidate));
                }
            }
            else {
                child = candidate;
            }
        }
        if (child == nullptr || shift == 8) {
            return static_cast<Segment *>(child);
        }
        node = static_cast<Node *>(child);
    }
    return nullptr;
}

const BlockDirectory::Segment *BlockDirectory::segment(uint32_t index) const
{
    const auto *node = &root_;
    for (unsigned shift = 24; shift >= 8; shift -= 8) {
        auto *child = node->children[(index >> shift) & 255].load(std::memory_order_acquire);
        if (child == nullptr || shift == 8) {
            return static_cast<const Segment *>(child);
        }
        node = static_cast<const Node *>(child);
    }
    return nullptr;
}

std::shared_ptr<FileMedium> BlockDirectory::get(uint32_t index, const std::shared_ptr<AllocationBudget> &budget) const
{
    auto        guard = epoch_.enter(budget ? budget : budget_);
    const auto *leaf  = segment(index);
    const auto *entry =
        leaf != nullptr ? live_entry(leaf->slots[index & 255].load(std::memory_order_acquire)) : nullptr;
    return entry != nullptr ? entry->medium : std::shared_ptr<FileMedium>{};
}

Status BlockDirectory::ensure(uint32_t index, const char *path, bool direct, std::shared_ptr<FileMedium> *out,
                              const std::shared_ptr<AllocationBudget> &budget)
try {
    auto  guard = epoch_.enter(budget ? budget : budget_);
    auto &slot  = segment(index, true, budget ? budget : budget_)->slots[index & 255];
    auto  value = slot.load(std::memory_order_acquire);
    if (auto *entry = live_entry(value)) {
        *out = entry->medium;
        return Status::Ok();
    }
    if (value != 0 || !slot.compare_exchange_strong(value, kBusy, std::memory_order_acq_rel)) {
        return Status::unavailable("block metadata operation already in progress");
    }

    struct Restore
    {
        Slot &slot;
        bool  published = false;

        ~Restore()
        {
            if (!published) {
                slot.store(0, std::memory_order_release);
            }
        }
    } restore{.slot = slot};

    std::unique_ptr<Entry, void (*)(Entry *)> prepared(create_metadata<Entry>(budget ? budget : budget_),
                                                       &destroy_metadata<Entry>);
    std::shared_ptr<FileMedium>               file;
    auto status = FileMedium::open_owned(path, direct, budget ? budget : budget_, &file, retirement_.service);
    if (!status.ok()) {
        return status;
    }
    prepared->medium = std::move(file);
    *out             = prepared->medium;
    live_.fetch_add(1, std::memory_order_relaxed);
    slot.store(reinterpret_cast<uintptr_t>(prepared.release()), std::memory_order_release);
    restore.published = true;
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status BlockDirectory::claim_erase(uint32_t index, const std::shared_ptr<AllocationBudget> &budget, Erase *erase)
{
    auto  guard = epoch_.enter(budget ? budget : budget_);
    auto *leaf  = segment(index, false, budget ? budget : budget_);
    if (leaf == nullptr) {
        return Status::Ok();
    }
    auto &slot  = leaf->slots[index & 255];
    auto  value = slot.load(std::memory_order_acquire);
    if (value == 0) {
        return Status::Ok();
    }
    auto *entry = live_entry(value);
    if (entry == nullptr || !slot.compare_exchange_strong(value, kBusy, std::memory_order_acq_rel)) {
        return Status::unavailable("block metadata operation already in progress");
    }
    *erase = {.slot = &slot, .entry = entry};
    return Status::Ok();
}

void BlockDirectory::finish_erase(Erase erase, bool removed) noexcept
{
    if (removed) {
        live_.fetch_sub(1, std::memory_order_relaxed);
        erase.slot->store(0, std::memory_order_release);
        epoch_.defer(erase.entry);
    }
    else {
        erase.slot->store(reinterpret_cast<uintptr_t>(erase.entry), std::memory_order_release);
    }
    (void)epoch_.reclaim_deferred();
}

Status BlockDirectory::erase(uint32_t index, const char *path, const std::shared_ptr<AllocationBudget> &budget)
{
    Erase erase;
    auto  status = claim_erase(index, budget, &erase);
    if (!status.ok() || erase.entry == nullptr) {
        return status;
    }
    const auto error = ::unlink(path) == 0 ? 0 : errno;
    finish_erase(erase, error == 0 || error == ENOENT);
    return error == 0 || error == ENOENT ? Status::Ok()
                                         : Status::io_error(std::string("unlink: ") + std::strerror(error));
}

#ifdef CROWDB_HAVE_LIBURING
struct BlockDirectory::Opening : EpochManager::Deferred
{
    struct Waiter
    {
        AsyncCompletion done;
        Waiter         *next = nullptr;
    };

    static Waiter settled;

    Opening(BlockDirectory &directory, Slot &slot, const AsyncCompletion &done)
        : directory(directory),
          slot(slot),
          path(MetadataAllocator<char>(done.allocations)),
          first(done)
    {
        destroy = [](EpochManager::Deferred *entry) noexcept {
            auto keep = std::move(static_cast<Opening *>(entry)->slot_owner);
        };
    }

    // A disengaged result means the writer is attached to this open. The
    // caller's epoch guard protects the tagged pointer through this handoff.
    std::optional<Status> attach(const AsyncCompletion &done)
    {
        auto *head = waiters.load(std::memory_order_acquire);
        if (head == &settled) {
            return result.without_message();
        }
        MetadataAllocator<Waiter> allocator(done.allocations);
        auto                     *waiter = allocator.allocate(1);
        std::construct_at(waiter, done);
        do {
            waiter->next = head;
            if (waiters.compare_exchange_weak(head, waiter, std::memory_order_acq_rel)) {
                return std::nullopt;
            }
        } while (head != &settled);
        std::destroy_at(waiter);
        allocator.deallocate(waiter, 1);
        return result.without_message();
    }

    static void opened(void *context, int fd) noexcept
    {
        auto &self = *static_cast<Opening *>(context);
        if (fd >= 0) {
            self.prepared->medium->fd_ = fd;
            self.directory.live_.fetch_add(1, std::memory_order_relaxed);
            self.finish(Status::Ok());
        }
        else {
            self.finish(fd == -ENOMEM ? Status::resource_exhausted() : Status::io_error());
        }
    }

    void finish(Status status) noexcept
    {
        if (!status.ok()) {
            prepared.reset();
        }
        const auto value = prepared ? reinterpret_cast<uintptr_t>(prepared.release()) : 0;
        slot.store(value, std::memory_order_release);
        result       = std::move(status);
        auto *waiter = waiters.exchange(&settled, std::memory_order_acq_rel);
        assert(waiter != &settled);
        auto leader = std::move(first);
        directory.epoch_.defer(this);
        (void)directory.epoch_.reclaim_deferred();

        leader.complete(result.without_message());
        while (waiter != nullptr) {
            auto                     *next = waiter->next;
            auto                      done = std::move(waiter->done);
            MetadataAllocator<Waiter> allocator(done.allocations);
            std::destroy_at(waiter);
            allocator.deallocate(waiter, 1);
            done.complete(result.without_message());
            waiter = next;
        }
    }

    BlockDirectory                           &directory;
    Slot                                     &slot;
    PageDiskStore::BlockPath                  path;
    std::unique_ptr<Entry, void (*)(Entry *)> prepared{nullptr, &destroy_metadata<Entry>};
    AsyncCompletion                           first;
    std::shared_ptr<Opening>                  slot_owner;
    std::atomic<Waiter *>                     waiters{nullptr};
    Status                                    result;
};

constinit BlockDirectory::Opening::Waiter BlockDirectory::Opening::settled{};

void BlockDirectory::submit_ensure(uint32_t index, const PageDiskStore &store, common::DiskIOUring &uring,
                                   AsyncCompletion done, std::string_view existing_name)
{
    std::shared_ptr<Opening> launch;
    Status                   status;
    try {
        auto  guard = epoch_.enter(done.allocations);
        auto &slot  = segment(index, true, done.allocations)->slots[index & 255];
        auto  value = slot.load(std::memory_order_acquire);
        if (value == 0) {
            auto candidate =
                std::allocate_shared<Opening>(MetadataAllocator<Opening>(done.allocations), *this, slot, done);
            assert((reinterpret_cast<uintptr_t>(candidate.get()) & kBusy) == 0);
            const auto opening = reinterpret_cast<uintptr_t>(candidate.get()) | kBusy;
            if (slot.compare_exchange_strong(value, opening, std::memory_order_acq_rel)) {
                candidate->slot_owner = candidate;
                launch                = std::move(candidate);
            }
        }
        if (!launch && (value & kBusy) != 0) {
            if (value == kBusy) {
                status = Status::unavailable("block metadata operation already in progress");
            }
            else {
                auto &opening  = *std::bit_cast<Opening *>(value & ~kBusy);
                auto  attached = opening.attach(done);
                if (!attached) {
                    return;
                }
                status = std::move(*attached);
            }
        }
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    if (!launch) {
        done.complete(std::move(status));
        return;
    }

    try {
        if (existing_name.empty()) {
            launch->path = store.block_path(index, done.allocations);
        }
        else {
            launch->path.assign(store.dir_);
            launch->path += '/';
            launch->path.append(existing_name);
        }
        launch->prepared.reset(create_metadata<Entry>(done.allocations));
        status = FileMedium::allocate_owned(done.allocations, &launch->prepared->medium, retirement_.service);
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    if (!status.ok()) {
        launch->finish(std::move(status));
        return;
    }
    const int flags = O_RDWR | (existing_name.empty() ? O_CREAT : 0) | (store.iu_size() > 1 ? O_DIRECT : 0);
    uring.submit_open_owned(launch->path.c_str(), flags, 0644,
                            {.context    = launch.get(),
                             .callback   = &Opening::opened,
                             .owner      = launch,
                             .allocation = detail::io_allocation(done.allocations)});
}

std::optional<size_t> BlockDirectory::open_allocation_bound(size_t path_bytes) noexcept
{
    return detail::AllocationBound(detail::shared_allocation_bound<Opening>())
        .add(sizeof(Opening::Waiter))
        .add_product(2, path_bytes)
        .add(common::DiskIOUring::owned_submission_allocation_bytes())
        .value();
}

void BlockDirectory::submit_erase(uint32_t index, const PageDiskStore &store, common::DiskIOUring &uring,
                                  AsyncCompletion done)
{
    struct Unlink
    {
        BlockDirectory          *directory;
        Erase                    erase;
        PageDiskStore::BlockPath path;
        AsyncCompletion          done;

        static void completed(void *context, int result) noexcept
        {
            auto      &self    = *static_cast<Unlink *>(context);
            const bool removed = result >= 0 || result == -ENOENT;
            self.directory->finish_erase(self.erase, removed);
            auto status = removed ? Status::Ok() : Status::io_error();
            if (result == -ENOMEM) {
                status = Status::resource_exhausted();
            }
            self.done.complete(std::move(status));
        }
    };

    Erase                   erase;
    std::shared_ptr<Unlink> request;
    Status                  status;
    try {
        status = claim_erase(index, done.allocations, &erase);
        if (status.ok() && erase.entry != nullptr) {
            request = std::allocate_shared<Unlink>(MetadataAllocator<Unlink>(done.allocations), this, erase,
                                                   store.block_path(index, done.allocations), done);
        }
    }
    catch (const std::bad_alloc &) {
        if (erase.entry != nullptr) {
            finish_erase(erase, false);
        }
        done.complete(Status::resource_exhausted());
        return;
    }
    if (!request) {
        (void)epoch_.reclaim_deferred();
        done.complete(status.ok() ? Status::not_found() : std::move(status));
        return;
    }
    uring.submit_unlink_owned(request->path.c_str(), {.context    = request.get(),
                                                      .callback   = &Unlink::completed,
                                                      .owner      = request,
                                                      .allocation = detail::io_allocation(done.allocations)});
}
#endif

bool BlockDirectory::next_node(const Node &node, unsigned shift, uint64_t prefix, uint64_t first, uint32_t *index,
                               std::shared_ptr<FileMedium> *out) const
{
    for (size_t i = 0; i < 256; ++i) {
        const uint64_t begin = prefix | (static_cast<uint64_t>(i) << shift);
        if (begin + (uint64_t{1} << shift) <= first) {
            continue;
        }
        const auto *child = node.children[i].load(std::memory_order_acquire);
        if (child == nullptr) {
            continue;
        }
        if (shift != 8) {
            if (next_node(*static_cast<const Node *>(child), shift - 8, begin, first, index, out)) {
                return true;
            }
            continue;
        }
        const auto &leaf = *static_cast<const Segment *>(child);
        for (auto j = static_cast<size_t>(std::max(first, begin) - begin); j < 256; ++j) {
            const auto *entry = live_entry(leaf.slots[j].load(std::memory_order_acquire));
            if (entry != nullptr) {
                *index = static_cast<uint32_t>(begin + j);
                *out   = entry->medium;
                return true;
            }
        }
    }
    return false;
}

bool BlockDirectory::next(uint64_t *cursor, uint32_t *index, std::shared_ptr<FileMedium> *out,
                          const std::shared_ptr<AllocationBudget> &budget) const
{
    if (*cursor > UINT32_MAX) {
        return false;
    }
    auto guard = epoch_.enter(budget ? budget : budget_);
    if (!next_node(root_, 24, 0, *cursor, index, out)) {
        return false;
    }
    *cursor = static_cast<uint64_t>(*index) + 1;
    return true;
}

size_t BlockDirectory::extent_allocation_bound() noexcept
{
    // A previously absent sparse path owns two nodes, one leaf segment, one
    // incarnation and its file object/control. File object header/alignment
    // and the supported standard-library control fit in the shared allowance.
    return (2 * sizeof(Node)) + sizeof(Segment) + sizeof(Entry) + detail::shared_allocation_bound<FileMedium>() +
           FileMedium::close_allocation_bound() + 128;
}

size_t BlockDirectory::count() const noexcept
{
    return live_.load(std::memory_order_relaxed);
}
} // namespace crowdb::tree
