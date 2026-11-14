// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/epoch.h"

#include <array>
#include <bit>
#include <optional>
#include <string_view>

namespace crowdb::tree
{
// Sparse stable radix nodes never move while this backend is open. A reader
// copies a file owner under a short epoch guard and releases the guard before
// doing I/O. Replaced slot owners enter epoch reclamation.
class BlockDirectory
{
    struct Entry : EpochManager::Deferred
    {
        explicit Entry(std::shared_ptr<AllocationBudget> owner = {});
        std::shared_ptr<AllocationBudget> budget;
        std::shared_ptr<FileMedium>       medium;
    };

    // Zero is empty. The low bit distinguishes an in-progress metadata
    // operation from a published Entry. Busy is the exclusive legacy open or
    // unlink marker, and any other tagged value points to a shared open.
    static constexpr uintptr_t kBusy = 1;
    using Slot                       = std::atomic<uintptr_t>;

    static Entry *live_entry(uintptr_t value) noexcept
    {
        return value != 0 && (value & kBusy) == 0 ? std::bit_cast<Entry *>(value) : nullptr;
    }

    struct Segment
    {
        explicit Segment(std::shared_ptr<AllocationBudget> owner) : budget(std::move(owner))
        {
        }

        std::shared_ptr<AllocationBudget> budget;
        std::array<Slot, 256>             slots{};
    };

    struct Node
    {
        explicit Node(std::shared_ptr<AllocationBudget> owner = {}) : budget(std::move(owner))
        {
        }

        std::shared_ptr<AllocationBudget>    budget;
        std::array<std::atomic<void *>, 256> children{};
    };

  public:
    explicit BlockDirectory(std::shared_ptr<AllocationBudget>      budget  = {},
                            std::shared_ptr<BackendReleaseService> release = {});
    ~BlockDirectory();
    void                                      release_owned() noexcept;
    [[nodiscard]] std::shared_ptr<FileMedium> get(uint32_t                                 index,
                                                  const std::shared_ptr<AllocationBudget> &budget = {}) const;
    Status ensure(uint32_t index, const char *path, bool direct, std::shared_ptr<FileMedium> *out,
                  const std::shared_ptr<AllocationBudget> &budget = {});

    Status ensure(uint32_t index, const std::string &path, bool direct, std::shared_ptr<FileMedium> *out)
    {
        return ensure(index, path.c_str(), direct, out);
    }

    // The caller must exclusively own the entire byte extent before unlinking.
    // Conflicting metadata operations return Unavailable rather than waiting.
    Status erase(uint32_t index, const char *path, const std::shared_ptr<AllocationBudget> &budget = {});

    Status erase(uint32_t index, const std::string &path)
    {
        return erase(index, path.c_str());
    }
#ifdef CROWDB_HAVE_LIBURING
    void submit_ensure(uint32_t index, const BlockPageStore &store, common::DiskIOUring &uring, AsyncCompletion done,
                       std::string_view existing_name = {});
    void submit_erase(uint32_t index, const BlockPageStore &store, common::DiskIOUring &uring, AsyncCompletion done);
    [[nodiscard]] static std::optional<size_t> open_allocation_bound(size_t path_bytes) noexcept;
#endif

    // Advance through populated radix branches only; no vector or dense gap scan.
    bool                        next(uint64_t *cursor, uint32_t *index, std::shared_ptr<FileMedium> *out,
                                     const std::shared_ptr<AllocationBudget> &budget = {}) const;
    [[nodiscard]] size_t        count() const noexcept;
    [[nodiscard]] static size_t extent_allocation_bound() noexcept;

  private:
#ifdef CROWDB_HAVE_LIBURING
    struct Opening;
#endif
    struct Erase
    {
        Slot  *slot  = nullptr;
        Entry *entry = nullptr;
    };

    Status claim_erase(uint32_t index, const std::shared_ptr<AllocationBudget> &budget, Erase *erase);
    void   finish_erase(Erase erase, bool removed) noexcept;

    Segment       *segment(uint32_t index, bool create, const std::shared_ptr<AllocationBudget> &budget);
    const Segment *segment(uint32_t index) const;
    bool           next_node(const Node &node, unsigned shift, uint64_t prefix, uint64_t first, uint32_t *index,
                             std::shared_ptr<FileMedium> *out) const;
    static void    destroy_node(Node &node, unsigned shift);
    bool           retire_step() noexcept;

    struct Retirement : BackendReleaseService::Node
    {
        BlockDirectory *directory = nullptr;
    };

    Retirement              retirement_;
    std::array<void *, 4>   retire_path_{};
    std::array<uint16_t, 4> retire_index_{};
    uint8_t                 retire_depth_    = 0;
    bool                    retiring_        = false;
    bool                    entries_drained_ = false;

    std::shared_ptr<AllocationBudget> budget_;
    Node                              root_;
    std::atomic<size_t>               live_{0};
    mutable EpochManager              epoch_;
};
} // namespace crowdb::tree
