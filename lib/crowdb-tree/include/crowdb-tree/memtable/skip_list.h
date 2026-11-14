// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Concurrent one-node-per-key map. Level zero establishes membership; upper
// links accelerate lookup. Published nodes remain linked for their lifetime.
// Callers protect borrowed pointers with the list's epoch manager.
#pragma once

#include "crowdb-tree/btree/cell.h"
#include "crowdb-tree/buffer.h"
#include "crowdb-tree/epoch.h"
#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/memtable/source_stats.h"
#include "crowdb-tree/slice.h"

#include <atomic>
#include <bit>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace crowdb::tree
{

// One node in a key's descending slot order. Its value can outlive the list.
// Owned buffers include the cell header. External buffers contain only value bytes.
struct CellVersion : EpochManager::Deferred
{
    static constexpr uintptr_t kRemoved = 1;
    static constexpr uintptr_t kEnd     = 2;

    std::atomic<uintptr_t> next_bits{0};
    uint64_t               slot;
    uint8_t                flags;
    AllocationLease        credit;
    buffer                 cell;
    AllocationCharge       allocation;

    CellVersion(buffer &&bytes, uint64_t version, uint8_t kind, std::shared_ptr<AllocationCounter> counter = nullptr,
                AllocationLease credit = {})
        : slot(version),
          flags(kind),
          credit(std::move(credit)),
          cell(std::move(bytes))
    {
        if (counter != nullptr) {
            allocation.set(std::move(counter), sizeof(CellVersion) + cell.size());
        }
    }

    struct Deleter
    {
        void operator()(CellVersion *version) const noexcept;
    };

    using Owner = std::unique_ptr<CellVersion, Deleter>;
    static Owner create(buffer &&cell, uint64_t slot, uint8_t flags, AllocationLease payload,
                        const std::shared_ptr<AllocationBudget>  &budget,
                        const std::shared_ptr<AllocationCounter> &counter, size_t key_bytes, void **key_storage);
    static Owner reference(std::shared_ptr<const CellVersion> cell, const std::shared_ptr<AllocationBudget> &budget,
                           const std::shared_ptr<AllocationCounter> &counter);
    [[nodiscard]] static size_t allocation_bound() noexcept;

    [[nodiscard]] static CellVersion *pointer(uintptr_t bits) noexcept
    {
        return std::bit_cast<CellVersion *>(bits & ~(kRemoved | kEnd));
    }

    [[nodiscard]] CellVersion *next() const noexcept
    {
        return pointer(next_bits.load(std::memory_order_acquire));
    }

    [[nodiscard]] const CellVersion *version_at(uint64_t frontier) const;

    // Keeps the published node alive until epoch retirement. A split tail
    // instead holds the original value owner and has its own list link.
    std::shared_ptr<const CellVersion> value_owner;
};

struct MutationStats
{
    uint64_t                     cas_retries   = 0;
    uint64_t                     overwrite     = 0;
    uint64_t                     keep          = 0;
    uint64_t                     merged        = 0;
    uint64_t                     keep_gap      = 0;
    uint64_t                     keep_pending  = 0;
    uint64_t                     keep_boundary = 0;
    const std::atomic<uint64_t> *admission     = nullptr;
    bool                         gap           = false;
};

struct Node : EpochManager::Deferred
{
    AllocationLease           allocation_credit;
    AllocationCharge          allocation;
    std::weak_ptr<const void> shared_allocation;
    std::atomic<uintptr_t>    versions_{0};
    std::atomic<uint64_t>     pruning_bound_{0};
    uint32_t                  height_{1};  // tower height
    uint32_t                  key_len_{0}; // inline key length

    // The tower follows the fixed fields. Access via next_ptr(level).
    [[nodiscard]] std::atomic<Node *> *next_ptr(uint32_t level)
    {
        return reinterpret_cast<std::atomic<Node *> *>(reinterpret_cast<char *>(this) + sizeof(Node)) + level;
    }

    [[nodiscard]] const std::atomic<Node *> *next_ptr(uint32_t level) const
    {
        return reinterpret_cast<const std::atomic<Node *> *>(reinterpret_cast<const char *>(this) + sizeof(Node)) +
               level;
    }

    [[nodiscard]] Node *next(uint32_t level) const
    {
        return next_ptr(level)->load(std::memory_order_acquire);
    }

    void set_next(uint32_t level, Node *n)
    {
        next_ptr(level)->store(n, std::memory_order_release);
    }

    [[nodiscard]] const char *key_data() const
    {
        return reinterpret_cast<const char *>(this) + sizeof(Node) + (sizeof(std::atomic<Node *>) * height_);
    }

    [[nodiscard]] Slice key_slice() const
    {
        return {key_data(), key_len_};
    }

    // Total allocation size for a node with `height` and `key_len`.
    [[nodiscard]] static size_t alloc_size(uint32_t height, size_t key_len)
    {
        return sizeof(Node) + (sizeof(std::atomic<Node *>) * height) + key_len;
    }
};

class ConcurrentSkipList
{
  public:
    static constexpr uint32_t kMaxHeight = 12;

    // Preparation is thread-bound and owns the objects needed by commit.
    class PreparedWrite
    {
      public:
        PreparedWrite()                                     = default;
        PreparedWrite(PreparedWrite &&) noexcept            = default;
        PreparedWrite &operator=(PreparedWrite &&) noexcept = default;
        PreparedWrite(const PreparedWrite &)                = delete;
        PreparedWrite &operator=(const PreparedWrite &)     = delete;

        [[nodiscard]] Slice key() const noexcept
        {
            return existing_ != nullptr ? existing_->key_slice() : candidate_->key_slice();
        }

        [[nodiscard]] const std::shared_ptr<const CellVersion> &cell_owner() const noexcept
        {
            return version_->value_owner;
        }

      private:
        friend class ConcurrentSkipList;

        struct NodeDeleter
        {
            void operator()(Node *node) const noexcept;
        };

        EpochManager::Guard                guard_;
        CellVersion::Owner                 version_;
        std::unique_ptr<Node, NodeDeleter> candidate_;
        Node                              *existing_        = nullptr;
        uint64_t                           bound_           = 0;
        bool                               initialize_slot_ = false;
    };

    [[nodiscard]] PreparedWrite prepare_write(Slice key, buffer &&cell, uint8_t flags, AllocationLease payload,
                                              uint64_t bound, const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] PreparedWrite prepare_reference(Slice key, std::shared_ptr<const CellVersion> cell, uint64_t bound,
                                                  const std::shared_ptr<AllocationBudget> &budget);
    bool commit_write(PreparedWrite write, uint64_t slot, MutationStats *stats = nullptr) noexcept;

    // Ordered cursor with one coherent version candidate per position. The
    // caller retains source ownership and epoch protection for its lifetime.
    class Cursor
    {
      public:
        Cursor() = default;

        explicit Cursor(const Node *n, uint64_t frontier = UINT64_MAX, uint64_t floor = 0,
                        std::optional<Slice> upper = std::nullopt)
            : cur_(n),
              frontier_(frontier),
              floor_(floor),
              upper_(upper)
        {
            select();
        }

        [[nodiscard]] bool valid() const
        {
            return cur_ != nullptr;
        }

        [[nodiscard]] Slice key() const
        {
            return cur_->key_slice();
        }

        [[nodiscard]] const CellVersion *cell_version() const
        {
            return candidate_ != nullptr ? candidate_->value_owner.get() : nullptr;
        }

        // Copy while the source and this thread's epoch guard are alive. The
        // result owns only this version, independently of the node/history.
        [[nodiscard]] std::shared_ptr<const CellVersion> cell_owner() const
        {
            return candidate_ != nullptr ? candidate_->value_owner : nullptr;
        }

        // Select this exact key at a captured frontier without advancing to
        // another key. The caller keeps the source and its epoch guard alive.
        [[nodiscard]] std::shared_ptr<const CellVersion> cell_owner_at(uint64_t frontier) const
        {
            if (cur_ == nullptr) {
                return {};
            }
            const auto *versions = CellVersion::pointer(cur_->versions_.load(std::memory_order_acquire));
            const auto *version  = versions->version_at(frontier);
            return version != nullptr ? version->value_owner : nullptr;
        }

        void set_floor(uint64_t floor)
        {
            floor_ = floor;
            select();
        }

        // Advance to the next key with an eligible version.
        void advance();

        // Prefetch the next node's memory (the one advance() will move to).
        // A non-faulting hint — brings the next node into CPU cache before
        // the merge loop calls advance(), overlapping the cache fill with
        // the current merge step's work.
        void prefetch_next() const
        {
            if (cur_ != nullptr) {
                if (const Node *n = cur_->next(0); n != nullptr) {
                    __builtin_prefetch(n, 0, 1);
                }
            }
        }

      private:
        void                 select();
        const Node          *cur_       = nullptr;
        const CellVersion   *candidate_ = nullptr;
        uint64_t             frontier_  = UINT64_MAX;
        uint64_t             floor_     = 0;
        std::optional<Slice> upper_;
    };

    explicit ConcurrentSkipList(EpochManager *epoch = nullptr, std::shared_ptr<AllocationBudget> budget = {});
    ~ConcurrentSkipList();

    ConcurrentSkipList(const ConcurrentSkipList &)            = delete;
    ConcurrentSkipList &operator=(const ConcurrentSkipList &) = delete;

#ifdef CROWDB_TREE_TEST_UTIL
    enum class PausePoint : uint8_t {
        kBeforePrepare,
        kBeforeSearch,
        kAfterLevelZero,
        kBeforeVersionCas,
        kBeforeInsertAccounting,
        kBeforeLeaveAccounting,
        kBeforeCloseAccounting
    };
    using TestHook = void (*)(void *, PausePoint, Slice);

    void set_hook_for_tests(void *context, TestHook hook)
    {
        test_context_ = context;
        test_hook_    = hook;
    }
#endif

    // Consumes the payload on every outcome. Bound is validated by admission.
    bool upsert(Slice key, std::shared_ptr<const CellVersion> cv, uint64_t bound, MutationStats *stats = nullptr,
                std::shared_ptr<AllocationBudget> budget = {});
    void prune(Slice key, uint64_t bound, MutationStats *stats = nullptr);
    [[nodiscard]] Cursor prefix_cursor(uint64_t frontier, uint64_t floor = 0) const;
    // Bounds are [lower, upper); an absent bound differs from an empty key.
    // Bound storage and source/epoch protection must outlive the cursor.
    [[nodiscard]] Cursor range_cursor(std::optional<Slice> lower, std::optional<Slice> upper, uint64_t frontier,
                                      uint64_t floor = 0) const;
    // Constant-time independently sampled logical/source capacities. These
    // counters describe the current linked versions, not physical allocation
    // identity or memory retained by old readers.
    [[nodiscard]] VersionMemory memory() const;
#ifdef CROWDB_TREE_TEST_UTIL
    [[nodiscard]] VersionMemory scan_memory_for_tests() const;
#endif

    [[nodiscard]] EpochManager &epoch() const
    {
        return *epoch_;
    }

    [[nodiscard]] const std::shared_ptr<AllocationBudget> &budget() const noexcept
    {
        return budget_;
    }

    // Point lookup: returns the CellVersion* for `key`, or nullptr. The
    // returned pointer is valid only while the caller's epoch guard is held
    // (a concurrent overwrite retires the old version via epoch).
    [[nodiscard]] const CellVersion *find(Slice key) const;

    // Return a cursor positioned at the first live node with key >
    // `start_after` (or the first live node if start_after is empty).
    [[nodiscard]] Cursor cursor(Slice start_after) const;

    // Return a cursor positioned at the first live node >= `start_key`, or
    // strictly greater when `inclusive` is false. Unlike cursor(), an empty
    // key is an explicit bound.
    [[nodiscard]] Cursor cursor_from(Slice start_key, bool inclusive) const;

    // Return the final live node <= `start_key`, or < it when `inclusive` is
    // false. When `has_start_bound` is false, return the final live node.
    [[nodiscard]] Cursor cursor_reverse(Slice start_key, bool has_start_bound, bool inclusive) const;

    [[nodiscard]] size_t count() const
    {
        return count_.load(std::memory_order_relaxed);
    }

    [[nodiscard]] bool empty() const
    {
        return count_.load(std::memory_order_relaxed) == 0;
    }

    [[nodiscard]] size_t approx_bytes() const
    {
        return bytes_.load(std::memory_order_relaxed);
    }

    void add_bytes(size_t n)
    {
        bytes_.fetch_add(n, std::memory_order_relaxed);
        if (source_ledger_) {
            source_ledger_->replace({}, {.logical_bytes = n});
        }
    }

    void sub_bytes(size_t n)
    {
        bytes_.fetch_sub(n, std::memory_order_relaxed);
        if (source_ledger_) {
            source_ledger_->replace({.logical_bytes = n}, {});
        }
    }

    // Public so MemTable can pass it to epoch_.retire() as the deleter.
    static void free_node(void *p);

  private:
    friend class MemTable;
    bool                                retire_step() noexcept;
    std::shared_ptr<SourceMemoryLedger> source_ledger_;
    void account_version(const CellVersion &version, const CellVersion *history, bool removed = false) noexcept;
    void account_node(const Node &node, const CellVersion &version) noexcept;
    friend class Cursor;

    [[nodiscard]] static Node  *alloc_node(uint32_t height, Slice key, const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] static Node  *construct_node(void *storage, uint32_t height, Slice key) noexcept;
    static void                 destroy_node(Node *node) noexcept;
    [[nodiscard]] PreparedWrite prepare_key(Slice key, uint64_t bound, const std::shared_ptr<AllocationBudget> &budget);
    [[nodiscard]] PreparedWrite prepare_cell(Slice key, buffer &&cell, uint64_t slot, uint8_t flags,
                                             AllocationLease payload, uint64_t bound,
                                             const std::shared_ptr<AllocationBudget> &budget);

    static uint32_t random_height();
    void            link_upper(Node *node);
    bool insert_version(Node &node, CellVersion::Owner &version, uint64_t bound, MutationStats *stats) noexcept;
    void prune_versions(Node &node, uint64_t bound, MutationStats *stats) noexcept;
    bool unlink_version(std::atomic<uintptr_t> &link, CellVersion &version, uintptr_t next,
                        MutationStats *stats) noexcept;

    Node *find_ge(Slice key, Node **prev, Node **successors = nullptr) const;

#ifdef CROWDB_TREE_TEST_UTIL
    void    *test_context_ = nullptr;
    TestHook test_hook_    = nullptr;
#endif
    Node                             *head_;
    CellVersion                      *retiring_version_ = nullptr;
    std::atomic<uint32_t>             max_height_{1};
    std::atomic<size_t>               count_{0};
    std::atomic<size_t>               bytes_{0};
    AllocationLease                   epoch_credit_;
    std::unique_ptr<EpochManager>     owned_epoch_;
    EpochManager                     *epoch_;
    std::shared_ptr<AllocationBudget> budget_;
    std::atomic<uint64_t>             history_count_{0};
    std::atomic<uint64_t>             history_bytes_{0};
    std::atomic<uint64_t>             descriptor_bytes_{0};
    std::atomic<uint64_t>             node_bytes_{0};
    std::atomic<uint64_t>             payload_bytes_{0};
};

} // namespace crowdb::tree
