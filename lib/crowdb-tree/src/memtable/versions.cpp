// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/memtable/skip_list.h"

#include <algorithm>

namespace crowdb::tree
{
static_assert(alignof(CellVersion) >= 4);

const CellVersion *CellVersion::version_at(uint64_t frontier) const
{
    for (const auto *version = this; version != nullptr; version = version->next()) {
        if (version->slot <= frontier) {
            return version;
        }
    }
    return nullptr;
}

bool ConcurrentSkipList::unlink_version(std::atomic<uintptr_t> &link, CellVersion &version, uintptr_t next,
                                        MutationStats *stats) noexcept
{
    auto expected  = reinterpret_cast<uintptr_t>(&version);
    auto successor = reinterpret_cast<uintptr_t>(CellVersion::pointer(next));
    if (!link.compare_exchange_strong(expected, successor, std::memory_order_acq_rel)) {
        return false;
    }
    account_version(version, &version, true);
    if (stats != nullptr) {
        ++stats->merged;
    }
    epoch_->defer(&version);
    return true;
}

void ConcurrentSkipList::prune_versions(Node &node, uint64_t bound, MutationStats *stats) noexcept
{
    auto effective = node.pruning_bound_.load(std::memory_order_seq_cst);
    while (effective < bound &&
           !node.pruning_bound_.compare_exchange_weak(effective, bound, std::memory_order_seq_cst)) {
    }
    for (;;) {
        effective    = node.pruning_bound_.load(std::memory_order_seq_cst);
        auto *anchor = CellVersion::pointer(node.versions_.load(std::memory_order_seq_cst));
        while (anchor != nullptr && anchor->slot > effective) {
            anchor = CellVersion::pointer(anchor->next_bits.load(std::memory_order_seq_cst));
        }
        if (anchor == nullptr) {
            return;
        }
        for (;;) {
            auto tail = anchor->next_bits.load(std::memory_order_acquire);
            if ((tail & CellVersion::kRemoved) != 0) {
                break;
            }
            auto *obsolete = CellVersion::pointer(tail);
            if (obsolete == nullptr) {
                // A delayed insert cannot reopen a trimmed suffix, even if it was empty.
                if (tail == CellVersion::kEnd ||
                    anchor->next_bits.compare_exchange_strong(tail, CellVersion::kEnd, std::memory_order_acq_rel)) {
                    return;
                }
                continue;
            }
            auto next = obsolete->next_bits.load(std::memory_order_acquire);
            // Mark the obsolete node's own link before detaching it. An inserter
            // paused at that link must retry against the live chain.
            if ((next & CellVersion::kRemoved) != 0 ||
                obsolete->next_bits.compare_exchange_strong(next, next | CellVersion::kRemoved,
                                                            std::memory_order_acq_rel)) {
                unlink_version(anchor->next_bits, *obsolete, next, stats);
            }
        }
    }
}

bool ConcurrentSkipList::insert_version(Node &node, CellVersion::Owner &version, uint64_t bound,
                                        MutationStats *stats) noexcept
{
    const auto slot = version->slot;
    for (;;) {
        auto        *link     = &node.versions_;
        CellVersion *previous = nullptr;
        auto         next     = link->load(std::memory_order_acquire);
        for (auto *current = CellVersion::pointer(next); current != nullptr; current = CellVersion::pointer(next)) {
            const auto following = current->next_bits.load(std::memory_order_acquire);
            if ((following & CellVersion::kRemoved) != 0) {
                if (!unlink_version(*link, *current, following, stats)) {
                    next = CellVersion::kRemoved;
                    break;
                }
                next = reinterpret_cast<uintptr_t>(CellVersion::pointer(following));
                continue;
            }
            if (current->slot <= slot) {
                if (current->slot == slot) {
                    return false;
                }
                break;
            }
            previous = current;
            link     = &current->next_bits;
            next     = following;
        }
        if (next == CellVersion::kRemoved) {
            continue;
        }

        const auto effective = std::max(bound, node.pruning_bound_.load(std::memory_order_acquire));
        if (previous != nullptr && previous->slot <= effective) {
            prune_versions(node, bound, stats);
            return false;
        }
        version->next_bits.store(next, std::memory_order_relaxed);
#ifdef CROWDB_TREE_TEST_UTIL
        if (test_hook_ != nullptr) {
            test_hook_(test_context_, PausePoint::kBeforeVersionCas, node.key_slice());
        }
#endif
        // Together with the bound load below and the pruner's ordered scan,
        // either this insertion sees the new bound or the scan sees this link.
        if (!link->compare_exchange_strong(next, reinterpret_cast<uintptr_t>(version.get()),
                                           std::memory_order_seq_cst)) {
            if (stats != nullptr) {
                ++stats->cas_retries;
            }
            continue;
        }

        auto       *published = version.release();
        const auto *history   = previous != nullptr ? published : CellVersion::pointer(next);
        account_version(*published, history);
        const auto current_bound = node.pruning_bound_.load(std::memory_order_seq_cst);
        if (bound > current_bound || slot <= current_bound) {
            prune_versions(node, bound, stats);
        }
        if (stats != nullptr) {
            stats->overwrite += static_cast<uint64_t>(previous == nullptr);
            if (previous != nullptr || slot > std::max(bound, current_bound)) {
                ++stats->keep;
                const bool closed =
                    stats->admission != nullptr && (stats->admission->load(std::memory_order_acquire) >> 63) != 0;
                if (closed) {
                    ++stats->keep_boundary;
                }
                else if (stats->gap) {
                    ++stats->keep_gap;
                }
                else {
                    ++stats->keep_pending;
                }
            }
        }
        return true;
    }
}

bool ConcurrentSkipList::upsert(Slice key, std::shared_ptr<const CellVersion> cv, uint64_t bound, MutationStats *stats,
                                std::shared_ptr<AllocationBudget> budget)
{
    if (epoch_->source_sealed()) {
        return false;
    }
    const auto slot = cv->slot;
    return commit_write(prepare_reference(key, std::move(cv), bound, budget ? budget : budget_), slot, stats);
}

void ConcurrentSkipList::prune(Slice key, uint64_t bound, MutationStats *stats)
{
    if (epoch_->source_sealed()) {
        return;
    }
    auto  guard = epoch_->enter();
    auto *node  = find_ge(key, nullptr);
    if (node != nullptr && node->key_slice() == key) {
        prune_versions(*node, bound, stats);
    }
}

#ifdef CROWDB_TREE_TEST_UTIL
VersionMemory ConcurrentSkipList::scan_memory_for_tests() const
{
    auto          guard = epoch_->enter();
    VersionMemory result;
    for (auto *node = head_->next(0); node != nullptr; node = node->next(0)) {
        auto *current = CellVersion::pointer(node->versions_.load(std::memory_order_acquire));
        for (auto *version = current; version != nullptr; version = version->next()) {
            const auto size = version->cell.size();
            result.descriptor_bytes += sizeof(CellVersion);
            result.payload_bytes += size;
            if (version != current) {
                ++result.history_count;
                result.history_bytes += size;
            }
        }
        result.node_bytes += Node::alloc_size(node->height_, node->key_len_);
    }
    return result;
}
#endif
} // namespace crowdb::tree
