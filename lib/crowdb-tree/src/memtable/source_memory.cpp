// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/skip_list.h"

#include <limits>

namespace crowdb::tree
{
namespace
{
uint64_t sample(const std::atomic<uint64_t> &counter) noexcept
{
    const auto value = counter.load(std::memory_order_relaxed);
    // A later CAS may subtract its predecessor before the earlier CAS adds
    // it. Such an independent sample is zero, never a wrapped huge capacity.
    return value <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ? value : 0;
}
} // namespace

void ConcurrentSkipList::account_version(const CellVersion &version, const CellVersion *history, bool removed) noexcept
{
    const auto          bytes = version.cell.size();
    const VersionMemory change{.history_count    = history != nullptr ? 1U : 0U,
                               .history_bytes    = history != nullptr ? history->cell.size() : 0,
                               .descriptor_bytes = sizeof(CellVersion),
                               .payload_bytes    = bytes};
    // The deltas commute when an unlink accounts before its insertion.
    const uint64_t sign = removed ? uint64_t{0} - 1 : 1;
    if (bytes != 0) {
        bytes_.fetch_add(sign * bytes, std::memory_order_relaxed);
        payload_bytes_.fetch_add(sign * bytes, std::memory_order_relaxed);
    }
    if (history != nullptr) {
        history_count_.fetch_add(sign, std::memory_order_relaxed);
        if (change.history_bytes != 0) {
            history_bytes_.fetch_add(sign * change.history_bytes, std::memory_order_relaxed);
        }
    }
    descriptor_bytes_.fetch_add(sign * change.descriptor_bytes, std::memory_order_relaxed);
    if (source_ledger_) {
        const SourceMemoryStats delta{.logical_bytes = bytes, .memory = change};
        if (removed) {
            source_ledger_->replace(delta, {});
        }
        else {
            source_ledger_->replace({}, delta);
        }
    }
}

void ConcurrentSkipList::account_node(const Node &node, const CellVersion &version) noexcept
{
    node_bytes_.fetch_add(Node::alloc_size(node.height_, node.key_len_), std::memory_order_relaxed);
    if (node.key_len_ != 0) {
        bytes_.fetch_add(node.key_len_, std::memory_order_relaxed);
    }
    account_version(version, nullptr);
    if (source_ledger_) {
        source_ledger_->replace({}, {.records       = 1,
                                     .logical_bytes = node.key_len_,
                                     .memory        = {.node_bytes = Node::alloc_size(node.height_, node.key_len_)}});
    }
}

VersionMemory ConcurrentSkipList::memory() const
{
    return {.history_count    = sample(history_count_),
            .history_bytes    = sample(history_bytes_),
            .descriptor_bytes = sample(descriptor_bytes_),
            .node_bytes       = sample(node_bytes_),
            .payload_bytes    = sample(payload_bytes_)};
}
} // namespace crowdb::tree
