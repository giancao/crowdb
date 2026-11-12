// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/source_stats.h"

#include <limits>

namespace crowdb::tree
{
namespace
{
uint64_t sample(const std::atomic<uint64_t> &value) noexcept
{
    const auto count = value.load(std::memory_order_relaxed);
    return count <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) ? count : 0;
}
} // namespace

SourceMemoryStats &SourceMemoryStats::operator+=(const SourceMemoryStats &other) noexcept
{
    tables += other.tables;
    frozen_tables += other.frozen_tables;
    writers += other.writers;
    freezing_writers += other.freezing_writers;
    frozen_retained_bytes += other.frozen_retained_bytes;
    records += other.records;
    logical_bytes += other.logical_bytes;
    memory.history_count += other.memory.history_count;
    memory.history_bytes += other.memory.history_bytes;
    memory.descriptor_bytes += other.memory.descriptor_bytes;
    memory.node_bytes += other.memory.node_bytes;
    memory.payload_bytes += other.memory.payload_bytes;
    known = known && other.known;
    return *this;
}

SourceMemoryStats SourceMemoryLedger::snapshot() const noexcept
{
    return {
        .tables                = sample(tables_),
        .frozen_tables         = sample(frozen_tables_),
        .writers               = sample(writers_),
        .freezing_writers      = sample(freezing_writers_),
        .frozen_retained_bytes = sample(frozen_retained_bytes_),
        .records               = sample(records_),
        .logical_bytes         = sample(logical_bytes_),
        .memory =
            {
                     .history_count    = sample(history_count_),
                     .history_bytes    = sample(history_bytes_),
                     .descriptor_bytes = sample(descriptor_bytes_),
                     .node_bytes       = sample(node_bytes_),
                     .payload_bytes    = sample(payload_bytes_),
                     },
        .known = true,
    };
}

void SourceMemoryLedger::replace(const SourceMemoryStats &before, const SourceMemoryStats &after) noexcept
{
    const auto update = [](std::atomic<uint64_t> &counter, uint64_t previous, uint64_t current) {
        if (current != previous) {
            counter.fetch_add(current - previous, std::memory_order_relaxed);
        }
    };
    update(tables_, before.tables, after.tables);
    update(frozen_tables_, before.frozen_tables, after.frozen_tables);
    update(writers_, before.writers, after.writers);
    update(freezing_writers_, before.freezing_writers, after.freezing_writers);
    update(frozen_retained_bytes_, before.frozen_retained_bytes, after.frozen_retained_bytes);
    update(records_, before.records, after.records);
    update(logical_bytes_, before.logical_bytes, after.logical_bytes);
    update(history_count_, before.memory.history_count, after.memory.history_count);
    update(history_bytes_, before.memory.history_bytes, after.memory.history_bytes);
    update(descriptor_bytes_, before.memory.descriptor_bytes, after.memory.descriptor_bytes);
    update(node_bytes_, before.memory.node_bytes, after.memory.node_bytes);
    update(payload_bytes_, before.memory.payload_bytes, after.memory.payload_bytes);
}

void SourceMemoryLedger::writer_enter() noexcept
{
    writers_.fetch_add(1, std::memory_order_relaxed);
}

void SourceMemoryLedger::writer_leave(bool closed) noexcept
{
    writers_.fetch_sub(1, std::memory_order_relaxed);
    if (closed) {
        freezing_writers_.fetch_sub(1, std::memory_order_relaxed);
    }
}

void SourceMemoryLedger::table_closed(uint64_t writers) noexcept
{
    frozen_tables_.fetch_add(1, std::memory_order_relaxed);
    freezing_writers_.fetch_add(writers, std::memory_order_relaxed);
}

void SourceMemoryLedger::table_frozen(uint64_t bytes) noexcept
{
    frozen_retained_bytes_.fetch_add(bytes, std::memory_order_relaxed);
}
} // namespace crowdb::tree
