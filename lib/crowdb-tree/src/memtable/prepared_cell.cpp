// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/memtable/memtable.h"

namespace crowdb::tree
{
MemTable::PreparedWrite MemTable::prepare_write(Slice key, buffer &&cell, uint8_t flags, AllocationLease payload,
                                                uint64_t bound, const std::shared_ptr<AllocationBudget> &budget)
{
    return list_.prepare_write(key, std::move(cell), flags, std::move(payload), bound, budget);
}

MemTable::PreparedWrite MemTable::prepare_reference(Slice key, std::shared_ptr<const CellVersion> cell, uint64_t bound,
                                                    const std::shared_ptr<AllocationBudget> &budget)
{
    return list_.prepare_reference(key, std::move(cell), bound, budget);
}

bool MemTable::commit_write(PreparedWrite write, uint64_t slot) noexcept
{
    if (slot <= durable_floor_.load(std::memory_order_relaxed) && !allow_old_slots_.load(std::memory_order_relaxed)) {
        return false;
    }
    const bool changed = list_.commit_write(std::move(write), slot);
    if (changed) {
        update_slot_range(slot);
    }
    return changed;
}
} // namespace crowdb::tree
