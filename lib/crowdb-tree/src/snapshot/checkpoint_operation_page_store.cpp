// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/checkpoint_operation_internal.h"

namespace crowdb::tree
{
bool CheckpointOperation::State::page_store_needs_recovery() const noexcept
{
    return chunk ? chunk->needs_recovery.load(std::memory_order_acquire)
                 : local_page_store->checkpoints->needs_recovery();
}

uint64_t CheckpointOperation::State::page_store_identity() const noexcept
{
    return chunk ? chunk->page_store_identity() : local_page_store->ledger->limits().page_store_id;
}

std::shared_ptr<const CapturedDurablePages> CheckpointOperation::State::inherited_pages() const
{
    return local_page_store ? local_page_store->inherited : nullptr;
}

void CheckpointOperation::State::begin_chunk_checkpoint()
{
    auto status = chunk->prepare_checkpoint({.pages      = written_pages,
                                             .view       = *view,
                                             .wal_offset = chunk_wal_offset,
                                             .store      = store,
                                             .runtime    = runtime,
                                             .lane       = lane,
                                             .budget     = budget,
                                             .owner      = owner},
                                            &chunk_checkpoint);
    if (!status.ok()) {
        reconcile(std::move(status));
        return;
    }
    write.reset();
    begin_child(chunk_checkpoint->sender(), Stage::Checkpoint);
}
} // namespace crowdb::tree
