// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/page_block_ref.h"
#include "crowdb-tree/page_store/async_page_store.h"

namespace crowdb::tree
{
// All paths retain the callback's exact buffer owner through the page store tail.
// Chunk reads require a scheduled page store; they never fall back to byte offsets.
void submit_page_block_read(AsyncPageStore &store, std::shared_ptr<const PageBlockRef> page_block, void *buffer,
                            size_t bytes, std::shared_ptr<AllocationBudget> metadata, AsyncCompletion complete);
void submit_chunk_write(AsyncPageStore &store, std::shared_ptr<ChunkPageBlock> page_block,
                        std::span<const uint8_t> input, std::shared_ptr<AllocationBudget> metadata,
                        AsyncCompletion complete);
} // namespace crowdb::tree
