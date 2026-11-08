// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_block_io.h"

#include "chunk/chunk_page_block.h"
#include "page_store/local/scheduled_page_store.h"

namespace crowdb::tree
{
void submit_page_block_read(AsyncPageStore &store, std::shared_ptr<const PageBlockRef> page_block, void *buffer,
                            size_t bytes, std::shared_ptr<AllocationBudget> metadata, AsyncCompletion complete)
{
    if (!page_block || !page_block->reloadable() || buffer == nullptr || bytes != page_block->length) {
        complete.complete(Status::invalid_argument("read requires one exact immutable page_block"));
        return;
    }
    if (page_block->storage == PageBlockStorage::kDurableBlob) {
        store.submit_read(page_block->address, buffer, bytes, std::move(complete));
        return;
    }
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(&store);
    if (scheduled == nullptr || !metadata) {
        complete.complete(Status::not_supported("Chunk reads require a paid backend lane"));
        return;
    }
    if (!page_block->chunk || page_block->chunk->read == nullptr ||
        !chunk_page_block_matches(*page_block->chunk, page_block->page_store_id, bytes)) {
        complete.complete(Status::corruption("read does not name one owned Chunk pack"));
        return;
    }
    page_block->chunk->read(*scheduled, page_block->chunk, {static_cast<uint8_t *>(buffer), bytes}, std::move(metadata),
                            std::move(complete));
}

void submit_chunk_write(AsyncPageStore &store, std::shared_ptr<ChunkPageBlock> page_block,
                        std::span<const uint8_t> input, std::shared_ptr<AllocationBudget> metadata,
                        AsyncCompletion complete)
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(&store);
    if (scheduled == nullptr || !page_block || page_block->write == nullptr || !metadata) {
        complete.complete(Status::not_supported("Chunk writes require a paid backend lane"));
        return;
    }
    const auto submit = page_block->write;
    submit(*scheduled, std::move(page_block), input, std::move(complete));
}
} // namespace crowdb::tree
