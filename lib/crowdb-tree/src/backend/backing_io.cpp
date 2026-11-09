// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backing_io.h"

#include "backend/local/scheduled_page_store.h"
#include "chunk/chunk_backing.h"

namespace crowdb::tree
{
void submit_backing_read(AsyncPageStore &store, std::shared_ptr<const BackingRef> backing, void *buffer, size_t bytes,
                         std::shared_ptr<AllocationBudget> metadata, AsyncCompletion complete)
{
    if (!backing || !backing->reloadable() || buffer == nullptr || bytes != backing->length) {
        complete.complete(Status::invalid_argument("read requires one exact immutable backing"));
        return;
    }
    if (backing->storage == BackingStorage::kDurableBlob) {
        store.submit_read(backing->address, buffer, bytes, std::move(complete));
        return;
    }
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(&store);
    if (scheduled == nullptr || !metadata) {
        complete.complete(Status::not_supported("Chunk reads require a paid backend lane"));
        return;
    }
    if (!backing->chunk || backing->chunk->read == nullptr ||
        !chunk_backing_matches(*backing->chunk, backing->backend_id, bytes)) {
        complete.complete(Status::corruption("read does not name one owned Chunk pack"));
        return;
    }
    backing->chunk->read(*scheduled, backing->chunk, {static_cast<uint8_t *>(buffer), bytes}, std::move(metadata),
                         std::move(complete));
}

void submit_chunk_write(AsyncPageStore &store, std::shared_ptr<ChunkPackBacking> backing,
                        std::span<const uint8_t> input, std::shared_ptr<AllocationBudget> metadata,
                        AsyncCompletion complete)
{
    auto *scheduled = dynamic_cast<ScheduledPageStore *>(&store);
    if (scheduled == nullptr || !backing || backing->write == nullptr || !metadata) {
        complete.complete(Status::not_supported("Chunk writes require a paid backend lane"));
        return;
    }
    const auto submit = backing->write;
    submit(*scheduled, std::move(backing), input, std::move(complete));
}
} // namespace crowdb::tree
