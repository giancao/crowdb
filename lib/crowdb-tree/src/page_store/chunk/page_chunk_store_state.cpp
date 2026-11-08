// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_store/chunk/page_chunk_store_state.h"

#include "page_store/chunk/chunk_frame.h"
#include "page_store/chunk/chunk_physical_registry.h"
#include "snapshot/chunk_root_commit.h"

namespace crowdb::tree
{
PageChunkStoreState::~PageChunkStoreState()
{
    close_writer();
    if (owns_writer_domain) {
        facade->release_runtime_domain();
    }
}

PageBlockStatsSampler PageChunkStoreState::page_block_sampler() const noexcept
{
    if (!domain || !domain->physical_registry) {
        return {};
    }
    const auto &registry = domain->physical_registry;
    return {.scope_id = registry->page_block_stats().scope_id, .metrics = registry->page_block_metrics()};
}

void PageChunkStoreState::close_writer() noexcept
{
    writer_session.reset();
}

void PageChunkStoreState::recovered(const ChunkRootIdentity                 &identity,
                                    std::shared_ptr<const ChunkRootManifest> root) noexcept
{
    committed_root.store(std::move(root), std::memory_order_release);
    durable_coverage.store(identity.coverage, std::memory_order_release);
    snapshot_sequence.store(identity.snapshot_sequence, std::memory_order_release);
    root_generation.store(identity.root_generation, std::memory_order_release);
    facade->committed(identity.root_generation, identity.wal_replay_offset);
}

void PageChunkStoreState::completed(const ChunkRootCommit &operation) noexcept
{
    if (operation.committed()) {
        recovered(operation.identity(), operation.manifest());
    }
    if (operation.needs_recovery() || !operation.done()) {
        needs_recovery.store(true, std::memory_order_release);
    }
}

ChunkRootIdentity PageChunkStoreState::next_identity(const TreeRoot &view) const noexcept
{
    return {.tree              = domain->tree,
            .root_generation   = root_generation.load(std::memory_order_acquire) + 1,
            .owner_epoch       = domain->epoch,
            .snapshot_sequence = snapshot_sequence.load(std::memory_order_acquire) + 1,
            .page_generation   = view.generation,
            .coverage          = view.coverage,
            .gc_floor          = view.gc_floor,
            .root_page_id      = view.root_page_id,
            .next_page_id      = view.next_page_id,
            .wal_replay_offset = facade->requested_wal_offset()};
}

PageWriteOperation::ReserveExtent PageChunkStoreState::reserve(const std::shared_ptr<AllocationBudget> &metadata) const
{
    return [domain = domain, ids = ids, metadata,
            capacity = facade->options().max_chunk_bytes](uint64_t bytes, PageWriteOperation::ExtentComplete complete) {
        if (bytes == 0 || bytes > UINT32_MAX || detail::chunk_framed_length(bytes) > capacity) {
            complete(Status::resource_exhausted(), {});
            return;
        }
        detail::ChunkStageId            stage;
        detail::ChunkId                 chunk;
        auto                            status = ids->next(PagePurpose::kBtreePage, &stage, &chunk);
        std::shared_ptr<ChunkPageBlock> page_block;
        if (status.ok()) {
            status = prepare_chunk_page_block(domain, stage, chunk, static_cast<uint32_t>(bytes),
                                              PagePurpose::kBtreePage, metadata, &page_block);
        }
        if (!status.ok()) {
            complete(std::move(status), {});
            return;
        }
        complete(Status::Ok(), {.page_store_id    = domain->page_store_id,
                                .address          = 0,
                                .length           = bytes,
                                .content_id       = stage.operation_low,
                                .generation       = domain->epoch,
                                .owner            = page_block,
                                .physical_metrics = {},
                                .chunk            = page_block});
    };
}
} // namespace crowdb::tree
