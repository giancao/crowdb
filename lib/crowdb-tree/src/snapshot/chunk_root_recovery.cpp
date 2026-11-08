// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_recovery.h"

namespace crowdb::tree
{
ChunkRootRecovery::ChunkRootRecovery(ConstructionKey /*unused*/, std::shared_ptr<AllocationBudget> metadata,
                                     std::shared_ptr<AllocationBudget> window)
    : metadata_(std::move(metadata)),
      window_(std::move(window)),
      root_(metadata_),
      index_bytes_(detail::ReservationAllocator<uint8_t>(window_)),
      raw_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(window_))
{
}

Status ChunkRootRecovery::create(ChunkRootManifest root, std::shared_ptr<const PageChunkStoreDomain> domain,
                                 std::shared_ptr<detail::ChunkRuntimeIds> ids, ChunkRecoveryLimits limits,
                                 std::shared_ptr<AllocationBudget> metadata, std::shared_ptr<AllocationBudget> window,
                                 std::shared_ptr<ChunkRootRecovery> *out)
try {
    if (!domain || !domain->supported() || !ids || !metadata || !window || out == nullptr || *out ||
        root.identity.tree != domain->tree || root.identity.root_generation == 0 ||
        limits.max_frame_bytes <= kFrameHeaderSize + kFrameTrailerSize || limits.max_blob_bytes < kDurableBlobHeader) {
        return Status::invalid_argument();
    }
    auto result          = detail::reserved_shared<ChunkRootRecovery>(metadata, ConstructionKey{}, metadata, window);
    result->domain_      = std::move(domain);
    result->ids_         = std::move(ids);
    result->limits_      = limits;
    result->root_        = std::move(root);
    result->pages_       = detail::reserved_shared<CapturedDurablePages>(metadata, metadata);
    const auto &identity = result->root_.identity;
    auto       &pages    = *result->pages_;
    pages.root_page_id   = identity.root_page_id;
    pages.next_page_id   = identity.next_page_id;
    pages.coverage       = identity.coverage;
    pages.root_summary   = result->root_.root_summary;
    pages.generation     = identity.page_generation;
    pages.page_store_id  = result->domain_->page_store_id;
    *out                 = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status ChunkRootRecovery::next_pin(detail::ChunkStageId *out)
{
    detail::ChunkId unused;
    return ids_->next(PagePurpose::kBtreePage, out, &unused);
}

std::shared_ptr<const ChunkRootManifest> ChunkRootRecovery::take_manifest()
{
    if (!done_ || !status_.ok()) {
        return {};
    }
    return detail::reserved_shared<ChunkRootManifest>(metadata_, std::move(root_));
}

Status ChunkRootRecovery::finish(Status status)
{
    status_ = std::move(status);
    done_   = true;
    if (!status_.ok()) {
        frame_reads_.reset();
        pages_.reset();
    }
    return status_;
}

} // namespace crowdb::tree
