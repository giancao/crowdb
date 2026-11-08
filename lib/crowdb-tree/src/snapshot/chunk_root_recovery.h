// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/overflow_page_block.h"
#include "page_store/chunk/chunk_runtime_ids.h"
#include "snapshot/captured_write.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree
{
struct ChunkRecoveryLimits
{
    size_t max_frame_bytes = 1024 * 1024;
    size_t max_blob_bytes  = 2 * 1024 * 1024;
};

// Recover ordered mapping segments (PageIndex packs) through async reads, then verify their
// dependencies. A successful result still requires graph validation.
class ChunkRootRecovery : public std::enable_shared_from_this<ChunkRootRecovery>
{
    struct ConstructionKey
    {
    };

  public:
    ChunkRootRecovery(ConstructionKey, std::shared_ptr<AllocationBudget> metadata,
                      std::shared_ptr<AllocationBudget> window);
    static Status create(ChunkRootManifest root, std::shared_ptr<const PageChunkStoreDomain> domain,
                         std::shared_ptr<detail::ChunkRuntimeIds> ids, ChunkRecoveryLimits limits,
                         std::shared_ptr<AllocationBudget> metadata, std::shared_ptr<AllocationBudget> window,
                         std::shared_ptr<ChunkRootRecovery> *out);
    void          submit_step(bool stopped, std::shared_ptr<ScheduledPageStore> cpu, AsyncCompletion complete) noexcept;

    [[nodiscard]] bool done() const noexcept
    {
        return done_;
    }

    [[nodiscard]] const Status &status() const noexcept
    {
        return status_;
    }

    [[nodiscard]] const std::shared_ptr<CapturedDurablePages> &pages() const noexcept
    {
        return pages_;
    }

    [[nodiscard]] size_t max_frame_bytes() const noexcept
    {
        return max_frame_bytes_;
    }

    [[nodiscard]] size_t max_blob_bytes() const noexcept
    {
        return max_blob_bytes_;
    }

    std::shared_ptr<const ChunkRootManifest> take_manifest();

  private:
    struct FrameReads;

    struct LeafProtection
    {
        std::shared_ptr<const void>                 primary;
        std::shared_ptr<const OverflowPageBlockSet> overflow;
    };

    Status finish_indexes();
    Status index_loaded();
    Status summaries_loaded();
    Status prepare_frame(ChunkPageEntry *out, size_t *offset, uint64_t left, std::optional<uint64_t> previous) const;
    Status frame_loaded(const ChunkPageEntry &record, std::shared_ptr<const ChunkPageBlock> pack,
                        std::span<const uint8_t> blob);
    Status validate_frame(DurableCapturedFrame &descriptor, std::span<const uint8_t> words,
                          std::span<const uint8_t> blob);
    Status dependencies();
    Status next_pin(detail::ChunkStageId *out);
    Status finish(Status status);
    void   complete_step(Status status);
    void   read_pinned(Status status);
    void   read_completed(Status status);
    AsyncCompletion                             async_reply(AsyncCompleteFn callback);
    std::shared_ptr<ScheduledPageStore>         cpu_;
    AsyncCompletion                             completion_;
    std::shared_ptr<const ChunkPageBlock>       async_pack_;
    std::shared_ptr<FrameReads>                 frame_reads_;
    std::shared_ptr<AllocationBudget>           metadata_;
    std::shared_ptr<AllocationBudget>           window_;
    std::shared_ptr<const PageChunkStoreDomain> domain_;
    std::shared_ptr<detail::ChunkRuntimeIds>    ids_;
    ChunkRecoveryLimits                         limits_;

    ChunkRootManifest                                         root_;
    std::shared_ptr<CapturedDurablePages>                     pages_;
    ChunkMetadataBytes                                        index_bytes_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> raw_;
    size_t                                                    max_frame_bytes_  = 0;
    size_t                                                    max_blob_bytes_   = 0;
    size_t                                                    index_            = 0;
    size_t                                                    offset_           = 0;
    uint64_t                                                  left_             = 0;
    uint64_t                                                  last_pid_         = 0;
    uint64_t                                                  references_       = 0;
    uint64_t                                                  steps_            = 0;
    bool                                                      have_pid_         = false;
    bool                                                      dependency_phase_ = false;
    bool                                                      summaries_ready_  = false;
    bool                                                      done_             = false;
    Status                                                    status_;
    CapturedDurablePages::Frames::iterator                    leaf_;
    size_t                                                    reference_   = 0;
    uint64_t                                                  pid_         = kInvalidPageId;
    uint64_t                                                  remaining_   = 0;
    uint64_t                                                  chain_steps_ = 0;
    std::shared_ptr<OverflowPageBlockSet>                     overflow_;
};
} // namespace crowdb::tree
