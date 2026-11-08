// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Private Chunk root catalog contracts.

#pragma once

#include "chunk_protection.h"
#include "chunk_stage.h"
#include "chunk_transport.h"
#include "crowdb-tree/c_api.h"
#include "crowdb-tree/chunk_protection_async_c_api.h"
#include "crowdb-tree/chunk_protection_c_api.h"
#include "crowdb-tree/chunk_stage_async_c_api.h"
#include "crowdb-tree/chunk_stage_c_api.h"
#include "crowdb-tree/page_store/async_page_store.h"
#include "crowdb-tree/page_store/page_store.h"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace crowdb::tree::detail
{

class ChunkPinReleaseQueue;

class RootCatalog : public std::enable_shared_from_this<RootCatalog>
{
  public:
    virtual ~RootCatalog() = default;

    // One catalog callback owner is one writer incarnation. A new open after
    // close must claim a fresh catalog; delayed quiescence cannot close it.
    bool claim_runtime_writer() noexcept
    {
        bool expected = false;
        return runtime_writer_claimed_.compare_exchange_strong(expected, true, std::memory_order_acq_rel);
    }

    [[nodiscard]] virtual std::shared_ptr<ChunkPinReleaseQueue> pin_release_queue() const noexcept
    {
        return {};
    }

    virtual Status bind_pin_release_queue(std::shared_ptr<ChunkPinReleaseQueue> /*unused*/)
    {
        return Status::not_supported();
    }

    [[nodiscard]] virtual ChunkProtectionCatalog *protection() noexcept
    {
        return nullptr;
    }

    [[nodiscard]] virtual bool stage_schema_supported() const noexcept
    {
        return false;
    }

    virtual Status load_stage(uint64_t /*tree_id*/, ChunkStageId /*id*/,
                              std::optional<ChunkStageRecord> * /*out*/) const
    {
        return Status::not_supported();
    }

    virtual Status compare_stage(uint64_t /*tree_id*/, uint64_t /*owner_epoch*/,
                                 const std::optional<ChunkStageRecord> & /*expected*/,
                                 const std::optional<ChunkStageRecord> & /*next*/)
    {
        return Status::not_supported();
    }

    virtual Status list_stages(uint64_t /*tree_id*/, std::optional<ChunkStageId> /*after*/, uint32_t /*limit*/,
                               std::vector<ChunkStageRecord> * /*out*/) const
    {
        return Status::not_supported();
    }

    // Returns one bounded record using the caller's actual metadata allocator.
    virtual Status next_stage(uint64_t /*tree_id*/, std::optional<ChunkStageId> /*after*/,
                              const std::shared_ptr<AllocationBudget> & /*budget*/,
                              std::optional<ChunkStageRecord> * /*out*/) const
    {
        return Status::not_supported();
    }

    virtual void submit_load_stage(uint64_t tree, ChunkStageId id, std::optional<ChunkStageRecord> *out,
                                   AsyncCompletion completion) const
    {
        completion.complete(load_stage(tree, id, out));
    }

    virtual void submit_compare_stage(uint64_t tree, uint64_t epoch, const std::optional<ChunkStageRecord> &expected,
                                      const std::optional<ChunkStageRecord> &next, AsyncCompletion completion)
    {
        completion.complete(compare_stage(tree, epoch, expected, next));
    }

    virtual void submit_list_stages(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                                    std::vector<ChunkStageRecord> *out, AsyncCompletion completion) const
    {
        completion.complete(list_stages(tree, after, limit, out));
    }

    virtual Status pin_generation(uint64_t /*tree_id*/, uint64_t /*transition_high*/, uint64_t /*transition_low*/,
                                  uint64_t /*generation*/)
    {
        return Status::not_supported();
    }

    virtual Status unpin_generation(uint64_t /*tree_id*/, uint64_t /*transition_high*/, uint64_t /*transition_low*/)
    {
        return Status::not_supported();
    }

    virtual uint64_t reclaim_before(uint64_t /*tree_id*/, uint64_t /*generation*/)
    {
        return 0;
    }

  private:
    std::atomic<bool> runtime_writer_claimed_{false};
};

class CallbackRootCatalog final : public RootCatalog, public ChunkProtectionCatalog
{
  public:
    [[nodiscard]] std::shared_ptr<ChunkPinReleaseQueue> pin_release_queue() const noexcept override
    {
        return release_queue_;
    }

    // Host initialization only, before this catalog is exposed to a Tree.
    Status bind_pin_release_queue(std::shared_ptr<ChunkPinReleaseQueue> queue) override
    {
        if (!queue || (release_queue_ && release_queue_ != queue)) {
            return Status::invalid_argument();
        }
        release_queue_ = std::move(queue);
        return Status::Ok();
    }

    CallbackRootCatalog(ct_root_catalog_callbacks callbacks, void *context);
    CallbackRootCatalog(ct_root_catalog_callbacks, ct_root_catalog_staging_callbacks, void *);
    CallbackRootCatalog(ct_root_catalog_callbacks, ct_root_catalog_staging_callbacks,
                        ct_root_catalog_protection_callbacks, void *);
    [[nodiscard]] ChunkProtectionCatalog *protection() noexcept override;
    [[nodiscard]] Status                  check_read_owner() const override;
    Status                                writer_quiesced(uint64_t tree, uint64_t epoch) override;
    Status load_root_bytes(uint64_t tree, uint64_t generation, std::vector<uint8_t> *out) const override;
    Status load_root_owned(uint64_t tree, uint64_t generation, ChunkRootBytes *out) const override;
    void   submit_load_root_owned(uint64_t tree, uint64_t generation, ChunkRootBytes *out,
                                  AsyncCompletion complete) const override;
    Status begin_candidate(uint64_t tree, const ChunkManifestCandidate & /*unused*/) override;
    Status load_candidate(uint64_t                             tree, ChunkStageId /*unused*/,
                          std::optional<ChunkCandidateStatus> *out) const override;
    Status list_candidates(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                           std::vector<ChunkCandidateStatus> *out) const override;
    Status prepare_references(uint64_t tree, ChunkStageId /*unused*/, uint64_t window,
                              std::span<const ChunkStageId> /*unused*/) override;
    Status finish_candidate(uint64_t tree, ChunkStageId /*unused*/, uint32_t action) override;
    Status release_references(uint64_t tree, ChunkStageId /*unused*/, uint64_t window) override;
    Status change_pin(uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool acquire) override;
    Status prune_pins(uint64_t tree, ChunkStageId /*unused*/, std::optional<ChunkStageId> after, uint32_t limit,
                      ChunkPinProgress *out) override;
    [[nodiscard]] bool stage_schema_supported() const noexcept override;
    Status load_stage(uint64_t tree_id, ChunkStageId id, std::optional<ChunkStageRecord> *out) const override;
    Status compare_stage(uint64_t tree_id, uint64_t owner_epoch, const std::optional<ChunkStageRecord> &expected,
                         const std::optional<ChunkStageRecord> &next) override;
    Status list_stages(uint64_t tree_id, std::optional<ChunkStageId> after, uint32_t limit,
                       std::vector<ChunkStageRecord> *out) const override;
    Status next_stage(uint64_t tree_id, std::optional<ChunkStageId> after,
                      const std::shared_ptr<AllocationBudget> &budget,
                      std::optional<ChunkStageRecord>         *out) const override;
    Status next_candidate(uint64_t tree, std::optional<ChunkStageId> after,
                          std::optional<ChunkCandidateStatus> *out) const override;

    Status bind_async_staging(const ct_root_catalog_staging_async_callbacks &callbacks);
    void   submit_load_stage(uint64_t tree, ChunkStageId id, std::optional<ChunkStageRecord> *out,
                             AsyncCompletion completion) const override;
    void   submit_compare_stage(uint64_t tree, uint64_t epoch, const std::optional<ChunkStageRecord> &expected,
                                const std::optional<ChunkStageRecord> &next, AsyncCompletion completion) override;
    void   submit_list_stages(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                              std::vector<ChunkStageRecord> *out, AsyncCompletion completion) const override;

    Status bind_async_protection(const ct_root_catalog_protection_async_callbacks &callbacks);
    void   submit_writer_quiesced(uint64_t tree, uint64_t epoch, AsyncCompletion done) override;
    void submit_begin_candidate(uint64_t tree, const ChunkManifestCandidate &candidate, AsyncCompletion done) override;
    void submit_load_candidate(uint64_t tree, ChunkStageId id, std::optional<ChunkCandidateStatus> *out,
                               AsyncCompletion done) const override;
    void submit_list_candidates(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                                std::vector<ChunkCandidateStatus> *out, AsyncCompletion done) const override;
    void submit_prepare_references(uint64_t tree, ChunkStageId id, uint64_t window,
                                   std::span<const ChunkStageId> references, AsyncCompletion done) override;
    void submit_finish_candidate(uint64_t tree, ChunkStageId id, uint32_t action, AsyncCompletion done) override;
    void submit_release_references(uint64_t tree, ChunkStageId id, uint64_t window, AsyncCompletion done) override;
    void submit_change_pin(uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool acquire,
                           AsyncCompletion done) override;
    void submit_prune_pins(uint64_t tree, ChunkStageId stage, std::optional<ChunkStageId> after, uint32_t limit,
                           ChunkPinProgress *out, AsyncCompletion done) override;

    ~CallbackRootCatalog() override;

    uint64_t reclaim_before(uint64_t tree_id, uint64_t generation) override;
    Status   pin_generation(uint64_t tree_id, uint64_t transition_high, uint64_t transition_low,
                            uint64_t generation) override;
    Status   unpin_generation(uint64_t tree_id, uint64_t transition_high, uint64_t transition_low) override;

  private:
    std::shared_ptr<ChunkPinReleaseQueue>      release_queue_;
    ct_root_catalog_protection_callbacks       protection_{};
    ct_root_catalog_protection_async_callbacks async_protection_{};
    ct_root_catalog_staging_callbacks          staging_{};
    ct_root_catalog_staging_async_callbacks    async_staging_{};
    ct_root_catalog_callbacks                  callbacks_;
    void                                      *context_;
};

} // namespace crowdb::tree::detail
