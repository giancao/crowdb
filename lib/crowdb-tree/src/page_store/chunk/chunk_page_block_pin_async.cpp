// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_page_block.h"
#include "chunk_physical_registry.h"

namespace crowdb::tree
{
namespace
{
struct PinPageBlock : std::enable_shared_from_this<PinPageBlock>
{
    std::shared_ptr<AllocationBudget>        metadata;
    std::shared_ptr<ChunkPageBlock>          page_block;
    std::shared_ptr<const ChunkPageBlock>   *output = nullptr;
    detail::ChunkStageId                     pin;
    detail::ChunkAllocationIdentity          receipt;
    std::optional<detail::ChunkStageRecord>  record;
    std::optional<detail::ChunkStageRecord> *verified_stage = nullptr;
    AsyncCompletion                          completion;

    void finish(Status status)
    {
        try {
            if (status.ok()) {
                status = page_block->domain->catalog->protection()->check_read_owner();
            }
            if (status.ok()) {
                page_block->physical_metrics = ChunkPhysicalRegistry::extent(page_block->physical);
                if (verified_stage != nullptr) {
                    *verified_stage = std::move(record);
                }
                *output = page_block;
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        auto done = std::exchange(completion, {});
        done.complete(std::move(status));
    }

    AsyncCompletion callback(AsyncCompleteFn fn)
    {
        return {.context = this, .complete_fn = fn, .owner = shared_from_this(), .allocations = metadata};
    }

    detail::ChunkTransportCompletion transport_callback(AsyncCompleteFn fn)
    {
        return {.complete_fn = fn, .context = this, .owner = shared_from_this(), .allocations = metadata};
    }

    static void loaded(void *raw, Status status)
    try {
        auto       &self   = *static_cast<PinPageBlock *>(raw);
        const auto &domain = *self.page_block->domain;
        if (status.ok()) {
            status = validate_chunk_pin_stage(domain, self.page_block->pack, self.record);
        }
        if (!status.ok()) {
            self.finish(std::move(status));
            return;
        }
        self.page_block->pin->arm();
        domain.catalog->protection()->submit_change_pin(domain.tree, self.page_block->pack.stage, self.pin, true,
                                                        self.callback(&PinPageBlock::pinned));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinPageBlock *>(raw)->finish(Status::resource_exhausted());
    }

    static void pinned(void *raw, Status status)
    try {
        auto       &self   = *static_cast<PinPageBlock *>(raw);
        const auto &domain = *self.page_block->domain;
        if (status.ok()) {
            status = domain.catalog->protection()->check_read_owner();
        }
        if (!status.ok() || !domain.physical_registry) {
            self.finish(std::move(status));
            return;
        }
        domain.physical_registry->submit_prepare(self.page_block->pack.stage, self.page_block->pack.chunk,
                                                 self.metadata, &self.page_block->physical, nullptr,
                                                 self.transport_callback(&PinPageBlock::prepared));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinPageBlock *>(raw)->finish(Status::resource_exhausted());
    }

    static void prepared(void *raw, Status status)
    try {
        auto &self = *static_cast<PinPageBlock *>(raw);
        if (!status.ok()) {
            self.finish(std::move(status));
            return;
        }
        self.page_block->domain->transport->submit_inspect_fixed_allocation(
            self.page_block->pack.chunk, self.page_block->pack.stage.owner_epoch, &self.receipt,
            self.transport_callback(&PinPageBlock::inspected));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinPageBlock *>(raw)->finish(Status::resource_exhausted());
    }

    static void inspected(void *raw, Status status)
    try {
        auto &self = *static_cast<PinPageBlock *>(raw);
        if (!status.ok()) {
            self.finish(std::move(status));
            return;
        }
        self.page_block->domain->physical_registry->submit_observe(
            self.page_block->physical, self.receipt, self.metadata, self.transport_callback(&PinPageBlock::observed));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinPageBlock *>(raw)->finish(Status::resource_exhausted());
    }

    static void observed(void *raw, Status status)
    {
        static_cast<PinPageBlock *>(raw)->finish(std::move(status));
    }
};
} // namespace

void submit_pin_chunk_page_block(const std::shared_ptr<const PageChunkStoreDomain> &domain,
                                 detail::ChunkFixedPackRef pack, detail::ChunkStageId pin,
                                 const std::shared_ptr<AllocationBudget> &metadata,
                                 std::shared_ptr<const ChunkPageBlock> *out, AsyncCompletion complete,
                                 std::optional<detail::ChunkStageRecord> *verified_stage)
try {
    if (!domain || !domain->supported() || !metadata || out == nullptr || *out || pin.owner_epoch != domain->epoch) {
        complete.complete(Status::invalid_argument());
        return;
    }
    auto state        = detail::reserved_shared<PinPageBlock>(metadata);
    state->metadata   = metadata;
    state->page_block = detail::reserved_shared<ChunkPageBlock>(metadata);
    initialize_chunk_page_block_io(*state->page_block);
    state->page_block->physical_budget = metadata;
    state->page_block->domain          = domain;
    state->page_block->pack            = pack;
    state->output                      = out;
    state->verified_stage              = verified_stage;
    state->pin                         = pin;
    // The caller still owns completion if catalog submission throws.
    state->completion = complete; // NOLINT(performance-use-std-move)
    auto status = domain->catalog->pin_release_queue()->prepare(metadata, domain->catalog, domain->tree, pack.stage,
                                                                pin, &state->page_block->pin);
    if (status.ok() && !chunk_page_block_matches(*state->page_block, domain->page_store_id, pack.length)) {
        status = Status::corruption("checkpoint pack identity is invalid");
    }
    if (status.ok()) {
        status = domain->catalog->protection()->check_read_owner();
    }
    if (!status.ok()) {
        state->finish(std::move(status));
        return;
    }
    domain->catalog->submit_load_stage(domain->tree, pack.stage, &state->record,
                                       state->callback(&PinPageBlock::loaded));
}
catch (const std::bad_alloc &) {
    complete.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree
