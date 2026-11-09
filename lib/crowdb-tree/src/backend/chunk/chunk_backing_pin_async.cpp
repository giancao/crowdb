// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_backing.h"
#include "chunk_physical_registry.h"

namespace crowdb::tree
{
namespace
{
struct PinBacking : std::enable_shared_from_this<PinBacking>
{
    std::shared_ptr<AllocationBudget>        metadata;
    std::shared_ptr<ChunkPackBacking>        backing;
    std::shared_ptr<const ChunkPackBacking> *output = nullptr;
    detail::ChunkStageId                     pin;
    detail::ChunkAllocationIdentity          receipt;
    std::optional<detail::ChunkStageRecord>  record;
    std::optional<detail::ChunkStageRecord> *verified_stage = nullptr;
    AsyncCompletion                          completion;

    void finish(Status status)
    {
        try {
            if (status.ok()) {
                status = backing->domain->catalog->protection()->check_read_owner();
            }
            if (status.ok()) {
                backing->physical_metrics = ChunkPhysicalRegistry::extent(backing->physical);
                if (verified_stage != nullptr) {
                    *verified_stage = std::move(record);
                }
                *output = backing;
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
        auto       &self   = *static_cast<PinBacking *>(raw);
        const auto &domain = *self.backing->domain;
        if (status.ok()) {
            status = validate_chunk_pin_stage(domain, self.backing->pack, self.record);
        }
        if (!status.ok()) {
            self.finish(std::move(status));
            return;
        }
        self.backing->pin->arm();
        domain.catalog->protection()->submit_change_pin(domain.tree, self.backing->pack.stage, self.pin, true,
                                                        self.callback(&PinBacking::pinned));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinBacking *>(raw)->finish(Status::resource_exhausted());
    }

    static void pinned(void *raw, Status status)
    try {
        auto       &self   = *static_cast<PinBacking *>(raw);
        const auto &domain = *self.backing->domain;
        if (status.ok()) {
            status = domain.catalog->protection()->check_read_owner();
        }
        if (!status.ok() || !domain.physical_registry) {
            self.finish(std::move(status));
            return;
        }
        domain.physical_registry->submit_prepare(self.backing->pack.stage, self.backing->pack.chunk, self.metadata,
                                                 &self.backing->physical, nullptr,
                                                 self.transport_callback(&PinBacking::prepared));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinBacking *>(raw)->finish(Status::resource_exhausted());
    }

    static void prepared(void *raw, Status status)
    try {
        auto &self = *static_cast<PinBacking *>(raw);
        if (!status.ok()) {
            self.finish(std::move(status));
            return;
        }
        self.backing->domain->transport->submit_inspect_fixed_allocation(
            self.backing->pack.chunk, self.backing->pack.stage.owner_epoch, &self.receipt,
            self.transport_callback(&PinBacking::inspected));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinBacking *>(raw)->finish(Status::resource_exhausted());
    }

    static void inspected(void *raw, Status status)
    try {
        auto &self = *static_cast<PinBacking *>(raw);
        if (!status.ok()) {
            self.finish(std::move(status));
            return;
        }
        self.backing->domain->physical_registry->submit_observe(self.backing->physical, self.receipt, self.metadata,
                                                                self.transport_callback(&PinBacking::observed));
    }
    catch (const std::bad_alloc &) {
        static_cast<PinBacking *>(raw)->finish(Status::resource_exhausted());
    }

    static void observed(void *raw, Status status)
    {
        static_cast<PinBacking *>(raw)->finish(std::move(status));
    }
};
} // namespace

void submit_pin_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkFixedPackRef pack,
                              detail::ChunkStageId pin, const std::shared_ptr<AllocationBudget> &metadata,
                              std::shared_ptr<const ChunkPackBacking> *out, AsyncCompletion complete,
                              std::optional<detail::ChunkStageRecord> *verified_stage)
try {
    if (!domain || !domain->supported() || !metadata || out == nullptr || *out || pin.owner_epoch != domain->epoch) {
        complete.complete(Status::invalid_argument());
        return;
    }
    auto state      = detail::reserved_shared<PinBacking>(metadata);
    state->metadata = metadata;
    state->backing  = detail::reserved_shared<ChunkPackBacking>(metadata);
    initialize_chunk_backing_io(*state->backing);
    state->backing->physical_budget = metadata;
    state->backing->domain          = domain;
    state->backing->pack            = pack;
    state->output                   = out;
    state->verified_stage           = verified_stage;
    state->pin                      = pin;
    // The caller still owns completion if catalog submission throws.
    state->completion = complete; // NOLINT(performance-use-std-move)
    auto status = domain->catalog->pin_release_queue()->prepare(metadata, domain->catalog, domain->tree, pack.stage,
                                                                pin, &state->backing->pin);
    if (status.ok() && !chunk_backing_matches(*state->backing, domain->backend_id, pack.length)) {
        status = Status::corruption("checkpoint pack identity is invalid");
    }
    if (status.ok()) {
        status = domain->catalog->protection()->check_read_owner();
    }
    if (!status.ok()) {
        state->finish(std::move(status));
        return;
    }
    domain->catalog->submit_load_stage(domain->tree, pack.stage, &state->record, state->callback(&PinBacking::loaded));
}
catch (const std::bad_alloc &) {
    complete.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree
