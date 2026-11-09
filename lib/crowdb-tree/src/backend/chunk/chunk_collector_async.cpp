// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "chunk_collector.h"

#include <cassert>

namespace crowdb::tree::detail
{
struct ChunkCollector::AsyncStep : std::enable_shared_from_this<AsyncStep>
{
    ChunkCollector                        *cursor = nullptr;
    std::shared_ptr<RootCatalog>           catalog;
    std::shared_ptr<ChunkTransport>        transport;
    std::shared_ptr<AllocationBudget>      budget;
    std::shared_ptr<ChunkPhysicalRegistry> registry;
    uint64_t                               tree  = 0;
    uint64_t                               epoch = 0;
    ChunkCollectionProgress               *out   = nullptr;
    AsyncCompletion                        completion;
    std::optional<ChunkCandidateStatus>    candidate;
    std::vector<ChunkCandidateStatus>      candidates;
    std::optional<ChunkStageRecord>        stage;
    std::vector<ChunkStageRecord>          stages;
    std::optional<ChunkStageRecord>        retired;
    ChunkPinProgress                       pins;
    ChunkStageGcResult                     reclaimed;

    AsyncCompletion reply(AsyncCompleteFn callback)
    {
        return {.context = this, .complete_fn = callback, .owner = shared_from_this(), .allocations = budget};
    }

    void finish(Status status)
    {
        out->more = !(cursor->candidates_end_ && cursor->stages_end_);
        auto done = std::exchange(completion, {});
        done.complete(std::move(status));
    }

    void advance_candidate(ChunkStageId id)
    {
        cursor->candidate_after_ = id;
        cursor->candidate_active_.reset();
    }

    void advance_stage(ChunkStageId id)
    {
        cursor->stage_after_ = id;
        cursor->stage_active_.reset();
        cursor->pin_after_.reset();
    }

    template <auto Method> static void receive(void *raw, Status status) noexcept
    {
        auto &self = *static_cast<AsyncStep *>(raw);
        try {
            (self.*Method)(std::move(status));
        }
        catch (const std::bad_alloc &) {
            self.finish(Status::resource_exhausted());
        }
        catch (...) {
            self.finish(Status::internal_error());
        }
    }

    void start(bool candidate_step)
    {
        if (candidate_step) {
            auto *protection = catalog->protection();
            if (cursor->candidate_active_) {
                protection->submit_load_candidate(tree, *cursor->candidate_active_, &candidate,
                                                  reply(&receive<&AsyncStep::candidate_loaded>));
            }
            else {
                protection->submit_list_candidates(tree, cursor->candidate_after_, 1, &candidates,
                                                   reply(&receive<&AsyncStep::candidates_listed>));
            }
        }
        else if (cursor->stage_active_) {
            catalog->submit_load_stage(tree, *cursor->stage_active_, &stage, reply(&receive<&AsyncStep::stage_loaded>));
        }
        else {
            catalog->submit_list_stages(tree, cursor->stage_after_, 1, &stages,
                                        reply(&receive<&AsyncStep::stages_listed>));
        }
    }

    void candidates_listed(Status status)
    {
        if (status.ok() && !candidates.empty()) {
            candidate = candidates.front();
        }
        candidate_loaded(std::move(status));
    }

    void candidate_loaded(Status status)
    {
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (!candidate) {
            if (cursor->candidate_active_) {
                finish(Status::corruption("active candidate disappeared before reference release"));
                return;
            }
            cursor->candidates_end_ = true;
            finish(Status::Ok());
            return;
        }
        if (!cursor->candidate_active_) {
            ++out->candidates_examined;
        }
        cursor->candidate_active_ = candidate->id;
        auto *protection          = catalog->protection();
        if (candidate->state == ChunkCandidateState::Preparing || candidate->state == ChunkCandidateState::Ready) {
            protection->submit_finish_candidate(tree, candidate->id, CT_CHUNK_CANDIDATE_RECOVER,
                                                reply(&receive<&AsyncStep::candidate_recovered>));
            return;
        }
        if ((candidate->state == ChunkCandidateState::Aborted || candidate->state == ChunkCandidateState::Reclaimed) &&
            candidate->released_windows < candidate->prepared_windows) {
            protection->submit_release_references(tree, candidate->id, candidate->released_windows,
                                                  reply(&receive<&AsyncStep::references_released>));
            return;
        }
        advance_candidate(candidate->id);
        finish(Status::Ok());
    }

    void candidate_recovered(Status status)
    {
        if (status.code() == Code::kUnavailable) {
            if (!candidate) {
                finish(Status::corruption());
                return;
            }
            advance_candidate(candidate->id);
            status = Status::Ok();
        }
        finish(std::move(status));
    }

    void references_released(Status status)
    {
        if (status.ok()) {
            ++out->reference_windows;
        }
        finish(std::move(status));
    }

    void stages_listed(Status status)
    {
        if (status.ok() && !stages.empty()) {
            stage = std::move(stages.front());
        }
        stage_loaded(std::move(status));
    }

    void stage_loaded(Status status)
    {
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (!stage) {
            if (cursor->stage_active_) {
                cursor->stage_after_ = cursor->stage_active_;
                cursor->stage_active_.reset();
                cursor->pin_after_.reset();
            }
            else {
                {
                    cursor->stages_end_ = true;
                }
            }
            finish(Status::Ok());
            return;
        }
        if (!cursor->stage_active_) {
            ++out->stages_examined;
            cursor->stage_active_ = stage->id;
        }
        if (stage->id.owner_epoch > epoch) {
            advance_stage(stage->id);
            finish(Status::Ok());
            return;
        }
        if (stage->state < ChunkStageState::Retired) {
            catalog->protection()->submit_prune_pins(tree, stage->id, cursor->pin_after_, 64, &pins,
                                                     reply(&receive<&AsyncStep::pins_pruned>));
            return;
        }
        submit_reclaim_retired_stage(catalog, transport, tree, epoch, stage->id, budget, registry, &reclaimed,
                                     reply(&receive<&AsyncStep::stage_reclaimed>));
    }

    void pins_pruned(Status status)
    {
        assert(stage);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (pins.examined > 64 || pins.released > pins.examined ||
            (pins.more && (!pins.after || (cursor->pin_after_ && *pins.after <= *cursor->pin_after_)))) {
            finish(Status::corruption("pin sweep did not advance its bounded cursor"));
            return;
        }
        out->pins_examined += pins.examined;
        out->pins_released += pins.released;
        cursor->pin_after_ = pins.after;
        if (pins.more) {
            finish(Status::Ok());
            return;
        }
        if (!stage || stage->revision == UINT64_MAX) {
            finish(Status::corruption());
            return;
        }
        auto &retired_stage = retired.emplace(*stage);
        ++retired_stage.revision;
        retired_stage.state = ChunkStageState::Retired;
        catalog->submit_compare_stage(tree, epoch, stage, retired, reply(&receive<&AsyncStep::stage_retired>));
    }

    void stage_retired(Status status)
    {
        if (status.ok()) {
            {
                cursor->pin_after_.reset();
            }
        }
        else if (status.code() == Code::kUnavailable) {
            if (!stage) {
                finish(Status::corruption());
                return;
            }
            advance_stage(stage->id);
            status = Status::Ok();
        }
        finish(std::move(status));
    }

    void stage_reclaimed(Status status)
    {
        if (status.ok() && !stage) {
            finish(Status::corruption());
            return;
        }
        if (status.ok() && stage) {
            if (reclaimed.stage_removed) {
                ++out->stages_removed;
                out->logical_capacity_bytes += stage->capacity;
            }
            advance_stage(stage->id);
        }
        finish(std::move(status));
    }
};

void ChunkCollector::submit_step(std::shared_ptr<RootCatalog> catalog, std::shared_ptr<ChunkTransport> transport,
                                 uint64_t tree, uint64_t epoch, std::shared_ptr<AllocationBudget> budget,
                                 ChunkCollectionProgress *out, std::shared_ptr<ChunkPhysicalRegistry> registry,
                                 AsyncCompletion complete)
try {
    if (!catalog || !transport || tree == 0 || epoch == 0 || !budget || out == nullptr) {
        complete.complete(Status::invalid_argument());
        return;
    }
    auto *protection = catalog->protection();
    if (protection == nullptr || !transport->fixed_staging_supported()) {
        complete.complete(Status::not_supported());
        return;
    }
    auto status = protection->check_read_owner();
    if (!status.ok()) {
        complete.complete(std::move(status));
        return;
    }
    auto step       = reserved_shared<AsyncStep>(budget);
    step->cursor    = this;
    step->catalog   = std::move(catalog);
    step->transport = std::move(transport);
    step->budget    = std::move(budget);
    step->registry  = std::move(registry);
    step->tree      = tree;
    step->epoch     = epoch;
    step->out       = out;
    // Retain the caller's owner if starting the first catalog operation throws.
    step->completion = complete; // NOLINT(performance-use-std-move)
    if (candidates_end_ && stages_end_) {
        candidates_end_ = stages_end_ = false;
        candidate_after_.reset();
        stage_after_.reset();
    }
    const bool candidate = !candidates_end_ && (next_candidate_ || stages_end_);
    next_candidate_      = !candidate;
    step->start(candidate);
}
catch (const std::bad_alloc &) {
    complete.complete(Status::resource_exhausted());
}
catch (...) {
    complete.complete(Status::internal_error());
}
} // namespace crowdb::tree::detail
