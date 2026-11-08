// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "chunk_physical_registry.h"
#include "chunk_stage_gc.h"
#include "crowdb-common/log.h"
#include "stdexec_adapter.h"

#include <cassert>
#include <exec/sequence.hpp>

namespace crowdb::tree::detail
{
namespace
{
struct StageGc : std::enable_shared_from_this<StageGc>
{
    std::shared_ptr<RootCatalog>           catalog;
    std::shared_ptr<ChunkTransport>        transport;
    std::shared_ptr<AllocationBudget>      budget;
    std::shared_ptr<ChunkPhysicalRegistry> registry;
    std::shared_ptr<ChunkPhysicalPack>     physical;
    std::optional<ChunkStageRecord>        record;
    std::optional<ChunkStageRecord>        next;
    std::optional<ChunkAllocationIdentity> receipt;
    ChunkAllocationIdentity                inspected;
    uint64_t                               tree  = 0;
    uint64_t                               epoch = 0;
    ChunkStageId                           id;
    ChunkStageGcResult                    *output = nullptr;
    AsyncCompletion                        completion;
    CallbackComplete                       current = nullptr;
    void                                  *context = nullptr;
    std::shared_ptr<StageGc>               accepted;
    std::shared_ptr<void>                  execution;

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        StageGc *owner;

        void set_value() noexcept
        {
            owner->finish(Status::Ok());
        }

        void set_error(Status status) noexcept
        {
            owner->finish(std::move(status));
        }

        void set_error(std::exception_ptr /*unused*/) noexcept
        {
            owner->finish(Status::internal_error());
        }

        void set_stopped() noexcept
        {
            owner->finish(Status::unavailable());
        }
    };

    void finish(Status status) noexcept
    {
        auto keep      = std::move(accepted);
        output->status = std::move(status);
        auto result    = output->status.without_message();
        auto done      = std::exchange(completion, {});
        try {
            done.complete(std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("Stage GC receiver threw after terminal");
        }
    }

    static void completed(void *raw, Status status) noexcept
    {
        auto &self = *static_cast<StageGc *>(raw);
        try {
            if (status.ok() && self.next) {
                self.record = std::move(self.next);
                self.next.reset();
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        try {
            self.current(self.context, status.ok() ? CallbackSignal::kValue : CallbackSignal::kError,
                         std::move(status));
        }
        catch (...) {
            CRB_LOG_ERROR("Stage GC step receiver threw after terminal");
        }
    }

    template <auto Method> static void submit(void *raw, CallbackComplete done, void *context) noexcept
    {
        auto &self   = *static_cast<StageGc *>(raw);
        self.current = done;
        self.context = context;
        try {
            (self.*Method)({.context = raw, .complete_fn = &completed, .allocations = self.budget});
        }
        catch (const std::bad_alloc &) {
            completed(raw, Status::resource_exhausted());
        }
        catch (...) {
            completed(raw, Status::internal_error());
        }
    }

    template <auto Method> auto sender()
    {
        return CallbackSender(this, &submit<Method>);
    }

    ChunkTransportCompletion transport_reply(AsyncCompleteFn fn = &completed)
    {
        return {.complete_fn = fn, .context = this, .allocations = budget};
    }

    bool deleting() const
    {
        return record && record->state == ChunkStageState::Deleting;
    }

    ChunkId chunk() const
    {
        if (!record) {
            throw std::bad_optional_access();
        }
        return {record->chunk_high, record->chunk_low};
    }

    void load(AsyncCompletion done)
    {
        catalog->submit_load_stage(tree, id, &record, std::move(done));
    }

    void mark_deleting(AsyncCompletion done)
    {
        if (!record || record->state == ChunkStageState::Reclaimed || deleting()) {
            done.complete(Status::Ok());
            return;
        }
        if (record->state != ChunkStageState::Retired) {
            done.complete(Status::unavailable("stage still protects a live or unresolved pack"));
            return;
        }
        transition(ChunkStageState::Deleting, std::move(done));
    }

    void transition(ChunkStageState state, AsyncCompletion done)
    {
        if (!record || record->revision == UINT64_MAX) {
            done.complete(Status::corruption());
            return;
        }
        next.emplace(*record);
        next->revision = record->revision + 1;
        next->state    = state;
        catalog->submit_compare_stage(tree, epoch, record, next, std::move(done));
    }

    void inspect(AsyncCompletion done)
    {
        if (!deleting()) {
            done.complete(Status::Ok());
            return;
        }
        transport->submit_inspect_fixed_allocation(chunk(), id.owner_epoch, &inspected,
                                                   transport_reply(+[](void *raw, Status status) {
                                                       auto &self = *static_cast<StageGc *>(raw);
                                                       if (status.ok()) {
                                                           self.receipt = self.inspected;
                                                       }
                                                       if (status.code() == Code::kNotFound) {
                                                           status = Status::Ok();
                                                       }
                                                       completed(raw, std::move(status));
                                                   }));
    }

    void prepare_registry(AsyncCompletion done)
    {
        if (!deleting() || !registry || !receipt) {
            done.complete(Status::Ok());
            return;
        }
        registry->submit_prepare(id, chunk(), budget, &physical, nullptr, transport_reply());
    }

    void observe_registry(AsyncCompletion done)
    {
        if (!physical) {
            done.complete(Status::Ok());
            return;
        }
        if (!receipt) {
            done.complete(Status::corruption());
            return;
        }
        registry->submit_observe(physical, *receipt, budget, transport_reply());
    }

    void drain(AsyncCompletion done)
    {
        if (!deleting()) {
            done.complete(Status::Ok());
            return;
        }
        transport->submit_drain_fixed_chunk(chunk(), transport_reply(+[](void *raw, Status status) {
                                                completed(raw, status.code() == Code::kNotFound ? Status::Ok()
                                                                                                : std::move(status));
                                            }));
    }

    void erase_chunk(AsyncCompletion done)
    {
        if (!deleting()) {
            done.complete(Status::Ok());
            return;
        }
        transport->submit_delete_chunk(chunk(), transport_reply(+[](void *raw, Status status) {
                                           auto &self                         = *static_cast<StageGc *>(raw);
                                           self.output->deletion_acknowledged = status.ok();
                                           completed(raw, std::move(status));
                                       }));
    }

    void record_deletion(AsyncCompletion done)
    {
        if (!deleting() || !registry) {
            done.complete(Status::Ok());
            return;
        }
        registry->submit_deleted(id, chunk(), receipt, budget, transport_reply());
    }

    void mark_reclaimed(AsyncCompletion done)
    {
        if (!deleting()) {
            done.complete(Status::Ok());
            return;
        }
        transition(ChunkStageState::Reclaimed, std::move(done));
    }

    void forget_registry(AsyncCompletion done)
    {
        if (!record || !registry) {
            done.complete(Status::Ok());
            return;
        }
        registry->submit_deleted(id, chunk(), {}, budget, transport_reply());
    }

    void erase_stage(AsyncCompletion done)
    {
        if (!record) {
            done.complete(Status::Ok());
            return;
        }
        catalog->submit_compare_stage(tree, epoch, record, {},
                                      {.context = this,
                                       .complete_fn =
                                           +[](void *raw, Status status) {
                                               auto &self                 = *static_cast<StageGc *>(raw);
                                               self.output->stage_removed = status.ok();
                                               completed(raw, std::move(status));
                                           },
                                       .allocations = budget});
    }
};

template <class State> auto gc_flow(State *state)
{
    return exec::sequence(
        state->template sender<&State::load>(), state->template sender<&State::mark_deleting>(),
        state->template sender<&State::inspect>(), state->template sender<&State::prepare_registry>(),
        state->template sender<&State::observe_registry>(), state->template sender<&State::drain>(),
        state->template sender<&State::erase_chunk>(), state->template sender<&State::record_deletion>(),
        state->template sender<&State::mark_reclaimed>(), state->template sender<&State::forget_registry>(),
        state->template sender<&State::erase_stage>());
}

template <class State> struct Execution
{
    using Connected =
        stdexec::connect_result_t<decltype(gc_flow(static_cast<State *>(nullptr))), typename State::Receiver>;

    explicit Execution(State *state) : connected(stdexec::connect(gc_flow(state), typename State::Receiver{state}))
    {
    }

    Connected connected;
};
} // namespace

void submit_reclaim_retired_stage(std::shared_ptr<RootCatalog> catalog, std::shared_ptr<ChunkTransport> transport,
                                  uint64_t tree, uint64_t epoch, ChunkStageId id,
                                  std::shared_ptr<AllocationBudget>      budget,
                                  std::shared_ptr<ChunkPhysicalRegistry> registry, ChunkStageGcResult *out,
                                  AsyncCompletion complete)
try {
    if (!catalog || !transport || !budget || out == nullptr || tree == 0 || epoch < id.owner_epoch) {
        complete.complete(Status::invalid_argument());
        return;
    }
    if (!catalog->stage_schema_supported() || !transport->fixed_staging_supported()) {
        complete.complete(Status::not_supported());
        return;
    }
    auto state         = reserved_shared<StageGc>(budget);
    state->catalog     = std::move(catalog);
    state->transport   = std::move(transport);
    state->budget      = std::move(budget);
    state->registry    = std::move(registry);
    state->tree        = tree;
    state->epoch       = epoch;
    state->id          = id;
    state->output      = out;
    *out               = {};
    auto execution     = reserved_shared<Execution<StageGc>>(state->budget, state.get());
    state->execution   = execution;
    state->completion  = std::move(complete);
    auto *accepted     = state.get();
    accepted->accepted = std::move(state);
    stdexec::start(execution->connected);
}
catch (const std::bad_alloc &) {
    complete.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
