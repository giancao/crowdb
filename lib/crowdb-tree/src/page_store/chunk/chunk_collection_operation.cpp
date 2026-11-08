// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_collection_operation.h"

#include "btree/allocation_bound.h"
#include "page_chunk_store_state.h"

namespace crowdb::tree
{
struct ChunkCollectionOperation::State : std::enable_shared_from_this<State>
{
    std::shared_ptr<PageChunkStoreState> page_store_state;
    PageLoadBudget                       budget;
    detail::ChunkCollectionProgress      progress;
    size_t                               steps     = 0;
    bool                                 owns_gate = false;
    AsyncCompletion                      completion;

    ~State()
    {
        release_gate();
        if (budget.reservation()) {
            budget.reservation()->seal();
        }
        if (budget.io) {
            budget.io->seal();
        }
    }

    void release_gate() noexcept
    {
        if (owns_gate) {
            owns_gate = false;
            page_store_state->collection_active.store(false, std::memory_order_release);
            page_store_state.reset();
        }
    }

    static void completed(void *raw, Status status)
    {
        auto &self = *static_cast<State *>(raw);
        ++self.steps;
        if (!status.ok() || self.done()) {
            self.release_gate();
        }
        auto done = std::exchange(self.completion, {});
        done.complete(std::move(status));
    }

    void submit_step(bool stopped, AsyncCompletion complete)
    {
        if (stopped) {
            release_gate();
            complete.complete(Status::unavailable());
            return;
        }
        completion         = std::move(complete);
        const auto &domain = *page_store_state->domain;
        page_store_state->collector.submit_step(
            domain.catalog, domain.transport, domain.tree, domain.epoch, budget.bytes, &progress,
            domain.physical_registry,
            {.context = this, .complete_fn = &completed, .owner = shared_from_this(), .allocations = budget.bytes});
    }

    [[nodiscard]] bool done() const noexcept
    {
        return steps == 8 || !progress.more;
    }
};

size_t ChunkCollectionOperation::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<ChunkCollectionOperation>() +
           ChunkProtocolOperation::allocation_bound() + detail::ChunkCollector::allocation_bound();
}

Status ChunkCollectionOperation::create(std::shared_ptr<PageChunkStoreState>        page_store_state,
                                        std::shared_ptr<AsyncPageStore>             store,
                                        std::shared_ptr<detail::FlushRuntime>       runtime,
                                        std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                                        std::shared_ptr<ChunkCollectionOperation> *out)
try {
    if (!page_store_state || !store || !budget.bytes || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto state              = detail::reserved_shared<State>(budget.bytes);
    state->page_store_state = std::move(page_store_state);
    state->budget           = budget;
    if (state->page_store_state->needs_recovery.load(std::memory_order_acquire) ||
        state->page_store_state->collection_active.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    state->owns_gate = true;
    auto operation   = detail::reserved_construct<ChunkCollectionOperation>(
        budget.bytes, [&](auto *storage) { ::new (static_cast<void *>(storage)) ChunkCollectionOperation(state); });
    auto status = ChunkProtocolOperation::create_async(
        std::move(store), std::move(runtime), std::move(lane), budget, state,
        [state](bool stopped, AsyncCompletion complete) { state->submit_step(stopped, std::move(complete)); },
        [state] { return state->done(); }, &operation->protocol_);
    if (status.ok()) {
        *out = std::move(operation);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

detail::CallbackSender ChunkCollectionOperation::sender() noexcept
{
    return protocol_->sender();
}

const detail::ChunkCollectionProgress &ChunkCollectionOperation::result() const noexcept
{
    return state_->progress;
}

void ChunkCollectionOperation::request_stop() noexcept
{
    protocol_->request_stop();
}
} // namespace crowdb::tree
