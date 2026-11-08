// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "host_completion.h"
#include "page_store/checkpoint_page_store.h"
#include "page_store/local/local_page_store_state.h"
#include "tree_core_internal.h"

namespace crowdb::tree
{
bool TreeCore::State::enter() noexcept
{
    auto value = ingress.load(std::memory_order_acquire);
    do {
        if ((value & (kClosed | kReplacing | kPrepared)) != 0) {
            return false;
        }
    } while (!ingress.compare_exchange_weak(value, value + 1, std::memory_order_acq_rel));
    return true;
}

void TreeCore::State::leave() noexcept
{
    const auto previous = ingress.fetch_sub(1, std::memory_order_acq_rel);
    if ((previous & kParticipants) == 1) {
        ingress.notify_all();
        if (previous == (kClosed | kCloseArmed | 1U)) {
            finish_ingress();
        }
        else if ((previous & kReplacementArmed) != 0) {
            finish_replacement_ingress();
        }
    }
}

void TreeCore::State::close_admission_async(AsyncCompletion complete) noexcept
{
    // A host ingress participant can be waiting on unaccepted soft work.
    // Stop that wait before joining ingress, then let armed work finish.
    ingress.fetch_or(kClosed, std::memory_order_acq_rel);
    source_rotation->stop();
    lifetime->close();
    apply_lifetime->close();
    replay_lifetime->close();
    on_close_drained = std::move(complete);
    // A prepared capability has no running publisher. Closing revokes it
    // without waiting for an external owner to destroy its handle. Publish
    // the continuation together with that revocation.
    auto value = ingress.load(std::memory_order_acquire);
    while (!ingress.compare_exchange_weak(value, (value & ~kPrepared) | kCloseArmed, std::memory_order_acq_rel)) {
    }
    if ((value & (kParticipants | kReplacing)) == 0) {
        finish_ingress();
    }
}

void TreeCore::State::finish_ingress() noexcept
{
    // Chosen work still has its prepaid apply and handoff path.
    apply_lifetime->close_async({.context = this, .complete_fn = &State::chosen_drained});
}

void TreeCore::State::chosen_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.replay_controls->stop_admission();
    self.apply_progress->advance();
    self.replay_lifetime->close_async({.context = &self, .complete_fn = &State::replay_drained});
}

void TreeCore::State::replay_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.source_rotation->drain_async({.context = &self, .complete_fn = &State::rotation_drained});
}

void TreeCore::State::rotation_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.apply_progress->close();
    self.split_handoff.reset();
    self.split_sources.reset();
    self.sources->close();
    self.flush->close();
    self.flush->release_pending();
    self.lifetime->close_async({.context = &self, .complete_fn = &State::operations_drained});
}

void TreeCore::State::operations_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.apply_dispatcher->close_async({.context = &self, .complete_fn = &State::dispatcher_drained});
}

void TreeCore::State::dispatcher_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.publisher->close();
    self.cache->close_async({.context = &self, .complete_fn = &State::cache_drained});
}

void TreeCore::State::cache_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.binding->backend_progress()->close_async({.context = &self, .complete_fn = &State::backend_progress_drained});
}

void TreeCore::State::backend_progress_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.binding->backend_lane()->close_async({.context = &self, .complete_fn = &State::backend_lane_drained});
}

void TreeCore::State::backend_lane_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    self.binding->lane()->close_async({.context = &self, .complete_fn = &State::cpu_lane_drained});
}

void TreeCore::State::cpu_lane_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    // The ordinary operation tickets, including Local GC, have already joined.
    self.binding->account()->when_retirements_drained({.context = &self, .complete_fn = &State::retirements_drained});
}

void TreeCore::State::retirements_drained(void *context, Status /*status*/) noexcept
{
    auto &self = *static_cast<State *>(context);
    if (self.chunk) {
        self.chunk->close_writer();
    }
    self.publisher->reclaim();
    self.sources->reclaim();
    if (self.release_on_close) {
        self.release_resources();
    }
    auto account = self.binding->account();
    account->when_retirements_drained({.context = &self, .complete_fn = &State::close_drained});
}

void TreeCore::State::close_drained(void *context, Status status) noexcept
{
    auto &self = *static_cast<State *>(context);
    auto  done = std::move(self.on_close_drained);
    if (self.release_on_close) {
        self.close_state.store(2, std::memory_order_release);
        self.close_state.notify_all();
    }
    done.complete(std::move(status));
}

bool TreeCore::State::fence_replacement() noexcept
{
    auto value = ingress.load(std::memory_order_acquire);
    do {
        if ((value & (kClosed | kReplacing | kPrepared)) != 0) {
            return false;
        }
    } while (!ingress.compare_exchange_weak(value, value | kReplacing, std::memory_order_acq_rel));
    source_rotation->stop();
    return true;
}

bool TreeCore::State::begin_replacement() noexcept
{
    if (!fence_replacement()) {
        return false;
    }
    drain_ingress();
    source_rotation->drain();
    return true;
}

void TreeCore::State::drain_replacement_ingress(AsyncCompletion complete) noexcept
{
    on_replacement_ingress_drained = std::move(complete);
    const auto previous            = ingress.fetch_or(kReplacementArmed, std::memory_order_acq_rel);
    if ((previous & kParticipants) == 0) {
        finish_replacement_ingress();
    }
}

void TreeCore::State::finish_replacement_ingress() noexcept
{
    auto done = std::move(on_replacement_ingress_drained);
    ingress.fetch_and(~kReplacementArmed, std::memory_order_acq_rel);
    done.complete(Status::Ok());
}

void TreeCore::State::drain_ingress() noexcept
{
    auto value = ingress.load(std::memory_order_acquire);
    while ((value & kParticipants) != 0) {
        ingress.wait(value, std::memory_order_acquire);
        value = ingress.load(std::memory_order_acquire);
    }
}

void TreeCore::State::finish_replacement() noexcept
{
    if ((ingress.load(std::memory_order_acquire) & kClosed) == 0) {
        source_rotation->restart();
    }
    const auto previous = ingress.fetch_and(~kReplacing, std::memory_order_acq_rel);
    if (previous == (kClosed | kCloseArmed | kReplacing)) {
        finish_ingress();
    }
}

bool TreeCore::close_async(AsyncCompletion complete) noexcept
{
    uint8_t expected = 0;
    if (!state_->close_state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
        return false;
    }
    state_->release_on_close = true;
    state_->close_admission_async(std::move(complete));
    return true;
}

void TreeCore::close_and_drain() noexcept
{
    uint8_t expected = 0;
    if (!state_->close_state.compare_exchange_strong(expected, 1, std::memory_order_acq_rel)) {
        while (expected != 2) {
            state_->close_state.wait(expected, std::memory_order_acquire);
            expected = state_->close_state.load(std::memory_order_acquire);
        }
        return;
    }
    detail::HostCompletion drained;
    state_->close_admission_async({.context = &drained, .complete_fn = [](void *context, Status /*status*/) noexcept {
                                       static_cast<detail::HostCompletion *>(context)->signal();
                                   }});
    drained.wait();
    state_->close_state.store(2, std::memory_order_release);
    state_->close_state.notify_all();
}

void TreeCore::close_facade() noexcept
{
    close_and_drain();
    if (!state_->release_on_close) {
        // A split capability can still hold the Core when the compatibility
        // facade is destroyed. Finish the host-only drain before it can leave
        // the last reference on the retirement worker.
        state_->release_resources();
        state_->binding->drain();
        state_->release_on_close = true;
    }
}

bool TreeCore::closed() const noexcept
{
    return (state_->ingress.load(std::memory_order_acquire) & State::kClosed) != 0;
}

const Config &TreeCore::config() const noexcept
{
    return state_->config;
}

const std::shared_ptr<TreeRuntimeBinding> &TreeCore::binding() const noexcept
{
    return state_->binding;
}

const std::shared_ptr<ApplyDispatcher> &TreeCore::cpu_progress() const noexcept
{
    return state_->apply_dispatcher;
}

const std::shared_ptr<SharedPageCache> &TreeCore::cache() const noexcept
{
    return state_->cache;
}

const std::shared_ptr<LocalPageStoreState> &TreeCore::local_page_store() const noexcept
{
    return state_->local;
}

const std::shared_ptr<CheckpointPageStore> &TreeCore::page_chunk_store() const noexcept
{
    return state_->chunk;
}

uint64_t TreeCore::durable_coverage() const noexcept
{
    if (state_->chunk) {
        return state_->chunk->durable_coverage.load(std::memory_order_acquire);
    }
    return state_->local ? state_->local->checkpoints->durable_coverage() : 0;
}

uint64_t TreeCore::snapshot_sequence() const noexcept
{
    if (state_->chunk) {
        return state_->chunk->snapshot_sequence.load(std::memory_order_acquire);
    }
    return state_->local ? state_->local->checkpoints->snapshot_sequence() : 0;
}

AsyncPageStore *TreeCore::async_page_store() const noexcept
{
    return state_->page_store.get();
}

PageLoadLimits TreeCore::load_limits() const noexcept
{
    return state_->loads;
}

const PublishedReadLimits &TreeCore::read_limits() const noexcept
{
    return state_->reads;
}

const std::shared_ptr<AllocationBudget> &TreeCore::accepted_allocations() const noexcept
{
    return state_->work_allocations;
}

std::shared_ptr<const TreeRoot> TreeCore::capture() const
{
    return state_->publisher->capture();
}

PublicationCoordinator &TreeCore::publisher() const noexcept
{
    return *state_->publisher;
}

TreeOperationLifetime &TreeCore::operation_lifetime() const noexcept
{
    return *state_->lifetime;
}

void TreeCore::request_gc_floor(uint64_t floor) noexcept
{
    State::Ingress admission(*state_);
    if (admission.owner != nullptr) {
        state_->publisher->request_gc_floor(floor);
    }
}

uint64_t TreeCore::requested_gc_floor() const noexcept
{
    return state_->publisher->requested_gc_floor();
}

uint64_t TreeCore::contiguous_slot() const noexcept
{
    return state_->frontier->contiguous().load(std::memory_order_acquire);
}

uint64_t TreeCore::max_seen_slot() const noexcept
{
    return state_->frontier->max_seen();
}

size_t TreeCore::memtable_count() const
{
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return 0;
    }
    size_t result = 0;
    for (const auto &table : state_->sources->catalog()->tables) {
        result += table->count();
    }
    return result;
}

size_t TreeCore::frozen_table_count() const
{
    State::Ingress admission(*state_);
    if (admission.owner == nullptr) {
        return 0;
    }
    const auto catalog = state_->sources->catalog();
    return catalog->tables.size() - 1;
}
} // namespace crowdb::tree
