// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "tree_generation_lease_internal.h"

#include <cassert>

namespace crowdb::tree
{
TreeGenerationLease::TreeGenerationLease(detail::ReservedUnique<TreeGenerationPreparation> state)
    : state_(std::move(state))
{
}

TreeGenerationLease::~TreeGenerationLease()
{
    cancel();
}

Status TreeGenerationLease::acquire()
try {
    auto &prepared = *state_;
    auto &core     = *prepared.owner;
    auto &state    = *core.state_;
    if (prepared.held || prepared.committed || !state.begin_replacement()) {
        return Status::unavailable("generation lease is already held or Tree is closed");
    }
    prepared.held = true;
    auto status   = check_replacement();
    if (status.ok()) {
        // A completed controller can retain its backend ticket. Releasing it
        // is reversible, and does not close the old sources or controller.
        state.flush->release_pending();
        state.lifetime->drain_accepted();
        state.binding->drain();
        status = mark_prepared();
    }
    if (!status.ok()) {
        cancel();
    }
    return status;
}

catch (const std::bad_alloc &) {
    cancel();
    return Status::resource_exhausted("generation acquisition exhausted its control credit");
}

catch (...) {
    cancel();
    return Status::internal_error("generation acquisition failed before installation");
}

Status TreeGenerationLease::check_replacement() const
{
    const auto &prepared = *state_;
    const auto &core     = *prepared.owner;
    const auto &state    = *core.state_;
    if (core.closed() || state.publisher->capture()->generation != prepared.previous_generation) {
        return Status::unavailable("Tree generation changed during preparation");
    }
    // An armed proposal can still choose a distant slot, and accepted replay
    // can already be waiting for a missing slot. With ingress fenced neither
    // can obtain new gap repair. Leave the old generation usable instead of
    // waiting for work whose prerequisite this lease would prevent.
    if (state.apply_lifetime->outstanding() != 0 || state.replay_lifetime->outstanding() != 0) {
        return Status::unavailable("generation replacement requires completed accepted apply and replay");
    }
    if (prepared.expected &&
        (state.publisher->capture() != prepared.expected || state.frontier->max_seen() > prepared.expected->coverage)) {
        return Status::unavailable("split destination changed before prepared installation");
    }
    return Status::Ok();
}

Status TreeGenerationLease::mark_prepared()
{
    auto &core  = *state_->owner;
    auto &state = *core.state_;
    state.cache->evict();
#ifdef CROWDB_TREE_TEST_UTIL
    if (state.before_generation_install_for_tests) {
        state.before_generation_install_for_tests();
    }
#endif
    if (core.closed()) {
        return Status::unavailable("Tree closed while preparing generation installation");
    }
    assert(state.publisher->idle());
    uint64_t expected = TreeCore::State::kReplacing;
    if (!state.ingress.compare_exchange_strong(expected, TreeCore::State::kPrepared, std::memory_order_acq_rel)) {
        return Status::unavailable("Tree closed before its prepared installation became ready");
    }
    return Status::Ok();
}

template <void (TreeGenerationLease::*Next)() noexcept> AsyncCompletion TreeGenerationLease::continue_acquire() noexcept
{
    return {.context     = this,
            .complete_fn = [](void *context,
                              Status /*status*/) noexcept { (static_cast<TreeGenerationLease *>(context)->*Next)(); },
            .owner       = weak_from_this().lock()};
}

Status TreeGenerationLease::acquire_async(AsyncCompletion complete) noexcept
{
    if (!complete) {
        return Status::invalid_argument();
    }
    auto &prepared = *state_;
    auto &state    = *prepared.owner->state_;
    if (prepared.held || prepared.committed || !state.fence_replacement()) {
        return Status::unavailable();
    }
    prepared.held        = true;
    prepared.on_acquired = std::move(complete);
    state.drain_replacement_ingress(continue_acquire<&TreeGenerationLease::drain_rotation>());
    return Status::Ok();
}

void TreeGenerationLease::drain_rotation() noexcept
{
    state_->owner->state_->source_rotation->drain_async(continue_acquire<&TreeGenerationLease::drain_operations>());
}

void TreeGenerationLease::drain_operations() noexcept
try {
    auto &state  = *state_->owner->state_;
    auto  status = check_replacement();
    if (!status.ok()) {
        complete_acquire(std::move(status));
        return;
    }
    state.flush->release_pending();
    state.lifetime->drain_async(continue_acquire<&TreeGenerationLease::drain_cpu>());
}
catch (const std::bad_alloc &) {
    complete_acquire(Status::resource_exhausted());
}
catch (...) {
    complete_acquire(Status::internal_error());
}

void TreeGenerationLease::drain_cpu() noexcept
{
    state_->owner->binding()->lane()->drain_async(continue_acquire<&TreeGenerationLease::drain_backend>());
}

void TreeGenerationLease::drain_backend() noexcept
{
    state_->owner->binding()->backend_lane()->drain_async(continue_acquire<&TreeGenerationLease::drain_retirements>());
}

void TreeGenerationLease::drain_retirements() noexcept
{
    state_->owner->binding()->account()->when_retirements_drained(
        continue_acquire<&TreeGenerationLease::finish_acquire>());
}

void TreeGenerationLease::finish_acquire() noexcept
try {
    complete_acquire(mark_prepared());
}
catch (const std::bad_alloc &) {
    complete_acquire(Status::resource_exhausted());
}
catch (...) {
    complete_acquire(Status::internal_error());
}

void TreeGenerationLease::complete_acquire(Status status) noexcept
{
    auto done = std::move(state_->on_acquired);
    if (!status.ok()) {
        cancel();
    }
    done.complete(std::move(status));
}

Status TreeGenerationLease::commit() noexcept
{
    auto &prepared = *state_;
    auto &core     = *prepared.owner;
    auto &state    = *core.state_;
    if (prepared.committed) {
        return Status::Ok();
    }
    if (!prepared.held) {
        return Status::unavailable();
    }
    uint64_t expected = TreeCore::State::kPrepared;
    if (!state.ingress.compare_exchange_strong(expected, TreeCore::State::kReplacing, std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    assert(state.publisher->idle());
    state.sources->close();
    state.flush->close();
    state.split_handoff.swap(prepared.previous_split_handoff);
    state.split_sources.swap(prepared.previous_split_sources);
    state.sources.swap(prepared.sources);
    state.flush.swap(prepared.flush);
    state.source_rotation->rebind(*state.sources, *state.flush);
    state.source_lifecycle.swap(prepared.source_lifecycle);
    state.frontier->reset_quiescent(prepared.frontier);
    state.automatic_slot.store(prepared.frontier, std::memory_order_release);
    state.publisher->install_replacement(prepared.publication);
    prepared.committed = true;
    // Old controls stay owned by this lease until its host-side release. Do
    // not reclaim epochs or destroy old controllers on the commit boundary.
    state.finish_replacement();
    prepared.held = false;
    return Status::Ok();
}

Status TreeGenerationLease::set_split_frontier(uint64_t frontier, SourceMemoryStats memory) noexcept
{
    auto &prepared = *state_;
    if (!prepared.held || prepared.committed || !prepared.inherited || frontier < prepared.coverage) {
        return Status::invalid_argument();
    }
    auto &range = prepared.inherited->inheritance;
    if (!range.has_value() || prepared.inherited->tables.empty()) {
        return Status::invalid_argument();
    }
    if (frontier == prepared.coverage) {
        prepared.inherited->inheritance.reset();
        prepared.inherited->tables.erase(prepared.inherited->tables.begin() + 1, prepared.inherited->tables.end());
    }
    else {
        range->frontier = frontier;
        range->memory   = memory;
    }
    prepared.frontier = frontier;
    return Status::Ok();
}

void TreeGenerationLease::cancel() noexcept
{
    if (!state_->held) {
        return;
    }
    auto    &state    = *state_->owner->state_;
    uint64_t expected = TreeCore::State::kPrepared;
    // The second case is acquire failing before its host-side guard handoff.
    if (state.ingress.compare_exchange_strong(expected, TreeCore::State::kReplacing, std::memory_order_acq_rel) ||
        (expected & TreeCore::State::kReplacing) != 0) {
        state.finish_replacement();
    }
    state_->held = false;
}
} // namespace crowdb::tree
