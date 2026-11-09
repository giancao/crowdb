// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "btree/local_compaction_internal.h"

namespace crowdb::tree
{
void LocalCompactionOperation::State::Receiver::set_value() noexcept
{
    set_error(Status::Ok());
}

void LocalCompactionOperation::State::Receiver::set_error(Status status) noexcept
{
    auto retained = std::move(owner);
    retained->child_done(std::move(status));
}

void LocalCompactionOperation::State::Receiver::set_stopped() noexcept
{
    set_error(Status::unavailable());
}

void LocalCompactionOperation::State::child_done(Status status) noexcept
{
    child_status = std::move(status);
    child_ready.store(true, std::memory_order_release);
    wake();
}

void LocalCompactionOperation::State::checkpoint()
{
    auto options                            = checkpoint_detail::defaults(*core);
    options.cause                           = TreeWorkCause::GarbageCollection;
    options.metadata.retire_previous_anchor = true;
    options.write.frames.exclusion = {.address = candidate.exclusion.address, .length = candidate.exclusion.length};
    const auto status              = core->prepare_checkpoint(options, &child, ticket);
    if (!status.ok()) {
        finish(status.without_message());
        return;
    }
    phase = candidate.exclusion.owner ? Phase::kCheckpoint : Phase::kRetireAnchor;
    child_ready.store(false, std::memory_order_release);
    attempt.emplace(child->sender(), weak_from_this().lock());
    stdexec::start(attempt->connected);
}

void LocalCompactionOperation::State::pump()
{
    if (phase == Phase::kDone) {
        return;
    }
    if (phase == Phase::kBegin) {
        if (stopped.load(std::memory_order_acquire)) {
            finish(Status::unavailable());
            return;
        }
        if (store == nullptr) {
            finish(Status::Ok());
            return;
        }
        if (local->checkpoints->needs_anchor_retirement()) {
            checkpoint();
            return;
        }
        phase = Phase::kSelected;
        backend_progress->request(backend_node, weak_from_this().lock());
        return;
    }
    if ((phase == Phase::kCheckpoint || phase == Phase::kRetireAnchor) && stopped.load(std::memory_order_acquire)) {
        child->request_stop();
    }
    if (!child_ready.load(std::memory_order_acquire)) {
        return;
    }
    if (phase == Phase::kCheckpoint) {
        report.pages_relocated += child->result().written_frames;
        report.bytes_relocated += child->result().written_bytes;
        attempt.reset();
        child.reset();
        ++rotations;
    }
    else if (phase == Phase::kRetireAnchor) {
        attempt.reset();
        child.reset();
    }
    if (!child_status.ok()) {
        finish(child_status.without_message());
        return;
    }
    if (stopped.load(std::memory_order_acquire)) {
        finish(Status::unavailable());
        return;
    }
    if (phase == Phase::kRetireAnchor) {
        phase = Phase::kSelected;
        child_ready.store(false, std::memory_order_release);
        backend_progress->request(backend_node, weak_from_this().lock());
        return;
    }
    if (phase == Phase::kCleaned || !candidate.exclusion.owner) {
        finish(Status::Ok());
        return;
    }
    if (rotations < 2) {
        checkpoint();
        return;
    }
    phase = Phase::kCleaned;
    child_ready.store(false, std::memory_order_release);
    backend_progress->request(backend_node, weak_from_this().lock());
}

void LocalCompactionOperation::State::await_retirements() noexcept
{
    core->binding()->account()->when_retirements_drained({
        .context = this,
        .complete_fn =
            [](void *context, Status) noexcept {
                auto &self = *static_cast<State *>(context);
                self.backend_progress->request(self.backend_node, self.weak_from_this().lock());
            },
        .owner = weak_from_this().lock(),
    });
}

void LocalCompactionOperation::State::backend_step()
{
    if (reclaim_step == ReclaimStep::kBegin) {
        core->publisher().reclaim();
        reclaim_step = ReclaimStep::kPublication;
        await_retirements();
        return;
    }
    if (phase == Phase::kSelected) {
        if (reclaim_step == ReclaimStep::kPublication) {
            collect_free_file_blocks(*store, local->ledger, local->gc_cursor.load(), {}, credit, backend,
                                     core->config().async_page_store, backend_progress,
                                     [owner = shared_from_this()](LocalBlockGcResult result) {
                                         owner->report.blocks_deleted += result.deleted_blocks;
                                         if (!result.status.ok()) {
                                             owner->local->gc_cursor.store(result.next_block);
                                             owner->child_done(std::move(result.status));
                                             return;
                                         }
                                         owner->reclaim_step = ReclaimStep::kCollected;
                                         owner->backend_progress->request(owner->backend_node, owner);
                                     });
            return;
        }
        reclaim_step = ReclaimStep::kBegin;
        select_sparse_local_block_async(
            *store, local->ledger, local->gc_cursor.load(), credit, options.free_threshold, backend,
            [owner = shared_from_this()](LocalSparseBlock selected) {
                owner->candidate = std::move(selected);
                owner->local->gc_cursor.store(owner->candidate.next_block);
                if (owner->candidate.exclusion.owner) {
                    owner->report.blocks_selected = 1;
                }
                const auto &status = owner->candidate.status;
                owner->child_done(status.code() == Code::kNotFound ? Status::Ok() : status.without_message());
            });
    }
    else if (reclaim_step == ReclaimStep::kPublication) {
        core->cache()->evict();
        core->publisher().reclaim();
        reclaim_step = ReclaimStep::kCache;
        await_retirements();
    }
    else {
        const LocalBlockGcLimits limits{.max_blocks = 1, .max_deleted_bytes = store->block_size()};
        collect_free_file_blocks(*store, local->ledger, candidate.exclusion.address / store->block_size(), limits,
                                 credit, backend, core->config().async_page_store, backend_progress,
                                 [owner = shared_from_this()](LocalBlockGcResult result) {
                                     owner->report.blocks_deleted += result.deleted_blocks;
                                     if (!result.status.ok()) {
                                         owner->local->gc_cursor.store(result.next_block);
                                     }
                                     owner->child_done(std::move(result.status));
                                 });
    }
}
} // namespace crowdb::tree
