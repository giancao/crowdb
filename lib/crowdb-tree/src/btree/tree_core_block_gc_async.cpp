// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "crowdb-common/log.h"
#include "page_store/local/block_account.h"
#include "page_store/local/local_sparse_gc.h"

#include <cassert>

namespace crowdb::tree::checkpoint_detail
{
namespace
{
struct Cleanup : std::enable_shared_from_this<Cleanup>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<Cleanup> owner;

        void set_value() noexcept
        {
            auto keep = std::move(owner);
            keep->completed(Status::Ok());
        }

        void set_error(Status status) noexcept
        {
            auto keep = std::move(owner);
            keep->completed(std::move(status));
        }

        void set_stopped() noexcept
        {
            auto keep = std::move(owner);
            keep->completed(Status::unavailable());
        }
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<Cleanup> owner)
            : connected(stdexec::connect(sender, Receiver{.owner = std::move(owner)}))
        {
        }

        Connected connected;
    };

    std::shared_ptr<AllocationBudget>              metadata;
    std::shared_ptr<AllocationBudget>              backend;
    std::shared_ptr<LocalPageStoreState>           local;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    TreeCore                                      *core = nullptr;
    ApplyDispatcher::Node                          node{&Cleanup::run, this};
    bool                                           owns_gate             = false;
    bool                                           retirements_drained   = false;
    bool                                           free_blocks_collected = false;
    ProtectedExtent                                exclusion;
    std::shared_ptr<CheckpointOperation>           operation;
    std::optional<Attempt>                         attempt;
    Status                                         outcome;
    size_t                                         remaining = 2;

    void completed(Status status) noexcept
    {
        outcome = std::move(status);
        binding->backend_progress()->request(node, weak_from_this().lock());
    }

    void collect(PageDiskStore &store, uint64_t first, LocalBlockGcLimits limits,
                 detail::InlineFunction<void(LocalBlockGcResult)> complete)
    {
        collect_free_file_blocks(store, local->ledger, first, limits, metadata, backend,
                                 core->config().async_page_store, binding->backend_progress(), std::move(complete));
    }

    void advance()
    {
        if (auto *memory = dynamic_cast<PageMemStore *>(core->config().page_store)) {
            if (!outcome.ok()) {
                return;
            }
            collect_free_memory_blocks(*memory, local->ledger, local->gc_cursor.load(), {}, metadata,
                                       [owner = shared_from_this()](LocalBlockGcResult result) {
                                           owner->local->gc_cursor.store(result.complete ? 0 : result.next_block,
                                                                         std::memory_order_relaxed);
                                           if (result.status.ok() && !result.complete) {
                                               owner->completed(Status::Ok());
                                           }
                                       });
            return;
        }
        auto *store = dynamic_cast<PageDiskStore *>(core->config().page_store);
        if (store == nullptr || store->block_size() == 0 || !outcome.ok()) {
            return;
        }
        attempt.reset();
        operation.reset();
        if (!exclusion.owner) {
            if (local->checkpoints->needs_anchor_retirement()) {
                auto options                            = defaults(*core);
                options.cause                           = TreeWorkCause::GarbageCollection;
                options.metadata.retire_previous_anchor = true;
                const auto status                       = core->prepare_checkpoint(options, &operation);
                if (!status.ok()) {
                    return;
                }
                attempt.emplace(operation->sender(), shared_from_this());
                stdexec::start(attempt->connected);
                return;
            }
            if (!retirements_drained) {
                core->publisher().reclaim();
                binding->account()->when_retirements_drained({
                    .context = this,
                    .complete_fn =
                        [](void *context, Status status) noexcept {
                            auto &self               = *static_cast<Cleanup *>(context);
                            self.retirements_drained = true;
                            self.completed(std::move(status));
                        },
                    .owner = shared_from_this(),
                });
                return;
            }
            if (!free_blocks_collected) {
                const auto cursor = local->gc_cursor.load(std::memory_order_relaxed);
                if (const auto first = store->next_file_index(cursor, backend)) {
                    collect(*store, *first, {}, [owner = shared_from_this()](LocalBlockGcResult result) {
                        owner->local->gc_cursor.store(result.complete ? 0 : result.next_block,
                                                      std::memory_order_relaxed);
                        if (!result.status.ok()) {
                            if (result.status.code() != Code::kResourceExhausted &&
                                result.status.code() != Code::kUnavailable) {
                                CRB_LOG_WARN("post-flush block cleanup deferred: {}", result.status.to_string());
                            }
                            return;
                        }
                        owner->free_blocks_collected = result.complete;
                        owner->completed(Status::Ok());
                    });
                    return;
                }
                local->gc_cursor.store(0, std::memory_order_relaxed);
                free_blocks_collected = true;
            }
            select_sparse_local_block_async(
                *store, local->ledger, local->gc_cursor.load(std::memory_order_relaxed), metadata, 0.5, backend,
                [owner = shared_from_this()](LocalSparseBlock candidate) {
                    owner->local->gc_cursor.store(candidate.next_block, std::memory_order_relaxed);
                    owner->exclusion = std::move(candidate.exclusion);
                    owner->completed(std::move(candidate.status));
                });
            return;
        }
        if (remaining == 0) {
            core->cache()->evict();
            // Quiescent publication roots can otherwise keep old extents
            // until the epoch batch reaches its normal collection threshold.
            core->publisher().reclaim();
            const LocalBlockGcLimits limits{.max_blocks = 1, .max_deleted_bytes = store->block_size()};
            collect(*store, exclusion.address / store->block_size(), limits,
                    [owner = shared_from_this()](LocalBlockGcResult result) {
                        if (!result.status.ok()) {
                            owner->local->gc_cursor.store(result.next_block, std::memory_order_relaxed);
                        }
                    });
            return;
        }
        --remaining;
        auto options                   = defaults(*core);
        options.cause                  = TreeWorkCause::GarbageCollection;
        options.write.frames.exclusion = {.address = exclusion.address, .length = exclusion.length};
        auto status                    = core->prepare_checkpoint(options, &operation);
        if (!status.ok()) {
            return;
        }
        attempt.emplace(operation->sender(), shared_from_this());
        stdexec::start(attempt->connected);
    }

    ~Cleanup()
    {
        detail::seal_backend_budget(backend);
        if (owns_gate) {
            local->gc_active.store(false, std::memory_order_release);
        }
        detail::seal_reservation(metadata);
    }

    static void run(void *context) noexcept
    {
        // The dispatcher holds this node's owner through the continuation.
        auto &cleanup = *static_cast<Cleanup *>(context);
        try {
            cleanup.advance();
        }
        catch (const std::bad_alloc &) {
            // The existing anchors and live page block still protect their bytes.
            return;
        }
        catch (...) {
            CRB_LOG_WARN("sparse block cleanup deferred after an exception");
        }
    }
};
} // namespace

void schedule_free_blocks(TreeCore &core, std::shared_ptr<TreeOperationLifetime::Ticket> ticket) noexcept
try {
    auto                     local = core.local_page_store();
    auto                    *store = core.config().page_store;
    const LocalBlockGcLimits limits;
    const auto               unit = store != nullptr ? local_block_gc_unit(*store) : 0;
    if (!ticket || !local || !local->ledger || unit == 0 || unit > limits.max_deleted_bytes ||
        local->gc_active.exchange(true, std::memory_order_acq_rel)) {
        return;
    }

    struct Gate
    {
        LocalPageStoreState *local;

        ~Gate()
        {
            if (local != nullptr) {
                local->gc_active.store(false, std::memory_order_release);
            }
        }
    } gate{local.get()};

    using namespace detail;
    auto metadata       = core.accepted_allocations();
    auto cleanup        = reserved_shared<Cleanup>(metadata);
    cleanup->metadata   = std::move(metadata);
    auto backend_status = ongoing_backend_operation(store, &cleanup->backend);
    if (!backend_status.ok()) {
        return;
    }
    cleanup->local     = std::move(local);
    cleanup->binding   = core.binding();
    cleanup->ticket    = std::move(ticket);
    cleanup->core      = &core;
    cleanup->owns_gate = true;
    gate.local         = nullptr;
    cleanup->completed(Status::Ok());
}
catch (const std::bad_alloc &) {
    // This checkpoint is already durable; a later pass can retry admission.
    return;
}
catch (const std::exception &error) {
    CRB_LOG_WARN("post-checkpoint cleanup submission deferred: {}", error.what());
}
catch (...) {
    CRB_LOG_WARN("post-checkpoint cleanup submission deferred after unknown exception");
}
} // namespace crowdb::tree::checkpoint_detail
