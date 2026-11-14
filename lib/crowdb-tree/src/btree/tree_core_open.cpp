// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/block_account.h"
#include "backend/local/local_backend_state.h"
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_account.h"
#include "btree/apply_dispatcher.h"
#include "btree/backing_identity.h"
#include "btree/tree_core.h"
#include "btree/tree_core_geometry.h"
#include "crowdb-tree/backend/page_store.h"
#include "host_completion.h"
#include "snapshot/local_recovery.h"
#include "snapshot/recovered_state.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
struct LocalOpen : std::enable_shared_from_this<LocalOpen>
{
    using Step = void (LocalOpen::*)();

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<LocalOpen> owner;
        Step                       next;

        void set_value() noexcept
        {
            set_error(Status::Ok());
        }

        void set_stopped() noexcept
        {
            set_error(Status::unavailable());
        }

        void set_error(Status status) noexcept
        {
            auto keep = std::move(owner);
            keep->schedule(next, std::move(status));
        }
    };

    using Connection = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    LocalOpen(const Config &config, const std::shared_ptr<TreeRuntimeBinding> &binding,
              std::shared_ptr<LocalBackendState> local, PageStore::RuntimeOpenComplete complete)
        : config(config),
          binding(binding),
          local(std::move(local)),
          complete(std::move(complete)),
          lifetime(binding->lifecycle_budget(), this->local->store)
    {
        limits.max_frame_bytes = std::max(limits.max_frame_bytes, physical_frame_capacity(config));
        limits.max_blob_bytes =
            round_up_to_iu(limits.max_frame_bytes + kDurableBlobHeader, config.page_store->iu_size());
    }

    void schedule(Step step, Status status = Status::Ok()) noexcept
    {
        next  = step;
        event = std::move(status);
        binding->view_cpu_progress()->request(ready, weak_from_this().lock());
    }

    static void run(void *context) noexcept
    {
        auto      &open   = *static_cast<LocalOpen *>(context);
        const auto step   = open.next;
        auto       status = std::move(open.event);
        open.connected.reset();
        if (!status.ok()) {
            open.finish(std::move(status));
            return;
        }
        try {
            (open.*step)();
        }
        catch (const std::bad_alloc &) {
            open.finish(Status::resource_exhausted());
        }
        catch (...) {
            open.finish(Status::internal_error());
        }
    }

    void start(detail::CallbackSender sender, Step step)
    {
        connected = detail::reserved_construct<Connection>(binding->lifecycle_budget(), [&](auto *storage) {
            ::new (static_cast<void *>(storage))
                Connection(stdexec::connect(sender, Receiver{.owner = shared_from_this(), .next = step}));
        });
        stdexec::start(*connected);
    }

#ifdef CROWDB_HAVE_LIBURING
    Status probe_size(BlockAsyncPageStore &block)
    {
        return block.submit_size(&file_size, {.context = this,
                                              .complete_fn =
                                                  [](void *context, Status status) noexcept {
                                                      static_cast<LocalOpen *>(context)->schedule(&LocalOpen::recover,
                                                                                                  std::move(status));
                                                  },
                                              .owner       = shared_from_this(),
                                              .allocations = budget.backend});
    }

#endif

    void recover()
    {
        auto status = lifetime.acquire(binding->lifecycle_budget(), &ticket);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        store = ticket->backend(local->store.get());
        ExtentLedgerLimits domain{.backend_id   = detail::allocate_backend_identity(),
                                  .region_begin = snapshot::region_base_for(config.page_store->iu_size()),
                                  .file_size    = file_size,
                                  .iu_size      = config.page_store->iu_size()};
        if (domain.backend_id == 0) {
            finish(Status::resource_exhausted());
            return;
        }
        status =
            LocalRecoveryOperation::create(store, binding->cpu(), binding->lane(), domain, limits, budget, &recovery);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        start(recovery->sender(), &LocalOpen::validate_graph);
    }

    void validate_graph()
    {
        local->ledger      = recovery->result().ledger;
        local->checkpoints = recovery->result().registry;
        local->inherited   = recovery->result().latest;
        recovery.reset();
        if (!local->inherited) {
            finish(Status::Ok());
            return;
        }
        PageLoadLimits loads;
        loads.max_blob_bytes  = limits.max_blob_bytes;
        loads.max_frame_bytes = limits.max_frame_bytes;
        auto loader           = detail::reserved_shared<AsyncStorePageLoader>(
            budget.bytes, store, binding->cpu(), binding->lane(), loads, OverflowBackingResolver{}, budget);
        auto status = RecoveredStateOperation::create(local->inherited, loader, binding->cpu(), binding->lane(), {},
                                                      budget.bytes, &graph);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        start(graph->sender(), &LocalOpen::finish_graph);
    }

    void finish_graph()
    {
        initial = graph->result().state;
        finish(Status::Ok());
    }

    void finish(Status status) noexcept
    {
        outcome = std::move(status);
        connected.reset();
        graph.reset();
        recovery.reset();
        store.reset();
        ticket.reset();
        lifetime.close_async({.context = this,
                              .complete_fn =
                                  [](void *context, Status /*closed*/) noexcept {
                                      static_cast<LocalOpen *>(context)->schedule(&LocalOpen::install);
                                  },
                              .owner = weak_from_this().lock()});
    }

    void install() noexcept
    {
        std::shared_ptr<TreeCore> opened;
        try {
            if (outcome.ok()) {
                outcome = TreeCore::create(std::move(config), binding,
                                           {.initial = std::move(initial), .local = std::move(local)}, &opened, true);
            }
        }
        catch (const std::bad_alloc &) {
            outcome = Status::resource_exhausted();
        }
        catch (...) {
            outcome = Status::internal_error();
        }
        local.reset();
        auto done = std::move(complete);
        done(std::move(outcome), std::move(opened));
    }

    Config                                         config;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<LocalBackendState>             local;
    PageStore::RuntimeOpenComplete                 complete;
    TreeOperationLifetime                          lifetime;
    PageLoadBudget                                 budget;
    LocalRecoveryLimits                            limits;
    uint64_t                                       file_size = 0;
    ApplyDispatcher::Node                          ready{&LocalOpen::run, this};
    Step                                           next = nullptr;
    Status                                         event;
    Status                                         outcome;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AsyncPageStore>                store;
    std::shared_ptr<LocalRecoveryOperation>        recovery;
    std::shared_ptr<RecoveredStateOperation>       graph;
    std::shared_ptr<const PublishedTreeState>      initial;
    std::shared_ptr<Connection>                    connected;
};

Status validate_open(const Config &config)
{
    if (!config.runtime || config.page_store == nullptr || config.page_store->iu_size() == 0 ||
        config.frame_bytes == 0 || config.frame_bytes % config.page_store->iu_size() != 0) {
        return Status::invalid_argument();
    }
    return Status::Ok();
}

Status submit_local_open(const Config &config, PageStore::RuntimeOpenComplete complete)
try {
    if ((config.async_page_store != nullptr && !config.async_page_store->write_completion_is_reloadable()) ||
        config.page_store->has_shared_ownership()) {
        return Status::not_supported("runtime local recovery requires an "
                                     "independently reloadable local backend");
    }
    auto status = config.key_range.validate();
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<TreeRuntimeBinding> binding;
    status = TreeRuntimeBinding::create(config.runtime, config.runtime_binding, &binding);
    if (!status.ok()) {
        return status;
    }
    if (detail::ResourceBudget::memory_exhausted(binding->account())) {
        return Status::resource_exhausted("Local open exceeds Tree or host memory admission");
    }
    auto local = detail::reserved_shared<LocalBackendState>(binding->lifecycle_budget());
    status     = ScheduledPageStore::create(config.page_store, config.async_page_store, config.backend_owner, binding,
                                            binding->lifecycle_budget(), binding->backend_lane(), &local->store);
    if (!status.ok()) {
        return status;
    }
    status =
        LocalBackendDomain::claim(config.page_store, config.backend_owner, binding->lifecycle_budget(), &local->domain);
    if (!status.ok()) {
        return status;
    }
    detail::BackendOpenBudget backend_metadata;
    status = detail::bind_block_account(config.page_store, binding->account(),
                                        config.runtime->options().backend_workers, &backend_metadata);
    if (!status.ok()) {
        return status;
    }
    auto open = detail::reserved_shared<LocalOpen>(binding->lifecycle_budget(), config, binding, std::move(local),
                                                   std::move(complete));
    std::shared_ptr<detail::AllocationAccount> allocations;
    status = detail::AllocationAccount::create_ongoing(binding->account(), detail::BudgetClass::kRouting, &allocations);
    if (status.ok()) {
        status = detail::ongoing_backend_operation(config.page_store, &open->budget.backend);
    }
    if (!status.ok()) {
        return status;
    }
    open->budget.bytes = std::move(allocations);
    // The exclusive domain remains held across the size probe and recovery.
#ifdef CROWDB_HAVE_LIBURING
    if (auto *block = dynamic_cast<BlockAsyncPageStore *>(config.async_page_store);
        block != nullptr && config.page_store->block_size() != 0) {
        return open->probe_size(*block);
    }
#endif
    open->file_size = detail::backend_size(*config.page_store, open->budget.backend);
    open->schedule(&LocalOpen::recover);
    return Status::Ok();
}

catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (const std::exception &) {
    return Status::internal_error();
}
} // namespace

Status TreeCore::open_local_async(const Config &config, PageStore::RuntimeOpenComplete complete)
{
    auto status = validate_open(config);
    if (!status.ok() || !complete || !config.backend_owner) {
        return Status::invalid_argument();
    }
    if (dynamic_cast<MemPageStore *>(config.page_store) != nullptr) {
        return submit_local_open(config, std::move(complete));
    }
#ifdef CROWDB_HAVE_LIBURING
    if (config.page_store->block_size() != 0 &&
        dynamic_cast<BlockAsyncPageStore *>(config.async_page_store) != nullptr) {
        return submit_local_open(config, std::move(complete));
    }
#endif
    return Status::not_supported("asynchronous Local open requires prepared memory or file extents");
}

Status TreeCore::open(const Config &config, std::shared_ptr<TreeCore> *out)
{
    auto status = validate_open(config);
    if (!status.ok() || out == nullptr) {
        return Status::invalid_argument();
    }
    if (const auto factory = config.page_store->runtime_factory()) {
        return factory(config, out);
    }
    detail::HostCompletion done;
    Status                 terminal;
    status = submit_local_open(config, [&](Status result, std::shared_ptr<TreeCore> opened) noexcept {
        terminal = std::move(result);
        if (terminal.ok()) {
            *out = std::move(opened);
        }
        done.signal();
    });
    if (!status.ok()) {
        return status;
    }
    done.wait();
    return terminal;
}
} // namespace crowdb::tree
