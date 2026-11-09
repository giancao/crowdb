// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_backend_state.h"
#include "backend/chunk/chunk_protocol_operation.h"
#include "backend/chunk/chunk_write_queue.h"
#include "btree/allocation_account.h"
#include "btree/apply_dispatcher.h"
#include "btree/backing_identity.h"
#include "btree/tree_core.h"
#include "btree/tree_core_geometry.h"
#include "host_completion.h"
#include "runtime/tree_runtime_internal.h"
#include "snapshot/chunk_root_recovery.h"
#include "snapshot/recovered_state.h"

namespace crowdb::tree
{
namespace
{
struct RootLoad : std::enable_shared_from_this<RootLoad>
{
    explicit RootLoad(const std::shared_ptr<AllocationBudget> &metadata)
        : metadata(metadata),
          bytes(MetadataAllocator<uint8_t>(metadata)),
          root(metadata)
    {
    }

    std::shared_ptr<const ChunkBackingDomain> domain;
    std::shared_ptr<AllocationBudget>         metadata;
    std::shared_ptr<ScheduledPageStore>       cpu;
    AsyncCompletion                           completion;
    detail::ChunkRootBytes                    bytes;
    ChunkRootManifest                         root;
    uint64_t                                  generation        = 0;
    uint64_t                                  latest_generation = 0;
    bool                                      loaded_current    = false;
    bool                                      absent            = false;
    bool                                      done              = false;

    void submit_step(bool stopped, std::shared_ptr<ScheduledPageStore> store, AsyncCompletion complete)
    {
        if (stopped) {
            complete.complete(Status::unavailable());
            return;
        }
        auto *catalog = domain->catalog->protection();
        auto  status  = catalog->check_read_owner();
        if (!status.ok()) {
            complete.complete(std::move(status));
            return;
        }
        cpu                  = std::move(store);
        completion           = std::move(complete);
        const auto requested = loaded_current ? generation : 0;
        catalog->submit_load_root_owned(
            domain->tree, requested, &bytes,
            {.context = this,
             .complete_fn =
                 +[](void *raw, Status status) {
                     auto self      = static_cast<RootLoad *>(raw)->shared_from_this();
                     auto done      = std::exchange(self->completion, {});
                     auto submitted = self->cpu->submit_step(
                         [self, status = std::move(status)]() mutable { return self->loaded(std::move(status)); },
                         done);
                     if (!submitted.ok()) {
                         done.complete(std::move(submitted));
                     }
                 },
             .owner       = shared_from_this(),
             .allocations = metadata});
    }

    Status loaded(Status status)
    {
        if (status.code() == Code::kNotFound && generation == 0) {
            absent = true;
            done   = true;
            return Status::Ok();
        }
        if (!status.ok()) {
            return status;
        }
        root.indexes.clear();
        status = decode_chunk_root(bytes, domain->tree, loaded_current ? generation : 0, &root);
        if (status.ok() && !loaded_current) {
            latest_generation = root.identity.root_generation;
            loaded_current    = true;
            if (generation != 0 && generation != latest_generation) {
                return Status::Ok();
            }
        }
        if (status.ok()) {
            status = domain->catalog->protection()->check_read_owner();
        }
        done = status.ok();
        return status;
    }
};

struct ChunkOpen : std::enable_shared_from_this<ChunkOpen>
{
    using Step = void (ChunkOpen::*)();

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<ChunkOpen> owner;
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
            // The next step can release this connection on another worker.
            auto keep = std::move(owner);
            keep->schedule(next, std::move(status));
        }
    };

    using Connection = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    ChunkOpen(Config config, std::shared_ptr<TreeRuntimeBinding> binding, std::shared_ptr<ChunkBackendState> backend,
              PageStore::RuntimeOpenComplete complete)
        : config(std::move(config)),
          binding(std::move(binding)),
          backend(std::move(backend)),
          complete(std::move(complete))
    {
    }

    void schedule(Step step, Status status) noexcept
    {
        next  = step;
        event = std::move(status);
        binding->view_cpu_progress()->request(ready, weak_from_this().lock());
    }

    static void run(void *context) noexcept
    {
        auto      &open   = *static_cast<ChunkOpen *>(context);
        const auto step   = open.next;
        auto       status = std::move(open.event);
        open.connected.reset();
        open.protocol.reset();
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

    static void registry_ready(void *context, Status status) noexcept
    {
        static_cast<ChunkOpen *>(context)->schedule(&ChunkOpen::load_root, std::move(status));
    }

    void start(detail::CallbackSender sender, Step next_step)
    {
        connected = detail::reserved_construct<Connection>(budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage))
                Connection(stdexec::connect(sender, Receiver{.owner = shared_from_this(), .next = next_step}));
        });
        stdexec::start(*connected);
    }

    template <class Protocol, class Done>
    void start_protocol(const std::shared_ptr<Protocol> &operation, Done done, Step next_step)
    {
        auto status = ChunkProtocolOperation::create_async(
            store, binding->cpu(), binding->lane(), budget, operation,
            [operation, scheduled = std::static_pointer_cast<ScheduledPageStore>(store)](bool            stopped,
                                                                                         AsyncCompletion complete) {
                operation->submit_step(stopped, scheduled, std::move(complete));
            },
            std::move(done), &protocol);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        start(protocol->sender(), next_step);
    }

    void load_root()
    {
        auto       *facade   = backend->facade;
        const auto &metadata = binding->lifecycle_budget();
        auto        status   = facade->make_domain(detail::allocate_backend_identity(), metadata, &backend->domain);
        if (status.ok()) {
            status = facade->prepare_writer(metadata);
        }
        if (status.ok()) {
            backend->writer_session = facade->take_writer();
            status                  = detail::ChunkRuntimeIds::create(backend->domain->epoch, metadata, &backend->ids);
        }
        if (status.ok()) {
            status = ScheduledPageStore::create(facade, nullptr, config.backend_owner, binding, metadata,
                                                binding->backend_lane(), &backend->store, {}, &ChunkWriteQueue::create);
        }
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        std::shared_ptr<detail::AllocationAccount> allocations;
        status =
            detail::AllocationAccount::create_ongoing(binding->account(), detail::BudgetClass::kRouting, &allocations);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        budget.bytes        = std::move(allocations);
        budget.cpu_progress = binding->backend_progress();
        lifetime.emplace(metadata, backend->store);
        status = lifetime->acquire(budget.bytes, &ticket);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        store            = ticket->backend(backend->store.get());
        root             = detail::reserved_shared<RootLoad>(budget.bytes, budget.bytes);
        root->domain     = backend->domain;
        root->generation = facade->options().open_generation;
        start_protocol(root, [loaded = root] { return loaded->done; }, &ChunkOpen::recover_packs);
    }

    void recover_packs()
    {
        if (root->absent) {
            finish(Status::Ok());
            return;
        }
        identity               = root->root.identity;
        limits.max_frame_bytes = std::max(limits.max_frame_bytes, physical_frame_capacity(config));
        limits.max_blob_bytes  = round_up_to_iu(limits.max_frame_bytes + kDurableBlobHeader, backend->iu_size);
        auto status            = ChunkRootRecovery::create(std::move(root->root), backend->domain, backend->ids, limits,
                                                           budget.bytes, budget.bytes, &recovery);
        root.reset();
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        start_protocol(recovery, [recovered = recovery] { return recovered->done(); }, &ChunkOpen::validate_graph);
    }

    void validate_graph()
    {
        PageLoadLimits loads;
        loads.max_frame_bytes      = limits.max_frame_bytes;
        loads.max_blob_bytes       = limits.max_blob_bytes;
        loads.max_chunk_blob_bytes = limits.max_blob_bytes;
        auto loader                = detail::reserved_shared<AsyncStorePageLoader>(
            budget.bytes, store, binding->cpu(), binding->lane(), loads, OverflowBackingResolver{}, budget);
        RecoveredStateLimits graph_limits;
        graph_limits.max_pages = std::max<size_t>(1, recovery->pages()->frames.size());
        auto status = RecoveredStateOperation::create(recovery->pages(), loader, binding->cpu(), binding->lane(),
                                                      graph_limits, budget.bytes, &graph);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        start(graph->sender(), &ChunkOpen::publish_graph);
    }

    void publish_graph()
    {
        initial                  = graph->result().state;
        backend->max_frame_bytes = recovery->max_frame_bytes();
        backend->max_blob_bytes  = recovery->max_blob_bytes();
        backend->recovered(identity, recovery->take_manifest());
        finish(Status::Ok());
    }

    void finish(Status status) noexcept
    {
        outcome = std::move(status);
        connected.reset();
        protocol.reset();
        graph.reset();
        recovery.reset();
        root.reset();
        store.reset();
        ticket.reset();
        if (!lifetime) {
            schedule(&ChunkOpen::install, Status::Ok());
            return;
        }
        lifetime->close_async({.context = this,
                               .complete_fn =
                                   [](void *context, Status) noexcept {
                                       static_cast<ChunkOpen *>(context)->schedule(&ChunkOpen::install, Status::Ok());
                                   },
                               .owner = weak_from_this().lock()});
    }

    void install() noexcept
    {
        lifetime.reset();
        budget = {};
        std::shared_ptr<TreeCore> opened;
        try {
            if (outcome.ok()) {
                outcome = TreeCore::create(std::move(config), binding,
                                           {.initial = std::move(initial), .local = {}, .chunk = std::move(backend)},
                                           &opened, true);
            }
        }
        catch (const std::bad_alloc &) {
            outcome = Status::resource_exhausted();
        }
        catch (...) {
            outcome = Status::internal_error();
        }
        backend.reset();
        auto done = std::move(complete);
        done(std::move(outcome), std::move(opened));
    }

    Config                                         config;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<ChunkBackendState>             backend;
    PageStore::RuntimeOpenComplete                 complete;
    ApplyDispatcher::Node                          ready{&ChunkOpen::run, this};
    Step                                           next = nullptr;
    Status                                         event;
    Status                                         outcome;
    PageLoadBudget                                 budget;
    std::optional<TreeOperationLifetime>           lifetime;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AsyncPageStore>                store;
    std::shared_ptr<RootLoad>                      root;
    std::shared_ptr<ChunkProtocolOperation>        protocol;
    std::shared_ptr<ChunkRootRecovery>             recovery;
    std::shared_ptr<RecoveredStateOperation>       graph;
    std::shared_ptr<const PublishedTreeState>      initial;
    std::shared_ptr<Connection>                    connected;
    ChunkRootIdentity                              identity;
    ChunkRecoveryLimits                            limits;
};

Status submit_chunk_open(const Config &config, PageStore::RuntimeOpenComplete complete)
try {
    auto *facade = dynamic_cast<detail::ChunkRuntimePageStore *>(config.page_store);
    if (!complete || facade == nullptr || !facade->supported() || !config.backend_owner ||
        config.async_page_store != nullptr) {
        return Status::invalid_argument();
    }
    if (!config.runtime || !facade->matches_host(TreeRuntimeAccess::lifecycle(*config.runtime).get()) ||
        config.frame_bytes == 0 || config.frame_bytes % facade->iu_size() != 0) {
        return Status::invalid_argument();
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
    auto chunk           = detail::reserved_shared<ChunkBackendState>(binding->lifecycle_budget());
    chunk->facade        = facade;
    chunk->backend_owner = config.backend_owner;
    chunk->iu_size       = facade->iu_size();
    if (!facade->try_claim_runtime_domain()) {
        return Status::unavailable();
    }
    chunk->owns_writer_domain = true;
    auto open = detail::reserved_shared<ChunkOpen>(binding->lifecycle_budget(), config, binding, std::move(chunk),
                                                   std::move(complete));
    return facade->submit_registry(binding->lifecycle_budget(),
                                   {.complete_fn = &ChunkOpen::registry_ready, .context = open.get(), .owner = open});
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}
} // namespace

Status TreeCore::open_chunk_async(const Config &config, PageStore::RuntimeOpenComplete complete)
{
    return submit_chunk_open(config, std::move(complete));
}

Status TreeCore::open_chunk(const Config &config, std::shared_ptr<TreeCore> *out)
{
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    detail::HostCompletion done;
    Status                 terminal;
    auto status = open_chunk_async(config, [&](Status result, std::shared_ptr<TreeCore> opened) noexcept {
        terminal = std::move(result);
        *out     = std::move(opened);
        done.signal();
    });
    if (!status.ok()) {
        return status;
    }
    done.wait();
    return terminal;
}
} // namespace crowdb::tree
