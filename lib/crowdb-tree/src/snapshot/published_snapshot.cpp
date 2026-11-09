// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot.h"

#include "backend/local/block_account.h"
#include "backend/local/local_backend_state.h"
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "btree/tree_core.h"

namespace crowdb::tree
{
namespace
{
struct SnapshotBackendOwner
{
    std::shared_ptr<TreeRuntimeBinding> binding;
    std::shared_ptr<LocalBackendDomain> domain;
    std::shared_ptr<const void>         backend;
    std::shared_ptr<AsyncPageStore>     scheduled;
};
} // namespace

struct PublishedReadViewContext::State : std::enable_shared_from_this<State>
{
    static void reads_drained(void *context, Status /*status*/) noexcept
    {
        auto &self = *static_cast<State *>(context);
        self.lane->close_async({.context = &self, .complete_fn = &cpu_drained, .owner = self.weak_from_this().lock()});
    }

    static void cpu_drained(void *context, Status /*status*/) noexcept
    {
        auto &self = *static_cast<State *>(context);
        if (self.backend_lane) {
            self.backend_lane->close_async(
                {.context = &self, .complete_fn = &backend_drained, .owner = self.weak_from_this().lock()});
        }
        else {
            backend_drained(context, Status::Ok());
        }
    }

    static void backend_drained(void *context, Status /*status*/) noexcept
    {
        auto &self = *static_cast<State *>(context);
        self.close_complete(self.close_context);
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<const PublishedTreeState>      view;
    std::shared_ptr<TreeRuntimeBinding>            binding;
    std::shared_ptr<detail::FlushRuntime::Lane>    lane;
    std::shared_ptr<detail::FlushRuntime::Lane>    backend_lane;
    std::shared_ptr<SnapshotBackendOwner>          owner;
    detail::ReservedUnique<TreeOperationLifetime>  lifetime;
    std::weak_ptr<TreeCore>                        core;
    std::shared_ptr<TreeWorkMetrics>               work_metrics;
    AsyncPageStore                                *backend = nullptr;
    PageLoadLimits                                 loads;
    PublishedReadLimits                            reads;
    PageKeyRange                                   range;
    bool                                           detached = false;
    std::atomic_flag                               closing  = ATOMIC_FLAG_INIT;
    void (*close_complete)(void *)                          = nullptr;
    void *close_context                                     = nullptr;
};

Status PublishedReadViewContext::close_async(void (*complete)(void *), void *context) const noexcept
{
    auto state = state_;
    if (complete == nullptr) {
        return Status::invalid_argument();
    }
    if (!state->lifetime) {
        return Status::not_supported();
    }
    if (state->closing.test_and_set(std::memory_order_acq_rel)) {
        return Status::unavailable();
    }
    state->close_complete = complete;
    state->close_context  = context;
    state->lifetime->close_async({.context = state.get(), .complete_fn = &State::reads_drained, .owner = state});
    return Status::Ok();
}

bool PublishedReadViewContext::closed() const noexcept
{
    return state_->lifetime && state_->lifetime->closed();
}

SnapshotReadWindow::~SnapshotReadWindow()
{
    seal();
}

SnapshotReadWindow &SnapshotReadWindow::operator=(SnapshotReadWindow &&other) noexcept
{
    if (this != &other) {
        seal();
        loader.reset();
        backend.reset();
        ticket.reset();
        budget = {};
        host_core.reset();
        host_core = std::move(other.host_core);
        budget    = std::move(other.budget);
        ticket    = std::move(other.ticket);
        backend   = std::move(other.backend);
        loader    = std::move(other.loader);
    }
    return *this;
}

void SnapshotReadWindow::seal() noexcept
{
    detail::seal_backend_budget(budget.backend);
    if (budget.io) {
        budget.io->seal();
    }
    if (auto reserved = budget.reservation()) {
        reserved->seal();
    }
}

size_t PublishedReadViewContext::allocation_bound() noexcept
{
    using namespace detail;
    return shared_allocation_bound<State>() + shared_allocation_bound<PublishedReadViewContext>() +
           shared_allocation_bound<AllocationReservation>() + shared_allocation_bound<PublishedTreeState>() +
           PublishedSources::allocation_bound() + shared_allocation_bound<FlushRuntime::Lane>() +
           shared_allocation_bound<SnapshotBackendOwner>() + sizeof(TreeOperationLifetime) +
           TreeOperationLifetime::allocation_bound() + TreeOperationLifetime::ticket_allocation_bound();
}

size_t PublishedReadViewContext::window_control_bound() noexcept
{
    return (2 * detail::shared_allocation_bound<detail::AllocationReservation>()) +
           TreeOperationLifetime::ticket_allocation_bound();
}

Status PublishedReadViewContext::capture(const std::shared_ptr<TreeCore>           &core,
                                         std::shared_ptr<PublishedReadViewContext> *out)
{
    return core ? core->capture_read_only_view(out)
                : Status::invalid_argument("snapshot capture requires an open Core");
}

Status PublishedReadViewContext::capture_admitted(const std::shared_ptr<TreeCore>           &core,
                                                  std::shared_ptr<PublishedReadViewContext> *out,
                                                  std::shared_ptr<const PublishedTreeState>  captured)
try {
    using namespace detail;
    if (!core || out == nullptr || *out) {
        return Status::invalid_argument("snapshot capture requires an open Core and empty output");
    }
    auto binding = core->binding();
    auto bound   = AllocationBound(allocation_bound())
                       .add(ScheduledPageStore::allocation_bound())
                       .add(shared_allocation_bound<FlushRuntime::Lane>())
                       .add_product(2, OwnedKey::allocation_overhead() + 1)
                       .add(core->config().key_range.start() ? core->config().key_range.start()->size() : 0)
                       .add(core->config().key_range.end() ? core->config().key_range.end()->size() : 0)
                       .value();
    if (!bound) {
        return Status::resource_exhausted("snapshot backend control bound overflow");
    }
    auto credit = ResourceBudget::try_reserve(binding->account(), BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot context reserve unavailable");
    }
    auto                                           metadata = AllocationReservation::create(std::move(*credit));
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto                                           status = core->operation_lifetime().acquire(metadata, &ticket);
    if (!status.ok()) {
        return status;
    }
    auto state      = reserved_shared<State>(metadata);
    state->metadata = metadata;
    if (core->config().key_range.start()) {
        state->range.lower = OwnedKey(Slice(*core->config().key_range.start()), metadata);
    }
    if (core->config().key_range.end()) {
        state->range.upper = OwnedKey(Slice(*core->config().key_range.end()), metadata);
    }
    state->work_metrics = core->publisher().work_metrics();
    state->binding      = std::move(binding);
    state->lane         = reserved_shared<FlushRuntime::Lane>(metadata, state->binding->options().cpu_queue_limit);
    if (!captured) {
        captured = core->publisher().capture_complete();
    }
    auto view      = reserved_shared<PublishedTreeState>(metadata, *captured);
    auto sources   = reserved_shared<PublishedSources>(metadata, metadata);
    sources->floor = view->coverage;
    view->sources  = std::move(sources);
    state->view    = std::move(view);
    state->loads   = core->load_limits();

    state->reads               = core->read_limits();
    state->reads.max_memtables = 1;
    state->backend             = core->config().async_page_store;
    state->detached            = (state->backend == nullptr && core->config().page_store == nullptr) ||
                                 static_cast<bool>(core->config().backend_owner);
    if (state->detached) {
        state->owner          = reserved_shared<SnapshotBackendOwner>(metadata);
        state->owner->binding = state->binding;
        state->owner->backend = core->config().backend_owner;
        if (const auto &local = core->local_backend()) {
            state->owner->domain = local->domain;
        }
        if (core->config().page_store != nullptr || state->backend != nullptr) {
            state->backend_lane =
                reserved_shared<FlushRuntime::Lane>(metadata, state->binding->options().backend_queue_limit);
            status = ScheduledPageStore::create(core->config().page_store, state->backend, core->config().backend_owner,
                                                state->binding, metadata, state->backend_lane, &state->owner->scheduled,
                                                state->binding->view_backend_progress());
            if (!status.ok()) {
                return status;
            }
            // The ticket aliases this store. Keep the wrapper in the ticket's
            // backend owner so dropping the view cannot invalidate a window.
            state->backend = state->owner->scheduled.get();
        }
        state->lifetime = reserved_unique<TreeOperationLifetime>(metadata, metadata, state->owner);
    }
    else {
        state->core = core;
    }
    *out = reserved_construct<PublishedReadViewContext>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) PublishedReadViewContext(std::move(state));
    });
    metadata->seal();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot context allocation failed");
}

Status PublishedReadViewContext::prepare_window(size_t bytes, SnapshotReadWindow *out) const
try {
    using namespace detail;
    if (out == nullptr || out->host_core || out->budget.bytes || out->loader || out->ticket) {
        return Status::invalid_argument("snapshot read window output must be empty");
    }
    auto bound = AllocationBound(bytes).add(window_control_bound()).value();
    if (!bound) {
        return Status::resource_exhausted("snapshot window bound overflow");
    }
    auto credit = ResourceBudget::try_reserve(state_->binding->account(), BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot read window reserve unavailable");
    }
    SnapshotReadWindow window;
    window.budget.bytes = AllocationReservation::create(std::move(*credit));

    window.budget.work_metrics = state_->work_metrics;
    window.budget.cause        = TreeWorkCause::Snapshot;
    return finish_window(std::move(window), out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot read window allocation failed");
}

Status PublishedReadViewContext::prepare_window(SnapshotReadWindow *out) const
try {
    using namespace detail;
    if (out == nullptr || out->host_core || out->budget.bytes || out->loader || out->ticket) {
        return Status::invalid_argument("snapshot read window output must be empty");
    }
    SnapshotReadWindow window;
    window.budget.bytes        = state_->reads.metadata;
    window.budget.work_metrics = state_->work_metrics;
    if (state_->detached) {
        window.budget.cpu_progress = state_->binding->view_cpu_progress();
    }
    window.budget.cause = TreeWorkCause::Snapshot;
    return finish_window(std::move(window), out);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot read window allocation failed");
}

Status PublishedReadViewContext::finish_window(SnapshotReadWindow window, SnapshotReadWindow *out) const
try {
    using namespace detail;
    Status          status;
    AsyncPageStore *backend = state_->backend;
    if (state_->detached) {
        status = state_->lifetime->acquire(window.budget.bytes, &window.ticket);
    }
    else {
        window.host_core = state_->core.lock();
        if (window.host_core) {
            status = window.host_core->acquire_snapshot_window(window.budget.bytes, &window.ticket, &backend);
        }
        else {
            status = Status::unavailable("snapshot source Tree closed");
        }
    }
    if (!status.ok()) {
        return status;
    }
    window.backend = window.ticket->backend(backend);
    window.loader =
        reserved_shared<AsyncStorePageLoader>(window.budget.bytes, window.backend, state_->binding->cpu(), state_->lane,
                                              state_->loads, OverflowBackingResolver{}, window.budget);
    *out = std::move(window);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot read window allocation failed");
}

const std::shared_ptr<const PublishedTreeState> &PublishedReadViewContext::view() const noexcept
{
    return state_->view;
}

const std::shared_ptr<TreeRuntimeBinding> &PublishedReadViewContext::binding() const noexcept
{
    return state_->binding;
}

const std::shared_ptr<detail::FlushRuntime::Lane> &PublishedReadViewContext::lane() const noexcept
{
    return state_->lane;
}

PageLoadLimits PublishedReadViewContext::load_limits() const noexcept
{
    return state_->loads;
}

PublishedReadLimits PublishedReadViewContext::read_limits() const noexcept
{
    auto       limits          = state_->reads;
    const auto shape           = state_->view->mapping.copy_bounds();
    limits.page_metadata_bytes = shape.max_leaf_bytes;
    return limits;
}

const PageKeyRange &PublishedReadViewContext::range() const noexcept
{
    return state_->range;
}

bool PublishedReadViewContext::detached() const noexcept
{
    return state_->detached;
}
} // namespace crowdb::tree
