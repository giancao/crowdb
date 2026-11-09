// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_dispatcher.h"
#include "native_import.h"
#include "portable_import.h"
#include "portable_import_plan.h"
#include "runtime_range_rebuild.h"
#include "snapshot/operation_wait.h"
#include "snapshot/recovered_state.h"
#include "tree_core_internal.h"
#include "tree_generation_lease.h"

namespace crowdb::tree
{
namespace
{
struct SealImport
{
    std::shared_ptr<detail::AllocationReservation> credit;

    ~SealImport()
    {
        if (credit) {
            credit->seal();
        }
    }
};

Status prepare_readable_graph(TreeCore &core, const std::shared_ptr<const ReadablePageCatalog> &catalog,
                              const std::shared_ptr<detail::AllocationReservation> &credit, SealImport &io,
                              std::shared_ptr<RecoveredStateOperation> *operation)
{
    auto reservation = detail::ResourceBudget::try_reserve(core.binding()->account(), detail::BudgetClass::kIo, 1);
    if (!reservation) {
        return Status::resource_exhausted("import validation IO admission unavailable");
    }
    io.credit = detail::reserved_shared<detail::AllocationReservation>(credit, std::move(*reservation));
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    auto                                           status = core.operation_lifetime().acquire(credit, &ticket);
    if (!status.ok()) {
        return status;
    }
    size_t max_frame_bytes = 0;
    bool   all_resident    = true;
    for (const auto &[pid, frame] : catalog->frames) {
        (void)pid;
        max_frame_bytes = std::max<size_t>(max_frame_bytes, frame.frame_bytes);
        all_resident    = all_resident && frame.backing && frame.backing->storage == BackingStorage::kResidentFrame;
    }
    auto loads  = all_resident ? resident_import_load_limits(
                                     core.load_limits(), std::max<size_t>(max_frame_bytes, core.config().frame_bytes))
                               : core.load_limits();
    auto loader = detail::reserved_shared<AsyncStorePageLoader>(
        credit, ticket->backend(core.async_backend()), core.binding()->cpu(), core.binding()->lane(), loads,
        OverflowBackingResolver{}, PageLoadBudget{.bytes = credit, .io = io.credit});
    return RecoveredStateOperation::create_readable(catalog, std::move(loader), core.binding()->cpu(),
                                                    core.binding()->lane(), {}, credit, operation);
}

struct ReadableInstall : std::enable_shared_from_this<ReadableInstall>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<ReadableInstall> owner;

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
            auto keep          = std::move(owner);
            keep->graph_status = std::move(status);
            keep->core.binding()->view_cpu_progress()->request(keep->ready, keep);
        }
    };

    using Connection = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    ReadableInstall(TreeCore &core, std::shared_ptr<RecoveredStateOperation> validation,
                    const std::shared_ptr<detail::AllocationReservation> &credit,
                    const std::shared_ptr<detail::AllocationReservation> &io, AsyncCompletion complete)
        : core(core),
          validation(std::move(validation)),
          credit{credit},
          io{io},
          complete(std::move(complete))
    {
    }

    void start()
    {
        connected = detail::reserved_construct<Connection>(core.binding()->lifecycle_budget(), [&](auto *storage) {
            ::new (static_cast<void *>(storage))
                Connection(stdexec::connect(validation->sender(), Receiver{.owner = shared_from_this()}));
        });
        stdexec::start(*connected);
    }

    static void validated(void *context) noexcept
    {
        static_cast<ReadableInstall *>(context)->install();
    }

    void install() noexcept
    try {
        std::shared_ptr<const PublishedTreeState> candidate;
        if (graph_status.ok()) {
            candidate = validation->result().state;
        }
        connected.reset();
        validation.reset();
        io.credit->seal();
        credit.credit->seal();
        auto status = std::move(graph_status);
        if (status.ok()) {
            status = TreeGenerationLease::prepare(core, std::move(candidate), nullptr, &lease);
        }
        if (status.ok()) {
            status = lease->acquire_async(
                {.context = this, .complete_fn = &ReadableInstall::acquired, .owner = weak_from_this().lock()});
        }
        if (!status.ok()) {
            finish(std::move(status));
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    static void acquired(void *context, Status status) noexcept
    {
        auto &self = *static_cast<ReadableInstall *>(context);
        if (status.ok()) {
            status = self.lease->commit();
        }
        self.finish(std::move(status));
    }

    void finish(Status status) noexcept
    {
        lease.reset();
        auto done = std::move(complete);
        done.complete(std::move(status));
    }

    TreeCore                                &core;
    std::shared_ptr<RecoveredStateOperation> validation;
    SealImport                               credit;
    SealImport                               io;
    AsyncCompletion                          complete;
    std::shared_ptr<TreeGenerationLease>     lease;
    std::shared_ptr<Connection>              connected;
    Status                                   graph_status;
    ApplyDispatcher::Node                    ready{&ReadableInstall::validated, this};
};
} // namespace

Status TreeCore::prepare_import_credit(std::shared_ptr<detail::AllocationReservation> *out)
{
    return prepare_import_credit(SIZE_MAX, out);
}

Status TreeCore::prepare_import_credit(size_t requested, std::shared_ptr<detail::AllocationReservation> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree import ingress closed or replacing");
    }
    // Diagnostic reserved/allocated samples are not coherent during credit
    // conversion. Plan from admission usage and include shared host pressure.
    const auto available = detail::ResourceBudget::available(binding()->account(), detail::BudgetClass::kReclaim);
    if (requested == SIZE_MAX) {
        requested = available;
    }
    if (requested > available) {
        return Status::resource_exhausted("import staging exceeds available reclaim capacity");
    }
    auto bytes = detail::ResourceBudget::try_reserve(binding()->account(), detail::BudgetClass::kReclaim, requested);
    if (!bytes) {
        return Status::resource_exhausted("import staging admission unavailable");
    }
    *out = detail::AllocationReservation::create(std::move(*bytes));
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("import admission allocation failed");
}

Status TreeCore::install_readable(std::shared_ptr<const ReadablePageCatalog>            catalog,
                                  const std::shared_ptr<detail::AllocationReservation> &credit)
{
    std::shared_ptr<const PublishedTreeState> candidate;
    auto                                      status = prepare_readable(std::move(catalog), credit, &candidate);
    return status.ok() ? install_generation(std::move(candidate)) : status;
}

Status TreeCore::install_readable_async(std::shared_ptr<const ReadablePageCatalog>            catalog,
                                        const std::shared_ptr<detail::AllocationReservation> &credit,
                                        AsyncCompletion                                       complete)
try {
    SealImport seal{credit};
    if (!catalog || !credit || credit->account() != binding()->account() || !complete) {
        return Status::invalid_argument();
    }
    std::shared_ptr<ReadableInstall> install;
    {
        State::Ingress ingress(*state_);
        if (ingress.owner == nullptr) {
            return Status::unavailable("Tree import ingress closed or replacing");
        }
        SealImport                               io;
        std::shared_ptr<RecoveredStateOperation> operation;
        auto status = prepare_readable_graph(*this, catalog, credit, io, &operation);
        if (!status.ok()) {
            return status;
        }
        install = detail::reserved_shared<ReadableInstall>(binding()->lifecycle_budget(), *this, std::move(operation),
                                                           credit, io.credit, std::move(complete));
        io.credit.reset();
    }
    catalog.reset();
    seal.credit.reset();
    install->start();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status TreeCore::prepare_readable(std::shared_ptr<const ReadablePageCatalog>            catalog,
                                  const std::shared_ptr<detail::AllocationReservation> &credit,
                                  std::shared_ptr<const PublishedTreeState>            *out)
try {
    SealImport seal{credit};
    if (!catalog || !credit || credit->account() != binding()->account() || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    std::shared_ptr<const PublishedTreeState> candidate;
    {
        State::Ingress ingress(*state_);
        if (ingress.owner == nullptr) {
            return Status::unavailable("Tree import ingress closed or replacing");
        }
        SealImport                               io;
        std::shared_ptr<RecoveredStateOperation> operation;
        auto status = prepare_readable_graph(*this, catalog, credit, io, &operation);
        if (!status.ok()) {
            return status;
        }
        status = detail::wait_snapshot_operation(operation);
        if (!status.ok()) {
            return status;
        }
        candidate = operation->result().state;
    }
    // The candidate retains exact page dependencies; discard the staging catalog
    // before returning unused admission and reserving replacement controls.
    catalog.reset();
    credit->seal();
    *out = std::move(candidate);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("import validation allocation failed");
}

Status TreeCore::install_native(std::vector<NativeFrame> frames, NativeStreamInfo info)
try {
    SealImport budget;
    auto       status = prepare_import_credit(&budget.credit);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<ReadablePageCatalog> catalog;
    status = build_native_catalog(frames, info, config(), state_->loads, {}, budget.credit, &catalog);
    return status.ok() ? install_readable(std::move(catalog), budget.credit) : status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("native import staging allocation failed");
}

Status TreeCore::install_portable(std::vector<leaf_entry> entries, uint64_t coverage)
try {
    PortableImportPlan plan(config(), state_->loads);
    for (const auto &entry : entries) {
        const CellView cell{Slice(entry.cell)};
        if (!cell.valid()) {
            return Status::invalid_argument("portable import cell has no complete header");
        }
        const auto status = plan.append(entry.key.size(), cell.value().size(), cell.is_tombstone());
        if (!status.ok()) {
            return status;
        }
    }
    const auto shape = plan.finish();
    if (!shape) {
        return Status::resource_exhausted("portable import shape exceeds its bounded frame namespace");
    }
    RuntimeRangeImport budget;
    auto               status = reserve_range_shape(*this, *shape, true, budget);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<ReadablePageCatalog> catalog;
    {
        PortableImportBuilder builder(config(), state_->loads, coverage, budget.credit);
        for (const auto &entry : entries) {
            status = builder.append(Slice(entry.key), Slice(entry.cell));
            if (!status.ok()) {
                return status;
            }
        }
        status = builder.finish(&catalog);
    }
    return status.ok() ? install_readable(std::move(catalog), budget.credit) : status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("portable import staging allocation failed");
}
} // namespace crowdb::tree
