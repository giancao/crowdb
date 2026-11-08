// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "page_load.h"
#include "page_store/page_block_io.h"
#include "resource_observation.h"
#include "tree_metrics.h"

namespace crowdb::tree
{
struct PageLoadProgress
{
    PageLoadProgress(const PageLoadBudget &budget, uint64_t started_at_ns)
        : metrics(budget.work_metrics, TreeStage::PageLoadPipeline, started_at_ns),
          backend_budget(budget.backend),
          cause(budget.cause)
    {
    }

    PageLoadProgress(const PageLoadProgress &)            = delete;
    PageLoadProgress &operator=(const PageLoadProgress &) = delete;

    TreeMetricsOperation              metrics;
    std::shared_ptr<AllocationBudget> backend_budget;
    std::shared_ptr<void>             loader_owner;
    TreeWorkCause                     cause;
};

inline void record_page_load_terminal(const PageLoadBudget &budget, uint64_t started_at_ns,
                                      const Status &status) noexcept
{
    TreeMetricsOperation metrics(budget.work_metrics, TreeStage::PageLoadPipeline, started_at_ns);
    (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
}

// One immutable callback identity per read, including every overflow step.
// Decoding waits for submit_read to return, so inline success followed by an
// exception cannot publish a result or start another read before rejection.
struct PageReadTicket
{
    PageReadTicket(std::shared_ptr<const void> lifetime, void *operation, AsyncCompleteFn callback,
                   const std::shared_ptr<AllocationBudget> &metadata, size_t bytes)
        : resources(metadata, TreeResourceMetric::ReadInflightBytes, bytes),
          owner(std::move(lifetime)),
          context(operation),
          complete(callback)
    {
    }

    ResourceObservation         resources;
    std::shared_ptr<const void> owner;
    void                       *context;
    AsyncCompleteFn             complete;
    Status                      status;
    std::atomic<bool>           callback_claimed{false};
    std::atomic<unsigned>       events{0};

    static void completed(void *context, Status result)
    {
        auto *ticket = static_cast<PageReadTicket *>(context);
        if (ticket->callback_claimed.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
        ticket->resources.reset();
        ticket->status = std::move(result);
        if ((ticket->events.fetch_or(1, std::memory_order_acq_rel) & 2U) != 0) {
            ticket->complete(ticket->context, std::move(ticket->status));
        }
    }

    void submitted()
    {
        if ((events.fetch_or(2, std::memory_order_acq_rel) & 1U) != 0) {
            complete(context, std::move(status));
        }
    }
};

template <class Load>
void queue_page_load(const std::shared_ptr<Load> &operation, detail::FlushRuntime::Task task) noexcept
{
    operation->queued_owner = operation;
    Status status;
    try {
        status = operation->runtime->try_submit(operation->lane, task, operation.get());
    }
    catch (...) {
        status = Status::resource_exhausted();
    }
    if (!status.ok()) {
        operation->queued_owner.reset();
        operation->finish(std::move(status));
    }
}

template <class Load> void page_read_completed(void *context, Status status) noexcept
{
    auto operation = static_cast<Load *>(context)->shared_from_this();
    if (operation->terminal.load(std::memory_order_acquire)) {
        return;
    }
    if (!status.ok()) {
        operation->finish(std::move(status));
    }
    else {
        queue_page_load(operation, &Load::decode);
    }
}

template <class Load>
void submit_page_read(const std::shared_ptr<Load> &operation, std::shared_ptr<const PageBlockRef> page_block,
                      void *buffer, size_t bytes, AsyncCompleteFn complete = &page_read_completed<Load>)
{
    auto ticket = detail::reserved_shared<PageReadTicket>(operation->metadata, operation, operation.get(), complete,
                                                          operation->metadata, operation->blob.capacity());
    try {
        submit_page_block_read(*operation->store, std::move(page_block), buffer, bytes, operation->metadata,
                               {.context     = ticket.get(),
                                .complete_fn = &PageReadTicket::completed,
                                .owner       = ticket,
                                .allocations = operation->backend_budget,
                                .cause       = operation->cause});
    }
    catch (const std::bad_alloc &) {
        operation->finish(Status::resource_exhausted());
    }
    catch (...) {
        operation->finish(Status::io_error());
    }
    // An uncertain submit retains the ticket and operation until the page store
    // releases its owner. A late callback cannot decode or finish twice.
    ticket->submitted();
}
} // namespace crowdb::tree
