// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "scheduled_page_store.h"

#include "btree/allocation_account.h"
#include "btree/allocation_bound.h"
#include "btree/tree_metrics.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/page_store/page_disk_store.h"
#include "page_store/local/block_account.h"

#include <cassert>
#include <typeinfo>

namespace crowdb::tree
{
struct ScheduledPageStore::State
{
    PageStore                                  *sync         = nullptr;
    AsyncPageStore                             *async        = nullptr;
    PageMemStore                               *memory       = nullptr;
    bool                                        direct_block = false;
    std::shared_ptr<const void>                 page_store_owner;
    std::shared_ptr<TreeRuntimeBinding>         binding;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    std::shared_ptr<ApplyDispatcher>            extent_owner;
    std::shared_ptr<AllocationBudget>           jobs;
    std::shared_ptr<AllocationBudget>           backend_allocations;
    std::shared_ptr<ChunkWriteQueue>            chunks;
};

struct ScheduledPageStore::Job
{
    // The consumer's read ticket must remain alive until the page store owner is released.
    AsyncCompletion                               consumer;
    std::shared_ptr<State>                        state;
    std::shared_ptr<Job>                          queued_owner;
    detail::InlineFunction<Status()>              step;
    detail::InlineFunction<void(AsyncCompletion)> async_step;
    PageAddr                                      address     = 0;
    void                                         *read        = nullptr;
    const void                                   *write       = nullptr;
    size_t                                        length      = 0;
    PagePurpose                                   purpose     = PagePurpose::kBtreePage;
    size_t                                        read_bytes  = 0;
    size_t                                        write_bytes = 0;
    bool                                          io_started  = false;
    std::atomic<bool>                             completed{false};
    bool                                          admitted = false;
    ApplyDispatcher::Node                         node{&Job::execute, this};

    ~Job()
    {
        if (admitted) {
            finish(Status::unavailable("backend dropped its accepted completion"));
        }
    }

    void finish(Status status, bool callback = false) noexcept
    {
        if (completed.exchange(true, std::memory_order_acq_rel)) {
            if (callback) {
                record_work(state->binding->work_metrics(), TreeWorkCounter::StaleCallbacks, 1, consumer.cause);
            }
            return;
        }
        if (io_started) {
            const auto &metrics = state->binding->work_metrics();
            if (read_bytes != 0) {
                record_work(metrics, status.ok() ? TreeWorkCounter::ReadBytes : TreeWorkCounter::ReadErrors,
                            status.ok() ? read_bytes : 1, consumer.cause);
            }
            if (write_bytes != 0) {
                record_work(metrics, status.ok() ? TreeWorkCounter::WriteBytes : TreeWorkCounter::WriteErrors,
                            status.ok() ? write_bytes : 1, consumer.cause);
            }
        }
        try {
            consumer.complete(std::move(status));
        }
        catch (...) {
            CRB_LOG_ERROR("backend completion receiver threw after terminal");
        }
    }

    static void completed_io(void *context, Status status)
    {
        static_cast<Job *>(context)->finish(std::move(status), true);
    }

    static void execute(void *context) noexcept
    {
        auto *job  = static_cast<Job *>(context);
        auto  self = std::move(job->queued_owner);
        try {
            job->io_started = true;
            record_work(job->state->binding->work_metrics(), TreeWorkCounter::ReadOperations,
                        static_cast<uint64_t>(job->read_bytes != 0), job->consumer.cause);
            record_work(job->state->binding->work_metrics(), TreeWorkCounter::WriteOperations,
                        static_cast<uint64_t>(job->write_bytes != 0), job->consumer.cause);
            if (job->step) {
                job->finish(job->step());
                return;
            }
            {
                AsyncCompletion callback{.context     = job,
                                         .complete_fn = &completed_io,
                                         .owner       = self,
                                         .allocations = job->consumer.allocations ? job->consumer.allocations
                                                                                  : job->state->backend_allocations,
                                         .cause       = job->consumer.cause};
                if (job->async_step) {
                    job->async_step(std::move(callback));
                    return;
                }
                if (job->state->async != nullptr) {
                    if (job->read != nullptr) {
                        job->state->async->submit_read(job->address, job->read, job->length, std::move(callback));
                    }
                    else if (job->write != nullptr) {
                        job->state->async->submit_typed_write(job->purpose, job->address, job->write, job->length,
                                                              std::move(callback));
                    }
                    else {
                        auto status = job->state->async->submit_fsync(std::move(callback));
                        if (!status.ok()) {
                            job->finish(std::move(status));
                        }
                    }
                    return;
                }
            }
            if (job->state->sync == nullptr) {
                job->finish(Status::unavailable("backend owner has no synchronous store"));
                return;
            }
            Status status;
            auto  *block = dynamic_cast<PageDiskStore *>(job->state->sync);
            if (block != nullptr) {
                const auto &budget =
                    job->consumer.allocations ? job->consumer.allocations : job->state->backend_allocations;
                if (job->read != nullptr) {
                    status = block->read_at(job->address, static_cast<uint8_t *>(job->read), job->length, budget);
                }
                else if (job->write != nullptr) {
                    status =
                        block->write_at(job->address, static_cast<const uint8_t *>(job->write), job->length, budget);
                }
                else {
                    status = block->sync(budget);
                }
                job->finish(std::move(status));
                return;
            }
            if (job->read != nullptr) {
                status = job->state->sync->read_at(job->address, static_cast<uint8_t *>(job->read), job->length);
            }
            else if (job->write != nullptr) {
                status = job->state->sync->write_typed_at(job->purpose, job->address,
                                                          static_cast<const uint8_t *>(job->write), job->length);
            }
            else {
                status = job->state->sync->sync();
            }
            job->finish(std::move(status));
        }
        catch (const std::bad_alloc &) {
            job->finish(Status::resource_exhausted());
        }
        catch (...) {
            job->finish(Status::io_error("backend submission threw"));
        }
    }
};

std::optional<size_t> ScheduledPageStore::allocation_bound() noexcept
{
    using namespace detail;
    return AllocationBound(shared_allocation_bound<State>() + shared_allocation_bound<ScheduledPageStore>()).value();
}

Status ScheduledPageStore::create(PageStore *sync, AsyncPageStore *async, std::shared_ptr<const void> page_store_owner,
                                  std::shared_ptr<TreeRuntimeBinding>                   binding,
                                  const std::shared_ptr<detail::AllocationReservation> &metadata,
                                  std::shared_ptr<detail::FlushRuntime::Lane>           lane,
                                  std::shared_ptr<AsyncPageStore> *out, std::shared_ptr<ApplyDispatcher> extent_owner,
                                  MakeChunkWrites make_chunk_writes)
try {
    if ((sync == nullptr && async == nullptr) || !binding || !metadata || !lane || out == nullptr || *out) {
        return Status::invalid_argument();
    }
    auto state   = detail::reserved_shared<State>(metadata);
    state->lane  = std::move(lane);
    state->sync  = sync;
    state->async = async;
#ifdef CROWDB_HAVE_LIBURING
    state->direct_block = async != nullptr && typeid(*async) == typeid(AsyncPageDiskStore);
#endif
    // Only the concrete memory store promises that reads never wait. An
    // injected subclass can override read_at with a different contract.
    if (async == nullptr && sync != nullptr && typeid(*sync) == typeid(PageMemStore)) {
        state->memory = static_cast<PageMemStore *>(sync);
    }
    state->page_store_owner = std::move(page_store_owner);
    state->binding          = std::move(binding);
    state->extent_owner     = extent_owner ? std::move(extent_owner) : state->binding->backend_progress();
    std::shared_ptr<detail::AllocationAccount> actual;
    auto                                       status =
        detail::AllocationAccount::create_ongoing(state->binding->account(), detail::BudgetClass::kRouting, &actual);
    if (!status.ok()) {
        return status;
    }
    status = detail::ongoing_backend_operation(sync, &state->backend_allocations);
    if (!status.ok()) {
        return status;
    }
    state->jobs = actual;
    if (make_chunk_writes != nullptr) {
        state->chunks = make_chunk_writes(actual);
    }
    *out = detail::reserved_construct<ScheduledPageStore>(
        metadata, [&](auto *storage) { ::new (static_cast<void *>(storage)) ScheduledPageStore(std::move(state)); });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

PageStore *ScheduledPageStore::synchronous_store() const noexcept
{
    return state_->sync;
}

bool ScheduledPageStore::uses_async_transport() const noexcept
{
    return state_->async != nullptr;
}

bool ScheduledPageStore::reads_inline() const noexcept
{
    return state_->memory != nullptr;
}

Status ScheduledPageStore::read_inline(PageAddr address, void *buffer, size_t length,
                                       TreeWorkCause cause) const noexcept
try {
    assert(state_->memory != nullptr);
    const auto &metrics = state_->binding->work_metrics();
    record_work(metrics, TreeWorkCounter::ReadOperations, 1, cause);
    auto status = state_->memory->read_at(address, static_cast<uint8_t *>(buffer), length);
    record_work(metrics, status.ok() ? TreeWorkCounter::ReadBytes : TreeWorkCounter::ReadErrors,
                status.ok() ? length : 1, cause);
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

size_t ScheduledPageStore::allocation_workers() const noexcept
{
    return std::max(state_->binding->cpu()->worker_count(), state_->binding->backend_cpu()->worker_count());
}

bool ScheduledPageStore::write_completion_is_reloadable() const noexcept
{
    return state_->async != nullptr ? state_->async->write_completion_is_reloadable()
                                    : !state_->sync->has_shared_ownership();
}

Status ScheduledPageStore::submit(PageAddr address, void *read, const void *write, size_t length, PagePurpose purpose,
                                  AsyncCompletion complete, detail::InlineFunction<Status()> step, size_t read_bytes,
                                  size_t write_bytes, detail::InlineFunction<void(AsyncCompletion)> async_step)
try {
    if (!complete || (length != 0 && read == nullptr && write == nullptr) || address > UINT64_MAX - length) {
        return Status::invalid_argument();
    }
    using namespace detail;
    auto job          = reserved_shared<Job>(state_->jobs);
    job->state        = state_;
    job->step         = std::move(step);
    job->async_step   = std::move(async_step);
    job->consumer     = std::move(complete);
    job->address      = address;
    job->read         = read;
    job->write        = write;
    job->length       = length;
    job->read_bytes   = read != nullptr ? length : read_bytes;
    job->write_bytes  = write != nullptr ? length : write_bytes;
    job->purpose      = purpose;
    job->admitted     = true;
    job->queued_owner = job;
    if (job->async_step || (state_->direct_block && !job->step)) {
        Job::execute(job.get());
        return Status::Ok();
    }
    // The synchronous compatibility directory has one extent creator. The
    // io_uring path shares first-open completion without serializing writes.
    if (write != nullptr && state_->async == nullptr) {
        const auto *block = dynamic_cast<const PageDiskStore *>(state_->sync);
        if (block != nullptr && block->block_size() != 0) {
            state_->extent_owner->request(job->node, job);
            return Status::Ok();
        }
    }
    auto status = state_->binding->backend_cpu()->try_submit(state_->lane, &Job::execute, job.get());
    if (!status.ok()) {
        job->admitted = false;
        job->queued_owner.reset();
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

uint64_t ScheduledPageStore::submit_read(PageAddr address, void *buffer, size_t length, AsyncCompletion complete)
{
    if (length == 0) {
        complete.complete(Status::Ok());
        return 0;
    }
    if (reads_inline()) {
        if (complete) {
            complete.complete(read_inline(address, buffer, length, complete.cause));
        }
        return 0;
    }
    auto status = submit(address, buffer, nullptr, length, PagePurpose::kBtreePage, complete);
    if (!status.ok()) {
        complete.complete(std::move(status));
    }
    return 0;
}

uint64_t ScheduledPageStore::submit_write(PageAddr address, const void *buffer, size_t length, AsyncCompletion complete)
{
    return submit_typed_write(PagePurpose::kBtreePage, address, buffer, length, std::move(complete));
}

uint64_t ScheduledPageStore::submit_typed_write(PagePurpose purpose, PageAddr address, const void *buffer,
                                                size_t length, AsyncCompletion complete)
{
    if (length == 0) {
        complete.complete(Status::Ok());
        return 0;
    }
    if (state_->memory != nullptr) {
        if (!complete) {
            return 0;
        }
        if (buffer == nullptr || address > UINT64_MAX - length) {
            complete.complete(Status::invalid_argument());
            return 0;
        }
        const auto &metrics = state_->binding->work_metrics();
        record_work(metrics, TreeWorkCounter::WriteOperations, 1, complete.cause);
        Status status;
        try {
            status = state_->memory->write_typed_at(purpose, address, static_cast<const uint8_t *>(buffer), length);
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        record_work(metrics, status.ok() ? TreeWorkCounter::WriteBytes : TreeWorkCounter::WriteErrors,
                    status.ok() ? length : 1, complete.cause);
        complete.complete(std::move(status));
        return 0;
    }
    auto status = submit(address, nullptr, buffer, length, purpose, complete);
    if (!status.ok()) {
        complete.complete(std::move(status));
    }
    return 0;
}

Status ScheduledPageStore::submit_fsync(AsyncCompletion complete)
{
    if (state_->memory != nullptr) {
        if (!complete) {
            return Status::invalid_argument();
        }
        complete.complete(state_->memory->sync());
        return Status::Ok();
    }
    return submit(0, nullptr, nullptr, 0, PagePurpose::kBtreePage, std::move(complete));
}

Status ScheduledPageStore::submit_step(detail::InlineFunction<Status()> step, AsyncCompletion complete,
                                       size_t read_bytes, size_t write_bytes)
{
    if (!step || (read_bytes != 0 && write_bytes != 0)) {
        return Status::invalid_argument();
    }
    return submit(0, nullptr, nullptr, 0, PagePurpose::kBtreePage, std::move(complete), std::move(step), read_bytes,
                  write_bytes);
}

Status ScheduledPageStore::submit_async_step(detail::InlineFunction<void(AsyncCompletion)> step,
                                             AsyncCompletion complete, size_t read_bytes, size_t write_bytes)
{
    if (!step || (read_bytes != 0 && write_bytes != 0)) {
        return Status::invalid_argument();
    }
    return submit(0, nullptr, nullptr, 0, PagePurpose::kBtreePage, std::move(complete), {}, read_bytes, write_bytes,
                  std::move(step));
}

void ScheduledPageStore::cancel(uint64_t /*operation*/)
{
    // Accepted operations keep their owners until real terminal or transport
    // disposal. Cancellation of a consumer never releases in-flight I/O early.
}

ChunkWriteQueue *ScheduledPageStore::chunk_writes() const noexcept
{
    return state_->chunks.get();
}
} // namespace crowdb::tree
