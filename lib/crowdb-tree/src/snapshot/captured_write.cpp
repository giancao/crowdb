// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_store/local/scheduled_page_store.h"
#include "snapshot/captured_write_internal.h"

namespace crowdb::tree
{
namespace
{
bool valid_write_limits(const CapturedWriteLimits &limits, const PageLoadBudget &budget)
{
    return !limits.frames.plan_only && limits.frames.encode.frame_bytes <= limits.write.max_frame_bytes &&
           PageWriteOperation::allocation_bound(limits.write).has_value() &&
           (!limits.frames.encode.metadata || limits.frames.encode.metadata == budget.bytes) &&
           (!budget.reservation() || budget.reservation()->category() == detail::BudgetClass::kRouting ||
            budget.reservation()->category() == detail::BudgetClass::kReclaim) &&
           (!budget.io || (budget.reservation() && budget.io->category() == detail::BudgetClass::kIo &&
                           budget.reservation()->account() == budget.io->account()));
}
} // namespace

Status CapturedWriteOperation::create(std::shared_ptr<const TreeRoot> captured, std::shared_ptr<PageLoader> loader,
                                      std::shared_ptr<AsyncPageStore>             store,
                                      std::shared_ptr<detail::FlushRuntime>       runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedWriteLimits limits,
                                      PageLoadBudget budget, CapturedFrameStream::ReserveOverflow reserve_overflow,
                                      PageWriteOperation::ReserveExtent           reserve_extent,
                                      std::shared_ptr<const CapturedDurablePages> inherited,
                                      std::shared_ptr<CapturedWriteOperation>    *out)
{
    if (!captured || !store || !runtime || !lane || !reserve_extent || out == nullptr ||
        !valid_write_limits(limits, budget)) {
        return Status::invalid_argument();
    }
    try {
        limits.frames.encode.metadata = budget.bytes;
        if (budget.work_metrics) {
            limits.frames.encode.work_metrics = budget.work_metrics;
            limits.frames.encode.cause        = budget.cause;
        }
        std::shared_ptr<CapturedFrameStream> stream;
        auto status = CapturedFrameStream::create(captured, std::move(loader), runtime, lane, limits.frames,
                                                  std::move(reserve_overflow), &stream);
        if (!status.ok()) {
            return status;
        }
        auto pages          = detail::reserved_shared<CapturedDurablePages>(budget.bytes, budget.bytes);
        pages->root_page_id = captured->root_page_id;
        pages->next_page_id = captured->next_page_id;
        pages->coverage     = captured->coverage;
        pages->generation   = captured->generation;
        return create(std::move(stream), std::move(pages), std::move(store), std::move(runtime), std::move(lane),
                      std::move(limits), std::move(budget), std::move(reserve_extent), std::move(inherited), out);
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
}

Status CapturedWriteOperation::create(std::shared_ptr<CapturedFrameStream>        stream,
                                      std::shared_ptr<CapturedDurablePages>       pages,
                                      std::shared_ptr<AsyncPageStore>             store,
                                      std::shared_ptr<detail::FlushRuntime>       runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane> lane, CapturedWriteLimits limits,
                                      PageLoadBudget budget, PageWriteOperation::ReserveExtent reserve_extent,
                                      std::shared_ptr<const CapturedDurablePages> inherited,
                                      std::shared_ptr<CapturedWriteOperation>    *out)
{
    if (!stream || !pages || !pages->frames.empty() || !store || !runtime || !lane || !reserve_extent ||
        out == nullptr || !valid_write_limits(limits, budget)) {
        return Status::invalid_argument();
    }
    if (limits.write.chunk_backed ? dynamic_cast<ScheduledPageStore *>(store.get()) == nullptr
                                  : !store->write_completion_is_reloadable()) {
        return Status::not_supported();
    }
    try {
        limits.frames.encode.metadata = budget.bytes;
        if (budget.work_metrics) {
            limits.frames.encode.work_metrics = budget.work_metrics;
            limits.frames.encode.cause        = budget.cause;
        }
        auto state       = detail::reserved_shared<State>(budget.bytes, budget.bytes);
        state->store     = std::move(store);
        state->runtime   = std::move(runtime);
        state->lane      = std::move(lane);
        state->limits    = std::move(limits);
        state->budget    = std::move(budget);
        state->reserve   = std::move(reserve_extent);
        state->inherited = std::move(inherited);
        state->pages     = std::move(pages);
        state->stream    = std::move(stream);
        *out             = detail::reserved_construct<CapturedWriteOperation>(state->budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) CapturedWriteOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

detail::CallbackSender CapturedWriteOperation::sender() noexcept
{
    return {this, &CapturedWriteOperation::submit};
}

const CapturedWriteResult &CapturedWriteOperation::result() const noexcept
{
    return state_->report;
}

void CapturedWriteOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void CapturedWriteOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<CapturedWriteOperation *>(source)->state_->start(complete, context);
}

void CapturedWriteOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
{
    uint8_t idle = 0;
    if (!lifecycle.compare_exchange_strong(idle, 1, std::memory_order_acq_rel)) {
        callback(argument, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    complete = callback;
    context  = argument;
    lifecycle.store(2, std::memory_order_release);
    wake();
}

void CapturedWriteOperation::State::Receiver::set_value() noexcept
{
    auto self = std::move(owner);
    self->child_done(Status::Ok());
}

void CapturedWriteOperation::State::Receiver::set_error(Status status) noexcept
{
    auto self = std::move(owner);
    self->child_done(std::move(status));
}

void CapturedWriteOperation::State::Receiver::set_stopped() noexcept
{
    auto self = std::move(owner);
    self->child_done(Status::unavailable());
}

void CapturedWriteOperation::State::child_done(Status status) noexcept
{
    child_status = std::move(status);
    child_ready.store(true, std::memory_order_release);
    wake();
}

void CapturedWriteOperation::State::begin_child(detail::CallbackSender sender, Stage next)
{
    stage   = next;
    waiting = true;
    child_ready.store(false, std::memory_order_release);
    attempt.emplace(sender, shared_from_this());
    stdexec::start(attempt->connected);
}

void CapturedWriteOperation::State::cancel_child() noexcept
{
    if (stage == Stage::Frame && stream) {
        stream->request_stop();
    }
    else if (stage == Stage::RelocateRead && relocation_read) {
        relocation_read->request_stop();
    }
    else if ((stage == Stage::Write || stage == Stage::ResidentWrite) && write) {
        write->request_stop();
    }
}

void CapturedWriteOperation::State::finish(Status status) noexcept
{
    if (overflow_writes) {
        join_overflow(std::move(status));
        return;
    }
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    const bool cancelled = stopped.load(std::memory_order_acquire);
    report.status        = cancelled ? Status::unavailable() : std::move(status);
    report.primary_io_failed =
        report.status.code() == Code::kIoError && stage == Stage::Write && descriptor.type != page_type::kOverflowFrame;
    if (report.status.ok()) {
        report.status = pages->initialize_overflow_counts();
    }
    if (report.status.ok()) {
        report.pages = std::move(pages);
    }
    pages.reset();
    descriptor.page_block.reset();
    resident_dependency.page_block.reset();
    resident_sources.clear();
    descriptor.stored_overflow.reset();
    frame.reset();
    dependencies.reset();
    overflow_page_block.reset();
    stream.reset();
    write.reset();
    relocation_read.reset();
    inherited.reset();
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(context, signal, report.status.without_message());
}
} // namespace crowdb::tree
