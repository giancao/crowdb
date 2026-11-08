// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_bound.h"
#include "page_store/chunk/chunk_page_block.h"
#include "page_store/local/scheduled_page_store.h"
#include "page_writeback_internal.h"

#include <bit>
#include <cassert>

namespace crowdb::tree
{
std::optional<size_t> PageWriteOperation::allocation_bound(PageWriteLimits limits) noexcept
{
    if (limits.max_frame_bytes < kFrameHeaderSize + kFrameTrailerSize ||
        limits.max_frame_bytes > UINT32_MAX - kDurableBlobHeader || limits.iu_size == 0 ||
        !std::has_single_bit(limits.buffer_alignment) ||
        (limits.iu_size > 1 && (limits.buffer_alignment < limits.iu_size || limits.buffer_alignment < 4096)) ||
        (limits.compression != compress_algo::kNone && limits.compression != compress_algo::kLz4)) {
        return {};
    }
    return detail::AllocationBound(
               detail::shared_allocation_bound<State>() + detail::shared_allocation_bound<PageWriteOperation>() +
               detail::shared_allocation_bound<PageBlockRef>() + detail::shared_allocation_bound<State::Protection>())
        .add(limits.padded_blob_bytes(limits.max_frame_bytes))
        .add(limits.buffer_alignment - 1)
        .add(durable_encode_scratch_bytes(limits.compression))
        .add(limits.chunk_backed
                 ? detail::ChunkFixedPackStore::write_allocation_bound(limits.padded_blob_bytes(limits.max_frame_bytes))
                 : std::optional<size_t>{0})
        .add(limits.chunk_backed ? detail::shared_allocation_bound<detail::AllocationReservation>() : 0)
        .add(limits.chunk_backed ? chunk_page_block_allocation_bound() : 0)
        .value();
}

size_t PageWriteOperation::retained_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<PageBlockRef>() + detail::shared_allocation_bound<State::Protection>();
}

PageWriteOperation::State::State(PageWriteInput source, PageWriteLimits bounds, PageLoadBudget credits)
    : input(std::move(source)),
      limits(bounds),
      budget(std::move(credits)),
      io_resources(budget.bytes, TreeResourceMetric::WriteInflightBytes),
      blob(input.raw_blob.empty() ? limits.padded_blob_bytes(limits.max_frame_bytes) + limits.buffer_alignment - 1 : 0,
           decltype(blob)::allocator_type(budget.bytes)),
      scratch(durable_encode_scratch_bytes(limits.compression),
              detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(budget.bytes)),
      page_block(detail::reserved_shared<PageBlockRef>(budget.bytes)),
      protection(detail::reserved_shared<Protection>(budget.bytes))
{
    const auto bytes = limits.padded_blob_bytes(limits.max_frame_bytes);
    if (input.raw_blob.empty()) {
        void *data     = blob.data();
        auto  capacity = blob.size();
        auto *address  = std::align(limits.buffer_alignment, bytes, data, capacity);
        assert(address != nullptr);
        aligned = {static_cast<uint8_t *>(address), bytes};
    }
    protection->dependencies = input.reload_dependencies;
    page_block->owner        = protection;
    if (limits.chunk_backed && budget.reservation()) {
        const auto bound = detail::ChunkFixedPackStore::write_allocation_bound(bytes);
        if (!bound || !budget.bytes) {
            throw std::bad_alloc();
        }
        chunk_credit = detail::AllocationReservation::create(budget.reservation(), *bound);
    }
    else if (limits.chunk_backed) {
        chunk_credit = budget.bytes;
    }
}

Status PageWriteOperation::create(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                                  std::shared_ptr<detail::FlushRuntime::Lane> lane, PageWriteInput input,
                                  PageWriteLimits limits, PageLoadBudget budget, ReserveExtent reserve,
                                  std::shared_ptr<PageWriteOperation> *out)
{
    if (!store || !runtime || !lane || !reserve || out == nullptr || !allocation_bound(limits) || !input.owner ||
        input.frame.size() != input.info.bytes || input.frame.size() > limits.max_frame_bytes ||
        input.frame.size() < kFrameHeaderSize + kFrameTrailerSize ||
        (budget.reservation() && budget.reservation()->category() != detail::BudgetClass::kRouting &&
         budget.reservation()->category() != detail::BudgetClass::kReclaim) ||
        (budget.io && (!budget.reservation() || budget.io->category() != detail::BudgetClass::kIo ||
                       budget.reservation()->account() != budget.io->account()))) {
        return Status::invalid_argument("page write requires owned input and valid allocation ownership");
    }
    if (limits.chunk_backed ? dynamic_cast<ScheduledPageStore *>(store.get()) == nullptr
                            : !store->write_completion_is_reloadable()) {
        return Status::not_supported("backend write needs checkpoint/seal before it is independently reloadable");
    }
    if (!input.raw_blob.empty() && (limits.compression != compress_algo::kNone ||
                                    input.raw_blob.size() != limits.padded_blob_bytes(input.frame.size()) ||
                                    reinterpret_cast<uintptr_t>(input.raw_blob.data()) % limits.buffer_alignment != 0 ||
                                    input.raw_blob.data() + kDurableBlobHeader != input.frame.data())) {
        return Status::invalid_argument("raw write buffer must contain the aligned input frame");
    }
    try {
        auto state     = detail::reserved_shared<State>(budget.bytes, std::move(input), limits, budget);
        state->store   = std::move(store);
        state->runtime = std::move(runtime);
        state->lane    = std::move(lane);
        state->reserve = std::move(reserve);
        *out           = detail::reserved_construct<PageWriteOperation>(budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) PageWriteOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

detail::CallbackSender PageWriteOperation::sender() noexcept
{
    return {this, &PageWriteOperation::submit};
}

const PageWriteResult &PageWriteOperation::result() const noexcept
{
    return state_->report;
}

void PageWriteOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
}

void PageWriteOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<PageWriteOperation *>(source)->state_->start(complete, context);
}

void PageWriteOperation::State::start(detail::CallbackComplete callback, void *context) noexcept
{
    if (started.exchange(true, std::memory_order_acq_rel)) {
        callback(context, detail::CallbackSignal::kError, Status::unavailable());
        return;
    }
    pending  = weak_from_this().lock();
    complete = callback;
    receiver = context;
    Status status;
    try {
        status = runtime->try_submit(lane, &State::encode, this);
    }
    catch (...) {
        status = Status::resource_exhausted();
    }
    if (!status.ok()) {
        finish(std::move(status));
    }
}

void PageWriteOperation::State::encode(void *context) noexcept
{
    auto self = static_cast<State *>(context)->pending;
    assert(self);
    if (self->stopped.load(std::memory_order_acquire)) {
        self->finish(Status::unavailable());
        return;
    }
    try {
        self->prepare();
    }
    catch (const std::bad_alloc &) {
        self->finish(Status::resource_exhausted());
    }
    catch (...) {
        self->finish(Status::io_error());
    }
}

void PageWriteOperation::State::written(void *context, Status status) noexcept
{
    auto *state = static_cast<State *>(context);
    if (state->io_complete.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (status.ok() && state->page_block->chunk) {
        state->page_block->physical_metrics = state->page_block->chunk->physical_metrics;
    }
    state->io_resources.reset();
    state->finish(std::move(status));
}

void PageWriteOperation::State::finish(Status status) noexcept
{
    if (finished.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    if (const auto reservation = std::dynamic_pointer_cast<detail::AllocationReservation>(chunk_credit)) {
        reservation->seal();
    }
    const bool cancelled = stopped.load(std::memory_order_acquire);
    report.status        = cancelled ? Status::unavailable() : std::move(status);
    if (report.status.ok()) {
        report.page_block = page_block;
    }
    input.owner.reset();
    input.info.dependencies.reset();
    input.reload_dependencies.reset();
    auto self   = std::move(pending);
    auto signal = report.status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    complete(receiver, signal, report.status.without_message());
}
} // namespace crowdb::tree
