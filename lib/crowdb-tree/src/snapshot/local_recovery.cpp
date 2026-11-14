// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/local_recovery_internal.h"

#include <algorithm>
#include <bit>
#include <cassert>

namespace crowdb::tree
{
LocalRecoveryOperation::State::Window::Window(size_t blob_bytes, size_t frame_bytes, size_t alignment,
                                              const std::shared_ptr<AllocationBudget> &metadata)
    : storage(blob_bytes + alignment - 1, detail::ReservationAllocator<uint8_t, AllocationKind::kIo>(metadata)),
      frame(frame_bytes, detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata))
{
    void *data      = storage.data();
    auto  available = storage.size();
    auto *aligned   = std::align(alignment, blob_bytes, data, available);
    assert(aligned != nullptr);
    bytes = {static_cast<uint8_t *>(aligned), blob_bytes};
}

LocalRecoveryOperation::State::State(PageLoadBudget credits)
    : budget(std::move(credits)),
      directory(detail::ReservationAllocator<DirEntry>(budget.bytes)),
      references(detail::ReservationAllocator<LeafReference>(budget.bytes)),
      report(detail::reserved_shared<LocalRecoveryResult>(budget.bytes))
{
}

Status LocalRecoveryOperation::create(std::shared_ptr<AsyncPageStore>             store,
                                      std::shared_ptr<detail::FlushRuntime>       runtime,
                                      std::shared_ptr<detail::FlushRuntime::Lane> lane, ExtentLedgerLimits domain,
                                      LocalRecoveryLimits limits, PageLoadBudget budget,
                                      std::shared_ptr<LocalRecoveryOperation> *out)
{
    const auto prepaid = budget.reservation();
    if (!store || !runtime || !lane || out == nullptr || domain.iu_size == 0 ||
        domain.region_begin < snapshot::region_base_for(domain.iu_size) || limits.max_segments == 0 ||
        limits.max_segments > kMaxMappingSegments || limits.max_frames == 0 ||
        limits.max_blob_bytes < kDurableBlobHeader || limits.max_blob_bytes > UINT32_MAX ||
        limits.max_frame_bytes < kFrameHeaderSize + kFrameTrailerSize || limits.max_frame_bytes > UINT32_MAX ||
        limits.max_overflow_references == 0 || limits.max_dependency_steps == 0 ||
        (prepaid && (prepaid->category() != detail::BudgetClass::kRouting &&
                     prepaid->category() != detail::BudgetClass::kReclaim)) ||
        (budget.io && (!budget.bytes || budget.io->category() != detail::BudgetClass::kIo ||
                       budget.bytes->domain() != budget.io->domain()))) {
        return Status::invalid_argument();
    }
    try {
        auto state            = detail::reserved_shared<State>(budget.bytes, budget);
        state->readable_bytes = domain.file_size;
        domain.file_size      = std::max(domain.file_size, domain.region_begin);
        state->domain         = domain;
        state->limits         = limits;
        state->store          = std::move(store);
        state->runtime        = std::move(runtime);
        state->lane           = std::move(lane);
        auto status           = ExtentLedger::create(domain, budget.bytes, &state->ledger);
        if (!status.ok()) {
            return status;
        }
        state->metadata_window = round_up_to_iu(segment_image_encoded_size(kMappingSegmentSize), domain.iu_size);
        state->window          = detail::reserved_shared<State::Window>(
            budget.bytes, std::max(limits.max_blob_bytes, state->metadata_window), limits.max_frame_bytes,
            std::bit_ceil(std::max<size_t>(4096, domain.iu_size)), budget.bytes);
        *out = detail::reserved_construct<LocalRecoveryOperation>(budget.bytes, [&](auto *storage) {
            ::new (static_cast<void *>(storage)) LocalRecoveryOperation(std::move(state));
        });
    }
    catch (const std::bad_alloc &) {
        return Status::resource_exhausted();
    }
    return Status::Ok();
}

LocalRecoveryOperation::LocalRecoveryOperation(std::shared_ptr<State> state)
    : state_(std::move(state)),
      result_(state_->report)
{
}

detail::CallbackSender LocalRecoveryOperation::sender() noexcept
{
    return {this, &LocalRecoveryOperation::submit};
}

const LocalRecoveryResult &LocalRecoveryOperation::result() const noexcept
{
    return *result_;
}

void LocalRecoveryOperation::request_stop() noexcept
{
    state_->stopped.store(true, std::memory_order_release);
    state_->wake();
}

void LocalRecoveryOperation::submit(void *source, detail::CallbackComplete complete, void *context)
{
    static_cast<LocalRecoveryOperation *>(source)->state_->start(complete, context);
}

void LocalRecoveryOperation::State::start(detail::CallbackComplete callback, void *argument) noexcept
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

void LocalRecoveryOperation::State::finish(Status status) noexcept
{
    if (lifecycle.exchange(3, std::memory_order_acq_rel) == 3) {
        return;
    }
    const bool cancelled = stopped.load(std::memory_order_acquire);
    report->status       = cancelled ? Status::unavailable() : std::move(status);
    if (report->status.ok()) {
        report->ledger  = ledger;
        report->anchors = std::move(anchors);
        uint64_t latest = 0;
        for (const auto &anchor : report->anchors) {
            if (anchor && anchor->anchor.snapshot_seq > latest) {
                latest         = anchor->anchor.snapshot_seq;
                report->latest = anchor->pages;
            }
        }
    }
    else {
        report->registry.reset();
    }
    anchors = {};
    image.reset();
    pages.reset();
    window.reset();
    recovered.owner.reset();
    protection.reset();
    overflow_backing.reset();
    ledger.reset();
    store.reset();
    auto signal = report->status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError;
    if (cancelled) {
        signal = detail::CallbackSignal::kStopped;
    }
    const auto result_status = report->status.without_message();
    // Completed backend callbacks may still retain State. Transfer the result
    // to the public operation before notifying its consumer so those callback
    // tails cannot retain the recovered checkpoint and its protected extents.
    report.reset();
    complete(context, signal, result_status);
}
} // namespace crowdb::tree
