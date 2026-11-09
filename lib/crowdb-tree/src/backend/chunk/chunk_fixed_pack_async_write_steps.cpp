// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_fixed_pack_async_write.h"

#include <cassert>

namespace crowdb::tree::detail
{
void ChunkFixedPackStore::AsyncWrite::prepare_registry(void *raw, CallbackComplete done, void *context)
try {
    auto       &self        = *static_cast<AsyncWrite *>(raw);
    const auto &reservation = prepared_record(self.reserved);
    auto        complete    = self.begin(done, context);
    if (self.writer.registry_) {
        self.writer.registry_->submit_prepare(reservation.id, {reservation.chunk_high, reservation.chunk_low},
                                              self.physical_budget, &self.physical, &self.admission,
                                              self.transport_completion());
    }
    else {
        complete.complete(Status::Ok());
    }
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::reserve(void *raw, CallbackComplete done, void *context)
try {
    auto &self = *static_cast<AsyncWrite *>(raw);
    assert(self.reserved && self.writing && self.final_record);
    self.writer.catalog_->submit_compare_stage(self.writer.tree_, self.writer.epoch_, {}, self.reserved,
                                               self.begin(done, context));
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::allocate(void *raw, CallbackComplete done, void *context)
try {
    auto       &self        = *static_cast<AsyncWrite *>(raw);
    const auto &reservation = prepared_record(self.reserved);
    self.begin(done, context);
    self.admission.accept();
    auto complete        = self.transport_completion();
    complete.complete_fn = &AsyncWrite::allocated;
    self.writer.transport_->submit_allocate_fixed_chunk(
        {reservation.chunk_high, reservation.chunk_low}, reservation.capacity, self.writer.epoch_,
        static_cast<PagePurpose>(reservation.purpose), std::move(complete));
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::allocated(void *raw, Status status)
try {
    auto       &self        = *static_cast<AsyncWrite *>(raw);
    const auto &reservation = prepared_record(self.reserved);
    self.allocation_status  = std::move(status);
    if (!self.writer.registry_) {
        completed(raw, std::move(self.allocation_status));
        return;
    }
    auto complete        = self.transport_completion();
    complete.complete_fn = &AsyncWrite::inspected;
    // Inspection also records an allocation whose mutation reply was lost.
    self.writer.transport_->submit_inspect_fixed_allocation({reservation.chunk_high, reservation.chunk_low},
                                                            self.writer.epoch_, &self.receipt, std::move(complete));
}
catch (const std::bad_alloc &) {
    completed(raw, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::inspected(void *raw, Status status)
try {
    auto &self = *static_cast<AsyncWrite *>(raw);
    assert(self.reserved && self.writing && self.final_record);
    if (!status.ok()) {
        completed(raw, self.allocation_status.ok() ? std::move(status) : std::move(self.allocation_status));
        return;
    }
    self.writer.registry_->submit_observe(
        self.physical, self.receipt, self.budget,
        {.complete_fn = &AsyncWrite::observed, .context = raw, .allocations = self.budget});
}
catch (const std::bad_alloc &) {
    completed(raw, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::observed(void *raw, Status status)
{
    auto &self = *static_cast<AsyncWrite *>(raw);
    assert(self.reserved && self.writing && self.final_record);
    completed(raw, self.allocation_status.ok() ? std::move(status) : std::move(self.allocation_status));
}

void ChunkFixedPackStore::AsyncWrite::fence(void *raw, CallbackComplete done, void *context)
try {
    auto       &self        = *static_cast<AsyncWrite *>(raw);
    const auto &reservation = prepared_record(self.reserved);
    self.begin(done, context);
    self.writer.transport_->submit_verify_fixed_read_fence({reservation.chunk_high, reservation.chunk_low},
                                                           self.writer.mirrors_, self.context,
                                                           self.transport_completion());
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::mark_writing(void *raw, CallbackComplete done, void *context)
try {
    auto &self = *static_cast<AsyncWrite *>(raw);
    assert(self.reserved && self.writing && self.final_record);
    self.writer.catalog_->submit_compare_stage(self.writer.tree_, self.writer.epoch_, self.reserved, self.writing,
                                               self.begin(done, context));
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::Mirror::submit(void *raw, CallbackComplete done, void *context)
try {
    auto &mirror      = *static_cast<Mirror *>(raw);
    mirror.complete   = done;
    mirror.context    = context;
    auto       &self  = *mirror.owner;
    const auto &write = prepared_record(self.writing);
    if (mirror.index >= self.writer.mirrors_) {
        done(context, CallbackSignal::kValue, Status::Ok());
        return;
    }
    self.writer.transport_->submit_write_fixed_mirror(
        {write.chunk_high, write.chunk_low}, mirror.index, 0, self.framed->bytes.data(), self.framed->bytes.size(),
        self.context, {.complete_fn = &Mirror::written, .context = raw, .allocations = self.budget});
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::Mirror::written(void *raw, Status status)
{
    auto &mirror = *static_cast<Mirror *>(raw);
    mirror.complete(mirror.context, status.ok() ? CallbackSignal::kValue : CallbackSignal::kError, std::move(status));
}

void ChunkFixedPackStore::AsyncWrite::advance(void *raw, CallbackComplete done, void *context)
try {
    auto       &self  = *static_cast<AsyncWrite *>(raw);
    const auto &write = prepared_record(self.writing);
    self.begin(done, context);
    self.writer.transport_->submit_advance_write({write.chunk_high, write.chunk_low}, 0, self.framed->bytes.size(),
                                                 self.transport_completion());
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::seal(void *raw, CallbackComplete done, void *context)
try {
    auto       &self  = *static_cast<AsyncWrite *>(raw);
    const auto &write = prepared_record(self.writing);
    self.begin(done, context);
    self.writer.transport_->submit_seal_chunk({write.chunk_high, write.chunk_low}, self.writer.epoch_,
                                              self.framed->bytes.size(), self.transport_completion());
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}

void ChunkFixedPackStore::AsyncWrite::reloadable(void *raw, CallbackComplete done, void *context)
try {
    auto &self = *static_cast<AsyncWrite *>(raw);
    assert(self.reserved && self.writing && self.final_record);
    self.writer.catalog_->submit_compare_stage(self.writer.tree_, self.writer.epoch_, self.writing, self.final_record,
                                               self.begin(done, context));
}
catch (const std::bad_alloc &) {
    done(context, CallbackSignal::kError, Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
