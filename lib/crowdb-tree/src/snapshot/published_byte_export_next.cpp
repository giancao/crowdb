// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/apply_dispatcher.h"
#include "crowdb-common/log.h"
#include "snapshot/published_byte_export_internal.h"
#include "snapshot/snapshot_wire.h"

#include <cstring>

namespace crowdb::tree
{
namespace
{
struct ChunkOutput
{
    ChunkOutput(size_t capacity, const std::shared_ptr<detail::AllocationReservation> &metadata)
        : bytes(capacity, detail::ReservationAllocator<uint8_t, AllocationKind::kPayload>(metadata))
    {
    }

    detail::ReservedVector<uint8_t, AllocationKind::kPayload> bytes;
};
} // namespace

struct PublishedByteExport::State::Output
{
    std::shared_ptr<detail::AllocationReservation> budget;
    std::shared_ptr<ChunkOutput>                   buffer;
    size_t                                         used = 0;
};

Status PublishedByteExport::State::record_piece(std::span<const uint8_t> *out)
{
    if (format == snapshot_format::kNative) {
        while (!frame && !source_done) {
            auto status = native->next(&frame, &source_done);
            if (!status.ok()) {
                return status;
            }
        }
        if (frame) {
            snapshot_detail::put_u64(fields.data(), frame->page_id);
            snapshot_detail::put_u32(fields.data() + 8, static_cast<uint32_t>(frame->bytes.size()));
            *out = part == 0 ? std::span<const uint8_t>(fields).first(12) : frame->bytes;
            return Status::Ok();
        }
    }
    else {
        if (entry) {
            auto status = snapshot_detail::entry_fields(*entry, &fields);
            if (!status.ok()) {
                return status;
            }
            switch (part) {
            case 0:
                *out = std::span<const uint8_t>(fields).first(4);
                break;
            case 1:
                *out = snapshot_detail::bytes(Slice(entry->key));
                break;
            case 2:
                *out = std::span<const uint8_t>(fields).subspan(4);
                break;
            default:
                *out = snapshot_detail::bytes(entry->cell.value);
                break;
            }
            return Status::Ok();
        }
    }
    phase = Phase::Trailer;
    return next_piece(out);
}

Status PublishedByteExport::State::next_piece(std::span<const uint8_t> *out)
{
    switch (phase) {
    case Phase::Header:
        *out = std::span(header).first(header_size);
        return Status::Ok();
    case Phase::Records:
        return record_piece(out);
    case Phase::Trailer:
        if (emitted_crc != info.final_crc || info.offset != info.total_bytes - 4 + piece_offset) {
            return Status::corruption("snapshot emission differs from its fixed preflight stream");
        }
        *out = trailer;
        return Status::Ok();
    case Phase::Done:
        *out = {};
        return Status::Ok();
    }
    return Status::internal_error("invalid snapshot wire phase");
}

void PublishedByteExport::State::consumed_piece()
{
    piece_offset = 0;
    switch (phase) {
    case Phase::Header:
        phase = Phase::Records;
        break;
    case Phase::Records:
        ++part;
        if (part == (format == snapshot_format::kNative ? 2U : 4U)) {
            part = 0;
            entry.reset();
            frame.reset();
            if (source_done) {
                phase = Phase::Trailer;
            }
        }
        break;
    case Phase::Trailer:
        phase = Phase::Done;
        break;
    case Phase::Done:
        break;
    }
}

Status PublishedByteExport::State::start_next(Output &out)
{
    if (phase == Phase::Done) {
        return Status::Ok();
    }
    // Cursor advancement may change stream fields, but never the capacity
    // allocated for this output window. Keep its subtraction bound local.
    const size_t capacity = info.chunk_bytes;
    if (capacity == 0 || capacity > kSnapshotChunkBytes || info.total_bytes < 4 || info.offset > info.total_bytes) {
        return Status::corruption("snapshot output has invalid fixed window geometry");
    }
    using detail::shared_allocation_bound;
    const auto bound =
        detail::AllocationBound(shared_allocation_bound<ChunkOutput>() +
                                shared_allocation_bound<detail::AllocationReservation>() + kSnapshotChunkHandleBytes)
            .add(capacity)
            .value();
    if (!bound) {
        return Status::resource_exhausted("snapshot chunk allocation bound overflow");
    }
    auto credit = detail::ResourceBudget::try_reserve(binding->account(), detail::BudgetClass::kRouting, *bound);
    if (!credit) {
        return Status::resource_exhausted("snapshot chunk reserve unavailable");
    }
    out.budget = detail::AllocationReservation::create(std::move(*credit));
    out.buffer = detail::reserved_shared<ChunkOutput>(out.budget, capacity, out.budget);
    return Status::Ok();
}

Status PublishedByteExport::State::advance(Output &out, bool *needs_entry)
{
    *needs_entry          = false;
    const size_t capacity = info.chunk_bytes;
    auto        &used     = out.used;
    while (used < capacity && phase != Phase::Done) {
        if (phase == Phase::Records && format == snapshot_format::kPortable && !entry && !source_done) {
            *needs_entry = true;
            return Status::Ok();
        }
        std::span<const uint8_t> piece;
        auto                     status = next_piece(&piece);
        if (!status.ok()) {
            return status;
        }
        if (piece_offset > piece.size()) {
            return Status::corruption("snapshot piece cursor exceeds its owner");
        }
        const auto take = std::min(capacity - used, piece.size() - piece_offset);
        if (info.offset > info.total_bytes || take > info.total_bytes - info.offset) {
            return Status::corruption("snapshot emission exceeds its preflight byte count");
        }
        if (used > out.buffer->bytes.size() || take > out.buffer->bytes.size() - used || take > kSnapshotChunkBytes) {
            return Status::corruption("snapshot piece exceeds its allocated output window");
        }
        const auto bytes = piece.subspan(piece_offset, take);
        if (take != 0) {
            const auto copy_bytes = static_cast<uint32_t>(take);
            std::memcpy(out.buffer->bytes.data() + used, bytes.data(), copy_bytes);
            record_work(out.budget, TreeWorkCounter::MaterializeCopyBytes, copy_bytes, TreeWorkCause::Snapshot);
        }
        if (phase != Phase::Trailer) {
            emitted_crc = snapshot_detail::crc_bytes(emitted_crc, bytes);
        }
        used += take;
        piece_offset += take;
        info.offset += take;
        if (piece_offset == piece.size()) {
            consumed_piece();
        }
    }
    return Status::Ok();
}

void PublishedByteExport::State::finish_next(Output &output, SnapshotChunk *out, bool *done)
{
    *done = phase == Phase::Done;
    if (output.buffer) {
        *out = SnapshotChunk{.bytes             = std::span<const uint8_t>(output.buffer->bytes).first(output.used),
                             .owner             = std::move(output.buffer),
                             .allocation_budget = std::move(output.budget)};
    }
    if (*done) {
        entries.reset();
        native.reset();
        snapshot.reset();
    }
}

Status PublishedByteExport::State::next(SnapshotChunk *out, bool *done)
{
    Output output;
    auto   status = start_next(output);
    while (status.ok()) {
        bool needs_entry = false;
        status           = advance(output, &needs_entry);
        if (!status.ok() || !needs_entry) {
            break;
        }
        status = entries->next(&entry, &source_done);
    }
    if (status.ok()) {
        finish_next(output, out, done);
    }
    return status;
}

struct PublishedByteExport::State::AsyncRead : std::enable_shared_from_this<AsyncRead>
{
    AsyncRead(std::shared_ptr<State> state, ReadComplete complete)
        : state(std::move(state)),
          complete(std::move(complete))
    {
    }

    static void start(void *context) noexcept
    {
        auto &work = *static_cast<AsyncRead *>(context);
        try {
            work.advance(work.state->start_next(work.output));
        }
        catch (const std::bad_alloc &) {
            work.finish(Status::resource_exhausted());
        }
        catch (...) {
            work.finish(Status::internal_error());
        }
    }

    void advance(Status status) noexcept
    try {
        bool needs_entry = false;
        if (status.ok()) {
            status = state->advance(output, &needs_entry);
        }
        if (status.ok() && needs_entry) {
            status = state->entries->next_async(
                [self = shared_from_this()](Status result, std::optional<PublishedScanEntry> item, bool end) {
                    self->state->entry       = std::move(item);
                    self->state->source_done = end;
                    self->advance(std::move(result));
                });
            if (status.ok()) {
                return;
            }
        }
        finish(std::move(status));
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    void finish(Status status) noexcept
    {
        SnapshotChunk chunk;
        bool          end = false;
        if (status.ok()) {
            state->finish_next(output, &chunk, &end);
        }
        else {
            state->failure = status.without_message();
        }
        auto callback = std::move(complete);
        state->busy.store(false, std::memory_order_release);
        try {
            callback(std::move(status), std::move(chunk), end);
        }
        catch (...) {
            CRB_LOG_ERROR("portable export read receiver threw after completion");
        }
    }

    std::shared_ptr<State> state;
    ReadComplete           complete;
    Output                 output;
    ApplyDispatcher::Node  ready{&AsyncRead::start, this};
};

Status PublishedByteExport::next_async(ReadComplete complete)
{
    if (!complete || state_->format != snapshot_format::kPortable) {
        return Status::invalid_argument("asynchronous export requires portable format and a completion");
    }
    auto state = state_;
    if (state->busy.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("snapshot exporter already has an active window");
    }
    Status status = state->failure.without_message();
    if (status.ok() && state->snapshot && !state->snapshot->detached()) {
        status = Status::not_supported();
    }
    if (status.ok()) {
        try {
            auto read = detail::reserved_shared<State::AsyncRead>(state->binding->lifecycle_budget(), state,
                                                                  std::move(complete));
            state->binding->view_cpu_progress()->request(read->ready, read);
            return Status::Ok();
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
    }
    state->busy.store(false, std::memory_order_release);
    return status;
}

Status PublishedByteExport::next(SnapshotChunk *out, bool *done)
{
    if (out == nullptr || done == nullptr) {
        return Status::invalid_argument("snapshot chunk requires result outputs");
    }
    *out  = {};
    *done = false;
    if (state_->busy.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("snapshot exporter already has an active window");
    }

    struct Leave
    {
        std::atomic<bool> &busy;

        ~Leave()
        {
            busy.store(false, std::memory_order_release);
        }
    } leave{state_->busy};

    if (!state_->failure.ok()) {
        return state_->failure;
    }
    try {
        auto status = state_->next(out, done);
        if (!status.ok()) {
            state_->failure = status.without_message();
        }
        return status;
    }
    catch (const std::bad_alloc &) {
        state_->failure = Status::resource_exhausted();
        return state_->failure;
    }
}
} // namespace crowdb::tree
