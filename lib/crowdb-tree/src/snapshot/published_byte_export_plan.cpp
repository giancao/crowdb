// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "snapshot/published_byte_export_internal.h"
#include "snapshot/snapshot_crc.h"
#include "snapshot/snapshot_wire.h"

namespace crowdb::tree
{
Status PublishedByteExport::State::measure_entry(const PublishedScanEntry &item, size_t &body_bytes, uint32_t &body_crc)
{
    auto status = snapshot_detail::entry_fields(item, &fields);
    if (!status.ok()) {
        return status;
    }
    const auto key   = snapshot_detail::bytes(Slice(item.key));
    const auto value = snapshot_detail::bytes(item.cell.value);
    const auto bytes = uint64_t{17} + key.size() + value.size();
    if (count == UINT64_MAX || bytes > SIZE_MAX - body_bytes - 29) {
        return Status::resource_exhausted("portable stream length overflow");
    }
    ++count;
    body_bytes += bytes;
    body_crc = snapshot_detail::crc_bytes(body_crc, std::span(fields).first(4));
    body_crc = snapshot_detail::crc_bytes(body_crc, key);
    body_crc = snapshot_detail::crc_bytes(body_crc, std::span(fields).subspan(4));
    body_crc = snapshot_detail::crc_bytes(body_crc, value);
    return Status::Ok();
}

void PublishedByteExport::State::finish_portable_preflight(size_t body_bytes, uint32_t body_crc)
{
    encode_header();
    const auto header_crc = snapshot_detail::crc_bytes(0, std::span(header).first(header_size));
    info.final_crc        = snapshot_detail::crc_concatenate(header_crc, body_crc, body_bytes);
    info.total_bytes      = header_size + body_bytes + 4;
    snapshot_detail::put_u32(trailer.data(), info.final_crc);
}

Status PublishedByteExport::State::portable_preflight(const KeyRange &range)
{
    auto status = PublishedSnapshotEntries::create(snapshot, &entries, range);
    if (!status.ok()) {
        return status;
    }
    uint32_t body_crc   = 0;
    size_t   body_bytes = 0;
    bool     complete   = false;
    while (!complete) {
        std::optional<PublishedScanEntry> item;
        status = entries->next(&item, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!item) {
            continue;
        }
        status = measure_entry(*item, body_bytes, body_crc);
        if (!status.ok()) {
            return status;
        }
    }
    finish_portable_preflight(body_bytes, body_crc);
    entries.reset();
    snapshot->lane()->drain();
    return PublishedSnapshotEntries::create(snapshot, &entries, range);
}

struct PublishedByteExport::State::AsyncPreflight : std::enable_shared_from_this<AsyncPreflight>
{
    AsyncPreflight(std::shared_ptr<PublishedByteExport> stream, OpenComplete complete)
        : stream(std::move(stream)),
          complete(std::move(complete))
    {
    }

    Status next()
    {
        return stream->state_->entries->next_async(
            [self = shared_from_this()](Status status, std::optional<PublishedScanEntry> item, bool end) {
                self->read(std::move(status), std::move(item), end);
            });
    }

    void read(Status status, std::optional<PublishedScanEntry> item, bool end) noexcept
    try {
        auto &state = *stream->state_;
        if (status.ok() && item) {
            status = state.measure_entry(*item, body_bytes, body_crc);
        }
        if (status.ok() && !end) {
            status = next();
            if (status.ok()) {
                return;
            }
        }
        if (status.ok()) {
            state.finish_portable_preflight(body_bytes, body_crc);
            state.entries.reset();
            status = PublishedSnapshotEntries::create(state.snapshot, &state.entries);
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
        auto done   = std::move(complete);
        auto result = status.ok() ? std::move(stream) : nullptr;
        stream.reset();
        try {
            done(std::move(status), std::move(result));
        }
        catch (...) {
            CRB_LOG_ERROR("portable export open receiver threw after completion");
        }
    }

    std::shared_ptr<PublishedByteExport> stream;
    OpenComplete                         complete;
    size_t                               body_bytes = 0;
    uint32_t                             body_crc   = 0;
};

Status PublishedByteExport::create_portable_async(std::shared_ptr<PublishedReadViewContext> view, size_t chunk_bytes,
                                                  OpenComplete complete)
try {
    if (!view || !complete || chunk_bytes > kSnapshotChunkBytes) {
        return Status::invalid_argument("portable export needs a fixed view, completion and bounded chunk size");
    }
    if (!view->detached()) {
        return Status::not_supported("asynchronous portable export requires owned source page_block");
    }
    auto stream = allocate(view->binding(), snapshot_format::kPortable, chunk_bytes);
    if (!stream) {
        return Status::resource_exhausted("portable export control reserve unavailable");
    }
    auto &state     = *stream->state_;
    state.info.slot = view->view()->coverage;
    state.snapshot  = std::move(view);
    auto status     = PublishedSnapshotEntries::create(state.snapshot, &state.entries);
    if (!status.ok()) {
        return status;
    }
    auto preflight = detail::reserved_shared<State::AsyncPreflight>(state.binding->lifecycle_budget(),
                                                                    std::move(stream), std::move(complete));
    return preflight->next();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status PublishedByteExport::State::native_preflight()
{
    encode_header();
    uint32_t crc      = snapshot_detail::crc_bytes(0, std::span(header).first(header_size));
    size_t   length   = header_size;
    bool     complete = false;
    while (!complete) {
        std::optional<NativeFrameView> item;
        auto                           status = native->next(&item, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!item) {
            continue;
        }
        if (item->bytes.size() > UINT32_MAX || length > SIZE_MAX - 16 || item->bytes.size() > SIZE_MAX - length - 16) {
            return Status::resource_exhausted("native stream length overflow");
        }
        snapshot_detail::put_u64(fields.data(), item->page_id);
        snapshot_detail::put_u32(fields.data() + 8, static_cast<uint32_t>(item->bytes.size()));
        crc = snapshot_detail::crc_bytes(crc, std::span(fields).first(12));
        crc = snapshot_detail::crc_bytes(crc, item->bytes);
        length += 12 + item->bytes.size();
    }
    info.total_bytes = length + 4;
    info.final_crc   = crc;
    return native->rewind();
}

Status PublishedByteExport::State::preflight(const KeyRange &range)
{
    if (format == snapshot_format::kPortable) {
        return portable_preflight(range);
    }
    auto status = native_preflight();
    if (status.ok()) {
        snapshot_detail::put_u32(trailer.data(), info.final_crc);
    }
    return status;
}
} // namespace crowdb::tree
