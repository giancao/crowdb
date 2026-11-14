// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/snapshot/snapshot_io.h"
#include "snapshot/published_native.h"
#include "snapshot/published_snapshot_entries.h"

namespace crowdb::tree
{
struct SnapshotByteInfo
{
    uint64_t slot        = 0;
    size_t   total_bytes = 0;
    size_t   chunk_bytes = 0;
    size_t   offset      = 0;
    uint32_t final_crc   = 0;
};

class PublishedByteExport
{
    struct State;

  public:
    using OpenComplete = std::function<void(Status, std::shared_ptr<PublishedByteExport>)>;
    using ReadComplete = std::function<void(Status, SnapshotChunk, bool)>;

    static Status create(const std::shared_ptr<TreeCore> &core, snapshot_format format, size_t chunk_bytes,
                         std::shared_ptr<PublishedByteExport> *out);
    static Status create(const std::shared_ptr<TreeCore> &core, snapshot_format format, size_t chunk_bytes,
                         std::shared_ptr<PublishedByteExport> *out, const KeyRange &range);
    static Status create_portable_async(std::shared_ptr<PublishedReadViewContext> view, size_t chunk_bytes,
                                        OpenComplete complete);
    Status        next(SnapshotChunk *out, bool *done);
    Status        next_async(ReadComplete complete);
    [[nodiscard]] const SnapshotByteInfo                               &info() const noexcept;
    [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &metadata() const noexcept;

  private:
    static std::shared_ptr<PublishedByteExport> allocate(const std::shared_ptr<TreeRuntimeBinding> &binding,
                                                         snapshot_format format, size_t chunk_bytes);

    explicit PublishedByteExport(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
