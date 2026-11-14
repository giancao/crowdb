// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/btree/key_range.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
// One logical entry per host window, including tombstones. The scan owns only
// a bounded path/current leaf; returned keys and values have independent
// owners.
class PublishedSnapshotEntries
{
    struct State;

  public:
    using Complete = std::function<void(Status, std::optional<PublishedScanEntry>, bool)>;

    static Status create(std::shared_ptr<PublishedReadViewContext>  snapshot,
                         std::shared_ptr<PublishedSnapshotEntries> *out, const KeyRange &range = KeyRange::unbounded());
    Status        next(std::optional<PublishedScanEntry> *out, bool *complete);
    // For detached views. Acceptance owns one window until its completion.
    Status                 next_async(Complete complete);
    [[nodiscard]] uint64_t skipped_subtrees() const noexcept;

  private:
    explicit PublishedSnapshotEntries(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    Status                 next_impl(Complete complete, std::shared_ptr<TreeCore> *host_core,
                                     const std::shared_ptr<HostReadTail> &tail = {});
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
