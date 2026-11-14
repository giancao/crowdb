// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/tree_core_checkpoint_internal.h"
#include "snapshot/snapshot_baseline.h"

namespace crowdb::tree
{
// The durable sink for one admitted flush. Independent page I/O overlaps;
// receipt installation uses the existing serialized CPU progress dispatcher.
class FlushPersistence : public std::enable_shared_from_this<FlushPersistence>
{
    struct Page;
    struct Commit;

  public:
    explicit FlushPersistence(std::shared_ptr<checkpoint_detail::Preparation> preparation, uint64_t wal_offset = 0);
    [[nodiscard]] static size_t control_allocation_bound() noexcept;
    [[nodiscard]] static size_t commit_control_allocation_bound() noexcept;
    void                        configure(FlushBatchOptions &options);

    void               write(PageEncodeInput input, FlushBatchOptions::ReserveOverflow reserve,
                             FlushBatchOptions::PersistComplete done);
    void               commit(std::shared_ptr<const PublishedTreeState> view, FlushBatch::MaintenanceComplete done);
    [[nodiscard]] bool selected(uint64_t pid, const PageKeyRange &range) const;
    [[nodiscard]] std::optional<CheckpointResult> result() const;

  private:
    friend struct Page;
    friend struct Commit;
    std::shared_ptr<checkpoint_detail::Preparation>                   preparation_;
    std::shared_ptr<const PublishedTreeState>                         initial_;
    std::shared_ptr<const CapturedDurablePages>                       inherited_;
    std::shared_ptr<CapturedDurablePages>                             pages_;
    Page                                                             *last_resident_page_ = nullptr;
    CheckpointResult                                                  report_;
    bool                                                              finished_   = false;
    uint64_t                                                          wal_offset_ = 0;
    std::shared_ptr<AllocationBudget>                                 selection_metadata_;
    std::optional<detail::ReservedVector<const SnapshotPageChange *>> pending_leaves_;
    void                                                              select_inputs();
};
} // namespace crowdb::tree
