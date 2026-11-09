// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-common/log.h"
#include "portable_import.h"
#include "portable_import_plan.h"
#include "runtime_range_rebuild.h"
#include "snapshot/published_snapshot_entries.h"
#include "snapshot/recovered_state.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
class RangeBuilder
{
  public:
    RangeBuilder(TreeCore &destination, uint64_t coverage, RuntimeRangeImport &staging)
        : staging_(staging),
          builder_(destination.config(), destination.load_limits(), coverage, staging.credit),
          cell_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(staging.credit))
    {
    }

    Status append(const PublishedScanEntry &entry)
    {
        cell_.resize(kCellHeaderSize + entry.cell.value.size());
        frame_put_u64(cell_.data(), 0, entry.cell.slot);
        cell_[sizeof(uint64_t)] = entry.cell.tombstone() ? kFlagTombstone : 0;
        if (!entry.cell.value.empty()) {
            std::copy_n(reinterpret_cast<const uint8_t *>(entry.cell.value.data()), entry.cell.value.size(),
                        cell_.data() + kCellHeaderSize);
        }
        auto status = builder_.append(Slice(entry.key), Slice(cell_.data(), cell_.size()));
        if (status.ok()) {
            ++staging_.stats.entries_examined;
            ++staging_.stats.entries_emitted;
        }
        return status;
    }

    Status finish(uint64_t skipped_subtrees)
    {
        staging_.stats.subtrees_skipped = skipped_subtrees;
        auto status                     = builder_.finish(&staging_.catalog);
        if (status.ok()) {
            staging_.stats.pages_rebuilt = staging_.catalog->frames.size();
        }
        return status;
    }

  private:
    RuntimeRangeImport                                       &staging_;
    PortableImportBuilder                                     builder_;
    detail::ReservedVector<uint8_t, AllocationKind::kScratch> cell_;
};

Status reserve_range(const std::shared_ptr<PublishedReadViewContext> &snapshot, TreeCore &destination,
                     const KeyRange &range, RuntimeRangeImport &staging)
{
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    auto                                      status = PublishedSnapshotEntries::create(snapshot, &cursor, range);
    if (!status.ok()) {
        return status;
    }
    PortableImportPlan plan(destination.config(), destination.load_limits());
    bool               complete = false;
    while (!complete) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &complete);
        if (!status.ok()) {
            return status;
        }
        if (entry) {
            status = plan.append(entry->key.size(), entry->cell.value.size(), entry->cell.tombstone());
            if (!status.ok()) {
                return status;
            }
        }
    }
    const auto shape = plan.finish();
    if (!shape) {
        return Status::resource_exhausted("range import shape overflow");
    }
    return reserve_range_shape(destination, *shape, true, staging);
}

struct AsyncRangeCopy : std::enable_shared_from_this<AsyncRangeCopy>
{
    AsyncRangeCopy(std::shared_ptr<PublishedReadViewContext> view, TreeCore &destination, KeyRange range,
                   RangeCopyComplete complete, std::shared_ptr<TreeOperationLifetime::Ticket> ticket)
        : view(std::move(view)),
          destination(destination),
          range(std::move(range)),
          complete(std::move(complete)),
          ticket(std::move(ticket)),
          plan(destination.config(), destination.load_limits())
    {
    }

    Status open_cursor()
    {
        cursor.reset();
        return PublishedSnapshotEntries::create(view, &cursor, range);
    }

    Status next()
    {
        return cursor->next_async(
            [self = shared_from_this()](Status status, std::optional<PublishedScanEntry> entry, bool end) {
                self->read(std::move(status), std::move(entry), end);
            });
    }

    void read(Status status, std::optional<PublishedScanEntry> entry, bool end) noexcept
    try {
        if (status.ok() && entry) {
            status = builder ? builder->append(*entry)
                             : plan.append(entry->key.size(), entry->cell.value.size(), entry->cell.tombstone());
        }
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (end) {
            if (builder) {
                finish(builder->finish(cursor->skipped_subtrees()));
                return;
            }
            auto shape = plan.finish();
            status     = shape ? reserve_range_shape(destination, *shape, true, staging)
                               : Status::resource_exhausted("range import shape overflow");
            if (status.ok()) {
                builder.emplace(destination, view->view()->coverage, staging);
                status = open_cursor();
            }
        }
        if (status.ok()) {
            status = next();
        }
        if (!status.ok()) {
            finish(std::move(status));
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }

    void finish(Status status) noexcept
    {
        builder.reset();
        cursor.reset();
        ticket.reset();
        auto done = std::move(complete);
        try {
            done(std::move(status), staging);
        }
        catch (...) {
            CRB_LOG_ERROR("range copy receiver threw after completion");
        }
    }

    std::shared_ptr<PublishedReadViewContext>      view;
    TreeCore                                      &destination;
    KeyRange                                       range;
    RangeCopyComplete                              complete;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    PortableImportPlan                             plan;
    RuntimeRangeImport                             staging;
    std::optional<RangeBuilder>                    builder;
    std::shared_ptr<PublishedSnapshotEntries>      cursor;
};
} // namespace

Status copy_portable_range(TreeCore &source, TreeCore &destination, const KeyRange &range, RuntimeRangeImport &staging)
{
    const auto                                frontier = source.contiguous_slot();
    std::shared_ptr<PublishedReadViewContext> snapshot;
    auto                                      status = source.capture_read_only_view(&snapshot);
    if (!status.ok()) {
        return status;
    }
    if (snapshot->view()->coverage < frontier) {
        snapshot.reset();
        status = source.flush();
        if (!status.ok()) {
            return status;
        }
        status = source.capture_read_only_view(&snapshot);
        if (!status.ok()) {
            return status;
        }
    }
    return copy_read_only_view_range(snapshot, destination, range, staging);
}

Status copy_read_only_view_range(const std::shared_ptr<PublishedReadViewContext> &snapshot, TreeCore &destination,
                                 const KeyRange &range, RuntimeRangeImport &staging)
{
    auto status = reserve_range(snapshot, destination, range, staging);
    if (!status.ok()) {
        return status;
    }
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    status = PublishedSnapshotEntries::create(snapshot, &cursor, range);
    if (!status.ok()) {
        return status;
    }
    RangeBuilder builder(destination, snapshot->view()->coverage, staging);
    bool         complete = false;
    while (!complete) {
        std::optional<PublishedScanEntry> entry;
        status = cursor->next(&entry, &complete);
        if (!status.ok()) {
            return status;
        }
        if (!entry) {
            continue;
        }
        status = builder.append(*entry);
        if (!status.ok()) {
            return status;
        }
    }
    return builder.finish(cursor->skipped_subtrees());
}

Status copy_read_only_view_range_async(const std::shared_ptr<PublishedReadViewContext> &view, TreeCore &destination,
                                       const KeyRange &range, RangeCopyComplete complete)
try {
    if (!view || !complete) {
        return Status::invalid_argument();
    }
    auto                                           credit = destination.binding()->lifecycle_budget();
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    AsyncPageStore                                *backend = nullptr;
    auto status = destination.acquire_snapshot_window(credit, &ticket, &backend);
    if (!status.ok()) {
        return status;
    }
    auto copy = detail::reserved_shared<AsyncRangeCopy>(credit, view, destination, range, std::move(complete),
                                                        std::move(ticket));
    status    = copy->open_cursor();
    if (!status.ok()) {
        return status;
    }
    return copy->next();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
