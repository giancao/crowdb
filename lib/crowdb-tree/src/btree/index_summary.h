// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_reservation.h"

#include <optional>
#include <span>

namespace crowdb::tree
{
struct IndexSummary
{
    uint64_t live_kv          = 0;
    uint64_t live_key_bytes   = 0;
    uint64_t live_value_bytes = 0;
    bool     exact            = false;

    bool operator==(const IndexSummary &) const noexcept = default;
};

class IndexSummaries
{
  public:
    struct Change
    {
        uint32_t pid;
        // An absent summary removes an override. An unknown summary overrides the frame.
        std::optional<IndexSummary> summary;
    };

    [[nodiscard]] std::optional<IndexSummary> find(uint64_t pid) const noexcept;

    [[nodiscard]] bool same(const IndexSummaries &other) const noexcept;

    [[nodiscard]] size_t size() const noexcept
    {
        return directory_ ? directory_->size : 0;
    }

    // Start at the next PID in order. The callback returns false to leave that PID for the next window.
    using Visitor = bool (*)(void *, uint32_t, IndexSummary);
    void visit_from(uint32_t &next_pid, void *context, Visitor visitor) const;
    // Changes must be ordered by PID, with one final value per PID.
    [[nodiscard]] IndexSummaries merge(std::span<const Change>                  changes,
                                       const std::shared_ptr<AllocationBudget> &budget) const;

  private:
    static constexpr uint32_t kSegmentSize = 1024;

    struct Record
    {
        uint64_t live_kv;
        uint64_t live_key_bytes;
        uint64_t live_value_bytes;
        uint32_t pid;
        bool     exact;

        bool operator==(const Record &) const noexcept = default;

        [[nodiscard]] IndexSummary value() const noexcept
        {
            return {.live_kv          = live_kv,
                    .live_key_bytes   = live_key_bytes,
                    .live_value_bytes = live_value_bytes,
                    .exact            = exact};
        }
    };

    static_assert(sizeof(Record) == 32);
    using Segment = detail::ReservedVector<Record>;

    struct SegmentEntry
    {
        uint32_t                       id;
        std::shared_ptr<const Segment> records;
    };

    struct Directory
    {
        explicit Directory(const std::shared_ptr<AllocationBudget> &budget)
            : segments(detail::ReservationAllocator<SegmentEntry>(budget))
        {
        }

        bool                                 retire_step() noexcept;
        detail::ReservedVector<SegmentEntry> segments;
        size_t                               size = 0;
    };

    std::shared_ptr<const Directory> directory_;
};
} // namespace crowdb::tree
