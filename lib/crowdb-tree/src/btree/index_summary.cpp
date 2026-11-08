// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/index_summary.h"

#include <algorithm>

namespace crowdb::tree
{
bool IndexSummaries::Directory::retire_step() noexcept
{
    for (size_t i = 0; i != 64 && !segments.empty(); ++i) {
        segments.pop_back();
    }
    return segments.empty();
}

std::optional<IndexSummary> IndexSummaries::find(uint64_t pid) const noexcept
{
    if (!directory_) {
        return std::nullopt;
    }
    const auto &segments = directory_->segments;
    const auto  id       = pid / kSegmentSize;
    const auto  segment  = std::ranges::lower_bound(segments, id, {}, &SegmentEntry::id);
    if (segment == segments.end() || segment->id != id) {
        return std::nullopt;
    }
    const auto &records = *segment->records;
    const auto  record  = std::ranges::lower_bound(records, pid, {}, &Record::pid);
    if (record == records.end() || record->pid != pid) {
        return std::nullopt;
    }
    return record->value();
}

bool IndexSummaries::same(const IndexSummaries &other) const noexcept
{
    if (directory_ == other.directory_) {
        return true;
    }
    if (!directory_ || !other.directory_ || size() != other.size()) {
        return false;
    }
    return std::ranges::equal(
        directory_->segments, other.directory_->segments, [](const SegmentEntry &left, const SegmentEntry &right) {
            return left.id == right.id && (left.records == right.records || *left.records == *right.records);
        });
}

void IndexSummaries::visit_from(uint32_t &next_pid, void *context, Visitor visitor) const
{
    if (!directory_) {
        return;
    }
    const auto &segments = directory_->segments;
    auto        segment  = std::ranges::lower_bound(segments, next_pid / kSegmentSize, {}, &SegmentEntry::id);
    if (segment == segments.end()) {
        return;
    }
    auto record = std::ranges::lower_bound(*segment->records, next_pid, {}, &Record::pid);
    for (;;) {
        for (; record != segment->records->end(); ++record) {
            if (!visitor(context, record->pid, record->value())) {
                next_pid = record->pid;
                return;
            }
            next_pid = record->pid + 1;
        }
        if (++segment == segments.end()) {
            return;
        }
        record = segment->records->begin();
    }
}

IndexSummaries IndexSummaries::merge(std::span<const Change>                  changes,
                                     const std::shared_ptr<AllocationBudget> &budget) const
{
    const auto previous =
        directory_ ? std::span<const SegmentEntry>(directory_->segments) : std::span<const SegmentEntry>{};
    auto                       copied = previous.begin();
    std::shared_ptr<Directory> next;
    while (!changes.empty()) {
        const auto id  = changes.front().pid / kSegmentSize;
        const auto end = std::ranges::partition_point(
            changes, [id](const Change &change) { return change.pid / kSegmentSize == id; });
        const auto group    = changes.first(static_cast<size_t>(end - changes.begin()));
        changes             = changes.subspan(group.size());
        const auto position = std::ranges::lower_bound(copied, previous.end(), id, {}, &SegmentEntry::id);
        const bool present  = position != previous.end() && position->id == id;
        const auto old      = present ? std::span<const Record>(*position->records) : std::span<const Record>{};
        size_t     size     = old.size();
        bool       changed  = false;
        auto       record   = old.begin();
        for (const auto &change : group) {
            record           = std::ranges::lower_bound(record, old.end(), change.pid, {}, &Record::pid);
            const bool found = record != old.end() && record->pid == change.pid;
            if (found && !change.summary) {
                --size;
                changed = true;
            }
            else if (!found && change.summary) {
                ++size;
                changed = true;
            }
            else if (found && record->value() != *change.summary) {
                changed = true;
            }
        }
        if (!changed) {
            continue;
        }
        if (!next) {
            next = detail::reserved_shared<Directory>(budget, budget);
            next->segments.reserve(previous.size());
            next->size = this->size();
        }
        next->size = next->size - old.size() + size;
        if (copied != position) {
            next->segments.insert(next->segments.end(), copied, position);
        }
        copied = present ? std::next(position) : position;
        if (size == 0) {
            continue;
        }
        auto records = detail::reserved_shared<Segment>(budget, detail::ReservationAllocator<Record>(budget));
        records->reserve(size);
        record = old.begin();
        for (const auto &change : group) {
            const auto boundary = std::ranges::lower_bound(record, old.end(), change.pid, {}, &Record::pid);
            if (record != boundary) {
                records->insert(records->end(), record, boundary);
            }
            record = boundary;
            if (record != old.end() && record->pid == change.pid) {
                ++record;
            }
            if (change.summary) {
                const auto &summary = *change.summary;
                records->push_back({.live_kv          = summary.live_kv,
                                    .live_key_bytes   = summary.live_key_bytes,
                                    .live_value_bytes = summary.live_value_bytes,
                                    .pid              = change.pid,
                                    .exact            = summary.exact});
            }
        }
        if (record != old.end()) {
            records->insert(records->end(), record, old.end());
        }
        next->segments.push_back({.id = id, .records = std::move(records)});
    }
    if (!next) {
        return *this;
    }
    if (copied != previous.end()) {
        next->segments.insert(next->segments.end(), copied, previous.end());
    }
    IndexSummaries result;
    if (!next->segments.empty()) {
        result.directory_ = std::move(next);
    }
    return result;
}
} // namespace crowdb::tree
