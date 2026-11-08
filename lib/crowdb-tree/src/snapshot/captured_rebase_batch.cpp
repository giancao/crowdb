// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/writeback_rebase.h"
#include "snapshot/captured_rebase_internal.h"
#include "snapshot/primary_page_block.h"

namespace crowdb::tree
{
namespace
{
bool captured_receipt_is_unchanged(const PageEntry &page, const DurableCapturedFrame &receipt)
{
    if (!page.primary || page.primary->frame.page_block != receipt.page_block) {
        return false;
    }
    if (page.level != 0) {
        return page.inner_page_block == receipt.page_block;
    }
    return page.leaf && page.leaf->overlays.empty() && page.leaf->bases.size() == 1 &&
           page.leaf->bases.front().page_block == receipt.page_block;
}
} // namespace

void CapturedRebaseOperation::State::finish_batch(TreeOutcome outcome)
{
    for (const auto &page : request->transaction.pages) {
        if (outcome == TreeOutcome::Success) {
            ++report.installed;
        }
        else {
            ++report.deferred;
            if (outcome == TreeOutcome::Stale) {
                record_work(publisher.work_metrics(), TreeWorkCounter::RebaseMismatches, 1, cause);
            }
        }
        if (writeback_metrics) {
            writeback_metrics->finish(page.page_id, outcome);
        }
    }
}

void CapturedRebaseOperation::State::prepare_batch()
{
    const auto segment = next->first / MappingTable::kFanout;
    const auto live    = publisher.capture();
    // One fixed mapping segment bounds both CPU work and radix copies. Each
    // mapping segment is visited once, even when only a subset of its receipts change.
    for (; next != pages->frames.end() && next->first / MappingTable::kFanout == segment; ++next) {
        const auto &receipt = next->second;
        if (receipt.type == page_type::kOverflowFrame) {
            continue;
        }
        const auto original = captured->mapping.get(receipt.page_id);
        if (original && captured_receipt_is_unchanged(*original, receipt)) {
            ++report.reused;
            if (writeback_metrics) {
                writeback_metrics->finish(receipt.page_id, TreeOutcome::Success);
            }
            continue;
        }
        const auto                       current = live->mapping.get(receipt.page_id);
        std::shared_ptr<const PageEntry> replacement;
        const auto status = live->generation != captured->generation
                              ? Status::unavailable()
                              : build_writeback_rebase(original, current, receipt, captured->coverage, metadata,
                                                       &replacement, max_sources, captured->gc_floor);
        if (!status.ok()) {
            defer(receipt.page_id, status);
            continue;
        }
        add_replacement(receipt.page_id, current, std::move(replacement));
    }
}

void CapturedRebaseOperation::State::defer(uint64_t pid, Status status)
{
    ++report.deferred;
    const bool mismatch = status.code() == Code::kUnavailable;
    if (mismatch) {
        record_work(publisher.work_metrics(), TreeWorkCounter::RebaseMismatches, 1, cause);
    }
    if (writeback_metrics) {
        writeback_metrics->finish(pid, mismatch ? TreeOutcome::Stale : TreeOutcome::Error);
    }
}

void CapturedRebaseOperation::State::add_replacement(uint64_t pid, std::shared_ptr<const PageEntry> current,
                                                     std::shared_ptr<const PageEntry> replacement)
{
    if (!request) {
        request                     = detail::reserved_shared<PublicationRequest>(metadata, metadata);
        request->transaction.record = detail::reserved_shared<InstallRecord>(
            metadata, InstallToken{.generation = captured->generation, .sequence = pid});
        request->transaction.pages.reserve(MappingTable::kFanout);
    }
    request->transaction.pages.push_back(
        {.page_id = pid, .expected = std::move(current), .replacement = std::move(replacement)});
}
} // namespace crowdb::tree
