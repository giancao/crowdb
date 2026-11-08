// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_subtree.h"
#include "page_load.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_page_block.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace crowdb::tree
{
namespace
{
bool apply_summary(IndexSummary &total, const IndexSummary &part, bool add)
{
    if (!total.exact || !part.exact) {
        return false;
    }
    auto update = [add](uint64_t &field, uint64_t value) {
        if (add ? value > UINT64_MAX - field : value > field) {
            return false;
        }
        field = add ? field + value : field - value;
        return true;
    };
    return update(total.live_kv, part.live_kv) && update(total.live_key_bytes, part.live_key_bytes) &&
           update(total.live_value_bytes, part.live_value_bytes);
}
} // namespace

IndexSummary FlushBatch::State::Subtree::previous_summary(uint64_t page_id) const
{
    if (page_id == owner.captured->root_page_id) {
        const auto &stats = owner.captured->stats;
        if (stats.kv_unknown_pages == 0) {
            return {.live_kv          = stats.kv_known_count,
                    .live_key_bytes   = stats.key_known_bytes,
                    .live_value_bytes = stats.kv_known_bytes - stats.key_known_bytes,
                    .exact            = true};
        }
        return {};
    }
    if (const auto summary = owner.captured->index_summaries.find(page_id)) {
        return *summary;
    }
    const auto page = owner.captured->mapping.get(page_id);
    if (!page) {
        return {};
    }
    if (page->leaf && page->metrics.kv_known) {
        const auto &m = page->metrics;
        return {.live_kv          = m.kv_count,
                .live_key_bytes   = m.key_bytes,
                .live_value_bytes = m.kv_bytes - m.key_bytes,
                .exact            = true};
    }
    if (!page->leaf && page->primary && page->primary->frame.summary) {
        const auto &s = *page->primary->frame.summary;
        if (s.live_counts_known) {
            return {.live_kv          = s.live_keys,
                    .live_key_bytes   = s.live_key_bytes,
                    .live_value_bytes = s.live_value_bytes,
                    .exact            = true};
        }
    }
    return {};
}

IndexSummary FlushBatch::State::Subtree::index_summary(uint64_t page_id) const
{
    if (first_index_summary && first_index_summary->first == page_id) {
        return first_index_summary->second;
    }
    const auto match = std::ranges::find(more_index_summaries, page_id, [](const auto &item) { return item.first; });
    if (match != more_index_summaries.end()) {
        return match->second;
    }
    const auto page = lookup(page_id);
    if (page && page->leaf && page->metrics.kv_known) {
        const auto &m = page->metrics;
        return {.live_kv          = m.kv_count,
                .live_key_bytes   = m.key_bytes,
                .live_value_bytes = m.kv_bytes - m.key_bytes,
                .exact            = true};
    }
    return page == owner.captured->mapping.get(page_id) ? previous_summary(page_id) : IndexSummary{};
}

void FlushBatch::State::Subtree::set_index_summary(uint64_t page_id, IndexSummary summary)
{
    if (!first_index_summary || first_index_summary->first == page_id) {
        first_index_summary = {page_id, summary};
        return;
    }
    const auto existing = std::ranges::find(more_index_summaries, page_id, [](const auto &item) { return item.first; });
    if (existing != more_index_summaries.end()) {
        existing->second = summary;
    }
    else {
        more_index_summaries.emplace_back(page_id, summary);
    }
}

IndexSummary FlushBatch::State::Subtree::child_totals(const InnerPageState &page)
{
    IndexSummary total{.exact = true};
    for (const auto child_pid : page.children) {
        const auto *task = page_owner(child_pid);
        if (!apply_summary(total, task != nullptr ? task->index_summary(child_pid) : previous_summary(child_pid),
                           true)) {
            return {};
        }
    }
    return total;
}

void FlushBatch::State::Subtree::join()
{
    prepare_builder();

    detail::ReservedVector<Child> ordered{detail::ReservationAllocator<Child>(owner.metadata)};
    for (size_t i = 0; i < children.size(); ++i) {
        auto lower = i != 0 ? std::optional(inner->separators[i - 1]) : bounds.lower;
        if (!children[i]) {
            ordered.push_back({.task = this, .pid = inner->children[i], .lower = std::move(lower), .touched = false});
            continue;
        }
        auto &child = *children[i];
        for (size_t j = 0; j < child.output.pids.size(); ++j) {
            if (j != 0) {
                lower = child.output.separators[j - 1];
            }
            ordered.push_back(
                {.task = &child, .pid = child.output.pids[j], .lower = lower, .touched = true, .shrank = child.shrank});
        }
    }
    for (size_t i = 0; i + 1 < ordered.size();) {
        auto &a      = ordered[i];
        auto &b      = ordered[i + 1];
        auto  left   = a.task->lookup(a.pid);
        auto  right  = b.task->lookup(b.pid);
        bool  merged = false;
        if (a.shrank || b.shrank) {
            a.task->prepare_builder();
            b.task->prepare_builder();
            if (left->leaf && right->leaf && left->metrics.page_size_known && right->metrics.page_size_known &&
                left->metrics.kv_known && right->metrics.kv_known) {
                PageVersion version;
                auto        result = detail::next_page_version(*left->leaf, right->leaf.get(), &version);
                if (!result.ok()) {
                    complete(std::move(result));
                    return;
                }
                auto plan = detail::plan_published_leaf_merge(*left, *right,
                                                              {.left          = left->leaf,
                                                               .right         = right->leaf,
                                                               .left_page_id  = a.pid,
                                                               .right_page_id = b.pid,
                                                               .left_parent   = pid,
                                                               .right_parent  = pid},
                                                              builder->options.pages, version, 1);
                if (!plan.logical.status.ok()) {
                    complete(std::move(plan.logical.status));
                    return;
                }
                if (plan.logical.page) {
                    result = a.task->builder->make_leaf(a.pid, left, std::move(plan.logical.page), plan.primary_bytes,
                                                        right->allocation_owner,
                                                        plan.statistics ? &plan.statistics.value() : nullptr);
                    if (!result.ok()) {
                        complete(std::move(result));
                        return;
                    }
                    merged = true;
                }
            }
            else if (left->inner && right->inner && b.lower &&
                     std::min(left->inner->separators.size(), right->inner->separators.size()) <
                         builder->options.inner_merge_keys &&
                     left->inner->children.size() + right->inner->children.size() <=
                         builder->options.max_inner_children) {
                InnerPageState combined(*left->inner, builder->options.metadata);
                combined.children.insert(combined.children.end(), right->inner->children.begin(),
                                         right->inner->children.end());
                combined.separators.push_back(*b.lower);
                combined.separators.insert(combined.separators.end(), right->inner->separators.begin(),
                                           right->inner->separators.end());
                size_t bytes = kFrameHeaderSize + kFrameTrailerSize + (combined.children.size() * sizeof(uint64_t)) +
                               (combined.separators.size() * kInnerSlotSize);
                for (const auto &key : combined.separators) {
                    bytes += key.size();
                }
                if (bytes <= builder->options.max_inner_bytes) {
                    auto                        total         = a.task->index_summary(a.pid);
                    const auto                  right_summary = b.task->index_summary(b.pid);
                    detail::MaintenanceChildren outputs(owner.metadata);
                    auto result = a.task->builder->partition_inner(a.pid, left->level, combined, &outputs);
                    if (!result.ok()) {
                        complete(std::move(result));
                        return;
                    }
                    if (outputs.pids.size() != 1) {
                        complete(Status::internal_error("accepted index merge split again"));
                        return;
                    }
                    a.task->set_index_summary(a.pid,
                                              apply_summary(total, right_summary, true) ? total : IndexSummary{});
                    merged = true;
                }
            }
        }
        if (merged) {
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::MergeOperations, 1, TreeWorkCause::Flush);
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::MergeInputPages, 2, TreeWorkCause::Flush);
            record_work(owner.publisher.work_metrics(), TreeWorkCounter::MergeOutputPages, 1, TreeWorkCause::Flush);
            auto result = b.task->builder->replace(b.pid, {});
            if (!result.ok()) {
                complete(std::move(result));
                return;
            }
            a.touched = true;
            a.shrank  = a.shrank || b.shrank;
            ordered.erase(ordered.begin() + static_cast<ptrdiff_t>(i + 1));
        }
        else {
            ++i;
        }
    }
    InnerPageState combined(builder->options.metadata);
    for (size_t i = 0; i < ordered.size(); ++i) {
        if (owner.options.persistent && ordered[i].touched) {
            final_pages.push_back({.task = ordered[i].task, .pid = ordered[i].pid, .level = expected->level - 1});
        }
        combined.children.push_back(ordered[i].pid);
        if (i != 0) {
            combined.separators.push_back(ordered[i].lower.value());
        }
    }
    output.pids.clear();
    output.separators.clear();
    auto sum_children = [&](std::span<const Child> range) {
        IndexSummary total{.exact = true};
        for (const auto &child : range) {
            const auto summary = child.touched ? child.task->index_summary(child.pid) : previous_summary(child.pid);
            if (!apply_summary(total, summary, true)) {
                return IndexSummary{};
            }
        }
        return total;
    };
    const bool fewer_children = combined.children.size() < inner->children.size();
    if (combined.children == inner->children && combined.separators == inner->separators) {
        output.pids.push_back(pid);
        auto total = previous_summary(pid);
        bool exact = total.exact;
        if (exact) {
            for (const auto &child : ordered) {
                if (!child.touched) {
                    continue;
                }
                const auto before = previous_summary(child.pid);
                const auto after  = child.task->index_summary(child.pid);
                if (before != after && !(apply_summary(total, before, false) && apply_summary(total, after, true))) {
                    exact = false;
                    break;
                }
            }
        }
        set_index_summary(pid, exact ? total : sum_children(ordered));
    }
    else {
        auto result = builder->partition_inner(pid, expected->level, combined, &output);
        if (!result.ok()) {
            complete(std::move(result));
            return;
        }
        auto remaining = std::span<const Child>(ordered);
        for (const auto page_id : output.pids) {
            const auto count = lookup(page_id)->inner->children.size();
            set_index_summary(page_id, sum_children(remaining.first(count)));
            remaining = remaining.subspan(count);
        }
    }
    shrank = fewer_children && output.pids.size() == 1;
#ifdef CROWDB_TREE_TEST_UTIL
    if (owner.options.after_page_for_tests) {
        owner.options.after_page_for_tests(pid, expected->level);
    }
#endif
    finish_pages();
}

std::shared_ptr<const PageEntry> FlushBatch::State::Subtree::lookup(uint64_t page_id) const
{
    return builder ? builder->lookup(page_id) : owner.captured->mapping.get(page_id);
}

Status FlushBatch::State::Subtree::collect(PublicationTransaction                         &transaction,
                                           detail::ReservedVector<IndexSummaries::Change> &summaries,
                                           uint64_t                                       &handoff_bytes)
{
    if (range) {
        handoff_bytes += range->bytes;
    }
    for (auto &child : children) {
        if (child) {
            auto status = child->collect(transaction, summaries, handoff_bytes);
            if (!status.ok()) {
                return status;
            }
        }
    }
    auto append_summary = [&](uint64_t id, const IndexSummary &summary) {
        if (id == *transaction.root_page_id) {
            return;
        }
        const auto page = lookup(id);
        if (!page || page->level == 0) {
            return;
        }
        IndexSummary baseline;
        if (page->primary && page->primary->frame.summary) {
            const auto &s = *page->primary->frame.summary;
            if (s.live_counts_known) {
                baseline = {.live_kv          = s.live_keys,
                            .live_key_bytes   = s.live_key_bytes,
                            .live_value_bytes = s.live_value_bytes,
                            .exact            = true};
            }
        }
        if (summary != baseline) {
            summaries.push_back({.pid = static_cast<uint32_t>(id), .summary = summary});
        }
        else if (owner.captured->index_summaries.find(id)) {
            summaries.push_back({.pid = static_cast<uint32_t>(id), .summary = std::nullopt});
        }
    };
    if (first_index_summary) {
        append_summary(first_index_summary->first, first_index_summary->second);
    }
    for (const auto &[id, summary] : more_index_summaries) {
        append_summary(id, summary);
    }
    if (!builder) {
        return Status::Ok();
    }
    MaintenancePublicationResult result(builder->options.metadata);
    auto                         status = builder->finish(&result);
    if (!status.ok()) {
        return status;
    }
    for (auto &page : result.transaction.pages) {
        transaction.pages.push_back(std::move(page));
    }
    return Status::Ok();
}

void FlushBatch::State::start_subtree()
{
    if (!subtree) {
        next_pid.store(captured->next_page_id, std::memory_order_relaxed);
        const auto first = first_input({});
        const bool selected =
            first.has_value() || (options.persistent && options.persistence &&
                                  options.persistence->selected(captured->root_page_id, options.selection));
        subtree =
            detail::reserved_shared<Subtree>(metadata, *this, captured->root_page_id, options.selection, selected);
    }
    else {
        subtree->reset_work();
    }
    next_pid.store(captured->next_page_id, std::memory_order_relaxed);
    operation = detail::reserved_unique<Attempt>(metadata, subtree->sender(), shared_from_this());
    stdexec::start(operation->connected);
}

Status FlushBatch::State::Subtree::prepare_root()
{
    prepare_builder();
    auto    &outputs = output;
    uint64_t root    = outputs.pids.front();
    if (outputs.pids.size() > 1) {
        auto status = builder->grow_root(std::move(outputs), expected->level);
        if (!status.ok()) {
            return status;
        }
        root = builder->root_page_id();
        MaintenancePublicationResult changes(owner.metadata);
        auto                         built = builder->finish(&changes);
        if (!built.ok()) {
            return built;
        }
        std::ranges::sort(changes.transaction.pages, {},
                          [](const auto &change) { return change.replacement ? change.replacement->level : 0; });
        for (const auto &change : changes.transaction.pages) {
            if (change.replacement && change.replacement->inner && change.page_id != pid &&
                !owner.captured->mapping.get(change.page_id)) {
                set_index_summary(change.page_id, child_totals(*change.replacement->inner));
            }
        }
    }
    // Only the root may contract its level. Descendant joins retain their
    // level until their parent has made its own sibling decision.
    auto find = [&](auto &&self, Subtree &task, uint64_t pid) -> std::shared_ptr<const PageEntry> {
        if (task.builder) {
            auto page = task.builder->lookup(pid);
            if (page != task.builder->view->mapping.get(pid)) {
                return page;
            }
        }
        for (auto &child : task.children) {
            if (!child) {
                continue;
            }
            auto page = self(self, *child, pid);
            if (page != owner.captured->mapping.get(pid)) {
                return page;
            }
        }
        return owner.captured->mapping.get(pid);
    };
    for (size_t depth = 0; active && depth < owner.options.max_route_depth; ++depth) {
        const auto page = find(find, *this, root);
        if (!page || !page->inner || page->inner->children.size() != 1) {
            break;
        }
        const auto child = page->inner->children.front();
        // Root deletions belong to the original task that owns that PID.
        auto remove = [&](auto &&self, Subtree &task) -> Status {
            if (task.pid == root ||
                (task.builder && task.builder->lookup(root) != task.builder->view->mapping.get(root))) {
                task.prepare_builder();
                return task.builder->replace(root, {});
            }
            for (auto &next : task.children) {
                if (next != nullptr) {
                    auto status = self(self, *next);
                    if (!status.ok()) {
                        return status;
                    }
                }
            }
            return Status::Ok();
        };
        auto removed = remove(remove, *this);
        if (!removed.ok()) {
            return removed;
        }
        root = child;
    }
    final_root = root;
    return Status::Ok();
}

Status FlushBatch::State::collect_subtree(PublicationTransaction &transaction, uint64_t &handoff_bytes)
{
    if (!options.consolidation) {
        return Status::internal_error("flush consolidation options are absent");
    }
    if (subtree->final_root == kInvalidPageId) {
        auto prepared = subtree->prepare_root();
        if (!prepared.ok()) {
            return prepared;
        }
    }
    const auto root                   = subtree->final_root;
    const auto snapshot_metadata      = options.consolidation->snapshot_metadata;
    transaction.root_page_id          = root;
    transaction.expected_root_page_id = captured->root_page_id;
    transaction.expected_next_page_id = captured->next_page_id;
    transaction.next_page_id          = next_pid.load(std::memory_order_relaxed);
    transaction.snapshot_metadata     = snapshot_metadata;
    detail::ReservedVector<IndexSummaries::Change> summaries{
        detail::ReservationAllocator<IndexSummaries::Change>(metadata)};
    auto status = subtree->collect(transaction, summaries, handoff_bytes);
    if (!status.ok()) {
        return status;
    }
    std::sort(transaction.pages.begin(), transaction.pages.end(),
              [](const auto &a, const auto &b) { return a.page_id < b.page_id; });
    for (const auto &page : transaction.pages) {
        if ((!page.replacement || page.replacement->level == 0) && page.expected && page.expected->level != 0 &&
            captured->index_summaries.find(page.page_id)) {
            summaries.push_back({.pid = static_cast<uint32_t>(page.page_id), .summary = std::nullopt});
        }
    }
    if (root != captured->root_page_id && captured->index_summaries.find(root)) {
        summaries.push_back({.pid = static_cast<uint32_t>(root), .summary = std::nullopt});
    }
    if (!summaries.empty()) {
        std::ranges::sort(summaries, {}, &IndexSummaries::Change::pid);
        const auto duplicates = std::ranges::unique(summaries, {}, &IndexSummaries::Change::pid);
        summaries.erase(duplicates.begin(), duplicates.end());
        transaction.index_summaries = captured->index_summaries.merge(summaries, metadata);
    }
    const auto reserved_metadata = std::dynamic_pointer_cast<detail::AllocationReservation>(metadata);
    if (!reserved_metadata) {
        return Status::Ok();
    }
    // Legacy standalone maintenance can still supply prepaid metadata.
    const auto bytes =
        detail::AllocationBound(detail::shared_allocation_bound<detail::AllocationReservation>())
            .add_product(transaction.pages.size(), PublicationCoordinator::snapshot_change_reserve_bytes())
            .value();
    if (!bytes) {
        return Status::resource_exhausted("flush publication metadata bound overflow");
    }
    const auto reserved_snapshot = std::dynamic_pointer_cast<detail::AllocationReservation>(snapshot_metadata);
    if (!reserved_snapshot || reserved_snapshot->available() < *bytes) {
        if (reserved_snapshot) {
            reserved_snapshot->seal();
        }
        auto credit =
            detail::ResourceBudget::try_reserve(reserved_metadata->account(), detail::BudgetClass::kRouting, *bytes);
        if (!credit) {
            return Status::resource_exhausted("flush publication metadata credit unavailable");
        }
        transaction.snapshot_metadata = detail::AllocationReservation::create(std::move(*credit));
    }
    return Status::Ok();
}

} // namespace crowdb::tree
