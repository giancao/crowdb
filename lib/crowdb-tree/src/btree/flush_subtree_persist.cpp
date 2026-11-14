// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "flush_subtree.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_backing.h"

#include <algorithm>

namespace crowdb::tree
{
struct FlushBatch::State::Subtree::Writes
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        Subtree *parent;

        [[nodiscard]] auto get_env() const noexcept
            -> stdexec::prop<stdexec::get_start_scheduler_t, stdexec::inline_scheduler>
        {
            return {stdexec::get_start_scheduler, stdexec::inline_scheduler{}};
        }

        void set_value() noexcept
        {
            auto keep = parent->page_writes;
            parent->schedule();
        }
    };

    using Join = stdexec::connect_result_t<decltype(std::declval<stdexec::simple_counting_scope &>().join()), Receiver>;

    Writes(Subtree &parent, std::span<FinalPage> pages, uint64_t gc_floor)
        : parent(parent),
          pages(pages),
          gc_floor(gc_floor),
          joined(stdexec::connect(scope.join(), Receiver{&parent}))
    {
    }

    void fail(Status result) noexcept
    {
        if (!failed.exchange(true, std::memory_order_relaxed)) {
            failure = std::move(result);
        }
    }

    static void submit(void *raw, detail::CallbackValueSender::Complete done, void *context) noexcept
    {
        auto &target = *static_cast<FinalPage *>(raw);
        auto &writes = *target.writes;
        auto &self   = writes.parent;
        if ((self.owner.lifecycle.load(std::memory_order_acquire) & kStopped) != 0) {
            writes.fail(Status::unavailable());
            done(context);
            return;
        }
        try {
            auto page = target.task->lookup(target.pid);
            if (!page) {
                done(context);
                return;
            }
            self.owner.options.persistence->write(
                {
                    .page         = page,
                    .page_id      = target.pid,
                    .coverage     = self.owner.report.frontier,
                    .gc_floor     = writes.gc_floor,
                    .next_page_id = self.owner.next_pid.load(std::memory_order_relaxed),
                    .context      = {.range = page->leaf ? page->leaf->range : self.bounds,
                                     .inner_fences =
                                         page->level != 0 ? std::optional(self.fences(target.pid)) : std::nullopt},
                    .children_summary =
                        page->level != 0 ? std::optional(self.child_summary(target.pid)) : std::nullopt
            },
                [&self, lease = self.owner.metadata](uint64_t count, ReservedOverflowIds *out) {
                    auto next = self.owner.next_pid.load(std::memory_order_relaxed);
                    do {
                        if (count > MappingView::kPageLimit - next) {
                            return Status::resource_exhausted("flush overflow PID space exhausted");
                        }
                    } while (!self.owner.next_pid.compare_exchange_weak(next, next + count, std::memory_order_relaxed));
                    *out = {.first = next, .count = count, .owner = lease};
                    return Status::Ok();
                },
                [&target, done, context, keep = self.owner.weak_from_this().lock()](Status result,
                                                                                    auto   replacement) noexcept {
                    if (!result.ok()) {
                        target.writes->fail(std::move(result));
                    }
                    target.replacement = std::move(replacement);
                    done(context);
                });
        }
        catch (const std::bad_alloc &) {
            writes.fail(Status::resource_exhausted());
            done(context);
        }
        catch (...) {
            writes.fail(Status::internal_error());
            done(context);
        }
    }

    Subtree                       &parent;
    std::span<FinalPage>           pages;
    uint64_t                       gc_floor;
    stdexec::simple_counting_scope scope;
    Join                           joined;
    std::atomic<bool>              failed{false};
    Status                         failure;
};

void FlushBatch::State::Subtree::finish_pages()
{
    if (this == owner.subtree.get()) {
        auto result = prepare_root();
        if (!result.ok()) {
            complete(std::move(result));
            return;
        }
        if (owner.options.persistent) {
            MaintenancePublicationResult changes(owner.metadata);
            result = builder->finish(&changes);
            if (!result.ok()) {
                complete(std::move(result));
                return;
            }
            for (const auto &change : changes.transaction.pages) {
                if (change.replacement) {
                    final_pages.push_back({.task = this, .pid = change.page_id, .level = change.replacement->level});
                }
            }
            // An unchanged root may still lack a durable representation.
            auto *root_owner = page_owner(final_root);
            root_owner       = root_owner != nullptr ? root_owner : this;
            if (const auto page = root_owner->lookup(final_root)) {
                final_pages.push_back({.task = root_owner, .pid = final_root, .level = page->level});
            }
            std::sort(final_pages.begin(), final_pages.end(),
                      [](const auto &a, const auto &b) { return std::tie(a.level, a.pid) < std::tie(b.level, b.pid); });
            final_pages.erase(std::unique(final_pages.begin(), final_pages.end(),
                                          [](const auto &a, const auto &b) { return a.pid == b.pid; }),
                              final_pages.end());
        }
    }
    joined = true;
    persist_next();
}

FlushBatch::State::Subtree *FlushBatch::State::Subtree::page_owner(uint64_t page_id)
{
    if (builder && builder->lookup(page_id) != builder->view->mapping.get(page_id)) {
        return this;
    }
    if (pid == page_id) {
        return this;
    }
    for (auto &child : children) {
        if (child) {
            if (auto *found = child->page_owner(page_id)) {
                return found;
            }
        }
    }
    return nullptr;
}

void FlushBatch::State::Subtree::persist_next()
{
    if (!owner.options.persistent) {
        complete(Status::Ok());
        return;
    }
    if (!owner.options.persistence || !owner.options.consolidation) {
        complete(Status::invalid_argument("persistent flush requires its page sink"));
        return;
    }
    if (page_writes) {
        auto completed = std::move(page_writes);
        if (completed->failed.load(std::memory_order_relaxed)) {
            complete(std::move(completed->failure));
            return;
        }
        for (auto &target : completed->pages) {
            if (target.replacement) {
                target.task->prepare_builder();
                auto result = target.task->builder->replace(target.pid, std::move(target.replacement));
                if (!result.ok()) {
                    complete(std::move(result));
                    return;
                }
            }
        }
    }
    if (persist_at == final_pages.size()) {
        complete(Status::Ok());
        return;
    }
    // Root growth can produce several levels. A parent summary uses the
    // receipts from the previous level; sibling writes have no such dependency.
    const auto level = final_pages[persist_at].level;
    auto       end   = persist_at + 1;
    while (end < final_pages.size() && final_pages[end].level == level) {
        ++end;
    }
    auto writes = detail::reserved_shared<Writes>(owner.metadata, *this,
                                                  std::span(final_pages).subspan(persist_at, end - persist_at),
                                                  owner.options.consolidation->pages.gc_floor);
    page_writes = writes;
    persist_at  = end;
    try {
        for (auto &target : writes->pages) {
            if (writes->failed.load(std::memory_order_relaxed)) {
                break;
            }
            target.writes = writes.get();
            stdexec::spawn(detail::CallbackValueSender(&target, &Writes::submit), writes->scope.get_token(),
                           stdexec::env{
                               stdexec::prop{stdexec::get_allocator, MetadataAllocator<std::byte>(owner.metadata)}
            });
        }
    }
    catch (const std::bad_alloc &) {
        writes->fail(Status::resource_exhausted());
    }
    catch (...) {
        writes->fail(Status::internal_error());
    }
    writes->scope.close();
    stdexec::start(writes->joined);
}

std::shared_ptr<const PublishedPage> FlushBatch::State::Subtree::find_page(uint64_t page_id) const
{
    if (builder) {
        auto page = builder->lookup(page_id);
        if (page != builder->view->mapping.get(page_id)) {
            return page;
        }
    }
    auto captured = owner.captured->mapping.get(page_id);
    // Descendants cannot replace an existing peer/ancestor PID. An existing
    // leaf also keeps its original task owner through sibling merge/split.
    // Prune unrelated branches instead of searching every leaf for each
    // index child's summary and fence.
    if (captured && expected &&
        (captured->level >= expected->level || (captured->leaf && !bounds.overlaps(captured->leaf->range)))) {
        return captured;
    }
    for (const auto &child : children) {
        if (child) {
            auto page = child->find_page(page_id);
            if (page != owner.captured->mapping.get(page_id)) {
                return page;
            }
        }
    }
    return captured;
}

FinalPageSummary FlushBatch::State::Subtree::child_summary(uint64_t page_id) const
{
    FinalPageSummary result;
    const auto       page = find_page(page_id);
    if (!page || !page->inner) {
        return result;
    }
    for (auto child : page->inner->children) {
        const auto output = find_page(child);
        if (!output || !output->primary || !output->primary->frame.summary ||
            !output->primary->frame.summary->live_counts_known) {
            return {};
        }
        const auto &part = *output->primary->frame.summary;
        if (part.live_keys > UINT64_MAX - result.live_keys ||
            part.live_key_bytes > UINT64_MAX - result.live_key_bytes ||
            part.live_value_bytes > UINT64_MAX - result.live_value_bytes) {
            return {};
        }
        result.oldest_tombstone_slot = std::min(result.oldest_tombstone_slot, part.oldest_tombstone_slot);
        result.live_keys += part.live_keys;
        result.live_key_bytes += part.live_key_bytes;
        result.live_value_bytes += part.live_value_bytes;
    }
    result.live_counts_known = true;
    return result;
}

InnerEncodeFences FlushBatch::State::Subtree::fences(uint64_t page_id) const
{
    auto edge = [&](bool upper) {
        auto next = page_id;
        for (size_t depth = 0; depth < owner.options.max_route_depth; ++depth) {
            auto page = find_page(next);
            if (!page) {
                return kInvalidPageId;
            }
            if (page->level == 0) {
                return next;
            }
            if (!page->inner) {
                if (page->primary && page->primary->frame.summary && page->primary->frame.summary->inner_fences) {
                    return upper ? page->primary->frame.summary->inner_fences->upper_leaf
                                 : page->primary->frame.summary->inner_fences->lower_leaf;
                }
                return kInvalidPageId;
            }
            next = upper ? page->inner->children.back() : page->inner->children.front();
        }
        return kInvalidPageId;
    };
    return {.lower_leaf = edge(false), .upper_leaf = edge(true)};
}
} // namespace crowdb::tree
