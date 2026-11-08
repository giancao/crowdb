// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/published_inspection.h"

#include "btree/allocation_bound.h"
#include "btree/page_encode.h"
#include "btree/tree_core_internal.h"
#include "btree/tree_core_read_internal.h"
#include "crowdb-common/log.h"
#include "host_completion.h"

#include <algorithm>
#include <array>

namespace crowdb::tree
{
namespace
{
constexpr uint32_t kInspectionBytes = 1024 * 1024;
constexpr size_t   kPathDepth       = 32;

struct CopiedFrame
{
    CopiedFrame(const std::shared_ptr<AllocationBudget> &credit, size_t capacity)
        : bytes(credit, capacity, AllocationKind::kFrame)
    {
    }

    AllocationLease             bytes;
    std::shared_ptr<const void> dependencies;
};

struct Inspection : std::enable_shared_from_this<Inspection>
{
    using Step = void (Inspection::*)();

    Inspection(core_read_detail::ReadPreparation prepared, TreeCore &core, std::span<const uint32_t> path,
               TreeCore::InspectComplete complete)
        : prepared(std::move(prepared)),
          core(core),
          view(this->prepared.view),
          complete(std::move(complete)),
          path_size(path.size()),
          target(view->root_page_id)
    {
        std::ranges::copy(path, steps.begin());
        frames.frame_bytes        = kInspectionBytes;
        frames.inline_value_bytes = kInspectionBytes;
        frames.max_sources        = this->prepared.limits.max_page_sources;
        frames.work_metrics       = core.publisher().work_metrics();
        frames.metadata           = this->prepared.credit;
    }

    void schedule() noexcept
    {
        core.cpu_progress()->request(ready, weak_from_this().lock());
    }

    static void run(void *context) noexcept
    {
        auto &inspection = *static_cast<Inspection *>(context);
        try {
            (inspection.*inspection.next)();
        }
        catch (const std::bad_alloc &) {
            inspection.finish(Status::resource_exhausted());
        }
        catch (...) {
            inspection.finish(Status::internal_error());
        }
    }

    void load(uint64_t pid, std::optional<size_t> base, Step continuation)
    {
        const auto page = view->mapping.get(pid);
        if (!page || (base && (!page->leaf || *base >= page->leaf->bases.size()))) {
            finish(Status::corruption("inspection route or base is absent"));
            return;
        }
        load_pid  = pid;
        load_base = base;
        resume    = continuation;
        next      = &Inspection::loaded;
        if (base) {
            prepared.loader->load_leaf(page->leaf->bases[*base], [self = shared_from_this()](Status status, auto leaf) {
                self->leaf        = std::move(leaf);
                self->load_status = std::move(status);
                self->schedule();
            });
        }
        else {
            prepared.loader->load_inner(page->inner_page_block, [self = shared_from_this()](Status status, auto inner) {
                self->inner       = std::move(inner);
                self->load_status = std::move(status);
                self->schedule();
            });
        }
    }

    void loaded()
    {
        if (!load_status.ok()) {
            finish(std::move(load_status));
            return;
        }
        const auto old  = view->mapping.get(load_pid);
        auto       page = detail::reserved_shared<PageEntry>(prepared.credit, *old);
        if (load_base) {
            if (!leaf) {
                finish(Status::corruption("inspection loader returned no leaf"));
                return;
            }
            auto       state   = detail::reserved_shared<PageState>(prepared.credit, *old->leaf, prepared.credit);
            const auto maximum = state->bases[*load_base].max_slot;
            if (maximum) {
                for (size_t i = 0; i < leaf->count(); ++i) {
                    if (CellView(leaf->cell(i)).slot() > *maximum) {
                        finish(Status::corruption("inspection base exceeds captured coverage"));
                        return;
                    }
                }
            }
            state->bases[*load_base].resident = std::move(leaf);
            page->leaf                        = std::move(state);
        }
        else {
            if (!inner) {
                finish(Status::corruption("inspection loader returned no index"));
                return;
            }
            page->inner = std::move(inner);
        }
        auto                    copied_view = detail::reserved_shared<TreeRoot>(prepared.credit, *view);
        const PageReplacement   change{.page_id = load_pid, .expected = old, .replacement = std::move(page)};
        MappingTable::CopyStats copied;
        copied_view->mapping = copied_view->mapping.replace({&change, 1}, copied, prepared.credit);
        view                 = std::move(copied_view);
        (this->*resume)();
    }

    void route()
    {
        for (; depth < path_size; ++depth) {
            const auto page = view->mapping.get(target);
            if (!page || page->level == 0) {
                finish(Status::unavailable("page path changed; restart inspection"));
                return;
            }
            if (!page->inner) {
                load(target, {}, &Inspection::route);
                return;
            }
            const auto  child = steps[depth];
            const auto &index = *page->inner;
            if (index.children.empty() || index.children.size() != index.separators.size() + 1 ||
                child >= index.children.size()) {
                finish(Status::invalid_argument("child outside inspected page"));
                return;
            }
            if (child != 0) {
                encode_context.range.lower = index.separators[child - 1];
            }
            if (child < index.separators.size()) {
                encode_context.range.upper = index.separators[child];
            }
            const auto next_page = view->mapping.get(index.children[child]);
            if (!next_page || next_page->level + 1 != page->level) {
                finish(Status::corruption("inspection route level mismatch"));
                return;
            }
            target = index.children[child];
        }
        const auto page = view->mapping.get(target);
        if (!page) {
            finish(Status::not_found());
            return;
        }
        if (page->level == 0) {
            encode_context.range = page->leaf->range;
            encode();
            return;
        }
        edge_pid = target;
        edge();
    }

    void edge()
    {
        while (edge_depth < prepared.limits.max_route_depth) {
            const auto page = view->mapping.get(edge_pid);
            if (!page) {
                finish(Status::corruption("inspection fence route is absent"));
                return;
            }
            if (page->level == 0) {
                if (right_edge) {
                    encode_context.inner_fences = InnerEncodeFences{.lower_leaf = lower_edge, .upper_leaf = edge_pid};
                    encode();
                    return;
                }
                lower_edge = edge_pid;
                right_edge = true;
                edge_pid   = target;
                edge_depth = 0;
                continue;
            }
            if (!page->inner) {
                load(edge_pid, {}, &Inspection::edge);
                return;
            }
            const auto &children = page->inner->children;
            if (children.empty()) {
                finish(Status::corruption("inspection index has no children"));
                return;
            }
            const auto child     = right_edge ? children.back() : children.front();
            const auto next_page = view->mapping.get(child);
            if (!next_page || next_page->level + 1 != page->level) {
                finish(Status::corruption("inspection fence route level mismatch"));
                return;
            }
            edge_pid = child;
            ++edge_depth;
        }
        finish(Status::resource_exhausted("inspection fence route exceeds depth bound"));
    }

    void encode()
    {
        if (attempts++ > frames.max_sources) {
            finish(Status::resource_exhausted("inspection source bound exceeded"));
            return;
        }
        auto encoded = prepare_final_page(view, target, encode_context, frames);
        if (!encoded.status.ok()) {
            finish(std::move(encoded.status));
            return;
        }
        if (encoded.needs_base) {
            load(target, encoded.needs_base->base_index, &Inspection::encode);
            return;
        }
        if (encoded.needs_inner) {
            load(target, {}, &Inspection::encode);
            return;
        }
        if (encoded.encoder->summary().new_overflow_frames != 0) {
            finish(Status::resource_exhausted("inspection frame exceeds its 1 MiB output bound"));
            return;
        }
        NativeFrame result;
        result.page_id = target;
        result.frame.resize(kInspectionBytes);
        auto owner = detail::reserved_shared<CopiedFrame>(prepared.credit, prepared.credit, result.frame.capacity());
        // Only reused overflow addresses still depend on captured page block.
        if (!encoded.encoder->reused_overflow().empty()) {
            owner->dependencies = encoded.encoder->dependency_owner();
        }
        result.allocation_owner = std::move(owner);
        auto output             = encoded.encoder->next(result.frame);
        if (!output.status.ok() || !output.frame || !output.complete) {
            finish(output.status.ok() ? Status::corruption("inspection did not produce one primary")
                                      : std::move(output.status));
            return;
        }
        const auto  current  = core.capture();
        const auto &captured = prepared.view;
        if (current->generation != captured->generation || current->commit_seq != captured->commit_seq) {
            finish(Status::unavailable("tree changed during inspection"));
            return;
        }
        const auto page   = view->mapping.get(target);
        const auto deltas = page->leaf ? static_cast<uint32_t>(page->leaf->overlays.size()) : 0;
        finish(Status::Ok(), std::move(result), deltas);
    }

    void finish(Status status, NativeFrame result = {}, uint32_t deltas = 0) noexcept
    {
        auto done = std::move(complete);
        try {
            done(std::move(status), std::move(result), prepared.view->commit_seq, prepared.view->root_page_id, deltas);
        }
        catch (...) {
            CRB_LOG_ERROR("page inspection receiver threw after completion");
        }
    }

    core_read_detail::ReadPreparation     prepared;
    TreeCore                             &core;
    std::shared_ptr<const TreeRoot>       view;
    TreeCore::InspectComplete             complete;
    std::array<uint32_t, kPathDepth>      steps{};
    size_t                                path_size;
    size_t                                depth = 0;
    uint64_t                              target;
    uint64_t                              edge_pid   = 0;
    uint64_t                              lower_edge = 0;
    size_t                                edge_depth = 0;
    bool                                  right_edge = false;
    size_t                                attempts   = 0;
    CapturedEncodeContext                 encode_context;
    PageEncodeLimits                      frames;
    uint64_t                              load_pid = 0;
    std::optional<size_t>                 load_base;
    std::shared_ptr<const LeafBase>       leaf;
    std::shared_ptr<const InnerPageState> inner;
    Status                                load_status;
    Step                                  next   = &Inspection::route;
    Step                                  resume = nullptr;
    ApplyDispatcher::Node                 ready{&Inspection::run, this};
};
} // namespace

Status TreeCore::inspect_page_async(std::span<const uint32_t> path, uint64_t expected_version, InspectComplete complete)
try {
    if (path.size() > kPathDepth || !complete) {
        return Status::invalid_argument("page inspection requires a callback and at most 32 levels");
    }
    State::Ingress ingress(*state_);
    if (ingress.owner == nullptr) {
        return Status::unavailable("Tree closed");
    }
    core_read_detail::ReadPreparation prepared;
    auto                              status = core_read_detail::prepare(*this, &prepared);
    if (!status.ok()) {
        return status;
    }
    if (prepared.limits.max_route_depth > kPathDepth + 1) {
        return Status::resource_exhausted("inspection depth exceeds its bound");
    }
    if (expected_version != kInvalidPageId && expected_version != prepared.view->commit_seq) {
        return Status::unavailable("tree changed; restart page inspection");
    }
    auto inspection =
        detail::reserved_shared<Inspection>(prepared.credit, std::move(prepared), *this, path, std::move(complete));
    inspection->schedule();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status inspect_published_page(TreeCore &core, const std::vector<uint32_t> &path, uint64_t expected_version,
                              NativeFrame *out, uint64_t *version, uint64_t *root, uint32_t *deltas)
{
    if (out == nullptr || version == nullptr || root == nullptr || deltas == nullptr) {
        return Status::invalid_argument("page inspection requires outputs");
    }
    detail::HostCompletion done;
    Status                 terminal;
    auto                   status = core.inspect_page_async(
        path, expected_version,
        [&](Status result, NativeFrame frame, uint64_t revision, uint64_t root_id, uint32_t overlays) noexcept {
            terminal = std::move(result);
            if (terminal.ok()) {
                *out     = std::move(frame);
                *version = revision;
                *root    = root_id;
                *deltas  = overlays;
            }
            done.signal();
        });
    if (!status.ok()) {
        return status;
    }
    done.wait();
    return terminal;
}
} // namespace crowdb::tree
