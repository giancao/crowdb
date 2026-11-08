// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/logical_page.h"
#include "btree/tree_root.h"

namespace crowdb::tree
{
// Owns one immutable mapping and the current root-to-leaf path. A successful
// step yields a leaf, end, or needs_inner(). The caller loads that page block and
// resumes; no thread-bound guard or worker wait is carried between steps.
class PublishedRouteCursor
{
  public:
    PublishedRouteCursor(uint64_t root_page_id, MappingTable mapping, size_t max_depth, bool reverse,
                         std::shared_ptr<AllocationBudget> metadata = {}, PageKeyRange selection = {})
        : root_(root_page_id),
          mapping_(std::move(mapping)),
          max_depth_(max_depth),
          reverse_(reverse),
          selection_(std::move(selection)),
          stack_(detail::ReservationAllocator<Frame>(std::move(metadata)))
    {
    }

    Status seek(std::optional<Slice> key, bool exclusive);
    Status next();
    Status resume_inner(std::shared_ptr<const InnerPageState> inner);

    [[nodiscard]] static std::optional<size_t> allocation_bound(size_t max_depth) noexcept
    {
        if (max_depth == 0 || max_depth > 64) {
            return {};
        }
        // Vector growth can temporarily retain both old and new arrays.
        return sizeof(PublishedRouteCursor) + (3 * max_depth * sizeof(Frame));
    }

    [[nodiscard]] uint64_t skipped_subtrees() const noexcept
    {
        return selection_.empty() && root_ != kInvalidPageId ? 1 : skipped_;
    }

    [[nodiscard]] const std::shared_ptr<const PageState> &leaf() const
    {
        return leaf_;
    }

    [[nodiscard]] uint64_t leaf_page_id() const
    {
        return leaf_id_;
    }

    [[nodiscard]] std::shared_ptr<const PageBlockRef> needs_inner() const
    {
        return pending_ ? pending_->page->inner_page_block : nullptr;
    }

  private:
    struct Frame
    {
        std::shared_ptr<const PageEntry>      page;
        std::shared_ptr<const InnerPageState> inner;
        PageKeyRange                          bounds;
        size_t                                child = 0;
    };

    struct Pending
    {
        uint64_t                         id;
        std::shared_ptr<const PageEntry> page;
        PageKeyRange                     bounds;
        std::optional<OwnedKey>          key;
        bool                             exclusive;
    };

    Status descend(uint64_t id, PageKeyRange bounds, std::optional<Slice> key, bool exclusive,
                   std::optional<uint32_t> expected_level, std::shared_ptr<const InnerPageState> loaded = {});
    [[nodiscard]] Status       validate_index(const InnerPageState &index, const PageKeyRange &bounds) const;
    [[nodiscard]] PageKeyRange child_range(const Frame &frame) const;
    uint64_t                   root_;
    MappingTable               mapping_;
    size_t                     max_depth_;
    bool                       reverse_;
    PageKeyRange               selection_;
    uint64_t                   skipped_ = 0;
    std::vector<Frame, detail::ReservationAllocator<Frame>> stack_;
    std::shared_ptr<const PageState>                        leaf_;
    uint64_t                                                leaf_id_ = kInvalidPageId;
    std::optional<Pending>                                  pending_;
};
} // namespace crowdb::tree
