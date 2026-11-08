// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/chunk_root_recovery.h"

namespace crowdb::tree
{
Status ChunkRootRecovery::dependencies()
{
    for (size_t work = 0; work < 64; ++work) {
        if (leaf_ == pages_->frames.end()) {
            return finish(steps_ == root_.dependency_entries ? pages_->initialize_overflow_counts()
                                                             : Status::corruption());
        }
        auto &leaf = leaf_->second;
        if (leaf.type != page_type::kLeafBase || reference_ == leaf.stored_overflow->size()) {
            ++leaf_;
            reference_ = 0;
            overflow_.reset();
            continue;
        }
        if (steps_ == root_.dependency_entries) {
            return Status::corruption();
        }
        if (!overflow_) {
            overflow_            = detail::reserved_shared<OverflowPageBlockSet>(metadata_, metadata_);
            auto owner           = detail::reserved_shared<LeafProtection>(metadata_);
            owner->primary       = leaf.page_block->physical_owner;
            owner->overflow      = overflow_;
            auto page_block      = detail::reserved_shared<PageBlockRef>(metadata_, *leaf.page_block);
            page_block->owner    = std::move(owner);
            page_block->overflow = overflow_;
            leaf.page_block      = std::move(page_block);
        }
        if (pid_ == kInvalidPageId) {
            const auto &ref = (*leaf.stored_overflow)[reference_];
            pid_            = ref.head;
            remaining_      = ref.bytes;
            chain_steps_    = 0;
        }
        if (++chain_steps_ > pages_->frames.size()) {
            return Status::corruption("overflow dependency cycle");
        }
        const auto found = pages_->frames.find(pid_);
        if (found == pages_->frames.end()) {
            return Status::corruption();
        }
        const auto &frame = found->second;
        if (frame.type != page_type::kOverflowFrame || frame.overflow_bytes == 0 || frame.overflow_bytes > remaining_ ||
            (frame.next_overflow == kInvalidPageId) != (frame.overflow_bytes == remaining_)) {
            return Status::corruption();
        }
        overflow_->pages.emplace(pid_, OverflowPageBlock{.page_block    = frame.page_block,
                                                         .next          = frame.next_overflow,
                                                         .bytes         = frame.overflow_bytes,
                                                         .frame_bytes   = frame.frame_bytes,
                                                         .logical_bytes = frame.logical_bytes});
        ++steps_;
        remaining_ -= frame.overflow_bytes;
        pid_ = frame.next_overflow;
        if (remaining_ == 0) {
            ++reference_;
        }
    }
    return Status::Ok();
}
} // namespace crowdb::tree
