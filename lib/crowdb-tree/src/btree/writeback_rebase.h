// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_write.h"
#include "tree_root.h"

namespace crowdb::tree
{
// The receipt must come from writing this captured page. Preparation compares
// identities against the latest page; the caller publishes with that latest
// page as its expected value. A conflict requires replan, never a blind swap.
Status build_writeback_rebase(const std::shared_ptr<const PageEntry> &captured,
                              const std::shared_ptr<const PageEntry> &current, const DurableCapturedFrame &written,
                              uint64_t coverage, const std::shared_ptr<AllocationBudget> &metadata,
                              std::shared_ptr<const PageEntry> *out, size_t max_sources = 128, uint64_t gc_floor = 0);
// A private layout can write several durable leaves for one captured source.
// Its binding proves the source identity and coverage independently of output
// PIDs. Newer overlays survive; the new base owns only the written range.
} // namespace crowdb::tree
