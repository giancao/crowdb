// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"

extern "C" {
struct ct_read_only_view;

// Host-thread operations. Capture shares one immutable L1 mapping and exact
// backing owners; it neither opens a writer nor materializes all entries.
// Keep the external runtime host alive until reads and close have drained.
// expected_sequence=0 permits an in-memory fixed coverage without a checkpoint.
ct_status ct_read_only_view_capture(ct_tree *tree, uint64_t expected_sequence, uint64_t expected_coverage,
                                    ct_read_only_view **out);
void      ct_read_only_view_free(ct_read_only_view *view);
// Register once. On success the callback runs once, possibly inline, after
// admission closes and accepted reads and view lane tails drain.
ct_status ct_read_only_view_close_async(ct_read_only_view *view, void (*complete)(void *), void *context);

// A successful submission calls complete once, possibly inline. The key is
// consumed before submission returns. On successful completion the callback
// owns a ready kGet future, even if the key was absent. On failure it gets null.
using ct_read_only_view_read_fn = void (*)(void *, ct_status, ct_future *);
ct_status ct_read_only_view_get_async(ct_read_only_view *view, const uint8_t *key, size_t size,
                                      ct_read_only_view_read_fn complete, void *context);
// The scan follows the same ownership rule with a ready kScan future. Read it
// with ct_future_poll_scan_v2. All option bytes are consumed during submission.
ct_status ct_read_only_view_scan_async(ct_read_only_view *view, const ct_scan_options_v2 *options,
                                       ct_read_only_view_read_fn complete, void *context);

// Rebuilds the destination's configured range from this fixed view and writes
// its own checkpoint. Destination is a separately opened runtime Tree/account.
ct_status ct_read_only_view_rebuild(ct_read_only_view *view, ct_tree *destination, ct_range_rebuild_stats *stats);
// Destination remains caller-owned. Keep it alive through completion. A
// successful submission copies the view owner and calls complete exactly once.
ct_status ct_read_only_view_rebuild_async(ct_read_only_view *view, ct_tree *destination,
                                          void (*complete)(void *, ct_status, ct_range_rebuild_stats), void *context);
// Uses the destination's bounded range. When the complete L1 is behind the
// accepted source frontier, continues from ordinary flush completion first.
ct_status ct_rebuild_bounded_range_async(ct_tree *source, ct_tree                                          *destination,
                                         void (*complete)(void *, ct_status, ct_range_rebuild_stats), void *context);
}
