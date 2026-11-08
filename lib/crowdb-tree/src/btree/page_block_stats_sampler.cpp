// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_block_stats_sampler.h"

#include "tree_metrics.h"

namespace crowdb::tree
{
PageBlockStatsSnapshot PageBlockStatsSampler::snapshot() const noexcept
{
    if (!metrics || scope_id == 0) {
        return {};
    }
    const auto value = metrics->snapshot();
    return {.scope_id = scope_id, .allocated_bytes = value.allocated_page_block_bytes};
}
} // namespace crowdb::tree
