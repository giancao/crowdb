// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/btree/diagnostics.h"
#include "crowdb-tree/page_block_stats_c_api.h"

namespace crowdb::tree
{
int32_t copy_page_block_stats(PageBlockStatsSnapshot value, ct_page_block_stats_v1 *out) noexcept;
}
