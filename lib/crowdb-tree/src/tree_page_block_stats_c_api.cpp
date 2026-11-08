// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_internal.h"
#include "crowdb-tree/btree/tree.h"
#include "crowdb-tree/page_block_stats_c_api.h"
#include "tree_page_block_stats_c_api_internal.h"

int32_t crowdb::tree::copy_page_block_stats(PageBlockStatsSnapshot value, ct_page_block_stats_v1 *out) noexcept
{
    if (out == nullptr || out->version != 1 || out->struct_size != sizeof(*out)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (value.scope_id == 0) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    *out = {.struct_size     = sizeof(*out),
            .version         = 1,
            .scope_id        = value.scope_id,
            .allocated_bytes = value.allocated_bytes};
    return 0;
}

int32_t ct_get_page_block_stats(ct_tree *tree, ct_page_block_stats_v1 *out)
{
    using namespace crowdb::tree;
    return tree == nullptr ? static_cast<int32_t>(Code::kInvalidArgument)
                           : copy_page_block_stats(c_api_tree(tree)->page_block_stats(), out);
}
