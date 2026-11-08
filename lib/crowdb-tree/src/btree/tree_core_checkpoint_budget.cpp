// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_checkpoint_internal.h"
#include "page_store/checkpoint_page_store.h"
#include "page_store/local/block_account.h"

#include <algorithm>
#include <bit>

namespace crowdb::tree::checkpoint_detail
{
Credits::~Credits()
{
    if (snapshot) {
        snapshot->seal();
    }
    detail::seal_backend_budget(budget.backend);
    if (budget.reservation()) {
        budget.reservation()->seal();
    }
}

Preparation::~Preparation()
{
    detail::seal_reservation(control);
    detail::seal_reservation(finish);
}

CheckpointOptions defaults(const TreeCore &core)
{
    CheckpointOptions options;
    const auto       &config                = core.config();
    options.write.frames.encode.frame_bytes = config.frame_bytes;
    options.write.frames.encode.inline_value_bytes =
        config.max_inline_value != 0 ? config.max_inline_value : config.frame_bytes / 4;
    options.write.write.max_frame_bytes = config.frame_bytes;
    options.write.write.compression     = config.compression;
    if (core.local_page_store()) {
        options.write.write.iu_size          = core.local_page_store()->ledger->limits().iu_size;
        options.write.write.buffer_alignment = std::bit_ceil(std::max<size_t>(4096, options.write.write.iu_size));
    }
    if (core.page_chunk_store()) {
        options.write.write.chunk_backed     = true;
        options.write.write.iu_size          = core.page_chunk_store()->iu_size;
        options.write.write.buffer_alignment = std::bit_ceil(std::max<size_t>(4096, options.write.write.iu_size));
    }
    return options;
}

Status Preparation::work(const TreeRoot &view, const CapturedWritePlan *plan, CheckpointWork *out)
{
    if (out == nullptr || !work_allocations || !view.mapping.get(view.root_page_id) ||
        (plan != nullptr && plan->reused_primary_frames > plan->primary_frames)) {
        return Status::invalid_argument("checkpoint requires accepted work and a valid root");
    }
    auto loads = core->load_limits();

    loads.max_overflow_pages = 1;
    out->budget              = {.bytes        = work_allocations,
                                .work_metrics = core->publisher().work_metrics(),
                                .cpu_progress = cpu_progress,
                                .cause        = options.cause};
    out->loader              = detail::reserved_shared<AsyncStorePageLoader>(
        work_allocations, ticket->page_store(core->async_page_store()), core->binding()->cpu(), core->binding()->lane(),
        loads, OverflowPageBlockResolver{}, out->budget);
    out->owner = work_allocations;
    return Status::Ok();
}
} // namespace crowdb::tree::checkpoint_detail
