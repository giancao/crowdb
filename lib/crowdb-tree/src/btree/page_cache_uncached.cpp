// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_cache_internal.h"

#include <algorithm>

namespace crowdb::tree
{
void SharedPageCache::State::uncached(Request *request)
{
    if (next_identity >= UINT64_MAX / 2) {
        deliver(request, Status::unavailable());
        return;
    }
    const auto identity = next_identity++;
    request->completion.store(identity << 1, std::memory_order_release);
    inflight.fetch_add(1, std::memory_order_relaxed);
    misses.fetch_add(1, std::memory_order_relaxed);
    record_work(request->metadata, TreeWorkCounter::CacheMisses, 1,
                request->priority == PageLoadPriority::Read ? TreeWorkCause::Read : TreeWorkCause::Flush);
    const auto finish = [self = shared_from_this(), request, identity](Status                                status,
                                                                       std::shared_ptr<const LeafBase>       leaf,
                                                                       std::shared_ptr<const InnerPageState> inner) {
        auto expected = identity << 1;
        if (!request->completion.compare_exchange_strong(expected, expected | 1, std::memory_order_acq_rel)) {
            return;
        }
        uint64_t max_slot = 0;
        if (status.ok()) {
            if (request->key.kind == Kind::Leaf && leaf && !inner) {
                for (size_t index = 0; index < leaf->count(); ++index) {
                    max_slot = std::max(max_slot, CellView(leaf->cell(index)).slot());
                }
            }
            else if (request->key.kind != Kind::Inner || !inner || leaf) {
                status = Status::corruption();
            }
        }
        self->inflight.fetch_sub(1, std::memory_order_relaxed);
        self->deliver(request, status, std::move(leaf), std::move(inner), max_slot);
    };
    auto upstream = request->upstream;
    try {
        if (request->key.kind == Kind::Leaf) {
            upstream->load_leaf(request->base,
                                [finish](Status status, auto leaf) { finish(std::move(status), std::move(leaf), {}); });
        }
        else {
            upstream->load_inner(request->base.backing, [finish](Status status, auto inner) {
                finish(std::move(status), {}, std::move(inner));
            });
        }
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted("uncached load allocation failed"), {}, {});
    }
    catch (...) {
        finish(Status::io_error(), {}, {});
    }
}

} // namespace crowdb::tree
