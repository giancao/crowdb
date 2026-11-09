// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_protection_async_callback.h"

namespace crowdb::tree::detail
{
namespace pc = protection_callback;
void CallbackRootCatalog::submit_load_candidate(uint64_t tree, ChunkStageId id,
                                                std::optional<ChunkCandidateStatus> *out, AsyncCompletion done) const
try {
    if (async_protection_.version == 0) {
        done.complete(load_candidate(tree, id, out));
        return;
    }
    if (tree == 0 || !pc::valid(id) || out == nullptr) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call       = protection_call(shared_from_this(), done);
    call->candidate = out;
    call->id        = id;
    call->wire.resize(1);
    call->callback_owner = call;
    call->submitted(async_protection_.load(context_, tree, pc::external(id), call->wire.data(),
                                           &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_list_candidates(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                                                 std::vector<ChunkCandidateStatus> *out, AsyncCompletion done) const
try {
    if (async_protection_.version == 0) {
        done.complete(list_candidates(tree, after, limit, out));
        return;
    }
    if (tree == 0 || limit == 0 || limit > kMaxStageScan || out == nullptr || (after && !pc::valid(*after))) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call        = protection_call(shared_from_this(), done);
    call->candidates = out;
    call->after      = after;
    call->length     = limit;
    call->wire.resize(limit);
    call->cursor         = pc::external(after.value_or(ChunkStageId{}));
    call->callback_owner = call;
    call->submitted(async_protection_.list(context_, tree, after ? &call->cursor : nullptr, limit, call->wire.data(),
                                           &call->length, &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_prune_pins(uint64_t tree, ChunkStageId stage, std::optional<ChunkStageId> after,
                                            uint32_t limit, ChunkPinProgress *out, AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(prune_pins(tree, stage, after, limit, out));
        return;
    }
    if (tree == 0 || !pc::valid(stage) || limit == 0 || limit > kMaxStageScan || out == nullptr ||
        (after && !pc::valid(*after))) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call            = protection_call(shared_from_this(), done);
    call->progress       = out;
    call->after          = after;
    call->limit          = limit;
    call->cursor         = pc::external(after.value_or(ChunkStageId{}));
    call->callback_owner = call;
    call->submitted(async_protection_.prune(context_, tree, pc::external(stage), after ? &call->cursor : nullptr, limit,
                                            &call->wire_progress, &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

} // namespace crowdb::tree::detail
