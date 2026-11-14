// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_protection_async_callback.h"

namespace crowdb::tree::detail
{
namespace pc = protection_callback;
void CallbackRootCatalog::submit_writer_quiesced(uint64_t tree, uint64_t epoch, AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(writer_quiesced(tree, epoch));
        return;
    }
    if (tree == 0 || epoch == 0) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call = protection_call(shared_from_this(), done);

    call->callback_owner = call;
    call->submitted(async_protection_.writer_quiesced(context_, tree, epoch, &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_begin_candidate(uint64_t tree, const ChunkManifestCandidate &candidate,
                                                 AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(begin_candidate(tree, candidate));
        return;
    }
    if (tree == 0 || !pc::valid(candidate.id) || candidate.expected_generation == UINT64_MAX ||
        candidate.reference_count > kMaxCandidateReferences || candidate.manifest.empty() ||
        candidate.manifest.size() > kMaxCandidateManifestBytes ||
        (candidate.reference_count == 0 && candidate.reference_checksum != 0)) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call = protection_call(shared_from_this(), done);
    call->manifest.assign(candidate.manifest.begin(), candidate.manifest.end());
    call->callback_owner = call;
    call->submitted(async_protection_.begin(context_, tree, pc::external(candidate.id), candidate.expected_generation,
                                            candidate.reference_count, candidate.reference_checksum,
                                            call->manifest.data(), call->manifest.size(), &ProtectionCallback::ready,
                                            call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_prepare_references(uint64_t tree, ChunkStageId id, uint64_t window,
                                                    std::span<const ChunkStageId> references, AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(prepare_references(tree, id, window, references));
        return;
    }
    if (tree == 0 || !pc::valid(id) || references.empty() || references.size() > kMaxReferenceWindow) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call = protection_call(shared_from_this(), done);
    for (size_t index = 0; index < references.size(); ++index) {
        if (!pc::valid(references[index]) || (index != 0 && references[index - 1] >= references[index])) {
            done.complete(Status::invalid_argument());
            return;
        }
        call->references.push_back(pc::external(references[index]));
    }
    call->callback_owner = call;
    call->submitted(async_protection_.prepare(context_, tree, pc::external(id), window, call->references.data(),
                                              call->references.size(), &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_finish_candidate(uint64_t tree, ChunkStageId id, uint32_t action, AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(finish_candidate(tree, id, action));
        return;
    }
    if (tree == 0 || !pc::valid(id) || action < CT_CHUNK_CANDIDATE_COMMIT || action > CT_CHUNK_CANDIDATE_RECOVER) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call = protection_call(shared_from_this(), done);

    call->callback_owner = call;
    call->submitted(
        async_protection_.finish(context_, tree, pc::external(id), action, &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_release_references(uint64_t tree, ChunkStageId id, uint64_t window,
                                                    AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(release_references(tree, id, window));
        return;
    }
    if (tree == 0 || !pc::valid(id)) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call = protection_call(shared_from_this(), done);

    call->callback_owner = call;
    call->submitted(
        async_protection_.release(context_, tree, pc::external(id), window, &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

void CallbackRootCatalog::submit_change_pin(uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool acquire,
                                            AsyncCompletion done)
try {
    if (async_protection_.version == 0) {
        done.complete(change_pin(tree, stage, pin, acquire));
        return;
    }
    if (tree == 0 || !pc::valid(stage) || !pc::valid(pin)) {
        done.complete(Status::invalid_argument());
        return;
    }
    auto call = protection_call(shared_from_this(), done);

    call->callback_owner = call;
    call->submitted(async_protection_.pin(context_, tree, pc::external(stage), pc::external(pin), acquire ? 1 : 0,
                                          &ProtectionCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    done.complete(Status::resource_exhausted());
}
catch (...) {
    done.complete(Status::internal_error());
}

} // namespace crowdb::tree::detail
