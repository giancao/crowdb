// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_protection_async_callback.h"

#include "chunk_c_api_internal.h"
#include "crowdb-common/log.h"

namespace crowdb::tree::detail
{
void ProtectionCallback::ready(void *raw, ct_status status) noexcept
{
    auto &call     = *static_cast<ProtectionCallback *>(raw);
    auto  retained = std::move(call.callback_owner);
    call.reply     = status;
    call.events.fetch_or(1, std::memory_order_acq_rel);
    call.publish();
}

void ProtectionCallback::submitted(ct_status status) noexcept
{
    submission = status;
    if (status != 0) {
        callback_owner.reset();
    }
    events.fetch_or(2, std::memory_order_acq_rel);
    publish();
}

Status ProtectionCallback::decode()
{
    auto status = protection_callback::status(submission != 0 ? submission : reply);
    if (submission == 0 && candidate != nullptr && status.code() == Code::kNotFound) {
        candidate->reset();
        return Status::Ok();
    }
    if (!status.ok()) {
        return status;
    }
    if (candidate != nullptr) {
        ChunkCandidateStatus parsed;
        status = protection_callback::decode(wire[0], &parsed);
        if (status.ok() && parsed.id != id) {
            status = Status::corruption();
        }
        if (status.ok()) {
            *candidate = parsed;
        }
    }
    if (candidates != nullptr) {
        if (length > wire.size()) {
            return Status::corruption();
        }
        std::vector<ChunkCandidateStatus> parsed;
        parsed.reserve(length);
        auto cursor = after;
        for (size_t index = 0; index < length; ++index) {
            ChunkCandidateStatus value;
            status = protection_callback::decode(wire[index], &value);
            if (!status.ok() || (cursor && value.id <= *cursor)) {
                return Status::corruption();
            }
            cursor = value.id;
            parsed.push_back(value);
        }
        *candidates = std::move(parsed);
    }
    if (progress != nullptr) {
        if (wire_progress.has_after > 1 || wire_progress.more > 1 || wire_progress.examined > limit ||
            wire_progress.released > wire_progress.examined ||
            ((wire_progress.examined != 0) != (wire_progress.has_after != 0)) ||
            (wire_progress.more != 0 && wire_progress.has_after == 0)) {
            return Status::corruption();
        }
        const auto cursor = protection_callback::internal(wire_progress.after);
        if (wire_progress.has_after != 0 && (!protection_callback::valid(cursor) || (after && cursor <= *after))) {
            return Status::corruption();
        }
        *progress = {.after    = wire_progress.has_after != 0 ? std::optional(cursor) : std::nullopt,
                     .examined = wire_progress.examined,
                     .released = wire_progress.released,
                     .more     = wire_progress.more != 0};
    }
    return status;
}

void ProtectionCallback::publish() noexcept
{
    const auto observed = events.load(std::memory_order_acquire);
    if ((observed & 2U) == 0 || (submission == 0 && (observed & 1U) == 0) ||
        reported.exchange(true, std::memory_order_acq_rel)) {
        return;
    }
    Status status;
    try {
        status = decode();
    }
    catch (const std::bad_alloc &) {
        status = Status::resource_exhausted();
    }
    catch (...) {
        status = Status::internal_error();
    }
    auto done = std::move(completion);
    try {
        done.complete(std::move(status));
    }
    catch (...) {
        CRB_LOG_ERROR("Protection catalog receiver threw after terminal");
    }
}

Status CallbackRootCatalog::bind_async_protection(const ct_root_catalog_protection_async_callbacks &callbacks)
{
    if (async_protection_.version != 0 || callbacks.version != 1 || callbacks.struct_bytes != sizeof(callbacks) ||
        callbacks.writer_quiesced == nullptr || callbacks.begin == nullptr || callbacks.load == nullptr ||
        callbacks.list == nullptr || callbacks.prepare == nullptr || callbacks.finish == nullptr ||
        callbacks.release == nullptr || callbacks.pin == nullptr || callbacks.prune == nullptr ||
        callbacks.root == nullptr) {
        return Status::invalid_argument();
    }
    async_protection_ = callbacks;
    return Status::Ok();
}
} // namespace crowdb::tree::detail

ct_status ct_callback_root_catalog_set_protection_async_v1(ct_root_catalog                                  *catalog,
                                                           const ct_root_catalog_protection_async_callbacks *callbacks)
try {
    auto *target = catalog == nullptr
                     ? nullptr
                     : dynamic_cast<crowdb::tree::detail::CallbackRootCatalog *>(catalog->catalog.get());
    return target == nullptr || callbacks == nullptr
             ? static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument)
             : static_cast<ct_status>(target->bind_async_protection(*callbacks).code());
}
catch (...) {
    return static_cast<ct_status>(crowdb::tree::Code::kInternal);
}
