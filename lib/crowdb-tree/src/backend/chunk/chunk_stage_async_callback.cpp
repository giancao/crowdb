// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_reservation.h"
#include "chunk_c_api_internal.h"
#include "chunk_protection_callback.h"
#include "crowdb-common/log.h"

namespace crowdb::tree::detail
{
namespace
{
struct StageCallback
{
    std::shared_ptr<const RootCatalog> catalog;
    std::shared_ptr<StageCallback>     callback_owner;
    ct_root_catalog_callbacks          callbacks;
    void                              *context = nullptr;
    AsyncCompletion                    completion;
    ChunkStageBytes                    expected;
    ChunkStageBytes                    next;
    uint64_t                           tree = 0;
    ChunkStageId                       id;
    std::optional<ChunkStageId>        after;
    ct_chunk_stage_id                  cursor{};
    uint32_t                           limit      = 0;
    std::optional<ChunkStageRecord>   *record     = nullptr;
    std::vector<ChunkStageRecord>     *records    = nullptr;
    ChunkRootBytes                    *root       = nullptr;
    const uint8_t                     *blob       = nullptr;
    size_t                             length     = 0;
    ct_status                          reply      = 0;
    ct_status                          submission = 0;
    std::atomic<unsigned>              events{0};
    std::atomic<bool>                  reported{false};

    explicit StageCallback(AsyncCompletion done)
        : completion(std::move(done)),
          expected(MetadataAllocator<uint8_t>(completion.allocations)),
          next(MetadataAllocator<uint8_t>(completion.allocations))
    {
    }

    ~StageCallback()
    {
        if (blob != nullptr) {
            callbacks.free_blob(context, blob, length);
        }
    }

    static void ready(void *context, ct_status status, const uint8_t *bytes, size_t size) noexcept
    {
        auto &call     = *static_cast<StageCallback *>(context);
        auto  retained = std::move(call.callback_owner);
        call.reply     = status;
        call.blob      = bytes;
        call.length    = size;
        call.events.fetch_or(1, std::memory_order_acq_rel);
        call.publish();
    }

    static void compared(void *context, ct_status status) noexcept
    {
        ready(context, status, nullptr, 0);
    }

    void submitted(ct_status status) noexcept
    {
        submission = status;
        if (status != 0) {
            callback_owner.reset();
        }
        events.fetch_or(2, std::memory_order_acq_rel);
        publish();
    }

    Status decode()
    {
        auto status = protection_callback::status(submission != 0 ? submission : reply);
        if (submission == 0 && record != nullptr && status.code() == Code::kNotFound) {
            record->reset();
            return Status::Ok();
        }
        if (!status.ok()) {
            return status;
        }
        if (root != nullptr) {
            if (blob == nullptr || length == 0 || length > kMaxCandidateManifestBytes) {
                return Status::corruption();
            }
            root->assign(blob, blob + length);
            return Status::Ok();
        }
        if (record == nullptr && records == nullptr) {
            return status;
        }
        const size_t maximum = record != nullptr ? kMaxStageBytes : 4 + (kMaxStageScan * (4 + kMaxStageBytes));
        if (blob == nullptr || length > maximum) {
            return Status::corruption();
        }
        if (record != nullptr) {
            ChunkStageRecord parsed;
            parsed.packs = decltype(parsed.packs)(MetadataAllocator<ChunkStagePack>(completion.allocations));
            status       = decode_stage_record({blob, length}, &parsed);
            if (status.ok() && (parsed.tree_id != tree || parsed.id != id)) {
                status = Status::corruption();
            }
            if (status.ok()) {
                *record = std::move(parsed);
            }
            return status;
        }
        std::vector<ChunkStageRecord> parsed;
        status = decode_stage_list({blob, length}, &parsed);
        if (!status.ok()) {
            return status;
        }
        if (parsed.size() > limit) {
            return Status::corruption();
        }
        auto cursor = after;
        for (const auto &item : parsed) {
            if (item.tree_id != tree || (cursor && item.id <= *cursor)) {
                return Status::corruption();
            }
            cursor = item.id;
        }
        *records = std::move(parsed);
        return Status::Ok();
    }

    void publish() noexcept
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
            CRB_LOG_ERROR("Stage catalog receiver threw after terminal");
        }
    }
};
} // namespace

Status CallbackRootCatalog::bind_async_staging(const ct_root_catalog_staging_async_callbacks &callbacks)
{
    if (async_staging_.version != 0 || callbacks.version != 1 || callbacks.struct_bytes != sizeof(callbacks) ||
        callbacks.load == nullptr || callbacks.compare == nullptr || callbacks.list == nullptr) {
        return Status::invalid_argument();
    }
    async_staging_ = callbacks;
    return Status::Ok();
}

void CallbackRootCatalog::submit_compare_stage(uint64_t tree, uint64_t epoch,
                                               const std::optional<ChunkStageRecord> &expected,
                                               const std::optional<ChunkStageRecord> &next, AsyncCompletion completion)
try {
    if (async_staging_.version == 0) {
        completion.complete(compare_stage(tree, epoch, expected, next));
        return;
    }
    auto status = validate_stage_transition(expected, next);
    if (status.ok() && ((expected && expected->tree_id != tree) || (next && next->tree_id != tree) || epoch == 0)) {
        status = Status::invalid_argument();
    }
    auto call       = reserved_shared<StageCallback>(completion.allocations, completion);
    call->catalog   = shared_from_this();
    call->callbacks = callbacks_;
    call->context   = context_;
    if (status.ok() && expected) {
        status = encode_stage_record_owned(*expected, &call->expected);
    }
    if (status.ok() && next) {
        status = encode_stage_record_owned(*next, &call->next);
    }
    if (!status.ok()) {
        completion.complete(std::move(status));
        return;
    }
    call->callback_owner = call;
    call->submitted(async_staging_.compare(context_, tree, epoch, call->expected.data(), call->expected.size(),
                                           call->next.data(), call->next.size(), &StageCallback::compared, call.get()));
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}

void CallbackRootCatalog::submit_load_stage(uint64_t tree, ChunkStageId id, std::optional<ChunkStageRecord> *out,
                                            AsyncCompletion completion) const
try {
    if (async_staging_.version == 0) {
        completion.complete(load_stage(tree, id, out));
        return;
    }
    if (out == nullptr) {
        completion.complete(Status::invalid_argument());
        return;
    }
    auto call            = reserved_shared<StageCallback>(completion.allocations, completion);
    call->catalog        = shared_from_this();
    call->callbacks      = callbacks_;
    call->context        = context_;
    call->tree           = tree;
    call->id             = id;
    call->record         = out;
    call->callback_owner = call;
    call->submitted(
        async_staging_.load(context_, tree, protection_callback::external(id), &StageCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}

void CallbackRootCatalog::submit_list_stages(uint64_t tree, std::optional<ChunkStageId> after, uint32_t limit,
                                             std::vector<ChunkStageRecord> *out, AsyncCompletion completion) const
try {
    if (async_staging_.version == 0) {
        completion.complete(list_stages(tree, after, limit, out));
        return;
    }
    if (out == nullptr || limit == 0 || limit > kMaxStageScan) {
        completion.complete(Status::invalid_argument());
        return;
    }
    auto call            = reserved_shared<StageCallback>(completion.allocations, completion);
    call->catalog        = shared_from_this();
    call->callbacks      = callbacks_;
    call->context        = context_;
    call->tree           = tree;
    call->after          = after;
    call->limit          = limit;
    call->records        = out;
    call->callback_owner = call;
    call->cursor         = protection_callback::external(after.value_or(ChunkStageId{}));
    call->submitted(
        async_staging_.list(context_, tree, after ? &call->cursor : nullptr, limit, &StageCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}

void CallbackRootCatalog::submit_load_root_owned(uint64_t tree, uint64_t generation, ChunkRootBytes *out,
                                                 AsyncCompletion complete) const
try {
    if (async_protection_.version == 0) {
        complete.complete(load_root_owned(tree, generation, out));
        return;
    }
    if (tree == 0 || out == nullptr) {
        complete.complete(Status::invalid_argument());
        return;
    }
    auto call            = reserved_shared<StageCallback>(complete.allocations, complete);
    call->catalog        = shared_from_this();
    call->callbacks      = callbacks_;
    call->context        = context_;
    call->root           = out;
    call->callback_owner = call;
    call->submitted(async_protection_.root(context_, tree, generation, &StageCallback::ready, call.get()));
}
catch (const std::bad_alloc &) {
    complete.complete(Status::resource_exhausted());
}
catch (...) {
    complete.complete(Status::internal_error());
}
} // namespace crowdb::tree::detail

ct_status ct_callback_root_catalog_set_staging_async_v1(ct_root_catalog                               *catalog,
                                                        const ct_root_catalog_staging_async_callbacks *callbacks)
try {
    auto *target = catalog == nullptr
                     ? nullptr
                     : dynamic_cast<crowdb::tree::detail::CallbackRootCatalog *>(catalog->catalog.get());
    return target == nullptr || callbacks == nullptr
             ? static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument)
             : static_cast<ct_status>(target->bind_async_staging(*callbacks).code());
}
catch (...) {
    return static_cast<ct_status>(crowdb::tree::Code::kInternal);
}
