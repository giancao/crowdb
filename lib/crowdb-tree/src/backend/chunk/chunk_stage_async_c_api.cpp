// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_c_api_internal.h"

namespace
{
using namespace crowdb::tree;
using namespace crowdb::tree::detail;

Status decode(const uint8_t *data, size_t length, std::optional<ChunkStageRecord> *out)
{
    if (length == 0) {
        return Status::Ok();
    }
    if (data == nullptr || length > kMaxStageBytes) {
        return Status::invalid_argument();
    }
    out->emplace();
    return decode_stage_record({data, length}, &**out);
}

struct CompareCall
{
    std::shared_ptr<CompareCall>    accepted_owner;
    std::shared_ptr<RootCatalog>    catalog;
    std::optional<ChunkStageRecord> expected;
    std::optional<ChunkStageRecord> next;
    ct_catalog_status_complete      complete = nullptr;
    void                           *context  = nullptr;
    std::atomic<bool>               terminal{false};

    static void finished(void *raw, Status status)
    {
        auto &self = *static_cast<CompareCall *>(raw);
        if (!self.terminal.exchange(true, std::memory_order_acq_rel)) {
            auto retained = std::move(self.accepted_owner);
            self.complete(self.context, static_cast<ct_status>(status.code()));
        }
    }
};
} // namespace

ct_status ct_root_catalog_stage_compare_async_v1(ct_root_catalog *catalog, uint64_t tree, uint64_t epoch,
                                                 const uint8_t *expected, size_t expected_length, const uint8_t *next,
                                                 size_t next_length, ct_catalog_status_complete complete, void *context)
try {
    if (catalog == nullptr || !catalog->catalog || complete == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto call   = std::make_shared<CompareCall>();
    auto status = decode(expected, expected_length, &call->expected);
    if (status.ok()) {
        status = decode(next, next_length, &call->next);
    }
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    call->catalog        = catalog->catalog;
    call->complete       = complete;
    call->context        = context;
    call->accepted_owner = call;
    try {
        call->catalog->submit_compare_stage(tree, epoch, call->expected, call->next,
                                            {.context = call.get(), .complete_fn = &CompareCall::finished});
    }
    catch (const std::bad_alloc &) {
        CompareCall::finished(call.get(), Status::resource_exhausted());
    }
    catch (...) {
        CompareCall::finished(call.get(), Status::internal_error());
    }
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}
