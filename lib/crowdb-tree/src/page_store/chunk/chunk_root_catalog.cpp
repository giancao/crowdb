// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "c_api_internal.h"
#include "chunk_c_api_internal.h"
#include "page_chunk_store.h"

namespace crowdb::tree::detail
{
namespace
{
Status callback_status(ct_status code, const char *message)
{
    switch (static_cast<Code>(code)) {
    case Code::kOk:
        return Status::Ok();
    case Code::kNotFound:
        return Status::not_found(message);
    case Code::kInvalidArgument:
        return Status::invalid_argument(message);
    case Code::kCorruption:
        return Status::corruption(message);
    case Code::kIoError:
        return Status::io_error(message);
    case Code::kNotSupported:
        return Status::not_supported(message);
    case Code::kInternal:
        return Status::internal_error(message);
    case Code::kResourceExhausted:
        return Status::resource_exhausted(message);
    case Code::kUnavailable:
    default:
        return Status::unavailable(message);
    }
}
} // namespace

CallbackRootCatalog::CallbackRootCatalog(ct_root_catalog_callbacks callbacks, void *context)
    : callbacks_(callbacks),
      context_(context)
{
}

CallbackRootCatalog::~CallbackRootCatalog()
{
    if (callbacks_.drop_context != nullptr) {
        callbacks_.drop_context(context_);
    }
}

uint64_t CallbackRootCatalog::reclaim_before(uint64_t tree_id, uint64_t generation)
{
    return callbacks_.reclaim_before == nullptr ? 0 : callbacks_.reclaim_before(context_, tree_id, generation);
}

Status CallbackRootCatalog::pin_generation(uint64_t tree_id, uint64_t transition_high, uint64_t transition_low,
                                           uint64_t generation)
{
    if (callbacks_.pin_generation == nullptr) {
        return Status::invalid_argument("callback root catalog pin is unavailable");
    }
    return callback_status(callbacks_.pin_generation(context_, tree_id, transition_high, transition_low, generation),
                           "callback root catalog pin failed");
}

Status CallbackRootCatalog::unpin_generation(uint64_t tree_id, uint64_t transition_high, uint64_t transition_low)
{
    if (callbacks_.unpin_generation == nullptr) {
        return Status::invalid_argument("callback root catalog unpin is unavailable");
    }
    return callback_status(callbacks_.unpin_generation(context_, tree_id, transition_high, transition_low),
                           "callback root catalog unpin failed");
}

} // namespace crowdb::tree::detail

ct_status ct_callback_root_catalog_open(const ct_root_catalog_callbacks *callbacks, void *context,
                                        ct_root_catalog **out)
{
    if (callbacks == nullptr || callbacks->load == nullptr || callbacks->free_blob == nullptr || out == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    auto handle     = std::make_unique<ct_root_catalog>();
    handle->catalog = std::make_shared<crowdb::tree::detail::CallbackRootCatalog>(*callbacks, context);
    *out            = handle.release();
    return static_cast<ct_status>(crowdb::tree::Code::kOk);
}

void ct_root_catalog_free(ct_root_catalog *catalog)
{
    delete catalog;
}

ct_status ct_chunk_page_store_set_wal_replay_offset(ct_page_store *store, uint64_t offset)
{
    if (store == nullptr || store->bundle == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    if (auto *runtime = dynamic_cast<crowdb::tree::detail::PageChunkStore *>(store->bundle->store.get())) {
        runtime->request_wal_offset(offset);
        return 0;
    }
    return 0;
}

ct_status ct_chunk_page_store_get_wal_replay_offset(const ct_page_store *store, uint64_t *offset)
{
    if (store == nullptr || store->bundle == nullptr || offset == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    if (const auto *runtime = dynamic_cast<const crowdb::tree::detail::PageChunkStore *>(store->bundle->store.get())) {
        *offset = runtime->durable_wal_offset();
        return 0;
    }
    *offset = 0;
    return 0;
}

ct_status ct_chunk_page_store_get_manifest_generation(const ct_page_store *store, uint64_t *generation)
{
    if (store == nullptr || store->bundle == nullptr || generation == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    if (const auto *runtime = dynamic_cast<const crowdb::tree::detail::PageChunkStore *>(store->bundle->store.get())) {
        *generation = runtime->manifest_generation();
        return 0;
    }
    return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
}

uint64_t ct_root_catalog_reclaim_before(ct_root_catalog *catalog, uint64_t tree_id, uint64_t generation)
{
    if (catalog == nullptr || catalog->catalog == nullptr) {
        return 0;
    }
    return catalog->catalog->reclaim_before(tree_id, generation);
}

ct_status ct_root_catalog_pin_generation(ct_root_catalog *catalog, uint64_t tree_id, uint64_t transition_high,
                                         uint64_t transition_low, uint64_t generation)
{
    if (catalog == nullptr || catalog->catalog == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    return static_cast<ct_status>(
        catalog->catalog->pin_generation(tree_id, transition_high, transition_low, generation).code());
}

ct_status ct_root_catalog_unpin_generation(ct_root_catalog *catalog, uint64_t tree_id, uint64_t transition_high,
                                           uint64_t transition_low)
{
    if (catalog == nullptr || catalog->catalog == nullptr) {
        return static_cast<ct_status>(crowdb::tree::Code::kInvalidArgument);
    }
    return static_cast<ct_status>(catalog->catalog->unpin_generation(tree_id, transition_high, transition_low).code());
}
