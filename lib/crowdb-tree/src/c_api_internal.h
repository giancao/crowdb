// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#include "crowdb-tree/c_api.h"
#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/page_store/async_page_store.h"
#include "crowdb-tree/page_store/page_store.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include <memory>
#include <string>

struct PageStoreBundle
{
#ifdef CROWDB_HAVE_LIBURING
    // The wrapper and store release before the reactor. An external release
    // driver can retain the reactor independently of both.
    std::shared_ptr<crowdb::common::DiskIOUring> uring;
#endif
    std::unique_ptr<crowdb::tree::PageStore>      store;
    std::unique_ptr<crowdb::tree::AsyncPageStore> async_store;
    crowdb::tree::AsyncPageStore                 *async_store_view = nullptr;
    std::string                                   page_store_label;
};

struct ct_page_store
{
    crowdb::tree::AllocationLease    allocation;
    std::shared_ptr<PageStoreBundle> bundle;
};

namespace crowdb::tree
{
class Crowdbtree;
struct Config;
struct CoreScanOptions;
} // namespace crowdb::tree
struct ct_runtime;
struct ct_tree_runtime_options;
crowdb::tree::Crowdbtree *c_api_tree(ct_tree *tree) noexcept;
crowdb::tree::PageStore  *c_api_page_store(ct_tree *tree) noexcept;
PageStoreBundle          *c_api_page_store_bundle(ct_tree *tree) noexcept;
struct ct_page_store_release_service;
ct_status c_api_bind_page_store_release(PageStoreBundle *bundle, bool start_block_reactor, void (*wake)(void *),
                                        void (*drop)(void *), void *context, ct_page_store_release_service **out);
#ifdef CROWDB_HAVE_LIBURING
std::shared_ptr<crowdb::common::DiskIOUring> c_api_create_block_reactor();
#endif
struct CompletionSignal;
std::shared_ptr<CompletionSignal> c_api_completion_signal(ct_tree *tree) noexcept;
void                              c_api_notify_completion(const std::shared_ptr<CompletionSignal> &signal) noexcept;
ct_status c_api_scan_options(const ct_scan_options_v2 *options, crowdb::tree::CoreScanOptions *out) noexcept;
ct_status ct_open_configured(const ct_options *options, ct_runtime *runtime, const ct_tree_runtime_options *binding,
                             ct_tree **out);

ct_status ct_rebuild_configured(ct_tree *source, const ct_options *destination_options, crowdb::tree::Config options,
                                ct_tree **out, ct_range_rebuild_stats *stats);
