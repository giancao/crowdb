// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/runtime_c_api.h"
#include "page_store/local/local_page_store_state.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>

namespace crowdb::tree
{
namespace
{
void continue_after_close(bool injected)
{
    tree_test::TempDir directory("snapshot_page_store_owner_");
    ct_runtime        *raw_runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &raw_runtime), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(raw_runtime, &ct_runtime_free);
    ct_page_store                                          *raw_store = nullptr;
    if (injected) {
        ASSERT_EQ(ct_page_store_open_mem(4096, &raw_store), 0);
    }
    std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> store(raw_store, &ct_page_store_free);
    ct_options                                                    options{};
    options.page_store       = store.get();
    options.path             = injected ? nullptr : directory.path.c_str();
    options.page_store_kind  = CT_PAGE_STORE_DISK;
    options.block_size       = 65536;
    options.iu_size          = 4096;
    options.frame_bytes      = 4096;
    options.max_inline_value = 64;
    options.sync_mode        = CT_SYNC_SKIP;
    ct_tree *raw_tree        = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime.get(), nullptr, &raw_tree), 0);
    std::unique_ptr<ct_tree, decltype(&ct_close)> tree(raw_tree, &ct_close);
    const std::string                             value(32768, 'v');
    for (uint8_t key : {uint8_t{'a'}, uint8_t{'b'}}) {
        ASSERT_EQ(ct_apply_put(tree.get(), static_cast<uint64_t>(key - 'a') + 1, &key, 1,
                               reinterpret_cast<const uint8_t *>(value.data()), value.size()),
                  0);
    }
    uint64_t coverage = 0;
    ASSERT_EQ(ct_flush_with_persistent(tree.get(), &coverage), 0);
    ASSERT_EQ(coverage, 2U);
    tree.reset();
    raw_tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime.get(), nullptr, &raw_tree), 0);
    tree.reset(raw_tree);
    store.reset();
    auto *core = TreeCoreAccess::get(*c_api_tree(tree.get()));
    ASSERT_NE(core, nullptr);
    std::weak_ptr<const void> backend = core->config().page_store_owner;
    std::weak_ptr<const void> domain  = core->local_page_store()->domain;
    ASSERT_FALSE(backend.expired());
    ASSERT_FALSE(domain.expired());
    ct_export *raw_export = nullptr;
    ASSERT_EQ(ct_snapshot_export_begin(tree.get(), 64, &raw_export), 0);
    std::unique_ptr<ct_export, decltype(&ct_snapshot_export_end)> stream(raw_export, &ct_snapshot_export_end);
    const auto                                                    total = ct_snapshot_export_total_bytes(stream.get());
    ct_snapshot_chunk                                            *first = nullptr;
    int32_t                                                       done  = 0;
    ASSERT_EQ(ct_snapshot_export_next_v2(stream.get(), 0, &first, &done), 0);
    ASSERT_EQ(done, 0);
    const uint8_t *first_data = nullptr;
    size_t         returned   = 0;
    ASSERT_EQ(ct_snapshot_chunk_data(first, &first_data, &returned), 0);
    ASSERT_GT(returned, 0U);
    ASSERT_LE(returned, 64U);
    ASSERT_NE(first_data, nullptr);
    ct_snapshot_chunk_free(first);
    if (injected) {
        // A consuming close must release the source without joining this
        // independent reader. The runtime host remains live for the stream.
        auto       completed = std::make_unique<std::promise<ct_status>>();
        auto       result    = completed->get_future();
        auto      *context   = completed.release();
        auto      *source    = tree.release();
        const auto accepted  = ct_close_async(
            source,
            [](void *context, ct_status status) {
                std::unique_ptr<std::promise<ct_status>> ready(static_cast<std::promise<ct_status> *>(context));
                ready->set_value(status);
            },
            context);
        if (accepted != 0) {
            tree.reset(source);
            completed.reset(context);
        }
        ASSERT_EQ(accepted, 0);
        ASSERT_EQ(result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        EXPECT_EQ(result.get(), 0);
    }
    else {
        tree.reset();
        runtime.reset();
    }
    ASSERT_FALSE(backend.expired());
    ASSERT_FALSE(domain.expired());
    while (done == 0) {
        ct_snapshot_chunk *chunk = nullptr;
        ASSERT_EQ(ct_snapshot_export_next_v2(stream.get(), returned, &chunk, &done), 0);
        std::unique_ptr<ct_snapshot_chunk, decltype(&ct_snapshot_chunk_free)> held(chunk, &ct_snapshot_chunk_free);
        const uint8_t                                                        *data   = nullptr;
        size_t                                                                length = 0;
        ASSERT_EQ(ct_snapshot_chunk_data(held.get(), &data, &length), 0);
        ASSERT_GT(length, 0U);
        ASSERT_LE(length, 64U);
        ASSERT_NE(data, nullptr);
        returned += length;
        ASSERT_LE(returned, total);
    }
    EXPECT_EQ(returned, total);
    stream.reset();
    if (runtime) {
        ct_runtime_drain_cpu(runtime.get());
    }
    EXPECT_TRUE(backend.expired());
    EXPECT_TRUE(domain.expired());
}

TEST(SnapshotPageStoreOwnerCAbi, OwnedBlockStreamContinuesColdReadsAfterTreeAndRuntimeClose)
{
    continue_after_close(false);
}

TEST(SnapshotPageStoreOwnerCAbi, InjectedStoreStreamRetainsItsIndependentBackendAndDomain)
{
    continue_after_close(true);
}
} // namespace
} // namespace crowdb::tree
