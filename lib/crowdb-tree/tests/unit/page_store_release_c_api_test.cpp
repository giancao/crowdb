// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_internal.h"
#include "crowdb-tree/page_store/page_disk_store.h"
#include "crowdb-tree/page_store/page_store_release_service.h"
#include "crowdb-tree/page_store_release_c_api.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestWake
{
    std::atomic<size_t> wakes{0};
    std::atomic<size_t> drops{0};

    static void wake(void *p)
    {
        static_cast<TestWake *>(p)->wakes.fetch_add(1);
    }

    static void drop(void *p)
    {
        static_cast<TestWake *>(p)->drops.fetch_add(1);
    }
};

TEST(PageStoreReleaseCAbi, HostHandleOutlivesTreeAndDropsContextOnlyAfterDisposal)
{
    tree_test::TempDir path("block_release_abi_");
    ct_options         options{};
    options.path              = path.path.c_str();
    options.page_store_kind   = CT_PAGE_STORE_DISK;
    options.iu_size           = 4096;
    options.frame_bytes       = 4096;
    options.block_size        = 1 << 20;
    options.buffer_pool_bytes = 1 << 20;
    ct_tree *tree             = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    const auto close = [](void *context, ct_status status) {
        std::unique_ptr<std::promise<ct_status>> completed(static_cast<std::promise<ct_status> *>(context));
        completed->set_value(status);
    };
    ASSERT_EQ(ct_close_async(tree, [](void *, ct_status) {}, nullptr), static_cast<ct_status>(Code::kNotSupported));
    TestWake                       wake;
    ct_page_store_release_service *service = nullptr;
    ASSERT_EQ(ct_tree_page_store_release_service(tree, TestWake::wake, TestWake::drop, &wake, &service), 0);
#ifdef CROWDB_HAVE_LIBURING
    EXPECT_NE(ct_page_store_release_service_is_async(service), 0);
#endif
    TestWake                       refused;
    ct_page_store_release_service *duplicate = nullptr;
    EXPECT_NE(ct_tree_page_store_release_service(tree, TestWake::wake, TestWake::drop, &refused, &duplicate), 0);
    EXPECT_EQ(refused.drops.load(), 0U);
    EXPECT_EQ(duplicate, nullptr);
    auto       completion = std::make_unique<std::promise<ct_status>>();
    auto       result     = completion->get_future();
    auto      *context    = completion.release();
    const auto accepted   = ct_close_async(tree, close, context);
    if (accepted != 0) {
        completion.reset(context);
        ct_close(tree);
    }
    ASSERT_EQ(accepted, 0);
    ASSERT_EQ(result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(result.get(), 0);
    EXPECT_EQ(wake.drops.load(), 0U);
    bool       drained  = false;
    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (std::chrono::steady_clock::now() < deadline) {
        const auto step = ct_page_store_release_service_pump(service, 1);
        EXPECT_LE(step.visited, 1U);
        if (step.drained != 0) {
            drained = true;
            break;
        }
        if (step.more == 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_TRUE(drained);
    ct_page_store_release_service_free(service);
    EXPECT_EQ(wake.drops.load(), 1U);
    EXPECT_GT(wake.wakes.load(), 0U);
}

TEST(PageStoreReleaseCAbi, UnsupportedBackendDoesNotConsumeWakeContext)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open(&options, &tree), 0);
    TestWake                       wake;
    ct_page_store_release_service *service = nullptr;
    EXPECT_EQ(ct_tree_page_store_release_service(tree, TestWake::wake, TestWake::drop, &wake, &service),
              static_cast<ct_status>(Code::kNotFound));
    EXPECT_EQ(service, nullptr);
    EXPECT_EQ(wake.drops.load(), 0U);
    ct_close(tree);
}
} // namespace
} // namespace crowdb::tree
