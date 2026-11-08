// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/page_store/page_store.h"
#include "local_checkpoint_test_support.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
struct TestInspection
{
    Config                        config;
    std::shared_ptr<PageMemStore> store = std::make_shared<PageMemStore>(4096);
    std::unique_ptr<Crowdbtree>   tree;
    uint64_t                      runtime_bytes = 0;

    TestInspection()
    {
        EXPECT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
        runtime_bytes                               = config.runtime->snapshot().allocated.routing_bytes;
        config.page_store                           = store.get();
        config.page_store_owner                     = store;
        config.frame_bytes                          = 4096;
        config.leaf_split_bytes                     = 3000;
        config.runtime_binding.limits.io_operations = 1;
        EXPECT_TRUE(Crowdbtree::open(config, &tree).ok());
    }
};

TEST(RuntimeInspection, LogicalOverlayIsEncodedWithoutChangingPublicationOrAllocatingPids)
{
    TestInspection test;
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    auto        metrics         = TreeCoreAccess::get(*test.tree)->publisher().work_metrics();
    const auto  encoding_before = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    const auto  before          = test.tree->version();
    NativeFrame frame;
    uint64_t    version = 0;
    uint64_t    root    = 0;
    uint32_t    deltas  = 0;
    auto        status  = test.tree->inspect_page({}, before, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encoding_before + 1);
    metrics.reset();
    EXPECT_EQ(before, version);
    EXPECT_EQ(before, test.tree->version());
    EXPECT_EQ(root, frame.page_id);
    ASSERT_TRUE(frame_validate(frame.frame.data(), frame.frame.size()));
    LeafFrameView leaf(frame.frame.data(), frame.frame.size());
    ASSERT_EQ(leaf.count(), 1U);
    EXPECT_EQ(leaf.key(0).to_string(), "key");
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "value");
    test.tree.reset();
    EXPECT_EQ(CellView(leaf.cell(0)).value().to_string(), "value");
    EXPECT_GE(test.config.runtime->snapshot().allocated.routing_bytes, test.runtime_bytes + frame.frame.capacity());
    frame = {};
    EXPECT_EQ(test.config.runtime->snapshot().allocated.routing_bytes, test.runtime_bytes);
}

TEST(RuntimeInspection, ColdIndexAndLeafUseOneIoPermitAndRejectStaleOrInvalidPaths)
{
    TestInspection test;
    Batch          batch;
    for (size_t i = 0; i < 100; ++i) {
        batch.ops.push_back({.key = std::to_string(1000 + i), .kind = OpKind::kPut, .value = std::string(100, 'v')});
    }
    ASSERT_TRUE(test.tree->apply(1, batch).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    test.tree.reset();
    ASSERT_TRUE(Crowdbtree::open(test.config, &test.tree).ok());
    NativeFrame frame;
    uint64_t    version = 0;
    uint64_t    root    = 0;
    uint32_t    deltas  = 0;
    auto        status  = test.tree->inspect_page({}, kInvalidPageId, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(frame_page_type(frame.frame.data()), page_type::kInnerBase);
    const auto revision = version;
    status              = test.tree->inspect_page({0}, revision, &frame, &version, &root, &deltas);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(frame_page_type(frame.frame.data()), page_type::kLeafBase);
    EXPECT_EQ(test.tree->inspect_page({UINT32_MAX}, revision, &frame, &version, &root, &deltas).code(),
              Code::kInvalidArgument);
    ASSERT_TRUE(test.tree->apply(2, {}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    EXPECT_EQ(test.tree->inspect_page({}, revision, &frame, &version, &root, &deltas).code(), Code::kUnavailable);
    EXPECT_EQ(
        test.tree->inspect_page(std::vector<uint32_t>(33), kInvalidPageId, &frame, &version, &root, &deltas).code(),
        Code::kInvalidArgument);
}

TEST(RuntimeInspection, ColdReadLetsTheOnlyBackendWorkerRunAndKeepsItsOriginalRevision)
{
    TestInspection test;
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "persisted"}}}).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    test.tree.reset();
    auto backend                 = std::make_shared<test_write::TestCheckpointStore>();
    backend->device              = test.store;
    test.config.async_page_store = backend.get();
    test.config.page_store_owner = backend;
    ASSERT_TRUE(Crowdbtree::open(test.config, &test.tree).ok());
    auto *core = TreeCoreAccess::get(*test.tree);

    std::promise<std::function<void()>> read;
    auto                                pending = read.get_future();
    backend->defer_read = [&, defer = true](PageAddr at, void *out, size_t bytes, AsyncCompletion &done) mutable {
        if (!std::exchange(defer, false)) {
            return false;
        }
        read.set_value([at, out, bytes, done = std::move(done), store = test.store] {
            done.complete(store->read_at(at, static_cast<uint8_t *>(out), bytes));
        });
        return true;
    };

    std::promise<Status> inspected;
    auto                 terminal = inspected.get_future();
    ASSERT_TRUE(core->inspect_page_async({}, test.tree->version(),
                                         [&](Status status, NativeFrame, uint64_t, uint64_t, uint32_t) {
                                             inspected.set_value(std::move(status));
                                         })
                    .ok());
    ASSERT_EQ(pending.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(terminal.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    auto resume = pending.get();

    std::promise<void> progress;
    auto               advanced = progress.get_future();
    auto               binding  = core->binding();
    ASSERT_TRUE(binding->backend_cpu()
                    ->try_submit(
                        binding->backend_lane(),
                        [](void *context) noexcept { static_cast<std::promise<void> *>(context)->set_value(); },
                        &progress)
                    .ok());
    EXPECT_EQ(advanced.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    ASSERT_TRUE(test.tree->apply(2, {}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    resume();
    resume = {};
    ASSERT_EQ(terminal.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(terminal.get().code(), Code::kUnavailable);
    binding->cpu()->drain();
    binding->backend_cpu()->drain();
}
} // namespace
} // namespace crowdb::tree
