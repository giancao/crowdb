// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/retirement_queue.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_checkpoint_internal.h"
#include "crowdb-tree/page_store/page_disk_store.h"
#include "page_store/local/local_page_store_state.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
TEST(TreeCoreRetirement, PublishedOwnersRemainValidAfterTreeAndExecutorDestruction)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.frame_bytes                 = 4096;
    config.leaf_split_bytes            = 3000;
    std::weak_ptr<TreeRuntime> runtime = config.runtime;
    auto                       tree    = std::make_unique<Crowdbtree>(config);
    auto                      *core    = TreeCoreAccess::get(*tree);
    auto                       account = core->binding()->account();
    for (size_t index = 0; index < 1024; ++index) {
        ASSERT_TRUE(tree->put(std::to_string(index), "retained value").ok());
    }
    auto    view = core->capture();
    GetView value;
    ASSERT_TRUE(core->get("512", &value).ok());
    ASSERT_TRUE(value.found());
    tree.reset();
    EXPECT_EQ(value.value().to_string(), "retained value");
    EXPECT_NE(view->sources->tables.back()->find("512"), nullptr);
    config.runtime.reset();
    EXPECT_TRUE(runtime.expired());
    view.reset();
    value = {};
    account->drain_retirements();
    const auto after = account->snapshot();
    for (size_t i = 0; i < detail::ResourceBudget::kClasses; ++i) {
        EXPECT_EQ(after.allocated[i], 0U) << i;
        EXPECT_EQ(after.reserved[i], 0U) << i;
    }
    for (const auto physical : after.physical) {
        EXPECT_EQ(physical, 0U);
    }
}

TEST(TreeCoreRetirement, OldPublicationRootsAreCollectedWhileTreeRemainsOpen)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree                    tree(config);
    auto                         *core    = TreeCoreAccess::get(tree);
    auto                          old     = core->capture();
    std::weak_ptr<const TreeRoot> retired = old;
    old.reset();
    ASSERT_TRUE(tree.put("key", "value").ok());
    const auto flushed = tree.flush();
    ASSERT_TRUE(flushed.ok()) << flushed.to_string();
    core->binding()->drain();
    EXPECT_TRUE(retired.expired());
    const auto work = core->binding()->lifecycle_budget()->retirement_queue()->snapshot();
    EXPECT_GT(work.enqueued, 0U);
    EXPECT_EQ(work.enqueued, work.freed);
    EXPECT_LE(work.max_step_nodes, detail::RetirementQueue::kNodesPerStep);
    const auto totals = core->publisher().work_metrics()->totals()[static_cast<size_t>(TreeStage::MemoryRetireWait)];
    EXPECT_GT(totals.attempts, 0U);
    EXPECT_EQ(totals.inflight, 0U);
}

TEST(TreeCoreRetirement, BackgroundLocalGcSuspendsWithoutHoldingTheBackendWorker)
{
    tree_test::TempDir             directory("runtime_gc_retirement_");
    std::unique_ptr<PageDiskStore> store;
    ASSERT_TRUE(PageDiskStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &store).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.page_store  = store.get();
    config.frame_bytes = 4096;
    std::shared_ptr<TreeCore> core;
    const auto                opened = TreeCore::open(config, &core);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    auto                                           binding = core->binding();
    auto                                           local   = core->local_page_store();
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    ASSERT_TRUE(core->operation_lifetime().acquire(binding->lifecycle_budget(), &ticket).ok());

    struct Probe
    {
        std::promise<void>    done;
        ApplyDispatcher::Node node{[](void *context) noexcept { static_cast<Probe *>(context)->done.set_value(); },
                                   this};
    };

    auto probe      = std::make_shared<Probe>();
    auto completed  = probe->done.get_future();
    auto retirement = core->accepted_allocations()->retirement_started();
    ASSERT_NE(retirement.finish, nullptr);
    checkpoint_detail::schedule_free_blocks(*core, std::move(ticket));
    binding->backend_progress()->request(probe->node, probe);
    const auto ready = completed.wait_for(std::chrono::seconds(3));
    EXPECT_TRUE(local->gc_active.load(std::memory_order_acquire));
    retirement.finish(retirement.owner.get());
    EXPECT_EQ(ready, std::future_status::ready);
    core->close_and_drain();
    EXPECT_FALSE(local->gc_active.load(std::memory_order_acquire));
}
} // namespace
} // namespace crowdb::tree
