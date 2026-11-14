// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/local_backend_state.h"
#include "btree/allocation_account.h"
#include "btree/tree_core.h"
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>

namespace crowdb::tree
{
TEST(CheckpointActualMemory, PersistentFlushFinishesAboveTheCeilingAndNewWritesResumeAfterRelease)
{
    constexpr size_t           partition = 32ULL * 1024 * 1024;
    MemPageStore               store{4096};
    const std::vector<uint8_t> orphan(160 * MemPageStore::kBackingPageBytes, 37);
    ASSERT_TRUE(store.write_at(0, orphan.data(), orphan.size()).ok());
    const std::array<uint8_t, 8192> anchors{};
    ASSERT_TRUE(store.write_at(0, anchors.data(), anchors.size()).ok());
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store             = &store;
    config.frame_bytes            = 4096;
    config.leaf_split_bytes       = 400;
    config.runtime_binding.limits = {.handoff_bytes    = partition,
                                     .routing_bytes    = partition,
                                     .reclaim_bytes    = partition,
                                     .writer_bytes     = partition,
                                     .gap_repair_bytes = partition,
                                     .io_operations    = 1};
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    Batch input;
    for (size_t i = 0; i < 40; ++i) {
        input.ops.push_back({.key = std::to_string(i), .kind = OpKind::kPut, .value = std::string(64, 'v')});
    }
    ASSERT_TRUE(core->apply(1, input).ok());
    {
        // Retain real allocated capacity from accepted work without touching
        // the unused bytes. The assertion covers total memory, not a forecast.
        BudgetAllocator<uint8_t, AllocationBudget, AllocationKind::kScratch> allocator(core->accepted_allocations());
        constexpr size_t                                                     bytes    = 10 * partition;
        auto                                                                *retained = allocator.allocate(bytes);
        const auto                                                           release =
            std::shared_ptr<uint8_t>(retained, [allocator](auto *data) mutable { allocator.deallocate(data, bytes); });
        ASSERT_TRUE(detail::ResourceBudget::memory_exhausted(core->binding()->account()));
        EXPECT_EQ(core->apply(2, input).code(), Code::kUnavailable);
        std::promise<std::pair<Status, uint64_t>> complete;
        auto                                      result = complete.get_future();
        core->checkpoint_async(
            [&](Status written, uint64_t coverage) { complete.set_value({std::move(written), coverage}); });
        ASSERT_EQ(result.wait_for(std::chrono::seconds(10)), std::future_status::ready);
        const auto [written, coverage] = result.get();
        ASSERT_TRUE(written.ok()) << written.to_string();
        EXPECT_EQ(coverage, 1U);
        EXPECT_EQ(core->durable_coverage(), 1U);
        core->operation_lifetime().drain_accepted();
        core->binding()->drain();
        EXPECT_LT(store.resident_bytes(), 3 * MemPageStore::kBackingPageBytes);
        EXPECT_EQ(core->apply(2, input).code(), Code::kUnavailable);
    }
    EXPECT_FALSE(detail::ResourceBudget::memory_exhausted(core->binding()->account()));
    ASSERT_TRUE(core->apply(2, input).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), 2U);
    GetView value;
    ASSERT_TRUE(core->get("1", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(64, 'v'));
}
} // namespace crowdb::tree
