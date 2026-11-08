// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/page_store/page_store_release_service.h"
#include "runtime/bootstrap_budget.h"

#include <gtest/gtest.h>

#include <atomic>
#include <latch>
#include <thread>
#include <vector>

namespace crowdb::tree
{
namespace
{
struct TestRelease : PageStoreReleaseService::Node
{
    std::shared_ptr<AllocationBudget> budget;
    std::atomic<size_t>              *completed;
    size_t                            remaining = 0;
    std::latch                       *entered   = nullptr;
    std::latch                       *proceed   = nullptr;

    TestRelease(std::shared_ptr<AllocationBudget> credit, std::atomic<size_t> *done, size_t steps)
        : budget(std::move(credit)),
          completed(done),
          remaining(steps)
    {
        dispose = [](PageStoreReleaseService::Node *base, common::DiskIOUring *) noexcept {
            auto *node = static_cast<TestRelease *>(base);
            if (node->remaining != 0) {
                --node->remaining;
                return PageStoreReleaseService::Disposal::kMore;
            }
            if (node->entered != nullptr) {
                node->entered->count_down();
                node->proceed->wait();
            }
            auto allocator = MetadataAllocator<TestRelease>(node->budget);
            node->completed->fetch_add(1);
            std::destroy_at(node);
            allocator.deallocate(node, 1);
            return PageStoreReleaseService::Disposal::kDone;
        };
    }
};

struct TestReleaseHost
{
    std::shared_ptr<detail::BootstrapBudget> budget = detail::BootstrapBudget::create(1024 * 1024);
    std::shared_ptr<PageStoreReleaseService> service;
    std::atomic<size_t>                      completed{0};

    explicit TestReleaseHost(size_t capacity = 64)
    {
        EXPECT_TRUE(PageStoreReleaseService::create(budget, capacity, {}, nullptr, nullptr, &service).ok());
    }

    TestRelease *prepare(size_t steps = 0)
    {
        MetadataAllocator<TestRelease> allocator(budget);
        auto                          *node = allocator.allocate(1);
        std::construct_at(node, budget, &completed, steps);
        auto status = service->register_node(*node);
        if (!status.ok()) {
            std::destroy_at(node);
            allocator.deallocate(node, 1);
            return nullptr;
        }
        return node;
    }
};

TEST(PageStoreReleaseService, FinalOwnerOnlyQueuesAndHostPumpOwnsActualDisposal)
{
    TestReleaseHost host;
    const auto      baseline = host.budget->snapshot().allocated;
    auto           *node     = host.prepare();
    ASSERT_NE(node, nullptr);
    const auto held = host.budget->snapshot().allocated;
    EXPECT_GT(held, baseline);
    std::thread reader([node] { PageStoreReleaseService::enqueue(*node); });
    reader.join();
    EXPECT_EQ(host.completed.load(), 0U);
    EXPECT_EQ(host.budget->snapshot().allocated, held);
    EXPECT_EQ(host.service->snapshot().registered, 1U);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.completed.load(), 1U);
    EXPECT_EQ(host.budget->snapshot().allocated, baseline);
}

TEST(PageStoreReleaseService, StopRejectsNewNodesButRetainedReaderCanReleaseLater)
{
    TestReleaseHost host(1);
    auto           *node = host.prepare();
    ASSERT_NE(node, nullptr);
    EXPECT_EQ(host.prepare(), nullptr);
    host.service->stop_admission();
    EXPECT_FALSE(host.service->drained());
    EXPECT_EQ(host.prepare(), nullptr);
    std::weak_ptr<PageStoreReleaseService> weak = host.service;
    host.service.reset();
    ASSERT_FALSE(weak.expired());
    PageStoreReleaseService::enqueue(*node);
    auto pump = weak.lock();
    ASSERT_NE(pump, nullptr);
    EXPECT_EQ(pump->pump(1).completed, 1U);
    EXPECT_TRUE(pump->drained());
    pump.reset();
    EXPECT_TRUE(weak.expired());
}

TEST(PageStoreReleaseService, FiniteBatchesPreventDeferredAndIncomingStarvation)
{
    TestReleaseHost host;
    auto           *slow = host.prepare(2);
    auto           *fast = host.prepare();
    ASSERT_NE(slow, nullptr);
    ASSERT_NE(fast, nullptr);
    PageStoreReleaseService::enqueue(*fast);
    PageStoreReleaseService::enqueue(*slow);
    EXPECT_EQ(host.service->pump(1).completed, 0U);
    auto *newer = host.prepare();
    ASSERT_NE(newer, nullptr);
    PageStoreReleaseService::enqueue(*newer);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.service->pump(1).completed, 0U);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.service->pump(1).completed, 1U);
    EXPECT_EQ(host.completed.load(), 3U);
}

TEST(PageStoreReleaseService, DrainedIsPublishedOnlyAfterActualNodeFree)
{
    TestReleaseHost host;
    const auto      baseline = host.budget->snapshot().allocated;
    auto           *node     = host.prepare();
    ASSERT_NE(node, nullptr);
    std::latch entered(1);
    std::latch proceed(1);
    node->entered = &entered;
    node->proceed = &proceed;
    PageStoreReleaseService::enqueue(*node);
    host.service->stop_admission();
    std::thread pump([&] { (void)host.service->pump(1); });
    entered.wait();
    EXPECT_FALSE(host.service->drained());
    EXPECT_GT(host.budget->snapshot().allocated, baseline);
    EXPECT_TRUE(host.service->pump(1).more);
    proceed.count_down();
    pump.join();
    EXPECT_TRUE(host.service->drained());
    EXPECT_EQ(host.budget->snapshot().allocated, baseline);
}

TEST(PageStoreReleaseService, ConcurrentFinalOwnersUseTheirPreallocatedNodes)
{
    TestReleaseHost            host;
    std::vector<TestRelease *> nodes;
    nodes.reserve(64);
    for (size_t index = 0; index < 64; ++index) {
        nodes.push_back(host.prepare());
    }
    const auto               held = host.budget->snapshot().allocated;
    std::vector<std::thread> readers;
    readers.reserve(4);
    for (size_t thread = 0; thread < 4; ++thread) {
        readers.emplace_back([&, thread] {
            for (size_t index = thread; index < nodes.size(); index += 4) {
                PageStoreReleaseService::enqueue(*nodes[index]);
            }
        });
    }
    for (auto &reader : readers) {
        reader.join();
    }
    EXPECT_EQ(host.budget->snapshot().allocated, held);
    host.service->stop_admission();
    while (!host.service->drained()) {
        const auto step = host.service->pump(3);
        EXPECT_LE(step.visited, 3U);
    }
    EXPECT_EQ(host.completed.load(), 64U);
}
} // namespace
} // namespace crowdb::tree
