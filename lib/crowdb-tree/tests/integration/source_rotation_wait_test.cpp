// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
class SourceRotationWait : public ::testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers = host.backend_workers = 1;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.memtable_flush_entries = 1;
        config.memtable_flush_bytes   = SIZE_MAX;
        config.max_memtable_count     = 2;
        tree                          = std::make_unique<Crowdbtree>(config);
        core                          = TreeCoreAccess::get(*tree);
        ASSERT_TRUE(tree->put("first", "value").ok());
        core->binding()->drain();
        core->set_flush_handoff_wait_for_tests([&](uint64_t, std::function<void()> ready) {
            if (!armed.exchange(false)) {
                return false;
            }
            resume = std::move(ready);
            paused.release();
            return true;
        });
        std::promise<Status> written;
        auto                 result = written.get_future();
        tree->get_async("first", [&](Status read, GetView value) {
            EXPECT_TRUE(read.ok()) << read.to_string();
            EXPECT_TRUE(value.found());
            written.set_value(tree->put("second", "next"));
        });
        ASSERT_EQ(result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        const auto status = result.get();
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_TRUE(paused.try_acquire_for(std::chrono::seconds(5)));
    }

    void resume_handoff()
    {
        if (resume) {
            auto ready = std::exchange(resume, {});
            ready();
        }
    }

    void TearDown() override
    {
        resume_handoff();
        if (core != nullptr) {
            core->binding()->drain();
            core->set_flush_handoff_wait_for_tests({});
        }
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
    std::atomic<bool>           armed{true};
    std::binary_semaphore       paused{0};
    std::function<void()>       resume;
};

TEST_F(SourceRotationWait, ReadyHandoffResumesWithoutAnotherWrite)
{
    resume_handoff();
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (core->capture()->coverage != 2 && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    ASSERT_EQ(core->capture()->coverage, 2U);
    core->binding()->drain();
    EXPECT_EQ(tree->get_view("second").value().to_string(), "next");
    ASSERT_TRUE(tree->put("third", "continued").ok());
}

TEST_F(SourceRotationWait, CloseDrainsAcceptedHandoffBeforeReleasingSources)
{
    auto close = std::async(std::launch::async, [&] { core->close_and_drain(); });
    EXPECT_EQ(close.wait_for(std::chrono::milliseconds(10)), std::future_status::timeout);
    resume_handoff();
    close.get();
    EXPECT_TRUE(core->closed());
    EXPECT_EQ(tree->contiguous_slot(), 2U);
}

TEST_F(SourceRotationWait, CloseReleasesAHostWriterWaitingForTheSoftPause)
{
    core->binding()->drain();
    const auto           baseline = core->operation_lifetime().outstanding();
    std::promise<Status> written;
    auto                 result = written.get_future();
    std::thread          writer([&] { written.set_value(tree->put("third", "accepted ingress")); });
    const auto           until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (core->operation_lifetime().outstanding() <= baseline &&
           result.wait_for(std::chrono::milliseconds(0)) != std::future_status::ready &&
           std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    EXPECT_GT(core->operation_lifetime().outstanding(), baseline);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    auto       close    = std::async(std::launch::async, [&] { core->close_and_drain(); });
    const auto released = result.wait_for(std::chrono::seconds(5));
    resume_handoff();
    writer.join();
    close.get();
    EXPECT_EQ(released, std::future_status::ready);
    const auto status = result.get();
    EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
    EXPECT_EQ(tree->contiguous_slot(), 2U);
}

TEST(SourceRotationProgress, RetainedReclaimCannotStrandAnAlreadyAppliedHostWrite)
{
    Config config;
    config.memtable_flush_entries = 1;
    config.memtable_flush_bytes   = SIZE_MAX;
    config.max_memtable_count     = 2;
    Crowdbtree  tree(config);
    auto *const core = TreeCoreAccess::get(tree);
    ASSERT_TRUE(tree.put("first", "value").ok());
    core->binding()->drain();
    const auto account   = core->binding()->account();
    const auto available = detail::ResourceBudget::available(account, detail::BudgetClass::kReclaim);
    auto       reserved  = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kReclaim, available);
    if (!reserved) {
        FAIL() << "Reclaim credit unavailable";
    }
    auto retained = reserved->allocate(available);
    ASSERT_TRUE(retained);
    std::atomic<bool> done{false};
    Status            status;
    std::thread       writer([&] {
        status = tree.put("second", "accepted");
        done.store(true, std::memory_order_release);
    });
    const auto        until = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    const bool returned_with_retained_owner = done.load(std::memory_order_acquire);
    retained.reset();
    reserved.reset();
    writer.join();
    EXPECT_TRUE(returned_with_retained_owner);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(tree.flush().ok());
    EXPECT_EQ(tree.get_view("second").value().to_string(), "accepted");
}
} // namespace
} // namespace crowdb::tree
