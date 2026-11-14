// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/apply_dispatcher.h"
#include "btree/tree_core_access.h"
#include "btree/tree_generation_lease.h"

#include <gtest/gtest.h>

#include <future>
#include <semaphore>
#include <thread>

namespace crowdb::tree
{
namespace
{
class PreparedGenerationLease : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions options;
        options.workers = 1;
        ASSERT_TRUE(TreeRuntime::create(options, &config.runtime).ok());
        tree = std::make_unique<Crowdbtree>(config);
        core = TreeCoreAccess::get(*tree);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(PreparedGenerationLease, CancelReopensTheUnchangedSourcesAndRoot)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    ASSERT_TRUE(tree->flush().ok());
    const auto                           original = core->capture();
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    ASSERT_TRUE(lease->acquire().ok());
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->put("blocked", "value").code(), Code::kUnavailable);
    lease->cancel();
    lease.reset();
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->get_view("old").value().to_string(), "retained");
    ASSERT_TRUE(tree->put("next", "accepted").ok());
    ASSERT_TRUE(tree->flush().ok());
    EXPECT_EQ(tree->get_view("next").value().to_string(), "accepted");
}

TEST_F(PreparedGenerationLease, AsyncAcquireLeavesTheOnlyWorkerAvailableAndCanReopenAdmission)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    ASSERT_TRUE(tree->flush().ok());
    const auto                           original = core->capture();
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    auto                                           binding = core->binding();
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    ASSERT_TRUE(core->operation_lifetime().acquire(binding->lifecycle_budget(), &ticket).ok());

    struct Call : std::enable_shared_from_this<Call>
    {
        std::shared_ptr<TreeGenerationLease> lease;
        std::promise<Status>                 submitted;
        std::promise<Status>                 completed;
        std::promise<void>                   probe;
        ApplyDispatcher::Node                start{[](void *raw) noexcept {
                                        auto &self = *static_cast<Call *>(raw);
                                        self.submitted.set_value(self.lease->acquire_async(
                                            {.context = &self,
                                             .complete_fn =
                                                 [](void *context, Status status) noexcept {
                                                     static_cast<Call *>(context)->completed.set_value(
                                                         std::move(status));
                                                 },
                                             .owner = self.weak_from_this().lock()}));
                                                   },
                                                   this};
        ApplyDispatcher::Node advance{[](void *raw) noexcept { static_cast<Call *>(raw)->probe.set_value(); }, this};
    };

    auto call      = std::make_shared<Call>();
    call->lease    = lease;
    auto submitted = call->submitted.get_future();
    auto completed = call->completed.get_future();
    auto probe     = call->probe.get_future();
    core->cpu_progress()->request(call->start, call);
    ASSERT_EQ(submitted.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    ASSERT_TRUE(submitted.get().ok());
    EXPECT_EQ(completed.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(tree->put("blocked", "value").code(), Code::kUnavailable);
    core->cpu_progress()->request(call->advance, call);
    EXPECT_EQ(probe.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    ticket.reset();
    ASSERT_EQ(completed.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_TRUE(completed.get().ok());
    lease->cancel();
    EXPECT_EQ(core->capture(), original);
    EXPECT_EQ(tree->get_view("old").value().to_string(), "retained");
    EXPECT_TRUE(tree->put("next", "accepted").ok());
}

TEST_F(PreparedGenerationLease, AsyncAcquireYieldsToCloseWhileAnAcceptedTicketDrains)
{
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    ASSERT_TRUE(core->operation_lifetime().acquire(core->binding()->lifecycle_budget(), &ticket).ok());
    auto prepared = std::make_shared<std::promise<Status>>();
    auto acquired = prepared->get_future();
    ASSERT_TRUE(lease
                    ->acquire_async({.context = prepared.get(),
                                     .complete_fn =
                                         [](void *raw, Status status) noexcept {
                                             static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
                                         },
                                     .owner = prepared})
                    .ok());
    core->binding()->lane()->drain();
    auto closing = std::make_shared<std::promise<Status>>();
    auto closed  = closing->get_future();
    ASSERT_TRUE(core->close_async({.context = closing.get(),
                                   .complete_fn =
                                       [](void *raw, Status status) noexcept {
                                           static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
                                       },
                                   .owner = closing}));
    EXPECT_EQ(closed.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    ticket.reset();
    ASSERT_EQ(acquired.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(acquired.get().code(), Code::kUnavailable);
    ASSERT_EQ(closed.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_TRUE(closed.get().ok());
    EXPECT_EQ(lease->commit().code(), Code::kUnavailable);
}

TEST_F(PreparedGenerationLease, CommitDoesNotWaitForCpuOrReadmitStorage)
{
    ASSERT_TRUE(tree->put("old", "retained").ok());
    auto                                 borrowed   = tree->get_view("old");
    const auto                           generation = core->capture()->generation;
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    ASSERT_TRUE(lease->acquire().ok());

    struct BlockedWorker
    {
        std::binary_semaphore entered{0};
        std::binary_semaphore release{0};
    } blocked;

    ASSERT_TRUE(core->binding()
                    ->cpu()
                    ->try_submit(
                        core->binding()->lane(),
                        [](void *context) noexcept {
                            auto &worker = *static_cast<BlockedWorker *>(context);
                            worker.entered.release();
                            worker.release.acquire();
                        },
                        &blocked)
                    .ok());
    blocked.entered.acquire();
    config.runtime->stop_admission();
    // A new caller thread has no existing publication Epoch participant. The
    // prepared commit must not attempt a capture or register one here.
    Status       installed;
    std::jthread caller([&] { installed = lease->commit(); });
    caller.join();
    blocked.release.release();
    // The commit returned with the worker still paused. Join its task before
    // the fixture reclaims the stack storage containing the semaphore.
    core->binding()->lane()->drain();
    ASSERT_TRUE(installed.ok()) << installed.to_string();
    EXPECT_EQ(core->capture()->generation, generation + 1);
    EXPECT_EQ(core->capture()->coverage, 0U);
    EXPECT_EQ(core->contiguous_slot(), 0U);
    EXPECT_EQ(borrowed.value().to_string(), "retained");
}

TEST_F(PreparedGenerationLease, CancelBeforeAcquireDoesNotFenceTheTree)
{
    const auto                           original = core->capture();
    std::shared_ptr<TreeGenerationLease> lease;
    ASSERT_TRUE(TreeGenerationLease::prepare(*core, {}, nullptr, &lease).ok());
    lease.reset();
    EXPECT_EQ(core->capture(), original);
    ASSERT_TRUE(tree->put("key", "value").ok());
    EXPECT_EQ(tree->get_view("key").slot(), 1U);
}
} // namespace
} // namespace crowdb::tree
