// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_apply_operation.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"

#include <gtest/gtest.h>

#include <array>
#include <latch>
#include <thread>
#include <vector>

namespace crowdb::tree
{
namespace
{
struct TestApplyCompletion
{
    std::atomic<bool> done{false};
    Status            status;

    static void complete(void *context, detail::CallbackSignal signal, Status result) noexcept
    {
        auto &self  = *static_cast<TestApplyCompletion *>(context);
        self.status = signal == detail::CallbackSignal::kStopped ? Status::unavailable() : std::move(result);
        self.done.store(true, std::memory_order_release);
        self.done.notify_all();
    }

    void wait() const
    {
        while (!done.load(std::memory_order_acquire)) {
            done.wait(false, std::memory_order_acquire);
        }
    }
};

class TreeCoreAdmitted : public testing::Test
{
  protected:
    void SetUp() override
    {
        TreeRuntimeOptions host;
        host.workers = 2;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.frame_bytes                        = 4096;
        config.leaf_split_bytes                   = 3000;
        config.buffer_pool_bytes                  = 4 * 1024 * 1024;
        config.execution.source_max_gap_entries   = 64;
        config.execution.source_max_gap_key_bytes = 128 * 1024;
        tree                                      = std::make_unique<Crowdbtree>(config);
        core                                      = TreeCoreAccess::get(*tree);
    }

    Status prepare(Slice key, Slice value, std::shared_ptr<TreeApplyOperation> *out)
    {
        return core->prepare_apply(
            1, [=](size_t) { return CoreWriteInput{.key = key, .value = value}; }, detail::BudgetClass::kAdmittedWriter,
            0, out);
    }

    Status fill_full_source()
    {
        const auto batch_for = [](uint64_t slot) {
            Batch batch;
            for (size_t index = 0; index < 64; ++index) {
                batch.ops.push_back(
                    {.key = "k" + std::to_string((slot * 64) + index), .kind = OpKind::kPut, .value = "v"});
            }
            return batch;
        };
        for (uint64_t slot = 1; slot < 128; ++slot) {
            const auto status = tree->apply(slot, batch_for(slot));
            if (!status.ok()) {
                return Status::internal_error("fill slot " + std::to_string(slot) + ": " + status.to_string());
            }
            // Keep history disposal separate from the final CPU callback,
            // which cannot join its own queued retirement work.
            core->binding()->account()->drain_retirements();
        }
        // The final batch runs from a CPU callback. A source transition must
        // continue asynchronously rather than waiting for its own executor.
        TestApplyCompletion completion;
        tree->get_async("k64", [&](Status read, GetView) {
            auto status = read.ok() ? tree->apply(128, batch_for(128)) : read;
            TestApplyCompletion::complete(&completion, detail::CallbackSignal::kValue, std::move(status));
        });
        completion.wait();
        core->binding()->lane()->drain();
        return completion.status;
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeCoreAdmitted, PreparationOwnsCallerBytesAndDoesNotBlockReplacementOrClose)
{
    std::shared_ptr<TreeApplyOperation> stale;
    ASSERT_TRUE(prepare("before", "value", &stale).ok());
    ASSERT_TRUE(core->clear().ok());
    EXPECT_EQ(stale->arm().code(), Code::kUnavailable);
    stale.reset();
    std::shared_ptr<TreeApplyOperation> unsubmitted;
    ASSERT_TRUE(prepare("after", "value", &unsubmitted).ok());
    tree.reset();
    EXPECT_EQ(unsubmitted->arm().code(), Code::kUnavailable);
}

TEST_F(TreeCoreAdmitted, SuspendedBackendOwnersDoNotLimitNewAdmittedApply)
{
    auto        &lifetime = core->operation_lifetime();
    const size_t maximum  = 2 * core->binding()->options().cpu_queue_limit;
    std::vector<std::shared_ptr<TreeOperationLifetime::Ticket>> occupied;
    occupied.reserve(maximum);
    for (uint64_t index = 0; index < maximum; ++index) {
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        ASSERT_TRUE(lifetime.acquire(core->binding()->lifecycle_budget(), &ticket).ok());
        occupied.push_back(std::move(ticket));
    }
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(prepare("admitted", "value", &operation).ok());
    ASSERT_TRUE(operation->arm().ok());
    auto usage = core->admission_resource_usage();
    EXPECT_EQ(usage[0].limit, 0U);
    EXPECT_EQ(usage[0].denied_tree, 0U);
    EXPECT_EQ(usage[0].used, maximum + 1);
    EXPECT_EQ(usage[1].used, 1U);
    TestApplyCompletion completed;
    operation->start(1, &TestApplyCompletion::complete, &completed);
    completed.wait();
    core->binding()->lane()->drain();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    usage = core->admission_resource_usage();
    EXPECT_EQ(usage[0].used, maximum);
    EXPECT_EQ(usage[1].used, 0U);
    EXPECT_EQ(usage[3].used, 1U);
    EXPECT_EQ(usage[5].used, 0U);
}

TEST_F(TreeCoreAdmitted, ArmedApplyFinishesAfterCloseFencesIngressAndCallerDropsTheHandle)
{
    std::shared_ptr<TreeApplyOperation> operation;
    std::string                         key   = "retained";
    std::string                         value = "owned before consensus";
    ASSERT_TRUE(prepare(key, value, &operation).ok());
    key.assign("changed");
    value.clear();
    ASSERT_TRUE(operation->arm().ok());
    std::atomic<bool> closed{false};
    std::jthread      closer([&] {
        core->close_and_drain();
        closed.store(true, std::memory_order_release);
    });
    while (!core->closed()) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(closed.load(std::memory_order_acquire));
    TestApplyCompletion completed;
    operation->start(1, &TestApplyCompletion::complete, &completed);
    operation.reset();
    completed.wait();
    closer.join();
    EXPECT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_TRUE(closed.load(std::memory_order_acquire));
    EXPECT_EQ(core->contiguous_slot(), 1U);
}

TEST_F(TreeCoreAdmitted, DistantNoopAwaitsAnActualFrontierTransitionWithoutSkippingTheGap)
{
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(core->prepare_apply(0, {}, detail::BudgetClass::kAdmittedWriter, 0, &operation).ok());
    ASSERT_TRUE(operation->arm().ok());
    TestApplyCompletion completed;
    operation->start(ApplyFrontier::kMaxWindow + 1, &TestApplyCompletion::complete, &completed);
    core->binding()->lane()->drain();
    EXPECT_FALSE(completed.done.load(std::memory_order_acquire));
    EXPECT_EQ(core->contiguous_slot(), 0U);
    core->force_advance_slot(1);
    completed.wait();
    EXPECT_TRUE(completed.status.ok());
    EXPECT_EQ(core->contiguous_slot(), 1U);
    EXPECT_EQ(core->max_seen_slot(), ApplyFrontier::kMaxWindow + 1);
}

TEST_F(TreeCoreAdmitted, FullSourceFlushIgnoresUnusedHandoffCreditAndConsumesTheAcceptedWrite)
{
    tree.reset();
    config.max_memtable_count     = 2;
    config.memtable_flush_entries = UINT32_MAX;
    config.memtable_flush_bytes   = SIZE_MAX;
    tree                          = std::make_unique<Crowdbtree>(config);
    core                          = TreeCoreAccess::get(*tree);
    // An unused reservation cannot block a flush that records actual allocations.
    auto pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kHandoff,
                                                        config.runtime_binding.limits.handoff_bytes);
    ASSERT_TRUE(pressure);
    std::shared_ptr<TreeApplyOperation> operation;
    std::string                         value = "owned";
    ASSERT_TRUE(prepare("last", value, &operation).ok());
    value.clear();
    ASSERT_TRUE(operation->arm().ok());
    const auto filled = fill_full_source();
    ASSERT_TRUE(filled.ok()) << filled.to_string();
    TestApplyCompletion completed;
    operation->start(129, &TestApplyCompletion::complete, &completed);
    operation.reset();
    completed.wait();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_EQ(tree->get_view("last").value().to_string(), "owned");
    EXPECT_EQ(core->contiguous_slot(), 129U);
    const auto terminal = core->publisher().work_metrics()->totals();
    EXPECT_EQ(terminal[static_cast<size_t>(TreeStage::FlushAdmissionWait)].count, 0U);
    EXPECT_EQ(terminal[static_cast<size_t>(TreeStage::BudgetWait)].count, 0U);
    EXPECT_EQ(terminal[static_cast<size_t>(TreeStage::BudgetWait)].inflight, 0U);
}

TEST_F(TreeCoreAdmitted, CompleteBatchMustFitTheGapSourceBeforeItCanBeProposed)
{
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = core->prepare_apply(
        65,
        [](size_t index) {
            static const std::array<std::string, 65> keys = [] {
                std::array<std::string, 65> result;
                for (size_t item = 0; item < result.size(); ++item) {
                    result[item] = std::to_string(item);
                }
                return result;
            }();
            return CoreWriteInput{.key = Slice(keys[index]), .value = Slice("value")};
        },
        detail::BudgetClass::kAdmittedWriter, 0, &operation);
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    EXPECT_EQ(core->contiguous_slot(), 0U);
}

TEST_F(TreeCoreAdmitted, OrdinaryFlushBatchesEightFullMemTables)
{
    tree.reset();
    config.max_memtable_count           = 16;
    config.memtable_flush_count         = 8;
    config.execution.source_max_entries = 1;
    tree                                = std::make_unique<Crowdbtree>(config);
    core                                = TreeCoreAccess::get(*tree);

    for (uint64_t slot = 1; slot <= 8; ++slot) {
        const auto                          key = "key" + std::to_string(slot);
        std::shared_ptr<TreeApplyOperation> operation;
        ASSERT_TRUE(prepare(key, "value", &operation).ok());
        ASSERT_TRUE(operation->arm().ok());
        TestApplyCompletion completed;
        operation->start(slot, &TestApplyCompletion::complete, &completed);
        completed.wait();
        ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
        core->binding()->drain();
        EXPECT_EQ(core->capture()->coverage, slot < 8 ? 0U : 8U);
        EXPECT_EQ(tree->frozen_table_count(), slot < 8 ? slot : 0U);
        EXPECT_TRUE(tree->get(key, nullptr, nullptr));
    }
    const auto totals = core->publisher().work_metrics()->totals();
    EXPECT_EQ(totals[static_cast<size_t>(TreeStage::FlushBatch)].count, 1U);
    EXPECT_EQ(core->durable_coverage(), 0U);
}

TEST_F(TreeCoreAdmitted, FullGapSourceRotatesBeforeTheBatchFlushThreshold)
{
    tree.reset();
    config.max_memtable_count               = 16;
    config.memtable_flush_count             = 8;
    config.execution.source_max_gap_entries = 1;
    tree                                    = std::make_unique<Crowdbtree>(config);
    core                                    = TreeCoreAccess::get(*tree);

    for (uint64_t slot : {4U, 1U, 2U}) {
        const auto                          key = "key" + std::to_string(slot);
        std::shared_ptr<TreeApplyOperation> operation;
        ASSERT_TRUE(prepare(key, "value", &operation).ok());
        ASSERT_TRUE(operation->arm().ok());
        TestApplyCompletion completed;
        operation->start(slot, &TestApplyCompletion::complete, &completed);
        completed.wait();
        ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
        core->binding()->drain();
        EXPECT_TRUE(tree->get(key, nullptr, nullptr));
    }
    EXPECT_EQ(core->capture()->coverage, 0U);
    EXPECT_EQ(tree->frozen_table_count(), 1U);
    EXPECT_EQ(tree->contiguous_slot(), 2U);
}

TEST_F(TreeCoreAdmitted, ReadyQueueBacklogDoesNotRejectAcceptedApply)
{
    tree.reset();
    config.max_memtable_count     = 2;
    config.memtable_flush_entries = UINT32_MAX;
    config.memtable_flush_bytes   = SIZE_MAX;
    tree                          = std::make_unique<Crowdbtree>(config);
    core                          = TreeCoreAccess::get(*tree);
    auto pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kHandoff,
                                                        config.runtime_binding.limits.handoff_bytes);
    ASSERT_TRUE(pressure);
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(prepare("accepted", "paid", &operation).ok());
    ASSERT_TRUE(operation->arm().ok());
    const auto filled = fill_full_source();
    ASSERT_TRUE(filled.ok()) << filled.to_string();
    const auto &runtime = core->binding()->cpu();
    const auto &lane    = core->binding()->lane();

    struct Gate
    {
        std::latch started{2};
        std::latch release{1};
    } gate;

    for (size_t index = 0; index < 2; ++index) {
        EXPECT_TRUE(runtime
                        ->try_submit(
                            lane,
                            +[](void *raw) noexcept {
                                auto &gate = *static_cast<Gate *>(raw);
                                gate.started.count_down();
                                gate.release.wait();
                            },
                            &gate)
                        .ok());
    }
    gate.started.wait();
    for (size_t index = 0; index <= runtime->queue_target(); ++index) {
        EXPECT_TRUE(runtime->try_submit(lane, +[](void *) noexcept {}, nullptr).ok());
    }
    EXPECT_GT(lane->queued(), runtime->queue_target());
    pressure.reset();
    TestApplyCompletion completed;
    operation->start(129, &TestApplyCompletion::complete, &completed);
    gate.release.count_down();
    completed.wait();
    lane->drain();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_EQ(core->contiguous_slot(), 129U);
    EXPECT_EQ(tree->get_view("accepted").value().to_string(), "paid");
}

} // namespace
} // namespace crowdb::tree
