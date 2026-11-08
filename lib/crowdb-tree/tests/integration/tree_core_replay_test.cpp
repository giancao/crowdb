// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_apply_operation.h"
#include "btree/tree_core.h"
#include "crowdb-tree/page_store/page_disk_store.h"
#include "page_store/local/local_page_store_state.h"
#include "test_tmp.h"

#include <gtest/gtest.h>

#include <array>
#include <thread>

namespace crowdb::tree
{
namespace
{
struct TestReplayCompletion
{
    std::atomic<bool> done{false};
    Status            status;

    static void complete(void *context, detail::CallbackSignal signal, Status status) noexcept
    {
        auto &result  = *static_cast<TestReplayCompletion *>(context);
        result.status = signal == detail::CallbackSignal::kStopped ? Status::unavailable() : std::move(status);
        result.done.store(true, std::memory_order_release);
        result.done.notify_all();
    }

    static void close(void *context, Status status) noexcept
    {
        complete(context, detail::CallbackSignal::kValue, std::move(status));
    }

    void wait() const
    {
        while (!done.load(std::memory_order_acquire)) {
            done.wait(false, std::memory_order_acquire);
        }
    }
};

struct TestReplayInput
{
    std::string key;
    std::string value;
};

class TreeCoreReplay : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(PageDiskStore::open_blocks(directory.path, 1, 2, 64 * 1024, 4096, &device).ok());
        TreeRuntimeOptions host;
        host.workers = 2;
        // Replay and source rotation retain fixed paths; one proposal must also fit.
        host.max_waiters_per_class = 3;
        ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
        config.frame_bytes                             = 4096;
        config.max_inline_value                        = 64;
        config.leaf_split_bytes                        = 3000;
        config.page_store                              = device.get();
        config.runtime_binding.limits.io_operations    = 1;
        config.runtime_binding.limits.gap_repair_bytes = 1024 * 1024;
        ASSERT_TRUE(TreeCore::open(config, &core).ok());
    }

    Status prepare(uint64_t slot, std::string key, std::string value, std::shared_ptr<TreeApplyOperation> *out)
    {
        auto input = std::make_shared<TestReplayInput>(std::move(key), std::move(value));
        return core->prepare_replay(
            slot, 1,
            [raw = input.get()](size_t) { return CoreWriteInput{.key = Slice(raw->key), .value = Slice(raw->value)}; },
            input, 0, out);
    }

    Status apply(uint64_t slot, std::string key, std::string value)
    {
        std::shared_ptr<TreeApplyOperation> operation;
        auto                                status = prepare(slot, std::move(key), std::move(value), &operation);
        if (!status.ok()) {
            return status;
        }
        TestReplayCompletion completed;
        operation->start(slot, &TestReplayCompletion::complete, &completed);
        operation.reset();
        completed.wait();
        return completed.status;
    }

    tree_test::TempDir             directory{"tree_replay_"};
    std::unique_ptr<PageDiskStore> device;
    Config                         config;
    std::shared_ptr<TreeCore>      core;
};

TEST_F(TreeCoreReplay, DefaultInlineGeometryPersistsUnderChosenPayloadPressure)
{
    core.reset();
    auto   store = std::make_shared<PageMemStore>(1);
    Config large;
    large.runtime          = config.runtime;
    large.page_store       = store.get();
    large.page_store_owner = store;
    large.frame_bytes      = 65536;
    // Keep the pressure workload fixed as the configurable defaults evolve.
    large.runtime_binding.limits.gap_repair_bytes = 64ULL * 1024 * 1024;
    auto status                                   = TreeCore::open(large, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(16384, 'v');
    for (uint64_t slot = 1; slot <= 4096; ++slot) {
        status = apply(slot, "key" + std::to_string(10000 + slot), value);
        ASSERT_TRUE(status.ok()) << slot << ": " << status.to_string();
    }
    EXPECT_EQ(core->contiguous_slot(), 4096U);
    EXPECT_GT(core->durable_coverage(), 0U);
    for (const auto slot : {1, 1764, 4096}) {
        GetView found;
        status = core->get("key" + std::to_string(10000 + slot), &found);
        ASSERT_TRUE(status.ok()) << status.to_string();
        ASSERT_TRUE(found.found());
        EXPECT_EQ(found.value().to_string(), value);
    }
}

TEST_F(TreeCoreReplay, CoalescedInlineReplaySplitsAndPersistsUnderGapPressure)
{
    core.reset();
    auto   store = std::make_shared<PageMemStore>(1);
    Config large;
    large.runtime                                 = config.runtime;
    large.page_store                              = store.get();
    large.page_store_owner                        = store;
    large.frame_bytes                             = 65536;
    large.runtime_binding.limits.gap_repair_bytes = 1024 * 1024;
    auto status                                   = TreeCore::open(large, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(16384, 'v');
    for (uint64_t slot = 1; slot <= 1024; ++slot) {
        auto input = std::make_shared<std::array<TestReplayInput, 2>>();
        for (size_t index = 0; index < input->size(); ++index) {
            (*input)[index] = {.key = "key" + std::to_string(10000 + (2 * slot) + index), .value = value};
        }
        std::shared_ptr<TreeApplyOperation> operation;
        status = core->prepare_replay(
            slot, input->size(),
            [raw = input.get()](size_t index) {
                return CoreWriteInput{.key = Slice((*raw)[index].key), .value = Slice((*raw)[index].value)};
            },
            input, 0, &operation);
        ASSERT_TRUE(status.ok()) << slot << ": " << status.to_string();
        TestReplayCompletion completed;
        operation->start(slot, &TestReplayCompletion::complete, &completed);
        operation.reset();
        completed.wait();
        ASSERT_TRUE(completed.status.ok()) << slot << ": " << completed.status.to_string();
    }
    EXPECT_EQ(core->contiguous_slot(), 1024U);
    EXPECT_GT(core->durable_coverage(), 0U);
    EXPECT_GT(core->publisher().work_metrics()->counters.snapshot()[static_cast<size_t>(
                  TreeWorkCause::CachePressure)][static_cast<size_t>(TreeWorkCounter::WriteOperations)],
              0U);
    for (const auto index : {10002, 11025, 12049}) {
        GetView found;
        ASSERT_TRUE(core->get("key" + std::to_string(index), &found).ok());
        ASSERT_TRUE(found.found());
        EXPECT_EQ(found.value().to_string(), value);
    }
}

TEST_F(TreeCoreReplay, DistantChosenInputsDoNotConsumeTheGapPayloadReserve)
{
    std::array<std::shared_ptr<TreeApplyOperation>, 3> operations;
    std::array<TestReplayCompletion, 3>                completed;
    const auto                                         before = core->binding()->account()->snapshot();
    for (size_t index = 0; index < operations.size(); ++index) {
        ASSERT_TRUE(prepare(index + 2, "key" + std::to_string(index), std::string(4096, 'v'), &operations[index]).ok());
        operations[index]->start(index + 2, &TestReplayCompletion::complete, &completed[index]);
    }
    core->binding()->lane()->drain();
    const auto waiting = core->binding()->account()->snapshot();
    const auto gap     = static_cast<size_t>(detail::BudgetClass::kGapRepair);
    EXPECT_EQ(waiting.allocated[gap] + waiting.reserved[gap], before.allocated[gap] + before.reserved[gap]);
    for (const auto &result : completed) {
        EXPECT_FALSE(result.done.load(std::memory_order_acquire));
    }
    ASSERT_TRUE(apply(1, "first", "value").ok());
    for (const auto &result : completed) {
        result.wait();
        EXPECT_TRUE(result.status.ok()) << result.status.to_string();
    }
    EXPECT_EQ(core->contiguous_slot(), 4U);
}

TEST_F(TreeCoreReplay, WholeGapGrantWaitsForActualQuotaRelease)
{
    auto pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kGapRepair,
                                                        config.runtime_binding.limits.gap_repair_bytes);
    ASSERT_TRUE(pressure);
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(prepare(1, "key", "value", &operation).ok());
    TestReplayCompletion completed;
    operation->start(1, &TestReplayCompletion::complete, &completed);
    operation.reset();
    core->binding()->lane()->drain();
    EXPECT_FALSE(completed.done.load(std::memory_order_acquire));
    EXPECT_EQ(core->contiguous_slot(), 0U);
    pressure.reset();
    completed.wait();
    EXPECT_TRUE(completed.status.ok()) << completed.status.to_string();
    EXPECT_EQ(core->contiguous_slot(), 1U);
}

TEST_F(TreeCoreReplay, PersistentFlushReleasesGapPayloadWithResourceWaitersAndSingleIo)
{
    // Fill every spare registration after the replay and source rotation paths.
    // Chosen pressure must use its startup-reserved path even when all classes are full.
    std::vector<detail::ResourceBudget::Registration> slots;
    for (auto category : {detail::BudgetClass::kHandoff, detail::BudgetClass::kRouting, detail::BudgetClass::kReclaim,
                          detail::BudgetClass::kGapRepair, detail::BudgetClass::kIo}) {
        while (auto slot = detail::ResourceBudget::try_register(core->binding()->account(), category)) {
            slots.push_back(std::move(*slot));
        }
        EXPECT_FALSE(detail::ResourceBudget::try_register(core->binding()->account(), category));
    }
    ASSERT_FALSE(slots.empty());
    // Only the third write should need a persistent flush to admit its payload.
    const std::string initial_value(192 * 1024, 'v');
    const std::string pressure_value(768 * 1024, 'p');
    ASSERT_TRUE(apply(1, "a", initial_value).ok());
    ASSERT_TRUE(apply(2, "b", initial_value).ok());
    ASSERT_EQ(core->durable_coverage(), 0U);
    const auto status = apply(3, "c", pressure_value);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->contiguous_slot(), 3U);
    EXPECT_EQ(core->local_page_store()->checkpoints->durable_coverage(), 2U);
    EXPECT_GT(core->publisher().work_metrics()->counters.snapshot()[static_cast<size_t>(
                  TreeWorkCause::CachePressure)][static_cast<size_t>(TreeWorkCounter::WriteOperations)],
              0U);
    for (const auto &[key, expected] : std::array{
             std::pair{"a", &initial_value },
             std::pair{"b", &initial_value },
             std::pair{"c", &pressure_value}
    }) {
        GetView result;
        ASSERT_TRUE(core->get(key, &result).ok());
        EXPECT_EQ(result.value().to_string(), *expected);
    }
}

TEST_F(TreeCoreReplay, DuplicateCurrentGapSerializesItsPrepaidWaitPath)
{
    auto pressure = detail::ResourceBudget::try_reserve(core->binding()->account(), detail::BudgetClass::kGapRepair,
                                                        config.runtime_binding.limits.gap_repair_bytes);
    ASSERT_TRUE(pressure);
    std::array<std::shared_ptr<TreeApplyOperation>, 2> operations;
    std::array<TestReplayCompletion, 2>                completed;
    for (size_t index = 0; index < operations.size(); ++index) {
        ASSERT_TRUE(prepare(1, "key", "same-chosen-value", &operations[index]).ok());
        operations[index]->start(1, &TestReplayCompletion::complete, &completed[index]);
    }
    core->binding()->lane()->drain();
    for (const auto &result : completed) {
        EXPECT_FALSE(result.done.load(std::memory_order_acquire));
    }
    pressure.reset();
    for (const auto &result : completed) {
        result.wait();
        EXPECT_TRUE(result.status.ok()) << result.status.to_string();
    }
    EXPECT_EQ(core->contiguous_slot(), 1U);
}

TEST_F(TreeCoreReplay, ProposalRejectsBeforeArmingWhenWaiterRegistrationIsOccupied)
{
    std::shared_ptr<TreeApplyOperation> first;
    std::shared_ptr<TreeApplyOperation> second;
    const CoreWriteReader               empty = [](size_t) { return CoreWriteInput{}; };
    const auto first_status = core->prepare_apply(0, empty, detail::BudgetClass::kAdmittedWriter, 0, &first);
    ASSERT_TRUE(first_status.ok()) << first_status.to_string();
    EXPECT_EQ(core->prepare_apply(0, empty, detail::BudgetClass::kAdmittedWriter, 0, &second).code(),
              Code::kUnavailable);
    EXPECT_EQ(core->contiguous_slot(), 0U);
    first.reset();
    ASSERT_TRUE(core->prepare_apply(0, empty, detail::BudgetClass::kAdmittedWriter, 0, &second).ok());
}

TEST_F(TreeCoreReplay, ReplacementRejectsWithoutClosingTheMissingSlotAdmission)
{
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(prepare(2, "second", "value", &operation).ok());
    TestReplayCompletion completed;
    operation->start(2, &TestReplayCompletion::complete, &completed);
    core->binding()->lane()->drain();
    const auto generation = core->capture()->generation;
    EXPECT_EQ(core->clear().code(), Code::kUnavailable);
    EXPECT_EQ(core->capture()->generation, generation);
    EXPECT_FALSE(completed.done.load(std::memory_order_acquire));
    ASSERT_TRUE(apply(1, "first", "value").ok());
    completed.wait();
    ASSERT_TRUE(completed.status.ok()) << completed.status.to_string();
    core->binding()->lane()->drain();
    EXPECT_EQ(core->contiguous_slot(), 2U);
    ASSERT_TRUE(core->clear().ok());
    EXPECT_EQ(core->capture()->generation, generation + 1);
}

TEST_F(TreeCoreReplay, ReplacementRejectsAnArmedProposalUntilItsOwnerCompletes)
{
    std::shared_ptr<TreeApplyOperation> operation;
    const CoreWriteReader               empty = [](size_t) { return CoreWriteInput{}; };
    ASSERT_TRUE(core->prepare_apply(0, empty, detail::BudgetClass::kAdmittedWriter, 0, &operation).ok());
    ASSERT_TRUE(operation->arm().ok());
    EXPECT_EQ(core->clear().code(), Code::kUnavailable);
    TestReplayCompletion completed;
    operation->start(1, &TestReplayCompletion::complete, &completed);
    completed.wait();
    ASSERT_TRUE(completed.status.ok());
    core->binding()->lane()->drain();
    ASSERT_TRUE(core->clear().ok());
}

TEST_F(TreeCoreReplay, ClosePreservesAcceptedReplayThenJoinsTheRemainingReadTicket)
{
    std::shared_ptr<TreeApplyOperation> replay;
    std::shared_ptr<TreeApplyOperation> later;
    ASSERT_TRUE(prepare(1, "accepted", "value", &replay).ok());
    ASSERT_TRUE(prepare(2, "later", "value", &later).ok());
    TestReplayCompletion later_completed;
    later->start(2, &TestReplayCompletion::complete, &later_completed);
    later.reset();
    core->binding()->lane()->drain();
    EXPECT_FALSE(later_completed.done.load(std::memory_order_acquire));
    std::shared_ptr<TreeOperationLifetime::Ticket> read;
    ASSERT_TRUE(core->operation_lifetime().acquire(core->binding()->lifecycle_budget(), &read).ok());
    std::atomic<bool> closed{false};
    std::jthread      closer([&] {
        core->close_and_drain();
        closed.store(true, std::memory_order_release);
    });
    while (!core->closed()) {
        std::this_thread::yield();
    }
    EXPECT_FALSE(closed.load(std::memory_order_acquire));
    std::shared_ptr<TreeApplyOperation> rejected;
    EXPECT_EQ(prepare(3, "rejected", "value", &rejected).code(), Code::kUnavailable);

    TestReplayCompletion completed;
    replay->start(1, &TestReplayCompletion::complete, &completed);
    replay.reset();
    completed.wait();
    EXPECT_TRUE(completed.status.ok()) << completed.status.to_string();
    later_completed.wait();
    EXPECT_TRUE(later_completed.status.ok()) << later_completed.status.to_string();
    EXPECT_FALSE(closed.load(std::memory_order_acquire));
    read.reset();
    closer.join();
    EXPECT_TRUE(closed.load(std::memory_order_acquire));
    EXPECT_EQ(core->contiguous_slot(), 2U);
}

TEST_F(TreeCoreReplay, CloseCompletesReplayWaitingForAnUnacceptedSlot)
{
    std::shared_ptr<TreeApplyOperation> later;
    ASSERT_TRUE(prepare(2, "later", "value", &later).ok());
    TestReplayCompletion replay;
    later->start(2, &TestReplayCompletion::complete, &replay);
    later.reset();
    core->binding()->lane()->drain();
    EXPECT_FALSE(replay.done.load(std::memory_order_acquire));

    TestReplayCompletion closed;
    ASSERT_TRUE(core->close_async({.context = &closed, .complete_fn = &TestReplayCompletion::close}));
    replay.wait();
    closed.wait();
    EXPECT_EQ(replay.status.code(), Code::kUnavailable);
    EXPECT_TRUE(closed.status.ok());
    EXPECT_EQ(core->contiguous_slot(), 0U);
}

TEST_F(TreeCoreReplay, DroppingUnstartedPredecessorReleasesReplayDuringClose)
{
    std::shared_ptr<TreeApplyOperation> predecessor;
    std::shared_ptr<TreeApplyOperation> later;
    ASSERT_TRUE(prepare(1, "predecessor", "value", &predecessor).ok());
    ASSERT_TRUE(prepare(2, "later", "value", &later).ok());
    TestReplayCompletion replay;
    later->start(2, &TestReplayCompletion::complete, &replay);
    later.reset();

    TestReplayCompletion closed;
    ASSERT_TRUE(core->close_async({.context = &closed, .complete_fn = &TestReplayCompletion::close}));
    core->binding()->lane()->drain();
    EXPECT_FALSE(replay.done.load(std::memory_order_acquire));
    predecessor.reset();

    replay.wait();
    closed.wait();
    EXPECT_EQ(replay.status.code(), Code::kUnavailable);
    EXPECT_TRUE(closed.status.ok());
}
} // namespace
} // namespace crowdb::tree
