// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_apply_operation.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"

#include <gtest/gtest.h>

#include <future>
#include <semaphore>

namespace crowdb::tree
{
namespace
{
class TreeApplyGrant : public testing::Test
{
  protected:
    void SetUp() override
    {
        ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.runtime_binding.limits.gap_repair_bytes = 16ULL * 1024 * 1024;
        tree                                           = std::make_unique<Crowdbtree>(config);
        core                                           = TreeCoreAccess::get(*tree);
    }

    Config                      config;
    std::unique_ptr<Crowdbtree> tree;
    TreeCore                   *core = nullptr;
};

TEST_F(TreeApplyGrant, CompleteGapBoundChargesTheFinalCellOnce)
{
    const std::string                   fitting(8 * 1024 * 1024, 'v');
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(core->prepare_apply(
                        1, [&](size_t) { return CoreWriteInput{.key = Slice("key"), .value = Slice(fitting)}; },
                        detail::BudgetClass::kAdmittedWriter, 0, &operation)
                    .ok());
    operation.reset();

    const std::string value(16 * 1024 * 1024, 'v');
    const auto        before = core->binding()->account()->snapshot();
    const auto        status = core->prepare_apply(
        1, [&](size_t) { return CoreWriteInput{.key = Slice("key"), .value = Slice(value)}; },
        detail::BudgetClass::kAdmittedWriter, 0, &operation);
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    const auto after = core->binding()->account()->snapshot();
    EXPECT_EQ(after.reserved, before.reserved);
    EXPECT_EQ(after.allocated, before.allocated);
    EXPECT_EQ(core->contiguous_slot(), 0U);
}

TEST_F(TreeApplyGrant, RetainedCallerControlsArePartOfTheSameCompleteBound)
{
    std::shared_ptr<TreeApplyOperation> operation;
    EXPECT_EQ(core->prepare_apply(0, {}, detail::BudgetClass::kAdmittedWriter,
                                  config.runtime_binding.limits.gap_repair_bytes, &operation)
                  .code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(operation);
    EXPECT_TRUE(core->prepare_apply(0, {}, detail::BudgetClass::kAdmittedWriter, 256, &operation).ok());
    EXPECT_TRUE(operation);
}

TEST_F(TreeApplyGrant, PermanentOrdinaryLimitDoesNotBecomeAnUnfinishableWait)
{
    Config smaller                              = config;
    smaller.runtime_binding.limits.writer_bytes = 1024 * 1024;
    Crowdbtree                          local(smaller);
    const std::string                   value(1024 * 1024, 'v');
    std::shared_ptr<TreeApplyOperation> operation;
    EXPECT_EQ(TreeCoreAccess::get(local)
                  ->prepare_apply(
                      1, [&](size_t) { return CoreWriteInput{.key = Slice("key"), .value = Slice(value)}; },
                      detail::BudgetClass::kAdmittedWriter, 0, &operation)
                  .code(),
              Code::kInvalidArgument);
    EXPECT_FALSE(operation);
}

TEST_F(TreeApplyGrant, DuplicateKeysKeepLastCell)
{
    const std::array<std::string, 3>    values{"first", "second", "last"};
    std::shared_ptr<TreeApplyOperation> operation;
    ASSERT_TRUE(
        core->prepare_apply(
                values.size(),
                [&](size_t index) { return CoreWriteInput{.key = Slice("same"), .value = Slice(values[index])}; },
                detail::BudgetClass::kAdmittedWriter, 0, &operation)
            .ok());
    ASSERT_TRUE(operation->arm().ok());

    struct Completion
    {
        std::atomic<bool> done{false};
        Status            status;
    } completion;

    operation->start(
        1,
        [](void *raw, detail::CallbackSignal, Status status) noexcept {
            auto &value  = *static_cast<Completion *>(raw);
            value.status = std::move(status);
            value.done.store(true, std::memory_order_release);
            value.done.notify_all();
        },
        &completion);
    while (!completion.done.load(std::memory_order_acquire)) {
        completion.done.wait(false, std::memory_order_acquire);
    }
    EXPECT_TRUE(completion.status.ok());
    EXPECT_EQ(tree->get_view("same").value().to_string(), "last");
}

#ifdef CROWDB_TREE_TEST_UTIL
TEST_F(TreeApplyGrant, IndependentReadyBatchesRunOnDifferentWorkers)
{
    std::array<std::shared_ptr<TreeApplyOperation>, 2> operations;
    for (size_t i = 0; i < operations.size(); ++i) {
        const auto key = i == 0 ? Slice("first") : Slice("second");
        ASSERT_TRUE(core->prepare_apply(
                            1, [key](size_t) { return CoreWriteInput{.key = key, .value = Slice("value")}; },
                            detail::BudgetClass::kAdmittedWriter, 0, &operations[i])
                        .ok());
        ASSERT_TRUE(operations[i]->arm().ok());
    }

    struct Pause
    {
        std::promise<void>    entered;
        std::binary_semaphore resume{0};
    } pause;

    auto entered = pause.entered.get_future();
    auto table   = core->capture()->sources->tables.back();
    table->set_hook_for_tests(&pause, [](void *raw, ConcurrentSkipList::PausePoint point, Slice key) {
        if (point == ConcurrentSkipList::PausePoint::kBeforePrepare && key == Slice("first")) {
            auto &pause = *static_cast<Pause *>(raw);
            pause.entered.set_value();
            pause.resume.acquire();
        }
    });
    std::array<std::promise<Status>, 2> completion;
    auto                                first  = completion[0].get_future();
    auto                                second = completion[1].get_future();
    const auto                          done   = +[](void *raw, detail::CallbackSignal, Status status) noexcept {
        static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
    };
    operations[0]->start(1, done, completion.data());
    const auto first_entered = entered.wait_for(std::chrono::seconds(2));
    operations[1]->start(2, done, &completion[1]);
    const auto second_ready = second.wait_for(std::chrono::seconds(2));
    const auto prefix       = core->contiguous_slot();
    pause.resume.release();
    EXPECT_TRUE(first.get().ok());
    EXPECT_TRUE(second.get().ok());
    table->set_hook_for_tests(nullptr, nullptr);

    EXPECT_EQ(first_entered, std::future_status::ready);
    EXPECT_EQ(second_ready, std::future_status::ready);
    EXPECT_EQ(prefix, 0U);
    EXPECT_EQ(core->contiguous_slot(), 2U);
}
#endif
} // namespace
} // namespace crowdb::tree
