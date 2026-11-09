// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/flush_controller_test.h"
#include "flush_preparation_test_support.h"

#include <future>
#include <latch>
#include <thread>

namespace crowdb::tree
{
namespace
{
TEST(FlushControllerClose, PendingReleaseDefersToTheActivePreparerWithoutWaiting)
{
    TestPreparedFlushTree           tree;
    std::shared_ptr<FlushOperation> previous;
    ASSERT_TRUE(tree.controller->prepare(tree.preparation, &previous).ok());
    std::weak_ptr<FlushOperation> retained = previous;
    previous.reset();
    ASSERT_FALSE(retained.expired());

    struct Paused
    {
        std::promise<void> entered;
        std::latch         resume{1};

        static void hook(void *context, bool acquired) noexcept
        {
            if (acquired) {
                auto &self = *static_cast<Paused *>(context);
                self.entered.set_value();
                self.resume.wait();
            }
        }
    } paused;

    auto                            entered = paused.entered.get_future();
    Status                          result;
    std::shared_ptr<FlushOperation> prepared;
    std::jthread                    worker([&] {
        set_flush_prepare_hook_for_tests(&Paused::hook, &paused);
        result = tree.controller->prepare(tree.preparation, &prepared);
        set_flush_prepare_hook_for_tests(nullptr, nullptr);
    });
    EXPECT_EQ(entered.wait_for(std::chrono::seconds(1)), std::future_status::ready);
    tree.controller->release_pending();
    EXPECT_FALSE(retained.expired());
    paused.resume.count_down();
    worker.join();
    EXPECT_TRUE(result.ok()) << result.to_string();
    EXPECT_TRUE(prepared);
    EXPECT_TRUE(retained.expired());
    // The last preparer must not retain a new controller reference either.
    std::weak_ptr<FlushOperation> finished = prepared;
    prepared.reset();
    EXPECT_TRUE(finished.expired());
}
} // namespace
} // namespace crowdb::tree
