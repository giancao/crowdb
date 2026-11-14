// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/scheduled_executor.h"
#include "crowdb-rpc/transport/socket_transport.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::rpc
{
TEST(ScheduledExecutorTest, CallbackRegistrationDeterminesTheNextDeadline)
{
    ScheduledExecutor executor;
    size_t            fired = 0;
    executor.schedule(
        [&] {
            ++fired;
            executor.schedule([&] { ++fired; }, 1000);
        },
        0);
    const auto next = executor.run_due_tasks();
    EXPECT_EQ(fired, 1U);
    EXPECT_GT(next, 0);
    EXPECT_LE(next, 1000);
    EXPECT_EQ(executor.pending_count(), 1U);
    executor.close();
}

TEST(ScheduledExecutorTest, CancellationAndContextDestructionCanReenter)
{
    ScheduledExecutor executor;
    size_t            cancelled = 0;
    size_t            destroyed = 0;
    auto              owner     = std::shared_ptr<int>(new int, [&](int *value) {
        delete value;
        ++destroyed;
        EXPECT_EQ(executor.pending_count(), 1U);
    });
    const auto        id        = executor.schedule([owner] {}, 1000,
                                                    [&] {
                                          ++cancelled;
                                          EXPECT_NE(executor.schedule([] {}, 1000), 0U);
                                                    });
    owner.reset();
    EXPECT_TRUE(executor.cancel(id));
    EXPECT_FALSE(executor.cancel(id));
    EXPECT_EQ(cancelled, 1U);
    EXPECT_EQ(destroyed, 1U);
    executor.close();
}

TEST(ScheduledExecutorTest, CloseCancelsOnceAndRejectsNewTimers)
{
    ScheduledExecutor executor;
    size_t            cancelled = 0;
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_NE(executor.schedule([] { ADD_FAILURE() << "closed timer fired"; }, 1000,
                                    [&] {
                                        ++cancelled;
                                        EXPECT_EQ(executor.schedule([] {}, 0), 0U);
                                    }),
                  0U);
    }
    executor.close();
    executor.close();
    EXPECT_EQ(cancelled, 32U);
    EXPECT_EQ(executor.pending_count(), 0U);
    EXPECT_EQ(executor.run_due_tasks(), -1);
}

TEST(SocketTimer, CrossThreadEarlierDeadlineRearmsTheExistingEngine)
{
    SocketTransport transport(1, 4, nullptr);
    transport.start();
    auto       late        = std::make_shared<std::promise<bool>>();
    auto       late_result = late->get_future();
    const auto later = transport.schedule([late] { late->set_value(true); }, 1000, [late] { late->set_value(false); });
    ASSERT_NE(later, 0U);
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    auto early        = std::make_shared<std::promise<std::thread::id>>();
    auto early_result = early->get_future();
    ASSERT_NE(transport.schedule([early] { early->set_value(std::this_thread::get_id()); }, 10), 0U);
    ASSERT_EQ(early_result.wait_for(std::chrono::milliseconds(500)), std::future_status::ready);
    EXPECT_NE(early_result.get(), std::this_thread::get_id());
    EXPECT_TRUE(transport.cancel_scheduled(later));
    EXPECT_FALSE(late_result.get());
    transport.stop();
}

TEST(SocketTimer, CallbackCanScheduleItsSuccessorWithoutOtherIo)
{
    SocketTransport transport(2, 4, nullptr);
    transport.start();
    auto complete = std::make_shared<std::promise<bool>>();
    auto result   = complete->get_future();
    ASSERT_NE(transport.schedule(
                  [&transport, complete] {
                      EXPECT_NE(transport.schedule([complete] { complete->set_value(true); }, 10,
                                                   [complete] { complete->set_value(false); }),
                                0U);
                  },
                  0),
              0U);
    ASSERT_EQ(result.wait_for(std::chrono::milliseconds(500)), std::future_status::ready);
    EXPECT_TRUE(result.get());
    transport.stop();
}

TEST(SocketTimer, StopCancelsQueuedContextsAndRejectsNewTimers)
{
    SocketTransport transport(1, nullptr);
    transport.start();
    auto complete = std::make_shared<std::promise<bool>>();
    auto result   = complete->get_future();
    ASSERT_NE(transport.schedule([complete] { complete->set_value(true); }, 60000,
                                 [complete] { complete->set_value(false); }),
              0U);
    transport.stop();
    ASSERT_EQ(result.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
    EXPECT_FALSE(result.get());
    EXPECT_EQ(transport.schedule([] {}, 0), 0U);
}

TEST(SocketTimer, CAbiDeliversCancellationOnce)
{
    const auto server = crowdb_rpc_server_create(nullptr);
    ASSERT_NE(server, nullptr);
    crowdb_rpc_server_start(server);
    std::promise<bool> complete;
    auto               result = complete.get_future();
    const auto         id     = crowdb_rpc_server_schedule(
        server, 60000,
        [](void *context, int fired) { static_cast<std::promise<bool> *>(context)->set_value(fired != 0); }, &complete);
    EXPECT_NE(id, 0U);
    EXPECT_TRUE(crowdb_rpc_server_cancel_scheduled(server, id));
    EXPECT_FALSE(crowdb_rpc_server_cancel_scheduled(server, id));
    crowdb_rpc_server_destroy(server);
    ASSERT_EQ(result.wait_for(std::chrono::milliseconds(100)), std::future_status::ready);
    EXPECT_FALSE(result.get());
}
} // namespace crowdb::rpc
