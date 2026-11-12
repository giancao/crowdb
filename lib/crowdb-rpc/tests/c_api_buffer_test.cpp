// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "../src/transport/rdma/rdma_control_stream.h"
#include "crowdb-rpc/c_api.h"
#include "crowdb-rpc/c_api_internal.h"
#include "crowdb-rpc/connection.h"

#include <gtest/gtest.h>

#include <cstring>

TEST(CApiBufferTest, ClonedHandlesReleaseIndependently)
{
    const uint8_t bytes[]  = {4, 8, 16};
    auto          original = crowdb_rpc_buffer_create(bytes, sizeof(bytes));
    ASSERT_NE(original, nullptr);
    auto clone = crowdb_rpc_buffer_ref(original);
    ASSERT_NE(clone, nullptr);
    ASSERT_NE(original, clone);
    EXPECT_EQ(crowdb_rpc_buffer_data(original), crowdb_rpc_buffer_data(clone));
    crowdb_rpc_buffer_release(original);
    EXPECT_EQ(crowdb_rpc_buffer_len(clone), sizeof(bytes));
    EXPECT_EQ(std::memcmp(crowdb_rpc_buffer_data(clone), bytes, sizeof(bytes)), 0);
    crowdb_rpc_buffer_release(clone);
}

TEST(CApiBufferTest, ExternalOwnerReleasesOnlyAfterLastIndependentHandle)
{
    uint32_t      releases = 0;
    const uint8_t bytes[]  = {3, 5, 7};
    auto          original = crowdb_rpc_buffer_create_external(
        bytes, sizeof(bytes), [](void *context) { ++*static_cast<uint32_t *>(context); }, &releases);
    ASSERT_NE(original, nullptr);
    auto clone = crowdb_rpc_buffer_ref(original);
    ASSERT_NE(original, clone);
    crowdb_rpc_buffer_release(clone);
    EXPECT_EQ(releases, 0U);
    EXPECT_EQ(std::memcmp(crowdb_rpc_buffer_data(original), bytes, sizeof(bytes)), 0);
    crowdb_rpc_buffer_release(original);
    EXPECT_EQ(releases, 1U);
}

TEST(CApiBufferTest, LogicalControlRetainsAdmissionAndOriginalStorageThroughClonedHandles)
{
    crowdb::rpc::ControlStreamBudget budget(32);
    auto                             frame = std::make_unique<crowdb::rpc::Frame>();
    frame->control.resize(32, 0x5a);
    frame->control_owner         = budget.try_reserve(frame->control.size());
    auto               *original = frame->control.data();
    crowdb_rpc_buffer_t control  = nullptr;
    crowdb_rpc_buffer_t data     = nullptr;
    crowdb::rpc::frame_to_c_handles(frame.release(), &control, &data);
    ASSERT_NE(control, nullptr);
    EXPECT_EQ(data, nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_data(control), original);
    EXPECT_EQ(crowdb_rpc_buffer_len(control), 32U);
    EXPECT_EQ(budget.used(), 32U);
    auto clone = crowdb_rpc_buffer_ref(control);
    ASSERT_NE(clone, nullptr);
    crowdb_rpc_buffer_release(control);
    EXPECT_EQ(budget.used(), 32U);
    EXPECT_EQ(crowdb_rpc_buffer_data(clone)[31], 0x5a);
    crowdb_rpc_buffer_release(clone);
    EXPECT_EQ(budget.used(), 0U);
}

TEST(CApiBufferTest, TakingRequestControlPreservesBytesAndReleasesConnectionWithFrame)
{
    auto frame = std::make_unique<crowdb::rpc::Frame>();
    frame->control.resize(16 * 1024, 0x5a);
    frame->control.reserve(32 * 1024);
    const auto capacity     = frame->control.capacity();
    frame->connection_owner = std::make_shared<crowdb::rpc::Connection>(1, "test", nullptr, 1 << 20, 8);
    std::weak_ptr<crowdb::rpc::Connection> connection = frame->connection_owner;
    const auto                            *original   = frame->control.data();

    auto *control = crowdb_rpc_frame_take_control(frame.get());
    ASSERT_NE(control, nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_data(control), original);
    EXPECT_EQ(crowdb_rpc_buffer_len(control), 16U * 1024);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_capacity(control), capacity);
    EXPECT_EQ(crowdb_rpc_frame_take_control(frame.get()), nullptr);
    EXPECT_FALSE(connection.expired());
    frame.reset();
    EXPECT_TRUE(connection.expired());
    EXPECT_EQ(crowdb_rpc_buffer_data(control)[(16 * 1024) - 1], 0x5a);
    crowdb_rpc_buffer_release(control);
}

TEST(CApiBufferTest, TakenControlWithReceiveCreditHasNoTransferableAllocation)
{
    crowdb::rpc::Frame frame;
    frame.control.resize(4096, 0x5a);
    frame.control_owner = std::make_shared<int>(1);
    auto *control       = crowdb_rpc_frame_take_control(&frame);
    ASSERT_NE(control, nullptr);
    EXPECT_EQ(crowdb_rpc_buffer_allocation_capacity(control), 0U);
    crowdb_rpc_buffer_release(control);
}
