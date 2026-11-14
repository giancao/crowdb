// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_root_catalog.h"
#include "load_budget_test_support.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
class TestAsyncRelease final : public test_chunk::TestRootCatalog
{
  public:
    using TestRootCatalog::TestRootCatalog;
    std::vector<AsyncCompletion> replies;
    std::vector<ChunkStageId>    identities;

    void submit_change_pin(uint64_t tree, ChunkStageId stage, ChunkStageId pin, bool acquire,
                           AsyncCompletion done) override
    {
        EXPECT_EQ(tree, 42U);
        EXPECT_FALSE(acquire);
        EXPECT_EQ(stage, pin);
        identities.push_back(pin);
        replies.push_back(std::move(done));
    }

    void submit_writer_quiesced(uint64_t tree, uint64_t epoch, AsyncCompletion done) override
    {
        EXPECT_EQ(tree, 42U);
        identities.push_back({.owner_epoch = epoch});
        replies.push_back(std::move(done));
    }
};
} // namespace

TEST(ChunkPinReleaseAsync, SuspendedUnpinAndWriterReleaseDoNotOccupyThePumpAndRetryKeepsIdentity)
{
    test_read::LoadCredits credits;
    auto                   catalog = std::make_shared<TestAsyncRelease>(credits.credits.reservation());
    auto                   wakes   = std::make_shared<size_t>(0);
    ASSERT_TRUE(ChunkPinReleaseQueue::create(
                    credits.credits.reservation(), 2, wakes, +[](void *raw) noexcept { ++*static_cast<size_t *>(raw); },
                    wakes.get(), &catalog->queue)
                    .ok());
    auto                           queue = catalog->queue;
    const ChunkStageId             stage{.owner_epoch = 7, .operation_high = 8, .operation_low = 9};
    std::shared_ptr<ChunkPinOwner> pin;
    std::shared_ptr<ChunkPinOwner> writer;
    ASSERT_TRUE(queue->prepare(credits.credits.reservation(), catalog, 42, stage, stage, &pin).ok());
    ASSERT_TRUE(queue->prepare_writer(credits.credits.reservation(), catalog, 42, 7, &writer).ok());
    pin->arm();
    writer->arm();
    pin.reset();
    writer.reset();
    queue->stop_admission();
    const auto first = queue->pump(8);
    ASSERT_EQ(catalog->replies.size(), 2U);
    EXPECT_EQ(first.completed, 0U);
    EXPECT_FALSE(first.more);
    EXPECT_EQ(queue->snapshot().registered, 2U);
    EXPECT_FALSE(queue->drained());
    const auto wake_count = *wakes;
    catalog->replies[0].complete(Status::unavailable());
    EXPECT_GT(*wakes, wake_count);
    const auto failed = queue->pump(0);
    EXPECT_EQ(failed.failed, 1U);
    EXPECT_TRUE(failed.more);
    catalog->replies[1].complete(Status::Ok());
    const auto retry = queue->pump(8);
    EXPECT_EQ(retry.completed, 1U);
    EXPECT_FALSE(retry.more);
    ASSERT_EQ(catalog->replies.size(), 3U);
    EXPECT_EQ(catalog->identities[0], catalog->identities[2]);
    EXPECT_EQ(queue->snapshot().registered, 1U);
    catalog->replies[2].complete(Status::Ok());
    EXPECT_EQ(queue->pump(8).completed, 1U);
    EXPECT_TRUE(queue->drained());
    EXPECT_EQ(queue->snapshot().pending, 0U);
}
} // namespace crowdb::tree::detail
