// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "common/test_tree_core_chunk.h"
#include "crowdb-tree/crowdb-tree.h"
#include "page_store/local/scheduled_page_store.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <future>
#include <memory>
#include <string>
#include <utility>

namespace crowdb::tree::detail
{
namespace
{

Status snapshot(Crowdbtree &tree, bool asynchronous)
{
    if (!asynchronous) {
        return tree.flush_with_persistent();
    }
    Status            result;
    std::atomic<bool> done{false};
    tree.flush_with_persistent_async([&](Status status, uint64_t /*unused*/) {
        result = std::move(status);
        done.store(true, std::memory_order_release);
        done.notify_one();
    });
    done.wait(false, std::memory_order_acquire);
    return result;
}

class TestFailingMirrorTransport final : public test_chunk::TestRootTransport
{
  public:
    using TestRootTransport::TestRootTransport;
    bool fail_mirror = false;

    Status write_fixed_mirror(ChunkId id, uint32_t mirror, uint64_t offset, const uint8_t *data, size_t length,
                              ChunkIoContext context) override
    {
        return fail_mirror && mirror == 1
                 ? Status::unavailable()
                 : TestRootTransport::write_fixed_mirror(id, mirror, offset, data, length, context);
    }
};

void check_snapshot_retry(bool asynchronous, bool write_while_unavailable = false)
{
    test_chunk::TestCoreChunk test;
    auto                      transport = std::make_shared<TestFailingMirrorTransport>(test.backend.catalog);
    test.backend.transport              = transport;
    test.configure_store(0);
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "old").ok());
    ASSERT_TRUE(snapshot(*test.tree, asynchronous).ok());
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    const auto prior        = test.backend.catalog->current_generation;
    auto       old_snapshot = test.tree->snapshot_view();

    ASSERT_TRUE(test.put(2, "new").ok());
    ASSERT_TRUE(test.tree->flush().ok());
    transport->fail_mirror = true;
    const auto failed      = snapshot(*test.tree, asynchronous);
    ASSERT_EQ(failed.code(), Code::kUnavailable) << failed.to_string();
    ASSERT_EQ(test.backend.catalog->current_generation, prior);
    std::string value;
    uint64_t    slot = 0;
    ASSERT_TRUE(old_snapshot->get(Slice("key"), &slot, &value));
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(value, "old");
    if (write_while_unavailable) {
        ASSERT_TRUE(test.put(3, "newest").ok());
        EXPECT_EQ(test.tree->flush().code(), Code::kUnavailable);
    }
    const auto still_unavailable = snapshot(*test.tree, asynchronous);
    ASSERT_EQ(still_unavailable.code(), Code::kUnavailable) << still_unavailable.to_string();
    ASSERT_EQ(test.backend.catalog->current_generation, prior);
    transport->fail_mirror = false;

    const auto retried = snapshot(*test.tree, asynchronous);
    ASSERT_TRUE(retried.ok()) << retried.to_string();
    ASSERT_GT(test.backend.catalog->current_generation, prior);
    ASSERT_TRUE(old_snapshot->get(Slice("key"), &slot, &value));
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(value, "old");
    old_snapshot.reset();
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->get(Slice("key"), &slot, &value));
    EXPECT_EQ(slot, write_while_unavailable ? 3U : 2U);
    EXPECT_EQ(value, write_while_unavailable ? "newest" : "new");
}

TEST(ChunkSnapshotRetry, SmallFramesCheckpointLargeValues)
{
    test_chunk::TestCoreChunk             test;
    std::shared_ptr<ChunkPinReleaseQueue> releases;
    ASSERT_TRUE(ChunkPinReleaseQueue::create(
                    test.backend.credits.credits.reservation(), 1024, std::make_shared<int>(1), [](void *) noexcept {},
                    nullptr, &releases)
                    .ok());
    test.backend.catalog->queue = std::move(releases);
    ASSERT_TRUE(test.open().ok());
    for (uint64_t slot = 1; slot <= 32; ++slot) {
        ASSERT_TRUE(test.tree
                        ->apply(slot, Batch{.ops = {{.key   = std::to_string(slot),
                                                     .kind  = OpKind::kPut,
                                                     .value = std::string(64 * 1024, 'v')}}})
                        .ok());
    }
    ASSERT_TRUE(test.tree->flush().ok());
    const auto status = test.tree->flush_with_persistent();
    const auto pins   = test.backend.catalog->queue->snapshot();
    ASSERT_TRUE(status.ok()) << status.to_string() << " registered=" << pins.registered << " pending=" << pins.pending;
    EXPECT_GE(test.tree->last_snapshot_pages_written(), 512U);
    EXPECT_EQ(test.tree->durable_coverage(), 32U);
    test.tree.reset();
    test.config.page_store_owner.reset();
    test.store.reset();
    const auto released = test.backend.catalog->queue->pump(test.backend.catalog->queue->snapshot().pending);
    ASSERT_EQ(released.failed, 0U);
    ASSERT_TRUE(test.open().ok());
    for (uint64_t slot = 1; slot <= 32; ++slot) {
        const auto value = test.tree->get_view(std::to_string(slot));
        ASSERT_TRUE(value.found());
        EXPECT_EQ(value.slot(), slot);
        EXPECT_EQ(value.value().to_string(), std::string(64 * 1024, 'v'));
    }
}

TEST(ChunkSnapshotRetry, SingleLeafSubmitsAnotherOverflowPackBeforeFirstCompletes)
{
    std::promise<ChunkTransportCompletion> first_pack;
    auto                                   held = first_pack.get_future();
    std::promise<void>                     second_pack;
    auto                                   second_started = second_pack.get_future();
    std::atomic<size_t>                    allocations{0};
    std::promise<Status>                   flushed;
    auto                                   result = flushed.get_future();
    test_chunk::TestCoreChunk              test;
    ASSERT_TRUE(test.open().ok());
    const std::string value(64 * 1024, 'v');
    ASSERT_TRUE(test.put(1, value).ok());
    test.backend.transport->after_allocate_async = [&](ChunkTransportCompletion completion) {
        const auto index = allocations.fetch_add(1);
        if (index == 0) {
            first_pack.set_value(std::move(completion));
            return true;
        }
        if (index == 1) {
            second_pack.set_value();
        }
        return false;
    };

    test.tree->flush_with_persistent_async(
        [&](Status status, uint64_t /*unused*/) { flushed.set_value(std::move(status)); });
    ASSERT_EQ(held.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(second_started.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(test.tree->durable_coverage(), 0U);
    auto  receipt = std::make_shared<ChunkTransportCompletion>(held.get());
    auto *backend = static_cast<ScheduledPageStore *>(TreeCoreAccess::get(*test.tree)->async_page_store());
    ASSERT_TRUE(backend
                    ->submit_step(
                        [receipt] {
                            receipt->complete(Status::Ok());
                            return Status::Ok();
                        },
                        {.complete_fn = +[](void *, Status status) { EXPECT_TRUE(status.ok()); }})
                    .ok());
    ASSERT_EQ(result.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    const auto status = result.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(test.tree->durable_coverage(), 1U);
    const auto read = test.tree->get_view("key");
    ASSERT_TRUE(read.found());
    EXPECT_EQ(read.value().to_string(), value);
}

TEST(ChunkSnapshotRetry, PublicationFailureDoesNotPoisonSynchronousSnapshotRetry)
{
    check_snapshot_retry(false);
}

TEST(ChunkSnapshotRetry, PublicationFailureDoesNotPoisonAsynchronousSnapshotRetry)
{
    check_snapshot_retry(true);
}

TEST(ChunkSnapshotRetry, SynchronousRetryPreservesWritesAfterFailedPublication)
{
    check_snapshot_retry(false, true);
}

TEST(ChunkSnapshotRetry, AsynchronousRetryPreservesWritesAfterFailedPublication)
{
    check_snapshot_retry(true, true);
}

} // namespace
} // namespace crowdb::tree::detail
