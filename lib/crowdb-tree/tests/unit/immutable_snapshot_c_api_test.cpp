// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "c_api_internal.h"
#include "crowdb-tree/read_only_view_c_api.h"
#include "local_checkpoint_test_support.h"
#include "runtime/tree_runtime_c_api.h"

#include <gtest/gtest.h>

#include <future>
#include <utility>

namespace crowdb::tree
{
namespace
{
using ReadResult = std::pair<ct_status, ct_future *>;

void read_completed(void *context, ct_status status, ct_future *result)
{
    static_cast<std::promise<ReadResult> *>(context)->set_value({status, result});
}

struct TestSnapshotTree
{
    ct_runtime        *runtime  = nullptr;
    ct_tree           *tree     = nullptr;
    ct_read_only_view *snapshot = nullptr;

    TestSnapshotTree()
    {
        EXPECT_EQ(ct_runtime_create(nullptr, &runtime), 0);
        ct_options options{};
        EXPECT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    }

    ~TestSnapshotTree()
    {
        ct_read_only_view_free(snapshot);
        ct_close(tree);
        ct_runtime_free(runtime);
    }

    void put(uint64_t slot, const std::string &key, const std::string &value) const
    {
        ASSERT_EQ(ct_apply_put(tree, slot, reinterpret_cast<const uint8_t *>(key.data()), key.size(),
                               reinterpret_cast<const uint8_t *>(value.data()), value.size()),
                  0);
    }

    void scan(const ct_scan_options_v2 &options, ct_scan_result **out) const
    {
        std::promise<ReadResult> completed;
        auto                     received = completed.get_future();
        ASSERT_EQ(ct_read_only_view_scan_async(snapshot, &options, read_completed, &completed), 0);
        ASSERT_EQ(received.wait_for(std::chrono::seconds(3)), std::future_status::ready);
        auto [status, future] = received.get();
        ASSERT_EQ(status, 0);
        int32_t done = 0;
        ASSERT_EQ(ct_future_poll_scan_v2(future, &done, out), 0);
        ASSERT_EQ(done, 1);
    }
};
} // namespace

TEST(ImmutableSnapshotCAbi, FixedViewSurvivesTreeCloseAndResultsSurviveRuntimeClose)
{
    TestSnapshotTree test;
    test.put(1, "", "empty");
    test.put(2, "key", "old");
    ASSERT_EQ(ct_flush(test.tree), 0);
    ASSERT_EQ(ct_read_only_view_capture(test.tree, 0, 2, &test.snapshot), 0);
    test.put(3, "key", "new");
    ASSERT_EQ(ct_flush(test.tree), 0);
    ct_close(std::exchange(test.tree, nullptr));

    std::promise<ReadResult> completed;
    auto                     received = completed.get_future();
    const uint8_t            key[]    = {'k', 'e', 'y'};
    ASSERT_EQ(ct_read_only_view_get_async(test.snapshot, key, sizeof(key), read_completed, &completed), 0);
    ASSERT_EQ(received.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto [status, get] = received.get();
    ASSERT_EQ(status, 0);
    int32_t  done  = 0;
    int32_t  found = 0;
    uint64_t slot  = 0;
    ct_buf   bytes{};
    ASSERT_EQ(ct_future_poll(get, &done, &found, &slot, &bytes), 0);
    ASSERT_EQ(done, 1);
    ASSERT_EQ(found, 1);
    EXPECT_EQ(slot, 2U);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(bytes.data), bytes.len), "old");
    ct_scan_options_v2 scan{};
    scan.struct_size       = sizeof(scan);
    scan.has_start_bound   = 1;
    scan.start_inclusive   = 1;
    scan.limit             = 1;
    ct_scan_result *result = nullptr;
    ASSERT_NO_FATAL_FAILURE(test.scan(scan, &result));
    ct_read_only_view_free(std::exchange(test.snapshot, nullptr));
    ct_runtime_drain_cpu(test.runtime);
    ct_runtime_free(std::exchange(test.runtime, nullptr));
    const uint8_t *packed    = nullptr;
    size_t         len       = 0;
    uint64_t       count     = 0;
    int32_t        truncated = 0;
    ASSERT_EQ(ct_scan_result_data(result, &packed, &len, &count, &truncated), 0);
    EXPECT_EQ(count, 1U);
    EXPECT_EQ(truncated, 1);
    ASSERT_GE(len, 5U);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(packed + len - 5), 5), "empty");
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(bytes.data), bytes.len), "old");
    ct_scan_result_free(result);
    ct_future_free(get);
}

TEST(ImmutableSnapshotCAbi, CaptureRejectsWrongCoverageAndStrictScanTruncatesOversizedFirstEntry)
{
    TestSnapshotTree test;
    test.put(1, "key", "value");
    ASSERT_EQ(ct_flush(test.tree), 0);
    EXPECT_EQ(ct_read_only_view_capture(test.tree, 0, 2, &test.snapshot), static_cast<ct_status>(Code::kUnavailable));
    EXPECT_EQ(test.snapshot, nullptr);
    ASSERT_EQ(ct_read_only_view_capture(test.tree, 0, 1, &test.snapshot), 0);
    ct_scan_options_v2 scan{};
    scan.struct_size        = sizeof(scan);
    scan.strict_byte_budget = 1;
    scan.byte_budget        = 1;
    ct_scan_result *result  = nullptr;
    ASSERT_NO_FATAL_FAILURE(test.scan(scan, &result));
    const uint8_t *packed    = nullptr;
    size_t         len       = 0;
    uint64_t       count     = 0;
    int32_t        truncated = 0;
    ASSERT_EQ(ct_scan_result_data(result, &packed, &len, &count, &truncated), 0);
    EXPECT_EQ(count, 0U);
    EXPECT_EQ(truncated, 1);
    ct_scan_result_free(result);
    result                  = nullptr;
    scan.strict_byte_budget = 0;
    scan.reverse            = 1;
    ASSERT_NO_FATAL_FAILURE(test.scan(scan, &result));
    ct_scan_result_free(result);
}

TEST(ImmutableSnapshotCAbi, AsyncRebuildRetainsTheReleasedViewThroughColdReadAndDurableCommit)
{
    TestSnapshotTree test;
    ct_close(std::exchange(test.tree, nullptr));
    ct_page_store *raw_source = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(4096, &raw_source), 0);
    std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> source(raw_source, &ct_page_store_free);
    ct_options                                                    options{};
    options.page_store  = source.get();
    options.frame_bytes = 4096;
    ASSERT_EQ(ct_open_with_runtime(&options, test.runtime, nullptr, &test.tree), 0);
    const std::string value(9000, 'v');
    test.put(1, "key", value);
    uint64_t coverage = 0;
    ASSERT_EQ(ct_flush_with_persistent(test.tree, &coverage), 0);
    ct_close(std::exchange(test.tree, nullptr));

    auto  io                    = std::make_unique<test_write::TestCheckpointStore>();
    auto *backend               = io.get();
    io->device                  = std::shared_ptr<PageStore>(source->bundle->store.get(), [](PageStore *) {});
    source->bundle->async_store = std::move(io);
    ASSERT_EQ(ct_open_with_runtime(&options, test.runtime, nullptr, &test.tree), 0);
    ASSERT_EQ(ct_read_only_view_capture(test.tree, 0, coverage, &test.snapshot), 0);
    ct_close(std::exchange(test.tree, nullptr));

    ct_page_store *raw_destination = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(4096, &raw_destination), 0);
    std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> destination(raw_destination, &ct_page_store_free);
    options.page_store = destination.get();
    ASSERT_EQ(ct_open_with_runtime(&options, test.runtime, nullptr, &test.tree), 0);

    auto deferred       = std::make_shared<std::promise<std::function<void()>>>();
    auto pending        = deferred->get_future();
    backend->defer_read = [deferred, device = backend->device, first = true](PageAddr at, void *out, size_t len,
                                                                             AsyncCompletion &done) mutable {
        if (!std::exchange(first, false)) {
            return false;
        }
        deferred->set_value([at, out, len, done = std::move(done), device] {
            done.complete(device->read_at(at, static_cast<uint8_t *>(out), len));
        });
        return true;
    };
    using Result          = std::pair<ct_status, ct_range_rebuild_stats>;
    auto       completion = std::make_unique<std::promise<Result>>();
    auto       completed  = completion->get_future();
    auto      *context    = completion.release();
    const auto accepted   = ct_read_only_view_rebuild_async(
        test.snapshot, test.tree,
        [](void *raw, ct_status status, ct_range_rebuild_stats stats) {
            std::unique_ptr<std::promise<Result>> result(static_cast<std::promise<Result> *>(raw));
            result->set_value({status, stats});
        },
        context);
    if (accepted != 0) {
        completion.reset(context);
    }
    ASSERT_EQ(accepted, 0);
    ASSERT_EQ(pending.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(completed.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    ct_read_only_view_free(std::exchange(test.snapshot, nullptr));
    auto resume = pending.get();
    resume();
    resume = {};
    ASSERT_EQ(completed.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    const auto [status, stats] = completed.get();
    ASSERT_EQ(status, 0);
    EXPECT_EQ(stats.entries_emitted, 1U);
    EXPECT_GT(stats.pages_rebuilt, 0U);
    uint64_t sequence           = 0;
    uint64_t recovered_coverage = 0;
    ASSERT_EQ(ct_durable_state(test.tree, &sequence, &recovered_coverage), 0);
    EXPECT_NE(sequence, 0U);
    EXPECT_EQ(recovered_coverage, coverage);
    ct_close(std::exchange(test.tree, nullptr));
    ASSERT_EQ(ct_open_with_runtime(&options, test.runtime, nullptr, &test.tree), 0);
    int32_t       found = 0;
    uint64_t      slot  = 0;
    ct_buf        bytes{};
    const uint8_t key[] = {'k', 'e', 'y'};
    ASSERT_EQ(ct_get(test.tree, key, sizeof(key), &found, &slot, &bytes), 0);
    ASSERT_EQ(found, 1);
    EXPECT_EQ(std::string(reinterpret_cast<const char *>(bytes.data), bytes.len), value);
    ct_free_buf(&bytes);
}
} // namespace crowdb::tree
