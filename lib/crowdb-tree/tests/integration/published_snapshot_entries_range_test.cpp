// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-tree/crowdb-tree.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_snapshot_entries.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
struct TestSnapshotRange
{
    Config                                    config;
    std::unique_ptr<Crowdbtree>               tree;
    std::shared_ptr<PublishedReadViewContext> snapshot;

    Status prepare()
    {
        auto status = TreeRuntime::create({}, &config.runtime);
        if (!status.ok()) {
            return status;
        }
        tree   = std::make_unique<Crowdbtree>(config);
        status = tree->apply(1, Batch{
                                    .ops = {{.key = "", .kind = OpKind::kPut, .value = "empty"},
                                            {.key = "a", .kind = OpKind::kPut, .value = "first"},
                                            {.key = "b", .kind = OpKind::kDelete, .value = ""},
                                            {.key = "c", .kind = OpKind::kPut, .value = "last"}}
        });
        if (status.ok()) {
            status = tree->flush();
        }
        return status.ok() ? TreeCoreAccess::get(*tree)->capture_read_only_view(&snapshot) : status;
    }

    Status keys(const KeyRange &range, std::vector<std::string> *out) const
    {
        std::shared_ptr<PublishedSnapshotEntries> cursor;
        auto                                      status = PublishedSnapshotEntries::create(snapshot, &cursor, range);
        if (!status.ok()) {
            return status;
        }
        bool done = false;
        while (!done) {
            std::optional<PublishedScanEntry> entry;
            status = cursor->next(&entry, &done);
            if (!status.ok()) {
                return status;
            }
            if (entry) {
                out->push_back(Slice(entry->key).to_string());
                EXPECT_EQ(entry->cell.tombstone(), Slice(entry->key) == Slice("b"));
            }
        }
        return Status::Ok();
    }
};

TEST(PublishedSnapshotEntriesRange, HalfOpenAndPresentEmptyBoundsKeepTheirMeaning)
{
    TestSnapshotRange test;
    auto              status = test.prepare();
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (const auto &bounds : {
             std::pair{"a", "c"},
             std::pair{"",  "a"},
             std::pair{"b", "b"}
    }) {
        std::vector<std::string> keys;
        status = test.keys(KeyRange::bounded(bounds.first, bounds.second), &keys);
        ASSERT_TRUE(status.ok()) << status.to_string();
        if (std::string_view(bounds.first) == "a") {
            EXPECT_EQ(keys, (std::vector<std::string>{"a", "b"}));
        }
        else if (std::string_view(bounds.first).empty()) {
            EXPECT_EQ(keys, (std::vector<std::string>{""}));
        }
        else {
            EXPECT_TRUE(keys.empty());
        }
    }
}

TEST(PublishedSnapshotEntriesRange, FixedSmallViewFitsTwoCursorsWithOnlySixteenMiBRemaining)
{
    TestSnapshotRange test;
    auto              status = test.prepare();
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto &core = *TreeCoreAccess::get(*test.tree);
    // This newer source value is outside the fixed snapshot. It cannot enlarge
    // the snapshot's L1 decode bound or add an L0 iterator to either cursor.
    ASSERT_TRUE(
        test.tree->apply(2, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = std::string(1024 * 1024, 'n')}}})
            .ok());
    core.binding()->drain();
    const auto       account   = core.binding()->account();
    const auto       used      = account->snapshot();
    constexpr auto   category  = detail::BudgetClass::kRouting;
    constexpr auto   index     = static_cast<size_t>(category);
    constexpr size_t available = 16 * 1024 * 1024;
    const auto       capacity  = detail::ResourceBudget::capacity(account, category);
    ASSERT_GT(capacity, used.allocated[index] + used.reserved[index] + available);
    auto blocker = detail::ResourceBudget::try_reserve(
        account, category, capacity - used.allocated[index] - used.reserved[index] - available);
    ASSERT_TRUE(blocker);
    std::shared_ptr<PublishedSnapshotEntries> first;
    std::shared_ptr<PublishedSnapshotEntries> second;
    status = PublishedSnapshotEntries::create(test.snapshot, &first);
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = PublishedSnapshotEntries::create(test.snapshot, &second);
    ASSERT_TRUE(status.ok()) << status.to_string();
    for (const auto &cursor : {first, second}) {
        bool                     done = false;
        std::vector<std::string> keys;
        while (!done) {
            std::optional<PublishedScanEntry> entry;
            status = cursor->next(&entry, &done);
            ASSERT_TRUE(status.ok()) << status.to_string();
            if (entry) {
                keys.push_back(Slice(entry->key).to_string());
                if (Slice(entry->key) == Slice("a")) {
                    EXPECT_EQ(entry->cell.value.to_string(), "first");
                }
            }
        }
        EXPECT_EQ(keys, (std::vector<std::string>{"", "a", "b", "c"}));
    }
}

TEST(PublishedSnapshotEntriesRange, FullValueCapRemainsReadableFromTheFixedView)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    Crowdbtree       tree(config);
    constexpr size_t bytes = 64 * 1024 * 1024;
    auto             status =
        tree.apply(1, Batch{.ops = {{.key = "large", .kind = OpKind::kPut, .value = std::string(bytes, 'v')}}});
    ASSERT_TRUE(status.ok()) << status.to_string();
    status = tree.flush();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<PublishedReadViewContext> snapshot;
    ASSERT_TRUE(TreeCoreAccess::get(tree)->capture_read_only_view(&snapshot).ok());
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    status = PublishedSnapshotEntries::create(snapshot, &cursor);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::optional<PublishedScanEntry> entry;
    bool                              done = false;
    do {
        status = cursor->next(&entry, &done);
        ASSERT_TRUE(status.ok()) << status.to_string();
    } while (!entry && !done);
    ASSERT_TRUE(entry);
    EXPECT_EQ(Slice(entry->key).to_string(), "large");
    ASSERT_EQ(entry->cell.value.size(), bytes);
    EXPECT_EQ(entry->cell.value.data()[0], 'v');
    EXPECT_EQ(entry->cell.value.data()[bytes - 1], 'v');
}

TEST(PublishedSnapshotEntriesRange, InvalidBoundsFailBeforeAnyControlAdmission)
{
    TestSnapshotRange test;
    auto              status = test.prepare();
    ASSERT_TRUE(status.ok()) << status.to_string();
    // Complete capture's deferred releases before comparing the two resource
    // partitions; returning an allocation to its reserve changes both gauges.
    TreeCoreAccess::get(*test.tree)->binding()->drain();
    const auto                                before = test.config.runtime->snapshot();
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    status = PublishedSnapshotEntries::create(test.snapshot, &cursor, KeyRange::bounded("c", "a"));
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(cursor);
    status = PublishedSnapshotEntries::create(
        test.snapshot, &cursor,
        KeyRange::bounded(std::string(test.snapshot->read_limits().max_key_bytes + 1, 'a'), {}));
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(cursor);
    EXPECT_EQ(test.config.runtime->snapshot().reserved.routing_bytes, before.reserved.routing_bytes);
    EXPECT_EQ(test.config.runtime->snapshot().allocated.routing_bytes, before.allocated.routing_bytes);
}

TEST(PublishedSnapshotEntriesRange, AsyncWindowKeepsTheFixedViewUntilALateReadCompletes)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    auto store              = std::make_shared<test_write::TestCheckpointStore>();
    store->device           = std::make_shared<PageMemStore>(4096);
    config.page_store       = store->device.get();
    config.page_store_owner = store;
    config.frame_bytes      = 4096;
    std::unique_ptr<Crowdbtree> tree;
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    ASSERT_TRUE(tree->apply(1, Batch{.ops = {{.key = "b", .kind = OpKind::kDelete, .value = ""}}}).ok());
    ASSERT_TRUE(tree->flush_with_persistent().ok());
    tree.reset();
    config.async_page_store = store.get();
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    std::shared_ptr<PublishedReadViewContext> view;
    ASSERT_TRUE(TreeCoreAccess::get(*tree)->capture_read_only_view(&view).ok());
    ASSERT_TRUE(view->detached());
    tree.reset();
    std::shared_ptr<PublishedSnapshotEntries> cursor;
    ASSERT_TRUE(PublishedSnapshotEntries::create(view, &cursor, KeyRange::bounded("b", "c")).ok());

    auto deferred     = std::make_shared<std::promise<std::function<void()>>>();
    auto waiting      = deferred->get_future();
    store->defer_read = [deferred, device = store->device, defer = true](PageAddr at, void *out, size_t len,
                                                                         AsyncCompletion &done) mutable {
        if (!std::exchange(defer, false)) {
            return false;
        }
        deferred->set_value([at, out, len, done = std::move(done), device] {
            done.complete(device->read_at(at, static_cast<uint8_t *>(out), len));
        });
        return true;
    };

    using EntryResult = std::pair<Status, std::optional<PublishedScanEntry>>;
    auto completion   = std::make_shared<std::promise<EntryResult>>();
    auto result       = completion->get_future();
    ASSERT_TRUE(cursor
                    ->next_async([completion](Status status, std::optional<PublishedScanEntry> entry, bool) {
                        completion->set_value({std::move(status), std::move(entry)});
                    })
                    .ok());
    ASSERT_EQ(waiting.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(cursor->next_async([](Status, std::optional<PublishedScanEntry>, bool) { ADD_FAILURE(); }).code(),
              Code::kUnavailable);

    auto  closing       = std::make_unique<std::promise<void>>();
    auto  closed        = closing->get_future();
    auto *close_context = closing.release();
    auto  close_status  = view->close_async(
        [](void *context) {
            std::unique_ptr<std::promise<void>> promise(static_cast<std::promise<void> *>(context));
            promise->set_value();
        },
        close_context);
    if (!close_status.ok()) {
        closing.reset(close_context);
    }
    ASSERT_TRUE(close_status.ok()) << close_status.to_string();
    EXPECT_EQ(closed.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    cursor.reset();
    view.reset();
    auto resume = waiting.get();
    resume();
    resume = {};
    ASSERT_EQ(result.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto [status, entry] = result.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    if (entry) {
        EXPECT_EQ(Slice(entry->key).to_string(), "b");
        EXPECT_TRUE(entry->cell.tombstone());
    }
    else {
        ADD_FAILURE() << "read-only view lost the tombstone";
    }
    ASSERT_EQ(closed.wait_for(std::chrono::seconds(3)), std::future_status::ready);
}
} // namespace
} // namespace crowdb::tree
