// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/maptable/frame_page.h"
#include "crowdb-tree/page_store/async_page_store.h"
#include "crowdb-tree/page_store/page_store.h"
#include "snapshot/primary_page_block.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <semaphore>

namespace crowdb::tree
{
#ifdef CROWDB_TREE_TEST_UTIL
namespace
{
struct TestPipelineStore : PageMemStore, AsyncPageStore
{
    TestPipelineStore() : PageMemStore(4096)
    {
    }

    std::atomic<size_t>   writes{0};
    std::atomic<bool>     notify_write{false};
    std::binary_semaphore written{0};
    std::atomic<uint64_t> blocked_address{UINT64_MAX};
    std::atomic<bool>     pause_read{false};
    std::atomic<bool>     read_blocked{false};
    std::atomic<bool>     notify_other_read{false};
    std::binary_semaphore read_entered{0};
    std::binary_semaphore other_read{0};
    std::atomic<uint64_t> counted_read_address{UINT64_MAX};
    std::atomic<size_t>   counted_reads{0};
    std::atomic<bool>     pause_write{false};
    std::atomic<bool>     hold_overflow{false};
    std::atomic<size_t>   primary_writes{0};
    std::atomic<bool>     fail_other_write{false};
    std::atomic<uint64_t> held_pid{kInvalidPageId};
    std::atomic<size_t>   write_failures{0};
    std::binary_semaphore write_entered{0};
    std::binary_semaphore failed_twice{0};
    AsyncCompletion       held_write;

    struct Read
    {
        PageAddr        address = 0;
        void           *data    = nullptr;
        size_t          bytes   = 0;
        AsyncCompletion complete;
    } held_read;

    uint64_t submit_read(PageAddr address, void *data, size_t bytes, AsyncCompletion complete) override
    {
        if (address == counted_read_address.load()) {
            counted_reads.fetch_add(1);
        }
        if (address == blocked_address.load() && pause_read.exchange(false)) {
            held_read = {.address = address, .data = data, .bytes = bytes, .complete = std::move(complete)};
            read_blocked.store(true);
            read_entered.release();
            return 0;
        }
        if (read_blocked.load() && notify_other_read.exchange(false)) {
            other_read.release();
        }
        complete.complete(PageMemStore::read_at(address, static_cast<uint8_t *>(data), bytes));
        return 0;
    }

    void resume_read()
    {
        if (read_blocked.exchange(false)) {
            auto read = std::move(held_read);
            read.complete.complete(PageMemStore::read_at(read.address, static_cast<uint8_t *>(read.data), read.bytes));
        }
    }

    uint64_t submit_write(PageAddr address, const void *data, size_t bytes, AsyncCompletion complete) override
    {
        return submit_typed_write(PagePurpose::kBtreePage, address, data, bytes, std::move(complete));
    }

    uint64_t submit_typed_write(PagePurpose purpose, PageAddr address, const void *data, size_t bytes,
                                AsyncCompletion complete) override
    {
        if (purpose == PagePurpose::kBtreePage && hold_overflow &&
            frame_page_type(static_cast<const uint8_t *>(data) + kDurableBlobHeader) == page_type::kLeafBase) {
            primary_writes.fetch_add(1);
        }
        if (purpose == PagePurpose::kBtreePage && (pause_write.load() || fail_other_write.load())) {
            const auto *frame = static_cast<const uint8_t *>(data) + kDurableBlobHeader;
            const auto  type  = hold_overflow ? page_type::kOverflowFrame : page_type::kLeafBase;
            if (frame_page_type(frame) == type) {
                const auto pid = frame_u64(frame, fh::kSelfpage_id);
                if (pause_write.exchange(false)) {
                    if (hold_overflow) {
                        auto result =
                            PageMemStore::write_typed_at(purpose, address, static_cast<const uint8_t *>(data), bytes);
                        if (!result.ok()) {
                            complete.complete(std::move(result));
                            return 0;
                        }
                    }
                    held_write = std::move(complete);
                    held_pid.store(pid);
                    write_entered.release();
                    return 0;
                }
                if (fail_other_write.load() && pid != held_pid.load()) {
                    if (hold_overflow) {
                        complete.complete(Status::io_error("injected overflow write failure"));
                        if (notify_write.exchange(false)) {
                            written.release();
                        }
                        return 0;
                    }
                    complete.complete(Status::io_error("injected leaf write failure"));
                    if (write_failures.fetch_add(1) == 1) {
                        failed_twice.release();
                    }
                    return 0;
                }
            }
        }
        complete.complete(write_typed_at(purpose, address, static_cast<const uint8_t *>(data), bytes));
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::Ok());
        return Status::Ok();
    }

    void cancel(uint64_t /*operation*/) override
    {
    }

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    Status write_typed_at(PagePurpose purpose, uint64_t address, const uint8_t *data, size_t bytes) override
    {
        const auto status = PageMemStore::write_typed_at(purpose, address, data, bytes);
        if (status.ok() && purpose == PagePurpose::kBtreePage) {
            writes.fetch_add(1, std::memory_order_relaxed);
            if (notify_write.exchange(false, std::memory_order_acq_rel)) {
                written.release();
            }
        }
        return status;
    }
};

class TestSiblingWrites : public ::testing::TestWithParam<bool>
{
};
} // namespace

TEST_P(TestSiblingWrites, JoinsReverseCompletionAndRetriesOnlyTheFailedPage)
{
    const bool         permanent = GetParam();
    Config             config;
    TreeRuntimeOptions host;
    host.workers         = 1;
    host.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    auto store              = std::make_shared<TestPipelineStore>();
    config.page_store       = store.get();
    config.async_page_store = store.get();
    config.page_store_owner = store;
    config.frame_bytes      = 4096;
    config.leaf_split_bytes = 400;
    config.max_inline_value = 32;
    config.inner_max_keys   = 16;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    Batch initial;
    for (char key = 'a'; key <= 't'; ++key) {
        initial.ops.push_back({.key = std::string(1, key), .kind = OpKind::kPut, .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(core->apply(1, initial).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    const auto before = core->capture();
    const auto root   = before->mapping.get(before->root_page_id);
    ASSERT_EQ(root->level, 1U);
    for (uint64_t pid = 0; pid < before->next_page_id; ++pid) {
        const auto page = before->mapping.get(pid);
        if (page && page->leaf && page->leaf->range.contains("t")) {
            ASSERT_TRUE(page->primary && page->primary->frame.page_block);
            store->counted_read_address.store(page->primary->frame.page_block->address);
        }
    }
    ASSERT_NE(store->counted_read_address.load(), UINT64_MAX);
    ASSERT_TRUE(core->apply(2,
                            Batch{
                                .ops = {{.key = "a", .kind = OpKind::kPut, .value = std::string(32, 'x')},
                                        {.key = "t", .kind = OpKind::kPut, .value = std::string(32, 'y')}}
    })
                    .ok());
    core->binding()->drain();
    store->counted_reads.store(0);
    store->pause_write.store(true);
    store->notify_write.store(true);
    store->fail_other_write.store(permanent);
    auto       checkpoint = std::async(std::launch::async, [&] { return core->checkpoint(); });
    const bool first      = store->write_entered.try_acquire_for(std::chrono::seconds(5));
    auto      &peer       = permanent ? store->failed_twice : store->written;
    const bool second     = first && peer.try_acquire_for(std::chrono::seconds(5));
    EXPECT_EQ(checkpoint.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    EXPECT_TRUE(core->capture()->mapping.same_root(before->mapping));
    EXPECT_EQ(core->durable_coverage(), 1U);
    if (first) {
        // Fail the held write once in the transient case, otherwise drain it
        // after the other leaf has exhausted its own retry.
        store->held_write.complete(Status::io_error("injected held write failure"));
        store->held_write = {};
    }
    const auto status = checkpoint.get();
    EXPECT_TRUE(first);
    EXPECT_TRUE(second);
    EXPECT_EQ(store->counted_reads.load(), 1U);
    EXPECT_EQ(store->write_failures.load(), permanent ? 2U : 0U);
    EXPECT_EQ(status.code(), permanent ? Code::kIoError : Code::kOk) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), permanent ? 1U : 2U);
    if (permanent) {
        EXPECT_TRUE(core->capture()->mapping.same_root(before->mapping));
        EXPECT_EQ(core->capture()->coverage, before->coverage);
        GetView pending;
        ASSERT_TRUE(core->get("t", &pending).ok());
        EXPECT_EQ(pending.value().to_string(), std::string(32, 'y'));
        store->fail_other_write.store(false);
    }
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    GetView value;
    ASSERT_TRUE(core->get("a", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(32, permanent ? 'v' : 'x'));
    ASSERT_TRUE(core->get("t", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(32, permanent ? 'v' : 'y'));
}

INSTANTIATE_TEST_SUITE_P(PersistentFlushPipeline, TestSiblingWrites, ::testing::Bool());

class TestLocalOverflowWrites : public ::testing::TestWithParam<bool>
{
};

TEST_P(TestLocalOverflowWrites, JoinsBeforeThePrimaryAndDrainsOnFailure)
{
    const bool         fail_peer = GetParam();
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    auto store              = std::make_shared<TestPipelineStore>();
    config.page_store       = store.get();
    config.async_page_store = store.get();
    config.page_store_owner = store;
    config.frame_bytes      = 4096;
    config.max_inline_value = 32;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    const std::string value(12000, 'v');
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = value}}}).ok());
    store->hold_overflow = true;
    store->pause_write.store(true);
    store->notify_write.store(true);
    store->fail_other_write.store(fail_peer);
    auto       checkpoint = std::async(std::launch::async, [&] { return core->checkpoint(); });
    const bool first      = store->write_entered.try_acquire_for(std::chrono::seconds(5));
    const bool other      = first && store->written.try_acquire_for(std::chrono::seconds(5));
    EXPECT_EQ(checkpoint.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(core->durable_coverage(), 0U);
    EXPECT_EQ(store->primary_writes.load(), 0U);
    if (first) {
        store->held_write.complete(Status::Ok());
        store->held_write = {};
    }
    const auto status = checkpoint.get();
    EXPECT_TRUE(first);
    EXPECT_TRUE(other);
    ASSERT_EQ(status.code(), fail_peer ? Code::kIoError : Code::kOk) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), fail_peer ? 0U : 1U);
    EXPECT_EQ(store->primary_writes.load(), fail_peer ? 0U : 1U);
    if (fail_peer) {
        GetView pending;
        ASSERT_TRUE(core->get("key", &pending).ok());
        EXPECT_EQ(pending.value().to_string(), value);
        return;
    }
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    GetView read;
    ASSERT_TRUE(core->get("key", &read).ok());
    EXPECT_EQ(read.value().to_string(), value);
}

INSTANTIATE_TEST_SUITE_P(PersistentFlushPipeline, TestLocalOverflowWrites, ::testing::Bool());

TEST(PersistentFlushPipeline, SplitLeavesReadTheirSharedColdPageBlockOnce)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    auto store              = std::make_shared<TestPipelineStore>();
    config.page_store       = store.get();
    config.async_page_store = store.get();
    config.page_store_owner = store;
    config.frame_bytes      = 4096;
    config.leaf_split_bytes = 400;
    config.max_inline_value = 32;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    Batch initial;
    for (const char key : {'a', 'z'}) {
        initial.ops.push_back({.key = std::string(1, key), .kind = OpKind::kPut, .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(core->apply(1, initial).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    const auto before = core->capture();
    const auto root   = before->mapping.get(before->root_page_id);
    ASSERT_TRUE(root->leaf && root->primary);
    const auto page_block = root->primary->frame.page_block;
    ASSERT_TRUE(page_block);
    Batch inserts;
    for (const char key : std::string_view("bcdefuvwxy")) {
        inserts.ops.push_back({.key = std::string(1, key), .kind = OpKind::kPut, .value = std::string(32, 'x')});
    }
    ASSERT_TRUE(core->apply(2, inserts).ok());
    ASSERT_TRUE(core->flush().ok());
    const auto split = core->capture();
    ASSERT_GT(split->mapping.get(split->root_page_id)->level, 0U);
    size_t clipped_leaves = 0;
    for (uint64_t pid = 0; pid < split->next_page_id; ++pid) {
        const auto page = split->mapping.get(pid);
        if (page && page->leaf && (page->leaf->range.contains("a") || page->leaf->range.contains("z"))) {
            ASSERT_EQ(page->leaf->bases.size(), 1U);
            EXPECT_EQ(page->leaf->bases.front().page_block, page_block);
            EXPECT_FALSE(page->leaf->bases.front().resident);
            ++clipped_leaves;
        }
    }
    ASSERT_EQ(clipped_leaves, 2U);
    store->counted_read_address.store(page_block->address);
    ASSERT_TRUE(core->checkpoint().ok());
    EXPECT_EQ(store->counted_reads.load(), 1U);
    EXPECT_EQ(core->durable_coverage(), 2U);
}

TEST(PersistentFlushPipeline, FinalPageWritesWhileAnUnrelatedSubtreeStillWaitsAtHandoff)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    auto store                         = std::make_shared<TestPipelineStore>();
    config.page_store                  = store.get();
    config.page_store_owner            = store;
    config.frame_bytes                 = 4096;
    config.leaf_split_bytes            = 400;
    config.max_inline_value            = 32;
    config.inner_max_keys              = 4;
    config.inner_merge_keys            = 1;
    config.execution.flush_parallelism = 4;
    std::shared_ptr<TreeCore> core;
    const auto                opened = TreeCore::open(config, &core);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    Batch initial;
    for (size_t i = 0; i < 200; ++i) {
        const auto digits = std::to_string(i);
        initial.ops.push_back(
            {.key = std::string(4 - digits.size(), '0') + digits, .kind = OpKind::kPut, .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(core->apply(1, initial).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    const auto before = core->capture();
    ASSERT_GE(before->mapping.get(before->root_page_id)->level, 2U);
    uint64_t blocked_pid = kInvalidPageId;
    for (uint64_t pid = 0; pid < before->next_page_id; ++pid) {
        const auto page = before->mapping.get(pid);
        if (page && page->leaf && page->leaf->range.contains("0199")) {
            blocked_pid = pid;
        }
    }
    ASSERT_NE(blocked_pid, kInvalidPageId);
    std::binary_semaphore entered{0};
    std::function<void()> resume;
    core->set_flush_handoff_wait_for_tests([&](uint64_t pid, std::function<void()> ready) {
        if (pid == blocked_pid) {
            resume = std::move(ready);
            entered.release();
            return true;
        }
        return false;
    });
    ASSERT_TRUE(core->apply(2,
                            Batch{
                                .ops = {{.key = "0000", .kind = OpKind::kPut, .value = std::string(32, 'a')},
                                        {.key = "0199", .kind = OpKind::kPut, .value = std::string(32, 'b')}}
    })
                    .ok());
    const auto writes = store->writes.load();
    store->notify_write.store(true, std::memory_order_release);
    auto       checkpoint = std::async(std::launch::async, [&] { return core->checkpoint(); });
    const auto blocked    = entered.try_acquire_for(std::chrono::seconds(10));
    const auto written    = store->written.try_acquire_for(std::chrono::seconds(10));
    const auto waiting    = checkpoint.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
    const auto during     = core->capture();
    EXPECT_EQ(during->coverage, before->coverage);
    EXPECT_TRUE(during->mapping.same_root(before->mapping));
    EXPECT_EQ(core->durable_coverage(), 1U);
    auto       reads       = std::async(std::launch::async, [&] {
        GetView value;
        EXPECT_TRUE(core->get("0000", &value).ok());
        EXPECT_EQ(value.value().to_string(), std::string(32, 'a'));
        EXPECT_TRUE(core->get("0199", &value).ok());
        EXPECT_EQ(value.value().to_string(), std::string(32, 'b'));
        std::vector<scan_entry> entries;
        bool                    truncated = false;
        CoreScanOptions         options;
        options.limit = 1;
        EXPECT_TRUE(core->scan(options, &entries, &truncated).ok());
        EXPECT_EQ(entries.size(), 1U);
        if (!entries.empty()) {
            EXPECT_EQ(entries.front().key, "0000");
            EXPECT_EQ(entries.front().value, std::string(32, 'a'));
        }
    });
    const auto reads_ready = reads.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    if (resume) {
        resume();
        resume = {};
    }
    reads.get();
    const auto status = checkpoint.get();
    core->binding()->drain();
    core->set_flush_handoff_wait_for_tests({});
    EXPECT_TRUE(blocked);
    EXPECT_TRUE(written);
    EXPECT_TRUE(waiting);
    EXPECT_TRUE(reads_ready);
    EXPECT_GT(store->writes.load(), writes);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), 2U);
    GetView value;
    ASSERT_TRUE(core->get("0000", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(32, 'a'));
    ASSERT_TRUE(core->get("0199", &value).ok());
    EXPECT_EQ(value.value().to_string(), std::string(32, 'b'));
}

TEST(PersistentFlushPipeline, AColdReadDoesNotBlockOtherReadsOrReadyPageWrites)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers                                = 1;
    host.backend_workers                        = 1;
    host.limits.io_operations                   = 1;
    config.runtime_binding.limits.io_operations = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    auto store                         = std::make_shared<TestPipelineStore>();
    config.page_store                  = store.get();
    config.async_page_store            = store.get();
    config.page_store_owner            = store;
    config.frame_bytes                 = 4096;
    config.leaf_split_bytes            = 400;
    config.max_inline_value            = 32;
    config.inner_max_keys              = 4;
    config.inner_merge_keys            = 1;
    config.execution.flush_parallelism = 1;
    std::shared_ptr<TreeCore> core;
    {
        const auto status = TreeCore::open(config, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    Batch initial;
    for (size_t i = 0; i < 200; ++i) {
        const auto digits = std::to_string(i);
        initial.ops.push_back(
            {.key = std::string(4 - digits.size(), '0') + digits, .kind = OpKind::kPut, .value = std::string(32, 'v')});
    }
    ASSERT_TRUE(core->apply(1, initial).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    {
        const auto status = TreeCore::open(config, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    const auto view = core->capture();
    ASSERT_GE(view->mapping.get(view->root_page_id)->level, 2U);
    std::atomic<bool>     arm_read{true};
    std::atomic<bool>     pause_handoff{true};
    std::binary_semaphore handoff_entered{0};
    std::function<void()> resume_handoff;
    core->set_flush_handoff_wait_for_tests([&](uint64_t pid, std::function<void()> ready) {
        const auto page = view->mapping.get(pid);
        if (page && page->primary && page->primary->frame.page_block && arm_read.exchange(false)) {
            store->blocked_address.store(page->primary->frame.page_block->address);
            store->pause_read.store(true);
            return false;
        }
        if (pause_handoff.exchange(false)) {
            resume_handoff = std::move(ready);
            handoff_entered.release();
            return true;
        }
        return false;
    });
    const Batch changes{
        .ops = {{.key = "0000", .kind = OpKind::kPut, .value = std::string(32, 'a')},
                {.key = "0199", .kind = OpKind::kPut, .value = std::string(32, 'b')}}
    };
    ASSERT_TRUE(core->apply(2, changes).ok());
    core->binding()->drain();
    store->notify_other_read.store(true);
    store->notify_write.store(true);
    auto       checkpoint    = std::async(std::launch::async, [&] { return core->checkpoint(); });
    const bool blocked       = store->read_entered.try_acquire_for(std::chrono::seconds(5));
    const bool handoff_waits = handoff_entered.try_acquire_for(std::chrono::seconds(5));
    if (handoff_waits) {
        resume_handoff();
        resume_handoff = {};
    }
    const bool another_read = store->other_read.try_acquire_for(std::chrono::seconds(2));
    const bool written      = store->written.try_acquire_for(std::chrono::seconds(2));
    const bool waiting      = checkpoint.wait_for(std::chrono::milliseconds(0)) == std::future_status::timeout;
    store->resume_read();
    const auto status = checkpoint.get();
    core->binding()->drain();
    core->set_flush_handoff_wait_for_tests({});
    EXPECT_TRUE(blocked);
    EXPECT_TRUE(handoff_waits);
    EXPECT_TRUE(another_read);
    EXPECT_TRUE(written);
    EXPECT_TRUE(waiting);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->durable_coverage(), 2U);
    core.reset();
    {
        const auto status = TreeCore::open(config, &core);
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    for (const auto *key : {"0000", "0199"}) {
        GetView value;
        ASSERT_TRUE(core->get(key, &value).ok());
        EXPECT_EQ(value.value().to_string(), std::string(32, std::string_view(key) == "0000" ? 'a' : 'b'));
    }
}
#endif
} // namespace crowdb::tree
