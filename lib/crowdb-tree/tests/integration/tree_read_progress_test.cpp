// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "local_recovery_test_support.h"
#include "snapshot/published_snapshot.h"

#include <chrono>
#include <future>
#include <semaphore>

namespace crowdb::tree
{
namespace
{
struct TestReadCpuGate
{
    std::shared_ptr<TreeRuntimeBinding>         binding;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    std::binary_semaphore                       entered{0};
    std::atomic<bool>                           released{false};

    explicit TestReadCpuGate(std::shared_ptr<TreeRuntimeBinding>         owner,
                             std::shared_ptr<detail::FlushRuntime::Lane> target = {})
        : binding(std::move(owner)),
          lane(target ? std::move(target) : binding->lane())
    {
        const auto status = binding->cpu()->try_submit(lane, &run, this);
        if (!status.ok()) {
            ADD_FAILURE() << status.to_string();
            return;
        }
        entered.acquire();
        for (size_t i = 0; i <= binding->options().cpu_queue_limit; ++i) {
            EXPECT_TRUE(binding->cpu()->try_submit(lane, +[](void *) noexcept {}, nullptr).ok());
        }
        EXPECT_GT(lane->queued(), binding->options().cpu_queue_limit);
    }

    ~TestReadCpuGate()
    {
        release();
        lane->drain();
    }

    static void run(void *context) noexcept
    {
        auto &self = *static_cast<TestReadCpuGate *>(context);
        self.entered.release();
        while (!self.released.load(std::memory_order_acquire)) {
            self.released.wait(false);
        }
    }

    void release()
    {
        released.store(true, std::memory_order_release);
        released.notify_all();
    }
};

struct TestProgressReadStore final : AsyncPageStore
{
    test_write::TestRecoveryStore store;
    std::binary_semaphore         entered{0};
    bool                          notify = false;

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    uint64_t submit_read(PageAddr at, void *out, size_t bytes, AsyncCompletion done) override
    {
        const auto id = store.submit_read(at, out, bytes, std::move(done));
        if (notify) {
            entered.release();
        }
        return id;
    }

    uint64_t submit_write(PageAddr at, const void *data, size_t bytes, AsyncCompletion done) override
    {
        return store.submit_write(at, data, bytes, std::move(done));
    }

    Status submit_fsync(AsyncCompletion done) override
    {
        return store.submit_fsync(std::move(done));
    }

    void cancel(uint64_t /*op_id*/) override
    {
    }
};

TEST(TreeReadProgress, AdmittedAsyncReadsWaitForBusyCpu)
{
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.cpu_queue_limit = 8;
    auto store                             = std::make_shared<MemPageStore>(4096);
    config.page_store                      = store.get();
    config.backend_owner                   = store;
    config.frame_bytes                     = 4096;
    config.leaf_split_bytes                = 3000;
    std::shared_ptr<TreeCore> core;
    const auto                opened = TreeCore::open(config, &core);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    core->binding()->drain();
    std::promise<Status> done;
    std::promise<Status> scan_done;
    auto                 completed      = done.get_future();
    auto                 scan_completed = scan_done.get_future();
    TestReadCpuGate      gate(core->binding());
    core->get_async("key", [&](Status status, GetView value) {
        if (status.ok()) {
            EXPECT_EQ(value.value().to_string(), "value");
        }
        done.set_value(std::move(status));
    });
    core->scan_async({}, [&](Status status, ScanPackedBuf packed, size_t count, bool truncated) {
        if (status.ok()) {
            EXPECT_NE(packed.size(), 0U);
            EXPECT_EQ(count, 1U);
            EXPECT_FALSE(truncated);
        }
        scan_done.set_value(std::move(status));
    });
    const auto pending      = completed.wait_for(std::chrono::milliseconds(20));
    const auto scan_pending = scan_completed.wait_for(std::chrono::milliseconds(0));
    gate.release();
    const auto status      = completed.get();
    const auto scan_status = scan_completed.get();
    EXPECT_EQ(pending, std::future_status::timeout);
    EXPECT_EQ(scan_pending, std::future_status::timeout);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(scan_status.ok()) << scan_status.to_string();
}

TEST(TreeReadProgress, ColdDecodeContinuesWithReadyQueueBacklog)
{
    auto store          = std::make_shared<TestProgressReadStore>();
    store->store.device = std::make_shared<MemPageStore>(4096);
    Config             config;
    TreeRuntimeOptions host;
    host.workers = 1;
    ASSERT_TRUE(TreeRuntime::create(host, &config.runtime).ok());
    config.runtime_binding.cpu_queue_limit = 8;
    config.page_store                      = store->store.device.get();
    config.async_page_store                = store.get();
    config.backend_owner                   = store;
    config.frame_bytes                     = 4096;
    config.leaf_split_bytes                = 3000;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    core->binding()->drain();
    store->store.defer_at = store->store.reads + 1;
    store->notify         = true;
    std::promise<Status> done;
    auto                 completed = done.get_future();
    core->get_async("key", [&](Status status, GetView value) {
        if (status.ok()) {
            EXPECT_EQ(value.value().to_string(), "value");
        }
        done.set_value(std::move(status));
    });
    store->entered.acquire();
    TestReadCpuGate gate(core->binding());
    store->store.finish();
    const auto pending = completed.wait_for(std::chrono::milliseconds(20));
    gate.release();
    const auto status = completed.get();
    EXPECT_EQ(pending, std::future_status::timeout);
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(TreeReadProgress, DetachedScanContinuesWithReadyQueueBacklog)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .cpu_queue_limit = 8}, &config.runtime).ok());
    config.runtime_binding.cpu_queue_limit = 8;
    auto store                             = std::make_shared<MemPageStore>(4096);
    config.page_store                      = store.get();
    config.backend_owner                   = store;
    config.frame_bytes                     = 4096;
    config.leaf_split_bytes                = 3000;
    std::shared_ptr<TreeCore> core;
    const auto                opened = TreeCore::open(config, &core);
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "key", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->flush().ok());
    std::shared_ptr<PublishedReadViewContext> view;
    ASSERT_TRUE(core->capture_read_only_view(&view).ok());
    core.reset();

    auto            done      = std::make_shared<std::promise<Status>>();
    auto            completed = done->get_future();
    TestReadCpuGate gate(view->binding(), view->lane());
    ASSERT_TRUE(view->scan_async({},
                                 [done](Status status, ScanPackedBuf packed, size_t count, bool truncated) {
                                     if (status.ok()) {
                                         EXPECT_NE(packed.size(), 0U);
                                         EXPECT_EQ(count, 1U);
                                         EXPECT_FALSE(truncated);
                                     }
                                     done->set_value(std::move(status));
                                 })
                    .ok());
    EXPECT_EQ(completed.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    gate.release();
    ASSERT_EQ(completed.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    const auto status = completed.get();
    EXPECT_TRUE(status.ok()) << status.to_string();
    view.reset();
    config.runtime->drain_cpu();
}
} // namespace
} // namespace crowdb::tree
