// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/page_store/page_store.h"
#include "host_completion.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_snapshot.h"

#include <gtest/gtest.h>

#include <future>

namespace crowdb::tree
{
namespace
{
TEST(TreeCoreSyncPageStore, AsyncLocalOpenLetsTheOnlyBackendWorkerRunWhileRecoveryReadWaits)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    auto store              = std::make_shared<test_write::TestCheckpointStore>();
    store->device           = std::make_shared<PageMemStore>(4096);
    config.page_store       = store->device.get();
    config.page_store_owner = store;
    config.frame_bytes      = 4096;
    std::shared_ptr<TreeCore> core;
    ASSERT_TRUE(TreeCore::open(config, &core).ok());
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    core.reset();

    std::shared_ptr<TreeRuntimeBinding> probe;
    ASSERT_TRUE(TreeRuntimeBinding::create(config.runtime, {}, &probe).ok());
    std::promise<std::function<void()>> pending;
    auto                                read = pending.get_future();
    store->defer_read = [&, defer = true](PageAddr at, void *data, size_t len, AsyncCompletion &done) mutable {
        if (std::exchange(defer, false)) {
            pending.set_value([at, data, len, done = std::move(done), device = store->device] {
                done.complete(device->read_at(at, static_cast<uint8_t *>(data), len));
            });
            return true;
        }
        return false;
    };
    config.async_page_store = store.get();
    std::promise<std::pair<Status, std::shared_ptr<TreeCore>>> result;
    auto                                                       opened = result.get_future();
    ASSERT_TRUE(TreeCore::open_local_async(config, [&](Status status, std::shared_ptr<TreeCore> recovered) {
                    result.set_value({std::move(status), std::move(recovered)});
                }).ok());
    ASSERT_EQ(read.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(opened.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    auto               resume = read.get();
    std::promise<void> progress;
    auto               advanced = progress.get_future();
    ASSERT_TRUE(probe->backend_cpu()
                    ->try_submit(
                        probe->backend_lane(),
                        [](void *context) noexcept { static_cast<std::promise<void> *>(context)->set_value(); },
                        &progress)
                    .ok());
    EXPECT_EQ(advanced.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    resume();
    resume = {};
    ASSERT_EQ(opened.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto [status, recovered] = opened.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    probe->backend_cpu()->drain();
    GetView value;
    ASSERT_TRUE(recovered->get("a", &value).ok());
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().to_string(), "value");
    EXPECT_EQ(recovered->capture()->coverage, 1U);
}

TEST(TreeCoreSyncPageStore, OwnedSynchronousBackendCheckpointsReopensAndOutlivesCore)
{
    Config             config;
    TreeRuntimeOptions runtime_options;
    runtime_options.workers         = 1;
    runtime_options.backend_workers = 1;
    ASSERT_TRUE(TreeRuntime::create(runtime_options, &config.runtime).ok());
    auto store                                  = std::make_shared<PageMemStore>(4096);
    config.page_store                           = store.get();
    config.page_store_owner                     = store;
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.runtime_binding.limits.io_operations = 1;
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string large(12000, 'v');
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = large}}}).ok());
    status = core->checkpoint();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::shared_ptr<PublishedReadViewContext> snapshot;
    ASSERT_TRUE(core->capture_read_only_view(&snapshot).ok());
    EXPECT_TRUE(snapshot->detached());
    core.reset();
    GetView old;
    status = snapshot->get("a", &old);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(old.found());
    EXPECT_EQ(old.value().to_string(), large);
    auto                   binding = snapshot->binding();
    detail::HostCompletion closed;
    ASSERT_TRUE(
        snapshot->close_async([](void *context) { static_cast<detail::HostCompletion *>(context)->signal(); }, &closed)
            .ok());
    closed.wait();
    snapshot.reset();
    // The host can observe the read and close before the worker releases its
    // final page store authority reference.
    binding->cpu()->drain();
    binding->backend_cpu()->drain();
    status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView recovered;
    status = core->get("a", &recovered);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(recovered.found());
    EXPECT_EQ(recovered.value().to_string(), large);
    EXPECT_EQ(core->capture()->coverage, 1U);
}

TEST(TreeCoreSyncPageStore, RawSynchronousSnapshotCannotOutliveItsBackendDomain)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    PageMemStore store(4096);
    config.page_store  = &store;
    config.frame_bytes = 4096;
    std::shared_ptr<TreeCore> core;
    auto                      status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(core->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "value"}}}).ok());
    ASSERT_TRUE(core->checkpoint().ok());
    std::shared_ptr<PublishedReadViewContext> snapshot;
    ASSERT_TRUE(core->capture_read_only_view(&snapshot).ok());
    EXPECT_FALSE(snapshot->detached());
    GetView value;
    ASSERT_TRUE(snapshot->get("a", &value).ok());
    core.reset();
    EXPECT_EQ(snapshot->get("a", &value).code(), Code::kUnavailable);
    status = TreeCore::open(config, &core);
    ASSERT_TRUE(status.ok()) << status.to_string();
}
} // namespace
} // namespace crowdb::tree
