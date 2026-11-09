// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "crowdb-tree/btree/scan_packed.h"
#include "host_completion.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_snapshot.h"

#include <array>
#include <future>
#include <latch>
#include <utility>

namespace crowdb::tree
{
namespace
{
struct TestSnapshotBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestSnapshotBackend()
    {
        store.device = device;
    }
};

struct TestSnapshotContext
{
    std::shared_ptr<TreeRuntime>         runtime;
    std::shared_ptr<TestSnapshotBackend> backend = std::make_shared<TestSnapshotBackend>();
    Config                               config;
    std::shared_ptr<TreeCore>            core;

    explicit TestSnapshotContext(bool owned, TreeRuntimeOptions options = {})
    {
        EXPECT_TRUE(TreeRuntime::create(options, &runtime).ok());
        config.runtime                              = runtime;
        config.page_store                           = backend->device.get();
        config.async_page_store                     = &backend->store;
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 64;
        config.runtime_binding.limits.io_operations = 1;
        if (owned) {
            config.backend_owner = backend;
        }
    }

    Status open()
    {
        return TreeCore::open(config, &core);
    }

    Status put(uint64_t slot, const std::string &value)
    {
        return core->apply(slot, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}});
    }
};

TEST(PublishedReadViewContext, ExcludesLiveSourcesAndKeepsOneImmutableL1Coverage)
{
    TestSnapshotContext test(true);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.put(1, "old").ok());
    ASSERT_TRUE(test.core->flush().ok());
    ASSERT_TRUE(test.put(2, "new").ok());
    std::shared_ptr<PublishedReadViewContext> snapshot;
    status = PublishedReadViewContext::capture(test.core, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(snapshot->view()->coverage, 1U);
    EXPECT_TRUE(snapshot->view()->sources->tables.empty());
    EXPECT_EQ(snapshot->view()->sources->floor, 1U);
    ASSERT_TRUE(test.core->flush().ok());
    GetView result;
    status = snapshot->get("a", &result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.slot(), 1U);
    EXPECT_EQ(result.value().to_string(), "old");
}

TEST(PublishedReadViewContext, SharedBackendAndDomainOutliveCoreWithoutPinningRegistry)
{
    TestSnapshotContext test(true);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'x');
    ASSERT_TRUE(test.put(1, value).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedReadViewContext> snapshot;
    status = PublishedReadViewContext::capture(test.core, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(snapshot->detached());
    std::weak_ptr<LocalBackendState> old_local = test.core->local_backend();
    test.core.reset();
    EXPECT_TRUE(old_local.expired());
    EXPECT_EQ(test.open().code(), Code::kUnavailable);
    GetView result;
    status = snapshot->get("a", &result);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(result.found());
    EXPECT_EQ(result.value().to_string(), value);
    EXPECT_EQ(result.slot(), 1U);
    auto                   binding = snapshot->binding();
    detail::HostCompletion closed;
    ASSERT_TRUE(
        snapshot->close_async([](void *context) { static_cast<detail::HostCompletion *>(context)->signal(); }, &closed)
            .ok());
    closed.wait();
    snapshot.reset();
    // A completion can notify this host before its native worker drops the
    // callback owner. Let the fixture's workers finish before claiming the
    // same backend authority again.
    binding->cpu()->drain();
    binding->backend_cpu()->drain();
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(result.value().to_string(), value);
}

TEST(PublishedReadViewContext, RawBackendIdleHandleDoesNotBlockCloseAndOwnedReadSurvives)
{
    TestSnapshotContext test(false);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.put(1, "retained").ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedReadViewContext> snapshot;
    status = PublishedReadViewContext::capture(test.core, &snapshot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_FALSE(snapshot->detached());
    GetView owned;
    status = snapshot->get("a", &owned);
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(owned.found());
    test.core.reset();
    GetView after;
    status = snapshot->get("a", &after);
    EXPECT_EQ(status.code(), Code::kUnavailable) << status.to_string();
    EXPECT_FALSE(after.found());
    EXPECT_EQ(owned.value().to_string(), "retained");
    status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
}

TEST(PublishedReadViewContext, LastViewReleaseOnItsLaneLeavesAnAcceptedWindowUsable)
{
    TestSnapshotContext test(true);
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.put(1, "value").ok());
    ASSERT_TRUE(test.core->checkpoint().ok());

    struct Release
    {
        std::shared_ptr<PublishedReadViewContext> view;
        std::promise<void>                        returned;
    } release;

    ASSERT_TRUE(test.core->capture_read_only_view(&release.view).ok());
    auto               lane = release.view->lane();
    auto               cpu  = release.view->binding()->cpu();
    SnapshotReadWindow window;
    ASSERT_TRUE(release.view->prepare_window(&window).ok());
    test.core.reset();

    auto returned = release.returned.get_future();
    ASSERT_TRUE(cpu->try_submit(
                       lane,
                       [](void *context) noexcept {
                           auto &operation = *static_cast<Release *>(context);
                           operation.view.reset();
                           operation.returned.set_value();
                       },
                       &release)
                    .ok());
    ASSERT_EQ(returned.wait_for(std::chrono::seconds(3)), std::future_status::ready);

    struct Read
    {
        alignas(4096) std::array<uint8_t, 4096> bytes{};
        std::promise<Status> done;
    };

    auto read = std::make_shared<Read>();
    auto done = read->done.get_future();
    window.backend->submit_read(
        0, read->bytes.data(), read->bytes.size(),
        {
            .context     = read.get(),
            .complete_fn = [](void  *context,
                              Status status) { static_cast<Read *>(context)->done.set_value(std::move(status)); },
            .owner       = read,
        });
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_TRUE(done.get().ok());
}

TEST(PublishedReadViewContext, CloseWaitsForAcceptedWindowAndItsWorkerTail)
{
    TestSnapshotContext test(true);
    ASSERT_TRUE(test.open().ok());
    std::shared_ptr<PublishedReadViewContext> view;
    ASSERT_TRUE(test.core->capture_read_only_view(&view).ok());

    struct CloseResult
    {
        std::atomic<bool>  completed{false};
        std::promise<void> done;
    } close;

    struct Release
    {
        SnapshotReadWindow window;
        const CloseResult *close = nullptr;
        std::promise<bool> completed_inside_job;
    } release;

    release.close = &close;
    ASSERT_TRUE(view->prepare_window(&release.window).ok());
    auto done = close.done.get_future();
    ASSERT_TRUE(view->close_async(
                        [](void *context) {
                            auto &result = *static_cast<CloseResult *>(context);
                            result.completed.store(true, std::memory_order_release);
                            result.done.set_value();
                        },
                        &close)
                    .ok());
    SnapshotReadWindow rejected;
    EXPECT_EQ(view->prepare_window(&rejected).code(), Code::kUnavailable);
    EXPECT_EQ(done.wait_for(std::chrono::milliseconds(1)), std::future_status::timeout);

    auto inside = release.completed_inside_job.get_future();
    ASSERT_TRUE(view->binding()
                    ->cpu()
                    ->try_submit(
                        view->lane(),
                        [](void *context) noexcept {
                            auto &job  = *static_cast<Release *>(context);
                            job.window = {};
                            job.completed_inside_job.set_value(job.close->completed.load(std::memory_order_acquire));
                        },
                        &release)
                    .ok());
    ASSERT_EQ(inside.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_FALSE(inside.get());
    EXPECT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
}

void expect_async_read_after_view_close(bool scan)
{
    TestSnapshotContext test(true, {.workers = 1, .backend_workers = 1});
    ASSERT_TRUE(test.open().ok());
    const std::string payload(9000, 'v');
    ASSERT_TRUE(test.put(1, payload).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedReadViewContext> view;
    ASSERT_TRUE(test.core->capture_read_only_view(&view).ok());
    test.core.reset();

    struct DeferredRead
    {
        PageAddr        address;
        void           *output;
        size_t          size;
        AsyncCompletion completion;
    };

    auto submitted                 = std::make_shared<std::promise<DeferredRead>>();
    auto pending                   = submitted->get_future();
    test.backend->store.defer_read = [submitted](PageAddr address, void *output, size_t size,
                                                 AsyncCompletion &completion) mutable {
        if (!submitted) {
            return false;
        }
        std::exchange(submitted, nullptr)
            ->set_value({.address = address, .output = output, .size = size, .completion = std::move(completion)});
        return true;
    };

    auto read   = std::make_shared<std::promise<Status>>();
    auto result = read->get_future();
    if (scan) {
        ASSERT_TRUE(
            view->scan_async(
                    {},
                    [read, &payload](Status status, ScanPackedBuf packed, size_t count, bool truncated) {
                        if (status.ok()) {
                            EXPECT_EQ(count, 1U);
                            EXPECT_FALSE(truncated);
                            EXPECT_GE(packed.size(), payload.size());
                            if (packed.size() >= payload.size()) {
                                EXPECT_EQ(
                                    Slice(packed.data() + packed.size() - payload.size(), payload.size()).to_string(),
                                    payload);
                            }
                        }
                        read->set_value(std::move(status));
                    })
                .ok());
    }
    else {
        ASSERT_TRUE(view->get_async("a",
                                    [read, &payload](Status status, GetView value) {
                                        if (status.ok()) {
                                            EXPECT_TRUE(value.found());
                                            EXPECT_EQ(value.slot(), 1U);
                                            EXPECT_EQ(value.value().to_string(), payload);
                                        }
                                        read->set_value(std::move(status));
                                    })
                        .ok());
    }
    ASSERT_EQ(pending.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(result.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    std::promise<void> unrelated;
    auto               cpu_available = unrelated.get_future();
    ASSERT_TRUE(view->binding()
                    ->cpu()
                    ->try_submit(
                        view->lane(),
                        [](void *context) noexcept { static_cast<std::promise<void> *>(context)->set_value(); },
                        &unrelated)
                    .ok());
    EXPECT_EQ(cpu_available.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    std::promise<void> closed;
    auto               done = closed.get_future();
    ASSERT_TRUE(
        view->close_async([](void *context) { static_cast<std::promise<void> *>(context)->set_value(); }, &closed)
            .ok());
    EXPECT_EQ(view->get_async("a", [](Status /*status*/, GetView /*value*/) { ADD_FAILURE(); }).code(),
              Code::kUnavailable);
    EXPECT_EQ(view->scan_async({}, [](Status /*status*/, ScanPackedBuf /*packed*/, size_t /*count*/,
                                      bool /*truncated*/) { ADD_FAILURE(); })
                  .code(),
              Code::kUnavailable);
    EXPECT_EQ(done.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    view.reset();

    auto io = pending.get();
    io.completion.complete(test.backend->device->read_at(io.address, static_cast<uint8_t *>(io.output), io.size));
    io.completion = {};
    ASSERT_EQ(result.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto status = result.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    // Final runtime shutdown is a separate host operation. It must cover
    // cleanup in the view close completion after the callback signals done.
    test.runtime->drain_cpu();
}

TEST(PublishedReadViewContext, AsyncGetCompletesColdReadAfterClosingAndReleasingView)
{
    expect_async_read_after_view_close(false);
}

TEST(PublishedReadViewContext, AsyncScanCompletesColdReadAfterClosingAndReleasingView)
{
    expect_async_read_after_view_close(true);
}

TEST(PublishedReadViewContext, DetachedBackendFallbackDoesNotDelaySourceTreeClose)
{
    TestSnapshotContext test(true, {.workers = 1, .backend_workers = 1, .backend_queue_limit = 4});
    ASSERT_TRUE(test.open().ok());
    const std::string payload(9000, 'v');
    ASSERT_TRUE(test.put(1, payload).ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    test.core->operation_lifetime().drain_accepted();
    test.core->binding()->drain();
    std::shared_ptr<PublishedReadViewContext> view;
    ASSERT_TRUE(test.core->capture_read_only_view(&view).ok());

    struct Gate
    {
        std::latch entered{1};
        std::latch release{1};
    } gate;

    auto backend = test.core->binding()->backend_cpu();
    auto filler  = std::make_shared<detail::FlushRuntime::Lane>(8);
    ASSERT_TRUE(backend
                    ->try_submit(
                        filler,
                        [](void *context) noexcept {
                            auto &state = *static_cast<Gate *>(context);
                            state.entered.count_down();
                            state.release.wait();
                        },
                        &gate)
                    .ok());
    gate.entered.wait();
    auto full = Status::Ok();
    while (full.ok()) {
        full = backend->try_submit(filler, [](void *) noexcept {}, nullptr);
    }
    EXPECT_EQ(full.code(), Code::kResourceExhausted);

    auto       read     = std::make_shared<std::promise<std::pair<Status, GetView>>>();
    auto       result   = read->get_future();
    const auto accepted = view->get_async(
        "a", [read](Status status, GetView value) { read->set_value({std::move(status), std::move(value)}); });
    auto       closing      = std::async(std::launch::async, [&test] { test.core->close_and_drain(); });
    const auto closed       = closing.wait_for(std::chrono::seconds(3));
    const auto read_pending = result.wait_for(std::chrono::milliseconds(0));
    gate.release.count_down();
    closing.get();
    EXPECT_EQ(closed, std::future_status::ready);
    EXPECT_EQ(read_pending, std::future_status::timeout);
    ASSERT_TRUE(accepted.ok()) << accepted.to_string();
    ASSERT_EQ(result.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto [status, value] = result.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(value.value().to_string(), payload);
    test.core.reset();
    view.reset();
    test.runtime->drain_cpu();
}

TEST(PublishedReadViewContext, OversizeWindowFailsBeforeIoOrAdmissionAndSingleIoGetsCanRepeat)
{
    TestSnapshotContext test(true);
    auto                status = test.open();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(test.put(1, "one").ok());
    ASSERT_TRUE(test.core->checkpoint().ok());
    std::shared_ptr<PublishedReadViewContext> snapshot;
    ASSERT_TRUE(PublishedReadViewContext::capture(test.core, &snapshot).ok());
    SnapshotReadWindow window;
    EXPECT_EQ(snapshot->prepare_window(SIZE_MAX, &window).code(), Code::kResourceExhausted);
    EXPECT_FALSE(window.loader);
    EXPECT_FALSE(window.ticket);
    GetView first;
    status = snapshot->get("a", &first);
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView second;
    status = snapshot->get("a", &second);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(first.value().to_string(), "one");
    EXPECT_EQ(second.value().to_string(), "one");
}
} // namespace
} // namespace crowdb::tree
