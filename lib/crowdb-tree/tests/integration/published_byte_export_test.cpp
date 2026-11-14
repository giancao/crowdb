// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "crowdb-common/crc32c.h"
#include "crowdb-tree/snapshot/snapshot_io.h"
#include "local_checkpoint_test_support.h"
#include "snapshot/published_byte_export.h"

#include <future>

namespace crowdb::tree
{
namespace
{
struct TestByteBackend
{
    std::shared_ptr<MemPageStore>   device = std::make_shared<MemPageStore>(4096);
    test_write::TestCheckpointStore store;

    TestByteBackend()
    {
        store.device = device;
    }
};

struct TestByteExport
{
    std::shared_ptr<TestByteBackend> backend = std::make_shared<TestByteBackend>();
    Config                           config;
    std::unique_ptr<Crowdbtree>      tree;

    TestByteExport()
    {
        EXPECT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
        config.page_store                           = backend->device.get();
        config.async_page_store                     = &backend->store;
        config.backend_owner                        = backend;
        config.frame_bytes                          = 4096;
        config.max_inline_value                     = 64;
        config.runtime_binding.limits.io_operations = 1;
    }

    [[nodiscard]] Status open()
    {
        return Crowdbtree::open(config, &tree);
    }

    static Status collect(SnapshotExport &exporter, std::string *wire, std::vector<SnapshotChunk> *retained = nullptr)
    {
        bool done = false;
        while (!done) {
            SnapshotChunk chunk;
            auto          status = exporter.next_owned(&chunk, &done);
            if (!status.ok()) {
                return status;
            }
            if (!chunk.bytes.empty()) {
                if (!chunk.owner || !chunk.allocation_budget) {
                    return Status::internal_error("runtime export returned unaccounted bytes");
                }
                wire->append(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size());
            }
            if (retained != nullptr) {
                retained->push_back(std::move(chunk));
            }
        }
        return Status::Ok();
    }
};

TEST(PublishedByteExport, PortableMatchesExistingWireAndRetainsOneCapturedCoverage)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    const std::string value(12000, 'v');
    ASSERT_TRUE(test.tree
                    ->apply(1,
                            Batch{
                                .ops = {{.key = "a", .kind = OpKind::kPut, .value = value},
                                        {.key = "empty", .kind = OpKind::kPut, .value = ""},
                                        {.key = "gone", .kind = OpKind::kPut, .value = "old"}}
    })
                    .ok());
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "gone", .kind = OpKind::kDelete, .value = ""}}}).ok());
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kPortable, 7, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(exporter->at_slot(), 2U);
    ASSERT_TRUE(test.tree->apply(3, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = "changed"}}}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    std::vector<leaf_entry> expected;
    expected.push_back({.key = "a", .cell = encode_cell_buf(1, OpKind::kPut, value)});
    expected.push_back({.key = "empty", .cell = encode_cell_buf(1, OpKind::kPut, "")});
    expected.push_back({.key = "gone", .cell = encode_cell_buf(2, OpKind::kDelete)});
    SnapshotExport reference(std::make_shared<Snapshot>(2, std::move(expected)), 4096);
    std::string    expected_wire;
    bool           done = false;
    while (!done) {
        std::string piece;
        ASSERT_TRUE(reference.next_chunk(&piece, &done).ok());
        expected_wire += piece;
    }
    EXPECT_EQ(exporter->total_bytes(), expected_wire.size());
    EXPECT_EQ(exporter->final_crc32c(), reference.final_crc32c());
    std::string                wire;
    std::vector<SnapshotChunk> retained;
    status = TestByteExport::collect(*exporter, &wire, &retained);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(wire, expected_wire);
    EXPECT_EQ(exporter->offset(), exporter->total_bytes());
    EXPECT_EQ(test.config.runtime->snapshot().reserved.io_operations, 0U);
    exporter.reset();
    test.tree.reset();
    std::string owned_again;
    for (const auto &chunk : retained) {
        if (!chunk.bytes.empty()) {
            owned_again.append(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size());
        }
    }
    EXPECT_EQ(owned_again, wire);
}

TEST(PublishedByteExport, NativeColdOverflowUsesTheSamePidLeaseForBothPasses)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    const std::string value(14000, 'n');
    ASSERT_TRUE(test.tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}}).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kNative, 4093, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto  next = TreeCoreAccess::get(*test.tree)->capture()->next_page_id;
    std::string wire;
    status = TestByteExport::collect(*exporter, &wire);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(TreeCoreAccess::get(*test.tree)->capture()->next_page_id, next);
    EXPECT_EQ(wire.size(), exporter->total_bytes());
    const auto crc = crowdb::common::crc32c(reinterpret_cast<const uint8_t *>(wire.data()), wire.size() - 4);
    EXPECT_EQ(crc, exporter->final_crc32c());
    Config target;
    target.frame_bytes = 4096;
    Crowdbtree     copy(target);
    SnapshotImport importer(copy);
    ASSERT_TRUE(importer.feed(wire).ok());
    uint64_t slot = 0;
    status        = importer.finish(&slot);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(slot, 1U);
    std::string restored;
    ASSERT_TRUE(copy.get("a", &slot, &restored));
    EXPECT_EQ(restored, value);
}

TEST(PublishedByteExport, AsyncPreflightAndOutputReleaseWorkersWhileReadingColdPages)
{
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    auto store              = std::make_shared<test_write::TestCheckpointStore>();
    store->device           = std::make_shared<MemPageStore>(4096);
    config.page_store       = store->device.get();
    config.backend_owner    = store;
    config.frame_bytes      = 4096;
    config.max_inline_value = 64;
    std::unique_ptr<Crowdbtree> tree;
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    const std::string value(14000, 'v');
    ASSERT_TRUE(tree->apply(1, Batch{.ops = {{.key = "a", .kind = OpKind::kPut, .value = value}}}).ok());
    ASSERT_TRUE(tree->flush_with_persistent().ok());
    tree.reset();
    config.async_page_store = store.get();
    ASSERT_TRUE(Crowdbtree::open(config, &tree).ok());
    std::shared_ptr<PublishedReadViewContext> view;
    ASSERT_TRUE(TreeCoreAccess::get(*tree)->capture_read_only_view(&view).ok());
    auto binding = view->binding();

    using Deferred    = std::promise<std::function<void()>>;
    auto pauses       = std::make_shared<std::array<Deferred, 2>>();
    auto preflight_io = (*pauses)[0].get_future();
    auto output_io    = (*pauses)[1].get_future();
    auto armed        = std::make_shared<std::atomic<unsigned>>(1);
    store->defer_read = [pauses, armed, device = store->device](PageAddr at, void *out, size_t len,
                                                                AsyncCompletion &done) {
        const auto phase = armed->exchange(0);
        if (phase == 0) {
            return false;
        }
        (*pauses)[phase - 1].set_value([at, out, len, done = std::move(done), device] {
            done.complete(device->read_at(at, static_cast<uint8_t *>(out), len));
        });
        return true;
    };
    auto check_progress = [&](const std::shared_ptr<ApplyDispatcher> &dispatcher) {
        struct Marker
        {
            std::promise<void>    done;
            ApplyDispatcher::Node node{[](void *context) noexcept { static_cast<Marker *>(context)->done.set_value(); },
                                       this};
        };
        auto marker = std::make_shared<Marker>();
        auto done   = marker->done.get_future();
        dispatcher->request(marker->node, marker);
        EXPECT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    };

    using OpenResult = std::pair<Status, std::shared_ptr<PublishedByteExport>>;
    auto opened      = std::make_shared<std::promise<OpenResult>>();
    auto open_result = opened->get_future();
    ASSERT_TRUE(PublishedByteExport::create_portable_async(
                    std::move(view), 2049,
                    [opened](Status status, std::shared_ptr<PublishedByteExport> stream) {
                        opened->set_value({std::move(status), std::move(stream)});
                    })
                    .ok());
    ASSERT_EQ(preflight_io.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(open_result.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    check_progress(binding->view_cpu_progress());
    check_progress(binding->view_backend_progress());
    tree.reset();
    preflight_io.get()();
    ASSERT_EQ(open_result.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto [status, stream] = open_result.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(stream);
    EXPECT_EQ(stream->info().slot, 1U);

    std::string wire;
    bool        done = false;
    armed->store(2);
    while (!done) {
        using ReadResult = std::tuple<Status, SnapshotChunk, bool>;
        auto completed   = std::make_shared<std::promise<ReadResult>>();
        auto result      = completed->get_future();
        ASSERT_TRUE(stream
                        ->next_async([completed](Status read, SnapshotChunk chunk, bool end) {
                            completed->set_value({std::move(read), std::move(chunk), end});
                        })
                        .ok());
        if (wire.empty()) {
            ASSERT_EQ(output_io.wait_for(std::chrono::seconds(3)), std::future_status::ready);
            EXPECT_EQ(result.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
            EXPECT_EQ(stream->next_async([](Status, SnapshotChunk, bool) { ADD_FAILURE(); }).code(),
                      Code::kUnavailable);
            check_progress(binding->view_cpu_progress());
            check_progress(binding->view_backend_progress());
            output_io.get()();
        }
        ASSERT_EQ(result.wait_for(std::chrono::seconds(3)), std::future_status::ready);
        auto [read, chunk, end] = result.get();
        ASSERT_TRUE(read.ok()) << read.to_string();
        wire.append(reinterpret_cast<const char *>(chunk.bytes.data()), chunk.bytes.size());
        done = end;
    }
    EXPECT_EQ(wire.size(), stream->info().total_bytes);
    EXPECT_EQ(crowdb::common::crc32c(reinterpret_cast<const uint8_t *>(wire.data()), wire.size() - 4),
              stream->info().final_crc);
    Crowdbtree     target(Config{});
    SnapshotImport importer(target);
    ASSERT_TRUE(importer.feed(wire).ok());
    ASSERT_TRUE(importer.finish(nullptr).ok());
    std::string restored;
    uint64_t    slot = 0;
    ASSERT_TRUE(target.get("a", &slot, &restored));
    EXPECT_EQ(restored, value);
}

TEST(PublishedByteExport, NativeFreshOverflowRewindsWithoutReservingAnotherInterval)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    const std::string value(14000, 'f');
    ASSERT_TRUE(test.tree->put("fresh", value).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    auto                           *core   = TreeCoreAccess::get(*test.tree);
    const auto                      before = core->capture()->next_page_id;
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kNative, 4093, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto leased = core->capture()->next_page_id;
    EXPECT_GT(leased, before);
    std::string wire;
    status = TestByteExport::collect(*exporter, &wire);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(core->capture()->next_page_id, leased);
    Config target;
    target.frame_bytes = 4096;
    Crowdbtree     copy(target);
    SnapshotImport importer(copy);
    ASSERT_TRUE(importer.feed(wire).ok());
    status = importer.finish(nullptr);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string restored;
    uint64_t    slot = 0;
    ASSERT_TRUE(copy.get("fresh", &slot, &restored));
    EXPECT_EQ(restored, value);
}

TEST(PublishedByteExport, EmptyPortableSupportsEveryHeaderAndTrailerByteBoundary)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kPortable, 1, &exporter);
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::string wire;
    status = TestByteExport::collect(*exporter, &wire);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(wire.size(), 29U);
    EXPECT_EQ(exporter->total_bytes(), 29U);
    EXPECT_EQ(exporter->offset(), 29U);
}

TEST(PublishedByteExport, OversizeChunkFailsBeforeHandoffOrCapture)
{
    TestByteExport test;
    auto           opened = test.open();
    ASSERT_TRUE(opened.ok()) << opened.to_string();
    ASSERT_TRUE(test.tree->put("a", "pending").ok());
    auto                           *core   = TreeCoreAccess::get(*test.tree);
    const auto                      before = core->capture();
    std::unique_ptr<SnapshotExport> exporter;
    auto status = snapshot_export_begin(*test.tree, snapshot_format::kPortable, kSnapshotChunkBytes + 1, &exporter);
    EXPECT_EQ(status.code(), Code::kInvalidArgument);
    EXPECT_FALSE(exporter);
    EXPECT_EQ(core->capture(), before);
    EXPECT_EQ(core->capture()->coverage, 0U);
}
} // namespace
} // namespace crowdb::tree
