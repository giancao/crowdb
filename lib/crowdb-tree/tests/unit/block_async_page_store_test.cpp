// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "async_completion_adapter.h"
#include "btree/tree_core.h"
#include "crowdb-common/diskio_uring.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "test_tmp.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <latch>
#include <memory>
#include <thread>
#include <utility>
#include <vector>

using namespace crowdb::tree;

namespace
{
crowdb::common::DiskIOUring make_uring()
{
    crowdb::common::Topology       topology;
    crowdb::common::PipelineConfig pipeline;
    pipeline.entries = 256;
    pipeline.mode    = crowdb::common::PollingMode::Hybrid;
    topology.pipelines.push_back(pipeline);
    return crowdb::common::DiskIOUring(std::move(topology));
}

bool wait_for(const std::atomic<unsigned> &completed)
{
    for (unsigned i = 0; i < 200; ++i) {
        if (completed.load(std::memory_order_acquire) != 0) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return completed.load(std::memory_order_acquire) != 0;
}

struct PollPause
{
    struct State
    {
        std::promise<int> entered;
        std::latch        resume{1};
    };

    PollPause(crowdb::common::DiskIOUring &uring, int fd)
    {
        uring.submit_fsync(fd, [state = state](int result) {
            state->entered.set_value(result);
            if (result >= 0) {
                state->resume.wait();
            }
        });
    }

    ~PollPause()
    {
        release();
    }

    void release()
    {
        if (state) {
            state->resume.count_down();
            state.reset();
        }
    }

    std::shared_ptr<State> state   = std::make_shared<State>();
    std::future<int>       entered = state->entered.get_future();
};

std::future<Status> write_bytes(BlockAsyncPageStore &store, uint64_t offset, size_t count, uint8_t value)
{
    auto bytes   = std::make_shared<std::vector<uint8_t>>(count, value);
    auto promise = std::make_shared<std::promise<Status>>();
    store.submit_write(offset, bytes->data(), bytes->size(),
                       crowdb::tree::detail::own_async_completion(
                           [bytes, promise](Status status) { promise->set_value(std::move(status)); }));
    return promise->get_future();
}
} // namespace

TEST(BlockAsyncPageStore, StartupOpensDiscoveredNamesWithoutRecreatingAMissingExtent)
{
    crowdb::tree_test::TempDir directory;
    auto                       uring = make_uring();
    ASSERT_TRUE(uring.valid());
    const auto        path         = std::filesystem::path(directory.path);
    const std::string noncanonical = "1-3.blk-0001old";
    const std::string missing      = "1-3.blk-0003";
    const std::string unrelated    = "1-4.blk-0002";
    std::ofstream(path / noncanonical).put('a');
    ASSERT_TRUE(std::filesystem::exists(path / noncanonical));

    std::unique_ptr<BlockPageStore> store;
    ASSERT_TRUE(BlockPageStore::prepare_blocks(directory.path, 1, 3, 4096, 1, &store).ok());
    EXPECT_EQ(store->num_extents(), 0U);
    std::unique_ptr<BlockPageStore> control;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 1, 5, 4096, 1, &control).ok());
    uint64_t  local = 0;
    PollPause paused(uring, control->fd_for_offset(0, &local));
    ASSERT_EQ(paused.entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(paused.entered.get(), 0);
    const auto       inflight = uring.total_in_flight_count();
    const std::array names{noncanonical, missing, unrelated};
    auto             submit = [&] {
        auto result = std::make_shared<std::promise<Status>>();
        auto done   = result->get_future();
        EXPECT_TRUE(store
                        ->submit_open_blocks(names, uring, detail::own_async_completion([result](Status status) {
                                                 result->set_value(std::move(status));
                                             }))
                        .ok());
        return done;
    };
    auto first = submit();
    EXPECT_EQ(uring.total_in_flight_count(), inflight + 2);
    EXPECT_EQ(first.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    paused.release();
    ASSERT_EQ(first.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(first.get().code(), Code::kIoError);
    EXPECT_EQ(store->num_extents(), 1U);
    EXPECT_FALSE(std::filesystem::exists(path / missing));
    EXPECT_FALSE(std::filesystem::exists(path / unrelated));
    EXPECT_FALSE(std::filesystem::exists(path / "1-3.blk-0001"));

    std::ofstream(path / missing).put('b');
    auto retry = submit();
    ASSERT_EQ(retry.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_TRUE(retry.get().ok());
    EXPECT_EQ(store->num_extents(), 2U);
    uint8_t value = 0;
    ASSERT_TRUE(store->read_at(4096, &value, 1).ok());
    EXPECT_EQ(value, 'a');
    ASSERT_TRUE(store->read_at(3 * 4096, &value, 1).ok());
    EXPECT_EQ(value, 'b');
}

TEST(BlockAsyncPageStore, SizeProbeJoinsSparseExtentsBeforeReturningTheLogicalEnd)
{
    crowdb::tree_test::TempDir      directory;
    std::unique_ptr<BlockPageStore> store;
    auto                            uring = make_uring();
    ASSERT_TRUE(uring.valid());
    constexpr uint64_t block_size = 64 * 1024;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, block_size, 4096, &store).ok());
    std::array<uint8_t, 4096> bytes{};
    const auto                sparse_offset = 3 * block_size;
    ASSERT_TRUE(store->write_at(sparse_offset + bytes.size(), bytes.data(), bytes.size()).ok());
    BlockAsyncPageStore asynchronous(store.get(), &uring);
    uint64_t            local = 0;
    PollPause           paused(uring, store->fd_for_offset(0, &local));
    ASSERT_EQ(paused.entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(paused.entered.get(), 0);
    const auto inflight = uring.total_in_flight_count();

    struct Result
    {
        uint64_t             size = UINT64_MAX;
        std::promise<Status> status;
    };

    auto result = std::make_shared<Result>();
    auto done   = result->status.get_future();
    ASSERT_TRUE(asynchronous
                    .submit_size(&result->size, detail::own_async_completion([result](Status status) {
                        result->status.set_value(std::move(status));
                    }))
                    .ok());
    EXPECT_EQ(uring.total_in_flight_count(), inflight + 2);
    EXPECT_EQ(done.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    paused.release();
    ASSERT_EQ(done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_TRUE(done.get().ok());
    EXPECT_EQ(result->size, sparse_offset + (2 * bytes.size()));
}

TEST(BlockAsyncPageStore, LocalOpenYieldsDuringSizeProbeAndPropagatesProbeFailure)
{
    struct Backend
    {
        crowdb::common::DiskIOUring          uring = make_uring();
        std::unique_ptr<BlockPageStore>      file;
        std::unique_ptr<BlockAsyncPageStore> asynchronous;
    };

    crowdb::tree_test::TempDir directory;
    auto                       backend = std::make_shared<Backend>();
    ASSERT_TRUE(backend->uring.valid());
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, 64 * 1024, 4096, &backend->file).ok());
    backend->asynchronous = std::make_unique<BlockAsyncPageStore>(backend->file.get(), &backend->uring);
    Config config;
    ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
    config.frame_bytes      = 4096;
    config.page_store       = backend->file.get();
    config.async_page_store = backend->asynchronous.get();
    config.backend_owner    = backend;

    std::shared_ptr<TreeCore> writer;
    ASSERT_TRUE(TreeCore::open(config, &writer).ok());
    ASSERT_TRUE(writer->apply(1, Batch{.ops = {{.key = "k", .kind = OpKind::kPut, .value = "durable"}}}).ok());
    ASSERT_TRUE(writer->checkpoint().ok());
    writer.reset();

    using Reply = std::pair<Status, std::shared_ptr<TreeCore>>;
    auto submit = [&](const Config &options, std::future<Reply> *received) {
        auto reply = std::make_shared<std::promise<Reply>>();
        *received  = reply->get_future();
        return TreeCore::open_local_async(options, [reply](Status status, std::shared_ptr<TreeCore> core) {
            reply->set_value({std::move(status), std::move(core)});
        });
    };
    crowdb::common::DiskIOUring unavailable(crowdb::common::Topology{});
    BlockAsyncPageStore         failed(backend->file.get(), &unavailable);
    config.async_page_store = &failed;
    std::future<Reply> received;
    ASSERT_TRUE(submit(config, &received).ok());
    ASSERT_EQ(received.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto [failure, absent] = received.get();
    EXPECT_EQ(failure.code(), Code::kIoError);
    EXPECT_FALSE(absent);

    config.async_page_store = backend->asynchronous.get();
    uint64_t  local         = 0;
    PollPause paused(backend->uring, backend->file->fd_for_offset(0, &local));
    ASSERT_EQ(paused.entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(paused.entered.get(), 0);
    const auto inflight = backend->uring.total_in_flight_count();
    ASSERT_TRUE(submit(config, &received).ok());
    EXPECT_GT(backend->uring.total_in_flight_count(), inflight);
    EXPECT_EQ(received.wait_for(std::chrono::seconds(0)), std::future_status::timeout);

    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(config.runtime, {}, &binding).ok());
    std::promise<void> probe;
    auto               progress = probe.get_future();
    ASSERT_TRUE(binding->backend_cpu()
                    ->try_submit(
                        binding->backend_lane(),
                        [](void *context) noexcept { static_cast<std::promise<void> *>(context)->set_value(); }, &probe)
                    .ok());
    EXPECT_EQ(progress.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    paused.release();
    binding->backend_cpu()->drain();
    ASSERT_EQ(received.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    auto [status, opened] = received.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    GetView value;
    ASSERT_TRUE(opened->get("k", &value).ok());
    ASSERT_TRUE(value.found());
    EXPECT_EQ(value.value().to_string(), "durable");
    EXPECT_EQ(opened->durable_coverage(), 1U);
}

TEST(BlockAsyncPageStore, MissingExtentOpenIsSharedWhileOtherExtentsCanPrepare)
{
    crowdb::tree_test::TempDir      directory;
    std::unique_ptr<BlockPageStore> store;
    auto                            uring = make_uring();
    ASSERT_TRUE(uring.valid());
    constexpr uint64_t block_size = 64 * 1024;
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, block_size, 4096, &store).ok());
    BlockAsyncPageStore asynchronous(store.get(), &uring);
    uint64_t            local = 0;
    PollPause           paused(uring, store->fd_for_offset(0, &local));
    ASSERT_EQ(paused.entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(paused.entered.get(), 0);
    const auto initial = uring.total_in_flight_count();

    std::array<std::future<Status>, 9> completions;
    std::array<std::jthread, 8>        writers;
    for (size_t i = 0; i < writers.size(); ++i) {
        writers[i] = std::jthread([&, i] {
            completions[i] = write_bytes(asynchronous, block_size + (i * 4096), 4096, static_cast<uint8_t>(i + 1));
        });
    }
    completions.back() = write_bytes(asynchronous, block_size * 2, 4096, 9);
    for (auto &writer : writers) {
        writer.join();
    }
    EXPECT_EQ(uring.total_in_flight_count(), initial + 2);
    EXPECT_EQ(store->num_extents(), 1U);
    for (auto &done : completions) {
        EXPECT_EQ(done.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    }
    paused.release();
    for (auto &done : completions) {
        ASSERT_EQ(done.wait_for(std::chrono::seconds(5)), std::future_status::ready);
        const auto status = done.get();
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    EXPECT_EQ(store->num_extents(), 3U);
    std::array<uint8_t, 4096> result{};
    for (size_t i = 0; i < writers.size(); ++i) {
        ASSERT_TRUE(store->read_at(block_size + (i * 4096), result.data(), result.size()).ok());
        EXPECT_TRUE(std::ranges::all_of(result, [i](uint8_t byte) { return byte == i + 1; }));
    }
    ASSERT_TRUE(store->read_at(2 * block_size, result.data(), result.size()).ok());
    EXPECT_TRUE(std::ranges::all_of(result, [](uint8_t byte) { return byte == 9; }));
}

TEST(BlockAsyncPageStore, FailedSharedOpenLeavesCrossExtentWriteUntouchedAndCanRetry)
{
    crowdb::tree_test::TempDir      directory;
    std::unique_ptr<BlockPageStore> store;
    auto                            uring = make_uring();
    ASSERT_TRUE(uring.valid());
    ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, 8192, 4096, &store).ok());
    BlockAsyncPageStore asynchronous(store.get(), &uring);
    const auto          blocked = std::filesystem::path(directory.path) / "0-0.blk-0001";
    ASSERT_TRUE(std::filesystem::create_directory(blocked));
    std::array<uint8_t, 4096> original;
    original.fill(41);
    ASSERT_TRUE(store->write_at(4096, original.data(), original.size()).ok());
    uint64_t  local = 0;
    PollPause paused(uring, store->fd_for_offset(0, &local));
    ASSERT_EQ(paused.entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(paused.entered.get(), 0);
    const auto initial = uring.total_in_flight_count();
    auto       first   = write_bytes(asynchronous, 4096, 8192, 7);
    auto       second  = write_bytes(asynchronous, 12288, 4096, 8);
    EXPECT_EQ(uring.total_in_flight_count(), initial + 1);
    paused.release();
    ASSERT_EQ(first.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(second.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    EXPECT_EQ(first.get().code(), Code::kIoError);
    EXPECT_EQ(second.get().code(), Code::kIoError);
    std::array<uint8_t, 4096> result{};
    ASSERT_TRUE(store->read_at(4096, result.data(), result.size()).ok());
    EXPECT_EQ(result, original);
    EXPECT_EQ(store->num_extents(), 1U);

    ASSERT_TRUE(std::filesystem::remove(blocked));
    auto retry = write_bytes(asynchronous, 4096, 8192, 7);
    ASSERT_EQ(retry.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    const auto status = retry.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    std::array<uint8_t, 8192> combined{};
    ASSERT_TRUE(store->read_at(4096, combined.data(), combined.size()).ok());
    EXPECT_TRUE(std::ranges::all_of(combined, [](uint8_t byte) { return byte == 7; }));
    EXPECT_EQ(store->num_extents(), 2U);
}

TEST(BlockAsyncPageStore, CrossExtentReadAfterReopenMatchesSync)
{
    struct ReadRange
    {
        uint64_t block_size;
        uint64_t offset;
        size_t   length;
    };

    for (const auto range : {
             ReadRange{.block_size = 64 * 1024 * 1024,  .offset = 133259264, .length = 1212416},
             ReadRange{.block_size = 128 * 1024 * 1024, .offset = 133259264, .length = 1212416},
             ReadRange{.block_size = 8192,              .offset = 4096,      .length = 16384  },
             ReadRange{.block_size = 8192,              .offset = 8192,      .length = 12288  }
    }) {
        SCOPED_TRACE(::testing::Message() << range.block_size << ':' << range.offset << '+' << range.length);
        crowdb::tree_test::TempDir      directory;
        auto                            uring = make_uring();
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, range.block_size, 4096, &store).ok());
        std::vector<uint8_t> expected(range.length);
        for (size_t i = 0; i < expected.size(); ++i) {
            expected[i] = static_cast<uint8_t>(((i / 4096) + i) % 251);
        }
        ASSERT_TRUE(store->write_at(range.offset, expected.data(), expected.size()).ok());
        ASSERT_TRUE(store->sync().ok());
        store.reset();
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, range.block_size, 4096, &store).ok());
        for (int fd : store->all_extent_fds()) {
            uring.register_fd(fd);
        }
        std::unique_ptr<void, decltype(&std::free)> output(std::aligned_alloc(4096, range.length), &std::free);
        ASSERT_NE(output, nullptr);
        ASSERT_TRUE(store->read_at(range.offset, static_cast<uint8_t *>(output.get()), range.length).ok());
        ASSERT_TRUE(std::equal(expected.begin(), expected.end(), static_cast<uint8_t *>(output.get())));
        std::fill_n(static_cast<uint8_t *>(output.get()), range.length, 0);
        BlockAsyncPageStore   asynchronous(store.get(), &uring);
        std::atomic<unsigned> completed{0};
        Status                status;
        asynchronous.submit_read(range.offset, output.get(), range.length,
                                 crowdb::tree::detail::own_async_completion([&](Status result) {
                                     status = std::move(result);
                                     completed.fetch_add(1, std::memory_order_release);
                                 }));
        ASSERT_TRUE(wait_for(completed));
        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_EQ(completed.load(), 1U);
        EXPECT_TRUE(std::equal(expected.begin(), expected.end(), static_cast<uint8_t *>(output.get())));
    }
}

TEST(BlockAsyncPageStore, CrossExtentReadRejectsMissingAndTruncatedTail)
{
    for (bool missing : {false, true}) {
        SCOPED_TRACE(missing);
        crowdb::tree_test::TempDir      directory;
        auto                            uring = make_uring();
        std::unique_ptr<BlockPageStore> store;
        ASSERT_TRUE(BlockPageStore::open_blocks(directory.path, 0, 0, 8192, 4096, &store).ok());
        std::vector<uint8_t> expected(12288, 0xA5);
        ASSERT_TRUE(store->write_at(4096, expected.data(), expected.size()).ok());
        if (missing) {
            ASSERT_TRUE(store->delete_block(1).ok());
        }
        else {
            uint64_t local = 0;
            ASSERT_EQ(::ftruncate(store->fd_for_offset(8192, &local), 4096), 0);
        }
        for (int fd : store->all_extent_fds()) {
            uring.register_fd(fd);
        }
        std::unique_ptr<void, decltype(&std::free)> output(std::aligned_alloc(4096, expected.size()), &std::free);
        ASSERT_NE(output, nullptr);
        BlockAsyncPageStore   asynchronous(store.get(), &uring);
        std::atomic<unsigned> completed{0};
        Status                status;
        asynchronous.submit_read(4096, output.get(), expected.size(),
                                 crowdb::tree::detail::own_async_completion([&](Status result) {
                                     status = std::move(result);
                                     completed.fetch_add(1, std::memory_order_release);
                                 }));
        ASSERT_TRUE(wait_for(completed));
        EXPECT_FALSE(status.ok());
        EXPECT_NE(status.to_string().find(missing ? "no fd for offset" : "short read (4096 of 8192 bytes)"),
                  std::string::npos);
        EXPECT_EQ(completed.load(), 1U);
    }
}
