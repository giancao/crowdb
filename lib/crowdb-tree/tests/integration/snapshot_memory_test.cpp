// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "buffer_pool_test_support.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "crowdb-tree/crowdb-tree.h"

#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#if defined(__GLIBC__) && !defined(__SANITIZE_ADDRESS__)
#    include <malloc.h>
#    include <unistd.h>

namespace crowdb::tree
{
namespace
{
size_t resident_bytes()
{
    malloc_trim(0);
    std::ifstream status("/proc/self/statm");
    size_t        virtual_pages  = 0;
    size_t        resident_pages = 0;
    status >> virtual_pages >> resident_pages;
    return resident_pages * static_cast<size_t>(sysconf(_SC_PAGESIZE));
}

// Reserve the device before sampling allocations. Backend growth must not be
// confused with encoded snapshot pages retained after their writes complete.
class TestSnapshotBufferStore final : public PageStore, public AsyncPageStore
{
  public:
    TestSnapshotBufferStore() : bytes_(96 * 1024 * 1024)
    {
    }

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    Status write_at(uint64_t off, const uint8_t *buf, size_t len) override
    {
        if (baseline > 0 && !prepared_bytes.has_value()) {
            const auto current = resident_bytes();
            prepared_bytes     = current > baseline ? current - baseline : 0;
        }
        if (off > bytes_.size() || len > bytes_.size() - off) {
            return Status::resource_exhausted("test device is full");
        }
        std::memcpy(bytes_.data() + off, buf, len);
        size_ = std::max(size_, off + len);
        return Status::Ok();
    }

    Status write_typed_at(PagePurpose purpose, uint64_t off, const uint8_t *buf, size_t len) override
    {
        if (purpose == PagePurpose::kPageIndex && prepared_bytes.has_value() && !retained_bytes.has_value()) {
            const auto current = resident_bytes();
            retained_bytes     = current > baseline ? current - baseline : 0;
        }
        return write_at(off, buf, len);
    }

    Status read_at(uint64_t off, uint8_t *buf, size_t len) const override
    {
        if (off > size_ || len > size_ - off) {
            return Status::not_found("test device range is absent");
        }
        std::memcpy(buf, bytes_.data() + off, len);
        return Status::Ok();
    }

    Status sync() override
    {
        return Status::Ok();
    }

    [[nodiscard]] uint64_t size() const override
    {
        return size_;
    }

    [[nodiscard]] uint32_t iu_size() const override
    {
        return 65536;
    }

    uint64_t submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion done) override
    {
        done.complete(read_at(addr, static_cast<uint8_t *>(buf), len));
        return 0;
    }

    uint64_t submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion done) override
    {
        return submit_typed_write(PagePurpose::kBtreePage, addr, buf, len, std::move(done));
    }

    uint64_t submit_typed_write(PagePurpose purpose, PageAddr addr, const void *buf, size_t len,
                                AsyncCompletion done) override
    {
        {
            std::scoped_lock lock(pending_mutex_);
            pending_.push_back({.addr = addr, .data = buf, .length = len, .done = std::move(done), .purpose = purpose});
        }
        ready_.notify_one();
        return 0;
    }

    Status submit_fsync(AsyncCompletion done) override
    {
        done.complete(sync());
        return Status::Ok();
    }

    void cancel(uint64_t /*operation_id*/) override
    {
    }

    void wake()
    {
        {
            std::scoped_lock lock(pending_mutex_);
            terminal_ = true;
        }
        ready_.notify_one();
    }

    bool complete_next()
    {
        Pending write;
        {
            std::unique_lock lock(pending_mutex_);
            ready_.wait(lock, [&] { return terminal_ || !pending_.empty(); });
            if (pending_.empty()) {
                return false;
            }
            write = std::move(pending_.front());
            pending_.pop_front();
        }
        auto status = write_typed_at(write.purpose, write.addr, static_cast<const uint8_t *>(write.data), write.length);
        write.done.complete(std::move(status));
        return true;
    }

    size_t                baseline = 0;
    std::optional<size_t> prepared_bytes;
    std::optional<size_t> retained_bytes;

  private:
    struct Pending
    {
        uint64_t        addr;
        const void     *data;
        size_t          length;
        AsyncCompletion done;
        PagePurpose     purpose;
    };

    std::vector<uint8_t>    bytes_;
    uint64_t                size_ = 0;
    std::mutex              pending_mutex_;
    std::condition_variable ready_;
    std::deque<Pending>     pending_;
    bool                    terminal_ = false;
};

class SnapshotMemoryTest : public ::testing::TestWithParam<bool>
{
};

TEST_P(SnapshotMemoryTest, ReleasesCompletedPageBuffersBeforeWritingMetadata)
{
    TestSnapshotBufferStore store;
    Config                  config;
    config.page_store       = &store;
    config.async_page_store = GetParam() ? &store : nullptr;
    config.frame_bytes      = 65536;
    config.compression      = compress_algo::kNone;
    const std::string value(65536, 's');
    {
        Crowdbtree tree(config);
        for (uint64_t i = 0; i < 256; ++i) {
            const Batch batch{{{.key = "snapshot-" + std::to_string(i), .kind = OpKind::kPut, .value = value}}};
            ASSERT_TRUE(tree.apply(i + 1, batch).ok());
        }
        ASSERT_TRUE(tree.flush().ok());
        store.baseline = resident_bytes();
        ASSERT_GT(store.baseline, 0U);
        if (GetParam()) {
            std::atomic<bool> complete{false};
            tree.flush_with_persistent_async([&](Status status, uint64_t applied) {
                EXPECT_TRUE(status.ok()) << status.to_string();
                EXPECT_EQ(applied, 256U);
                complete.store(true, std::memory_order_release);
                store.wake();
            });
            while (!complete.load(std::memory_order_acquire)) {
                if (!store.complete_next()) {
                    ASSERT_TRUE(complete.load(std::memory_order_acquire));
                }
            }
        }
        else {
            ASSERT_TRUE(tree.flush_with_persistent().ok());
        }
        ASSERT_TRUE(store.retained_bytes.has_value());
        ASSERT_TRUE(store.prepared_bytes.has_value());
        const auto prepared_bytes = store.prepared_bytes.value_or(std::numeric_limits<size_t>::max());
        std::cout << "Snapshot prepared RSS growth: " << prepared_bytes << " bytes\n";
        EXPECT_LT(prepared_bytes, 48U * 1024 * 1024) << "snapshot preparation retained device padding for every page";
        const auto retained_bytes = store.retained_bytes.value_or(std::numeric_limits<size_t>::max());
        std::cout << "Snapshot retained RSS growth: " << retained_bytes << " bytes\n";
        EXPECT_LT(retained_bytes, 8U * 1024 * 1024) << "completed page writes still retain their encoded buffers";
    }
    config.async_page_store = nullptr;
    std::unique_ptr<Crowdbtree> reopened;
    ASSERT_TRUE(Crowdbtree::open(config, &reopened).ok());
    for (uint64_t i = 0; i < 256; ++i) {
        std::string actual;
        uint64_t    revision = 0;
        ASSERT_TRUE(reopened->get(Slice("snapshot-" + std::to_string(i)), &revision, &actual));
        EXPECT_EQ(actual, value);
        EXPECT_EQ(revision, i + 1);
    }
}

INSTANTIATE_TEST_SUITE_P(WritePaths, SnapshotMemoryTest, ::testing::Bool());

TEST(SnapshotMemory, UnusedFrameCapacityDoesNotPrefaultTheArena)
{
    MemPageStore   store(65536);
    const auto     before = resident_bytes();
    TestBufferPool pool(256U * 1024 * 1024, 65536, &store);
    const auto     current = resident_bytes();
    const auto     growth  = current > before ? current - before : 0;
    std::cout << "Unused frame-cache RSS growth: " << growth << " bytes\n";
    EXPECT_LT(growth, 8U * 1024 * 1024);
    EXPECT_EQ(pool.stats().num_frames, 4096U);
    EXPECT_EQ(pool.stats().used, 0U);

    uint32_t index = 0;
    uint8_t *bytes = nullptr;
    ASSERT_TRUE(pool.acquire_frame(&index, &bytes).ok());
    EXPECT_TRUE(std::all_of(bytes, bytes + 65536, [](uint8_t byte) { return byte == 0; }));
    std::memset(bytes, 0x53, 65536);
    pool.release_frame(index);
    EXPECT_EQ(pool.stats().num_frames, 4096U);
}
} // namespace
} // namespace crowdb::tree
#endif
