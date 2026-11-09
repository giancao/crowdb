// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#    include "crowdb-common/runtime_path.h"

#    include <fcntl.h>
#    include <gtest/gtest.h>
#    include <sys/stat.h>
#    include <unistd.h>

#    include <array>
#    include <chrono>
#    include <filesystem>
#    include <latch>

namespace crowdb::common
{
namespace
{
struct TestAllocation
{
    size_t              limit = 0;
    std::atomic<size_t> used{0};
    std::atomic<size_t> acquired{0};
    std::atomic<size_t> freed{0};

    static void *allocate(void *context, size_t bytes, size_t alignment)
    {
        auto &self = *static_cast<TestAllocation *>(context);
        if (bytes > self.limit) {
            throw std::bad_alloc();
        }
        auto *storage = ::operator new(bytes, std::align_val_t{alignment});
        self.used.fetch_add(bytes);
        self.acquired.fetch_add(1);
        return storage;
    }

    static void deallocate(void *context, void *storage, size_t bytes, size_t alignment) noexcept
    {
        auto &self = *static_cast<TestAllocation *>(context);
        ::operator delete(storage, std::align_val_t{alignment});
        self.used.fetch_sub(bytes);
        self.freed.fetch_add(1);
    }
};

struct TestCompletion
{
    std::atomic<int>                result{INT_MIN};
    std::atomic<size_t>             live_at_callback{0};
    std::shared_ptr<TestAllocation> allocations;

    static void done(void *context, int result) noexcept
    {
        auto &self = *static_cast<TestCompletion *>(context);
        self.live_at_callback.store(self.allocations->used.load());
        self.result.store(result, std::memory_order_release);
    }

    IOCompletion completion()
    {
        return {
            .context    = this,
            .callback   = &done,
            .owner      = {},
            .allocation = {.owner      = allocations,
                           .context    = allocations.get(),
                           .allocate   = &TestAllocation::allocate,
                           .deallocate = &TestAllocation::deallocate}
        };
    }
};

struct TestFile
{
    std::filesystem::path path;
    int                   fd = -1;

    TestFile()
    {
        auto root = test_runtime_path("owned-uring");
        std::filesystem::create_directories(root);
        auto name = (root / "io-XXXXXX").string();
        fd        = ::mkstemp(name.data());
        path      = name;
    }

    ~TestFile()
    {
        if (fd >= 0) {
            ::close(fd);
        }
        std::filesystem::remove(path);
    }
};

template <class F> bool wait_until(F predicate)
{
    for (size_t i = 0; i < 200; ++i) {
        if (predicate()) {
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    return predicate();
}

TEST(DiskIOOwned, ExactEntryStorageRemainsChargedThroughCallbackAndFreesAfterwards)
{
    TestFile file;
    ASSERT_GE(file.fd, 0);
    Topology topology;
    topology.pipelines.push_back({.entries = 16, .mode = PollingMode::Classic});
    DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    uring.register_fd(file.fd);
    auto budget   = std::make_shared<TestAllocation>();
    budget->limit = DiskIOUring::owned_submission_allocation_bytes();
    TestCompletion completed;
    completed.allocations = budget;
    const std::array<uint8_t, 3> expected{1, 2, 3};
    uring.submit_write_owned(file.fd, expected.data(), expected.size(), 0, completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    EXPECT_EQ(completed.result.load(), 3);
    EXPECT_EQ(completed.live_at_callback.load(), budget->limit);
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    EXPECT_EQ(budget->acquired.load(), 1U);
    EXPECT_EQ(budget->freed.load(), 1U);
    completed.result.store(INT_MIN);
    std::array<uint8_t, 3> actual{};
    uring.submit_read_owned(file.fd, actual.data(), actual.size(), 0, completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    EXPECT_EQ(completed.result.load(), 3);
    EXPECT_EQ(actual, expected);
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    completed.result.store(INT_MIN);
    uring.submit_fsync_owned(file.fd, true, completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    EXPECT_EQ(completed.result.load(), 0);
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    EXPECT_EQ(budget->acquired.load(), 3U);
    EXPECT_EQ(budget->freed.load(), 3U);
}

TEST(DiskIOOwned, EntryQuotaFailureDoesNotPublishOrLeakInflight)
{
    TestFile file;
    ASSERT_GE(file.fd, 0);
    Topology topology;
    topology.pipelines.push_back({.entries = 16, .mode = PollingMode::Classic});
    DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    uring.register_fd(file.fd);
    TestCompletion completed;
    completed.allocations        = std::make_shared<TestAllocation>();
    completed.allocations->limit = DiskIOUring::owned_submission_allocation_bytes() - 1;
    const uint8_t byte           = 1;
    uring.submit_write_owned(file.fd, &byte, 1, 0, completed.completion());
    EXPECT_EQ(completed.result.load(), -ENOMEM);
    EXPECT_EQ(completed.allocations->used.load(), 0U);
    EXPECT_EQ(completed.allocations->acquired.load(), 0U);
    EXPECT_EQ(uring.total_in_flight_count(), 0U);
    EXPECT_EQ(uring.in_flight_count(file.fd), 0U);
    EXPECT_EQ(::lseek(file.fd, 0, SEEK_END), 0);
}

TEST(DiskIOOwned, PendingRequestsReturnWithoutSqSpaceAndAllAcceptedWritesComplete)
{
    TestFile file;
    ASSERT_GE(file.fd, 0);
    Topology topology;
    topology.pipelines.push_back({.entries = 2, .mode = PollingMode::Classic});
    DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    uring.register_fd(file.fd);

    struct Paused
    {
        std::atomic<bool> started{false};
        std::latch        resume{1};
    };

    auto paused = std::make_shared<Paused>();

    struct Resume
    {
        std::shared_ptr<Paused> paused;

        ~Resume()
        {
            if (paused) {
                paused->resume.count_down();
            }
        }
    } resume{paused};

    static const uint8_t byte = 7;
    uring.submit_write(file.fd, &byte, 1, 0, [paused](int) {
        paused->started.store(true, std::memory_order_release);
        paused->resume.wait();
    });
    ASSERT_TRUE(wait_until([&] { return paused->started.load(std::memory_order_acquire); }));
    // Fill the kernel-facing SQ while the poll owner is suspended in the hook.
    for (size_t i = 0; i < 2; ++i) {
        uring.submit_write(file.fd, &byte, 1, 0, [](int) {});
    }
    auto budget   = std::make_shared<TestAllocation>();
    budget->limit = DiskIOUring::owned_submission_allocation_bytes();
    std::vector<std::shared_ptr<TestCompletion>> completions;
    for (size_t i = 0; i < 1024; ++i) {
        auto completed         = std::make_shared<TestCompletion>();
        completed->allocations = budget;
        auto completion        = completed->completion();
        completion.owner       = completed;
        uring.submit_write_owned(file.fd, &byte, 1, static_cast<off_t>(i), std::move(completion));
        completions.push_back(std::move(completed));
    }
    EXPECT_EQ(uring.in_flight_count(file.fd), 1026U);
    EXPECT_EQ(budget->acquired.load(), 1024U);
    EXPECT_EQ(budget->freed.load(), 0U);
    TestCompletion close;
    close.allocations = budget;
    EXPECT_EQ(uring.try_submit_close_owned(file.fd, close.completion()), -EBUSY);
    EXPECT_EQ(close.result.load(), INT_MIN);
    uring.submit_close_owned(file.fd, close.completion());
    EXPECT_EQ(close.result.load(), -EBUSY);
    // The guard resumes the poll owner before the engine is destroyed.
    paused->resume.count_down();
    resume.paused.reset();
    ASSERT_TRUE(wait_until([&] { return budget->freed.load() == 1024; }));
    for (const auto &completed : completions) {
        EXPECT_EQ(completed->result.load(std::memory_order_acquire), 1);
    }
    EXPECT_EQ(uring.in_flight_count(file.fd), 0U);
    EXPECT_EQ(uring.total_in_flight_count(), 0U);
    EXPECT_EQ(::lseek(file.fd, 0, SEEK_END), 1024);
}

TEST(DiskIOOwned, StopCompletionFollowsTheLastPollerCallback)
{
    struct Tail
    {
        std::atomic<bool> started{false};
        std::atomic<bool> stopped{false};
        std::latch        resume{1};
    } tail;

    TestFile file;
    ASSERT_GE(file.fd, 0);
    Topology topology;
    topology.pipelines.push_back({.entries = 16, .mode = PollingMode::Classic});
    DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    uring.register_fd(file.fd);

    struct Resume
    {
        std::latch *latch;

        ~Resume()
        {
            if (latch != nullptr) {
                latch->count_down();
            }
        }
    } resume{&tail.resume};

    uring.submit_fsync(file.fd, [&tail](int) {
        tail.started.store(true, std::memory_order_release);
        tail.resume.wait();
    });
    ASSERT_TRUE(wait_until([&] { return tail.started.load(std::memory_order_acquire); }));
    ASSERT_EQ(uring.total_in_flight_count(), 0U);
    uring.stop_after_drain({.context = &tail, .callback = [](void *context, int) noexcept {
                                static_cast<Tail *>(context)->stopped.store(true, std::memory_order_release);
                            }});
    EXPECT_FALSE(tail.stopped.load(std::memory_order_acquire));
    tail.resume.count_down();
    resume.latch = nullptr;
    EXPECT_TRUE(wait_until([&] { return tail.stopped.load(std::memory_order_acquire); }));
}

TEST(DiskIOOwned, FileMetadataCompletesThroughTheSameOwnedLifetime)
{
    TestFile file;
    ASSERT_GE(file.fd, 0);
    Topology topology;
    topology.pipelines.push_back({.entries = 2, .mode = PollingMode::Classic});
    DiskIOUring uring(std::move(topology));
    ASSERT_TRUE(uring.valid());
    auto budget   = std::make_shared<TestAllocation>();
    budget->limit = DiskIOUring::owned_submission_allocation_bytes();
    TestCompletion completed;
    completed.allocations = budget;
    const auto path       = file.path.string();
    uring.submit_open_owned(path.c_str(), O_RDWR | O_CLOEXEC, 0, completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    const int fd = completed.result.load();
    ASSERT_GE(fd, 0);
    uring.register_fd(fd);
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    struct statx info{};
    completed.result.store(INT_MIN);
    uring.submit_stat_owned(fd, &info, completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    EXPECT_EQ(completed.result.load(), 0);
    EXPECT_TRUE(S_ISREG(info.stx_mode));
    EXPECT_EQ(info.stx_size, 0U);
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    completed.result.store(INT_MIN);
    uring.submit_close_owned(fd, completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    EXPECT_EQ(completed.result.load(), 0);
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    completed.result.store(INT_MIN);
    uring.submit_unlink_owned(path.c_str(), completed.completion());
    ASSERT_TRUE(wait_until([&] { return completed.result.load(std::memory_order_acquire) != INT_MIN; }));
    EXPECT_EQ(completed.result.load(), 0);
    EXPECT_FALSE(std::filesystem::exists(file.path));
    ASSERT_TRUE(wait_until([&] { return budget->used.load() == 0; }));
    EXPECT_EQ(budget->acquired.load(), 4U);
    EXPECT_EQ(budget->freed.load(), 4U);
    EXPECT_EQ(uring.total_in_flight_count(), 0U);
}
} // namespace
} // namespace crowdb::common
#endif // CROWDB_HAVE_LIBURING
