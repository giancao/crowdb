// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "allocation_account_test_support.h"
#include "crowdb-tree/page_store/page_disk_store.h"
#include "crowdb-tree/page_store/page_store_release_service.h"
#include "test_tmp.h"
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"

#    include <future>
#    include <latch>
#endif

#include <fcntl.h>
#include <gtest/gtest.h>

#include <atomic>
#include <cerrno>
#include <thread>

namespace crowdb::tree
{
namespace
{
TEST(BlockReleaseService, BackendFinalOwnerQueuesDirectoryAndLateFileWithoutClosingOnCallback)
{
    tree_test::TempDir             path("block_release_");
    std::unique_ptr<PageDiskStore> store;
    ASSERT_TRUE(PageDiskStore::open_blocks(path.path, 3, 4, 4096, 1, &store).ok());
    const uint8_t value = 37;
    ASSERT_TRUE(store->write_at(uint64_t{UINT32_MAX} * 4096, &value, 1).ok());
    auto service       = store->release_service();
    auto notifications = std::make_shared<std::atomic<size_t>>(0);
    ASSERT_TRUE(service
                    ->bind_wake(
                        notifications,
                        [](void *context) noexcept { static_cast<std::atomic<size_t> *>(context)->fetch_add(1); },
                        notifications.get())
                    .ok());
    uint64_t local  = 0;
    auto     reader = store->file_for_offset(uint64_t{UINT32_MAX} * 4096, &local);
    ASSERT_NE(reader, nullptr);
    const auto  fd = reader->fd();
    std::thread callback([owned = std::move(store)]() mutable { owned.reset(); });
    callback.join();
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_FALSE(service->drained());
    EXPECT_EQ(service->snapshot().pending, 1U);
    size_t steps = 0;
    while (service->snapshot().pending != 0 && steps < 256) {
        const auto step = service->pump(1);
        EXPECT_LE(step.visited, 1U);
        ++steps;
    }
    EXPECT_LT(steps, 256U);
    EXPECT_GT(steps, 1U);
    EXPECT_EQ(service->snapshot().registered, 1U);
    uint8_t actual = 0;
    EXPECT_TRUE(reader->pread_at(0, &actual, 1).ok());
    EXPECT_EQ(actual, value);
    std::thread late([owned = std::move(reader)]() mutable { owned.reset(); });
    late.join();
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(service->snapshot().pending, 1U);
    EXPECT_EQ(service->pump(1).completed, 1U);
    EXPECT_TRUE(service->drained());
    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);
    EXPECT_GT(notifications->load(), 0U);
}

TEST(BlockReleaseService, ManualHostDestructionDrainsCurrentWorkAndPreservesLateReaders)
{
    tree_test::TempDir             path("block_manual_release_");
    std::unique_ptr<PageDiskStore> store;
    ASSERT_TRUE(PageDiskStore::open_blocks(path.path, 3, 4, 4096, 1, &store).ok());
    auto service = store->release_service();
    auto account = std::dynamic_pointer_cast<detail::AllocationAccount>(service->allocation_budget());
    ASSERT_NE(account, nullptr);
    detail::ResourceBudget host(detail::test_allocation_account::limits(1 << 20), 1);
    auto                   ledger = host.make_account(detail::test_allocation_account::limits(1 << 20));
    ASSERT_TRUE(account->rebind(ledger).ok());
    uint64_t local  = 0;
    auto     reader = store->file_for_offset(0, &local);
    ASSERT_NE(reader, nullptr);
    const auto fd = reader->fd();
    store.reset();
    EXPECT_EQ(service->snapshot().pending, 0U);
    EXPECT_EQ(service->snapshot().registered, 1U);
    EXPECT_FALSE(service->drained());
    reader.reset();
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(service->pump(1).completed, 1U);
    EXPECT_TRUE(service->drained());
    EXPECT_GT(account->owned_bytes(), 0U);
    service.reset();
    // The stable account pays for its own object/control until its last owner.
    std::shared_ptr<detail::AllocationAccount> empty_account;
    ASSERT_TRUE(detail::AllocationAccount::create_unbound(&empty_account).ok());
    EXPECT_EQ(account->owned_bytes(), empty_account->owned_bytes());
    account.reset();
    EXPECT_EQ(ledger->snapshot().allocated[detail::test_allocation_account::kRouting], 0U);
    EXPECT_EQ(ledger->snapshot().physical[detail::test_allocation_account::kMetadata], 0U);
}

TEST(BlockReleaseService, FailedSingleFileOpenReturnsAllQueuedCloseControls)
{
    tree_test::TempDir             path("block_open_release_");
    std::unique_ptr<PageDiskStore> store;
    const auto                     status = PageDiskStore::open(path.path + "/missing/file", 1, &store);
    EXPECT_FALSE(status.ok());
    EXPECT_FALSE(store);
}

#ifdef CROWDB_HAVE_LIBURING
TEST(BlockReleaseService, CloseDoesNotDrainUntilKernelCompletionReleasesItsStorage)
{
    tree_test::TempDir             path("block_kernel_release_");
    std::unique_ptr<PageDiskStore> store;
    ASSERT_TRUE(PageDiskStore::open_blocks(path.path, 3, 4, 4096, 1, &store).ok());
    common::Topology topology;
    topology.pipelines.push_back({});
    common::DiskIOUring reactor(std::move(topology));
    ASSERT_TRUE(reactor.valid());
    const uint8_t value = 23;
    ASSERT_TRUE(store->write_at(4096, &value, 1).ok());
    auto service  = store->release_service();
    auto notified = std::make_shared<std::atomic<size_t>>(0);
    ASSERT_TRUE(service
                    ->bind_wake(
                        notified,
                        [](void *context) noexcept {
                            static_cast<std::atomic<size_t> *>(context)->fetch_add(1, std::memory_order_release);
                        },
                        notified.get())
                    .ok());

    uint64_t local  = 0;
    auto     reader = store->file_for_offset(4096, &local);
    ASSERT_TRUE(reader);
    const int fd = reader->fd();
    ASSERT_TRUE(store->delete_block(1).ok());
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);

    struct Gate
    {
        std::promise<int> entered;
        std::latch        resume{1};
    };

    auto gate = std::make_shared<Gate>();

    struct Resume
    {
        std::shared_ptr<Gate> gate;

        ~Resume()
        {
            if (gate) {
                gate->resume.count_down();
            }
        }
    } resume{gate};

    auto entered = gate->entered.get_future();
    reactor.submit_fsync(store->fd_for_offset(0, &local), [gate](int result) {
        gate->entered.set_value(result);
        if (result >= 0) {
            gate->resume.wait();
        }
    });
    ASSERT_EQ(entered.wait_for(std::chrono::seconds(5)), std::future_status::ready);
    ASSERT_EQ(entered.get(), 0);

    reader.reset();
    const auto start = service->pump(1, &reactor);
    EXPECT_EQ(start.visited, 1U);
    EXPECT_EQ(start.completed, 0U);
    EXPECT_FALSE(start.more);
    EXPECT_EQ(service->snapshot().pending, 1U);
    EXPECT_EQ(reactor.total_in_flight_count(), 1U);
    EXPECT_GE(::fcntl(fd, F_GETFD), 0);
    EXPECT_EQ(service->pump(1, &reactor).visited, 0U);
    const auto before = notified->load(std::memory_order_acquire);
    gate->resume.count_down();
    resume.gate.reset();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (notified->load(std::memory_order_acquire) == before && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    EXPECT_GT(notified->load(std::memory_order_acquire), before);
    EXPECT_EQ(service->pump(1, &reactor).completed, 1U);
    EXPECT_EQ(service->snapshot().pending, 0U);
    errno = 0;
    EXPECT_EQ(::fcntl(fd, F_GETFD), -1);
    EXPECT_EQ(errno, EBADF);

    store.reset();
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!service->drained() && std::chrono::steady_clock::now() < drain_deadline) {
        (void)service->pump(64, &reactor);
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }
    ASSERT_TRUE(service->drained());
    EXPECT_EQ(reactor.total_in_flight_count(), 0U);
}
#endif
} // namespace
} // namespace crowdb::tree
