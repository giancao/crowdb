// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <barrier>
#include <thread>

using namespace crowdb::tree;

TEST(MemoryBacking, ConcurrentSparseGrowthAndDisjointRangesShareOnePhysicalPage)
{
    MemPageStore                      store;
    constexpr size_t                  workers = 8;
    std::barrier                      start(workers + 1);
    std::array<std::jthread, workers> threads;
    for (size_t worker = 0; worker < workers; ++worker) {
        threads[worker] = std::jthread([&, worker] {
            std::array<uint8_t, 4096> input;
            input.fill(static_cast<uint8_t>(worker + 1));
            const auto remote = (uint64_t{worker} + 1) << 44;
            start.arrive_and_wait();
            for (size_t iteration = 0; iteration < 16; ++iteration) {
                EXPECT_TRUE(store.write_at(worker * input.size(), input.data(), input.size()).ok());
                EXPECT_TRUE(store.write_at(remote, input.data(), input.size()).ok());
                std::array<uint8_t, 4096> output{};
                EXPECT_TRUE(store.read_at(worker * input.size(), output.data(), output.size()).ok());
                EXPECT_EQ(output, input);
                EXPECT_TRUE(store.read_at(remote, output.data(), output.size()).ok());
                EXPECT_EQ(output, input);
            }
        });
    }
    start.arrive_and_wait();
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_EQ(store.size(), (uint64_t{workers} << 44) + 4096);
    EXPECT_EQ(store.resident_bytes(), (workers + 1) * MemPageStore::kBackingPageBytes);
    std::array<uint8_t, 4096> hole;
    hole.fill(0xff);
    ASSERT_TRUE(store.read_at(uint64_t{1} << 43, hole.data(), hole.size()).ok());
    EXPECT_EQ(hole, (std::array<uint8_t, 4096>{}));
}

TEST(MemoryBacking, ExclusivePageRemovalAndReuseDoesNotSerializeOtherPageIo)
{
    MemPageStore       store;
    constexpr uint64_t page_bytes = MemPageStore::kBackingPageBytes;
    const uint8_t      initial    = 0x12;
    ASSERT_TRUE(store.write_at(0, &initial, 1).ok());
    std::barrier start(2);
    std::jthread writer([&] {
        std::array<uint8_t, 4096> input;
        input.fill(0x78);
        start.arrive_and_wait();
        for (size_t iteration = 0; iteration < 512; ++iteration) {
            EXPECT_TRUE(store.write_at(page_bytes, input.data(), input.size()).ok());
            std::array<uint8_t, 4096> output{};
            EXPECT_TRUE(store.read_at(page_bytes, output.data(), output.size()).ok());
            EXPECT_EQ(output, input);
        }
    });
    start.arrive_and_wait();
    // No other byte user accesses page zero, matching its exclusive GC lease.
    for (size_t iteration = 0; iteration < 512; ++iteration) {
        EXPECT_TRUE(store.release_backing_page(0));
        EXPECT_FALSE(store.release_backing_page(0));
        const auto input = static_cast<uint8_t>(iteration);
        ASSERT_TRUE(store.write_at(0, &input, 1).ok());
        uint8_t output = 0;
        ASSERT_TRUE(store.read_at(0, &output, 1).ok());
        EXPECT_EQ(output, input);
    }
    writer.join();
    EXPECT_EQ(store.resident_bytes(), 2 * page_bytes);
    EXPECT_EQ(store.size(), page_bytes + 4096);
}
