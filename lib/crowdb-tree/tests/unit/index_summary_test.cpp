// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/index_summary.h"

#include <gtest/gtest.h>

#include <array>
#include <vector>

namespace crowdb::tree
{
TEST(IndexSummaries, SparsePidRangesCanOutgrowTheSegmentWidth)
{
    std::vector<IndexSummaries::Change> changes;
    const IndexSummary exact{.live_kv = 1, .live_key_bytes = 10, .live_value_bytes = 100, .exact = true};
    constexpr uint32_t last_pid = (1024 * 1024) + 2;
    for (uint32_t segment = 0; segment <= 1024; ++segment) {
        changes.push_back({.pid = (segment * 1024) + 2, .summary = exact});
    }
    const auto before = IndexSummaries{}.merge(changes, {});
    EXPECT_EQ(before.find(2), exact);
    EXPECT_EQ(before.find(last_pid), exact);
    EXPECT_FALSE(before.find(last_pid + 1));

    const IndexSummary unknown{};
    const std::array   updates{
        IndexSummaries::Change{.pid = 1,    .summary = exact       },
        IndexSummaries::Change{.pid = 2,    .summary = unknown     },
        IndexSummaries::Change{.pid = 1026, .summary = std::nullopt}
    };
    const auto after = before.merge(updates, {});
    EXPECT_EQ(after.find(1), exact);
    EXPECT_EQ(after.find(2), unknown);
    EXPECT_FALSE(after.find(1026));
    EXPECT_EQ(after.find(last_pid), exact);
    EXPECT_FALSE(before.find(1));
    EXPECT_EQ(before.find(2), exact);
    EXPECT_EQ(before.find(1026), exact);
    const std::array revert{
        IndexSummaries::Change{.pid = 1,    .summary = std::nullopt},
        IndexSummaries::Change{.pid = 2,    .summary = exact       },
        IndexSummaries::Change{.pid = 1026, .summary = exact       }
    };
    EXPECT_TRUE(before.same(after.merge(revert, {})));
}

TEST(IndexSummaries, RemovingTheLastOverrideLeavesNoRecord)
{
    const std::array initial{
        IndexSummaries::Change{.pid = 2049, .summary = IndexSummary{}}
    };
    const auto       before = IndexSummaries{}.merge(initial, {});
    const std::array remove{
        IndexSummaries::Change{.pid = 2049, .summary = std::nullopt}
    };
    const auto after = before.merge(remove, {});
    EXPECT_TRUE(before.find(2049));
    EXPECT_FALSE(after.find(2049));
    EXPECT_EQ(after.merge(initial, {}).find(2049), IndexSummary{});
}
} // namespace crowdb::tree
