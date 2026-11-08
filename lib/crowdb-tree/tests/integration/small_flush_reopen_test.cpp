// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#include "crowdb-tree/crowdb-tree.h"
#include "crowdb-tree/page_store/page_store.h"

#include <gtest/gtest.h>

#include <array>
#include <cstdio>
#include <map>
#include <memory>
#include <string>
#include <vector>

using namespace crowdb::tree;

namespace
{
Batch put_one(const std::string &k, const std::string &v)
{
    return Batch{{{.key = k, .kind = OpKind::kPut, .value = v}}};
}

Batch del_one(const std::string &k)
{
    return Batch{{{.key = k, .kind = OpKind::kDelete, .value = ""}}};
}

std::string make_key(int i)
{
    std::array<char, 16> b{};
    snprintf(b.data(), b.size(), "k%04d", i);
    return b.data();
}
} // namespace

TEST(LocalRuntimeReopen, SmallFlushesPreserveOverwritesAndDeletes)
{
    PageMemStore store(1);
    Config       opt;
    opt.page_store       = &store;
    opt.frame_bytes      = 4096;
    opt.leaf_split_bytes = 1024; // keep multiple leaves

    std::map<std::string, std::string> oracle;
    uint64_t                           slot = 0;
    {
        Crowdbtree t(opt);
        // Seed a base, then stream small single-key flushes.
        for (int i = 0; i < 60; ++i) {
            ++slot;
            std::string v = "v" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(i)] = v;
        }
        // Overwrites + deletes (exercise overlay shadowing + tombstone deltas).
        for (int i = 0; i < 60; i += 3) {
            ++slot;
            std::string v = "up" + std::to_string(i);
            ASSERT_TRUE(t.apply(slot, put_one(make_key(i), v)).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle[make_key(i)] = v;
        }
        for (int i = 1; i < 60; i += 7) {
            ++slot;
            ASSERT_TRUE(t.apply(slot, del_one(make_key(i))).ok());
            ASSERT_TRUE(t.flush().ok());
            oracle.erase(make_key(i));
        }

        for (const auto &kv : oracle) {
            std::string v;
            uint64_t    s;
            ASSERT_TRUE(t.get(Slice(kv.first), &s, &v)) << "missing " << kv.first;
            EXPECT_EQ(v, kv.second);
        }
        ASSERT_TRUE(t.flush_with_persistent(nullptr).ok());
    }

    std::unique_ptr<Crowdbtree> t2;
    ASSERT_TRUE(Crowdbtree::open(opt, &t2).ok());
    for (const auto &kv : oracle) {
        std::string v;
        uint64_t    s;
        ASSERT_TRUE(t2->get(Slice(kv.first), &s, &v)) << "missing after reopen " << kv.first;
        EXPECT_EQ(v, kv.second);
    }
    // Deleted keys stay gone.
    std::string v;
    uint64_t    s;
    EXPECT_FALSE(t2->get(Slice(make_key(1)), &s, &v));
}
