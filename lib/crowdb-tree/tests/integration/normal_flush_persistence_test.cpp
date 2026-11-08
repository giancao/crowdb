// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core.h"
#include "common/test_tree_core_chunk.h"
#include "crowdb-tree/page_store/page_store.h"
#include "snapshot/checkpoint_operation.h"
#include "snapshot/operation_wait.h"
#include "snapshot/primary_page_block.h"
#include "snapshot/snapshot_baseline.h"

#include <gtest/gtest.h>

#include <tuple>

namespace crowdb::tree
{
namespace
{
class NormalFlushPersistence : public ::testing::TestWithParam<std::tuple<bool, bool>>
{
  protected:
    std::unique_ptr<test_chunk::TestCoreChunk> chunk;
    std::shared_ptr<PageMemStore>              local_store;
    Config                                     config;
    std::unique_ptr<Crowdbtree>                local_tree;

    void SetUp() override
    {
        if (std::get<0>(GetParam())) {
            chunk  = std::make_unique<test_chunk::TestCoreChunk>();
            config = chunk->config;
        }
        else {
            ASSERT_TRUE(TreeRuntime::create({.workers = 1, .backend_workers = 1}, &config.runtime).ok());
            local_store             = std::make_shared<PageMemStore>(4096);
            config.page_store       = local_store.get();
            config.page_store_owner = local_store;
        }
        config.frame_bytes                          = 4096;
        config.leaf_split_bytes                     = 400;
        config.max_inline_value                     = 32;
        config.inner_max_keys                       = 3;
        config.runtime_binding.limits.io_operations = 1;
        if (chunk) {
            chunk->config = config;
        }
        const auto status = open();
        ASSERT_TRUE(status.ok()) << status.to_string();
    }

    Status open()
    {
        return chunk ? chunk->open() : Crowdbtree::open(config, &local_tree);
    }

    Crowdbtree &tree()
    {
        return chunk ? *chunk->tree : *local_tree;
    }

    TreeCore &core()
    {
        return *TreeCoreAccess::get(tree());
    }

    void expect_durable(uint64_t coverage, uint64_t sequence)
    {
        EXPECT_EQ(core().durable_coverage(), coverage);
        EXPECT_EQ(core().snapshot_sequence(), sequence);
        if (chunk) {
            EXPECT_EQ(chunk->store->manifest_generation(), sequence);
            EXPECT_EQ(chunk->store->durable_wal_offset(), coverage * 4096);
            EXPECT_EQ(chunk->backend.catalog->commit_calls, sequence);
        }
    }
};
} // namespace

TEST_P(NormalFlushPersistence, SplitAndPartialWritebackKeepTheRootUntilPersistentFlush)
{
    const bool        commit = std::get<1>(GetParam());
    const std::string old_value(32, 'v');
    const std::string new_value(32, 'x');
    ASSERT_TRUE(core()
                    .apply(1,
                           Batch{
                               .ops = {{.key = "a", .kind = OpKind::kPut, .value = old_value},
                                       {.key = "z", .kind = OpKind::kPut, .value = old_value}}
    })
                    .ok());
    if (chunk) {
        chunk->store->request_wal_offset(4096);
    }
    ASSERT_TRUE(tree().flush_with_persistent().ok());
    expect_durable(1, 1);
    uint64_t expected_root = 0;
    {
        const auto before = core().capture();
        expected_root     = before->root_page_id;
        ASSERT_EQ(before->mapping.get(expected_root)->level, 0U);
        ASSERT_TRUE(before->snapshot_changes.empty());
        Batch inserts;
        for (char key = 'a'; key < 'z'; ++key) {
            inserts.ops.push_back({.key = std::string(1, key), .kind = OpKind::kPut, .value = new_value});
        }
        ASSERT_TRUE(core().apply(2, inserts).ok());
        if (chunk) {
            chunk->store->request_wal_offset(8192);
        }
        const auto flushed = tree().flush();
        ASSERT_TRUE(flushed.ok()) << flushed.to_string();
        const auto split = core().capture();
        ASSERT_GT(split->mapping.get(split->root_page_id)->level, 0U);
        ASSERT_NE(split->root_page_id, before->root_page_id);
        ASSERT_GT(split->stats.dirty_pages, 1U);
        EXPECT_EQ(split->coverage, 2U);
        EXPECT_EQ(split->snapshot_baseline, before->snapshot_baseline);
        ASSERT_FALSE(split->snapshot_changes.empty());
        expect_durable(1, 1);

        // Write back the old root's surviving leaf. It already exists in the
        // baseline, so the next checkpoint must select its pending change even
        // after writeback clears dirty. Other split pages remain dirty.
        const auto selected = split->mapping.oldest_writeback_leaf();
        if (!selected.has_value()) {
            FAIL() << "Expected a writeback leaf after splitting the root";
        }
        ASSERT_EQ(*selected, before->root_page_id);
        ASSERT_TRUE(before->snapshot_baseline);
        ASSERT_TRUE(before->snapshot_baseline->pages->frames.contains(*selected));
        std::shared_ptr<CheckpointOperation> writeback;
        ASSERT_TRUE(core().prepare_writeback(&writeback).ok());
        const auto written = detail::wait_snapshot_operation(writeback);
        ASSERT_TRUE(written.ok()) << written.to_string();
        EXPECT_TRUE(writeback->result().writeback_completed);
        EXPECT_FALSE(writeback->result().durable_committed);
        EXPECT_EQ(writeback->result().primary_frames, 1U);
        EXPECT_EQ(writeback->result().written_frames, 1U);
        const auto partial = core().capture();
        const auto clean   = partial->mapping.get(*selected);
        ASSERT_TRUE(clean->primary);
        EXPECT_FALSE(clean->metrics.dirty);
        EXPECT_EQ(partial->stats.dirty_pages, split->stats.dirty_pages - 1);
        EXPECT_GT(partial->stats.dirty_pages, 0U);
        EXPECT_TRUE(partial->snapshot_changes.same_root(split->snapshot_changes));
        EXPECT_EQ(partial->snapshot_baseline, before->snapshot_baseline);
        expect_durable(1, 1);
        writeback.reset();

        if (commit) {
            // No new writes: persist the remaining dirty pages, reuse the
            // written leaf, and publish the complete split topology once.
            const auto persisted = tree().flush_with_persistent();
            ASSERT_TRUE(persisted.ok()) << persisted.to_string();
            expect_durable(2, 2);
            const auto durable = core().capture();
            EXPECT_EQ(durable->stats.dirty_pages, 0U);
            EXPECT_TRUE(durable->snapshot_changes.empty());
            ASSERT_TRUE(durable->mapping.get(*selected)->primary);
            EXPECT_EQ(durable->mapping.get(*selected)->primary->frame.page_block, clean->primary->frame.page_block);
            EXPECT_GT(tree().last_snapshot_pages_written(), 0U);
            EXPECT_LT(tree().last_snapshot_pages_written(), split->stats.dirty_pages);
            expected_root = durable->root_page_id;
        }
    }
    if (chunk) {
        chunk->tree.reset();
    }
    else {
        local_tree.reset();
    }
    const auto reopened = open();
    ASSERT_TRUE(reopened.ok()) << reopened.to_string();
    EXPECT_EQ(core().durable_coverage(), commit ? 2U : 1U);
    EXPECT_EQ(core().snapshot_sequence(), commit ? 2U : 1U);
    EXPECT_EQ(core().capture()->root_page_id, expected_root);
    if (chunk) {
        EXPECT_EQ(chunk->store->durable_wal_offset(), commit ? 8192U : 4096U);
    }
    for (char key = 'a'; key <= 'z'; ++key) {
        GetView value;
        ASSERT_TRUE(core().get(std::string(1, key), &value).ok());
        const bool present = commit || key == 'a' || key == 'z';
        ASSERT_EQ(value.found(), present) << key;
        if (present) {
            EXPECT_EQ(value.value().to_string(), commit && key != 'z' ? new_value : old_value) << key;
            EXPECT_EQ(value.slot(), commit && key != 'z' ? 2U : 1U) << key;
        }
    }
}

INSTANTIATE_TEST_SUITE_P(LocalAndChunk, NormalFlushPersistence,
                         ::testing::Combine(::testing::Bool(), ::testing::Bool()),
                         [](const ::testing::TestParamInfo<NormalFlushPersistence::ParamType> &info) {
                             return std::string(std::get<0>(info.param) ? "Chunk" : "Local") +
                                    (std::get<1>(info.param) ? "Commit" : "ReopenBeforeCommit");
                         });
} // namespace crowdb::tree
