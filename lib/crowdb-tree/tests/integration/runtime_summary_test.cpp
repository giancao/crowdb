// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_tree_core_chunk.h"
#include "snapshot/chunk_root_codec.h"

namespace crowdb::tree
{
namespace
{
Batch summary_data()
{
    Batch batch;
    for (size_t i = 0; i < 80; ++i) {
        batch.ops.push_back(
            {.key = std::to_string(1000 + i), .kind = OpKind::kPut, .value = std::string(i == 0 ? 20000 : 60, 'x')});
    }
    return batch;
}

void check_summary(const Crowdbtree &tree, uint64_t keys, uint64_t key_bytes, uint64_t values)
{
    const auto summary = tree.tree_summary();
    ASSERT_TRUE(summary.exact);
    EXPECT_EQ(summary.live_kv, keys);
    EXPECT_EQ(summary.live_key_bytes, key_bytes);
    EXPECT_EQ(summary.live_value_bytes, values);
    EXPECT_EQ(summary.live_logical_bytes, key_bytes + values);
    EXPECT_GT(summary.reachable_leaf_pages, 1U);
    EXPECT_GT(summary.reachable_inner_pages, 0U);
    EXPECT_GT(summary.reachable_overflow_pages, 0U);
    EXPECT_EQ(summary.reachable_page_capacity_bytes,
              4096 * (summary.reachable_leaf_pages + summary.reachable_inner_pages + summary.reachable_overflow_pages));
}
} // namespace

TEST(RuntimeSummary, CheckpointAndRecoveryExposeCompleteCountsWithoutLoadingPages)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    check_summary(*test.tree, 80, 320, 20000 + (79 * 60));
    const auto saved = test.tree->tree_summary();
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    const auto metrics = TreeCoreAccess::get(*test.tree)->publisher().work_metrics();
    const auto loads   = metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count;
    const auto encodes = metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count;
    check_summary(*test.tree, 80, 320, 20000 + (79 * 60));
    EXPECT_EQ(test.tree->tree_summary().reachable_page_capacity_bytes, saved.reachable_page_capacity_bytes);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageLoadPipeline)].count, loads);
    EXPECT_EQ(metrics->totals()[static_cast<size_t>(TreeStage::PageEncode)].count, encodes);
}

TEST(RuntimeSummary, UnflushedAndPublishedWritesInvalidateOnlyTheCurrentSummary)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    auto old = TreeCoreAccess::get(*test.tree)->capture();
    if (!old->summary) {
        FAIL() << "fixed summary missing after checkpoint";
    }
    ASSERT_TRUE(test.tree->apply(2, Batch{.ops = {{.key = "1001", .kind = OpKind::kDelete, .value = {}}}}).ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    ASSERT_TRUE(test.tree->flush().ok());
    EXPECT_FALSE(test.tree->tree_summary().exact);
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    check_summary(*test.tree, 79, 316, 20000 + (78 * 60));
    EXPECT_EQ(old->summary->live_kv, 80U);
    EXPECT_EQ(old->summary->live_value_bytes, 20000 + (79 * 60));
}

TEST(RuntimeSummary, OrdinaryAndPersistentFlushKeepRoutingForSummaryOnlyChanges)
{
    test_chunk::TestCoreChunk test(1024);
    test.config.inner_max_keys   = 3;
    test.config.leaf_split_bytes = 800;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    auto      *core  = TreeCoreAccess::get(*test.tree);
    const auto prior = core->capture();
    ASSERT_GT(prior->stats.height, 2U);

    ASSERT_TRUE(
        test.tree->apply(2, Batch{.ops = {{.key = "1001", .kind = OpKind::kPut, .value = std::string(59, 'y')}}}).ok());
    ASSERT_TRUE(test.tree->flush().ok());
    const auto ordinary = core->capture();
    EXPECT_EQ(test.tree->tree_summary().live_value_bytes, 20000 + (79 * 60) - 1);
    size_t     updated       = 0;
    const auto check_indexes = [&](const TreeRoot &view) {
        size_t found = 0;
        for (uint64_t pid = 0; pid < prior->next_page_id; ++pid) {
            const auto page = prior->mapping.get(pid);
            if (!page || page->level == 0) {
                continue;
            }
            EXPECT_EQ(view.mapping.get(pid), page);
            if (const auto summary = view.index_summaries.find(pid)) {
                EXPECT_TRUE(summary->exact);
                ++found;
            }
        }
        return found;
    };
    updated = check_indexes(*ordinary);
    EXPECT_GT(updated, 0U);
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    const auto durable = core->capture();
    EXPECT_EQ(check_indexes(*durable), updated);
    EXPECT_EQ(test.tree->tree_summary().live_value_bytes, 20000 + (79 * 60) - 1);
    ASSERT_TRUE(
        test.tree->apply(3, Batch{.ops = {{.key = "1079", .kind = OpKind::kPut, .value = std::string(57, 'z')}}}).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    const auto latest = core->capture();
    EXPECT_GT(check_indexes(*latest), updated);
    for (uint64_t pid = 0; pid < prior->next_page_id; ++pid) {
        if (const auto old = durable->index_summaries.find(pid)) {
            EXPECT_EQ(latest->index_summaries.find(pid), old);
        }
    }

    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    const auto reopened = TreeCoreAccess::get(*test.tree)->capture();
    ASSERT_EQ(reopened->index_summaries.size(), latest->index_summaries.size());
    for (uint64_t pid = 0; pid < prior->next_page_id; ++pid) {
        EXPECT_EQ(reopened->index_summaries.find(pid), latest->index_summaries.find(pid));
    }
    EXPECT_EQ(test.tree->tree_summary().live_value_bytes, 20000 + (79 * 60) - 4);
    const auto writes = test.backend.transport->writes;
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    EXPECT_EQ(test.backend.transport->writes, writes);
}

TEST(RuntimeSummary, FailedChunkRootCommitKeepsPriorSummaryAndRetriesTheSameInput)
{
    test_chunk::TestCoreChunk test(1024);
    test.config.inner_max_keys   = 3;
    test.config.leaf_split_bytes = 800;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    const auto committed = test.backend.catalog->committed_manifest;

    ASSERT_TRUE(
        test.tree->apply(2, Batch{.ops = {{.key = "1001", .kind = OpKind::kPut, .value = std::string(59, 'y')}}}).ok());
    test.backend.catalog->fail_commit = true;
    EXPECT_EQ(test.tree->flush_with_persistent().code(), Code::kUnavailable);
    EXPECT_EQ(test.tree->durable_coverage(), 1U);
    EXPECT_EQ(test.backend.catalog->committed_manifest, committed);
    EXPECT_EQ(test.backend.catalog->abort_calls, 1U);
    ChunkRootManifest aborted(test.backend.credits.credits.reservation());
    ASSERT_TRUE(decode_chunk_root(test.backend.catalog->manifest, 19, 2, &aborted).ok());
    if (!aborted.summaries) {
        FAIL() << "aborted candidate had no summary pack";
    }
    EXPECT_GT(aborted.summaries->length, 0U);

    test.backend.catalog->fail_commit = false;
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    EXPECT_EQ(test.tree->durable_coverage(), 2U);
    const auto saved = TreeCoreAccess::get(*test.tree)->capture();
    ASSERT_GT(saved->index_summaries.size(), 0U);
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    const auto restored = TreeCoreAccess::get(*test.tree)->capture();
    EXPECT_TRUE(restored->index_summaries.same(saved->index_summaries));
    EXPECT_EQ(test.tree->tree_summary().live_value_bytes, 20000 + (79 * 60) - 1);
    EXPECT_EQ(test.tree->get_view("1001").value().to_string(), std::string(59, 'y'));
}

TEST(RuntimeSummary, RetiredIndexOverridesLeaveTheNewRootAndStayVisibleInTheOldView)
{
    test_chunk::TestCoreChunk test(1024);
    test.config.inner_max_keys   = 3;
    test.config.leaf_split_bytes = 800;
    ASSERT_TRUE(test.open().ok());
    ASSERT_TRUE(test.tree->apply(1, summary_data()).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    ASSERT_TRUE(
        test.tree->apply(2, Batch{.ops = {{.key = "1079", .kind = OpKind::kPut, .value = std::string(59, 'y')}}}).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    auto      *core = TreeCoreAccess::get(*test.tree);
    const auto old  = core->capture();
    auto       held = test.tree->get_view("1079");
    ASSERT_GT(old->index_summaries.size(), 0U);

    for (size_t i = 2; i < 80; ++i) {
        ASSERT_TRUE(
            test.tree
                ->apply(i + 1, Batch{.ops = {{.key = std::to_string(1000 + i), .kind = OpKind::kDelete, .value = {}}}})
                .ok());
        test.tree->set_gc_watermark(i + 1, i + 1);
        ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    }
    const auto current = core->capture();
    size_t     removed = 0;
    for (uint64_t pid = 0; pid < old->next_page_id; ++pid) {
        if (old->index_summaries.find(pid) && !current->mapping.get(pid)) {
            EXPECT_FALSE(current->index_summaries.find(pid));
            ++removed;
        }
    }
    EXPECT_GT(removed, 0U);
    EXPECT_EQ(held.value().to_string(), std::string(59, 'y'));
    test.tree.reset();
    ASSERT_TRUE(test.open().ok());
    EXPECT_TRUE(TreeCoreAccess::get(*test.tree)->capture()->index_summaries.same(current->index_summaries));
    const auto summary = test.tree->tree_summary();
    EXPECT_TRUE(summary.exact);
    EXPECT_EQ(summary.live_kv, 2U);
    EXPECT_EQ(summary.live_value_bytes, 20060U);
    EXPECT_FALSE(test.tree->get_view("1079").found());
}
} // namespace crowdb::tree
