// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/overflow_page_block.h"
#include "btree/tree_core_access.h"
#include "local_checkpoint_test_support.h"
#include "native_import_test_support.h"

namespace crowdb::tree
{
namespace
{
struct TestNativePageStore
{
    std::shared_ptr<PageMemStore>   device = std::make_shared<PageMemStore>(4096);
    test_write::TestCheckpointStore store;

    TestNativePageStore()
    {
        store.device = device;
    }
};

TEST(TreeCoreNativeCheckpoint, ImportedResidentOverflowBecomesDurableAndReopens)
{
    auto   backend = std::make_shared<TestNativePageStore>();
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store                           = backend->device.get();
    config.async_page_store                     = &backend->store;
    config.page_store_owner                     = backend;
    config.frame_bytes                          = 4096;
    config.max_inline_value                     = 64;
    config.runtime_binding.limits.io_operations = 1;
    std::unique_ptr<Crowdbtree> tree;
    auto                        status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    const std::string value(12000, 'p');
    auto              frames = test_import::overflow_tree(value);
    const auto        count  = frames.size();
    status                   = tree->install_snapshot_native(std::move(frames), 0, 7, count);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto retained = tree->get_view("key");
    ASSERT_TRUE(retained.found());
    uint64_t coverage = 0;
    uint64_t sequence = 0;
    status            = tree->flush_with_persistent(&coverage, &sequence);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(coverage, 7U);
    EXPECT_EQ(sequence, 1U);
    const auto page = TreeCoreAccess::get(*tree)->capture()->mapping.get(0);
    ASSERT_NE(page, nullptr);
    const auto primary = page->leaf->bases.front().page_block;
    ASSERT_NE(primary, nullptr);
    EXPECT_TRUE(primary->reloadable());
    ASSERT_NE(primary->overflow, nullptr);
    EXPECT_EQ(primary->overflow->pages.size(), count - 1);
    for (const auto &[pid, dependency] : primary->overflow->pages) {
        EXPECT_NE(pid, 0U);
        EXPECT_TRUE(dependency.page_block->reloadable());
        EXPECT_EQ(dependency.page_block->page_store_id, primary->page_store_id);
    }
    tree.reset();
    status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->durable_coverage(), 7U);
    EXPECT_EQ(tree->get_view("key").value().to_string(), value);
    EXPECT_EQ(retained.value().to_string(), value);
    ASSERT_TRUE(tree->put("after", "value").ok());
    EXPECT_EQ(tree->get_view("after").slot(), 8U);
}

TEST(TreeCoreNativeCheckpoint, SharedResidentChainIsWrittenOnceWithoutPinningTheRawImport)
{
    auto   backend = std::make_shared<TestNativePageStore>();
    Config config;
    ASSERT_TRUE(TreeRuntime::create({}, &config.runtime).ok());
    config.page_store       = backend->device.get();
    config.async_page_store = &backend->store;
    config.page_store_owner = backend;
    config.frame_bytes      = 4096;
    config.max_inline_value = 64;
    config.leaf_merge_bytes = 0;
    std::unique_ptr<Crowdbtree> tree;
    auto                        status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    NativeFrame index{.page_id = 0, .frame = std::vector<uint8_t>(4096)};
    ASSERT_TRUE(inner_frame_build(index.frame.data(), 4096, 0, {1, 2}, {Slice("m")}));
    frame_set_inner_fence_pages(index.frame.data(), 4096, 1, 2);
    const std::string value(1000, 's');
    const auto        cell = encode_overflow_cell_buf(7, 3, value.size());
    NativeFrame       overflow{.page_id = 3, .frame = std::vector<uint8_t>(4096)};
    overflow_frame_build(overflow.frame.data(), 4096, 3, kInvalidPageId,
                         reinterpret_cast<const uint8_t *>(value.data()), value.size());
    std::vector<NativeFrame> frames;
    frames.push_back(std::move(index));
    frames.push_back(test_import::leaf(1, "a", Slice(cell), 2));
    frames.push_back(test_import::leaf(2, "z", Slice(cell)));
    frames.push_back(std::move(overflow));
    status = tree->install_snapshot_native(std::move(frames), 0, 7, 4);
    ASSERT_TRUE(status.ok()) << status.to_string();
    auto                     *core = TreeCoreAccess::get(*tree);
    std::weak_ptr<const void> raw_owner =
        core->capture()->mapping.get(1)->leaf->bases.front().page_block->overflow->pages.at(3).page_block->owner;
    status = tree->flush_with_persistent();
    ASSERT_TRUE(status.ok()) << status.to_string();
    const auto first  = core->capture()->mapping.get(1)->leaf->bases.front().page_block;
    const auto second = core->capture()->mapping.get(2)->leaf->bases.front().page_block;
    ASSERT_NE(first->overflow, nullptr);
    ASSERT_NE(second->overflow, nullptr);
    EXPECT_EQ(first->overflow->pages.at(3).page_block, second->overflow->pages.at(3).page_block);
    EXPECT_TRUE(first->overflow->pages.at(3).page_block->reloadable());
    tree.reset();
    EXPECT_TRUE(raw_owner.expired());
    status = Crowdbtree::open(config, &tree);
    ASSERT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(tree->get_view("a").value().to_string(), value);
    EXPECT_EQ(tree->get_view("z").value().to_string(), value);
}
} // namespace
} // namespace crowdb::tree
