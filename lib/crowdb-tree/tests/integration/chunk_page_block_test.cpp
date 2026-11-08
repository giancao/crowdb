// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_cache_identity.h"
#include "common/test_chunk_page_block.h"
#include "page_store/page_block_io.h"
#include "page_writeback_test_support.h"

#include <future>

namespace crowdb::tree
{
using test_chunk::TestPageChunkStore;

TEST(ChunkPageBlock, SealedWriteCreditProducesDirectLeafReloadWithoutActiveManifest)
{
    TestPageChunkStore              test;
    std::shared_ptr<ChunkPageBlock> pack;
    ASSERT_TRUE(test.prepare(8192, &pack).ok());
    PageWriteLimits limits;
    limits.max_frame_bytes = 4096;
    limits.compression     = compress_algo::kNone;
    limits.chunk_backed    = true;
    std::shared_ptr<PageWriteOperation> write;
    ASSERT_TRUE(PageWriteOperation::create(
                    test.scheduled, test.binding->cpu(), test.binding->lane(), test_write::write_input(), limits,
                    test.credits.credits,
                    [pack](uint64_t length, PageWriteOperation::ExtentComplete complete) {
                        complete(Status::Ok(), {.page_store_id    = 17,
                                                .address          = 0,
                                                .length           = length,
                                                .content_id       = 0,
                                                .generation       = 1,
                                                .owner            = pack,
                                                .physical_metrics = {},
                                                .chunk            = pack});
                    },
                    &write)
                    .ok());
    test.credits.seal();
    ASSERT_TRUE(stdexec::sync_wait(write->sender()));
    test.binding->drain();
    const auto page_block = write->result().page_block;
    ASSERT_TRUE(page_block);
    EXPECT_EQ(page_block->storage, PageBlockStorage::kChunkPack);
    EXPECT_TRUE(page_block->reloadable());
    EXPECT_EQ(test.empty_device.size(), 0U);
    test_read::LoadCredits reads;
    PageLoadLimits         read_limits;
    read_limits.max_blob_bytes       = 8192;
    read_limits.max_frame_bytes      = 4096;
    read_limits.max_chunk_blob_bytes = 8192;
    AsyncStorePageLoader            loader(test.scheduled, test.binding->cpu(), test.binding->lane(), read_limits, {},
                                           reads.credits);
    std::promise<Status>            completed;
    std::shared_ptr<const LeafBase> loaded;
    loader.load_leaf({.identity = 7, .max_slot = 3, .page_block = page_block}, [&](Status status, auto leaf) {
        loaded = std::move(leaf);
        completed.set_value(std::move(status));
    });
    ASSERT_TRUE(completed.get_future().get().ok());
    test.binding->drain();
    ASSERT_TRUE(loaded);
    EXPECT_EQ(CellView(loaded->cell(0)).value(), Slice("value"));
    EXPECT_EQ(test.catalog->reader_checks, 4U);
}

TEST(ChunkPageBlock, ProcessFenceAfterIoRejectsTheReadBeforeExposingBytes)
{
    TestPageChunkStore              test;
    std::shared_ptr<ChunkPageBlock> pack;
    ASSERT_TRUE(test.prepare(32, &pack).ok());
    std::array<uint8_t, 32> input{};
    ASSERT_TRUE(write_chunk_page_block(*pack, input, test.credits.credits.reservation()).ok());
    test.catalog->fence_at    = 4;
    auto page_block           = std::make_shared<PageBlockRef>();
    page_block->page_store_id = 17;
    page_block->length        = input.size();
    page_block->storage       = PageBlockStorage::kChunkPack;
    page_block->chunk         = pack;
    page_block->owner         = pack;
    std::array<uint8_t, 32> output{};
    std::promise<Status>    completed;
    submit_page_block_read(
        *test.scheduled, page_block, output.data(), output.size(), test.credits.credits.reservation(),
        {.context     = &completed,
         .complete_fn = [](void  *raw,
                           Status status) { static_cast<std::promise<Status> *>(raw)->set_value(std::move(status)); },
         .owner       = page_block});
    EXPECT_EQ(completed.get_future().get().code(), Code::kUnavailable);
    test.binding->drain();
    EXPECT_EQ(test.catalog->reader_checks, 4U);
}

TEST(ChunkPageBlock, UnknownAllocationRetainsItsExactPaidPinUntilHostReleaseSucceeds)
{
    TestPageChunkStore              test;
    std::shared_ptr<ChunkPageBlock> pack;
    ASSERT_TRUE(test.prepare(32, &pack).ok());
    test.transport->unknown_allocate = true;
    std::array<uint8_t, 32> input{};
    EXPECT_EQ(write_chunk_page_block(*pack, input, test.credits.credits.reservation()).code(), Code::kUnavailable);
    if (!test.catalog->record) {
        FAIL() << "unknown Allocate lost its StageReserved record";
    }
    EXPECT_EQ(test.catalog->record->state, detail::ChunkStageState::Reserved);
    EXPECT_EQ(test.catalog->queue->snapshot().registered, 1U);
    test.catalog->fail_unpin = true;
    pack.reset();
    EXPECT_EQ(test.catalog->queue->pump(1).failed, 1U);
    EXPECT_EQ(test.catalog->queue->snapshot().registered, 1U);
    EXPECT_EQ(test.transport->allocations, 1U);
    test.catalog->fail_unpin = false;
    EXPECT_EQ(test.catalog->queue->pump(1).completed, 1U);
    EXPECT_EQ(test.catalog->queue->snapshot().registered, 0U);
}

TEST(ChunkPageBlock, CacheIdentityIncludesExactChunkAndStageInsteadOfSyntheticAddress)
{
    TestPageChunkStore              test;
    std::shared_ptr<ChunkPageBlock> pack;
    ASSERT_TRUE(test.prepare(32, &pack).ok());
    auto a           = std::make_shared<PageBlockRef>();
    a->storage       = PageBlockStorage::kChunkPack;
    a->page_store_id = 17;
    a->length        = 32;
    a->owner         = pack;
    a->chunk         = pack;
    auto other       = std::make_shared<ChunkPageBlock>(*pack);
    auto b           = std::make_shared<PageBlockRef>(*a);
    b->chunk         = other;
    const auto key   = page_cache_key(PageCacheKind::Leaf, {.identity = 7, .page_block = a});
    EXPECT_EQ(key, page_cache_key(PageCacheKind::Leaf, {.identity = 7, .page_block = b}));
    ++other->pack.chunk.low;
    EXPECT_NE(key, page_cache_key(PageCacheKind::Leaf, {.identity = 7, .page_block = b}));
    other->pack = pack->pack;
    ++other->pack.stage.operation_high;
    EXPECT_NE(key, page_cache_key(PageCacheKind::Leaf, {.identity = 7, .page_block = b}));
    EXPECT_FALSE((PageBlockExclusion{.address = 0, .length = 4096}).overlaps(*a));
}
} // namespace crowdb::tree
