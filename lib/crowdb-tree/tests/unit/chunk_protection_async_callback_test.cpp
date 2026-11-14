// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_c_api_internal.h"
#include "common/test_protection_callbacks.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
struct TestAsyncProtection : test_protection::TestProtection
{
    ct_catalog_status_complete complete           = nullptr;
    void                      *completion_context = nullptr;
    const ct_chunk_stage_id   *cursor             = nullptr;
    ct_chunk_candidate_status *candidates         = nullptr;
    size_t                    *count              = nullptr;
    ct_chunk_pin_progress     *progress           = nullptr;
    ct_catalog_blob_complete   root_complete      = nullptr;
    uint64_t                   generation         = 0;
    bool                       inline_pin         = false;
    ct_status                  rejection          = 0;

    ct_status accept(ct_catalog_status_complete callback, void *context)
    {
        if (rejection != 0) {
            return rejection;
        }
        complete           = callback;
        completion_context = context;
        return 0;
    }

    static ct_root_catalog_protection_async_callbacks asynchronous()
    {
        return {.version      = 1,
                .struct_bytes = sizeof(ct_root_catalog_protection_async_callbacks),
                .writer_quiesced =
                    +[](void *, uint64_t, uint64_t, ct_catalog_status_complete, void *) -> ct_status { return -5; },
                .begin = +[](void *, uint64_t, ct_chunk_stage_id, uint64_t, uint64_t, uint32_t, const uint8_t *, size_t,
                             ct_catalog_status_complete, void *) -> ct_status { return -5; },
                .load  = +[](void *, uint64_t, ct_chunk_stage_id, ct_chunk_candidate_status *,
                             ct_catalog_status_complete, void *) -> ct_status { return -5; },
                .list =
                    +[](void *raw, uint64_t, const ct_chunk_stage_id *cursor, uint32_t, ct_chunk_candidate_status *out,
                        size_t *count, ct_catalog_status_complete done, void *context) -> ct_status {
                    auto &self      = *static_cast<TestAsyncProtection *>(raw);
                    self.cursor     = cursor;
                    self.candidates = out;
                    self.count      = count;
                    return self.accept(done, context);
                },
                .prepare = +[](void *, uint64_t, ct_chunk_stage_id, uint64_t, const ct_chunk_stage_id *, size_t,
                               ct_catalog_status_complete, void *) -> ct_status { return -5; },
                .finish  = +[](void *, uint64_t, ct_chunk_stage_id, uint32_t, ct_catalog_status_complete,
                               void *) -> ct_status { return -5; },
                .release = +[](void *, uint64_t, ct_chunk_stage_id, uint64_t, ct_catalog_status_complete,
                               void *) -> ct_status { return -5; },
                .pin     = +[](void                      *raw, uint64_t, ct_chunk_stage_id, ct_chunk_stage_id, uint8_t,
                               ct_catalog_status_complete done, void *context) -> ct_status {
                    auto &self = *static_cast<TestAsyncProtection *>(raw);
                    if (self.inline_pin) {
                        done(context, 0);
                        return 0;
                    }
                    return self.accept(done, context);
                },
                .prune = +[](void *raw, uint64_t, ct_chunk_stage_id, const ct_chunk_stage_id *cursor, uint32_t,
                             ct_chunk_pin_progress *out, ct_catalog_status_complete done, void *context) -> ct_status {
                    auto &self    = *static_cast<TestAsyncProtection *>(raw);
                    self.cursor   = cursor;
                    self.progress = out;
                    return self.accept(done, context);
                },
                .root = +[](void *raw, uint64_t tree, uint64_t generation, ct_catalog_blob_complete done,
                            void *context) -> ct_status {
                    auto &self = *static_cast<TestAsyncProtection *>(raw);
                    EXPECT_EQ(tree, 42U);
                    self.generation         = generation;
                    self.root_complete      = done;
                    self.completion_context = context;
                    return 0;
                }};
    }

    std::shared_ptr<CallbackRootCatalog> open()
    {
        auto            catalog = std::make_shared<CallbackRootCatalog>(base(), stages(), protection(), this);
        ct_root_catalog handle{.catalog = catalog, .transport = {}};
        const auto      callbacks = asynchronous();
        EXPECT_EQ(ct_callback_root_catalog_set_protection_async_v1(&handle, &callbacks), 0);
        return catalog;
    }
};

struct TestReply
{
    size_t calls = 0;
    Status status;

    AsyncCompletion callback()
    {
        return {.context = this, .complete_fn = +[](void *raw, Status status) {
                    auto &self = *static_cast<TestReply *>(raw);
                    ++self.calls;
                    self.status = std::move(status);
                }};
    }
};

constexpr ChunkStageId stage{.owner_epoch = 1, .operation_high = 2, .operation_low = 3};
} // namespace

TEST(ChunkProtectionAsyncCallback, ListRetainsCursorOutputAndCatalogUntilDelayedCompletion)
{
    TestAsyncProtection                context;
    auto                               catalog  = context.open();
    std::weak_ptr<CallbackRootCatalog> retained = catalog;
    std::vector<ChunkCandidateStatus>  result;
    TestReply                          reply;
    catalog->submit_list_candidates(42, stage, 1, &result, reply.callback());
    EXPECT_EQ(reply.calls, 0U);
    catalog.reset();
    EXPECT_FALSE(retained.expired());
    ASSERT_NE(context.cursor, nullptr);
    EXPECT_EQ(context.cursor->operation_low, 3U);
    *context.count        = 1;
    context.candidates[0] = {
        .id                  = {.owner_epoch = 1, .operation_high = 2, .operation_low = 4},
        .generation          = 1,
        .reference_count     = 1,
        .prepared_references = 1,
        .prepared_windows    = 1,
        .released_windows    = 0,
        .state               = 2
    };
    context.complete(context.completion_context, 0);
    EXPECT_EQ(reply.calls, 1U);
    ASSERT_TRUE(reply.status.ok());
    ASSERT_EQ(result.size(), 1U);
    EXPECT_EQ(result[0].id.operation_low, 4U);
    EXPECT_TRUE(retained.expired());
    EXPECT_EQ(context.drops, 1);
}

TEST(ChunkProtectionAsyncCallback, InlineReplyAndRejectedSubmissionEachCompleteExactlyOnce)
{
    TestAsyncProtection context;
    auto                catalog = context.open();
    TestReply           reply;
    context.inline_pin = true;
    catalog->submit_change_pin(42, stage, stage, true, reply.callback());
    EXPECT_EQ(reply.calls, 1U);
    EXPECT_TRUE(reply.status.ok());
    context.inline_pin = false;
    context.rejection  = -8;
    catalog->submit_change_pin(42, stage, stage, true, reply.callback());
    EXPECT_EQ(reply.calls, 2U);
    EXPECT_EQ(reply.status.code(), Code::kUnavailable);
}

TEST(ChunkProtectionAsyncCallback, PruneRetainsCursorAndRejectsAReplyThatDoesNotAdvance)
{
    TestAsyncProtection context;
    auto                catalog = context.open();
    TestReply           reply;
    ChunkPinProgress    progress;
    catalog->submit_prune_pins(42, stage, stage, 1, &progress, reply.callback());
    EXPECT_EQ(reply.calls, 0U);
    ASSERT_NE(context.cursor, nullptr);
    EXPECT_EQ(context.cursor->operation_low, 3U);
    *context.progress = {.after = *context.cursor, .examined = 1, .released = 0, .has_after = 1, .more = 1};
    context.complete(context.completion_context, 0);
    EXPECT_EQ(reply.calls, 1U);
    EXPECT_EQ(reply.status.code(), Code::kCorruption);
}

TEST(ChunkProtectionAsyncCallback, RootBytesKeepTheRequestedGenerationAndReleaseTheBlobAfterReply)
{
    TestAsyncProtection context;
    auto                catalog = context.open();
    TestReply           reply;
    ChunkRootBytes      bytes;
    catalog->submit_load_root_owned(42, 9, &bytes, reply.callback());
    EXPECT_EQ(reply.calls, 0U);
    EXPECT_EQ(context.generation, 9U);
    catalog.reset();
    EXPECT_EQ(context.drops, 0);
    context.root_complete(context.completion_context, 0, context.root.data(), context.root.size());
    EXPECT_EQ(reply.calls, 1U);
    EXPECT_TRUE(reply.status.ok());
    EXPECT_EQ(bytes, (ChunkRootBytes{1, 2, 3}));
    EXPECT_EQ(context.free_blobs, 1);
    EXPECT_EQ(context.drops, 1);
}
} // namespace crowdb::tree::detail
