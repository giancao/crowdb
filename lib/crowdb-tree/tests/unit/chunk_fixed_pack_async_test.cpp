// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_root.h"

#include <future>

namespace crowdb::tree::test_chunk
{
TEST(ChunkFixedPackAsync, MirrorFailureJoinsEveryAcceptedWriteBeforeTerminatingAndKeepsTheStage)
{
    TestRootPageStore                                              backend;
    std::array<std::optional<detail::ChunkTransportCompletion>, 2> pending;
    backend.transport->after_write_async = [&](uint32_t mirror, auto completion) {
        pending[mirror] = std::move(completion);
        return true;
    };
    detail::ChunkFixedPackStore store(19, 1, 2, backend.catalog, backend.transport);
    detail::ChunkStageId        stage;
    detail::ChunkId             chunk;
    ASSERT_TRUE(backend.ids->next(PagePurpose::kBtreePage, &stage, &chunk).ok());
    std::array<uint8_t, 32>                  bytes{};
    detail::ChunkFixedPackRef                ref;
    const detail::ChunkFixedPackStore::Write input{.input = bytes, .out = &ref};
    std::promise<Status>                     result;
    auto                                     future = result.get_future();
    store.submit_write_many(stage, chunk, PagePurpose::kBtreePage, std::span(&input, 1),
                            backend.credits.credits.reservation(),
                            {.context = &result, .complete_fn = +[](void *raw, Status status) {
                                 static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
                             }});
    ASSERT_TRUE(pending[1]);
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    auto &first_pending = pending[0];
    if (!first_pending) {
        FAIL() << "first mirror write was not submitted";
    }
    auto first = std::move(*first_pending);
    first_pending.reset();
    first.complete(Status::io_error());
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    detail::ChunkLayout layout;
    ASSERT_TRUE(backend.transport->query_chunk(chunk, &layout).ok());
    EXPECT_EQ(layout.acknowledged_bytes, 0U);
    auto &second_pending = pending[1];
    if (!second_pending) {
        FAIL() << "second mirror write was not submitted";
    }
    auto second = std::move(*second_pending);
    second_pending.reset();
    second.complete(Status::Ok());
    ASSERT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::ready);
    EXPECT_EQ(future.get().code(), Code::kIoError);
    EXPECT_EQ(ref.length, 0U);
    std::optional<detail::ChunkStageRecord> record;
    ASSERT_TRUE(backend.catalog->load_stage(19, stage, &record).ok());
    if (!record) {
        FAIL() << "stage record was not retained after a mirror failed";
    }
    EXPECT_EQ(record->state, detail::ChunkStageState::Writing);
}
} // namespace crowdb::tree::test_chunk
