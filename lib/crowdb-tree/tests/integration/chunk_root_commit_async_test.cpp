// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_protocol_operation.h"
#include "backend/chunk/chunk_write_queue.h"
#include "common/test_chunk_root.h"

#include <future>

namespace crowdb::tree
{
namespace
{
struct TestRootAsync
{
    test_chunk::TestRootBackend                                   backend;
    std::shared_ptr<TreeRuntime>                                  runtime;
    std::shared_ptr<TreeRuntimeBinding>                           binding;
    std::shared_ptr<AsyncPageStore>                               store;
    MemPageStore                                                  device{4096};
    std::shared_ptr<ChunkRootCommit>                              commit;
    std::shared_ptr<ChunkProtocolOperation>                       operation;
    std::array<std::promise<detail::ChunkTransportCompletion>, 2> pending;
    std::array<std::future<detail::ChunkTransportCompletion>, 2>  receipts{pending[0].get_future(),
                                                                           pending[1].get_future()};
    std::atomic<size_t>                                           submitted{0};

    TestRootAsync()
    {
        TreeRuntimeOptions options;
        options.workers         = 1;
        options.backend_workers = 1;
        EXPECT_TRUE(TreeRuntime::create(options, &runtime).ok());
        EXPECT_TRUE(TreeRuntimeBinding::create(runtime, {}, &binding).ok());
        EXPECT_TRUE(ScheduledPageStore::create(&device, nullptr, {}, binding, binding->lifecycle_budget(),
                                               binding->backend_lane(), &store, {}, &ChunkWriteQueue::create)
                        .ok());
        commit                                  = backend.commit(backend.pages(65));
        backend.transport->after_allocate_async = [this](auto completion) {
            const auto index = submitted.fetch_add(1);
            EXPECT_LT(index, pending.size());
            if (index < pending.size()) {
                pending[index].set_value(std::move(completion));
            }
            return true;
        };
        auto scheduled = std::static_pointer_cast<ScheduledPageStore>(store);
        EXPECT_TRUE(ChunkProtocolOperation::create_async(
                        store, binding->cpu(), binding->lane(), {.bytes = backend.credits.credits.reservation()},
                        commit,
                        [commit = commit, scheduled](bool stopped, AsyncCompletion complete) {
                            commit->submit_step(stopped, scheduled, std::move(complete));
                        },
                        [commit = commit] { return commit->done(); }, &operation)
                        .ok());
    }

    ~TestRootAsync()
    {
        binding->drain();
    }

    void resume(size_t index, Status status = Status::Ok())
    {
        struct Receipt
        {
            detail::ChunkTransportCompletion completion;
            Status                           status;
        };

        auto receipt   = std::make_shared<Receipt>(receipts[index].get(), std::move(status));
        auto scheduled = std::static_pointer_cast<ScheduledPageStore>(store);
        ASSERT_TRUE(scheduled
                        ->submit_step(
                            [receipt] {
                                receipt->completion.complete(std::move(receipt->status));
                                return Status::Ok();
                            },
                            {.complete_fn = +[](void *, Status status) { EXPECT_TRUE(status.ok()); }})
                        .ok());
    }
};

struct TestRootReceiver
{
    using receiver_concept = stdexec::receiver_tag;
    std::promise<Status> *result;

    void set_value() noexcept
    {
        result->set_value(Status::Ok());
    }

    void set_error(Status status) noexcept
    {
        result->set_value(std::move(status));
    }

    void set_stopped() noexcept
    {
        result->set_value(Status::unavailable());
    }
};

TEST(ChunkRootCommitAsync, IndependentIndexWritesStartBeforeEitherReplyAndUnknownMutationsKeepTheirIdentity)
{
    TestRootAsync test;
    test.backend.catalog->lose_begin   = true;
    test.backend.catalog->lose_prepare = true;
    test.backend.catalog->lose_commit  = true;
    std::promise<Status> result;
    auto                 future  = result.get_future();
    auto                 running = stdexec::connect(test.operation->sender(), TestRootReceiver{&result});
    stdexec::start(running);
    for (auto &receipt : test.receipts) {
        EXPECT_EQ(receipt.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    }
    EXPECT_EQ(test.submitted.load(), 2U);
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    EXPECT_EQ(test.backend.catalog->begin_calls, 0U);
    test.resume(1);
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    test.resume(0);
    const auto status = future.get();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(test.commit->committed());
    EXPECT_EQ(test.commit->manifest()->indexes.size(), 2U);
    EXPECT_LT(test.commit->manifest()->indexes[0].last_page, test.commit->manifest()->indexes[1].first_page);
    EXPECT_EQ(test.backend.catalog->begin_calls, 1U);
    EXPECT_EQ(test.backend.catalog->prepare_calls, 2U);
    EXPECT_EQ(test.backend.catalog->commit_calls, 1U);
}

TEST(ChunkRootCommitAsync, IndexFailureWaitsForEveryAcceptedReplyBeforeReturning)
{
    TestRootAsync        test;
    std::promise<Status> result;
    auto                 future  = result.get_future();
    auto                 running = stdexec::connect(test.operation->sender(), TestRootReceiver{&result});
    stdexec::start(running);
    for (auto &receipt : test.receipts) {
        EXPECT_EQ(receipt.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    }
    test.resume(0, Status::io_error());
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    test.resume(1);
    EXPECT_EQ(future.get().code(), Code::kIoError);
    EXPECT_FALSE(test.commit->committed());
    EXPECT_EQ(test.backend.catalog->begin_calls, 0U);
}

TEST(ChunkRootCommitAsync, CancellationDrainsTheAcceptedIndexWritesBeforeStoppingRootPublication)
{
    TestRootAsync        test;
    std::promise<Status> result;
    auto                 future  = result.get_future();
    auto                 running = stdexec::connect(test.operation->sender(), TestRootReceiver{&result});
    stdexec::start(running);
    for (auto &receipt : test.receipts) {
        EXPECT_EQ(receipt.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    }
    test.operation->request_stop();
    EXPECT_EQ(future.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    test.resume(0);
    test.resume(1);
    EXPECT_EQ(future.get().code(), Code::kUnavailable);
    EXPECT_FALSE(test.commit->committed());
    EXPECT_EQ(test.backend.catalog->begin_calls, 0U);
}

TEST(ChunkRootCommitAsync, KnownCommitFailureAbortsTheSameCandidateAndReleasesAllPreparedWindows)
{
    TestRootAsync test;
    test.backend.catalog->fail_commit = true;
    std::promise<Status> result;
    auto                 future  = result.get_future();
    auto                 running = stdexec::connect(test.operation->sender(), TestRootReceiver{&result});
    stdexec::start(running);
    for (auto &receipt : test.receipts) {
        EXPECT_EQ(receipt.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    }
    test.resume(0);
    test.resume(1);
    EXPECT_EQ(future.get().code(), Code::kUnavailable);
    EXPECT_TRUE(test.commit->done());
    EXPECT_FALSE(test.commit->committed());
    EXPECT_FALSE(test.commit->needs_recovery());
    EXPECT_EQ(test.backend.catalog->begin_calls, 1U);
    EXPECT_EQ(test.backend.catalog->abort_calls, 1U);
    EXPECT_EQ(test.backend.catalog->release_calls, 2U);
    const auto &candidate = test.backend.catalog->candidate;
    if (!candidate) {
        FAIL() << "candidate disappeared before its release was verified";
    }
    EXPECT_EQ(candidate->id, test.commit->candidate());
    EXPECT_EQ(candidate->released_windows, 2U);
}
} // namespace
} // namespace crowdb::tree
