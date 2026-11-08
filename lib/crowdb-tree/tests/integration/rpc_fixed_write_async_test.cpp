// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "common/test_chunk_route_service.h"
#include "diskio_generated.h"
#include "load_budget_test_support.h"
#include "page_store/chunk/rpc_chunk_transport.h"

#include <future>
#include <semaphore>

namespace crowdb::tree::detail
{
namespace
{
struct TestFixedWriteService
{
    struct Reply
    {
        uint64_t id;
        void    *connection;
        void    *frame;
        Reply   *next;
    };

    crowdb_rpc_server_t           server = nullptr;
    std::atomic<Reply *>          pending{nullptr};
    std::counting_semaphore<1024> entered{0};
    std::atomic<uint64_t>         created_ms{0};
    bool                          sync_ack = true;

    ~TestFixedWriteService()
    {
        finish();
    }

    static void handle(uint64_t id, uint64_t /*stream*/, uint16_t /*type*/, const uint8_t *control, uint32_t bytes,
                       const uint8_t * /*data*/, uint32_t /*data_bytes*/, void *connection, void *frame, void *context)
    {
        auto                 &self = *static_cast<TestFixedWriteService *>(context);
        flatbuffers::Verifier verifier(control, bytes);
        EXPECT_TRUE(verifier.VerifyBuffer<crowdb::diskio::proto::FBDiskWriteRequest>(nullptr));
        const auto *request = flatbuffers::GetRoot<crowdb::diskio::proto::FBDiskWriteRequest>(control);
        EXPECT_TRUE(request->sync());
        self.created_ms.store(request->write_create_time_ms());
        auto *reply = new Reply{.id = id, .connection = connection, .frame = frame, .next = nullptr};
        auto *head  = self.pending.load(std::memory_order_relaxed);
        do {
            reply->next = head;
        } while (
            !self.pending.compare_exchange_weak(head, reply, std::memory_order_release, std::memory_order_relaxed));
        self.entered.release();
    }

    void finish()
    {
        auto *reply = pending.exchange(nullptr, std::memory_order_acquire);
        while (reply != nullptr) {
            auto                          *next = reply->next;
            flatbuffers::FlatBufferBuilder builder;
            builder.Finish(crowdb::diskio::proto::CreateFBDiskWriteResponse(
                builder, reply->id, 0, crowdb::diskio::proto::FBDiskIoRetCode_Success, sync_ack));
            static_cast<void>(crowdb_rpc_server_submit_response(
                server, reply->connection, builder.GetBufferPointer(), builder.GetSize(), nullptr, 0,
                crowdb::rpc::proto::FBMsgType_EDiskWriteResponse, reply->id));
            crowdb_rpc_frame_release(reply->frame);
            delete reply;
            reply = next;
        }
    }
};

struct TestWriteResult
{
    std::promise<Status> done;
    std::atomic<size_t>  calls{0};

    static void completed(void *context, Status status)
    {
        auto &result = *static_cast<TestWriteResult *>(context);
        EXPECT_EQ(result.calls.fetch_add(1), 0U);
        result.done.set_value(std::move(status));
    }
};

class RpcFixedMirrorWrite : public testing::Test
{
  protected:
    TestChunkRouteFixture              fixture;
    TestFixedWriteService              service;
    std::unique_ptr<RpcChunkTransport> transport;
    ChunkId                            id{0x0200'0000'0000'0042ULL, 144};

    void SetUp() override
    {
        service.server = fixture.services[0].server;
        crowdb_rpc_server_register_handler(service.server, crowdb::rpc::proto::FBMsgType_EDiskWriteRequest,
                                           &TestFixedWriteService::handle, &service, nullptr);
        const ct_chunk_rpc_disk_route  disk{.disk_id_high = 9, .disk_id_low = 10, .route = fixture.routes[0]};
        ct_chunk_rpc_transport_options options{};
        options.disk_routes      = &disk;
        options.disk_route_count = 1;
        options.rpc_timeout_ms   = 5000;
        options.chunkdb_resolver = fixture.resolver();
        transport                = std::make_unique<RpcChunkTransport>(options);
        ASSERT_TRUE(await_chunk_rpc<&RpcChunkTransport::submit_allocate_fixed_chunk>(*transport, id, 4096, 17,
                                                                                     PagePurpose::kBtreePage)
                        .ok());
    }
};

TEST_F(RpcFixedMirrorWrite, WaitingRepliesDoNotPreventOtherWritesAndRetainActualOwners)
{
    test_read::LoadCredits             credits;
    std::shared_ptr<AllocationAccount> allocations;
    ASSERT_TRUE(AllocationAccount::create_ongoing(credits.account, BudgetClass::kRouting, &allocations).ok());
    const auto                baseline = credits.account->snapshot().allocated[test_read::LoadCredits::kBytes];
    auto                      payload  = std::make_shared<std::vector<uint8_t>>(4096, 71);
    std::weak_ptr<const void> retained = payload;
    const auto                context  = ChunkIoContext::capture();
    TestWriteResult           first;
    TestWriteResult           second;
    auto                      a = first.done.get_future();
    auto                      b = second.done.get_future();
    for (auto *result : {&first, &second}) {
        transport->submit_write_fixed_mirror(id, 0, 0, payload->data(), payload->size(), context,
                                             {.complete_fn = &TestWriteResult::completed,
                                              .context     = result,
                                              .owner       = payload,
                                              .allocations = allocations});
    }
    payload.reset();
    const bool started_a = service.entered.try_acquire_for(std::chrono::seconds(3));
    const bool started_b = service.entered.try_acquire_for(std::chrono::seconds(3));
    EXPECT_TRUE(started_a);
    EXPECT_TRUE(started_b);
    EXPECT_EQ(a.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    EXPECT_EQ(b.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    EXPECT_FALSE(retained.expired());
    EXPECT_GT(credits.account->snapshot().allocated[test_read::LoadCredits::kBytes], baseline);
    EXPECT_EQ(service.created_ms.load(), context.created_ms);
    service.finish();
    ASSERT_EQ(a.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    ASSERT_EQ(b.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_TRUE(a.get().ok());
    EXPECT_TRUE(b.get().ok());
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while ((!retained.expired() || credits.account->snapshot().allocated[test_read::LoadCredits::kBytes] != baseline) &&
           std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    EXPECT_TRUE(retained.expired());
    EXPECT_EQ(credits.account->snapshot().allocated[test_read::LoadCredits::kBytes], baseline);
    EXPECT_EQ(first.calls.load(), 1U);
    EXPECT_EQ(second.calls.load(), 1U);
}

TEST_F(RpcFixedMirrorWrite, SuccessWithoutTheRequestedSyncAcknowledgementFails)
{
    service.sync_ack = false;
    const std::array<uint8_t, 4> payload{1, 2, 3, 4};
    TestWriteResult              result;
    auto                         done = result.done.get_future();
    transport->submit_write_fixed_mirror(id, 0, 0, payload.data(), payload.size(), ChunkIoContext::capture(),
                                         {.complete_fn = &TestWriteResult::completed, .context = &result});
    EXPECT_TRUE(service.entered.try_acquire_for(std::chrono::seconds(3)));
    service.finish();
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(done.get().code(), Code::kNotSupported);
    EXPECT_EQ(result.calls.load(), 1U);
}

TEST_F(RpcFixedMirrorWrite, QueryRefreshesTheSameChunkThroughTheExistingAsyncTimer)
{
    fixture.stale = true;
    ChunkLayout     layout;
    TestWriteResult result;
    auto            done = result.done.get_future();
    const ChunkId   requested(id.high, 145);
    transport->submit_query_chunk(requested, &layout, {.complete_fn = &TestWriteResult::completed, .context = &result});
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    const auto status = done.get();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(layout.chunk_id, requested);
    EXPECT_EQ(fixture.services[0].queries.load(), 1U);
    EXPECT_EQ(fixture.services[1].queries.load(), 1U);
    EXPECT_EQ(fixture.refreshes.load(), 1U);
    EXPECT_EQ(result.calls.load(), 1U);
}

TEST_F(RpcFixedMirrorWrite, UnknownAsyncAllocationNeverSubmitsAnotherIdentityOrRetriesTheMutation)
{
    fixture.services[1].drop_allocation = true;
    crowdb_rpc_client_start_reaper(fixture.client, 50'000'000, 10'000'000);
    const ChunkId   requested(id.high, 145);
    TestWriteResult result;
    auto            done = result.done.get_future();
    transport->submit_allocate_fixed_chunk(requested, 4096, 17, PagePurpose::kBtreePage,
                                           {.complete_fn = &TestWriteResult::completed, .context = &result});
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(done.get().code(), Code::kUnavailable);
    EXPECT_EQ(fixture.services[0].allocations.load(), 1U);
    EXPECT_EQ(fixture.services[1].allocations.load(), 1U);
    EXPECT_EQ(fixture.services[1].requested_low.load(), requested.low);
    EXPECT_EQ(result.calls.load(), 1U);
}

TEST_F(RpcFixedMirrorWrite, DrainSubmitsEveryMirrorBeforeAnyReplyIsReady)
{
    fixture.services[0].mirror_copies = 2;
    TestWriteResult result;
    auto            done = result.done.get_future();
    transport->submit_drain_fixed_chunk(id, {.complete_fn = &TestWriteResult::completed, .context = &result});
    EXPECT_TRUE(service.entered.try_acquire_for(std::chrono::seconds(3)));
    EXPECT_TRUE(service.entered.try_acquire_for(std::chrono::seconds(3)));
    EXPECT_EQ(done.wait_for(std::chrono::milliseconds(0)), std::future_status::timeout);
    service.finish();
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    const auto status = done.get();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_EQ(result.calls.load(), 1U);
}

TEST_F(RpcFixedMirrorWrite, AClosedConnectionRejectsWithoutLeakingItsCallbackOwner)
{
    test_read::LoadCredits credits;
    const auto             baseline = credits.account->snapshot().allocated[test_read::LoadCredits::kBytes];
    crowdb_rpc_conn_close(static_cast<crowdb_rpc_conn_t>(fixture.routes[0].connection));
    const std::array<uint8_t, 4> payload{1, 2, 3, 4};
    TestWriteResult              result;
    auto                         done = result.done.get_future();
    transport->submit_write_fixed_mirror(
        id, 0, 0, payload.data(), payload.size(), ChunkIoContext::capture(),
        {.complete_fn = &TestWriteResult::completed, .context = &result, .allocations = credits.credits.bytes});
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_FALSE(done.get().ok());
    EXPECT_EQ(result.calls.load(), 1U);
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
    while (credits.account->snapshot().allocated[test_read::LoadCredits::kBytes] != baseline &&
           std::chrono::steady_clock::now() < until) {
        std::this_thread::yield();
    }
    EXPECT_EQ(credits.account->snapshot().allocated[test_read::LoadCredits::kBytes], baseline);
}
} // namespace
} // namespace crowdb::tree::detail
