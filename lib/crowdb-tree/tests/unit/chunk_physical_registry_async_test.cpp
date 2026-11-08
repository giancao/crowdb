// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "common/test_chunk_physical.h"

#include <future>

namespace crowdb::tree::test_chunk
{
namespace
{
struct TestRegistryChain : detail::RetirementNode
{
    TestPhysicalHost                  &host;
    std::shared_ptr<ChunkPhysicalPack> pack;
    ChunkPhysicalAdmission             admission;
    std::promise<Status>               result;
    bool                               submission_returned = false;
    bool                               prepared            = false;
    bool                               observed            = false;

    explicit TestRegistryChain(TestPhysicalHost &value) : host(value)
    {
        dispose = +[](detail::RetirementNode *node) noexcept {
            auto &self = *static_cast<TestRegistryChain *>(node);
            self.host.registry->submit_prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk, self.host.budget,
                                               &self.pack, &self.admission,
                                               {.complete_fn = &TestRegistryChain::prepare_done, .context = &self});
            self.submission_returned = true;
            return true;
        };
    }

    static void prepare_done(void *context, Status status)
    {
        auto &self = *static_cast<TestRegistryChain *>(context);
        EXPECT_TRUE(self.submission_returned);
        self.prepared = true;
        if (!status.ok()) {
            self.result.set_value(std::move(status));
            return;
        }
        self.admission.accept();
        self.host.registry->submit_observe(self.pack, TestPhysicalHost::receipt, self.host.budget,
                                           {.complete_fn = &TestRegistryChain::observe_done, .context = &self});
    }

    static void observe_done(void *context, Status status)
    {
        auto &self    = *static_cast<TestRegistryChain *>(context);
        self.observed = true;
        EXPECT_EQ(self.host.registry->page_block_stats().allocated_bytes, 8192U);
        if (!status.ok()) {
            self.result.set_value(std::move(status));
            return;
        }
        self.host.registry->submit_deleted(
            TestPhysicalHost::stage, TestPhysicalHost::chunk, TestPhysicalHost::receipt, self.host.budget,
            {.complete_fn =
                 +[](void *context, Status status) {
                     auto &self = *static_cast<TestRegistryChain *>(context);
                     EXPECT_EQ(self.host.registry->page_block_stats().allocated_bytes, 0U);
                     self.result.set_value(std::move(status));
                 },
             .context = &self});
    }
};

TEST(ChunkPhysicalRegistryAsync, AcknowledgesReentrantCommandsWithoutWaitingOnItsOwnHostQueue)
{
    TestPhysicalHost  host;
    TestRegistryChain chain(host);
    auto              result = chain.result.get_future();
    host.queue->retire(&chain);
    const auto status = result.get();
    EXPECT_TRUE(status.ok()) << status.to_string();
    EXPECT_TRUE(chain.submission_returned);
    EXPECT_TRUE(chain.prepared);
    EXPECT_TRUE(chain.observed);
    host.lane->drain();
}

TEST(ChunkPhysicalRegistryAsync, FailedCommandAllocationCompletesWithoutPublishingAnAllocation)
{
    TestPhysicalHost host;
    host.budget->reject = true;
    std::shared_ptr<ChunkPhysicalPack> pack;
    size_t                             calls = 0;
    host.registry->submit_prepare(TestPhysicalHost::stage, TestPhysicalHost::chunk, host.budget, &pack, nullptr,
                                  {.complete_fn =
                                       +[](void *context, Status status) {
                                           ++*static_cast<size_t *>(context);
                                           EXPECT_EQ(status.code(), Code::kResourceExhausted);
                                       },
                                   .context = &calls});
    EXPECT_EQ(calls, 1U);
    EXPECT_FALSE(pack);
    EXPECT_EQ(host.registry->page_block_stats().allocated_bytes, 0U);
    host.budget->reject = false;
}
} // namespace
} // namespace crowdb::tree::test_chunk
