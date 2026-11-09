// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/chunk/chunk_pin_release.h"
#include "common/test_protection_callbacks.h"

#include <gtest/gtest.h>

namespace crowdb::tree::detail
{
namespace
{
using test_protection::TestProtection;

constexpr ChunkStageId stage{.owner_epoch = 1, .operation_high = 2, .operation_low = 3};
} // namespace

TEST(ChunkProtectionCallback, OpaqueRootAndFencedReaderKeepOneContextOwner)
{
    TestProtection context;
    {
        CallbackRootCatalog catalog(context.base(), TestProtection::stages(), TestProtection::protection(), &context);
        ASSERT_NE(catalog.protection(), nullptr);
        EXPECT_EQ(catalog.check_read_owner().code(), Code::kUnavailable);
        context.reader = 0;
        EXPECT_TRUE(catalog.check_read_owner().ok());
        std::vector<uint8_t> bytes;
        ASSERT_TRUE(catalog.load_root_bytes(42, 0, &bytes).ok());
        EXPECT_EQ(bytes, (std::vector<uint8_t>{1, 2, 3}));
        EXPECT_EQ(context.free_blobs, 1);
        EXPECT_TRUE(catalog.change_pin(42, stage, stage, true).ok());
        EXPECT_EQ(context.pins, 1);
        EXPECT_TRUE(catalog.writer_quiesced(42, 1).ok());
        EXPECT_EQ(context.pins, 1);
        EXPECT_TRUE(catalog.change_pin(42, stage, stage, false).ok());
        EXPECT_EQ(context.pins, 0);
    }
    EXPECT_EQ(context.drops, 1);
}

TEST(ChunkProtectionCallback, CandidateValidationRejectsCorruptOrUnboundedReplies)
{
    TestProtection      context;
    CallbackRootCatalog catalog(context.base(), TestProtection::stages(), TestProtection::protection(), &context);
    std::optional<ChunkCandidateStatus> candidate;
    ASSERT_TRUE(catalog.load_candidate(42, stage, &candidate).ok());
    ASSERT_TRUE(candidate);
    if (candidate.has_value()) {
        EXPECT_EQ(candidate->state, ChunkCandidateState::Ready);
    }
    context.corrupt = true;
    EXPECT_EQ(catalog.load_candidate(42, stage, &candidate).code(), Code::kCorruption);
    context.corrupt = false;
    std::vector<ChunkCandidateStatus> list;
    EXPECT_EQ(catalog.list_candidates(42, {}, 65, &list).code(), Code::kInvalidArgument);
    EXPECT_EQ(catalog.list_candidates(42, stage, 64, &list).code(), Code::kCorruption);
    EXPECT_TRUE(catalog.finish_candidate(42, stage, CT_CHUNK_CANDIDATE_RECOVER).ok());
    EXPECT_EQ(context.action, CT_CHUNK_CANDIDATE_RECOVER);
    EXPECT_EQ(catalog.finish_candidate(42, stage, 99).code(), Code::kInvalidArgument);
}

TEST(ChunkProtectionCallback, ReferenceWindowsAndPinCursorRemainFiniteAndOrdered)
{
    TestProtection      context;
    CallbackRootCatalog catalog(context.base(), TestProtection::stages(), TestProtection::protection(), &context);
    std::array<ChunkStageId, 2> duplicate{stage, stage};
    EXPECT_EQ(catalog.prepare_references(42, stage, 0, duplicate).code(), Code::kInvalidArgument);
    EXPECT_TRUE(catalog.prepare_references(42, stage, 0, {duplicate.data(), 1}).ok());
    ChunkPinProgress progress;
    EXPECT_TRUE(catalog.prune_pins(42, stage, {}, 64, &progress).ok());
    EXPECT_EQ(progress.released, 1);
    EXPECT_EQ(progress.after, stage);
    EXPECT_EQ(catalog.prune_pins(42, stage, stage, 64, &progress).code(), Code::kCorruption);
    const std::array<ChunkStageId, 2> ids{
        stage, ChunkStageId{.owner_epoch = 1, .operation_high = 2, .operation_low = 4}
    };
    EXPECT_EQ(chunk_reference_checksum(0, ids),
              chunk_reference_checksum(chunk_reference_checksum(0, {ids.data(), 1}), {ids.data() + 1, 1}));
}

namespace
{
class TestPinBudget final : public AllocationBudget
{
  public:
    bool claim(size_t bytes) noexcept override
    {
        if (deny) {
            return false;
        }
        used += bytes;
        return true;
    }

    void release(size_t bytes) noexcept override
    {
        used -= bytes;
    }

    size_t used = 0;
    bool   deny = false;
};

struct TestPinQueue
{
    std::shared_ptr<TestPinBudget>        budget = std::make_shared<TestPinBudget>();
    std::shared_ptr<size_t>               wakes  = std::make_shared<size_t>(0);
    std::shared_ptr<ChunkPinReleaseQueue> queue;

    TestPinQueue()
    {
        EXPECT_TRUE(ChunkPinReleaseQueue::create(
                        budget, 1, wakes, +[](void *context) noexcept { ++*static_cast<size_t *>(context); },
                        wakes.get(), &queue)
                        .ok());
    }
};
} // namespace

TEST(ChunkPinRelease, LastOwnerOnlyEnqueuesAndUnknownUnpinKeepsPaidIdentity)
{
    TestPinQueue   service;
    TestProtection context;
    auto           catalog  = std::make_shared<CallbackRootCatalog>(context.base(), TestProtection::stages(),
                                                                    TestProtection::protection(), &context);
    const auto     baseline = service.budget->used;
    std::shared_ptr<ChunkPinOwner> owner;
    ASSERT_TRUE(service.queue->prepare(service.budget, catalog, 42, stage, stage, &owner).ok());
    owner->arm();
    context.pins         = 1;
    service.budget->deny = true;
    owner.reset();
    EXPECT_EQ(context.pins, 1);
    EXPECT_GT(service.budget->used, baseline);
    EXPECT_EQ(service.queue->snapshot().pending, 1);
    context.pin_error = -8;
    auto first        = service.queue->pump(8);
    EXPECT_EQ(first.failed, 1);
    EXPECT_EQ(first.completed, 0);
    EXPECT_EQ(service.queue->snapshot().registered, 1);
    context.pin_error = 0;
    auto second       = service.queue->pump(8);
    EXPECT_EQ(second.completed, 1);
    EXPECT_EQ(context.pins, 0);
    EXPECT_EQ(service.budget->used, baseline);
    service.queue->stop_admission();
    EXPECT_TRUE(service.queue->drained());
}

TEST(ChunkPinRelease, HostStopPreservesLateReaderAndCatalogUntilRealRelease)
{
    TestPinQueue   service;
    TestProtection context;
    auto           catalog = std::make_shared<CallbackRootCatalog>(context.base(), TestProtection::stages(),
                                                                   TestProtection::protection(), &context);
    std::shared_ptr<ChunkPinOwner> retained;
    ASSERT_TRUE(service.queue->prepare(service.budget, catalog, 42, stage, stage, &retained).ok());
    retained->arm();
    context.pins = 1;
    ASSERT_TRUE(catalog->writer_quiesced(42, 1).ok());
    catalog.reset();
    service.queue->stop_admission();
    EXPECT_FALSE(service.queue->drained());
    EXPECT_EQ(context.drops, 0);
    EXPECT_EQ(service.queue->pump(8).completed, 0);
    retained.reset();
    EXPECT_EQ(context.drops, 0);
    EXPECT_FALSE(service.queue->drained());
    EXPECT_EQ(service.queue->pump(8).completed, 1);
    EXPECT_TRUE(service.queue->drained());
    EXPECT_EQ(context.drops, 1);
}

TEST(ChunkPinRelease, FixedCapacityAndUnarmedFailureReleaseWithoutMetadata)
{
    TestPinQueue   service;
    TestProtection context;
    auto           catalog  = std::make_shared<CallbackRootCatalog>(context.base(), TestProtection::stages(),
                                                                    TestProtection::protection(), &context);
    const auto     baseline = service.budget->used;
    const auto     empty    = service.queue->snapshot();
    EXPECT_EQ(empty.capacity, 1U);
    EXPECT_EQ(empty.registered, 0U);
    EXPECT_EQ(empty.pending, 0U);
    EXPECT_EQ(empty.denied, 0U);
    std::shared_ptr<ChunkPinOwner> first;
    std::shared_ptr<ChunkPinOwner> second;
    ASSERT_TRUE(service.queue->prepare(service.budget, catalog, 42, stage, stage, &first).ok());
    const auto paid = service.budget->used;
    for (size_t sample = 0; sample < 10; ++sample) {
        const auto held = service.queue->snapshot();
        EXPECT_EQ(held.registered, 1U);
        EXPECT_EQ(held.pending, 0U);
        EXPECT_EQ(held.denied, 0U);
    }
    EXPECT_EQ(service.budget->used, paid);
    EXPECT_EQ(service.queue->prepare(service.budget, catalog, 42, stage, stage, &second).code(),
              Code::kResourceExhausted);
    EXPECT_EQ(service.queue->snapshot().denied, 1U);
    service.queue->stop_admission();
    EXPECT_EQ(service.queue->prepare(service.budget, catalog, 42, stage, stage, &second).code(), Code::kUnavailable);
    first.reset();
    EXPECT_EQ(service.budget->used, baseline);
    EXPECT_EQ(context.pins, 0);
    EXPECT_TRUE(service.queue->drained());
    EXPECT_EQ(service.queue->snapshot().denied, 1U);
    EXPECT_EQ(service.queue->pump(8).completed, 0);
}

TEST(ChunkPinRelease, FailedIdentityCannotStarveAnUntouchedOlderRelease)
{
    auto                                  budget = std::make_shared<TestPinBudget>();
    auto                                  wakes  = std::make_shared<size_t>(0);
    std::shared_ptr<ChunkPinReleaseQueue> queue;
    ASSERT_TRUE(
        ChunkPinReleaseQueue::create(
            budget, 2, wakes, +[](void *context) noexcept { ++*static_cast<size_t *>(context); }, wakes.get(), &queue)
            .ok());
    TestProtection healthy;
    TestProtection unavailable;
    healthy.pins = unavailable.pins = 1;
    unavailable.pin_error           = -8;
    auto good_catalog               = std::make_shared<CallbackRootCatalog>(healthy.base(), TestProtection::stages(),
                                                                            TestProtection::protection(), &healthy);
    auto bad_catalog = std::make_shared<CallbackRootCatalog>(unavailable.base(), TestProtection::stages(),
                                                             TestProtection::protection(), &unavailable);
    std::shared_ptr<ChunkPinOwner> good;
    std::shared_ptr<ChunkPinOwner> bad;
    ASSERT_TRUE(queue->prepare(budget, good_catalog, 42, stage, stage, &good).ok());
    ASSERT_TRUE(queue->prepare(budget, bad_catalog, 42, stage, stage, &bad).ok());
    good->arm();
    bad->arm();
    good.reset();
    bad.reset();
    EXPECT_EQ(queue->pump(1).failed, 1);
    EXPECT_EQ(queue->pump(1).completed, 1);
    EXPECT_EQ(healthy.pins, 0);
    EXPECT_EQ(unavailable.pins, 1);
    unavailable.pin_error = 0;
    EXPECT_EQ(queue->pump(1).completed, 1);
    queue->stop_admission();
    EXPECT_TRUE(queue->drained());
}
} // namespace crowdb::tree::detail
