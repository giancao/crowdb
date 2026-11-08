// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_account.h"
#include "btree/allocation_bound.h"
#include "common/test_chunk_root.h"
#include "snapshot/chunk_root_codec_internal.h"

namespace crowdb::tree
{
namespace
{
void complete(const std::shared_ptr<ChunkRootCommit> &operation)
{
    for (size_t i = 0; i < 100 && !operation->done(); ++i) {
        (void)operation->step();
    }
    ASSERT_TRUE(operation->done());
}
} // namespace

TEST(ChunkRootCommit, LostMutationRepliesResolveTheSameCandidateWithoutRepeatingReferenceWindows)
{
    test_chunk::TestRootPageStore test;
    auto                          pages     = test.pages(65);
    auto                          operation = test.commit(pages);
    test.catalog->lose_begin                = true;
    test.catalog->lose_prepare              = true;
    test.catalog->lose_commit               = true;
    complete(operation);
    ASSERT_TRUE(operation->status().ok()) << operation->status().to_string();
    EXPECT_TRUE(operation->committed());
    EXPECT_FALSE(operation->needs_recovery());
    EXPECT_EQ(test.catalog->begin_calls, 1U);
    EXPECT_EQ(test.catalog->prepare_calls, 2U);
    EXPECT_EQ(test.catalog->commit_calls, 1U);
    EXPECT_EQ(test.catalog->abort_calls, 0U);
    ChunkRootManifest root(test.credits.credits.reservation());
    ASSERT_TRUE(decode_chunk_root(test.catalog->manifest, 19, 7, &root).ok());
    EXPECT_EQ(root.frame_count, 65U);
    EXPECT_EQ(root.indexes.size(), 2U);
    EXPECT_EQ(root.identity.coverage, 15U);
    EXPECT_EQ(root.identity.wal_replay_offset, 4096U);
    EXPECT_EQ(root.stored_references, 0U);
    EXPECT_EQ(root.dependency_entries, 0U);
    detail::ChunkStageId pin;
    detail::ChunkId      unused;
    ASSERT_TRUE(test.ids->next(PagePurpose::kPageIndex, &pin, &unused).ok());
    std::shared_ptr<const ChunkPageBlock> index;
    ASSERT_TRUE(
        pin_chunk_page_block(test.domain, root.indexes[0].pack, pin, test.credits.credits.reservation(), &index).ok());
    std::vector<uint8_t> bytes(index->pack.length);
    ASSERT_TRUE(read_chunk_page_block(*index, bytes, test.credits.credits.reservation()).ok());
    uint64_t count  = 0;
    size_t   offset = 0;
    ASSERT_TRUE(decode_chunk_mapping_segment_header(bytes, root.identity, 0, &count, &offset).ok());
    EXPECT_EQ(count, 64U);
}

TEST(ChunkRootCommit, KnownFailedCommitAbortsAndReleasesEveryAcceptedWindow)
{
    test_chunk::TestRootPageStore test;
    auto                          operation = test.commit(test.pages(65));
    test.catalog->fail_commit               = true;
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kUnavailable);
    EXPECT_FALSE(operation->committed());
    EXPECT_FALSE(operation->needs_recovery());
    EXPECT_EQ(test.catalog->abort_calls, 1U);
    EXPECT_EQ(test.catalog->release_calls, 2U);
}

TEST(ChunkRootCommit, UnqueryableCommitKeepsCandidateProtectionAndDoesNotReportDurability)
{
    test_chunk::TestRootPageStore test;
    auto                          operation = test.commit(test.pages(1));
    test.catalog->lose_commit               = true;
    test.catalog->fail_query                = true;
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kUnavailable);
    EXPECT_FALSE(operation->committed());
    EXPECT_TRUE(operation->needs_recovery());
    if (!test.catalog->candidate) {
        FAIL() << "candidate protection disappeared";
    }
    EXPECT_EQ(test.catalog->candidate->state, detail::ChunkCandidateState::Committed);
    EXPECT_EQ(test.catalog->abort_calls, 0U);
    EXPECT_EQ(test.catalog->release_calls, 0U);
}

TEST(ChunkRootCommit, CancellationReleasesOnlyTheWindowsThatWereActuallyPrepared)
{
    test_chunk::TestRootPageStore test;
    auto                          operation = test.commit(test.pages(65));
    for (size_t i = 0; i < 30 && test.catalog->prepare_calls == 0; ++i) {
        ASSERT_TRUE(operation->step().ok());
    }
    ASSERT_EQ(test.catalog->prepare_calls, 1U);
    ASSERT_TRUE(operation->step(true).ok());
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kUnavailable);
    EXPECT_EQ(test.catalog->prepare_calls, 1U);
    EXPECT_EQ(test.catalog->commit_calls, 0U);
    EXPECT_EQ(test.catalog->release_calls, 1U);
}

TEST(ChunkRootCommit, AdmittedMetadataAndOneWindowFinishWithHostCreditExhausted)
{
    test_chunk::TestRootPageStore test;
    auto                          pages    = test.pages(65);
    const auto                    metadata = ChunkRootCommit::allocation_bound(65);
    const auto                    window   = ChunkRootCommit::window_allocation_bound(0);
    if (!metadata || !window) {
        FAIL() << "checkpoint bound overflow";
    }
    detail::ResourceBudget::Limits limits{};
    const auto                     category = static_cast<size_t>(detail::BudgetClass::kRouting);
    limits[category] = *metadata + *window + detail::shared_allocation_bound<detail::AllocationReservation>();
    detail::ResourceBudget budget(limits, 4);
    auto                   account = budget.make_account(limits);
    auto grant = detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, limits[category]);
    if (!grant) {
        FAIL() << "checkpoint reservation failed";
    }
    auto credit = detail::AllocationReservation::create(std::move(*grant));
    EXPECT_FALSE(detail::ResourceBudget::try_reserve(account, detail::BudgetClass::kRouting, 1));
    ChunkRootIdentity                identity{.tree              = 19,
                                              .root_generation   = 7,
                                              .owner_epoch       = 1,
                                              .snapshot_sequence = 4,
                                              .page_generation   = 3,
                                              .coverage          = 15,
                                              .gc_floor          = 8,
                                              .root_page_id      = 0,
                                              .next_page_id      = 65,
                                              .wal_replay_offset = 4096};
    std::shared_ptr<ChunkRootCommit> operation;
    ASSERT_TRUE(ChunkRootCommit::create(pages, test.domain, test.ids, identity, credit, credit, &operation).ok());
    complete(operation);
    ASSERT_TRUE(operation->status().ok()) << operation->status().to_string();
    EXPECT_TRUE(operation->committed());
}

TEST(ChunkRootCommit, DependencyTraversalContinuesPastTheFormerCumulativeWorkLimit)
{
    constexpr size_t                           leaves     = 257;
    constexpr size_t                           references = 4096;
    test_chunk::TestRootPageStore              test(512);
    auto                                       pages = test.pages(leaves + 1);
    std::shared_ptr<detail::AllocationAccount> metadata;
    ASSERT_TRUE(
        detail::AllocationAccount::create_ongoing(test.credits.account, detail::BudgetClass::kRouting, &metadata).ok());
    auto shared = detail::reserved_shared<StoredOverflowReferences>(
        metadata, detail::ReservationAllocator<StoredOverflowReference>(metadata));
    shared->assign(references, {.head = leaves, .bytes = 1});
    for (size_t pid = 0; pid < leaves; ++pid) {
        pages->frames.at(pid).stored_overflow = shared;
    }
    auto &overflow          = pages->frames.at(leaves);
    overflow.type           = page_type::kOverflowFrame;
    overflow.overflow_bytes = 1;
    overflow.stored_overflow.reset();
    auto   operation = test.commit(pages, metadata);
    size_t steps     = 0;
    while (!operation->done() && steps++ < 100000) {
        const auto status = operation->step();
        ASSERT_TRUE(status.ok()) << status.to_string();
    }
    ASSERT_TRUE(operation->done());
    ASSERT_TRUE(operation->status().ok()) << operation->status().to_string();
    ASSERT_TRUE(operation->committed());
    EXPECT_EQ(operation->manifest()->dependency_entries, leaves * references);
    EXPECT_GT(steps, 1024U);
}

TEST(ChunkRootCommit, OverflowCyclesFailByGraphIdentityWithoutExhaustingAWorkQuota)
{
    test_chunk::TestRootPageStore test;
    auto                          pages      = test.pages(3);
    auto                          references = detail::reserved_shared<StoredOverflowReferences>(
        test.credits.credits.bytes, detail::ReservationAllocator<StoredOverflowReference>(test.credits.credits.bytes));
    references->push_back({.head = 1, .bytes = UINT64_MAX});
    pages->frames.at(0).stored_overflow = references;
    for (uint64_t pid : {1, 2}) {
        auto &frame          = pages->frames.at(pid);
        frame.type           = page_type::kOverflowFrame;
        frame.overflow_bytes = 1;
        frame.next_overflow  = pid == 1 ? 2 : 1;
        frame.stored_overflow.reset();
    }
    auto operation = test.commit(pages);
    complete(operation);
    EXPECT_EQ(operation->status().code(), Code::kCorruption);
    EXPECT_FALSE(operation->committed());
    EXPECT_EQ(test.catalog->begin_calls, 0U);
}
} // namespace crowdb::tree
