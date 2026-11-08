// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "cold_leaf_test_support.h"
#include "load_budget_test_support.h"

namespace crowdb::tree
{
using namespace test_read;

TEST(ColdLeafLoad, PreservesSortedCellsAndTombstones)
{
    TestColdLeaf                    fixture(true);
    auto                            loader = fixture.loader();
    std::shared_ptr<const LeafBase> result;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(leaf);
    });
    fixture.runtime->drain();
    ASSERT_NE(result, nullptr);
    EXPECT_EQ(result->view().self_page_id(), 17U);
    EXPECT_EQ(result->right_sibling(), 23U);
    EXPECT_LE(result->page_bytes(), fixture.frame.size());
    EXPECT_TRUE(frame_validate(result->frame(), result->page_bytes()));
    const LeafFrameView raw(fixture.frame.data(), fixture.frame.size());
    ASSERT_EQ(result->count(), raw.count());
    for (uint32_t index = 0; index < raw.count(); ++index) {
        EXPECT_EQ(result->key(index), raw.key(index));
        EXPECT_EQ(result->cell(index), raw.cell(index));
    }
    ASSERT_GE(result->find("zero"), 0);
    EXPECT_TRUE(CellView(result->cell(result->find("zero"))).is_tombstone());
    EXPECT_EQ(CellView(result->cell(result->find("zero"))).slot(), 0U);
    EXPECT_EQ(CellView(result->cell(result->find("a"))).value(), Slice("value"));
    EXPECT_EQ(CellView(result->cell(result->find("b"))).value(), Slice("base"));
    EXPECT_EQ(CellView(result->cell(result->find("new"))).value(), Slice("first"));
    EXPECT_EQ(CellView(result->cell(result->find("tie"))).value(), Slice("base-tie"));
}

TEST(ColdLeafLoad, SlotBoundRejectsOutOfRangeCells)
{
    for (const auto bound : {0U, 5U, 6U}) {
        TestColdLeaf fixture;
        fixture.reference.max_slot = bound;
        auto   loader              = fixture.loader();
        size_t calls               = 0;
        loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
            EXPECT_EQ(status.code(), Code::kCorruption);
            EXPECT_FALSE(leaf);
            ++calls;
        });
        fixture.runtime->drain();
        EXPECT_EQ(calls, 1U);
    }
}

TEST(ColdLeafLoad, RejectsMalformedCells)
{
    TestColdLeaf        fixture;
    const LeafFrameView view(fixture.frame.data(), fixture.frame.size());
    fixture.frame[static_cast<size_t>(view.cell(0).bytes() - fixture.frame.data()) + 8] = 0x80;
    frame_restamp_crc(fixture.frame.data(), fixture.frame.size());
    fixture.encode();
    auto   loader = fixture.loader();
    size_t calls  = 0;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kCorruption);
        EXPECT_FALSE(leaf);
        ++calls;
    });
    fixture.runtime->drain();
    EXPECT_EQ(calls, 1U);
}

TEST(ColdLeafLoad, EntryAndKeyWindowsCountTheEntireEncodedFrame)
{
    for (bool entries : {false, true}) {
        TestColdLeaf   fixture;
        PageLoadLimits limits;
        if (entries) {
            limits.max_leaf_entries = 5;
        }
        else {
            limits.max_leaf_key_bytes = 12;
        }
        auto   loader = fixture.loader(limits);
        size_t calls  = 0;
        loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
            EXPECT_EQ(status.code(), Code::kResourceExhausted);
            EXPECT_FALSE(leaf);
            ++calls;
        });
        fixture.runtime->drain();
        EXPECT_EQ(calls, 1U);
    }
}

TEST(ColdLeafLoad, LeafRetainsOnlyItsPageBlockProtectionAndActualFrameCharge)
{
    TestColdLeaf                     fixture(true);
    LoadCredits                      credits;
    auto                             loader     = fixture.loader({}, credits.credits);
    std::weak_ptr<const void>        protection = fixture.reference.page_block->owner;
    std::weak_ptr<TestColdLeafStore> store      = fixture.store;
    std::shared_ptr<const LeafBase>  result;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_TRUE(status.ok()) << status.to_string();
        result = std::move(leaf);
    });
    fixture.runtime->drain();
    ASSERT_NE(result, nullptr);
    loader.reset();
    fixture.reference = {};
    fixture.store.reset();
    credits.seal();
    EXPECT_TRUE(store.expired());
    EXPECT_FALSE(protection.expired());
    EXPECT_EQ(credits.budget.snapshot().reserved[LoadCredits::kBytes], 0U);
    EXPECT_GE(credits.budget.snapshot().allocated[LoadCredits::kBytes], result->page_bytes() + sizeof(LeafBase));
    const CellView overflow(result->cell(result->find("zzz")));
    EXPECT_EQ(overflow.overflow_head(), 41U);
    EXPECT_EQ(overflow.overflow_len(), 16384U);
    result.reset();
    EXPECT_TRUE(protection.expired());
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}

TEST(ColdLeafLoad, OwnerAllocationExhaustionReleasesIoCredit)
{
    TestColdLeaf fixture;
    fixture.store->deferred = true;
    LoadCredits credits;
    auto        loader = fixture.loader({}, credits.credits);
    size_t      calls  = 0;
    loader->load_leaf(fixture.reference, [&](Status status, auto leaf) {
        EXPECT_EQ(status.code(), Code::kResourceExhausted);
        EXPECT_FALSE(leaf);
        ++calls;
    });
    const auto held = credits.credits.reservation()->available();
    ASSERT_TRUE(credits.credits.reservation()->claim(held));
    fixture.store->pending.complete(Status::Ok());
    fixture.store->pending = {};
    fixture.runtime->drain();
    EXPECT_EQ(calls, 1U);
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kIo], 0U);
    loader.reset();
    credits.credits.reservation()->release(held);
    credits.seal();
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace crowdb::tree
