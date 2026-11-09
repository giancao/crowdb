// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "load_budget_test_support.h"
#include "runtime/tree_operation_lifetime.h"

#include <future>
#include <thread>

namespace crowdb::tree
{
namespace
{
using test_read::LoadCredits;

TEST(TreeOperationLifetime, CloseRejectsIngressAndWaitsForEveryAcceptedOwner)
{
    LoadCredits                                    credits;
    TreeOperationLifetime                          lifetime(credits.credits.reservation());
    std::shared_ptr<TreeOperationLifetime::Ticket> first;
    std::shared_ptr<TreeOperationLifetime::Ticket> second;
    std::shared_ptr<TreeOperationLifetime::Ticket> rejected;
    ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &first).ok());
    ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &second).ok());
    lifetime.close();
    EXPECT_EQ(lifetime.acquire(credits.credits.reservation(), &rejected).code(), Code::kUnavailable);
    std::atomic<bool>  drained{false};
    std::promise<void> started;
    auto               ready = started.get_future();
    std::jthread       closer([&] {
        started.set_value();
        lifetime.close_and_drain();
        drained.store(true, std::memory_order_release);
    });
    ready.wait();
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    first.reset();
    EXPECT_EQ(lifetime.outstanding(), 1U);
    EXPECT_FALSE(drained.load(std::memory_order_acquire));
    second.reset();
    closer.join();
    EXPECT_TRUE(drained.load(std::memory_order_acquire));
    EXPECT_EQ(lifetime.outstanding(), 0U);
}

TEST(TreeOperationLifetime, ObjectAndControlBlockAllocationFailuresReturnCapacityExactlyOnce)
{
    LoadCredits           credits;
    TreeOperationLifetime lifetime;
    // Exercise both object allocation and the later shared control allocation.
    // The latter destroys the newly constructed Ticket during rollback.
    const auto bound    = TreeOperationLifetime::ticket_allocation_bound();
    size_t     rejected = 0;
    size_t     admitted = 0;
    for (size_t available = 0; available <= bound; ++available) {
        AllocationLease pressure(credits.credits.reservation(), credits.credits.reservation()->available() - available);
        const auto      baseline = credits.budget.snapshot().allocated[LoadCredits::kBytes];
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        const auto status = lifetime.acquire(credits.credits.reservation(), &ticket);
        if (status.ok()) {
            ++admitted;
            EXPECT_EQ(lifetime.outstanding(), 1U);
            ticket.reset();
        }
        else {
            ++rejected;
            EXPECT_EQ(status.code(), Code::kResourceExhausted);
            EXPECT_FALSE(ticket);
        }
        EXPECT_EQ(lifetime.outstanding(), 0U);
        EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], baseline);
    }
    EXPECT_GT(rejected, 0U);
    EXPECT_GT(admitted, 0U);
    lifetime.close_and_drain();
}

TEST(TreeOperationLifetime, CompletionCanRegisterAfterAdmissionWasFenced)
{
    LoadCredits credits;
    for (bool release_first : {false, true}) {
        std::atomic<unsigned>                          completed{0};
        TreeOperationLifetime                          lifetime(credits.credits.reservation());
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &ticket).ok());
        lifetime.close();
        if (release_first) {
            ticket.reset();
        }
        lifetime.close_async({.context = &completed, .complete_fn = [](void *context, Status status) noexcept {
                                  EXPECT_TRUE(status.ok());
                                  static_cast<std::atomic<unsigned> *>(context)->fetch_add(1,
                                                                                           std::memory_order_relaxed);
                              }});
        EXPECT_EQ(completed.load(), release_first ? 1U : 0U);
        EXPECT_EQ(lifetime.outstanding(), release_first ? 0U : 1U);
        std::shared_ptr<TreeOperationLifetime::Ticket> rejected;
        EXPECT_EQ(lifetime.acquire(credits.credits.reservation(), &rejected).code(), Code::kUnavailable);
        ticket.reset();
        lifetime.close();
        EXPECT_EQ(completed.load(), 1U);
        EXPECT_EQ(lifetime.outstanding(), 0U);
    }
}

TEST(TreeOperationLifetime, SuspendedOperationCountUsesActualMemoryRatherThanACpuWindow)
{
    LoadCredits                                                 credits;
    TreeOperationLifetime                                       lifetime(credits.credits.reservation());
    std::vector<std::shared_ptr<TreeOperationLifetime::Ticket>> tickets;
    for (size_t index = 0; index < 1024; ++index) {
        std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
        const auto status = index % 2 == 0 ? lifetime.acquire(credits.credits.reservation(), &ticket)
                                           : lifetime.try_acquire(credits.credits.reservation(), &ticket);
        ASSERT_TRUE(status.ok());
        tickets.push_back(std::move(ticket));
    }
    EXPECT_EQ(lifetime.resource_usage().limit, 0U);
    EXPECT_EQ(lifetime.outstanding(), tickets.size());
    EXPECT_EQ(lifetime.resource_usage().denied_tree, 0U);
    tickets.clear();
    EXPECT_EQ(lifetime.outstanding(), 0U);
    lifetime.close_and_drain();
}

TEST(TreeOperationLifetime, TicketRetainsChargedStateAndHostAfterIngressOwnerIsDropped)
{
    LoadCredits                                    credits;
    auto                                           host = std::make_shared<int>(17);
    const std::weak_ptr<int>                       weak = host;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    {
        TreeOperationLifetime lifetime(credits.credits.reservation(), host);
        ASSERT_TRUE(lifetime.acquire(credits.credits.reservation(), &ticket).ok());
        host.reset();
        EXPECT_FALSE(weak.expired());
    }
    credits.seal();
    EXPECT_FALSE(weak.expired());
    EXPECT_GT(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
    ticket.reset();
    EXPECT_TRUE(weak.expired());
    EXPECT_EQ(credits.budget.snapshot().allocated[LoadCredits::kBytes], 0U);
}
} // namespace
} // namespace crowdb::tree
