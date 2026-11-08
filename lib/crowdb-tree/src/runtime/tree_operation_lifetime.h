// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/allocation_reservation.h"
#include "crowdb-tree/page_store/async_page_store.h"
#include "crowdb-tree/resource_usage.h"

namespace crowdb::tree
{
// Tracks accepted Tree operations through the last transport owner, including
// submissions whose acceptance is unknown. CPU drain alone cannot establish
// that a host-owned page store is safe to destroy.
class TreeOperationLifetime
{
    struct State;

  public:
    class Ticket : public std::enable_shared_from_this<Ticket>
    {
      public:
        ~Ticket();
        Ticket(const Ticket &)            = delete;
        Ticket &operator=(const Ticket &) = delete;

        // The page store remains host-owned. A loader/writeback holds this alias
        // through every callback ticket, so close_and_drain cannot finish while
        // the page store could still access its buffer or callback context.
        [[nodiscard]] std::shared_ptr<AsyncPageStore> page_store(AsyncPageStore *store);

      private:
        friend class TreeOperationLifetime;

        explicit Ticket(std::shared_ptr<State> state) : state_(std::move(state))
        {
        }

        std::shared_ptr<State> state_;
    };

    explicit TreeOperationLifetime(const std::shared_ptr<AllocationBudget> &metadata   = {},
                                   std::shared_ptr<const void>              host_owner = {});
    ~TreeOperationLifetime();
    TreeOperationLifetime(const TreeOperationLifetime &)            = delete;
    TreeOperationLifetime &operator=(const TreeOperationLifetime &) = delete;

    [[nodiscard]] Status acquire(const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<Ticket> *out);
    // Proposal and replay callers share actual-allocation admission. Pending
    // ticket count does not limit the ready CPU pool or suspended operations.
    [[nodiscard]] Status try_acquire(const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<Ticket> *out);
    [[nodiscard]] ResourceUsage resource_usage() const noexcept;
    [[nodiscard]] static size_t allocation_bound() noexcept;
    [[nodiscard]] static size_t ticket_allocation_bound() noexcept;
    [[nodiscard]] size_t        outstanding() const noexcept;
    [[nodiscard]] bool          closed() const noexcept;
    void                        close() noexcept;
    // Register once, including after close() fenced admission. Completion may
    // run inline or on the thread releasing the last accepted ticket.
    void close_async(AsyncCompletion complete) noexcept;
    // Host thread only. Accepted operations may continue submitting child I/O
    // through their existing Ticket after ingress closes. Do not call from a
    // callback holding a Ticket, or before releasing controller repair owners.
    void close_and_drain() noexcept;
    // Caller fences new admission separately. Keeps this lifetime reusable.
    void drain_accepted() noexcept;
    // Caller fences admission and serializes drain/close registrations.
    void drain_async(AsyncCompletion complete) noexcept;

  private:
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
