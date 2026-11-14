// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "allocation_reservation.h"

namespace crowdb::tree
{
// Fixed control storage, prepaid independently of the payload partition. Two
// slots are reserved for the current gap so distant replay cannot occupy them.
class ReplayControls
{
    struct Slot;

  public:
    class Lease
    {
      public:
        Lease() = default;
        ~Lease();
        Lease(Lease &&other) noexcept;
        Lease &operator=(Lease &&other) noexcept;
        Lease(const Lease &)                                                                         = delete;
        Lease                                                              &operator=(const Lease &) = delete;
        [[nodiscard]] const std::shared_ptr<detail::AllocationReservation> &budget() const noexcept;
        void                                                                clear_replay_slot() noexcept;

        [[nodiscard]] explicit operator bool() const noexcept
        {
            return static_cast<bool>(slot_);
        }

      private:
        friend class ReplayControls;

        explicit Lease(std::shared_ptr<Slot> slot) : slot_(std::move(slot))
        {
        }

        std::shared_ptr<Slot> slot_;
    };

    static Status       create(const std::shared_ptr<detail::ResourceBudget::Account> &account, size_t ordinary_slots,
                               size_t control_bytes, std::shared_ptr<ReplayControls> *out);
    [[nodiscard]] Lease acquire(uint64_t replay_slot, bool current_gap) noexcept;
    void                stop_admission() noexcept;
    [[nodiscard]] bool  can_wait_for(uint64_t missing_slot) const noexcept;

  private:
    explicit ReplayControls(std::shared_ptr<detail::AllocationReservation> metadata);
    std::shared_ptr<detail::AllocationReservation> metadata_;
    detail::ReservedVector<std::shared_ptr<Slot>>  slots_;
    std::atomic<bool>                              admission_drained_{false};
};
} // namespace crowdb::tree
