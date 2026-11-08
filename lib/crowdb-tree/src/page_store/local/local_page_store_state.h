// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/local_checkpoint.h"

namespace crowdb::tree
{
class PageStore;

// The claim outlives registry/Core owners when a detached snapshot still
// reads protected extents. The page store owner is released after the claim.
class LocalPageStoreDomain
{
  public:
    static Status claim(PageStore *store, std::shared_ptr<const void> page_store_owner,
                        const std::shared_ptr<detail::AllocationReservation> &metadata,
                        std::shared_ptr<LocalPageStoreDomain>                *out);
    ~LocalPageStoreDomain();
    LocalPageStoreDomain(const LocalPageStoreDomain &)            = delete;
    LocalPageStoreDomain &operator=(const LocalPageStoreDomain &) = delete;

  private:
    explicit LocalPageStoreDomain(std::shared_ptr<const void> owner) : page_store_owner_(std::move(owner))
    {
    }

    std::shared_ptr<const void> page_store_owner_;
    PageStore                  *store_ = nullptr;
};

// One allocation authority for a local Tree lifetime. The registry retains
// both anchors and an uncertain candidate; live page block and I/O owners retain
// their own extents independently. Legacy allocation must not use this domain.
struct LocalPageStoreState
{
    LocalPageStoreState()                                                           = default;
    ~LocalPageStoreState()                                                          = default;
    LocalPageStoreState(const LocalPageStoreState &)                                = delete;
    LocalPageStoreState                     &operator=(const LocalPageStoreState &) = delete;
    std::shared_ptr<AsyncPageStore>          store;
    std::shared_ptr<ExtentLedger>            ledger;
    std::shared_ptr<LocalCheckpointRegistry> checkpoints;
    // Startup-only recovered descriptors, consumed while sizing Core loaders.
    // Live primary page blocks retain their exact typed overflow dependencies.
    std::shared_ptr<const CapturedDurablePages> inherited;
    // Set only after claiming the PageStore runtime allocation domain.
    std::shared_ptr<LocalPageStoreDomain> domain;
    std::atomic<uint64_t>                 gc_cursor{0};
    std::atomic<bool>                     gc_active{false};
};
} // namespace crowdb::tree
