// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/resource_budget.h"

namespace crowdb::tree
{
class PageStore;
class PageDiskStore;
class AsyncPageStore;

namespace detail
{
struct PageStoreOperationLimits
{
    size_t max_io_bytes = 0;
    size_t write_calls  = 0;
    size_t workers      = 1;
    bool   barriers     = false;
    bool   asynchronous = false;
};

std::optional<size_t> backend_operation_bound(PageStore *backend, PageStoreOperationLimits limits) noexcept;
Status reserve_backend_operation(PageStore *backend, const std::shared_ptr<ResourceBudget::Account> &account,
                                 PageStoreOperationLimits limits, std::shared_ptr<AllocationBudget> *out);
std::optional<size_t> backend_operation_bound(AsyncPageStore *backend, PageStoreOperationLimits limits) noexcept;
Status reserve_backend_operation(AsyncPageStore *backend, const std::shared_ptr<ResourceBudget::Account> &account,
                                 PageStoreOperationLimits limits, std::shared_ptr<AllocationBudget> *out);
// Consume an already admitted complete operation bound. No second resource
// acquisition occurs between the asynchronous grant and page store submission.
Status   bind_backend_operation(PageStore *backend, PageStoreOperationLimits limits, ResourceBudget::Credit credit,
                                std::shared_ptr<AllocationBudget> *out);
Status   bind_backend_operation(AsyncPageStore *backend, PageStoreOperationLimits limits, ResourceBudget::Credit credit,
                                std::shared_ptr<AllocationBudget> *out);
uint64_t page_store_size(PageStore &store, const std::shared_ptr<AllocationBudget> &budget);

void seal_backend_budget(const std::shared_ptr<AllocationBudget> &budget) noexcept;

struct PageStoreOpenBudget
{
    size_t owned_bytes  = 0;
    size_t growth_bytes = 0;
};

std::shared_ptr<AllocationBudget> block_allocation_budget(const PageDiskStore                     &store,
                                                          const std::shared_ptr<AllocationBudget> &budget = {});

bool valid_block_budget(const PageDiskStore &store, const std::shared_ptr<AllocationBudget> &budget) noexcept;

// Requires the PageStore runtime-domain lease and no page store growth in flight.
// Existing fd owners may free concurrently. Non-Block stores own their external
// implementation accounting; this adapter only binds the local page disk store.
Status bind_block_account(PageStore *store, const std::shared_ptr<ResourceBudget::Account> &account,
                          size_t backend_workers, PageStoreOpenBudget *out);
Status ongoing_backend_operation(PageStore *store, std::shared_ptr<AllocationBudget> *out);
} // namespace detail
} // namespace crowdb::tree
