// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_store/local/local_page_store_state.h"

#include "crowdb-tree/page_store/page_store.h"

namespace crowdb::tree
{
Status LocalPageStoreDomain::claim(PageStore *store, std::shared_ptr<const void> page_store_owner,
                                   const std::shared_ptr<detail::AllocationReservation> &metadata,
                                   std::shared_ptr<LocalPageStoreDomain>                *out)
try {
    if (store == nullptr || out == nullptr || *out) {
        return Status::invalid_argument("backend domain requires a store and empty output");
    }
    auto domain = detail::reserved_construct<LocalPageStoreDomain>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) LocalPageStoreDomain(std::move(page_store_owner));
    });
    if (!store->try_claim_runtime_domain()) {
        return Status::unavailable("local backend already has a runtime allocation authority");
    }
    domain->store_ = store;
    *out           = std::move(domain);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("backend domain allocation failed");
}

LocalPageStoreDomain::~LocalPageStoreDomain()
{
    if (store_ != nullptr) {
        store_->release_runtime_domain();
    }
}
} // namespace crowdb::tree
