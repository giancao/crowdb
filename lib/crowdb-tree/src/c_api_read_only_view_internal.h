// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/read_only_view_c_api.h"
#include "snapshot/published_snapshot.h"

struct ct_read_only_view
{
    explicit ct_read_only_view(std::shared_ptr<crowdb::tree::PublishedReadViewContext> value)
        : allocation(value->binding()->lifecycle_budget(), sizeof(ct_read_only_view),
                     crowdb::tree::AllocationKind::kMetadata),
          context(std::move(value))
    {
    }

    crowdb::tree::AllocationLease                           allocation;
    std::shared_ptr<crowdb::tree::PublishedReadViewContext> context;
};

struct ct_future_impl;
void c_api_complete_view_read(const std::shared_ptr<ct_future_impl> &value, const crowdb::tree::Status &status,
                              ct_read_only_view_read_fn complete, void *context);
