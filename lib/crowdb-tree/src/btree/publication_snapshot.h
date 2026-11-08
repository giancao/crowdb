// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "tree_root.h"

namespace crowdb::tree
{
[[nodiscard]] bool   logical_page_change(const PageReplacement &page);
[[nodiscard]] Status validate_snapshot_publication(const PublicationTransaction &transaction, const TreeRoot &current);
void                 initialize_snapshot_changes(TreeRoot &state, const std::shared_ptr<AllocationBudget> &metadata);
void prepare_snapshot_changes(const PublicationTransaction &transaction, const TreeRoot &current, TreeRoot &next);
} // namespace crowdb::tree
