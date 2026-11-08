// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "tree_root.h"

namespace crowdb::tree
{
class CheckpointPendingMetrics;
Status validate_checkpoint(const PublicationTransaction &transaction, const TreeRoot &current,
                           const CheckpointPendingMetrics &pending);
} // namespace crowdb::tree
