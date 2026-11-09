// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/status.h"

namespace crowdb::tree
{
struct Config;

// Standalone entry points share one process-owned host. An explicitly injected
// host retains its own limits and shutdown authority.
// Asynchronous callers pass a flag to observe a competing cold initialization
// without waiting or constructing a second set of workers.
Status configure_default_runtime(Config *config, bool *initializing = nullptr);
} // namespace crowdb::tree
