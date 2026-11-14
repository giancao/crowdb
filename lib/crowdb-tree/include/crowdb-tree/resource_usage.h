// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include <array>
#include <cstdint>

namespace crowdb::tree
{
// Six budget partitions, CPU/backend ready queue depth, then host CPU/backend
// workers. Tree samples omit the last two shared resources. Ready queue usage
// counts waiting jobs, with the configured target reported in limit.
struct ResourceUsage
{
    uint64_t limit       = 0;
    uint64_t used        = 0;
    uint64_t reserved    = 0;
    uint64_t allocated   = 0;
    uint64_t denied_tree = 0;
    uint64_t denied_host = 0;
    uint64_t waits       = 0;
    uint64_t wait_ns     = 0;
    uint64_t waiters     = 0;
};

using ResourceUsageSnapshot = std::array<ResourceUsage, 10>;
} // namespace crowdb::tree
