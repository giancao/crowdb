// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include <atomic>
#include <cstddef>
#include <cstdint>

namespace crowdb::tree
{
// Publication ownership for accepted Tree work. Each request owns its memory;
// suspended children do not consume a bounded publication slot.
class PublicationLane
{
  private:
    friend class PublicationCoordinator;

    explicit PublicationLane(uint64_t publisher) : publisher_(publisher)
    {
    }

    const uint64_t publisher_;
};
} // namespace crowdb::tree
