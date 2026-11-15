// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/config.h"

namespace crowdb::tree
{
struct PageMaintenanceLimits;
Status configure_leaf_geometry(const Config &config, PageMaintenanceLimits *pages);
size_t leaf_record_capacity(uint32_t frame_bytes) noexcept;
// In-memory page frame size. Config::frame_bytes is the durable size of an
// uncompressed page and therefore includes the durable blob header.
size_t page_frame_bytes(const Config &config) noexcept;
size_t physical_frame_capacity(const Config &config) noexcept;
} // namespace crowdb::tree
