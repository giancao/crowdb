// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "snapshot/captured_frames.h"

namespace crowdb::tree
{
// Select from this exact immutable view. Dirty leaves are materialized first;
// an inner's missing child receipt is completed before that inner is selected.
Status select_writeback_page(const TreeRoot &view, const CapturedFrameLimits &limits, std::optional<uint64_t> *out);
Status writeback_inner_fences(const TreeRoot &view, const PageEntry &page, size_t max_children, InnerEncodeFences *out);
} // namespace crowdb::tree
