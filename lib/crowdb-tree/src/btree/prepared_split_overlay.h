// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/page_store/async_page_store.h"
#include "crowdb-tree/status.h"

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>

namespace crowdb::tree
{
class TreeCore;

struct SplitOverlayLimits
{
    size_t max_records = 0;
    size_t max_bytes   = 0;
};

struct InstalledSplitOverlay
{
    uint64_t generation = 0;
    uint64_t frontier   = 0;
};

// An independent bounded delta and the child's already prepared publication.
// Preparation is asynchronous; installation never waits for maintenance.
class PreparedSplitOverlay : public std::enable_shared_from_this<PreparedSplitOverlay>
{
    struct State;

  public:
    using Armed = std::function<void(Status, std::shared_ptr<PreparedSplitOverlay>, uint64_t)>;

    static Status arm(TreeCore &source, TreeCore &destination, SplitOverlayLimits limits,
                      std::shared_ptr<PreparedSplitOverlay> *out, uint64_t *start_frontier);
    static Status arm_async(TreeCore &source, TreeCore &destination, SplitOverlayLimits limits, Armed complete);
    Status        prepare(uint64_t replayed_prefix);
    Status        prepare_async(uint64_t replayed_prefix, AsyncCompletion complete) noexcept;
    // The consuming release holds an owner until it registers the callback,
    // including when preparation is still returning its result.
    void notify_released(void (*complete)(void *), void *context) noexcept;
    ~PreparedSplitOverlay();
    PreparedSplitOverlay(const PreparedSplitOverlay &)            = delete;
    PreparedSplitOverlay &operator=(const PreparedSplitOverlay &) = delete;

    Status                                try_install(uint64_t frontier, InstalledSplitOverlay *out) noexcept;
    [[nodiscard]] std::optional<uint64_t> committed_frontier() const noexcept;

  private:
    explicit PreparedSplitOverlay(std::shared_ptr<State> state);
    Status                 check_armed();
    void                   after_flush(Status status) noexcept;
    void                   finish_preparation(Status status) noexcept;
    std::shared_ptr<State> state_;
    void (*on_released_)(void *) = nullptr;
    void *release_context_       = nullptr;
};
} // namespace crowdb::tree
