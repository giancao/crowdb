// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/published_read.h"
#include "crowdb-tree/btree/read_result.h"
#include "runtime/tree_operation_lifetime.h"
#include "runtime/tree_runtime_binding.h"

#include <functional>

namespace crowdb::tree
{
class TreeCore;
struct CoreScanOptions;
class ScanPackedBuf;

namespace detail
{
class HostCompletion;
}

// Retained only by a synchronous export window and its accepted I/O.
struct HostReadTail
{
    explicit HostReadTail(detail::HostCompletion &done) : done(done)
    {
    }

    ~HostReadTail();

    detail::HostCompletion         &done;
    std::shared_ptr<AsyncPageStore> backend;
};

// One accepted window. Idle cursor handles keep no I/O reserve or Tree
// ticket. Lower-level callback owners independently retain accepted I/O.
// Windows that borrow a Core must release the host guard off runtime workers.
// Detached windows can complete and release their resources on a worker.
struct SnapshotReadWindow
{
    // Host-only guard; released after the loader and ticket, so a last Core
    // release cannot synchronously close while waiting on this same ticket.
    std::shared_ptr<TreeCore>                      host_core;
    PageLoadBudget                                 budget;
    std::shared_ptr<TreeOperationLifetime::Ticket> ticket;
    std::shared_ptr<AsyncPageStore>                backend;
    std::shared_ptr<PageLoader>                    loader;

    SnapshotReadWindow() = default;
    ~SnapshotReadWindow();
    SnapshotReadWindow(const SnapshotReadWindow &)            = delete;
    SnapshotReadWindow &operator=(const SnapshotReadWindow &) = delete;
    SnapshotReadWindow(SnapshotReadWindow &&) noexcept        = default;
    SnapshotReadWindow &operator=(SnapshotReadWindow &&other) noexcept;
    void                seal() noexcept;
};

// A captured L1 header/mapping with no live L0 catalog. Shared backends can
// detach from the Core; raw backends admit each window through a weak Core.
class PublishedReadViewContext
{
    struct State;

  public:
    static Status capture(const std::shared_ptr<TreeCore> &core, std::shared_ptr<PublishedReadViewContext> *out);
    [[nodiscard]] static size_t allocation_bound() noexcept;
    [[nodiscard]] static size_t window_control_bound() noexcept;
    // bytes covers the caller's operation, loader and output capacities.
    // This adds reservation-control/ticket overhead and reserves one I/O.
    Status prepare_window(size_t bytes, SnapshotReadWindow *out) const;
    Status prepare_window(SnapshotReadWindow *out, const std::shared_ptr<HostReadTail> &tail = {}) const;
    Status get(Slice key, GetView *out) const;
    // For detached views. Once accepted, complete can run inline or after I/O.
    Status get_async(Slice key, std::function<void(Status, GetView)> complete) const;
    Status scan(CoreScanOptions options, ScanPackedBuf *out, size_t *count, bool *truncated) const;
    Status scan_async(CoreScanOptions options, std::function<void(Status, ScanPackedBuf, size_t, bool)> complete) const;
    // Starts one close on an independently owned view. The callback can run
    // inline. An accepted close retains its state after the view is released.
    Status             close_async(void (*complete)(void *), void *context) const noexcept;
    [[nodiscard]] bool closed() const noexcept;

    [[nodiscard]] const std::shared_ptr<const PublishedTreeState>   &view() const noexcept;
    [[nodiscard]] const std::shared_ptr<TreeRuntimeBinding>         &binding() const noexcept;
    [[nodiscard]] const std::shared_ptr<detail::FlushRuntime::Lane> &lane() const noexcept;
    [[nodiscard]] PageLoadLimits                                     load_limits() const noexcept;
    [[nodiscard]] PublishedReadLimits                                read_limits() const noexcept;
    [[nodiscard]] bool                                               detached() const noexcept;
    [[nodiscard]] const PageKeyRange                                &range() const noexcept;

  private:
    friend class TreeCore;
    static Status capture_admitted(const std::shared_ptr<TreeCore>           &core,
                                   std::shared_ptr<PublishedReadViewContext> *out,
                                   std::shared_ptr<const PublishedTreeState>  captured = {});

    explicit PublishedReadViewContext(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    Status finish_window(SnapshotReadWindow window, SnapshotReadWindow *out,
                         const std::shared_ptr<HostReadTail> &tail = {}) const;

    std::shared_ptr<State> state_;
};

// A long-lived traversal can retain this delegate across idle windows. Its
// active loader is installed only while the serialized next operation owns
// a SnapshotReadWindow. Completed leaf/value owners never retain it.
class SnapshotPageLoader final : public PageLoader
{
  public:
    Status begin(std::shared_ptr<PageLoader> loader);
    void   end() noexcept;
    void   load_leaf(BaseRef base, LeafComplete complete) override;
    void   load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete) override;
    void   load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                         ValueComplete complete) override;

  private:
    std::shared_ptr<PageLoader> active_;
};
} // namespace crowdb::tree
