// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/tree_core_read_internal.h"

namespace crowdb::tree::core_read_detail
{
struct CoreScan
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        CoreScan *owner;

        void set_value() const noexcept
        {
            owner->window();
        }

        void set_error(Status status) const noexcept
        {
            owner->finish(std::move(status), false);
        }

        void set_stopped() const noexcept
        {
            owner->finish(Status::unavailable("scan stopped"), false);
        }
    };

    using Operation = detail::CallbackSender::Operation<Receiver>;
    ReadPreparation                             prepared;
    std::shared_ptr<PublishedScan>              read;
    std::shared_ptr<Operation>                  operation;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    std::shared_ptr<CoreScan>                   owner;
    ScanComplete                                complete;
    ScanPackedBuf                               packed;
    size_t                                      count       = 0;
    size_t                                      bytes       = 0;
    size_t                                      limit       = 0;
    size_t                                      byte_limit  = 0;
    size_t                                      value_limit = 0;
    uint64_t                                    deadline_ms = 0;
    bool                                        strict      = false;

    void               finish(Status status, bool truncated) noexcept;
    void               schedule() noexcept;
    void               window() noexcept;
    static void        step(void *context) noexcept;
    [[nodiscard]] bool expired() const noexcept;
};

Status       prepare_scan(TreeCore &core, CoreScanOptions options, std::shared_ptr<CoreScan> *out);
Status       scan_limits(CoreScanOptions options, PublishedReadLimits *limits);
PageKeyRange scan_range(CoreScanOptions options, const std::shared_ptr<AllocationBudget> &credit);
Status       prepare_scan(CoreScanOptions options, ReadPreparation ready, PageKeyRange range, size_t value_limit,
                          std::shared_ptr<CoreScan> *out);
} // namespace crowdb::tree::core_read_detail
