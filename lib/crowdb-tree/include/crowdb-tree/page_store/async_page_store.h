// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// AsyncPageStore: the page store completion interface for read/write/fsync.
// Submission returns without waiting; completion may be inline or delivered
// later by the page store's I/O path.
//
// AsyncPageDiskStore provides page disk store I/O through a caller-owned
// DiskIOUring, mapping global byte offsets to per-extent fds. A page mem store
// can complete bounded memory operations inline without a dedicated async type.
#pragma once

#include "crowdb-tree/btree/work_cause.h"
#include "crowdb-tree/maptable/page_types.h" // PageAddr
#include "crowdb-tree/status.h"

#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#endif

#include "crowdb-tree/memory_budget.h"
#include "crowdb-tree/page_store/page_purpose.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

namespace crowdb::tree
{

using AsyncCompleteFn = void (*)(void *, Status);

struct AsyncCompletion
{
    void           *context     = nullptr;
    AsyncCompleteFn complete_fn = nullptr;
    // Optional context/buffer lifetime, copied with the callback through a
    // transport. This also keeps uncertain submissions safe until drain.
    std::shared_ptr<const void>       owner       = nullptr;
    std::shared_ptr<AllocationBudget> allocations = nullptr;
    TreeWorkCause                     cause       = TreeWorkCause::Other;

    void complete(Status status) const
    {
        if (complete_fn != nullptr) {
            complete_fn(context, std::move(status));
        }
    }

    [[nodiscard]] explicit operator bool() const
    {
        return complete_fn != nullptr;
    }
};

class AsyncPageStore
{
  public:
    virtual ~AsyncPageStore() = default;

    // A successful write can be read by its supplied address without a later
    // checkpoint/seal. Address reclamation additionally requires an extent
    // protection owner supplied by the allocator. Staging-only stores keep
    // the default until their pack-reference read path is available.
    [[nodiscard]] virtual bool write_completion_is_reloadable() const noexcept
    {
        return false;
    }

    // Submit an async read/write of `len` bytes at durable offset `addr`
    // (the same PageAddr/byte-offset domain as PageStore::read_at/write_at).
    // `on_complete` fires exactly once with the outcome. Immediate results
    // and rejected submissions may complete inline; accepted I/O completes
    // from the page store's worker. Returns an opaque nonzero operation id when the
    // page store supports per-operation cancellation; 0 means no cancellable
    // operation was accepted.
    virtual uint64_t submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete)        = 0;
    virtual uint64_t submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete) = 0;

    virtual uint64_t submit_typed_write(PagePurpose /*purpose*/, PageAddr addr, const void *buf, size_t len,
                                        AsyncCompletion on_complete)
    {
        return submit_write(addr, buf, len, on_complete);
    }

    // Durability barrier, submitted async. Returns the *submission* status
    // (e.g. invalid_argument if the store has no page block fd); the barrier's
    // own completion status arrives via `on_complete`, same as read/write.
    virtual Status submit_fsync(AsyncCompletion on_complete) = 0;

    // Best-effort cancellation. Page stores without per-operation cancellation
    // leave this as a no-op and may use transport-level cancellation instead.
    virtual void cancel(uint64_t op_id) = 0;
};

// PageDiskStore's async twin: delegates all I/O to DiskIOUring using
// PageDiskStore::fd_for_offset() to map a global byte offset to the
// underlying per-extent fd + local offset. submit_write mirrors submit_read;
// submit_fsync chains uring fsync across all dirty extent fds.
class PageDiskStore;

#ifdef CROWDB_HAVE_LIBURING
class AsyncPageDiskStore : public AsyncPageStore
{
  public:
    AsyncPageDiskStore(const AsyncPageDiskStore &)            = delete;
    AsyncPageDiskStore &operator=(const AsyncPageDiskStore &) = delete;

    // `store` and `uring` are both non-owning; caller must keep them alive
    // for at least as long as this object.
    AsyncPageDiskStore(PageDiskStore *store, ::crowdb::common::DiskIOUring *uring);

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    uint64_t submit_read(PageAddr addr, void *buf, size_t len, AsyncCompletion on_complete) override;
    uint64_t submit_write(PageAddr addr, const void *buf, size_t len, AsyncCompletion on_complete) override;
    Status   submit_fsync(AsyncCompletion on_complete) override;
    void     cancel(uint64_t op_id) override;
    // Startup holds the exclusive page store domain through completion. The
    // output is borrowed and is assigned only when every extent was probed.
    Status submit_size(uint64_t *size, AsyncCompletion on_complete);
    // Caller holds an exclusive free-range lease through completion.
    void submit_delete_block(uint32_t block_idx, AsyncCompletion on_complete);

  private:
    PageDiskStore                 *store_;
    ::crowdb::common::DiskIOUring *uring_;
};
#endif // CROWDB_HAVE_LIBURING

} // namespace crowdb::tree
