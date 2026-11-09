// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/inline_function.h"
#include "crowdb-tree/backend/async_page_store.h"
#include "crowdb-tree/backend/page_store.h"
#include "runtime/tree_runtime_binding.h"

#include <span>

namespace crowdb::tree
{
class ChunkWriteQueue;

// Dispatches backend work through the shared runtime. Suspended async I/O
// retains its buffer owner without holding a worker or an in-flight permit;
// operation controls are charged to the admitted work's actual allocations.
class ScheduledPageStore final : public AsyncPageStore, public std::enable_shared_from_this<ScheduledPageStore>
{
    struct State;
    struct Job;

  public:
    using MakeChunkWrites = std::shared_ptr<ChunkWriteQueue> (*)(const std::shared_ptr<AllocationBudget> &);

    static std::optional<size_t> allocation_bound() noexcept;
    static Status create(PageStore *sync, AsyncPageStore *async, std::shared_ptr<const void> backend_owner,
                         std::shared_ptr<TreeRuntimeBinding>                   binding,
                         const std::shared_ptr<detail::AllocationReservation> &metadata,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, std::shared_ptr<AsyncPageStore> *out,
                         std::shared_ptr<ApplyDispatcher> fallback = {}, MakeChunkWrites make_chunk_writes = nullptr);
    [[nodiscard]] bool       write_completion_is_reloadable() const noexcept override;
    [[nodiscard]] PageStore *synchronous_store() const noexcept;
    [[nodiscard]] bool       uses_async_transport() const noexcept;
    [[nodiscard]] size_t     allocation_workers() const noexcept;
    uint64_t submit_read(PageAddr address, void *buffer, size_t length, AsyncCompletion complete) override;
    uint64_t submit_write(PageAddr address, const void *buffer, size_t length, AsyncCompletion complete) override;
    uint64_t submit_typed_write(PagePurpose purpose, PageAddr address, const void *buffer, size_t length,
                                AsyncCompletion complete) override;
    Status   submit_fsync(AsyncCompletion complete) override;
    void     cancel(uint64_t operation) override;
    // A finite, paid backend capability step uses the same admission and drain
    // boundary as file I/O; it never executes on the submitting CPU worker.
    Status submit_step(detail::InlineFunction<Status()> step, AsyncCompletion complete, size_t read_bytes = 0,
                       size_t write_bytes = 0);
    Status submit_async_step(detail::InlineFunction<void(AsyncCompletion)> step, AsyncCompletion complete,
                             size_t read_bytes = 0, size_t write_bytes = 0);
    [[nodiscard]] ChunkWriteQueue *chunk_writes() const noexcept;

  private:
    explicit ScheduledPageStore(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    Status submit(PageAddr address, void *read, const void *write, size_t length, PagePurpose purpose,
                  AsyncCompletion complete, detail::InlineFunction<Status()> step = {}, size_t read_bytes = 0,
                  size_t write_bytes = 0, detail::InlineFunction<void(AsyncCompletion)> async_step = {});
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
