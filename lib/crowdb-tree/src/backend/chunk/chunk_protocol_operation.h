// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "backend/local/scheduled_page_store.h"
#include "btree/page_load.h"

namespace crowdb::tree
{
// A metadata protocol yields between CPU steps and suspends on accepted I/O.
// The protocol resolves cancellation/unknown results itself; the runner never
// drops an accepted mutation or destroys its context ahead of the callback.
class ChunkProtocolOperation
{
    struct State;

  public:
    using Step      = detail::InlineFunction<Status(bool)>;
    using Done      = detail::InlineFunction<bool()>;
    using AsyncStep = detail::InlineFunction<void(bool, AsyncCompletion)>;
    static Status create(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                         std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                         std::shared_ptr<const void> owner, Step step, Done done,
                         std::shared_ptr<ChunkProtocolOperation> *out);
    static Status create_async(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                               std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                               std::shared_ptr<const void> owner, AsyncStep step, Done done,
                               std::shared_ptr<ChunkProtocolOperation> *out);
    [[nodiscard]] static size_t          allocation_bound() noexcept;
    [[nodiscard]] detail::CallbackSender sender() noexcept;
    void                                 request_stop() noexcept;

  private:
    explicit ChunkProtocolOperation(std::shared_ptr<State> state) : state_(std::move(state))
    {
    }

    static Status create_impl(std::shared_ptr<AsyncPageStore> store, std::shared_ptr<detail::FlushRuntime> runtime,
                              std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadBudget budget,
                              std::shared_ptr<const void> owner, Step step, AsyncStep async_step, Done done,
                              std::shared_ptr<ChunkProtocolOperation> *out);
    static void   submit(void *, detail::CallbackComplete, void *);
    std::shared_ptr<State> state_;
};
} // namespace crowdb::tree
