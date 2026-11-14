// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "chunk_fixed_pack.h"
#include "chunk_physical_registry.h"
#include "stdexec_adapter.h"

#include <cassert>

namespace crowdb::tree::detail
{
struct ChunkFixedPackStore::AsyncWrite
{
    struct Mirror
    {
        AsyncWrite      *owner    = nullptr;
        uint32_t         index    = 0;
        CallbackComplete complete = nullptr;
        void            *context  = nullptr;
        static void      submit(void *, CallbackComplete, void *);
        static void      written(void *, Status);
    };

    static void                           prepare_registry(void *, CallbackComplete, void *);
    static void                           reserve(void *, CallbackComplete, void *);
    static void                           allocate(void *, CallbackComplete, void *);
    static void                           allocated(void *, Status);
    static void                           inspected(void *, Status);
    static void                           observed(void *, Status);
    static void                           fence(void *, CallbackComplete, void *);
    static void                           mark_writing(void *, CallbackComplete, void *);
    static void                           advance(void *, CallbackComplete, void *);
    static void                           seal(void *, CallbackComplete, void *);
    static void                           reloadable(void *, CallbackComplete, void *);
    static void                           completed(void *, Status);
    template <uint32_t Index> static void write_mirror(void *raw, CallbackComplete done, void *context);

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        AsyncWrite *owner;

        void set_value() noexcept
        {
            owner->finish(Status::Ok());
        }

        void set_error(Status status) noexcept
        {
            owner->finish(std::move(status));
        }

        void set_error(std::exception_ptr /*error*/) noexcept
        {
            owner->finish(Status::internal_error());
        }

        void set_stopped() noexcept
        {
            owner->finish(Status::unavailable());
        }
    };

    AsyncWrite(ChunkFixedPackStore value, std::shared_ptr<AllocationBudget> account, AsyncCompletion done);
    Status prepare(ChunkStageId operation, ChunkId fixed, PagePurpose purpose, std::span<const Write> supplied);
    AsyncCompletion          begin(CallbackComplete done, void *receiver);
    ChunkTransportCompletion transport_completion();
    void                     finish(Status status) noexcept;

    // The records stay engaged for the attempt's lifetime. The optional
    // representation lets catalog CAS borrow them without copying their packs.
    static ChunkStageRecord &prepared_record(std::optional<ChunkStageRecord> &record) noexcept
    {
        assert(record.has_value());
        return *record; // NOLINT(bugprone-unchecked-optional-access)
    }

    ChunkFixedPackStore                  writer;
    std::shared_ptr<AllocationBudget>    budget;
    std::shared_ptr<AllocationBudget>    physical_budget;
    AsyncCompletion                      completion;
    std::shared_ptr<AsyncWrite>          accepted_owner;
    ReservedVector<Write>                inputs;
    std::shared_ptr<ChunkPackWindow>     framed;
    std::optional<ChunkStageRecord>      reserved{std::in_place};
    std::optional<ChunkStageRecord>      writing{std::in_place};
    std::optional<ChunkStageRecord>      final_record{std::in_place};
    std::shared_ptr<ChunkPhysicalPack>   physical;
    std::shared_ptr<ChunkPhysicalPack>  *physical_out = nullptr;
    ChunkPhysicalAdmission               admission;
    ChunkAllocationIdentity              receipt;
    ChunkIoContext                       context = ChunkIoContext::capture();
    Status                               allocation_status;
    CallbackComplete                     current_complete = nullptr;
    void                                *current_context  = nullptr;
    std::array<Mirror, kMaxMirrorCopies> mirrors;
    std::shared_ptr<void>                execution;
};

template <uint32_t Index>
void ChunkFixedPackStore::AsyncWrite::write_mirror(void *raw, CallbackComplete done, void *context)
{
    auto &self = *static_cast<AsyncWrite *>(raw);
    Mirror::submit(&self.mirrors[Index], done, context);
}
} // namespace crowdb::tree::detail
