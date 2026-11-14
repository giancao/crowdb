// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "btree/allocation_reservation.h"
#include "chunk_protection_callback.h"

namespace crowdb::tree::detail
{
struct ProtectionCallback
{
    std::shared_ptr<const RootCatalog>        catalog;
    std::shared_ptr<ProtectionCallback>       callback_owner;
    AsyncCompletion                           completion;
    ChunkStageBytes                           manifest;
    ReservedVector<ct_chunk_stage_id>         references;
    ReservedVector<ct_chunk_candidate_status> wire;
    ct_chunk_pin_progress                     wire_progress{};
    ct_chunk_stage_id                         cursor{};
    size_t                                    length = 0;
    ChunkStageId                              id;
    std::optional<ChunkStageId>               after;
    uint32_t                                  limit      = 0;
    std::optional<ChunkCandidateStatus>      *candidate  = nullptr;
    std::vector<ChunkCandidateStatus>        *candidates = nullptr;
    ChunkPinProgress                         *progress   = nullptr;
    ct_status                                 reply      = 0;
    ct_status                                 submission = 0;
    std::atomic<unsigned>                     events{0};
    std::atomic<bool>                         reported{false};

    ProtectionCallback(std::shared_ptr<const RootCatalog> owner, AsyncCompletion done)
        : catalog(std::move(owner)),
          completion(std::move(done)),
          manifest(MetadataAllocator<uint8_t>(completion.allocations)),
          references(ReservationAllocator<ct_chunk_stage_id>(completion.allocations)),
          wire(ReservationAllocator<ct_chunk_candidate_status>(completion.allocations))
    {
    }

    static void ready(void *, ct_status) noexcept;
    void        submitted(ct_status) noexcept;
    Status      decode();
    void        publish() noexcept;
};

inline auto protection_call(std::shared_ptr<const RootCatalog> owner, AsyncCompletion done)
{
    auto call = reserved_shared<ProtectionCallback>(done.allocations, std::move(owner), done);
    return call;
}
} // namespace crowdb::tree::detail
