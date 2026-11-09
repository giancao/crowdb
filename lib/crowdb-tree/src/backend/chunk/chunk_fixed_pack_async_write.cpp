// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_fixed_pack_async_write.h"

#include "chunk_frame.h"
#include "crowdb-common/crc32c.h"
#include "crowdb-common/log.h"

#include <cassert>
#include <exec/sequence.hpp>

namespace crowdb::tree::detail
{
namespace
{
template <class State> auto fixed_pack_write_flow(State *state)
{
    auto mirrors = stdexec::when_all(CallbackSender(state, &State::template write_mirror<0>),
                                     CallbackSender(state, &State::template write_mirror<1>),
                                     CallbackSender(state, &State::template write_mirror<2>),
                                     CallbackSender(state, &State::template write_mirror<3>),
                                     CallbackSender(state, &State::template write_mirror<4>));
    return exec::sequence(CallbackSender(state, &State::prepare_registry), CallbackSender(state, &State::reserve),
                          CallbackSender(state, &State::allocate), CallbackSender(state, &State::fence),
                          CallbackSender(state, &State::mark_writing), std::move(mirrors),
                          CallbackSender(state, &State::advance), CallbackSender(state, &State::seal),
                          CallbackSender(state, &State::reloadable));
}

template <class State> struct PackExecution
{
    using Connected = stdexec::connect_result_t<decltype(fixed_pack_write_flow(static_cast<State *>(nullptr))),
                                                typename State::Receiver>;

    explicit PackExecution(State *owner)
        : connected(stdexec::connect(fixed_pack_write_flow(owner), typename State::Receiver{owner}))
    {
    }

    Connected connected;
};
} // namespace

ChunkFixedPackStore::AsyncWrite::AsyncWrite(ChunkFixedPackStore value, std::shared_ptr<AllocationBudget> account,
                                            AsyncCompletion done)
    : writer(std::move(value)),
      budget(std::move(account)),
      completion(std::move(done)),
      inputs(ReservationAllocator<Write>(budget))
{
    assert(reserved && writing && final_record);
    reserved->packs     = decltype(reserved->packs)(MetadataAllocator<ChunkStagePack>(budget));
    writing->packs      = decltype(writing->packs)(MetadataAllocator<ChunkStagePack>(budget));
    final_record->packs = decltype(final_record->packs)(MetadataAllocator<ChunkStagePack>(budget));
    for (uint32_t index = 0; index < mirrors.size(); ++index) {
        mirrors[index].owner = this;
        mirrors[index].index = index;
    }
}

Status ChunkFixedPackStore::AsyncWrite::prepare(ChunkStageId operation, ChunkId fixed, PagePurpose purpose,
                                                std::span<const Write> supplied)
{
    assert(reserved && writing && final_record);
    if (!writer.supported()) {
        return Status::not_supported("fixed staged chunk writes are unavailable");
    }
    if (!budget || supplied.empty() || supplied.size() > kMaxStagePacks || operation.owner_epoch != writer.epoch_ ||
        (writer.registry_ && physical_out == nullptr)) {
        return Status::invalid_argument();
    }
    size_t bytes = 0;
    for (const auto &item : supplied) {
        if (item.out == nullptr || item.input.empty() || item.input.size() > kMaxChunkPackBytes) {
            return Status::invalid_argument();
        }
        const auto framed_bytes = chunk_framed_length(item.input.size());
        if (framed_bytes > kMaxChunkPackBytes - bytes) {
            return Status::invalid_argument("aggregate exceeds Chunk pack geometry");
        }
        bytes += framed_bytes;
    }
    inputs.assign(supplied.begin(), supplied.end());
    framed = reserved_shared<ChunkPackWindow>(budget, budget);
    framed->bytes.resize(bytes);
    auto &reservation      = prepared_record(reserved);
    reservation.tree_id    = writer.tree_;
    reservation.id         = operation;
    reservation.revision   = 1;
    reservation.chunk_high = fixed.high;
    reservation.chunk_low  = fixed.low;
    reservation.capacity   = bytes;
    reservation.purpose    = static_cast<uint32_t>(purpose);
    reservation.state      = ChunkStageState::Reserved;
    auto status            = validate_stage_record(reservation);
    if (!status.ok()) {
        return status;
    }
    writing     = reserved;
    auto &write = prepared_record(writing);
    write.revision++;
    write.state   = ChunkStageState::Writing;
    size_t offset = 0;
    for (const auto &item : inputs) {
        const auto length = chunk_framed_length(item.input.size());
        status = encode_fixed_pack(fixed, purpose, item.input, std::span(framed->bytes).subspan(offset, length));
        if (!status.ok()) {
            return status;
        }
        write.packs.push_back({.offset   = offset,
                               .length   = static_cast<uint32_t>(item.input.size()),
                               .checksum = common::crc32c(item.input.data(), item.input.size())});
        offset += length;
    }
    final_record     = writing;
    auto &reloadable = prepared_record(final_record);
    reloadable.revision++;
    reloadable.state = ChunkStageState::Reloadable;
    return Status::Ok();
}

AsyncCompletion ChunkFixedPackStore::AsyncWrite::begin(CallbackComplete done, void *receiver)
{
    current_complete = done;
    current_context  = receiver;
    return {.context = this, .complete_fn = &AsyncWrite::completed, .allocations = budget};
}

ChunkTransportCompletion ChunkFixedPackStore::AsyncWrite::transport_completion()
{
    return {.complete_fn = &AsyncWrite::completed, .context = this, .allocations = budget};
}

void ChunkFixedPackStore::AsyncWrite::completed(void *raw, Status status)
{
    auto       &self     = *static_cast<AsyncWrite *>(raw);
    const auto  complete = self.current_complete;
    auto *const receiver = self.current_context;
    complete(receiver, status.ok() ? CallbackSignal::kValue : CallbackSignal::kError, std::move(status));
}

void ChunkFixedPackStore::AsyncWrite::finish(Status status) noexcept
{
    auto retained = std::move(accepted_owner);
    if (physical_out != nullptr) {
        *physical_out = physical;
    }
    if (status.ok()) {
        const auto &write = prepared_record(writing);
        for (size_t index = 0; index < inputs.size(); ++index) {
            const auto &pack   = write.packs[index];
            *inputs[index].out = {
                .stage    = write.id,
                .chunk    = {write.chunk_high, write.chunk_low},
                .offset   = pack.offset,
                .length   = pack.length,
                .checksum = pack.checksum,
                .purpose  = static_cast<PagePurpose>(write.purpose)
            };
        }
    }
    auto done = std::move(completion);
    try {
        done.complete(std::move(status));
    }
    catch (...) {
        CRB_LOG_ERROR("Fixed pack receiver threw after terminal");
    }
}

void ChunkFixedPackStore::submit_write_many(ChunkStageId operation, ChunkId fixed, PagePurpose purpose,
                                            std::span<const Write>                   inputs,
                                            const std::shared_ptr<AllocationBudget> &budget, AsyncCompletion completion,
                                            std::shared_ptr<ChunkPhysicalPack>      *physical,
                                            const std::shared_ptr<AllocationBudget> &physical_budget)
try {
    auto state             = reserved_shared<AsyncWrite>(budget, *this, budget, completion);
    state->physical_out    = physical;
    state->physical_budget = physical_budget ? physical_budget : budget;
    auto status            = state->prepare(operation, fixed, purpose, inputs);
    if (!status.ok()) {
        completion.complete(std::move(status));
        return;
    }
    auto execution        = reserved_shared<PackExecution<AsyncWrite>>(budget, state.get());
    state->execution      = execution;
    state->accepted_owner = std::move(state);
    stdexec::start(execution->connected);
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
