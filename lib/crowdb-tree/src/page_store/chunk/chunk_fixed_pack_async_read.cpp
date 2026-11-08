// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_fixed_pack.h"
#include "chunk_frame.h"
#include "page_store/local/scheduled_page_store.h"

namespace crowdb::tree::detail
{
struct ChunkFixedPackStore::AsyncRead : std::enable_shared_from_this<AsyncRead>
{
    std::shared_ptr<ChunkTransport>     transport;
    std::shared_ptr<ScheduledPageStore> cpu;
    std::shared_ptr<AllocationBudget>   budget;
    std::shared_ptr<ChunkPackWindow>    framed;
    ChunkFixedPackRef                   ref;
    std::span<uint8_t>                  output;
    AsyncCompletion                     completion;
    ChunkIoContext                      context   = ChunkIoContext::capture();
    uint32_t                            mirror    = 0;
    uint32_t                            mirrors   = 0;
    bool                                responded = false;

    void finish(Status status)
    {
        auto done = std::exchange(completion, {});
        done.complete(std::move(status));
    }

    static void decoded(void *raw, Status status)
    {
        auto &self = *static_cast<AsyncRead *>(raw);
        if (status.code() == Code::kCorruption) {
            self.next();
        }
        else {
            self.finish(std::move(status));
        }
    }

    static void received(void *raw, Status status)
    try {
        auto &self = *static_cast<AsyncRead *>(raw);
        if (!status.ok()) {
            self.next();
            return;
        }
        self.responded = true;
        if (!self.cpu) {
            decoded(raw, decode_fixed_pack(self.ref, self.framed->bytes, self.output));
            return;
        }
        auto result = self.cpu->submit_step(
            [owner = self.shared_from_this()] {
                return decode_fixed_pack(owner->ref, owner->framed->bytes, owner->output);
            },
            {.context     = raw,
             .complete_fn = &AsyncRead::decoded,
             .owner       = self.shared_from_this(),
             .allocations = self.budget});
        if (!result.ok()) {
            self.finish(std::move(result));
        }
    }
    catch (const std::bad_alloc &) {
        static_cast<AsyncRead *>(raw)->finish(Status::resource_exhausted());
    }
    catch (...) {
        static_cast<AsyncRead *>(raw)->finish(Status::internal_error());
    }

    void next() noexcept
    try {
        if (mirror == mirrors) {
            finish(responded ? Status::corruption("no mirror supplied the fixed pack")
                             : Status::unavailable("fixed pack is unavailable on every mirror"));
            return;
        }
        transport->submit_read_fixed_mirror(
            ref.chunk, mirror++, ref.offset, framed->bytes.data(), framed->bytes.size(), context,
            {.complete_fn = &AsyncRead::received, .context = this, .owner = shared_from_this(), .allocations = budget});
    }
    catch (const std::bad_alloc &) {
        finish(Status::resource_exhausted());
    }
    catch (...) {
        finish(Status::internal_error());
    }
};

void ChunkFixedPackStore::submit_read_into(const ChunkFixedPackRef                 &ref,
                                           const std::shared_ptr<AllocationBudget> &budget, std::span<uint8_t> output,
                                           AsyncCompletion completion, std::shared_ptr<ScheduledPageStore> cpu) const
try {
    if (!transport_ || !budget || output.size() != ref.length || ref.length == 0 || ref.length > kMaxChunkPackBytes ||
        ref.offset > UINT64_MAX - chunk_framed_length(ref.length)) {
        completion.complete(Status::invalid_argument());
        return;
    }
    auto state       = reserved_shared<AsyncRead>(budget);
    state->transport = transport_;
    state->cpu       = std::move(cpu);
    state->budget    = budget;
    state->ref       = ref;
    state->output    = output;
    state->mirrors   = mirrors_;
    state->framed    = reserved_shared<ChunkPackWindow>(budget, budget);
    state->framed->bytes.resize(chunk_framed_length(ref.length));
    state->completion = std::move(completion);
    state->next();
}
catch (const std::bad_alloc &) {
    completion.complete(Status::resource_exhausted());
}
} // namespace crowdb::tree::detail
