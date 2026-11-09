// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/scheduled_page_store.h"
#include "snapshot/chunk_root_recovery.h"

namespace crowdb::tree
{
AsyncCompletion ChunkRootRecovery::async_reply(AsyncCompleteFn callback)
{
    return {.context = this, .complete_fn = callback, .owner = shared_from_this(), .allocations = metadata_};
}

void ChunkRootRecovery::complete_step(Status status)
{
    if (!status.ok()) {
        status = finish(std::move(status));
    }
    auto done = std::exchange(completion_, {});
    done.complete(std::move(status));
}

void ChunkRootRecovery::read_completed(Status status)
try {
    if (status.ok()) {
        status = domain_->catalog->protection()->check_read_owner();
    }
    if (status.ok()) {
        status = reading_index_ ? index_loaded() : frame_loaded(async_record_, std::move(async_pack_));
    }
    async_pack_.reset();
    complete_step(std::move(status));
}
catch (const std::bad_alloc &) {
    complete_step(Status::resource_exhausted());
}
catch (...) {
    complete_step(Status::internal_error());
}

void ChunkRootRecovery::read_pinned(Status status)
try {
    if (!status.ok()) {
        complete_step(std::move(status));
        return;
    }
    auto self      = shared_from_this();
    auto submitted = cpu_->submit_async_step(
        [self](AsyncCompletion complete) {
            const auto        &pack = self->async_pack_->pack;
            std::span<uint8_t> output;
            if (self->reading_index_) {
                self->index_bytes_.resize(pack.length);
                output = self->index_bytes_;
            }
            else {
                self->blob_.resize(pack.length);
                output = self->blob_;
            }
            const auto                 &domain = *self->domain_;
            detail::ChunkFixedPackStore reader(domain.tree, domain.epoch, domain.mirrors, domain.catalog,
                                               domain.transport);
            reader.submit_read_into(pack, self->window_, output, std::move(complete), self->cpu_);
        },
        async_reply(+[](void *raw, Status status) {
            static_cast<ChunkRootRecovery *>(raw)->read_completed(std::move(status));
        }));
    if (!submitted.ok()) {
        complete_step(std::move(submitted));
    }
}
catch (const std::bad_alloc &) {
    complete_step(Status::resource_exhausted());
}
catch (...) {
    complete_step(Status::internal_error());
}

void ChunkRootRecovery::submit_step(bool stopped, std::shared_ptr<ScheduledPageStore> cpu,
                                    AsyncCompletion complete) noexcept
{
    completion_ = std::move(complete);
    try {
        if (done_) {
            complete_step(status_);
            return;
        }
        if (stopped) {
            complete_step(Status::unavailable());
            return;
        }
        auto status = domain_->catalog->protection()->check_read_owner();
        if (!status.ok()) {
            complete_step(std::move(status));
            return;
        }
        if (dependency_phase_) {
            complete_step(dependencies());
            return;
        }
        reading_index_ = left_ == 0;
        if (reading_index_ && index_ == root_.indexes.size()) {
            complete_step(index());
            return;
        }
        detail::ChunkFixedPackRef pack;
        if (reading_index_) {
            pack = root_.indexes[index_].pack;
            if (pack.length > kChunkIndexWindowBytes) {
                complete_step(Status::corruption());
                return;
            }
        }
        else {
            status = prepare_frame(&async_record_);
            if (!status.ok()) {
                complete_step(std::move(status));
                return;
            }
            pack = async_record_.pack;
        }
        detail::ChunkStageId pin;
        status = next_pin(&pin);
        if (!status.ok()) {
            complete_step(std::move(status));
            return;
        }
        cpu_ = std::move(cpu);
        async_pack_.reset();
        submit_pin_chunk_backing(domain_, pack, pin, metadata_, &async_pack_,
                                 async_reply(+[](void *raw, Status status) {
                                     static_cast<ChunkRootRecovery *>(raw)->read_pinned(std::move(status));
                                 }));
    }
    catch (const std::bad_alloc &) {
        complete_step(Status::resource_exhausted());
    }
    catch (...) {
        complete_step(Status::internal_error());
    }
}
} // namespace crowdb::tree
