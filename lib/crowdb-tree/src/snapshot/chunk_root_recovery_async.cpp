// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/scheduled_page_store.h"
#include "snapshot/chunk_root_recovery.h"

#include <algorithm>
#include <stdexec/execution.hpp>

namespace crowdb::tree
{
struct ChunkRootRecovery::FrameReads : std::enable_shared_from_this<FrameReads>
{
    struct Work
    {
        AsyncCompletion begin(FrameReads &reads, detail::CallbackValueSender::Complete done, void *receiver)
        {
            complete = done;
            context  = receiver;
            return {.context     = this,
                    .complete_fn = &Work::finished,
                    .owner       = reads.shared_from_this(),
                    .allocations = reads.root.metadata_};
        }

        static void finished(void *raw, Status status)
        {
            auto &work = *static_cast<Work *>(raw);
            // Each child reports its outcome for ordered validation after join.
            work.status = std::move(status);
            work.complete(work.context);
        }

        detail::CallbackValueSender::Complete complete = nullptr;
        void                                 *context  = nullptr;
        Status                                status;
    };

    struct Pack : Work
    {
        static void submit(void *raw, detail::CallbackValueSender::Complete done, void *context)
        {
            auto &self = *static_cast<Pack *>(raw);
            auto &root = self.reads->root;
            submit_pin_chunk_backing(root.domain_, self.first, self.pin, root.metadata_, &self.backing,
                                     self.begin(*self.reads, done, context), &self.stage);
        }

        FrameReads                             *reads = nullptr;
        detail::ChunkFixedPackRef               first;
        detail::ChunkStageId                    pin;
        std::optional<detail::ChunkStageRecord> stage;
        std::shared_ptr<const ChunkPackBacking> backing;
    };

    struct Frame : Work
    {
        explicit Frame(const std::shared_ptr<AllocationBudget> &budget)
            : blob(detail::ReservationAllocator<uint8_t, AllocationKind::kIo>(budget))
        {
        }

        static void submit(void *raw, detail::CallbackValueSender::Complete done, void *context)
        {
            auto                       &self   = *static_cast<Frame *>(raw);
            auto                       &root   = self.reads->root;
            const auto                 &domain = *root.domain_;
            detail::ChunkFixedPackStore reader(domain.tree, domain.epoch, domain.mirrors, domain.catalog,
                                               domain.transport);
            reader.submit_read_into(self.record.pack, root.window_, self.blob, self.begin(*self.reads, done, context),
                                    root.cpu_);
        }

        FrameReads                                          *reads = nullptr;
        ChunkIndexFrame                                      record;
        size_t                                               pack = 0;
        std::shared_ptr<const ChunkPackBacking>              backing;
        detail::ReservedVector<uint8_t, AllocationKind::kIo> blob;
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<FrameReads>        reads;
        std::shared_ptr<ChunkRootRecovery> root;

        [[nodiscard]] auto get_env() const noexcept
            -> stdexec::prop<stdexec::get_start_scheduler_t, stdexec::inline_scheduler>
        {
            return {stdexec::get_start_scheduler, stdexec::inline_scheduler{}};
        }

        void set_value() noexcept
        {
            auto self     = std::move(reads);
            auto recovery = std::move(root);
            recovery->complete_step(std::move(self->failure));
        }
    };

    struct Phase
    {
        using Joined =
            stdexec::connect_result_t<decltype(std::declval<stdexec::simple_counting_scope &>().join()), Receiver>;

        explicit Phase(Receiver receiver) : joined(stdexec::connect(scope.join(), std::move(receiver)))
        {
        }

        stdexec::simple_counting_scope scope;
        Joined                         joined;
    };

    explicit FrameReads(ChunkRootRecovery &recovery)
        : root(recovery),
          packs(detail::ReservationAllocator<Pack>(root.metadata_)),
          frames(detail::ReservationAllocator<Frame>(root.metadata_))
    {
    }

    Status prepare()
    {
        packs.reserve(root.left_);
        frames.reserve(root.left_);
        auto offset   = root.offset_;
        auto previous = root.have_pid_ ? std::optional{root.last_pid_} : std::nullopt;
        for (uint64_t left = root.left_; left != 0; --left) {
            auto &frame = frames.emplace_back(root.window_);
            frame.reads = this;
            auto status = root.prepare_frame(&frame.record, &offset, left, previous);
            if (!status.ok()) {
                return status;
            }
            previous          = frame.record.page_id;
            const auto &ref   = frame.record.pack;
            const auto  found = std::find_if(packs.begin(), packs.end(),
                                             [&](const Pack &pack) { return pack.first.stage == ref.stage; });
            frame.pack        = static_cast<size_t>(found - packs.begin());
            if (found == packs.end()) {
                auto &pack = packs.emplace_back();
                pack.reads = this;
                pack.first = ref;
                status     = root.next_pin(&pack.pin);
                if (!status.ok()) {
                    return status;
                }
            }
        }
        root.offset_ = offset;
        return Status::Ok();
    }

    template <class Job, class Allocator> void launch(std::vector<Job, Allocator> &jobs, bool pins)
    {
        auto  self  = shared_from_this();
        auto &phase = (pins ? pin_phase : read_phase).emplace(Receiver{.reads = self, .root = root.shared_from_this()});
        try {
            for (auto &job : jobs) {
                stdexec::spawn(
                    detail::CallbackValueSender(&job, &Job::submit), phase.scope.get_token(),
                    stdexec::env{
                        stdexec::prop{stdexec::get_allocator, MetadataAllocator<std::byte>(root.metadata_)}
                });
            }
        }
        catch (const std::bad_alloc &) {
            failure = Status::resource_exhausted();
        }
        catch (...) {
            failure = Status::internal_error();
        }
        phase.scope.close();
        stdexec::start(phase.joined);
    }

    Status install()
    {
        for (auto &frame : frames) {
            if (!frame.status.ok()) {
                return std::move(frame.status);
            }
            auto status = root.frame_loaded(frame.record, std::move(frame.backing), frame.blob);
            if (!status.ok()) {
                return status;
            }
        }
        return Status::Ok();
    }

    Status prepare_reads()
    {
        for (auto &frame : frames) {
            auto &pack = packs[frame.pack];
            if (!pack.status.ok()) {
                return std::move(pack.status);
            }
            auto status = validate_chunk_pin_stage(*root.domain_, frame.record.pack, pack.stage);
            if (!status.ok()) {
                return status;
            }
            auto backing  = detail::reserved_shared<ChunkPackBacking>(root.metadata_, *pack.backing);
            backing->pack = frame.record.pack;
            frame.backing = std::move(backing);
            frame.blob.resize(frame.record.pack.length);
        }
        return Status::Ok();
    }

    ChunkRootRecovery            &root;
    detail::ReservedVector<Pack>  packs;
    detail::ReservedVector<Frame> frames;
    std::optional<Phase>          pin_phase;
    std::optional<Phase>          read_phase;
    Status                        failure;
};

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
        status = index_loaded();
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
            const auto &pack = self->async_pack_->pack;
            self->index_bytes_.resize(pack.length);
            const auto                 &domain = *self->domain_;
            detail::ChunkFixedPackStore reader(domain.tree, domain.epoch, domain.mirrors, domain.catalog,
                                               domain.transport);
            reader.submit_read_into(pack, self->window_, self->index_bytes_, std::move(complete), self->cpu_);
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
        cpu_ = std::move(cpu);
        if (left_ != 0) {
            if (!frame_reads_) {
                frame_reads_ = detail::reserved_shared<FrameReads>(metadata_, *this);
                status       = frame_reads_->prepare();
                if (!status.ok()) {
                    complete_step(std::move(status));
                    return;
                }
                frame_reads_->launch(frame_reads_->packs, true);
                return;
            }
            if (!frame_reads_->read_phase) {
                status = frame_reads_->prepare_reads();
                if (!status.ok()) {
                    complete_step(std::move(status));
                    return;
                }
                frame_reads_->launch(frame_reads_->frames, false);
                return;
            }
            auto reads = std::move(frame_reads_);
            complete_step(reads->install());
            return;
        }
        if (index_ == root_.indexes.size()) {
            complete_step(finish_indexes());
            return;
        }
        const auto &pack = root_.indexes[index_].pack;
        if (pack.length > kChunkIndexWindowBytes) {
            complete_step(Status::corruption());
            return;
        }
        detail::ChunkStageId pin;
        status = next_pin(&pin);
        if (!status.ok()) {
            complete_step(std::move(status));
            return;
        }
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
