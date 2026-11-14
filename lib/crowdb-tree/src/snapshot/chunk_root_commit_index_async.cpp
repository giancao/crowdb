// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/backing_io.h"
#include "backend/local/scheduled_page_store.h"
#include "chunk_root_commit.h"
#include "crowdb-common/log.h"

namespace crowdb::tree
{
struct ChunkRootCommit::IndexFlow : std::enable_shared_from_this<IndexFlow>
{
    struct Job
    {
        explicit Job(const std::shared_ptr<AllocationBudget> &window)
            : bytes(detail::ReservationAllocator<uint8_t>(window))
        {
        }

        IndexFlow                                                      *owner = nullptr;
        std::array<const DurableCapturedFrame *, kChunkIndexFrameLimit> frames{};
        size_t                                                          count = 0;
        ChunkMetadataBytes                                              bytes;
        std::shared_ptr<ChunkPackBacking>                               written;
        std::shared_ptr<const ChunkPackBacking>                         result;
        std::optional<ChunkIndexReference>                              previous;
        detail::ChunkStageId                                            pin;
        detail::CallbackComplete                                        complete = nullptr;
        void                                                           *context  = nullptr;

        static void submit(void *raw, detail::CallbackComplete done, void *context) noexcept
        {
            auto &self    = *static_cast<Job *>(raw);
            self.complete = done;
            self.context  = context;
            try {
                auto                 &root = *self.owner->root;
                const AsyncCompletion callback{
                    .context = raw, .complete_fn = &Job::finished, .allocations = root.metadata_};
                if (self.previous) {
                    submit_pin_chunk_backing(root.domain_, self.previous->pack, self.pin, root.metadata_, &self.result,
                                             callback);
                }
                else {
                    submit_chunk_write(*self.owner->store, self.written, self.bytes, root.window_, callback);
                }
            }
            catch (const std::bad_alloc &) {
                finished(raw, Status::resource_exhausted());
            }
            catch (...) {
                finished(raw, Status::internal_error());
            }
        }

        static void finished(void *raw, Status status)
        {
            auto &self = *static_cast<Job *>(raw);
            if (status.ok() && !self.previous) {
                self.result = self.written;
            }
            self.complete(self.context, status.ok() ? detail::CallbackSignal::kValue : detail::CallbackSignal::kError,
                          std::move(status));
        }
    };

    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        IndexFlow *owner;

        [[nodiscard]] auto get_env() const noexcept
            -> stdexec::prop<stdexec::get_start_scheduler_t, stdexec::inline_scheduler>
        {
            return {stdexec::get_start_scheduler, stdexec::inline_scheduler{}};
        }

        void set_value() noexcept
        {
            owner->joined();
        }
    };

    using Joined =
        stdexec::connect_result_t<decltype(std::declval<stdexec::simple_counting_scope &>().join()), Receiver>;

    IndexFlow(std::shared_ptr<ChunkRootCommit> owner, std::shared_ptr<ScheduledPageStore> cpu)
        : root(std::move(owner)),
          store(std::move(cpu)),
          jobs(detail::ReservationAllocator<std::shared_ptr<Job>>(root->metadata_)),
          joined_operation(stdexec::connect(scope.join(), Receiver{this}))
    {
    }

    std::shared_ptr<ChunkRootCommit>             root;
    std::shared_ptr<ScheduledPageStore>          store;
    detail::ReservedVector<std::shared_ptr<Job>> jobs;
    stdexec::simple_counting_scope               scope;
    Joined                                       joined_operation;
    AsyncCompletion                              completion;
    std::atomic<bool>                            failed{false};
    Status                                       failure;

    void error(Status status) noexcept
    {
        if (!failed.exchange(true, std::memory_order_acq_rel)) {
            failure = std::move(status);
        }
    }

    Status prepare_one()
    {
        auto job                            = detail::reserved_shared<Job>(root->metadata_, root->window_);
        job->owner                          = this;
        const ChunkIndexReference *previous = nullptr;
        if (root->previous_) {
            while (root->previous_index_ < root->previous_->indexes.size() &&
                   root->previous_->indexes[root->previous_index_].last_page < root->page_->first) {
                ++root->previous_index_;
            }
            if (root->previous_index_ < root->previous_->indexes.size()) {
                previous = &root->previous_->indexes[root->previous_index_];
            }
        }
        auto     next = root->page_;
        uint64_t last = UINT64_MAX;
        if (previous != nullptr) {
            last = next->first < previous->first_page ? previous->first_page - 1 : previous->last_page;
        }
        while (next != root->pages_->frames.end() && job->count < job->frames.size() && next->first <= last) {
            if (next->first != next->second.page_id) {
                return Status::corruption("Chunk checkpoint page identity mismatch");
            }
            job->frames[job->count++] = &next->second;
            ++next;
        }
        const auto      selected = std::span(job->frames).first(job->count);
        detail::ChunkId chunk;
        auto            status = root->ids_->next(PagePurpose::kPageIndex, &job->pin, &chunk);
        if (!status.ok()) {
            return status;
        }
        if (previous != nullptr && root->unchanged(selected, *previous)) {
            job->previous = *previous;
        }
        else {
            status = encode_chunk_index(root->root_.identity, jobs.size(), selected, &job->bytes);
            if (status.ok()) {
                status = prepare_chunk_backing(root->domain_, job->pin, chunk, static_cast<uint32_t>(job->bytes.size()),
                                               PagePurpose::kPageIndex, root->metadata_, &job->written);
            }
            if (!status.ok()) {
                return status;
            }
        }
        jobs.push_back(job);
        auto sender = detail::CallbackSender(job.get(), &Job::submit) |
                      stdexec::upon_error([job](Status status) noexcept { job->owner->error(std::move(status)); }) |
                      stdexec::upon_stopped([job]() noexcept { job->owner->error(Status::unavailable()); });
        stdexec::spawn(std::move(sender), scope.get_token(),
                       stdexec::env{
                           stdexec::prop{stdexec::get_allocator, MetadataAllocator<std::byte>(root->metadata_)}
        });
        root->page_ = next;
        return Status::Ok();
    }

    void join(AsyncCompletion done) noexcept
    {
        completion = std::move(done);
        scope.close();
        stdexec::start(joined_operation);
    }

    Status aggregate() noexcept
    {
        Status status;
        try {
            if (failed.load(std::memory_order_acquire)) {
                status = std::move(failure);
            }
            else {
                for (const auto &job : jobs) {
                    for (size_t index = 0; index < job->count; ++index) {
                        const auto &frame = *job->frames[index];
                        root->references_.insert(frame.backing->chunk->pack.stage);
                        root->root_.stored_references += frame.stored_overflow ? frame.stored_overflow->size() : 0;
                    }
                    if (root->root_.stored_references > detail::kMaxCandidateReferences) {
                        status = Status::resource_exhausted();
                        break;
                    }
                    root->references_.insert(job->result->pack.stage);
                    root->root_.indexes.push_back({.first_page  = job->frames[0]->page_id,
                                                   .last_page   = job->frames[job->count - 1]->page_id,
                                                   .frame_count = job->count,
                                                   .pack        = job->result->pack});
                    root->index_pins_.push_back(job->result);
                }
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        if (status.ok()) {
            root->page_  = root->pages_->frames.begin();
            root->stage_ = Stage::Dependencies;
        }
        else {
            status = root->fail(std::move(status));
        }
        root->index_flow_.reset();
        return status;
    }

    void joined() noexcept
    {
        auto   self = weak_from_this().lock();
        auto   done = std::exchange(completion, {});
        Status status;
        try {
            status = self ? store->submit_step([self] { return self->aggregate(); }, done) : Status::internal_error();
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        if (!status.ok()) {
            status = root->fail(std::move(status));
            root->index_flow_.reset();
            try {
                done.complete(std::move(status));
            }
            catch (...) {
                CRB_LOG_ERROR("Chunk index join receiver threw after terminal");
            }
        }
    }
};

void ChunkRootCommit::submit_index(bool stopped, std::shared_ptr<ScheduledPageStore> store,
                                   AsyncCompletion complete) noexcept
{
    try {
        if (!index_flow_) {
            index_flow_ = detail::reserved_shared<IndexFlow>(metadata_, shared_from_this(), std::move(store));
        }
        auto flow = index_flow_;
        if (stopped) {
            flow->error(Status::unavailable("Chunk checkpoint stopped before root commit"));
        }
        if (flow->failed.load(std::memory_order_acquire) || page_ == pages_->frames.end()) {
            flow->join(std::move(complete));
            return;
        }
        auto status = flow->prepare_one();
        if (!status.ok()) {
            flow->error(std::move(status));
            flow->join(std::move(complete));
            return;
        }
        complete.complete(Status::Ok());
    }
    catch (const std::bad_alloc &) {
        if (index_flow_) {
            index_flow_->error(Status::resource_exhausted());
            index_flow_->join(std::move(complete));
        }
        else {
            complete.complete(fail(Status::resource_exhausted()));
        }
    }
    catch (...) {
        if (index_flow_) {
            index_flow_->error(Status::internal_error());
            index_flow_->join(std::move(complete));
        }
        else {
            complete.complete(fail(Status::internal_error()));
        }
    }
}
} // namespace crowdb::tree
