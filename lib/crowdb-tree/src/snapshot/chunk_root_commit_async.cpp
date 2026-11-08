// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_root_commit.h"
#include "page_store/local/scheduled_page_store.h"
#include "page_store/page_block_io.h"

namespace crowdb::tree
{
AsyncCompletion ChunkRootCommit::step_reply(AsyncCompleteFn callback)
{
    return {.context = this, .complete_fn = callback, .owner = shared_from_this(), .allocations = metadata_};
}

void ChunkRootCommit::complete_step(Status status)
{
    auto done = std::exchange(step_completion_, {});
    done.complete(std::move(status));
}

void ChunkRootCommit::submit_step(bool stopped, std::shared_ptr<ScheduledPageStore> store,
                                  AsyncCompletion complete) noexcept
{
    if (stage_ == Stage::Index) {
        submit_index(stopped, std::move(store), std::move(complete));
        return;
    }
    step_completion_ = std::move(complete);
    try {
        if (done()) {
            complete_step(status_.without_message());
            return;
        }
        if (stopped && stage_ != Stage::QueryBegin && stage_ != Stage::QueryPrepare && stage_ != Stage::QueryCommit &&
            stage_ != Stage::Abort && stage_ != Stage::QueryAbort && stage_ != Stage::Release) {
            complete_step(fail(Status::unavailable("Chunk checkpoint stopped before root commit")));
            return;
        }
        auto *catalog = domain_->catalog->protection();
        switch (stage_) {
        case Stage::Summaries: {
            auto status = prepare_summaries();
            if (!status.ok()) {
                complete_step(fail(std::move(status)));
                return;
            }
            auto completion = step_reply(+[](void *raw, Status result) {
                auto &self = *static_cast<ChunkRootCommit *>(raw);
                try {
                    if (result.ok() && self.new_summary_pack_) {
                        self.summary_pack_ = std::move(self.new_summary_pack_);
                    }
                    result = result.ok() ? self.install_summaries() : self.fail(std::move(result));
                }
                catch (const std::bad_alloc &) {
                    result = self.fail(Status::resource_exhausted());
                }
                self.complete_step(std::move(result));
            });
            if (root_.summaries) {
                detail::ChunkStageId pin;
                detail::ChunkId      unused;
                status = ids_->next(PagePurpose::kPageIndex, &pin, &unused);
                if (status.ok()) {
                    submit_pin_chunk_page_block(domain_, *root_.summaries, pin, metadata_, &summary_pack_,
                                                std::move(completion));
                }
                else {
                    completion.complete(std::move(status));
                }
            }
            else if (new_summary_pack_) {
                submit_chunk_write(*store, new_summary_pack_, summary_bytes_, window_, std::move(completion));
            }
            else {
                completion.complete(Status::Ok());
            }
            return;
        }
        case Stage::Begin: {
            auto status = catalog->check_read_owner();
            if (!status.ok()) {
                complete_step(fail(std::move(status)));
                return;
            }
            const detail::ChunkManifestCandidate candidate{.id                  = candidate_,
                                                           .expected_generation = root_.identity.root_generation - 1,
                                                           .reference_count     = references_.size(),
                                                           .reference_checksum  = checksum_,
                                                           .manifest            = manifest_};
            candidate_started_ = true;
            stage_             = Stage::QueryBegin;
            catalog->submit_begin_candidate(domain_->tree, candidate, step_reply(+[](void *raw, Status status) {
                                                auto &self   = *static_cast<ChunkRootCommit *>(raw);
                                                self.status_ = std::move(status);
                                                if (self.status_.ok()) {
                                                    self.stage_ =
                                                        self.references_.empty() ? Stage::Commit : Stage::Prepare;
                                                }
                                                self.complete_step(Status::Ok());
                                            }));
            return;
        }
        case Stage::Prepare: {
            pending_count_ = 0;
            auto next      = reference_;
            while (next != references_.end() && pending_count_ < pending_.size()) {
                pending_[pending_count_++] = *next++;
            }
            stage_ = Stage::QueryPrepare;
            catalog->submit_prepare_references(domain_->tree, candidate_, prepared_,
                                               std::span(pending_).first(pending_count_),
                                               step_reply(+[](void *raw, Status status) {
                                                   auto &self   = *static_cast<ChunkRootCommit *>(raw);
                                                   self.status_ = std::move(status);
                                                   if (self.status_.ok()) {
                                                       self.accepted_window();
                                                   }
                                                   self.complete_step(Status::Ok());
                                               }));
            return;
        }
        case Stage::Commit:
        case Stage::Abort: {
            const bool commit = stage_ == Stage::Commit;
            stage_            = commit ? Stage::QueryCommit : Stage::QueryAbort;
            catalog->submit_finish_candidate(domain_->tree, candidate_,
                                             commit ? CT_CHUNK_CANDIDATE_COMMIT : CT_CHUNK_CANDIDATE_ABORT,
                                             step_reply(+[](void *raw, Status status) {
                                                 auto &self = *static_cast<ChunkRootCommit *>(raw);
                                                 if (!status.ok()) {
                                                     if (self.status_.ok()) {
                                                         self.status_ = std::move(status);
                                                     }
                                                 }
                                                 else if (self.stage_ == Stage::QueryCommit) {
                                                     self.committed_ = true;
                                                     self.finish(Status::Ok());
                                                 }
                                                 else {
                                                     self.stage_ = Stage::Release;
                                                 }
                                                 self.complete_step(Status::Ok());
                                             }));
            return;
        }
        case Stage::QueryBegin:
        case Stage::QueryPrepare:
        case Stage::QueryCommit:
        case Stage::QueryAbort:
            candidate_query_.reset();
            catalog->submit_load_candidate(domain_->tree, candidate_, &candidate_query_,
                                           step_reply(+[](void *raw, Status status) {
                                               auto &self = *static_cast<ChunkRootCommit *>(raw);
                                               try {
                                                   status = self.query_loaded(std::move(status), self.candidate_query_);
                                               }
                                               catch (const std::bad_alloc &) {
                                                   status = self.finish(Status::resource_exhausted(), true);
                                               }
                                               catch (...) {
                                                   status = self.finish(Status::internal_error(), true);
                                               }
                                               self.complete_step(std::move(status));
                                           }));
            return;
        case Stage::Release:
            if (released_ == prepared_) {
                complete_step(finish(status_.without_message()));
                return;
            }
            catalog->submit_release_references(domain_->tree, candidate_, released_,
                                               step_reply(+[](void *raw, Status status) {
                                                   auto &self = *static_cast<ChunkRootCommit *>(raw);
                                                   if (status.ok()) {
                                                       ++self.released_;
                                                   }
                                                   else {
                                                       status = self.finish(std::move(status), true);
                                                   }
                                                   self.complete_step(std::move(status));
                                               }));
            return;
        default:
            complete_step(advance());
            return;
        }
    }
    catch (const std::bad_alloc &) {
        if (stage_ == Stage::QueryBegin || stage_ == Stage::QueryPrepare || stage_ == Stage::QueryCommit ||
            stage_ == Stage::QueryAbort) {
            status_ = Status::resource_exhausted();
            complete_step(Status::Ok());
        }
        else {
            complete_step(fail(Status::resource_exhausted()));
        }
    }
    catch (...) {
        complete_step(fail(Status::internal_error()));
    }
}
} // namespace crowdb::tree
