// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/overflow_page_block.h"
#include "btree/page_cache.h"
#include "btree/writeback_rebase.h"
#include "crowdb-common/log.h"
#include "page_store/checkpoint_page_store.h"
#include "page_store/local/extent_writeback.h"
#include "snapshot/flush_persistence.h"
#include "snapshot/primary_page_block.h"

#include <algorithm>
#include <bit>
#include <cassert>

namespace crowdb::tree
{
namespace
{
size_t final_frame_bytes(const PageEntry &page, size_t normal)
{
    const auto required = std::max(page.metrics.page_size_known ? page.metrics.page_size_bytes : 0,
                                   page.primary ? size_t{page.primary->frame.frame_bytes} : 0);
    return required > kMaxPhysicalPageBytes ? required : std::max(normal, std::bit_ceil(required));
}
} // namespace

struct FlushPersistence::Page : std::enable_shared_from_this<Page>
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        std::shared_ptr<Page> owner;

        void set_value() noexcept
        {
            auto self = std::move(owner);
            if (self->has_resident_overflow) {
                self->resume(Status::Ok());
                return;
            }
            // A successful write already completed on this CPU pool.
            self->status = Status::Ok();
            Page::run(self.get());
        }

        void set_error(Status error) noexcept
        {
            auto self = std::move(owner);
            self->resume(std::move(error));
        }

        void set_stopped() noexcept
        {
            set_error(Status::unavailable());
        }
    };

    using Connected = stdexec::connect_result_t<detail::CallbackSender, Receiver>;

    struct Attempt
    {
        Attempt(detail::CallbackSender sender, std::shared_ptr<Page> page)
            : connected(stdexec::connect(sender, Receiver{std::move(page)}))
        {
        }

        Connected connected;
    };

    std::shared_ptr<FlushPersistence>       owner;
    PageEncodeInput                         input;
    FlushBatchOptions::ReserveOverflow      reserve;
    FlushBatchOptions::PersistComplete      done;
    CheckpointWork                          work;
    CapturedWriteLimits                     limits;
    std::shared_ptr<CapturedFrameStream>    stream;
    std::shared_ptr<CapturedWriteOperation> write;
    std::optional<Attempt>                  attempt;
    Status                                  status;
    std::optional<TreeMetricsOperation>     metrics;
    LeafDecode                              decode_bases;
    std::shared_ptr<const PageEntry>        replacement;
    ApplyDispatcher::Node                   node{&Page::run, this};
    std::shared_ptr<Page>                   next_resident_page;
    bool                                    has_resident_overflow = false;
    bool                                    owns_resident_write   = false;
    bool                                    retried               = false;

    void start()
    {
        metrics.emplace(owner->preparation_->core->publisher().work_metrics(), TreeStage::PageWritebackPipeline,
                        tree_metrics_now_ns());
        has_resident_overflow =
            input.page->leaf && std::ranges::any_of(input.page->leaf->bases, [](const BaseRef &base) {
                return base.page_block && base.page_block->storage == PageBlockStorage::kResidentFrame &&
                       base.page_block->overflow;
            });
        if (has_resident_overflow) {
            resume(Status::Ok());
        }
        else {
            run(this);
        }
    }

    void prepare()
    {
        if (decode_bases) {
            auto decode = std::move(decode_bases);
            auto result = decode();
            if (!result.ok()) {
                finish(std::move(result));
                return;
            }
        }
        auto &preparation = *owner->preparation_;
        limits            = preparation.options.write;
        const auto bytes  = final_frame_bytes(*input.page, limits.frames.encode.frame_bytes);
        if (bytes > kMaxPhysicalPageBytes) {
            finish(Status::invalid_argument("final page exceeds the physical page safety bound"));
            return;
        }
        if (bytes > limits.frames.encode.frame_bytes) {
            CRB_LOG_WARN("persistent page {} uses {} bytes above the ordinary {} byte target", input.page_id, bytes,
                         limits.frames.encode.frame_bytes);
        }
        limits.frames.encode.frame_bytes = static_cast<uint32_t>(bytes);
        limits.write.max_frame_bytes     = bytes;
        if (input.page->leaf) {
            limits.frames.encode.max_sources =
                std::max<size_t>(1, input.page->leaf->bases.size() + input.page->leaf->overlays.size());
        }
        limits.frames.max_depth           = 1;
        limits.frames.reuse_page_store_id = preparation.core->local_page_store()
                                              ? preparation.core->local_page_store()->ledger->limits().page_store_id
                                              : preparation.core->page_chunk_store()->page_store_identity();
        if (!work.loader) {
            auto result = preparation.work(*owner->initial_, nullptr, &work);
            if (!result.ok()) {
                finish(std::move(result));
                return;
            }
        }
        if (input.page->leaf &&
            std::ranges::any_of(input.page->leaf->bases, [](const BaseRef &base) { return !base.resident; })) {
            auto page  = detail::reserved_shared<PageEntry>(work.budget.bytes, *input.page);
            auto leaf  = detail::reserved_shared<PageState>(work.budget.bytes, *page->leaf, work.budget.bytes);
            page->leaf = leaf;
            input.page = std::move(page);
            if (preparation.core->cache() &&
                std::ranges::any_of(leaf->bases, [](const BaseRef &base) { return base.clipped && !base.resident; })) {
                work.loader =
                    preparation.core->cache()->bind(work.loader, work.budget.bytes, PageLoadPriority::Maintenance);
            }
            auto loaded = read_leaf_bases(*work.loader, *leaf, work.budget.bytes,
                                          [self = shared_from_this()](Status status, LeafDecode decode) noexcept {
                                              self->decode_bases = std::move(decode);
                                              self->resume(std::move(status));
                                          });
            if (!loaded) {
                return;
            }
            if (!loaded->ok()) {
                finish(std::move(*loaded));
                return;
            }
        }
        limits.frames.encode.metadata     = work.budget.bytes;
        limits.frames.encode.work_metrics = work.budget.work_metrics;
        limits.frames.encode.cause        = work.budget.cause;
        limits.frames.plan_only           = false;
        if (limits.write.compression == compress_algo::kNone) {
            limits.frames.raw_write = limits.write;
        }
        const auto &config = preparation.core->config();
        limits.frames.retain_overflow_buffers =
            limits.write.chunk_backed || config.async_page_store != nullptr ||
            (config.page_store != nullptr && typeid(*config.page_store) == typeid(PageMemStore));
        auto result = CapturedFrameStream::create(input, work.loader, preparation.core->binding()->cpu(),
                                                  preparation.core->binding()->lane(), limits.frames, reserve, &stream);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        auto pages          = detail::reserved_shared<CapturedDurablePages>(work.budget.bytes, work.budget.bytes);
        pages->coverage     = input.coverage;
        pages->generation   = owner->initial_->generation;
        pages->root_page_id = input.page_id;
        pages->next_page_id = input.next_page_id;
        auto chunk          = preparation.core->page_chunk_store();
        auto inherited      = owner->inherited_;
        if (owns_resident_write) {
            if (owner->pages_) {
                inherited = owner->pages_;
            }
        }
        else {
            // Independent output pages only read the immutable baseline.
            work.budget.cpu_progress.reset();
        }
        result = CapturedWriteOperation::create(
            stream, pages, preparation.ticket->page_store(preparation.core->async_page_store()),
            preparation.core->binding()->cpu(), preparation.core->binding()->lane(), limits, work.budget,
            chunk ? chunk->reserve(work.budget.bytes)
                  : extent_write_reservation(preparation.core->local_page_store()->ledger, work.budget.bytes),
            std::move(inherited), &write);
        if (!result.ok()) {
            finish(std::move(result));
            return;
        }
        begin(write->sender());
    }

    void begin(detail::CallbackSender sender)
    {
        attempt.emplace(sender, shared_from_this());
        stdexec::start(attempt->connected);
    }

    void resume(Status result) noexcept
    {
        status    = std::move(result);
        auto self = weak_from_this().lock();
        if (has_resident_overflow) {
            owner->preparation_->cpu_progress->request(node, std::move(self));
            return;
        }
        const auto &binding = owner->preparation_->core->binding();
        try {
            auto submitted = binding->cpu()->try_submit(binding->lane(), &Page::run, this, self);
            if (!submitted.ok()) {
                attempt.reset();
                finish(std::move(submitted));
            }
        }
        catch (const std::bad_alloc &) {
            attempt.reset();
            finish(Status::resource_exhausted());
        }
    }

    static void run(void *raw) noexcept
    {
        auto self = static_cast<Page *>(raw)->weak_from_this().lock();
        assert(self);
        try {
            self->step();
        }
        catch (const std::bad_alloc &) {
            self->finish(Status::resource_exhausted("flush page reservation exhausted"));
        }
        catch (...) {
            self->finish(Status::internal_error("flush page persistence failed"));
        }
    }

    void step()
    {
        if (replacement) {
            install();
            return;
        }
        attempt.reset();
        if (!status.ok()) {
            if (write && write->result().primary_io_failed && owner->preparation_->core->local_page_store() &&
                owner->preparation_->core->config().async_page_store != nullptr &&
                limits.frames.exclusion.length == 0 && !retried) {
                // The failed local write reached terminal completion. Its
                // unreferenced extents return to the existing ledger/GC path.
                retried = true;
                write.reset();
                stream.reset();
                prepare();
            }
            else {
                finish(status);
            }
            return;
        }
        if (write) {
            written();
        }
        else {
            // Imported raw overflow can be shared by different leaf pages.
            // Finish staging it before the next importer looks up its PID.
            if (has_resident_overflow && !owns_resident_write) {
                if (auto *previous = std::exchange(owner->last_resident_page_, this)) {
                    previous->next_resident_page = shared_from_this();
                    return;
                }
                owns_resident_write = true;
            }
            prepare();
        }
    }

    void written()
    {
        const auto &result = write->result();
        const auto  found  = result.pages->frames.find(input.page_id);
        if (found == result.pages->frames.end()) {
            finish(Status::corruption("flush page has no durable receipt"));
            return;
        }
        auto receipt = found->second;
        if (input.children_summary && receipt.summary) {
            receipt.summary->oldest_tombstone_slot = input.children_summary->oldest_tombstone_slot;
        }
        auto status = build_writeback_rebase(input.page, input.page, receipt, input.coverage, work.budget.bytes,
                                             &replacement, limits.frames.encode.max_sources, input.gc_floor);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        if (owns_resident_write) {
            install();
        }
        else {
            owner->preparation_->cpu_progress->request(node, weak_from_this().lock());
        }
    }

    void install()
    {
        const auto &result = write->result();
        // The catalog receives only final page receipts and their protected raw
        // dependencies. Structural deletions are applied at the root join.
        if (!owner->pages_) {
            owner->pages_ = detail::reserved_shared<CapturedDurablePages>(work.budget.bytes, work.budget.bytes);
            if (owner->inherited_) {
                owner->pages_->frames.inherit(owner->inherited_->frames);
                owner->pages_->overflow_counts_known  = owner->inherited_->overflow_counts_known;
                owner->pages_->stored_reference_count = owner->inherited_->stored_reference_count;
            }
            auto initialized = owner->pages_->initialize_overflow_counts();
            if (!initialized.ok()) {
                finish(std::move(initialized));
                return;
            }
        }
        owner->pages_->frames.budget(work.budget.bytes);
        auto status = owner->pages_->replace_primary(*result.pages);
        if (status.ok()) {
            owner->pages_->frames.at(input.page_id).summary = replacement->primary->frame.summary;
        }
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        owner->report_.written_frames += result.written_frames;
        owner->report_.written_bytes += result.written_bytes;
        owner->report_.reused_frames += result.reused_frames;
        ++owner->report_.primary_frames;
        ++owner->report_.rebased_pages;
        finish(Status::Ok(), std::move(replacement));
    }

    void finish(Status result, std::shared_ptr<const PageEntry> replacement = {}) noexcept
    {
        if (owns_resident_write) {
            if (auto next = std::move(next_resident_page)) {
                next->owns_resident_write = true;
                next->resume(Status::Ok());
            }
            else {
                owner->last_resident_page_ = nullptr;
            }
        }
        if (metrics) {
            (void)metrics->finish(result.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        }
        if (!result.ok()) {
            CRB_LOG_ERROR("persistent page {} failed: {}", input.page_id, result.to_string());
        }
        try {
            done(std::move(result), std::move(replacement));
        }
        catch (...) {
            CRB_LOG_ERROR("persistent page receiver threw");
        }
    }
};

void FlushPersistence::write(PageEncodeInput input, FlushBatchOptions::ReserveOverflow reserve,
                             FlushBatchOptions::PersistComplete done)
try {
    const auto  backend     = preparation_->core->local_page_store()
                                ? preparation_->core->local_page_store()->ledger->limits().page_store_id
                                : preparation_->core->page_chunk_store()->page_store_identity();
    const auto &limits      = preparation_->options.write.frames;
    const auto  frame_bytes = final_frame_bytes(*input.page, limits.encode.frame_bytes);
    if (frame_bytes > kMaxPhysicalPageBytes) {
        done(Status::invalid_argument("final page exceeds the physical page safety bound"), {});
        return;
    }
    const bool same_gc_evidence =
        !input.children_summary ||
        (input.page->primary && input.page->primary->frame.summary &&
         input.page->primary->frame.summary->oldest_tombstone_slot == input.children_summary->oldest_tombstone_slot);
    if (same_gc_evidence && limits.exclusion.length == 0 && inherited_ && input.page->primary &&
        inherited_->frames.contains(input.page_id) &&
        input.page->primary->frame.page_block == inherited_->frames.at(input.page_id).page_block &&
        !limits.exclusion.overlaps(*inherited_->frames.at(input.page_id).page_block) &&
        (input.page->leaf ? can_reuse_primary(*input.page, input.page_id, input.context.range, input.gc_floor, backend,
                                              static_cast<uint32_t>(frame_bytes))
                          : input.context.inner_fences &&
                                can_reuse_inner_primary(*input.page, input.page_id, *input.context.inner_fences,
                                                        backend, static_cast<uint32_t>(frame_bytes)))) {
        done(Status::Ok(), {});
        return;
    }
    auto page     = detail::reserved_shared<Page>(preparation_->work_allocations);
    page->owner   = shared_from_this();
    page->input   = std::move(input);
    page->reserve = std::move(reserve);
    page->done    = done;
    page->start();
}
catch (const std::bad_alloc &) {
    done(Status::resource_exhausted("flush page control reserve exhausted"), {});
}

size_t FlushPersistence::control_allocation_bound() noexcept
{
    return detail::shared_allocation_bound<FlushPersistence>() + commit_control_allocation_bound();
}
} // namespace crowdb::tree
