// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "btree/page_load_leaf.h"
#include "btree/page_load_lifetime.h"
#include "page_store/local/scheduled_page_store.h"

#include <atomic>
#include <cassert>
#include <stdexcept>

namespace crowdb::tree
{
void PageLoader::read_leaf(BaseRef &base, LeafReadComplete complete)
{
    load_leaf(base, [&base, complete = std::move(complete)](Status status, auto decoded) noexcept {
        if (status.ok() && !decoded) {
            status = Status::corruption();
        }
        if (!status.ok()) {
            complete_page_load(complete, std::move(status), LeafDecode{});
            return;
        }
        complete_page_load(complete, Status::Ok(), LeafDecode([&base, decoded = std::move(decoded)]() mutable noexcept {
                               base.resident = std::move(decoded);
                               return Status::Ok();
                           }));
    });
}

void PageLoader::load_overflow(std::shared_ptr<const PageBlockRef> /*primary*/, uint64_t head, uint64_t length,
                               ValueComplete complete)
{
    load_overflow(head, length, std::move(complete));
}

void PageLoader::load_overflow(uint64_t /*head*/, uint64_t /*length*/, ValueComplete complete)
{
    complete(Status::not_supported("captured overflow page_block resolver is unavailable"), {});
}

namespace
{
void complete_leaf(LeafCompletion &complete, Status status, std::shared_ptr<const LeafBase> leaf = {}) noexcept
{
    if (auto *callback = std::get_if<LeafReadComplete>(&complete)) {
        auto done = std::move(*callback);
        complete_page_load(done, std::move(status), LeafDecode{});
    }
    else if (auto *callback = std::get_if<PageLoader::LeafComplete>(&complete)) {
        auto done = std::move(*callback);
        complete_page_load(done, std::move(status), std::move(leaf));
    }
}

struct LeafLoad : PageLoadProgress, std::enable_shared_from_this<LeafLoad>
{
    LeafLoad(const PageLoadBudget &budget, uint64_t started_at_ns)
        : PageLoadProgress(budget, started_at_ns),
          metadata(budget.bytes),
          blob(detail::ReservationAllocator<uint8_t>(metadata))
    {
    }

    std::shared_ptr<AllocationBudget>           metadata;
    std::shared_ptr<AsyncPageStore>             store;
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    PageLoadLimits                              limits;
    BaseRef                                     base;
    AlignedLeafLoadBytes                        blob;
    LeafCompletion                              complete;
    BaseRef                                    *decode_target = nullptr;
    std::shared_ptr<LeafLoad>                   queued_owner;
    std::atomic<bool>                           terminal{false};

    Status decode_into(std::shared_ptr<const LeafBase> *leaf) noexcept
    {
        try {
            return decode_loaded_leaf(blob.storage(), {blob.data(), blob.size()}, base, limits, metadata, leaf);
        }
        catch (const std::bad_alloc &) {
            return Status::resource_exhausted();
        }
    }

    void finish_metrics(const Status &status) noexcept
    {
        (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
    }

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true)) {
            return;
        }
        std::shared_ptr<const LeafBase> leaf;
        if (status.ok()) {
            status = decode_into(&leaf);
        }
        finish_metrics(status);
        complete_leaf(complete, std::move(status), std::move(leaf));
    }

    static void read_complete(void *context, Status status) noexcept
    {
        auto self = static_cast<LeafLoad *>(context)->weak_from_this().lock();
        assert(self);
        if (!status.ok()) {
            self->finish(std::move(status));
            return;
        }
        if (self->terminal.exchange(true)) {
            return;
        }
        auto *ready = std::get_if<LeafReadComplete>(&self->complete);
        assert(ready != nullptr);
        auto callback = std::move(*ready);
        complete_page_load(callback, Status::Ok(), LeafDecode([self = std::move(self)]() mutable noexcept {
                               auto load   = std::move(self);
                               auto result = load->decode_into(&load->decode_target->resident);
                               load->finish_metrics(result);
                               return result;
                           }));
    }

    static void decode(void *context) noexcept
    {
        auto self = std::move(static_cast<LeafLoad *>(context)->queued_owner);
        self->finish(Status::Ok());
    }
};
} // namespace

std::optional<size_t> AsyncStorePageLoader::primary_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                                  size_t workers, size_t retained_leaf_pages,
                                                                  size_t                cached_pages,
                                                                  std::optional<size_t> cached_inner_pages) noexcept
{
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    if (limits.max_leaf_entries == 0) {
        return {};
    }
    const auto chunk_window = chunk_load_window_bound(limits);
    const auto leaf         = decoded_leaf_allocation_bound(limits);
    const auto retained     = AllocationBound(retained_leaf_pages).add(cached_pages).add(workers).add(2).value();
    const auto operations   = AllocationBound(workers).add(1).value();
    const auto decode       = AllocationBound()
                                  .add_product(2, limits.max_frame_bytes)
                                  .add_product(limits.max_leaf_entries, sizeof(uint32_t))
                                  .value();
    if (!leaf || !retained || !operations || !decode || !chunk_window) {
        return {};
    }
    return AllocationBound()
        .add(routing_reserve_bound(limits, depth, workers, cached_inner_pages.value_or(cached_pages)))
        .add_product(*retained, *leaf)
        .add_product(workers, *decode)
        .add_product(*operations, shared_allocation_bound<LeafLoad>() + shared_allocation_bound<PageReadTicket>())
        .add_product(*operations, limits.max_blob_bytes)
        .add_product(*operations, *chunk_window)
        .add_product(*operations, AlignedLoadBytes::kAlignment - 1)
        .value();
}

std::optional<size_t> AsyncStorePageLoader::read_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                               size_t workers, size_t retained_leaf_pages,
                                                               size_t retained_values, size_t value_bytes,
                                                               size_t                cached_pages,
                                                               std::optional<size_t> cached_inner_pages) noexcept
{
    return detail::AllocationBound()
        .add(primary_reserve_bound(limits, depth, workers, retained_leaf_pages, cached_pages, cached_inner_pages))
        .add(overflow_read_allocation_bound(limits, workers, retained_values, value_bytes))
        .value();
}

AsyncStorePageLoader::AsyncStorePageLoader(std::shared_ptr<AsyncPageStore>             store,
                                           std::shared_ptr<detail::FlushRuntime>       runtime,
                                           std::shared_ptr<detail::FlushRuntime::Lane> lane, PageLoadLimits limits,
                                           OverflowPageBlockResolver overflow, PageLoadBudget budget)
{
    const auto prepaid = budget.reservation();
    if ((prepaid && prepaid->category() == detail::BudgetClass::kIo) ||
        (budget.io && (!budget.bytes || budget.io->category() != detail::BudgetClass::kIo ||
                       budget.bytes->domain() != budget.io->domain()))) {
        throw std::invalid_argument("page load needs memory and I/O credit from the same runtime");
    }
    state_           = detail::reserved_shared<State>(budget.bytes);
    state_->store    = std::move(store);
    state_->runtime  = std::move(runtime);
    state_->lane     = std::move(lane);
    state_->limits   = limits;
    state_->overflow = std::move(overflow);
    state_->budget   = std::move(budget);
}

void AsyncStorePageLoader::load_leaf(BaseRef base, LeafComplete complete)
{
    state_->start_leaf(std::move(base), std::move(complete), state_);
}

void AsyncStorePageLoader::read_leaf(BaseRef &base, LeafReadComplete complete)
{
    state_->start_leaf(base, std::move(complete), state_, &base);
}

std::optional<Status> AsyncStorePageLoader::try_load_leaf_bases(PageState &leaf)
{
    const auto *store = dynamic_cast<const ScheduledPageStore *>(state_->store.get());
    if (store == nullptr || !store->reads_inline()) {
        return std::nullopt;
    }
    const auto &budget = state_->budget;
    for (auto &base : leaf.bases) {
        if (base.resident) {
            continue;
        }
        if (base.page_block && base.page_block->storage == PageBlockStorage::kChunkPack) {
            return std::nullopt;
        }
        const auto started = tree_metrics_now_ns();
        auto       status  = state_->validate_leaf(base);
        if (status.ok() && base.page_block) {
            try {
                FrameStore::Bytes blob{FrameStore::Bytes::allocator_type(budget.bytes)};
                if (base.page_block->storage == PageBlockStorage::kDurableBlob) {
                    blob.resize(base.page_block->length);
                    status = store->read_inline(base.page_block->address, blob.data(), blob.size(), budget.cause);
                }
                if (status.ok()) {
                    status = decode_loaded_leaf(blob, blob, base, state_->limits, budget.bytes, &base.resident);
                }
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted();
            }
        }
        record_page_load_terminal(budget, started, status);
        if (!status.ok()) {
            return status;
        }
    }
    return Status::Ok();
}

Status AsyncStorePageLoader::State::validate_leaf(const BaseRef &base) const
{
    if (!runtime || !lane || !base.valid() || !base.page_block || base.page_block->encoding != 0 ||
        (!store && base.page_block->storage != PageBlockStorage::kResidentFrame)) {
        return Status::invalid_argument("leaf load requires a supported independently owned page_block");
    }
    if (base.page_block->length > limits.max_blob_bytes || !chunk_load_allowed(*base.page_block, limits)) {
        return Status::resource_exhausted("leaf blob exceeds load budget");
    }
    return Status::Ok();
}

void AsyncStorePageLoader::State::start_leaf(BaseRef base, LeafCompletion complete, const std::shared_ptr<State> &owner,
                                             BaseRef *decode_target)
{
    const auto started_at = tree_metrics_now_ns();
    const auto reject     = [&](Status status) {
        record_page_load_terminal(budget, started_at, status);
        complete_leaf(complete, std::move(status));
    };
    if (base.resident) {
        record_page_load_terminal(budget, started_at, Status::Ok());
        complete_leaf(complete, Status::Ok(), std::move(base.resident));
        return;
    }
    auto status = validate_leaf(base);
    if (!status.ok()) {
        reject(std::move(status));
        return;
    }
    std::shared_ptr<LeafLoad> operation;
    try {
        operation                = detail::reserved_shared<LeafLoad>(budget.bytes, budget, started_at);
        operation->complete      = std::move(complete);
        operation->store         = store;
        operation->runtime       = runtime;
        operation->lane          = lane;
        operation->loader_owner  = owner;
        operation->limits        = limits;
        operation->base          = std::move(base);
        operation->decode_target = decode_target;
        if (operation->base.page_block->storage == PageBlockStorage::kResidentFrame) {
            if (decode_target != nullptr) {
                LeafLoad::read_complete(operation.get(), Status::Ok());
            }
            else {
                queue_page_load(operation, &LeafLoad::decode);
            }
        }
        else {
            operation->blob.resize(operation->base.page_block->length);
            submit_page_read(operation, operation->base.page_block, operation->blob.data(), operation->blob.size(),
                             decode_target != nullptr ? &LeafLoad::read_complete : &page_read_completed<LeafLoad>);
        }
    }
    catch (const std::bad_alloc &) {
        if (operation) {
            operation->finish(Status::resource_exhausted("leaf load allocation failed"));
        }
        else {

            reject(Status::resource_exhausted("leaf load allocation failed"));
        }
    }
    catch (...) {
        if (operation) {
            operation->finish(Status::io_error());
        }
        else {
            reject(Status::io_error());
        }
    }
}

} // namespace crowdb::tree
