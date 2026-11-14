// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "btree/page_load_lifetime.h"
#include "btree/published_state.h"
#include "crowdb-tree/maptable/compressor.h"

namespace crowdb::tree
{
void PageLoader::load_inner(std::shared_ptr<const BackingRef> /*backing*/, InnerComplete complete)
{
    complete(Status::not_supported("asynchronous index loader is unavailable"), {});
}

namespace
{
Status read_inner_proof(const uint8_t *frame, uint32_t bytes, const PageLoadLimits &limits,
                        const std::shared_ptr<AllocationBudget> &metadata, size_t key_bytes, InnerFrameProof *proof)
{
    proof->self_page_id   = frame_u64(frame, fh::kSelfpage_id);
    proof->frame_bytes    = bytes;
    proof->has_fences     = frame_has_lower_fence(frame);
    proof->page_id_fences = frame_fences_are_page_ids(frame);
    if (proof->page_id_fences) {
        proof->lower_leaf = frame_lower_fence_page_id(frame);
        proof->upper_leaf = frame_upper_fence_page_id(frame);
    }
    else if (proof->has_fences) {
        for (const auto key : {frame_lower_fence(frame), frame_upper_fence(frame)}) {
            if (key.size() > limits.max_inner_key_bytes - key_bytes) {
                return Status::resource_exhausted("loaded index exceeds fence byte budget");
            }
            key_bytes += key.size();
        }
        proof->lower = OwnedKey(frame_lower_fence(frame), metadata);
        proof->upper = OwnedKey(frame_upper_fence(frame), metadata);
    }
    return Status::Ok();
}

Status decode_inner(std::span<const uint8_t> blob, const BackingRef &backing, const PageLoadLimits &limits,
                    const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<const InnerPageState> *out)
{
    LoadBytes                scratch{LoadBytes::allocator_type(metadata)};
    std::span<const uint8_t> frame;
    auto                     status = decode_backing_frame(blob, backing, limits, scratch, &frame);
    const auto               bytes  = static_cast<uint32_t>(frame.size());
    if (!status.ok()) {
        return status;
    }
    if (!frame_validate(frame.data(), bytes) || frame_page_type(frame.data()) != page_type::kInnerBase) {
        return Status::corruption("loaded backing is not a valid index frame");
    }
    const InnerFrameView index(frame.data(), bytes);
    if (index.num_children() > limits.max_inner_children) {
        return Status::resource_exhausted("loaded index exceeds child budget");
    }
    size_t key_bytes = 0;
    for (uint32_t i = 0; i < index.num_separators(); ++i) {
        const size_t size = index.separator_at(i).size();
        if (size > limits.max_inner_key_bytes - key_bytes) {
            return Status::resource_exhausted("loaded index exceeds separator byte budget");
        }
        key_bytes += size;
    }
    for (uint32_t i = 0; i < index.num_children(); ++i) {
        if (index.child_at(i) >= MappingView::kPageLimit) {
            return Status::corruption("loaded index contains an invalid child PID");
        }
    }
    auto inner = detail::reserved_shared<InnerPageState>(metadata, metadata);
    inner->children.reserve(index.num_children());
    inner->separators.reserve(index.num_separators());
    for (uint32_t i = 0; i < index.num_children(); ++i) {
        inner->children.push_back(index.child_at(i));
    }
    for (uint32_t i = 0; i < index.num_separators(); ++i) {
        inner->separators.emplace_back(index.separator_at(i), metadata);
    }
    status = read_inner_proof(frame.data(), bytes, limits, metadata, key_bytes, &inner->frame_proof.emplace());
    if (!status.ok()) {
        return status;
    }
    *out = std::move(inner);
    return Status::Ok();
}

void complete_inner_error(InnerCompletion &complete, Status status) noexcept
{
    if (auto *ready = std::get_if<InnerReadComplete>(&complete)) {
        complete_page_load(*ready, std::move(status), InnerDecode{});
    }
    else if (auto *loaded = std::get_if<PageLoader::InnerComplete>(&complete)) {
        complete_page_load(*loaded, std::move(status), std::shared_ptr<const InnerPageState>{});
    }
}

struct InnerLoad : PageLoadProgress, std::enable_shared_from_this<InnerLoad>
{
    InnerLoad(const PageLoadBudget &budget, uint64_t started_at_ns)
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
    std::shared_ptr<const BackingRef>           backing;
    AlignedLoadBytes                            blob;
    InnerCompletion                             complete;
    std::shared_ptr<InnerLoad>                  queued_owner;
    std::atomic<bool>                           terminal{false};

    Status decode_into(std::shared_ptr<const InnerPageState> *inner) noexcept
    {
        Status status;
        try {
            status = decode_inner({blob.data(), blob.size()}, *backing, limits, metadata, inner);
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted("index decode allocation failed");
        }
        catch (...) {
            status = Status::internal_error("index decode failed");
        }
        (void)metrics.finish(status.ok() ? TreeOutcome::Success : TreeOutcome::Error, tree_metrics_now_ns());
        return status;
    }

    void finish(Status status) noexcept
    {
        if (terminal.exchange(true)) {
            return;
        }
        if (!status.ok()) {
            (void)metrics.finish(TreeOutcome::Error, tree_metrics_now_ns());
            complete_inner_error(complete, std::move(status));
            return;
        }
        if (auto *ready = std::get_if<InnerReadComplete>(&complete)) {
            auto callback = std::move(*ready);
            complete_page_load(
                callback, Status::Ok(),
                InnerDecode([self = weak_from_this().lock()](std::shared_ptr<const InnerPageState> *out) noexcept {
                    return self->decode_into(out);
                }));
            return;
        }
        if (auto *loaded = std::get_if<PageLoader::InnerComplete>(&complete)) {
            std::shared_ptr<const InnerPageState> inner;
            status = decode_into(&inner);
            complete_page_load(*loaded, std::move(status), std::move(inner));
        }
    }

    static void read_complete(void *context, Status status) noexcept
    {
        static_cast<InnerLoad *>(context)->finish(std::move(status));
    }

    static void decode(void *context) noexcept
    {
        auto self = std::move(static_cast<InnerLoad *>(context)->queued_owner);
        self->finish(Status::Ok());
    }
};
} // namespace

std::optional<size_t> AsyncStorePageLoader::routing_reserve_bound(const PageLoadLimits &limits, size_t depth,
                                                                  size_t workers, size_t cached_pages) noexcept
{
    if (depth > 64 || workers == 0 || limits.max_inner_children == 0 || limits.max_inner_children > 4096 ||
        limits.max_blob_bytes == 0 || limits.max_frame_bytes == 0) {
        return {};
    }
    using detail::AllocationBound;
    using detail::shared_allocation_bound;
    const auto chunk_window = chunk_load_window_bound(limits);
    const auto inner        = AllocationBound(InnerPageState::allocation_bound())
                                  .add_product(limits.max_inner_children,
                                               sizeof(OwnedKey) + sizeof(uint64_t) + OwnedKey::allocation_overhead())
                                  .add(limits.max_inner_key_bytes)
                                  .add_product(2, OwnedKey::allocation_overhead())
                                  .value();
    const auto retained     = AllocationBound(depth).add(workers).add(cached_pages).add(2).value();
    const auto operations   = AllocationBound(workers).add(1).value();
    if (!inner || !retained || !operations || !chunk_window) {
        return {};
    }
    return AllocationBound(shared_allocation_bound<AsyncStorePageLoader>() + shared_allocation_bound<State>())
        .add_product(*retained, *inner)
        .add_product(*operations, shared_allocation_bound<InnerLoad>() + shared_allocation_bound<PageReadTicket>())
        .add_product(*operations, limits.max_blob_bytes)
        .add_product(*operations, *chunk_window)
        .add_product(*operations, AlignedLoadBytes::kAlignment - 1)
        .add_product(workers, limits.max_frame_bytes)
        .value();
}

void AsyncStorePageLoader::load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete)
{
    if (state_->budget.cause == TreeWorkCause::Flush && detail::FlushRuntime::on_worker_thread()) {
        std::shared_ptr<const InnerPageState> inner;
        if (auto result = state_->try_read_inner_inline(backing, &inner)) {
            complete_page_load(complete, std::move(*result), std::move(inner));
            return;
        }
    }
    state_->start_inner(std::move(backing), std::move(complete), state_);
}

std::optional<Status> AsyncStorePageLoader::read_inner(std::shared_ptr<const BackingRef>      backing,
                                                       std::shared_ptr<const InnerPageState> *out,
                                                       InnerReadComplete                      complete)
{
    if (auto result = state_->try_read_inner_inline(backing, out)) {
        return result;
    }
    state_->start_inner(std::move(backing), std::move(complete), state_);
    return std::nullopt;
}

std::optional<Status>
AsyncStorePageLoader::State::try_read_inner_inline(const std::shared_ptr<const BackingRef> &backing,
                                                   std::shared_ptr<const InnerPageState>   *out)
{
    const auto *scheduled = dynamic_cast<const ScheduledPageStore *>(store.get());
    if (backing &&
        (backing->storage == BackingStorage::kResidentFrame ||
         (scheduled != nullptr && scheduled->reads_inline() && backing->storage == BackingStorage::kDurableBlob))) {
        const auto started = tree_metrics_now_ns();
        auto       status  = validate_inner(backing);
        try {
            OverwriteByteVector<AllocationKind::kIo> blob{
                OverwriteByteVector<AllocationKind::kIo>::allocator_type(budget.bytes)};
            if (status.ok() && backing->storage == BackingStorage::kDurableBlob) {
                blob.resize(backing->length);
                status = scheduled->read_inline(backing->address, blob.data(), blob.size(), budget.cause);
            }
            if (status.ok()) {
                status = decode_inner(blob, *backing, limits, budget.bytes, out);
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        record_page_load_terminal(budget, started, status);
        return status;
    }
    return std::nullopt;
}

Status AsyncStorePageLoader::State::validate_inner(const std::shared_ptr<const BackingRef> &backing) const
{
    if (!runtime || !lane || !backing || !backing->readable() || backing->encoding != 0 ||
        (!store && backing->storage != BackingStorage::kResidentFrame)) {
        return Status::invalid_argument("index load requires a supported independently owned backing and runtime");
    }
    if (backing->length > limits.max_blob_bytes || !chunk_load_allowed(*backing, limits)) {
        return Status::resource_exhausted("index blob exceeds load budget");
    }
    return Status::Ok();
}

void AsyncStorePageLoader::State::start_inner(std::shared_ptr<const BackingRef> backing, InnerCompletion complete,
                                              const std::shared_ptr<State> &owner)
{
    const auto started_at = tree_metrics_now_ns();
    const auto reject     = [&](Status status) {
        record_page_load_terminal(budget, started_at, status);
        complete_inner_error(complete, std::move(status));
    };
    auto status = validate_inner(backing);
    if (!status.ok()) {
        reject(std::move(status));
        return;
    }
    std::shared_ptr<InnerLoad> operation;
    try {
        operation               = detail::reserved_shared<InnerLoad>(budget.bytes, budget, started_at);
        operation->store        = store;
        operation->runtime      = runtime;
        operation->lane         = lane;
        operation->loader_owner = owner;
        operation->limits       = limits;
        operation->backing      = std::move(backing);
        operation->complete     = std::move(complete);
        if (operation->backing->storage == BackingStorage::kResidentFrame) {
            queue_page_load(operation, &InnerLoad::decode);
        }
        else {
            operation->blob.resize(operation->backing->length);
            submit_page_read(operation, operation->backing, operation->blob.data(), operation->blob.size(),
                             std::holds_alternative<InnerReadComplete>(operation->complete)
                                 ? &InnerLoad::read_complete
                                 : &page_read_completed<InnerLoad>);
        }
    }
    catch (const std::bad_alloc &) {
        if (operation) {
            operation->finish(Status::resource_exhausted("index load allocation failed"));
        }
        else {

            reject(Status::resource_exhausted("index load allocation failed"));
        }
    }
    catch (...) {
        if (operation) {
            operation->finish(Status::io_error("index read submission threw"));
        }
        else {
            reject(Status::io_error("index read setup threw"));
        }
    }
}
} // namespace crowdb::tree
