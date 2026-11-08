// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/page_load_leaf.h"

#include "btree/allocation_bound.h"
#include "btree/page_load_internal.h"
#include "crowdb-tree/maptable/compressor.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
struct LeafBaseLoads
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        LeafBaseLoads *owner;

        [[nodiscard]] auto get_env() const noexcept
            -> stdexec::prop<stdexec::get_start_scheduler_t, stdexec::inline_scheduler>
        {
            return {stdexec::get_start_scheduler, stdexec::inline_scheduler{}};
        }

        void set_value() noexcept
        {
            auto keep = std::move(owner->lifetime);
            auto done = std::move(owner->done);
            if (!owner->failure.ok()) {
                complete_page_load(done, std::move(owner->failure), LeafDecode{});
                return;
            }
            complete_page_load(done, Status::Ok(), LeafDecode([keep]() mutable {
                                   auto loads = std::move(keep);
                                   for (auto &job : loads->jobs) {
                                       if (job.decode) {
                                           auto result = job.decode();
                                           if (!result.ok()) {
                                               return result;
                                           }
                                       }
                                   }
                                   return Status::Ok();
                               }));
        }
    };

    struct Job
    {
        LeafBaseLoads *owner;
        PageLoader    *loader;
        BaseRef       *base;
        LeafDecode     decode;

        static void submit(void *raw, detail::CallbackValueSender::Complete done, void *context) noexcept
        {
            auto &job = *static_cast<Job *>(raw);
            try {
                job.loader->read_leaf(*job.base, [&job, done, context](Status result, LeafDecode decode) noexcept {
                    if (!result.ok()) {
                        job.owner->fail(std::move(result));
                    }
                    job.decode = std::move(decode);
                    done(context);
                });
            }
            catch (const std::bad_alloc &) {
                job.owner->fail(Status::resource_exhausted());
                done(context);
            }
            catch (...) {
                job.owner->fail(Status::internal_error());
                done(context);
            }
        }
    };

    using Join = stdexec::connect_result_t<decltype(std::declval<stdexec::simple_counting_scope &>().join()), Receiver>;

    LeafBaseLoads(const std::shared_ptr<AllocationBudget> &metadata, LeafReadComplete complete)
        : jobs(detail::ReservationAllocator<Job>(metadata)),
          done(std::move(complete)),
          joined(stdexec::connect(scope.join(), Receiver{this}))
    {
    }

    void fail(Status status) noexcept
    {
        if (!failed.exchange(true, std::memory_order_relaxed)) {
            failure = std::move(status);
        }
    }

    detail::ReservedVector<Job>    jobs;
    LeafReadComplete               done;
    stdexec::simple_counting_scope scope;
    Join                           joined;
    std::shared_ptr<LeafBaseLoads> lifetime;
    std::atomic<bool>              failed{false};
    Status                         failure;
};

struct LoadedLeaf final : LeafBase
{
    LoadedLeaf(FrameStore::Bytes frame, size_t offset, uint32_t length, std::shared_ptr<const PageBlockRef> source)
        : LeafBase(std::move(frame), offset, length),
          page_block(std::move(source))
    {
    }

    std::shared_ptr<const PageBlockRef> page_block;
};

Status check_leaf_window(std::span<const uint8_t> frame, const PageLoadLimits &limits)
{
    const auto    *data  = frame.data();
    const uint32_t count = frame_u32(data, fh::kSlotCount);
    const size_t   body  = frame.size() - kFrameTrailerSize;
    if (count > (body - kFrameHeaderSize) / kLeafSlotSize) {
        return Status::corruption("loaded leaf slot directories exceed frame bounds");
    }
    if (count > limits.max_leaf_entries) {
        return Status::resource_exhausted("loaded leaf exceeds entry budget");
    }
    size_t key_bytes = 0;
    for (uint32_t i = 0; i < count; ++i) {
        const auto     offset = kFrameHeaderSize + (i * kLeafSlotSize);
        const uint32_t size   = frame_u32(data + offset, 4);
        if (size > limits.max_leaf_key_bytes - key_bytes) {
            return Status::resource_exhausted("loaded leaf exceeds key byte budget");
        }
        key_bytes += size;
    }
    return Status::Ok();
}

Status check_cells(const LeafFrameView &view, const std::optional<uint64_t> &max_slot)
{
    for (uint32_t i = 0; i < view.count(); ++i) {
        const auto     raw = view.cell(i);
        const CellView cell(raw);
        if (!cell.valid() || (cell.flags() != 0 && cell.flags() != kFlagTombstone && cell.flags() != kFlagOverflow) ||
            (cell.is_tombstone() && raw.size() != kCellHeaderSize) ||
            (cell.is_overflow() &&
             (raw.size() != kOverflowCellSize || cell.overflow_head() == kInvalidPageId || cell.overflow_len() == 0))) {
            return Status::corruption("loaded leaf contains an invalid cell");
        }
        if (max_slot && cell.slot() > *max_slot) {
            return Status::corruption("loaded leaf exceeds its proven slot bound");
        }
    }
    return Status::Ok();
}

} // namespace

std::optional<Status> read_leaf_bases(PageLoader &loader, PageState &leaf,
                                      const std::shared_ptr<AllocationBudget> &metadata, LeafReadComplete complete)
{
    if (auto *store_loader = dynamic_cast<AsyncStorePageLoader *>(&loader)) {
        if (auto result = store_loader->try_load_leaf_bases(leaf)) {
            return result;
        }
    }
    auto first = std::ranges::find_if(leaf.bases, [](const BaseRef &base) { return !base.resident; });
    if (first == leaf.bases.end()) {
        return Status::Ok();
    }
    if (std::none_of(first + 1, leaf.bases.end(), [](const BaseRef &base) { return !base.resident; })) {
        loader.read_leaf(*first, std::move(complete));
        return std::nullopt;
    }
    auto loads = detail::reserved_shared<LeafBaseLoads>(metadata, metadata, std::move(complete));
    for (auto it = first; it != leaf.bases.end(); ++it) {
        if (!it->resident) {
            loads->jobs.push_back({.owner = loads.get(), .loader = &loader, .base = &*it, .decode = {}});
        }
    }
    loads->lifetime = loads;
    try {
        for (auto &job : loads->jobs) {
            if (loads->failed.load(std::memory_order_relaxed)) {
                break;
            }
            stdexec::spawn(detail::CallbackValueSender(&job, &LeafBaseLoads::Job::submit), loads->scope.get_token(),
                           stdexec::env{
                               stdexec::prop{stdexec::get_allocator, MetadataAllocator<std::byte>(metadata)}
            });
        }
    }
    catch (const std::bad_alloc &) {
        loads->fail(Status::resource_exhausted());
    }
    catch (...) {
        loads->fail(Status::internal_error());
    }
    loads->scope.close();
    stdexec::start(loads->joined);
    return std::nullopt;
}

std::optional<size_t> decoded_leaf_allocation_bound(const PageLoadLimits &limits) noexcept
{
    const auto raw = detail::AllocationBound(limits.max_blob_bytes).add(AlignedLeafLoadBytes::kAlignment - 1).value();
    return raw ? detail::AllocationBound(detail::shared_allocation_bound<LoadedLeaf>())
                     .add(std::max<size_t>(limits.max_frame_bytes, *raw))
                     .value()
               : std::nullopt;
}

Status decode_loaded_leaf(FrameStore::Bytes &input, std::span<const uint8_t> blob, const BaseRef &base,
                          const PageLoadLimits &limits, const std::shared_ptr<AllocationBudget> &metadata,
                          std::shared_ptr<const LeafBase> *out)
{
    FrameStore::Bytes        decoded{FrameStore::Bytes::allocator_type(metadata)};
    std::span<const uint8_t> frame;
    auto                     status = decode_page_block_frame(blob, *base.page_block, limits, decoded, &frame);
    const auto               length = static_cast<uint32_t>(frame.size());
    if (!status.ok()) {
        return status;
    }
    if (frame_page_type(frame.data()) != page_type::kLeafBase) {
        return Status::corruption("loaded page_block is not a leaf frame");
    }
    status = check_leaf_window(frame, limits);
    if (!status.ok()) {
        return status;
    }
    if (!frame_validate(frame.data(), length) || frame_page_type(frame.data()) != page_type::kLeafBase) {
        return Status::corruption("loaded page_block is not a valid leaf frame");
    }
    status = check_cells(LeafFrameView(frame.data(), length), base.max_slot);
    if (!status.ok()) {
        return status;
    }
    if (decoded.empty()) {
        if (base.page_block->storage != PageBlockStorage::kResidentFrame) {
            const auto offset = static_cast<size_t>(frame.data() - input.data());
            *out = detail::reserved_shared<LoadedLeaf>(metadata, std::move(input), offset, length, base.page_block);
            return Status::Ok();
        }
        decoded.assign(frame.begin(), frame.end());
        record_work(metadata, TreeWorkCounter::MaterializeCopyBytes, length);
    }
    *out = detail::reserved_shared<LoadedLeaf>(metadata, std::move(decoded), size_t{0}, length, base.page_block);
    return Status::Ok();
}
} // namespace crowdb::tree
