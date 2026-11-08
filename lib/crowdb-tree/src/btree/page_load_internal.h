// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "btree/page_load.h"
#include "crowdb-common/log.h"
#include "page_store/chunk/chunk_fixed_pack.h"

#include <variant>

namespace crowdb::tree
{
using LeafCompletion  = std::variant<PageLoader::LeafComplete, LeafReadComplete>;
using InnerCompletion = std::variant<PageLoader::InnerComplete, InnerReadComplete>;

struct AsyncStorePageLoader::State
{
    void                  start_leaf(BaseRef base, LeafCompletion complete, const std::shared_ptr<State> &owner,
                                     BaseRef *decode_target = nullptr);
    [[nodiscard]] Status  validate_leaf(const BaseRef &base) const;
    void                  start_inner(std::shared_ptr<const PageBlockRef> page_block, InnerCompletion complete,
                                      const std::shared_ptr<State> &owner);
    [[nodiscard]] Status  validate_inner(const std::shared_ptr<const PageBlockRef> &page_block) const;
    std::optional<Status> try_read_inner_inline(const std::shared_ptr<const PageBlockRef> &page_block,
                                                std::shared_ptr<const InnerPageState>     *out);

    std::shared_ptr<AsyncPageStore>             store;
    std::shared_ptr<detail::FlushRuntime>       runtime;
    std::shared_ptr<detail::FlushRuntime::Lane> lane;
    PageLoadLimits                              limits;
    OverflowPageBlockResolver                   overflow;
    PageLoadBudget                              budget;
};

inline std::optional<size_t> chunk_load_window_bound(const PageLoadLimits &limits) noexcept
{
    return limits.max_chunk_blob_bytes == 0
             ? std::optional<size_t>{0}
             : detail::ChunkFixedPackStore::read_into_allocation_bound(limits.max_chunk_blob_bytes);
}

inline bool chunk_load_allowed(const PageBlockRef &page_block, const PageLoadLimits &limits) noexcept
{
    return page_block.storage != PageBlockStorage::kChunkPack || page_block.length <= limits.max_chunk_blob_bytes;
}

using LoadBytes = OverwriteByteVector<AllocationKind::kScratch>;

Status decode_page_block_frame(std::span<const uint8_t> blob, const PageBlockRef &page_block,
                               const PageLoadLimits &limits, LoadBytes &scratch, std::span<const uint8_t> *out);
Status decode_page_block_frame(std::span<const uint8_t> blob, const PageBlockRef &page_block,
                               const PageLoadLimits &limits, FrameStore::Bytes &scratch, std::span<const uint8_t> *out);

[[nodiscard]] std::optional<size_t> decoded_leaf_allocation_bound(const PageLoadLimits &limits) noexcept;
[[nodiscard]] std::optional<size_t> overflow_read_allocation_bound(const PageLoadLimits &limits, size_t workers,
                                                                   size_t retained_values, size_t value_bytes) noexcept;

// The charged storage supplies direct-I/O alignment itself, so a Block read
// does not allocate an unaccounted second copy in the page store adapter.
template <class Bytes> class AlignedPageLoadBytes
{
  public:
    static constexpr size_t kAlignment = 4096;

    explicit AlignedPageLoadBytes(detail::ReservationAllocator<uint8_t> allocator)
        : storage_(typename Bytes::allocator_type(allocator.owner()))
    {
    }

    void resize(size_t length)
    {
        if (length > SIZE_MAX - (kAlignment - 1)) {
            throw std::bad_alloc();
        }
        const auto required = length + kAlignment - 1;
        if (required > storage_.size()) {
            // Release an old overflow window before growing; vector growth
            // must not retain two windows or geometrically exceed the bound.
            storage_ = decltype(storage_)(storage_.get_allocator());
            storage_.resize(required);
        }
        void *data      = storage_.data();
        auto  available = storage_.size();
        auto *aligned   = std::align(kAlignment, length, data, available);
        bytes_          = {static_cast<uint8_t *>(aligned), length};
    }

    [[nodiscard]] uint8_t *data() noexcept
    {
        return bytes_.data();
    }

    [[nodiscard]] const uint8_t *data() const noexcept
    {
        return bytes_.data();
    }

    [[nodiscard]] size_t size() const noexcept
    {
        return bytes_.size();
    }

    [[nodiscard]] size_t capacity() const noexcept
    {
        return storage_.capacity();
    }

    [[nodiscard]] Bytes &storage() noexcept
    {
        return storage_;
    }

  private:
    Bytes              storage_;
    std::span<uint8_t> bytes_;
};

using AlignedLoadBytes     = AlignedPageLoadBytes<OverwriteByteVector<AllocationKind::kIo>>;
using AlignedLeafLoadBytes = AlignedPageLoadBytes<FrameStore::Bytes>;

template <class Callback, class... Results>
void complete_page_load(Callback &callback, Status status, Results... results) noexcept
{
    try {
        callback(std::move(status), std::move(results)...);
    }
    catch (...) {
        CRB_LOG_ERROR("page load receiver threw after terminal");
    }
}

} // namespace crowdb::tree
