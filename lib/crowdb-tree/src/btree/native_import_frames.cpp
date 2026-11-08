// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "native_import.h"
#include "overflow_page_block.h"
#include "snapshot/stored_overflow.h"

namespace crowdb::tree
{
namespace
{
struct ResidentImportFrame
{
    ResidentImportFrame(std::span<const uint8_t> input, const std::shared_ptr<detail::AllocationReservation> &metadata)
        : bytes(input.begin(), input.end(), detail::ReservationAllocator<uint8_t, AllocationKind::kFrame>(metadata))
    {
    }

    detail::ReservedVector<uint8_t, AllocationKind::kFrame> bytes;
};

Status validate_import_cell(Slice raw, uint64_t coverage, const PageLoadLimits &limits)
{
    const CellView cell(raw);
    if (!cell.valid() || cell.slot() > coverage ||
        (cell.flags() != 0 && cell.flags() != kFlagTombstone && cell.flags() != kFlagOverflow) ||
        (cell.is_tombstone() && raw.size() != kCellHeaderSize) ||
        (cell.is_overflow() &&
         (raw.size() != kOverflowCellSize || cell.overflow_head() == kInvalidPageId || cell.overflow_len() == 0))) {
        return Status::corruption("native import has an invalid cell or coverage");
    }
    const auto length = cell.is_overflow() ? cell.overflow_len() : cell.value().size();
    return length <= limits.max_value_bytes ? Status::Ok()
                                            : Status::resource_exhausted("native import value exceeds read limit");
}

Status inspect_import_leaf(const ResidentImportFrame &owned, uint64_t coverage, const PageLoadLimits &limits,
                           const std::shared_ptr<detail::AllocationReservation> &metadata,
                           ReadablePageFrame                                    *descriptor)
{
    const LeafFrameView leaf(owned.bytes.data(), static_cast<uint32_t>(owned.bytes.size()));
    if (leaf.count() > limits.max_leaf_entries) {
        return Status::resource_exhausted("native import leaf exceeds entry limit");
    }
    for (uint32_t i = 0; i < leaf.count(); ++i) {
        const auto cell   = leaf.cell(i);
        auto       status = validate_import_cell(cell, coverage, limits);
        if (!status.ok()) {
            return status;
        }
    }
    return capture_stored_overflow(leaf, limits.max_leaf_entries, metadata, &descriptor->stored_overflow);
}
} // namespace

std::optional<size_t> native_import_allocation_bound(const NativeImportShape &shape) noexcept
{
    using detail::shared_allocation_bound;
    if (shape.frames > NativeImportLimits{}.max_frames || shape.primary_pages > shape.frames ||
        shape.leaf_pages > shape.primary_pages || shape.dependency_steps > NativeImportLimits{}.max_dependency_steps) {
        return {};
    }
    const auto frame = detail::AllocationBound(shared_allocation_bound<ResidentImportFrame>())
                           .add(shape.max_frame_bytes)
                           .add(shared_allocation_bound<PageBlockRef>())
                           .add(shared_allocation_bound<ReadablePageCatalog::Entry>())
                           .value();
    if (!frame) {
        return {};
    }
    return detail::AllocationBound(shared_allocation_bound<ReadablePageCatalog>())
        .add_product(shape.frames, *frame)
        .add_product(shape.leaf_pages, shared_allocation_bound<StoredOverflowReferences>() +
                                           shared_allocation_bound<PageBlockRef>() +
                                           shared_allocation_bound<OverflowPageBlockSet>())
        .add_product(shape.max_leaf_entries, sizeof(Slice))
        .add_product(shape.stored_references, sizeof(StoredOverflowReference))
        .add_product(shape.dependency_steps, shared_allocation_bound<OverflowPageBlockSet::Entry>())
        .value();
}

PageLoadLimits resident_import_load_limits(PageLoadLimits limits, size_t frame_bytes) noexcept
{
    limits.max_frame_bytes      = std::min(limits.max_frame_bytes, frame_bytes);
    limits.max_blob_bytes       = limits.max_frame_bytes + kDurableBlobHeader;
    limits.max_chunk_blob_bytes = 0;
    limits.max_inner_children   = std::min(limits.max_inner_children, frame_bytes / sizeof(uint64_t));
    limits.max_inner_key_bytes  = std::min(limits.max_inner_key_bytes, frame_bytes);
    limits.max_leaf_entries     = std::min(limits.max_leaf_entries, frame_bytes / kLeafSlotSize);
    limits.max_leaf_key_bytes   = std::min(limits.max_leaf_key_bytes, frame_bytes);
    return limits;
}

Status append_native_frame(ReadablePageCatalog &catalog, uint64_t pid, std::span<const uint8_t> frame,
                           const Config &config, const PageLoadLimits &loads, NativeImportLimits limits,
                           const std::shared_ptr<detail::AllocationReservation> &metadata)
{
    if (catalog.frames.size() == limits.max_frames) {
        return Status::resource_exhausted("native import frame limit exceeded");
    }
    if (pid >= MappingTable::kPageLimit || frame.size() > loads.max_frame_bytes || frame.size() > UINT32_MAX) {
        return Status::resource_exhausted("native import PID or frame exceeds configured limits");
    }
    const auto bytes = static_cast<uint32_t>(frame.size());
    if (!frame_validate_key_range(frame.data(), bytes, config.key_range)) {
        return Status::corruption("native import frame CRC, structure, or key range is invalid");
    }
    if (frame_u64(frame.data(), fh::kSelfpage_id) != pid) {
        return Status::corruption("native import frame identity differs from its descriptor");
    }
    auto owned                 = detail::reserved_shared<ResidentImportFrame>(metadata, frame, metadata);
    auto page_block            = detail::reserved_shared<PageBlockRef>(metadata);
    page_block->content_id     = pid + 1;
    page_block->generation     = catalog.generation;
    page_block->storage        = PageBlockStorage::kResidentFrame;
    page_block->durability     = PageBlockDurability::kResident;
    page_block->resident_frame = owned->bytes;
    page_block->owner          = owned;
    ReadablePageFrame descriptor{.page_id       = pid,
                                 .type          = frame_page_type(owned->bytes.data()),
                                 .frame_bytes   = bytes,
                                 .logical_bytes = bytes,
                                 .page_block    = page_block};
    if (descriptor.type == page_type::kLeafBase) {
        auto status = inspect_import_leaf(*owned, catalog.coverage, loads, metadata, &descriptor);
        if (!status.ok()) {
            return status;
        }
    }
    else if (descriptor.type == page_type::kOverflowFrame) {
        const OverflowFrameView overflow(owned->bytes.data(), bytes);
        descriptor.next_overflow  = overflow.next_page_id();
        descriptor.overflow_bytes = overflow.chunk_len();
        if (descriptor.overflow_bytes == 0) {
            return Status::corruption("native import contains an empty overflow frame");
        }
    }
    else if (descriptor.type != page_type::kInnerBase) {
        return Status::corruption("native import contains an unsupported frame");
    }
    if (!catalog.frames.emplace(pid, std::move(descriptor)).second) {
        return Status::corruption("native import contains duplicate PIDs");
    }
    catalog.next_page_id = std::max(catalog.next_page_id, pid + 1);
    return Status::Ok();
}

Status build_native_catalog(std::span<const NativeFrame> frames, NativeStreamInfo info, const Config &config,
                            const PageLoadLimits &loads, NativeImportLimits limits,
                            const std::shared_ptr<detail::AllocationReservation> &metadata,
                            std::shared_ptr<ReadablePageCatalog>                 *out)
{
    if (out == nullptr || *out || frames.size() != info.frame_count || info.next_page_id > MappingTable::kPageLimit) {
        return Status::invalid_argument("native import requires consistent bounded stream metadata");
    }
    if (frames.size() > limits.max_frames) {
        return Status::resource_exhausted("native import frame limit exceeded");
    }
    auto catalog          = detail::reserved_shared<ReadablePageCatalog>(metadata, metadata);
    catalog->root_page_id = info.root_page_id;
    catalog->next_page_id = info.next_page_id;
    catalog->coverage     = info.at_slot;
    catalog->generation   = 1;
    // Resident identities never claim a page store domain. Each page's owner
    // distinguishes separate imports, including equal PIDs in old captures.
    catalog->page_store_id = 0;
    for (const auto &input : frames) {
        const auto status = append_native_frame(*catalog, input.page_id, input.frame, config, loads, limits, metadata);
        if (!status.ok()) {
            return status;
        }
    }
    auto status = attach_native_dependencies(*catalog, limits, metadata);
    if (status.ok()) {
        *out = std::move(catalog);
    }
    return status;
}
} // namespace crowdb::tree
