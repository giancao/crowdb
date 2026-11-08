// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "snapshot/chunk_root_codec_internal.h"
#include "snapshot/index_summary_codec.h"

namespace crowdb::tree
{
namespace
{
bool valid_identity(const ChunkRootIdentity &id, uint64_t frames)
{
    return id.tree != 0 && id.root_generation != 0 && id.owner_epoch != 0 && id.snapshot_sequence != 0 &&
           id.page_generation != 0 && id.gc_floor <= id.coverage && id.next_page_id <= MappingTable::kPageLimit &&
           ((frames == 0 && id.root_page_id == kInvalidPageId) || (frames != 0 && id.root_page_id < id.next_page_id));
}

bool valid_indexes(const ChunkRootManifest &root)
{
    uint64_t count    = 0;
    uint64_t previous = 0;
    for (const auto &entry : root.indexes) {
        if (entry.frame_count == 0 || entry.frame_count > kChunkMappingSegmentEntryLimit ||
            entry.first_page > entry.last_page || entry.last_page >= root.identity.next_page_id ||
            (count != 0 && entry.first_page <= previous) ||
            !chunk_codec::valid_pack(entry.pack, PagePurpose::kPageIndex) ||
            entry.pack.length > kChunkMappingSegmentWindowBytes || count > UINT64_MAX - entry.frame_count) {
            return false;
        }
        count += entry.frame_count;
        previous = entry.last_page;
    }
    return count == root.frame_count &&
           (!root.summaries || (chunk_codec::valid_pack(*root.summaries, PagePurpose::kPageIndex) &&
                                root.summaries->length % snapshot::kIndexSummaryRecordBytes == 0 &&
                                root.summaries->length / snapshot::kIndexSummaryRecordBytes < root.frame_count)) &&
           root.stored_references <= detail::kMaxCandidateReferences &&
           root.dependency_entries <= detail::kMaxCandidateReferences &&
           root.dependency_entries >= root.stored_references &&
           (root.stored_references != 0 || root.dependency_entries == 0);
}
} // namespace

Status encode_chunk_root(const ChunkRootManifest &root, ChunkMetadataBytes *out)
try {
    using namespace chunk_codec;
    if (out == nullptr || !out->empty() || !valid_identity(root.identity, root.frame_count) || !valid_indexes(root)) {
        return Status::invalid_argument("invalid Chunk root identity or directory");
    }
    const auto size = detail::AllocationBound(kChunkRootHeaderBytes + 4)
                          .add_product(root.indexes.size(), kChunkRootEntryBytes)
                          .value();
    if (!size || *size > detail::kMaxCandidateManifestBytes) {
        return Status::resource_exhausted();
    }
    out->resize(*size);
    auto           bytes = std::span(*out);
    const auto    &id    = root.identity;
    const uint64_t words[]{kRootMagic,
                           2,
                           id.tree,
                           id.root_generation,
                           id.owner_epoch,
                           id.snapshot_sequence,
                           id.page_generation,
                           id.coverage,
                           id.gc_floor,
                           id.root_page_id,
                           id.next_page_id,
                           id.wal_replay_offset,
                           root.frame_count,
                           root.indexes.size(),
                           root.stored_references,
                           root.dependency_entries};
    for (size_t i = 0; i < std::size(words); ++i) {
        put(bytes, i, words[i]);
    }
    if (root.root_summary) {
        snapshot::encode_index_summary(bytes.data() + 128, 1, *root.root_summary);
    }
    if (root.summaries) {
        pack(bytes.subspan(160, 72), *root.summaries);
    }
    size_t offset = kChunkRootHeaderBytes;
    for (const auto &entry : root.indexes) {
        auto encoded = bytes.subspan(offset, kChunkRootEntryBytes);
        put(encoded, 0, entry.first_page);
        put(encoded, 1, entry.last_page);
        put(encoded, 2, entry.frame_count);
        pack(encoded.subspan(24), entry.pack);
        offset += kChunkRootEntryBytes;
    }
    seal(bytes);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status decode_chunk_root(std::span<const uint8_t> bytes, uint64_t tree, uint64_t exact_generation,
                         ChunkRootManifest *out)
try {
    using namespace chunk_codec;
    if (out == nullptr || !out->indexes.empty() || tree == 0) {
        return Status::invalid_argument();
    }
    if (bytes.size() > detail::kMaxCandidateManifestBytes || !verified(bytes, kChunkRootHeaderBytes) ||
        word(bytes, 0) != kRootMagic || word(bytes, 1) != 2 || word(bytes, 2) != tree ||
        (exact_generation != 0 && word(bytes, 3) != exact_generation)) {
        return Status::corruption("Chunk root checksum, format or identity mismatch");
    }
    const auto count = word(bytes, 13);
    if (count > (bytes.size() - kChunkRootHeaderBytes - 4) / kChunkRootEntryBytes ||
        bytes.size() != kChunkRootHeaderBytes + 4 + (count * kChunkRootEntryBytes)) {
        return Status::corruption("Chunk root directory length mismatch");
    }
    out->identity           = {.tree              = tree,
                               .root_generation   = word(bytes, 3),
                               .owner_epoch       = word(bytes, 4),
                               .snapshot_sequence = word(bytes, 5),
                               .page_generation   = word(bytes, 6),
                               .coverage          = word(bytes, 7),
                               .gc_floor          = word(bytes, 8),
                               .root_page_id      = word(bytes, 9),
                               .next_page_id      = word(bytes, 10),
                               .wal_replay_offset = word(bytes, 11)};
    out->frame_count        = word(bytes, 12);
    out->stored_references  = word(bytes, 14);
    out->dependency_entries = word(bytes, 15);
    uint32_t     summary_present;
    IndexSummary summary;
    if (!snapshot::decode_index_summary(bytes.data() + 128, &summary_present, &summary) || summary_present > 1 ||
        (summary_present == 0 && summary.exact)) {
        return Status::corruption("invalid Chunk root summary");
    }
    if (summary_present != 0) {
        out->root_summary = summary;
    }
    const auto encoded_summaries = bytes.subspan(160, 72);
    if (std::ranges::any_of(encoded_summaries, [](auto byte) { return byte != 0; })) {
        detail::ChunkFixedPackRef ref;
        if (!unpack(encoded_summaries, PagePurpose::kPageIndex, &ref)) {
            return Status::corruption("invalid Chunk index summary reference");
        }
        out->summaries = ref;
    }
    out->indexes.reserve(count);
    for (uint64_t i = 0; i < count; ++i) {
        const auto encoded = bytes.subspan(kChunkRootHeaderBytes + (i * kChunkRootEntryBytes), kChunkRootEntryBytes);
        ChunkMappingSegmentRef entry{
            .first_page = word(encoded, 0), .last_page = word(encoded, 1), .frame_count = word(encoded, 2), .pack = {}};
        if (!unpack(encoded.subspan(24), PagePurpose::kPageIndex, &entry.pack)) {
            return Status::corruption("Chunk root index pack identity mismatch");
        }
        out->indexes.push_back(entry);
    }
    return valid_identity(out->identity, out->frame_count) && valid_indexes(*out)
             ? Status::Ok()
             : Status::corruption("Chunk root directory is unordered or incomplete");
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
} // namespace crowdb::tree
