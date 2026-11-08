// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "logical_page.h"

#include <compare>

namespace crowdb::tree
{
enum class PageCacheKind : uint8_t { Leaf, Inner };

struct PageCacheKey
{
    PageCacheKind           kind            = PageCacheKind::Leaf;
    uint64_t                backend         = 0;
    uint64_t                address         = 0;
    uint64_t                length          = 0;
    uint64_t                content         = 0;
    uint64_t                generation      = 0;
    uint64_t                checksum        = 0;
    uint32_t                encoding        = 0;
    uint64_t                base            = 0;
    uint64_t                base_generation = 0;
    uintptr_t               legacy_owner    = 0;
    PageBlockStorage        storage         = PageBlockStorage::kDurableBlob;
    std::array<uint64_t, 8> chunk{};
    auto                    operator<=>(const PageCacheKey &) const = default;
};

inline PageCacheKey page_cache_key(PageCacheKind kind, const BaseRef &base)
{
    const auto &page_block = *base.page_block;
    return {.kind            = kind,
            .backend         = page_block.page_store_id,
            .address         = page_block.address,
            .length          = page_block.length,
            .content         = page_block.content_id,
            .generation      = page_block.generation,
            .checksum        = page_block.checksum,
            .encoding        = page_block.encoding,
            .base            = kind == PageCacheKind::Leaf ? base.identity : 0,
            .base_generation = kind == PageCacheKind::Leaf ? base.generation : 0,
            .legacy_owner    = page_block.page_store_id == 0 ? reinterpret_cast<uintptr_t>(page_block.owner.get()) : 0,
            .storage         = page_block.storage,
            .chunk = page_block.chunk ? chunk_page_block_identity(*page_block.chunk) : std::array<uint64_t, 8>{}};
}
} // namespace crowdb::tree
