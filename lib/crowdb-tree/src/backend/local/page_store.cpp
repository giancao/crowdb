// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Backend-neutral in-memory page-store implementation.

#include "crowdb-tree/backend/page_store.h"

#include "memory_backing_test.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <new>
#include <vector>

namespace crowdb::tree
{
#ifdef CROWDB_TREE_TEST_UTIL
namespace
{
thread_local std::optional<size_t> new_pages_before_failure;
}

void set_memory_backing_failure_after_for_tests(std::optional<size_t> pages) noexcept
{
    new_pages_before_failure = pages;
}
#endif

// ── MemPageStore ──────────────────────────────────────────────────

struct MemPageStore::Data
{
    static constexpr size_t kPageBytes = MemPageStore::kBackingPageBytes;
    using Page                         = std::array<uint8_t, kPageBytes>;

    struct Node
    {
        std::array<std::atomic<void *>, 256> children{};
    };

    Node                  root;
    std::atomic<uint64_t> bytes{0};
    std::atomic<size_t>   pages{0};

    ~Data()
    {
        destroy(root, 40);
    }

    static void destroy(Node &node, unsigned shift)
    {
        for (const auto &slot : node.children) {
            auto *child = slot.load(std::memory_order_relaxed);
            if (child == nullptr) {
                continue;
            }
            if (shift == 0) {
                delete static_cast<Page *>(child);
            }
            else {
                auto *next = static_cast<Node *>(child);
                destroy(*next, shift - 8);
                delete next;
            }
        }
    }

    std::atomic<void *> *slot(uint64_t index, bool create)
    {
        if (index >> 48 != 0) {
            return nullptr;
        }
        auto *node = &root;
        for (unsigned shift = 40; shift != 0; shift -= 8) {
            auto &entry = node->children[(index >> shift) & 255];
            auto *child = entry.load(std::memory_order_acquire);
            if (child == nullptr && create) {
                auto fresh = std::make_unique<Node>();
                if (entry.compare_exchange_strong(child, fresh.get(), std::memory_order_acq_rel,
                                                  std::memory_order_acquire)) {
                    child = fresh.release();
                }
            }
            if (child == nullptr) {
                return nullptr;
            }
            node = static_cast<Node *>(child);
        }
        return &node->children[index & 255];
    }

    void prepare(uint64_t off, size_t len)
    {
        using Prepared = std::pair<std::atomic<void *> *, std::unique_ptr<Page>>;
        std::vector<Prepared> prepared;
        const auto            last = (off + len - 1) / kPageBytes;
        for (auto index = off / kPageBytes; index <= last; ++index) {
            auto *entry = slot(index, true);
            if (entry->load(std::memory_order_acquire) == nullptr) {
#ifdef CROWDB_TREE_TEST_UTIL
                if (new_pages_before_failure) {
                    if (*new_pages_before_failure == 0) {
                        throw std::bad_alloc();
                    }
                    --*new_pages_before_failure;
                }
#endif
                prepared.emplace_back(entry, std::make_unique<Page>());
            }
        }
        // Prepare the complete write before publishing any new data page.
        // The caller's range owner excludes GC until this I/O completes.
        for (auto &[entry, page] : prepared) {
            void *empty = nullptr;
            if (entry->compare_exchange_strong(empty, page.get(), std::memory_order_release,
                                               std::memory_order_relaxed)) {
                [[maybe_unused]] auto *published = page.release();
                pages.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    void write(uint64_t off, const uint8_t *buf, size_t len)
    {
        while (len != 0) {
            const auto offset = static_cast<size_t>(off % kPageBytes);
            const auto count  = std::min(len, kPageBytes - offset);
            auto      *page   = static_cast<Page *>(slot(off / kPageBytes, false)->load(std::memory_order_acquire));
            std::memcpy(page->data() + offset, buf, count);
            off += count;
            buf += count;
            len -= count;
        }
        auto known = bytes.load(std::memory_order_relaxed);
        while (known < off &&
               !bytes.compare_exchange_weak(known, off, std::memory_order_release, std::memory_order_relaxed)) {
        }
    }

    void read(uint64_t off, uint8_t *buf, size_t len)
    {
        while (len != 0) {
            const auto  offset = static_cast<size_t>(off % kPageBytes);
            const auto  count  = std::min(len, kPageBytes - offset);
            auto       *entry  = slot(off / kPageBytes, false);
            const auto *page = entry != nullptr ? static_cast<Page *>(entry->load(std::memory_order_acquire)) : nullptr;
            if (page == nullptr) {
                std::memset(buf, 0, count);
            }
            else {
                std::memcpy(buf, page->data() + offset, count);
            }
            off += count;
            buf += count;
            len -= count;
        }
    }
};

MemPageStore::MemPageStore(uint32_t iu_size) : data_(std::make_unique<Data>()), iu_size_(iu_size)
{
}

MemPageStore::~MemPageStore() = default;

Status MemPageStore::write_at(uint64_t off, const uint8_t *buf, size_t len)
try {
    if (len == 0) {
        return Status::Ok();
    }
    if (buf == nullptr || off > std::numeric_limits<uint64_t>::max() - len) {
        return Status::invalid_argument("memory write has a null input or overflowing range");
    }
    // Grow only the written pages. Existing data never moves, and sparse holes
    // do not become physical allocations during checkpoint address growth.
    data_->prepare(off, len);
    data_->write(off, buf, len);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("memory backing page allocation failed");
}

Status MemPageStore::read_at(uint64_t off, uint8_t *buf, size_t len) const
{
    const auto end = data_->bytes.load(std::memory_order_acquire);
    if (off > end || len > end - off) {
        return Status::io_error("MemPageStore: read past end");
    }
    if (len != 0 && buf == nullptr) {
        return Status::invalid_argument("memory read has a null output");
    }
    data_->read(off, buf, len);
    return Status::Ok();
}

uint64_t MemPageStore::size() const
{
    return data_->bytes.load(std::memory_order_acquire);
}

size_t MemPageStore::resident_bytes() const
{
    return data_->pages.load(std::memory_order_relaxed) * Data::kPageBytes;
}

bool MemPageStore::release_backing_page(uint64_t index)
{
    auto *entry = data_->slot(index, false);
    if (entry == nullptr) {
        return false;
    }
    auto *page = static_cast<Data::Page *>(entry->exchange(nullptr, std::memory_order_acq_rel));
    if (page == nullptr) {
        return false;
    }
    // The exclusive whole-page free-range lease excludes all byte users.
    delete page;
    data_->pages.fetch_sub(1, std::memory_order_relaxed);
    return true;
}

} // namespace crowdb::tree
