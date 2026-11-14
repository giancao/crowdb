// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "crowdb-tree/memtable/skip_list.h"

#include <array>
#include <bit>
#include <cstddef>

namespace crowdb::tree
{
namespace
{
// Reserve the variable-size key node at the end of the shared cell allocation.
// The key keeps a weak owner, so retiring the cell still releases its payload.
template <class T> class CellWithKeyAllocator
{
    template <class> friend class CellWithKeyAllocator;

  public:
    using value_type = T;

    CellWithKeyAllocator(const std::shared_ptr<AllocationBudget> &budget, size_t key_bytes, void **key_storage) noexcept
        : storage_(budget),
          key_bytes_(key_bytes),
          key_storage_address_(std::bit_cast<decltype(key_storage_address_)>(key_storage))
    {
    }

    template <class U>
    CellWithKeyAllocator(const CellWithKeyAllocator<U> &other) noexcept
        : storage_(other.storage_),
          key_bytes_(other.key_bytes_),
          key_storage_address_(other.key_storage_address_)
    {
    }

    [[nodiscard]] T *allocate(size_t count)
    {
        static_assert(alignof(T) <= alignof(std::max_align_t));
        static_assert(alignof(Node) <= alignof(std::max_align_t));
        const auto bytes = detail::AllocationBound()
                               .add_product(count, sizeof(T))
                               .add(alignof(Node) - 1)
                               .add(key_bytes_)
                               .add(sizeof(std::max_align_t) - 1)
                               .value();
        if (!bytes) {
            throw std::bad_alloc();
        }
        auto *memory = storage_.allocate(units(count));
        // The allocator can outlive the output variable. Recover its address
        // only here, during the call to allocate_shared.
        *std::bit_cast<void **>(key_storage_address_) = reinterpret_cast<std::byte *>(memory) + key_offset(count);
        return reinterpret_cast<T *>(memory);
    }

    void deallocate(T *memory, size_t count) noexcept
    {
        storage_.deallocate(reinterpret_cast<std::max_align_t *>(memory), units(count));
    }

    template <class U> bool operator==(const CellWithKeyAllocator<U> &other) const noexcept
    {
        return storage_ == other.storage_ && key_bytes_ == other.key_bytes_;
    }

  private:
    static size_t key_offset(size_t count) noexcept
    {
        const auto bytes = count * sizeof(T);
        return (bytes + alignof(Node) - 1) / alignof(Node) * alignof(Node);
    }

    [[nodiscard]] size_t units(size_t count) const noexcept
    {
        return (key_offset(count) + key_bytes_ + sizeof(std::max_align_t) - 1) / sizeof(std::max_align_t);
    }

    MetadataAllocator<std::max_align_t>    storage_;
    size_t                                 key_bytes_;
    std::array<std::byte, sizeof(void **)> key_storage_address_;
};
} // namespace

void CellVersion::Deleter::operator()(CellVersion *version) const noexcept
{
    version->destroy(version);
}

CellVersion::Owner CellVersion::create(buffer &&cell, uint64_t slot, uint8_t flags, AllocationLease payload,
                                       const std::shared_ptr<AllocationBudget>  &budget,
                                       const std::shared_ptr<AllocationCounter> &counter, size_t key_bytes,
                                       void **key_storage)
{
    auto stored =
        key_bytes == 0
            ? std::allocate_shared<CellVersion>(MetadataAllocator<CellVersion>(budget), std::move(cell), slot, flags,
                                                counter, std::move(payload))
            : std::allocate_shared<CellVersion>(CellWithKeyAllocator<CellVersion>(budget, key_bytes, key_storage),
                                                std::move(cell), slot, flags, counter, std::move(payload));
    auto *version    = stored.get();
    version->destroy = [](EpochManager::Deferred *entry) noexcept {
        // Move the owner's reference out before it can destroy this allocation.
        auto owner = std::move(static_cast<CellVersion *>(entry)->value_owner);
    };
    version->value_owner = std::move(stored);
    return Owner(version);
}

CellVersion::Owner CellVersion::reference(std::shared_ptr<const CellVersion>        cell,
                                          const std::shared_ptr<AllocationBudget>  &budget,
                                          const std::shared_ptr<AllocationCounter> &counter)
{
    AllocationLease credit(budget, sizeof(CellVersion), AllocationKind::kMetadata);
    auto            version =
        std::make_unique<CellVersion>(buffer::wrap(cell->cell.data(), cell->cell.size()), cell->slot, cell->flags);
    version->credit      = std::move(credit);
    version->value_owner = std::move(cell);
    version->allocation.set(counter, sizeof(CellVersion));
    version->destroy = [](EpochManager::Deferred *entry) noexcept {
        auto *version = static_cast<CellVersion *>(entry);
        auto  credit  = std::move(version->credit);
        delete version;
    };
    return Owner(version.release());
}

size_t CellVersion::allocation_bound() noexcept
{
    return detail::shared_allocation_bound<CellVersion>() + alignof(Node) - 1 + sizeof(std::max_align_t) - 1;
}
} // namespace crowdb::tree
