// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "apply_frontier.h"
#include "crowdb-tree/btree/tree.h"
#include "crowdb-tree/external_allocation.h"
#include "inline_function.h"
#include "runtime/tree_operation_lifetime.h"
#include "source_admission.h"

namespace crowdb::tree
{
class ApplyProgress;
class SourceRotation;

struct CoreWriteInput
{
    Slice   key;
    Slice   value;
    buffer *movable = nullptr;
    uint8_t flags   = 0;
    bool    encoded = false;
};

struct ExternalValue
{
    Slice  initialized;
    size_t capacity = 0;

    [[nodiscard]] bool contains(Slice bytes) const noexcept
    {
        const auto base  = reinterpret_cast<uintptr_t>(initialized.data());
        const auto start = reinterpret_cast<uintptr_t>(bytes.data());
        return bytes.empty() || (base != 0 && start >= base && start - base <= initialized.size() &&
                                 bytes.size() <= initialized.size() - (start - base));
    }
};

struct ApplyInputOwner
{
    ApplyInputOwner(void (*release)(void *), void *argument) : drop(release), context(argument)
    {
    }

    ApplyInputOwner(ApplyInputOwner &&other) noexcept
        : control(std::move(other.control)),
          drop(std::exchange(other.drop, nullptr)),
          context(other.context)
    {
    }

    ~ApplyInputOwner()
    {
        if (drop != nullptr) {
            drop(context);
        }
    }

    ExternalAllocation::Owner take() noexcept
    {
        control.reset();
        return {context, std::exchange(drop, nullptr)};
    }

    AllocationLease control;
    void (*drop)(void *);
    void *context;
};

using CoreWriteReader = detail::InlineFunction<CoreWriteInput(size_t)>;

// Validate both the primary leaf cell and a possible inner separator before
// admitting an input whose configured key/inline limits may exceed a frame.
[[nodiscard]] bool primary_write_fits(const Config &config, size_t key_bytes, size_t value_bytes,
                                      bool tombstone) noexcept;

Status apply_source_batch(const Config &config, SourceAdmission &sources, ApplyFrontier &frontier,
                          TreeOperationLifetime &lifetime, size_t max_value_bytes,
                          std::optional<uint64_t> supplied_slot, std::atomic<uint64_t> &automatic, size_t count,
                          const CoreWriteReader &read, ApplyProgress *progress = nullptr,
                          SourceRotation *rotation = nullptr);
} // namespace crowdb::tree
