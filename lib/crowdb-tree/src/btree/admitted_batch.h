// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "tree_core_batch.h"

#include <span>

namespace crowdb::tree
{
// Owns one validated, deduplicated request before consensus assigns its slot.
// This owner has no source entrance or epoch guard. Incoming bytes and commit
// descriptors are charged separately from the prepaid source allocation.
class AdmittedBatch
{
  public:
    static constexpr size_t kMaxPlanningEntries = 4096;

    struct Plan
    {
        size_t                       staging_bytes = 0;
        size_t                       source_bytes  = 0;
        WriteFootprint               footprint;
        std::optional<ExternalValue> external;
    };

    // Derive the bound and place the selected indices at the start of scratch.
    static Status plan(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes, size_t count,
                       const CoreWriteReader &read, std::span<size_t> scratch, Plan *out,
                       const ExternalValue *external = nullptr);
    static Status bind_prepaid(const SourceWriteOptions &options, size_t count, const CoreWriteReader &read,
                               detail::BudgetClass category, const Plan &plan, detail::ResourceBudget::Credit staging,
                               detail::ResourceBudget::Credit source, std::shared_ptr<AdmittedBatch> *out,
                               std::span<const size_t> selected = {}, ApplyInputOwner *owner = nullptr);
    static Status create(const Config &config, const SourceWriteOptions &options, size_t max_value_bytes, size_t count,
                         const CoreWriteReader &read, detail::BudgetClass category,
                         std::shared_ptr<AdmittedBatch> *out);

    // A busy source or distant frontier leaves all bytes and credit intact.
    // Once a source is bound, this batch is consumed exactly once. The caller
    // retains the generation and supplies asynchronous progress on busy.
    Status try_commit(SourceAdmission &sources, ApplyFrontier &frontier, uint64_t slot);

    [[nodiscard]] bool consumed() const noexcept
    {
        return consumed_;
    }

    [[nodiscard]] size_t size() const noexcept
    {
        return cells_.size();
    }

    [[nodiscard]] WriteFootprint footprint() const noexcept
    {
        return source_.footprint();
    }

  private:
    struct Cell
    {
        AllocationLease charge;
        buffer          value;
        size_t          key_offset = 0;
        size_t          key_size   = 0;
        uint8_t         flags      = 0;
    };

    explicit AdmittedBatch(std::shared_ptr<detail::AllocationReservation> metadata);
    Status initialize(const SourceWriteOptions &options, size_t count, const CoreWriteReader &read,
                      detail::BudgetClass category, SourceWriteReservation prepaid = {});
    void   initialize_cells(std::span<const size_t> order, const CoreWriteReader &read);
    void   initialize_owned_cell(const CoreWriteInput &input, const ExternalValue &external,
                                 ExternalAllocation::Owner owner);
    Status prepare_cells(SourceAdmission::Writer &writer);

    std::shared_ptr<detail::AllocationReservation>             metadata_;
    SourceWriteReservation                                     source_;
    detail::ReservedVector<uint8_t, AllocationKind::kMetadata> keys_;
    detail::ReservedVector<Cell>                               cells_;
    detail::ReservedVector<SourceAdmission::Writer::Prepared>  prepared_;
    bool                                                       consumed_ = false;
};
} // namespace crowdb::tree
