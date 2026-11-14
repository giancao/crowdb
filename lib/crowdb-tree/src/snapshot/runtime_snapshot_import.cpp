// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "runtime_snapshot_import.h"

#include "crowdb-common/log.h"

namespace crowdb::tree
{
Status RuntimeSnapshotImport::create(TreeCore &core, std::shared_ptr<RuntimeSnapshotImport> *out)
try {
    if (out == nullptr || *out) {
        return Status::invalid_argument();
    }
    std::shared_ptr<detail::AllocationReservation> metadata;
    auto                                           status = core.prepare_import_credit(&metadata);
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_shared<RuntimeSnapshotImport>(metadata, core, metadata);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot import control allocation failed");
}

RuntimeSnapshotImport::RuntimeSnapshotImport(TreeCore &core, std::shared_ptr<detail::AllocationReservation> metadata)
    : core_(core.weak_from_this()),
      metadata_(std::move(metadata)),
      loads_(core.load_limits()),
      max_key_(core.config().max_key_size != 0 ? core.config().max_key_size : core.config().frame_bytes / 2),
      key_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata_)),
      window_(detail::ReservationAllocator<uint8_t, AllocationKind::kScratch>(metadata_))
{
}

RuntimeSnapshotImport::~RuntimeSnapshotImport()
{
    if (metadata_) {
        metadata_->seal();
    }
}

Status RuntimeSnapshotImport::fail(Status status)
{
    if (status_.ok()) {
        status_ = std::move(status);
    }
    portable_.reset();
    catalog_.reset();
    key_    = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(key_.get_allocator());
    window_ = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(window_.get_allocator());
    if (metadata_) {
        metadata_->seal();
    }
    return status_;
}

Status RuntimeSnapshotImport::feed(Slice bytes)
try {
    if (!status_.ok()) {
        return status_;
    }
    if (finishing_) {
        return Status::unavailable("snapshot import is already finished");
    }
    auto core = core_.lock();
    if (!core || core->closed()) {
        return fail(Status::unavailable("snapshot import Tree is closed"));
    }
    auto status = parse({reinterpret_cast<const uint8_t *>(bytes.data()), bytes.size()}, *core);
    return status.ok() ? status : fail(std::move(status));
}
catch (const std::bad_alloc &) {
    return fail(Status::resource_exhausted("snapshot import staging allocation failed"));
}

Status RuntimeSnapshotImport::prepare_finish(std::shared_ptr<TreeCore> *core)
{
    if (!status_.ok()) {
        return status_;
    }
    if (finishing_) {
        return Status::unavailable("snapshot import is already finished");
    }
    finishing_ = true;
    if (part_ != Part::Complete || parsed_ != expected_) {
        return fail(Status::invalid_argument("snapshot import stream is truncated"));
    }
    *core = core_.lock();
    if (!*core || (*core)->closed()) {
        return fail(Status::unavailable("snapshot import Tree is closed"));
    }
    if (native_) {
        status_ = attach_native_dependencies(*catalog_, {}, metadata_);
    }
    else {
        status_ = portable_->finish(&catalog_);
    }
    portable_.reset();
    key_    = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(key_.get_allocator());
    window_ = detail::ReservedVector<uint8_t, AllocationKind::kScratch>(window_.get_allocator());
    return status_;
}

Status RuntimeSnapshotImport::finish(uint64_t *coverage)
try {
    std::shared_ptr<TreeCore> core;
    auto                      status = prepare_finish(&core);
    if (status.ok()) {
        status = core->install_readable(std::move(catalog_), metadata_);
    }
    if (!status.ok()) {
        return fail(std::move(status));
    }
    if (coverage != nullptr) {
        *coverage = coverage_;
    }
    metadata_->seal();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return fail(Status::resource_exhausted("snapshot import validation allocation failed"));
}

Status RuntimeSnapshotImport::finish_async(SnapshotImportComplete complete)
try {
    if (!complete) {
        return Status::invalid_argument();
    }
    std::shared_ptr<TreeCore> core;
    auto                      status = prepare_finish(&core);
    if (!status.ok()) {
        return fail(std::move(status));
    }

    struct Completion
    {
        std::shared_ptr<TreeCore> core;
        SnapshotImportComplete    receiver;
        uint64_t                  coverage;

        static void done(void *context, Status result) noexcept
        {
            auto &self = *static_cast<Completion *>(context);
            try {
                self.receiver(std::move(result), self.coverage);
            }
            catch (...) {
                CRB_LOG_ERROR("portable import receiver threw after completion");
            }
        }
    };

    auto callback =
        detail::reserved_shared<Completion>(core->binding()->lifecycle_budget(), core, std::move(complete), coverage_);
    status =
        core->install_readable_async(std::move(catalog_), metadata_,
                                     {.context = callback.get(), .complete_fn = &Completion::done, .owner = callback});
    if (!status.ok()) {
        return fail(std::move(status));
    }
    // The install owns the reservation and seals it after graph validation.
    metadata_.reset();
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return fail(Status::resource_exhausted());
}
} // namespace crowdb::tree
