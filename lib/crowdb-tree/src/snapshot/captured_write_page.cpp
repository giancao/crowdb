// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"
#include "snapshot/primary_page_block.h"

namespace crowdb::tree
{
struct CapturedWriteOperation::State::OverflowWrites
{
    struct Result
    {
        DurableCapturedFrame frame;
        uint64_t             physical_bytes = 0;
        Status               status;
    };

    using Join = stdexec::connect_result_t<decltype(std::declval<stdexec::simple_counting_scope &>().join()), Receiver>;

    explicit OverflowWrites(std::shared_ptr<State> owner)
        : results(detail::ReservationAllocator<std::shared_ptr<Result>>(owner->budget.bytes)),
          joined(stdexec::connect(scope.join(), Receiver{std::move(owner)}))
    {
    }

    detail::ReservedVector<std::shared_ptr<Result>> results;
    stdexec::simple_counting_scope                  scope;
    Join                                            joined;
    Status                                          status;
};

bool CapturedWriteOperation::State::submit_overflow(DurableCapturedFrame value, PageWriteInput input)
{
    if (!overflow_writes) {
        overflow_writes = detail::reserved_shared<OverflowWrites>(budget.bytes, shared_from_this());
    }
    auto item   = detail::reserved_shared<OverflowWrites::Result>(budget.bytes);
    item->frame = std::move(value);
    std::shared_ptr<PageWriteOperation> operation;
    auto                                status =
        PageWriteOperation::create(store, runtime, lane, std::move(input), limits.write, budget, reserve, &operation);
    if (!status.ok()) {
        finish(std::move(status));
        return false;
    }
    overflow_writes->results.push_back(item);
    auto fail = [this, item](Status result) noexcept {
        item->status = std::move(result);
        overflow_failed.store(true, std::memory_order_release);
    };
    auto sender = operation->sender() | stdexec::then([item, operation]() noexcept {
                      const auto &written       = operation->result();
                      item->frame.page_block    = written.page_block;
                      item->frame.logical_bytes = written.logical_bytes;
                      item->physical_bytes      = written.physical_bytes;
                  }) |
                  stdexec::upon_error(fail) | stdexec::upon_stopped([fail]() noexcept { fail(Status::unavailable()); });
    stdexec::spawn(std::move(sender), overflow_writes->scope.get_token(),
                   stdexec::env{
                       stdexec::prop{stdexec::get_allocator, MetadataAllocator<std::byte>(budget.bytes)}
    });
    return true;
}

void CapturedWriteOperation::State::join_overflow(Status status) noexcept
{
    if (stage == Stage::JoinOverflow) {
        return;
    }
    stage   = Stage::JoinOverflow;
    waiting = true;
    child_ready.store(false, std::memory_order_release);
    overflow_writes->status = std::move(status);
    overflow_writes->scope.close();
    stdexec::start(overflow_writes->joined);
}

void CapturedWriteOperation::State::collect_overflow()
{
    auto pending = std::move(overflow_writes);
    if (!pending->status.ok() || stopped.load(std::memory_order_acquire)) {
        finish(std::move(pending->status));
        return;
    }
    for (auto &item : pending->results) {
        if (!item->status.ok()) {
            finish(std::move(item->status));
            return;
        }
        // Each completed write still owns its protected extent. The primary
        // carries those owners and the complete overflow map into publication.
        dependencies->owners.push_back(item->frame.page_block->physical_owner);
        auto status = install_written(std::move(item->frame), item->physical_bytes);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
    }
    stage = Stage::Dependencies;
    wake();
}

void CapturedWriteOperation::State::begin_write()
{
    if (!frame.has_value()) {
        finish(Status::internal_error());
        return;
    }
    if (frame->reuse) {
        if (limits.frames.exclusion.length != 0) {
            auto rebound = rebind_primary();
            if (!rebound.ok()) {
                finish(std::move(rebound));
                return;
            }
        }
        auto status = insert(descriptor, true);
        if (!status.ok()) {
            finish(std::move(status));
            return;
        }
        finish_frame();
        return;
    }
    PageWriteInput input{.info                = frame->info,
                         .frame               = frame->buffer->bytes(),
                         .owner               = frame->buffer,
                         .reload_dependencies = dependencies,
                         .raw_blob            = frame->buffer->raw_blob()};
    auto status = PageWriteOperation::create(store, runtime, lane, std::move(input), limits.write, budget,
                                             reserve_page(descriptor.type == page_type::kOverflowFrame), &write);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(write->sender(), Stage::Write);
}

PageWriteOperation::ReserveExtent CapturedWriteOperation::State::reserve_page(bool protect_dependency)
{
    const auto weak = weak_from_this();
    return [weak, protect_dependency](uint64_t length, PageWriteOperation::ExtentComplete done) {
        const auto self = weak.lock();
        if (!self) {
            done(Status::unavailable(), {});
            return;
        }
        auto event      = detail::reserved_shared<Reservation>(self->budget.bytes);
        event->complete = std::move(done);
        if (protect_dependency) {
            event->dependencies = self->dependencies;
        }
        self->reserve(length, [event](Status status, ReservedWriteExtent extent) {
            if (event->claimed.exchange(true, std::memory_order_acq_rel)) {
                return;
            }
            try {
                if (status.ok() && event->dependencies) {
                    event->dependencies->owners.push_back(extent.owner);
                }
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted();
            }
            event->complete(std::move(status), std::move(extent));
        });
    };
}

void CapturedWriteOperation::State::received_write()
{
    const auto &written      = write->result();
    descriptor.page_block    = written.page_block;
    descriptor.logical_bytes = written.logical_bytes;
    auto status              = install_written(descriptor, written.physical_bytes);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    finish_frame();
}

Status CapturedWriteOperation::State::install_written(DurableCapturedFrame value, uint64_t physical_bytes)
{
    if (value.type == page_type::kOverflowFrame) {
        overflow_page_block->pages.emplace(value.page_id, OverflowPageBlock{.page_block    = value.page_block,
                                                                            .next          = value.next_overflow,
                                                                            .bytes         = value.overflow_bytes,
                                                                            .frame_bytes   = value.frame_bytes,
                                                                            .logical_bytes = value.logical_bytes});
    }
    else if (value.type == page_type::kLeafBase) {
        auto primary      = detail::reserved_shared<PageBlockRef>(budget.bytes, *value.page_block);
        primary->overflow = overflow_page_block;
        value.page_block  = std::move(primary);
    }
    auto status = insert(std::move(value), false);
    if (status.ok()) {
        ++report.written_frames;
        report.written_bytes += physical_bytes;
    }
    return status;
}

void CapturedWriteOperation::State::finish_frame()
{
    if (descriptor.type != page_type::kOverflowFrame) {
        dependencies.reset();
        overflow_page_block.reset();
    }
    write.reset();
    frame.reset();
    if (last_frame) {
        finish(Status::Ok());
        return;
    }
    stage = Stage::Next;
    wake();
}
} // namespace crowdb::tree
