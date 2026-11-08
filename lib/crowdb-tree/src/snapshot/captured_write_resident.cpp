// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/captured_write_internal.h"

namespace crowdb::tree
{
namespace
{
bool same_resident_owner(const PageBlockRef &source, const PageBlockRef &candidate)
{
    return !source.owner.owner_before(candidate.owner) && !candidate.owner.owner_before(source.owner) &&
           source.resident_frame.data() == candidate.resident_frame.data() &&
           source.resident_frame.size() == candidate.resident_frame.size();
}
} // namespace

void CapturedWriteOperation::State::attach_dependency(const DurableCapturedFrame &value, bool retain_owner)
{
    if (retain_owner) {
        dependencies->owners.push_back(value.page_block->physical_owner ? value.page_block->physical_owner
                                                                        : value.page_block->owner);
    }
    overflow_page_block->pages.emplace(dependency_pid, OverflowPageBlock{.page_block    = value.page_block,
                                                                         .next          = value.next_overflow,
                                                                         .bytes         = value.overflow_bytes,
                                                                         .frame_bytes   = value.frame_bytes,
                                                                         .logical_bytes = value.logical_bytes});
    advance_dependency(value);
}

void CapturedWriteOperation::State::advance_dependency(const DurableCapturedFrame &value)
{
    dependency_bytes -= value.overflow_bytes;
    dependency_pid = value.next_overflow;
    if (dependency_bytes == 0) {
        ++dependency_index;
    }
    stage = Stage::Dependencies;
    wake();
}

void CapturedWriteOperation::State::use_resident_dependency(DurableCapturedFrame value)
{
    const auto written  = pages->frames.find(value.page_id);
    const auto original = resident_sources.find(value.page_id);
    if (written == pages->frames.end()) {
        if (overflow_writes && original != resident_sources.end()) {
            if (!value.page_block || !same_resident_owner(*original->second.page_block, *value.page_block) ||
                original->second.dependencies != dependencies ||
                value.page_block->resident_frame.size() != value.frame_bytes) {
                finish(Status::corruption("resident overflow conflicts with a pending frame"));
                return;
            }
            const OverflowFrameView raw(value.page_block->resident_frame.data(), value.frame_bytes);
            if (raw.next_page_id() != value.next_overflow || raw.chunk_len() != value.overflow_bytes) {
                finish(Status::corruption("pending resident overflow differs from its frame"));
                return;
            }
            advance_dependency(value);
            return;
        }
        if (inherited) {
            const auto staged = inherited->frames.find(value.page_id);
            if (staged != inherited->frames.end()) {
                const auto source = staged->second.resident_source.lock();
                if (!source || !value.page_block || !same_resident_owner(*source, *value.page_block) ||
                    staged->second.type != value.type || staged->second.next_overflow != value.next_overflow ||
                    staged->second.overflow_bytes != value.overflow_bytes ||
                    staged->second.frame_bytes != value.frame_bytes) {
                    finish(Status::corruption("resident overflow conflicts with the flush's staged PID"));
                    return;
                }
                auto status = insert(staged->second, true);
                if (!status.ok()) {
                    finish(std::move(status));
                    return;
                }
                attach_dependency(staged->second, true);
                return;
            }
        }
        begin_resident_write(std::move(value));
        return;
    }
    const auto &page_block = value.page_block;
    if (!page_block || original == resident_sources.end() || !page_block->readable() ||
        !same_resident_owner(*original->second.page_block, *page_block) || written->second.type != value.type ||
        written->second.next_overflow != value.next_overflow ||
        written->second.overflow_bytes != value.overflow_bytes || written->second.frame_bytes != value.frame_bytes) {
        finish(Status::corruption("resident overflow PID conflicts with an already written frame"));
        return;
    }
    // A frame written in this group already contributed its extent lease.
    // Retaining its PageBlockRef owner would create a dependency cycle.
    attach_dependency(written->second, original->second.dependencies != dependencies);
}

void CapturedWriteOperation::State::begin_resident_write(DurableCapturedFrame value)
{
    const auto &page_block = value.page_block;
    if (!page_block || !page_block->readable() || page_block->storage != PageBlockStorage::kResidentFrame ||
        page_block->resident_frame.size() != value.frame_bytes || value.frame_bytes > limits.write.max_frame_bytes ||
        !frame_validate(page_block->resident_frame.data(), value.frame_bytes)) {
        finish(Status::corruption("resident overflow frame has no valid independent raw owner"));
        return;
    }
    const OverflowFrameView raw(page_block->resident_frame.data(), value.frame_bytes);
    if (frame_page_type(page_block->resident_frame.data()) != page_type::kOverflowFrame ||
        raw.self_page_id() != value.page_id || raw.next_page_id() != value.next_overflow ||
        raw.chunk_len() != value.overflow_bytes) {
        finish(Status::corruption("resident overflow descriptor differs from its frame"));
        return;
    }
    PageWriteInput input{
        .info                = {.page_id = value.page_id, .type = value.type, .bytes = value.frame_bytes},
        .frame               = page_block->resident_frame,
        .owner               = page_block->owner,
        .reload_dependencies = dependencies,
        .raw_blob            = {},
    };
    resident_sources.emplace(value.page_id, ResidentSource{.page_block = page_block, .dependencies = dependencies});
    if (limits.frames.retain_overflow_buffers) {
        value.resident_source = page_block;
        if (submit_overflow(value, std::move(input))) {
            advance_dependency(value);
        }
        return;
    }
    resident_dependency = std::move(value);
    auto status         = PageWriteOperation::create(store, runtime, lane, std::move(input), limits.write, budget,
                                                     reserve_page(true), &write);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    begin_child(write->sender(), Stage::ResidentWrite);
}

void CapturedWriteOperation::State::received_resident_write()
{
    const auto &result                  = write->result();
    resident_dependency.resident_source = resident_dependency.page_block;
    resident_dependency.page_block      = result.page_block;
    resident_dependency.logical_bytes   = result.logical_bytes;
    auto status                         = insert(resident_dependency, false);
    if (!status.ok()) {
        finish(std::move(status));
        return;
    }
    ++report.written_frames;
    report.written_bytes += result.physical_bytes;
    attach_dependency(resident_dependency, false);
    write.reset();
    relocation_read.reset();
    resident_dependency.page_block.reset();
}
} // namespace crowdb::tree
