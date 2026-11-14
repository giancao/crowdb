// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "chunk_backing.h"

#include "backend/local/scheduled_page_store.h"
#include "btree/allocation_bound.h"
#include "chunk_frame.h"
#include "chunk_physical_registry.h"
#include "chunk_write_queue.h"

#include <algorithm>

namespace crowdb::tree
{
namespace
{
struct ChunkRead : std::enable_shared_from_this<ChunkRead>
{
    std::shared_ptr<const ChunkPackBacking> pack;
    std::shared_ptr<ScheduledPageStore>     store;
    std::shared_ptr<AllocationBudget>       metadata;
    std::span<uint8_t>                      output;
    AsyncCompletion                         complete;

    static void finished(void *raw, Status status)
    {
        auto &self = *static_cast<ChunkRead *>(raw);
        if (status.ok()) {
            status = self.pack->domain->catalog->protection()->check_read_owner();
        }
        auto done = std::exchange(self.complete, {});
        done.complete(std::move(status));
    }

    void start(AsyncCompletion callback) noexcept
    {
        complete = std::move(callback);
        try {
            auto status = pack->domain->catalog->protection()->check_read_owner();
            if (!status.ok()) {
                finished(this, std::move(status));
                return;
            }
            const auto                 &domain = *pack->domain;
            detail::ChunkFixedPackStore reader(domain.tree, domain.epoch, domain.mirrors, domain.catalog,
                                               domain.transport);
            reader.submit_read_into(pack->pack, metadata, output,
                                    {.context     = this,
                                     .complete_fn = &ChunkRead::finished,
                                     .owner       = shared_from_this(),
                                     .allocations = metadata},
                                    store);
        }
        catch (const std::bad_alloc &) {
            finished(this, Status::resource_exhausted());
        }
        catch (...) {
            finished(this, Status::internal_error());
        }
    }
};
} // namespace

static void submit_chunk_pack_read(ScheduledPageStore &scheduled, std::shared_ptr<const ChunkPackBacking> backing,
                                   std::span<uint8_t> output, std::shared_ptr<AllocationBudget> metadata,
                                   AsyncCompletion complete)
{
    try {
        auto read      = detail::reserved_shared<ChunkRead>(metadata);
        read->pack     = std::move(backing);
        read->store    = scheduled.shared_from_this();
        read->metadata = std::move(metadata);
        read->output   = output;
        auto status    = scheduled.submit_async_step(
            [read](AsyncCompletion callback) { read->start(std::move(callback)); }, complete, output.size(), 0);
        if (!status.ok()) {
            complete.complete(std::move(status));
        }
    }
    catch (const std::bad_alloc &) {
        complete.complete(Status::resource_exhausted());
    }
}

static void submit_chunk_pack_write(ScheduledPageStore &scheduled, std::shared_ptr<ChunkPackBacking> backing,
                                    std::span<const uint8_t> input, AsyncCompletion complete)
{
    auto *queue = scheduled.chunk_writes();
    if (queue == nullptr) {
        complete.complete(Status::not_supported("Chunk writes require a Chunk backend lane"));
        return;
    }
    queue->submit(scheduled.shared_from_this(), std::move(backing), input, std::move(complete));
}

void initialize_chunk_backing_io(ChunkPackBacking &backing) noexcept
{
    backing.read  = &submit_chunk_pack_read;
    backing.write = &submit_chunk_pack_write;
}

namespace
{
Status validate_pin_pack(const ChunkBackingDomain &domain, const detail::ChunkFixedPackRef &pack,
                         const std::shared_ptr<AllocationBudget> &metadata)
{
    std::optional<detail::ChunkStageRecord> record(detail::ChunkStageRecord{
        .tree_id             = 0,
        .id                  = {},
        .revision            = 0,
        .chunk_high          = 0,
        .chunk_low           = 0,
        .capacity            = 0,
        .manifest_generation = 0,
        .purpose             = 0,
        .state               = detail::ChunkStageState::Reserved,
        .packs = decltype(detail::ChunkStageRecord::packs)(MetadataAllocator<detail::ChunkStagePack>(metadata))});
    auto                                    status = domain.catalog->load_stage(domain.tree, pack.stage, &record);
    if (!status.ok()) {
        return status;
    }
    return validate_chunk_pin_stage(domain, pack, record);
}
} // namespace

Status validate_chunk_pin_stage(const ChunkBackingDomain &domain, const detail::ChunkFixedPackRef &pack,
                                const std::optional<detail::ChunkStageRecord> &record)
{
    if (!record || record->tree_id != domain.tree || record->id != pack.stage ||
        record->chunk_high != pack.chunk.high || record->chunk_low != pack.chunk.low ||
        record->purpose != static_cast<uint32_t>(pack.purpose) || pack.offset > record->capacity ||
        detail::chunk_framed_length(pack.length) > record->capacity - pack.offset ||
        std::none_of(record->packs.begin(), record->packs.end(), [&](const auto &stored) {
            return stored.offset == pack.offset && stored.length == pack.length && stored.checksum == pack.checksum;
        })) {
        return Status::corruption("Chunk pack is not an exact member of its protected stage");
    }
    return record->state == detail::ChunkStageState::Reloadable || record->state == detail::ChunkStageState::Referenced
             ? Status::Ok()
             : Status::unavailable("Chunk stage no longer admits readers");
}

size_t chunk_pin_validation_bound() noexcept
{
    return 2 * detail::kMaxStagePacks * sizeof(detail::ChunkStagePack);
}

Status prepare_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkStageId stage,
                             detail::ChunkId chunk, uint32_t length, PagePurpose purpose,
                             const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<ChunkPackBacking> *out)
try {
    if (!domain || !domain->supported() || !metadata || out == nullptr || *out || stage.owner_epoch != domain->epoch ||
        detail::chunk_purpose(chunk) != purpose || detail::chunk_framed_length(length) > domain->max_chunk_bytes ||
        !detail::ChunkFixedPackStore::write_allocation_bound(length)) {
        return Status::invalid_argument();
    }
    auto result = std::allocate_shared<ChunkPackBacking>(MetadataAllocator<ChunkPackBacking>(metadata));
    initialize_chunk_backing_io(*result);
    auto status = domain->catalog->pin_release_queue()->prepare(metadata, domain->catalog, domain->tree, stage, stage,
                                                                &result->pin);
    if (!status.ok()) {
        return status;
    }
    result->physical_budget = metadata;
    result->domain          = domain;
    result->pack = {.stage = stage, .chunk = chunk, .offset = 0, .length = length, .checksum = 0, .purpose = purpose};
    *out         = std::move(result);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status pin_chunk_backing(const std::shared_ptr<const ChunkBackingDomain> &domain, detail::ChunkFixedPackRef pack,
                         detail::ChunkStageId pin, const std::shared_ptr<AllocationBudget> &metadata,
                         std::shared_ptr<const ChunkPackBacking> *out)
try {
    if (!domain || !domain->supported() || !metadata || out == nullptr || *out || pin.owner_epoch != domain->epoch) {
        return Status::invalid_argument();
    }
    auto result = std::allocate_shared<ChunkPackBacking>(MetadataAllocator<ChunkPackBacking>(metadata));
    initialize_chunk_backing_io(*result);
    result->domain = domain;
    result->pack   = pack;
    auto status    = domain->catalog->pin_release_queue()->prepare(metadata, domain->catalog, domain->tree, pack.stage,
                                                                   pin, &result->pin);
    if (!status.ok()) {
        return status;
    }
    if (!chunk_backing_matches(*result, domain->backend_id, pack.length)) {
        return Status::corruption("checkpoint pack identity is invalid");
    }
    auto *protection = domain->catalog->protection();
    status           = protection->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    status = validate_pin_pack(*domain, pack, metadata);
    if (!status.ok()) {
        return status;
    }
    result->pin->arm();
    status = protection->change_pin(domain->tree, pack.stage, pin, true);
    if (status.ok()) {
        status = protection->check_read_owner();
    }
    if (status.ok() && domain->physical_registry) {
        auto registry = domain->physical_registry;
        status        = registry->prepare(pack.stage, pack.chunk, metadata, &result->physical);
        detail::ChunkAllocationIdentity receipt;
        if (status.ok()) {
            status = domain->transport->inspect_fixed_allocation(pack.chunk, pack.stage.owner_epoch, &receipt);
        }
        if (status.ok()) {
            status = registry->observe(result->physical, receipt, metadata);
        }
        if (status.ok()) {
            result->physical_metrics = ChunkPhysicalRegistry::extent(result->physical);
        }
    }
    if (status.ok()) {
        *out = std::move(result);
    }
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}

Status read_chunk_backing(const ChunkPackBacking &backing, std::span<uint8_t> output,
                          const std::shared_ptr<AllocationBudget> &budget)
{
    if (!backing.domain || !chunk_backing_matches(backing, backing.domain->backend_id, output.size())) {
        return Status::corruption("read does not name one owned Chunk pack");
    }
    const auto &domain     = *backing.domain;
    auto       *protection = domain.catalog->protection();
    auto        status     = protection->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    detail::ChunkFixedPackStore store(domain.tree, domain.epoch, domain.mirrors, domain.catalog, domain.transport);
    status = store.read_into(backing.pack, budget, output);
    if (status.ok()) {
        status = protection->check_read_owner();
    }
    return status;
}

Status write_chunk_backing(ChunkPackBacking &backing, std::span<const uint8_t> input,
                           const std::shared_ptr<AllocationBudget> &budget)
{
    if (!backing.domain || !chunk_backing_matches(backing, backing.domain->backend_id, input.size()) ||
        backing.pack.stage.owner_epoch != backing.domain->epoch) {
        return Status::invalid_argument("write does not name one reserved Chunk pack");
    }
    const auto &domain = *backing.domain;
    auto        status = domain.catalog->protection()->check_read_owner();
    if (!status.ok()) {
        return status;
    }
    backing.pin->arm();
    detail::ChunkFixedPackStore store(domain.tree, domain.epoch, domain.mirrors, domain.catalog, domain.transport,
                                      domain.physical_registry);
    detail::ChunkFixedPackRef   result;
    status = store.write(backing.pack.stage, backing.pack.chunk, backing.pack.purpose, input, budget, &result,
                         &backing.physical, backing.physical_budget);
    if (status.ok()) {
        status = domain.catalog->protection()->check_read_owner();
    }
    backing.physical_metrics = ChunkPhysicalRegistry::extent(backing.physical);
    if (status.ok()) {
        backing.pack = result;
    }
    return status;
}
} // namespace crowdb::tree
