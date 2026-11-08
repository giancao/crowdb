// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/budget_memory_resource.h"
#include "btree/retired_shared.h"
#include "chunk_physical_registry.h"
#include "chunk_transport.h"
#include "host_completion.h"

#include <map>

namespace crowdb::tree::detail
{
namespace
{
struct RegistryPoolState : std::enable_shared_from_this<RegistryPoolState>
{
    struct Key
    {
        const RetirementQueue *host = nullptr;
        uint64_t               tree = 0;
    };

    struct Less
    {
        bool operator()(Key left, Key right) const noexcept
        {
            return left.host == right.host ? left.tree < right.tree
                                           : std::less<const RetirementQueue *>{}(left.host, right.host);
        }
    };

    struct Entry
    {
        // The scope's host identity remains owned even after its executor closes.
        std::shared_ptr<RetirementQueue>       host;
        std::shared_ptr<ChunkPhysicalRegistry> registry;
    };

    struct Request : RetirementNode
    {
        std::shared_ptr<RegistryPoolState>      state;
        std::shared_ptr<Request>                queued_owner;
        std::shared_ptr<AllocationBudget>       budget;
        std::shared_ptr<RetirementQueue>        host;
        std::shared_ptr<ChunkPhysicalRegistry> *out  = nullptr;
        uint64_t                                tree = 0;
        ChunkTransportCompletion                complete;
    };

    explicit RegistryPoolState(std::shared_ptr<RetirementQueue> executor) : queue(std::move(executor))
    {
    }

    bool retire_step() noexcept
    {
        for (size_t count = 0; count < 32 && !entries.empty(); ++count) {
            entries.erase(entries.begin());
        }
        return entries.empty();
    }

    static bool dispatch(RetirementNode *node) noexcept
    {
        auto   request = std::move(static_cast<Request *>(node)->queued_owner);
        Status status;
        try {
            auto     &self = *request->state;
            const Key key{.host = request->host.get(), .tree = request->tree};
            auto      found = self.entries.find(key);
            if (found == self.entries.end()) {
                std::shared_ptr<ChunkPhysicalRegistry> registry;
                status = ChunkPhysicalRegistry::create(request->budget, &registry);
                if (status.ok()) {
                    BudgetMemoryResource::Scope credit(self.memory, request->budget);
                    found = self.entries
                                .emplace(key, Entry{.host = std::move(request->host), .registry = std::move(registry)})
                                .first;
                }
            }
            if (status.ok()) {
                *request->out = found->second.registry;
            }
        }
        catch (const std::bad_alloc &) {
            status = Status::resource_exhausted();
        }
        catch (...) {
            status = Status::internal_error();
        }
        request->state.reset();
        request->complete.complete(std::move(status));
        return true;
    }

    void submit(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata, std::shared_ptr<RetirementQueue> host,
                std::shared_ptr<ChunkPhysicalRegistry> *out, ChunkTransportCompletion complete)
    {
        auto request               = std::allocate_shared<Request>(MetadataAllocator<Request>(metadata));
        request->state             = shared_from_this();
        request->queued_owner      = request;
        request->budget            = metadata;
        request->host              = std::move(host);
        request->retirement_budget = metadata;
        request->tree              = tree;
        request->out               = out;
        request->complete          = std::move(complete);
        request->dispose           = &dispatch;
        queue->retire(request.get());
    }

    std::shared_ptr<RetirementQueue> queue;
    BudgetMemoryResource             memory;
    std::pmr::map<Key, Entry, Less>  entries{&memory};
};
} // namespace

struct ChunkRegistryPool
{
    std::shared_ptr<AllocationBudget>  metadata;
    std::shared_ptr<RegistryPoolState> state;

    static void destroy(ChunkRegistryPool *pool) noexcept
    {
        if (pool != nullptr) {
            MetadataAllocator<ChunkRegistryPool> allocator(pool->metadata);
            std::destroy_at(pool);
            allocator.deallocate(pool, 1);
        }
    }
};

ChunkTransport::~ChunkTransport()
{
    ChunkRegistryPool::destroy(registry_pool_.load(std::memory_order_relaxed));
}

Status ChunkTransport::submit_physical_registry(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                                                std::shared_ptr<ChunkPhysicalRegistry> *out,
                                                ChunkTransportCompletion                complete) noexcept
try {
    if (tree == 0 || !metadata || out == nullptr || *out || complete.complete_fn == nullptr) {
        return Status::invalid_argument();
    }
    auto host = metadata->retirement_queue();
    if (!host) {
        return Status::invalid_argument();
    }
    auto *pool = registry_pool_.load(std::memory_order_acquire);
    if (pool == nullptr) {
        auto                                 state = retired_shared<RegistryPoolState>(metadata, host, host);
        MetadataAllocator<ChunkRegistryPool> allocator(metadata);
        auto                                *prepared = allocator.allocate(1);
        std::construct_at(prepared, ChunkRegistryPool{.metadata = metadata, .state = std::move(state)});
        if (registry_pool_.compare_exchange_strong(pool, prepared, std::memory_order_release,
                                                   std::memory_order_acquire)) {
            pool = prepared;
        }
        else {
            ChunkRegistryPool::destroy(prepared);
        }
    }
    pool->state->submit(tree, metadata, std::move(host), out, std::move(complete));
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}

Status ChunkTransport::physical_registry(uint64_t tree, const std::shared_ptr<AllocationBudget> &metadata,
                                         std::shared_ptr<ChunkPhysicalRegistry> *out)
{
    struct OpenResult
    {
        HostCompletion done;
        Status         status;
    } result;

    auto status = submit_physical_registry(tree, metadata, out,
                                           {.complete_fn =
                                                [](void *context, Status status) noexcept {
                                                    auto &result  = *static_cast<OpenResult *>(context);
                                                    result.status = std::move(status);
                                                    result.done.signal();
                                                },
                                            .context = &result});
    if (!status.ok()) {
        return status;
    }
    result.done.wait();
    return result.status;
}
} // namespace crowdb::tree::detail
