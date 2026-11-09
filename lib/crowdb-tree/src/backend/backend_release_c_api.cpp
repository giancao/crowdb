// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend_release_c_api.h"

#include "c_api_internal.h"
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"

using namespace crowdb::tree;

namespace
{
struct WakeContext
{
    void (*wake)(void *) = nullptr;
    void (*drop)(void *) = nullptr;
    void *value          = nullptr;
#ifdef CROWDB_HAVE_LIBURING
    std::atomic<bool> reactor_stopped{false};
#endif

    ~WakeContext()
    {
        if (drop != nullptr) {
            drop(value);
        }
    }

    static void invoke(void *context) noexcept
    {
        auto &self = *static_cast<WakeContext *>(context);
        self.wake(self.value);
    }

#ifdef CROWDB_HAVE_LIBURING
    static void stopped(void *context, int /*result*/) noexcept
    {
        auto &self = *static_cast<WakeContext *>(context);
        self.reactor_stopped.store(true, std::memory_order_release);
        self.wake(self.value);
    }
#endif
};
} // namespace

struct ct_backend_release_service
{
    std::shared_ptr<AllocationBudget>      budget;
    std::shared_ptr<BackendReleaseService> queue;
#ifdef CROWDB_HAVE_LIBURING
    std::shared_ptr<WakeContext>                 callback;
    std::shared_ptr<crowdb::common::DiskIOUring> reactor;
    std::atomic<bool>                            stop_started{false};
#endif
};

ct_status ct_tree_backend_release_service(ct_tree *tree, void (*wake)(void *), void (*drop)(void *), void *context,
                                          ct_backend_release_service **out)
{
    if (tree == nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    return c_api_bind_backend_release(c_api_backend_bundle(tree), false, wake, drop, context, out);
}

ct_status c_api_bind_backend_release(PageStoreBundle *bundle, bool start_block_reactor, void (*wake)(void *),
                                     void (*drop)(void *), void *context, ct_backend_release_service **out)
try {
    if (wake == nullptr || drop == nullptr || out == nullptr || *out != nullptr) {
        return static_cast<ct_status>(Code::kInvalidArgument);
    }
    auto *store = bundle != nullptr ? dynamic_cast<BlockPageStore *>(bundle->store.get()) : nullptr;
    if (store == nullptr || !store->release_service()) {
        return static_cast<ct_status>(Code::kNotFound);
    }
    const auto &queue    = store->release_service();
    const auto &budget   = queue->allocation_budget();
    auto        callback = std::allocate_shared<WakeContext>(MetadataAllocator<WakeContext>(budget));
    callback->wake       = wake;
    callback->value      = context;
    MetadataAllocator<ct_backend_release_service> allocator(budget);
    auto                                         *storage = std::construct_at(allocator.allocate(1));
    std::unique_ptr<ct_backend_release_service, decltype(&ct_backend_release_service_free)> handle(
        storage, &ct_backend_release_service_free);
    handle->budget = budget;
    handle->queue  = queue;
#ifdef CROWDB_HAVE_LIBURING
    if (start_block_reactor) {
        // The fresh store has no files and no other driver. Allocate release
        // state before starting its poller, then bind before admitting I/O.
        bundle->uring = c_api_create_block_reactor();
        if (!bundle->uring) {
            return static_cast<ct_status>(Code::kNotSupported);
        }
    }
    handle->callback = callback;
    handle->reactor  = bundle->uring;
#else
    if (start_block_reactor) {
        return static_cast<ct_status>(Code::kNotSupported);
    }
#endif
    const auto status = queue->bind_wake(callback, &WakeContext::invoke, callback.get());
    if (!status.ok()) {
        return static_cast<ct_status>(status.code());
    }
    callback->drop = drop;
    *out           = handle.release();
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<ct_status>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<ct_status>(Code::kInternal);
}

ct_backend_release_step ct_backend_release_service_pump(ct_backend_release_service *service, size_t limit)
{
    if (service == nullptr || limit == 0 || limit > 64) {
        return {};
    }
    crowdb::common::DiskIOUring *reactor = nullptr;
#ifdef CROWDB_HAVE_LIBURING
    reactor = service->reactor.get();
#endif
    const auto step    = service->queue->pump(limit, reactor);
    bool       drained = service->queue->drained();
#ifdef CROWDB_HAVE_LIBURING
    if (drained && reactor != nullptr) {
        if (!service->stop_started.exchange(true, std::memory_order_acq_rel)) {
            const auto &callback = service->callback;
            reactor->stop_after_drain(
                {.context = callback.get(), .callback = &WakeContext::stopped, .owner = callback});
        }
        drained = service->callback->reactor_stopped.load(std::memory_order_acquire);
    }
#endif
    return {.visited   = step.visited,
            .completed = step.completed,
            .more      = static_cast<uint8_t>(step.more),
            .drained   = static_cast<uint8_t>(drained)};
}

uint8_t ct_backend_release_service_is_async(const ct_backend_release_service *service)
{
#ifdef CROWDB_HAVE_LIBURING
    return static_cast<uint8_t>(service != nullptr && service->reactor != nullptr);
#else
    (void)service;
    return 0;
#endif
}

void ct_backend_release_service_stop(ct_backend_release_service *service)
{
    if (service != nullptr) {
        service->queue->stop_admission();
    }
}

void ct_backend_release_service_free(ct_backend_release_service *service)
{
    if (service == nullptr) {
        return;
    }
    auto budget = service->budget;
    std::destroy_at(service);
    MetadataAllocator<ct_backend_release_service>(budget).deallocate(service, 1);
}
