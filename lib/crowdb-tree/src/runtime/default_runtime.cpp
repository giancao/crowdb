// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "default_runtime.h"

#include "crowdb-tree/config.h"
#include "tree_runtime_internal.h"

#include <thread>

namespace crowdb::tree
{
namespace
{
struct DefaultRuntime
{
    std::shared_ptr<TreeRuntime>      runtime;
    std::shared_ptr<AllocationBudget> metadata;

    static void destroy(DefaultRuntime *entry) noexcept
    {
        if (entry != nullptr) {
            MetadataAllocator<DefaultRuntime> allocator(entry->metadata);
            std::destroy_at(entry);
            allocator.deallocate(entry, 1);
        }
    }
};

struct ProcessRuntime
{
    ~ProcessRuntime()
    {
        DefaultRuntime::destroy(value.load(std::memory_order_relaxed));
    }

    std::atomic<DefaultRuntime *> value{nullptr};
    std::atomic_flag              initializing = ATOMIC_FLAG_INIT;
};

ProcessRuntime process_runtime;

Status default_runtime(std::shared_ptr<TreeRuntime> *out, bool *initializing)
{
    auto *entry = process_runtime.value.load(std::memory_order_acquire);
    if (entry == nullptr) {
        while (process_runtime.initializing.test_and_set(std::memory_order_acquire)) {
            if (auto *ready = process_runtime.value.load(std::memory_order_acquire); ready != nullptr) {
                *out = ready->runtime;
                return Status::Ok();
            }
            if (initializing != nullptr) {
                *initializing = true;
                return Status::unavailable();
            }
            std::this_thread::yield();
        }
        auto release = [](std::atomic_flag *owner) { owner->clear(std::memory_order_release); };
        const std::unique_ptr<std::atomic_flag, decltype(release)> owner(&process_runtime.initializing, release);
        entry = process_runtime.value.load(std::memory_order_acquire);
        if (entry == nullptr) {
            std::shared_ptr<TreeRuntime> runtime;
            auto                         status = TreeRuntime::create({}, &runtime);
            if (!status.ok()) {
                return status;
            }
            auto                              metadata = TreeRuntimeAccess::lifecycle(*runtime);
            MetadataAllocator<DefaultRuntime> allocator(metadata);
            entry = allocator.allocate(1);
            std::construct_at(entry, DefaultRuntime{.runtime = std::move(runtime), .metadata = std::move(metadata)});
            process_runtime.value.store(entry, std::memory_order_release);
        }
    }
    *out = entry->runtime;
    return Status::Ok();
}
} // namespace

Status configure_default_runtime(Config *config, bool *initializing)
try {
    if (initializing != nullptr) {
        *initializing = false;
    }
    if (config == nullptr) {
        return Status::invalid_argument();
    }
    return config->runtime ? Status::Ok() : default_runtime(&config->runtime, initializing);
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted();
}
catch (...) {
    return Status::internal_error();
}
} // namespace crowdb::tree
