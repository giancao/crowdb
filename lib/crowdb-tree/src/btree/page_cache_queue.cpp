// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "page_cache_internal.h"

#include <cassert>

namespace crowdb::tree
{
SharedPageCache::State::Request *SharedPageCache::State::claim(const std::shared_ptr<AllocationBudget> &metadata,
                                                               bool                                     accept) noexcept
{
    Request *claimed = nullptr;
    if (!closed.load(std::memory_order_acquire)) {
        const auto first  = next_request.fetch_add(1, std::memory_order_relaxed) % requests.size();
        const auto probes = accept ? size_t{1} : requests.size();
        for (size_t offset = 0; offset < probes; ++offset) {
            auto &request = requests[(first + offset) % requests.size()];
            bool  free    = false;
            if (request.occupied.compare_exchange_strong(free, true, std::memory_order_acquire)) {
                claimed = &request;
                break;
            }
        }
        if (claimed == nullptr && accept) {
            try {
                auto request             = detail::reserved_shared<Request>(metadata);
                claimed                  = request.get();
                claimed->command.context = claimed;
                claimed->allocation      = std::move(request);
            }
            catch (const std::bad_alloc &) {
                // The caller can still use its upstream without cache state.
                rejected.fetch_add(1, std::memory_order_relaxed);
                return nullptr;
            }
        }
    }
    if (claimed == nullptr) {
        rejected.fetch_add(1, std::memory_order_relaxed);
    }
    else if (accept) {
        claimed->owner = weak_from_this().lock();
        assert(claimed->owner);
        waiters.fetch_add(1, std::memory_order_relaxed);
        ordinary_waiters.fetch_add(1, std::memory_order_relaxed);
    }
    return claimed;
}

void SharedPageCache::State::submit(Request *request) noexcept
{
    request->registered_at_ns = tree_metrics_now_ns();
    enqueue(&request->command);
}

void SharedPageCache::State::enqueue(Command *command) noexcept
{
    auto *head = incoming.load(std::memory_order_relaxed);
    do {
        command->next = head;
    } while (!incoming.compare_exchange_weak(head, command, std::memory_order_release, std::memory_order_relaxed));
    wake();
}

void SharedPageCache::State::wake() noexcept
{
    auto owner = weak_from_this().lock();
    assert(owner);
    if (!running.exchange(true, std::memory_order_acq_rel)) {
        drain();
    }
}

void SharedPageCache::State::drain() noexcept
{
    for (;;) {
        auto    *list    = incoming.exchange(nullptr, std::memory_order_acquire);
        Command *ordered = nullptr;
        while (list != nullptr) {
            auto *next = list->next;
            list->next = ordered;
            ordered    = list;
            list       = next;
        }
        while (ordered != nullptr) {
            auto *next = ordered->next;
            if (ordered->kind == CommandKind::Request) {
                auto *request = static_cast<Request *>(ordered->context);
                try {
                    process(request);
                }
                catch (const std::bad_alloc &) {
                    deliver(request, Status::resource_exhausted("cache request allocation failed"));
                }
                catch (...) {
                    deliver(request, Status::internal_error());
                }
            }
            else {
                loaded(static_cast<Entry *>(ordered->context));
            }
            ordered = next;
        }
        drain_trims();
        if (evict_requested.exchange(false, std::memory_order_acq_rel)) {
            evict_ready();
        }
        // A producer that observed running publishes its command before its
        // exchange. Acquire that handoff before rechecking the incoming lists.
        running.exchange(false, std::memory_order_acq_rel);
        if ((incoming.load(std::memory_order_acquire) == nullptr && trims.load(std::memory_order_acquire) == nullptr &&
             !evict_requested.load(std::memory_order_acquire)) ||
            running.exchange(true, std::memory_order_acq_rel)) {
            return;
        }
    }
}
} // namespace crowdb::tree
