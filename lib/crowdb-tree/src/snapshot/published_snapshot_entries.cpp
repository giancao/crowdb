// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "snapshot/published_snapshot_entries.h"

#include "btree/apply_dispatcher.h"
#include "crowdb-common/log.h"
#include "host_completion.h"

namespace crowdb::tree
{
namespace
{
void complete_entries(const PublishedSnapshotEntries::Complete &complete, Status status,
                      std::optional<PublishedScanEntry> entry, bool end) noexcept
{
    try {
        complete(std::move(status), std::move(entry), end);
    }
    catch (...) {
        CRB_LOG_ERROR("read-only view entry receiver threw after completion");
    }
}
} // namespace

struct PublishedSnapshotEntries::State
{
    struct Window;

    std::shared_ptr<PublishedReadViewContext> snapshot;
    std::shared_ptr<AllocationBudget>         metadata;
    std::shared_ptr<SnapshotPageLoader>       loader;
    std::shared_ptr<PublishedScan>            scan;
    std::atomic<bool>                         busy{false};
    Status                                    failure;
    bool                                      complete = false;
};

struct PublishedSnapshotEntries::State::Window
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        Window *window;

        void set_value() const noexcept
        {
            window->finish(Status::Ok());
        }

        void set_error(Status status) const noexcept
        {
            window->finish(std::move(status));
        }

        void set_stopped() const noexcept
        {
            window->finish(Status::unavailable());
        }
    };

    Window(std::shared_ptr<State> state, SnapshotReadWindow window, Complete complete)
        : state(std::move(state)),
          window(std::move(window)),
          complete(std::move(complete)),
          connected(stdexec::connect(this->state->scan->next_sender(), Receiver{.window = this}))
    {
    }

    static void start(void *context) noexcept
    {
        auto &work = *static_cast<Window *>(context);
        auto  keep = work.owner;
        stdexec::start(work.connected);
    }

    void finish(Status status) noexcept
    {
        auto                              keep = std::move(owner);
        std::optional<PublishedScanEntry> entry;
        if (status.ok()) {
            const auto &result = state->scan->result();
            try {
                if (!result.entries.empty()) {
                    entry.emplace(result.entries.front());
                }
                state->complete = result.done;
            }
            catch (const std::bad_alloc &) {
                status = Status::resource_exhausted();
            }
        }
        if (!status.ok()) {
            state->failure = status.without_message();
        }
        const bool end = status.ok() && state->complete;
        state->loader->end();
        window.seal();
        state->busy.store(false, std::memory_order_release);
        complete_entries(complete, std::move(status), std::move(entry), end);
    }

    std::shared_ptr<State>                      state;
    SnapshotReadWindow                          window;
    Complete                                    complete;
    std::shared_ptr<Window>                     owner;
    detail::CallbackSender::Operation<Receiver> connected;
    ApplyDispatcher::Node                       ready{&Window::start, this};
};

Status PublishedSnapshotEntries::create(std::shared_ptr<PublishedReadViewContext>  snapshot,
                                        std::shared_ptr<PublishedSnapshotEntries> *out, const KeyRange &range)
try {
    if (!snapshot || out == nullptr || *out) {
        return Status::invalid_argument("snapshot entry cursor requires fixed inputs and empty output");
    }
    auto status = range.validate();
    if (!status.ok()) {
        return status;
    }
    auto limits = snapshot->read_limits();
    if ((range.start() && range.start()->size() > limits.max_key_bytes) ||
        (range.end() && range.end()->size() > limits.max_key_bytes)) {
        return Status::invalid_argument("snapshot range exceeds key budget");
    }
    limits.max_entries = 1;
    // Snapshot capture removes L0. Its fixed mapping cannot observe future
    // source writes, so neither source cursors nor their key copies are needed.
    limits.max_memtables   = 1;
    const auto root        = snapshot->view()->mapping.get(snapshot->view()->root_page_id);
    limits.max_route_depth = root ? root->level + 1ULL : 1;
    if (limits.max_route_depth > 64) {
        return Status::resource_exhausted("snapshot entry cursor exceeds route depth bound");
    }
    auto metadata   = limits.metadata;
    auto state      = detail::reserved_shared<State>(metadata);
    state->snapshot = std::move(snapshot);
    state->metadata = metadata;
    state->loader   = detail::reserved_shared<SnapshotPageLoader>(metadata);
    limits.metadata = metadata;
    PageKeyRange selection;
    if (range.start()) {
        selection.lower = OwnedKey(Slice(*range.start()), metadata);
    }
    if (range.end()) {
        selection.upper = OwnedKey(Slice(*range.end()), metadata);
    }
    const auto &domain = state->snapshot->range();
    if (domain.lower && (!selection.lower || *domain.lower > *selection.lower)) {
        selection.lower = domain.lower;
    }
    if (domain.upper && (!selection.upper || *domain.upper < *selection.upper)) {
        selection.upper = domain.upper;
    }
    status =
        PublishedScan::create(state->snapshot->view(), state->loader, std::move(selection), false, limits, &state->scan,
                              {.start = {}, .include_tombstones = true, .allow_oversized_first = true});
    if (!status.ok()) {
        return status;
    }
    *out = detail::reserved_construct<PublishedSnapshotEntries>(metadata, [&](auto *storage) {
        ::new (static_cast<void *>(storage)) PublishedSnapshotEntries(std::move(state));
    });
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot entry cursor allocation failed");
}

Status PublishedSnapshotEntries::next_async(Complete complete)
{
    return next_impl(std::move(complete), nullptr);
}

Status PublishedSnapshotEntries::next_impl(Complete complete, std::shared_ptr<TreeCore> *host_core)
{
    if (!complete) {
        return Status::invalid_argument("read-only view entry cursor needs a completion");
    }
    auto state = state_;
    if (host_core == nullptr && !state->snapshot->detached()) {
        return Status::unavailable("read-only view borrows its backend");
    }
    if (state->busy.exchange(true, std::memory_order_acq_rel)) {
        return Status::unavailable("read-only view entry cursor already has an active window");
    }

    struct Leave
    {
        State *state;

        ~Leave()
        {
            if (state != nullptr) {
                state->busy.store(false, std::memory_order_release);
            }
        }
    } leave{state.get()};

    if (!state->failure.ok()) {
        return state->failure;
    }
    if (state->complete) {
        leave.state = nullptr;
        state->busy.store(false, std::memory_order_release);
        complete_entries(complete, Status::Ok(), {}, true);
        return Status::Ok();
    }
    try {
        SnapshotReadWindow window;
        auto               status = state->snapshot->prepare_window(&window);
        if (!status.ok()) {
            state->failure = status.without_message();
            return status;
        }
        if (host_core != nullptr) {
            // A borrowed backend must release its final Core on the host
            // after the read ticket and callback have finished.
            *host_core = std::move(window.host_core);
        }
        auto pending =
            detail::reserved_shared<State::Window>(state->metadata, state, std::move(window), std::move(complete));
        status = state->loader->begin(pending->window.loader);
        if (!status.ok()) {
            state->failure = status.without_message();
            return status;
        }
        pending->owner = pending;
        leave.state    = nullptr;
        if (host_core != nullptr) {
            State::Window::start(pending.get());
        }
        else {
            state->snapshot->binding()->view_cpu_progress()->request(pending->ready, pending);
        }
        return Status::Ok();
    }
    catch (const std::bad_alloc &) {
        state->failure = Status::resource_exhausted();
        return state->failure;
    }
}

Status PublishedSnapshotEntries::next(std::optional<PublishedScanEntry> *out, bool *complete)
{
    if (out == nullptr || complete == nullptr) {
        return Status::invalid_argument("read-only view entry cursor needs result outputs");
    }
    out->reset();
    *complete = false;
    std::shared_ptr<TreeCore> host_core;
    detail::HostCompletion    done;
    Status                    terminal;
    auto                      status = next_impl(
        [&](Status result, std::optional<PublishedScanEntry> entry, bool end) noexcept {
            terminal  = std::move(result);
            *out      = std::move(entry);
            *complete = end;
            done.signal();
        },
        &host_core);
    if (!status.ok()) {
        return status;
    }
    done.wait();
    return terminal;
}

uint64_t PublishedSnapshotEntries::skipped_subtrees() const noexcept
{
    return state_->scan->skipped_subtrees();
}
} // namespace crowdb::tree
