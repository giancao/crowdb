// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/read_value_bound.h"
#include "snapshot/operation_wait.h"
#include "snapshot/published_snapshot.h"

namespace crowdb::tree
{
namespace
{
struct ReadViewGet
{
    struct Receiver
    {
        using receiver_concept = stdexec::receiver_tag;
        ReadViewGet *read;

        void set_value() const noexcept
        {
            read->finish(Status::Ok());
        }

        void set_error(Status status) const noexcept
        {
            read->finish(std::move(status));
        }

        void set_stopped() const noexcept
        {
            read->finish(Status::unavailable("read stopped"));
        }
    };

    ReadViewGet(SnapshotReadWindow accepted, std::shared_ptr<PublishedGet> get,
                std::function<void(Status, GetView)> done)
        : window(std::move(accepted)),
          read(std::move(get)),
          complete(std::move(done)),
          operation(stdexec::connect(read->sender(), Receiver{this}))
    {
    }

    void finish(Status status) noexcept
    {
        auto    self = std::move(owner);
        GetView result;
        if (status.ok() && read->result().found) {
            const auto &cell = read->result().cell;
            result           = GetView::from_owner(cell.slot, cell.value, cell.owner);
        }
        try {
            complete(std::move(status), std::move(result));
        }
        catch (...) {
            // Completion is terminal; a throwing consumer cannot be retried.
            return;
        }
    }

    SnapshotReadWindow                          window;
    std::shared_ptr<PublishedGet>               read;
    std::function<void(Status, GetView)>        complete;
    std::shared_ptr<ReadViewGet>                owner;
    detail::CallbackSender::Operation<Receiver> operation;
};

Status prepare_get(const PublishedReadViewContext &context, Slice key, SnapshotReadWindow *window,
                   std::shared_ptr<PublishedGet> *read)
{
    auto       limits = context.read_limits();
    const auto root   = context.view()->mapping.get(context.view()->root_page_id);
    if (!root || root->level >= limits.max_route_depth) {
        return Status::corruption("snapshot has no bounded published root");
    }
    limits.max_route_depth = root->level + 1ULL;
    auto status            = context.prepare_window(window);
    if (!status.ok()) {
        return status;
    }
    limits.metadata = window->budget.bytes;
    return PublishedGet::create(context.view(), window->loader, key, limits, read);
}
} // namespace

Status PublishedReadViewContext::get(Slice key, GetView *out) const
try {
    if (out == nullptr) {
        return Status::invalid_argument("snapshot point read needs output");
    }
    *out = {};
    if (!range().contains(key)) {
        return Status::Ok();
    }
    SnapshotReadWindow            window;
    std::shared_ptr<PublishedGet> read;
    auto                          status = prepare_get(*this, key, &window, &read);
    if (status.ok()) {
        status = detail::wait_snapshot_operation(read);
    }
    if (status.ok() && read->result().found) {
        const auto &cell = read->result().cell;
        *out             = GetView::from_owner(cell.slot, cell.value, cell.owner);
    }
    window.seal();
    return status;
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("snapshot point read allocation failed");
}

Status PublishedReadViewContext::get_async(Slice key, std::function<void(Status, GetView)> complete) const
try {
    if (!complete) {
        return Status::invalid_argument("point read needs a completion");
    }
    if (!detached() || closed()) {
        return Status::unavailable("read-only view is closed or borrows its backend");
    }
    if (!range().contains(key)) {
        try {
            complete(Status::Ok(), {});
        }
        catch (...) {
            // Completion is terminal even if the consumer throws.
            return Status::Ok();
        }
        return Status::Ok();
    }
    SnapshotReadWindow            window;
    std::shared_ptr<PublishedGet> read;
    auto                          status = prepare_get(*this, key, &window, &read);
    if (!status.ok()) {
        return status;
    }
    auto credit = window.budget.bytes;
    auto pending =
        detail::reserved_shared<ReadViewGet>(credit, std::move(window), std::move(read), std::move(complete));
    pending->owner = pending;
    stdexec::start(pending->operation);
    return Status::Ok();
}
catch (const std::bad_alloc &) {
    return Status::resource_exhausted("read-only view point read allocation failed");
}

Status SnapshotPageLoader::begin(std::shared_ptr<PageLoader> loader)
{
    if (active_ || !loader) {
        return Status::unavailable("snapshot loader already has an active window");
    }
    active_ = std::move(loader);
    return Status::Ok();
}

void SnapshotPageLoader::end() noexcept
{
    active_.reset();
}

void SnapshotPageLoader::load_leaf(BaseRef base, LeafComplete complete)
{
    if (auto loader = active_) {
        loader->load_leaf(std::move(base), std::move(complete));
    }
    else {
        complete(Status::unavailable("snapshot loader has no active window"), {});
    }
}

void SnapshotPageLoader::load_inner(std::shared_ptr<const BackingRef> backing, InnerComplete complete)
{
    if (auto loader = active_) {
        loader->load_inner(std::move(backing), std::move(complete));
    }
    else {
        complete(Status::unavailable("snapshot loader has no active window"), {});
    }
}

void SnapshotPageLoader::load_overflow(std::shared_ptr<const BackingRef> primary, uint64_t head, uint64_t length,
                                       ValueComplete complete)
{
    if (auto loader = active_) {
        loader->load_overflow(std::move(primary), head, length, std::move(complete));
    }
    else {
        complete(Status::unavailable("snapshot loader has no active window"), {});
    }
}
} // namespace crowdb::tree
