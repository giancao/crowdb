// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "crowdb-tree/backend/backend_release_service.h"
#include "crowdb-tree/backend/block_page_store.h"
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"
#    include "crowdb-common/log.h"
#endif

#include <unistd.h>

#include <cassert>
#include <cstddef>

namespace crowdb::tree
{
struct FileMedium::CloseNode : BackendReleaseService::Node
{
    std::shared_ptr<AllocationBudget> budget;
    int                               fd = -1;
#ifdef CROWDB_HAVE_LIBURING
    int result = 0;
#endif

    using Allocator = MetadataAllocator<std::max_align_t>;

    static constexpr size_t words(size_t bytes) noexcept
    {
        return (bytes + sizeof(std::max_align_t) - 1) / sizeof(std::max_align_t);
    }

    static size_t storage_words() noexcept
    {
        static_assert(alignof(CloseNode) <= alignof(std::max_align_t));
        auto count = words(sizeof(CloseNode));
#ifdef CROWDB_HAVE_LIBURING
        count += words(common::DiskIOUring::owned_submission_allocation_bytes());
#endif
        return count;
    }

    static void destroy(CloseNode *node) noexcept
    {
        Allocator allocator(node->budget);
        std::destroy_at(node);
        allocator.deallocate(reinterpret_cast<std::max_align_t *>(node), storage_words());
    }

#ifdef CROWDB_HAVE_LIBURING
    static void *allocate_submission(void *context, size_t bytes, size_t alignment) noexcept
    {
        assert(bytes == common::DiskIOUring::owned_submission_allocation_bytes());
        assert(alignment <= alignof(std::max_align_t));
        (void)bytes;
        (void)alignment;
        const auto offset = words(sizeof(CloseNode)) * sizeof(std::max_align_t);
        return static_cast<std::byte *>(context) + offset;
    }

    static void release_submission(void *context, void * /*storage*/, size_t /*bytes*/, size_t /*alignment*/) noexcept
    {
        auto &node = *static_cast<CloseNode *>(context);
        // The CallbackEntry has already been destroyed. The host can now free
        // this node and its inline storage.
        node.fd = -1;
        BackendReleaseService::resume(node);
    }

    common::IOCompletion completion() noexcept
    {
        return {
            .context  = this,
            .callback = [](void *context, int result) noexcept { static_cast<CloseNode *>(context)->result = result; },
            .allocation =
            {.owner      = service,
                                                                .context    = this,
                                                                .allocate   = &allocate_submission,
                                                                .deallocate = &release_submission }
        };
    }
#endif

    explicit CloseNode(std::shared_ptr<AllocationBudget> owner) : budget(std::move(owner))
    {
        dispose = [](BackendReleaseService::Node *base, common::DiskIOUring *reactor) noexcept {
            auto *node = static_cast<CloseNode *>(base);
#ifdef CROWDB_HAVE_LIBURING
            if (reactor != nullptr && node->fd >= 0) {
                const auto status = reactor->try_submit_close_owned(node->fd, node->completion());
                if (status == 0) {
                    return BackendReleaseService::Disposal::kPending;
                }
                if (node->result != status) {
                    CRB_LOG_WARN("io_uring close submission failed for fd {}: errno {}", node->fd, -status);
                }
                node->result = status;
                return BackendReleaseService::Disposal::kMore;
            }
            if (node->result < 0 && node->fd < 0) {
                CRB_LOG_WARN("io_uring close completed with errno {}", -node->result);
            }
#else
            (void)reactor;
#endif
            // close consumes this descriptor incarnation even on Linux EINTR.
            // Retrying the numeric descriptor could close an unrelated file.
            if (node->fd >= 0) {
                (void)::close(node->fd);
            }
            destroy(node);
            return BackendReleaseService::Disposal::kDone;
        };
    }
};

Status FileMedium::prepare_close(const std::shared_ptr<AllocationBudget>      &budget,
                                 const std::shared_ptr<BackendReleaseService> &service)
{
    if (!service) {
        return {};
    }
    CloseNode::Allocator allocator(budget);
    auto *node   = ::new (static_cast<void *>(allocator.allocate(CloseNode::storage_words()))) CloseNode(budget);
    auto  status = service->register_node(*node);
    if (!status.ok()) {
        CloseNode::destroy(node);
        return status;
    }
    close_ = node;
    return {};
}

FileMedium::~FileMedium()
{
    if (close_ != nullptr) {
        close_->fd = fd_;
        BackendReleaseService::enqueue(*close_);
    }
    else if (fd_ >= 0) {
        // Unique legacy tooling has an explicit synchronous host boundary.
        (void)::close(fd_);
    }
}

size_t FileMedium::close_allocation_bound() noexcept
{
    return CloseNode::storage_words() * sizeof(std::max_align_t);
}
} // namespace crowdb::tree
