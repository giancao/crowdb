// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#ifdef CROWDB_HAVE_LIBURING
#    include "crowdb-common/diskio_uring.h"

#    include <fcntl.h>
#    include <sys/stat.h>

#    include <cerrno>
#    include <chrono>
#    include <climits>
#    include <cstddef>
#    include <new>

namespace crowdb::common
{
size_t DiskIOUring::owned_submission_allocation_bytes() noexcept
{
    static_assert(alignof(CallbackEntry) <= alignof(std::max_align_t));
    return sizeof(CallbackEntry);
}

void DiskIOUring::complete_entry(CallbackEntry *entry, int result)
{
    if (entry->owned.callback != nullptr) {
        entry->owned.complete(result);
    }
    else if (entry->cb) {
        entry->cb(result);
    }
}

void DiskIOUring::delete_entry(CallbackEntry *entry) noexcept
{
    if (entry->owned.allocation.deallocate == nullptr) {
        delete entry;
        return;
    }
    auto allocation = std::move(entry->owned.allocation);
    std::destroy_at(entry);
    allocation.deallocate(allocation.context, entry, sizeof(CallbackEntry), alignof(CallbackEntry));
}

void DiskIOUring::submit_owned_command(int fd, const struct io_uring_sqe &command, IOCompletion complete)
{
    const auto result = try_submit_owned_command(fd, command, complete);
    if (result < 0) {
        complete.complete(result);
    }
}

int DiskIOUring::try_submit_owned_command(int fd, const struct io_uring_sqe &command, IOCompletion &complete)
{
    const bool closing = command.opcode == IORING_OP_CLOSE;
    const bool tracked = fd >= 0 && fd < fd_table_size_;
    if (fd < -1 || (!closing && fd >= fd_table_size_) || pipelines_.empty()) {
        return -EBADF;
    }
    if (complete.callback == nullptr || !complete.allocation.owner || complete.allocation.allocate == nullptr ||
        complete.allocation.deallocate == nullptr) {
        return -EINVAL;
    }
    auto &pipeline = *pipelines_[tracked && fd_table_[fd].registered ? fd_table_[fd].pipeline : 0];
    if (!pipeline.valid) {
        return -EIO;
    }
    if (closing && tracked && fd_in_flight_[fd].load(std::memory_order_acquire) != 0) {
        return -EBUSY;
    }
    CallbackEntry *entry = nullptr;
    try {
        void *storage =
            complete.allocation.allocate(complete.allocation.context, sizeof(CallbackEntry), alignof(CallbackEntry));
        if (storage == nullptr) {
            return -ENOMEM;
        }
        entry               = ::new (storage) CallbackEntry{};
        entry->owned        = std::move(complete);
        entry->fd           = closing ? -1 : fd;
        entry->command      = command;
        entry->queued_nanos = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
                .count());
    }
    catch (const std::bad_alloc &) {
        return -ENOMEM;
    }
    if (closing && tracked) {
        fd_table_[fd].registered = false;
    }
    enqueue_owned(pipeline, entry);
    return 0;
}

void DiskIOUring::submit_owned(int fd, void *buffer, size_t length, off_t offset, uint8_t opcode, bool data_only,
                               IOCompletion complete)
{
    if (fd < 0) {
        complete.complete(-EBADF);
        return;
    }
    if (length > INT_MAX || offset < 0) {
        complete.complete(-EINVAL);
        return;
    }
    struct io_uring_sqe command{};
    if (opcode == IORING_OP_READ) {
        ::io_uring_prep_read(&command, fd, buffer, static_cast<unsigned>(length), static_cast<__u64>(offset));
    }
    else if (opcode == IORING_OP_WRITE) {
        ::io_uring_prep_write(&command, fd, buffer, static_cast<unsigned>(length), static_cast<__u64>(offset));
    }
    else {
        ::io_uring_prep_fsync(&command, fd, data_only ? IORING_FSYNC_DATASYNC : 0);
    }
    submit_owned_command(fd, command, std::move(complete));
}

void DiskIOUring::submit_read_owned(int fd, void *buffer, size_t length, off_t offset, IOCompletion complete)
{
    submit_owned(fd, buffer, length, offset, IORING_OP_READ, false, std::move(complete));
}

void DiskIOUring::submit_write_owned(int fd, const void *buffer, size_t length, off_t offset, IOCompletion complete)
{
    submit_owned(fd, const_cast<void *>(buffer), length, offset, IORING_OP_WRITE, false, std::move(complete));
}

void DiskIOUring::submit_fsync_owned(int fd, bool data_only, IOCompletion complete)
{
    submit_owned(fd, nullptr, 0, 0, IORING_OP_FSYNC, data_only, std::move(complete));
}

void DiskIOUring::submit_open_owned(const char *path, int flags, mode_t mode, IOCompletion complete)
{
    if (path == nullptr) {
        complete.complete(-EINVAL);
        return;
    }
    struct io_uring_sqe command{};
    ::io_uring_prep_openat(&command, AT_FDCWD, path, flags, mode);
    submit_owned_command(-1, command, std::move(complete));
}

void DiskIOUring::submit_stat_owned(int fd, struct statx *output, IOCompletion complete)
{
    if (fd < 0 || output == nullptr) {
        complete.complete(fd < 0 ? -EBADF : -EINVAL);
        return;
    }
    struct io_uring_sqe command{};
    ::io_uring_prep_statx(&command, fd, "", AT_EMPTY_PATH, STATX_BASIC_STATS, output);
    submit_owned_command(fd, command, std::move(complete));
}

void DiskIOUring::submit_unlink_owned(const char *path, IOCompletion complete)
{
    if (path == nullptr) {
        complete.complete(-EINVAL);
        return;
    }
    struct io_uring_sqe command{};
    ::io_uring_prep_unlinkat(&command, AT_FDCWD, path, 0);
    submit_owned_command(-1, command, std::move(complete));
}

void DiskIOUring::submit_close_owned(int fd, IOCompletion complete)
{
    const auto result = try_submit_close_owned(fd, complete);
    if (result < 0) {
        complete.complete(result);
    }
}

int DiskIOUring::try_submit_close_owned(int fd, IOCompletion complete)
{
    if (fd < 0) {
        return -EBADF;
    }
    struct io_uring_sqe command{};
    ::io_uring_prep_close(&command, fd);
    return try_submit_owned_command(fd, command, complete);
}
} // namespace crowdb::common

#endif // CROWDB_HAVE_LIBURING
