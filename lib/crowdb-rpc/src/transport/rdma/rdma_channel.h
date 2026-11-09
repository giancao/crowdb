// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#pragma once

#ifdef CROWDB_RPC_HAVE_RDMA

#    include <fcntl.h>
#    include <rdma/rdma_cma.h>

namespace crowdb::rpc::rdma_detail
{
inline bool make_nonblocking(int fd)
{
    if (fd < 0) {
        return false;
    }
    const int flags = ::fcntl(fd, F_GETFL, 0);
    return flags >= 0 && ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

} // namespace crowdb::rpc::rdma_detail

#endif
