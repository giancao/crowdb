// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "crowdb-tree/maptable/compressor.h"
#include "published_read_test_support.h"

#include <cstring>

namespace crowdb::tree::test_read
{
class TestColdLeafStore final : public AsyncPageStore
{
  public:
    std::vector<uint8_t> blob;
    bool                 deferred = false;
    AsyncCompletion      pending;

    uint64_t submit_read(PageAddr /*address*/, void *destination, size_t length, AsyncCompletion complete) override
    {
        EXPECT_EQ(length, blob.size());
        std::memcpy(destination, blob.data(), std::min(length, blob.size()));
        if (deferred) {
            pending = std::move(complete);
        }
        else {
            complete.complete(Status::Ok());
        }
        return 1;
    }

    uint64_t submit_write(PageAddr /*address*/, const void * /*source*/, size_t /*length*/,
                          AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return 0;
    }

    Status submit_fsync(AsyncCompletion complete) override
    {
        complete.complete(Status::not_supported());
        return Status::not_supported();
    }

    void cancel(uint64_t /*id*/) override
    {
    }
};

struct TestColdLeaf
{
    std::shared_ptr<detail::FlushRuntime>       runtime = std::make_shared<detail::FlushRuntime>(1, 8);
    std::shared_ptr<detail::FlushRuntime::Lane> lane    = std::make_shared<detail::FlushRuntime::Lane>(8);
    std::shared_ptr<TestColdLeafStore>          store   = std::make_shared<TestColdLeafStore>();
    std::vector<uint8_t>                        frame   = std::vector<uint8_t>(4096);
    BaseRef                                     reference;

    explicit TestColdLeaf(bool overflow = false)
    {
        LeafFrameBuilder builder(frame.data(), static_cast<uint32_t>(frame.size()));
        for (const auto &record : std::initializer_list<Record>{
                 {.key = "a", .slot = 6, .value = "value"},
                 {.key = "b", .slot = 4, .value = "base"},
                 {.key = "c", .slot = 3, .value = {}, .tombstone = true},
                 {.key = "new", .slot = 5, .value = "first"},
                 {.key = "tie", .slot = 7, .value = "base-tie"},
                 {.key = "zero", .slot = 0, .value = {}, .tombstone = true}
        }) {
            const auto cell =
                encode_cell_buf(record.slot, record.tombstone ? OpKind::kDelete : OpKind::kPut, Slice(record.value));
            EXPECT_TRUE(builder.try_append_sorted(record.key, Slice(cell)));
        }
        if (overflow) {
            EXPECT_TRUE(builder.try_append_sorted("zzz", Slice(encode_overflow_cell_buf(7, 41, 16384))));
        }
        builder.finish(17, 23);
        auto page_block   = std::make_shared<PageBlockRef>();
        page_block->owner = std::make_shared<int>(1);
        reference         = {.identity = 1, .max_slot = 7, .page_block = page_block};
        encode();
    }

    void encode()
    {
        EXPECT_TRUE(frame_validate(frame.data(), static_cast<uint32_t>(frame.size())));
        encode_durable_page(frame.data(), static_cast<uint32_t>(frame.size()), compress_algo::kNone, &store->blob);
        auto page_block      = std::make_shared<PageBlockRef>(*reference.page_block);
        page_block->length   = store->blob.size();
        reference.page_block = std::move(page_block);
    }

    std::shared_ptr<AsyncStorePageLoader> loader(PageLoadLimits limits = {}, PageLoadBudget budget = {})
    {
        return std::make_shared<AsyncStorePageLoader>(store, runtime, lane, limits, OverflowPageBlockResolver{},
                                                      budget);
    }

    void complete(detail::CallbackSender sender) const
    {
        Completion done;
        auto       operation = stdexec::connect(sender, Receiver{&done});
        stdexec::start(operation);
        runtime->drain();
        EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
        EXPECT_EQ(done.errors.load(), 0U);
    }
};
} // namespace crowdb::tree::test_read
