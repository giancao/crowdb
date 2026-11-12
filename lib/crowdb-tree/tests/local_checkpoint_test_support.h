// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once

#include "captured_write_test_support.h"
#include "crowdb-tree/backend/block_page_store.h"
#include "crowdb-tree/crowdb-tree.h"
#include "snapshot/local_checkpoint.h"

#include <atomic>
#include <functional>
#include <mutex>

namespace crowdb::tree::test_write
{
class TestCheckpointStore final : public AsyncPageStore
{
  public:
    std::shared_ptr<PageStore>                                       device;
    std::function<bool(PageAddr, void *, size_t, AsyncCompletion &)> defer_read;
    std::vector<std::string>                                         events;
    size_t                                                           defer_at        = 0;
    size_t                                                           fail_at         = 0;
    size_t                                                           fail_count      = 1;
    size_t                                                           throw_at        = 0;
    size_t                                                           inline_throw_at = 0;
    AsyncCompletion                                                  pending;
    std::atomic<bool>                                                deferred{false};

    [[nodiscard]] bool write_completion_is_reloadable() const noexcept override
    {
        return true;
    }

    uint64_t submit_read(PageAddr at, void *out, size_t size, AsyncCompletion callback) override
    {
        if (defer_read && defer_read(at, out, size, callback)) {
            return 0;
        }
        auto *block = dynamic_cast<BlockPageStore *>(device.get());
        callback.complete(block != nullptr ? block->read_at(at, static_cast<uint8_t *>(out), size, callback.allocations)
                                           : device->read_at(at, static_cast<uint8_t *>(out), size));
        return 0;
    }

    uint64_t submit_write(PageAddr at, const void *data, size_t size, AsyncCompletion callback) override
    {
        EXPECT_EQ(reinterpret_cast<uintptr_t>(data) % 4096, 0U);
        EXPECT_EQ(at % 4096, 0U);
        EXPECT_EQ(size % 4096, 0U);
        return submit(at < 8192 ? "anchor" : "metadata",
                      {.address = at, .input = static_cast<const uint8_t *>(data), .bytes = size}, std::move(callback));
    }

    Status submit_fsync(AsyncCompletion callback) override
    {
        submit("barrier", {.barrier = true}, std::move(callback));
        return Status::Ok();
    }

    void cancel(uint64_t /*id*/) override
    {
    }

    void finish()
    {
        AsyncCompletion callback;
        Write           work;
        {
            const std::scoped_lock lock(mutex_);
            callback = std::exchange(pending, {});
            work     = pending_write_;
            deferred.store(false, std::memory_order_release);
        }
        ASSERT_TRUE(callback);
        complete_write(work, callback);
    }

    void wait_deferred() const
    {
        while (!deferred.load(std::memory_order_acquire)) {
            deferred.wait(false, std::memory_order_acquire);
        }
    }

  private:
    struct Write
    {
        uint64_t       address = 0;
        const uint8_t *input   = nullptr;
        size_t         bytes   = 0;
        bool           barrier = false;
        bool           fail    = false;
    };

    uint64_t submit(const char *event, Write work, AsyncCompletion callback)
    {
        size_t ordinal;
        bool   throw_after;
        {
            const std::scoped_lock lock(mutex_);
            events.emplace_back(event);
            ordinal     = events.size();
            work.fail   = fail_at != 0 && ordinal >= fail_at && ordinal - fail_at < fail_count;
            throw_after = ordinal == inline_throw_at;
            if (ordinal == defer_at || ordinal == throw_at) {
                EXPECT_FALSE(pending);
                pending_write_ = work;
                pending        = std::exchange(callback, {});
                if (ordinal == throw_at) {
                    throw std::runtime_error("unknown acceptance");
                }
                deferred.store(true, std::memory_order_release);
                deferred.notify_all();
            }
        }
        if (callback) {
            complete_write(work, callback);
        }
        if (throw_after) {
            throw std::runtime_error("inline completion then failed submission");
        }
        return ordinal;
    }

    void complete_write(const Write &work, const AsyncCompletion &callback)
    {
        auto status = Status::io_error();
        if (!work.fail) {
            auto *block = dynamic_cast<BlockPageStore *>(device.get());
            if (block != nullptr) {
                status = work.barrier ? block->sync(callback.allocations)
                                      : block->write_at(work.address, work.input, work.bytes, callback.allocations);
            }
            else {
                status = work.barrier ? device->sync() : device->write_at(work.address, work.input, work.bytes);
            }
        }
        callback.complete(status);
    }

    std::mutex mutex_;
    Write      pending_write_;
};

struct TestCheckpoint
{
    TestCapturedWrite                         source;
    std::shared_ptr<TestCheckpointStore>      store = std::make_shared<TestCheckpointStore>();
    std::shared_ptr<LocalCheckpointRegistry>  registry;
    std::shared_ptr<LocalCheckpointOperation> operation;

    TestCheckpoint()
    {
        store->device = source.store->device;
        EXPECT_TRUE(
            LocalCheckpointRegistry::create(source.ledger, source.credits.credits.reservation(), {}, &registry).ok());
    }

    std::shared_ptr<const CapturedDurablePages> materialize(std::string value = "v", uint64_t coverage = 10)
    {
        auto captured      = captured_leaf(page({
            {.key = "a", .value = std::move(value)}
        }));
        captured->coverage = coverage;
        EXPECT_TRUE(source
                        .prepare(std::move(captured), {},
                                 {.frames = {.encode = {.frame_bytes = 4096, .inline_value_bytes = 16}},
                                  .write  = {.max_frame_bytes = 4096}})
                        .ok());
        Completion done;
        source.run(&done);
        EXPECT_EQ(done.values.load(), 1U) << done.status.to_string();
        return source.operation->result().pages;
    }

    Status prepare(std::shared_ptr<const CapturedDurablePages> pages = {}, LocalCheckpointLimits limits = {})
    {
        if (!pages) {
            pages = materialize();
        }
        return LocalCheckpointOperation::create(std::move(pages), registry, store, source.runtime, source.lane, limits,
                                                source.credits.credits, &operation);
    }

    void run(Completion *done)
    {
        auto connected = stdexec::connect(operation->sender(), Receiver{.done = done});
        stdexec::start(connected);
        source.runtime->drain();
    }

    void expect_read(const std::string &expected)
    {
        Config config;
        config.page_store  = store->device.get();
        config.frame_bytes = 4096;
        std::unique_ptr<Crowdbtree> tree;
        const auto                  status = Crowdbtree::open(config, &tree);
        ASSERT_TRUE(status.ok()) << status.to_string();
        std::string value;
        uint64_t    slot = 0;
        ASSERT_TRUE(tree->get(Slice("a"), &slot, &value));
        EXPECT_EQ(value, expected);
    }
};
} // namespace crowdb::tree::test_write
