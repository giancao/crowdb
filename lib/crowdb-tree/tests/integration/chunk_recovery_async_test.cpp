// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "backend/local/scheduled_page_store.h"
#include "common/test_tree_core_chunk.h"

#include <future>

namespace crowdb::tree
{
namespace
{
void probe_worker(ScheduledPageStore &store)
{
    std::promise<Status> completed;
    auto                 ready = completed.get_future();
    ASSERT_TRUE(store
                    .submit_step([] { return Status::Ok(); },
                                 {.context = &completed,
                                  .complete_fn =
                                      +[](void *raw, Status status) {
                                          static_cast<std::promise<Status> *>(raw)->set_value(std::move(status));
                                      }})
                    .ok());
    ASSERT_EQ(ready.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_TRUE(ready.get().ok());
}
} // namespace

TEST(ChunkRecoveryAsync, RootAndIndexReadsReleaseTheOnlyBackendWorkerUntilTheirRepliesArrive)
{
    test_chunk::TestCoreChunk test;
    ASSERT_TRUE(test.open().ok());
    const std::string value(12000, 'v');
    ASSERT_TRUE(test.put(1, value).ok());
    ASSERT_TRUE(test.tree->flush_with_persistent().ok());
    test.tree.reset();
    test.backend.catalog            = test.backend.catalog->next_owner();
    test.backend.transport->catalog = test.backend.catalog;
    test.configure_store(0);

    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(test.config.runtime, {}, &binding).ok());
    MemPageStore                    memory(4096);
    std::shared_ptr<AsyncPageStore> probe;
    ASSERT_TRUE(ScheduledPageStore::create(&memory, nullptr, {}, binding, binding->lifecycle_budget(),
                                           binding->backend_lane(), &probe)
                    .ok());
    auto                          scheduled = std::static_pointer_cast<ScheduledPageStore>(probe);
    std::promise<AsyncCompletion> root;
    auto                          root_reply    = root.get_future();
    test.backend.catalog->after_root_load_async = [&](auto complete) {
        root.set_value(std::move(complete));
        return true;
    };
    std::promise<detail::ChunkTransportCompletion> index;
    auto                                           index_reply = index.get_future();
    std::promise<detail::ChunkTransportCompletion> frame;
    auto                                           frame_reply = frame.get_future();
    std::promise<void>                             sibling;
    auto                                           sibling_started = sibling.get_future();
    size_t                                         reads           = 0;
    test.backend.transport->after_read_async                       = [&](auto complete) {
        const auto submitted = reads++;
        if (submitted == 0) {
            index.set_value(std::move(complete));
            return true;
        }
        if (submitted == 1) {
            frame.set_value(std::move(complete));
            return true;
        }
        if (submitted == 2) {
            sibling.set_value();
        }
        return false;
    };

    std::promise<std::pair<Status, std::shared_ptr<TreeCore>>> result;
    auto                                                       open    = result.get_future();
    const auto                                                 factory = test.store->runtime_async_factory();
    ASSERT_TRUE(factory(test.config, [&](Status status, std::shared_ptr<TreeCore> tree) {
                    result.set_value({std::move(status), std::move(tree)});
                }).ok());
    ASSERT_EQ(root_reply.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(open.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    probe_worker(*scheduled);
    ASSERT_TRUE(scheduled
                    ->submit_step(
                        [reply = root_reply.get()] {
                            reply.complete(Status::Ok());
                            return Status::Ok();
                        },
                        {.complete_fn = +[](void *, Status status) { EXPECT_TRUE(status.ok()); }})
                    .ok());
    ASSERT_EQ(index_reply.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(open.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    probe_worker(*scheduled);
    ASSERT_TRUE(scheduled
                    ->submit_step(
                        [reply = index_reply.get()] {
                            reply.complete(Status::Ok());
                            return Status::Ok();
                        },
                        {.complete_fn = +[](void *, Status status) { EXPECT_TRUE(status.ok()); }})
                    .ok());
    ASSERT_EQ(frame_reply.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(sibling_started.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(open.wait_for(std::chrono::seconds(0)), std::future_status::timeout);
    ASSERT_TRUE(scheduled
                    ->submit_step(
                        [reply = frame_reply.get()] {
                            reply.complete(Status::Ok());
                            return Status::Ok();
                        },
                        {.complete_fn = +[](void *, Status status) { EXPECT_TRUE(status.ok()); }})
                    .ok());
    ASSERT_EQ(open.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    auto [status, core] = open.get();
    ASSERT_TRUE(status.ok()) << status.to_string();
    TreeCoreAccess::finish_open(core, &test.tree);
    test.backend.catalog->after_root_load_async = {};
    test.backend.transport->after_read_async    = {};
    std::string restored;
    uint64_t    slot = 0;
    ASSERT_TRUE(test.tree->get("key", &slot, &restored));
    EXPECT_EQ(slot, 1U);
    EXPECT_EQ(restored, value);
    binding->drain();
}

TEST(ChunkRecoveryAsync, FailedBootstrapDrainsPrivateCacheOnOneWorker)
{
    test_chunk::TestCoreChunk test;
    test.config.leaf_split_bytes = 0;
    std::promise<Code> finished;
    auto               ready   = finished.get_future();
    const auto         factory = test.store->runtime_async_factory();
    ASSERT_TRUE(factory(test.config, [&](Status status, std::shared_ptr<TreeCore> tree) {
                    EXPECT_FALSE(tree);
                    finished.set_value(status.code());
                }).ok());
    ASSERT_EQ(ready.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(ready.get(), Code::kInvalidArgument);

    std::promise<Code> drained;
    auto               done = drained.get_future();
    ASSERT_TRUE(test.config.runtime
                    ->drain_async({.context = &drained,
                                   .complete_fn =
                                       [](void *context, Status status) {
                                           static_cast<std::promise<Code> *>(context)->set_value(status.code());
                                       }})
                    .ok());
    ASSERT_EQ(done.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(done.get(), Code::kOk);
    EXPECT_EQ(test.config.runtime->snapshot().bindings, 0U);
    EXPECT_GT(test.backend.catalog->queue->pump(64).completed, 0U);
    EXPECT_EQ(test.backend.catalog->writer_releases, 1U);
}
} // namespace crowdb::tree
