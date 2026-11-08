// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/tree_core_access.h"
#include "c_api_internal.h"
#include "crowdb-tree/c_api.h"
#include "runtime/tree_runtime_binding.h"
#include "runtime/tree_runtime_c_api.h"

#include <gtest/gtest.h>

#include <chrono>
#include <future>
#include <vector>

namespace crowdb::tree
{
namespace
{
TEST(TreeRuntimeCAbi, NullRuntimeUsesTheProcessHostAndEnforcesWriterCredit)
{
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.limits.writer_bytes = 1024 * 1024;
    Config first;
    Config second;
    ASSERT_TRUE(configure_tree_runtime(nullptr, &binding, &first).ok());
    ASSERT_TRUE(configure_tree_runtime(nullptr, nullptr, &second).ok());
    ASSERT_EQ(first.runtime, second.runtime);
    EXPECT_EQ(first.runtime_binding.limits.writer_bytes, 1024U * 1024U);

    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, nullptr, &binding, &tree), 0);
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    EXPECT_EQ(core->config().runtime, first.runtime);
    EXPECT_EQ(core->binding()->options().limits.writer_bytes, 1024U * 1024U);
    const std::vector<uint8_t> value(2 * 1024 * 1024);
    EXPECT_EQ(ct_apply_put(tree, 1, reinterpret_cast<const uint8_t *>("key"), 3, value.data(), value.size()),
              static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(core->contiguous_slot(), 0U);
    ct_close(tree);
}

TEST(TreeRuntimeCAbi, NullRuntimeDoesNotBypassUnfundedOpenOrRebuildBinding)
{
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.lifecycle_bytes = 1;
    ct_options options{};
    ct_tree   *tree = nullptr;
    EXPECT_EQ(ct_open_with_runtime(&options, nullptr, &binding, &tree), static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(tree, nullptr);

    ct_tree *source = nullptr;
    ASSERT_EQ(ct_open(&options, &source), 0);
    ct_page_store *store = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(1, &store), 0);
    options.page_store = store;
    EXPECT_EQ(ct_rebuild_range_with_runtime(source, &options, nullptr, &binding, &tree, nullptr),
              static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(tree, nullptr);
    ct_page_store_free(store);
    ct_close(source);
}

TEST(TreeRuntimeCAbi, NullRuntimeKeepsTheDefaultReloadableMemoryBackend)
{
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, nullptr, nullptr, &tree), 0);
    const uint8_t key   = 'k';
    const uint8_t value = 'v';
    ASSERT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    uint64_t generation = 0;
    uint64_t coverage   = 0;
    EXPECT_EQ(ct_flush_with_persistent_info(tree, &generation, &coverage), 0);
    EXPECT_GT(generation, 0U);
    EXPECT_EQ(coverage, 1U);
    ct_close(tree);
}

TEST(TreeRuntimeCAbi, AsyncMemBlockOpenDoesNotNeedAPath)
{
    using namespace std::chrono_literals;
    ct_runtime *handle = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &handle), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(handle, &ct_runtime_free);

    ct_options options{};
    options.page_store_kind = CT_PAGE_STORE_MEM;
    using Reply             = std::promise<std::pair<ct_status, ct_tree *>>;
    auto      *reply        = new Reply;
    auto       opened       = reply->get_future();
    const auto accepted     = ct_open_with_runtime_async(
        &options, handle, nullptr,
        [](void *context, ct_status status, ct_tree *tree) {
            std::unique_ptr<Reply> owner(static_cast<Reply *>(context));
            owner->set_value({status, tree});
        },
        reply);
    if (accepted != 0) {
        delete reply;
    }
    ASSERT_EQ(accepted, 0);
    ASSERT_EQ(opened.wait_for(3s), std::future_status::ready);
    const auto [status, tree] = opened.get();
    std::unique_ptr<ct_tree, decltype(&ct_close)> owned_tree(tree, &ct_close);
    ASSERT_EQ(status, 0);
    ASSERT_NE(tree, nullptr);
    const uint8_t key   = 'k';
    const uint8_t value = 'v';
    EXPECT_EQ(ct_apply_put(tree, 1, &key, 1, &value, 1), 0);
    uint64_t generation = 0;
    uint64_t coverage   = 0;
    EXPECT_EQ(ct_flush_with_persistent_info(tree, &generation, &coverage), 0);
    EXPECT_EQ(coverage, 1U);
}

TEST(TreeRuntimeCAbi, VersionedOptionsRejectWrongSizeAndUnfundedStorage)
{
    ct_runtime_options options{};
    ct_runtime_options_default(&options);
    EXPECT_EQ(options.struct_size, sizeof(options));
    ct_runtime *runtime = nullptr;
    --options.struct_size;
    EXPECT_EQ(ct_runtime_create(&options, &runtime), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(runtime, nullptr);
    ++options.struct_size;
    options.lifecycle_bytes = 1;
    EXPECT_EQ(ct_runtime_create(&options, &runtime), static_cast<int32_t>(Code::kResourceExhausted));
    EXPECT_EQ(runtime, nullptr);
    ct_runtime_free(nullptr);
}

TEST(TreeRuntimeCAbi, FreeHandleRetainsIndependentConfigAndBindingOwners)
{
    ct_runtime *handle = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &handle), 0);
    Config config;
    ASSERT_TRUE(configure_tree_runtime(handle, nullptr, &config).ok());
    auto                                owner = config.runtime;
    std::shared_ptr<TreeRuntimeBinding> binding;
    ASSERT_TRUE(TreeRuntimeBinding::create(owner, config.runtime_binding, &binding).ok());
    const auto before = owner->snapshot().allocated.routing_bytes;
    ct_runtime_free(handle);
    EXPECT_FALSE(owner->stopped());
    EXPECT_LT(owner->snapshot().allocated.routing_bytes, before);
    config.runtime.reset();
    owner.reset();
    EXPECT_TRUE(binding->cpu()->try_submit(binding->lane(), [](void *) noexcept {}, nullptr).ok());
    binding->cpu()->drain();
}

TEST(TreeRuntimeCAbi, OpenRuntimeUsesOwnedCoreAndRejectsInvalidBinding)
{
    ct_runtime *runtime = nullptr;
    ASSERT_EQ(ct_runtime_create(nullptr, &runtime), 0);
    ct_options options{};
    ct_tree   *tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, runtime, nullptr, &tree), 0);
    ASSERT_NE(tree, nullptr);
    uint64_t checkpoint = 0;
    EXPECT_EQ(ct_flush_with_persistent(tree, &checkpoint), static_cast<int32_t>(Code::kUnavailable));
    ct_close(tree);
    tree = nullptr;
    ct_tree_runtime_options binding{};
    ct_tree_runtime_options_default(&binding);
    binding.cpu_queue_limit = 0;
    EXPECT_EQ(ct_open_with_runtime(&options, runtime, &binding, &tree), static_cast<int32_t>(Code::kInvalidArgument));
    EXPECT_EQ(tree, nullptr);
    ct_runtime_stop_admission(runtime);
    Config config;
    EXPECT_EQ(configure_tree_runtime(runtime, nullptr, &config).code(), Code::kUnavailable);
    ct_runtime_drain_cpu(runtime);
    ct_runtime_free(runtime);
}

TEST(TreeRuntimeCAbi, ConsumingCloseLetsTheOnlyWorkerRunBeforeRetirementCompletes)
{
    using namespace std::chrono_literals;
    ct_runtime_options runtime_options{};
    ct_runtime_options_default(&runtime_options);
    runtime_options.workers = 1;
    ct_runtime *handle      = nullptr;
    ASSERT_EQ(ct_runtime_create(&runtime_options, &handle), 0);
    std::unique_ptr<ct_runtime, decltype(&ct_runtime_free)> runtime(handle, &ct_runtime_free);
    ct_page_store                                          *store = nullptr;
    ASSERT_EQ(ct_page_store_open_mem(1, &store), 0);
    std::unique_ptr<ct_page_store, decltype(&ct_page_store_free)> owned_store(store, &ct_page_store_free);
    ct_options                                                    options{};
    options.page_store = store;
    ct_tree *tree      = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, handle, nullptr, &tree), 0);
    EXPECT_EQ(ct_close_async(tree, nullptr, nullptr), static_cast<ct_status>(Code::kInvalidArgument));
    auto *core       = TreeCoreAccess::get(*c_api_tree(tree));
    auto  cpu        = core->binding()->cpu();
    auto  retirement = core->accepted_allocations()->retirement_started();
    ASSERT_NE(retirement.finish, nullptr);

    struct Probe
    {
        ct_tree                *tree;
        std::promise<ct_status> submitted;
        std::promise<ct_status> completed;

        static void close(void *argument) noexcept
        {
            auto &probe = *static_cast<Probe *>(argument);
            probe.submitted.set_value(ct_close_async(probe.tree, &Probe::complete, &probe));
        }

        static void complete(void *argument, ct_status status) noexcept
        {
            static_cast<Probe *>(argument)->completed.set_value(status);
        }
    } probe{.tree = tree, .submitted = {}, .completed = {}};

    auto       submitted = probe.submitted.get_future();
    auto       completed = probe.completed.get_future();
    auto       lane      = std::make_shared<detail::FlushRuntime::Lane>(1);
    const auto queued    = cpu->try_submit(lane, &Probe::close, &probe);
    const auto returned  = queued.ok() ? submitted.wait_for(3s) : std::future_status::deferred;
    EXPECT_TRUE(queued.ok());
    EXPECT_EQ(returned, std::future_status::ready);
    EXPECT_EQ(completed.wait_for(0ms), std::future_status::timeout);
    retirement.finish(retirement.owner.get());
    lane->drain();
    if (!queued.ok() || submitted.get() != 0) {
        ct_close(tree);
        FAIL() << "close was not accepted";
    }
    ASSERT_EQ(completed.wait_for(3s), std::future_status::ready);
    EXPECT_EQ(completed.get(), 0);
    // Closing releases the allocation authority before notifying the caller.
    tree = nullptr;
    ASSERT_EQ(ct_open_with_runtime(&options, handle, nullptr, &tree), 0);
    ct_close(tree);
}
} // namespace
} // namespace crowdb::tree
