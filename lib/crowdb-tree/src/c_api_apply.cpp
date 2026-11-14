// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#include "btree/allocation_bound.h"
#include "btree/tree_apply_operation.h"
#include "btree/tree_core_access.h"
#include "btree/tree_core_batch.h"
#include "c_api_future_internal.h"
#include "c_api_internal.h"
#include "crowdb-common/log.h"
#include "crowdb-tree/apply_c_api.h"

using namespace crowdb::tree;

namespace
{
struct ApplyCall : std::enable_shared_from_this<ApplyCall>
{
    using Future = std::unique_ptr<ct_future_handle, decltype(&free_ct_future_handle)>;

    ApplyCall(std::shared_ptr<TreeApplyOperation> operation, std::shared_ptr<CompletionSignal> signal)
        : metadata(operation->allocation_budget()),
          operation(std::move(operation)),
          result(signal ? make_ct_future(metadata) : ct_future_handle{}),
          future(result ? make_ct_future_handle(result) : nullptr, free_ct_future_handle),
          signal(std::move(signal))
    {
        if (result) {
            result->kind = ct_future_impl::Kind::kApply;
        }
    }

    static void completed(void *context, detail::CallbackSignal signal, Status status) noexcept
    {
        auto      &call     = *static_cast<ApplyCall *>(context);
        auto       retained = std::move(call.self);
        const auto code =
            static_cast<ct_status>(signal == detail::CallbackSignal::kStopped ? Code::kUnavailable : status.code());
        call.operation.reset();
        if (call.callback != nullptr) {
            call.callback(call.context, code);
            return;
        }
        call.result->status = code;
        call.result->done.store(true, std::memory_order_release);
        c_api_notify_completion(call.signal);
    }

    std::shared_ptr<detail::AllocationReservation> metadata;
    std::shared_ptr<TreeApplyOperation>            operation;
    ct_future_handle                               result;
    Future                                         future;
    std::shared_ptr<CompletionSignal>              signal;
    std::shared_ptr<ApplyCall>                     self;
    std::atomic_flag                               submitted = ATOMIC_FLAG_INIT;
    ct_apply_complete                              callback  = nullptr;
    void                                          *context   = nullptr;
};

using PermitHandle = std::shared_ptr<ApplyCall>;

PermitHandle *handle(ct_apply_permit *permit) noexcept
{
    return reinterpret_cast<PermitHandle *>(permit);
}

CoreWriteInput write_input(const ct_kv_ref &input) noexcept
{
    return {.key = Slice(input.key, input.key_len), .value = Slice(input.value, input.value_len), .flags = input.kind};
}

CoreWriteReader replay_reader(ct_replay_read read, void *context)
{
    if (read == nullptr) {
        return {};
    }
    return [read, context](size_t index) {
        ct_kv_ref input{};
        read(context, index, &input);
        return write_input(input);
    };
}
} // namespace

static int32_t apply_prepare(ct_tree *tree, const ct_kv_ref *operations, size_t count, int32_t gap_repair,
                             ct_apply_permit **out, bool polling, const ExternalValue *external = nullptr,
                             ApplyInputOwner *owner = nullptr)
try {
    if (tree == nullptr || out == nullptr || (count != 0 && operations == nullptr) ||
        (gap_repair != 0 && gap_repair != 1)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out       = nullptr;
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    for (size_t index = 0; index < count; ++index) {
        const auto &input = operations[index];
        if ((input.key == nullptr && input.key_len != 0) || (input.value == nullptr && input.value_len != 0) ||
            input.kind > 1) {
            return static_cast<int32_t>(Code::kInvalidArgument);
        }
    }
    constexpr size_t future_bytes = detail::shared_allocation_bound<ct_future_impl>() + sizeof(ct_future_handle);
    const size_t     retained =
        detail::shared_allocation_bound<ApplyCall>() + sizeof(PermitHandle) + (polling ? future_bytes : 0);
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = core->prepare_apply(
        count, [operations](size_t index) { return write_input(operations[index]); },
        gap_repair != 0 ? detail::BudgetClass::kGapRepair : detail::BudgetClass::kAdmittedWriter, retained, &operation,
        external, owner);
    if (!status.ok()) {
        if (status.code() == Code::kInvalidArgument) {
            CRB_LOG_WARN("apply admission rejected {} operations: {}", count, status.to_string());
        }
        return static_cast<int32_t>(status.code());
    }
    auto metadata = operation->allocation_budget();
    auto call     = detail::reserved_shared<ApplyCall>(metadata, std::move(operation),
                                                       polling ? c_api_completion_signal(tree) : nullptr);
    detail::ReservationAllocator<PermitHandle> allocator(metadata);
    auto                                      *result = allocator.allocate(1);
    std::construct_at(result, std::move(call));
    *out = reinterpret_cast<ct_apply_permit *>(result);
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

int32_t ct_apply_prepare(ct_tree *tree, const ct_kv_ref *operations, size_t count, int32_t gap_repair,
                         ct_apply_permit **out)
{
    return apply_prepare(tree, operations, count, gap_repair, out, true);
}

int32_t ct_apply_prepare_callback(ct_tree *tree, const ct_kv_ref *operations, size_t count, int32_t gap_repair,
                                  ct_apply_permit **out)
{
    return apply_prepare(tree, operations, count, gap_repair, out, false);
}

int32_t ct_apply_prepare_owned_callback(ct_tree *tree, const ct_kv_ref *operation, ct_apply_value_owner owner,
                                        ct_apply_permit **out)
{
    if (owner.drop_context == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    ApplyInputOwner     input(owner.drop_context, owner.context);
    const ExternalValue external{.initialized = Slice(owner.data, owner.length), .capacity = owner.capacity};
    return apply_prepare(tree, operation, 1, 0, out, false, &external, &input);
}

int32_t ct_apply_arm(ct_apply_permit *permit)
{
    if (permit == nullptr || !(*handle(permit))->operation) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    return static_cast<int32_t>((*handle(permit))->operation->arm().code());
}

int32_t ct_apply_submit(ct_apply_permit *permit, uint64_t slot, ct_future **out)
{
    if (permit == nullptr || out == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    *out      = nullptr;
    auto call = *handle(permit);
    if (!call->result || call->submitted.test_and_set(std::memory_order_acq_rel)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    call->self = call;
    *out       = reinterpret_cast<ct_future *>(call->future.release());
    call->operation->start(slot, &ApplyCall::completed, call.get());
    return 0;
}

int32_t ct_apply_submit_callback(ct_apply_permit *permit, uint64_t slot, ct_apply_complete complete, void *context)
{
    if (permit == nullptr || complete == nullptr) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    auto call = *handle(permit);
    if (call->result || call->submitted.test_and_set(std::memory_order_acq_rel)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    call->callback = complete;
    call->context  = context;
    call->self     = call;
    call->operation->start(slot, &ApplyCall::completed, call.get());
    return 0;
}

void ct_apply_permit_free(ct_apply_permit *permit)
{
    if (permit != nullptr) {
        auto                                      *owned = handle(permit);
        detail::ReservationAllocator<PermitHandle> allocator((*owned)->metadata);
        std::destroy_at(owned);
        allocator.deallocate(owned, 1);
    }
}

static int32_t apply_replay(ct_tree *tree, uint64_t slot, size_t count, CoreWriteReader read,
                            void (*drop_context)(void *), void *context, size_t owner_control_bytes, ct_future **out,
                            ct_apply_complete complete, void *completion_context,
                            const ExternalValue *external = nullptr)
try {
    ApplyInputOwner incoming(drop_context, context);
    const bool      polling = out != nullptr;
    if (tree == nullptr || (!polling && complete == nullptr) || drop_context == nullptr || !read ||
        (external != nullptr && context == nullptr)) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    if (polling) {
        *out = nullptr;
    }
    auto *core = TreeCoreAccess::get(*c_api_tree(tree));
    if (core == nullptr) {
        return static_cast<int32_t>(Code::kUnavailable);
    }
    for (size_t index = 0; index < count; ++index) {
        const auto input = read(index);
        if ((input.key.data() == nullptr && !input.key.empty()) ||
            (input.value.data() == nullptr && !input.value.empty()) || input.flags > 1 ||
            (external != nullptr && !external->contains(input.key))) {
            return static_cast<int32_t>(Code::kInvalidArgument);
        }
    }
    constexpr size_t future_bytes = detail::shared_allocation_bound<ct_future_impl>() + sizeof(ct_future_handle);
    const auto       retained     = detail::AllocationBound(detail::shared_allocation_bound<ApplyCall>())
                                        .add(detail::shared_allocation_bound<ApplyInputOwner>())
                                        .add(polling ? future_bytes : 0)
                                        .add(owner_control_bytes)
                                        .value();
    if (!retained) {
        return static_cast<int32_t>(Code::kInvalidArgument);
    }
    std::shared_ptr<TreeApplyOperation> operation;
    auto                                status = core->prepare_replay_adopt(
        slot, count, std::move(read),
        [&incoming, owner_control_bytes, external](const std::shared_ptr<detail::AllocationReservation> &budget,
                                                   ApplyInputOwner **retained_owner) -> std::shared_ptr<const void> {
            auto owner     = detail::reserved_shared<ApplyInputOwner>(budget, std::move(incoming));
            owner->control = AllocationLease(budget, owner_control_bytes, AllocationKind::kMetadata);
            if (external != nullptr) {
                *retained_owner = owner.get();
            }
            return owner;
        },
        *retained, &operation, external);
    if (!status.ok()) {
        if (status.code() == Code::kInvalidArgument) {
            CRB_LOG_WARN("apply admission rejected {} operations: {}", count, status.to_string());
        }
        return static_cast<int32_t>(status.code());
    }
    auto metadata = operation->allocation_budget();
    auto call     = detail::reserved_shared<ApplyCall>(metadata, std::move(operation),
                                                       polling ? c_api_completion_signal(tree) : nullptr);
    call->self    = call;
    if (polling) {
        *out = reinterpret_cast<ct_future *>(call->future.release());
    }
    else {
        call->callback = complete;
        call->context  = completion_context;
    }
    call->operation->start(slot, &ApplyCall::completed, call.get());
    return 0;
}
catch (const std::bad_alloc &) {
    return static_cast<int32_t>(Code::kResourceExhausted);
}
catch (...) {
    return static_cast<int32_t>(Code::kInternal);
}

int32_t ct_apply_replay(ct_tree *tree, uint64_t slot, size_t count, ct_replay_read read, void (*drop_context)(void *),
                        void *context, size_t owner_control_bytes, ct_future **out)
{
    return apply_replay(tree, slot, count, replay_reader(read, context), drop_context, context, owner_control_bytes,
                        out, nullptr, nullptr);
}

int32_t ct_apply_replay_callback(ct_tree *tree, uint64_t slot, size_t count, ct_replay_read read,
                                 void (*drop_context)(void *), void *context, size_t owner_control_bytes,
                                 ct_apply_complete complete, void *completion_context)
{
    return apply_replay(tree, slot, count, replay_reader(read, context), drop_context, context, owner_control_bytes,
                        nullptr, complete, completion_context);
}

int32_t ct_apply_replay_owned_callback(ct_tree *tree, uint64_t slot, ct_kv_ref operation, ct_apply_value_owner owner,
                                       ct_apply_complete complete, void *completion_context)
{
    const ExternalValue external{.initialized = Slice(owner.data, owner.length), .capacity = owner.capacity};
    return apply_replay(
        tree, slot, 1, [operation](size_t) { return write_input(operation); }, owner.drop_context, owner.context, 0,
        nullptr, complete, completion_context, &external);
}
