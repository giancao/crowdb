// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::ffi::c_void;
use std::future::{poll_fn, Future};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Weak};
use std::task::Poll;

use bytes::RetainedAllocation;
use futures::task::AtomicWaker;

use crate::error::check;
use crate::{sys, BatchOp, Crowdbtree, CtError, ExtOp};

/// An owned, charged copy of a complete batch. Preparation holds no source
/// writer, epoch guard, or Tree close ticket, and may be cancelled by dropping it.
/// Arm immediately before consensus may choose a slot for this proposal.
pub struct PreparedApply {
    raw: NonNull<sys::ct_apply_permit>,
    tree: Weak<Crowdbtree>,
}

// SAFETY: this opaque handle is exclusively owned. Native prepare/arm/submit
// permit movement between threads but require serialized access to one handle.
unsafe impl Send for PreparedApply {}

/// A generation-pinned proposal. Dropping it before submit cancels admission;
/// after submit, the accepted operation drains even if its future is dropped.
pub struct ArmedApply {
    // Release the native permit before dropping the last possible Tree owner.
    permit: PreparedApply,
    tree: Arc<Crowdbtree>,
}

impl Crowdbtree {
    /// Prepare one PUT while transferring its original value allocation.
    pub fn prepare_owned_put(
        self: &Arc<Self>,
        key: &[u8],
        value: &[u8],
        allocation: RetainedAllocation<'_>,
    ) -> Result<PreparedApply, CtError> {
        let operation = kv_ref(BatchOp::Put { key, value });
        let owner = value_owner(&allocation)?;
        let mut raw = std::ptr::null_mut();
        check(unsafe { sys::ct_apply_prepare_owned_callback(self.as_ptr(), &operation, owner, &mut raw) })?;
        Ok(PreparedApply {
            raw: NonNull::new(raw).ok_or(CtError::Internal)?,
            tree: Arc::downgrade(self),
        })
    }

    /// Copy and admit a complete local proposal before choosing its slot.
    /// Resource pressure is reported before consensus accepts the proposal.
    pub fn prepare_proposal(self: &Arc<Self>, ops: &[BatchOp<'_>]) -> Result<PreparedApply, CtError> {
        self.prepare_in_partition(ops.len(), ops.iter().cloned().map(Ok), false)
    }

    /// Prepare a checked stream of borrowed mutations directly into native descriptors.
    pub fn prepare_proposal_from<'a>(
        self: &Arc<Self>,
        count: usize,
        ops: impl Iterator<Item = Result<BatchOp<'a>, CtError>>,
    ) -> Result<PreparedApply, CtError> {
        self.prepare_in_partition(count, ops, false)
    }

    /// Copy a follower or replay batch using its independent progress partition.
    pub fn prepare_replay(self: &Arc<Self>, ops: &[BatchOp<'_>]) -> Result<PreparedApply, CtError> {
        self.prepare_in_partition(ops.len(), ops.iter().cloned().map(Ok), true)
    }

    fn prepare_in_partition<'a>(
        self: &Arc<Self>,
        count: usize,
        mut ops: impl Iterator<Item = Result<BatchOp<'a>, CtError>>,
        gap_repair: bool,
    ) -> Result<PreparedApply, CtError> {
        if count <= 1 {
            let op = ops.next().transpose()?.map(kv_ref);
            if op.as_slice().len() != count || ops.next().is_some() {
                return Err(CtError::InvalidArgument);
            }
            return self.prepare_refs(op.as_slice(), gap_repair);
        }
        let mut refs = Vec::with_capacity(count);
        for op in ops.by_ref().take(count) {
            refs.push(kv_ref(op?));
        }
        if refs.len() != count || ops.next().is_some() {
            return Err(CtError::InvalidArgument);
        }
        self.prepare_refs(&refs, gap_repair)
    }

    fn prepare_refs(
        self: &Arc<Self>,
        refs: &[sys::ct_kv_ref],
        gap_repair: bool,
    ) -> Result<PreparedApply, CtError> {
        let mut raw = std::ptr::null_mut();
        check(unsafe {
            sys::ct_apply_prepare_callback(
                self.as_ptr(),
                refs.as_ptr(),
                refs.len(),
                i32::from(gap_repair),
                &mut raw,
            )
        })?;
        Ok(PreparedApply {
            raw: NonNull::new(raw).ok_or(CtError::Internal)?,
            tree: Arc::downgrade(self),
        })
    }
}

fn kv_ref(op: BatchOp<'_>) -> sys::ct_kv_ref {
    match op {
        BatchOp::Put { key, value } => sys::ct_kv_ref {
            key: key.as_ptr(),
            key_len: key.len(),
            value: value.as_ptr(),
            value_len: value.len(),
            kind: 0,
        },
        BatchOp::Delete { key } => sys::ct_kv_ref {
            key: key.as_ptr(),
            key_len: key.len(),
            value: std::ptr::null(),
            value_len: 0,
            kind: 1,
        },
    }
}

impl PreparedApply {
    /// Pin this prepared generation before a proposal can become chosen.
    /// Generation replacement or Tree closure makes an old permit unavailable.
    pub fn arm(self) -> Result<ArmedApply, CtError> {
        let tree = self.tree.upgrade().ok_or(CtError::Unavailable)?;
        check(unsafe { sys::ct_apply_arm(self.raw.as_ptr()) })?;
        Ok(ArmedApply { permit: self, tree })
    }
}

impl Drop for PreparedApply {
    fn drop(&mut self) {
        unsafe { sys::ct_apply_permit_free(self.raw.as_ptr()) };
    }
}

impl ArmedApply {
    /// Consume this admission for exactly one chosen slot. CPU and budget
    /// continuations run in the Tree runtime; this future only awaits completion.
    pub async fn apply(self, slot: u64) -> Result<(), CtError> {
        self.start(slot)?.await
    }

    /// Submit on the current thread and return this operation's completion.
    pub fn start(self, slot: u64) -> Result<impl Future<Output = Result<(), CtError>> + Send, CtError> {
        let completion = ApplyCompletion::submit(|context| unsafe {
            sys::ct_apply_submit_callback(self.permit.raw.as_ptr(), slot, apply_completed, context)
        })?;
        let tree = self.tree;
        Ok(async move {
            let _tree = tree;
            completion.wait().await
        })
    }
}

struct ApplyCompletion {
    status: AtomicI32,
    waker: AtomicWaker,
}

impl ApplyCompletion {
    const PENDING: i32 = i32::MIN;

    fn submit(submit: impl FnOnce(*mut c_void) -> i32) -> Result<Arc<Self>, CtError> {
        let completion = Arc::new(Self {
            status: AtomicI32::new(Self::PENDING),
            waker: AtomicWaker::new(),
        });
        let context = Arc::into_raw(Arc::clone(&completion)).cast_mut().cast();
        if let Err(error) = check(submit(context)) {
            // A rejected submission did not take the callback reference.
            drop(unsafe { Arc::from_raw(context.cast::<Self>()) });
            return Err(error);
        }
        Ok(completion)
    }

    async fn wait(&self) -> Result<(), CtError> {
        poll_fn(|cx| {
            let mut status = self.status.load(Ordering::Acquire);
            if status == Self::PENDING {
                self.waker.register(cx.waker());
                status = self.status.load(Ordering::Acquire);
            }
            if status == Self::PENDING {
                Poll::Pending
            } else {
                Poll::Ready(check(status))
            }
        })
        .await
    }
}

unsafe extern "C" fn apply_completed(context: *mut c_void, status: i32) {
    // The accepted native operation consumes exactly one callback reference.
    let completion = unsafe { Arc::from_raw(context.cast::<ApplyCompletion>()) };
    completion.status.store(status, Ordering::Release);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| completion.waker.wake()));
}

// Native retains this bridge while reading the immutable caller input. Its Box
// allocation is charged separately from the controlled native batch copy.
struct ReplayOwner {
    ops: Vec<ExtOp>,
}

fn value_owner(owner: &RetainedAllocation<'_>) -> Result<sys::ct_apply_value_owner, CtError> {
    let (_, allocation, capacity) = owner.parts();
    let (context, drop_context) = owner.retain().ok_or(CtError::InvalidArgument)?;
    Ok(sys::ct_apply_value_owner {
        data: allocation.as_ptr(),
        length: allocation.len(),
        capacity,
        drop_context,
        context,
    })
}

unsafe extern "C" fn read_replay(context: *mut std::ffi::c_void, index: usize, out: *mut sys::ct_kv_ref) {
    // SAFETY: native retains this box, serializes reads, and uses index < count.
    let owner = unsafe { &*context.cast::<ReplayOwner>() };
    unsafe { out.write(ext_ref(&owner.ops[index])) };
}

fn ext_ref(operation: &ExtOp) -> sys::ct_kv_ref {
    match operation {
        ExtOp::Put { key, value } => sys::ct_kv_ref {
            key: key.as_ptr(),
            key_len: key.len(),
            value: value.as_ptr(),
            value_len: value.len(),
            kind: 0,
        },
        ExtOp::Delete { key } => sys::ct_kv_ref {
            key: key.as_ptr(),
            key_len: key.len(),
            value: std::ptr::null(),
            value_len: 0,
            kind: 1,
        },
    }
}

unsafe extern "C" fn release_replay(context: *mut std::ffi::c_void) {
    // SAFETY: ct_apply_replay consumes one Box and invokes this callback once.
    drop(unsafe { Box::from_raw(context.cast::<ReplayOwner>()) });
}

impl Crowdbtree {
    /// Submit one chosen PUT with its original allocation to the replay path.
    pub fn start_replay_owned_put(
        self: &Arc<Self>,
        slot: u64,
        key: &[u8],
        allocation: RetainedAllocation<'_>,
    ) -> Result<impl Future<Output = Result<(), CtError>> + Send + 'static, CtError> {
        let (value, _, _) = allocation.parts();
        let operation = kv_ref(BatchOp::Put { key, value });
        let owner = value_owner(&allocation)?;
        let completion = ApplyCompletion::submit(|completion_context| unsafe {
            sys::ct_apply_replay_owned_callback(
                self.as_ptr(),
                slot,
                operation,
                owner,
                apply_completed,
                completion_context,
            )
        })?;
        let tree = Arc::clone(self);
        Ok(async move {
            let _tree = tree;
            completion.wait().await
        })
    }

    /// Apply one already-chosen follower or WAL batch. High slots retain the
    /// immutable input while the missing contiguous slot owns the gap reserve.
    /// Native quota waits and staged writeback continue if this future drops.
    pub async fn apply_replay(self: &Arc<Self>, slot: u64, operations: Vec<ExtOp>) -> Result<(), CtError> {
        let count = operations.len();
        let completion = ApplyCompletion::submit(|completion_context| {
            let context = Box::into_raw(Box::new(ReplayOwner { ops: operations })).cast();
            unsafe {
                sys::ct_apply_replay_callback(
                    self.as_ptr(),
                    slot,
                    count,
                    read_replay,
                    release_replay,
                    context,
                    std::mem::size_of::<ReplayOwner>(),
                    apply_completed,
                    completion_context,
                )
            }
        })?;
        completion.wait().await
    }
}
