// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_rpc_ffi::Buffer;
use crowdb_tree_ffi::{ArmedApply, BatchOp as NativeOp, Crowdbtree, CtError, ExtOp};

use crate::kv::{AdmittedApply, ApplyAdmissionError, ApplyAdmissionResult, Batch, KVFuture, Op};
use crate::paxos::roles::ProposalPayload;

struct NativeAdmission(ArmedApply);

impl AdmittedApply for NativeAdmission {
    fn apply(self: Box<Self>, slot: u64) -> KVFuture<Result<(), String>> {
        match self.0.start(slot) {
            Ok(completion) => KVFuture::Pending(Box::pin(async move {
                completion.await.map_err(|error| error.to_string())
            })),
            Err(error) => KVFuture::ready(Err(error.to_string())),
        }
    }
}

pub(super) fn prepare(tree: &Arc<Crowdbtree>, proposal: &ProposalPayload) -> ApplyAdmissionResult {
    if let Some(parts) = proposal.segments() {
        let operations = parts.batches().flat_map(|batch| {
            let ranges = Batch::operation_ranges(batch);
            let invalid = ranges.as_ref().err().map(|_| Err(CtError::InvalidArgument));
            invalid.into_iter().chain(
                ranges.into_iter().flat_map(|(_, operations)| {
                    operations.map(|operation| native_operation(batch, operation))
                }),
            )
        });
        return arm(tree
            .prepare_proposal_from(parts.operation_count(), operations)
            .map_err(admission_error)?);
    }
    let payload = proposal.contiguous().expect("contiguous proposal");
    if let Some((key, value)) = Batch::single_put_ranges(payload) {
        let key = &payload[key];
        let value = &payload[value];
        if let Some(allocation) = control_allocation(payload, value.len()) {
            match tree.prepare_owned_put(key, value, allocation) {
                Ok(prepared) => return arm(prepared),
                Err(CtError::InvalidArgument | CtError::Unavailable) => (),
                Err(error) => return Err(admission_error(error)),
            }
        }
        return arm(tree
            .prepare_proposal(&[NativeOp::Put { key, value }])
            .map_err(admission_error)?);
    }
    let (count, ranges) =
        Batch::operation_ranges(payload).map_err(|error| ApplyAdmissionError::Rejected(error.to_owned()))?;
    let operations = ranges.map(|operation| native_operation(payload, operation));
    let prepared = tree
        .prepare_proposal_from(count, operations)
        .map_err(admission_error)?;
    arm(prepared)
}

fn native_operation<'a>(
    payload: &'a [u8],
    operation: Result<super::super::op::OperationRanges, &'static str>,
) -> Result<NativeOp<'a>, CtError> {
    let (key, value) = operation.map_err(|_| CtError::InvalidArgument)?;
    let key = &payload[key];
    Ok(match value {
        Some(value) => NativeOp::Put {
            key,
            value: &payload[value],
        },
        None => NativeOp::Delete { key },
    })
}

fn arm(prepared: crowdb_tree_ffi::PreparedApply) -> ApplyAdmissionResult {
    let armed = prepared.arm().map_err(admission_error)?;
    Ok(Some(Box::new(NativeAdmission(armed))))
}

fn control_allocation(bytes: &Bytes, value_len: usize) -> Option<bytes::RetainedAllocation<'_>> {
    if value_len <= 24 {
        return None;
    }
    let allocation = Buffer::control_allocation(bytes)?;
    let (_, _, capacity) = allocation.parts();
    // Keep the copied cell for small values or disproportionately large roots.
    (value_len >= capacity.div_ceil(2)).then_some(allocation)
}

fn admission_error(error: CtError) -> ApplyAdmissionError {
    match error {
        CtError::Unavailable => ApplyAdmissionError::Busy,
        other => ApplyAdmissionError::Rejected(format!("native apply admission: {other}")),
    }
}

pub(super) fn replay(tree: Arc<Crowdbtree>, slot: u64, batch: &Batch) -> KVFuture<Result<(), String>> {
    if let [entry] = batch.ops.as_slice() {
        if let Op::Put(value) = &entry.op {
            let key = entry.key.clone();
            let value = value.clone();
            return KVFuture::Pending(Box::pin(async move {
                if let Some(allocation) = control_allocation(&value, value.len()) {
                    match tree.start_replay_owned_put(slot, &key, allocation) {
                        Ok(completion) => return completion.await.map_err(|error| error.to_string()),
                        Err(CtError::InvalidArgument) => (),
                        Err(error) => return Err(error.to_string()),
                    }
                }
                tree.apply_replay(slot, vec![ExtOp::Put { key, value }])
                    .await
                    .map_err(|error| error.to_string())
            }));
        }
    }
    let operations = batch
        .ops
        .iter()
        .map(|entry| match &entry.op {
            Op::Put(value) => ExtOp::Put {
                key: entry.key.clone(),
                value: value.clone(),
            },
            Op::Delete => ExtOp::Delete {
                key: entry.key.clone(),
            },
        })
        .collect();
    KVFuture::Pending(Box::pin(async move {
        tree.apply_replay(slot, operations)
            .await
            .map_err(|error| error.to_string())
    }))
}

pub(super) fn replay_empty(tree: Arc<Crowdbtree>, slot: u64) -> KVFuture<Result<(), String>> {
    KVFuture::Pending(Box::pin(async move {
        tree.apply_replay(slot, Vec::new())
            .await
            .map_err(|error| error.to_string())
    }))
}
