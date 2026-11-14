// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{boundary, submit};
use crate::chunk::protection::CandidateStatus;
use crate::chunk::stage::StageId;
use crate::chunk::stage::MAX_STAGE_SCAN;
use crate::chunk::stage_async_callbacks::BlobReply;
use crate::chunk::stage_callbacks::internal;
use crate::chunk::{CatalogContext, RootCatalogObject};
use crate::{sys, CtError};
use std::ffi::c_void;

pub(super) unsafe extern "C" fn root(
    context: *mut c_void,
    tree: u64,
    generation: u64,
    complete: sys::ct_catalog_blob_complete,
    completion_context: *mut c_void,
) -> i32 {
    boundary(|| {
        let context = context
            .cast::<CatalogContext>()
            .as_ref()
            .ok_or(CtError::InvalidArgument)?;
        let runtime = context.runtime.as_ref().ok_or(CtError::Unavailable)?;
        let store = context.store.clone();
        let object = if generation == 0 {
            RootCatalogObject::CurrentManifest
        } else {
            RootCatalogObject::Manifest(generation)
        };
        let reply = BlobReply::new(completion_context, complete);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.spawn(async move {
                reply.finish(store.load_async(tree, object).await);
            })
        }));
        Ok(0)
    })
}

fn external(id: StageId) -> sys::ct_chunk_stage_id {
    sys::ct_chunk_stage_id {
        owner_epoch: id.owner_epoch,
        operation_high: id.operation_high,
        operation_low: id.operation_low,
    }
}

fn status(value: CandidateStatus) -> sys::ct_chunk_candidate_status {
    sys::ct_chunk_candidate_status {
        id: external(value.id),
        generation: value.generation,
        reference_count: value.reference_count,
        prepared_references: value.prepared_references,
        prepared_windows: value.prepared_windows,
        released_windows: value.released_windows,
        state: value.state as u32,
    }
}

pub(super) unsafe extern "C" fn load(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    out: *mut sys::ct_chunk_candidate_status,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if out.is_null() {
            return Err(CtError::InvalidArgument);
        }
        let output = out as usize;
        let id = internal(id);
        submit(context, complete, done, move |store| async move {
            let value = store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .load_manifest_candidate_async(tree, id)
                .await?
                .ok_or(CtError::NotFound)?;
            *(output as *mut sys::ct_chunk_candidate_status) = status(value);
            Ok(())
        })
    })
}

pub(super) unsafe extern "C" fn list(
    context: *mut c_void,
    tree: u64,
    after: *const sys::ct_chunk_stage_id,
    limit: u32,
    out: *mut sys::ct_chunk_candidate_status,
    length: *mut usize,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if out.is_null()
            || length.is_null()
            || !(1..=MAX_STAGE_SCAN).contains(&limit)
            || *length < limit as usize
        {
            return Err(CtError::InvalidArgument);
        }
        let output = out as usize;
        let output_length = length as usize;
        let after = after.as_ref().copied().map(internal);
        submit(context, complete, done, move |store| async move {
            let values = store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .list_manifest_candidates_async(tree, after, limit)
                .await?;
            if values.len() > limit as usize {
                return Err(CtError::Corruption);
            }
            for (index, value) in values.iter().copied().enumerate() {
                (output as *mut sys::ct_chunk_candidate_status)
                    .add(index)
                    .write(status(value));
            }
            *(output_length as *mut usize) = values.len();
            Ok(())
        })
    })
}

pub(super) unsafe extern "C" fn prune(
    context: *mut c_void,
    tree: u64,
    stage: sys::ct_chunk_stage_id,
    after: *const sys::ct_chunk_stage_id,
    limit: u32,
    out: *mut sys::ct_chunk_pin_progress,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if out.is_null() || !(1..=MAX_STAGE_SCAN).contains(&limit) {
            return Err(CtError::InvalidArgument);
        }
        let output = out as usize;
        let stage = internal(stage);
        let after = after.as_ref().copied().map(internal);
        submit(context, complete, done, move |store| async move {
            let value = store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .prune_stage_pins_async(tree, stage, after, limit)
                .await?;
            *(output as *mut sys::ct_chunk_pin_progress) = sys::ct_chunk_pin_progress {
                after: external(value.after.unwrap_or(StageId {
                    owner_epoch: 0,
                    operation_high: 0,
                    operation_low: 0,
                })),
                examined: value.examined,
                released: value.released,
                has_after: u8::from(value.after.is_some()),
                more: u8::from(value.more),
            };
            Ok(())
        })
    })
}
