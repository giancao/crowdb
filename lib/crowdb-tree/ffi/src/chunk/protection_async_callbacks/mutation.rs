// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{boundary, submit};
use crate::chunk::protection::{ManifestCandidate, MAX_CANDIDATE_MANIFEST_BYTES, MAX_REFERENCE_WINDOW};
use crate::chunk::stage_callbacks::internal;
use crate::{sys, CtError};
use std::ffi::c_void;

pub(super) unsafe extern "C" fn begin(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    expected_generation: u64,
    reference_count: u64,
    reference_checksum: u32,
    manifest: *const u8,
    length: usize,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if manifest.is_null() || length == 0 || length > MAX_CANDIDATE_MANIFEST_BYTES {
            return Err(CtError::InvalidArgument);
        }
        let candidate = ManifestCandidate {
            id: internal(id),
            expected_generation,
            reference_count,
            reference_checksum,
            manifest: std::slice::from_raw_parts(manifest, length).to_vec(),
        };
        candidate.validate()?;
        submit(context, complete, done, move |store| async move {
            store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .begin_manifest_candidate_async(tree, &candidate)
                .await
        })
    })
}

pub(super) unsafe extern "C" fn prepare(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    window: u64,
    references: *const sys::ct_chunk_stage_id,
    length: usize,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if references.is_null() || length == 0 || length > MAX_REFERENCE_WINDOW {
            return Err(CtError::InvalidArgument);
        }
        let references: Vec<_> = std::slice::from_raw_parts(references, length)
            .iter()
            .copied()
            .map(internal)
            .collect();
        let id = internal(id);
        submit(context, complete, done, move |store| async move {
            store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .prepare_manifest_references_async(tree, id, window, &references)
                .await
        })
    })
}

pub(super) unsafe extern "C" fn finish(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    action: u32,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if !(1..=3).contains(&action) {
            return Err(CtError::InvalidArgument);
        }
        let id = internal(id);
        submit(context, complete, done, move |store| async move {
            let catalog = store.stage_protection().ok_or(CtError::NotSupported)?;
            match action {
                1 => catalog.commit_manifest_candidate_async(tree, id).await,
                2 => catalog.abort_manifest_candidate_async(tree, id).await,
                _ => catalog.recover_manifest_candidate_async(tree, id).await,
            }
        })
    })
}

pub(super) unsafe extern "C" fn release(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    window: u64,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        let id = internal(id);
        submit(context, complete, done, move |store| async move {
            store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .release_manifest_references_async(tree, id, window)
                .await
        })
    })
}

pub(super) unsafe extern "C" fn pin(
    context: *mut c_void,
    tree: u64,
    stage: sys::ct_chunk_stage_id,
    pin: sys::ct_chunk_stage_id,
    acquire: u8,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        if acquire > 1 {
            return Err(CtError::InvalidArgument);
        }
        let stage = internal(stage);
        let pin = internal(pin);
        submit(context, complete, done, move |store| async move {
            let catalog = store.stage_protection().ok_or(CtError::NotSupported)?;
            if acquire != 0 {
                catalog.pin_stage_async(tree, stage, pin).await
            } else {
                catalog.unpin_stage_async(tree, stage, pin).await
            }
        })
    })
}
