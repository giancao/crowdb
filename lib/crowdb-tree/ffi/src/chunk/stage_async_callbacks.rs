// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::stage::{encode_stage_list, MAX_STAGE_SCAN};
use super::stage_callbacks::{internal, optional};
use super::{error_code, sys, CatalogContext, CtError};
use std::ffi::c_void;

pub(super) struct BlobReply {
    context: usize,
    complete: sys::ct_catalog_blob_complete,
    active: bool,
}

impl BlobReply {
    pub(super) fn new(context: *mut c_void, complete: sys::ct_catalog_blob_complete) -> Self {
        Self {
            context: context as usize,
            complete,
            active: true,
        }
    }
    pub(super) fn finish(mut self, result: Result<Option<Vec<u8>>, CtError>) {
        self.active = false;
        let (status, data, length) = match result {
            Ok(Some(bytes)) => {
                let bytes = bytes.into_boxed_slice();
                let length = bytes.len();
                (0, Box::into_raw(bytes).cast::<u8>().cast_const(), length)
            }
            Ok(None) => (-1, std::ptr::null(), 0),
            Err(error) => (error_code(error), std::ptr::null(), 0),
        };
        unsafe { (self.complete)(self.context as *mut c_void, status, data, length) };
    }
}

impl Drop for BlobReply {
    fn drop(&mut self) {
        if self.active {
            unsafe { (self.complete)(self.context as *mut c_void, -6, std::ptr::null(), 0) };
        }
    }
}

pub(super) struct StatusReply {
    context: usize,
    complete: sys::ct_catalog_status_complete,
    active: bool,
}

impl StatusReply {
    pub(super) fn new(context: *mut c_void, complete: sys::ct_catalog_status_complete) -> Self {
        Self {
            context: context as usize,
            complete,
            active: true,
        }
    }
    pub(super) fn finish(mut self, result: Result<(), CtError>) {
        self.active = false;
        unsafe {
            (self.complete)(
                self.context as *mut c_void,
                result.map_or_else(error_code, |()| 0),
            )
        };
    }
}

impl Drop for StatusReply {
    fn drop(&mut self) {
        if self.active {
            unsafe { (self.complete)(self.context as *mut c_void, -6) };
        }
    }
}

pub(super) fn callbacks() -> sys::ct_root_catalog_staging_async_callbacks {
    sys::ct_root_catalog_staging_async_callbacks {
        version: 1,
        struct_bytes: std::mem::size_of::<sys::ct_root_catalog_staging_async_callbacks>() as u32,
        load: Some(load),
        compare: Some(compare),
        list: Some(list),
    }
}

unsafe extern "C" fn load(
    context: *mut c_void,
    tree: u64,
    id: sys::ct_chunk_stage_id,
    complete: sys::ct_catalog_blob_complete,
    completion_context: *mut c_void,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(context) = context.cast::<CatalogContext>().as_ref() else {
            return -2;
        };
        let Some(runtime) = context.runtime.as_ref() else {
            return -8;
        };
        let store = context.store.clone();
        let id = internal(id);
        let reply = BlobReply {
            context: completion_context as usize,
            complete,
            active: true,
        };
        // Once the guard owns the context, cancellation or a spawn panic also
        // completes it. Submission is accepted even if the future is dropped.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.spawn(async move {
                let result = store
                    .load_stage_async(tree, id)
                    .await
                    .and_then(|record| record.map(|record| record.encode()).transpose());
                reply.finish(result);
            })
        }));
        0
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn compare(
    context: *mut c_void,
    tree: u64,
    epoch: u64,
    expected: *const u8,
    expected_length: usize,
    next: *const u8,
    next_length: usize,
    complete: sys::ct_catalog_status_complete,
    completion_context: *mut c_void,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let Some(context) = context.cast::<CatalogContext>().as_ref() else {
            return -2;
        };
        let Some(runtime) = context.runtime.as_ref() else {
            return -8;
        };
        let (expected, next) = match (optional(expected, expected_length), optional(next, next_length)) {
            (Ok(expected), Ok(next)) => (expected, next),
            (Err(error), _) | (_, Err(error)) => return error_code(error),
        };
        let store = context.store.clone();
        let reply = StatusReply::new(completion_context, complete);
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.spawn(async move {
                let result = store
                    .compare_stage_async(tree, epoch, expected.as_ref(), next.as_ref())
                    .await;
                reply.finish(result);
            })
        }));
        0
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn list(
    context: *mut c_void,
    tree: u64,
    after: *const sys::ct_chunk_stage_id,
    limit: u32,
    complete: sys::ct_catalog_blob_complete,
    completion_context: *mut c_void,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if !(1..=MAX_STAGE_SCAN).contains(&limit) {
            return -2;
        }
        let Some(context) = context.cast::<CatalogContext>().as_ref() else {
            return -2;
        };
        let Some(runtime) = context.runtime.as_ref() else {
            return -8;
        };
        let after = after.as_ref().copied().map(internal);
        let store = context.store.clone();
        let reply = BlobReply {
            context: completion_context as usize,
            complete,
            active: true,
        };
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            runtime.spawn(async move {
                let result = store
                    .list_stages_async(tree, after, limit)
                    .await
                    .and_then(|records| encode_stage_list(&records))
                    .map(Some);
                reply.finish(result);
            })
        }));
        0
    }))
    .unwrap_or(-6)
}
