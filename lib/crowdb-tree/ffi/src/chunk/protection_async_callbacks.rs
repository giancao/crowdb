// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::stage_async_callbacks::StatusReply;
use super::{error_code, sys, CatalogContext, CtError, RootCatalogStore};
use std::ffi::c_void;
use std::future::Future;
use std::sync::Arc;

mod mutation;
mod query;

fn boundary(operation: impl FnOnce() -> Result<i32, CtError>) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation))
        .unwrap_or(Err(CtError::Internal))
        .unwrap_or_else(error_code)
}

unsafe fn submit<F, Fut>(
    context: *mut c_void,
    complete: sys::ct_catalog_status_complete,
    completion_context: *mut c_void,
    operation: F,
) -> Result<i32, CtError>
where
    F: FnOnce(Arc<dyn RootCatalogStore>) -> Fut + Send + 'static,
    Fut: Future<Output = Result<(), CtError>> + Send + 'static,
{
    let context = context
        .cast::<CatalogContext>()
        .as_ref()
        .ok_or(CtError::InvalidArgument)?;
    let runtime = context.runtime.as_ref().ok_or(CtError::Unavailable)?;
    let store = context.store.clone();
    let reply = StatusReply::new(completion_context, complete);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        runtime.spawn(async move {
            reply.finish(operation(store).await);
        })
    }));
    Ok(0)
}

pub(super) fn callbacks() -> sys::ct_root_catalog_protection_async_callbacks {
    sys::ct_root_catalog_protection_async_callbacks {
        version: 1,
        struct_bytes: std::mem::size_of::<sys::ct_root_catalog_protection_async_callbacks>() as u32,
        writer_quiesced: Some(writer_quiesced),
        begin: Some(mutation::begin),
        load: Some(query::load),
        list: Some(query::list),
        prepare: Some(mutation::prepare),
        finish: Some(mutation::finish),
        release: Some(mutation::release),
        pin: Some(mutation::pin),
        prune: Some(query::prune),
        root: Some(query::root),
    }
}

unsafe extern "C" fn writer_quiesced(
    context: *mut c_void,
    tree: u64,
    epoch: u64,
    complete: sys::ct_catalog_status_complete,
    done: *mut c_void,
) -> i32 {
    boundary(|| {
        submit(context, complete, done, move |store| async move {
            store
                .stage_protection()
                .ok_or(CtError::NotSupported)?
                .writer_quiesced_async(tree, epoch)
                .await
        })
    })
}
