// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::Arc;

use crate::error::{check, CtError};
use crate::{sys, PageStore};

pub mod pin_service;
mod runtime;
pub use runtime::ChunkRuntimeOptions;
pub mod protection;
mod protection_async_callbacks;
mod protection_callbacks;
pub mod stage;
mod stage_async_callbacks;
mod stage_callbacks;
use stage::{StageId, StageRecord};

pub struct ChunkRootCatalog {
    ptr: NonNull<sys::ct_root_catalog>,
    callback: Option<Arc<CatalogContext>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RootCatalogObject {
    CurrentManifest,
    Manifest(u64),
}

/// Future returned by a catalog operation on the host runtime.
pub type CatalogFuture<'a, T> =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, CtError>> + Send + 'a>>;

pub trait RootCatalogStore: Send + Sync + 'static {
    /// Durable reference intents share the same authority as pack deletion.
    fn stage_protection(&self) -> Option<&dyn protection::StageProtectionCatalog> {
        None
    }
    /// Schema support alone does not enable independent staged writeback.
    fn supports_staging(&self) -> bool {
        false
    }
    fn load_stage(&self, _tree_id: u64, _id: StageId) -> Result<Option<StageRecord>, CtError> {
        Err(CtError::NotSupported)
    }
    fn compare_stage(
        &self,
        _tree_id: u64,
        _owner_epoch: u64,
        _expected: Option<&StageRecord>,
        _next: Option<&StageRecord>,
    ) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    fn list_stages(
        &self,
        _tree_id: u64,
        _after: Option<StageId>,
        _limit: u32,
    ) -> Result<Vec<StageRecord>, CtError> {
        Err(CtError::NotSupported)
    }
    fn load_stage_async(&self, tree_id: u64, id: StageId) -> CatalogFuture<'_, Option<StageRecord>> {
        Box::pin(async move { self.load_stage(tree_id, id) })
    }
    fn compare_stage_async<'a>(
        &'a self,
        tree_id: u64,
        owner_epoch: u64,
        expected: Option<&'a StageRecord>,
        next: Option<&'a StageRecord>,
    ) -> CatalogFuture<'a, ()> {
        Box::pin(async move { self.compare_stage(tree_id, owner_epoch, expected, next) })
    }
    fn list_stages_async(
        &self,
        tree_id: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> CatalogFuture<'_, Vec<StageRecord>> {
        Box::pin(async move { self.list_stages(tree_id, after, limit) })
    }
    fn load(&self, _tree_id: u64, _object: RootCatalogObject) -> Result<Option<Vec<u8>>, CtError> {
        Err(CtError::NotSupported)
    }
    fn load_async(&self, tree_id: u64, object: RootCatalogObject) -> CatalogFuture<'_, Option<Vec<u8>>> {
        Box::pin(async move { self.load(tree_id, object) })
    }
    fn publish<'a>(
        &'a self,
        _tree_id: u64,
        _expected_generation: u64,
        _owner_epoch: u64,
        _generation: u64,
        _manifest: &'a [u8],
    ) -> CatalogFuture<'a, ()> {
        Box::pin(async { Err(CtError::NotSupported) })
    }
    fn reclaim_before(&self, _tree_id: u64, _generation: u64) -> u64 {
        0
    }
    /// Prunes retained root history without using the synchronous callback.
    /// Callback stores that support history pruning on the async path override this.
    fn reclaim_before_async(&self, _tree_id: u64, _generation: u64) -> CatalogFuture<'_, u64> {
        Box::pin(async { Err(CtError::NotSupported) })
    }
    fn pin_generation(
        &self,
        _tree_id: u64,
        _transition_high: u64,
        _transition_low: u64,
        _generation: u64,
    ) -> Result<(), CtError> {
        Ok(())
    }
    /// Pins an exact generation without entering the synchronous callback.
    fn pin_generation_async(
        &self,
        _tree_id: u64,
        _transition_high: u64,
        _transition_low: u64,
        _generation: u64,
    ) -> CatalogFuture<'_, ()> {
        Box::pin(async { Err(CtError::NotSupported) })
    }
    fn unpin_generation(
        &self,
        _tree_id: u64,
        _transition_high: u64,
        _transition_low: u64,
    ) -> Result<(), CtError> {
        Ok(())
    }
    /// Releases an exact generation pin without entering the synchronous callback.
    fn unpin_generation_async(
        &self,
        _tree_id: u64,
        _transition_high: u64,
        _transition_low: u64,
    ) -> CatalogFuture<'_, ()> {
        Box::pin(async { Err(CtError::NotSupported) })
    }
}

impl ChunkRootCatalog {
    /// Persistent intent/pin schema; the complete backend must also support fenced IO and drain.
    #[must_use]
    pub fn supports_protection_schema(&self) -> bool {
        unsafe { sys::ct_root_catalog_protection_capabilities(self.ptr.as_ptr()) & 1 != 0 }
    }
    /// Opens a catalog whose opaque manifests are persisted by `store`.
    ///
    /// # Errors
    ///
    /// Returns an invalid callback configuration error, or `Unavailable` if
    /// staging is enabled and the caller has no Tokio runtime.
    pub fn open_callback(store: Arc<dyn RootCatalogStore>) -> Result<Self, CtError> {
        let supports_staging = store.supports_staging();
        let supports_protection = store.stage_protection().is_some();
        let runtime = tokio::runtime::Handle::try_current().ok();
        if supports_staging && runtime.is_none() {
            return Err(CtError::Unavailable);
        }
        let callback = Arc::new(CatalogContext { store, runtime });
        let context = Arc::into_raw(callback.clone()).cast_mut().cast::<c_void>();
        let callbacks = sys::ct_root_catalog_callbacks {
            load: Some(catalog_load),
            free_blob: Some(catalog_free_blob),
            reclaim_before: Some(catalog_reclaim_before),
            pin_generation: Some(catalog_pin_generation),
            unpin_generation: Some(catalog_unpin_generation),
            drop_context: Some(catalog_drop_context),
        };
        let mut out = std::ptr::null_mut();
        let status = unsafe {
            if supports_staging && supports_protection {
                sys::ct_callback_root_catalog_open_v3(
                    &callbacks,
                    &stage_callbacks::callbacks(),
                    &protection_callbacks::callbacks(),
                    context,
                    &mut out,
                )
            } else if supports_staging {
                sys::ct_callback_root_catalog_open_v2(
                    &callbacks,
                    &stage_callbacks::callbacks(),
                    context,
                    &mut out,
                )
            } else {
                sys::ct_callback_root_catalog_open(&callbacks, context, &mut out)
            }
        };
        if let Err(error) = check(status) {
            unsafe { drop(Arc::from_raw(context.cast::<CatalogContext>())) };
            return Err(error);
        }
        let catalog = Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
            callback: Some(callback),
        };
        if supports_staging {
            check(unsafe {
                sys::ct_callback_root_catalog_set_staging_async_v1(
                    catalog.ptr.as_ptr(),
                    &stage_async_callbacks::callbacks(),
                )
            })?;
        }
        if supports_staging && supports_protection {
            check(unsafe {
                sys::ct_callback_root_catalog_set_protection_async_v1(
                    catalog.ptr.as_ptr(),
                    &protection_async_callbacks::callbacks(),
                )
            })?;
        }
        Ok(catalog)
    }

    pub fn reclaim_before(&self, tree_id: u64, generation: u64) -> u64 {
        unsafe { sys::ct_root_catalog_reclaim_before(self.ptr.as_ptr(), tree_id, generation) }
    }

    /// Prunes root history on the catalog runtime. Dropping the waiter does
    /// not cancel a submitted authority mutation or its local cleanup.
    /// # Errors
    /// Returns an error if async reclamation is unavailable or the task fails.
    pub async fn reclaim_before_async(&self, tree_id: u64, generation: u64) -> Result<u64, CtError> {
        let Some(callback) = &self.callback else {
            return Ok(self.reclaim_before(tree_id, generation));
        };
        let runtime = callback.runtime.as_ref().ok_or(CtError::Unavailable)?;
        let store = callback.store.clone();
        runtime
            .spawn(async move { store.reclaim_before_async(tree_id, generation).await })
            .await
            .map_err(|_| CtError::Internal)?
    }

    pub fn pin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_root_catalog_pin_generation(
                self.ptr.as_ptr(),
                tree_id,
                transition_high,
                transition_low,
                generation,
            )
        })
    }

    pub fn unpin_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_root_catalog_unpin_generation(self.ptr.as_ptr(), tree_id, transition_high, transition_low)
        })
    }

    /// Pins a generation on the catalog runtime. Canceling the waiter leaves
    /// a submitted pin running with the original transition identity.
    /// # Errors
    /// Returns the catalog or runtime error for the pin.
    pub async fn pin_generation_async(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), CtError> {
        let Some(callback) = &self.callback else {
            return self.pin_generation(tree_id, transition_high, transition_low, generation);
        };
        let runtime = callback.runtime.as_ref().ok_or(CtError::Unavailable)?;
        let store = callback.store.clone();
        runtime
            .spawn(async move {
                store
                    .pin_generation_async(tree_id, transition_high, transition_low, generation)
                    .await
            })
            .await
            .map_err(|_| CtError::Internal)?
    }

    /// Releases a generation pin on the catalog runtime. Canceling the
    /// waiter leaves a submitted release running.
    /// # Errors
    /// Returns the catalog or runtime error for the release.
    pub async fn unpin_generation_async(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), CtError> {
        let Some(callback) = &self.callback else {
            return self.unpin_generation(tree_id, transition_high, transition_low);
        };
        let runtime = callback.runtime.as_ref().ok_or(CtError::Unavailable)?;
        let store = callback.store.clone();
        runtime
            .spawn(async move {
                store
                    .unpin_generation_async(tree_id, transition_high, transition_low)
                    .await
            })
            .await
            .map_err(|_| CtError::Internal)?
    }
}

fn catalog_object(kind: i32, object_id: u64) -> Result<RootCatalogObject, CtError> {
    match kind {
        1 => Ok(RootCatalogObject::CurrentManifest),
        2 => Ok(RootCatalogObject::Manifest(object_id)),
        _ => Err(CtError::InvalidArgument),
    }
}

fn error_code(error: CtError) -> i32 {
    match error {
        CtError::NotFound => -1,
        CtError::InvalidArgument => -2,
        CtError::Corruption => -3,
        CtError::IoError => -4,
        CtError::NotSupported => -5,
        CtError::Internal => -6,
        CtError::ResourceExhausted => -7,
        CtError::Unavailable | CtError::Unknown(_) => -8,
    }
}

struct CatalogContext {
    store: Arc<dyn RootCatalogStore>,
    runtime: Option<tokio::runtime::Handle>,
}

unsafe fn catalog_store_ref<'a>(context: *mut c_void) -> &'a Arc<dyn RootCatalogStore> {
    &(*context.cast::<CatalogContext>()).store
}

unsafe extern "C" fn catalog_load(
    context: *mut c_void,
    kind: i32,
    tree_id: u64,
    object_id: u64,
    out: *mut *const u8,
    len: *mut usize,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() || out.is_null() || len.is_null() {
            return -2;
        }
        let result = catalog_object(kind, object_id)
            .and_then(|object| catalog_store_ref(context).load(tree_id, object));
        match result {
            Ok(Some(bytes)) if !bytes.is_empty() => {
                let bytes = bytes.into_boxed_slice();
                *len = bytes.len();
                *out = Box::into_raw(bytes).cast::<u8>();
                0
            }
            Ok(_) => -1,
            Err(error) => error_code(error),
        }
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_free_blob(_context: *mut c_void, data: *const u8, len: usize) {
    if !data.is_null() {
        drop(Box::from_raw(std::ptr::slice_from_raw_parts_mut(
            data.cast_mut(),
            len,
        )));
    }
}

unsafe extern "C" fn catalog_reclaim_before(context: *mut c_void, tree_id: u64, generation: u64) -> u64 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return 0;
        }
        catalog_store_ref(context).reclaim_before(tree_id, generation)
    }))
    .unwrap_or(0)
}

unsafe extern "C" fn catalog_pin_generation(
    context: *mut c_void,
    tree_id: u64,
    transition_high: u64,
    transition_low: u64,
    generation: u64,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return -2;
        }
        catalog_store_ref(context)
            .pin_generation(tree_id, transition_high, transition_low, generation)
            .map_or_else(error_code, |()| 0)
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_unpin_generation(
    context: *mut c_void,
    tree_id: u64,
    transition_high: u64,
    transition_low: u64,
) -> i32 {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        if context.is_null() {
            return -2;
        }
        catalog_store_ref(context)
            .unpin_generation(tree_id, transition_high, transition_low)
            .map_or_else(error_code, |()| 0)
    }))
    .unwrap_or(-6)
}

unsafe extern "C" fn catalog_drop_context(context: *mut c_void) {
    if !context.is_null() {
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            drop(Arc::from_raw(context.cast::<CatalogContext>()));
        }));
    }
}

impl std::fmt::Debug for ChunkRootCatalog {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("ChunkRootCatalog").finish_non_exhaustive()
    }
}

unsafe impl Send for ChunkRootCatalog {}
unsafe impl Sync for ChunkRootCatalog {}

impl Drop for ChunkRootCatalog {
    fn drop(&mut self) {
        unsafe { sys::ct_root_catalog_free(self.ptr.as_ptr()) };
    }
}

mod transport;
pub use transport::{ChunkRpcDiskRoute, ChunkRpcRoute, ChunkRpcTransportOptions, ChunkTransport};
#[cfg(feature = "chunk-rpc")]
pub use transport::{ChunkRpcRouteResolver, OwnedChunkRpcDiskRoute, OwnedChunkRpcTransportOptions};

impl PageStore {
    /// Sets the WAL byte offset that the next chunk-root publication makes
    /// recoverable. Non-chunk stores accept the hint without persisting it.
    ///
    /// # Errors
    ///
    /// Returns an invalid-argument, availability, or corruption error.
    pub fn set_wal_replay_offset(&self, offset: u64) -> Result<(), CtError> {
        check(unsafe { sys::ct_chunk_page_store_set_wal_replay_offset(self.ptr.as_ptr(), offset) })
    }

    /// Returns the WAL replay offset persisted with the current chunk root.
    /// Non-chunk stores return zero.
    ///
    /// # Errors
    ///
    /// Returns an availability or corruption error when the current root
    /// cannot be validated.
    pub fn wal_replay_offset(&self) -> Result<u64, CtError> {
        let mut offset = 0;
        check(unsafe { sys::ct_chunk_page_store_get_wal_replay_offset(self.ptr.as_ptr(), &mut offset) })?;
        Ok(offset)
    }

    /// Returns the root-catalog manifest generation currently backing this
    /// page-store view. This is distinct from the tree snapshot sequence.
    pub fn chunk_manifest_generation(&self) -> Result<u64, CtError> {
        let mut generation = 0;
        check(unsafe {
            sys::ct_chunk_page_store_get_manifest_generation(self.ptr.as_ptr(), &mut generation)
        })?;
        Ok(generation)
    }

    /// Reclaim catalog generations older than the supplied published
    /// retention watermark. Non-chunk stores have no catalog generations.
    #[must_use]
    pub fn reclaim_chunk_generations_before(&self, tree_id: u64, generation: u64) -> u64 {
        self.chunk_catalog
            .as_ref()
            .map_or(0, |catalog| catalog.reclaim_before(tree_id, generation))
    }

    /// Prunes retained catalog history without blocking the caller's worker.
    /// # Errors
    /// Returns a catalog or runtime error when the async operation cannot complete.
    pub async fn reclaim_chunk_generations_before_async(
        &self,
        tree_id: u64,
        generation: u64,
    ) -> Result<u64, CtError> {
        match &self.chunk_catalog {
            Some(catalog) => catalog.reclaim_before_async(tree_id, generation).await,
            None => Ok(0),
        }
    }

    pub fn pin_chunk_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), CtError> {
        self.chunk_catalog.as_ref().map_or(Ok(()), |catalog| {
            catalog.pin_generation(tree_id, transition_high, transition_low, generation)
        })
    }

    pub fn unpin_chunk_generation(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), CtError> {
        self.chunk_catalog.as_ref().map_or(Ok(()), |catalog| {
            catalog.unpin_generation(tree_id, transition_high, transition_low)
        })
    }

    /// Pins a Chunk generation asynchronously. Local stores have no catalog pins.
    /// # Errors
    /// Returns the catalog or runtime error for the pin.
    pub async fn pin_chunk_generation_async(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
        generation: u64,
    ) -> Result<(), CtError> {
        match &self.chunk_catalog {
            Some(catalog) => {
                catalog
                    .pin_generation_async(tree_id, transition_high, transition_low, generation)
                    .await
            }
            None => Ok(()),
        }
    }

    /// Releases a Chunk generation pin asynchronously.
    /// # Errors
    /// Returns the catalog or runtime error for the release.
    pub async fn unpin_chunk_generation_async(
        &self,
        tree_id: u64,
        transition_high: u64,
        transition_low: u64,
    ) -> Result<(), CtError> {
        match &self.chunk_catalog {
            Some(catalog) => {
                catalog
                    .unpin_generation_async(tree_id, transition_high, transition_low)
                    .await
            }
            None => Ok(()),
        }
    }
}

#[cfg(feature = "test-util")]
pub mod test_util;
