// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod split_overlay;
pub use split_overlay::{InstalledSplit, PreparedSplitOverlay, SplitOverlayLimits};

use std::ffi::{c_void, CString, OsString};
use std::os::fd::{AsRawFd, RawFd};
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::Once;

use tokio::io::unix::AsyncFd;
use tokio::sync::oneshot;

use crate::close::CloseState;
use crate::config::{Config, KeyRange, PageStoreBackend};
use crate::error::{check, take_buf, CtError};
use crate::page::PageInspection;
use crate::reactor::{drain_eventfd, duplicate_eventfd, EventfdPump};
use crate::stats::RangeRebuildStats;
use crate::sys;

/// Owning handle to a crowdb-tree engine. Send + Sync: the C++ engine serializes
/// writes internally and keeps reads lock-free.
pub struct Crowdbtree {
    ptr: NonNull<sys::ct_tree>,
    backend_release: Option<crate::BackendReleaseService>,
    runtime: Option<Arc<crate::TreeRuntime>>,
    hosted_close: Option<(tokio::runtime::Handle, TreeClose)>,
    close_started: bool,
    // Lazily-spawned eventfd pumps for this tree's DiskIOUring eventfds
    // (one per pipeline), shared by every concurrently-pending
    // drive_ct_future call. Empty once resolved means no DiskIOUring is
    // wired (or the pumps failed to spawn); see `eventfd_notify`/`EventfdPump`.
    eventfd_pumps: std::sync::OnceLock<Vec<EventfdPump>>,
}

unsafe impl Send for Crowdbtree {}
unsafe impl Sync for Crowdbtree {}

/// The result of one Tree close. Waiting may be canceled or retried.
#[derive(Clone, Debug)]
pub struct TreeClose(Arc<CloseState>);

impl TreeClose {
    /// Waits for the native Tree facade and its accepted work to retire.
    pub async fn wait(&self) -> Result<(), CtError> {
        self.0.wait().await
    }
}

unsafe extern "C" fn tree_closed(context: *mut c_void, status: c_int) {
    // The accepted native close calls this exactly once and owns this Arc.
    unsafe { Arc::from_raw(context.cast::<CloseState>()) }.finish(status);
}

struct OpenReply {
    status: c_int,
    tree: Option<NonNull<sys::ct_tree>>,
}

// The accepted reply exclusively owns its facade until Tokio takes it.
unsafe impl Send for OpenReply {}

impl Drop for OpenReply {
    fn drop(&mut self) {
        if let Some(tree) = self.tree.take() {
            let status =
                unsafe { sys::ct_close_async(tree.as_ptr(), undelivered_tree_closed, std::ptr::null_mut()) };
            if status != 0 {
                eprintln!("critical: undelivered Tree close rejected with status {status}");
            }
        }
    }
}

unsafe extern "C" fn undelivered_tree_closed(_: *mut c_void, status: c_int) {
    if status != 0 {
        eprintln!("critical: undelivered Tree close failed with status {status}");
    }
}

unsafe extern "C" fn tree_opened(context: *mut c_void, status: c_int, tree: *mut sys::ct_tree) {
    let sender = unsafe { Box::from_raw(context.cast::<oneshot::Sender<OpenReply>>()) };
    let _ = sender.send(OpenReply {
        status,
        tree: NonNull::new(tree),
    });
}

impl std::fmt::Debug for Crowdbtree {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crowdbtree")
            .field("ptr", &self.ptr)
            .finish_non_exhaustive()
    }
}

type InspectionReply = oneshot::Sender<Result<PageInspection, CtError>>;

unsafe extern "C" fn page_inspected(
    context: *mut c_void,
    status: c_int,
    version: u64,
    root: u64,
    page: u64,
    deltas: u32,
    frame: sys::ct_buf,
) {
    let sender = unsafe { Box::from_raw(context.cast::<InspectionReply>()) };
    let frame = take_buf(frame);
    let result = check(status).and_then(|()| PageInspection::decode(version, root, page, deltas, frame));
    let _ = sender.send(result);
}

unsafe extern "C" fn pages_evicted(context: *mut c_void, count: u64) {
    let sender = unsafe { Box::from_raw(context.cast::<oneshot::Sender<u64>>()) };
    let _ = sender.send(count);
}

impl Crowdbtree {
    /// Copies one actual base page, without flushing memory or reading overflow values.
    pub fn inspect_page(
        &self,
        path: &[u32],
        version: Option<u64>,
    ) -> Result<crate::page::PageInspection, CtError> {
        let (mut revision, mut root, mut page, mut deltas) = (0, 0, 0, 0);
        let mut frame = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        check(unsafe {
            sys::ct_inspect_page(
                self.ptr.as_ptr(),
                path.as_ptr(),
                path.len(),
                version.unwrap_or(u64::MAX),
                &mut revision,
                &mut root,
                &mut page,
                &mut deltas,
                &mut frame,
            )
        })?;
        crate::page::PageInspection::decode(revision, root, page, deltas, take_buf(frame))
    }

    /// Copies a structural page, awaiting cold reads on the native runtime.
    ///
    /// # Errors
    /// Returns a stale version, invalid path, or native read error.
    pub async fn inspect_page_async(
        &self,
        path: &[u32],
        version: Option<u64>,
    ) -> Result<PageInspection, CtError> {
        let (sender, received) = oneshot::channel();
        {
            let context = Box::into_raw(Box::<InspectionReply>::new(sender)).cast::<c_void>();
            let status = unsafe {
                sys::ct_inspect_page_async(
                    self.as_ptr(),
                    path.as_ptr(),
                    path.len(),
                    version.unwrap_or(u64::MAX),
                    page_inspected,
                    context,
                )
            };
            if let Err(error) = check(status) {
                unsafe { drop(Box::from_raw(context.cast::<InspectionReply>())) };
                return Err(error);
            }
        }
        received.await.map_err(|_| CtError::Internal)?
    }

    /// Open (recovering durable state when `path` is set, else fresh in-memory).
    pub fn open(opt: &Config) -> Result<Self, CtError> {
        Self::open_impl(opt)
    }

    /// Opens a typed Chunk or a `MemBlock` Tree on its selected runtime.
    /// Omitting a runtime uses the process-owned host.
    /// A canceled waiter leaves recovery running and closes any undelivered Tree.
    /// Keep the Tokio host and the shared runtime alive through shutdown.
    ///
    /// # Errors
    /// Returns an invalid binding or native recovery error.
    pub async fn open_async(opt: &Config) -> Result<Self, CtError> {
        Self::open_async_impl(opt, None).await
    }

    /// Opens a Linux Block Tree using basenames already discovered by the host.
    /// An empty list creates the first extent. On failure, waits for the
    /// accepted opens and their file releases before reporting the error.
    ///
    /// # Errors
    /// Returns invalid options, unavailable io_uring, or a native open error.
    pub async fn open_block_from_names(opt: &Config, names: &[OsString]) -> Result<Self, CtError> {
        Self::open_async_impl(opt, Some(names)).await
    }

    async fn open_async_impl(opt: &Config, block_names: Option<&[OsString]>) -> Result<Self, CtError> {
        let runtime = opt.runtime.clone();
        if block_names.is_some() && runtime.is_none() {
            return Err(CtError::InvalidArgument);
        }
        let executor = tokio::runtime::Handle::try_current().map_err(|_| CtError::InvalidArgument)?;
        let names = block_names
            .map(|names| {
                names
                    .iter()
                    .map(|name| CString::new(name.as_encoded_bytes()).map_err(|_| CtError::InvalidArgument))
                    .collect::<Result<Vec<_>, _>>()
            })
            .transpose()?;
        let (release, received) = loop {
            let (sender, received) = oneshot::channel::<OpenReply>();
            let mut retry = false;
            let release = Self::with_open_options(opt, |options| {
                let native_runtime = runtime
                    .as_ref()
                    .map_or(std::ptr::null_mut(), |host| host.as_ptr());
                let binding = opt.runtime_binding.as_raw();
                let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
                let result = if let Some(names) = &names {
                    let pointers: Vec<_> = names.iter().map(|name| name.as_ptr()).collect();
                    crate::BackendReleaseService::attach_with(
                        &executor,
                        |wake, drop, wake_context, native| unsafe {
                            sys::ct_open_block_with_runtime_async(
                                options,
                                native_runtime,
                                &binding,
                                pointers.as_ptr(),
                                pointers.len(),
                                tree_opened,
                                context,
                                wake,
                                drop,
                                wake_context,
                                native,
                            )
                        },
                    )
                    .map(Some)
                } else {
                    let status = unsafe {
                        sys::ct_open_with_runtime_async(
                            options,
                            native_runtime,
                            &binding,
                            tree_opened,
                            context,
                        )
                    };
                    retry = status == sys::CT_OPEN_RETRY;
                    check(status).map(|()| None)
                };
                if result.is_err() {
                    unsafe { drop(Box::from_raw(context.cast::<oneshot::Sender<OpenReply>>())) };
                }
                result
            });
            if !retry {
                break (release?, received);
            }
            tokio::task::yield_now().await;
        };
        let (delivery, wait) = oneshot::channel();
        executor.spawn(async move {
            let result = received
                .await
                .map_err(|_| CtError::Internal)
                .and_then(|mut reply| {
                    check(reply.status)?;
                    let native = reply.tree.take().ok_or(CtError::Internal)?;
                    let mut tree = Self::from_native(native.as_ptr(), runtime)?;
                    tree.backend_release = release.clone();
                    tree.hosted_close = Some((
                        tokio::runtime::Handle::current(),
                        TreeClose(Arc::new(CloseState::new(true))),
                    ));
                    Ok(tree)
                });
            if result.is_err() {
                if let Some(release) = release {
                    release.shutdown().await;
                }
            }
            let _ = delivery.send(result);
        });
        wait.await.map_err(|_| CtError::Internal)?
    }

    fn open_impl(opt: &Config) -> Result<Self, CtError> {
        let out = Self::with_open_options(opt, |options| {
            let mut out: *mut sys::ct_tree = std::ptr::null_mut();
            let status = unsafe {
                sys::ct_open_with_runtime(
                    options,
                    opt.runtime
                        .as_ref()
                        .map_or(std::ptr::null_mut(), |host| host.as_ptr()),
                    &opt.runtime_binding.as_raw(),
                    &mut out,
                )
            };
            check(status)?;
            Ok(out)
        })?;
        Self::from_opened(out, opt.runtime.clone())
    }

    fn with_open_options<T>(
        opt: &Config,
        submit: impl FnOnce(&sys::ct_options) -> Result<T, CtError>,
    ) -> Result<T, CtError> {
        #[cfg(feature = "test-util")]
        ct_init_test_logging();

        let cpath: Option<CString> = opt
            .path
            .as_ref()
            .map(|p| CString::new(p.as_str()).map_err(|_| CtError::InvalidArgument))
            .transpose()?;
        let clog_dir: Option<CString> = if opt.log_dir.is_empty() {
            None
        } else {
            Some(CString::new(opt.log_dir.as_str()).map_err(|_| CtError::InvalidArgument)?)
        };
        let clog_level: Option<CString> = if opt.log_level.is_empty() {
            None
        } else {
            Some(CString::new(opt.log_level.as_str()).map_err(|_| CtError::InvalidArgument)?)
        };
        let clog_prefix: Option<CString> = if opt.log_file_prefix.is_empty() {
            None
        } else {
            Some(CString::new(opt.log_file_prefix.as_str()).map_err(|_| CtError::InvalidArgument)?)
        };
        let (range_bounded, range_start, range_end) = match &opt.key_range {
            KeyRange::Unbounded => (0, None, None),
            KeyRange::Bounded { start, end } => (1, start.as_deref(), end.as_deref()),
        };
        let tuning = opt.tuning.as_raw();
        let copt = sys::ct_options {
            page_store: opt
                .page_store
                .as_ref()
                .map_or(std::ptr::null_mut(), |store| store.as_ptr()),
            range_bounded,
            range_start: range_start.map_or(std::ptr::null(), <[u8]>::as_ptr),
            range_start_len: range_start.map_or(0, <[u8]>::len),
            range_end: range_end.map_or(std::ptr::null(), <[u8]>::as_ptr),
            range_end_len: range_end.map_or(0, <[u8]>::len),
            path: cpath.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            iu_size: opt.iu_size,
            frame_bytes: opt.frame_bytes,
            buffer_pool_bytes: opt.buffer_pool_bytes,
            compression: u8::from(opt.compression_lz4),
            max_inline_value: opt.max_inline_value,
            backend: match opt.backend {
                PageStoreBackend::File => 0,
                PageStoreBackend::Block => 1,
                PageStoreBackend::MemBlock => 2,
            },
            block_size: opt.block_size,
            store_id: opt.store_id,
            group_id: opt.group_id,
            sync_mode: opt.sync_mode.as_u8(),
            log_dir: clog_dir.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            log_level: clog_level.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            log_file_prefix: clog_prefix.as_ref().map_or(std::ptr::null(), |c| c.as_ptr()),
            log_max_file_mb: opt.log_max_file_mb,
            log_max_files: opt.log_max_files,
            tuning: &tuning,
        };
        submit(&copt)
    }

    fn from_opened(
        out: *mut sys::ct_tree,
        runtime: Option<Arc<crate::TreeRuntime>>,
    ) -> Result<Self, CtError> {
        let mut tree = Self::from_native(out, runtime)?;
        tree.backend_release = crate::BackendReleaseService::attach(tree.ptr)?;
        Ok(tree)
    }

    fn from_native(
        out: *mut sys::ct_tree,
        runtime: Option<Arc<crate::TreeRuntime>>,
    ) -> Result<Self, CtError> {
        Ok(Self {
            ptr: NonNull::new(out).ok_or(CtError::Internal)?,
            backend_release: None,
            runtime,
            hosted_close: None,
            close_started: false,
            eventfd_pumps: std::sync::OnceLock::new(),
        })
    }

    /// Transfers the native handle to an asynchronous close. A rejected
    /// submission returns the Tree. Keep the Tokio host, its explicit Tree
    /// runtime and backend release services alive through shutdown.
    ///
    /// # Errors
    /// Returns the native rejection or a missing Tokio host for an explicit
    /// runtime. Retrying a wait on the result does not resubmit the close.
    pub fn begin_close(mut self) -> Result<TreeClose, (CtError, Self)> {
        let (executor, close) = match &self.hosted_close {
            Some((executor, close)) => (Some(executor.clone()), close.clone()),
            None => {
                let executor = if self.runtime.is_some() {
                    match tokio::runtime::Handle::try_current() {
                        Ok(executor) => Some(executor),
                        Err(_) => return Err((CtError::InvalidArgument, self)),
                    }
                } else {
                    None
                };
                (executor, TreeClose(Arc::new(CloseState::new(true))))
            }
        };
        match check(self.submit_close(executor, &close)) {
            Ok(()) => Ok(close),
            Err(error) => Err((error, self)),
        }
    }

    fn submit_close(&mut self, executor: Option<tokio::runtime::Handle>, close: &TreeClose) -> i32 {
        let context = Arc::into_raw(close.0.clone()).cast_mut().cast::<c_void>();
        let status = unsafe { sys::ct_close_async(self.as_ptr(), tree_closed, context) };
        if status != 0 {
            // Rejected submissions never take the facade or callback context.
            drop(unsafe { Arc::from_raw(context.cast::<CloseState>()) });
            return status;
        }
        self.close_started = true;
        if let (Some(executor), Some(runtime)) = (executor, self.runtime.take()) {
            let close = close.clone();
            executor.spawn(async move {
                let _ = close.wait().await;
                drop(runtime);
            });
        }
        status
    }

    /// Observes the close of a hosted Tree without retaining the native facade.
    /// The close starts when the last owner drops or transfers the Tree.
    #[must_use]
    pub fn close_result(&self) -> Option<TreeClose> {
        self.hosted_close.as_ref().map(|(_, close)| close.clone())
    }

    /// Host release handle, present for filesystem Block backends opened from
    /// a Tokio context. Retain it through host shutdown after all readers drop.
    pub fn backend_release_service(&self) -> Option<crate::BackendReleaseService> {
        self.backend_release.clone()
    }

    pub(crate) fn as_ptr(&self) -> *mut sys::ct_tree {
        self.ptr.as_ptr()
    }

    /// Returns an advisory resident index separator without I/O or a full scan.
    pub fn approximate_split_key(&self) -> Result<Option<Vec<u8>>, CtError> {
        let mut found = 0;
        let mut key = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        check(unsafe { sys::ct_approximate_split_key(self.ptr.as_ptr(), &mut found, &mut key) })?;
        let key = take_buf(key);
        Ok((found != 0).then_some(key))
    }

    /// Build an independently owned tree containing exactly `opt.key_range`.
    /// The destination must use an injected store, whose ownership is retained
    /// by the returned tree handle.
    pub fn rebuild_range(&self, opt: &Config) -> Result<(Self, RangeRebuildStats), CtError> {
        let store = opt.page_store.as_ref().ok_or(CtError::InvalidArgument)?;
        let (range_bounded, range_start, range_end) = match &opt.key_range {
            KeyRange::Unbounded => (0, None, None),
            KeyRange::Bounded { start, end } => (1, start.as_deref(), end.as_deref()),
        };
        let tuning = opt.tuning.as_raw();
        let copt = sys::ct_options {
            page_store: store.as_ptr(),
            range_bounded,
            range_start: range_start.map_or(std::ptr::null(), <[u8]>::as_ptr),
            range_start_len: range_start.map_or(0, <[u8]>::len),
            range_end: range_end.map_or(std::ptr::null(), <[u8]>::as_ptr),
            range_end_len: range_end.map_or(0, <[u8]>::len),
            path: std::ptr::null(),
            iu_size: opt.iu_size,
            frame_bytes: opt.frame_bytes,
            buffer_pool_bytes: opt.buffer_pool_bytes,
            compression: u8::from(opt.compression_lz4),
            max_inline_value: opt.max_inline_value,
            backend: 0,
            block_size: 0,
            store_id: opt.store_id,
            group_id: opt.group_id,
            sync_mode: opt.sync_mode.as_u8(),
            log_dir: std::ptr::null(),
            log_level: std::ptr::null(),
            log_file_prefix: std::ptr::null(),
            log_max_file_mb: 0,
            log_max_files: 0,
            tuning: &tuning,
        };
        let mut out = std::ptr::null_mut();
        let mut raw = sys::ct_range_rebuild_stats::default();
        let status = match &opt.runtime {
            Some(runtime) => unsafe {
                sys::ct_rebuild_range_with_runtime(
                    self.as_ptr(),
                    &copt,
                    runtime.as_ptr(),
                    &opt.runtime_binding.as_raw(),
                    &mut out,
                    &mut raw,
                )
            },
            None => unsafe {
                sys::ct_rebuild_range_with_runtime(
                    self.as_ptr(),
                    &copt,
                    std::ptr::null_mut(),
                    &opt.runtime_binding.as_raw(),
                    &mut out,
                    &mut raw,
                )
            },
        };
        check(status)?;
        let tree = Self::from_opened(out, opt.runtime.clone())?;
        Ok((
            tree,
            RangeRebuildStats {
                entries_examined: raw.entries_examined,
                entries_emitted: raw.entries_emitted,
                entries_filtered: raw.entries_filtered,
                pages_reused: raw.pages_reused,
                pages_rebuilt: raw.pages_rebuilt,
                subtrees_skipped: raw.subtrees_skipped,
            },
        ))
    }

    pub fn apply_put(&self, slot: u64, key: &[u8], value: &[u8]) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_apply_put(
                self.as_ptr(),
                slot,
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
            )
        })
    }

    pub fn apply_delete(&self, slot: u64, key: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_apply_delete(self.as_ptr(), slot, key.as_ptr(), key.len()) })
    }

    pub fn force_advance_slot(&self, slot: u64) {
        unsafe { sys::ct_force_advance_slot(self.as_ptr(), slot) }
    }

    /// Convenience: auto-assign the next slot and apply a put (single-writer only).
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_put(
                self.as_ptr(),
                key.as_ptr(),
                key.len(),
                value.as_ptr(),
                value.len(),
            )
        })
    }

    /// Convenience: auto-assign the next slot and apply a delete (single-writer only).
    pub fn del(&self, key: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_del(self.as_ptr(), key.as_ptr(), key.len()) })
    }

    pub fn flush(&self) -> Result<(), CtError> {
        check(unsafe { sys::ct_flush(self.as_ptr()) })
    }

    /// Starts one split-owned shared memtable view and returns its generation fence.
    pub fn begin_split_memtable_view(&self) -> Result<(u64, u64), CtError> {
        let mut generation = 0;
        let mut journal_frontier = 0;
        check(unsafe {
            sys::ct_begin_split_memtable_view(self.as_ptr(), &mut generation, &mut journal_frontier)
        })?;
        Ok((generation, journal_frontier))
    }

    /// Borrows the source's current L0 generations for immediate split reads.
    pub fn install_split_memtable_overlay(
        &self,
        source: &Self,
        journal_frontier: u64,
    ) -> Result<(), CtError> {
        check(unsafe {
            sys::ct_install_split_memtable_overlay(self.as_ptr(), source.as_ptr(), journal_frontier)
        })
    }

    /// Stops borrowing the source L0 after filtered publication completes.
    pub fn clear_split_memtable_overlay(&self, source: &Self) -> Result<(), CtError> {
        check(unsafe { sys::ct_clear_split_memtable_overlay(self.as_ptr(), source.as_ptr()) })
    }

    /// Bulk-publishes the split-owned shared memtable view into one range tree.
    pub fn publish_split_memtable_view(
        &self,
        generation: u64,
        journal_frontier: u64,
        destination: &Self,
        range: &KeyRange,
    ) -> Result<(), CtError> {
        let (start, end) = match range {
            KeyRange::Unbounded => (None, None),
            KeyRange::Bounded { start, end } => (start.as_deref(), end.as_deref()),
        };
        check(unsafe {
            sys::ct_publish_split_memtable_view(
                self.as_ptr(),
                generation,
                journal_frontier,
                destination.as_ptr(),
                start.map_or(std::ptr::null(), <[u8]>::as_ptr),
                start.map_or(0, <[u8]>::len),
                i32::from(start.is_some()),
                end.map_or(std::ptr::null(), <[u8]>::as_ptr),
                end.map_or(0, <[u8]>::len),
                i32::from(end.is_some()),
            )
        })
    }

    /// Releases a shared memtable view after every destination is durable.
    pub fn release_split_memtable_view(&self, generation: u64) -> Result<(), CtError> {
        check(unsafe { sys::ct_release_split_memtable_view(self.as_ptr(), generation) })
    }

    /// Exact checkpoint coverage. A flush only publishes memory and does not
    /// advance this value. Volatile Trees remain at zero.
    pub fn durable_coverage(&self) -> u64 {
        unsafe { sys::ct_durable_coverage(self.as_ptr()) }
    }

    pub fn last_applied_slot(&self) -> u64 {
        unsafe { sys::ct_last_applied_slot(self.as_ptr()) }
    }

    /// Number of frozen memtables waiting to be drained. The maintenance
    /// loop polls this to decide whether to flush immediately.
    pub fn frozen_table_count(&self) -> usize {
        unsafe { sys::ct_frozen_table_count(self.as_ptr()) }
    }

    /// Logical retention GC watermark: `gc_slot = min(snapshot_slot, safe_slot)`.
    /// See `crow::tree::Crowdbtree::set_gc_watermark`.
    pub fn set_gc_watermark(&self, snapshot_slot: u64, safe_slot: u64) {
        unsafe { sys::ct_set_gc_watermark(self.as_ptr(), snapshot_slot, safe_slot) }
    }

    /// True if a demand-load hit an I/O error or CRC mismatch on a committed
    /// page (the offending read degraded to a miss). Latched until cleared.
    pub fn io_failed(&self) -> bool {
        unsafe { sys::ct_io_failed(self.as_ptr()) != 0 }
    }

    pub fn clear_io_error(&self) {
        unsafe { sys::ct_clear_io_error(self.as_ptr()) }
    }

    /// Wipe every key/value back to a fresh, empty tree (the same wipe
    /// `snapshot_import` performs before loading imported entries, exposed
    /// standalone for a caller with nothing to load afterward -- e.g.
    /// resetting a diverged/corrupted replica in place before a later
    /// snapshot import). Not durable by itself -- an explicit `snapshot`/
    /// `flush` afterward is required to persist the wipe to a file-backed
    /// store.
    pub fn clear(&self) -> Result<(), CtError> {
        check(unsafe { sys::ct_clear(self.as_ptr()) })
    }

    /// Flush C++ metrics into a formatted string for the `cpp-tree`
    /// log section. `width` overrides per-section max name length for
    /// column alignment with the Rust section (0 = use C++ internal max).
    pub fn flush_metrics_str(&self, window_secs: f64, timestamp: &str, width: usize) -> String {
        let c_ts = CString::new(timestamp).unwrap_or_default();
        let ptr = unsafe { sys::ct_flush_metrics_str(self.as_ptr(), window_secs, c_ts.as_ptr(), width) };
        if ptr.is_null() {
            return String::new();
        }
        let result = unsafe { std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned() };
        unsafe { sys::ct_free_string(ptr) };
        result
    }

    /// Extended flush with negotiated column widths (count_w, tps_w).
    pub fn flush_metrics_str_ext(
        &self,
        window_secs: f64,
        timestamp: &str,
        width: usize,
        count_w: usize,
        tps_w: usize,
    ) -> String {
        let c_ts = CString::new(timestamp).unwrap_or_default();
        let ptr = unsafe {
            sys::ct_flush_metrics_str_ext(self.as_ptr(), window_secs, c_ts.as_ptr(), width, count_w, tps_w)
        };
        if ptr.is_null() {
            return String::new();
        }
        let result = unsafe { std::ffi::CStr::from_ptr(ptr).to_string_lossy().into_owned() };
        unsafe { sys::ct_free_string(ptr) };
        result
    }

    /// Negotiate column widths: returns C++ preferred (count_w, tps_w).
    pub fn negotiate_widths(&self, rust_count_w: usize, rust_tps_w: usize) -> (usize, usize) {
        let input = sys::ct_column_widths {
            count_w: rust_count_w,
            tps_w: rust_tps_w,
        };
        let mut out = sys::ct_column_widths { count_w: 0, tps_w: 0 };
        unsafe { sys::ct_negotiate_widths(self.as_ptr(), input, &mut out) };
        (out.count_w, out.tps_w)
    }

    /// Current max metric name length from the C++ registry.
    pub fn max_name_len(&self) -> usize {
        unsafe { sys::ct_max_name_len(self.as_ptr()) }
    }

    /// Evict clean, delta-free resident leaf bases down to at most
    /// `max_resident_leaves`. Test/ops hook -- forces the
    /// demand-load path a subsequent `get`/`AsyncCrowdbtree::get` will have to
    /// take. Returns the number of leaves evicted.
    pub fn evict_clean_leaves(&self, max_resident_leaves: u64) -> u64 {
        unsafe { sys::ct_evict_clean_leaves(self.as_ptr(), max_resident_leaves) }
    }

    /// D3: same contract, but for resident *inner* bases --
    /// a genuinely separate ranked budget/pass from [`Self::evict_clean_leaves`],
    /// never evicting a leaf. Returns the number of inner bases evicted.
    pub fn evict_clean_inner(&self, max_resident_inner: u64) -> u64 {
        unsafe { sys::ct_evict_clean_inner(self.as_ptr(), max_resident_inner) }
    }

    /// Evicts clean leaf bases and awaits the number of references removed.
    ///
    /// # Errors
    /// Returns a native admission error if eviction cannot start.
    pub async fn evict_clean_leaves_async(&self, max_resident: u64) -> Result<u64, CtError> {
        self.evict_clean_pages_async(false, max_resident).await
    }

    /// Evicts clean index bases and awaits the number of references removed.
    ///
    /// # Errors
    /// Returns a native admission error if eviction cannot start.
    pub async fn evict_clean_inner_async(&self, max_resident: u64) -> Result<u64, CtError> {
        self.evict_clean_pages_async(true, max_resident).await
    }

    async fn evict_clean_pages_async(&self, inner: bool, max_resident: u64) -> Result<u64, CtError> {
        let (sender, received) = oneshot::channel::<u64>();
        {
            let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
            let status = unsafe {
                sys::ct_evict_clean_pages_async(
                    self.as_ptr(),
                    u8::from(inner),
                    max_resident,
                    pages_evicted,
                    context,
                )
            };
            if let Err(error) = check(status) {
                unsafe { drop(Box::from_raw(context.cast::<oneshot::Sender<u64>>())) };
                return Err(error);
            }
        }
        received.await.map_err(|_| CtError::Internal)
    }

    /// The tree's DiskIOUring eventfds (one per pipeline), or empty if
    /// none is wired (an in-memory tree, or a build without liburing).
    /// DiskIOUring-owned -- callers
    /// must never close them.
    fn uring_eventfds(&self) -> Vec<RawFd> {
        // Query the count first, then fetch the fds.
        let count = unsafe { sys::ct_uring_eventfds(self.as_ptr(), std::ptr::null_mut(), 0) };
        if count == 0 {
            return Vec::new();
        }
        let mut fds = vec![0i32; count];
        let got = unsafe { sys::ct_uring_eventfds(self.as_ptr(), fds.as_mut_ptr(), count) };
        fds.truncate(got);
        fds
    }

    /// True when the tree was opened with a durable path and the build has
    /// an io_uring DiskIOUring wired. In-memory trees and non-Linux/liburing
    /// builds return `false`; `ct_get_async`/`ct_scan_async` then complete
    /// synchronously, so callers cannot observe a genuine `Pending` future.
    #[must_use]
    pub fn is_reactor_available(&self) -> bool {
        !self.uring_eventfds().is_empty()
    }

    /// Lazily spawns (once) and returns the `Notify` fanned out by this
    /// tree's eventfd pumps -- one pump per pipeline eventfd, all sharing
    /// a single `Arc<Notify>`. See the module-level fan-out note in the reactor module for why a single Notify (not a per-future registration)
    /// is required. `None` if there's no DiskIOUring wired, or all pumps
    /// failed to spawn. Must be called from within a Tokio runtime context;
    /// every call site is inside an async fn body driven by one, so this
    /// always holds.
    pub(crate) fn eventfd_notify(&self) -> Option<Arc<tokio::sync::Notify>> {
        let pumps = self.eventfd_pumps.get_or_init(|| {
            let fds = self.uring_eventfds();
            if fds.is_empty() {
                return Vec::new();
            }
            let notify = Arc::new(tokio::sync::Notify::new());
            let mut pumps = Vec::with_capacity(fds.len());
            for fd in fds {
                // SAFETY: self retains the native owner throughout duplication.
                let owned_fd = match unsafe { duplicate_eventfd(fd) } {
                    Ok(fd) => fd,
                    Err(_) => continue,
                };
                let async_fd = match AsyncFd::new(owned_fd) {
                    Ok(af) => af,
                    Err(_) => continue, // skip this pump, try the rest
                };
                let pump_notify = notify.clone();
                let task = tokio::spawn(async move {
                    loop {
                        let mut guard = match async_fd.readable().await {
                            Ok(g) => g,
                            Err(_) => return, // I/O driver gone; stop pumping.
                        };
                        drain_eventfd(async_fd.as_raw_fd());
                        guard.clear_ready();
                        pump_notify.notify_waiters();
                    }
                })
                .abort_handle();
                pumps.push(EventfdPump {
                    notify: notify.clone(),
                    task,
                });
            }
            pumps
        });
        if pumps.is_empty() {
            None
        } else {
            Some(pumps[0].notify.clone())
        }
    }

    /// Point read. Returns `None` for absent / tombstoned keys.
    pub fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let mut found: c_int = 0;
        let mut slot = 0u64;
        let mut val = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        check(unsafe {
            sys::ct_get(
                self.as_ptr(),
                key.as_ptr(),
                key.len(),
                &mut found,
                &mut slot,
                &mut val,
            )
        })?;
        let value = take_buf(val);
        if found == 0 {
            Ok(None)
        } else {
            Ok(Some((slot, value)))
        }
    }
}

impl Drop for Crowdbtree {
    fn drop(&mut self) {
        // Cancel pumps before native teardown. Their independently owned fds
        // stay live until task cancellation has deregistered them from Tokio,
        // so opening another Tree immediately cannot reuse an active fd.
        if let Some(pumps) = self.eventfd_pumps.get() {
            for pump in pumps {
                pump.task.abort();
            }
        }
        if self.close_started {
            return;
        }
        if let Some((executor, close)) = self.hosted_close.take() {
            let status = self.submit_close(Some(executor), &close);
            if status != 0 {
                // Hosted constructors guarantee async close capability. A
                // rejected close must be visible to the host and must not
                // synchronously drain on the thread releasing the last owner.
                close.0.finish(status);
                eprintln!("critical: hosted Tree close rejected with status {status}");
            }
        } else {
            unsafe { sys::ct_close(self.ptr.as_ptr()) };
        }
    }
}

/// Initialize the C++ spdlog async file logger. Call this once at process
/// startup, before any `Crowdbtree::open`. All parameters map to the C++
/// `init_logging()` function. Safe to call when the build has no spdlog
/// (no-op).
///
/// - `log_dir` — directory for log files (empty => stderr)
/// - `level` — spdlog level name (trace/debug/info/warn/error/off)
/// - `max_file_mb` — max file size before rotation (0 => 30)
/// - `max_files` — max rotated files to keep (0 => 5)
/// - `file_prefix` — filename prefix (empty => "crowdb-tree")
pub fn ct_init_logging(log_dir: &str, level: &str, max_file_mb: usize, max_files: usize, file_prefix: &str) {
    let log_dir_c = std::ffi::CString::new(log_dir).unwrap_or_default();
    let level_c = std::ffi::CString::new(level).unwrap_or_default();
    let prefix_c = std::ffi::CString::new(file_prefix).unwrap_or_default();
    unsafe {
        sys::ct_init_logging(
            log_dir_c.as_ptr(),
            level_c.as_ptr(),
            max_file_mb,
            max_files,
            prefix_c.as_ptr(),
        );
    }
}

/// Flush buffered C++ log messages to disk without stopping the logger.
/// Safe to call when logging was never initialized (no-op).
pub fn ct_flush_logging() {
    unsafe { sys::ct_flush_logging() };
}

/// Flush and stop the C++ spdlog async logger. Call this during process
/// shutdown (after all `Crowdbtree` instances are dropped) to ensure
/// buffered log messages are written to disk. Safe to call when logging
/// was never initialized (no-op in that case).
pub fn ct_shutdown_logging() {
    #[cfg(feature = "chunk-rpc")]
    assert!(
        crowdb_rpc_ffi::OwnedClientRoute::drain_retired(std::time::Duration::from_secs(30)),
        "RPC route owner teardown did not drain before native logging shutdown"
    );
    unsafe { sys::ct_shutdown_logging() };
}

/// Add a stderr sink with a per-sink level filter (e.g. "error").
/// Only messages at or above `level` go to stderr; file sinks keep their
/// original level. No-op when the C++ build has no spdlog.
pub fn ct_add_log_stderr(level: &str) {
    let level_c = CString::new(level).unwrap_or_default();
    unsafe { sys::ct_add_log_stderr(level_c.as_ptr()) };
}

static TEST_LOGGING_INIT: Once = Once::new();

/// Initialize C++ spdlog to write to a per-process directory under
/// `<workspace_root>/.crowdb-runtime/ephemeral/`. Idempotent (guarded by `Once`); safe
/// to call from every test. Redirects C++ tree/engine logs to files
/// under the process runtime namespace instead of stderr. Error-
/// level messages are also mirrored to stderr so they are visible in CI
/// output for debugging. No-op when the C++ build was compiled without
/// `CROWDB_HAVE_SPDLOG`.
pub fn ct_init_test_logging() {
    TEST_LOGGING_INIT.call_once(|| {
        let dir = test_log_dir().join(format!("crowdb-tree-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let dir_str = dir.to_string_lossy().into_owned();
        ct_init_logging(&dir_str, "info", 30, 5, "test");
        ct_add_log_stderr("error");
    });
}

/// Find the workspace root by walking up from `CARGO_MANIFEST_DIR`
/// until a `pixi.toml` marker is found.
fn workspace_root() -> std::path::PathBuf {
    let mut dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    while !dir.join("pixi.toml").exists() {
        if !dir.pop() {
            return std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        }
    }
    dir
}

/// Process-local log root below the workspace runtime namespace.
fn test_log_dir() -> std::path::PathBuf {
    let dir = workspace_root()
        .join(".crowdb-runtime")
        .join("ephemeral")
        .join(format!("legacy-process-{}", std::process::id()))
        .join("log");
    let _ = std::fs::create_dir_all(&dir);
    dir
}

#[cfg(test)]
mod open_reply_tests {
    use super::*;
    use crate::{PageStore, TreeRuntime, TreeRuntimeOptions};

    #[tokio::test(flavor = "current_thread")]
    async fn undelivered_native_open_releases_its_runtime_binding() {
        let runtime = Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
        );
        for receive in [true, false] {
            let config = Config {
                page_store: Some(Arc::new(PageStore::open_mem(1).unwrap())),
                ..Default::default()
            };
            let (sender, received) = oneshot::channel::<OpenReply>();
            let received = if receive {
                Some(received)
            } else {
                drop(received);
                None
            };
            Crowdbtree::with_open_options(&config, |options| {
                let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
                let status = unsafe {
                    sys::ct_open_with_runtime_async(
                        options,
                        runtime.as_ptr(),
                        &config.runtime_binding.as_raw(),
                        tree_opened,
                        context,
                    )
                };
                if status != 0 {
                    unsafe { drop(Box::from_raw(context.cast::<oneshot::Sender<OpenReply>>())) };
                }
                check(status)
            })
            .unwrap();
            if let Some(received) = received {
                let reply = received.await.unwrap();
                check(reply.status).unwrap();
                assert!(reply.tree.is_some());
            }
        }
        tokio::time::timeout(std::time::Duration::from_secs(5), runtime.shutdown())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(runtime.snapshot().unwrap().bindings, 0);
    }
}
