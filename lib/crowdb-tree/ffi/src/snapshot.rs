// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::c_void;
use std::os::raw::c_int;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::sync::oneshot;

use crate::error::{check, take_buf, CtError};
use crate::scan::ViewEntry;
use crate::sys;
use crate::tree::Crowdbtree;
use crate::AsyncCrowdbtree;

mod owner;
pub use owner::OwnedSnapshotChunk;
type ChunkResult = Result<OwnedSnapshotChunk, CtError>;

/// Immutable metadata captured when a portable snapshot export begins.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SnapshotMetadata {
    pub at_slot: u64,
    pub total_bytes: u64,
    pub final_crc32c: u32,
    pub chunk_bytes: usize,
}

/// One sequential chunk returned by a snapshot export session.
#[derive(Debug, PartialEq, Eq)]
pub struct SnapshotChunk {
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub done: bool,
}

/// Non-clone owner of one C snapshot-export handle.
pub struct SnapshotExportSession {
    handle: Arc<ExportHandle>,
    _tree: Option<Arc<Crowdbtree>>,
    metadata: SnapshotMetadata,
    pending: Option<(u64, oneshot::Receiver<ChunkResult>)>,
}

struct ExportHandle(NonNull<sys::ct_export>);

// SAFETY: the session and its one outstanding native operation use the cursor
// sequentially. Callback owners share only its lifetime, through the Arc.
unsafe impl Send for ExportHandle {}
unsafe impl Sync for ExportHandle {}

impl Drop for ExportHandle {
    fn drop(&mut self) {
        unsafe { sys::ct_snapshot_export_end(self.0.as_ptr()) };
    }
}

type ExportReply = oneshot::Sender<Result<ExportHandle, CtError>>;

unsafe extern "C" fn export_ready(context: *mut c_void, status: c_int, handle: *mut sys::ct_export) {
    let reply = unsafe { Box::from_raw(context.cast::<ExportReply>()) };
    let handle = NonNull::new(handle).map(ExportHandle);
    let _ = reply.send(check(status).and_then(|()| handle.ok_or(CtError::Internal)));
}

struct ReadReply {
    _handle: Arc<ExportHandle>,
    offset: u64,
    sender: oneshot::Sender<ChunkResult>,
}

unsafe extern "C" fn chunk_ready(
    context: *mut c_void,
    status: c_int,
    chunk: *mut sys::ct_snapshot_chunk,
    done: c_int,
) {
    let reply = unsafe { Box::from_raw(context.cast::<ReadReply>()) };
    let result = check(status).and_then(|()| owner::from_raw(chunk, reply.offset, done));
    let _ = reply.sender.send(result);
}

impl SnapshotExportSession {
    fn new(handle: ExportHandle, tree: Option<Arc<Crowdbtree>>) -> Self {
        let ptr = handle.0.as_ptr();
        let metadata = SnapshotMetadata {
            at_slot: unsafe { sys::ct_snapshot_export_at_slot(ptr) },
            total_bytes: unsafe { sys::ct_snapshot_export_total_bytes(ptr) },
            final_crc32c: unsafe { sys::ct_snapshot_export_final_crc32c(ptr) },
            chunk_bytes: unsafe { sys::ct_snapshot_export_chunk_bytes(ptr) },
        };
        Self {
            _tree: tree,
            handle: Arc::new(handle),
            metadata,
            pending: None,
        }
    }

    #[must_use]
    pub const fn metadata(&self) -> SnapshotMetadata {
        self.metadata
    }

    #[must_use]
    pub fn offset(&self) -> u64 {
        self.pending.as_ref().map_or_else(
            || unsafe { sys::ct_snapshot_export_offset(self.handle.0.as_ptr()) },
            |(offset, _)| *offset,
        )
    }

    /// Retains the native chunk allocation through every returned Bytes clone
    /// and slice, even after this session and its Tree are closed.
    pub fn read_owned(&mut self, offset: u64) -> Result<OwnedSnapshotChunk, CtError> {
        if self.pending.is_some() {
            return Err(CtError::Unavailable);
        }
        owner::read(self.handle.0.as_ptr(), offset)
    }

    /// Awaits one owned chunk. A canceled wait can resume at the same offset.
    pub async fn read_owned_async(&mut self, offset: u64) -> Result<OwnedSnapshotChunk, CtError> {
        if offset != self.offset() {
            return Err(CtError::InvalidArgument);
        }
        if self.pending.is_none() {
            let (sender, receiver) = oneshot::channel();
            let context = Box::into_raw(Box::new(ReadReply {
                _handle: Arc::clone(&self.handle),
                offset,
                sender,
            }));
            if let Err(error) = check(unsafe {
                sys::ct_snapshot_export_next_async(
                    self.handle.0.as_ptr(),
                    offset,
                    chunk_ready,
                    context.cast(),
                )
            }) {
                unsafe { drop(Box::from_raw(context)) };
                return Err(error);
            }
            self.pending = Some((offset, receiver));
        }
        let (_, receiver) = self.pending.as_mut().expect("export read submitted");
        let result = receiver.await;
        self.pending = None;
        result.map_err(|_| CtError::Internal)?
    }

    /// Read the next sequential chunk. A source registry may cache this result
    /// to serve an exact retry without advancing the C encoder twice.
    pub fn read(&mut self, offset: u64) -> Result<SnapshotChunk, CtError> {
        if self.pending.is_some() {
            return Err(CtError::Unavailable);
        }
        let mut chunk = sys::ct_buf {
            data: std::ptr::null_mut(),
            len: 0,
        };
        let mut done: c_int = 0;
        check(unsafe {
            sys::ct_snapshot_export_next(self.handle.0.as_ptr(), offset, &mut chunk, &mut done)
        })?;
        Ok(SnapshotChunk {
            offset,
            bytes: take_buf(chunk),
            done: done != 0,
        })
    }
}

impl AsyncCrowdbtree {
    /// Flushes and preflights a portable export on one fixed published view.
    pub async fn snapshot_export_begin(&self, chunk_bytes: usize) -> Result<SnapshotExportSession, CtError> {
        if chunk_bytes == 0 || chunk_bytes > 1024 * 1024 {
            return Err(CtError::InvalidArgument);
        }
        let flush = self.flush_report().await?;
        flush.status?;
        let (reply, result) = oneshot::channel::<Result<ExportHandle, CtError>>();
        let context = Box::into_raw(Box::new(reply));
        if let Err(error) = check(unsafe {
            sys::ct_snapshot_export_begin_published_async(
                self.handle().as_ptr(),
                chunk_bytes,
                export_ready,
                context.cast(),
            )
        }) {
            unsafe { drop(Box::from_raw(context)) };
            return Err(error);
        }
        let handle = result.await.map_err(|_| CtError::Internal)??;
        let session = SnapshotExportSession::new(handle, None);
        if session.metadata().at_slot < flush.published_coverage {
            return Err(CtError::Unavailable);
        }
        Ok(session)
    }
}

/// Non-clone owner of one C snapshot-import handle.
pub struct SnapshotImportSession {
    _tree: Arc<Crowdbtree>,
    ptr: NonNull<sys::ct_import>,
}

type ImportReply = oneshot::Sender<Result<u64, CtError>>;

unsafe extern "C" fn import_finished(context: *mut c_void, status: c_int, coverage: u64) {
    let reply = unsafe { Box::from_raw(context.cast::<ImportReply>()) };
    let _ = reply.send(check(status).map(|()| coverage));
}

// As with export, the importer is moved between tasks but is only ever
// accessed through its unique mutable owner.
unsafe impl Send for SnapshotImportSession {}

impl SnapshotImportSession {
    pub fn feed(&mut self, chunk: &[u8]) -> Result<(), CtError> {
        check(unsafe { sys::ct_snapshot_import_feed(self.ptr.as_ptr(), chunk.as_ptr(), chunk.len()) })
    }

    pub fn finish(self) -> Result<u64, CtError> {
        let mut at_slot = 0;
        check(unsafe { sys::ct_snapshot_import_finish(self.ptr.as_ptr(), &mut at_slot) })?;
        Ok(at_slot)
    }

    /// Installs the staged image and awaits Tree publication. Dropping this
    /// wait leaves an accepted installation to finish on the native runtime.
    pub async fn finish_async(self) -> Result<u64, CtError> {
        let (reply, result) = oneshot::channel::<Result<u64, CtError>>();
        let context = Box::into_raw(Box::new(reply));
        if let Err(error) = check(unsafe {
            sys::ct_snapshot_import_finish_async(self.ptr.as_ptr(), import_finished, context.cast())
        }) {
            unsafe { drop(Box::from_raw(context)) };
            return Err(error);
        }
        result.await.map_err(|_| CtError::Internal)?
    }

    pub fn abort(self) {}
}

impl Drop for SnapshotImportSession {
    fn drop(&mut self) {
        unsafe { sys::ct_snapshot_import_end(self.ptr.as_ptr()) };
    }
}

impl Crowdbtree {
    /// Begin a bounded portable export. The returned owner keeps this tree
    /// alive and closes the C handle exactly once on drop.
    pub fn snapshot_export_begin(
        self: &Arc<Self>,
        chunk_bytes: usize,
    ) -> Result<SnapshotExportSession, CtError> {
        let mut raw: *mut sys::ct_export = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_export_begin(self.as_ptr(), chunk_bytes, &mut raw) })?;
        let handle = ExportHandle(NonNull::new(raw).ok_or(CtError::Internal)?);
        Ok(SnapshotExportSession::new(handle, Some(Arc::clone(self))))
    }

    /// Begin a portable import. Feeding does not change the tree; finish
    /// verifies the complete stream before installing it.
    pub fn snapshot_import_begin(self: &Arc<Self>) -> Result<SnapshotImportSession, CtError> {
        let mut raw: *mut sys::ct_import = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_import_begin(self.as_ptr(), &mut raw) })?;
        Ok(SnapshotImportSession {
            _tree: Arc::clone(self),
            ptr: NonNull::new(raw).ok_or(CtError::Internal)?,
        })
    }

    pub fn flush_with_persistent(&self) -> Result<u64, CtError> {
        let mut last = 0u64;
        check(unsafe { sys::ct_flush_with_persistent(self.as_ptr(), &mut last) })?;
        Ok(last)
    }

    /// Runs flush with persistent and returns `(durable sequence, applied slot)`.
    pub fn flush_with_persistent_info(&self) -> Result<(u64, u64), CtError> {
        let mut generation = 0_u64;
        let mut last_applied = 0_u64;
        check(unsafe {
            sys::ct_flush_with_persistent_info(self.as_ptr(), &mut generation, &mut last_applied)
        })?;
        Ok((generation, last_applied))
    }

    /// Return the current durable sequence and frontier.
    pub fn durable_state(&self) -> Result<(u64, u64), CtError> {
        let mut generation = 0;
        let mut last_applied = 0;
        check(unsafe { sys::ct_durable_state(self.as_ptr(), &mut generation, &mut last_applied) })?;
        Ok((generation, last_applied))
    }

    /// Run one bounded pass that replaces shared immutable page store objects
    /// with objects owned by this tree's lineage.
    pub fn materialize_ownership(&self) -> Result<(u64, bool), CtError> {
        let mut bytes_written = 0;
        let mut complete = 0;
        check(unsafe { sys::ct_materialize_ownership(self.as_ptr(), &mut bytes_written, &mut complete) })?;
        Ok((bytes_written, complete != 0))
    }

    /// Materialize the durable snapshot view (key-sorted, includes tombstones).
    pub fn snapshot_view(&self) -> Result<(u64, Vec<ViewEntry>), CtError> {
        let mut view: *mut sys::ct_view = std::ptr::null_mut();
        check(unsafe { sys::ct_snapshot_view(self.as_ptr(), &mut view) })?;
        let at = unsafe { sys::ct_view_at_slot(view) };
        let mut it: *mut sys::ct_iter = std::ptr::null_mut();
        let rc = unsafe { sys::ct_view_iter(view, &mut it) };
        if rc != 0 {
            unsafe { sys::ct_view_release(view) };
            return Err(check(rc).unwrap_err());
        }
        let mut out = Vec::new();
        loop {
            let mut key = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut value = sys::ct_buf {
                data: std::ptr::null_mut(),
                len: 0,
            };
            let mut slot = 0u64;
            let mut kind = 0u8;
            let mut valid: c_int = 0;
            let rc = unsafe { sys::ct_iter_next(it, &mut key, &mut slot, &mut kind, &mut value, &mut valid) };
            if rc != 0 {
                unsafe {
                    sys::ct_iter_release(it);
                    sys::ct_view_release(view);
                }
                return Err(check(rc).unwrap_err());
            }
            let k = take_buf(key);
            let v = take_buf(value);
            if valid == 0 {
                break;
            }
            out.push(ViewEntry {
                key: k,
                slot,
                tombstone: kind == 1,
                value: v,
            });
        }
        unsafe {
            sys::ct_iter_release(it);
            sys::ct_view_release(view);
        }
        Ok((at, out))
    }
}
