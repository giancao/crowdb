// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Independently owned immutable runtime view. No second writer is opened.
use crate::close::CloseState;
use crate::error::check;
use crate::reactor::{try_poll_ct_future, FutureGuard, FutureKind, RawOutcome};
use crate::{sys, Config, Crowdbtree, CtError, RangeRebuildStats, ScanDirection, ScanEntry, ScanOptions};
use std::ffi::c_void;
use std::ptr::NonNull;
use std::sync::Arc;
use tokio::sync::oneshot;

unsafe extern "C" fn close_completed(context: *mut c_void) {
    unsafe { Arc::from_raw(context.cast::<CloseState>()) }.finish(0);
}

type ReadCompletion = oneshot::Sender<(i32, FutureGuard)>;

unsafe extern "C" fn read_completed(context: *mut c_void, status: i32, result: *mut sys::ct_future) {
    let sender = unsafe { Box::from_raw(context.cast::<ReadCompletion>()) };
    let _ = sender.send((status, FutureGuard(result)));
}

fn submit_read(
    submit: impl FnOnce(*mut c_void) -> i32,
) -> Result<oneshot::Receiver<(i32, FutureGuard)>, CtError> {
    let (sender, receiver) = oneshot::channel();
    let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
    let status = submit(context);
    if status != 0 {
        // Rejected submissions leave callback ownership with the caller.
        drop(unsafe { Box::from_raw(context.cast::<ReadCompletion>()) });
    }
    check(status)?;
    Ok(receiver)
}

async fn read_result(
    receiver: oneshot::Receiver<(i32, FutureGuard)>,
    kind: FutureKind,
) -> Result<RawOutcome, CtError> {
    let (status, mut guard) = receiver.await.map_err(|_| CtError::Unavailable)?;
    check(status)?;
    try_poll_ct_future(&mut guard, kind).ok_or(CtError::Internal)?
}

type RebuildCompletion = oneshot::Sender<Result<RangeRebuildStats, CtError>>;

unsafe extern "C" fn rebuild_completed(
    context: *mut c_void,
    status: i32,
    stats: sys::ct_range_rebuild_stats,
) {
    let sender = unsafe { Box::from_raw(context.cast::<RebuildCompletion>()) };
    let _ = sender.send(check(status).map(|()| range_stats(stats)));
}

fn range_stats(stats: sys::ct_range_rebuild_stats) -> RangeRebuildStats {
    RangeRebuildStats {
        entries_examined: stats.entries_examined,
        entries_emitted: stats.entries_emitted,
        entries_filtered: stats.entries_filtered,
        pages_reused: stats.pages_reused,
        pages_rebuilt: stats.pages_rebuilt,
        subtrees_skipped: stats.subtrees_skipped,
    }
}

/// An immutable view independent of the source Tree. The external runtime host
/// must remain alive until accepted reads and close completion have drained.
#[derive(Debug)]
pub struct ReadOnlyTreeView {
    handle: NonNull<sys::ct_read_only_view>,
    sequence: u64,
    coverage: u64,
    close: Arc<CloseState>,
}

// SAFETY: the native handle owns an immutable mapping; each read has independent
// paid operation state and page store ownership. Destruction follows host ownership.
unsafe impl Send for ReadOnlyTreeView {}
unsafe impl Sync for ReadOnlyTreeView {}

impl Drop for ReadOnlyTreeView {
    fn drop(&mut self) {
        unsafe { sys::ct_read_only_view_free(self.handle.as_ptr()) };
    }
}

impl Crowdbtree {
    /// Capture exactly the expected completed L1 coverage. A nonzero sequence
    /// also requires the current durable sequence and coverage to match.
    /// # Errors
    /// Rejects changed coverage, unsupported page stores, or unavailable budget.
    pub fn read_only_view(&self, sequence: u64, coverage: u64) -> Result<ReadOnlyTreeView, CtError> {
        let mut raw = std::ptr::null_mut();
        check(unsafe { sys::ct_read_only_view_capture(self.as_ptr(), sequence, coverage, &mut raw) })?;
        Ok(ReadOnlyTreeView {
            handle: NonNull::new(raw).ok_or(CtError::Internal)?,
            sequence,
            coverage,
            close: Arc::new(CloseState::new(false)),
        })
    }

    /// Rebuilds a bounded range from the live Tree's completed L1. An ordinary
    /// flush catches up the source if necessary, then only the destination is
    /// persisted.
    ///
    /// # Errors
    /// Returns a flush, admission, read, import, or durable commit failure.
    pub async fn rebuild_bounded_range_async(
        &self,
        options: &Config,
    ) -> Result<(Crowdbtree, RangeRebuildStats), CtError> {
        if !matches!(options.key_range, crate::KeyRange::Bounded { .. }) {
            return Err(CtError::InvalidArgument);
        }
        rebuild_into_async(options, |destination, context| unsafe {
            sys::ct_rebuild_bounded_range_async(
                self.as_ptr(),
                destination.as_ptr(),
                rebuild_completed,
                context,
            )
        })
        .await
    }
}

impl ReadOnlyTreeView {
    /// Stops read admission and awaits the accepted windows and lane tails.
    /// A dropped waiter or `tokio::time::timeout` leaves the same close
    /// running. Keep the external runtime host alive through completion.
    ///
    /// # Errors
    /// Returns a failure to initiate the native close.
    pub async fn close(&self) -> Result<(), CtError> {
        if self.close.start() {
            let context = Arc::into_raw(self.close.clone()).cast_mut().cast::<c_void>();
            let status =
                unsafe { sys::ct_read_only_view_close_async(self.handle.as_ptr(), close_completed, context) };
            if status != 0 {
                unsafe { Arc::from_raw(context.cast::<CloseState>()) }.finish(status);
            }
        }
        self.close.wait().await
    }

    #[must_use]
    pub const fn checkpoint_state(&self) -> (u64, u64) {
        (self.sequence, self.coverage)
    }

    /// Read a key from the fixed view. The copying result is application-owned.
    /// # Errors
    /// Returns a typed read or admission failure.
    pub async fn get(&self, key: &[u8]) -> Result<Option<(u64, Vec<u8>)>, CtError> {
        let receiver = submit_read(|context| unsafe {
            sys::ct_read_only_view_get_async(
                self.handle.as_ptr(),
                key.as_ptr(),
                key.len(),
                read_completed,
                context,
            )
        })?;
        let result = read_result(receiver, FutureKind::Get).await?;
        Ok(result.found.then_some((result.slot, result.value)))
    }

    /// Read one bounded window with independently owned result bytes.
    /// # Errors
    /// Returns invalid boundaries, read failures, or insufficient budget.
    pub async fn scan(&self, options: &ScanOptions) -> Result<(Vec<ScanEntry>, bool), CtError> {
        let receiver = submit_read(|context| {
            let start = options.start.as_deref().unwrap_or_default();
            let raw = sys::ct_scan_options_v2 {
                struct_size: std::mem::size_of::<sys::ct_scan_options_v2>(),
                prefix: options.prefix.as_ptr(),
                prefix_len: options.prefix.len(),
                start: start.as_ptr(),
                start_len: start.len(),
                end: options.end.as_ptr(),
                end_len: options.end.len(),
                begin: options.begin.as_ptr(),
                begin_len: options.begin.len(),
                limit: options.limit,
                byte_budget: options.byte_budget,
                deadline_ms: options.deadline_ms,
                reverse: i32::from(options.direction == ScanDirection::Reverse),
                keys_only: i32::from(options.keys_only),
                include_tombstones: i32::from(options.include_tombstones),
                has_start_bound: i32::from(options.start.is_some()),
                start_inclusive: i32::from(options.start_inclusive),
                strict_byte_budget: i32::from(options.strict_byte_budget),
            };
            unsafe { sys::ct_read_only_view_scan_async(self.handle.as_ptr(), &raw, read_completed, context) }
        })?;
        let result = read_result(receiver, FutureKind::ScanOwned).await?;
        let packed = result.packed.ok_or(CtError::Internal)?;
        let count = usize::try_from(result.slot).map_err(|_| CtError::Corruption)?;
        Ok((crate::scan::decode_scan_bytes(packed, count)?, result.found))
    }

    /// Creates a separate runtime destination/account and rebuilds only its
    /// configured range. Captured pages remain valid after the source closes.
    /// # Errors
    /// Returns destination admission, read, import, or durable commit failures.
    pub fn rebuild_range(&self, options: &Config) -> Result<(Crowdbtree, RangeRebuildStats), CtError> {
        if options.page_store.is_none() {
            return Err(CtError::InvalidArgument);
        }
        let destination = Crowdbtree::open(options)?;
        let mut stats = sys::ct_range_rebuild_stats::default();
        check(unsafe {
            sys::ct_read_only_view_rebuild(self.handle.as_ptr(), destination.as_ptr(), &mut stats)
        })?;
        Ok((destination, range_stats(stats)))
    }

    /// Rebuilds into an independent Tree and waits for its durable publication.
    /// Accepted work retains the destination when the caller cancels its wait.
    ///
    /// # Errors
    /// Returns a destination admission, read, import, or durable commit failure.
    pub async fn rebuild_range_async(
        &self,
        options: &Config,
    ) -> Result<(Crowdbtree, RangeRebuildStats), CtError> {
        rebuild_into_async(options, |destination, context| unsafe {
            sys::ct_read_only_view_rebuild_async(
                self.handle.as_ptr(),
                destination.as_ptr(),
                rebuild_completed,
                context,
            )
        })
        .await
    }
}

async fn rebuild_into_async(
    options: &Config,
    submit: impl FnOnce(&Crowdbtree, *mut c_void) -> i32,
) -> Result<(Crowdbtree, RangeRebuildStats), CtError> {
    if options.page_store.is_none() {
        return Err(CtError::InvalidArgument);
    }
    let executor = tokio::runtime::Handle::try_current().map_err(|_| CtError::InvalidArgument)?;
    let tree = Crowdbtree::open_async(options).await?;
    let (sender, received) = oneshot::channel::<Result<RangeRebuildStats, CtError>>();
    {
        let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
        if let Err(error) = check(submit(&tree, context)) {
            drop(unsafe { Box::from_raw(context.cast::<RebuildCompletion>()) });
            return Err(error);
        }
    }
    executor
        .spawn(async move {
            match received.await.unwrap_or(Err(CtError::Internal)) {
                Ok(stats) => Ok((tree, stats)),
                Err(error) => {
                    if let Ok(close) = tree.begin_close() {
                        let _ = close.wait().await;
                    }
                    Err(error)
                }
            }
        })
        .await
        .map_err(|_| CtError::Internal)?
}
