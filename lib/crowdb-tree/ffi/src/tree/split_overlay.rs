// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::{error::check, sys, Crowdbtree, CtError, KeyRange};
use std::ffi::c_void;
use std::{ptr::NonNull, sync::Arc};
use tokio::sync::oneshot;

#[derive(Clone, Copy, Debug)]
pub struct SplitOverlayLimits {
    pub max_records: u64,
    pub max_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct InstalledSplit {
    pub generation: u64,
    pub frontier: u64,
}

/// An owned native installation capability; dropping it cancels an unused lease.
pub struct PreparedSplitOverlay {
    raw: Option<NonNull<sys::ct_prepared_split_overlay>>,
    start_frontier: u64,
    async_release: bool,
    _source: Arc<Crowdbtree>,
    _destination: Arc<Crowdbtree>,
}

// SAFETY: the native capability serializes state transitions atomically and
// supports concurrent read-only queries. Its Tree owners outlive every call.
unsafe impl Send for PreparedSplitOverlay {}
unsafe impl Sync for PreparedSplitOverlay {}

struct ArmReply {
    raw: usize,
    status: i32,
    start_frontier: u64,
}

impl Drop for ArmReply {
    fn drop(&mut self) {
        if let Some(raw) = NonNull::new(self.raw as *mut _) {
            release_detached(raw);
        }
    }
}

unsafe extern "C" fn overlay_armed(
    context: *mut c_void,
    status: i32,
    prepared: *mut sys::ct_prepared_split_overlay,
    start_frontier: u64,
) {
    let sender = unsafe { Box::from_raw(context.cast::<oneshot::Sender<ArmReply>>()) };
    let _ = sender.send(ArmReply {
        raw: prepared as usize,
        status,
        start_frontier,
    });
}

unsafe extern "C" fn status_completed(context: *mut c_void, status: i32) {
    let sender = unsafe { Box::from_raw(context.cast::<oneshot::Sender<i32>>()) };
    let _ = sender.send(status);
}

fn submit_status(submit: impl FnOnce(*mut c_void) -> i32) -> Result<oneshot::Receiver<i32>, CtError> {
    let (sender, receiver) = oneshot::channel::<i32>();
    let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
    if let Err(error) = check(submit(context)) {
        drop(unsafe { Box::from_raw(context.cast::<oneshot::Sender<i32>>()) });
        return Err(error);
    }
    Ok(receiver)
}

unsafe extern "C" fn overlay_released(context: *mut c_void) {
    let sender = unsafe { Box::from_raw(context.cast::<oneshot::Sender<()>>()) };
    let _ = sender.send(());
}

fn release_detached(raw: NonNull<sys::ct_prepared_split_overlay>) {
    let status =
        unsafe { sys::ct_prepared_split_overlay_release_async(raw.as_ptr(), None, std::ptr::null_mut()) };
    if status != 0 {
        unsafe { sys::ct_prepared_split_overlay_destroy(raw.as_ptr()) };
    }
}

fn split_limits(limits: SplitOverlayLimits) -> Result<sys::ct_split_overlay_limits_v1, CtError> {
    Ok(sys::ct_split_overlay_limits_v1 {
        version: 1,
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_split_overlay_limits_v1>())
            .map_err(|_| CtError::Internal)?,
        max_records: limits.max_records,
        max_bytes: limits.max_bytes,
    })
}

impl Crowdbtree {
    /// Publishes an already installed split prefix through the native flush.
    /// Returns `false` when the child still needs baseline or overlay preparation.
    ///
    /// # Errors
    /// Returns an incompatible range, changed split identity, or flush failure.
    pub async fn publish_installed_split_view_async(
        &self,
        generation: u64,
        journal_frontier: u64,
        destination: &Self,
        range: &KeyRange,
    ) -> Result<bool, CtError> {
        let (start, end) = match range {
            KeyRange::Unbounded => (None, None),
            KeyRange::Bounded { start, end } => (start.as_deref(), end.as_deref()),
        };
        let receiver = match submit_status(|context| unsafe {
            sys::ct_publish_installed_split_view_async(
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
                status_completed,
                context,
            )
        }) {
            Err(CtError::NotFound) => return Ok(false),
            result => result?,
        };
        check(receiver.await.map_err(|_| CtError::Unavailable)?)?;
        Ok(true)
    }

    /// Prepare all source, destination, and allocation work on a host thread.
    /// The destination's durable checkpoint stays unchanged.
    ///
    /// # Errors
    /// Returns an incompatible destination, exhausted budget, or closed source.
    pub fn arm_split_overlay(
        self: &Arc<Self>,
        destination: Arc<Self>,
        limits: SplitOverlayLimits,
    ) -> Result<PreparedSplitOverlay, CtError> {
        let limits = split_limits(limits)?;
        let mut raw = std::ptr::null_mut();
        let mut start_frontier = 0;
        check(unsafe {
            sys::ct_arm_split_overlay(
                self.as_ptr(),
                destination.as_ptr(),
                &raw const limits,
                &raw mut raw,
                &raw mut start_frontier,
            )
        })?;
        Ok(PreparedSplitOverlay {
            raw: Some(NonNull::new(raw).ok_or(CtError::Internal)?),
            start_frontier,
            async_release: false,
            _source: self.clone(),
            _destination: destination,
        })
    }

    /// Registers the observer and yields while pre-registration writes finish.
    /// Dropping the waiter leaves the accepted operation to finish and release
    /// its unclaimed capability on the host.
    ///
    /// # Errors
    /// Returns an incompatible destination, exhausted budget, or closed source.
    pub async fn arm_split_overlay_async(
        self: &Arc<Self>,
        destination: Arc<Self>,
        limits: SplitOverlayLimits,
    ) -> Result<PreparedSplitOverlay, CtError> {
        let executor = tokio::runtime::Handle::try_current().map_err(|_| CtError::InvalidArgument)?;
        let limits = split_limits(limits)?;
        let (sender, receiver) = oneshot::channel::<ArmReply>();
        {
            let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
            let status = unsafe {
                sys::ct_arm_split_overlay_async(
                    self.as_ptr(),
                    destination.as_ptr(),
                    &raw const limits,
                    overlay_armed,
                    context,
                )
            };
            if let Err(error) = check(status) {
                drop(unsafe { Box::from_raw(context.cast::<oneshot::Sender<ArmReply>>()) });
                return Err(error);
            }
        }
        let source = self.clone();
        executor
            .spawn(async move {
                let mut reply = receiver.await.map_err(|_| CtError::Unavailable)?;
                check(reply.status)?;
                let raw = NonNull::new(std::mem::take(&mut reply.raw) as *mut _).ok_or(CtError::Internal)?;
                Ok(PreparedSplitOverlay {
                    raw: Some(raw),
                    start_frontier: reply.start_frontier,
                    async_release: true,
                    _source: source,
                    _destination: destination,
                })
            })
            .await
            .map_err(|_| CtError::Unavailable)?
    }
}

impl PreparedSplitOverlay {
    #[must_use]
    pub const fn start_frontier(&self) -> u64 {
        self.start_frontier
    }

    /// Finish destination preparation after replay reaches the observer start.
    ///
    /// # Errors
    /// Returns an expired observer or changed destination.
    pub fn prepare(&self, replayed_prefix: u64) -> Result<(), CtError> {
        let raw = self.raw.ok_or(CtError::Internal)?;
        check(unsafe { sys::ct_prepared_split_overlay_prepare(raw.as_ptr(), replayed_prefix) })
    }

    /// Waits for the child flush and its generation lease without occupying a worker.
    ///
    /// # Errors
    /// Returns an expired observer or changed destination.
    pub async fn prepare_async(&self, replayed_prefix: u64) -> Result<(), CtError> {
        let receiver = {
            let raw = self.raw.ok_or(CtError::Internal)?;
            submit_status(|context| unsafe {
                sys::ct_prepared_split_overlay_prepare_async(
                    raw.as_ptr(),
                    replayed_prefix,
                    status_completed,
                    context,
                )
            })?
        };
        check(receiver.await.map_err(|_| CtError::Unavailable)?)
    }

    /// Releases the native capability and waits for its old controls to retire.
    ///
    /// # Errors
    /// Returns a failure to submit the release or receive its completion.
    pub async fn release(mut self) -> Result<(), CtError> {
        let (sender, receiver) = oneshot::channel::<()>();
        {
            let raw = self.raw.ok_or(CtError::Internal)?;
            let context = Box::into_raw(Box::new(sender)).cast::<c_void>();
            let status = unsafe {
                sys::ct_prepared_split_overlay_release_async(raw.as_ptr(), Some(overlay_released), context)
            };
            if let Err(error) = check(status) {
                drop(unsafe { Box::from_raw(context.cast::<oneshot::Sender<()>>()) });
                return Err(error);
            }
            self.raw = None;
        }
        receiver.await.map_err(|_| CtError::Unavailable)
    }

    /// Atomically commit one exact frontier without I/O, allocation, or waiting.
    ///
    /// # Errors
    /// Returns a busy, expired, or conflicting installation without publishing it.
    pub fn try_install(&self, frontier: u64) -> Result<InstalledSplit, CtError> {
        let raw = self.raw.ok_or(CtError::Internal)?;
        let mut result = sys::ct_installed_split_overlay_v1 {
            version: 1,
            struct_size: u32::try_from(std::mem::size_of::<sys::ct_installed_split_overlay_v1>())
                .map_err(|_| CtError::Internal)?,
            ..Default::default()
        };
        check(unsafe {
            sys::ct_prepared_split_overlay_try_install(raw.as_ptr(), frontier, &raw mut result)
        })?;
        Ok(InstalledSplit {
            generation: result.generation,
            frontier: result.frontier,
        })
    }

    #[must_use]
    pub fn committed_frontier(&self) -> Option<u64> {
        let raw = self.raw?;
        let mut frontier = 0;
        (unsafe { sys::ct_prepared_split_overlay_committed_frontier(raw.as_ptr(), &raw mut frontier) } != 0)
            .then_some(frontier)
    }
}

impl Drop for PreparedSplitOverlay {
    fn drop(&mut self) {
        if let Some(raw) = self.raw.take() {
            if self.async_release {
                release_detached(raw);
            } else {
                unsafe { sys::ct_prepared_split_overlay_destroy(raw.as_ptr()) };
            }
        }
    }
}
