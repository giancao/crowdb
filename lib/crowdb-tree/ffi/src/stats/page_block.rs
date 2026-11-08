// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crate::error::check;
use crate::{sys, Crowdbtree, CtError};

/// Constant-cost page store-ledger gauges, independent of a writer incarnation.
/// Equal nonzero scopes are projections of the same ledger; deduplicate by
/// `scope_id` rather than summing those views. Allocated capacity includes old
/// and staged output until physical deletion is acknowledged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PageBlockStatsSnapshot {
    pub scope_id: u64,
    pub allocated_bytes: u64,
}

impl Crowdbtree {
    /// Samples a stable page block ledger without scanning pages or performing I/O.
    ///
    /// # Errors
    /// Returns `Unavailable` when this page store has no independently owned ledger.
    pub fn page_block_stats(&self) -> Result<PageBlockStatsSnapshot, CtError> {
        sample(|out| unsafe { sys::ct_get_page_block_stats(self.as_ptr(), out) })
    }
}

pub(super) fn sample(
    get: impl FnOnce(*mut sys::ct_page_block_stats_v1) -> i32,
) -> Result<PageBlockStatsSnapshot, CtError> {
    let mut raw = sys::ct_page_block_stats_v1 {
        struct_size: u32::try_from(std::mem::size_of::<sys::ct_page_block_stats_v1>())
            .map_err(|_| CtError::Internal)?,
        version: 1,
        ..Default::default()
    };
    check(get(&mut raw))?;
    Ok(PageBlockStatsSnapshot {
        scope_id: raw.scope_id,
        allocated_bytes: raw.allocated_bytes,
    })
}
