// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bounded durable reference preparation before a root becomes visible.

use super::stage::StageId;
use crate::{crc32c_update, CtError};

// Final release also writes the candidate, window, manifest and authority.
pub const MAX_REFERENCE_WINDOW: usize = 60;
pub const MAX_CANDIDATE_REFERENCES: u64 = 16 * 1024 * 1024;
pub const MAX_CANDIDATE_MANIFEST_BYTES: usize = 16 * 1024 * 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestCandidate {
    pub id: StageId,
    pub expected_generation: u64,
    pub reference_count: u64,
    pub reference_checksum: u32,
    /// Immutable root descriptor; reference windows are persisted separately.
    pub manifest: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u64)]
pub enum CandidateState {
    Preparing = 1,
    Ready = 2,
    Committed = 3,
    Aborted = 4,
    Reclaimed = 5,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CandidateStatus {
    pub id: StageId,
    pub state: CandidateState,
    pub generation: u64,
    pub reference_count: u64,
    pub prepared_references: u64,
    pub prepared_windows: u64,
    pub released_windows: u64,
}

pub fn validate_operation(id: StageId) -> Result<(), CtError> {
    if id.owner_epoch == 0 || (id.operation_high == 0 && id.operation_low == 0) {
        return Err(CtError::InvalidArgument);
    }
    Ok(())
}

/// Update in the same sorted order used by prepare_manifest_references.
#[must_use]
pub fn reference_checksum(mut checksum: u32, references: &[StageId]) -> u32 {
    for id in references {
        for value in [id.owner_epoch, id.operation_high, id.operation_low] {
            checksum = crc32c_update(checksum, &value.to_le_bytes());
        }
    }
    checksum
}

impl ManifestCandidate {
    pub fn validate(&self) -> Result<(), CtError> {
        validate_operation(self.id)?;
        if self.expected_generation == u64::MAX
            || self.reference_count > MAX_CANDIDATE_REFERENCES
            || self.manifest.is_empty()
            || self.manifest.len() > MAX_CANDIDATE_MANIFEST_BYTES
            || (self.reference_count == 0 && self.reference_checksum != 0)
        {
            return Err(CtError::InvalidArgument);
        }
        Ok(())
    }
}

/// Implementations serialize these operations with root publication and GC.
/// No method may infer an unknown outcome from a timeout or create a new ID.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PinPruneProgress {
    pub examined: u32,
    pub released: u32,
    pub after: Option<StageId>,
    pub more: bool,
}

pub trait StageProtectionCatalog: Send + Sync {
    /// Local fixed-process lease check before submission and after each read window.
    /// # Errors
    /// Returns unavailable for an expired owner or unsupported for legacy catalogs.
    fn check_read_owner(&self) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Called after closing native write admission and draining every accepted backend operation.
    /// A lost reply retries this same writer identity; it never releases live reader pins.
    /// # Errors
    /// Returns an authority, metadata availability, or unsupported error.
    fn writer_quiesced(&self, _tree_id: u64, _owner_epoch: u64) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Resolves an earlier owner's uncommitted candidate after actual root and WAL recovery.
    /// # Errors
    /// Returns an authority, incomplete recovery, or unsupported error.
    fn recover_manifest_candidate(&self, _tree_id: u64, _id: StageId) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Remove only pins whose process exclusion has been authoritatively confirmed.
    /// # Errors
    /// Rejects incomplete recovery, unavailable metadata, or unsupported catalogs.
    fn prune_stage_pins(
        &self,
        _tree_id: u64,
        _stage: StageId,
        _after: Option<StageId>,
        _limit: u32,
    ) -> Result<PinPruneProgress, CtError> {
        Err(CtError::NotSupported)
    }

    fn load_manifest_candidate(
        &self,
        _tree_id: u64,
        _id: StageId,
    ) -> Result<Option<CandidateStatus>, CtError> {
        Err(CtError::NotSupported)
    }
    fn list_manifest_candidates(
        &self,
        _tree_id: u64,
        _after: Option<StageId>,
        _limit: u32,
    ) -> Result<Vec<CandidateStatus>, CtError> {
        Err(CtError::NotSupported)
    }
    fn begin_manifest_candidate(&self, _tree_id: u64, _candidate: &ManifestCandidate) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    fn prepare_manifest_references(
        &self,
        _tree_id: u64,
        _id: StageId,
        _window: u64,
        _references: &[StageId],
    ) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    fn commit_manifest_candidate(&self, _tree_id: u64, _id: StageId) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    fn abort_manifest_candidate(&self, _tree_id: u64, _id: StageId) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// Release one previously prepared window after abort or root reclamation.
    fn release_manifest_references(&self, _tree_id: u64, _id: StageId, _window: u64) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    fn pin_stage(&self, _tree_id: u64, _stage: StageId, _pin: StageId) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }
    /// A fenced owner may release its own pin; it may never acquire a new one.
    fn unpin_stage(&self, _tree_id: u64, _stage: StageId, _pin: StageId) -> Result<(), CtError> {
        Err(CtError::NotSupported)
    }

    fn recover_manifest_candidate_async<'a>(
        &'a self,
        tree: u64,
        id: StageId,
    ) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.recover_manifest_candidate(tree, id) })
    }
    fn writer_quiesced_async<'a>(&'a self, tree: u64, epoch: u64) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.writer_quiesced(tree, epoch) })
    }
    fn prune_stage_pins_async<'a>(
        &'a self,
        tree: u64,
        stage: StageId,
        after: Option<StageId>,
        limit: u32,
    ) -> super::CatalogFuture<'a, PinPruneProgress> {
        Box::pin(async move { self.prune_stage_pins(tree, stage, after, limit) })
    }
    fn load_manifest_candidate_async<'a>(
        &'a self,
        tree: u64,
        id: StageId,
    ) -> super::CatalogFuture<'a, Option<CandidateStatus>> {
        Box::pin(async move { self.load_manifest_candidate(tree, id) })
    }
    fn list_manifest_candidates_async<'a>(
        &'a self,
        tree: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> super::CatalogFuture<'a, Vec<CandidateStatus>> {
        Box::pin(async move { self.list_manifest_candidates(tree, after, limit) })
    }
    fn begin_manifest_candidate_async<'a>(
        &'a self,
        tree: u64,
        candidate: &'a ManifestCandidate,
    ) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.begin_manifest_candidate(tree, candidate) })
    }
    fn prepare_manifest_references_async<'a>(
        &'a self,
        tree: u64,
        id: StageId,
        window: u64,
        refs: &'a [StageId],
    ) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.prepare_manifest_references(tree, id, window, refs) })
    }
    fn commit_manifest_candidate_async<'a>(&'a self, tree: u64, id: StageId) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.commit_manifest_candidate(tree, id) })
    }
    fn abort_manifest_candidate_async<'a>(&'a self, tree: u64, id: StageId) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.abort_manifest_candidate(tree, id) })
    }
    fn release_manifest_references_async<'a>(
        &'a self,
        tree: u64,
        id: StageId,
        window: u64,
    ) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.release_manifest_references(tree, id, window) })
    }
    fn pin_stage_async<'a>(
        &'a self,
        tree: u64,
        stage: StageId,
        pin: StageId,
    ) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.pin_stage(tree, stage, pin) })
    }
    fn unpin_stage_async<'a>(
        &'a self,
        tree: u64,
        stage: StageId,
        pin: StageId,
    ) -> super::CatalogFuture<'a, ()> {
        Box::pin(async move { self.unpin_stage(tree, stage, pin) })
    }
}
