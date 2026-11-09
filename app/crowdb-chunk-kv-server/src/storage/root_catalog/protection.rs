// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reference intents, live pins and GC use one root authority revision.

use bytes::Bytes;
use crowdb_paxos_kv_client::BatchOp;
use crowdb_tree_ffi::chunk::protection::{
    validate_operation, CandidateStatus, ManifestCandidate, StageProtectionCatalog,
};
use crowdb_tree_ffi::chunk::stage::StageId;
use crowdb_tree_ffi::CtError;

use super::{catalog_key, decode_authority, KvRootCatalogStore};
mod candidate;
mod commit;
mod guards;
mod operation;
mod pin_owner;
mod process;
mod prune;
mod record;
mod release;
mod scan;
mod writer;
pub(super) use process::RecoveryReceipt;
use record::{operation_key, CandidateRecord};

// Native metadata proposals reserve progress for at most 64 distinct keys.
pub(super) const MAX_METADATA_MUTATIONS: usize = 64;

pub(super) struct Authority {
    key: Vec<u8>,
    value: Bytes,
    revision: u64,
    pub(super) epoch: u64,
    generation: u64,
}

impl KvRootCatalogStore {
    pub(super) async fn protection_authority(&self, require_owner: bool) -> Result<Authority, CtError> {
        let key = catalog_key(self.tree_id, b"authority", 0);
        let (value, revision) = self
            .get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .ok_or(CtError::Unavailable)?;
        let (epoch, generation) = decode_authority(&value).ok_or(CtError::Corruption)?;
        if require_owner {
            self.check_registered_writer().await?;
        }
        if require_owner && epoch != self.owner_epoch {
            return Err(CtError::Unavailable);
        }
        Ok(Authority {
            key,
            value,
            revision,
            epoch,
            generation,
        })
    }

    pub(super) async fn protection_write(
        &self,
        authority: Authority,
        mut ops: Vec<BatchOp>,
    ) -> super::transaction::MutationResult<()> {
        ops.push(BatchOp::Put {
            key: Bytes::from(authority.key.clone()),
            value: authority.value,
        });
        self.kv
            .batch_write_cas(
                self.store_id,
                self.group_id,
                &ops,
                &authority.key,
                authority.revision,
            )
            .await
            .map(|_| ())
            .map_err(|error| super::transaction::MutationError::from_cas(&error))
    }

    fn protection_identity(&self, tree_id: u64, id: StageId, owner: bool) -> Result<(), CtError> {
        validate_operation(id)?;
        if tree_id != self.tree_id {
            return Err(CtError::InvalidArgument);
        }
        if owner && id.owner_epoch != self.owner_epoch {
            return Err(CtError::Unavailable);
        }
        Ok(())
    }

    async fn candidate_record(&self, id: StageId) -> Result<Option<CandidateRecord>, CtError> {
        let key = operation_key(self.tree_id, b"candidate", id);
        self.get(&key)
            .await
            .map_err(|_| CtError::Unavailable)?
            .map(|(bytes, _)| CandidateRecord::decode(id, &bytes))
            .transpose()
    }
}

impl StageProtectionCatalog for KvRootCatalogStore {
    fn check_read_owner(&self) -> Result<(), CtError> {
        self.check_process_owner()
    }

    fn recover_manifest_candidate_async(
        &self,
        tree: u64,
        id: StageId,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            self.protection_identity(tree, id, false)?;
            self.recover_candidate(id).await
        })
    }
    fn writer_quiesced_async(&self, tree: u64, epoch: u64) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            if tree != self.tree_id || epoch != self.owner_epoch {
                return Err(CtError::InvalidArgument);
            }
            self.publish_writer_quiescence().await
        })
    }
    fn prune_stage_pins_async(
        &self,
        tree: u64,
        stage: StageId,
        after: Option<StageId>,
        limit: u32,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, crowdb_tree_ffi::chunk::protection::PinPruneProgress> {
        Box::pin(async move {
            self.protection_identity(tree, stage, false)?;
            self.prune_pins(stage, after, limit).await
        })
    }
    fn load_manifest_candidate_async(
        &self,
        tree: u64,
        id: StageId,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, Option<CandidateStatus>> {
        Box::pin(async move {
            self.protection_identity(tree, id, false)?;
            self.candidate_record(id)
                .await
                .map(|record| record.map(|record| record.status))
        })
    }
    fn list_manifest_candidates_async(
        &self,
        tree: u64,
        after: Option<StageId>,
        limit: u32,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, Vec<CandidateStatus>> {
        Box::pin(async move {
            if tree != self.tree_id {
                return Err(CtError::InvalidArgument);
            }
            self.scan_candidates(after, limit).await
        })
    }
    fn begin_manifest_candidate_async<'a>(
        &'a self,
        tree: u64,
        candidate: &'a ManifestCandidate,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'a, ()> {
        Box::pin(async move {
            candidate.validate()?;
            self.protection_identity(tree, candidate.id, true)?;
            self.authorize_publication().await?;
            self.begin_candidate(candidate).await
        })
    }
    fn prepare_manifest_references_async<'a>(
        &'a self,
        tree: u64,
        id: StageId,
        window: u64,
        refs: &'a [StageId],
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'a, ()> {
        Box::pin(async move {
            self.protection_identity(tree, id, true)?;
            self.prepare_candidate(id, window, refs).await
        })
    }
    fn commit_manifest_candidate_async(
        &self,
        tree: u64,
        id: StageId,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            self.protection_identity(tree, id, true)?;
            self.commit_candidate(id).await
        })
    }
    fn abort_manifest_candidate_async(
        &self,
        tree: u64,
        id: StageId,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            self.protection_identity(tree, id, true)?;
            self.abort_candidate(id).await
        })
    }
    fn release_manifest_references_async(
        &self,
        tree: u64,
        id: StageId,
        window: u64,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            self.protection_identity(tree, id, false)?;
            self.release_candidate(id, window).await
        })
    }
    fn pin_stage_async(
        &self,
        tree: u64,
        stage: StageId,
        pin: StageId,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            self.protection_identity(tree, stage, false)?;
            self.protection_identity(tree, pin, true)?;
            self.change_stage_pin(stage, pin, true).await
        })
    }
    fn unpin_stage_async(
        &self,
        tree: u64,
        stage: StageId,
        pin: StageId,
    ) -> crowdb_tree_ffi::chunk::CatalogFuture<'_, ()> {
        Box::pin(async move {
            self.protection_identity(tree, stage, false)?;
            self.protection_identity(tree, pin, true)?;
            self.change_stage_pin(stage, pin, false).await
        })
    }
}
