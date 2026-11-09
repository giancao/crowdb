// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Durable system-store ownership, published before any store creation.

use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::{extract::State, http::StatusCode, Json};
use crowdb_protocol::mgmt::{SystemBootstrapIdentity, SystemPrepareRequest};
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{self, File, OpenOptions},
    io::{AsyncReadExt, AsyncWriteExt},
};

use super::{err_json, ErrorResponse, RegistryArc};

pub(super) type ManagementError = (StatusCode, Json<ErrorResponse>);
const OWNERSHIP_FILE: &str = "system-bootstrap.json";
const RETIRED_FILE: &str = "system-bootstrap-cleanup.json";
static NEXT_TEMP: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct AcceptedBootstrap {
    version: u32,
    replica_id: u64,
    identity: SystemBootstrapIdentity,
}

pub(super) struct BootstrapExecution(Arc<crate::store_registry::PaxosKvStoreRegistry>);

impl BootstrapExecution {
    /// The admitted operation owns the node directory until it completes,
    /// even when the HTTP request drops its wait for the result.
    pub(super) async fn run<T: Send + 'static>(
        self,
        operation: impl std::future::Future<Output = Result<T, ManagementError>> + Send + 'static,
    ) -> Result<T, ManagementError> {
        tokio::spawn(async move {
            let _execution = self;
            operation.await
        })
        .await
        .map_err(|error| err_json(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
    }
}

impl Drop for BootstrapExecution {
    fn drop(&mut self) {
        self.0.bootstrap_executing.store(false, Ordering::Release);
        self.0.bootstrap_idle.notify_waiters();
    }
}

pub(super) fn begin(state: &RegistryArc) -> Result<BootstrapExecution, ManagementError> {
    state
        .bootstrap_executing
        .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
        .map_err(|_| {
            err_json(
                StatusCode::CONFLICT,
                "a node directory operation is executing, retry the same operation",
            )
        })?;
    Ok(BootstrapExecution(Arc::clone(&state.registry)))
}

pub(super) async fn prepare(
    State(state): State<RegistryArc>,
    Json(req): Json<SystemPrepareRequest>,
) -> Result<Json<SystemPrepareRequest>, ManagementError> {
    begin(&state)?
        .run(async move {
            accept(&state, req.replica_id, Some(&req.bootstrap)).await?;
            Ok(Json(req))
        })
        .await
}

pub(super) async fn has_owner(state: &RegistryArc) -> Result<bool, ManagementError> {
    match fs::symlink_metadata(state.config.config_root.join(OWNERSHIP_FILE)).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(storage_error(&error)),
    }
}

pub(super) async fn accept(
    state: &RegistryArc,
    replica_id: u64,
    identity: Option<&SystemBootstrapIdentity>,
) -> Result<(), ManagementError> {
    let root = &state.config.config_root;
    let path = root.join(OWNERSHIP_FILE);
    if system_cleanup_pending(root)
        .await
        .map_err(|error| storage_error(&error))?
    {
        return Err(err_json(StatusCode::CONFLICT, "system cleanup has not completed"));
    }
    let Some(identity) = identity else {
        return if has_owner(state).await? {
            Err(err_json(
                StatusCode::CONFLICT,
                "system store belongs to an identified bootstrap operation",
            ))
        } else {
            Ok(())
        };
    };
    validate(replica_id, identity)?;
    if fs::try_exists(root.join(format!("system-bootstrap-retired-{}.json", identity.operation_id)))
        .await
        .map_err(|error| storage_error(&error))?
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "bootstrap operation was explicitly retired",
        ));
    }
    if system_cleanup_pending(root)
        .await
        .map_err(|error| storage_error(&error))?
    {
        return Err(err_json(StatusCode::CONFLICT, "system cleanup has not completed"));
    }
    let accepted = AcceptedBootstrap {
        version: 1,
        replica_id,
        identity: identity.clone(),
    };
    if has_owner(state).await? {
        return verify(&path, &accepted).await;
    }
    if state.contains_store(0) {
        return Err(err_json(
            StatusCode::CONFLICT,
            "existing system store has no matching bootstrap ownership",
        ));
    }
    fs::create_dir_all(root)
        .await
        .map_err(|error| storage_error(&error))?;
    let temporary = root.join(format!(
        ".system-bootstrap-{}-{}.tmp",
        std::process::id(),
        NEXT_TEMP.fetch_add(1, Ordering::Relaxed)
    ));
    let result = async {
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&temporary)
            .await
            .map_err(|error| storage_error(&error))?;
        let bytes =
            serde_json::to_vec(&accepted).map_err(|error| storage_error(&std::io::Error::other(error)))?;
        file.write_all(&bytes)
            .await
            .map_err(|error| storage_error(&error))?;
        file.flush().await.map_err(|error| storage_error(&error))?;
        file.sync_all().await.map_err(|error| storage_error(&error))?;
        match fs::hard_link(&temporary, &path).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(storage_error(&error)),
        }
        sync_directory(root).await?;
        verify(&path, &accepted).await
    }
    .await;
    let cleanup = fs::remove_file(&temporary).await;
    if result.is_ok() {
        cleanup.map_err(|error| storage_error(&error))?;
    }
    result
}

fn validate(replica_id: u64, identity: &SystemBootstrapIdentity) -> Result<(), ManagementError> {
    fn uuid(text: &str) -> bool {
        text.len() == 36
            && text.bytes().enumerate().all(|(index, byte)| {
                if [8, 13, 18, 23].contains(&index) {
                    byte == b'-'
                } else {
                    byte.is_ascii_hexdigit()
                }
            })
            && text
                .bytes()
                .any(|byte| matches!(byte, b'1'..=b'9' | b'a'..=b'f' | b'A'..=b'F'))
    }
    if replica_id == 0
        || !uuid(&identity.cluster_id)
        || !uuid(&identity.operation_id)
        || identity.configuration_digest.len() != 64
        || !identity
            .configuration_digest
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "bootstrap requires nonzero replica, cluster/operation UUIDs and SHA-256 configuration digest",
        ));
    }
    Ok(())
}

async fn verify(path: &Path, expected: &AcceptedBootstrap) -> Result<(), ManagementError> {
    let metadata = fs::symlink_metadata(path)
        .await
        .map_err(|error| storage_error(&error))?;
    if !metadata.file_type().is_file()
        || metadata.len() > 4096
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err(err_json(
            StatusCode::CONFLICT,
            "invalid durable system bootstrap ownership",
        ));
    }
    let mut bytes = Vec::with_capacity(4096);
    File::open(path)
        .await
        .map_err(|error| storage_error(&error))?
        .take(4097)
        .read_to_end(&mut bytes)
        .await
        .map_err(|error| storage_error(&error))?;
    let actual: AcceptedBootstrap = serde_json::from_slice(&bytes)
        .map_err(|_| err_json(StatusCode::CONFLICT, "invalid durable system bootstrap ownership"))?;
    if &actual != expected {
        return Err(err_json(
            StatusCode::CONFLICT,
            "system store is reserved by a conflicting bootstrap operation",
        ));
    }
    Ok(())
}

async fn sync_directory(root: &Path) -> Result<(), ManagementError> {
    let directory = File::open(root).await.map_err(|error| storage_error(&error))?;
    directory.sync_all().await.map_err(|error| storage_error(&error))
}

fn storage_error(error: &std::io::Error) -> ManagementError {
    err_json(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!("system bootstrap persistence failed: {error}"),
    )
}

mod cleanup;
pub(super) use cleanup::cleanup;

/// # Errors
/// Reports unreadable cleanup markers instead of restarting a retired system store.
pub async fn system_cleanup_pending(root: &Path) -> std::io::Result<bool> {
    match fs::symlink_metadata(root.join(RETIRED_FILE)).await {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}
