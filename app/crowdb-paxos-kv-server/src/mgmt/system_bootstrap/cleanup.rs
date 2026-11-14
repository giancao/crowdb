// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    begin, err_json, has_owner, storage_error, sync_directory, validate, verify, AcceptedBootstrap,
    BootstrapExecution, ManagementError, RegistryArc, OWNERSHIP_FILE, RETIRED_FILE,
};
use axum::{extract::State, http::StatusCode, Json};
use crowdb_protocol::mgmt::SystemBootstrapIdentity;
use serde::Deserialize;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
};

#[derive(Deserialize)]
pub(crate) struct CleanupRequest {
    bootstrap: SystemBootstrapIdentity,
    confirm_delete_system_store: bool,
}

pub(crate) async fn cleanup(
    State(state): State<RegistryArc>,
    Json(request): Json<CleanupRequest>,
) -> Result<StatusCode, ManagementError> {
    let execution = begin(&state)?;
    tokio::spawn(cleanup_owned(state, request, execution))
        .await
        .map_err(|error| err_json(StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?
}

async fn cleanup_owned(
    state: RegistryArc,
    request: CleanupRequest,
    execution: BootstrapExecution,
) -> Result<StatusCode, ManagementError> {
    if !request.confirm_delete_system_store {
        return Err(err_json(
            StatusCode::BAD_REQUEST,
            "explicit system-store deletion confirmation required",
        ));
    }
    validate(1, &request.bootstrap)?;
    let root = &state.config.config_root;
    let retired = root.join(format!(
        "system-bootstrap-retired-{}.json",
        request.bootstrap.operation_id
    ));
    if !has_owner(&state).await? {
        if fs::try_exists(&retired)
            .await
            .map_err(|error| storage_error(&error))?
        {
            finish_retired(root, &retired, &request.bootstrap).await?;
            return Ok(StatusCode::OK);
        }
        if state.contains_store(0) {
            return Err(err_json(
                StatusCode::CONFLICT,
                "existing system store has no matching ownership",
            ));
        }
        fs::create_dir_all(root)
            .await
            .map_err(|error| storage_error(&error))?;
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&retired)
            .await
            .map_err(|error| storage_error(&error))?;
        file.write_all(
            &serde_json::to_vec(&request.bootstrap)
                .map_err(|error| storage_error(&std::io::Error::other(error)))?,
        )
        .await
        .map_err(|error| storage_error(&error))?;
        file.flush().await.map_err(|error| storage_error(&error))?;
        file.sync_all().await.map_err(|error| storage_error(&error))?;
        sync_directory(root).await?;
        return Ok(StatusCode::OK);
    }
    let bytes = fs::read(root.join(OWNERSHIP_FILE))
        .await
        .map_err(|error| storage_error(&error))?;
    let accepted: AcceptedBootstrap =
        serde_json::from_slice(&bytes).map_err(|error| storage_error(&std::io::Error::other(error)))?;
    verify(&root.join(OWNERSHIP_FILE), &accepted).await?;
    if accepted.identity != request.bootstrap {
        return Err(err_json(
            StatusCode::CONFLICT,
            "cleanup belongs to another bootstrap operation",
        ));
    }
    if !fs::try_exists(&retired)
        .await
        .map_err(|error| storage_error(&error))?
    {
        fs::hard_link(root.join(OWNERSHIP_FILE), &retired)
            .await
            .map_err(|error| storage_error(&error))?;
    }
    if !super::system_cleanup_pending(root)
        .await
        .map_err(|error| storage_error(&error))?
    {
        fs::hard_link(&retired, root.join(RETIRED_FILE))
            .await
            .map_err(|error| storage_error(&error))?;
    }
    sync_directory(root).await?;
    let removal = if state.contains_store(0) {
        Some(super::super::store_ops::prepare_store_removal(&state, 0).await?)
    } else {
        None
    };
    let timeout = std::time::Duration::from_millis(state.config.server.shutdown_timeout_ms);
    super::super::runtime_shutdown::finish_removal(timeout, "cleanup system store".to_owned(), async move {
        let _execution = execution;
        if let Some(removal) = removal {
            removal.await?;
        } else {
            super::super::store_ops::remove_store_files(&state, 0).await?;
        }
        finish_cleanup(&state.config.config_root).await?;
        Ok(StatusCode::OK)
    })
    .await
}

async fn finish_cleanup(root: &std::path::Path) -> Result<(), ManagementError> {
    fs::remove_file(root.join(OWNERSHIP_FILE))
        .await
        .map_err(|error| storage_error(&error))?;
    sync_directory(root).await?;
    fs::remove_file(root.join(RETIRED_FILE))
        .await
        .map_err(|error| storage_error(&error))?;
    sync_directory(root).await
}

async fn finish_retired(
    root: &std::path::Path,
    retired: &std::path::Path,
    bootstrap: &SystemBootstrapIdentity,
) -> Result<(), ManagementError> {
    if super::system_cleanup_pending(root)
        .await
        .map_err(|error| storage_error(&error))?
    {
        let accepted: AcceptedBootstrap =
            serde_json::from_slice(&fs::read(retired).await.map_err(|error| storage_error(&error))?)
                .map_err(|error| storage_error(&std::io::Error::other(error)))?;
        if accepted.identity != *bootstrap {
            return Err(err_json(
                StatusCode::CONFLICT,
                "cleanup belongs to another bootstrap operation",
            ));
        }
        verify(&root.join(RETIRED_FILE), &accepted).await?;
        fs::remove_file(root.join(RETIRED_FILE))
            .await
            .map_err(|error| storage_error(&error))?;
        sync_directory(root).await?;
    }
    Ok(())
}
