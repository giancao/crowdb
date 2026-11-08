// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{err_json, ErrorResponse, RegistryArc};
use axum::{http::StatusCode, Json};
use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::kv::CrowdbTreeEngine;
use crowdb_tree_ffi::{PageStoreReleaseService, TreeClose};
use std::future::Future;
use std::time::Duration;

use super::system_bootstrap::ManagementError;

pub(super) fn tree_close(group: &PxGroup) -> Option<(TreeClose, Option<PageStoreReleaseService>)> {
    let engine = group.local_replica().learner.engine();
    let tree = engine.as_any().downcast_ref::<CrowdbTreeEngine>()?.handle();
    Some((tree.close_result()?, tree.page_store_release_service()))
}

pub(super) async fn wait_tree_close(
    close: Option<(TreeClose, Option<PageStoreReleaseService>)>,
) -> Result<(), ManagementError> {
    if let Some((close, release)) = close {
        close.wait().await.map_err(|error| {
            err_json(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("old Tree close failed: {error}"),
            )
        })?;
        if let Some(release) = release {
            release.shutdown().await;
        }
    }
    Ok(())
}

pub(super) async fn finish_removal<T: Send + 'static>(
    timeout: Duration,
    target: String,
    operation: impl Future<Output = Result<T, ManagementError>> + Send + 'static,
) -> Result<T, ManagementError> {
    let mut task = tokio::spawn(async move {
        let result = operation.await;
        if let Err((status, body)) = &result {
            tracing::error!(%status, error = %body.error, "management removal failed");
        }
        result
    });
    match tokio::time::timeout(timeout, &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => Err(err_json(StatusCode::INTERNAL_SERVER_ERROR, error.to_string())),
        Err(_) => {
            tracing::warn!(%target, timeout_ms = timeout.as_millis(), "waiting for old Tree and directory operation");
            Err(err_json(
                StatusCode::SERVICE_UNAVAILABLE,
                format!("{target} is still waiting for old Tree release or directory completion"),
            ))
        }
    }
}

pub(super) async fn before_group_removal(
    state: &RegistryArc,
    group: &PxGroup,
) -> Result<(), (StatusCode, Json<ErrorResponse>)> {
    if !group.local_replica().learner.engine().requires_contiguous_apply() {
        return Ok(());
    }
    let report = group
        .shutdown(Duration::from_millis(state.config.server.shutdown_timeout_ms))
        .await;
    if report.is_clean() {
        return Ok(());
    }
    Err(err_json(
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "runtime group retained; shutdown incomplete: {}",
            report.errors.join("; ")
        ),
    ))
}
