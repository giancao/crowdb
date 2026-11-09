// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
//! Host-wide shutdown keeps every consensus listener alive until every
//! accepted runtime prefix has drained, including peers hosted in this process.
use crate::store_registry::PaxosKvStoreRegistry;
use crowdb_paxos_kv::common::OperationReport;
use crowdb_paxos_kv::kv::CrowdbTreeEngine;
use std::{collections::HashMap, sync::Arc, time::Duration};

/// Perform one bounded shutdown attempt. An incomplete accepted drain returns
/// an error before any store is torn down, so callers may repair and retry.
pub async fn shutdown_stores_once(registry: &PaxosKvStoreRegistry) -> OperationReport {
    let stores = registry.stores_snapshot();
    let timeout = Duration::from_millis(registry.config.server.shutdown_timeout_ms);
    for store in stores.values() {
        store.close_runtime_proposals();
    }
    let mut report = OperationReport::new();
    for store in stores.values() {
        report.merge(store.prepare_runtime_shutdown(timeout).await);
    }
    if !report.is_clean() {
        return report;
    }
    for store in stores.values() {
        report.merge(store.shutdown(timeout).await);
    }
    report
}

/// Complete store shutdown. A timed-out accepted prefix is reported while the
/// process retains its communication/repair owners and awaits real progress.
/// Backend checkpoint failures remain explicit terminal errors in the result.
pub async fn shutdown_stores(registry: &PaxosKvStoreRegistry) -> OperationReport {
    loop {
        let report = shutdown_stores_once(registry).await;
        let stores = registry.stores_snapshot();
        if !stores.values().any(|store| store.runtime_shutdown_pending()) {
            return report;
        }
        for error in &report.errors {
            tracing::error!(error = %error, "shutdown incomplete; consensus remains live; next step: restore peer progress to drain accepted work");
        }
        for store in stores.values() {
            store.await_runtime_shutdown_ready().await;
        }
    }
}

/// Waits for the stopped stores' Tree closes and backend releases. The caller
/// has already stopped serving and joined its workers.
pub async fn release_hosted_trees(registry: Arc<PaxosKvStoreRegistry>) -> OperationReport {
    let timeout = Duration::from_millis(registry.config.server.shutdown_timeout_ms);
    let runtime = registry.tree_runtime.clone();
    let trees: Vec<_> = {
        let mut trees = HashMap::new();
        for (&store_id, store) in registry.stores_snapshot().iter() {
            store.for_each_group(|group| {
                let engine = group.local_replica().learner.engine();
                if let Some(engine) = engine.as_any().downcast_ref::<CrowdbTreeEngine>() {
                    let tree = engine.handle();
                    trees
                        .entry(Arc::as_ptr(&tree))
                        .or_insert((store_id, group.group_id, tree));
                }
            });
        }
        trees.into_values().collect()
    };
    runtime.stop_admission();
    drop(registry);

    let mut report = OperationReport::new();
    let mut closing = tokio::task::JoinSet::new();
    for (store_id, group_id, tree) in trees {
        let release = tree.backend_release_service();
        let close = if let Some(close) = tree.close_result() {
            drop(tree);
            close
        } else {
            let Ok(tree) = Arc::try_unwrap(tree) else {
                report.push_error(format!(
                    "critical: store {store_id} group {group_id} Tree still has a serving owner"
                ));
                continue;
            };
            match tree.begin_close() {
                Ok(close) => close,
                Err((error, _tree)) => {
                    report.push_error(format!(
                        "critical: store {store_id} group {group_id} Tree close rejected: {error}"
                    ));
                    continue;
                }
            }
        };
        closing.spawn(async move {
            let result = if let Ok(result) = tokio::time::timeout(timeout, close.wait()).await {
                result
            } else {
                tracing::warn!(
                    store_id,
                    group_id,
                    timeout_ms = timeout.as_millis(),
                    "waiting for native Tree close"
                );
                close.wait().await
            };
            if let Some(release) = release {
                if tokio::time::timeout(timeout, release.shutdown()).await.is_err() {
                    tracing::warn!(
                        store_id,
                        group_id,
                        timeout_ms = timeout.as_millis(),
                        "waiting for Tree backend release"
                    );
                    release.shutdown().await;
                }
            }
            result.map_err(|error| {
                format!("critical: store {store_id} group {group_id} Tree close failed: {error}")
            })
        });
    }
    while let Some(result) = closing.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(error)) => report.push_error(error),
            Err(error) => report.push_error(format!("critical: Tree close waiter failed: {error}")),
        }
    }
    if report.is_clean() {
        let result = if let Ok(result) = tokio::time::timeout(timeout, runtime.shutdown()).await {
            result
        } else {
            let progress = runtime.snapshot().ok();
            tracing::warn!(
                timeout_ms = timeout.as_millis(),
                ?progress,
                "waiting for Tree runtime exit"
            );
            runtime.shutdown().await
        };
        if let Err(error) = result {
            report.push_error(format!("critical: Tree runtime shutdown failed: {error}"));
        }
    }
    report
}
