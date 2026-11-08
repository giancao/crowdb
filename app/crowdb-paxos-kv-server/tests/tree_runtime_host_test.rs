// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::PxLocalReplicaRole;
use crowdb_paxos_kv::common::config::PaxosKvConfig;
use crowdb_paxos_kv_server::recovery::startup::{create_group_with_wal, create_group_with_wal_on_runtime};
use crowdb_paxos_kv_server::store_registry::PaxosKvStoreRegistry;

fn config(root: &std::path::Path) -> PaxosKvConfig {
    let mut config = PaxosKvConfig {
        wal_backend: "mem-block".to_owned(),
        crowtree_store_kind: "mem-block".to_owned(),
        wal_root: root.join("wal"),
        config_root: root.join("config"),
        data_root: root.join("tree"),
        log_dir: root.join("log").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.tree_runtime.host.workers = Some(1);
    config
}

#[test]
fn incompatible_proposal_window_is_rejected_before_startup() {
    let mut config = PaxosKvConfig::default();
    config.tree_runtime.tree.cpu_queue_limit = Some(16);
    let Err(error) = PaxosKvStoreRegistry::try_with_config(config) else {
        panic!("proposal window must fit its paid Tree lane");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("max_inflight_proposals"));
}

#[tokio::test(flavor = "current_thread")]
async fn standalone_constructor_uses_runtime_without_a_mode_flag() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-runtime-host");
    let registry = PaxosKvStoreRegistry::try_with_config(config(root.path())).unwrap();
    let result = create_group_with_wal(
        1,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &registry.config,
        Arc::clone(&registry.wal_backend),
        registry.crowtree_store_kind,
    )
    .await;
    let group = result.unwrap();
    let engine = group.local_replica().learner.engine();
    assert!(engine.requires_contiguous_apply());
    let permit = engine.admit_proposal(Bytes::new().into()).await.unwrap().unwrap();
    permit.apply(1).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn every_group_uses_the_same_registry_owned_native_host() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-runtime-host");
    let registry = PaxosKvStoreRegistry::try_with_config(config(root.path())).unwrap();
    let mut groups = Vec::new();
    for group_id in 1..=2 {
        groups.push(
            create_group_with_wal_on_runtime(
                1,
                group_id,
                1,
                PxLocalReplicaRole::Leader,
                &registry.config,
                Arc::clone(&registry.wal_backend),
                registry.crowtree_store_kind,
                registry.tree_runtime.clone(),
            )
            .await
            .unwrap(),
        );
    }
    for group in &groups {
        let engine = group.local_replica().learner.engine();
        assert!(engine.requires_contiguous_apply());
        let permit = engine.admit_proposal(Bytes::new().into()).await.unwrap().unwrap();
        permit.apply(1).await.unwrap();
    }
    registry.tree_runtime.stop_admission();
    for group in &groups {
        // This one host transition must be visible in both independent Trees.
        assert!(group
            .local_replica()
            .learner
            .engine()
            .admit_proposal(Bytes::new().into())
            .await
            .is_err());
    }
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn host_awaits_block_tree_release_before_reopening_persistent_data() {
    use crowdb_paxos_kv::cluster::{PaxosKvServer, PxKvStore};
    use crowdb_paxos_kv::kv::CrowdbTreeEngine;
    use crowdb_paxos_kv_server::recovery::startup::store_crowdb_tree_path;
    use crowdb_paxos_kv_server::shutdown::{release_hosted_trees, shutdown_stores};

    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-runtime-close");
    let mut options = config(root.path());
    options.crowtree_store_kind = "block".to_owned();
    let registry = Arc::new(PaxosKvStoreRegistry::try_with_config(options.clone()).unwrap());
    let runtime = registry.tree_runtime.clone();
    let group = create_group_with_wal_on_runtime(
        1,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &registry.config,
        registry.wal_backend.clone(),
        registry.crowtree_store_kind,
        runtime.clone(),
    )
    .await
    .unwrap();
    let engine = group
        .local_replica()
        .learner
        .engine()
        .as_any()
        .downcast_ref::<CrowdbTreeEngine>()
        .unwrap();
    let tree = engine.handle();
    let release = tree.page_store_release_service().unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    drop(tree);
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    store.add_group_without_election(group);
    store.start().await.unwrap();
    registry.add_store(1, &store);
    drop(store);
    let report = shutdown_stores(&registry).await;
    assert!(report.is_clean(), "{:?}", report.errors);
    let report = tokio::time::timeout(std::time::Duration::from_secs(5), release_hosted_trees(registry))
        .await
        .unwrap();
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_eq!(runtime.snapshot().unwrap().bindings, 0);
    tokio::time::timeout(std::time::Duration::from_millis(100), release.shutdown())
        .await
        .unwrap();

    let directory = store_crowdb_tree_path(&options.data_root, 1, 1);
    let canonical = directory.join("1-1.blk-0000");
    let old_name = directory.join("1-1.blk-0000old");
    tokio::fs::rename(&canonical, &old_name).await.unwrap();

    let reopened = PaxosKvStoreRegistry::try_with_config(options).unwrap();
    let group = create_group_with_wal_on_runtime(
        1,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &reopened.config,
        reopened.wal_backend.clone(),
        reopened.crowtree_store_kind,
        reopened.tree_runtime.clone(),
    )
    .await
    .unwrap();
    let engine = group
        .local_replica()
        .learner
        .engine()
        .as_any()
        .downcast_ref::<CrowdbTreeEngine>()
        .unwrap();
    let tree = engine.handle();
    assert_eq!(tree.get(b"key").unwrap(), Some((1, b"value".to_vec())));
    assert!(old_name.exists());
    assert!(!canonical.exists());
    let release = tree.page_store_release_service().unwrap();
    drop(group);
    let close = Arc::try_unwrap(tree).unwrap().begin_close().unwrap();
    close.wait().await.unwrap();
    release.shutdown().await;
}
