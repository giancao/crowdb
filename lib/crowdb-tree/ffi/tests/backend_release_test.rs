// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{Config, Crowdbtree, PageStoreBackend, TreeRuntime, TreeRuntimeOptions};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn block_host_driver_survives_tree_and_cpu_runtime_then_joins_real_release() {
    let path = std::env::temp_dir().join(format!("crowdb-block-release-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let tree = Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(runtime.clone()),
            path: Some(path.to_string_lossy().into_owned()),
            backend: PageStoreBackend::Block,
            block_size: 1 << 20,
            ..Default::default()
        })
        .unwrap(),
    );
    let service = tree.backend_release_service().unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    tree.flush_with_persistent().unwrap();
    let mut snapshot = tree.snapshot_export_begin(64).unwrap();
    drop(tree);
    drop(runtime);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), service.shutdown())
            .await
            .is_err()
    );
    assert!(!snapshot.read_owned(0).unwrap().bytes.is_empty());
    drop(snapshot);
    tokio::time::timeout(Duration::from_secs(5), service.shutdown())
        .await
        .expect("file release and reactor shutdown must complete");
    service.shutdown().await;
    std::fs::remove_dir_all(path).unwrap();
}

#[tokio::test]
async fn memory_backend_does_not_start_a_release_driver() {
    let tree = Crowdbtree::open(&Config::default()).unwrap();
    assert!(tree.backend_release_service().is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn async_block_open_releases_partial_open_before_retrying_recovery() {
    use crowdb_tree_ffi::AsyncCrowdbtree;
    use std::ffi::OsString;

    let path = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-async-block-open");
    let runtime = Arc::new(
        TreeRuntime::new(TreeRuntimeOptions {
            workers: 1,
            backend_workers: 1,
            ..Default::default()
        })
        .unwrap(),
    );
    let config = Config {
        runtime: Some(runtime.clone()),
        path: Some(path.path().to_string_lossy().into_owned()),
        backend: PageStoreBackend::Block,
        store_id: 1,
        group_id: 3,
        block_size: 1 << 20,
        ..Default::default()
    };

    tokio::time::timeout(Duration::from_secs(5), async {
        let tree = Crowdbtree::open_block_from_names(&config, &[]).await.unwrap();
        let release = tree.backend_release_service().unwrap();
        let tree = Arc::new(tree);
        tree.apply_put(1, b"key", b"durable").unwrap();
        AsyncCrowdbtree::from_handle(tree.clone())
            .flush_with_persistent()
            .await
            .unwrap();
        Arc::try_unwrap(tree)
            .unwrap()
            .begin_close()
            .unwrap()
            .wait()
            .await
            .unwrap();
        release.shutdown().await;

        // The next open uses the actual directory basename, including its suffix.
        let file = OsString::from("1-3.blk-0000");
        let legacy = OsString::from("1-3.blk-0000old");
        std::fs::rename(path.path().join(file), path.path().join(&legacy)).unwrap();
        let missing = OsString::from("1-3.blk-0001");
        assert!(
            Crowdbtree::open_block_from_names(&config, &[legacy.clone(), missing.clone()])
                .await
                .is_err()
        );
        assert!(!path.path().join(&missing).exists());

        let reopened = Crowdbtree::open_block_from_names(&config, &[legacy])
            .await
            .unwrap();
        let release = reopened.backend_release_service().unwrap();
        let reopened = Arc::new(reopened);
        let mut export = AsyncCrowdbtree::from_handle(reopened.clone())
            .snapshot_export_begin(7)
            .await
            .unwrap();
        assert_eq!(export.metadata().at_slot, 1);
        Arc::try_unwrap(reopened)
            .unwrap()
            .begin_close()
            .unwrap()
            .wait()
            .await
            .unwrap();
        let mut wire = Vec::new();
        loop {
            let chunk = export.read_owned_async(wire.len() as u64).await.unwrap();
            wire.extend_from_slice(&chunk.bytes);
            if chunk.done {
                break;
            }
        }
        let metadata = export.metadata();
        assert_eq!(wire.len() as u64, metadata.total_bytes);
        assert!(wire.windows(7).any(|part| part == b"durable"));
        assert_eq!(
            crowdb_tree_ffi::crc32c(&wire[..wire.len() - 4]),
            metadata.final_crc32c
        );
        drop(export);
        release.shutdown().await;
        runtime.shutdown().await.unwrap();
    })
    .await
    .expect("single-worker Block open, failure and retry must complete");
}
