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

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn block_gc_retains_old_view_across_persistent_flushes() {
    use crowdb_tree_ffi::AsyncCrowdbtree;
    use std::os::unix::fs::MetadataExt;

    let path = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-block-gc-view");
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let config = Config {
        runtime: Some(runtime.clone()),
        path: Some(path.path().to_string_lossy().into_owned()),
        backend: PageStoreBackend::Block,
        block_size: 1 << 16,
        frame_bytes: 4096,
        max_inline_value: 64,
        ..Default::default()
    };
    let tree = Arc::new(Crowdbtree::open_block_from_names(&config, &[]).await.unwrap());
    let release = tree.backend_release_service().unwrap();
    let async_tree = AsyncCrowdbtree::from_handle(tree.clone());
    let original = vec![0x37; 512 * 1024];
    tree.apply_put(1, b"key", &original).unwrap();
    let report = async_tree.flush_with_persistent_report().await.unwrap();
    report.status.unwrap();
    let view = tree.read_only_view(report.snapshot_sequence, 1).unwrap();
    let old_files: Vec<_> = std::fs::read_dir(path.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains(".blk-"))
        .filter_map(|entry| Some((entry.path(), entry.metadata().ok()?.ino())))
        .collect();
    assert!(old_files.len() > 3, "the value must span Block extents");
    let old_files_remaining = || {
        old_files
            .iter()
            .filter(|(file, ino)| std::fs::metadata(file).is_ok_and(|metadata| metadata.ino() == *ino))
            .count()
    };

    for slot in 2_u64..=5 {
        tree.apply_put(slot, b"key", &slot.to_be_bytes()).unwrap();
        assert_eq!(async_tree.flush_with_persistent().await.unwrap(), slot);
        let (old_slot, value) = view.get(b"key").await.unwrap().unwrap();
        assert_eq!(old_slot, 1);
        assert_eq!(value, original);
    }
    let protected = old_files_remaining();
    assert!(protected > 1, "the old view must keep its Block extents");
    view.close().await.unwrap();
    drop(view);
    tree.apply_delete(6, b"key").unwrap();
    assert_eq!(async_tree.flush_with_persistent().await.unwrap(), 6);
    assert!(async_tree.get(b"key".to_vec()).await.unwrap().is_none());
    tokio::time::timeout(Duration::from_secs(5), async {
        while old_files_remaining() >= protected {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("background GC must remove extents after the old view releases them");

    drop(async_tree);
    Arc::try_unwrap(tree)
        .unwrap()
        .begin_close()
        .unwrap()
        .wait()
        .await
        .unwrap();
    release.shutdown().await;
    runtime.shutdown().await.unwrap();
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn abandoned_block_read_drains_before_last_handle_closes() {
    use crowdb_tree_ffi::{AsyncCrowdbtree, GetOutcome};

    struct ResumeIo(std::path::PathBuf);
    impl Drop for ResumeIo {
        fn drop(&mut self) {
            let _ = std::fs::write(self.0.join("resume"), []);
        }
    }

    let path = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-block-abandoned-read");
    let runtime = Arc::new(TreeRuntime::new(TreeRuntimeOptions::default()).unwrap());
    let config = Config {
        runtime: Some(runtime.clone()),
        path: Some(path.path().to_string_lossy().into_owned()),
        backend: PageStoreBackend::Block,
        frame_bytes: 4096,
        max_inline_value: 64,
        ..Default::default()
    };
    let tree = Arc::new(Crowdbtree::open_block_from_names(&config, &[]).await.unwrap());
    let release = tree.backend_release_service().unwrap();
    let async_tree = AsyncCrowdbtree::from_handle(tree.clone());
    tree.apply_put(1, b"key", &vec![0x4d; 64 * 1024]).unwrap();
    async_tree.flush_with_persistent().await.unwrap();
    assert!(async_tree.get(b"key".to_vec()).await.unwrap().is_some());
    assert!(tree.evict_clean_leaves(0) > 0);

    let gate = std::env::var_os("CROWDB_TEST_BLOCK_READ_GATE").map(|directory| ResumeIo(directory.into()));
    if let Some(gate) = &gate {
        std::fs::write(gate.0.join("armed"), []).unwrap();
    }
    let wait = async_tree.try_get(b"key");
    if let Some(gate) = &gate {
        assert!(matches!(&wait, GetOutcome::Pending(_)));
        tokio::time::timeout(Duration::from_secs(5), async {
            while !gate.0.join("submitted").exists() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("a real Block read must be submitted before abandoning the waiter");
    }
    drop(wait);
    drop(async_tree);
    let closing = Arc::try_unwrap(tree).unwrap().begin_close().unwrap();
    if gate.is_some() {
        assert!(tokio::time::timeout(Duration::from_millis(30), closing.wait())
            .await
            .is_err());
    }
    drop(gate);
    tokio::time::timeout(Duration::from_secs(5), async {
        assert_eq!(tokio::join!(closing.wait(), closing.wait()), (Ok(()), Ok(())));
        release.shutdown().await;
        runtime.shutdown().await.unwrap();
    })
    .await
    .expect("the late read completion must release the Tree, files and reactor");
    assert!(std::fs::read_dir("/proc/self/fd")
        .unwrap()
        .filter_map(Result::ok)
        .filter_map(|entry| std::fs::read_link(entry.path()).ok())
        .all(|target| !target.starts_with(path.path())));
}
