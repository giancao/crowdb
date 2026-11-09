// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_tree_ffi::{
    Config, Crowdbtree, CtError, KeyRange, PageStore, ScanDirection, ScanOptions, TreeRuntime,
};
use std::sync::Arc;

fn config(runtime: Arc<TreeRuntime>) -> Config {
    Config {
        runtime: Some(runtime),
        page_store: Some(Arc::new(PageStore::open_mem(4096).unwrap())),
        frame_bytes: 4096,
        max_inline_value: 64,
        ..Config::default()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn read_only_view_close_shares_completion_and_rejects_new_reads() {
    let runtime = Arc::new(TreeRuntime::new(Default::default()).unwrap());
    let tree = Crowdbtree::open(&config(runtime.clone())).unwrap();
    tree.apply_put(1, b"key", b"value").unwrap();
    let (sequence, coverage) = tree.flush_with_persistent_info().unwrap();
    let view = tree.read_only_view(sequence, coverage).unwrap();

    let (first, second) = tokio::join!(view.close(), view.close());
    assert_eq!(first, Ok(()));
    assert_eq!(second, Ok(()));
    assert_eq!(view.get(b"key").await, Err(CtError::Unavailable));
    assert_eq!(
        view.scan(&ScanOptions::default()).await.unwrap_err(),
        CtError::Unavailable
    );
    assert!(tree.get(b"key").unwrap().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn fixed_snapshot_scans_both_directions_and_keeps_results_after_all_handles_close() {
    let runtime = Arc::new(TreeRuntime::new(Default::default()).unwrap());
    let tree = Crowdbtree::open(&config(runtime.clone())).unwrap();
    tree.apply_put(1, b"", b"empty").unwrap();
    tree.apply_put(2, b"b", b"old").unwrap();
    tree.apply_put(3, b"c", b"deleted").unwrap();
    tree.apply_delete(4, b"c").unwrap();
    let (sequence, coverage) = tree.flush_with_persistent_info().unwrap();
    let snapshot = tree.read_only_view(sequence, coverage).unwrap();
    assert_eq!(
        tree.read_only_view(sequence, coverage + 1).unwrap_err(),
        CtError::Unavailable
    );
    tree.apply_put(5, b"b", b"new").unwrap();
    tree.flush_with_persistent_info().unwrap();
    let closing = tree.begin_close().unwrap();
    let (first, second) = tokio::join!(closing.wait(), closing.wait());
    assert_eq!(first, Ok(()));
    assert_eq!(second, Ok(()));
    assert_eq!(snapshot.get(b"b").await.unwrap().unwrap(), (2, b"old".to_vec()));
    let (forward, truncated) = snapshot
        .scan(&ScanOptions {
            start: Some(vec![]),
            start_inclusive: true,
            limit: 8,
            ..ScanOptions::default()
        })
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(
        forward.iter().map(|e| e.key.as_ref()).collect::<Vec<_>>(),
        vec![b"".as_slice(), b"b"]
    );
    let (reverse, truncated) = snapshot
        .scan(&ScanOptions {
            direction: ScanDirection::Reverse,
            include_tombstones: true,
            keys_only: true,
            limit: 8,
            ..ScanOptions::default()
        })
        .await
        .unwrap();
    assert!(!truncated);
    assert_eq!(
        reverse.iter().map(|e| e.key.as_ref()).collect::<Vec<_>>(),
        vec![b"c".as_slice(), b"b", b""]
    );
    assert!(reverse[0].tombstone);
    assert!(reverse.iter().all(|e| e.value.is_empty()));
    let (bounded, truncated) = snapshot
        .scan(&ScanOptions {
            byte_budget: 1,
            strict_byte_budget: true,
            ..ScanOptions::default()
        })
        .await
        .unwrap();
    assert!(bounded.is_empty());
    assert!(truncated);
    snapshot.close().await.unwrap();
    drop(snapshot);
    runtime.drain_cpu();
    drop(runtime);
    assert_eq!(forward[1].value.as_ref(), b"old");
}

#[tokio::test(flavor = "current_thread")]
async fn immutable_rebuild_copies_only_target_range_and_keeps_captured_frontier() {
    let runtime = Arc::new(TreeRuntime::new(Default::default()).unwrap());
    let tree = Crowdbtree::open(&config(runtime.clone())).unwrap();
    tree.apply_put(1, b"a", b"left").unwrap();
    tree.apply_put(2, b"z", &vec![b'v'; 9000]).unwrap();
    let (sequence, coverage) = tree.flush_with_persistent_info().unwrap();
    let snapshot = tree.read_only_view(sequence, coverage).unwrap();
    tree.apply_put(3, b"z", b"changed").unwrap();
    tree.begin_close().unwrap().wait().await.unwrap();
    let options = Config {
        key_range: KeyRange::Bounded {
            start: Some(b"m".to_vec()),
            end: None,
        },
        ..config(runtime.clone())
    };
    let (child, stats) = snapshot.rebuild_range_async(&options).await.unwrap();
    assert_eq!(stats.entries_emitted, 1);
    assert_eq!(stats.pages_reused, 0);
    assert!(stats.pages_rebuilt > 0);
    assert_eq!(child.durable_state().unwrap().1, coverage);
    assert_eq!(child.get(b"z").unwrap().unwrap().1, vec![b'v'; 9000]);
    assert_eq!(child.get(b"a").unwrap_err(), CtError::InvalidArgument);
    snapshot.close().await.unwrap();
    drop(snapshot);
    child.begin_close().unwrap().wait().await.unwrap();
    let reopened = Crowdbtree::open_async(&options).await.unwrap();
    assert_eq!(reopened.get(b"z").unwrap().unwrap().1, vec![b'v'; 9000]);
    reopened.begin_close().unwrap().wait().await.unwrap();
    runtime.shutdown().await.unwrap();
}
