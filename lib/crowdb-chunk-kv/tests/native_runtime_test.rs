// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/root_catalog.rs"]
mod root_catalog;

use crowdb_chunk_kv::{CrowdbPartitionTree, MutationOperation, PartitionRange, PartitionTree};
use crowdb_chunk_kv::{Partition, PartitionConfig, PartitionId};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamPurpose, StreamRecord,
    StreamRecordState,
};
use crowdb_tree_ffi::chunk::test_util::TestPageChunkStore;
use crowdb_tree_ffi::Config;
use root_catalog::TestCatalogLoad;
use std::sync::atomic::Ordering;
use std::sync::Arc;

fn config(backend: &TestPageChunkStore) -> Config {
    Config {
        page_store: Some(backend.page_store(0).unwrap()),
        ..backend.config()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn range_rebuild_keeps_the_runtime_available_during_catalog_io() {
    let observed = Arc::new(TestCatalogLoad::default());
    let source_backend = TestPageChunkStore::new(91, 1).unwrap();
    let source = CrowdbPartitionTree::open(91, &config(&source_backend)).unwrap();
    source
        .apply(
            1,
            &MutationOperation::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            },
        )
        .await
        .unwrap();
    source.checkpoint(0).await.unwrap();
    for (sequence, key, value) in [(2, b"key", b"new"), (3, b"out", b"old")] {
        source
            .apply(
                sequence,
                &MutationOperation::Put {
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
            )
            .await
            .unwrap();
    }
    let mut child_backend = TestPageChunkStore::new(92, 1).unwrap();
    let hook = observed.clone();
    child_backend
        .on_root_load_for_tests(move || hook.wait_for_async_progress())
        .unwrap();
    let child_config = config(&child_backend);
    let progress = observed.clone();
    let heartbeat = tokio::spawn(async move {
        progress.entered.notified().await;
        progress.async_progress.store(true, Ordering::Release);
    });
    observed.armed.store(true, Ordering::Release);
    let (source, child, stats) = tokio::spawn(async move {
        let (child, stats) = source
            .rebuild_range(
                92,
                &PartitionRange {
                    start: Some(b"k".to_vec()),
                    end: Some(b"l".to_vec()),
                },
                child_config,
            )
            .await
            .unwrap();
        (source, child, stats)
    })
    .await
    .unwrap();
    heartbeat.await.unwrap();
    assert!(
        observed.observed_progress.load(Ordering::Acquire),
        "catalog IO blocked the only async worker"
    );
    assert_eq!(source.checkpoint_state().unwrap().1, 1);
    assert_eq!(child.checkpoint_state().unwrap().1, 3);
    assert_eq!(stats.entries_emitted, 1);
    assert_eq!(child.get(b"key").await.unwrap().unwrap().value, b"new");
    assert!(child.get(b"out").await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn native_recovery_keeps_the_runtime_available_during_catalog_io() {
    let observed = Arc::new(TestCatalogLoad::default());
    let mut backend = TestPageChunkStore::new(93, 1).unwrap();
    let hook = observed.clone();
    backend
        .on_root_load_for_tests(move || hook.wait_for_async_progress())
        .unwrap();
    let config = config(&backend);
    let pages = config.page_store.clone().unwrap();
    let store = Arc::new(MemoryStreamStore::new(4096));
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: StreamName { high: 1, low: 93 },
            metadata_group_id: 7,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: None,
            created_at_ms: StreamRecord::creation_time_ms(),
        },
        1,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    let progress = observed.clone();
    let heartbeat = tokio::spawn(async move {
        progress.entered.notified().await;
        progress.async_progress.store(true, Ordering::Release);
    });
    observed.armed.store(true, Ordering::Release);
    let recovered = tokio::spawn(async move {
        Partition::recover_native_latest_prepared_assignment(
            PartitionId { high: 1, low: 93 },
            PartitionRange::default(),
            1,
            93,
            PartitionConfig::default(),
            config,
            pages,
            stream,
        )
        .await
        .unwrap()
    })
    .await
    .unwrap();
    heartbeat.await.unwrap();
    assert!(
        observed.observed_progress.load(Ordering::Acquire),
        "native recovery blocked the only async worker"
    );
    assert_eq!(recovered.snapshot().applied_seq, 0);
    recovered.close().await.unwrap();
}
