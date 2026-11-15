// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogStore, RoutedCatalogStore},
    file::canonical_file_key,
    gc::{CandidatePhase, GcPhase, GcRepository, GcStalledReason, GcTask, GcTaskKind},
    key::{CatalogScope, IcebergKey, OperationId, TableId},
    record::StorageRecord,
    table::{storage_head_key, TableHead, TablePurgeTask},
};
use crowdb_chunkdb::{
    routing::{BindingCache, BindingTable},
    storage::ChunkStore,
};
use crowdb_paxos_kv_client::{ChunkSlotMapClient, PaxosKvClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkType};

use super::storage::TestPayloads;
use crate::access_iceberg::native::TestNativeStorage;

pub(super) struct TestRangeProof {
    chunks: ChunkStore,
    before: Chunk,
    expected: Vec<(u64, u64)>,
}

impl TestRangeProof {
    pub(super) async fn new(stack: &TestNativeStorage, data: &TestPayloads) -> Self {
        let kv = Arc::new(PaxosKvClient::new(crowdb_paxos_kv_client::ClientConfig::new(
            stack.seeds.clone(),
        )));
        let maps = ChunkSlotMapClient::new(kv.clone());
        let bindings = BindingCache::new();
        bindings
            .replace(BindingTable::new(maps.read_storage().await.unwrap()))
            .unwrap();
        let chunks = ChunkStore::new(kv, bindings);
        let before = chunks.get_chunk(&data.chunk).await.unwrap();
        assert_eq!(before.id, Some(data.chunk));
        assert_eq!(before.chunk_type, ChunkType::IcebergTableShared as i32);
        assert_eq!(before.state, ChunkState::Sealed as i32);
        assert_ne!(before.writer_epoch, 0);
        assert_ne!(before.bitmap_unit_bytes, 0);
        assert!(data
            .dead_ranges
            .iter()
            .all(|(a, b)| a < b && *b <= before.acknowledged_cursor));
        assert!(data.live_range.1 <= before.acknowledged_cursor);
        let expected = union(&data.dead_ranges);
        assert!(expected
            .iter()
            .all(|(start, end)| *end <= data.live_range.0 || data.live_range.1 <= *start));
        Self {
            chunks,
            before,
            expected,
        }
    }

    pub(super) async fn verify(&self, data: &TestPayloads) -> serde_json::Value {
        let chunk = self.chunks.get_chunk(&data.chunk).await.unwrap();
        // Range retirement only clears validity bits; the layout and seal length stay.
        let mut layout = chunk.clone();
        layout.modify_ts = self.before.modify_ts;
        for (strip, before) in layout.strips.iter_mut().zip(&self.before.strips) {
            strip.usage_bitmap.clone_from(&before.usage_bitmap);
        }
        assert_eq!(
            layout, self.before,
            "logical retirement must not change recorded layout"
        );
        let unit = u64::from(chunk.bitmap_unit_bytes);
        for (start, end) in &self.expected {
            assert_eq!(start % unit, 0, "retired objects start on a bitmap unit");
            let mut offset = *start;
            while offset < *end {
                assert!(!unit_is_set(&chunk, offset), "unit at {offset} must be cleared");
                offset += unit;
            }
        }
        let mut offset = data.live_range.0 / unit * unit;
        while offset < data.live_range.1 {
            assert!(unit_is_set(&chunk, offset), "live unit at {offset} must stay set");
            offset += unit;
        }
        serde_json::json!({
            "chunk_id": chunk.id,
            "bitmap_unit_bytes": chunk.bitmap_unit_bytes,
            "sealed_bytes": chunk.acknowledged_cursor,
            "cleared_ranges": self.expected,
        })
    }
}

fn unit_is_set(chunk: &Chunk, offset: u64) -> bool {
    let unit = u64::from(chunk.bitmap_unit_bytes);
    chunk.strips.iter().any(|strip| {
        let start = u64::from(strip.chunk_offset) * 1024;
        let end = start + u64::from(strip.capacity) * 1024;
        if offset < start || offset >= end {
            return false;
        }
        let bit = usize::try_from((offset - start) / unit).unwrap();
        strip.usage_bitmap[bit / 8] >> (bit % 8) & 1 == 1
    })
}

fn union(input: &[(u64, u64)]) -> Vec<(u64, u64)> {
    let mut input = input.to_vec();
    input.sort_unstable();
    let mut ranges: Vec<(u64, u64)> = Vec::new();
    for (start, end) in input {
        if let Some(last) = ranges.last_mut().filter(|last| last.1 >= start) {
            last.1 = last.1.max(end);
        } else {
            ranges.push((start, end));
        }
    }
    ranges
}

pub(super) fn identity(table: TableId) -> OperationId {
    OperationId::from_bytes(table.as_bytes()).unwrap()
}

pub(super) async fn wait_for_task(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
) -> GcTask {
    let repository = GcRepository::new(store);
    loop {
        if let Some(task) = repository.task(context.catalog, identity(table)).await.unwrap() {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub(super) async fn wait_complete(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
    dropped_ms: u64,
) -> GcTask {
    let repository = GcRepository::new(store);
    let mut revision = 0;
    loop {
        let task = repository
            .task(context.catalog, identity(table))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.kind, GcTaskKind::PurgeTable);
        assert_eq!(task.context, context);
        assert_eq!(task.created_ms, dropped_ms);
        assert_eq!(task.not_before_ms, dropped_ms + GcTask::PURGE_DELAY_MS);
        assert!(!task.paused);
        assert_ne!(task.phase, GcPhase::Quarantined, "{task:?}");
        if task.revision != revision {
            println!(
                "gc_notification_progress {}",
                serde_json::json!({
                    "revision":task.revision,"phase":format!("{:?}",task.phase),
                    "deleted":task.deleted,"retry_at_ms":task.retry_at_ms,
                    "stalled":format!("{:?}",task.stalled),
                })
            );
            revision = task.revision;
        }
        if task.phase == GcPhase::Complete {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub(super) async fn verify_resources(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    task: &GcTask,
    data: &TestPayloads,
    stale: &TablePurgeTask,
    live_head: &TableHead,
) {
    assert_eq!(task.deleted, 2);
    assert_eq!(
        task.reclaimed_bytes,
        data.dead.iter().map(|file| file.length).sum::<u64>()
    );
    assert_eq!(task.stalled, GcStalledReason::None);
    assert_eq!(task.retry_at_ms, 0);
    assert!(!task.deferred_ranges && !task.fenced);
    let head = task.head.as_ref().unwrap();
    for file in &data.dead {
        assert!(store
            .get(&canonical_file_key(&file.location).encode().unwrap())
            .await
            .unwrap()
            .is_none());
        let mut suffix = head.table.as_bytes().to_vec();
        suffix.extend_from_slice(&head.generation.to_be_bytes());
        suffix.extend_from_slice(file.file.as_bytes());
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::GcCandidate,
            suffix,
        };
        let value = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
        let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
            panic!("candidate expected")
        };
        assert_eq!(candidate.file, *file);
        assert_eq!(candidate.task, task.identity);
        assert_eq!(candidate.generation, head.generation);
        assert_eq!(candidate.phase, CandidatePhase::Complete);
        assert!(candidate.completed_round > 0 && candidate.completed_round <= task.sweep_round);
        assert!(candidate.cursor.frames.is_empty() && candidate.cursor.pending.is_none());
    }
    let head_key = storage_head_key(data.live.location.table());
    let value = store.get(&head_key.encode().unwrap()).await.unwrap().unwrap();
    assert_eq!(
        StorageRecord::decode(&head_key, &value.bytes).unwrap(),
        StorageRecord::TableHead(Box::new(live_head.clone()))
    );
    assert!(store.get(&stale.key().encode().unwrap()).await.unwrap().is_none());
    assert!(GcRepository::new(store)
        .task(context.catalog, identity(stale.head.table))
        .await
        .unwrap()
        .is_none());
}
