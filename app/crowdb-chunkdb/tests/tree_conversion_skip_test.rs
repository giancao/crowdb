// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tree chunks never enter Mirror-to-EC conversion.

mod common;

use std::sync::Arc;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID};
use crowdb_chunkdb::conversion::{ConversionCoordinator, ConversionError};
use crowdb_chunkdb::routing::{default_binding_table, BindingCache};
use crowdb_chunkdb::task::TaskStore;
use crowdb_protocol::chunk_task::TASK_KIND_MIRROR_TO_EC;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkType, StripType};

async fn sealed_mirror_group(harness: &ChunkdbHarness, chunk_type: ChunkType) -> Chunk {
    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 8, StripType::Mirror, 0, 0, 3, chunk_type, 0, 0)
        .await
        .expect("allocate mirror range");
    harness
        .handler
        .seal_chunk_bytes(&chunk.id.unwrap(), u64::from(chunk.capacity) * 1024)
        .await
        .expect("seal mirror range")
}

#[tokio::test]
async fn automatic_and_explicit_conversion_skip_tree_chunks() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let bindings = BindingCache::new();
    bindings
        .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
        .unwrap();
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), Arc::clone(&tasks));

    let mut tree_chunks = Vec::new();
    for chunk_type in [ChunkType::BtreePage, ChunkType::PageIndex] {
        let chunk = sealed_mirror_group(&harness, chunk_type).await;
        let id = chunk.id.unwrap();
        assert_eq!(coordinator.trigger_chunk(id, 8, 4, u64::MAX).await.unwrap(), 0);
        let prepared = coordinator
            .prepare(
                id,
                chunk.modify_ts,
                0,
                chunk.strips.clone(),
                8,
                4,
                7001,
                30_000,
                100,
            )
            .await;
        assert!(
            matches!(prepared, Err(ConversionError::Conflict)),
            "{chunk_type:?}"
        );
        tree_chunks.push(chunk);
    }
    let convertible = sealed_mirror_group(&harness, ChunkType::S3Dedicated).await;

    let accepted = coordinator
        .trigger_batch(false, 256, 8, 4, 8, 0, u64::MAX)
        .await
        .unwrap();
    assert_eq!(accepted, 1);
    let ready = tasks.scan_ready(u64::MAX, 16).await.unwrap();
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].kind, TASK_KIND_MIRROR_TO_EC);
    assert_eq!(ready[0].partition_id, convertible.id.unwrap());

    for chunk in tree_chunks {
        let current = harness.handler.query_chunk(&chunk.id.unwrap()).await.unwrap();
        assert_eq!(current, chunk);
    }
}

#[tokio::test]
async fn conversion_carries_validity_bits_into_the_ec_strip() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let bindings = BindingCache::new();
    bindings
        .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
        .unwrap();
    let tasks = Arc::new(TaskStore::new(cluster.make_crowdb_client(), bindings));
    let coordinator = ConversionCoordinator::new(Arc::clone(&harness.handler), tasks);

    let chunk = harness
        .handler
        .allocate_chunk(None, 1, 8, StripType::Mirror, 0, 0, 3, ChunkType::S3Shared, 0, 0)
        .await
        .unwrap();
    let id = chunk.id.unwrap();
    let sealed = harness
        .handler
        .seal_chunk_bytes(&id, u64::from(chunk.capacity) * 1024)
        .await
        .unwrap();
    let per_strip = sealed.strips[0].usage_bitmap.len();
    assert!(sealed
        .strips
        .iter()
        .all(|strip| strip.usage_bitmap == vec![0xff; per_strip]));

    let prepared = coordinator
        .prepare(
            id,
            sealed.modify_ts,
            0,
            sealed.strips.clone(),
            8,
            4,
            7001,
            30_000,
            100,
        )
        .await
        .expect("prepare conversion");
    let converted = coordinator
        .complete(id, prepared.task_id, 7001, 101)
        .await
        .expect("publish EC replacement");
    assert_eq!(converted.bitmap_unit_bytes, sealed.bitmap_unit_bytes);
    assert_eq!(converted.strips.len(), 1);
    assert_eq!(converted.strips[0].usage_bitmap, vec![0xff; 8 * per_strip]);
    let retry = coordinator
        .complete(id, prepared.task_id, 7001, 102)
        .await
        .unwrap();
    assert_eq!(retry, converted);
}
