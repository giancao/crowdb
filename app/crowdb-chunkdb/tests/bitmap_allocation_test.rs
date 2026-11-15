// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Bitmap chunks start with exact-length all-zero strip bitmaps.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, UNIT_SIZE_BYTES};
use std::sync::Arc;

use crowdb_chunkdb::lifecycle::{LifecycleError, LifecycleHandler};
use crowdb_protocol::chunk_bitmap::{is_well_formed, BITMAP_UNIT_BYTES};
use crowdb_protocol::chunkdb::rpc::{ChunkType, StripType};

#[tokio::test]
async fn bitmap_types_allocate_and_append_zeroed_bitmaps_and_reject_mismatched_units() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let unit_kb = UNIT_SIZE_BYTES / 1024;
    let expected_bytes = usize::try_from(UNIT_SIZE_BYTES / BITMAP_UNIT_BYTES / 8).unwrap();

    for chunk_type in [
        ChunkType::BtreePage,
        ChunkType::PageIndex,
        ChunkType::S3Shared,
        ChunkType::IcebergTableShared,
        ChunkType::DatasetShared,
    ] {
        let chunk = harness
            .handler
            .allocate_chunk(
                None,
                unit_kb,
                2,
                StripType::Mirror,
                0,
                0,
                3,
                chunk_type,
                17,
                30_000,
            )
            .await
            .unwrap();
        assert_eq!(chunk.bitmap_unit_bytes, BITMAP_UNIT_BYTES);
        for strip in &chunk.strips {
            assert_eq!(strip.usage_bitmap, vec![0; expected_bytes]);
            assert!(is_well_formed(
                &strip.usage_bitmap,
                strip.capacity,
                BITMAP_UNIT_BYTES
            ));
        }
        let id = chunk.id.unwrap();
        assert_eq!(harness.store.get_chunk(&id).await.unwrap(), chunk);

        let appended = harness
            .handler
            .append_chunk(&id, chunk.modify_ts, 1, StripType::Mirror, 0, 0, 3, 1)
            .await
            .unwrap();
        assert_eq!(appended.strips.len(), 1);
        let persisted = harness.store.get_chunk(&id).await.unwrap();
        assert_eq!(persisted.bitmap_unit_bytes, BITMAP_UNIT_BYTES);
        assert_eq!(persisted.strips.len(), 3);
        assert!(persisted
            .strips
            .iter()
            .all(|strip| strip.usage_bitmap == vec![0; expected_bytes]));
    }

    let dedicated = harness
        .handler
        .allocate_chunk(
            None,
            unit_kb,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::S3Dedicated,
            0,
            0,
        )
        .await
        .unwrap();
    assert_eq!(dedicated.bitmap_unit_bytes, 0);
    assert!(dedicated.strips.iter().all(|strip| strip.usage_bitmap.is_empty()));

    for (chunk_type, unit) in [
        (ChunkType::S3Shared, 0),
        (ChunkType::BtreePage, 0),
        (ChunkType::S3Shared, 16 * 1024),
        (ChunkType::S3Dedicated, BITMAP_UNIT_BYTES),
    ] {
        let result = harness
            .handler
            .allocate_chunk_owned(
                None,
                unit_kb,
                1,
                StripType::Mirror,
                0,
                0,
                3,
                chunk_type,
                17,
                30_000,
                Vec::new(),
                unit,
            )
            .await;
        assert!(
            matches!(result, Err(LifecycleError::InvalidRequest(_))),
            "{chunk_type:?} with unit {unit}"
        );
    }
}

#[tokio::test]
async fn cursor_advance_and_seal_mark_only_the_newly_confirmed_range() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    // No lock-map cache: the test edits the stored record directly to model a durable clear.
    let handler = LifecycleHandler::new(
        Arc::clone(&harness.store),
        Arc::clone(&harness.allocator),
        harness.topology.clone(),
    );
    let unit_kb = UNIT_SIZE_BYTES / 1024;
    let unit = u64::from(BITMAP_UNIT_BYTES);
    let chunk = handler
        .allocate_chunk(
            None,
            unit_kb,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::S3Shared,
            17,
            30_000,
        )
        .await
        .unwrap();
    let id = chunk.id.unwrap();

    // Two units become valid; everything after the cursor stays zero.
    let advanced = handler
        .advance_chunk_write(&id, 17, chunk.modify_ts, 2 * unit, None, 30_000)
        .await
        .unwrap();
    assert_eq!(advanced.strips[0].usage_bitmap[0], 0b0000_0011);
    assert!(advanced.strips[0].usage_bitmap[1..].iter().all(|byte| *byte == 0));

    // A durable clear below the cursor is not restored by a later advance.
    let mut cleared = harness.store.get_chunk(&id).await.unwrap();
    cleared.strips[0].usage_bitmap[0] &= !0b0000_0001;
    cleared.modify_ts += 1;
    harness.store.put_chunk(&cleared).await.unwrap();
    let advanced = handler
        .advance_chunk_write(&id, 17, cleared.modify_ts, 5 * unit, None, 30_000)
        .await
        .unwrap();
    assert_eq!(advanced.strips[0].usage_bitmap[0], 0b0001_1110);

    // Seal confirms the tail; the earlier clear and the seal length both hold.
    let sealed = handler.seal_chunk_bytes(&id, 9 * unit).await.unwrap();
    assert_eq!(sealed.acknowledged_cursor, 9 * unit);
    assert_eq!(sealed.strips[0].usage_bitmap[0], 0b1111_1110);
    assert_eq!(sealed.strips[0].usage_bitmap[1], 0b0000_0001);
    assert_eq!(harness.store.get_chunk(&id).await.unwrap(), sealed);

    // Chunks without a bitmap are untouched by cursor advances.
    let dedicated = handler
        .allocate_chunk(
            None,
            unit_kb,
            1,
            StripType::Mirror,
            0,
            0,
            3,
            ChunkType::S3Dedicated,
            23,
            30_000,
        )
        .await
        .unwrap();
    let advanced = handler
        .advance_chunk_write(
            &dedicated.id.unwrap(),
            23,
            dedicated.modify_ts,
            unit,
            None,
            30_000,
        )
        .await
        .unwrap();
    assert!(advanced.strips[0].usage_bitmap.is_empty());
}
