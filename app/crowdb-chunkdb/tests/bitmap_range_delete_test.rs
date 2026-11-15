// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Range deletion durably clears bitmap units and never touches other chunks.

mod common;

use common::cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, UNIT_SIZE_BYTES};
use crowdb_chunkdb::lifecycle::LifecycleError;
use crowdb_protocol::chunkdb::rpc::{ChunkType, DeleteChunkRangeRequest, StripType};
use crowdb_protocol::common::ChunkId;

fn range(id: ChunkId, offset: u32, size: u32) -> DeleteChunkRangeRequest {
    DeleteChunkRangeRequest {
        chunk_id: Some(id),
        chunk_offset: offset,
        chunk_size: size,
        expected_modify_ts: 0,
    }
}

#[tokio::test]
async fn range_delete_clears_whole_units_of_confirmed_ranges_and_is_idempotent() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let handler = &harness.handler;
    let unit_kb = UNIT_SIZE_BYTES / 1024;
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
    // Eight units are confirmed: 11111111.
    let advanced = handler
        .advance_chunk_write(&id, 17, chunk.modify_ts, 8 * 4096, None, 30_000)
        .await
        .unwrap();
    assert_eq!(advanced.strips[0].usage_bitmap[0], 0b1111_1111);

    // An object at unit 2 with a 5000-byte encoding owns units 2 and 3.
    assert!(
        !handler
            .delete_chunk_range(&range(id, 2 * 4096, 5000))
            .await
            .unwrap()
            .reclaimed
    );
    let cleared = handler.query_chunk(&id).await.unwrap();
    assert_eq!(cleared.strips[0].usage_bitmap[0], 0b1111_0011);
    assert_eq!(cleared.acknowledged_cursor, 8 * 4096);
    assert_eq!(cleared.modify_ts, advanced.modify_ts + 1);

    // Repeated and overlapping notifications change nothing further.
    handler
        .delete_chunk_range(&range(id, 2 * 4096, 5000))
        .await
        .unwrap();
    handler.delete_chunk_range(&range(id, 3 * 4096, 1)).await.unwrap();
    assert_eq!(handler.query_chunk(&id).await.unwrap(), cleared);

    // Unaligned offsets, unconfirmed ranges and empty ranges fail before any mutation.
    for request in [range(id, 100, 4096), range(id, 4096, 0)] {
        assert!(matches!(
            handler.delete_chunk_range(&request).await,
            Err(LifecycleError::InvalidRequest(_))
        ));
    }
    assert!(matches!(
        handler.delete_chunk_range(&range(id, 8 * 4096, 4096)).await,
        Err(LifecycleError::StateConflict)
    ));
    let mut stale = range(id, 0, 4096);
    stale.expected_modify_ts = advanced.modify_ts;
    assert!(matches!(
        handler.delete_chunk_range(&stale).await,
        Err(LifecycleError::StateConflict)
    ));
    assert_eq!(handler.query_chunk(&id).await.unwrap(), cleared);

    // A later cursor advance and the seal keep the clear and the seal length.
    let advanced = handler
        .advance_chunk_write(&id, 17, cleared.modify_ts, 10 * 4096, None, 30_000)
        .await
        .unwrap();
    assert_eq!(advanced.strips[0].usage_bitmap[0], 0b1111_0011);
    assert_eq!(advanced.strips[0].usage_bitmap[1], 0b0000_0011);
    let sealed = handler.seal_chunk_bytes(&id, 10 * 4096).await.unwrap();
    assert_eq!(sealed.acknowledged_cursor, 10 * 4096);
    assert_eq!(sealed.strips[0].usage_bitmap[0], 0b1111_0011);
    assert_eq!(harness.store.get_chunk(&id).await.unwrap(), sealed);
}

#[tokio::test]
async fn range_delete_fails_for_chunks_without_a_bitmap_and_works_for_tree_chunks() {
    let cluster = KvCluster::start_pinned().await;
    seed_hardware(&cluster.make_hardware_client()).await;
    let _diskdb = DiskdbServer::start(&cluster).await;
    let harness = ChunkdbHarness::start(&cluster).await;
    let handler = &harness.handler;
    let unit_kb = UNIT_SIZE_BYTES / 1024;

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
            0,
            0,
        )
        .await
        .unwrap();
    let dedicated_id = dedicated.id.unwrap();
    let sealed = handler.seal_chunk_bytes(&dedicated_id, 8192).await.unwrap();
    assert!(matches!(
        handler.delete_chunk_range(&range(dedicated_id, 0, 8192)).await,
        Err(LifecycleError::InvalidRequest(_))
    ));
    assert_eq!(handler.query_chunk(&dedicated_id).await.unwrap(), sealed);

    for chunk_type in [ChunkType::BtreePage, ChunkType::PageIndex] {
        let chunk = handler
            .allocate_chunk(
                None,
                unit_kb,
                1,
                StripType::Mirror,
                0,
                0,
                3,
                chunk_type,
                9,
                30_000,
            )
            .await
            .unwrap();
        let id = chunk.id.unwrap();
        handler
            .advance_chunk_write(&id, 9, chunk.modify_ts, 4 * 4096, None, 30_000)
            .await
            .unwrap();
        handler.delete_chunk_range(&range(id, 4096, 8192)).await.unwrap();
        let cleared = handler.query_chunk(&id).await.unwrap();
        assert_eq!(cleared.strips[0].usage_bitmap[0], 0b0000_1001, "{chunk_type:?}");
    }
}
