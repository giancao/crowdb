// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_protocol::chunkdb_fb::FBChunkType;

#[test]
fn business_chunk_tags_use_dedicated_and_shared_values() {
    for (tag, kind) in [
        (1, ChunkType::Wal),
        (2, ChunkType::BtreePage),
        (3, ChunkType::PageIndex),
        (4, ChunkType::Stream),
        (5, ChunkType::S3Dedicated),
        (6, ChunkType::S3Shared),
        (7, ChunkType::IcebergTableDedicated),
        (8, ChunkType::IcebergTableShared),
        (9, ChunkType::DatasetDedicated),
        (10, ChunkType::DatasetShared),
    ] {
        assert_eq!(kind as i32, tag);
        assert_eq!(ChunkType::try_from(tag).unwrap(), kind);
    }
    assert_eq!(FBChunkType::DatasetShared.0, 10);
}

#[test]
fn unknown_chunk_tags_are_rejected() {
    for tag in [0, 11] {
        assert!(ChunkType::try_from(tag).is_err(), "tag {tag}");
    }
}
