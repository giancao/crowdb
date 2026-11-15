// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_bitmap::{
    align_up, bit_count, carry_over, is_supported_unit, is_well_formed, mark_written, same_layout, set_range,
    zeroed, BITMAP_UNIT_BYTES,
};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, ChunkType};

#[test]
fn only_shared_and_tree_types_select_a_bitmap_unit() {
    for kind in [
        ChunkType::BtreePage,
        ChunkType::PageIndex,
        ChunkType::S3Shared,
        ChunkType::IcebergTableShared,
        ChunkType::DatasetShared,
    ] {
        assert!(kind.uses_bitmap());
        assert_eq!(kind.bitmap_unit_bytes(), BITMAP_UNIT_BYTES);
    }
    for kind in [
        ChunkType::Wal,
        ChunkType::Stream,
        ChunkType::S3Dedicated,
        ChunkType::IcebergTableDedicated,
        ChunkType::DatasetDedicated,
    ] {
        assert!(!kind.uses_bitmap());
        assert_eq!(kind.bitmap_unit_bytes(), 0);
    }
    assert!(is_supported_unit(4096));
    for unit in [0, 1024, 8192, 16384] {
        assert!(!is_supported_unit(unit));
    }
}

#[test]
fn zeroed_bitmap_has_one_bit_per_unit() {
    assert_eq!(bit_count(1024, 4096), Some(256));
    assert_eq!(zeroed(1024, 4096), Some(vec![0; 32]));
    assert_eq!(zeroed(4096, 4096).unwrap().len(), 128);
    // 12 KiB is three units: one partial byte.
    assert_eq!(zeroed(12, 4096), Some(vec![0]));
    assert_eq!(bit_count(1024, 0), None);
    // 6 KiB is not a whole number of 4 KiB units.
    assert_eq!(zeroed(6, 4096), None);
}

#[test]
fn well_formed_bitmap_matches_geometry_and_has_no_tail_bits() {
    assert!(is_well_formed(&[0; 32], 1024, 4096));
    assert!(is_well_formed(&[0xff; 32], 1024, 4096));
    assert!(!is_well_formed(&[], 1024, 4096));
    assert!(!is_well_formed(&[0; 31], 1024, 4096));
    assert!(!is_well_formed(&[0; 33], 1024, 4096));
    assert!(is_well_formed(&[0b0000_0111], 12, 4096));
    assert!(!is_well_formed(&[0b0000_1000], 12, 4096));
    assert!(!is_well_formed(&[0; 32], 1024, 0));
}

#[test]
fn align_up_rounds_to_whole_units_and_ignores_chunks_without_a_bitmap() {
    assert_eq!(align_up(0, 4096), 0);
    assert_eq!(align_up(1, 4096), 4096);
    assert_eq!(align_up(4096, 4096), 4096);
    assert_eq!(align_up(5000, 4096), 8192);
    assert_eq!(align_up(5000, 0), 5000);
}

#[test]
fn set_range_marks_exact_units_and_rejects_unaligned_or_out_of_range_input() {
    let mut bitmap = zeroed(1024, 4096).unwrap();
    assert!(set_range(&mut bitmap, 4096, 0, 8192));
    assert_eq!(bitmap[0], 0b0000_0011);
    assert!(set_range(&mut bitmap, 4096, 7 * 4096, 10 * 4096));
    assert_eq!((bitmap[0], bitmap[1]), (0b1000_0011, 0b0000_0011));
    assert!(set_range(&mut bitmap, 4096, 4096, 4096));
    let before = bitmap.clone();
    for (start, end) in [(1, 4096), (0, 5000), (8192, 4096), (0, 257 * 4096)] {
        assert!(!set_range(&mut bitmap, 4096, start, end));
    }
    assert!(!set_range(&mut bitmap, 0, 0, 4096));
    assert_eq!(bitmap, before);
    assert!(set_range(&mut bitmap, 4096, 0, 256 * 4096));
    assert_eq!(bitmap, vec![0xff; 32]);
}

fn bitmap_chunk(unit_bytes: u32) -> Chunk {
    let strip = |sequence: u32| ChunkStrip {
        chunk_offset: sequence * 1024,
        strip_sequence: sequence,
        capacity: 1024,
        usage_bitmap: if unit_bytes == 0 {
            Vec::new()
        } else {
            zeroed(1024, unit_bytes).unwrap()
        },
        ..ChunkStrip::default()
    };
    Chunk {
        bitmap_unit_bytes: unit_bytes,
        capacity: 2048,
        strips: vec![strip(0), strip(1)],
        ..Chunk::default()
    }
}

#[test]
fn mark_written_covers_touched_units_across_strips_and_keeps_other_bits() {
    let mut chunk = bitmap_chunk(4096);
    // 5000 encoded bytes at offset 0 occupy two units.
    mark_written(&mut chunk, 0, 5000);
    assert_eq!(chunk.strips[0].usage_bitmap[0], 0b0000_0011);
    // A cleared bit below the newly written range is left alone.
    chunk.strips[0].usage_bitmap[0] = 0b0000_0010;
    let strip = 1024 * 1024;
    mark_written(&mut chunk, strip - 4096, strip + 8192);
    assert_eq!(chunk.strips[0].usage_bitmap[0], 0b0000_0010);
    assert_eq!(chunk.strips[0].usage_bitmap[31], 0b1000_0000);
    assert_eq!(chunk.strips[1].usage_bitmap[0], 0b0000_0011);
    assert!(chunk.strips[1].usage_bitmap[1..].iter().all(|byte| *byte == 0));
    // Empty ranges and ranges past the capacity change nothing.
    let before = chunk.clone();
    mark_written(&mut chunk, 4096, 4096);
    mark_written(&mut chunk, 2 * strip, 3 * strip);
    assert_eq!(chunk, before);

    let mut plain = bitmap_chunk(0);
    mark_written(&mut plain, 0, strip);
    assert!(plain.strips.iter().all(|strip| strip.usage_bitmap.is_empty()));
}

#[test]
fn carry_over_regroups_bits_by_logical_offset_and_rejects_mismatched_ranges() {
    let mut chunk = bitmap_chunk(4096);
    chunk.strips[0].usage_bitmap[0] = 0b0000_0101;
    chunk.strips[1].usage_bitmap[31] = 0b1000_0000;
    let mut merged = vec![ChunkStrip {
        capacity: 2048,
        ..ChunkStrip::default()
    }];
    assert!(carry_over(&chunk.strips, &mut merged, 4096));
    assert_eq!(merged[0].usage_bitmap.len(), 64);
    assert_eq!(merged[0].usage_bitmap[0], 0b0000_0101);
    assert_eq!(merged[0].usage_bitmap[63], 0b1000_0000);
    assert!(merged[0].usage_bitmap[1..63].iter().all(|byte| *byte == 0));

    // Splitting back restores the per-strip bitmaps.
    let mut split = bitmap_chunk(4096).strips;
    assert!(carry_over(&merged, &mut split, 4096));
    assert!(same_layout(&split, &chunk.strips));
    assert_eq!(split, chunk.strips);

    let mut short = vec![ChunkStrip {
        capacity: 1024,
        ..ChunkStrip::default()
    }];
    assert!(!carry_over(&chunk.strips, &mut short, 4096));
    let mut malformed = chunk.strips.clone();
    malformed[0].usage_bitmap.pop();
    assert!(!carry_over(&malformed, &mut merged, 4096));

    let mut other = chunk.strips.clone();
    other[0].usage_bitmap[0] = 0;
    assert!(same_layout(&other, &chunk.strips));
    other[0].capacity = 2048;
    assert!(!same_layout(&other, &chunk.strips));
}
