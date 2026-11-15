// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Ordinary-mode prefix scan over objects padded to the chunk's bitmap unit.

use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::{
    encode_frame, FrameMagic, FramePrefixScanner, FrameScanConfig, FrameScanError, FrameScanMode,
    FrameScanStop,
};

const CHUNK: ChunkId = ChunkId {
    high: 6 << 56,
    low: 9,
};
const UNIT: usize = 4096;

fn config(limit: usize, required_end: usize) -> FrameScanConfig {
    FrameScanConfig {
        chunk_id: CHUNK,
        mode: FrameScanMode::Ordinary,
        physical_start: 0,
        physical_limit: limit as u64,
        required_end: required_end as u64,
        logical_start: 0,
        max_append_bytes: 64 * 1024 * 1024,
        align: u32::try_from(UNIT).unwrap(),
    }
}

/// One single-frame object followed by zero padding to the next unit boundary.
fn padded_object(payload_len: usize, fill: u8) -> (Vec<u8>, usize) {
    let mut bytes = encode_frame(FrameMagic::RepoSmallV2, CHUNK, &vec![fill; payload_len], 1).unwrap();
    let encoded = bytes.len();
    bytes.resize(encoded.next_multiple_of(UNIT), 0);
    (bytes, encoded)
}

fn feed(scanner: &mut FramePrefixScanner, bytes: &[u8], step: usize) -> Vec<(u64, u64)> {
    let mut extents = Vec::new();
    let mut offset = 0;
    while offset < bytes.len() && scanner.stop() == FrameScanStop::Running {
        let end = (offset + step).min(bytes.len());
        let mut consumed = offset;
        while consumed < end && scanner.stop() == FrameScanStop::Running {
            let batch = scanner.push(consumed as u64, &bytes[consumed..end], 16).unwrap();
            extents.extend(
                batch
                    .extents
                    .iter()
                    .map(|extent| (extent.physical_offset, extent.physical_length)),
            );
            consumed += batch.consumed;
        }
        offset = end;
    }
    extents
}

#[test]
fn padding_between_aligned_objects_is_skipped_and_counted_in_the_prefix() {
    let (first, first_encoded) = padded_object(1000, 0x31);
    let (second, second_encoded) = padded_object(5000, 0x32);
    assert_eq!((first.len(), second.len()), (UNIT, 2 * UNIT));
    let mut image = [first, second].concat();
    let written = image.len();
    image.resize(written + 2 * UNIT, 0);

    for step in [1, 7, 4096, image.len()] {
        let mut scanner = FramePrefixScanner::new(config(image.len(), written)).unwrap();
        let extents = feed(&mut scanner, &image, step);
        assert_eq!(
            extents,
            vec![(0, first_encoded as u64), (UNIT as u64, second_encoded as u64)],
            "step {step}"
        );
        let summary = scanner.finish().unwrap();
        assert_eq!(summary.stop, FrameScanStop::Unwritten);
        assert_eq!(summary.physical_end, written as u64);
        assert_eq!(summary.logical_end, 6000);
    }
}

#[test]
fn exactly_filled_units_need_no_padding_and_reach_exact_capacity() {
    let (first, _) = padded_object(1000, 0x31);
    let (second, _) = padded_object(2000, 0x32);
    let image = [first, second].concat();
    let mut scanner = FramePrefixScanner::new(config(image.len(), image.len())).unwrap();
    assert_eq!(feed(&mut scanner, &image, 512).len(), 2);
    let summary = scanner.finish().unwrap();
    assert_eq!(summary.stop, FrameScanStop::ExactCapacity);
    assert_eq!(summary.physical_end, image.len() as u64);
}

#[test]
fn nonzero_padding_is_rejected_and_unaligned_scan_keeps_contiguous_frames() {
    let (mut first, first_encoded) = padded_object(1000, 0x31);
    first[first_encoded + 3] = 1;
    let (second, _) = padded_object(100, 0x32);
    let image = [first, second].concat();
    let mut scanner = FramePrefixScanner::new(config(image.len(), 0)).unwrap();
    let error = scanner.push(0, &image, 16).unwrap_err();
    assert!(matches!(error, FrameScanError::Frame { .. }), "{error}");

    // Without a bitmap unit, objects are contiguous and zero bytes end the prefix.
    let (padded, encoded) = padded_object(1000, 0x31);
    let mut unaligned = config(padded.len(), encoded);
    unaligned.align = 0;
    let mut scanner = FramePrefixScanner::new(unaligned).unwrap();
    assert_eq!(feed(&mut scanner, &padded, 64), vec![(0, encoded as u64)]);
    let summary = scanner.finish().unwrap();
    assert_eq!(summary.stop, FrameScanStop::Unwritten);
    assert_eq!(summary.physical_end, encoded as u64);
}
