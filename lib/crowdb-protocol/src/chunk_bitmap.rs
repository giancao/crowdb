// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Logical-range validity bitmap carried by bitmap strips.
//!
//! One bit covers `bitmap_unit_bytes` of a strip's logical data capacity.
//! Bit `i` lives in byte `i / 8` at position `i % 8`.

/// The only bitmap granularity currently accepted for new chunks.
pub const BITMAP_UNIT_BYTES: u32 = 4096;

/// Returns whether `unit_bytes` is a granularity new bitmap chunks may select.
#[must_use]
pub const fn is_supported_unit(unit_bytes: u32) -> bool {
    unit_bytes == BITMAP_UNIT_BYTES
}

/// Number of bits covering a strip of `capacity_kb`, or `None` when the
/// capacity is not a whole number of units.
#[must_use]
pub fn bit_count(capacity_kb: u32, unit_bytes: u32) -> Option<u32> {
    if unit_bytes == 0 {
        return None;
    }
    let capacity_bytes = u64::from(capacity_kb) * 1024;
    if capacity_bytes % u64::from(unit_bytes) != 0 {
        return None;
    }
    u32::try_from(capacity_bytes / u64::from(unit_bytes)).ok()
}

/// An all-zero bitmap for a newly allocated strip.
#[must_use]
pub fn zeroed(capacity_kb: u32, unit_bytes: u32) -> Option<Vec<u8>> {
    bit_count(capacity_kb, unit_bytes).map(|bits| vec![0; (bits as usize).div_ceil(8)])
}

/// Returns whether `bitmap` has exactly the length required by the geometry
/// and no bit set beyond the last unit.
#[must_use]
pub fn is_well_formed(bitmap: &[u8], capacity_kb: u32, unit_bytes: u32) -> bool {
    let Some(bits) = bit_count(capacity_kb, unit_bytes) else {
        return false;
    };
    let bits = bits as usize;
    if bitmap.len() != bits.div_ceil(8) {
        return false;
    }
    let tail = bits % 8;
    tail == 0 || bitmap.last().map_or(true, |last| last >> tail == 0)
}

/// Rounds `value` up to the next multiple of `unit_bytes`; a zero unit means
/// the chunk has no bitmap and nothing is aligned.
#[must_use]
pub const fn align_up(value: u64, unit_bytes: u32) -> u64 {
    if unit_bytes == 0 {
        return value;
    }
    let unit = unit_bytes as u64;
    value.div_ceil(unit) * unit
}

/// Sets the bits covering `[start, end)` within one strip's logical range.
/// Offsets are relative to the strip start and must be unit-aligned and
/// inside the bitmap; returns false without changing anything otherwise.
pub fn set_range(bitmap: &mut [u8], unit_bytes: u32, start: u64, end: u64) -> bool {
    if unit_bytes == 0 || start > end {
        return false;
    }
    let unit = u64::from(unit_bytes);
    if start % unit != 0 || end % unit != 0 || end / unit > bitmap.len() as u64 * 8 {
        return false;
    }
    for bit in start / unit..end / unit {
        bitmap[(bit / 8) as usize] |= 1 << (bit % 8);
    }
    true
}

/// Marks every bitmap unit touched by the chunk-logical byte range
/// `[start, end)` as written. Strips without a bitmap are skipped, so the call
/// is a no-op for chunks that have none. Only the given range is touched:
/// bits outside it keep their value, which preserves earlier range clears.
pub fn mark_written(chunk: &mut crate::chunkdb::rpc::Chunk, start: u64, end: u64) {
    let unit = u64::from(chunk.bitmap_unit_bytes);
    if unit == 0 || start >= end {
        return;
    }
    for strip in &mut chunk.strips {
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let strip_end = strip_start + u64::from(strip.capacity) * 1024;
        let (from, to) = (start.max(strip_start), end.min(strip_end));
        if from >= to || strip.usage_bitmap.is_empty() {
            continue;
        }
        let first = (from - strip_start) / unit;
        let last = (to - strip_start)
            .div_ceil(unit)
            .min(strip.usage_bitmap.len() as u64 * 8);
        for bit in first..last {
            strip.usage_bitmap[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }
}

/// Moves validity bits from `old` strips to `replacement` strips that cover
/// the same logical range with a different grouping, such as eight mirror
/// strips replaced by one EC strip. Returns false when either side does not
/// match the bitmap geometry or the ranges differ in size.
pub fn carry_over(
    old: &[crate::chunkdb::rpc::ChunkStrip],
    replacement: &mut [crate::chunkdb::rpc::ChunkStrip],
    unit_bytes: u32,
) -> bool {
    let mut bits = Vec::new();
    for strip in old {
        if !is_well_formed(&strip.usage_bitmap, strip.capacity, unit_bytes) {
            return false;
        }
        let count = bit_count(strip.capacity, unit_bytes).unwrap_or(0) as usize;
        bits.extend((0..count).map(|bit| strip.usage_bitmap[bit / 8] >> (bit % 8) & 1 == 1));
    }
    let mut next = 0;
    for strip in replacement.iter_mut() {
        let Some(count) = bit_count(strip.capacity, unit_bytes) else {
            return false;
        };
        let count = count as usize;
        if next + count > bits.len() {
            return false;
        }
        let mut bitmap = vec![0_u8; count.div_ceil(8)];
        for (bit, set) in bits[next..next + count].iter().enumerate() {
            if *set {
                bitmap[bit / 8] |= 1 << (bit % 8);
            }
        }
        strip.usage_bitmap = bitmap;
        next += count;
    }
    next == bits.len()
}

/// Layout equality that ignores validity bits, which change independently of
/// the physical layout a replacement request describes.
#[must_use]
pub fn same_layout(
    left: &[crate::chunkdb::rpc::ChunkStrip],
    right: &[crate::chunkdb::rpc::ChunkStrip],
) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            let mut left = left.clone();
            left.usage_bitmap.clone_from(&right.usage_bitmap);
            left == *right
        })
}

/// Clears every bitmap unit inside the unit-aligned chunk-logical range
/// `[start, end)`. Returns whether any bit changed, so a repeated clear is a
/// detectable no-op. Strips without a bitmap are skipped.
pub fn clear_range(chunk: &mut crate::chunkdb::rpc::Chunk, start: u64, end: u64) -> bool {
    let unit = u64::from(chunk.bitmap_unit_bytes);
    if unit == 0 || start >= end {
        return false;
    }
    let mut changed = false;
    for strip in &mut chunk.strips {
        let strip_start = u64::from(strip.chunk_offset) * 1024;
        let strip_end = strip_start + u64::from(strip.capacity) * 1024;
        let (from, to) = (start.max(strip_start), end.min(strip_end));
        if from >= to || strip.usage_bitmap.is_empty() {
            continue;
        }
        let first = (from - strip_start) / unit;
        let last = (to - strip_start)
            .div_ceil(unit)
            .min(strip.usage_bitmap.len() as u64 * 8);
        for bit in first..last {
            let (byte, mask) = ((bit / 8) as usize, 1_u8 << (bit % 8));
            changed |= strip.usage_bitmap[byte] & mask != 0;
            strip.usage_bitmap[byte] &= !mask;
        }
    }
    changed
}
