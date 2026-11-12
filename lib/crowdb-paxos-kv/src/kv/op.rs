// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use std::ops::Range;

use crate::paxos::roles::ProposalPayload;

pub(super) type OperationRanges = (Range<usize>, Option<Range<usize>>);

/// A single mutation within a batch: set a value or delete (tombstone) a key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put(Bytes),
    Delete,
}

/// One `(key, op)` mutation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BatchOp {
    pub key: Bytes,
    pub op: Op,
}

/// An ordered list of mutations that all share one slot.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Batch {
    pub ops: Vec<BatchOp>,
}

/// Per-key single-version cell: a live value or a tombstone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Cell {
    Value(Vec<u8>),
    Tombstone,
}

/// A logical difference between two engines at one key. `None` means the key
/// is absent on that side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineDiff {
    pub key: Vec<u8>,
    pub left: Option<(u64, Cell)>,
    pub right: Option<(u64, Cell)>,
}

impl Batch {
    pub(crate) fn single_put_key(payload: &[u8]) -> Option<&[u8]> {
        Self::single_put_ranges(payload).map(|(key, _)| &payload[key])
    }

    pub(crate) fn single_put_ranges(payload: &[u8]) -> Option<(Range<usize>, Range<usize>)> {
        if !payload.starts_with(&[1, 0, 0]) {
            return None;
        }
        let mut cursor = 3;
        let key = read_field_range(payload, &mut cursor).ok()?;
        let value = read_field_range(payload, &mut cursor).ok()?;
        (cursor == payload.len()).then_some((key, value))
    }

    /// Validate an entire consensus payload before admitting or applying it.
    /// Empty payloads represent gap-repair `NoOp` entries.
    ///
    /// # Errors
    /// Rejects truncated fields, unknown operations, nonempty delete values,
    /// and bytes following the declared operation count.
    pub fn try_decode(payload: &Bytes) -> Result<Self, &'static str> {
        let (count, ranges) = Self::operation_ranges(payload)?;
        let mut ops = Vec::with_capacity(count);
        Self::append_operations(payload, ranges, &mut ops)?;
        Ok(Self { ops })
    }

    pub(crate) fn try_decode_proposal(payload: &ProposalPayload) -> Result<Self, &'static str> {
        if let Some(bytes) = payload.contiguous() {
            return Self::try_decode(bytes);
        }
        let parts = payload.segments().expect("segmented proposal");
        let count = parts.operation_count();
        let mut ops = Vec::with_capacity(count);
        for batch in parts.batches() {
            let (_, ranges) = Self::operation_ranges(batch)?;
            Self::append_operations(batch, ranges, &mut ops)?;
        }
        if ops.len() != count {
            return Err("coalesced batch operation count mismatch");
        }
        Ok(Self { ops })
    }

    fn append_operations(
        payload: &Bytes,
        ranges: impl Iterator<Item = Result<OperationRanges, &'static str>>,
        ops: &mut Vec<BatchOp>,
    ) -> Result<(), &'static str> {
        for operation in ranges {
            let (key, value) = operation?;
            ops.push(BatchOp {
                key: payload.slice(key),
                op: value.map_or(Op::Delete, |value| Op::Put(payload.slice(value))),
            });
        }
        Ok(())
    }

    pub(crate) fn operation_ranges(
        payload: &[u8],
    ) -> Result<
        (
            usize,
            impl Iterator<Item = Result<OperationRanges, &'static str>> + '_,
        ),
        &'static str,
    > {
        let mut cursor = 0;
        let count = if payload.is_empty() {
            0
        } else {
            let count = payload.get(..2).ok_or("truncated batch operation count")?;
            cursor = 2;
            usize::from(u16::from_le_bytes([count[0], count[1]]))
        };
        if count > (payload.len() - cursor) / 9 {
            return Err("truncated batch operation");
        }
        if count == 0 && cursor != payload.len() {
            return Err("trailing bytes after batch operations");
        }
        let mut remaining = count;
        let operations = std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            remaining -= 1;
            let mut read = || {
                let kind = *payload.get(cursor).ok_or("truncated operation kind")?;
                cursor += 1;
                let key = read_field_range(payload, &mut cursor)?;
                let value = read_field_range(payload, &mut cursor)?;
                let value = match kind {
                    0 => Some(value),
                    1 if value.is_empty() => None,
                    1 => return Err("delete operation carries a value"),
                    _ => return Err("unknown batch operation kind"),
                };
                if remaining == 0 && cursor != payload.len() {
                    return Err("trailing bytes after batch operations");
                }
                Ok((key, value))
            };
            Some(read())
        });
        Ok((count, operations))
    }

    /// Decode the minimal KV payload emitted by
    /// `PxKvStore::encode_kv_payload` / `encode_kv_batch_items`.
    ///
    /// Wire format:
    ///   [`op_count`: u16 LE]
    ///   for each op:
    ///     [`kind`: u8]  0 = Put, 1 = Delete
    ///     [`key_len`: u32 LE][key bytes]
    ///     [`value_len`: u32 LE][value bytes]  (`value_len` = 0 for Delete)
    ///
    /// An empty payload (e.g. a `NoOp` repair entry) decodes to an empty
    /// batch. Malformed/truncated input stops decoding at the bad record.
    #[must_use]
    pub fn decode(payload: &Bytes) -> Self {
        let mut ops = Vec::new();
        if payload.is_empty() {
            return Self { ops };
        }
        let op_count =
            u16::from_le_bytes([*payload.first().unwrap_or(&0), *payload.get(1).unwrap_or(&0)]) as usize;
        let mut offset = 2usize;
        for _ in 0..op_count {
            if offset >= payload.len() {
                break;
            }
            let kind = payload[offset];
            offset += 1;

            let key_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let key = if offset + key_len <= payload.len() {
                payload.slice(offset..offset + key_len)
            } else {
                Bytes::new()
            };
            offset += key_len;

            let value_len = read_u32_le(payload, offset) as usize;
            offset += 4;
            let value = if offset + value_len <= payload.len() {
                payload.slice(offset..offset + value_len)
            } else {
                Bytes::new()
            };
            offset += value_len;

            let op = if kind == 0 { Op::Put(value) } else { Op::Delete };
            ops.push(BatchOp { key, op });
        }
        Self { ops }
    }
}

fn read_u32_le(buf: &Bytes, offset: usize) -> u32 {
    let b = buf.get(offset..offset + 4).unwrap_or(&[0; 4]);
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}

fn read_field_range(payload: &[u8], cursor: &mut usize) -> Result<Range<usize>, &'static str> {
    let end = cursor.checked_add(4).ok_or("batch field length overflow")?;
    let length = payload.get(*cursor..end).ok_or("truncated batch field length")?;
    let length = u32::from_le_bytes([length[0], length[1], length[2], length[3]]) as usize;
    *cursor = end;
    let end = cursor.checked_add(length).ok_or("batch field length overflow")?;
    if end > payload.len() {
        return Err("truncated batch field bytes");
    }
    let field = *cursor..end;
    *cursor = end;
    Ok(field)
}
