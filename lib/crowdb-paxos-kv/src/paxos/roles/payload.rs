// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use crowdb_rpc_ffi::BufferChain;

// Small coalesced writes are cheaper to copy once than to wrap and retain
// each incoming RPC buffer. This only selects an in-memory representation.
pub(crate) const MIN_PAYLOAD_VIEW_BYTES: usize = 16 * 1024;

/// Immutable canonical proposal bytes. A coalesced proposal owns the original
/// inputs and replaces their individual operation counts with one total count.
#[derive(Clone, Debug)]
pub struct ProposalPayload {
    storage: Storage,
}

#[derive(Clone, Debug)]
enum Storage {
    Contiguous(Bytes),
    Segmented(Arc<Segments>),
}

#[derive(Debug)]
pub(crate) struct Segments {
    count: [u8; 2],
    first: Bytes,
    rest: Vec<Bytes>,
    len: usize,
    pub(crate) tails: BufferChain,
}

impl Segments {
    pub(crate) fn operation_count(&self) -> usize {
        usize::from(u16::from_le_bytes(self.count))
    }

    pub(crate) fn batches(&self) -> impl Iterator<Item = &Bytes> {
        std::iter::once(&self.first).chain(&self.rest)
    }
}

impl From<Bytes> for ProposalPayload {
    fn from(bytes: Bytes) -> Self {
        Self {
            storage: Storage::Contiguous(bytes),
        }
    }
}

impl From<Vec<u8>> for ProposalPayload {
    fn from(bytes: Vec<u8>) -> Self {
        Bytes::from(bytes).into()
    }
}

impl ProposalPayload {
    pub(crate) fn coalesced(first: Bytes, rest: Vec<Bytes>, count: u16) -> Self {
        if rest.is_empty() {
            return first.into();
        }
        let count = count.to_le_bytes();
        let additional = rest.iter().map(|batch| batch.len() - count.len()).sum::<usize>();
        let len = first.len() + additional;
        if len >= MIN_PAYLOAD_VIEW_BYTES && rest.len() < BufferChain::maximum_views() {
            let bodies = std::iter::once(&first)
                .chain(&rest)
                .map(|batch| batch.slice(count.len()..));
            if let Ok(tails) = BufferChain::from_owned_bytes(bodies) {
                return Self {
                    storage: Storage::Segmented(Arc::new(Segments {
                        count,
                        first,
                        rest,
                        len,
                        tails,
                    })),
                };
            }
        }
        let mut bytes = match first.try_into_mut() {
            Ok(mut first) => {
                first.reserve(additional);
                first
            }
            Err(first) => {
                let mut bytes = BytesMut::with_capacity(len);
                bytes.extend_from_slice(&first);
                bytes
            }
        };
        for batch in rest {
            bytes.extend_from_slice(&batch[count.len()..]);
        }
        bytes[..count.len()].copy_from_slice(&count);
        bytes.freeze().into()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::Contiguous(bytes) => bytes.len(),
            Storage::Segmented(parts) => parts.len,
        }
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[must_use]
    pub fn contiguous(&self) -> Option<&Bytes> {
        match &self.storage {
            Storage::Contiguous(bytes) => Some(bytes),
            Storage::Segmented(_) => None,
        }
    }

    pub(crate) fn segments(&self) -> Option<&Segments> {
        match &self.storage {
            Storage::Contiguous(_) => None,
            Storage::Segmented(parts) => Some(parts),
        }
    }

    pub(crate) fn part_count(&self) -> usize {
        self.segments().map_or(1, |parts| parts.rest.len() + 2)
    }

    pub(crate) fn parts(&self) -> impl Iterator<Item = &[u8]> {
        let (prefix, segments) = match &self.storage {
            Storage::Contiguous(bytes) => (bytes.as_ref(), None),
            Storage::Segmented(parts) => (parts.count.as_slice(), Some(parts.as_ref())),
        };
        std::iter::once(prefix).chain(
            segments
                .into_iter()
                .flat_map(|parts| parts.batches().map(|batch| &batch[2..])),
        )
    }

    pub(crate) fn operation_count_bytes(&self) -> Option<&[u8; 2]> {
        self.segments().map(|parts| &parts.count)
    }

    /// Materialize canonical bytes for consumers requiring one contiguous buffer.
    #[must_use]
    pub fn to_bytes(&self) -> Bytes {
        if let Some(bytes) = self.contiguous() {
            return bytes.clone();
        }
        let mut bytes = Vec::with_capacity(self.len());
        for part in self.parts() {
            bytes.extend_from_slice(part);
        }
        bytes.into()
    }
}

impl PartialEq for ProposalPayload {
    fn eq(&self, other: &Self) -> bool {
        match (&self.storage, &other.storage) {
            (Storage::Contiguous(left), Storage::Contiguous(right)) => return left == right,
            (Storage::Segmented(left), Storage::Segmented(right)) if Arc::ptr_eq(left, right) => return true,
            _ => (),
        }
        if self.len() != other.len() {
            return false;
        }
        let mut right_parts = other.parts();
        let mut right = right_parts.next().unwrap_or_default();
        for mut left in self.parts() {
            while !left.is_empty() {
                if right.is_empty() {
                    let Some(part) = right_parts.next() else {
                        return false;
                    };
                    right = part;
                }
                let count = left.len().min(right.len());
                if left[..count] != right[..count] {
                    return false;
                }
                left = &left[count..];
                right = &right[count..];
            }
        }
        true
    }
}

impl Eq for ProposalPayload {}
