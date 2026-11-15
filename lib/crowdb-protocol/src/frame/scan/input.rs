// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    frame_error, FramePrefixScanner, FrameScanBatch, FrameScanError, FrameScanMode, FrameScanStop,
    MAX_FRAME_SCAN_EXTENTS, MAX_FRAME_SCAN_INPUT_BYTES,
};
use crate::frame::{frame_length, parse_header, FRAME_HEADER_PREFIX_BYTES};

impl FramePrefixScanner {
    /// Parses at most a 1 MiB input window and emits at most 1,024 complete
    /// extents. Resume unconsumed bytes at the returned contiguous input offset.
    /// # Errors
    /// Rejects invalid bounds/budgets, gaps, corruption, wrong roles and identities.
    /// Discard the scanner after any error; a partial failed observation is invalid.
    pub fn push(
        &mut self,
        offset: u64,
        input: &[u8],
        max_extents: usize,
    ) -> Result<FrameScanBatch, FrameScanError> {
        if self.stop == FrameScanStop::Failed {
            return Err(FrameScanError::FailedObservation);
        }
        let result = self.push_input(offset, input, max_extents);
        if result.is_err() {
            self.stop = FrameScanStop::Failed;
        }
        result
    }

    fn push_input(
        &mut self,
        offset: u64,
        input: &[u8],
        max_extents: usize,
    ) -> Result<FrameScanBatch, FrameScanError> {
        let input_len = u64::try_from(input.len()).map_err(|_| FrameScanError::InvalidBounds)?;
        if max_extents == 0
            || max_extents > MAX_FRAME_SCAN_EXTENTS
            || input.len() > MAX_FRAME_SCAN_INPUT_BYTES
            || offset
                .checked_add(input_len)
                .map_or(true, |end| end > self.config.physical_limit)
        {
            return Err(FrameScanError::InvalidBounds);
        }
        if offset != self.input_end {
            return Err(FrameScanError::OutOfOrder);
        }
        let mut result = FrameScanBatch {
            consumed: 0,
            extents: Vec::new(),
        };
        while result.consumed < input.len()
            && result.extents.len() < max_extents
            && self.stop == FrameScanStop::Running
        {
            if self.skip_alignment_padding(input, &mut result.consumed)? {
                continue;
            }
            self.copy_scan_input(input, &mut result.consumed, FRAME_HEADER_PREFIX_BYTES);
            if self.buffer.len() < FRAME_HEADER_PREFIX_BYTES {
                break;
            }
            let frame_start = self.input_end - self.buffer.len() as u64;
            if self.buffer.iter().all(|byte| *byte == 0) {
                self.stop = FrameScanStop::Unwritten;
                self.check_required()?;
                break;
            }
            let header = parse_header(&self.buffer).map_err(|error| frame_error(frame_start, error))?;
            let length = frame_length(header).map_err(|error| frame_error(frame_start, error))?;
            if frame_start
                .checked_add(length as u64)
                .map_or(true, |end| end > self.config.physical_limit)
            {
                return Err(frame_error(
                    frame_start,
                    crate::frame::FrameError::InvalidRegionLength,
                ));
            }
            self.copy_scan_input(input, &mut result.consumed, length);
            if self.buffer.len() < length {
                break;
            }
            let mut bytes = std::mem::take(&mut self.buffer);
            let extent = self.accept_scan_frame(frame_start, &bytes)?;
            bytes.clear();
            self.buffer = bytes;
            if let Some(extent) = extent {
                result.extents.push(extent);
            }
        }
        Ok(result)
    }

    /// A frame never starts with a zero byte, so a zero byte at an unaligned
    /// frame boundary is the padding that completes an object's last unit.
    fn skip_alignment_padding(&mut self, input: &[u8], consumed: &mut usize) -> Result<bool, FrameScanError> {
        let align = u64::from(self.config.align);
        if align == 0
            || self.config.mode != FrameScanMode::Ordinary
            || !self.buffer.is_empty()
            || self.input_end % align == 0
            || input[*consumed] != 0
        {
            return Ok(false);
        }
        let to_boundary = align - self.input_end % align;
        let count =
            usize::try_from(to_boundary).map_or(usize::MAX, |bytes| bytes.min(input.len() - *consumed));
        if input[*consumed..*consumed + count].iter().any(|byte| *byte != 0) {
            return Err(frame_error(
                self.input_end,
                crate::frame::FrameError::InvalidRegionLength,
            ));
        }
        *consumed += count;
        self.input_end += count as u64;
        if self.input_end % align == 0 {
            self.physical_end = self.input_end;
        }
        Ok(true)
    }

    fn copy_scan_input(&mut self, input: &[u8], consumed: &mut usize, target: usize) {
        let count = target
            .saturating_sub(self.buffer.len())
            .min(input.len() - *consumed);
        self.buffer
            .extend_from_slice(&input[*consumed..*consumed + count]);
        *consumed += count;
        self.input_end += count as u64;
    }
}
