// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{CacheHint, LifecycleError, LifecycleHandler, LockPolicy};
use crowdb_protocol::chunk_bitmap;
use crowdb_protocol::chunkdb::rpc::{ChunkState, DeleteChunkRangeRequest, DeleteChunkRangeResponse};

impl LifecycleHandler {
    /// Durably clears the bitmap units of a confirmed logical range. The caller
    /// has proven the range has no live or future reference. Only bitmap chunks
    /// accept range deletion; the offset must sit on a unit boundary and the
    /// tail is rounded up to the unit that the same allocation owns.
    pub async fn delete_chunk_range(
        &self,
        request: &DeleteChunkRangeRequest,
    ) -> Result<DeleteChunkRangeResponse, LifecycleError> {
        let id = request
            .chunk_id
            .ok_or_else(|| LifecycleError::InvalidRequest("missing chunk identity".into()))?;
        self.check_range(&id)?;
        if request.chunk_size == 0 {
            return Err(LifecycleError::InvalidRequest("empty range".into()));
        }
        let mut guard = if let Some(locks) = &self.locks {
            Some(
                locks
                    .acquire(&id, &self.store, &LockPolicy::default(), CacheHint::Cache)
                    .await?,
            )
        } else {
            None
        };
        let mut chunk = match &guard {
            Some(guard) => guard
                .chunk()
                .unwrap_or_else(|| unreachable!("acquire guarantees chunk on Ok"))
                .clone(),
            None => self.store.get_chunk(&id).await?,
        };
        if chunk.state == ChunkState::Deleted as i32 {
            return Ok(DeleteChunkRangeResponse {
                reclaimed: chunk.strips.is_empty() && chunk.cleanup_intents.is_empty(),
            });
        }
        let unit = chunk.bitmap_unit_bytes;
        if unit == 0 {
            return Err(LifecycleError::InvalidRequest(
                "range deletion requires a bitmap chunk".into(),
            ));
        }
        let start = u64::from(request.chunk_offset);
        if start % u64::from(unit) != 0 {
            return Err(LifecycleError::InvalidRequest(
                "range offset is not on a bitmap unit boundary".into(),
            ));
        }
        let end = chunk_bitmap::align_up(start + u64::from(request.chunk_size), unit);
        // Only a confirmed range can be retired, on active and sealed chunks
        // alike: a zero bit beyond the cursor means unwritten.
        if end > chunk_bitmap::align_up(chunk.acknowledged_cursor, unit) {
            return Err(LifecycleError::StateConflict);
        }
        if !chunk_bitmap::clear_range(&mut chunk, start, end) {
            // Every unit was already clear: repeating a retirement is a no-op.
            return Ok(DeleteChunkRangeResponse { reclaimed: false });
        }
        if request.expected_modify_ts != 0 && chunk.modify_ts != request.expected_modify_ts {
            return Err(LifecycleError::StateConflict);
        }
        chunk.modify_ts = chunk.modify_ts.saturating_add(1);
        self.store.put_chunk(&chunk).await.map_err(|error| {
            if let Some(guard) = &guard {
                guard.invalidate();
            }
            error
        })?;
        if let Some(ref mut guard) = guard {
            guard.refresh(chunk);
        }
        Ok(DeleteChunkRangeResponse { reclaimed: false })
    }
}
