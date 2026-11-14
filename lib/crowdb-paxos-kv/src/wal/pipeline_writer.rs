// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Per-pipeline dedicated writer task (W3).
//!
//! Each pipeline has one long-running async task that owns the active
//! `WalSegment` exclusively. Append callers use one command queue. Strict
//! appends carry an encoded record and an ack. Early-ack Accepted records
//! are encoded by the writer and do not carry a waiter. The writer drains
//! queued records in a batch, writes them to the segment in one `pwrite`,
//! issues a single `fdatasync`, then resolves every pending ack.
//!
//! ## Scheduling
//!
//! The writer parks on `timeout(watchdog, rx.recv())` when idle. On wake it
//! drains all ready records with `try_recv`, then flushes once and acks the
//! whole batch. The watchdog (`wal_flush_watchdog_ms`, default 100 ms) is a
//! safety-net timer that fires periodically while idle to drain any queued
//! record in case of a missed wake — the idle wakeup does a `try_recv` and
//! re-parks if nothing is queued (no I/O, ~10 wakeups/s at the default).
//!
//! ## Durability contract
//!
//! An ack resolves `Ok` only after the covering `fdatasync` succeeds. On
//! failure the writer marks the WAL failed, fails all batched acks, drains
//! and fails remaining queued records, then exits.

use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use tokio::sync::{mpsc, oneshot};
use tokio::time::{timeout, Instant};
use tracing::{debug, error, trace};

use crate::metrics::{Bandwidth, LatencySummary};
use crate::paxos::roles::SlotIndex;
use crate::paxos::PxGroupId;

use super::index::{SegmentMeta, ShardedSegmentIndex, SlotLocation};
use super::record::{RecordFrame, WALRecord, WalRecordFormat};
use super::segment::WalSegment;
use super::IoBackend;

/// Encoded record ready for the writer. Binary records use a zero-copy frame
/// that retains the original payload owners; text-line records keep the formatted
/// line as bytes.
pub(crate) enum EncodedRecord {
    Binary(RecordFrame),
    TextLine(Vec<u8>),
}

impl EncodedRecord {
    pub fn encode(record: &WALRecord, format: WalRecordFormat) -> Self {
        match format {
            WalRecordFormat::Binary => Self::Binary(record.encode_frame()),
            WalRecordFormat::TextLine => Self::TextLine(record.encode_text_line().into_bytes()),
            WalRecordFormat::Auto => unreachable!("Auto must be resolved at pipeline creation"),
        }
    }

    /// Total on-disk byte length.
    #[must_use]
    pub fn total_len(&self) -> usize {
        match self {
            Self::Binary(frame) => frame.total_len(),
            Self::TextLine(bytes) => bytes.len(),
        }
    }

    fn io_slice_count(&self) -> usize {
        match self {
            Self::Binary(frame) => frame.io_slice_count(),
            Self::TextLine(_) => 1,
        }
    }

    /// Append this record's slices to `slices` for a vectored write.
    pub fn append_io_slices<'a>(&'a self, slices: &mut Vec<std::io::IoSlice<'a>>) {
        match self {
            Self::Binary(frame) => frame.append_io_slices(slices),
            Self::TextLine(bytes) => slices.push(std::io::IoSlice::new(bytes)),
        }
    }
}

/// A pending write request queued by an append caller.
pub(crate) struct PendingWrite {
    /// Already-encoded record.
    pub encoded: EncodedRecord,
    /// Slot index for index update (0 = metadata record, not indexed).
    pub slot: SlotIndex,
    pub term: u64,
    pub is_vote: bool,
    pub completion: WriteCompletion,
}

pub(crate) enum WriteCompletion {
    Ack(oneshot::Sender<io::Result<SlotLocation>>),
    Early(Option<(std::time::Instant, Arc<LatencySummary>)>),
}

impl WriteCompletion {
    fn fail(self, slot: SlotIndex, error: io::Error) {
        match self {
            Self::Ack(ack) => {
                let _ = ack.send(Err(error));
            }
            Self::Early(_) => error!(slot, %error, "WAL persist Accepted failed (early ack)"),
        }
    }
}

pub(crate) enum WriteRequest {
    Encoded(PendingWrite),
    EarlyAccepted {
        record: WALRecord,
        completion: WriteCompletion,
    },
}

impl WriteRequest {
    fn encode(self, format: WalRecordFormat) -> PendingWrite {
        match self {
            Self::Encoded(write) => write,
            Self::EarlyAccepted { record, completion } => PendingWrite {
                encoded: EncodedRecord::encode(&record, format),
                slot: record.slot,
                term: record.term,
                is_vote: false,
                completion,
            },
        }
    }
}

/// Acknowledgment sender for Seal/Flush commands.
type WriterAck = oneshot::Sender<io::Result<()>>;

/// Commands sent to the writer task.
pub(crate) enum WriterCommand {
    /// A pending write from an append caller.
    Write(WriteRequest),
    /// Seal the active segment and open a new one. The oneshot is resolved
    /// after the seal is durable.
    Seal { ack: oneshot::Sender<io::Result<()>> },
    /// Durably flush the active segment (real `fsync`/`sync_all`). Used
    /// during shutdown to persist data even when `--no-fsync` is set.
    /// Does NOT seal or rotate the segment.
    Flush { ack: oneshot::Sender<io::Result<()>> },
}

/// Spawn the writer task for one pipeline. Returns the command sender.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_pipeline_writer(
    pipeline_idx: usize,
    backend: Arc<IoBackend>,
    pipeline_path: std::path::PathBuf,
    record_format: WalRecordFormat,
    group_id: PxGroupId,
    next_segment_id: Arc<AtomicU64>,
    segment_size: u64,
    watchdog: Duration,
    batch_bytes: usize,
    failed: Arc<AtomicBool>,
    index: Arc<ShardedSegmentIndex>,
    flush_count: Arc<AtomicU64>,
    records_flushed: Arc<AtomicU64>,
    watchdog_wakeups: Arc<AtomicU64>,
    skip_fsync: bool,
    fsync_summary: Arc<OnceLock<Arc<LatencySummary>>>,
    write_bandwidth: Arc<OnceLock<Arc<Bandwidth>>>,
) -> (mpsc::UnboundedSender<WriterCommand>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = mpsc::unbounded_channel::<WriterCommand>();
    let jh = tokio::spawn(pipeline_writer_loop(
        rx,
        pipeline_idx,
        backend,
        pipeline_path,
        record_format,
        group_id,
        next_segment_id,
        segment_size,
        watchdog,
        batch_bytes,
        failed,
        index,
        flush_count,
        records_flushed,
        watchdog_wakeups,
        skip_fsync,
        fsync_summary,
        write_bandwidth,
    ));
    (tx, jh)
}

#[allow(clippy::too_many_arguments)]
async fn pipeline_writer_loop(
    mut rx: mpsc::UnboundedReceiver<WriterCommand>,
    pipeline_idx: usize,
    backend: Arc<IoBackend>,
    pipeline_path: std::path::PathBuf,
    record_format: WalRecordFormat,
    group_id: PxGroupId,
    next_segment_id: Arc<AtomicU64>,
    segment_size: u64,
    watchdog: Duration,
    batch_bytes: usize,
    failed: Arc<AtomicBool>,
    index: Arc<ShardedSegmentIndex>,
    flush_count: Arc<AtomicU64>,
    records_flushed: Arc<AtomicU64>,
    watchdog_wakeups: Arc<AtomicU64>,
    skip_fsync: bool,
    fsync_summary: Arc<OnceLock<Arc<LatencySummary>>>,
    write_bandwidth: Arc<OnceLock<Arc<Bandwidth>>>,
) {
    // Reusable per-batch buffers — declared outside the loop so their
    // allocations persist across wake cycles (no per-batch malloc/free).
    let mut batch: Vec<PendingWrite> = Vec::new();
    let mut offsets: Vec<u64> = Vec::new();

    // Create the initial active segment.
    let mut segment = match create_segment(
        &backend,
        &pipeline_path,
        &next_segment_id,
        group_id,
        record_format,
    )
    .await
    {
        Ok(seg) => seg,
        Err(e) => {
            failed.store(true, Ordering::Release);
            error!(pipeline_idx, error = %e, "failed to create initial WAL segment");
            drain_and_fail_all(&mut rx).await;
            return;
        }
    };

    loop {
        let Some(cmd) = recv_command(
            &mut rx,
            watchdog,
            &mut segment,
            &index,
            pipeline_idx,
            &watchdog_wakeups,
        )
        .await
        else {
            return;
        };

        match cmd {
            WriterCommand::Seal { ack } => {
                if !handle_seal_command(
                    &mut segment,
                    ack,
                    &mut rx,
                    &backend,
                    &pipeline_path,
                    &next_segment_id,
                    group_id,
                    record_format,
                    &index,
                    pipeline_idx,
                    &failed,
                )
                .await
                {
                    return;
                }
            }
            WriterCommand::Write(first) => {
                if !handle_write_command(
                    first.encode(record_format),
                    &mut batch,
                    &mut offsets,
                    &mut segment,
                    &mut rx,
                    &backend,
                    &pipeline_path,
                    record_format,
                    group_id,
                    &next_segment_id,
                    segment_size,
                    batch_bytes,
                    skip_fsync,
                    &fsync_summary,
                    &write_bandwidth,
                    &index,
                    pipeline_idx,
                    &flush_count,
                    &records_flushed,
                    &failed,
                )
                .await
                {
                    return;
                }
            }
            WriterCommand::Flush { ack } => {
                // Drain any pending writes first, then do a real fsync
                // on the active segment. This is the shutdown path —
                // always durable, regardless of --no-fsync.
                let result = segment.flush().await;
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                let _ = ack.send(result);
            }
        }
    }
}

/// Process a Write command: batch-drain, write, fsync, index, rotate, ack.
/// Returns `false` if a fatal error occurred and the writer loop should exit.
#[allow(clippy::too_many_arguments)]
async fn handle_write_command(
    first: PendingWrite,
    batch: &mut Vec<PendingWrite>,
    offsets: &mut Vec<u64>,
    segment: &mut WalSegment,
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    record_format: WalRecordFormat,
    group_id: PxGroupId,
    next_segment_id: &Arc<AtomicU64>,
    segment_size: u64,
    batch_bytes: usize,
    skip_fsync: bool,
    fsync_summary: &OnceLock<Arc<LatencySummary>>,
    write_bandwidth: &OnceLock<Arc<Bandwidth>>,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    flush_count: &Arc<AtomicU64>,
    records_flushed: &Arc<AtomicU64>,
    failed: &Arc<AtomicBool>,
) -> bool {
    batch.clear();
    batch.push(first);
    let mut batch_bytes_acc = batch[0].encoded.total_len();
    let (mut pending_seal_acks, pending_flush_acks) =
        drain_pending_commands(rx, batch, &mut batch_bytes_acc, batch_bytes, record_format);

    // A MemBlock segment already owns its normal capacity. Rotate before a
    // batch would grow it. An oversized batch can fill a fresh segment.
    if matches!(backend.as_ref(), IoBackend::MemBlock(_))
        && segment.record_count != 0
        && batch_bytes_acc as u64 > segment_size.saturating_sub(segment.len())
    {
        if let Err(error) = rotate_segment(
            segment,
            backend,
            pipeline_path,
            next_segment_id,
            group_id,
            record_format,
            index,
            pipeline_idx,
        )
        .await
        {
            failed.store(true, Ordering::Release);
            fail_batch(batch, &error);
            drain_and_fail_all(rx).await;
            return false;
        }
    }

    // Capture segment_id before potential rotation.
    let segment_id = segment.segment_id;

    let write_result = write_batch(
        segment,
        batch,
        offsets,
        skip_fsync,
        fsync_summary,
        write_bandwidth,
    )
    .await;

    match write_result {
        Ok(()) => {
            flush_count.fetch_add(1, Ordering::Relaxed);
            records_flushed.fetch_add(batch.len() as u64, Ordering::Relaxed);
            update_index_for_batch(index, batch, offsets, pipeline_idx, segment_id);

            if segment.is_full(segment_size) {
                if let Err(e) = rotate_segment(
                    segment,
                    backend,
                    pipeline_path,
                    next_segment_id,
                    group_id,
                    record_format,
                    index,
                    pipeline_idx,
                )
                .await
                {
                    failed.store(true, Ordering::Release);
                    fail_batch(batch, &e);
                    drain_and_fail_all(rx).await;
                    return false;
                }
            }

            resolve_batch_acks(batch, offsets, pipeline_idx, segment_id);

            if !process_pending_seal_acks(
                segment,
                &mut pending_seal_acks,
                rx,
                backend,
                pipeline_path,
                next_segment_id,
                group_id,
                record_format,
                index,
                pipeline_idx,
                failed,
            )
            .await
            {
                for ack in pending_flush_acks {
                    let _ = ack.send(Err(io::Error::other("WAL disk failed")));
                }
                return false;
            }
            for ack in pending_flush_acks {
                let result = segment.flush().await;
                if result.is_err() {
                    failed.store(true, Ordering::Release);
                }
                let _ = ack.send(result);
            }
            true
        }
        Err(e) => {
            failed.store(true, Ordering::Release);
            fail_batch(batch, &e);
            drain_and_fail_all(rx).await;
            false
        }
    }
}

/// Park until a command arrives or the watchdog fires. Returns `None` when
/// the channel is closed (engine dropped), after sealing and flushing. On a
/// watchdog wakeup with no queued command, re-parks and waits again.
async fn recv_command(
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    watchdog: Duration,
    segment: &mut WalSegment,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    watchdog_wakeups: &Arc<AtomicU64>,
) -> Option<WriterCommand> {
    loop {
        match timeout(watchdog, rx.recv()).await {
            Ok(Some(cmd)) => return Some(cmd),
            Ok(None) => {
                let _ = segment.seal().await;
                let _ = segment.flush().await;
                register_sealed(index, segment, pipeline_idx);
                return None;
            }
            Err(_) => {
                watchdog_wakeups.fetch_add(1, Ordering::Relaxed);
                if let Ok(cmd) = rx.try_recv() {
                    return Some(cmd);
                }
            }
        }
    }
}

/// Handle a Seal command: seal the current segment, register it, open a new
/// one, and resolve the ack. Returns `false` if a fatal error occurred.
#[allow(clippy::too_many_arguments)]
async fn handle_seal_command(
    segment: &mut WalSegment,
    ack: oneshot::Sender<io::Result<()>>,
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    next_segment_id: &Arc<AtomicU64>,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    failed: &Arc<AtomicBool>,
) -> bool {
    let result = segment.seal().await;
    if result.is_err() {
        failed.store(true, Ordering::Release);
    } else {
        register_sealed(index, segment, pipeline_idx);
    }
    if result.is_ok() {
        match create_segment(backend, pipeline_path, next_segment_id, group_id, record_format).await {
            Ok(new_seg) => *segment = new_seg,
            Err(e) => {
                failed.store(true, Ordering::Release);
                error!(pipeline_idx, error = %e, "failed to create new segment after seal");
                let _ = ack.send(Err(e));
                drain_and_fail_all(rx).await;
                return false;
            }
        }
    }
    let _ = ack.send(result);
    true
}

/// Drain all commands already queued in the channel, accumulating writes into
/// `batch` and collecting any seal/flush acks. Returns the pending seal and
/// flush acks that must be processed after the batch is flushed. At most one
/// of the two vectors is non-empty (the first Seal/Flush causes a break).
fn drain_pending_commands(
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    batch: &mut Vec<PendingWrite>,
    batch_bytes_acc: &mut usize,
    batch_bytes: usize,
    record_format: WalRecordFormat,
) -> (Vec<WriterAck>, Vec<WriterAck>) {
    let mut pending_seal_acks: Vec<WriterAck> = Vec::new();
    let mut pending_flush_acks: Vec<WriterAck> = Vec::new();
    loop {
        match rx.try_recv() {
            Ok(WriterCommand::Write(req)) => {
                let req = req.encode(record_format);
                *batch_bytes_acc += req.encoded.total_len();
                batch.push(req);
                if *batch_bytes_acc >= batch_bytes {
                    break;
                }
            }
            Ok(WriterCommand::Seal { ack }) => {
                pending_seal_acks.push(ack);
                break;
            }
            Ok(WriterCommand::Flush { ack }) => {
                pending_flush_acks.push(ack);
                break;
            }
            Err(_) => break,
        }
    }
    (pending_seal_acks, pending_flush_acks)
}

/// Insert index entries for every non-metadata record in the batch.
fn update_index_for_batch(
    index: &Arc<ShardedSegmentIndex>,
    batch: &[PendingWrite],
    offsets: &[u64],
    pipeline_idx: usize,
    segment_id: u64,
) {
    for (i, req) in batch.iter().enumerate() {
        if req.slot != 0 {
            index.insert(
                pipeline_idx,
                req.slot,
                SlotLocation {
                    disk_idx: pipeline_idx,
                    segment_id,
                    file_offset: offsets[i],
                },
            );
        }
    }
}

/// Seal the current segment, register it, and open the next segment.
/// Returns `Err` if sealing or creating the new segment fails.
#[allow(clippy::too_many_arguments)]
async fn rotate_segment(
    segment: &mut WalSegment,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    next_segment_id: &Arc<AtomicU64>,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
) -> io::Result<()> {
    segment.seal().await?;
    register_sealed(index, segment, pipeline_idx);
    *segment = create_segment(backend, pipeline_path, next_segment_id, group_id, record_format).await?;
    Ok(())
}

/// Send each pending write its resolved `SlotLocation`.
fn resolve_batch_acks(batch: &mut Vec<PendingWrite>, offsets: &[u64], pipeline_idx: usize, segment_id: u64) {
    for (i, req) in batch.drain(..).enumerate() {
        match req.completion {
            WriteCompletion::Ack(ack) => {
                let _ = ack.send(Ok(SlotLocation {
                    disk_idx: pipeline_idx,
                    segment_id,
                    file_offset: offsets[i],
                }));
            }
            WriteCompletion::Early(Some((started, summary))) => {
                #[allow(clippy::cast_possible_truncation)]
                summary.observe(started.elapsed().as_nanos() as u64);
            }
            WriteCompletion::Early(None) => {}
        }
    }
}

/// Process pending seal acks collected during the wake-drain phase.
/// Each seal ack triggers a segment seal + new-segment open cycle.
/// Returns `false` if a fatal error occurred and the writer loop should exit.
#[allow(clippy::too_many_arguments)]
async fn process_pending_seal_acks(
    segment: &mut WalSegment,
    pending_seal_acks: &mut Vec<oneshot::Sender<io::Result<()>>>,
    rx: &mut mpsc::UnboundedReceiver<WriterCommand>,
    backend: &Arc<IoBackend>,
    pipeline_path: &std::path::Path,
    next_segment_id: &Arc<AtomicU64>,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
    index: &Arc<ShardedSegmentIndex>,
    pipeline_idx: usize,
    failed: &Arc<AtomicBool>,
) -> bool {
    for seal_ack in pending_seal_acks.drain(..) {
        let result = segment.seal().await;
        if result.is_err() {
            failed.store(true, Ordering::Release);
        } else {
            register_sealed(index, segment, pipeline_idx);
        }
        if result.is_ok() {
            match create_segment(backend, pipeline_path, next_segment_id, group_id, record_format).await {
                Ok(new_seg) => *segment = new_seg,
                Err(e) => {
                    failed.store(true, Ordering::Release);
                    let _ = seal_ack.send(Err(e));
                    drain_and_fail_all(rx).await;
                    return false;
                }
            }
        }
        let _ = seal_ack.send(result);
    }
    true
}

/// Maximum number of `IoSlice` entries per `writev` call. Linux `IOV_MAX` is
/// 1024; macOS is the same. We use a conservative constant to avoid platform
/// probes at runtime.
const MAX_IOV: usize = 1024;

/// Write a batch of encoded records to the segment in vectored writes,
/// then `fdatasync`. Fills `offsets` with the file offset of each record in
/// the batch. The caller-owned `offsets` Vec is cleared and reused across
/// batches to avoid per-batch heap allocation.
async fn write_batch(
    segment: &mut WalSegment,
    batch: &[PendingWrite],
    offsets: &mut Vec<u64>,
    skip_fsync: bool,
    fsync_summary: &OnceLock<Arc<LatencySummary>>,
    write_bandwidth: &OnceLock<Arc<Bandwidth>>,
) -> io::Result<()> {
    if batch.is_empty() {
        return Ok(());
    }

    // Compute per-record offsets from the current segment tail.
    offsets.clear();
    offsets.reserve(batch.len());
    let base_offset = segment.len();
    let mut cur = base_offset;
    let mut slice_count = 0;
    for req in batch {
        offsets.push(cur);
        cur += req.encoded.total_len() as u64;
        slice_count += req.encoded.io_slice_count();
    }
    let total_len = cur - base_offset;

    // Retain whole records in each submission. This also bounds the
    // descriptor allocation when a batch needs multiple writes.
    let split = slice_count > MAX_IOV;
    let mut io_slices = Vec::with_capacity(slice_count.min(MAX_IOV));
    for req in batch {
        if split && io_slices.len() + req.encoded.io_slice_count() > MAX_IOV {
            segment.write_raw_vectored(&io_slices).await?;
            io_slices.clear();
        }
        req.encoded.append_io_slices(&mut io_slices);
    }
    segment.write_raw_vectored(&io_slices).await?;

    // Update segment metadata (min/max slot, record_count).
    for req in batch {
        if req.slot != 0 {
            if req.slot < segment.min_slot {
                segment.min_slot = req.slot;
            }
            if req.slot > segment.max_slot {
                segment.max_slot = req.slot;
            }
        }
        segment.record_count += 1;
        segment.max_term = segment.max_term.max(req.term);
        if req.is_vote {
            segment.vote_term = Some(segment.vote_term.map_or(req.term, |term| term.max(req.term)));
        }
    }

    // Single durable flush for the whole batch — skipped when skip_fsync is
    // set (benchmark isolation mode: data written but not durably flushed).
    if !skip_fsync {
        let fsync_start = Instant::now();
        segment.fdatasync().await?;
        if let Some(s) = fsync_summary.get() {
            #[allow(clippy::cast_possible_truncation)]
            s.observe(fsync_start.elapsed().as_nanos() as u64);
        }
    }

    // Observe batch write bytes (regardless of fsync — the write happened).
    if let Some(bw) = write_bandwidth.get() {
        #[allow(clippy::cast_possible_truncation)]
        bw.observe(total_len);
    }

    trace!(
        segment_id = segment.segment_id,
        records = batch.len(),
        bytes = total_len,
        "batch written and flushed"
    );

    Ok(())
}

/// Create a new segment, allocating the next segment id.
async fn create_segment(
    backend: &IoBackend,
    path: &std::path::Path,
    next_segment_id: &AtomicU64,
    group_id: PxGroupId,
    record_format: WalRecordFormat,
) -> io::Result<WalSegment> {
    let seg_id = next_segment_id.fetch_add(1, Ordering::Relaxed);
    WalSegment::create_with_format(backend, path, seg_id, group_id, record_format).await
}

/// Register a sealed segment in the index.
fn register_sealed(index: &ShardedSegmentIndex, segment: &WalSegment, pipeline_idx: usize) {
    let meta = SegmentMeta {
        segment_id: segment.segment_id,
        disk_idx: pipeline_idx,
        min_slot: segment.min_slot,
        max_slot: segment.max_slot,
        record_count: segment.record_count,
        max_term: segment.max_term,
        vote_term: segment.vote_term,
    };
    index.register_segment(pipeline_idx, meta);
    debug!(
        segment_id = segment.segment_id,
        min_slot = segment.min_slot,
        max_slot = segment.max_slot,
        "segment sealed"
    );
}

/// Fail all pending writes in a batch with the given error. Drains the
/// batch Vec in place so the allocation can be reused across wake cycles.
fn fail_batch(batch: &mut Vec<PendingWrite>, e: &io::Error) {
    let kind = e.kind();
    for req in batch.drain(..) {
        req.completion
            .fail(req.slot, io::Error::new(kind, "WAL durable flush failed"));
    }
}

/// Reject new commands and fail every command already accepted by the channel.
async fn drain_and_fail_all(rx: &mut mpsc::UnboundedReceiver<WriterCommand>) {
    rx.close();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            WriterCommand::Write(req) => {
                let (slot, completion) = match req {
                    WriteRequest::Encoded(req) => (req.slot, req.completion),
                    WriteRequest::EarlyAccepted { record, completion } => (record.slot, completion),
                };
                completion.fail(slot, io::Error::other("WAL disk failed"));
            }
            WriterCommand::Seal { ack } | WriterCommand::Flush { ack } => {
                let _ = ack.send(Err(io::Error::other("WAL disk failed")));
            }
        }
    }
}
