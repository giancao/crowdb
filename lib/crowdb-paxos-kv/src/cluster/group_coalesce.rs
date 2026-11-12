// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::too_many_lines, clippy::cast_possible_truncation)]

use std::sync::{Arc, Weak};

use bytes::Bytes;

use crate::cluster::group::{PendingBatch, ProposeResult, PxGroup, RemoteReplicaKind};
use crate::paxos::roles::{ProposalPayload, RequestIdentity};
use tracing::{info_span, Instrument};

/// Coalescer watchdog interval in microseconds. The watchdog sleeps for
/// this duration, then flushes any stuck non-empty batch. Full batches
/// still flush immediately; this bounds the tail when low-rate control
/// traffic keeps arriving after the main workload stops.
const WATCHDOG_US: u64 = 1_000_000;

impl PendingBatch {
    fn append_payload(&mut self, payload: Bytes) {
        if payload.len() < 2 || payload.as_ref() == [0, 0] {
            return;
        }
        if self.first_payload.is_empty() {
            self.encoded_bytes = payload.len();
            self.first_payload = payload;
        } else {
            self.encoded_bytes += payload.len() - 2;
            self.payloads.push(payload);
        }
    }

    fn take_payload(&mut self) -> ProposalPayload {
        let first = std::mem::take(&mut self.first_payload);
        let first = if first.is_empty() {
            Bytes::from_static(&[0, 0])
        } else {
            first
        };
        ProposalPayload::coalesced(first, std::mem::take(&mut self.payloads), self.op_count)
    }
}

impl PxGroup {
    fn rdma_accept_payload_target(&self) -> Option<usize> {
        self.remote_replicas
            .iter()
            .filter_map(|replica| {
                let RemoteReplicaKind::Real(remote) = replica else {
                    return None;
                };
                remote
                    .rpc_transport()?
                    .rdma_accept_payload_target(remote.endpoint_str().as_str())
            })
            .min()
    }

    /// Start the single long-running watchdog task if not already running.
    /// Called lazily on first enqueue. The watchdog sleeps for
    /// `WATCHDOG_US`, then flushes any non-empty pending batch.
    fn coalesce_start_watchdog(&self) {
        let Some(weak) = self.self_weak.get().cloned() else {
            return;
        };
        let g = self.group_id;
        let replica = self.local_replica().id;
        let span = self.log_store_id().map_or_else(
            || info_span!("coalesce_watchdog", g, replica),
            |s| info_span!("coalesce_watchdog", s, g, replica),
        );
        let cancel = self.tenure_cancel.clone();
        self.coalesce_watchdog_handle.get_or_init(|| {
            Arc::new(tokio::sync::Mutex::new(Some(tokio::spawn(
                async move {
                    loop {
                        tokio::select! {
                            () = cancel.cancelled() => break,
                            () = tokio::time::sleep(std::time::Duration::from_micros(WATCHDOG_US)) => {}
                        }
                        let Some(group) = weak.upgrade() else { break };
                        // Atomically swap the batch for a fresh empty one so
                        // no op can sneak in and start a 1-op round during
                        // the swap.
                        let batch = group.coalesce_take_pending();
                        let Some(batch) = batch else { continue };
                        if !batch.waiters.is_empty() {
                            tracing::debug!(
                                g = group.group_id,
                                op_count = batch.op_count,
                                "coalescer watchdog flushing pending batch"
                            );
                            group.coalesce_spawn_round(batch);
                        }
                        // Empty batch — just drop it (go idle).
                    }
                }
                .instrument(span),
            ))))
        });
    }

    /// Spawn a Paxos round for the given batch. The caller must have
    /// already swapped in a fresh `PendingBatch` to the coalescer (so
    /// new ops accumulate for the next round). This function only
    /// builds the payload and spawns the round task.
    fn coalesce_spawn_round(&self, mut batch: PendingBatch) {
        if self.tenure_cancel.is_cancelled() || self.inflight.closed() {
            for waiter in batch.waiters {
                let _ = waiter.send(ProposeResult::Err("coalescer shutting down".to_string()));
            }
            return;
        }
        let payload = batch.take_payload();
        let identities = batch.identities;
        let waiters = batch.waiters;
        let Some(group) = self.self_weak.get().and_then(Weak::upgrade) else {
            for waiter in waiters {
                let _ = waiter.send(ProposeResult::Err("group dropped".to_string()));
            }
            return;
        };
        group
            .coalesced_rounds_inflight
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        tokio::spawn(
            async move {
                #[cfg(feature = "test-util")]
                group.coalesce_await_round_gate().await;
                let result = group.propose_inner(payload, &identities).await;
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
                group
                    .coalesced_rounds_inflight
                    .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                group.coalesce_drain_after_round();
            }
            .instrument(tracing::Span::current()),
        );
    }

    /// Enqueue one atomic request into the coalescer.
    ///
    /// - Idle (no batch): start a 1-op round immediately. Open a pending
    ///   batch for concurrent ops.
    /// - Batch exists: append to the pending batch. If the batch fills to
    ///   `max_keys`, flush it as a concurrent round.
    ///
    /// A watchdog periodically drains a pending batch.
    pub(crate) async fn coalesce_enqueue(
        &self,
        mut payload: Bytes,
        identity: Option<RequestIdentity>,
    ) -> ProposeResult {
        if self.tenure_cancel.is_cancelled() {
            return ProposeResult::Err("coalescer shutting down".to_string());
        }
        let (tx, rx) = tokio::sync::oneshot::channel();
        let request_op_count = payload
            .get(..2)
            .map_or(0, |count| u16::from_le_bytes([count[0], count[1]]));
        let max_keys = self.coalesce_max_keys.load(std::sync::atomic::Ordering::Relaxed);
        self.coalesce_start_watchdog();

        let (preceding_round, start_round) = {
            let mut guard = self.coalescer.lock();
            match &mut *guard {
                Some(batch)
                    if u32::from(batch.op_count) + u32::from(request_op_count) <= u32::from(max_keys) =>
                {
                    let target = if batch.waiters.is_empty() {
                        self.rdma_accept_payload_target()
                    } else {
                        batch.rdma_payload_target
                    };
                    let preceding = target
                        .filter(|_| batch.encoded_bytes != 0 && payload.len() > 2)
                        .filter(|target| batch.encoded_bytes.saturating_add(payload.len() - 2) > *target)
                        .map(|_| std::mem::take(batch));
                    batch.rdma_payload_target = target;
                    batch.append_payload(payload);
                    batch.op_count = batch.op_count.saturating_add(request_op_count);
                    if let Some(identity) = identity {
                        batch.identities.push(identity);
                    }
                    batch.waiters.push(tx);
                    let ready = batch.op_count >= max_keys
                        || target.is_some_and(|target| batch.encoded_bytes >= target);
                    let next = if ready {
                        // Batch full: flush as a concurrent round.
                        Some(std::mem::take(batch))
                    } else {
                        None
                    };
                    (preceding, next)
                }
                pending => {
                    // Start immediately when idle, or run this request separately
                    // if appending it would exceed the existing batch's limit.
                    if pending.is_none() {
                        *pending = Some(PendingBatch::default());
                        if payload.len() < 2 {
                            payload = Bytes::from_static(&[0, 0]);
                        }
                    }
                    (
                        None,
                        Some(PendingBatch {
                            first_payload: payload,
                            identities: identity.into_iter().collect(),
                            waiters: vec![tx],
                            ..PendingBatch::default()
                        }),
                    )
                }
            }
        };

        for batch in [preceding_round, start_round].into_iter().flatten() {
            self.coalesce_spawn_round(batch);
        }
        match rx.await {
            Ok(result) => result,
            Err(_) => ProposeResult::Err("coalescer round dropped".to_string()),
        }
    }

    /// Called after a coalesced round completes. Drains the pending
    /// batch (ops that accumulated during the round). If non-empty,
    /// flushes it as the next round immediately. If empty, goes idle
    /// (coalescer → `None`) so the next op starts a 1-op round — the
    /// zero-latency-floor behavior at low load.
    ///
    /// R45b drain threshold: if the number of other coalesced rounds is at or
    /// above `coalesce_drain_threshold`, skip the drain. The `max_keys`
    /// overflow path handles high load with full batches, and the last
    /// coalesced finisher drains the shared pending batch. Conditional and
    /// tenure-bound proposals do not participate in this decision.
    ///
    /// The swap is atomic: the old batch is taken and a fresh empty
    /// batch is put back in a single locked section, so no concurrent
    /// `coalesce_enqueue` can see `None` and start a 1-op round that
    /// would fragment the batch.
    fn coalesce_drain_after_round(&self) {
        if self.tenure_cancel.is_cancelled() {
            if let Some(batch) = self.coalesce_take_pending() {
                for waiter in batch.waiters {
                    let _ = waiter.send(ProposeResult::Err("coalescer shutting down".to_string()));
                }
            }
            return;
        }
        let threshold = self.config.paxos.coalesce_drain_threshold;
        if threshold > 0
            && self
                .coalesced_rounds_inflight
                .load(std::sync::atomic::Ordering::Acquire)
                >= u64::try_from(threshold).unwrap_or(u64::MAX)
        {
            return;
        }
        let batch = self.coalesce_take_pending();
        let Some(batch) = batch else { return };
        if batch.waiters.is_empty() {
            #[cfg(feature = "test-util")]
            self.coalesce_pause_empty_drain_for_tests();
            // The queue became idle atomically while taking this empty batch.
            // A subsequent enqueue owns the new queue and must never be reset
            // by this finisher. Other active rounds retain their empty batch.
            return;
        }
        // Flush the accumulated batch as the next round immediately.
        self.coalesce_spawn_round(batch);
    }

    /// Remove pending work and choose the next queue state under one guard.
    fn coalesce_take_pending(&self) -> Option<PendingBatch> {
        let mut guard = self.coalescer.lock();
        let batch = guard.take()?;
        if !batch.waiters.is_empty()
            || self
                .coalesced_rounds_inflight
                .load(std::sync::atomic::Ordering::Acquire)
                > 0
        {
            *guard = Some(PendingBatch::default());
        }
        Some(batch)
    }

    /// Test-only: await the coalesce round gate if set. Consumed by the
    /// first round that runs after the gate is installed.
    #[cfg(feature = "test-util")]
    pub(crate) async fn coalesce_await_round_gate(&self) {
        let gate = self.coalesce_round_gate.lock().take();
        if let Some(gate) = gate {
            let _ = gate.await;
        }
    }

    #[cfg(feature = "test-util")]
    fn coalesce_pause_empty_drain_for_tests(&self) {
        use std::sync::atomic::Ordering;
        if self
            .coalesce_empty_drain_gate
            .compare_exchange(1, 2, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            while self.coalesce_empty_drain_gate.load(Ordering::Acquire) == 2 {
                std::thread::yield_now();
            }
        }
    }
}
