// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::local_replica::PxLocalReplica;
use crate::paxos::roles::{Acceptor, PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::paxos::PxTerm;
use crate::wal::record::WALRecord;
use tracing::debug;

impl PxLocalReplica {
    /// Phase-1 `Prepare` handler with election-term fence.
    ///
    /// Two-fence rule (term fencing + acceptor ballot fencing):
    /// - `req.term < current_term` → `PxPrepareReply::TermStale { new_term }`.
    /// - `req.term > current_term` → adopt via [`PxLocalReplica::become_follower`], then
    ///   forward to the acceptor (this replica is now in the new term).
    /// - `req.term == current_term` → forward to the acceptor unchanged.
    pub async fn on_prepare(&self, slot: u64, ballot: PxBallot, term: PxTerm) -> PxPrepareReply {
        let local_term = self.current_term_snapshot();
        if term < local_term {
            return PxPrepareReply::TermStale {
                slot,
                new_term: local_term,
            };
        }
        if term > local_term {
            self.become_follower(term);
        }
        let reply = self.acceptor.prepare(slot, ballot).await;

        // Ack contract (W6): persist Promised record before replying.
        if matches!(reply, PxPrepareReply::Promised { .. }) {
            if let Some(wal) = &self.wal {
                let record = WALRecord::from_promised(wal.group_id(), term, slot, ballot);
                if let Err(e) = wal.append(&record).await {
                    tracing::error!(slot, ?ballot, error = %e, "WAL persist Promised failed");
                }
            }
        }

        reply
    }

    /// Phase-2 `Accept` handler with election-term fence.
    ///
    /// Same two-fence rule as [`PxLocalReplica::on_prepare`] but the term lives on
    /// `entry.term` (because the accept message carries the value).
    pub async fn on_accept(&self, entry: &PxLogEntry) -> PxAcceptReply {
        let reply = self.on_accept_inner(entry).await;
        if matches!(reply, PxAcceptReply::Accepted { .. }) {
            self.on_accept_persist(entry).await;
        }
        reply
    }

    /// Acceptor CAS only — term fence + `acceptor.accept`, no WAL persist.
    /// Returns the reply immediately. Used by R16b early-ack path where the
    /// proposer tracks WAL persist separately from quorum.
    pub async fn on_accept_inner(&self, entry: &PxLogEntry) -> PxAcceptReply {
        let req_term = entry.term;
        let local_term = self.current_term_snapshot();
        if req_term < local_term {
            return PxAcceptReply::TermStale {
                slot: entry.slot,
                new_term: local_term,
            };
        }
        if req_term > local_term {
            self.become_follower(req_term);
        }

        let reply = self.acceptor.accept(entry).await;
        if matches!(reply, PxAcceptReply::Accepted { .. }) {
            debug!(
                replica = self.id,
                slot = entry.slot,
                round = entry.ballot.round,
                leader_id = entry.ballot.leader_id,
                term = entry.term,
                "on_accept_inner: accepted leader proposal"
            );
        }
        reply
    }

    /// WAL persist of an Accepted record. Called after [`PxLocalReplica::on_accept_inner`]
    /// when the reply is `Accepted`. In the default path this completes before
    /// the reply is observed by the proposer (W6 contract). In the R16b
    /// early-ack path it runs concurrently with remote RPC fan-out.
    pub async fn on_accept_persist(&self, entry: &PxLogEntry) {
        if let Some(wal) = &self.wal {
            let record = WALRecord::from_accepted(wal.group_id(), entry);
            if let Err(e) = wal.append(&record).await {
                tracing::error!(
                    slot = entry.slot,
                    ballot = ?entry.ballot,
                    error = %e,
                    "WAL persist Accepted failed"
                );
            }
        }
        // The value may fill a known chosen gap while its apply loop is asleep.
        // Acceptance alone does not establish chosen-ness or advance a frontier.
        if self.learner.engine().requires_contiguous_apply()
            && (entry.slot <= self.known_commit_slot.load(std::sync::atomic::Ordering::Acquire)
                || self.learner.is_chosen(entry.slot))
        {
            self.wake_apply_loop();
        }
    }

    /// Hand the local Accepted record directly to its WAL pipeline.
    pub(super) fn enqueue_accept_persist(&self, entry: &PxLogEntry) {
        let Some(wal) = &self.wal else { return };
        let record = WALRecord::from_accepted(wal.group_id(), entry);
        #[cfg(feature = "test-util")]
        if let Some(gate) = self.persist_gate.lock().clone() {
            let wal = wal.clone();
            tokio::spawn(async move {
                gate.notified().await;
                enqueue_accepted(&wal, record);
            });
            return;
        }
        enqueue_accepted(wal, record);
    }
}

fn enqueue_accepted(wal: &crate::wal::wal_engine::WalEngine, record: WALRecord) {
    let slot = record.slot;
    let ballot = record.ballot;
    if let Err(error) = wal.enqueue_accepted(record) {
        tracing::error!(slot, ?ballot, %error, "WAL enqueue Accepted failed (early ack)");
    }
}
