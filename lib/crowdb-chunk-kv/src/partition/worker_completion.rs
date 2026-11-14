// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use futures::{
    future::{join_all, BoxFuture, Shared},
    FutureExt,
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use tokio::sync::{mpsc, oneshot};

use super::{run_worker, Partition, WorkerRequest, WorkerState};
use crate::{ChunkKvError, Result};

pub(super) type WorkerCompletion = Shared<BoxFuture<'static, Result<()>>>;

pub(super) fn spawn(state: WorkerState, receiver: mpsc::Receiver<WorkerRequest>) -> WorkerCompletion {
    let (completed, completion) = oneshot::channel();
    tokio::spawn(async move {
        run_worker(state, receiver).await;
        let _ = completed.send(());
    });
    async move { completion.await.map_err(|_| ChunkKvError::WriteStalled) }
        .boxed()
        .shared()
}

impl Partition {
    /// Closes all hosted mutation workers and the native Trees they share.
    /// The host must first stop admitting work and release other serving handles.
    /// A canceled waiter leaves the accepted shutdown running on the host.
    ///
    /// # Errors
    /// Reports the first worker or Tree close failure after all groups finish.
    pub async fn close_hosted(mut partitions: Vec<Self>) -> Result<()> {
        let groups: Vec<_> = {
            let mut groups = HashMap::new();
            let mut visited = HashSet::new();
            while let Some(partition) = partitions.pop() {
                // An installed split can still own a writer only through its dispatcher.
                if visited.insert(Arc::as_ptr(&partition.split_ingress)) {
                    if let Some(ingress) = partition.split_ingress() {
                        partitions.push(ingress.retained_parent);
                        partitions.push(ingress.child);
                    }
                }
                let key = Arc::as_ptr(&partition.tree).cast::<()>();
                let (_, workers) = groups
                    .entry(key)
                    .or_insert_with(|| (Arc::clone(&partition.tree), Vec::new()));
                workers.push(partition.worker_completion.clone());
            }
            // Drop all the collected senders before joining any worker.
            groups.into_values().collect()
        };
        tokio::spawn(async move {
            let results = join_all(groups.into_iter().map(|(tree, workers)| async move {
                let worker_error = join_all(workers).await.into_iter().find_map(Result::err);
                let closed = tree.close_hosted().await;
                worker_error.map_or(closed, Err)
            }))
            .await;
            results.into_iter().find_map(Result::err).map_or(Ok(()), Err)
        })
        .await
        .map_err(|error| ChunkKvError::MaintenanceDegraded(error.to_string()))?
    }

    /// Consumes this handle and waits until the mutation worker releases its
    /// Tree and WAL owners. Other owning partition clones must also be dropped.
    /// If this is the last writer owner, waits for the native Tree close too.
    /// Complete the transfer or shutdown protocol before joining a serving owner;
    /// this join does not change fencing, checkpoints, or repair state.
    ///
    /// # Errors
    /// Returns a mutation worker or native Tree close error.
    pub async fn close(self) -> Result<()> {
        let completion = self.worker_completion.clone();
        let tree = self.tree.clone();
        drop(self);
        let worker = completion.await;
        let closed = tree.close_if_last().await;
        worker.and(closed)
    }
}
