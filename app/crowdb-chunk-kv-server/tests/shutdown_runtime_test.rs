// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/sampling_tree.rs"]
mod sampling_tree;
#[path = "common/split_storage.rs"]
mod split_storage;
#[path = "common/process.rs"]
mod test_process;

use std::{
    future::{poll_fn, Future},
    process::Command,
    sync::{atomic::Ordering, Arc, Barrier},
    task::Poll,
    time::Duration,
};

use crowdb_chunk_kv::{CrowdbPartitionTree, Partition, PartitionId, PartitionLifecycle, PartitionRange};
use crowdb_chunk_kv_server::ChunkKvService;
use crowdb_tree_ffi::chunk::test_util::TestChunkBackend;
use sampling_tree::TestSamplingTree;
use split_storage::TestSplitStorage;

const CHILD: &str = "CROWDB_TEST_SAMPLING_SHUTDOWN_CHILD";

#[tokio::test(flavor = "current_thread")]
async fn hosted_close_joins_both_workers_and_releases_their_shared_native_writer() {
    let storage = TestSplitStorage::new();
    let mut backend = TestChunkBackend::new(71, 1).unwrap();
    let mut config = backend.config();
    config.page_store = Some(backend.page_store(0).unwrap());
    let tree = Arc::new(CrowdbPartitionTree::open(71, &config).unwrap());
    let service = ChunkKvService::new(test_process::TestProcess::authority(1), 8).unwrap();
    let mut held = None;
    for id in [1, 2] {
        let (partition, _) = storage
            .partition(
                PartitionId { high: 71, low: id },
                PartitionRange::default(),
                1,
                tree.clone(),
            )
            .await;
        service.install_partition(&partition).unwrap();
        if id == 2 {
            held = Some(partition);
        }
    }
    drop(tree);
    service.drain_writers().await.unwrap();
    let mut closing = std::pin::pin!(service.release_hosted_trees());
    assert!(tokio::time::timeout(Duration::from_millis(20), closing.as_mut())
        .await
        .is_err());
    assert_eq!(backend.runtime.snapshot().unwrap().bindings, 1);
    drop(held);
    tokio::time::timeout(Duration::from_secs(5), closing)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(backend.runtime.snapshot().unwrap().bindings, 0);
    drop(config);
    backend.next_writer().unwrap();
}

#[test]
fn service_drain_owns_sampling_until_runtime_shutdown() {
    isolated_child(
        "service_drain_owns_sampling_until_runtime_shutdown",
        run_shutdown_child,
    );
}

fn isolated_child(name: &str, run: fn()) {
    if std::env::var_os(CHILD).is_some() {
        run();
        return;
    }
    let output = Command::new("pixi")
        .arg("run")
        .args(["timeout", "--signal=KILL", "20s"])
        .arg(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD, "1")
        .env("RUST_BACKTRACE", "full")
        .output()
        .expect("run isolated shutdown regression");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "shutdown child {name} failed: {}\nstdout:\n{stdout}\nstderr:\n{stderr}",
        output.status
    );
    assert!(
        !stderr.contains("but it is being shutdown"),
        "timer polled after shutdown:\n{stderr}"
    );
}

fn run_shutdown_child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let escaped = runtime.block_on(async {
        let storage = TestSplitStorage::new();
        let tree = Arc::new(TestSamplingTree::new());
        let (partition, _) = storage
            .partition(
                PartitionId { high: 71, low: 1 },
                PartitionRange::default(),
                1,
                tree.clone(),
            )
            .await;
        let service = ChunkKvService::new(test_process::TestProcess::authority(1), 8).unwrap();
        service.install_partition(&partition).unwrap();
        service
            .registry_observation_with_load_samples(4096, 0, 2)
            .await
            .unwrap();
        tree.entered.notified().await;
        println!("production sampler registered its timer");

        service.begin_drain();
        // Finish the real journal release first, so unrelated writer scheduling
        // cannot make a WAL-only service drain look like a sampler barrier.
        partition.suspend_for_shutdown(1).await.unwrap();
        let mut drain = std::pin::pin!(service.drain_writers());
        let first = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx))).await;
        match first {
            Poll::Ready(result) => {
                result.unwrap();
                assert!(!tree.finished.load(Ordering::Acquire));
                println!("service drain returned with the production sampling read still active");
                true
            }
            Poll::Pending => {
                tree.release.notify_one();
                drain.await.unwrap();
                assert!(tree.finished.load(Ordering::Acquire));
                assert_eq!(
                    partition.snapshot().lifecycle,
                    PartitionLifecycle::TransferQuiesced
                );
                false
            }
        }
    });
    drop(runtime);
    assert!(
        !escaped,
        "service drain completed before its sampling job; runtime shutdown overtook owned storage work"
    );
}

fn sampling_service(
    runtime: &tokio::runtime::Runtime,
) -> (Arc<ChunkKvService>, Partition, Arc<TestSamplingTree>) {
    runtime.block_on(async {
        let storage = TestSplitStorage::new();
        let tree = Arc::new(TestSamplingTree::new());
        let (partition, _) = storage
            .partition(
                PartitionId { high: 71, low: 1 },
                PartitionRange::default(),
                1,
                tree.clone(),
            )
            .await;
        let service = Arc::new(ChunkKvService::new(test_process::TestProcess::authority(1), 8).unwrap());
        service.install_partition(&partition).unwrap();
        (service, partition, tree)
    })
}

#[test]
fn observation_after_drain_admission_cannot_launch_sampling() {
    isolated_child(
        "observation_after_drain_admission_cannot_launch_sampling",
        run_closed_observation_child,
    );
}

fn run_closed_observation_child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (service, partition, tree) = sampling_service(&runtime);
    // Keep the partition Serving: fencing its WAL first would let the old
    // sampler skip it for an unrelated reason and conceal missing admission.
    assert_eq!(partition.snapshot().lifecycle, PartitionLifecycle::Serving);
    service.begin_drain();
    runtime
        .block_on(service.registry_observation_with_load_samples(4096, 0, 2))
        .unwrap();
    drop(runtime);
    assert_eq!(tree.started.load(Ordering::Acquire), 0);
}

#[test]
fn concurrent_observation_and_close_cannot_escape_completed_drain() {
    isolated_child(
        "concurrent_observation_and_close_cannot_escape_completed_drain",
        run_concurrent_close_child,
    );
}

fn run_concurrent_close_child() {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .unwrap();
    let (service, partition, tree) = sampling_service(&runtime);
    let barrier = Arc::new(Barrier::new(3));
    let observer = {
        let service = service.clone();
        let barrier = barrier.clone();
        let handle = runtime.handle().clone();
        std::thread::spawn(move || {
            let _entered = handle.enter();
            barrier.wait();
            handle
                .block_on(service.registry_observation_with_load_samples(4096, 0, 2))
                .unwrap();
        })
    };
    let closer = {
        let service = service.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            service.begin_drain();
        })
    };
    barrier.wait();
    observer.join().unwrap();
    closer.join().unwrap();
    runtime.block_on(async {
        // Remove WAL-release scheduling as a competing reason to be Pending.
        partition.suspend_for_shutdown(1).await.unwrap();
        let mut drain = std::pin::pin!(service.drain_writers());
        if tree.active.load(Ordering::Acquire) {
            let first = poll_fn(|cx| Poll::Ready(drain.as_mut().poll(cx))).await;
            assert!(first.is_pending(), "drain escaped an active sampling read");
        }
        // Also releases a read that passed lifecycle validation just before
        // fencing but has not yet reached the injected storage boundary.
        tree.release.notify_one();
        drain.await.unwrap();
        assert!(!tree.active.load(Ordering::Acquire));
    });
    drop(service);
    drop(partition);
    // The ordinary mutation worker also owns the tree until its closed
    // receiver is polled. This is eventual fixture cleanup, not the sampler
    // barrier oracle above.
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(1), async {
            while Arc::strong_count(&tree) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("ordinary partition worker did not release the fixture tree");
    });
    drop(runtime);
}
