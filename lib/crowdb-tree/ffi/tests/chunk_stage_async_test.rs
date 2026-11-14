// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_tree_ffi::chunk::stage::{StageId, StageRecord, StageState};
use crowdb_tree_ffi::chunk::{CatalogFuture, RootCatalogObject, RootCatalogStore};
use crowdb_tree_ffi::{ChunkRootCatalog, CtError};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::Semaphore;

struct TestAsyncCatalog {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    completed: Arc<AtomicUsize>,
    panic: AtomicBool,
}

impl RootCatalogStore for TestAsyncCatalog {
    fn supports_staging(&self) -> bool {
        true
    }
    fn load(&self, _: u64, _: RootCatalogObject) -> Result<Option<Vec<u8>>, CtError> {
        Err(CtError::NotSupported)
    }
    fn reclaim_before(&self, _: u64, _: u64) -> u64 {
        panic!("async history pruning used the synchronous callback");
    }
    fn reclaim_before_async(&self, tree: u64, generation: u64) -> CatalogFuture<'_, u64> {
        Box::pin(async move {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            assert_eq!((tree, generation), (42, 7));
            self.completed.fetch_add(1, Ordering::Release);
            Ok(2)
        })
    }
    fn pin_generation(&self, _: u64, _: u64, _: u64, _: u64) -> Result<(), CtError> {
        panic!("async generation pin used the synchronous callback");
    }
    fn pin_generation_async(&self, tree: u64, high: u64, low: u64, generation: u64) -> CatalogFuture<'_, ()> {
        assert_eq!((tree, high, low, generation), (42, 8, 9, 7));
        self.generation_mutation()
    }
    fn unpin_generation(&self, _: u64, _: u64, _: u64) -> Result<(), CtError> {
        panic!("async generation release used the synchronous callback");
    }
    fn unpin_generation_async(&self, tree: u64, high: u64, low: u64) -> CatalogFuture<'_, ()> {
        assert_eq!((tree, high, low), (42, 8, 9));
        self.generation_mutation()
    }
    fn compare_stage(
        &self,
        _: u64,
        _: u64,
        _: Option<&StageRecord>,
        _: Option<&StageRecord>,
    ) -> Result<(), CtError> {
        panic!("native flow called the synchronous catalog callback");
    }
    fn compare_stage_async<'a>(
        &'a self,
        tree: u64,
        epoch: u64,
        expected: Option<&'a StageRecord>,
        next: Option<&'a StageRecord>,
    ) -> CatalogFuture<'a, ()> {
        Box::pin(async move {
            self.entered.add_permits(1);
            if self.panic.load(Ordering::Acquire) {
                panic!("injected catalog future panic")
            }
            self.release
                .acquire()
                .await
                .map_err(|_| CtError::Unavailable)?
                .forget();
            assert_eq!(tree, 42);
            assert_eq!(epoch, 7);
            assert!(expected.is_none());
            assert_eq!(next.unwrap(), &record());
            self.completed.fetch_add(1, Ordering::Release);
            Ok(())
        })
    }
}

impl TestAsyncCatalog {
    fn generation_mutation(&self) -> CatalogFuture<'_, ()> {
        Box::pin(async {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            self.completed.fetch_add(1, Ordering::Release);
            Ok(())
        })
    }
}

fn record() -> StageRecord {
    StageRecord {
        tree_id: 42,
        id: StageId {
            owner_epoch: 7,
            operation_high: 8,
            operation_low: 9,
        },
        revision: 1,
        chunk_high: 2_u64 << 56,
        chunk_low: 123,
        capacity: 4096,
        purpose: 2,
        state: StageState::Reserved,
        manifest_generation: 0,
        packs: vec![],
    }
}

fn store() -> Arc<TestAsyncCatalog> {
    Arc::new(TestAsyncCatalog {
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        completed: Arc::new(AtomicUsize::new(0)),
        panic: AtomicBool::new(false),
    })
}

async fn entered(store: &TestAsyncCatalog, count: u32) {
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        store.entered.acquire_many(count),
    )
    .await
    .unwrap()
    .unwrap()
    .forget();
}

#[test]
fn staging_catalog_requires_a_runtime_for_its_callbacks() {
    assert!(matches!(
        ChunkRootCatalog::open_callback(store()),
        Err(CtError::Unavailable)
    ));
}

#[tokio::test(flavor = "current_thread")]
async fn native_stage_callbacks_suspend_without_occupying_the_only_runtime_worker() {
    let store = store();
    let catalog = Arc::new(ChunkRootCatalog::open_callback(store.clone()).unwrap());
    let mut tasks = Vec::new();
    for _ in 0..2 {
        let catalog = catalog.clone();
        tasks.push(tokio::spawn(async move {
            catalog.compare_stage_async(42, 7, None, Some(&record())).await
        }));
    }
    entered(&store, 2).await;
    assert!(tasks.iter().all(|task| !task.is_finished()));
    assert_eq!(tokio::spawn(async { 37 }).await.unwrap(), 37);
    store.release.add_permits(2);
    for task in tasks {
        assert_eq!(task.await.unwrap(), Ok(()))
    }
    assert_eq!(store.completed.load(Ordering::Acquire), 2);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_caller_keeps_catalog_and_input_alive_until_the_late_callback() {
    let store = store();
    let entered_signal = store.entered.clone();
    let release = store.release.clone();
    let completed = store.completed.clone();
    let retained = Arc::downgrade(&store);
    let catalog = Arc::new(ChunkRootCatalog::open_callback(store.clone()).unwrap());
    drop(store);
    let task = tokio::spawn(async move { catalog.compare_stage_async(42, 7, None, Some(&record())).await });
    entered_signal.acquire().await.unwrap().forget();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(retained.upgrade().is_some());
    release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while retained.upgrade().is_some() {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    assert_eq!(completed.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn history_pruning_suspends_and_finishes_after_its_waiter_is_cancelled() {
    let store = store();
    let catalog = Arc::new(ChunkRootCatalog::open_callback(store.clone()).unwrap());
    let completed = store.completed.clone();
    let release = store.release.clone();
    let retained = Arc::downgrade(&store);
    let pending = tokio::spawn(async move { catalog.reclaim_before_async(42, 7).await });
    entered(&store, 1).await;
    assert!(!pending.is_finished());
    assert_eq!(tokio::spawn(async { 17 }).await.unwrap(), 17);
    drop(store);
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    assert!(retained.upgrade().is_some());
    release.add_permits(1);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        while retained.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(completed.load(Ordering::Acquire), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn generation_pin_and_release_finish_after_canceling_the_waiter() {
    let store = store();
    let catalog = Arc::new(ChunkRootCatalog::open_callback(store.clone()).unwrap());
    for pin in [true, false] {
        let catalog = catalog.clone();
        let pending = tokio::spawn(async move {
            if pin {
                catalog.pin_generation_async(42, 8, 9, 7).await
            } else {
                catalog.unpin_generation_async(42, 8, 9).await
            }
        });
        entered(&store, 1).await;
        assert!(!pending.is_finished());
        pending.abort();
        assert!(pending.await.unwrap_err().is_cancelled());
        store.release.add_permits(1);
        let completed = if pin { 1 } else { 2 };
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            while store.completed.load(Ordering::Acquire) != completed {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
}

#[tokio::test(flavor = "current_thread")]
async fn a_panicking_catalog_future_completes_error_and_releases_its_context() {
    let store = store();
    store.panic.store(true, Ordering::Release);
    let catalog = ChunkRootCatalog::open_callback(store.clone()).unwrap();
    assert_eq!(
        catalog.compare_stage_async(42, 7, None, Some(&record())).await,
        Err(CtError::Internal)
    );
    store.panic.store(false, Ordering::Release);
    store.release.add_permits(1);
    assert_eq!(
        catalog.compare_stage_async(42, 7, None, Some(&record())).await,
        Ok(())
    );
    assert_eq!(store.completed.load(Ordering::Acquire), 1);
}
