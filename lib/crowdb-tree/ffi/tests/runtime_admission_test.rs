// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use std::sync::Arc;

use crowdb_tree_ffi::{BatchOp, Config, Crowdbtree, CtError, TreeRuntime, TreeRuntimeOptions};

fn tree() -> Arc<Crowdbtree> {
    Arc::new(
        Crowdbtree::open(&Config {
            runtime: Some(Arc::new(
                TreeRuntime::new(TreeRuntimeOptions {
                    workers: 1,
                    ..Default::default()
                })
                .unwrap(),
            )),
            ..Default::default()
        })
        .unwrap(),
    )
}

#[test]
fn unarmed_admission_does_not_keep_a_tree_alive() {
    let tree = tree();
    let weak = Arc::downgrade(&tree);
    let prepared = tree
        .prepare_proposal(&[BatchOp::Put {
            key: b"key",
            value: b"value",
        }])
        .unwrap();
    drop(tree);
    assert!(weak.upgrade().is_none());
    assert!(matches!(prepared.arm(), Err(CtError::Unavailable)));
}

#[test]
fn generation_replacement_invalidates_unarmed_admission() {
    let tree = tree();
    let prepared = tree
        .prepare_proposal(&[BatchOp::Put {
            key: b"old",
            value: b"generation",
        }])
        .unwrap();
    tree.clear().unwrap();
    assert!(matches!(prepared.arm(), Err(CtError::Unavailable)));
    assert!(tree.get(b"old").unwrap().is_none());
}

#[test]
fn cancelled_armed_proposal_releases_close_tickets_and_future_is_send() {
    fn require_send<T: Send>(_: T) {}
    let tree = tree();
    let armed = tree.prepare_proposal(&[]).unwrap().arm().unwrap();
    require_send(armed.apply(1));
    // Dropping an unpolled Rust future cancels the still unsubmitted permit.
    tree.clear().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn admitted_apply_owns_bytes_and_preserves_last_operation() {
    let tree = tree();
    let mut key = b"key".to_vec();
    let mut value = b"owned".to_vec();
    let prepared = tree
        .prepare_proposal(&[
            BatchOp::Put {
                key: &key,
                value: b"first",
            },
            BatchOp::Delete { key: &key },
            BatchOp::Put {
                key: &key,
                value: &value,
            },
        ])
        .unwrap();
    key.fill(b'x');
    value.fill(b'y');
    prepared.arm().unwrap().apply(1).await.unwrap();
    assert_eq!(tree.get(b"key").unwrap(), Some((1, b"owned".to_vec())));
    tree.prepare_replay(&[BatchOp::Delete { key: b"key" }])
        .unwrap()
        .arm()
        .unwrap()
        .apply(2)
        .await
        .unwrap();
    assert!(tree.get(b"key").unwrap().is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn replay_noop_advances_the_same_frontier() {
    let tree = tree();
    tree.prepare_replay(&[])
        .unwrap()
        .arm()
        .unwrap()
        .apply(1)
        .await
        .unwrap();
    tree.prepare_proposal(&[BatchOp::Put {
        key: b"after",
        value: b"noop",
    }])
    .unwrap()
    .arm()
    .unwrap()
    .apply(2)
    .await
    .unwrap();
    assert_eq!(tree.get(b"after").unwrap(), Some((2, b"noop".to_vec())));
    tree.flush().unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn completing_one_apply_does_not_wake_another_and_submitted_apply_survives_drop() {
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Wake, Waker};

    struct Wakes(AtomicUsize);
    impl Wake for Wakes {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    let tree = tree();
    let mut later = Box::pin(tree.apply_replay(
        3,
        vec![crowdb_tree_ffi::ExtOp::Put {
            key: bytes::Bytes::from_static(b"later"),
            value: bytes::Bytes::from_static(b"value"),
        }],
    ));
    let wakes = Arc::new(Wakes(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wakes));
    assert!(later.as_mut().poll(&mut Context::from_waker(&waker)).is_pending());
    tree.apply_replay(1, Vec::new()).await.unwrap();
    assert_eq!(wakes.0.load(Ordering::Relaxed), 0);

    let armed = tree
        .prepare_proposal(&[BatchOp::Put {
            key: b"submitted",
            value: b"owned",
        }])
        .unwrap()
        .arm()
        .unwrap();
    drop(armed.start(2).unwrap());
    tokio::time::timeout(std::time::Duration::from_secs(2), later)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(tree.get(b"submitted").unwrap(), Some((2, b"owned".to_vec())));
}

#[tokio::test]
async fn standalone_tree_uses_the_same_prepared_admission() {
    let tree = Arc::new(Crowdbtree::open(&Config::default()).unwrap());
    tree.prepare_proposal(&[BatchOp::Put {
        key: b"key",
        value: b"value",
    }])
    .unwrap()
    .arm()
    .unwrap()
    .apply(1)
    .await
    .unwrap();
    assert_eq!(tree.get(b"key").unwrap(), Some((1, b"value".to_vec())));
    tree.flush().unwrap();
}
