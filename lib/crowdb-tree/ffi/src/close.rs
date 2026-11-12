// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicI32, Ordering};
use tokio::sync::Notify;

use crate::{error::check, CtError};

const IDLE: i32 = i32::MAX;
const PENDING: i32 = i32::MAX - 1;

#[derive(Debug)]
pub(crate) struct CloseState {
    status: AtomicI32,
    completed: Notify,
}

impl CloseState {
    pub(crate) fn new(started: bool) -> Self {
        Self {
            status: AtomicI32::new(if started { PENDING } else { IDLE }),
            completed: Notify::new(),
        }
    }

    pub(crate) fn start(&self) -> bool {
        self.status
            .compare_exchange(IDLE, PENDING, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn finish(&self, status: i32) {
        self.status.store(status, Ordering::Release);
        self.completed.notify_waiters();
    }

    pub(crate) async fn wait(&self) -> Result<(), CtError> {
        loop {
            let notified = self.completed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let status = self.status.load(Ordering::Acquire);
            if status != PENDING {
                return check(status);
            }
            notified.await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{ptr, sync::Arc, time::Duration};

    use crate::{sys, Config, Crowdbtree, PageStore, TreeRuntime, TreeRuntimeOptions};

    struct Permit(*mut sys::ct_apply_permit);

    impl Permit {
        fn arm(tree: &Crowdbtree) -> Self {
            let mut permit = Self(ptr::null_mut());
            assert_eq!(
                unsafe { sys::ct_apply_prepare_callback(tree.as_ptr(), ptr::null(), 0, 0, &mut permit.0) },
                0
            );
            assert_eq!(unsafe { sys::ct_apply_arm(permit.0) }, 0);
            permit
        }
    }

    impl Drop for Permit {
        fn drop(&mut self) {
            unsafe { sys::ct_apply_permit_free(self.0) };
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn canceling_a_wait_keeps_the_accepted_native_close_alive() {
        let runtime = Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                ..Default::default()
            })
            .unwrap(),
        );
        let tree = Crowdbtree::open(&Config {
            runtime: Some(runtime.clone()),
            ..Config::default()
        })
        .unwrap();
        // The native permit has its own ticket. The public Rust permit also
        // keeps a Tree Arc and therefore prevents transferring the facade.
        let permit = Permit::arm(&tree);
        let closing = tree.begin_close().unwrap();
        let expired = tokio::time::timeout(Duration::from_millis(20), closing.wait())
            .await
            .is_err();
        drop(permit);
        assert!(expired);
        let second = closing.clone();
        let (first, second) = tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(closing.wait(), second.wait())
        })
        .await
        .unwrap();
        assert_eq!((first, second), (Ok(()), Ok(())));
        assert_eq!(runtime.snapshot().unwrap().bindings, 0);
        runtime.drain_cpu();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn hosted_close_survives_last_drop_with_an_accepted_ticket() {
        let runtime = Arc::new(
            TreeRuntime::new(TreeRuntimeOptions {
                workers: 1,
                backend_workers: 1,
                ..Default::default()
            })
            .unwrap(),
        );
        let options = Config {
            runtime: Some(runtime.clone()),
            page_store: Some(Arc::new(PageStore::open_mem(1).unwrap())),
            ..Default::default()
        };
        let tree = Arc::new(Crowdbtree::open_async(&options).await.unwrap());
        let close = tree.close_result().unwrap();
        let short_lived_reader = tree.clone();
        let permit = Permit::arm(&tree);
        drop(tree);
        assert!(tokio::time::timeout(Duration::from_millis(5), close.wait())
            .await
            .is_err());
        drop(short_lived_reader);
        assert!(tokio::time::timeout(Duration::from_millis(5), close.wait())
            .await
            .is_err());
        drop(permit);
        tokio::time::timeout(Duration::from_secs(5), close.wait())
            .await
            .unwrap()
            .unwrap();

        let reopened = Crowdbtree::open_async(&options).await.unwrap();
        let observer = reopened.close_result().unwrap();
        let close = reopened.begin_close().unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            assert_eq!(tokio::join!(observer.wait(), close.wait()), (Ok(()), Ok(())));
            runtime.shutdown().await.unwrap();
        })
        .await
        .unwrap();
    }
}
