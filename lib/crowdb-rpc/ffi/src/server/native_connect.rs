// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::ffi::{c_void, CString};
use std::os::fd::BorrowedFd;
use std::ptr::{self, NonNull};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{unix::AsyncFd, Interest};

use super::{Connection, ConnectionOwnership, RpcError, RpcServer};
use crate::owned_route::{begin_transport_retirement, finish_transport_retirement};
use crate::{sys, OwnedClientRoute};

const CONNECT_DONE: i32 = 0;
const CONNECT_READ: i32 = 1;
const CONNECT_WRITE: i32 = 2;
const CONNECT_RETRY: i32 = 3;

struct ConnectOwner {
    attempt: NonNull<sys::crowdb_rpc_connect_s>,
    server: Arc<RpcServer>,
}

// Native setup has a single driver. The server remains alive through all
// steps and final retirement, including when Tokio moves or cancels the task.
unsafe impl Send for ConnectOwner {}

impl Drop for ConnectOwner {
    fn drop(&mut self) {
        unsafe { sys::crowdb_rpc_connect_destroy(self.attempt.as_ptr()) };
    }
}

struct PendingConnect(Option<ConnectOwner>);

impl Drop for PendingConnect {
    fn drop(&mut self) {
        if let Some(owner) = self.0.take() {
            if unsafe { sys::crowdb_rpc_connect_requires_retirement(owner.attempt.as_ptr()) } {
                schedule_retirement(Arc::new(RetiringConnect {
                    owner: Some(owner),
                    queued_at: begin_transport_retirement(),
                }));
            } else {
                OwnedClientRoute::retire_transport(owner);
            }
        }
    }
}

struct RetiringConnect {
    owner: Option<ConnectOwner>,
    queued_at: Instant,
}

// Each accepted timer has one callback. A retry is submitted only after its
// preceding native step returns. Shared ownership pins the attempt even if
// a callback runs before submission returns.
unsafe impl Sync for RetiringConnect {}

impl Drop for RetiringConnect {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.take() {
            finish_transport_retirement(owner, self.queued_at);
        }
    }
}

fn schedule_retirement(retiring: Arc<RetiringConnect>) {
    let owner = retiring.owner.as_ref().expect("retiring connect owner");
    let context = Arc::into_raw(Arc::clone(&retiring)).cast_mut().cast::<c_void>();
    let accepted =
        unsafe { sys::crowdb_rpc_server_schedule(owner.server.handle, 1, resume_retirement, context) };
    if accepted == 0 {
        // A rejected timer does not invoke the callback.
        drop(unsafe { Arc::from_raw(context.cast::<RetiringConnect>()) });
    }
}

unsafe extern "C" fn resume_retirement(context: *mut c_void, fired: i32) {
    let retiring = unsafe { Arc::from_raw(context.cast::<RetiringConnect>()) };
    let owner = retiring.owner.as_ref().expect("retiring connect owner");
    if fired != 0 && unsafe { sys::crowdb_rpc_connect_retire_step(owner.attempt.as_ptr()) } == 0 {
        schedule_retirement(retiring);
    }
}

impl RpcServer {
    /// Connect and use this server's dispatch for both directions.
    /// Install the request client before starting the server.
    ///
    /// # Errors
    /// Returns an error if transport setup fails.
    ///
    /// # Panics
    /// Panics if native setup publishes a connection that already has a Rust wrapper.
    pub async fn connect_for_requests(
        self: &Arc<Self>,
        addr: &str,
        port: i32,
    ) -> Result<Connection, RpcError> {
        let mut connection = self.connect_native_route(addr, port).await?;
        Arc::get_mut(&mut connection.inner)
            .expect("new connection is not shared")
            .ownership = ConnectionOwnership::Outbound {
            _server: Some(Arc::clone(self)),
        };
        unsafe { sys::crowdb_rpc_server_route_connection(self.handle, connection.handle()) };
        Ok(connection)
    }

    /// Drives a dedicated native route's preflight and CM readiness on Tokio.
    /// Keep the server started until its connection attempts and routes retire.
    pub async fn connect_native_route(
        self: &Arc<Self>,
        addr: &str,
        port: i32,
    ) -> Result<Connection, RpcError> {
        let pending = {
            let addr = CString::new(addr).map_err(|_| RpcError::InvalidArg)?;
            let mut connection = ptr::null_mut();
            let attempt =
                unsafe { sys::crowdb_rpc_connect_begin(self.handle, addr.as_ptr(), port, &mut connection) };
            let Some(attempt) = NonNull::new(attempt) else {
                return Connection::from_owned(connection);
            };
            PendingConnect(Some(ConnectOwner {
                attempt,
                server: Arc::clone(self),
            }))
        };

        let mut event = 0;
        loop {
            let (status, fd, timeout_ms) = {
                let owner = pending.0.as_ref().expect("active connect owner");
                let mut connection = ptr::null_mut();
                let mut fd = -1;
                let mut timeout_ms = 0;
                let status = unsafe {
                    sys::crowdb_rpc_connect_step(
                        owner.attempt.as_ptr(),
                        event,
                        &mut fd,
                        &mut timeout_ms,
                        &mut connection,
                    )
                };
                if status == CONNECT_DONE {
                    return Connection::from_owned(connection);
                }
                (status, fd, timeout_ms)
            };
            let interest = match status {
                CONNECT_READ => Interest::READABLE,
                CONNECT_WRITE => Interest::WRITABLE,
                CONNECT_RETRY => {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    event = 0;
                    continue;
                }
                _ => return Err(RpcError::ConnectionError),
            };
            if timeout_ms == 0 {
                event = -1;
                continue;
            }
            if fd < 0 {
                return Err(RpcError::ConnectionError);
            }
            // The attempt exclusively owns the fd. This registration is dropped
            // before advancing and before pending can transfer it for retirement.
            let descriptor = AsyncFd::with_interest(unsafe { BorrowedFd::borrow_raw(fd) }, interest)
                .map_err(|_| RpcError::ConnectionError)?;
            event = match tokio::time::timeout(
                Duration::from_millis(timeout_ms.into()),
                descriptor.ready(interest),
            )
            .await
            {
                Ok(Ok(_)) => 1,
                Ok(Err(_)) => return Err(RpcError::ConnectionError),
                Err(_) => -1,
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::RdmaConfig;

    #[test]
    fn retirement_timer_drains_delivered_rejected_and_canceled_owners() {
        if std::env::var_os("CROWDB_RDMA_E2E").is_none() {
            return;
        }
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = i32::from(listener.local_addr().unwrap().port());
        let server = Arc::new(RpcServer::new(None));
        server
            .configure_rdma(&RdmaConfig {
                enabled: true,
                listen_enabled: false,
                device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
                ..RdmaConfig::default()
            })
            .unwrap();

        let attempt = || {
            let address = CString::new("127.0.0.1").unwrap();
            let mut connection = ptr::null_mut();
            let attempt = unsafe {
                sys::crowdb_rpc_connect_begin(server.handle, address.as_ptr(), port, &mut connection)
            };
            assert!(connection.is_null());
            Arc::new(RetiringConnect {
                owner: Some(ConnectOwner {
                    attempt: NonNull::new(attempt).unwrap(),
                    server: Arc::clone(&server),
                }),
                queued_at: begin_transport_retirement(),
            })
        };
        for started in [false, true] {
            if started {
                server.start();
            }
            let retiring = attempt();
            assert!(!OwnedClientRoute::drain_retired(Duration::ZERO));
            schedule_retirement(retiring);
            assert!(OwnedClientRoute::drain_retired(Duration::from_secs(2)));
            assert_eq!(Arc::strong_count(&server), 1);
        }

        let context = Arc::into_raw(attempt()).cast_mut().cast::<c_void>();
        let timer =
            unsafe { sys::crowdb_rpc_server_schedule(server.handle, 30_000, resume_retirement, context) };
        if timer == 0 {
            drop(unsafe { Arc::from_raw(context.cast::<RetiringConnect>()) });
            panic!("started server rejected the retirement timer");
        }
        server.stop();
        assert!(OwnedClientRoute::drain_retired(Duration::from_secs(2)));
        assert_eq!(Arc::strong_count(&server), 1);
    }
}
