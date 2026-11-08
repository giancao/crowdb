// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Server lifecycle for a `PxKvStore`.
//!
//! Defines the [`PaxosKvServer`] trait (start / join / stop / `listen_addr`)
//! and implements it on `Arc<PxKvStore>`. Client-facing and consensus
//! listeners own separate RPC transports. Lifecycle state lives on
//! [`RpcTaskState`] inside the store so
//! [`PxKvStore::shutdown_server`] can drive a timed graceful stop from
//! the cascade shutdown path.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::px_kv_store::PxKvStore;
use crate::rpc::{PaxosKvRpcForwarder, PaxosKvRpcService, PxRpcService, PxRpcTransport};
use crowdb_rpc_ffi::RpcServer;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, info_span, Instrument};

#[allow(async_fn_in_trait)]
pub trait PaxosKvServer {
    async fn start(&self) -> Result<(), String>;

    async fn join(&self);

    fn stop(&self);

    fn listen_addr(&self) -> Option<SocketAddr>;
}

#[derive(Default)]
pub(crate) struct RpcTaskState {
    pub(crate) handle: Option<tokio::task::JoinHandle<()>>,
    pub(crate) shutdown_tx: Option<tokio::sync::oneshot::Sender<()>>,
    pub(crate) listen_addr: Option<SocketAddr>,
}

/// crowdb-rpc server state. Holds the `RpcServer` handle + the shared
/// `PxRpcTransport` for outbound RPCs to peers.
#[derive(Default)]
pub(crate) struct RpcServerState {
    pub(crate) server: Option<Arc<RpcServer>>,
    pub(crate) transport: Option<Arc<PxRpcTransport>>,
}
#[allow(async_fn_in_trait)]
impl PaxosKvServer for Arc<PxKvStore> {
    #[allow(clippy::unused_async_trait_impl)]
    async fn start(&self) -> Result<(), String> {
        {
            let state = self.server_state.lock();
            if state.handle.is_some() {
                debug!(
                    s = self.store_id,
                    "kv server start skipped because server is already running"
                );
                return Ok(());
            }
        }

        let bound_addr = self.listen_addr;

        let client = self.bind_rpc_listener(bound_addr)?;
        let peer = self.bind_rpc_listener(SocketAddr::new(bound_addr.ip(), 0))?;
        let peer_port = u16::try_from(peer.port()).map_err(|_| "invalid Paxos peer listener port")?;
        let actual_port = u16::try_from(client.port()).map_err(|_| "invalid KV client listener port")?;
        let actual_addr = SocketAddr::new(bound_addr.ip(), actual_port);
        let rt = tokio::runtime::Handle::current();
        let px_service = Arc::new(PxRpcService::new(self.clone(), rt.clone()));
        px_service.register_handlers(&peer);

        let forwarder = Arc::new(PaxosKvRpcForwarder::with_workers(self.rpc_workers));
        let kv_service = Arc::new(PaxosKvRpcService::new(self.clone(), rt, forwarder));
        kv_service.register_handlers(&client);
        PxRpcTransport::register_listener_discovery(&client, peer_port);

        let transport = PxRpcTransport::on_listener(Arc::clone(&peer), self);
        peer.start();
        peer.register_conn_count_gauge("rpc.consensus.connections");
        client.start();
        client.register_conn_count_gauge("rpc.server.connections");

        let (tx, rx) = tokio::sync::oneshot::channel();
        let (peer_task, client_task) = (Arc::clone(&peer), Arc::clone(&client));
        let handle = tokio::spawn(
            async move {
                let _ = rx.await;
                client_task.stop();
                peer_task.stop();
            }
            .instrument(info_span!("rpc_server", s = self.store_id)),
        );

        {
            let mut state = self.server_state.lock();
            state.listen_addr = Some(actual_addr);
            state.handle = Some(handle);
            state.shutdown_tx = Some(tx);
        }
        {
            let mut state = self.rpc_server_state.lock();
            state.server = Some(peer);
            state.transport = Some(transport.clone());
        }
        self.client_rpc_server_state.lock().server = Some(client);

        let group_count = self.group_count();
        for entry in self.groups_snapshot().values() {
            entry.local_replica().set_endpoint(actual_addr.to_string());
            // Wire the shared transport into remote replicas that were
            // created by apply_config during restore (before start()).
            let mut wired = 0usize;
            for remote in &entry.remote_replicas {
                if let Some(real) = remote.as_real() {
                    real.set_rpc_transport(transport.clone());
                    wired += 1;
                }
            }
            debug!(
                s = self.store_id,
                g = entry.group_id(),
                remote_count = entry.remote_replicas.len(),
                wired,
                "start: wired rpc transport into remote replicas"
            );
        }

        info!(s = self.store_id, listen_addr = %actual_addr, peer_port, group_count, "kv server started");
        Ok(())
    }

    async fn join(&self) {
        let handle = {
            let mut state = self.server_state.lock();
            state.handle.take()
        };
        if let Some(task) = handle {
            debug!(s = self.store_id, "joining kv server task");
            let _ = task.await;
            debug!(s = self.store_id, "kv server task joined");
        }
    }

    fn stop(&self) {
        let sender = {
            let mut state = self.server_state.lock();
            state.shutdown_tx.take()
        };
        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(s = self.store_id, "kv server shutdown requested");
        }
    }

    fn listen_addr(&self) -> Option<SocketAddr> {
        self.server_state.lock().listen_addr
    }
}

impl PxKvStore {
    fn bind_rpc_listener(&self, address: SocketAddr) -> Result<Arc<RpcServer>, String> {
        let server = Arc::new(RpcServer::with_engines(None, 1, self.rpc_workers));
        server.set_tcp_nodelay(!self.enable_nagle);
        server.set_quickack(self.quickack);
        server.set_event_write(self.event_write);
        server.set_send_queue_capacity(self.send_queue_capacity);
        server
            .listen(&address.ip().to_string(), i32::from(address.port()))
            .map_err(|error| format!("failed to bind Paxos-KV RPC listener on {address}: {error:?}"))?;
        Ok(server)
    }

    /// Stop the server task and await its join under a timeout.
    ///
    /// Used by [`PxKvStore::shutdown`] after runtime accepted work drains.
    /// Aborts the task on hang; idempotent.
    pub(crate) async fn shutdown_server(&self, timeout: Duration) -> Result<(), String> {
        self.stop_rpc_server();
        self.stop_client_rpc_server();

        let (handle, sender) = {
            let mut state = self.server_state.lock();
            (state.handle.take(), state.shutdown_tx.take())
        };
        if handle.is_none() && sender.is_none() {
            debug!(
                s = self.store_id,
                "kv server shutdown is a no-op (already stopped)"
            );
            return Ok(());
        }

        if let Some(tx) = sender {
            let _ = tx.send(());
            info!(s = self.store_id, "kv server shutdown requested");
        }
        let Some(task) = handle else { return Ok(()) };
        let abort = task.abort_handle();
        match tokio::time::timeout(timeout, task).await {
            Ok(Ok(())) => {
                debug!(s = self.store_id, "kv server task joined");
                Ok(())
            }
            Ok(Err(join_err)) if join_err.is_cancelled() => {
                debug!(s = self.store_id, "kv server task was already cancelled");
                Ok(())
            }
            Ok(Err(join_err)) => {
                let msg = format!("critical: kv server task ended abnormally: {join_err}; next step: inspect server logs for panic");
                error!(s = self.store_id, error = %join_err, "{msg}");
                Err(msg)
            }
            Err(_elapsed) => {
                abort.abort();
                let msg = format!("critical: kv server shutdown hung > {timeout:?}; aborted task; next step: investigate stuck handlers");
                error!(
                    s = self.store_id,
                    timeout_ms = timeout.as_millis() as u64,
                    "{msg}"
                );
                Err(msg)
            }
        }
    }

    /// Get the shared `PxRpcTransport` for outbound RPCs. Returns
    /// `None` if `start()` has not been called.
    pub fn rpc_transport(&self) -> Option<Arc<PxRpcTransport>> {
        self.rpc_server_state.lock().transport.clone()
    }

    /// Sample transport-level stats (syscall counts, frame aggregation,
    /// submit→writev queue wait) from the crowdb-rpc server. Returns
    /// `None` if the server has not been started.
    pub fn rpc_transport_stats(&self) -> Option<crowdb_rpc_ffi::CrowdbRpcTransportStats> {
        self.rpc_server_state
            .lock()
            .server
            .as_ref()
            .map(|s| s.transport_stats())
    }

    /// Sample the peer RPC transport and its live connections.
    pub fn rpc_transport_status(&self) -> Option<Vec<u8>> {
        let server = self.rpc_server_state.lock().server.clone()?;
        server.transport_status_json().ok()
    }

    /// Sample the client RPC transport and its live connections.
    pub fn client_rpc_transport_status(&self) -> Option<Vec<u8>> {
        let server = self.client_rpc_server_state.lock().server.clone()?;
        server.transport_status_json().ok()
    }

    /// Wire the shared `PxRpcTransport` into all existing remote
    /// replicas across all groups. Must be called after
    /// [`PaxosKvServer::start`]. Remote replicas added later (via
    /// the management API) get the transport via
    /// [`Self::rpc_transport`] at construction time.
    pub fn wire_rpc_transport(&self) {
        let transport = self.rpc_transport();
        let Some(transport) = transport else { return };
        for entry in self.groups_snapshot().values() {
            for remote in &entry.remote_replicas {
                if let Some(real) = remote.as_real() {
                    real.set_rpc_transport(transport.clone());
                }
            }
        }
    }

    /// Stop the crowdb-rpc server. Called from [`Self::shutdown_server`]
    /// or directly for testing.
    pub(crate) fn stop_rpc_server(&self) {
        let server = {
            let mut state = self.rpc_server_state.lock();
            state.server.take()
        };
        if let Some(server) = server {
            server.stop();
            info!(s = self.store_id, "crowdb-rpc server stopped");
        }
    }

    /// Stop the client-facing crowdb-rpc server. Called from
    /// [`Self::shutdown_server`] or directly for testing.
    pub(crate) fn stop_client_rpc_server(&self) {
        let server = {
            let mut state = self.client_rpc_server_state.lock();
            state.server.take()
        };
        if let Some(server) = server {
            server.stop();
            info!(s = self.store_id, "client crowdb-rpc server stopped");
        }
    }
}
