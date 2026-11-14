// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::Notify;

use crowdb_rpc_ffi::{OwnedClientRoute, RpcClient, RpcServer};

use super::{topology, DiskioClient, DiskioError, DiskioResult, NativeDiskIoRoutes};

struct ResolverClient(Option<Arc<DiskioClient>>);

impl ResolverClient {
    fn get(&self) -> &DiskioClient {
        self.0.as_deref().expect("live resolver client")
    }
}

impl Drop for ResolverClient {
    fn drop(&mut self) {
        if let Some(client) = self.0.take() {
            OwnedClientRoute::retire_transport(client);
        }
    }
}

struct NativeTransport(Option<(Arc<RpcClient>, Arc<RpcServer>)>);

impl Drop for NativeTransport {
    fn drop(&mut self) {
        if let Some(owners) = self.0.take() {
            OwnedClientRoute::retire_transport(owners);
        }
    }
}

/// Retained native routes refreshed outside synchronous tree callbacks.
pub struct NativeDiskRouteResolver {
    routes: Arc<ArcSwap<NativeDiskIoRoutes>>,
    refresh: Arc<Notify>,
    worker: tokio::task::JoinHandle<()>,
}

impl NativeDiskRouteResolver {
    #[must_use]
    pub fn resolve(&self, high: u64, low: u64) -> Option<OwnedClientRoute> {
        let id = crate::DiskId::new(high, low);
        let routes = self.routes.load();
        let route = routes.routes.iter().find(|(disk, _)| *disk == id)?.1.clone();
        if route.is_open() {
            Some(route)
        } else {
            self.refresh.notify_one();
            None
        }
    }
}

impl Drop for NativeDiskRouteResolver {
    fn drop(&mut self) {
        self.worker.abort();
    }
}

impl DiskioClient {
    /// Keep native `DiskIO` connections current after endpoint restarts.
    ///
    /// # Errors
    /// Returns the initial topology or connection error.
    pub async fn native_route_resolver(
        self: &Arc<Self>,
        timeout: Duration,
    ) -> DiskioResult<Arc<NativeDiskRouteResolver>> {
        let client = ResolverClient(Some(Arc::clone(self)));
        let routes = Arc::new(ArcSwap::from_pointee(
            self.native_routes_with_timeout(timeout).await?,
        ));
        let refresh = Arc::new(Notify::new());
        let worker_routes = Arc::clone(&routes);
        let worker_refresh = Arc::clone(&refresh);
        let mut observed = self.routes.load_full();
        let worker = tokio::spawn(async move {
            let client = client.get();
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let closing = client.closing.notified();
                tokio::pin!(closing);
                closing.as_mut().enable();
                if client.closed.load(std::sync::atomic::Ordering::Acquire) {
                    return;
                }
                tokio::select! {
                    () = &mut closing => return,
                    _ = tick.tick() => {},
                    () = worker_refresh.notified() => {},
                }
                tokio::select! {
                    () = &mut closing => return,
                    () = client.refresh_topology_if_due() => {},
                }
                let current = client.routes.load_full();
                if observed.routes == current.routes
                    && worker_routes
                        .load()
                        .routes
                        .iter()
                        .all(|(_, route)| route.is_open())
                {
                    continue;
                }
                let result = tokio::select! {
                    () = &mut closing => return,
                    result = client.native_routes_with_timeout(timeout) => result,
                };
                match result {
                    Ok(snapshot) => {
                        worker_routes.store(Arc::new(snapshot));
                        observed = current;
                    }
                    Err(error) => tracing::warn!(%error, "native DiskIO route refresh failed"),
                }
            }
        });
        Ok(Arc::new(NativeDiskRouteResolver {
            routes,
            refresh,
            worker,
        }))
    }

    /// Export dedicated connections for the native tree transport's request-ID
    /// namespace. Native callers must not share Rust's RPC completion slots.
    ///
    /// # Errors
    /// Returns topology or connection errors for unavailable disk endpoints.
    pub async fn native_routes(&self) -> DiskioResult<NativeDiskIoRoutes> {
        self.native_routes_with_timeout(Duration::from_secs(10)).await
    }

    async fn native_routes_with_timeout(&self, timeout: Duration) -> DiskioResult<NativeDiskIoRoutes> {
        let snapshot = self.routes.load_full();
        if snapshot.routes.is_empty() {
            return Ok(NativeDiskIoRoutes { routes: Vec::new() });
        }
        let server = Arc::new(RpcServer::with_engines(None, 1, self.config.rpc_workers));
        server.start();
        let client = Arc::new(RpcClient::new());
        client.set_completion_pool_size(1024);
        client.start_reaper(u64::try_from(timeout.as_nanos()).unwrap_or(u64::MAX), 100_000_000);
        let transport = NativeTransport(Some((client, server)));
        let (client, server) = transport.0.as_ref().expect("native transport owners");
        let mut endpoints = HashMap::new();
        let mut routes = Vec::with_capacity(snapshot.routes.len());
        for (disk_id, target) in &snapshot.routes {
            let route = if let Some(route) = endpoints.get(&target.endpoint) {
                OwnedClientRoute::clone(route)
            } else {
                let (host, port) = topology::parse_endpoint(&target.endpoint)?;
                let connection = server.connect_native_route(host, port).await.map_err(|error| {
                    DiskioError::TransportUnavailable(format!("native DiskIO connection: {error:?}"))
                })?;
                client.attach(&connection);
                let route = OwnedClientRoute::new(Arc::clone(client), Arc::clone(server), connection);
                endpoints.insert(target.endpoint.clone(), route.clone());
                route
            };
            routes.push((*disk_id, route));
        }
        Ok(NativeDiskIoRoutes { routes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::semantic::{DiskRoute, RouteSnapshot};
    use crate::{DiskId, DiskioClientConfig};

    #[tokio::test(flavor = "current_thread")]
    async fn canceled_resolver_transfers_its_last_discovery_client_to_retirement() {
        struct HoldRetirement {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
        }

        impl Drop for HoldRetirement {
            fn drop(&mut self) {
                let _ = self.entered.take().unwrap().send(());
                let _ = self.release.recv_timeout(Duration::from_secs(5));
            }
        }

        let client =
            Arc::new(DiskioClient::connect_for_tests(vec![], DiskioClientConfig::default()).unwrap());
        let weak = Arc::downgrade(&client);
        let resolver = client
            .native_route_resolver(Duration::from_secs(1))
            .await
            .unwrap();
        let mut resolver = Arc::into_inner(resolver).unwrap();
        drop(client);

        let (entered, ready) = tokio::sync::oneshot::channel();
        let (release, held) = std::sync::mpsc::channel();
        OwnedClientRoute::retire_transport(HoldRetirement {
            entered: Some(entered),
            release: held,
        });
        tokio::time::timeout(Duration::from_secs(3), ready)
            .await
            .unwrap()
            .unwrap();

        resolver.worker.abort();
        let canceled = tokio::time::timeout(Duration::from_secs(3), &mut resolver.worker)
            .await
            .unwrap();
        assert!(canceled.unwrap_err().is_cancelled());
        assert!(weak.upgrade().is_some());
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while weak.strong_count() != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn native_routes_handle_empty_topology_and_connection_failure() {
        let client = DiskioClient::connect_for_tests(vec![], DiskioClientConfig::default()).unwrap();
        assert!(client
            .native_routes()
            .await
            .unwrap()
            .into_owned_routes()
            .is_empty());

        // Inject a target directly so the failure occurs in the dedicated
        // native transport, after the semantic client's own setup succeeded.
        let mut snapshot = RouteSnapshot::empty();
        snapshot.routes.insert(
            DiskId::new(1, 2),
            DiskRoute {
                rack_id: 1,
                node_id: 1,
                disk_group_id: 1,
                instance_id: 1,
                endpoint: Arc::from("invalid:1234"),
                pool_key: Arc::from("1@invalid:1234"),
            },
        );
        client.routes.store(Arc::new(snapshot));
        assert!(matches!(
            client.native_routes().await,
            Err(DiskioError::TransportUnavailable(_))
        ));
    }
}
