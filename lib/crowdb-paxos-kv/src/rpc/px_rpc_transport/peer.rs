// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::time::{SystemTime, UNIX_EPOCH};

use crowdb_protocol::kv_consensus_fb::{
    FBPeerConnectionRegistration, FBPeerConnectionRegistrationArgs, FBPeerConnectionResponse,
    FBPeerConnectionResponseArgs, FBPeerListenerRequest, FBPeerListenerRequestArgs, FBPeerListenerResponse,
    FBPeerListenerResponseArgs,
};
use crowdb_rpc_ffi::{Connection, ServerRequest};

use super::{
    normalize_endpoint, parse_endpoint, rpc_error_to_px, Arc, Buffer, FBMsgType, FlatBufferBuilder,
    PxKvStore, PxReplicaError, PxRpcTransport, RpcError, RpcServer, SelectedConnection,
};

const REGISTER_PEER: u16 = FBMsgType::EPeerConnectionRegistration.0 as u16;
const PEER_RESPONSE: u16 = FBMsgType::EPeerConnectionResponse.0 as u16;
const LISTENER_REQUEST: u16 = FBMsgType::EPeerListenerRequest.0 as u16;
const LISTENER_RESPONSE: u16 = FBMsgType::EPeerListenerResponse.0 as u16;

impl PxRpcTransport {
    pub(crate) fn for_snapshot_join() -> Self {
        Self {
            discover_peer_listener: true,
            ..Self::new()
        }
    }

    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn with_peer_discovery_for_tests() -> Self {
        Self::for_snapshot_join()
    }

    pub(crate) fn register_listener_discovery(server: &Arc<RpcServer>, port: u16) {
        server.register_owned_handler(LISTENER_REQUEST, move |request| {
            if flatbuffers::root::<FBPeerListenerRequest>(request.control()).is_err() {
                return;
            }
            let mut builder = FlatBufferBuilder::new();
            let response = FBPeerListenerResponse::create(
                &mut builder,
                &FBPeerListenerResponseArgs {
                    id: request.request_id,
                    rpc_create_nano: request.rpc_create_nano,
                    port,
                },
            );
            builder.finish(response, None);
            let _ = request.reply(
                Buffer::from_bytes(builder.finished_data()),
                None,
                LISTENER_RESPONSE,
            );
        });
    }

    pub(crate) fn on_listener(server: Arc<RpcServer>, store: &Arc<PxKvStore>) -> Arc<Self> {
        let transport = Arc::new(Self::with_server(
            server,
            store.peer_pool_size,
            Some(Arc::downgrade(store)),
        ));
        transport.server.set_request_client(&transport.rpc);
        let weak = Arc::downgrade(&transport);
        transport
            .server
            .register_owned_handler(REGISTER_PEER, move |request| {
                if let Some(transport) = weak.upgrade() {
                    transport.register_peer(request);
                }
            });
        transport
    }

    pub(super) async fn connect_peer(&self, endpoint: &str) -> Result<SelectedConnection, PxReplicaError> {
        let (host, port) = parse_endpoint(endpoint).map_err(PxReplicaError::Internal)?;
        let port = match self.peer_listener_port(&host, port).await {
            Ok(port) => port,
            Err(error) => return self.connections.get(endpoint).ok_or(error),
        };
        let identity = self.peer_identity(endpoint);
        let sequence = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |duration| {
                u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
            });
        let host = host.as_str();
        let connections =
            futures::future::try_join_all((0..self.connections.pool_size()).map(|_| async move {
                let connection = self.server.connect_for_requests(host, port).await?;
                let accepted = match identity {
                    Some(identity) => self.confirm_peer(&connection, identity, sequence).await?,
                    None => false,
                };
                Ok((connection, accepted))
            }))
            .await;
        match connections {
            Ok(connections) if self.peer_identity(endpoint) == identity => {
                let confirmed = connections.iter().all(|(_, accepted)| *accepted);
                let priority = identity
                    .filter(|_| confirmed)
                    .map_or(u128::MAX, |(group, source, _)| peer_priority(group, source));
                let connections: Vec<_> = connections
                    .into_iter()
                    .map(|(connection, _)| connection)
                    .collect();
                self.connections
                    .publish_peer_connections(endpoint, priority, sequence, &connections);
            }
            Ok(_) => {}
            Err(error) => {
                return self
                    .connections
                    .get(endpoint)
                    .ok_or_else(|| rpc_error_to_px(error));
            }
        }
        self.connections
            .get(endpoint)
            .ok_or_else(|| rpc_error_to_px(RpcError::ConnectionClosed))
    }

    pub(super) async fn connect_snapshot_peer(
        &self,
        endpoint: &str,
    ) -> Result<SelectedConnection, PxReplicaError> {
        let (host, port) = parse_endpoint(endpoint).map_err(PxReplicaError::Internal)?;
        let port = self.peer_listener_port(&host, port).await?;
        let connection = self
            .server
            .connect_native_route(&host, port)
            .await
            .map_err(rpc_error_to_px)?;
        self.rpc.attach(&connection);
        self.connections
            .publish_peer_connections(endpoint, u128::MAX, 0, &[connection]);
        self.connections
            .get(endpoint)
            .ok_or_else(|| rpc_error_to_px(RpcError::ConnectionClosed))
    }

    async fn peer_listener_port(&self, host: &str, port: i32) -> Result<i32, PxReplicaError> {
        let connection = self.server.connect_tcp(host, port).map_err(rpc_error_to_px)?;
        self.rpc.attach(&connection);
        let id = self.next_id();
        let mut builder = FlatBufferBuilder::new();
        let request = FBPeerListenerRequest::create(
            &mut builder,
            &FBPeerListenerRequestArgs {
                id,
                rpc_create_nano: 0,
            },
        );
        builder.finish(request, None);
        let response = self
            .rpc
            .call(
                &self.server,
                &connection,
                id,
                Buffer::from_bytes(builder.finished_data()),
                None,
                LISTENER_REQUEST,
            )
            .map_err(rpc_error_to_px)?
            .await
            .map_err(rpc_error_to_px)?;
        response
            .control
            .as_ref()
            .and_then(|buffer| flatbuffers::root::<FBPeerListenerResponse>(buffer.bytes()).ok())
            .filter(|reply| reply.id() == id && reply.port() != 0)
            .map(|reply| i32::from(reply.port()))
            .ok_or_else(|| PxReplicaError::Transport("Paxos peer listener discovery returned no port".into()))
    }

    async fn confirm_peer(
        &self,
        connection: &Connection,
        (group_id, source_replica_id, target_replica_id): (u64, u64, u64),
        pool_sequence: u64,
    ) -> Result<bool, RpcError> {
        let id = self.next_id();
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let request = FBPeerConnectionRegistration::create(
            &mut builder,
            &FBPeerConnectionRegistrationArgs {
                id,
                rpc_create_nano: 0,
                group_id,
                source_replica_id,
                target_replica_id,
                target_cluster_id,
                pool_sequence,
            },
        );
        builder.finish(request, None);
        let reply = self
            .rpc
            .call(
                &self.server,
                connection,
                id,
                Buffer::from_bytes(builder.finished_data()),
                None,
                REGISTER_PEER,
            )?
            .await?;
        reply
            .control
            .as_ref()
            .and_then(|buffer| flatbuffers::root::<FBPeerConnectionResponse>(buffer.bytes()).ok())
            .filter(|reply| {
                reply.id() == id
                    && reply.pool_sequence() == pool_sequence
                    && reply.target_replica_id() == target_replica_id
            })
            .map(|reply| reply.accepted())
            .ok_or(RpcError::ConnectionError)
    }

    fn peer_identity(&self, endpoint: &str) -> Option<(u64, u64, u64)> {
        let store = self.peer_store.as_ref()?.upgrade()?;
        store
            .groups_snapshot()
            .values()
            .filter_map(|group| {
                let remote = group
                    .remote_replicas
                    .iter()
                    .filter_map(|remote| remote.as_real())
                    .find(|remote| normalize_endpoint(remote.endpoint_str().as_str()) == endpoint)?;
                Some((group.group_id(), group.local_replica().id, remote.node_id))
            })
            .min()
    }

    fn register_peer(&self, request: ServerRequest) {
        let Ok(registration) = flatbuffers::root::<FBPeerConnectionRegistration>(request.control()) else {
            return;
        };
        let accepted = self.publish_accepted_peer(&request, registration);
        let mut builder = FlatBufferBuilder::new();
        let response = FBPeerConnectionResponse::create(
            &mut builder,
            &FBPeerConnectionResponseArgs {
                id: request.request_id,
                rpc_create_nano: request.rpc_create_nano,
                pool_sequence: registration.pool_sequence(),
                target_replica_id: registration.target_replica_id(),
                accepted,
            },
        );
        builder.finish(response, None);
        let _ = request.reply(Buffer::from_bytes(builder.finished_data()), None, PEER_RESPONSE);
    }

    fn publish_accepted_peer(
        &self,
        request: &ServerRequest,
        registration: FBPeerConnectionRegistration<'_>,
    ) -> bool {
        if registration.target_cluster_id() != self.cluster_id.as_deref() {
            return false;
        }
        let Some(store) = self.peer_store.as_ref().and_then(std::sync::Weak::upgrade) else {
            return false;
        };
        let Some(group) = store.get_group(registration.group_id()) else {
            return false;
        };
        if group.local_replica().id != registration.target_replica_id() {
            return false;
        }
        let Some(remote) = group
            .remote_replicas
            .iter()
            .filter_map(|remote| remote.as_real())
            .find(|remote| remote.node_id == registration.source_replica_id())
        else {
            return false;
        };
        let endpoint = remote.endpoint_str();
        if endpoint.is_empty() {
            return false;
        }
        let Ok(connection) = request.retain_connection() else {
            return false;
        };
        self.connections.publish_peer_connections(
            &normalize_endpoint(&endpoint),
            peer_priority(registration.group_id(), registration.source_replica_id()),
            registration.pool_sequence(),
            &[connection],
        );
        true
    }
}

fn peer_priority(group: u64, initiator: u64) -> u128 {
    (u128::from(group) << 64) | u128::from(initiator)
}
