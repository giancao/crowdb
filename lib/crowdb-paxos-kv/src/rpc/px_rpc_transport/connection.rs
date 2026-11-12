// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{FBKvRetCode, PxReplicaError, PxRpcTransport, RpcError, SnapshotTransferError};

impl PxRpcTransport {
    pub(crate) fn clear_connections(&self) {
        self.connections.clear();
    }
}

// ── Error mapping ────────────────────────────────────────────────

pub(super) fn rpc_error_to_px(e: RpcError) -> PxReplicaError {
    match e {
        RpcError::Timeout => PxReplicaError::Timeout("crowdb-rpc deadline expired".into()),
        RpcError::SendQueueFull => PxReplicaError::Backpressure("crowdb-rpc send queues are full".into()),
        RpcError::ConnectionClosed | RpcError::ConnectionError | RpcError::AllDown => {
            PxReplicaError::Transport(format!("crowdb-rpc error: {e:?}"))
        }
        RpcError::Ok | RpcError::RegistrationFailed | RpcError::InvalidArg | RpcError::Unknown(_) => {
            PxReplicaError::Internal(format!("crowdb-rpc error: {e:?}"))
        }
    }
}

pub(super) fn rpc_error_invalidates_connection(e: RpcError) -> bool {
    matches!(e, RpcError::ConnectionClosed | RpcError::ConnectionError)
}

pub(super) fn check_ret_code(code: FBKvRetCode, msg: Option<&str>) -> Result<(), PxReplicaError> {
    match code {
        FBKvRetCode::Success => Ok(()),
        FBKvRetCode::NotFound => Err(PxReplicaError::GroupNotFound(0)),
        FBKvRetCode::Unavailable => Err(PxReplicaError::ShuttingDown),
        FBKvRetCode::Internal | FBKvRetCode::InvalidArgument => {
            Err(PxReplicaError::Internal(msg.unwrap_or("internal error").into()))
        }
        _ => Err(PxReplicaError::Internal(format!("unknown ret_code: {code:?}"))),
    }
}

pub(super) fn check_snapshot_ret_code(
    code: FBKvRetCode,
    msg: Option<&str>,
) -> Result<(), SnapshotTransferError> {
    let message = msg.unwrap_or("remote snapshot failure").to_string();
    match code {
        FBKvRetCode::Success => Ok(()),
        FBKvRetCode::SnapshotNotFound | FBKvRetCode::NotFound => {
            Err(SnapshotTransferError::NotFound(message))
        }
        FBKvRetCode::SnapshotExpired => Err(SnapshotTransferError::Expired(message)),
        FBKvRetCode::SnapshotInvalidOffset => Err(SnapshotTransferError::InvalidOffset(message)),
        FBKvRetCode::SnapshotBackpressure => Err(SnapshotTransferError::Backpressure(message)),
        FBKvRetCode::SnapshotTopologyChanged => Err(SnapshotTransferError::TopologyChanged(message)),
        FBKvRetCode::SnapshotIntegrity => Err(SnapshotTransferError::Integrity(message)),
        FBKvRetCode::Unavailable => Err(SnapshotTransferError::Transport(PxReplicaError::ShuttingDown)),
        FBKvRetCode::Internal | FBKvRetCode::InvalidArgument => Err(SnapshotTransferError::Protocol(message)),
        _ => Err(SnapshotTransferError::Protocol(format!(
            "unknown ret_code: {code:?}"
        ))),
    }
}

// ── Endpoint parsing ─────────────────────────────────────────────

/// Use the socket endpoint as the pool key. Unspecified local listeners
/// are reached through loopback.
pub(super) fn normalize_endpoint(endpoint: &str) -> std::borrow::Cow<'_, str> {
    let endpoint = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    match endpoint.strip_prefix("0.0.0.0:") {
        Some(port) => std::borrow::Cow::Owned(format!("127.0.0.1:{port}")),
        None => std::borrow::Cow::Borrowed(endpoint),
    }
}

/// Parse `http://host:port` into `(host, port)`.
pub(super) fn parse_endpoint(endpoint: &str) -> Result<(String, i32), String> {
    let without_scheme = endpoint
        .strip_prefix("http://")
        .or_else(|| endpoint.strip_prefix("https://"))
        .unwrap_or(endpoint);
    let (host, port_str) = without_scheme
        .rsplit_once(':')
        .ok_or_else(|| format!("invalid endpoint: {endpoint}"))?;
    let port: i32 = port_str
        .parse()
        .map_err(|_| format!("invalid port in endpoint: {endpoint}"))?;
    Ok((host.to_string(), port))
}

/// Logical destination remains explicit even if its IP is reassigned.
#[derive(Clone, Copy)]
pub struct ReplicaDestination<'a> {
    pub endpoint: &'a str,
    pub replica_id: u64,
}
