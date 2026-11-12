// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! R32 integration tests: exercise the KV consensus hot path over
//! crowdb-rpc (flatbuffer transport) instead of the legacy transport.

mod common;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::group_operations::{encode_mutations, KvGroupMutation};
use crowdb_paxos_kv::cluster::paxos_kv_server::PaxosKvServer;
use crowdb_paxos_kv::cluster::replica::PxReplicaError;
use crowdb_paxos_kv::cluster::replica::ReplicaClient;
use crowdb_paxos_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_paxos_kv::kv::{Batch, Op};
use crowdb_paxos_kv::paxos::roles::{PxBallot, PxPrepareReply};
use crowdb_paxos_kv::rpc::PxRpcTransport;
use crowdb_paxos_kv_client::PaxosKvRpcTransport;
use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::kv_client_fb::{
    FBKvClientRetCode, FBKvResponse, FBKvWriteRequest, FBKvWriteRequestArgs,
};
use crowdb_rpc_ffi::{Buffer, RpcClient, RpcError, RpcServer};

use common::logging::init_test_subscriber;
use common::net_lock::lock;

/// Cluster handle — holds the net lock until dropped.
struct CrowdbRpcCluster {
    leader: Arc<PxKvStore>,
    follower: Arc<PxKvStore>,
    _net: tokio::sync::MutexGuard<'static, ()>,
}

/// Start a 2-node cluster where consensus RPCs flow over crowdb-rpc.
async fn start_crowdb_rpc_cluster() -> CrowdbRpcCluster {
    start_crowdb_rpc_cluster_at(Ipv4Addr::LOCALHOST.into()).await
}

async fn start_crowdb_rpc_cluster_at(address: IpAddr) -> CrowdbRpcCluster {
    init_test_subscriber();
    let net = lock().await;

    let leader_id: u64 = 1;
    let follower_id: u64 = 2;

    let bind = SocketAddr::new(address, 0);
    let leader_store = Arc::new(PxKvStore::new(leader_id, bind));
    let follower_store = Arc::new(PxKvStore::new(follower_id, bind));

    leader_store.start().await.expect("leader start");
    follower_store.start().await.expect("follower start");

    let leader_transport = leader_store.rpc_transport().expect("leader transport");
    let follower_transport = follower_store.rpc_transport().expect("follower transport");

    let leader_endpoint = leader_store
        .listen_addr()
        .expect("leader listen_addr")
        .to_string();
    let follower_endpoint = follower_store
        .listen_addr()
        .expect("follower listen_addr")
        .to_string();

    // Leader group: local replica is Leader, remote is follower.
    let leader_replica = PxLocalReplica::new(leader_id, PxLocalReplicaRole::Leader);
    let follower_remote =
        PxRemoteReplica::new(follower_id, follower_endpoint).with_rpc_transport(leader_transport);
    let mut leader_group = PxGroup::new(1, leader_replica);
    leader_group.set_remote_replicas(vec![follower_remote]);
    leader_store.add_group(leader_group);

    // Follower group: local replica is Follower, remote is leader.
    let follower_replica = PxLocalReplica::new(follower_id, PxLocalReplicaRole::Follower);
    follower_replica.set_believed_leader(leader_id);
    let leader_remote =
        PxRemoteReplica::new(leader_id, leader_endpoint).with_rpc_transport(follower_transport);
    let mut follower_group = PxGroup::new(1, follower_replica);
    follower_group.set_remote_replicas(vec![leader_remote]);
    follower_store.add_group(follower_group);

    CrowdbRpcCluster {
        leader: leader_store,
        follower: follower_store,
        _net: net,
    }
}

impl CrowdbRpcCluster {
    async fn shutdown(self) {
        self.leader.stop();
        self.follower.stop();
        self.leader.join().await;
        self.follower.join().await;
    }
}

#[tokio::test]
async fn crowdb_rpc_put_reuses_wire_payload_for_single_and_coalesced_rounds() {
    verify_coalesced_put(start_crowdb_rpc_cluster().await, 16 * 1024 + 1).await;
}

#[tokio::test]
async fn crowdb_rpc_oversized_accept_keeps_its_own_slot_and_spans_rdma_fragments() {
    let Some(address) = rdma_test_address() else {
        return;
    };
    let cluster = start_crowdb_rpc_cluster_at(address).await;
    let group = cluster.leader.get_group(1).unwrap();
    group.set_coalesce_max_keys_for_tests(16);
    let endpoint = cluster.leader.listen_addr().unwrap().to_string();
    let client = PaxosKvRpcTransport::new();
    let warmup = client
        .send_put(&endpoint, b"warmup", b"", 10, 1, 50, 10, 1)
        .await
        .unwrap();
    assert!(warmup.ok);

    let (release, gate) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate);
    let large: Vec<u8> = (0..=250).cycle().take(1024 * 1024 + 17).collect();
    let first = client.send_put(&endpoint, b"first", b"", 11, 1, 51, 10, 1);
    let following = async {
        while !group.has_coalesce_pending_for_tests() {
            tokio::task::yield_now().await;
        }
        let small = client.send_put(&endpoint, b"small", b"", 12, 1, 52, 11, 1);
        let oversized = async {
            while group.coalesce_pending_count_for_tests() != 1 {
                tokio::task::yield_now().await;
            }
            client
                .send_put(&endpoint, b"large", &large, 13, 1, 53, 12, 1)
                .await
        };
        let (small, oversized) = tokio::join!(small, oversized);
        release.send(()).unwrap();
        (small.unwrap(), oversized.unwrap())
    };
    let (first, (small, oversized)) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(first, following)
    })
    .await
    .expect("oversized PUT did not close the preceding batch");
    assert!(first.unwrap().ok && small.ok && oversized.ok);
    assert_ne!(small.revision, oversized.revision);
    let follower = cluster.follower.get_group(1).unwrap();
    let large_entry = follower
        .local_replica()
        .accepted_at(oversized.revision)
        .await
        .unwrap();
    let batch = Batch::try_decode(large_entry.payload.contiguous().unwrap()).unwrap();
    assert_eq!(batch.ops.len(), 1);
    assert_eq!(batch.ops[0].key.as_ref(), b"large");
    assert_eq!(batch.ops[0].op, Op::Put(large.into()));
    drop(client);
    cluster.shutdown().await;
}

#[tokio::test]
async fn crowdb_rpc_rdma_accept_batches_follow_encoded_bytes() {
    let Some(address) = rdma_test_address() else {
        return;
    };
    let cluster = start_crowdb_rpc_cluster_at(address).await;
    let group = cluster.leader.get_group(1).unwrap();
    group.set_coalesce_max_keys_for_tests(16);
    let endpoint = cluster.leader.listen_addr().unwrap().to_string();
    let client = PaxosKvRpcTransport::new();
    assert!(
        client
            .send_put(&endpoint, b"warmup", b"", 1, 1, 101, 0, 1)
            .await
            .unwrap()
            .ok
    );
    let fragments = || {
        let status: serde_json::Value =
            serde_json::from_slice(&cluster.leader.rpc_transport_status().unwrap()).unwrap();
        status["rdma"]["control_fragments_sent"].as_u64().unwrap()
    };
    let cases: [(&[usize], usize); 3] = [(&[16; 16], 15), (&[64; 4], 3), (&[64, 16, 64, 16, 64, 16, 64], 6)];
    let payload = vec![0x53; 64 * 1024];
    for (case, (sizes, expected)) in cases.into_iter().enumerate() {
        let id_base = u64::try_from(case).unwrap() * 100 + 2;
        let before = fragments();
        let (release, gate) = tokio::sync::oneshot::channel();
        group.set_coalesce_round_gate_for_tests(gate);
        let blocker = client.send_put(&endpoint, b"blocker", b"", id_base, 1, 100 + id_base, 0, 1);
        let batch = async {
            while !group.has_coalesce_pending_for_tests() {
                tokio::task::yield_now().await;
            }
            let client = &client;
            let endpoint = &endpoint;
            let payload = &payload;
            let group = &group;
            let replies =
                futures::future::join_all(sizes.iter().enumerate().map(|(index, size)| async move {
                    while usize::from(group.coalesce_pending_count_for_tests()) != index {
                        tokio::task::yield_now().await;
                    }
                    let id = id_base + u64::try_from(index).unwrap() + 1;
                    client
                        .send_put(
                            endpoint,
                            &id.to_le_bytes(),
                            &payload[..size * 1024],
                            id,
                            1,
                            100 + id,
                            0,
                            1,
                        )
                        .await
                        .unwrap()
                }))
                .await;
            release.send(()).unwrap();
            replies
        };
        let (blocker, replies) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            tokio::join!(blocker, batch)
        })
        .await
        .unwrap();
        assert!(blocker.unwrap().ok);
        let mut revisions = std::collections::BTreeMap::<u64, usize>::new();
        for reply in replies {
            assert!(reply.ok);
            *revisions.entry(reply.revision).or_default() += 1;
        }
        let mut counts: Vec<_> = revisions.values().copied().collect();
        counts.sort_unstable();
        assert_eq!(counts, [1, expected]);
        assert_eq!(fragments() - before, 2, "one SEND per nonempty Accept payload");
    }
    cluster.shutdown().await;
}

async fn verify_coalesced_put(cluster: CrowdbRpcCluster, coalesced_value_len: usize) {
    let group = cluster.leader.get_group(1).unwrap();
    group.set_coalesce_max_keys_for_tests(16);
    let endpoint = cluster.leader.listen_addr().unwrap().to_string();
    let client = PaxosKvRpcTransport::new();
    let (release, gate) = tokio::sync::oneshot::channel();
    group.set_coalesce_round_gate_for_tests(gate);
    let first_value: Vec<u8> = (0..=250).cycle().take(128 * 1024 + 1).collect();
    let third_value: Vec<u8> = (0..=250).cycle().take(coalesced_value_len).collect();
    let first = client.send_put(&endpoint, b"key", &first_value, 1, 1, 41, 10, 1);
    let following = async {
        while !group.has_coalesce_pending_for_tests() {
            tokio::task::yield_now().await;
        }
        let second = client.send_put(&endpoint, b"second", b"", 2, 1, 42, 11, 1);
        let third = async {
            while group.coalesce_pending_count_for_tests() != 1 {
                tokio::task::yield_now().await;
            }
            client
                .send_put(&endpoint, b"third", &third_value, 3, 1, 43, 12, 1)
                .await
        };
        let release = async {
            while group.coalesce_pending_count_for_tests() != 2 {
                tokio::task::yield_now().await;
            }
            release.send(()).unwrap();
        };
        let (second, third, ()) = tokio::join!(second, third, release);
        (second.unwrap(), third.unwrap())
    };
    let (first, (second, third)) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(first, following)
    })
    .await
    .expect("PUT rounds did not complete");
    let first = first.unwrap();
    assert!(first.ok && second.ok && third.ok);
    assert_ne!(first.revision, second.revision);
    assert_eq!(second.revision, third.revision);
    let retry = client
        .send_put(&endpoint, b"key", b"replacement", 1, 1, 44, 13, 1)
        .await
        .unwrap();
    assert!(retry.ok);
    assert_eq!(retry.revision, first.revision);
    drop(client);

    let first = group.local_replica().accepted_at(first.revision).await.unwrap();
    let follower = cluster.follower.get_group(1).unwrap();
    let follower_first = follower.local_replica().accepted_at(first.slot).await.unwrap();
    assert_eq!(follower_first.payload, first.payload);
    for accepted in [&first, &follower_first] {
        let allocation = Buffer::control_allocation(accepted.payload.contiguous().unwrap()).unwrap();
        let (_, initialized, capacity) = allocation.parts();
        assert!(capacity >= initialized.len());
        assert!(initialized.len() > accepted.payload.len());
    }
    let first = Batch::try_decode(first.payload.contiguous().unwrap()).unwrap();
    assert_eq!(first.ops.len(), 1);
    assert_eq!(first.ops[0].key.as_ref(), b"key");
    assert_eq!(first.ops[0].op, Op::Put(first_value.into()));
    let second = group.local_replica().accepted_at(second.revision).await.unwrap();
    let follower_second = follower.local_replica().accepted_at(second.slot).await.unwrap();
    let expected = encode_mutations(&[
        KvGroupMutation::Put {
            key: bytes::Bytes::from_static(b"second"),
            value: bytes::Bytes::new(),
        },
        KvGroupMutation::Put {
            key: bytes::Bytes::from_static(b"third"),
            value: third_value.into(),
        },
    ]);
    assert!(second.payload.contiguous().is_none());
    assert!(follower_second.payload.contiguous().is_some());
    assert_eq!(second.payload.to_bytes().as_ref(), expected.as_slice());
    assert_eq!(follower_second.payload, second.payload);
    let record = crowdb_paxos_kv::wal::record::WALRecord::from_accepted(1, &second);
    let canonical_record = crowdb_paxos_kv::wal::record::WALRecord::from_accepted(1, &follower_second);
    assert_eq!(record.encode(), canonical_record.encode());
    assert_eq!(record.encode_text_line(), canonical_record.encode_text_line());
    let frame = record.encode_frame();
    let mut slices = Vec::new();
    frame.append_io_slices(&mut slices);
    assert_eq!(slices.len(), 5, "WAL retains both original record slices");
    let follower_remote = group.get_remote_replica(2).unwrap();
    assert!(matches!(
        follower_remote.send_accept(&second, 1, group.membership_epoch()).await.unwrap(),
        crowdb_paxos_kv::paxos::roles::PxAcceptReply::Accepted { slot, .. } if slot == second.slot
    ));
    cluster.shutdown().await;
}

#[tokio::test]
async fn crowdb_rpc_put_rejects_invalid_consensus_payloads_before_proposing() {
    let cluster = start_crowdb_rpc_cluster().await;
    let client = RpcServer::with_engines(None, 1, 1);
    client.start();
    let connection = client
        .connect(
            "127.0.0.1",
            i32::from(cluster.leader.listen_addr().unwrap().port()),
        )
        .unwrap();
    let rpc = RpcClient::new();
    rpc.attach(&connection);
    let group = cluster.leader.get_group(1).unwrap();
    let replica = group.local_replica();
    let chosen = replica.contiguous_chosen();
    let put = FBMsgType::EKvPutRequest;
    let batch = FBMsgType::EKvBatchPayloadRequest;
    for (id, (msg_type, payload)) in [
        (put, &[][..]),
        (put, &[1, 0, 0, 4, 0, 0, 0, b'k'][..]),
        (put, &[1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0][..]),
        (put, &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0][..]),
        (put, &[1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0][..]),
        (
            put,
            &[2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0][..],
        ),
        (
            batch,
            &[2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 0][..],
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let id = id as u64 + 1;
        let mut builder = flatbuffers::FlatBufferBuilder::new();
        let payload = builder.create_vector(payload);
        let request = FBKvWriteRequest::create(
            &mut builder,
            &FBKvWriteRequestArgs {
                id,
                group_id: 1,
                payload: Some(payload),
                ..FBKvWriteRequestArgs::default()
            },
        );
        builder.finish(request, None);
        let (bytes, head) = builder.collapse();
        let reply = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            rpc.call(
                &client,
                &connection,
                id,
                Buffer::from_vec_offset(bytes, head),
                None,
                u16::try_from(msg_type.0).unwrap(),
            )
            .unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
        let control = reply.control.unwrap();
        let response = flatbuffers::root::<FBKvResponse>(control.bytes()).unwrap();
        assert_eq!(response.ret_code(), FBKvClientRetCode::InvalidArgument);
    }
    assert_eq!(replica.contiguous_chosen(), chosen);
    assert!(replica.accepted_at(chosen + 1).await.is_none());
    client.stop();
    cluster.shutdown().await;
}

#[tokio::test]
async fn recovered_routes_preserve_membership_and_reject_reused_peer_addresses() {
    let cluster = start_crowdb_rpc_cluster().await;
    let group = cluster.leader.get_group(1).unwrap();
    let before: Vec<_> = group
        .remote_replica_info()
        .into_iter()
        .map(|(id, endpoint, voting)| (id, endpoint.to_owned(), voting))
        .collect();
    let quorum = group.quorum();
    let wrong = cluster.leader.listen_addr().unwrap();
    group.set_recovery_routes(&std::collections::BTreeMap::from([(2, wrong)]));
    let remote = group.get_remote_replica(2).unwrap();
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(200),
            remote.send_prepare(91, PxBallot::new(5, 1), 0, 1, 0)
        )
        .await
        .is_err(),
        "a live endpoint hosting another replica must not contribute a promise"
    );
    group.set_recovery_routes(&std::collections::BTreeMap::new());
    assert!(
        remote
            .send_prepare(92, PxBallot::new(5, 1), 0, 1, 0)
            .await
            .is_err(),
        "missing proof cannot fall back to the original IP"
    );
    group.set_recovery_routes(&std::collections::BTreeMap::from([(
        2,
        cluster.follower.listen_addr().unwrap(),
    )]));
    assert!(remote
        .send_prepare(93, PxBallot::new(5, 1), 0, 1, 0)
        .await
        .is_ok());
    let after: Vec<_> = group
        .remote_replica_info()
        .into_iter()
        .map(|(id, endpoint, voting)| (id, endpoint.to_owned(), voting))
        .collect();
    assert_eq!(before, after);
    assert_eq!(group.quorum(), quorum);
    cluster.shutdown().await;
}

#[tokio::test]
async fn crowdb_rpc_prepare_accept_roundtrip() {
    verify_prepare_accept_roundtrip(start_crowdb_rpc_cluster().await, 4 * 1024 * 1024 + 17).await;
}

#[tokio::test]
async fn crowdb_rpc_prepare_accept_rdma_roundtrip() {
    let Some(address) = rdma_test_address() else {
        return;
    };
    verify_prepare_accept_roundtrip(start_crowdb_rpc_cluster_at(address).await, 16 * 1024 + 17).await;
}

#[tokio::test]
async fn paxos_peers_share_one_tcp_pool() {
    verify_peer_connection_reuse(start_crowdb_rpc_cluster().await).await;
}

#[tokio::test]
async fn paxos_peers_share_one_rdma_pool() {
    let Some(address) = rdma_test_address() else {
        return;
    };
    verify_peer_connection_reuse(start_crowdb_rpc_cluster_at(address).await).await;
}

async fn verify_peer_connection_reuse(cluster: CrowdbRpcCluster) {
    use crowdb_protocol::kv_consensus_fb::{FBPeerConnectionRegistration, FBPeerConnectionRegistrationArgs};
    use std::time::Duration;

    let leader = cluster.leader.get_group(1).unwrap();
    let follower = cluster.follower.get_group(1).unwrap();
    let to_follower = leader.get_remote_replica(2).unwrap();
    let to_leader = follower.get_remote_replica(1).unwrap();
    let endpoint = cluster.leader.listen_addr().unwrap();
    let client = RpcServer::new(None);
    client.start();
    let unrelated = client
        .connect(&endpoint.ip().to_string(), endpoint.port().into())
        .unwrap();
    let rpc = RpcClient::new();
    rpc.attach(&unrelated);
    let mut builder = flatbuffers::FlatBufferBuilder::new();
    let cluster_id = std::env::var("CROWDB_PAXOS_KV_CLUSTER_ID").ok();
    let target_cluster_id = cluster_id.as_deref().map(|id| builder.create_string(id));
    let registration = FBPeerConnectionRegistration::create(
        &mut builder,
        &FBPeerConnectionRegistrationArgs {
            id: 1,
            group_id: 1,
            source_replica_id: 2,
            target_replica_id: 999,
            target_cluster_id,
            pool_sequence: 1,
            ..Default::default()
        },
    );
    builder.finish(registration, None);
    rpc.send_one_way(
        &client,
        &unrelated,
        1,
        Buffer::from_bytes(builder.finished_data()),
        None,
        FBMsgType::EPeerConnectionRegistration.0 as u16,
    )
    .unwrap();

    let (forward, reverse) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(
            to_follower.send_prepare(11, PxBallot::new(1, 1), 0, 1, 0),
            to_leader.send_prepare(12, PxBallot::new(1, 2), 0, 1, 0),
        )
    })
    .await
    .unwrap();
    assert!(matches!(
        forward.unwrap(),
        PxPrepareReply::Promised { slot: 11, .. }
    ));
    assert!(matches!(
        reverse.unwrap(),
        PxPrepareReply::Promised { slot: 12, .. }
    ));
    unrelated.close();
    client.stop();

    let (leader_connection, follower_connection) = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let (Some(a), Some(b)) = (connection_ids(&cluster.leader), connection_ids(&cluster.follower)) {
                break (a, b);
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("both directions should converge on the same connection");
    assert!(to_leader
        .send_prepare(13, PxBallot::new(1, 2), 0, 1, 0)
        .await
        .is_ok());
    assert_eq!(connection_ids(&cluster.leader).as_ref(), Some(&leader_connection));
    assert_eq!(connection_ids(&cluster.follower), Some(follower_connection));

    verify_peer_reconnect(&cluster, to_follower, &leader_connection).await;
    cluster.shutdown().await;
}

fn connection_ids(store: &PxKvStore) -> Option<Vec<u64>> {
    let snapshot: serde_json::Value = serde_json::from_slice(&store.rpc_transport_status().unwrap()).unwrap();
    let connections = snapshot["connections"].as_array().unwrap();
    (connections.len() == 2).then(|| {
        let mut ids: Vec<_> = connections
            .iter()
            .map(|connection| connection["connection_id"].as_u64().unwrap())
            .collect();
        ids.sort_unstable();
        ids
    })
}

async fn verify_peer_reconnect(
    cluster: &CrowdbRpcCluster,
    to_follower: &PxRemoteReplica,
    old_connections: &[u64],
) {
    use std::time::Duration;

    let endpoint = cluster.leader.listen_addr().unwrap();
    let follower_address = cluster.follower.listen_addr().unwrap();
    cluster.follower.stop();
    cluster.follower.join().await;
    let restarted = Arc::new(PxKvStore::new(2, follower_address));
    let mut restarted_group = PxGroup::new(1, PxLocalReplica::new(2, PxLocalReplicaRole::Follower));
    restarted_group.set_remote_replicas(vec![PxRemoteReplica::new(1, endpoint.to_string())]);
    restarted.add_group_without_election(restarted_group);
    restarted.start().await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if to_follower
                .send_prepare(14, PxBallot::new(1, 1), 0, 1, 0)
                .await
                .is_ok()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("forward requests should reconnect after restart");
    let restarted_group = restarted.get_group(1).unwrap();
    let reverse = restarted_group.get_remote_replica(1).unwrap();
    assert!(reverse
        .send_prepare(15, PxBallot::new(1, 2), 0, 1, 0)
        .await
        .is_ok());
    assert!(connection_ids(&restarted).is_some());
    assert_ne!(connection_ids(&cluster.leader).unwrap(), old_connections);
    restarted.stop();
    restarted.join().await;
}

fn rdma_test_address() -> Option<IpAddr> {
    std::env::var_os("CROWDB_RDMA_E2E")?;
    assert_eq!(std::env::var("CROWDB_RDMA_ENABLED").as_deref(), Ok("1"));
    assert_eq!(
        std::env::var("CROWDB_RDMA_ALLOW_TCP_FALLBACK").as_deref(),
        Ok("0")
    );
    Some(
        std::env::var("CROWDB_RDMA_E2E_LISTEN_ADDR")
            .unwrap()
            .parse()
            .unwrap(),
    )
}

async fn verify_prepare_accept_roundtrip(cluster: CrowdbRpcCluster, payload_len: usize) {
    // Get the leader's remote replica (points at follower).
    let leader_group = cluster.leader.get_group(1).expect("leader group");
    let follower_remote = leader_group.get_remote_replica(2).expect("follower remote");

    // Send a Prepare for slot 1, ballot (1, leader_id=1).
    let ballot = PxBallot::new(1, 1);
    let prepare_result = follower_remote.send_prepare(1, ballot, 0, 1, 0).await;
    assert!(
        prepare_result.is_ok(),
        "prepare should succeed over crowdb-rpc: {:?}",
        prepare_result.err()
    );
    match prepare_result.unwrap() {
        PxPrepareReply::Promised { slot, .. } => {
            assert_eq!(slot, 1, "promised slot should be 1");
        }
        other => panic!("expected Promised, got {other:?}"),
    }

    // Send an Accept for slot 1.
    let entry = crowdb_paxos_kv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot,
        term: 0,
        payload: (0..=250).cycle().take(payload_len).collect::<Vec<u8>>().into(),
    };
    let accept_result = follower_remote.send_accept(&entry, 1, 0).await;
    assert!(
        accept_result.is_ok(),
        "accept should succeed over crowdb-rpc: {:?}",
        accept_result.err()
    );
    match accept_result.unwrap() {
        crowdb_paxos_kv::paxos::roles::PxAcceptReply::Accepted { slot, .. } => {
            assert_eq!(slot, 1, "accepted slot should be 1");
        }
        other => panic!("expected Accepted, got {other:?}"),
    }
    assert!(matches!(
        follower_remote.send_accept(&entry, 1, 0).await.unwrap(),
        crowdb_paxos_kv::paxos::roles::PxAcceptReply::Accepted { slot: 1, .. }
    ));

    // Verify the follower actually stored the value.
    let follower_group = cluster.follower.get_group(1).expect("follower group");
    let follower_replica = follower_group.local_replica();
    let accepted = follower_replica.accepted_at(1).await;
    assert!(accepted.is_some(), "follower should have accepted slot 1");
    let accepted = accepted.unwrap();
    assert_eq!(
        accepted.payload.to_bytes().as_ref(),
        entry.payload.to_bytes().as_ref(),
        "follower should have the correct payload"
    );

    cluster.shutdown().await;
}

#[tokio::test]
async fn crowdb_rpc_chosen_notification_fire_and_forget() {
    let cluster = start_crowdb_rpc_cluster().await;

    let leader_group = cluster.leader.get_group(1).expect("leader group");
    let follower_remote = leader_group.get_remote_replica(2).expect("follower remote");

    // First, Prepare + Accept slot 1 so the follower has a value.
    let ballot = PxBallot::new(1, 1);
    let entry = crowdb_paxos_kv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot,
        term: 0,
        // One KV Put: count, kind, key length/key, value length/value.
        payload: bytes::Bytes::from_static(b"\x01\x00\x00\x03\x00\x00\x00key\x05\x00\x00\x00value").into(),
    };
    follower_remote
        .send_prepare(1, ballot, 0, 1, 0)
        .await
        .expect("prepare");
    follower_remote.send_accept(&entry, 1, 0).await.expect("accept");

    let follower_group = cluster.follower.get_group(1).expect("follower group");
    let follower_replica = follower_group.local_replica();
    assert_eq!(follower_replica.learner.contiguous_chosen(), 0);
    assert_eq!(follower_replica.learner.contiguous_applied(), 0);

    // Send a fire-and-forget ChosenNotification.
    let result = follower_remote.send_chosen_notice(1, 0, 1, 1, ballot.round);
    assert!(
        result.is_ok(),
        "chosen notification should succeed: {:?}",
        result.err()
    );

    // Accept alone leaves both frontiers at zero. The notification must
    // causally advance chosen and wake the apply path.
    let poll_start = std::time::Instant::now();
    loop {
        if follower_replica.learner.contiguous_applied() >= 1 {
            break;
        }
        assert!(
            poll_start.elapsed() < std::time::Duration::from_secs(5),
            "chosen notice did not advance frontiers: chosen={}, applied={}",
            follower_replica.learner.contiguous_chosen(),
            follower_replica.learner.contiguous_applied()
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert_eq!(follower_replica.learner.contiguous_chosen(), 1);
    assert_eq!(
        follower_replica.learner.engine_get(b"key").await,
        Some((1, b"value".to_vec()))
    );

    cluster.shutdown().await;
}

#[test]
fn crowdb_rpc_errors_preserve_pressure_and_connection_health() {
    assert!(matches!(
        PxRpcTransport::classify_error_for_tests(RpcError::SendQueueFull),
        PxReplicaError::Backpressure(_)
    ));
    assert!(!PxRpcTransport::error_invalidates_connection_for_tests(
        RpcError::SendQueueFull
    ));
    assert!(!PxRpcTransport::error_invalidates_connection_for_tests(
        RpcError::Timeout
    ));
    assert!(PxRpcTransport::error_invalidates_connection_for_tests(
        RpcError::ConnectionClosed
    ));
    assert!(matches!(
        PxRpcTransport::classify_error_for_tests(RpcError::Timeout),
        PxReplicaError::Timeout(_)
    ));
}

#[tokio::test]
async fn crowdb_rpc_fetch_gap() {
    let cluster = start_crowdb_rpc_cluster().await;

    // Prepare + Accept slot 1 on both leader (locally) and follower (via RPC).
    let ballot = PxBallot::new(1, 1);
    let entry = crowdb_paxos_kv::paxos::roles::PxLogEntry {
        slot: 1,
        ballot,
        term: 0,
        payload: bytes::Bytes::from_static(b"gap-test").into(),
    };

    // Leader accepts locally.
    let leader_group = cluster.leader.get_group(1).expect("leader group");
    let leader_replica = leader_group.local_replica();
    let prepare_reply = leader_replica.on_prepare(1, ballot, 0).await;
    assert!(
        matches!(prepare_reply, PxPrepareReply::Promised { .. }),
        "leader prepare should succeed: {prepare_reply:?}"
    );
    let accept_reply = leader_replica.on_accept(&entry).await;
    assert!(
        matches!(
            accept_reply,
            crowdb_paxos_kv::paxos::roles::PxAcceptReply::Accepted { .. }
        ),
        "leader accept should succeed: {accept_reply:?}"
    );

    // Follower accepts via crowdb-rpc.
    let follower_remote = leader_group.get_remote_replica(2).expect("follower remote");
    follower_remote
        .send_prepare(1, ballot, 0, 1, 0)
        .await
        .expect("follower prepare");
    let accepted = follower_remote
        .send_accept(&entry, 1, 0)
        .await
        .expect("follower accept");
    assert!(matches!(
        accepted,
        crowdb_paxos_kv::paxos::roles::PxAcceptReply::Accepted { .. }
    ));
    // Both voters have accepted. Publish that quorum decision before serving
    // the chosen value; an Accepted response alone is not a learner proof.
    leader_replica.learn_chosen(&entry, &[]).await;

    // Use the follower's remote (points at leader) to fetch gap.
    let follower_group = cluster.follower.get_group(1).expect("follower group");
    let leader_remote = follower_group.get_remote_replica(1).expect("leader remote");

    // FetchGap from the leader for slot 1.
    let result = leader_remote.send_fetch_gap(1, 0, 1, 1).await;
    assert!(result.is_ok(), "fetch_gap should succeed: {:?}", result.err());
    let reply = result.unwrap();
    assert_eq!(reply.slot, 1, "fetch_gap reply slot");
    assert_eq!(reply.payload.as_ref(), b"gap-test", "fetch_gap reply payload");

    cluster.shutdown().await;
}
