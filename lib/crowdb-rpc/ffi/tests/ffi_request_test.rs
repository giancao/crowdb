// Copyright 2026-present Gian <crow.db@outlook.com>.
// Licensed under the Apache License, Version 2.0.

//! E2E tests for bidirectional request-response (R114):
//! - Server→client request-response via server handler + request_client
//! - Client→server regression (existing path still works)
//! - Client response routing precedence (response routed to pending, not handler)
//! - Client handler dispatch (server sends request to client via handler chain)

use crowdb_common::RequestIdGen;
use crowdb_protocol::fb::{
    ConnectionPingRequest, ConnectionPingRequestArgs, ConnectionPingResponse, ConnectionPingResponseArgs,
    FBMsgType, FBRetCode,
};
use crowdb_rpc_ffi::{init_test_logging, Buffer, BufferPool, CallFuture, RpcClient, RpcServer};
use flatbuffers::FlatBufferBuilder;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[tokio::test]
async fn one_way_push_with_pending_request_id_does_not_complete_the_call() {
    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).unwrap();
    let push_client = Arc::new(RpcClient::new());
    server.register_handler(32001, {
        let server = Arc::clone(&server);
        move |req| {
            push_client
                .send_one_way_to_handle(
                    &server,
                    req.conn_handle,
                    req.request_id,
                    test_ping_control(1, 2),
                    None,
                    32002,
                )
                .unwrap();
            unsafe {
                server
                    .submit_response(
                        req.conn_handle,
                        test_ping_control(1, 3).as_slice(),
                        None,
                        32003,
                        req.request_id,
                    )
                    .unwrap();
            }
        }
    });
    server.start();
    let conn = server.connect("127.0.0.1", server.port()).unwrap();
    let client = RpcClient::new();
    client.attach(&conn);
    let seen = Arc::new(AtomicBool::new(false));
    client.register_handler(32002, {
        let seen = Arc::clone(&seen);
        move |req| {
            assert_eq!(req.control(), test_ping_control(1, 2).as_slice());
            seen.store(true, Ordering::Release);
        }
    });
    let call = client
        .call(&server, &conn, 1, test_ping_control(1, 1), None, 32001)
        .unwrap();
    let response = tokio::time::timeout(std::time::Duration::from_secs(3), call)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        response.control.unwrap().as_slice(),
        test_ping_control(1, 3).as_slice()
    );
    assert!(seen.load(Ordering::Acquire));
    server.stop();
}

fn test_ping_control(id: u64, tag: u64) -> Buffer {
    let mut builder = FlatBufferBuilder::new();
    let request = ConnectionPingRequest::create(
        &mut builder,
        &ConnectionPingRequestArgs {
            id,
            rpc_create_nano: tag,
        },
    );
    builder.finish(request, None);
    Buffer::from_bytes(builder.finished_data())
}

#[tokio::test]
async fn retained_connection_dispatches_same_id_in_both_directions() {
    use std::time::Duration;
    use tokio::time::timeout;

    const REGISTER: u16 = 32010;
    const REQUEST: u16 = 32011;
    const RESPONSE: u16 = 32012;
    const HOLD: u16 = 32013;
    let listener = Arc::new(RpcServer::new(None));
    let dialer = Arc::new(RpcServer::new(None));
    let listener_client = RpcClient::new();
    let dialer_client = RpcClient::new();
    listener.set_request_client(&listener_client);
    dialer.set_request_client(&dialer_client);
    listener.listen("127.0.0.1", 0).unwrap();
    let (accepted_tx, mut accepted_rx) = tokio::sync::mpsc::unbounded_channel();
    listener.register_owned_handler(REGISTER, move |request| {
        let connection = request.retain_connection().unwrap();
        drop(request);
        accepted_tx.send(connection).unwrap();
    });
    let (hold_tx, mut hold_rx) = tokio::sync::mpsc::unbounded_channel();
    dialer.register_owned_handler(HOLD, move |request| hold_tx.send(request).unwrap());
    for server in [&listener, &dialer] {
        server.register_owned_handler(REQUEST, move |request| {
            let control = Buffer::from_bytes(request.control());
            request.reply(control, None, RESPONSE).unwrap();
        });
        server.start();
    }
    let outgoing = dialer
        .connect_for_requests("127.0.0.1", listener.port())
        .await
        .unwrap();
    dialer_client
        .send_one_way(&dialer, &outgoing, 1, test_ping_control(1, 0), None, REGISTER)
        .unwrap();
    let accepted = timeout(Duration::from_secs(3), accepted_rx.recv())
        .await
        .unwrap()
        .unwrap();

    let forward = dialer_client
        .call(&dialer, &outgoing, 2, test_ping_control(2, 3), None, REQUEST)
        .unwrap();
    let reverse = listener_client
        .call(&listener, &accepted, 2, test_ping_control(2, 4), None, REQUEST)
        .unwrap();
    let (forward, reverse) = timeout(Duration::from_secs(3), async { tokio::join!(forward, reverse) })
        .await
        .unwrap();
    assert_eq!(
        forward.unwrap().control.unwrap().as_slice(),
        test_ping_control(2, 3).as_slice()
    );
    assert_eq!(
        reverse.unwrap().control.unwrap().as_slice(),
        test_ping_control(2, 4).as_slice()
    );

    let pending = listener_client
        .call(&listener, &accepted, 3, test_ping_control(3, 5), None, HOLD)
        .unwrap();
    let held = timeout(Duration::from_secs(3), hold_rx.recv())
        .await
        .unwrap()
        .unwrap();
    outgoing.close();
    assert!(matches!(
        timeout(Duration::from_secs(3), pending).await.unwrap(),
        Err(crowdb_rpc_ffi::RpcError::ConnectionClosed)
    ));
    drop(held);
    dialer.stop();
    listener.stop();
}

#[tokio::test]
async fn rdma_owned_reply_returns_request_cache_while_payload_stays_live() {
    if std::env::var_os("CROWDB_RDMA_E2E").is_none() {
        return;
    }
    use crowdb_rpc_ffi::{RdmaConfig, RdmaWireLayout};
    use std::time::Duration;
    use tokio::time::timeout;

    const SIZE: usize = 96 * 1024;
    const REQUEST_TYPE: u16 = 310;
    let address = std::env::var("CROWDB_RDMA_E2E_LISTEN_ADDR").unwrap();
    let config = RdmaConfig {
        enabled: true,
        allow_tcp_fallback: false,
        listen_enabled: true,
        device: std::env::var("CROWDB_RDMA_DEVICE").unwrap_or_default(),
        source_address: address.clone(),
        sq_depth: 4,
        rq_depth: 4,
        posted_receive_target: 4,
        cq_depth: 16,
        wire_layout: RdmaWireLayout::ControlData,
        wire_slot_capacity: 64 * 1024,
        max_control_message_bytes: SIZE as u32,
        control_receive_budget_bytes: SIZE as u64,
        control_send_budget_bytes: 2 * SIZE as u64,
        ..RdmaConfig::default()
    };
    let server = Arc::new(RpcServer::new(None));
    server.configure_rdma(&config).unwrap();
    server.listen(&address, 0).unwrap();
    let (tx, mut requests) = tokio::sync::mpsc::unbounded_channel();
    server.register_owned_handler(REQUEST_TYPE, move |mut request| {
        let address = request.control().as_ptr();
        let payload = request.take_control_bytes().unwrap();
        assert_eq!(payload.as_ptr(), address);
        let allocation = Buffer::control_allocation(&payload).unwrap();
        assert_eq!(allocation.parts().2, SIZE);
        tx.send((request, payload)).unwrap();
    });
    server.start();

    let outgoing = RpcServer::new(None);
    outgoing
        .configure_rdma(&RdmaConfig {
            listen_enabled: false,
            ..config
        })
        .unwrap();
    outgoing.start();
    let connection = outgoing.connect(&address, server.port()).unwrap();
    assert!(connection.is_rdma());
    let client = RpcClient::new();
    client.attach(&connection);
    let encoded = |id| {
        let mut bytes = test_ping_control(id, 0).as_slice().to_vec();
        bytes.resize(SIZE, 0);
        bytes[SIZE - 1] = u8::try_from(id).unwrap();
        Buffer::from_vec(bytes)
    };
    let first = client
        .call(&outgoing, &connection, 1, encoded(1), None, REQUEST_TYPE)
        .unwrap();
    let (first_request, held) = timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    let second = client
        .call(&outgoing, &connection, 2, encoded(2), None, REQUEST_TYPE)
        .unwrap();
    assert!(timeout(Duration::from_millis(50), requests.recv()).await.is_err());
    let sent: serde_json::Value = serde_json::from_slice(&outgoing.transport_status_json().unwrap()).unwrap();
    assert_eq!(sent["rdma"]["control_fragments_sent"], 4);
    assert_eq!(sent["rdma"]["copied_control_send_bytes"], 2 * SIZE);
    assert_eq!(sent["rdma"]["control_send_bytes"], 0);
    let incoming: serde_json::Value =
        serde_json::from_slice(&server.transport_status_json().unwrap()).unwrap();
    assert_eq!(incoming["connections"][0]["request_cache"]["bytes"], SIZE);
    assert!(
        incoming["connections"][0]["request_cache"]["stalls"]
            .as_u64()
            .unwrap()
            > 0
    );

    let response_type = FBMsgType::EConnectionPingResponse.0 as u16;
    first_request.reply(encoded(1), None, response_type).unwrap();
    let first_response = timeout(Duration::from_secs(3), first).await.unwrap().unwrap();
    assert_eq!(first_response.control.unwrap().as_slice()[SIZE - 1], 1);
    let (second_request, second_payload) = timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(held[SIZE - 1], 1);
    assert_eq!(second_payload[SIZE - 1], 2);
    second_request.reply(encoded(2), None, response_type).unwrap();
    let second_response = timeout(Duration::from_secs(3), second).await.unwrap().unwrap();
    assert_eq!(second_response.control.unwrap().as_slice()[SIZE - 1], 2);

    client
        .send_one_way(&outgoing, &connection, 3, encoded(3), None, REQUEST_TYPE)
        .unwrap();
    let (one_way, one_way_payload) = timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    let last = client
        .call(&outgoing, &connection, 4, encoded(4), None, REQUEST_TYPE)
        .unwrap();
    drop(one_way);
    let (last_request, _) = timeout(Duration::from_secs(3), requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(one_way_payload[SIZE - 1], 3);
    last_request
        .reply(encoded(4), None, FBMsgType::EUnknownResponse.0 as u16)
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(3), last)
            .await
            .unwrap()
            .unwrap()
            .control
            .unwrap()
            .as_slice()[SIZE - 1],
        4
    );
    outgoing.stop();
    server.stop();
}

// ── Client→server regression ──────────────────────────────────────
// Verify the existing client→server path still works after the
// on_response return-bool change and the server dispatch reorder.
#[tokio::test]
async fn client_to_server_regression() {
    init_test_logging();
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 12345,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let caller = RpcClient::new();
    caller.attach(&conn);
    let future = caller
        .call(
            &server,
            &conn,
            12345,
            ctrl,
            None,
            FBMsgType::EConnectionPingRequest.0 as u16,
        )
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, 12345, "response id should match");

    server.stop();
}

// ── Server dispatch with request_client (handler-first order) ─────
// Register a handler on the SERVER for a custom msg_type. The client
// sends a request with that msg_type. The server's dispatch finds the
// handler (handler-first order) and dispatches it. The handler submits
// a response. The response goes back to the client's on_frame →
// on_response (matches) → callback invoked.
//
// This verifies the server's dispatch order: handler first, then
// on_response. Without this order, the request frame would be
// intercepted by on_response (since the req_id is in the
// request_client's pending map in a loopback).
#[tokio::test]
async fn server_dispatch_handler_first_order() {
    init_test_logging();
    const CUSTOM_MSG_TYPE: u16 = 300;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // Register a handler on the SERVER for the custom msg_type.
    let server_for_handler = server.clone();
    server.register_handler(CUSTOM_MSG_TYPE, move |req| {
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let ctrl_bytes = fbb.finished_data().to_vec();
        let resp_msg_type = FBMsgType::EConnectionPingResponse.0 as u16;
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ctrl_bytes,
                None,
                resp_msg_type,
                req.request_id,
            );
        }
    });

    // Wire a request_client into the server (for response routing).
    // In this test, the request_client is the same as the caller —
    // the client's pending map has the req_id. The server's dispatch
    // checks for a handler FIRST, so the request is not intercepted
    // by on_response.
    let caller = RpcClient::new();
    server.set_request_client(&caller);

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");
    caller.attach(&conn);

    let id_gen = RequestIdGen::new();
    let req_id = id_gen.next().as_u64();

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = caller
        .call(&server, &conn, req_id, ctrl, None, CUSTOM_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, req_id, "response id should match");

    server.stop();
}

// ── Client response routing precedence ────────────────────────────
// Verify that a response frame (matching a pending client-sent request)
// is routed to the pending callback, NOT to the handler dispatch.
// We register a handler for the ping response msg_type, then send a
// ping request. The response should still go to the call() future,
// not the handler.
#[tokio::test]
async fn client_response_routing_precedence() {
    init_test_logging();
    let server = RpcServer::new(None);
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");

    let handler_called = Arc::new(AtomicBool::new(false));
    let handler_called_clone = handler_called.clone();

    let caller = RpcClient::new();
    caller.set_transport(&server);
    // Register a handler for ConnectionPingResponse — but the response
    // should NOT reach it (it should be routed to the pending callback).
    caller.register_handler(FBMsgType::EConnectionPingResponse.0 as u16, move |_req| {
        handler_called_clone.store(true, Ordering::SeqCst);
    });
    caller.attach(&conn);

    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 555,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = caller
        .call(
            &server,
            &conn,
            555,
            ctrl,
            None,
            FBMsgType::EConnectionPingRequest.0 as u16,
        )
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for response")
        .expect("callback dropped");

    assert_eq!(response.request_id, 555, "response id should match");
    assert!(
        !handler_called.load(Ordering::SeqCst),
        "handler should NOT be called for response frames"
    );

    server.stop();
}

// ── Client handler dispatch (server→client via handler chain) ─────
// Test the client's handler dispatch (dispatch_request) by chaining:
// 1. Register a handler on the SERVER for PING that sends a NOTIFY
//    request back to the client via request_client.call() on the
//    server-side connection (Connection::from_handle).
// 2. Register a handler on the CLIENT for NOTIFY that submits an ack.
// 3. Client sends PING → server's PING handler fires → sends NOTIFY
//    to client → client's NOTIFY handler fires → submits ack →
//    server's on_response matches the NOTIFY req_id.
//
// This tests the full server→client→server roundtrip: the server
// initiates a request to the client, the client handles it and acks.
// R32's Connection::from_handle unblocks the server→client send path
// (R114 originally dropped the NOTIFY buffer here).
#[tokio::test]
async fn client_handler_dispatch_via_server_chain() {
    init_test_logging();
    const PING_MSG_TYPE: u16 = FBMsgType::EConnectionPingRequest.0 as u16;
    const NOTIFY_MSG_TYPE: u16 = 310;
    const NOTIFY_ACK_MSG_TYPE: u16 = FBMsgType::EConnectionPingResponse.0 as u16;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // The request_client is used by the server's PING handler to send
    // a NOTIFY request to the client. The ack will be routed back to
    // this client's pending map via the server's dispatch.
    let request_client = Arc::new(RpcClient::new());
    request_client.set_completion_pool_size(64);

    // Wire the request_client into the server for ack routing.
    server.set_request_client(&request_client);

    // Shared state: the NOTIFY CallFuture (awaited after the PING ack)
    // + the NOTIFY req_id (to verify the ack matches) + a flag to
    // verify the client's NOTIFY handler actually fired.
    let notify_future: Arc<Mutex<Option<CallFuture>>> = Arc::new(Mutex::new(None));
    let notify_req_id = Arc::new(AtomicU64::new(0));
    let notify_handler_fired = Arc::new(AtomicBool::new(false));

    // Register a handler on the SERVER for PING. When invoked, it
    // acks the PING, then sends a NOTIFY request back to the client
    // via request_client.call() using Connection::from_handle.
    let server_for_handler = server.clone();
    let request_client_for_handler = request_client.clone();
    let notify_future_for_handler = notify_future.clone();
    let notify_req_id_for_handler = notify_req_id.clone();

    server.register_handler(PING_MSG_TYPE, move |req| {
        // First, ack the original PING request.
        let mut fbb2 = FlatBufferBuilder::new();
        let ping_resp = ConnectionPingResponse::create(
            &mut fbb2,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb2.finish(ping_resp, None);
        let ping_ack_bytes = fbb2.finished_data().to_vec();
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ping_ack_bytes,
                None,
                NOTIFY_ACK_MSG_TYPE,
                req.request_id,
            );
        }

        // Build a NOTIFY request to send back to the client.
        let id_gen = RequestIdGen::new();
        let nreq_id = id_gen.next().as_u64();
        notify_req_id_for_handler.store(nreq_id, Ordering::SeqCst);

        let mut fbb = FlatBufferBuilder::new();
        let nreq = ConnectionPingRequest::create(
            &mut fbb,
            &ConnectionPingRequestArgs {
                id: nreq_id,
                rpc_create_nano: 0,
            },
        );
        fbb.finish(nreq, None);
        let fb_bytes = fbb.finished_data();

        // Standalone buffer (no pool): the buffer is queued for send and
        // released by the I/O worker after this handler returns, so a
        // pool-allocated buffer would dangle when the local pool drops.
        let ctrl = Buffer::from_vec(fb_bytes.to_vec());

        // Send the NOTIFY request via request_client.call_to_handle()
        // on the server-side connection (raw conn_handle from the
        // handler). The frame goes to the client's on_frame →
        // dispatch_request → NOTIFY handler. The ack will be routed
        // back to request_client's pending map via the server's
        // dispatch.
        if let Ok(fut) = request_client_for_handler.call_to_handle(
            &server_for_handler,
            req.conn_handle,
            nreq_id,
            ctrl,
            None,
            NOTIFY_MSG_TYPE,
        ) {
            *notify_future_for_handler.lock().unwrap() = Some(fut);
        }
    });

    // Register a handler on the CLIENT for NOTIFY.
    let server_for_client_handler = server.clone();
    let notify_handler_fired_for_client = notify_handler_fired.clone();
    let client = RpcClient::new();
    client.set_transport(&server);
    client.register_handler(NOTIFY_MSG_TYPE, move |req| {
        notify_handler_fired_for_client.store(true, Ordering::SeqCst);

        // Ack the NOTIFY request.
        let mut fbb = FlatBufferBuilder::new();
        let resp = ConnectionPingResponse::create(
            &mut fbb,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb.finish(resp, None);
        let ctrl_bytes = fbb.finished_data().to_vec();
        unsafe {
            let _ = server_for_client_handler.submit_response(
                req.conn_handle,
                &ctrl_bytes,
                None,
                NOTIFY_ACK_MSG_TYPE,
                req.request_id,
            );
        }
    });

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");
    client.attach(&conn);

    // Client sends PING → server's PING handler acks + sends NOTIFY →
    // client receives ack.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 777,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = client
        .call(&server, &conn, 777, ctrl, None, PING_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for ping ack")
        .expect("callback dropped");

    assert_eq!(response.request_id, 777, "ping ack id should match");

    // The server's PING handler also sent a NOTIFY request to the
    // client. Verify the client's NOTIFY handler fired and the ack
    // was routed back to request_client's pending map.
    let notify_fut = notify_future
        .lock()
        .unwrap()
        .take()
        .expect("NOTIFY CallFuture was not stored (handler did not fire?)");

    let notify_response = tokio::time::timeout(std::time::Duration::from_secs(10), notify_fut)
        .await
        .expect("timeout waiting for NOTIFY ack")
        .expect("NOTIFY callback dropped");

    let expected_nreq_id = notify_req_id.load(Ordering::SeqCst);
    assert!(
        expected_nreq_id > 0,
        "NOTIFY req_id should have been stored by the PING handler"
    );
    assert_eq!(
        notify_response.request_id, expected_nreq_id,
        "NOTIFY ack id should match the NOTIFY request id"
    );
    assert!(
        notify_handler_fired.load(Ordering::SeqCst),
        "client NOTIFY handler should have fired"
    );

    server.stop();
}

// ── Server→client timeout (no handler, reaper times out) ──────────
// Test the timeout/error path for server-initiated requests: the
// server sends a request with a msg_type the client has NO handler
// for (and no transport set, so the frame is dropped — no
// UnknownMessage response). The request_client's reaper scans the
// pending slab and fails the entry with Timeout. This covers the
// server_to_client_timeout_no_handler test that was dropped during
// R114's dispatch-order fix. R117's WatchNotify uses fire-and-forget
// send() (no pending entry), so this gap does not affect it — but
// any future server→client call() (request-response) needs this
// timeout path to be correct.
#[tokio::test]
async fn server_to_client_timeout_no_handler() {
    init_test_logging();
    const PING_MSG_TYPE: u16 = FBMsgType::EConnectionPingRequest.0 as u16;
    const NO_HANDLER_MSG_TYPE: u16 = 320;
    const PING_ACK_MSG_TYPE: u16 = FBMsgType::EConnectionPingResponse.0 as u16;

    let server = Arc::new(RpcServer::new(None));
    server.listen("127.0.0.1", 0).expect("listen failed");
    let port = server.port();
    assert!(port > 0);

    // request_client with a short reaper timeout so the test doesn't
    // wait long. The reaper scans every 50ms for entries past their
    // 300ms deadline.
    let request_client = Arc::new(RpcClient::new());
    request_client.set_completion_pool_size(64);
    request_client.start_reaper(300_000_000, 50_000_000);

    server.set_request_client(&request_client);

    // Shared state: the no-handler CallFuture (awaited after PING ack).
    let no_handler_future: Arc<Mutex<Option<CallFuture>>> = Arc::new(Mutex::new(None));

    // Register a handler on the SERVER for PING. When invoked, it
    // acks the PING, then sends a request with NO_HANDLER_MSG_TYPE
    // to the client via request_client.call(). The client has no
    // handler for this msg_type and no transport set → the frame is
    // dropped → no response → the reaper times out the pending entry.
    let server_for_handler = server.clone();
    let request_client_for_handler = request_client.clone();
    let no_handler_future_for_handler = no_handler_future.clone();

    server.register_handler(PING_MSG_TYPE, move |req| {
        // First, ack the original PING request.
        let mut fbb2 = FlatBufferBuilder::new();
        let ping_resp = ConnectionPingResponse::create(
            &mut fbb2,
            &ConnectionPingResponseArgs {
                id: req.request_id,
                rpc_create_nano: req.rpc_create_nano,
                response_create_nano: 0,
                ret: FBRetCode::Success,
            },
        );
        fbb2.finish(ping_resp, None);
        let ping_ack_bytes = fbb2.finished_data().to_vec();
        unsafe {
            let _ = server_for_handler.submit_response(
                req.conn_handle,
                &ping_ack_bytes,
                None,
                PING_ACK_MSG_TYPE,
                req.request_id,
            );
        }

        // Build a request the client has no handler for.
        let id_gen = RequestIdGen::new();
        let nreq_id = id_gen.next().as_u64();

        let mut fbb = FlatBufferBuilder::new();
        let nreq = ConnectionPingRequest::create(
            &mut fbb,
            &ConnectionPingRequestArgs {
                id: nreq_id,
                rpc_create_nano: 0,
            },
        );
        fbb.finish(nreq, None);
        let fb_bytes = fbb.finished_data();

        // Standalone buffer (no pool): the buffer is queued for send and
        // released by the I/O worker after this handler returns, so a
        // pool-allocated buffer would dangle when the local pool drops.
        let ctrl = Buffer::from_vec(fb_bytes.to_vec());

        // Send via request_client.call() on the server-side connection.
        // The client will drop the frame (no handler, no transport) →
        // the reaper will time out this pending entry.
        // Send via request_client.call_to_handle() on the raw
        // server-side connection handle. The client will drop the
        // frame (no handler, no transport) → the reaper will time
        // out this pending entry.
        if let Ok(fut) = request_client_for_handler.call_to_handle(
            &server_for_handler,
            req.conn_handle,
            nreq_id,
            ctrl,
            None,
            NO_HANDLER_MSG_TYPE,
        ) {
            *no_handler_future_for_handler.lock().unwrap() = Some(fut);
        }
    });

    // Client: attached to conn, NO handler for NO_HANDLER_MSG_TYPE,
    // NO set_transport (so unmatched frames are dropped, not responded
    // to with UnknownMessage — which would prevent the timeout).
    let client = RpcClient::new();

    server.start();

    let conn = server.connect("127.0.0.1", port).expect("connect failed");
    client.attach(&conn);

    // Client sends PING → server handler acks + sends no-handler request.
    let mut fbb = FlatBufferBuilder::new();
    let req = ConnectionPingRequest::create(
        &mut fbb,
        &ConnectionPingRequestArgs {
            id: 888,
            rpc_create_nano: 0,
        },
    );
    fbb.finish(req, None);
    let fb_bytes = fbb.finished_data();

    let pool = BufferPool::new(256);
    let mut ctrl = pool.alloc_buffer(fb_bytes.len() as u32).expect("alloc control");
    ctrl.write(fb_bytes);

    let future = client
        .call(&server, &conn, 888, ctrl, None, PING_MSG_TYPE)
        .expect("call submit failed");

    let response = tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .expect("timeout waiting for ping ack")
        .expect("callback dropped");

    assert_eq!(response.request_id, 888, "ping ack id should match");

    // The server's PING handler also sent a no-handler request to the
    // client. The client dropped it (no handler, no transport). The
    // request_client's reaper should time out the pending entry.
    let no_handler_fut = no_handler_future
        .lock()
        .unwrap()
        .take()
        .expect("no-handler CallFuture was not stored (handler did not fire?)");

    let result = tokio::time::timeout(std::time::Duration::from_secs(5), no_handler_fut)
        .await
        .expect("timeout waiting for reaper to fire (reaper did not time out the entry?)");

    assert!(
        result.is_err(),
        "no-handler request should time out, got Ok: {result:?}"
    );
    assert_eq!(
        result.unwrap_err(),
        crowdb_rpc_ffi::RpcError::Timeout,
        "no-handler request should fail with Timeout (reaper fires, no client response)"
    );

    server.stop();
}
