// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Instant;

use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::kv_client_fb::{FBKvClientRetCode, FBKvSetRequest, FBKvWriteRequest};
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};

use crate::cluster::px_kv_store::PxKvStore;
use crate::kv::Batch;

use super::{
    build_kv_error, build_kv_response, owner_fence, record_group_context, spawn_request, submit_fb_response,
    submit_kv_error, PaxosKvRpcService,
};

fn check_write_payload(payload: &[u8], single_put: bool) -> Result<Option<&[u8]>, &'static str> {
    if single_put {
        return Batch::single_put_key(payload)
            .map(Some)
            .ok_or("invalid single PUT payload");
    }
    if payload.is_empty() {
        return Err("missing batch operation count");
    }
    let (_, operations) = Batch::operation_ranges(payload)?;
    let mut reserved = None;
    for operation in operations {
        let (key, _) = operation?;
        let key = &payload[key];
        if crowdb_protocol::owner_fence::is_owner_fence_key(key) {
            reserved = Some(key);
        }
    }
    Ok(reserved)
}

impl PaxosKvRpcService {
    pub(super) fn handle_write_payload(&self, mut req: ServerRequest) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EKvResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let single_put = req.msg_type == FBMsgType::EKvPutRequest.0 as u16;
        let hist = self
            .latency
            .as_ref()
            .map(|l| Arc::clone(if single_put { &l.put } else { &l.batch_write }));
        let operation = if single_put { "put" } else { "batch_write" };
        spawn_request(&self.rt, operation, store.store_id, async move {
            let start = Instant::now();
            let reply = |request: ServerRequest, (bytes, head)| {
                let _ = request.reply(Buffer::from_vec_offset(bytes, head), None, msg_type);
            };
            let send_error = |request, code, message: &str| {
                reply(request, build_kv_error(req_id, create_nano, code, message));
            };
            let Ok(fb_req) = flatbuffers::root::<FBKvWriteRequest>(req.control()) else {
                send_error(
                    req,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid write flatbuffer",
                );
                return;
            };
            let payload = fb_req.payload().bytes();
            let key = match check_write_payload(payload, single_put) {
                Ok(key) => key,
                Err(error) => {
                    send_error(req, FBKvClientRetCode::InvalidArgument, error);
                    return;
                }
            };
            let group_id = fb_req.group_id();
            record_group_context(&store, group_id);
            let client_id = fb_req.client_id();
            let seq = fb_req.seq();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();
            let resp = if let Some(response) =
                key.and_then(|key| PxKvStore::reserved_put_response(key, request_id, request_create_ms))
            {
                response
            } else {
                let address = payload.as_ptr() as usize;
                let offset = address - req.control().as_ptr() as usize;
                let range = offset..offset + payload.len();
                let Some(control) = req.take_control_bytes() else {
                    send_error(req, FBKvClientRetCode::Unavailable, "cannot retain write payload");
                    return;
                };
                let payload = control.slice(range);
                debug_assert_eq!(payload.as_ptr() as usize, address);
                store
                    .propose_write_and_respond(
                        group_id,
                        payload,
                        client_id,
                        seq,
                        request_id,
                        request_create_ms,
                    )
                    .await
            };
            let ctrl = build_kv_response(req_id, create_nano, &resp);
            reply(req, ctrl);
            if let Some(h) = hist {
                h.observe(start.elapsed().as_nanos() as u64);
            }
        });
    }

    pub(super) fn handle_put_cas(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EKvResponse.0 as u16;

        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        let hist = self.latency.as_ref().map(|l| Arc::clone(&l.put));
        spawn_request(&self.rt, "put", store.store_id, async move {
            let start = Instant::now();
            let Ok(fb_req) = flatbuffers::root::<FBKvSetRequest>(req.control()) else {
                submit_kv_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            record_group_context(&store, group_id);
            let key = fb_req.key().map(|v| v.bytes()).unwrap_or_default();
            let value = fb_req.value().map(|v| v.bytes()).unwrap_or_default();
            let client_id = fb_req.client_id();
            let seq = fb_req.seq();
            let request_id = fb_req.request_id();
            let request_create_ms = fb_req.request_create_ms();
            let Some(precondition) = fb_req.precondition() else {
                submit_kv_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvClientRetCode::InvalidArgument,
                    "conditional PUT requires a precondition",
                );
                return;
            };
            let precondition_key = precondition.key().map(|key| key.bytes()).unwrap_or_default();
            let resp =
                if owner_fence::valid_conditional_put(group_id, client_id, key, precondition_key, value) {
                    let payload = PxKvStore::encode_kv_payload(&[(key, Some(value))]);
                    store
                        .propose_cas_and_respond(
                            group_id,
                            payload,
                            bytes::Bytes::copy_from_slice(precondition_key),
                            precondition.expected_revision(),
                            client_id,
                            seq,
                            request_id,
                            request_create_ms,
                        )
                        .await
                } else {
                    crate::rpc::KvResponse::cas_error(
                        crate::rpc::KvErrorCode::KvErrorCasFailed,
                        0,
                        "invalid conditional put precondition",
                        request_id,
                        request_create_ms,
                    )
                };
            let ctrl = build_kv_response(req_id, create_nano, &resp);
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            if let Some(h) = hist {
                h.observe(start.elapsed().as_nanos() as u64);
            }
            // req dropped here, frame released
        });
    }
}
