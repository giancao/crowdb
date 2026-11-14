// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::responses::build_error_response;
use super::{
    build_accepted_response, px_error_to_ret_code, record_group_context, spawn_request, warn, Arc, Buffer,
    FBAcceptRequest, FBKvRetCode, FBMsgType, PxAcceptReply, PxBallot, PxLocalReplica, PxLogEntry,
    PxRpcService, ReplicaHandler, ServerRequest,
};
use bytes::Bytes;

impl PxRpcService {
    // ── Accept ───────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    pub(super) fn handle_accept(&self, mut req: ServerRequest) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAcceptedResponse.0 as u16;
        let store = Arc::clone(&self.store);
        spawn_request(&self.rt, "accept", store.store_id, async move {
            let reply = |request: ServerRequest, (bytes, head)| {
                let _ = request.reply(Buffer::from_vec_offset(bytes, head), None, msg_type);
            };
            let reply_error = |request, code, message: &str| {
                reply(request, build_error_response(req_id, create_nano, code, message));
            };
            let Ok(fb_req) = flatbuffers::root::<FBAcceptRequest>(req.control()) else {
                reply_error(req, FBKvRetCode::InvalidArgument, "invalid request flatbuffer");
                return;
            };
            let group_id = fb_req.group_id();
            record_group_context(&store, group_id);
            let slot = fb_req.slot();
            let round = fb_req.round();
            let leader_id = fb_req.leader_id();
            let term = fb_req.term();
            let membership_epoch = fb_req.membership_epoch();

            let Some(fb_value) = fb_req.value() else {
                reply_error(req, FBKvRetCode::InvalidArgument, "missing value");
                return;
            };
            let payload_range = fb_value.payload().map(|value| {
                let bytes = value.bytes();
                let offset = bytes.as_ptr() as usize - req.control().as_ptr() as usize;
                offset..offset + bytes.len()
            });

            let Some(group) = store.get_group(group_id) else {
                reply_error(req, FBKvRetCode::NotFound, "px group not found");
                return;
            };
            let responder_epoch = group.membership_epoch();

            if membership_epoch != responder_epoch {
                let converged_epoch = group.adopt_membership_epoch(membership_epoch);
                warn!(
                    group_id,
                    slot,
                    round,
                    leader_id,
                    proposer_epoch = membership_epoch,
                    responder_epoch,
                    converged_epoch,
                    "accept rejected by membership-epoch fence; adopting higher epoch from proposer"
                );
                let term = group.local_replica().current_term_snapshot();
                let ctrl = build_accepted_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    slot,
                    round,
                    leader_id,
                    false,
                    0,
                    0,
                    term,
                    false,
                    responder_epoch,
                    true,
                );
                reply(req, ctrl);
                return;
            }

            let payload = match payload_range {
                Some(range) if !range.is_empty() => match req.take_control_bytes() {
                    Some(control) => control.slice(range),
                    None => Bytes::copy_from_slice(&req.control()[range]),
                },
                _ => Bytes::new(),
            };
            let entry = PxLogEntry {
                slot,
                ballot: PxBallot { round, leader_id },
                term,
                payload: payload.into(),
            };
            let replica = group.local_replica();
            let result = <PxLocalReplica as ReplicaHandler>::on_accept(replica, &entry, group_id).await;
            let (rejected, rejected_round, rejected_leader_id, term_stale, reply_term) = match result {
                Ok(PxAcceptReply::Accepted { .. }) => (false, 0, 0, false, replica.current_term_snapshot()),
                Ok(PxAcceptReply::Rejected { current_promised, .. }) => {
                    warn!(
                        group_id,
                        slot,
                        current_round = current_promised.round,
                        current_leader_id = current_promised.leader_id,
                        "accept rejected; next step: proposer should run prepare with a higher ballot"
                    );
                    (
                        true,
                        current_promised.round,
                        current_promised.leader_id,
                        false,
                        replica.current_term_snapshot(),
                    )
                }
                Ok(PxAcceptReply::TermStale { new_term, .. }) => {
                    warn!(
                        group_id,
                        slot, new_term, "accept rejected by term fence; proposer should step down"
                    );
                    (false, 0, 0, true, new_term)
                }
                Ok(PxAcceptReply::EpochMismatch { .. }) => {
                    unreachable!("on_accept does not produce EpochMismatch")
                }
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    let ctrl = build_accepted_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        slot,
                        round,
                        leader_id,
                        false,
                        0,
                        0,
                        replica.current_term_snapshot(),
                        false,
                        responder_epoch,
                        false,
                    );
                    reply(req, ctrl);
                    return;
                }
            };
            let ctrl = build_accepted_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                slot,
                round,
                leader_id,
                rejected,
                rejected_round,
                rejected_leader_id,
                reply_term,
                term_stale,
                responder_epoch,
                false,
            );
            reply(req, ctrl);
        });
    }
}
