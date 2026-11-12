// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_kv_ref, ct_tree};
use std::ffi::c_void;

type ApplyComplete = unsafe extern "C" fn(*mut c_void, i32);

#[repr(C)]
pub struct ct_apply_permit {
    _private: [u8; 0],
}

#[repr(C)]
pub struct ct_apply_value_owner {
    pub data: *const u8,
    pub length: usize,
    pub capacity: usize,
    pub drop_context: unsafe extern "C" fn(*mut c_void),
    pub context: *mut c_void,
}

extern "C" {
    pub fn ct_apply_replay_callback(
        tree: *mut ct_tree,
        slot: u64,
        count: usize,
        read: unsafe extern "C" fn(*mut std::ffi::c_void, usize, *mut ct_kv_ref),
        drop_context: unsafe extern "C" fn(*mut std::ffi::c_void),
        context: *mut std::ffi::c_void,
        owner_control_bytes: usize,
        complete: ApplyComplete,
        completion_context: *mut c_void,
    ) -> i32;
    pub fn ct_apply_prepare_callback(
        tree: *mut ct_tree,
        operations: *const ct_kv_ref,
        count: usize,
        gap_repair: i32,
        out: *mut *mut ct_apply_permit,
    ) -> i32;
    pub fn ct_apply_prepare_owned_callback(
        tree: *mut ct_tree,
        operation: *const ct_kv_ref,
        owner: ct_apply_value_owner,
        out: *mut *mut ct_apply_permit,
    ) -> i32;
    pub fn ct_apply_replay_owned_callback(
        tree: *mut ct_tree,
        slot: u64,
        operation: ct_kv_ref,
        owner: ct_apply_value_owner,
        complete: ApplyComplete,
        completion_context: *mut c_void,
    ) -> i32;
    pub fn ct_apply_arm(permit: *mut ct_apply_permit) -> i32;
    pub fn ct_apply_submit_callback(
        permit: *mut ct_apply_permit,
        slot: u64,
        complete: ApplyComplete,
        context: *mut c_void,
    ) -> i32;
    pub fn ct_apply_permit_free(permit: *mut ct_apply_permit);
}
