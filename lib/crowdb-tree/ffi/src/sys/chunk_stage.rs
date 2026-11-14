// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{ct_buf, ct_root_catalog, ct_root_catalog_callbacks};
use std::ffi::c_void;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ct_chunk_stage_id {
    pub owner_epoch: u64,
    pub operation_high: u64,
    pub operation_low: u64,
}
#[repr(C)]
pub struct ct_root_catalog_staging_callbacks {
    pub version: u32,
    pub struct_bytes: u32,
    pub capabilities: u64,
    pub load:
        Option<unsafe extern "C" fn(*mut c_void, u64, ct_chunk_stage_id, *mut *const u8, *mut usize) -> i32>,
    pub compare:
        Option<unsafe extern "C" fn(*mut c_void, u64, u64, *const u8, usize, *const u8, usize) -> i32>,
    pub list: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            *const ct_chunk_stage_id,
            u32,
            *mut *const u8,
            *mut usize,
        ) -> i32,
    >,
}
pub type ct_catalog_status_complete = unsafe extern "C" fn(*mut c_void, i32);
pub type ct_catalog_blob_complete = unsafe extern "C" fn(*mut c_void, i32, *const u8, usize);
#[repr(C)]
pub struct ct_root_catalog_staging_async_callbacks {
    pub version: u32,
    pub struct_bytes: u32,
    pub load: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            ct_chunk_stage_id,
            ct_catalog_blob_complete,
            *mut c_void,
        ) -> i32,
    >,
    pub compare: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            u64,
            *const u8,
            usize,
            *const u8,
            usize,
            ct_catalog_status_complete,
            *mut c_void,
        ) -> i32,
    >,
    pub list: Option<
        unsafe extern "C" fn(
            *mut c_void,
            u64,
            *const ct_chunk_stage_id,
            u32,
            ct_catalog_blob_complete,
            *mut c_void,
        ) -> i32,
    >,
}
extern "C" {
    pub fn ct_root_catalog_stage_compare_async_v1(
        catalog: *mut ct_root_catalog,
        tree: u64,
        epoch: u64,
        expected: *const u8,
        expected_length: usize,
        next: *const u8,
        next_length: usize,
        complete: ct_catalog_status_complete,
        context: *mut c_void,
    ) -> i32;
    pub fn ct_callback_root_catalog_set_staging_async_v1(
        catalog: *mut ct_root_catalog,
        callbacks: *const ct_root_catalog_staging_async_callbacks,
    ) -> i32;
    pub fn ct_callback_root_catalog_open_v2(
        callbacks: *const ct_root_catalog_callbacks,
        staging: *const ct_root_catalog_staging_callbacks,
        context: *mut c_void,
        out: *mut *mut ct_root_catalog,
    ) -> i32;
    pub fn ct_root_catalog_stage_capabilities(catalog: *const ct_root_catalog) -> u64;
    pub fn ct_root_catalog_stage_load(
        catalog: *mut ct_root_catalog,
        tree_id: u64,
        id: ct_chunk_stage_id,
        out: *mut ct_buf,
    ) -> i32;
    pub fn ct_root_catalog_stage_compare(
        catalog: *mut ct_root_catalog,
        tree_id: u64,
        owner_epoch: u64,
        expected: *const u8,
        expected_length: usize,
        next: *const u8,
        next_length: usize,
    ) -> i32;
    pub fn ct_root_catalog_stage_list(
        catalog: *mut ct_root_catalog,
        tree_id: u64,
        after: *const ct_chunk_stage_id,
        limit: u32,
        out: *mut ct_buf,
    ) -> i32;
}
