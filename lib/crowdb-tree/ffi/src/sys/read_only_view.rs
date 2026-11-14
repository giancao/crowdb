// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_future, ct_range_rebuild_stats, ct_scan_options_v2, ct_tree};
use std::ffi::{c_int, c_void};

#[repr(C)]
pub struct ct_read_only_view {
    _private: [u8; 0],
}
extern "C" {
    pub fn ct_read_only_view_capture(
        tree: *mut ct_tree,
        expected_sequence: u64,
        expected_coverage: u64,
        out: *mut *mut ct_read_only_view,
    ) -> c_int;
    pub fn ct_read_only_view_free(view: *mut ct_read_only_view);
    pub fn ct_read_only_view_close_async(
        view: *mut ct_read_only_view,
        complete: unsafe extern "C" fn(*mut c_void),
        context: *mut c_void,
    ) -> c_int;
    pub fn ct_read_only_view_get_async(
        view: *mut ct_read_only_view,
        key: *const u8,
        size: usize,
        complete: unsafe extern "C" fn(*mut c_void, c_int, *mut ct_future),
        context: *mut c_void,
    ) -> c_int;
    pub fn ct_read_only_view_scan_async(
        view: *mut ct_read_only_view,
        options: *const ct_scan_options_v2,
        complete: unsafe extern "C" fn(*mut c_void, c_int, *mut ct_future),
        context: *mut c_void,
    ) -> c_int;
    pub fn ct_read_only_view_rebuild(
        view: *mut ct_read_only_view,
        destination: *mut ct_tree,
        stats: *mut ct_range_rebuild_stats,
    ) -> c_int;
    pub fn ct_read_only_view_rebuild_async(
        view: *mut ct_read_only_view,
        destination: *mut ct_tree,
        complete: unsafe extern "C" fn(*mut c_void, c_int, ct_range_rebuild_stats),
        context: *mut c_void,
    ) -> c_int;
    pub fn ct_rebuild_bounded_range_async(
        source: *mut ct_tree,
        destination: *mut ct_tree,
        complete: unsafe extern "C" fn(*mut c_void, c_int, ct_range_rebuild_stats),
        context: *mut c_void,
    ) -> c_int;
}
