// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{ct_options, ct_runtime, ct_tree, ct_tree_runtime_options};
use std::ffi::{c_char, c_void};
#[repr(C)]
pub struct ct_backend_release_service {
    _private: [u8; 0],
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
pub struct ct_backend_release_step {
    pub visited: usize,
    pub completed: usize,
    pub more: u8,
    pub drained: u8,
}
unsafe extern "C" {
    pub fn ct_tree_backend_release_service(
        tree: *mut ct_tree,
        wake: unsafe extern "C" fn(*mut c_void),
        drop_context: unsafe extern "C" fn(*mut c_void),
        context: *mut c_void,
        out: *mut *mut ct_backend_release_service,
    ) -> i32;
    pub fn ct_open_block_with_runtime_async(
        opt: *const ct_options,
        runtime: *mut ct_runtime,
        binding: *const ct_tree_runtime_options,
        names: *const *const c_char,
        count: usize,
        complete: unsafe extern "C" fn(*mut c_void, i32, *mut ct_tree),
        open_context: *mut c_void,
        wake: unsafe extern "C" fn(*mut c_void),
        drop_context: unsafe extern "C" fn(*mut c_void),
        release_context: *mut c_void,
        release: *mut *mut ct_backend_release_service,
    ) -> i32;
    pub fn ct_backend_release_service_is_async(service: *const ct_backend_release_service) -> u8;
    pub fn ct_backend_release_service_pump(
        service: *mut ct_backend_release_service,
        limit: usize,
    ) -> ct_backend_release_step;
    pub fn ct_backend_release_service_stop(service: *mut ct_backend_release_service);
    pub fn ct_backend_release_service_free(service: *mut ct_backend_release_service);
}
