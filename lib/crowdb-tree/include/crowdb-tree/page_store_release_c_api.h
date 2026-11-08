// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#pragma once
#include "crowdb-tree/c_api.h"
#ifdef __cplusplus
extern "C" {
#endif
typedef struct ct_page_store_release_service ct_page_store_release_service;

typedef struct ct_page_store_release_step
{
    size_t  visited;
    size_t  completed;
    uint8_t more;
    uint8_t drained;
} ct_page_store_release_step;

// Bind once before publishing the opened Tree to other callers. Returns
// NotFound for page stores without deferred file release. Context transfers only
// on success. Wake must neither block nor allocate. The host retains this
// service through drained. It can own a reactor without retaining the Tree,
// CPU runtime, or page store store.
ct_status ct_tree_page_store_release_service(ct_tree *tree, void (*wake)(void *), void (*drop_context)(void *),
                                             void *context, ct_page_store_release_service **out);
// Asynchronously opens a Linux Block Tree from host-discovered basenames.
// The names are consumed before return. Accepted submissions transfer both
// contexts and return the release service even if opening later fails. The
// host pumps that service through drained, including after a canceled wait.
ct_status ct_open_block_with_runtime_async(const ct_options *opt, ct_runtime *runtime,
                                           const ct_tree_runtime_options *binding, const char *const *names,
                                           size_t count, void (*complete)(void *, ct_status, ct_tree *),
                                           void  *open_context, void (*wake)(void *), void (*drop_context)(void *),
                                           void *release_context, ct_page_store_release_service **release);
// An async service can be pumped directly. The legacy service requires an
// external host boundary for file close. Limit is in [1,64].
uint8_t                    ct_page_store_release_service_is_async(const ct_page_store_release_service *service);
ct_page_store_release_step ct_page_store_release_service_pump(ct_page_store_release_service *service, size_t limit);
// Stop new page store files only after host ingress is closed. Existing readers
// remain valid and may enqueue their final release after this call.
void ct_page_store_release_service_stop(ct_page_store_release_service *service);
void ct_page_store_release_service_free(ct_page_store_release_service *service);
#ifdef __cplusplus
}
#endif
