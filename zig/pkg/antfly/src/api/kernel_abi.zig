// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Layout-only types shared by the API kernel caller and implementation.
//! Keep this file free of production imports so both codegen units agree on one
//! ABI definition without pulling either implementation across the boundary.

const error_abi = @import("../runtime_error_abi.zig");

pub const Status = error_abi.Status;
pub const BoundaryErrors = error_abi.BoundaryErrors;
pub const statusCode = error_abi.statusCode;
pub const statusFromError = error_abi.statusFromError;
pub const errorFromStatus = error_abi.errorFromStatus;

pub const CreateContext = extern struct {
    owner_alloc: *const anyopaque,
    cfg: *const anyopaque,
    source: *const anyopaque,
    table_reads: *const anyopaque,
    table_writes: *const anyopaque,
    fallible: bool,
    out_handle: *?*anyopaque,
    out_request_alloc: *anyopaque,
};

pub const CallContext = extern struct {
    handle: *anyopaque,
    input: ?*const anyopaque = null,
    output: ?*anyopaque = null,
};

pub const HandlerCreateContext = extern struct {
    api_server_handle: *anyopaque,
    out_handle: *?*anyopaque,
};

pub const HttpMethod = enum(c_int) {
    get,
    post,
    put,
    delete,
};

pub const RouteContext = extern struct {
    server: *anyopaque,
    method: HttpMethod,
    path_ptr: [*]const u8,
    path_len: usize,
    handler: *const anyopaque,
};

pub const HandlerStats = extern struct {
    query_capacity: usize,
    query_in_flight: usize,
    query_peak_in_flight: usize,
    query_rejected_total: u64,
    query_body_capacity: usize,
    query_body_in_flight: usize,
    query_body_peak_in_flight: usize,
    query_body_rejected_total: u64,
    cancellation_watcher_start_failures_total: u64,
    peer_disconnect_cancellations_total: u64,
    peer_observer_failures_total: u64,
    active_peer_observers: usize,
};
