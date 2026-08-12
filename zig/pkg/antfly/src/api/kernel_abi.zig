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
const http_abi = @import("../runtime_http_abi.zig");
pub const memory_abi = @import("../runtime_memory_abi.zig");
pub const native_abi = @import("../runtime_native_abi.zig");

pub const Status = error_abi.Status;
pub const StatusCode = error_abi.Code;
pub const StatusDetail = error_abi.Detail;
/// Version of the API-kernel control structs below. This is intentionally
/// independent of the status ABI: adding flags/reserved fields must invalidate
/// an older context before the callee reads beyond its layout.
pub const abi_version: u32 = 4;
pub const statusFromError = error_abi.statusFromError;
pub const errorFromStatus = error_abi.errorFromStatus;

pub const CreateContext = extern struct {
    abi_version: u32,
    flags: u32 = 0,
    owner_alloc: *const memory_abi.Allocator,
    cfg: *const anyopaque,
    cfg_contract: native_abi.TypeContract,
    source: *const anyopaque,
    source_contract: native_abi.TypeContract,
    table_reads: *const anyopaque,
    table_reads_contract: native_abi.TypeContract,
    table_writes: *const anyopaque,
    table_writes_contract: native_abi.TypeContract,
    out_handle: *?*anyopaque,
    out_request_alloc: *?*const memory_abi.Allocator,

    pub const fallible_init: u32 = 1 << 0;
};

pub const CallContext = extern struct {
    abi_version: u32,
    _reserved: u32 = 0,
    handle: *anyopaque,
    input: ?*const anyopaque = null,
    input_contract: native_abi.TypeContract = .of(void),
    output: ?*anyopaque = null,
    output_contract: native_abi.TypeContract = .of(void),
};

pub const HandlerCreateContext = extern struct {
    abi_version: u32,
    _reserved: u32 = 0,
    api_server_handle: *anyopaque,
    out_handle: *?*anyopaque,
};

pub const InferencePermission = enum(c_int) {
    read = 1,
    write = 2,
};

pub const AuthorizationDecision = enum(c_int) {
    allowed = 0,
    unauthorized = 1,
    forbidden = 2,
    not_ready = 3,
};

pub const AuthorizeInferenceContext = extern struct {
    abi_version: u32,
    _reserved: u32 = 0,
    handle: *anyopaque,
    authorization: OptionalBytes = .{},
    trusted_principal: OptionalBytes = .{},
    permission: InferencePermission,
    out_decision: *AuthorizationDecision,
};

pub const HttpMethod = http_abi.HttpMethod;
pub const Bytes = http_abi.Bytes;
pub const OptionalBytes = http_abi.OptionalBytes;
pub const HeaderView = http_abi.HeaderView;
pub const RouteParamView = http_abi.RouteParamView;
pub const HttpRequestView = http_abi.HttpRequestView;
pub const HttpResponseView = http_abi.HttpResponseView;

pub const HttpHandleContext = extern struct {
    abi_version: u32,
    _reserved: u32 = 0,
    route_handle: *anyopaque,
    request: *const HttpRequestView,
    out_response_handle: *?*anyopaque,
    out_response: *HttpResponseView,
};

pub const RouteContext = extern struct {
    abi_version: u32,
    _reserved: u32 = 0,
    server: *anyopaque,
    route_handle: *anyopaque,
    method: HttpMethod,
    path_ptr: [*]const u8,
    path_len: usize,
};

pub const HandlerStats = extern struct {
    query_capacity: usize,
    query_in_flight: usize,
    query_peak_in_flight: usize,
    query_rejected_total: u64,
    write_capacity: usize,
    write_in_flight: usize,
    write_peak_in_flight: usize,
    write_rejected_total: u64,
    query_body_capacity: usize,
    query_body_in_flight: usize,
    query_body_peak_in_flight: usize,
    query_body_rejected_total: u64,
    cancellation_watcher_start_failures_total: u64,
    peer_disconnect_cancellations_total: u64,
    peer_observer_failures_total: u64,
    active_peer_observers: usize,
};

test "API kernel control contexts retain C layout" {
    const std = @import("std");
    try std.testing.expectEqual(.@"extern", @typeInfo(CreateContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(CallContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(HandlerCreateContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(AuthorizeInferenceContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(native_abi.TypeContract).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(memory_abi.Allocator).@"struct".layout);
}
