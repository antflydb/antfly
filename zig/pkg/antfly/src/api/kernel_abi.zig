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
pub const abi_version: u32 = 14;
pub const statusFromError = error_abi.statusFromError;
pub const errorFromStatus = error_abi.errorFromStatus;

pub const CreateContext = extern struct {
    abi_version: u32,
    struct_size: u32 = @sizeOf(@This()),
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
    struct_size: u32 = @sizeOf(@This()),
    handle: *anyopaque,
    input: ?*const anyopaque = null,
    input_contract: native_abi.TypeContract = .of(void),
    output: ?*anyopaque = null,
    output_contract: native_abi.TypeContract = .of(void),
};

pub const HandlerCreateContext = extern struct {
    abi_version: u32,
    struct_size: u32 = @sizeOf(@This()),
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
    struct_size: u32 = @sizeOf(@This()),
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
pub const RequestBodyMode = http_abi.RequestBodyMode;
pub const CancellationView = http_abi.CancellationView;
pub const RequestBodySource = http_abi.RequestBodySource;
pub const StreamSink = http_abi.StreamSink;

pub const HttpHandleContext = extern struct {
    abi_version: u32,
    struct_size: u32 = @sizeOf(@This()),
    route_handle: *anyopaque,
    request: *const HttpRequestView,
    cancellation: CancellationView = .{},
    body_source: RequestBodySource = .{},
    stream: StreamSink = .{},
    /// Request-scoped host executor. It must not be retained after the call.
    executor: native_abi.IoBorrow,
    out_response_handle: *?*anyopaque,
    out_response: *HttpResponseView,
};

pub const RouteManifestEntry = extern struct {
    route_handle: *anyopaque,
    method: HttpMethod,
    path: Bytes,
    request_body: RequestBodyMode,
    streaming_response: u8,
};

pub const RouteManifestContext = extern struct {
    abi_version: u32,
    struct_size: u32 = @sizeOf(@This()),
    handler_handle: *anyopaque,
    out_entries: *?[*]const RouteManifestEntry,
    out_len: *usize,
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
    inference_capacity: usize,
    inference_in_flight: usize,
    inference_peak_in_flight: usize,
    inference_rejected_total: u64,
    query_body_capacity: usize,
    query_body_in_flight: usize,
    query_body_peak_in_flight: usize,
    query_body_rejected_total: u64,
};

/// Features exposed by the independently code-generated API-kernel archive.
/// Consumers must test a capability before using its corresponding optional
/// portion of the function table.
pub const Capability = struct {
    pub const core: u64 = 1 << 0;
    pub const route_manifest: u64 = 1 << 3;
};

/// The sole discovery point for the API-kernel ABI. Keeping the table itself
/// append-only lets a caller validate the fixed prefix it understands while a
/// newer archive appends optional operations.
pub const FunctionTable = extern struct {
    abi_version: u32,
    struct_size: u32,
    capabilities: u64,

    create: *const fn (*const CreateContext) callconv(.c) Status,
    destroy: *const fn (*anyopaque) callconv(.c) void,
    request_stats: *const fn (*const CallContext) callconv(.c) Status,
    query_admission_stats: *const fn (*const CallContext) callconv(.c) Status,
    write_admission_stats: *const fn (*const CallContext) callconv(.c) Status,
    inference_admission_stats: *const fn (*const CallContext) callconv(.c) Status,
    set_provider: *const fn (*const CallContext) callconv(.c) Status,
    set_ha_executor: *const fn (*const CallContext) callconv(.c) Status,
    attach_runtime_restore_store: *const fn (*const CallContext) callconv(.c) Status,
    attach_replicated_restore_store: *const fn (u32, *anyopaque, *const anyopaque) callconv(.c) Status,
    resume_restore_jobs: *const fn (*const CallContext) callconv(.c) Status,
    poll_restore_jobs: *const fn (*const CallContext) callconv(.c) Status,
    prepare_restore_leadership: *const fn (*const CallContext) callconv(.c) Status,
    schedule_session_maintenance: *const fn (*const CallContext) callconv(.c) Status,
    storage_maintenance_active: *const fn (*const CallContext) callconv(.c) Status,
    check_ready: *const fn (*const CallContext) callconv(.c) Status,
    authorize_inference: *const fn (*const AuthorizeInferenceContext) callconv(.c) Status,
    handler_create: *const fn (*const HandlerCreateContext) callconv(.c) Status,
    handler_init: *const fn (*const CallContext) callconv(.c) Status,
    handler_stats: *const fn (*const CallContext) callconv(.c) Status,
    handler_route_manifest: *const fn (*const RouteManifestContext) callconv(.c) Status,
    handler_handle_http: *const fn (*const HttpHandleContext) callconv(.c) Status,
    handler_destroy_http_response: *const fn (*anyopaque) callconv(.c) void,
    handler_destroy: *const fn (*anyopaque) callconv(.c) void,
};

pub fn validContext(comptime T: type, version: u32, struct_size: u32) bool {
    return version == abi_version and struct_size == @sizeOf(T);
}

pub fn validFunctionTable(table: *const FunctionTable, required_capabilities: u64) bool {
    return table.abi_version == abi_version and
        table.struct_size >= @sizeOf(FunctionTable) and
        table.capabilities & required_capabilities == required_capabilities;
}

test "API kernel control contexts retain C layout" {
    const std = @import("std");
    try std.testing.expectEqual(.@"extern", @typeInfo(CreateContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(CallContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(HandlerCreateContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(RouteManifestContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(AuthorizeInferenceContext).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(native_abi.TypeContract).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(memory_abi.Allocator).@"struct".layout);
    try std.testing.expectEqual(.@"extern", @typeInfo(FunctionTable).@"struct".layout);
    try std.testing.expectEqual(@as(u32, @sizeOf(CreateContext)), (CreateContext{ .abi_version = abi_version, .owner_alloc = undefined, .cfg = undefined, .cfg_contract = .of(void), .source = undefined, .source_contract = .of(void), .table_reads = undefined, .table_reads_contract = .of(void), .table_writes = undefined, .table_writes_contract = .of(void), .out_handle = undefined, .out_request_alloc = undefined }).struct_size);
}

test "API kernel ABI rejects mismatched context and function-table prefixes" {
    const std = @import("std");
    try std.testing.expect(validContext(CallContext, abi_version, @sizeOf(CallContext)));
    try std.testing.expect(!validContext(CallContext, abi_version - 1, @sizeOf(CallContext)));
    try std.testing.expect(!validContext(CallContext, abi_version, @sizeOf(CallContext) - 1));

    var table: FunctionTable = undefined;
    table.abi_version = abi_version;
    table.struct_size = @sizeOf(FunctionTable);
    table.capabilities = Capability.core;
    try std.testing.expect(validFunctionTable(&table, Capability.core));
    try std.testing.expect(!validFunctionTable(&table, Capability.route_manifest));
    table.struct_size -= 1;
    try std.testing.expect(!validFunctionTable(&table, Capability.core));
}
