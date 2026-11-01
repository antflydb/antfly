// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Storage-free wire contract for a complete synchronous local query.
//! Native DB layout is checked only when the private provider resolves a borrow.
const failure_abi = @import("runtime_failure_abi");
// Initial independent query contract preserves the existing diagnostic version.
pub const abi_version: u32 = 86;
pub const Status = failure_abi.Status;
pub const FailureIdentity = failure_abi.FailureIdentity;
pub const CancellationCheckFn = *const fn (?*anyopaque) callconv(.c) u8;

pub const BorrowedBytes = extern struct {
    ptr: ?[*]const u8 = null,
    len: u64 = 0,

    pub fn fromSlice(value: []const u8) BorrowedBytes {
        return .{
            .ptr = if (value.len == 0) null else value.ptr,
            .len = @intCast(value.len),
        };
    }

    pub fn slice(self: BorrowedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

pub const OwnedBytes = extern struct {
    ptr: ?[*]u8 = null,
    len: u64 = 0,

    pub fn slice(self: OwnedBytes) []const u8 {
        if (self.len == 0) return "";
        return self.ptr.?[0..@intCast(self.len)];
    }
};

/// Provider-owned encoded query response plus the exact storage snapshot
/// generation used to produce it. The generation is transport metadata, not
/// part of the public JSON representation.
pub const QueryOwnedResponse = extern struct {
    buffer: OwnedBytes = .{},
    identity_read_generation: u64 = 0,
    has_identity_read_generation: u8 = 0,
    _reserved0: [7]u8 = @splat(0),
};

/// Selects the existing query wire dialect. Distributed/local-owner calls use
/// the resolved internal representation; public C API and Lite calls retain
/// public validation and translation inside the query provider.
pub const LocalQueryDialect = enum(u32) {
    internal = 0,
    public = 1,
};

/// Complete operation families owned by the compiled local-query provider. New
/// values are append-only. The storage owner remains responsible for handle
/// lifetime and admission; parsing, physical execution, and response encoding
/// stay together on the provider side.
pub const LocalQueryKind = enum(u32) {
    search = 0,
    graph_expand = 1,
    graph_hydrate = 2,
    graph_edges = 3,
    text_stats = 4,
    algebraic_partials = 5,
    preflight = 6,
};

/// Stable diagnostic identities for the stages of one local-query call. These
/// values are ABI data: append new operations, never renumber existing ones.
/// Control flow continues to use `FailureIdentity.status`; `operation` tells
/// operators which side and stage produced that exact status.
pub const LocalQueryOperation = enum(u32) {
    validate_request = 1,
    parse_internal_request = 2,
    parse_public_request = 3,
    execute_internal_query = 4,
    execute_public_query = 5,
    encode_internal_response = 6,
    encode_public_response = 7,
    validate_provider_response = 8,
    text_stats = 9,
    algebraic_partials = 10,
    parse_graph_expand = 11,
    execute_graph_expand = 12,
    encode_graph_expand = 13,
    parse_graph_hydrate = 14,
    execute_graph_hydrate = 15,
    encode_graph_hydrate = 16,
    parse_graph_edges = 17,
    execute_graph_edges = 18,
    encode_graph_edges = 19,
    parse_aggregation = 20,
    execute_aggregation = 21,
    encode_aggregation = 22,
    preflight = 23,
    graph_metric_maintenance = 24,
    scan_stream = 25,
};

pub const LocalQueryReturnMode = enum(u32) {
    parent = 0,
    chunk = 1,
    parent_with_chunks = 2,
    unit = 3,
    unit_with_chunks = 4,
    /// Return each indexed source member without hierarchy grouping.
    /// Appended to preserve the established numeric ABI.
    member = 5,
};

/// Request-local execution details that are not losslessly represented by the
/// existing public/distributed JSON envelope. They are applied only when
/// `enabled` is set, after parsing and before DB execution.
pub const LocalQueryExecutionOptions = extern struct {
    enabled: u8 = 0,
    include_stored: u8 = 1,
    /// Return a raw shard result for coordinator finalization. Keep aggregation
    /// specifications during search: they also govern exact candidate totals.
    raw_search_result: u8 = 0,
    _reserved0: u8 = 0,
    return_mode: LocalQueryReturnMode = .parent,
    max_chunks_per_parent: u32 = 0,
    /// Presentation only; never used for storage lookup or authorization.
    /// Borrowed through this synchronous call and response serialization.
    response_table_name: BorrowedBytes = .{},
};

/// A synchronous native borrow. Only private static archives built by the same
/// toolchain may resolve it; public APIs never expose this descriptor. The
/// caller retains the DB and its readable lease until execution returns.
pub const LocalQueryDb = extern struct {
    const Contract = @import("antfly_runtime_abi").native_abi.TypeContract;
    ptr: ?*anyopaque = null,
    layout: Contract = .{ .version = 0, .alignment = 0, .size = 0, .bit_size = 0, .type_id = 0, .layout_id = 0, .compiler_id = 0 },

    pub fn borrow(db: anytype) LocalQueryDb {
        const Pointer = @typeInfo(@TypeOf(db)).pointer;
        if (Pointer.size != .one or Pointer.attrs.@"const") @compileError("local queries require a mutable DB borrow");
        return .{ .ptr = @ptrCast(db), .layout = Contract.of(Pointer.child) };
    }

    pub fn resolve(self: LocalQueryDb, comptime DB: type) !*DB {
        const ptr = self.ptr orelse return error.InvalidArgument;
        if (!self.layout.matches(Contract.of(DB))) return error.InvalidAbiVersion;
        if (@intFromPtr(ptr) % @alignOf(DB) != 0) return error.InvalidArgument;
        return @ptrCast(@alignCast(ptr));
    }
};

/// One complete local query against a DB owned by the storage archive. The DB
/// and cancellation token are opaque borrowed handles valid only for this
/// synchronous call. No posting, candidate, stored document, or index handle
/// crosses the compiled boundary.
pub const LocalQueryRequest = extern struct {
    version: u32 = abi_version,
    dialect: LocalQueryDialect = .internal,
    kind: LocalQueryKind = .search,
    has_execution_deadline: u8 = 0,
    _reserved0: [3]u8 = @splat(0),
    db: LocalQueryDb = .{},
    table_name: BorrowedBytes = .{},
    request_json: BorrowedBytes = .{},
    execution_options: LocalQueryExecutionOptions = .{},
    cancellation_ctx: ?*anyopaque = null,
    cancellation_fn: ?CancellationCheckFn = null,
    execution_deadline_ns: u64 = 0,
};

pub extern fn antfly_local_query_execute(
    request: *const LocalQueryRequest,
    out_response: *QueryOwnedResponse,
    out_failure: *FailureIdentity,
) callconv(.c) Status;

pub extern fn antfly_local_query_buffer_destroy(buffer: *OwnedBytes) callconv(.c) void;

test "query DB borrow rejects incompatible contracts before pointer use" {
    const std = @import("std");
    const DB = struct { generation: u64 };
    var db: DB = .{ .generation = 42 };
    const borrow = LocalQueryDb.borrow(&db);
    try std.testing.expectEqual(&db, try borrow.resolve(DB));
    try std.testing.expectError(error.InvalidArgument, (LocalQueryDb{}).resolve(DB));
    inline for (.{ "version", "alignment", "size", "bit_size", "type_id", "layout_id", "compiler_id" }) |field| {
        var invalid = borrow;
        @field(invalid.layout, field) +%= 1;
        invalid.ptr = @ptrFromInt(1);
        try std.testing.expectError(error.InvalidAbiVersion, invalid.resolve(DB));
    }
    var misaligned = borrow;
    misaligned.ptr = @ptrFromInt(1);
    try std.testing.expectError(error.InvalidArgument, misaligned.resolve(DB));
}
