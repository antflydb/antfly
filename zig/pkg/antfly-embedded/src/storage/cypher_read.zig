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

//! Leased owner graph reads. Remote owners use the private JSON protocol;
//! coordinators may also expose a typed in-process operation.
const std = @import("std");
const rpc = @import("cypher").rpc;
const callbacks = @import("../runtime_callback_abi.zig");
pub const Input = struct {
    /// Internal cohort capture: the first operation MUST be validate while
    /// every owner's mutation fence remains held. Other operations fail closed.
    defer_validation: bool = false,
    catalog_json: []const u8,
    /// Private catalog allowlist for a SQL statement referencing several graphs.
    catalogs_json: []const u8 = "",
    table: []const u8,
    table_id: u64,
    schema_version: u32 = 0,
    cut_id: [64]u8,
    expires_ms: u64,
    ttl_now_ns: u64,
    principal_proof: []const u8 = "",
    database: []const u8 = "default",
    filter_query_json: []const u8 = "",
    /// Null retains every managed projection; empty retains primary data only.
    indexes: ?[]const []const u8 = null,
};
pub const Table = struct { indexes: ?[]const []const u8 = null, name: []const u8, table_id: u64, schema_version: u32, principal_proof: []const u8 = "", database: []const u8 = "default", filter_query_json: []const u8 = "" };
pub const Statement = struct {
    partial_graph: bool = false,
    catalog_json: []const u8,
    /// Private catalog allowlist for a SQL statement referencing several graphs.
    catalogs_json: []const u8 = "",
    tables: []const Table,
    cut_id: [64]u8,
    expires_ms: u64,
    ttl_now_ns: u64,
    deadline_ns: ?u64 = null,
    cancellation: @import("antfly_cancellation").CancellationToken = .none,
};
pub const Read = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: Abi.Dispatch = Abi.local_dispatch,
    pub const VTable = struct {
        invoke: *const fn (*anyopaque, std.mem.Allocator, []const u8) anyerror![]u8,
        invoke_typed: ?*const fn (*anyopaque, std.mem.Allocator, std.mem.Allocator, rpc.Request) anyerror!rpc.Reply = null,
        release: *const fn (*anyopaque) void,
        open_sql: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, []const u8, @import("db/types.zig").ScanOptions) anyerror!@import("relational_read_view.zig").View = null,
    };
    const Abi = callbacks.Boundary(VTable);
    pub fn invoke(self: Read, a: std.mem.Allocator, request: []const u8) ![]u8 {
        return Abi.call("invoke", self.boundary_dispatch, self.vtable.invoke, .{ self.ptr, a, request });
    }
    pub fn invokeTyped(self: Read, a: std.mem.Allocator, request: rpc.Request) !rpc.Reply {
        return self.invokeTypedWithScratch(a, @import("cypher").backend.reclaimingAllocator(a), request);
    }
    /// Scratch is an explicitly reclaimable allocator, separate from the
    /// reply arena. Arena vtable identities are local to each native runtime.
    pub fn invokeTypedWithScratch(self: Read, a: std.mem.Allocator, scratch: std.mem.Allocator, request: rpc.Request) !rpc.Reply {
        if (self.vtable.invoke_typed) |callback| return Abi.call("invoke_typed", self.boundary_dispatch, callback, .{ self.ptr, a, scratch, request });
        const body = try std.json.Stringify.valueAlloc(a, request, .{});
        defer a.free(body);
        if (body.len > 1 << 20) return error.CypherRequestTooLarge;
        const response = try self.invoke(a, body);
        defer a.free(response);
        if (response.len > 32 << 20) return error.CypherResponseTooLarge;
        return std.json.parseFromSliceLeaky(rpc.Reply, a, response, .{ .allocate = .alloc_always });
    }
    pub fn openSql(self: Read, a: std.mem.Allocator, table: []const u8, from: []const u8, to: []const u8, opts: @import("db/types.zig").ScanOptions) !@import("relational_read_view.zig").View {
        const callback = self.vtable.open_sql orelse return error.SqlStatementSnapshotRequired;
        return Abi.call("open_sql", self.boundary_dispatch, callback, .{ self.ptr, a, table, from, to, opts });
    }
    pub fn deinit(self: Read) void {
        Abi.call("release", self.boundary_dispatch, self.vtable.release, .{self.ptr}) catch unreachable;
    }
};
