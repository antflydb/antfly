// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Storage-owned statement snapshot capability shared with the API coordinator.
const std = @import("std");
const callbacks = @import("../runtime_callback_abi.zig");
const types = @import("db/types.zig");

pub const View = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: Abi.Dispatch = Abi.local_dispatch,

    pub const Row = struct {
        id: []const u8,
        version: u64,
        schema_version: u32,
        value: std.json.Value,
        expected_content_digest: ?[32]u8 = null,
        document: ?std.json.Value = null,
        /// SQL NULL flags aligned with value.object insertion order. A JSON
        /// payload containing null has a false flag.
        sql_nulls: ?[]const bool = null,
    };
    pub const Page = struct {
        arena: std.heap.ArenaAllocator,
        rows: []const Row,
        after: ?[]const u8,

        pub fn deinit(self: *Page) void {
            self.arena.deinit();
            self.* = undefined;
        }
    };
    pub const VTable = struct {
        range_proofs: ?*const fn (*anyopaque, std.mem.Allocator) anyerror![]@import("range_protection.zig").Proof = null,
        next: *const fn (*anyopaque, std.mem.Allocator, u32) anyerror!Page,
        normalize: ?*const fn (*anyopaque, std.mem.Allocator, []const types.BatchWrite) anyerror![]types.BatchWrite = null,
        close: *const fn (*anyopaque) void,
    };
    const Abi = callbacks.Boundary(VTable);

    pub fn next(self: View, alloc: std.mem.Allocator, limit: u32) !Page {
        return Abi.call("next", self.boundary_dispatch, self.vtable.next, .{ self.ptr, alloc, limit });
    }
    pub fn rangeProofs(self: View, alloc: std.mem.Allocator) ![]@import("range_protection.zig").Proof {
        const callback = self.vtable.range_proofs orelse return error.SqlRangeTrackingRequired;
        return Abi.call("range_proofs", self.boundary_dispatch, callback, .{ self.ptr, alloc });
    }
    pub fn normalize(self: View, alloc: std.mem.Allocator, writes: []const types.BatchWrite) ![]types.BatchWrite {
        const callback = self.vtable.normalize orelse return error.UnsupportedSqlExecution;
        return Abi.call("normalize", self.boundary_dispatch, callback, .{ self.ptr, alloc, writes });
    }
    pub fn deinit(self: View) void {
        Abi.call("close", self.boundary_dispatch, self.vtable.close, .{self.ptr}) catch unreachable;
    }
};
