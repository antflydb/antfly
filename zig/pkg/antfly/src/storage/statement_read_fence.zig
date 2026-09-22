// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Short owner-held capture capability. It blocks primary and replay mutation
//! only while the coordinator pins snapshots, never while scanning their rows.
const callbacks = @import("../runtime_callback_abi.zig");
const std = @import("std");
const types = @import("db/types.zig");
const View = @import("relational_read_view.zig").View;

pub const Fence = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: Abi.Dispatch = Abi.local_dispatch,

    pub const VTable = struct {
        validate: *const fn (*anyopaque) anyerror!void,
        open: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, types.ScanOptions) anyerror!View,
        release: *const fn (*anyopaque) void,
    };
    const Abi = callbacks.Boundary(VTable);

    pub fn validate(self: Fence) !void {
        return Abi.call("validate", self.boundary_dispatch, self.vtable.validate, .{self.ptr});
    }

    /// The fence itself is the admission proof. Re-running a Raft read-index
    /// barrier while mutation is frozen could wait for an apply it blocks.
    pub fn open(self: Fence, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !View {
        return Abi.call("open", self.boundary_dispatch, self.vtable.open, .{ self.ptr, alloc, from, to, opts });
    }

    pub fn deinit(self: Fence) void {
        Abi.call("release", self.boundary_dispatch, self.vtable.release, .{self.ptr}) catch unreachable;
    }
};
