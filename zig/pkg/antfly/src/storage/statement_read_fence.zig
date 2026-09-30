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
        capture_snapshot: ?*const fn (*anyopaque, std.mem.Allocator) anyerror!Snapshot = null,
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

    /// Forkable immutable cut, captured while the short owner fence is held.
    /// Callers release the fence before opening delayed SQL branches.
    pub fn captureSnapshot(self: Fence, alloc: std.mem.Allocator) !Snapshot {
        const capture = self.vtable.capture_snapshot orelse return error.SqlStatementSnapshotRequired;
        return Abi.call("capture_snapshot", self.boundary_dispatch, capture, .{ self.ptr, alloc });
    }

    pub fn deinit(self: Fence) void {
        Abi.call("release", self.boundary_dispatch, self.vtable.release, .{self.ptr}) catch unreachable;
    }
};

pub const Snapshot = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    boundary_dispatch: SnapshotAbi.Dispatch = SnapshotAbi.local_dispatch,

    pub const VTable = struct {
        open: *const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, types.ScanOptions) anyerror!View,
        release: *const fn (*anyopaque) void,
    };
    const SnapshotAbi = callbacks.Boundary(VTable);

    pub fn open(self: Snapshot, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !View {
        return SnapshotAbi.call("open", self.boundary_dispatch, self.vtable.open, .{ self.ptr, alloc, from, to, opts });
    }

    pub fn deinit(self: Snapshot) void {
        SnapshotAbi.call("release", self.boundary_dispatch, self.vtable.release, .{self.ptr}) catch unreachable;
    }
};

test "dynamic statement snapshot must be explicitly supplied by the fenced owner" {
    const Fixture = struct {
        fn validate(_: *anyopaque) !void {}
        fn open(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !View {
            return error.TestUnexpectedResult;
        }
        fn release(_: *anyopaque) void {}
    };
    var token: u8 = 0;
    const fence: Fence = .{ .ptr = &token, .vtable = &.{ .validate = Fixture.validate, .open = Fixture.open, .release = Fixture.release } };
    try std.testing.expectError(error.SqlStatementSnapshotRequired, fence.captureSnapshot(std.testing.allocator));
}
