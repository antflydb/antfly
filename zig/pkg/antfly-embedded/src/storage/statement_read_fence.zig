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

//! Short owner-held capture capability. It blocks primary and replay mutation
//! only while the coordinator pins snapshots, never while scanning their rows.
const callbacks = @import("../runtime_callback_abi.zig");
const std = @import("std");
const types = @import("db/types.zig");
const Admission = @import("../common/read_admission.zig").Port;
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
        open_with_admission: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, types.ScanOptions, Admission) anyerror!View = null,
        capture_snapshot_with_admission: ?*const fn (*anyopaque, std.mem.Allocator, Admission) anyerror!Snapshot = null,
        validate_with_admission: ?*const fn (*anyopaque, Admission) anyerror!void = null,
    };
    const Abi = callbacks.Boundary(VTable);

    pub fn validate(self: Fence) !void {
        return Abi.call("validate", self.boundary_dispatch, self.vtable.validate, .{self.ptr});
    }

    pub fn validateWithAdmission(self: Fence, original: Admission) !void {
        var retained_dependency: @import("../common/read_admission.zig").RetainedDependency = .{};
        const control = retained_dependency.bind(original).?;
        try control.checkpoint();
        if (self.vtable.validate_with_admission) |callback|
            try Abi.call("validate_with_admission", self.boundary_dispatch, callback, .{ self.ptr, control })
        else
            try self.validate();
        try control.checkpoint();
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

    pub fn captureSnapshotWithAdmission(self: Fence, alloc: std.mem.Allocator, original: Admission) !Snapshot {
        var retained_dependency: @import("../common/read_admission.zig").RetainedDependency = .{};
        const control = retained_dependency.bind(original).?;
        try control.checkpoint();
        const snapshot = if (self.vtable.capture_snapshot_with_admission) |callback|
            try Abi.call("capture_snapshot_with_admission", self.boundary_dispatch, callback, .{ self.ptr, alloc, control })
        else
            try self.captureSnapshot(alloc);
        errdefer snapshot.deinit();
        try control.checkpoint();
        return snapshot;
    }

    /// Creation borrows admission for this call; ScanOptions retain only the
    /// independently owned resource lifetime controls.
    pub fn openWithAdmission(self: Fence, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, original: Admission) !View {
        var retained_dependency: @import("../common/read_admission.zig").RetainedDependency = .{};
        const control = retained_dependency.bind(original).?;
        try control.checkpoint();
        const view = if (self.vtable.open_with_admission) |callback|
            try Abi.call("open_with_admission", self.boundary_dispatch, callback, .{ self.ptr, alloc, from, to, opts, control })
        else
            try self.open(alloc, from, to, opts);
        errdefer view.deinit();
        try control.checkpoint();
        return view;
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
        open_with_admission: ?*const fn (*anyopaque, std.mem.Allocator, []const u8, []const u8, types.ScanOptions, Admission) anyerror!View = null,
    };
    const SnapshotAbi = callbacks.Boundary(VTable);

    pub fn open(self: Snapshot, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions) !View {
        return SnapshotAbi.call("open", self.boundary_dispatch, self.vtable.open, .{ self.ptr, alloc, from, to, opts });
    }

    /// Creation borrows admission for this call; ScanOptions retain only the
    /// independently owned resource lifetime controls.
    pub fn openWithAdmission(self: Snapshot, alloc: std.mem.Allocator, from: []const u8, to: []const u8, opts: types.ScanOptions, control: Admission) !View {
        try control.checkpoint();
        const view = if (self.vtable.open_with_admission) |callback|
            try SnapshotAbi.call("open_with_admission", self.boundary_dispatch, callback, .{ self.ptr, alloc, from, to, opts, control })
        else
            try self.open(alloc, from, to, opts);
        errdefer view.deinit();
        try control.checkpoint();
        return view;
    }

    pub fn deinit(self: Snapshot) void {
        SnapshotAbi.call("release", self.boundary_dispatch, self.vtable.release, .{self.ptr}) catch unreachable;
    }
};

test "dynamic statement snapshot must be explicitly supplied by the fenced owner" {
    const Fixture = struct {
        pub fn validate(_: *anyopaque) !void {}
        fn open(_: *anyopaque, _: std.mem.Allocator, _: []const u8, _: []const u8, _: types.ScanOptions) !View {
            return error.TestUnexpectedResult;
        }
        fn release(_: *anyopaque) void {}
    };
    var token: u8 = 0;
    const fence: Fence = .{ .ptr = &token, .vtable = &.{ .validate = Fixture.validate, .open = Fixture.open, .release = Fixture.release } };
    try std.testing.expectError(error.SqlStatementSnapshotRequired, fence.captureSnapshot(std.testing.allocator));
}
