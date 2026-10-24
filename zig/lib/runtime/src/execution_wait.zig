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

//! Borrowed runnable ownership at an actual storage/network wait. The caller
//! keeps stable inputs and original resource owners until I/O and cleanup join.
//! CPU preparation/decoding and whole backend calls are not wait boundaries.
const std = @import("std");
const callbacks = @import("runtime_callback_abi.zig");

pub const Boundary = struct {
    ptr: *anyopaque,
    begin: *const fn (*anyopaque) anyerror!void,
    end: *const fn (*anyopaque) anyerror!void,
    boundary_dispatch: Abi.Dispatch = local_dispatch,

    pub const local_dispatch = Abi.local_dispatch;

    const VTable = struct {
        begin: *const fn (*anyopaque) anyerror!void,
        end: *const fn (*anyopaque) anyerror!void,
    };
    const Abi = callbacks.Boundary(VTable);

    /// Before entering a genuine wait, release only runnable ownership.
    pub fn beginWait(self: Boundary) !void {
        try Abi.call("begin", self.boundary_dispatch, self.begin, .{self.ptr});
    }
    /// Reacquire and revalidate authority before consuming the I/O result.
    pub fn endWait(self: Boundary) !void {
        try Abi.call("end", self.boundary_dispatch, self.end, .{self.ptr});
    }

    pub fn run(self: Boundary, comptime operation: anytype, args: anytype) anyerror!Payload(@TypeOf(operation)) {
        try self.beginWait();
        const result = @call(.auto, operation, args);
        try self.endWait();
        return result;
    }

    /// Mandatory discard uses the original owner and must not ask for another
    /// grant. Failed begin performs no I/O. Failed end cannot publish a newly
    /// acquired handle, allocation or remote-result owner.
    pub fn runOwned(self: Boundary, comptime operation: anytype, args: anytype, comptime discard: anytype, discard_args: anytype) anyerror!Payload(@TypeOf(operation)) {
        try self.beginWait();
        const result = @call(.auto, operation, args);
        self.endWait() catch |err| {
            if (result) |value| @call(.auto, discard, .{value} ++ discard_args) else |_| {}
            return err;
        };
        return result;
    }

    fn Payload(comptime Operation: type) type {
        return @typeInfo(@typeInfo(Operation).@"fn".return_type.?).error_union.payload;
    }
};

test "execution wait checked dispatch preserves scheduler and authority errors" {
    const error_abi = @import("runtime_error_abi.zig");
    const native_abi = @import("runtime_native_abi.zig");
    const Owner = struct {
        begins: usize = 0,
        ends: usize = 0,
        fail: ?anyerror = null,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.begins += 1;
            if (self.fail) |err| return err;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            if (self.fail) |err| return err;
        }
        // A separate trampoline address forces the checked C status path;
        // this is not a proof of separately compiled archive integration.
        fn dispatch(contract: *const native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) error_abi.Status {
            return Boundary.Abi.local_dispatch(contract, callback, args, output);
        }
        fn invalidDispatch(contract: *const native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: ?*anyopaque) callconv(.c) error_abi.Status {
            var invalid = contract.*;
            invalid.version += 1;
            return Boundary.Abi.local_dispatch(&invalid, callback, args, output);
        }
    };
    var owner: Owner = .{};
    var boundary: Boundary = .{ .ptr = &owner, .begin = Owner.begin, .end = Owner.end, .boundary_dispatch = Owner.dispatch };
    try boundary.beginWait();
    try boundary.endWait();
    const failures = [_]anyerror{ error.RunnableWaitTimeout, error.SchedulerClosed, error.SchedulerFull, error.InvalidRunnableTransition, error.InvalidReadFenceTransition, error.ExecutionWaitUnsupported, error.ReadSafetyBarrierUnavailable, error.BackendRuntimeUnavailable, error.Forbidden, error.CatalogGenerationChanged, error.RowPolicyCatalogChanged, error.Canceled, error.DeadlineExceeded, error.OutOfMemory };
    for (failures) |err| {
        owner.fail = err;
        try std.testing.expect(error_abi.errorHasStableDetail(err));
        try std.testing.expectError(err, boundary.beginWait());
        try std.testing.expectError(err, boundary.endWait());
    }
    const begins = owner.begins;
    const ends = owner.ends;
    boundary.boundary_dispatch = Owner.invalidDispatch;
    try std.testing.expectError(error.UnsupportedVersion, boundary.beginWait());
    try std.testing.expectError(error.UnsupportedVersion, boundary.endWait());
    try std.testing.expectEqual(begins, owner.begins);
    try std.testing.expectEqual(ends, owner.ends);
}

test "execution wait owned storage results unwind under original owner before error return" {
    const Case = enum { success, begin_failure, io_failure, end_failure, both_fail };
    const Owner = struct {
        case: Case,
        running: bool = true,
        opened: usize = 0,
        retired: usize = 0,
        ends: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.case == .begin_failure) return error.Canceled;
            self.running = false;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            if (self.case == .end_failure or self.case == .both_fail) return error.Forbidden;
            self.running = true;
        }
        fn open(self: *@This()) !usize {
            try std.testing.expect(!self.running);
            if (self.case == .io_failure or self.case == .both_fail) return error.InputOutput;
            self.opened += 1;
            return 7;
        }
        fn discard(handle: usize, self: *@This()) void {
            std.debug.assert(handle == 7 and self.opened == 1 and self.retired == 0);
            self.retired += 1;
        }
    };
    for (std.enums.values(Case)) |case| {
        var owner: Owner = .{ .case = case };
        const boundary: Boundary = .{ .ptr = &owner, .begin = Owner.begin, .end = Owner.end };
        const result = boundary.runOwned(Owner.open, .{&owner}, Owner.discard, .{&owner});
        switch (case) {
            .success => {
                const handle = try result;
                try std.testing.expect(owner.running);
                try std.testing.expectEqual(@as(usize, 0), owner.retired);
                Owner.discard(handle, &owner);
            },
            .begin_failure => try std.testing.expectError(error.Canceled, result),
            .io_failure => try std.testing.expectError(error.InputOutput, result),
            .end_failure, .both_fail => try std.testing.expectError(error.Forbidden, result),
        }
        try std.testing.expectEqual(owner.opened, owner.retired);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), owner.ends);
    }
}
