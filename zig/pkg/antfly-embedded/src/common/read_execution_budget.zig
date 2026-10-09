// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const runtime = @import("antfly_runtime_abi");

/// Synchronous read admission under the caller's original controls. The budget
/// and executor outlive all cursor callbacks; no returned owner retains them.
pub fn Budget(comptime Controls: type, comptime check: anytype) type {
    return struct {
        controls: Controls,
        io: std.Io,
        rejection: ?anyerror = null,
        parked_class: ?runtime.runnable_abi.Class = null,

        pub fn checkpoint(self: *@This()) !void {
            if (self.rejection) |err| return err;
            check(self.controls) catch |err| {
                self.rejection = err;
                return err;
            };
        }
        pub fn executionBoundary(self: *@This()) ?@import("execution_wait.zig").Boundary {
            if (self.controls.execution_runnable == null) return null;
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkpoint();
            if (self.parked_class != null) return error.InvalidRunnableTransition;
            const borrow = self.controls.execution_runnable orelse return error.ExecutionWaitUnsupported;
            try borrow.validate();
            const phase = try borrow.currentClass();
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.parked_class = phase;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rejection) |err| return err;
            const phase = self.parked_class orelse return error.InvalidRunnableTransition;
            const borrow = self.controls.execution_runnable orelse return error.ExecutionWaitUnsupported;
            checked(borrow.enter(borrow.context, @backingInt(phase))) catch |err| {
                self.rejection = err;
                return err;
            };
            self.parked_class = null;
            try self.checkpoint();
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.checkpoint();
            if (self.executionBoundary()) |boundary| {
                boundary.run(std.Io.sleep, .{ self.io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake }) catch |err| {
                    self.rejection = err;
                    return err;
                };
            } else try self.io.sleep(.fromMilliseconds(1), .awake);
            try self.checkpoint();
        }
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
    };
}
