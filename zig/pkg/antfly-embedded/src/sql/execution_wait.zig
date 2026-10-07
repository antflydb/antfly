// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Borrowed execution ownership at a private read/spill I/O boundary. The
//! caller prepares stable buffers before entering and resumes before decoding
//! or propagating an I/O failure. This is not a durable-write suspension hook.
const std = @import("std");

pub const Boundary = struct {
    ptr: *anyopaque,
    begin: *const fn (*anyopaque) anyerror!void,
    end: *const fn (*anyopaque) anyerror!void,
};

/// Reacquisition also runs when I/O fails. A failed begin performs no I/O;
/// failed reacquisition prevents application execution with a stale permit.
pub fn run(boundary: ?Boundary, comptime operation: anytype, args: anytype) anyerror!@typeInfo(@typeInfo(@TypeOf(operation)).@"fn".return_type.?).error_union.payload {
    if (boundary) |wait| try wait.begin(wait.ptr);
    const result = @call(.auto, operation, args);
    if (boundary) |wait| try wait.end(wait.ptr);
    return result;
}

test "SQL I/O boundary reacquires before reporting a failed read" {
    const Owner = struct {
        running: bool = true,
        read: bool = false,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.running);
            self.running = false;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!self.running);
            self.running = true;
        }
        fn fail(self: *@This()) !usize {
            try std.testing.expect(!self.running);
            self.read = true;
            return error.BadRead;
        }
    };
    var owner: Owner = .{};
    try std.testing.expectError(error.BadRead, run(.{ .ptr = &owner, .begin = Owner.begin, .end = Owner.end }, Owner.fail, .{&owner}));
    try std.testing.expect(owner.running and owner.read);
}

test "SQL I/O boundary refuses stale execution after failed reacquisition" {
    const Owner = struct {
        begun: bool = false,
        calls: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.begun) return error.QueryCanceled;
            self.begun = true;
        }
        fn end(_: *anyopaque) !void {
            return error.QueryCanceled;
        }
        fn read(self: *@This()) !usize {
            self.calls += 1;
            return 7;
        }
    };
    var owner: Owner = .{};
    const boundary: Boundary = .{ .ptr = &owner, .begin = Owner.begin, .end = Owner.end };
    try std.testing.expectError(error.QueryCanceled, run(boundary, Owner.read, .{&owner}));
    try std.testing.expectError(error.QueryCanceled, run(boundary, Owner.read, .{&owner}));
    try std.testing.expectEqual(@as(usize, 1), owner.calls);
}
