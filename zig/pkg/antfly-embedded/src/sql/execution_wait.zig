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

/// An I/O result can acquire a handle before cancellation, expiry or shutdown
/// prevents reacquisition. Its original owner must discard that result; callers
/// never receive an owned value under a stale runnable permit. Discard is
/// mandatory cleanup and must not depend on a new execution grant.
pub fn runOwned(boundary: ?Boundary, comptime operation: anytype, args: anytype, comptime discard: anytype, discard_args: anytype) anyerror!@typeInfo(@typeInfo(@TypeOf(operation)).@"fn".return_type.?).error_union.payload {
    if (boundary) |wait| try wait.begin(wait.ptr);
    const result = @call(.auto, operation, args);
    if (boundary) |wait| wait.end(wait.ptr) catch |err| {
        if (result) |value| @call(.auto, discard, .{value} ++ discard_args) else |_| {}
        return err;
    };
    return result;
}

test "SQL owned I/O result retires after failed resume without skipping cleanup" {
    const Case = enum { success, begin_failure, io_failure, resume_failure, both_fail };
    const Owner = struct {
        case: Case,
        running: bool = true,
        calls: usize = 0,
        ends: usize = 0,
        live: usize = 0,
        discarded: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.case == .begin_failure) return error.QueryCanceled;
            self.running = false;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ends += 1;
            if (self.case == .resume_failure or self.case == .both_fail) return error.SchedulerClosed;
            self.running = true;
        }
        fn open(self: *@This()) !usize {
            try std.testing.expect(!self.running);
            self.calls += 1;
            if (self.case == .io_failure or self.case == .both_fail) return error.InputOutput;
            self.live += 1;
            return 7;
        }
        fn discard(handle: usize, self: *@This()) void {
            std.debug.assert(handle == 7 and self.live == 1);
            self.live -= 1;
            self.discarded += 1;
        }
    };
    for (std.enums.values(Case)) |case| {
        var owner: Owner = .{ .case = case };
        const result = runOwned(.{ .ptr = &owner, .begin = Owner.begin, .end = Owner.end }, Owner.open, .{&owner}, Owner.discard, .{&owner});
        switch (case) {
            .success => {
                const handle = try result;
                try std.testing.expect(owner.running);
                try std.testing.expectEqual(@as(usize, 0), owner.discarded);
                Owner.discard(handle, &owner);
            },
            .begin_failure => try std.testing.expectError(error.QueryCanceled, result),
            .io_failure => try std.testing.expectError(error.InputOutput, result),
            .resume_failure, .both_fail => try std.testing.expectError(error.SchedulerClosed, result),
        }
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), owner.calls);
        try std.testing.expectEqual(owner.calls, owner.ends);
        try std.testing.expectEqual(@as(usize, 0), owner.live);
        try std.testing.expectEqual(@as(usize, if (case == .success or case == .resume_failure) 1 else 0), owner.discarded);
    }
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
