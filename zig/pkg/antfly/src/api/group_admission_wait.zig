// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const sources = @import("antfly_local_sources");
const runtime = @import("antfly_runtime_abi");
const Context = sources.api_local_transaction_contract.PreDecisionContext;
const Controls = struct {
    context: Context = .{},
    execution_runnable: ?runtime.runnable_abi.Borrowed = null,
};
fn check(controls: Controls) !void {
    try sources.api_local_table_writes.ensurePreDecisionContextActive(controls.context);
}
pub const Budget = sources.common_read_execution_budget.Budget(Controls, check);
pub fn preDecision(context: Context, io: std.Io) Budget {
    return .{ .io = io, .controls = .{ .context = context, .execution_runnable = context.execution_runnable } };
}
pub fn completion(cancellation: sources.storage_db_types.CancellationToken, io: std.Io) Budget {
    return preDecision(.{ .cancellation = cancellation }, io);
}

/// Caller owns the mutex on entry and on every return, including rejection.
/// Never await CPU reentry with the activity mutex retained.
pub fn wait(io: std.Io, mutex: *std.Io.Mutex, condition: *std.Io.Condition, controls: anytype) !void {
    try controls.checkpoint();
    const boundary = controls.executionBoundary();
    if (boundary) |b| try b.beginWait();
    const result = condition.waitTimeout(io, mutex, .{ .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake } });
    mutex.unlock(io);
    const resumed = if (boundary) |b| b.endWait() else @as(anyerror!void, {});
    mutex.lockUncancelable(io);
    try resumed;
    result catch |err| switch (err) {
        error.Timeout => {},
        else => return err,
    };
    try controls.checkpoint();
}

test "group admission wait releases activity mutex before mandatory CPU resumption" {
    const Boundary = @TypeOf(@as(*Budget, undefined).executionBoundary().?);
    const Fixture = struct {
        io: std.Io,
        mutex: *std.Io.Mutex,
        rejected: bool,
        expired: bool = false,
        parks: usize = 0,
        resumes: usize = 0,
        pub fn checkpoint(self: *@This()) !void {
            if (self.expired) return error.DeadlineExceeded;
        }
        pub fn executionBoundary(self: *@This()) ?Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.parks += 1;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // An operation admitted while we were parked can acquire this
            // mutex during our original CPU reentry, avoiding a lock cycle.
            try std.testing.expect(self.mutex.tryLock());
            self.mutex.unlock(self.io);
            self.resumes += 1;
            if (self.rejected) return error.SchedulerClosed;
            self.expired = true;
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |rejected| {
        var mutex: std.Io.Mutex = .init;
        var condition: std.Io.Condition = .init;
        var fixture: Fixture = .{ .io = io, .mutex = &mutex, .rejected = rejected };
        mutex.lockUncancelable(io);
        defer mutex.unlock(io);
        const expected: anyerror = if (rejected) error.SchedulerClosed else error.DeadlineExceeded;
        try std.testing.expectError(expected, wait(io, &mutex, &condition, &fixture));
        // Rejection returns locked for the caller's registered-waiter cleanup.
        try std.testing.expect(!mutex.tryLock());
        try std.testing.expectEqual(@as(usize, 1), fixture.parks);
        try std.testing.expectEqual(fixture.parks, fixture.resumes);
    }
}

test "group admission wait retains caller clock and sticky completion rejection" {
    const Clock = struct {
        now_ns: i96 = 100,
        fn now(raw: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return .{ .nanoseconds = self.now_ns };
        }
        const vtable = blk: {
            var value = std.Io.failing.vtable.*;
            value.now = now;
            break :blk value;
        };
    };
    var clock: Clock = .{};
    const owner_io: std.Io = .{ .userdata = &clock, .vtable = &Clock.vtable };
    const io = std.testing.io;
    var mutex: std.Io.Mutex = .init;
    var condition: std.Io.Condition = .init;
    mutex.lockUncancelable(io);
    defer mutex.unlock(io);
    var original = preDecision(.{ .deadline_ns = 200, .deadline_io = runtime.io_abi.Borrow.init(&owner_io) }, io);
    // The condition's physical poll does not replace the caller's clock.
    try wait(io, &mutex, &condition, &original);
    clock.now_ns = 200;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, wait(io, &mutex, &condition, &original));
    clock.now_ns = 100;
    try std.testing.expectError(error.PreDecisionDeadlineExceeded, original.checkpoint());
    var canceled = std.atomic.Value(bool).init(false);
    var retained = completion(sources.storage_db_types.CancellationToken.fromAtomic(&canceled), io);
    try wait(io, &mutex, &condition, &retained);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, wait(io, &mutex, &condition, &retained));
    canceled.store(false, .release);
    try std.testing.expectError(error.Canceled, retained.checkpoint());
}
