// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const runtime = @import("antfly_runtime_abi");

/// One retained recovery ceiling. A completion clock may differ from the
/// transport clock: project its remaining duration, never its absolute value.
/// Sampling transport time first avoids granting time spent querying the owner.
pub const Budget = struct {
    now: *const fn () u64,
    maximum_ns: u64,
    ceiling: ?u64 = null,

    pub fn deadline(self: *Budget, completion: ?runtime.runnable_abi.Borrowed) !u64 {
        const before = self.now();
        if (self.ceiling) |ceiling| if (before >= ceiling) return error.DeadlineExceeded;
        const remaining = if (completion) |borrow| @min(self.maximum_ns, try borrow.completionRemainingNs()) else self.maximum_ns;
        if (remaining == 0) return error.DeadlineExceeded;
        const projected = before +| remaining;
        self.ceiling = if (self.ceiling) |prior| @min(prior, projected) else projected;
        if (self.now() >= self.ceiling.?) return error.DeadlineExceeded;
        return self.ceiling.?;
    }
};

const Fixture = struct {
    var time: u64 = 0;
    remaining: u64 = 100,
    observation_ns: u64 = 0,
    failure: ?anyerror = null,
    fn now() u64 {
        return time;
    }
    fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
        return .ok;
    }
    fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
        return .ok;
    }
    fn budget(raw: *anyopaque, out: *u64) callconv(.c) runtime.error_abi.Status {
        const self: *@This() = @ptrCast(@alignCast(raw));
        time += self.observation_ns;
        if (self.failure) |err| return runtime.error_abi.statusFromError(err);
        out.* = self.remaining;
        return .ok;
    }
    fn borrow(self: *@This()) runtime.runnable_abi.Borrowed {
        return .{ .context = self, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .checkpoint = check, .park = phase, .enter = phase, .completion_remaining_ns = budget };
    }
};

test "decision recovery budget projects independent durations and never renews a ceiling" {
    Fixture.time = 1000;
    var fixture: Fixture = .{};
    var budget: Budget = .{ .now = Fixture.now, .maximum_ns = 300 };
    try std.testing.expectEqual(@as(u64, 1100), try budget.deadline(fixture.borrow()));
    Fixture.time = 1050;
    // Even a misbehaving callback reporting a fresh budget cannot extend it.
    try std.testing.expectEqual(@as(u64, 1100), try budget.deadline(fixture.borrow()));
    fixture.remaining = 10;
    try std.testing.expectEqual(@as(u64, 1060), try budget.deadline(fixture.borrow()));
    Fixture.time = 1060;
    try std.testing.expectError(error.DeadlineExceeded, budget.deadline(fixture.borrow()));
}

test "decision recovery budget charges observation time and fails closed on missing authority" {
    Fixture.time = 0;
    var fixture: Fixture = .{ .remaining = 10, .observation_ns = 10 };
    var budget: Budget = .{ .now = Fixture.now, .maximum_ns = 300 };
    try std.testing.expectError(error.DeadlineExceeded, budget.deadline(fixture.borrow()));
    Fixture.time = 0;
    budget.ceiling = null;
    fixture.observation_ns = 0;
    var missing = fixture.borrow();
    missing.completion_remaining_ns = null;
    try std.testing.expectError(error.UnsupportedOperation, budget.deadline(missing));
    try std.testing.expect(budget.ceiling == null);
    fixture.failure = error.ExecutionWaitUnsupported;
    try std.testing.expectError(error.ExecutionWaitUnsupported, budget.deadline(fixture.borrow()));
    fixture.failure = null;
    fixture.remaining = 0;
    try std.testing.expectError(error.DeadlineExceeded, budget.deadline(fixture.borrow()));
    try std.testing.expect(budget.ceiling == null);
}

test "decision recovery budget bounds legacy execution without a team" {
    Fixture.time = 20;
    var budget: Budget = .{ .now = Fixture.now, .maximum_ns = 30 };
    try std.testing.expectEqual(@as(u64, 50), try budget.deadline(null));
    Fixture.time = 49;
    try std.testing.expectEqual(@as(u64, 50), try budget.deadline(null));
    Fixture.time = 50;
    try std.testing.expectError(error.DeadlineExceeded, budget.deadline(null));
}
