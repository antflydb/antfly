// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Original HA lifetime under retained cache/native-owner leases. This layer
//! never parks CPU or replaces the caller's deadline/cleanup authority.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const cancellation_mod = @import("antfly_cancellation");
const clock_mod = @import("antfly_platform").clock;
pub const Control = runtime.runnable_abi.Reentry;

pub fn checkpoint(control: ?*const Control) !void {
    if (control) |original| try original.checkpoint();
}
pub fn cancellation(control: ?*const Control) cancellation_mod.CancellationToken {
    const original = control orelse return .none;
    return .{ .ptr = original, .check_fn = check };
}
fn check(raw: *const anyopaque) !void {
    const control: *const Control = @ptrCast(@alignCast(raw));
    try control.checkpoint();
}
pub fn lockAtomic(io: std.Io, mutex: *std.atomic.Mutex, control: *const Control) !void {
    try checkpoint(control);
    while (!mutex.tryLock()) {
        try checkpoint(control);
        io.sleep(.fromMilliseconds(1), .awake) catch |err| {
            try checkpoint(control);
            return err;
        };
    }
    errdefer mutex.unlock();
    try checkpoint(control);
}
pub fn lockIo(io: std.Io, mutex: *std.Io.Mutex, control: *const Control) !void {
    try checkpoint(control);
    while (!mutex.tryLock()) {
        try checkpoint(control);
        io.sleep(.fromMilliseconds(1), .awake) catch |err| {
            try checkpoint(control);
            return err;
        };
    }
    errdefer mutex.unlock(io);
    try checkpoint(control);
}
/// Preserve the existing 25 ms retry pacing, polling the original lifetime
/// during it. The native maintenance deadline stays in its own clock domain.
pub fn retryWait(clock: clock_mod.Clock, deadline_ns: u64, control: ?*const Control, io: ?std.Io) !void {
    try checkpoint(control);
    const start = clock.nowRealtimeNs();
    if (start >= deadline_ns) return error.EnrichmentWaitTimeout;
    const until = @min(deadline_ns, start +| 25 * std.time.ns_per_ms);
    const wait_ms = @max(1, (until - start +| std.time.ns_per_ms - 1) / std.time.ns_per_ms);
    if (control == null) {
        // Keep legacy clock-driven retry semantics, including manual clocks
        // whose sleep yields to a worker without advancing simulated time.
        clock.sleepMs(wait_ms);
        return;
    }
    var remaining_ms = wait_ms;
    while (remaining_ms != 0) : (remaining_ms -= 1) {
        try checkpoint(control);
        if (clock.nowRealtimeNs() >= deadline_ns) return error.EnrichmentWaitTimeout;
        if (io) |executor| {
            executor.sleep(.fromMilliseconds(1), .awake) catch |err| {
                try checkpoint(control);
                return err;
            };
        } else clock.sleepMs(1);
    }
    try checkpoint(control);
    if (clock.nowRealtimeNs() >= deadline_ns) return error.EnrichmentWaitTimeout;
}

test "HA seed maintenance lifetime retains original errors during native retry pacing" {
    const TestClock = struct {
        now_ns: u64 = 0,
        fn now(raw: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.now_ns;
        }
        fn sleep(raw: ?*anyopaque, ms: u64) void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.now_ns += ms * std.time.ns_per_ms;
        }
        fn clock(self: *@This()) clock_mod.Clock {
            return .{ .ctx = self, .now_realtime_ns_fn = now, .sleep_ms_fn = sleep };
        }
    };
    var clock: TestClock = .{};
    const Fixture = struct {
        clock: *TestClock,
        cause: anyerror,
        calls: usize = 0,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.clock.clock().nowRealtimeNs() >= 3 * std.time.ns_per_ms) return runtime.error_abi.statusFromError(self.cause);
            return .ok;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |cause| {
        clock = .{};
        var fixture: Fixture = .{ .clock = &clock, .cause = cause };
        const control: Control = .{ .context = &fixture, .check = Fixture.check };
        try std.testing.expectError(cause, retryWait(clock.clock(), 100 * std.time.ns_per_ms, &control, null));
        try std.testing.expectEqual(@as(u64, 3 * std.time.ns_per_ms), clock.clock().nowRealtimeNs());
        try std.testing.expectError(cause, cancellation(&control).check());
    }
    clock = .{};
    try retryWait(clock.clock(), 100 * std.time.ns_per_ms, null, null);
    try std.testing.expectEqual(@as(u64, 25 * std.time.ns_per_ms), clock.clock().nowRealtimeNs());
    try retryWait(clock.clock(), 27 * std.time.ns_per_ms, null, null);
    try std.testing.expectError(error.EnrichmentWaitTimeout, retryWait(clock.clock(), 27 * std.time.ns_per_ms, null, null));
}

test "HA seed maintenance lifetime rejects retained mutex contention and releases only acquired locks" {
    const Fixture = struct {
        calls: usize = 0,
        fail_after: usize,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls >= self.fail_after) return runtime.error_abi.statusFromError(error.DeadlineExceeded);
            return .ok;
        }
    };
    for ([_]bool{ false, true }) |held| {
        var atomic_mutex: std.atomic.Mutex = .unlocked;
        if (held) try std.testing.expect(atomic_mutex.tryLock());
        defer if (held) atomic_mutex.unlock();
        var fixture: Fixture = .{ .fail_after = if (held) 3 else 2 };
        const control: Control = .{ .context = &fixture, .check = Fixture.check };
        try std.testing.expectError(error.DeadlineExceeded, lockAtomic(std.testing.io, &atomic_mutex, &control));
        try std.testing.expectEqual(!held, atomic_mutex.tryLock());
        if (!held) atomic_mutex.unlock();
        var io_mutex: std.Io.Mutex = .init;
        if (held) io_mutex.lockUncancelable(std.testing.io);
        defer if (held) io_mutex.unlock(std.testing.io);
        fixture = .{ .fail_after = if (held) 3 else 2 };
        try std.testing.expectError(error.DeadlineExceeded, lockIo(std.testing.io, &io_mutex, &control));
        try std.testing.expectEqual(!held, io_mutex.tryLock());
        if (!held) io_mutex.unlock(std.testing.io);
    }
}
