// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Supervised, close-safe warming. Durable progress belongs to the repository;
//! this owner only schedules bounded turns and supplies cancellation.
const std = @import("std");
const runtime = @import("../background_runtime.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
pub const Owner = struct {
    port: Port = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    failures: u32 = 0,
    pub const Port = struct { ptr: *anyopaque, runtime: *runtime.BackendRuntime, turn: *const fn (*anyopaque, Cancellation) anyerror!bool };
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    pub fn start(self: *Owner, port: Port) void {
        if (@import("builtin").is_test) return;
        self.lock();
        defer self.mutex.unlock();
        if (self.registration != null or self.stopping.load(.acquire)) return;
        const scheduler = port.runtime.maintenanceScheduler() catch return;
        self.port = port;
        self.registration = scheduler.registerClass(.maintenance, self, step) catch null;
    }
    pub fn stop(self: *Owner, backend: *runtime.BackendRuntime) void {
        self.lock();
        self.stopping.store(true, .release);
        var registration = self.registration;
        self.registration = null;
        self.mutex.unlock();
        if (registration) |*handle| handle.cancel(backend.io().?);
    }
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const Owner = @ptrCast(@alignCast(raw));
        return self.stopping.load(.acquire);
    }
    fn step(self: *Owner) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        const complete = self.port.turn(self.port.ptr, .{ .ptr = self, .is_cancelled_fn = cancelled }) catch |err| {
            if (self.stopping.load(.acquire)) return null;
            self.failures +|= 1;
            if (self.failures == 1 or self.failures % 16 == 0) std.log.warn("native checkpoint warming deferred: {}", .{err});
            return @min(@as(u64, 60000), @as(u64, 1000) << @intCast(@min(self.failures, 6)));
        };
        self.failures = 0;
        return if (complete) 60000 else 1000;
    }
};

test "external lake native warming bounds retry turns and honors close cancellation" {
    const Fixture = struct {
        calls: usize = 0,
        complete: bool = false,
        fail: bool = false,
        fn turn(raw: *anyopaque, cancellation: Cancellation) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try cancellation.check();
            self.calls += 1;
            if (self.fail) return error.ProviderUnavailable;
            return self.complete;
        }
    };
    var fixture: Fixture = .{};
    var owner: Owner = .{ .port = .{ .ptr = &fixture, .runtime = undefined, .turn = Fixture.turn } };
    try std.testing.expectEqual(@as(?u64, 1000), owner.step());
    fixture.complete = true;
    try std.testing.expectEqual(@as(?u64, 60000), owner.step());
    fixture.fail = true;
    try std.testing.expectEqual(@as(?u64, 2000), owner.step());
    owner.stopping.store(true, .release);
    try std.testing.expectEqual(@as(?u64, null), owner.step());
    try std.testing.expectEqual(@as(usize, 3), fixture.calls);
}
