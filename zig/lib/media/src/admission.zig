// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Nonblocking shared admission. Share one Pool across request-local readers and
//! preparers. Tokens are move-only and release only after resource destruction.
const std = @import("std");
pub const Resources = struct {
    host_bytes: u64 = 0,
    device_bytes: u64 = 0,
    commands: u64 = 0,
};
pub const Pool = struct {
    limits: Resources,
    used: Resources = .{},
    high_water: Resources = .{},
    locked: std.atomic.Value(bool) = .init(false),
    fn lock(self: *Pool) void {
        while (self.locked.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.atomic.spinLoopHint();
    }
    fn unlock(self: *Pool) void {
        self.locked.store(false, .release);
    }
    /// Atomic across all dimensions; denial mutates no counters. The caller can
    /// retry through its scheduler, retaining its original cancellation deadline.
    pub fn acquire(self: *Pool, requested: Resources) !Token {
        self.lock();
        defer self.unlock();
        inline for (@typeInfo(Resources).@"struct".field_names) |name| {
            if (@field(requested, name) > @field(self.limits, name) -| @field(self.used, name)) return error.SharedAdmissionExceeded;
        }
        inline for (@typeInfo(Resources).@"struct".field_names) |name| {
            @field(self.used, name) += @field(requested, name);
            @field(self.high_water, name) = @max(@field(self.high_water, name), @field(self.used, name));
        }
        return .{ .pool = self, .resources = requested };
    }
    pub fn snapshot(self: *Pool) Resources {
        self.lock();
        defer self.unlock();
        return self.used;
    }
};
pub const Token = struct {
    pool: ?*Pool = null,
    resources: Resources = .{},
    pub fn deinit(self: *Token) void {
        if (self.pool) |pool| {
            pool.lock();
            inline for (@typeInfo(Resources).@"struct".field_names) |name| {
                std.debug.assert(@field(pool.used, name) >= @field(self.resources, name));
                @field(pool.used, name) -= @field(self.resources, name);
            }
            pool.unlock();
        }
        self.* = .{};
    }
};
test "shared admission is atomic and retains outputs until their owner releases" {
    var pool = Pool{ .limits = .{ .host_bytes = 100, .device_bytes = 80, .commands = 2 } };
    var first = try pool.acquire(.{ .host_bytes = 70, .device_bytes = 50, .commands = 1 });
    defer first.deinit();
    try std.testing.expectError(error.SharedAdmissionExceeded, pool.acquire(.{ .host_bytes = 20, .device_bytes = 40 }));
    try std.testing.expectEqual(Resources{ .host_bytes = 70, .device_bytes = 50, .commands = 1 }, pool.snapshot());
    var second = try pool.acquire(.{ .host_bytes = 30, .device_bytes = 30, .commands = 1 });
    second.deinit();
    second.deinit();
    first.deinit();
    try std.testing.expectEqual(Resources{}, pool.snapshot());
    try std.testing.expectEqual(pool.limits, pool.high_water);
}

test "shared admission serializes competing native worker reservations" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .wasi) return error.SkipZigTest;
    const Worker = struct {
        fn run(pool: *Pool, successes: *std.atomic.Value(usize)) void {
            for (0..1000) |_| {
                var token = pool.acquire(.{ .host_bytes = 10, .commands = 1 }) catch {
                    std.atomic.spinLoopHint();
                    continue;
                };
                const used = pool.snapshot();
                std.debug.assert(used.host_bytes <= 20 and used.commands <= 2);
                _ = successes.fetchAdd(1, .monotonic);
                token.deinit();
            }
        }
    };
    var pool = Pool{ .limits = .{ .host_bytes = 20, .commands = 2 } };
    var successes = std.atomic.Value(usize).init(0);
    var workers: [4]std.Thread = undefined;
    var count: usize = 0;
    defer for (workers[0..count]) |worker| worker.join();
    for (&workers) |*worker| {
        worker.* = try std.Thread.spawn(.{}, Worker.run, .{ &pool, &successes });
        count += 1;
    }
    for (workers[0..count]) |worker| worker.join();
    count = 0;
    try std.testing.expect(successes.load(.monotonic) > 0);
    try std.testing.expectEqual(Resources{}, pool.snapshot());
}
