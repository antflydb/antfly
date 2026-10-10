// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! Supervised, close-safe warming. Durable progress belongs to the repository;
//! this owner only schedules bounded turns and supplies cancellation.
const std = @import("std");
const runtime = @import("../background_runtime.zig");
const Cancellation = @import("antfly_cancellation").CancellationToken;
/// One original callback budget, borrowed through the physical turn. Provider
/// cancellation is boolean; the outer fence restores the exact lifetime cause.
pub const Pass = struct {
    pub const budget_ns = 10 * std.time.ns_per_s;
    parent: Cancellation,
    deadline_ns: u64,
    now_ns: *const fn () u64 = @import("antfly_platform").time.monotonicNs,
    pub fn check(self: *const Pass) !void {
        try self.parent.check();
        if (self.now_ns() >= self.deadline_ns) return error.DeadlineExceeded;
    }
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const Pass = @ptrCast(@alignCast(raw));
        self.check() catch return true;
        return false;
    }
    pub fn token(self: *const Pass) Cancellation {
        return .{ .ptr = self, .is_cancelled_fn = cancelled };
    }
    fn run(self: *const Pass, ptr: *anyopaque, turn: *const fn (*anyopaque, *const Pass) anyerror!bool) !bool {
        try self.check();
        const complete = turn(ptr, self) catch |err| {
            try self.check();
            return err;
        };
        try self.check();
        return complete;
    }
};

pub const Owner = struct {
    port: Port = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    failures: u32 = 0,
    pub const Port = struct { ptr: *anyopaque, runtime: *runtime.BackendRuntime, turn: *const fn (*anyopaque, *const Pass) anyerror!bool };
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    pub fn start(self: *Owner, port: Port) void {
        self.startInner(port, false);
    }
    fn startInner(self: *Owner, port: Port, allow_test_background: bool) void {
        if (@import("builtin").is_test and !allow_test_background) return;
        self.lock();
        defer self.mutex.unlock();
        if (self.registration != null or self.stopping.load(.acquire)) return;
        const scheduler = port.runtime.maintenanceScheduler() catch return;
        self.port = port;
        self.registration = scheduler.registerClass(.maintenance, self, step) catch null;
    }
    pub fn stop(self: *Owner, backend: *runtime.BackendRuntime) void {
        const io = backend.io();
        self.lock();
        if (self.stop_started) {
            self.mutex.unlock();
            // A detached registration still borrows its DB until physical join.
            if (io) |lane| self.joined.waitUncancelable(lane) else std.debug.assert(self.joined.isSet());
            return;
        }
        self.stop_started = true;
        self.stopping.store(true, .release);
        var registration = self.registration;
        self.registration = null;
        if (registration == null) {
            if (io) |lane| self.joined.set(lane) else @atomicStore(std.Io.Event, &self.joined, .is_set, .release);
            self.mutex.unlock();
            return;
        }
        self.mutex.unlock();
        registration.?.cancel(io orelse @panic("native warming registration outlived runtime I/O"));
        self.joined.set(io.?);
    }
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const Owner = @ptrCast(@alignCast(raw));
        return self.stopping.load(.acquire);
    }
    fn step(self: *Owner) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        const pass: Pass = .{ .parent = .{ .ptr = self, .is_cancelled_fn = cancelled }, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| Pass.budget_ns };
        const complete = pass.run(self.port.ptr, self.port.turn) catch |err| {
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
        fn turn(raw: *anyopaque, pass: *const Pass) !bool {
            const cancellation = pass.token();
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

test "maintenance runtime native warming concurrent shutdown retains physical callback" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var backend = try @import("../background_runtime.zig").BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer backend.deinit();
    var threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threads.deinit();
    const io = threads.io();
    var owner: Owner = .{};
    defer owner.stop(backend.ptr());
    const F = struct {
        owner: *Owner,
        runtime: *@import("../background_runtime.zig").BackendRuntime,
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        first_done: std.Io.Event = .unset,
        second_entered: std.Io.Event = .unset,
        second_done: std.Io.Event = .unset,
        unlocked: std.atomic.Value(bool) = .init(false),
        canceled: std.atomic.Value(bool) = .init(false),
        fn turn(ptr: *anyopaque, pass: *const Pass) !bool {
            const cancellation = pass.token();
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            // A physical join must not hold the callback's lifecycle mutex.
            self.owner.lock();
            self.owner.mutex.unlock();
            self.unlocked.store(true, .release);
            cancellation.check() catch |err| {
                self.canceled.store(err == error.Canceled, .release);
            };
            return false;
        }

        fn first(self: *@This()) void {
            self.owner.stop(self.runtime);
            self.first_done.set(self.io);
        }
        fn second(self: *@This()) void {
            self.second_entered.set(self.io);
            self.owner.stop(self.runtime);
            self.second_done.set(self.io);
        }
    };
    var f: F = .{ .owner = &owner, .runtime = backend.ptr(), .io = io };
    owner.startInner(.{ .ptr = &f, .runtime = backend.ptr(), .turn = F.turn }, true);
    defer f.release.set(io);
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } };
    try f.entered.waitTimeout(io, timeout);
    var first: ?std.Io.Future(void) = try io.concurrent(F.first, .{&f});
    defer if (first) |*task| {
        f.release.set(io);
        task.await(io);
    };
    const deadline = @import("antfly_platform").time.monotonicNs() + 2 * std.time.ns_per_s;
    while (!owner.stopping.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline)
        try io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expect(owner.stopping.load(.acquire));
    var second: ?std.Io.Future(void) = try io.concurrent(F.second, .{&f});
    defer if (second) |*task| {
        f.release.set(io);
        task.await(io);
    };
    try f.second_entered.waitTimeout(io, timeout);
    try std.testing.expectError(error.Timeout, f.second_done.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(100), .clock = .awake } }));
    try std.testing.expect(!f.first_done.isSet());
    try std.testing.expect(!owner.joined.isSet());
    try std.testing.expectEqual(@as(usize, 1), (try backend.ptr().maintenanceScheduler()).snapshot().active);
    f.release.set(io);
    try f.first_done.waitTimeout(io, timeout);
    try f.second_done.waitTimeout(io, timeout);
    first.?.await(io);
    first = null;
    second.?.await(io);
    second = null;
    try std.testing.expect(owner.joined.isSet());
    try std.testing.expect(f.unlocked.load(.acquire));
    try std.testing.expect(f.canceled.load(.acquire));
    try std.testing.expect(owner.registration == null);
    try std.testing.expectEqual(@as(usize, 0), (try backend.ptr().maintenanceScheduler()).snapshot().active);
}

test "external lake native warming retains one original pass deadline through provider cancellation" {
    const F = struct {
        var now: u64 = 0;
        calls: usize = 0,
        publish: usize = 0,
        expire: bool = false,
        swallow: bool = false,
        fn clock() u64 {
            return now;
        }
        fn turn(raw: *anyopaque, pass: *const Pass) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try pass.check();
            now = 9;
            try pass.token().check();
            if (self.expire) now = 10;
            if (self.swallow) return true;
            // A provider sees boolean cancellation, but this cannot replace
            // the pass's original deadline cause or authorize publication.
            try pass.token().check();
            try pass.check();
            self.publish += 1;
            return true;
        }
    };
    var canceled = std.atomic.Value(bool).init(false);
    for ([_]bool{ false, true }) |swallow| {
        F.now = 0;
        var f: F = .{ .expire = true, .swallow = swallow };
        const pass: Pass = .{ .parent = Cancellation.fromAtomic(&canceled), .deadline_ns = 10, .now_ns = F.clock };
        try std.testing.expectError(error.DeadlineExceeded, pass.run(&f, F.turn));
        try std.testing.expectEqual(@as(usize, 1), f.calls);
        try std.testing.expectEqual(@as(usize, 0), f.publish);
        try std.testing.expectEqual(@as(u64, 10), pass.deadline_ns);
    }
    F.now = 0;
    var f: F = .{};
    const pass: Pass = .{ .parent = Cancellation.fromAtomic(&canceled), .deadline_ns = 10, .now_ns = F.clock };
    try std.testing.expect(try pass.run(&f, F.turn));
    try std.testing.expectEqual(@as(usize, 1), f.publish);
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, pass.run(&f, F.turn));
    try std.testing.expectEqual(@as(usize, 1), f.calls);
    canceled.store(false, .release);
    F.now = 10;
    try std.testing.expectError(error.DeadlineExceeded, pass.run(&f, F.turn));
    try std.testing.expectEqual(@as(usize, 1), f.calls);
}
