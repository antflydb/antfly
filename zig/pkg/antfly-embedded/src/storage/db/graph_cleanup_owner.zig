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

const std = @import("std");
const runtime = @import("../background_runtime.zig");
/// One original pre-decision lifetime for a physical graph cleanup pass.
/// Accepted storage work still owns mandatory completion and retirement.
pub const Pass = struct {
    pub const budget_ns = 10 * std.time.ns_per_s;
    stopping: *const std.atomic.Value(bool),
    deadline_ns: u64,
    now_ns: *const fn () u64 = @import("antfly_platform").time.monotonicNs,
    pub fn check(self: *const Pass) !void {
        if (self.stopping.load(.acquire)) return error.Canceled;
        if (self.now_ns() >= self.deadline_ns) return error.DeadlineExceeded;
    }
    pub fn lifetime(self: *const Pass) @import("../../search/work_authority.zig").Authority {
        return .{ .context = self, .check_fn = checkLifetime };
    }
    fn checkLifetime(raw: *const anyopaque) !void {
        const self: *const Pass = @ptrCast(@alignCast(raw));
        try self.check();
    }
    fn run(self: *const Pass, ptr: *anyopaque, turn: *const fn (*anyopaque, *const Pass) anyerror!bool) !bool {
        try self.check();
        const progressed = turn(ptr, self) catch |err| {
            try self.check();
            return err;
        };
        try self.check();
        return progressed;
    }
};
/// Local bounded cleanup lifecycle. Storage owns write eligibility and each fenced page.
pub const Owner = struct {
    pub const Port = struct { ptr: *anyopaque, runtime: *runtime.BackendRuntime, run: *const fn (*anyopaque, *const Pass) anyerror!bool };
    port: Port = undefined,
    mutex: std.atomic.Mutex = .unlocked,
    registration: ?runtime.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    stop_progress: std.Io.Event = .unset,
    fn lock(self: *Owner) void {
        while (!self.mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }
    pub fn start(self: *Owner, port: Port) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.stopping.load(.acquire) or self.registration != null) return;
        const scheduler = port.runtime.maintenanceScheduler() catch |err| {
            std.log.warn("graph cleanup scheduler unavailable: {}", .{err});
            return;
        };
        self.port = port;
        self.registration = scheduler.register(self, scheduledStep) catch |err| {
            std.log.warn("graph cleanup worker unavailable: {}", .{err});
            return;
        };
    }
    fn shutdown(self: *Owner) @import("maintenance_stop.zig").Borrowed {
        return .{ .mutex = &self.mutex, .registration = &self.registration, .stopping = &self.stopping, .finisher = &self.stop_started, .joined = &self.joined, .changed = &self.stop_progress };
    }
    pub fn stop(self: *Owner, io: ?std.Io) void {
        self.shutdown().stop(io, false);
    }
    pub fn stopWithAdmission(self: *Owner, io: ?std.Io, original: @import("../../common/read_admission.zig").Port) !void {
        try self.shutdown().stopWithAdmission(io, original);
    }
    fn scheduledStep(self: *Owner) ?u64 {
        return self.step(self.port);
    }
    pub fn step(self: *Owner, port: Port) ?u64 {
        if (self.stopping.load(.acquire)) return null;
        const pass: Pass = .{ .stopping = &self.stopping, .deadline_ns = @import("antfly_platform").time.monotonicNs() +| Pass.budget_ns };
        const progressed = pass.run(port.ptr, port.run) catch |err| {
            if (self.stopping.load(.acquire)) return null;
            std.log.warn("graph endpoint cleanup deferred: {}", .{err});
            return 250;
        };
        return if (progressed) 1 else 100;
    }
};

test "graph cleanup owner preserves bounded cadence and permanent shutdown" {
    const F = struct {
        progress: bool = false,
        fail: bool = false,
        calls: usize = 0,
        fn run(ptr: *anyopaque, _: *const Pass) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            if (self.fail) return error.WriterLocked;
            return self.progress;
        }
    };
    var f: F = .{};
    var owner: Owner = .{};
    const port: Owner.Port = .{ .ptr = &f, .runtime = undefined, .run = F.run };
    try std.testing.expectEqual(@as(?u64, 100), owner.step(port));
    f.progress = true;
    try std.testing.expectEqual(@as(?u64, 1), owner.step(port));
    f.fail = true;
    try std.testing.expectEqual(@as(?u64, 250), owner.step(port));
    owner.stop(null);
    owner.start(port);
    try std.testing.expect(owner.registration == null);
    try std.testing.expect(owner.step(port) == null);
    try std.testing.expectEqual(@as(usize, 3), f.calls);
}

test "maintenance runtime graph cleanup concurrent shutdown retains physical callback" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var backend = try @import("../background_runtime.zig").BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer backend.deinit();
    var threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threads.deinit();
    const io = threads.io();
    var owner: Owner = .{};
    defer owner.stop(backend.ptr().io());
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
        fn run(ptr: *anyopaque, _: *const Pass) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            // A physical join must not hold the callback's lifecycle mutex.
            self.owner.lock();
            self.owner.mutex.unlock();
            self.unlocked.store(true, .release);
            return false;
        }

        fn first(self: *@This()) void {
            self.owner.stop(self.runtime.io());
            self.first_done.set(self.io);
        }
        fn second(self: *@This()) void {
            self.second_entered.set(self.io);
            self.owner.stop(self.runtime.io());
            self.second_done.set(self.io);
        }
    };
    var f: F = .{ .owner = &owner, .runtime = backend.ptr(), .io = io };
    owner.start(.{ .ptr = &f, .runtime = backend.ptr(), .run = F.run });
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
    try std.testing.expect(owner.registration == null);
    try std.testing.expectEqual(@as(usize, 0), (try backend.ptr().maintenanceScheduler()).snapshot().active);
}

test "graph cleanup owner original pass fences apply waits and restores exact expiry" {
    const Apply = @import("apply_rw_lock.zig").ApplyRwLock;
    const Mode = enum { expiry_wait, stop_wait, provider_expiry, swallowed_expiry, success };
    const Clock = struct {
        var value: u64 = 0;
        var calls: usize = 0;
        var mode: Mode = .success;
        var stopping: *std.atomic.Value(bool) = undefined;
        fn now() u64 {
            calls += 1;
            if (calls >= 4) switch (mode) {
                .expiry_wait => value = 10,
                .stop_wait => stopping.store(true, .release),
                else => {},
            };
            return value;
        }
    };
    const Fixture = struct {
        apply: *Apply,
        mode: Mode,
        calls: usize = 0,
        publications: usize = 0,
        fn turn(raw: *anyopaque, pass: *const Pass) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.mode == .provider_expiry or self.mode == .swallowed_expiry) {
                Clock.value = 10;
                if (self.mode == .provider_expiry) return error.Canceled;
                return true;
            }
            try self.apply.lockExclusiveControlledIo(std.testing.io, .{ .lifetime = pass.lifetime() });
            defer self.apply.unlockExclusive();
            try pass.check();
            self.publications += 1;
            return true;
        }
    };
    for (std.enums.values(Mode)) |mode| {
        var stopping: std.atomic.Value(bool) = .init(false);
        Clock.value = 0;
        Clock.calls = 0;
        Clock.mode = mode;
        Clock.stopping = &stopping;
        const pass: Pass = .{ .stopping = &stopping, .deadline_ns = 10, .now_ns = Clock.now };
        var apply: Apply = .{};
        var held = mode == .expiry_wait or mode == .stop_wait;
        if (held) apply.lockShared();
        defer if (held) apply.unlockShared();
        var fixture: Fixture = .{ .apply = &apply, .mode = mode };
        if (mode == .success) {
            try std.testing.expect(try pass.run(&fixture, Fixture.turn));
            try std.testing.expectEqual(@as(usize, 1), fixture.publications);
        } else {
            try std.testing.expectError(if (mode == .stop_wait) error.Canceled else error.DeadlineExceeded, pass.run(&fixture, Fixture.turn));
            try std.testing.expectEqual(@as(usize, 0), fixture.publications);
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expectEqual(@as(u64, 10), pass.deadline_ns);
        try std.testing.expectEqual(@as(u64, 0), apply.exclusive_waiters.load(.acquire));
        if (held) {
            // Rejected cleanup does not revoke the original physical reader.
            try std.testing.expect(!apply.tryLockExclusive());
            apply.unlockShared();
            held = false;
        }
        try std.testing.expect(apply.tryLockExclusive());
        apply.unlockExclusive();
        // Rejected entry never invokes the physical callback.
        stopping.store(true, .release);
        try std.testing.expectError(error.Canceled, pass.run(&fixture, Fixture.turn));
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    }
}
