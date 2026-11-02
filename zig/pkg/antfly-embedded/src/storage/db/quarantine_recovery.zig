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
const builtin = @import("builtin");
const runtime_mod = @import("../background_runtime.zig");

/// Local index-load recovery supervision, independent of serving coordination.
pub const Owner = struct {
    pub const Port = struct {
        ptr: *anyopaque,
        runtime: *runtime_mod.BackendRuntime,
        has_failures: *const fn (*anyopaque) bool,
        retry: *const fn (*anyopaque) anyerror!usize,
    };
    port: Port = undefined,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    registration: ?runtime_mod.MaintenanceScheduler.Handle = null,
    stopping: std.atomic.Value(bool) = .init(false),
    stop_started: bool = false,
    joined: std.Io.Event = .unset,
    stop_progress: std.Io.Event = .unset,
    stop_generation: std.atomic.Value(u64) = .init(0),
    stop_waiters: std.atomic.Value(u32) = .init(0),
    finished: std.atomic.Value(bool) = .init(false),
    start_address_for_test: usize = 0,

    fn lock(self: *Owner) void {
        while (!self.lifecycle_mutex.tryLock()) @import("antfly_platform").time.yieldNow();
    }

    pub fn start(self: *Owner, port: Port) void {
        self.startInner(port, false);
    }

    fn startInner(self: *Owner, port: Port, allow_test_background: bool) void {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        if (self.stopping.load(.acquire)) return;
        if (builtin.is_test and !allow_test_background) {
            if (self.start_address_for_test == 0) self.start_address_for_test = @intFromPtr(port.ptr);
            return;
        }
        if (self.registration) |*task| {
            if (!self.finished.load(.acquire)) return;
            // A completed registration still owns its slot until joined. Reap
            // it before admitting a new failure cohort.
            task.await(port.runtime.io().?);
            self.registration = null;
        }
        if (!port.has_failures(port.ptr)) return;
        const scheduler = port.runtime.maintenanceScheduler() catch |err| {
            std.log.warn("quarantine retry scheduler unavailable: {}", .{err});
            return;
        };
        self.port = port;
        self.finished.store(false, .release);
        self.registration = scheduler.register(self, step) catch |err| {
            std.log.warn("quarantine retry worker spawn failed: {}", .{err});
            return;
        };
    }

    fn shutdown(self: *Owner) @import("maintenance_stop.zig").Borrowed {
        return .{ .mutex = &self.lifecycle_mutex, .registration = &self.registration, .stopping = &self.stopping, .finisher = &self.stop_started, .joined = &self.joined, .changed = &self.stop_progress, .generation = &self.stop_generation, .waiters = &self.stop_waiters };
    }
    pub fn stop(self: *Owner, runtime: *runtime_mod.BackendRuntime) void {
        self.shutdown().stop(runtime.io(), false);
    }
    pub fn stopWithAdmission(self: *Owner, runtime: *runtime_mod.BackendRuntime, original: @import("../../common/read_admission.zig").Port) !void {
        try self.shutdown().stopWithAdmission(runtime.io(), original);
    }
    fn step(self: *Owner) ?u64 {
        const delay_ms: u64 = 10 * 1000;
        if (self.stopping.load(.acquire)) {
            self.finished.store(true, .release);
            return null;
        }
        const remaining = self.port.retry(self.port.ptr) catch |err| {
            std.log.warn("quarantine retry pass failed: {}", .{err});
            return delay_ms;
        };
        if (remaining == 0) {
            self.lock();
            // A new load failure may have appeared after retry's empty result
            // while start observed this registration as active. Retain it until
            // that cohort is serviced; otherwise start can join the finished
            // registration and admit the next one.
            const finished = self.stopping.load(.acquire) or !self.port.has_failures(self.port.ptr);
            if (finished) self.finished.store(true, .release);
            self.lifecycle_mutex.unlock();
            return if (finished) null else delay_ms;
        }
        return delay_ms;
    }
};

test "quarantine recovery reaps a finished cohort before restarting and honors close" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const Fixture = struct {
        calls: std.atomic.Value(usize) = .init(0),
        pending: std.atomic.Value(bool) = .init(false),
        fn hasFailures(ptr: *anyopaque) bool {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).pending.load(.acquire);
        }
        fn retry(ptr: *anyopaque) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = self.calls.fetchAdd(1, .acq_rel);
            self.pending.store(false, .release);
            return 0;
        }
    };
    var fixture: Fixture = .{};
    var owner: Owner = .{};
    defer owner.stop(runtime.ptr());
    const port: Owner.Port = .{ .ptr = &fixture, .runtime = runtime.ptr(), .has_failures = Fixture.hasFailures, .retry = Fixture.retry };
    for (1..3) |expected| {
        fixture.pending.store(true, .release);
        owner.startInner(port, true);
        const deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
        while (!owner.finished.load(.acquire) and @import("antfly_platform").time.monotonicNs() < deadline) {
            runtime.ptr().io().?.sleep(.fromMilliseconds(1), .awake) catch {};
        }
        try std.testing.expect(owner.finished.load(.acquire));
        try std.testing.expectEqual(expected, fixture.calls.load(.acquire));
    }
    owner.stop(runtime.ptr());
    owner.startInner(port, true);
    try std.testing.expect(owner.registration == null);
    try std.testing.expectEqual(@as(usize, 2), fixture.calls.load(.acquire));
}

test "quarantine recovery final handshake retains a newly failed cohort" {
    const Fixture = struct {
        pending: bool = false,
        calls: usize = 0,
        fn hasFailures(ptr: *anyopaque) bool {
            return (@as(*@This(), @ptrCast(@alignCast(ptr)))).pending;
        }
        fn retry(ptr: *anyopaque) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            // Simulate a new failure after retry has found its cohort empty.
            self.pending = self.calls == 1;
            return 0;
        }
    };
    var fixture: Fixture = .{};
    var owner: Owner = .{ .port = .{ .ptr = &fixture, .runtime = undefined, .has_failures = Fixture.hasFailures, .retry = Fixture.retry } };
    try std.testing.expectEqual(@as(?u64, 10000), owner.step());
    try std.testing.expect(!owner.finished.load(.acquire));
    try std.testing.expect(owner.step() == null);
    try std.testing.expect(owner.finished.load(.acquire));
    try std.testing.expectEqual(@as(usize, 2), fixture.calls);
}

test "maintenance runtime quarantine recovery concurrent shutdown retains physical callback" {
    if (@import("builtin").single_threaded or @import("builtin").os.tag == .freestanding) return error.SkipZigTest;
    var runtime = try runtime_mod.BackendRuntimeHandle.init(std.testing.allocator, .{});
    defer runtime.deinit();
    var threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threads.deinit();
    const io = threads.io();
    var owner: Owner = .{};
    defer owner.stop(runtime.ptr());
    const F = struct {
        owner: *Owner,
        runtime: *runtime_mod.BackendRuntime,
        io: std.Io,
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        first_done: std.Io.Event = .unset,
        second_entered: std.Io.Event = .unset,
        second_done: std.Io.Event = .unset,
        unlocked: std.atomic.Value(bool) = .init(false),
        fn run(ptr: *anyopaque) !usize {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            // A physical join must not hold the callback's lifecycle mutex.
            self.owner.lock();
            self.owner.lifecycle_mutex.unlock();
            self.unlocked.store(true, .release);
            return 0;
        }
        fn hasFailures(_: *anyopaque) bool {
            return true;
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
    var f: F = .{ .owner = &owner, .runtime = runtime.ptr(), .io = io };
    owner.startInner(.{ .ptr = &f, .runtime = runtime.ptr(), .has_failures = F.hasFailures, .retry = F.run }, true);
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
    try std.testing.expectEqual(@as(usize, 1), (try runtime.ptr().maintenanceScheduler()).snapshot().active);
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
    try std.testing.expectEqual(@as(usize, 0), (try runtime.ptr().maintenanceScheduler()).snapshot().active);
}
