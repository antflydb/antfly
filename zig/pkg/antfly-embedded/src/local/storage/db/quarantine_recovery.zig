// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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

    pub fn stop(self: *Owner, runtime: *runtime_mod.BackendRuntime) void {
        self.lock();
        self.stopping.store(true, .release);
        var task = self.registration;
        self.registration = null;
        self.lifecycle_mutex.unlock();
        // The last step rechecks its failure cohort under the lifecycle lock.
        // Join outside that lock so close cannot deadlock its final handshake.
        if (task) |*handle| handle.await(runtime.io().?);
        self.lock();
        self.lifecycle_mutex.unlock();
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
