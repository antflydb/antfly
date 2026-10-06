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

//! Close-safe retry driver for durable publication. Storage owns the fenced
//! drain and startup barrier; this driver owns admission, backoff and probes.
const std = @import("std");
pub const Driver = struct {
    state: std.atomic.Value(u8) = .init(0),
    failure_streak: u32 = 0,
    next_attempt_ns: @import("antfly_platform").atomic.Value(u64) = .init(0),
    pub const Failure = enum { admission, delivery };
    pub const Port = struct {
        ptr: *anyopaque,
        eligible: *const fn (*anyopaque) bool,
        closing: *const fn (*anyopaque) bool,
        pending: *const fn (*anyopaque) bool,
        may_disarm: *const fn (*anyopaque) bool,
        now: *const fn (*anyopaque) u64,
        drain: *const fn (*anyopaque) anyerror!void,
        submit: *const fn (*anyopaque) anyerror!void,
        arm: *const fn (*anyopaque) void,
        disarm: *const fn (*anyopaque) void,
        retry_delay: *const fn (*anyopaque, u32) u64,
        report: *const fn (*anyopaque, Failure, anyerror, u32, u64) void,
    };
    pub fn schedule(self: *Driver, port: Port) void {
        if (!port.eligible(port.ptr)) return;
        if (!port.pending(port.ptr)) {
            if (port.may_disarm(port.ptr)) port.disarm(port.ptr);
            return;
        }
        if (port.now(port.ptr) < self.next_attempt_ns.load(.acquire)) {
            port.arm(port.ptr);
            return;
        }
        if (self.state.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) return;
        // A previous flight can publish backoff between the optimistic clock
        // check and admission. Recheck while owning the flight before enqueueing.
        if (port.now(port.ptr) < self.next_attempt_ns.load(.acquire)) {
            self.state.store(0, .release);
            port.arm(port.ptr);
            return;
        }
        port.submit(port.ptr) catch |err| {
            const delay = 250 * std.time.ns_per_ms;
            const failures = self.failure_streak;
            self.next_attempt_ns.store(port.now(port.ptr) +| delay, .release);
            self.state.store(0, .release);
            port.arm(port.ptr);
            port.report(port.ptr, .admission, err, failures, delay);
        };
    }
    pub fn run(self: *Driver, port: Port) void {
        // A successful delivery retains the maybe-bit until a fenced empty
        // scan. A second pass proves emptiness without scanning on each clear.
        for (0..2) |_| {
            port.drain(port.ptr) catch |err| {
                if (port.closing(port.ptr)) {
                    self.state.store(0, .release);
                    return;
                }
                self.failure_streak +|= 1;
                const failures = self.failure_streak;
                const delay = port.retry_delay(port.ptr, failures - 1);
                self.next_attempt_ns.store(port.now(port.ptr) +| delay, .release);
                self.state.store(0, .release);
                port.arm(port.ptr);
                port.report(port.ptr, .delivery, err, failures, delay);
                return;
            };
            if (!port.pending(port.ptr)) break;
        }
        self.failure_streak = 0;
        self.next_attempt_ns.store(0, .release);
        self.state.store(0, .release);
        if (port.pending(port.ptr)) self.schedule(port) else if (port.may_disarm(port.ptr)) port.disarm(port.ptr);
    }
};

test "publication outbox recovery retries admission and delivery until fenced emptiness" {
    const Fixture = struct {
        driver: Driver = .{},
        now_ns: u64 = 1,
        has_pending: bool = true,
        closed: bool = false,
        refuse: bool = true,
        fail_delivery: bool = true,
        submissions: usize = 0,
        drains: usize = 0,
        arms: usize = 0,
        disarms: usize = 0,
        fn self(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn eligible(ptr: *anyopaque) bool {
            return !self(ptr).closed;
        }
        fn closing(ptr: *anyopaque) bool {
            return self(ptr).closed;
        }
        fn pending(ptr: *anyopaque) bool {
            return self(ptr).has_pending;
        }
        fn mayDisarm(_: *anyopaque) bool {
            return true;
        }
        fn now(ptr: *anyopaque) u64 {
            return self(ptr).now_ns;
        }
        fn drain(ptr: *anyopaque) !void {
            const f = self(ptr);
            if (f.fail_delivery) return error.PublisherUnavailable;
            f.drains += 1;
            if (f.drains == 2) f.has_pending = false;
        }
        fn submit(ptr: *anyopaque) !void {
            const f = self(ptr);
            if (f.refuse) return error.QueueFull;
            f.submissions += 1;
        }
        fn arm(ptr: *anyopaque) void {
            self(ptr).arms += 1;
        }
        fn disarm(ptr: *anyopaque) void {
            self(ptr).disarms += 1;
        }
        fn delay(_: *anyopaque, _: u32) u64 {
            return 10;
        }
        fn report(_: *anyopaque, _: Driver.Failure, _: anyerror, _: u32, _: u64) void {}
        fn port(f: *@This()) Driver.Port {
            return .{ .ptr = f, .eligible = eligible, .closing = closing, .pending = pending, .may_disarm = mayDisarm, .now = now, .drain = drain, .submit = submit, .arm = arm, .disarm = disarm, .retry_delay = delay, .report = report };
        }
    };
    var fixture: Fixture = .{};
    fixture.driver.schedule(fixture.port());
    try std.testing.expectEqual(@as(u8, 0), fixture.driver.state.load(.acquire));
    fixture.refuse = false;
    fixture.driver.schedule(fixture.port());
    try std.testing.expectEqual(@as(usize, 0), fixture.submissions);
    fixture.now_ns = fixture.driver.next_attempt_ns.load(.acquire);
    fixture.driver.schedule(fixture.port());
    fixture.driver.schedule(fixture.port());
    try std.testing.expectEqual(@as(usize, 1), fixture.submissions);
    fixture.driver.run(fixture.port());
    try std.testing.expectEqual(@as(u32, 1), fixture.driver.failure_streak);
    fixture.fail_delivery = false;
    fixture.now_ns += 10;
    fixture.driver.schedule(fixture.port());
    fixture.driver.run(fixture.port());
    try std.testing.expectEqual(@as(usize, 2), fixture.drains);
    try std.testing.expectEqual(@as(u32, 0), fixture.driver.failure_streak);
    try std.testing.expectEqual(@as(usize, 1), fixture.disarms);
    fixture.closed = true;
    fixture.has_pending = true;
    fixture.driver.schedule(fixture.port());
    try std.testing.expectEqual(@as(usize, 2), fixture.submissions);
    // Teardown owns the job drain; a failing in-flight job must not rearm it.
    const arms = fixture.arms;
    fixture.fail_delivery = true;
    fixture.driver.state.store(1, .release);
    fixture.driver.run(fixture.port());
    try std.testing.expectEqual(@as(u8, 0), fixture.driver.state.load(.acquire));
    try std.testing.expectEqual(arms, fixture.arms);
    try std.testing.expectEqual(@as(u32, 0), fixture.driver.failure_streak);
}

test "a previous recovery failure must retain its own diagnostic counter after rearming" {
    const F = struct {
        driver: Driver = .{},
        now_ns: u64 = 1,
        pending_work: bool = true,
        refuse_delivery: bool = true,
        reenter: bool = true,
        reported: ?u32 = null,
        fn f(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn yes(_: *anyopaque) bool {
            return true;
        }
        fn no(_: *anyopaque) bool {
            return false;
        }
        fn pending(ptr: *anyopaque) bool {
            return f(ptr).pending_work;
        }
        fn now(ptr: *anyopaque) u64 {
            return f(ptr).now_ns;
        }
        fn drain(ptr: *anyopaque) !void {
            const fixture = f(ptr);
            if (fixture.refuse_delivery) return error.PublisherUnavailable;
            fixture.pending_work = false;
        }
        fn submit(_: *anyopaque) !void {}
        fn arm(ptr: *anyopaque) void {
            const fixture = f(ptr);
            if (!fixture.reenter) return;
            fixture.reenter = false;
            fixture.refuse_delivery = false;
            fixture.now_ns = fixture.driver.next_attempt_ns.load(.acquire);
            // Model a due maintenance probe and queued successor running
            // before the previous worker reaches its logging callback.
            fixture.driver.schedule(fixture.port());
            fixture.driver.run(fixture.port());
        }
        fn disarm(_: *anyopaque) void {}
        fn delay(_: *anyopaque, _: u32) u64 {
            return 10;
        }
        fn report(ptr: *anyopaque, _: Driver.Failure, _: anyerror, failures: u32, _: u64) void {
            f(ptr).reported = failures;
        }
        fn port(fixture: *@This()) Driver.Port {
            return .{ .ptr = fixture, .eligible = yes, .closing = no, .pending = pending, .may_disarm = yes, .now = now, .drain = drain, .submit = submit, .arm = arm, .disarm = disarm, .retry_delay = delay, .report = report };
        }
    };
    var fixture: F = .{};
    fixture.driver.schedule(fixture.port());
    fixture.driver.run(fixture.port());
    try std.testing.expectEqual(@as(?u32, 1), fixture.reported);
}
