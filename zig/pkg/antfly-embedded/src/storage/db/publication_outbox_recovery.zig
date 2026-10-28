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
    /// A changed generation with remaining runnable work permits queueing the
    /// next bounded chunk. Unchanged/unknown progress retains delayed retry.
    pub const WorkProgress = struct { generation: u64 = 0, runnable: bool = false };
    pub const Progress = struct {
        generation: u64 = 0,
        runnable: bool = false,
        provider: WorkProgress = .{},

        pub fn advancedSince(self: @This(), before: @This()) bool {
            // Readiness belongs to the source whose generation advanced. A
            // completed capture cannot borrow an unchanged provider's readiness.
            return (self.runnable and self.generation != before.generation) or
                (self.provider.runnable and self.provider.generation != before.provider.generation);
        }
    };
    pub const Port = struct {
        ptr: *anyopaque,
        eligible: *const fn (*anyopaque) bool,
        closing: *const fn (*anyopaque) bool,
        pending: *const fn (*anyopaque) bool,
        may_disarm: *const fn (*anyopaque) bool,
        now: *const fn (*anyopaque) u64,
        drain: *const fn (*anyopaque) anyerror!void,
        progress: ?*const fn (*anyopaque) Progress = null,
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
            const before = if (port.progress) |probe| probe(port.ptr) else Progress{};
            port.drain(port.ptr) catch |err| {
                if (port.closing(port.ptr)) {
                    self.state.store(0, .release);
                    return;
                }
                if (err == error.CommitCompletionPending) {
                    const after = if (port.progress) |probe| probe(port.ptr) else Progress{};
                    if (after.advancedSince(before)) {
                        // Yield to the bounded queue after making progress.
                        // Resubmission never invokes drain recursively; queue
                        // refusal takes the existing admission backoff path.
                        self.next_attempt_ns.store(0, .release);
                        self.state.store(0, .release);
                        self.schedule(port);
                    } else {
                        // Ack/fence waits and unchanged or contended probes
                        // must not turn into a ready-job polling loop.
                        self.next_attempt_ns.store(port.now(port.ptr) +| 250 * std.time.ns_per_ms, .release);
                        self.state.store(0, .release);
                        port.arm(port.ptr);
                    }
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

test "publication recovery step pending acknowledgement keeps short retry cadence without failure backoff" {
    const F = struct {
        now_ns: u64 = 1,
        submissions: usize = 0,
        failures: usize = 0,
        arms: usize = 0,
        fn cast(ptr: *anyopaque) *@This() {
            return @ptrCast(@alignCast(ptr));
        }
        fn yes(_: *anyopaque) bool {
            return true;
        }
        fn no(_: *anyopaque) bool {
            return false;
        }
        fn now(ptr: *anyopaque) u64 {
            return cast(ptr).now_ns;
        }
        fn drain(_: *anyopaque) !void {
            return error.CommitCompletionPending;
        }
        fn submit(ptr: *anyopaque) !void {
            cast(ptr).submissions += 1;
        }
        fn arm(ptr: *anyopaque) void {
            cast(ptr).arms += 1;
        }
        fn disarm(_: *anyopaque) void {}
        fn delay(_: *anyopaque, _: u32) u64 {
            return 30 * std.time.ns_per_s;
        }
        fn report(ptr: *anyopaque, _: Driver.Failure, _: anyerror, _: u32, _: u64) void {
            cast(ptr).failures += 1;
        }
    };
    var fixture: F = .{};
    var driver: Driver = .{};
    const port: Driver.Port = .{ .ptr = &fixture, .eligible = F.yes, .closing = F.no, .pending = F.yes, .may_disarm = F.no, .now = F.now, .drain = F.drain, .submit = F.submit, .arm = F.arm, .disarm = F.disarm, .retry_delay = F.delay, .report = F.report };
    for (0..3) |_| {
        driver.schedule(port);
        const submissions = fixture.submissions;
        driver.run(port);
        try std.testing.expectEqual(@as(u32, 0), driver.failure_streak);
        try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
        try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
        driver.schedule(port);
        try std.testing.expectEqual(submissions, fixture.submissions);
        fixture.now_ns = driver.next_attempt_ns.load(.acquire);
    }
    try std.testing.expectEqual(@as(usize, 3), fixture.submissions);
    try std.testing.expectEqual(@as(usize, 0), fixture.failures);
    try std.testing.expect(fixture.arms >= 3);
}

const RunnableRetryTest = struct {
    generation: u64 = 0,
    runnable: bool = false,
    advance: bool = true,
    remaining_work: bool = true,
    provider: Driver.WorkProgress = .{},
    advance_provider: bool = false,
    provider_remaining_work: bool = true,
    close_during_drain: bool = false,
    closed: bool = false,
    refuse_submit: bool = false,
    now_ns: u64 = 1,
    submissions: usize = 0,
    drains: usize = 0,
    arms: usize = 0,
    admission_reports: usize = 0,
    delivery_reports: usize = 0,
    fn cast(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn yes(_: *anyopaque) bool {
        return true;
    }
    fn no(_: *anyopaque) bool {
        return false;
    }
    fn closing(ptr: *anyopaque) bool {
        return cast(ptr).closed;
    }
    fn now(ptr: *anyopaque) u64 {
        return cast(ptr).now_ns;
    }
    fn progress(ptr: *anyopaque) Driver.Progress {
        const self = cast(ptr);
        return .{ .generation = self.generation, .runnable = self.runnable, .provider = self.provider };
    }
    fn drain(ptr: *anyopaque) !void {
        const self = cast(ptr);
        self.drains += 1;
        if (self.advance) {
            self.generation +%= 1;
            self.runnable = self.remaining_work;
        }
        if (self.advance_provider) {
            self.provider.generation +%= 1;
            self.provider.runnable = self.provider_remaining_work;
        }
        if (self.close_during_drain) self.closed = true;
        return error.CommitCompletionPending;
    }
    fn submit(ptr: *anyopaque) !void {
        const self = cast(ptr);
        if (self.refuse_submit) return error.TestRunnableQueueFull;
        self.submissions += 1;
    }
    fn arm(ptr: *anyopaque) void {
        cast(ptr).arms += 1;
    }
    fn disarm(_: *anyopaque) void {}
    fn delay(_: *anyopaque, _: u32) u64 {
        return 30 * std.time.ns_per_s;
    }
    fn report(ptr: *anyopaque, failure: Driver.Failure, _: anyerror, _: u32, _: u64) void {
        const self = cast(ptr);
        switch (failure) {
            .admission => self.admission_reports += 1,
            .delivery => self.delivery_reports += 1,
        }
    }
    fn port(self: *@This()) Driver.Port {
        return .{ .ptr = self, .eligible = yes, .closing = closing, .pending = yes, .may_disarm = no, .now = now, .drain = drain, .progress = progress, .submit = submit, .arm = arm, .disarm = disarm, .retry_delay = delay, .report = report };
    }
};

test "publication outbox recovery queues progressing chunks without sleeping and retains waits for unchanged progress" {
    var fixture: RunnableRetryTest = .{};
    var driver: Driver = .{};
    const port = fixture.port();
    driver.schedule(port);
    for (0..8) |index| {
        const submissions = fixture.submissions;
        driver.run(port);
        try std.testing.expectEqual(index + 1, fixture.drains);
        try std.testing.expectEqual(submissions + 1, fixture.submissions);
        try std.testing.expectEqual(@as(u8, 1), driver.state.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), driver.next_attempt_ns.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), fixture.arms);
        // Another wakeup cannot enqueue a second flight or recurse into work.
        driver.schedule(port);
        try std.testing.expectEqual(submissions + 1, fixture.submissions);
    }
    fixture.advance = false;
    const submissions = fixture.submissions;
    driver.run(port);
    try std.testing.expectEqual(submissions, fixture.submissions);
    try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
    try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
    driver.schedule(port);
    try std.testing.expectEqual(submissions, fixture.submissions);
    fixture.now_ns = driver.next_attempt_ns.load(.acquire);
    fixture.runnable = false; // Acknowledgement/fence waiting remains delayed.
    driver.schedule(port);
    driver.run(port);
    try std.testing.expectEqual(submissions + 1, fixture.submissions);
    try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
    // Finishing a CPU chunk before an external wait changes the generation,
    // but must still back off when the remaining work is not runnable.
    fixture.now_ns = driver.next_attempt_ns.load(.acquire);
    fixture.advance = true;
    fixture.remaining_work = false;
    const before = fixture.generation;
    driver.schedule(port);
    driver.run(port);
    try std.testing.expect(fixture.generation != before);
    try std.testing.expectEqual(submissions + 2, fixture.submissions);
    try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
    try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.admission_reports + fixture.delivery_reports);
    try std.testing.expectEqual(@as(u32, 0), driver.failure_streak);
}

test "publication outbox recovery runnable admission refusal and close retain bounded retry barriers" {
    var fixture: RunnableRetryTest = .{};
    var driver: Driver = .{};
    const port = fixture.port();
    driver.schedule(port);
    fixture.refuse_submit = true;
    driver.run(port);
    try std.testing.expectEqual(@as(usize, 1), fixture.drains);
    try std.testing.expectEqual(@as(usize, 1), fixture.submissions);
    try std.testing.expectEqual(@as(usize, 1), fixture.admission_reports);
    try std.testing.expectEqual(@as(usize, 0), fixture.delivery_reports);
    try std.testing.expectEqual(@as(u32, 0), driver.failure_streak);
    try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
    try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
    fixture.now_ns = driver.next_attempt_ns.load(.acquire);
    fixture.refuse_submit = false;
    fixture.close_during_drain = true;
    driver.schedule(port);
    driver.run(port);
    try std.testing.expectEqual(@as(usize, 2), fixture.submissions);
    try std.testing.expectEqual(@as(usize, 2), fixture.drains);
    try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), fixture.admission_reports);
    try std.testing.expectEqual(@as(usize, 0), fixture.delivery_reports);
}

test "publication outbox recovery tracks provider progress independently without borrowing stale readiness" {
    var fixture: RunnableRetryTest = .{ .advance = false, .advance_provider = true };
    var driver: Driver = .{};
    const port = fixture.port();
    driver.schedule(port);
    for (0..8) |index| {
        driver.run(port);
        try std.testing.expectEqual(@as(u64, 0), fixture.generation);
        try std.testing.expectEqual(@as(u64, index + 1), fixture.provider.generation);
        try std.testing.expectEqual(index + 2, fixture.submissions);
        try std.testing.expectEqual(@as(u8, 1), driver.state.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), driver.next_attempt_ns.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), fixture.arms);
    }
    const submissions = fixture.submissions;
    // Capture finishes work while the provider still advertises old readiness.
    // Only the capture token changes, and it has no runnable work remaining.
    fixture.advance_provider = false;
    fixture.advance = true;
    fixture.remaining_work = false;
    driver.run(port);
    try std.testing.expectEqual(submissions, fixture.submissions);
    try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
    try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
    // A finished provider now waits for acknowledgement, despite having moved
    // its token. Capture's old readiness cannot make this provider runnable.
    fixture.now_ns = driver.next_attempt_ns.load(.acquire);
    fixture.advance = false;
    fixture.runnable = true;
    fixture.advance_provider = true;
    fixture.provider_remaining_work = false;
    driver.schedule(port);
    driver.run(port);
    try std.testing.expectEqual(submissions + 1, fixture.submissions);
    try std.testing.expectEqual(@as(u8, 0), driver.state.load(.acquire));
    try std.testing.expectEqual(fixture.now_ns + 250 * std.time.ns_per_ms, driver.next_attempt_ns.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), fixture.admission_reports + fixture.delivery_reports);
}
