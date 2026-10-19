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

//! Allocation-free FIFO admission. Queue entries belong to their callers and
//! are retired under the same lock used by grant/close. Waiting occupies an
//! existing runtime task: callers must bound that lane independently and keep
//! completion/control execution independent from queued foreground work.
const std = @import("std");

pub const Waiting = struct {
    max_queued_requests: u32 = 0,
    max_queued_bytes: u64 = 0,
    max_wait_ms: u32 = 0,

    pub fn validate(self: Waiting) !void {
        if (self.max_wait_ms > 60_000) return error.InvalidConfig;
        if (self.max_wait_ms == 0) {
            if (self.max_queued_requests != 0 or self.max_queued_bytes != 0) return error.InvalidConfig;
        } else if (self.max_queued_requests == 0 or self.max_queued_bytes == 0) return error.InvalidConfig;
    }
};

pub const Options = struct {
    io: std.Io,
    deadline_ns: ?u64 = null,
    clock_io: ?std.Io = null,
    native_now_ns: ?*const fn () u64 = null,
    cancellation: ?struct {
        ptr: *const anyopaque,
        requested: *const fn (*const anyopaque) bool,
    } = null,
    /// Owned input bytes plus adapter metadata. Execution allocations remain
    /// with existing transport, plan-cache and storage resource owners.
    retained_bytes: u64 = 0,
    /// Adapters lacking independent completion headroom retain fail-fast behavior.
    allow_wait: bool = true,

    pub fn now(self: Options) u64 {
        if (self.native_now_ns) |now_fn| return now_fn();
        return @intCast(@max(0, std.Io.Clock.now(.awake, self.clock_io orelse self.io).nanoseconds));
    }

    pub fn check(self: Options) !void {
        try self.io.checkCancel();
        if (self.cancellation) |token| if (token.requested(token.ptr)) return error.Canceled;
        if (self.deadline_ns) |deadline| if (self.now() >= deadline) return error.DeadlineExceeded;
    }
};

/// Charge the decoded parameter cells and their retained strings/containers.
/// Plan-cache allocations and transport arenas have separate resource owners.
pub fn parameterBytes(values: []const std.json.Value) u64 {
    var bytes: u64 = @as(u64, values.len) *| @sizeOf(std.json.Value);
    for (values) |value| bytes +|= switch (value) {
        .string, .number_string => |text| text.len,
        .array => |array| @as(u64, array.capacity - array.items.len) *| @sizeOf(std.json.Value) +| parameterBytes(array.items),
        .object => |object| objectBytes(object),
        else => 0,
    };
    return bytes;
}

fn objectBytes(object: std.json.ObjectMap) u64 {
    var bytes: u64 = @as(u64, object.count()) *| (@sizeOf(std.json.Value) + @sizeOf([]const u8));
    var it = object.iterator();
    while (it.next()) |entry| {
        bytes +|= entry.key_ptr.len;
        bytes +|= parameterBytes((&[_]std.json.Value{entry.value_ptr.*})[0..]);
    }
    return bytes;
}

pub const wait_bucket_ms = [_]u64{ 1, 5, 10, 25, 50, 100, 250, 1000, 5000, 60000 };
pub const wait_bucket_seconds = [_][]const u8{ "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "1", "5", "60", "+Inf" };

pub const Controller = struct {
    capacity: usize,
    waiting: Waiting = .{},
    mutex: std.atomic.Mutex = .unlocked,
    active: usize = 0,
    peak: usize = 0,
    granted: u64 = 0,
    rejected: u64 = 0,
    queued: usize = 0,
    queued_bytes: u64 = 0,
    waited: u64 = 0,
    wait_ns: u64 = 0,
    wait_completed: u64 = 0,
    lease_completed: u64 = 0,
    lease_hold_ns: u64 = 0,
    lease_hold_ns_max: u64 = 0,
    wait_buckets: [wait_bucket_ms.len + 1]u64 = @splat(0),
    expired: u64 = 0,
    cancelled: u64 = 0,
    closed: bool = false,
    suspended: bool = false,
    policy_version: u64 = 1,
    configured_capacity: usize,
    configured_waiting: Waiting = .{},
    policy_retired: u64 = 0,
    retiring: usize = 0,
    head: ?*Entry = null,
    tail: ?*Entry = null,

    /// Nonblocking queue ownership also supports continuation-based adapters.
    /// A caller must retire an entry before destroying it, even after close.
    pub const Entry = struct {
        next: ?*Entry = null,
        previous: ?*Entry = null,
        io: std.Io,
        ready: std.Io.Event = .unset,
        bytes: u64,
        was_queued: bool = false,
        max_wait_ms: u32 = 0,
        failure: ?anyerror = null,
        state: enum { fresh, queued, granted, closed, retired } = .fresh,
    };

    pub fn init(capacity: usize) Controller {
        return .{ .capacity = capacity, .configured_capacity = capacity };
    }

    pub fn initConfigured(capacity: usize, waiting: Waiting) !Controller {
        try waiting.validate();
        return .{ .capacity = capacity, .configured_capacity = capacity, .waiting = waiting, .configured_waiting = waiting };
    }

    fn lock(self: *Controller) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn grant(self: *Controller) void {
        self.active += 1;
        self.peak = @max(self.peak, self.active);
        self.granted +|= 1;
    }

    pub fn tryAcquire(self: *Controller) bool {
        self.lock();
        defer self.mutex.unlock();
        // New callers cannot bypass existing waiters.
        if (!self.closed and !self.suspended and self.head == null and (self.capacity == 0 or self.active < self.capacity)) {
            self.grant();
            return true;
        }
        self.rejected +|= 1;
        return false;
    }

    pub const Lease = struct {
        admission: *Controller,
        active: bool = true,
        clock: ?struct { io: std.Io, native_now_ns: ?*const fn () u64 } = null,
        started_ns: u64 = 0,

        fn track(self: *Lease, options: Options) void {
            self.clock = .{ .io = options.clock_io orelse options.io, .native_now_ns = options.native_now_ns };
            self.started_ns = options.now();
        }

        pub fn release(self: *Lease) void {
            if (!self.active) return;
            self.active = false;
            if (self.clock) |clock| {
                const now: u64 = if (clock.native_now_ns) |now_fn| now_fn() else @intCast(@max(0, std.Io.Clock.now(.awake, clock.io).nanoseconds));
                const elapsed = now -| self.started_ns;
                self.admission.lock();
                self.admission.lease_completed +|= 1;
                self.admission.lease_hold_ns +|= elapsed;
                self.admission.lease_hold_ns_max = @max(self.admission.lease_hold_ns_max, elapsed);
                self.admission.mutex.unlock();
            }
            self.admission.release();
        }
    };

    pub fn tryAcquireLease(self: *Controller) ?Lease {
        if (!self.tryAcquire()) return null;
        return .{ .admission = self };
    }

    pub fn enqueue(self: *Controller, entry: *Entry) !void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.state == .fresh);
        if (self.closed) return self.reject(error.AdmissionClosed);
        if (self.suspended) return self.reject(error.AdmissionClosed);
        entry.max_wait_ms = self.waiting.max_wait_ms;
        if (self.head == null and (self.capacity == 0 or self.active < self.capacity)) {
            self.grant();
            entry.state = .granted;
            return;
        }
        if (self.waiting.max_wait_ms == 0) return self.reject(error.AdmissionFull);
        if (entry.bytes > self.waiting.max_queued_bytes) return self.reject(error.AdmissionRequestTooLarge);
        if (self.queued >= self.waiting.max_queued_requests) return self.reject(error.AdmissionQueueFull);
        if (entry.bytes > self.waiting.max_queued_bytes - self.queued_bytes) return self.reject(error.AdmissionBytesExhausted);
        entry.state = .queued;
        entry.was_queued = true;
        entry.previous = self.tail;
        if (self.tail) |tail| tail.next = entry else self.head = entry;
        self.tail = entry;
        self.queued += 1;
        self.queued_bytes += entry.bytes;
        self.waited +|= 1;
    }

    fn reject(self: *Controller, err: anyerror) anyerror {
        self.rejected +|= 1;
        return err;
    }

    fn unlink(self: *Controller, entry: *Entry) void {
        std.debug.assert(entry.state == .queued);
        if (entry.previous) |previous| previous.next = entry.next else self.head = entry.next;
        if (entry.next) |next| next.previous = entry.previous else self.tail = entry.previous;
        entry.previous = null;
        entry.next = null;
        self.queued -= 1;
        self.queued_bytes -= entry.bytes;
    }

    fn dispatch(self: *Controller) void {
        while (!self.closed and !self.suspended and (self.capacity == 0 or self.active < self.capacity)) {
            const entry = self.head orelse break;
            self.unlink(entry);
            self.grant();
            entry.state = .granted;
            // Notification is not a callback. The caller must rejoin mutex
            // before retirement, so this cannot touch freed entry storage.
            entry.ready.set(entry.io);
        }
    }

    pub fn release(self: *Controller) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.active > 0);
        self.active -= 1;
        self.dispatch();
    }

    /// Resolves grant/cancel races. Returns the lease on success; otherwise
    /// removes queue membership or returns a concurrently granted permit.
    pub fn retire(self: *Controller, entry: *Entry, failure: ?anyerror) !Lease {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(entry.state != .fresh and entry.state != .retired);
        const state = entry.state;
        if (state == .queued) self.unlink(entry);
        if (state == .closed) self.retiring -= 1;
        entry.state = .retired;
        if (failure) |err| {
            if (state == .granted) {
                self.active -= 1;
                self.dispatch();
            }
            if (err == error.Canceled) self.cancelled +|= 1 else {
                self.rejected +|= 1;
                if (err == error.DeadlineExceeded or err == error.AdmissionWaitTimeout) self.expired +|= 1;
            }
            return err;
        }
        if (state == .closed) {
            return self.reject(entry.failure orelse error.AdmissionClosed);
        }
        std.debug.assert(state == .granted);
        return .{ .admission = self };
    }

    pub fn acquire(self: *Controller, options: Options) !Lease {
        try options.check();
        if (!options.allow_wait) {
            var lease = self.tryAcquireLease() orelse return error.AdmissionFull;
            options.check() catch |err| {
                lease.release();
                return err;
            };
            lease.track(options);
            return lease;
        }
        const started = options.now();
        var entry: Entry = .{ .io = options.io, .bytes = options.retained_bytes };
        try self.enqueue(&entry);
        const until = @min(options.deadline_ns orelse std.math.maxInt(u64), started +| @as(u64, entry.max_wait_ms) * std.time.ns_per_ms);
        const did_wait = entry.was_queued;
        while (true) {
            const failure: ?anyerror = if (options.check()) |_| (if (did_wait and options.now() >= until) error.AdmissionWaitTimeout else null) else |err| err;
            self.lock();
            const done = entry.state != .queued;
            self.mutex.unlock();
            if (failure != null or done) {
                if (did_wait) self.recordWait(options.now() -| started);
                var lease = try self.retire(&entry, failure);
                lease.track(options);
                return lease;
            }
            // Atomic/external cancellation has no Event wake hook. Bound its
            // observation latency without spinning or extending the deadline.
            const remaining = until -| options.now();
            entry.ready.waitTimeout(options.io, .{ .duration = .{
                .raw = .fromNanoseconds(@intCast(@min(remaining, 5 * std.time.ns_per_ms))),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.recordWait(options.now() -| started);
                    return self.retire(&entry, err);
                },
            };
        }
    }

    fn recordWait(self: *Controller, elapsed: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.wait_ns +|= elapsed;
        self.wait_completed +|= 1;
        for (wait_bucket_ms, 0..) |bound, index| {
            if (elapsed <= bound * std.time.ns_per_ms) self.wait_buckets[index] +|= 1;
        }
        self.wait_buckets[wait_bucket_ms.len] +|= 1;
    }

    pub fn close(self: *Controller) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        self.policy_version +|= 1;
        while (self.head) |entry| {
            self.unlink(entry);
            entry.state = .closed;
            self.retiring += 1;
            entry.ready.set(entry.io);
        }
    }

    /// Live policies may shrink and later restore startup reservations. Growing
    /// transport/worker envelopes requires a restart. Zero remains unlimited.
    /// Already granted leases are never revoked; queue shrink keeps oldest work.
    pub fn update(self: *Controller, expected_version: u64, capacity: usize, waiting: Waiting, suspended: bool) !void {
        try waiting.validate();
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.AdmissionClosed;
        if (expected_version != self.policy_version) return error.PolicyConflict;
        if (self.configured_capacity != 0 and (capacity == 0 or capacity > self.configured_capacity)) return error.InvalidConfig;
        if (waiting.max_queued_requests > self.configured_waiting.max_queued_requests or
            waiting.max_queued_bytes > self.configured_waiting.max_queued_bytes or
            waiting.max_wait_ms > self.configured_waiting.max_wait_ms) return error.InvalidConfig;
        self.capacity = capacity;
        self.waiting = waiting;
        self.suspended = suspended;
        self.policy_version +|= 1;
        while (self.tail) |entry| {
            if (!suspended and waiting.max_wait_ms != 0 and self.queued <= waiting.max_queued_requests and self.queued_bytes <= waiting.max_queued_bytes) break;
            self.unlink(entry);
            entry.state = .closed;
            entry.failure = error.AdmissionClosed;
            self.retiring += 1;
            self.policy_retired +|= 1;
            entry.ready.set(entry.io);
        }
        self.dispatch();
    }

    pub fn drain(self: *Controller, expected_version: u64) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.AdmissionClosed;
        if (expected_version != self.policy_version) return error.PolicyConflict;
        self.closed = true;
        self.policy_version +|= 1;
        while (self.head) |entry| {
            self.unlink(entry);
            entry.state = .closed;
            self.retiring += 1;
            self.policy_retired +|= 1;
            entry.ready.set(entry.io);
        }
    }

    pub const Stats = struct {
        capacity: usize,
        in_flight: usize,
        peak_in_flight: usize,
        rejected_total: u64,
        /// Operation admission grants, including leases without timing clocks.
        /// Grant/cancel races count admission, not application execution.
        /// Null means a projected/legacy snapshot has no authoritative count.
        granted_total: ?u64 = null,
        queued: usize = 0,
        queued_bytes: u64 = 0,
        waited_total: u64 = 0,
        wait_ns_total: u64 = 0,
        wait_completed_total: u64 = 0,
        /// Granted operation ownership, including slow consumers/held portals.
        /// Queue waiting is excluded; this is elapsed ownership, not CPU time.
        lease_completed_total: u64 = 0,
        lease_hold_ns_total: u64 = 0,
        lease_hold_ns_max: u64 = 0,
        wait_buckets: [wait_bucket_ms.len + 1]u64 = @splat(0),
        expired_total: u64 = 0,
        cancelled_total: u64 = 0,
        draining: bool = false,
        waiting: Waiting = .{},
        sql_waiting_enabled: ?bool = null,
        configured_capacity: usize = 0,
        configured_waiting: Waiting = .{},
        policy_version: u64 = 1,
        policy_retired_total: u64 = 0,
        suspended: bool = false,
        quiescent: bool = true,
        retiring_waiters: usize = 0,
    };

    pub fn stats(self: *const Controller) Stats {
        const mutable = @constCast(self);
        mutable.lock();
        defer mutable.mutex.unlock();
        return .{
            .capacity = self.capacity,
            .in_flight = self.active,
            .peak_in_flight = self.peak,
            .rejected_total = self.rejected,
            .granted_total = self.granted,
            .queued = self.queued,
            .queued_bytes = self.queued_bytes,
            .waited_total = self.waited,
            .wait_ns_total = self.wait_ns,
            .wait_completed_total = self.wait_completed,
            .lease_completed_total = self.lease_completed,
            .lease_hold_ns_total = self.lease_hold_ns,
            .lease_hold_ns_max = self.lease_hold_ns_max,
            .wait_buckets = self.wait_buckets,
            .expired_total = self.expired,
            .cancelled_total = self.cancelled,
            .draining = self.closed,
            .waiting = self.waiting,
            .configured_capacity = self.configured_capacity,
            .configured_waiting = self.configured_waiting,
            .policy_version = self.policy_version,
            .policy_retired_total = self.policy_retired,
            .suspended = self.suspended,
            .quiescent = self.active == 0 and self.queued == 0 and self.retiring == 0,
            .retiring_waiters = self.retiring,
        };
    }
};

test "bounded admission contextual leases measure ownership once under both policies" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    inline for (.{ false, true }) |allow_wait| {
        var gate = Controller.init(1);
        Clock.value = 100;
        var lease = try gate.acquire(.{ .io = std.testing.io, .native_now_ns = Clock.now, .allow_wait = allow_wait });
        Clock.value = 250;
        lease.release();
        Clock.value = 500;
        lease.release();
        const stats = gate.stats();
        try std.testing.expectEqual(@as(u64, 1), stats.granted_total.?);
        try std.testing.expectEqual(@as(u64, 1), stats.lease_completed_total);
        try std.testing.expectEqual(@as(u64, 150), stats.lease_hold_ns_total);
        try std.testing.expectEqual(@as(u64, 150), stats.lease_hold_ns_max);
        try std.testing.expectEqual(@as(u64, 0), stats.wait_ns_total);
        try std.testing.expectEqual(@as(usize, 0), stats.in_flight);
    }
}

test "bounded admission FIFO grant cancellation and close transfer one owner" {
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 2, .max_queued_bytes = 20, .max_wait_ms = 100 });
    var active = gate.tryAcquireLease().?;
    var a: Controller.Entry = .{ .io = std.testing.io, .bytes = 12 };
    var b: Controller.Entry = .{ .io = std.testing.io, .bytes = 8 };
    try gate.enqueue(&a);
    try gate.enqueue(&b);
    try std.testing.expect(!gate.tryAcquire());
    try std.testing.expectEqual(@as(u64, 1), gate.stats().granted_total.?);
    active.release();
    try std.testing.expectEqual(@as(u64, 2), gate.stats().granted_total.?);
    try std.testing.expect(a.state == .granted and b.state == .queued);
    try std.testing.expectError(error.Canceled, gate.retire(&a, error.Canceled));
    try std.testing.expect(b.state == .granted);
    var granted = try gate.retire(&b, null);
    granted.release();
    granted.release();
    try std.testing.expectEqual(@as(u64, 3), gate.stats().granted_total.?);
    try std.testing.expectEqual(@as(usize, 0), gate.stats().in_flight);
    try std.testing.expectEqual(@as(u64, 0), gate.stats().queued_bytes);
    active = gate.tryAcquireLease().?;
    a = .{ .io = std.testing.io, .bytes = 12 };
    try gate.enqueue(&a);
    gate.close();
    try std.testing.expectError(error.AdmissionClosed, gate.retire(&a, null));
    active.release();
    try std.testing.expect(!gate.tryAcquire());
    try std.testing.expectEqual(@as(u64, 4), gate.stats().granted_total.?);
}

test "bounded admission rejects count bytes oversized and invalid policies" {
    try std.testing.expectError(error.InvalidConfig, Controller.initConfigured(1, .{ .max_wait_ms = 1 }));
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 2, .max_queued_bytes = 20, .max_wait_ms = 100 });
    var active = gate.tryAcquireLease().?;
    defer active.release();
    var a: Controller.Entry = .{ .io = std.testing.io, .bytes = 12 };
    var b: Controller.Entry = .{ .io = std.testing.io, .bytes = 9 };
    try gate.enqueue(&a);
    try std.testing.expectError(error.AdmissionBytesExhausted, gate.enqueue(&b));
    b.bytes = 21;
    try std.testing.expectError(error.AdmissionRequestTooLarge, gate.enqueue(&b));
    b.bytes = 8;
    try gate.enqueue(&b);
    var c: Controller.Entry = .{ .io = std.testing.io, .bytes = 0 };
    try std.testing.expectError(error.AdmissionQueueFull, gate.enqueue(&c));
    try std.testing.expectError(error.Canceled, gate.retire(&b, error.Canceled));
    try std.testing.expectError(error.DeadlineExceeded, gate.retire(&a, error.DeadlineExceeded));
    try std.testing.expectEqual(@as(usize, 0), gate.stats().queued);
}

test "bounded admission wait ceiling and expired fast path never execute" {
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 1, .max_queued_bytes = 20, .max_wait_ms = 1 });
    var active = gate.tryAcquireLease().?;
    defer active.release();
    try std.testing.expectError(error.AdmissionWaitTimeout, gate.acquire(.{ .io = std.testing.io, .retained_bytes = 1 }));
    try std.testing.expectEqual(@as(usize, 0), gate.stats().queued);
    try std.testing.expectEqual(@as(u64, 1), gate.stats().expired_total);
    var free = Controller.init(1);
    try std.testing.expectError(error.DeadlineExceeded, free.acquire(.{ .io = std.testing.io, .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(usize, 0), free.stats().in_flight);
}

test "bounded admission native queue leaves completion capacity and drains a burst" {
    // Native waiting retains one already-dispatched task per waiter. Eight
    // waiters have a finite eight-task envelope; the ninth slot is protected
    // for a completion task. This tests the actual pinned Threaded runtime.
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(9) });
    defer runtime.deinit();
    const io = runtime.io();
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 8, .max_queued_bytes = 80, .max_wait_ms = 5000 });
    var initial = gate.tryAcquireLease().?;
    defer initial.release();
    const Job = struct {
        gate: *Controller,
        io: std.Io,
        err: ?anyerror = null,
        entered: bool = false,

        fn run(self: *@This()) void {
            var lease = self.gate.acquire(.{ .io = self.io, .retained_bytes = 10 }) catch |err| {
                self.err = err;
                return;
            };
            defer lease.release();
            self.entered = true;
        }
    };
    var jobs: [8]Job = undefined;
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (&jobs) |*job| {
        job.* = .{ .gate = &gate, .io = io };
        try group.concurrent(io, Job.run, .{job});
    }
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (gate.stats().queued != jobs.len) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    var completion = try io.concurrent(struct {
        fn run(lease: *Controller.Lease) void {
            lease.release();
        }
    }.run, .{&initial});
    completion.await(io);
    try group.await(io);
    for (jobs) |job| {
        try std.testing.expect(job.err == null);
        try std.testing.expect(job.entered);
    }
    try std.testing.expectEqual(@as(usize, 1), gate.stats().peak_in_flight);
    try std.testing.expectEqual(@as(usize, 0), gate.stats().queued);
    try std.testing.expectEqual(@as(u64, 0), gate.stats().queued_bytes);
    try std.testing.expectEqual(@as(usize, 0), gate.stats().in_flight);
}

test "bounded admission queued external cancellation retires without execution" {
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 1, .max_queued_bytes = 20, .max_wait_ms = 100 });
    var active = gate.tryAcquireLease().?;
    defer active.release();
    const Probe = struct {
        calls: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            return self.calls > 1;
        }
    };
    var probe: Probe = .{};
    try std.testing.expectError(error.Canceled, gate.acquire(.{
        .io = std.testing.io,
        .retained_bytes = 1,
        .cancellation = .{ .ptr = &probe, .requested = Probe.requested },
    }));
    try std.testing.expectEqual(@as(usize, 0), gate.stats().queued);
    try std.testing.expectEqual(@as(usize, 1), gate.stats().in_flight);
    try std.testing.expectEqual(@as(u64, 1), gate.stats().cancelled_total);
}

test "bounded admission unsupported executor remains fail fast without enqueue" {
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 8, .max_queued_bytes = 100, .max_wait_ms = 100 });
    var active = gate.tryAcquireLease().?;
    defer active.release();
    try std.testing.expectError(error.AdmissionFull, gate.acquire(.{ .io = std.testing.io, .allow_wait = false }));
    try std.testing.expectEqual(@as(usize, 0), gate.stats().queued);
    try std.testing.expectEqual(@as(u64, 0), gate.stats().waited_total);
}

test "bounded admission live shrink preserves grants and retires newest waiters" {
    const waiting: Waiting = .{ .max_queued_requests = 3, .max_queued_bytes = 30, .max_wait_ms = 100 };
    var gate = try Controller.initConfigured(2, waiting);
    var first = gate.tryAcquireLease().?;
    var second = gate.tryAcquireLease().?;
    var entries: [3]Controller.Entry = undefined;
    for (&entries) |*entry| {
        entry.* = .{ .io = std.testing.io, .bytes = 10 };
        try gate.enqueue(entry);
    }
    try gate.update(1, 1, .{ .max_queued_requests = 1, .max_queued_bytes = 10, .max_wait_ms = 50 }, false);
    try std.testing.expectEqual(@as(usize, 2), gate.stats().in_flight);
    try std.testing.expectEqual(@as(usize, 2), gate.stats().retiring_waiters);
    try std.testing.expectError(error.AdmissionClosed, gate.retire(&entries[2], null));
    try std.testing.expectError(error.Canceled, gate.retire(&entries[1], error.Canceled));
    first.release();
    try std.testing.expect(entries[0].state == .queued);
    second.release();
    var surviving = try gate.retire(&entries[0], null);
    surviving.release();
    try std.testing.expect(gate.stats().quiescent);
    try std.testing.expectError(error.PolicyConflict, gate.update(1, 2, waiting, false));
    try std.testing.expectError(error.InvalidConfig, gate.update(2, 0, waiting, false));
    try gate.update(2, 2, waiting, false);
    try std.testing.expectEqual(@as(u64, 3), gate.stats().policy_version);
}

test "bounded admission suspension and terminal drain reconcile caller ownership" {
    const waiting: Waiting = .{ .max_queued_requests = 1, .max_queued_bytes = 10, .max_wait_ms = 100 };
    var gate = try Controller.initConfigured(1, waiting);
    var lease = gate.tryAcquireLease().?;
    var entry: Controller.Entry = .{ .io = std.testing.io, .bytes = 10 };
    try gate.enqueue(&entry);
    try gate.update(1, 1, waiting, true);
    lease.release();
    try std.testing.expect(!gate.stats().quiescent);
    try std.testing.expectError(error.AdmissionClosed, gate.retire(&entry, null));
    try std.testing.expect(gate.stats().quiescent);
    try std.testing.expect(!gate.tryAcquire());
    try gate.update(2, 1, waiting, false);
    lease = gate.tryAcquireLease().?;
    entry = .{ .io = std.testing.io, .bytes = 10 };
    try gate.enqueue(&entry);
    try gate.drain(3);
    try std.testing.expect(gate.stats().draining);
    try std.testing.expectError(error.AdmissionClosed, gate.update(4, 1, waiting, false));
    try std.testing.expectError(error.AdmissionClosed, gate.retire(&entry, null));
    lease.release();
    try std.testing.expect(gate.stats().quiescent);
    try std.testing.expectEqual(@as(u64, 2), gate.stats().policy_retired_total);
}
