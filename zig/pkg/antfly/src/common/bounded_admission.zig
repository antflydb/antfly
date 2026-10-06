// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

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
    rejected: u64 = 0,
    queued: usize = 0,
    queued_bytes: u64 = 0,
    waited: u64 = 0,
    wait_ns: u64 = 0,
    wait_completed: u64 = 0,
    wait_buckets: [wait_bucket_ms.len + 1]u64 = @splat(0),
    expired: u64 = 0,
    cancelled: u64 = 0,
    closed: bool = false,
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
        state: enum { fresh, queued, granted, closed, retired } = .fresh,
    };

    pub fn init(capacity: usize) Controller {
        return .{ .capacity = capacity };
    }

    pub fn initConfigured(capacity: usize, waiting: Waiting) !Controller {
        try waiting.validate();
        return .{ .capacity = capacity, .waiting = waiting };
    }

    fn lock(self: *Controller) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn grant(self: *Controller) void {
        self.active += 1;
        self.peak = @max(self.peak, self.active);
    }

    pub fn tryAcquire(self: *Controller) bool {
        self.lock();
        defer self.mutex.unlock();
        // New callers cannot bypass existing waiters.
        if (!self.closed and self.head == null and (self.capacity == 0 or self.active < self.capacity)) {
            self.grant();
            return true;
        }
        self.rejected +|= 1;
        return false;
    }

    pub const Lease = struct {
        admission: *Controller,
        active: bool = true,

        pub fn release(self: *Lease) void {
            if (!self.active) return;
            self.active = false;
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
        while (!self.closed and (self.capacity == 0 or self.active < self.capacity)) {
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
        if (state == .closed) return self.reject(error.AdmissionClosed);
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
            return lease;
        }
        const started = options.now();
        const until = @min(options.deadline_ns orelse std.math.maxInt(u64), started +| @as(u64, self.waiting.max_wait_ms) * std.time.ns_per_ms);
        var entry: Entry = .{ .io = options.io, .bytes = options.retained_bytes };
        try self.enqueue(&entry);
        const did_wait = entry.was_queued;
        while (true) {
            const failure: ?anyerror = if (options.check()) |_| (if (did_wait and options.now() >= until) error.AdmissionWaitTimeout else null) else |err| err;
            self.lock();
            const done = entry.state != .queued;
            self.mutex.unlock();
            if (failure != null or done) {
                if (did_wait) self.recordWait(options.now() -| started);
                return self.retire(&entry, failure);
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
        while (self.head) |entry| {
            self.unlink(entry);
            entry.state = .closed;
            entry.ready.set(entry.io);
        }
    }

    pub const Stats = struct {
        capacity: usize,
        in_flight: usize,
        peak_in_flight: usize,
        rejected_total: u64,
        queued: usize = 0,
        queued_bytes: u64 = 0,
        waited_total: u64 = 0,
        wait_ns_total: u64 = 0,
        wait_completed_total: u64 = 0,
        wait_buckets: [wait_bucket_ms.len + 1]u64 = @splat(0),
        expired_total: u64 = 0,
        cancelled_total: u64 = 0,
        draining: bool = false,
        waiting: Waiting = .{},
        sql_waiting_enabled: ?bool = null,
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
            .queued = self.queued,
            .queued_bytes = self.queued_bytes,
            .waited_total = self.waited,
            .wait_ns_total = self.wait_ns,
            .wait_completed_total = self.wait_completed,
            .wait_buckets = self.wait_buckets,
            .expired_total = self.expired,
            .cancelled_total = self.cancelled,
            .draining = self.closed,
            .waiting = self.waiting,
        };
    }
};

test "bounded admission FIFO grant cancellation and close transfer one owner" {
    var gate = try Controller.initConfigured(1, .{ .max_queued_requests = 2, .max_queued_bytes = 20, .max_wait_ms = 100 });
    var active = gate.tryAcquireLease().?;
    var a: Controller.Entry = .{ .io = std.testing.io, .bytes = 12 };
    var b: Controller.Entry = .{ .io = std.testing.io, .bytes = 8 };
    try gate.enqueue(&a);
    try gate.enqueue(&b);
    try std.testing.expect(!gate.tryAcquire());
    active.release();
    try std.testing.expect(a.state == .granted and b.state == .queued);
    try std.testing.expectError(error.Canceled, gate.retire(&a, error.Canceled));
    try std.testing.expect(b.state == .granted);
    var granted = try gate.retire(&b, null);
    granted.release();
    granted.release();
    try std.testing.expectEqual(@as(usize, 0), gate.stats().in_flight);
    try std.testing.expectEqual(@as(u64, 0), gate.stats().queued_bytes);
    active = gate.tryAcquireLease().?;
    a = .{ .io = std.testing.io, .bytes = 12 };
    try gate.enqueue(&a);
    gate.close();
    try std.testing.expectError(error.AdmissionClosed, gate.retire(&a, null));
    active.release();
    try std.testing.expect(!gate.tryAcquire());
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
