// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Fixed execution scheduling over the resource ownership ledger. Callers must
//! already own their request/state and provide a verified minimum working set.
//! Unknown/non-yielding operators remain in the general lane. Lock order is
//! scheduler -> ledger; cancellation callbacks and operators run outside both.
const std = @import("std");
const resources = @import("workload_resources.zig");
const Options = @import("workload_admission.zig").Options;

pub const Policy = struct {
    weights: [resources.lane_count]u16 = @splat(1),
    backfill_scan: u8 = 8,
    max_bypasses: u8 = 8,
    barrier_age_ms: u32 = 25,
    max_wait_ms: u32 = 100,

    pub fn validate(self: Policy) !void {
        if (self.max_wait_ms > 60_000 or self.barrier_age_ms > 60_000) return error.InvalidPolicy;
        for (self.weights) |weight| if (weight == 0) return error.InvalidPolicy;
    }
};

pub const Scheduler = struct {
    ledger: *resources.Ledger,
    policy: Policy,
    mutex: std.atomic.Mutex = .unlocked,
    lanes: [resources.lane_count]Queue = @splat(.{}),
    cursor: usize = 0,
    virtual_service: i128 = 0,
    next_sequence: u64 = 0,
    closed: bool = false,

    const max_debt: i64 = 1_048_576;
    const Queue = struct {
        head: ?*Waiter = null,
        tail: ?*Waiter = null,
        service: i128 = 0,
    };
    const Waiter = struct {
        next: ?*Waiter = null,
        previous: ?*Waiter = null,
        queue: ?resources.QueueLease = null,
        resume_queue: ?resources.ResumeQueueLease = null,
        continuation: ?*resources.RetainedStateLease = null,
        job: ?resources.RunnableLease = null,
        minimum: resources.Bundle,
        estimate: u16,
        lane: resources.Lane,
        sequence: u64,
        started: u64,
        wait_until: u64,
        options: Options,
        ready: std.Io.Event = .unset,
        finished: bool = false,
        failure: ?anyerror = null,
        bypasses: u8 = 0,
        blocked: bool = false,
    };

    pub const Job = struct {
        scheduler: *Scheduler,
        lease: ?resources.RunnableLease,
        lane: resources.Lane,
        estimate: u16,

        /// Measured work uses the same bounded unit as the caller's estimate.
        /// Release only at quiescence; live helpers own their own runnable lease.
        pub fn release(self: *Job, measured_units: u64) void {
            var lease = self.lease orelse return;
            self.lease = null;
            const owner = self.scheduler;
            owner.lock();
            defer owner.mutex.unlock();
            lease.release() catch unreachable;
            owner.account(self.lane, @as(i128, @min(measured_units, max_debt)) - self.estimate);
            owner.schedule();
        }

        /// Verified suspension keeps completion credits and live state owned.
        /// Reserve the maximum minimum completion bundle before first execution;
        /// resuming from these credits cannot deadlock behind new allocations.
        pub fn yieldState(self: *Job, retained_bytes: u64, measured_units: u64) !resources.RetainedStateLease {
            const lease = if (self.lease) |*value| value else return error.LeaseRetired;
            const owner = self.scheduler;
            owner.lock();
            defer owner.mutex.unlock();
            const retained = try lease.exchange(.retained_state, .{ .retained_bytes = retained_bytes });
            self.lease = null;
            owner.account(self.lane, @as(i128, @min(measured_units, max_debt)) - self.estimate);
            owner.schedule();
            return retained;
        }
    };

    pub fn init(ledger: *resources.Ledger, policy: Policy) !Scheduler {
        try policy.validate();
        return .{ .ledger = ledger, .policy = policy };
    }

    fn lock(self: *Scheduler) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn remove(self: *Scheduler, waiter: *Waiter) void {
        const lane = &self.lanes[@intFromEnum(waiter.lane)];
        if (waiter.previous) |prior| prior.next = waiter.next else lane.head = waiter.next;
        if (waiter.next) |next| next.previous = waiter.previous else lane.tail = waiter.previous;
        waiter.next = null;
        waiter.previous = null;
    }

    fn enqueue(self: *Scheduler, waiter: *Waiter) void {
        const lane = &self.lanes[@intFromEnum(waiter.lane)];
        if (lane.head == null) lane.service = @max(lane.service, self.virtual_service);
        // Existing continuations must be able to consume their completion
        // credits and retire. New-start barriers cannot strand those credits.
        if (waiter.continuation != null) {
            var cursor = lane.head;
            while (cursor) |next| : (cursor = next.next) {
                if (next.continuation != null) continue;
                waiter.next = next;
                waiter.previous = next.previous;
                if (next.previous) |prior| prior.next = waiter else lane.head = waiter;
                next.previous = waiter;
                return;
            }
        }
        waiter.previous = lane.tail;
        if (lane.tail) |tail| tail.next = waiter else lane.head = waiter;
        lane.tail = waiter;
    }

    fn account(self: *Scheduler, lane: resources.Lane, units: i128) void {
        const index = @intFromEnum(lane);
        // Fixed-point virtual service implements weighted shares without
        // earning credits from timer polls or blocking an otherwise idle CPU.
        const delta = @divTrunc(units * 65_536, self.policy.weights[index]);
        const bound: i128 = max_debt * 65_536;
        self.lanes[index].service = std.math.clamp(self.lanes[index].service + delta, self.virtual_service - bound, self.virtual_service + bound);
    }

    fn serviceOrder(self: *Scheduler) [resources.lane_count]usize {
        var order: [resources.lane_count]usize = undefined;
        for (&order, 0..) |*index, offset| index.* = (self.cursor + offset) % resources.lane_count;
        // Eight fixed lanes: bounded insertion sort, preserving rotating ties.
        for (1..order.len) |i| {
            var j = i;
            while (j > 0 and self.lanes[order[j]].service < self.lanes[order[j - 1]].service) : (j -= 1)
                std.mem.swap(usize, &order[j], &order[j - 1]);
        }
        return order;
    }

    fn finish(self: *Scheduler, waiter: *Waiter, failure: ?anyerror) void {
        self.remove(waiter);
        if (failure != null) retireWaiting(waiter);
        waiter.failure = failure;
        waiter.finished = true;
        waiter.ready.set(waiter.options.io);
    }

    fn retireWaiting(waiter: *Waiter) void {
        if (waiter.queue) |*queue| queue.release() catch unreachable;
        if (waiter.resume_queue) |*wake| {
            const bytes = (wake.reserved() catch unreachable).retained_bytes;
            waiter.continuation.?.* = wake.exchange(.retained_state, .{ .retained_bytes = bytes }) catch unreachable;
        }
    }

    fn expired(self: *Scheduler, waiter: *Waiter) ?anyerror {
        if (self.closed) return error.AdmissionClosed;
        const now = waiter.options.now();
        if (waiter.options.deadline_ns) |end| if (now >= end) return error.DeadlineExceeded;
        if (now >= waiter.wait_until) return error.AdmissionWaitTimeout;
        return null;
    }

    fn barrier(self: *Scheduler) ?*Waiter {
        var oldest: ?*Waiter = null;
        for (&self.lanes) |*lane| if (lane.head) |head| {
            if (!head.blocked or (head.bypasses < self.policy.max_bypasses and
                head.options.now() -| head.started < @as(u64, self.policy.barrier_age_ms) * std.time.ns_per_ms)) continue;
            if (oldest == null or head.sequence < oldest.?.sequence) oldest = head;
        };
        return oldest;
    }

    fn mayBorrow(self: *Scheduler, waiter: *Waiter, oldest: ?*Waiter) bool {
        const blocked = oldest orelse return true;
        if (blocked.lane == waiter.lane) return true;
        const snapshot = self.ledger.snapshot();
        const current = snapshot.lanes[@intFromEnum(waiter.lane)];
        const floor = snapshot.policy.lanes[@intFromEnum(waiter.lane)].floor;
        // A large-admission barrier reserves no partial bundle. Other lanes
        // keep their floors, but cannot newly borrow resources while it drains.
        inline for (.{ "runnable", "retained_bytes", "io" }) |field| {
            if (@field(waiter.minimum, field) > 0 and
                @field(current, field) +| @field(waiter.minimum, field) > @field(floor, field)) return false;
        }
        return true;
    }

    fn schedule(self: *Scheduler) void {
        var again = true;
        while (again) {
            again = false;
            const oldest = self.barrier();
            for (self.serviceOrder()) |index| {
                const lane = &self.lanes[index];
                const head = lane.head orelse continue;
                var candidate: ?*Waiter = head;
                var scanned: usize = 0;
                while (candidate) |waiter| {
                    const next = waiter.next;
                    if (self.expired(waiter)) |err| {
                        self.finish(waiter, err);
                        again = true;
                        break; // recompute the head/barrier after retirement
                    }
                    if (!self.mayBorrow(waiter, oldest)) break;
                    const result = if (waiter.resume_queue) |*continuation|
                        continuation.exchange(.runnable, waiter.minimum)
                    else
                        waiter.queue.?.exchange(.runnable, waiter.minimum);
                    if (result) |lease| {
                        waiter.job = lease;
                        self.virtual_service = @max(self.virtual_service, lane.service);
                        self.account(waiter.lane, waiter.estimate);
                        self.cursor = (index + 1) % resources.lane_count;
                        if (waiter != head) head.bypasses +|= 1;
                        self.finish(waiter, null);
                        again = true;
                        break;
                    } else |err| switch (err) {
                        error.ResourceTemporarilyUnavailable => {
                            if (waiter == head) waiter.blocked = true;
                        },
                        else => {
                            self.finish(waiter, err);
                            again = true;
                            break;
                        },
                    }
                    if (head.bypasses >= self.policy.max_bypasses or
                        head.options.now() -| head.started >= @as(u64, self.policy.barrier_age_ms) * std.time.ns_per_ms or
                        scanned >= self.policy.backfill_scan) break;
                    scanned += 1;
                    candidate = next;
                }
                if (again) break; // choose the least-served fitting lane again
            }
        }
    }

    fn cancel(self: *Scheduler, waiter: *Waiter) void {
        self.lock();
        defer self.mutex.unlock();
        if (waiter.finished) {
            if (waiter.job) |*job| {
                if (waiter.continuation) |continuation|
                    continuation.* = job.exchange(.retained_state, .{ .retained_bytes = waiter.minimum.retained_bytes }) catch unreachable
                else
                    job.release() catch unreachable;
                self.account(waiter.lane, -@as(i128, waiter.estimate));
            }
        } else {
            self.remove(waiter);
            retireWaiting(waiter);
        }
        self.schedule();
    }

    /// The request and its separately owned retained state outlive this call.
    /// Queue bytes are a reference to that state, not a second memory charge.
    pub fn acquire(self: *Scheduler, request: *const resources.RequestLease, lane: resources.Lane, minimum: resources.Bundle, estimated_units: u16, queue_bytes: u64, options: Options) !Job {
        return self.acquireInner(request, lane, minimum, estimated_units, queue_bytes, options, null);
    }

    pub fn acquireResume(self: *Scheduler, request: *const resources.RequestLease, lane: resources.Lane, continuation: *resources.RetainedStateLease, minimum: resources.Bundle, estimated_units: u16, queue_bytes: u64, options: Options) !Job {
        return self.acquireInner(request, lane, minimum, estimated_units, queue_bytes, options, continuation);
    }

    fn acquireInner(self: *Scheduler, request: *const resources.RequestLease, lane: resources.Lane, minimum: resources.Bundle, estimated_units: u16, queue_bytes: u64, options: Options, continuation: ?*resources.RetainedStateLease) !Job {
        if (estimated_units == 0 or estimated_units > 1024) return error.InvalidEstimate;
        if (continuation) |state| {
            try self.ledger.validateContinuation(request, state);
            if (minimum.retained_bytes < (try state.reserved()).retained_bytes) return error.RetainedStateStillLive;
        }
        try options.check();
        const started = options.now();
        self.lock();
        if (self.closed) {
            self.mutex.unlock();
            return error.AdmissionClosed;
        }
        // A mismatched identity must not schedule a charge under another lane's
        // weight. The ledger remains authoritative for the request's class.
        const request_lane = self.ledger.requestLane(request) catch |err| {
            self.mutex.unlock();
            return err;
        };
        if (request_lane != lane) {
            self.mutex.unlock();
            return error.InvalidLease;
        }
        var empty = true;
        for (self.lanes) |pending| if (pending.head != null) {
            empty = false;
            break;
        };
        if (empty) {
            const immediate = if (continuation) |state| state.exchange(.runnable, minimum) else self.ledger.acquire(.runnable, request, minimum);
            if (immediate) |lease| {
                self.lanes[@intFromEnum(lane)].service = @max(self.lanes[@intFromEnum(lane)].service, self.virtual_service);
                self.account(lane, estimated_units);
                self.mutex.unlock();
                var job: Job = .{ .scheduler = self, .lease = lease, .lane = lane, .estimate = estimated_units };
                errdefer if (continuation) |state| {
                    state.* = job.yieldState(minimum.retained_bytes, 0) catch unreachable;
                } else job.release(0);
                try options.check();
                return job;
            } else |err| {
                if (err != error.ResourceTemporarilyUnavailable) {
                    self.mutex.unlock();
                    return err;
                }
            }
        }
        if (self.policy.max_wait_ms == 0) {
            self.mutex.unlock();
            return error.AdmissionFull;
        }
        // A continuation already reserves its waiter storage and retained byte
        // credits. Reuse that ownership instead of competing with new starts
        // for queue slots, bytes, or metadata needed to finish existing work.
        var queue: ?resources.QueueLease = null;
        var resume_queue: ?resources.ResumeQueueLease = null;
        if (continuation) |state| {
            const held = state.reserved() catch |err| {
                self.mutex.unlock();
                return err;
            };
            resume_queue = state.exchange(.resume_queue, .{ .retained_bytes = held.retained_bytes }) catch |err| {
                self.mutex.unlock();
                return err;
            };
        } else {
            queue = self.ledger.acquire(.queue, request, .{ .queued = 1, .queued_bytes = queue_bytes }) catch |err| {
                self.mutex.unlock();
                return err;
            };
        }
        const sequence = self.next_sequence;
        self.next_sequence +|= 1;
        var waiter: Waiter = .{
            .queue = queue,
            .resume_queue = resume_queue,
            .continuation = continuation,
            .minimum = minimum,
            .estimate = estimated_units,
            .lane = lane,
            .sequence = sequence,
            .started = started,
            .wait_until = started +| @as(u64, self.policy.max_wait_ms) * std.time.ns_per_ms,
            .options = options,
        };
        self.enqueue(&waiter);
        self.schedule();
        self.mutex.unlock();
        while (true) {
            options.check() catch |err| {
                self.cancel(&waiter);
                return err;
            };
            self.lock();
            self.schedule();
            if (waiter.finished) {
                self.mutex.unlock();
                if (waiter.failure) |err| return err;
                var job: Job = .{ .scheduler = self, .lease = waiter.job, .lane = lane, .estimate = estimated_units };
                errdefer if (continuation) |state| {
                    state.* = job.yieldState(minimum.retained_bytes, 0) catch unreachable;
                } else job.release(0);
                try options.check();
                return job;
            }
            const end = @min(waiter.wait_until, options.deadline_ns orelse std.math.maxInt(u64));
            const interval = @min(5 * std.time.ns_per_ms, end -| options.now());
            self.mutex.unlock();
            waiter.ready.waitTimeout(options.io, .{ .duration = .{ .raw = .fromNanoseconds(interval), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.cancel(&waiter);
                    return err;
                },
            };
        }
    }

    pub fn close(self: *Scheduler) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        self.schedule();
    }
};

fn testPolicy() resources.Policy {
    const total: resources.Bundle = .{ .handles = 64, .requests = 16, .queued = 16, .queued_bytes = 100, .runnable = 4, .retained_bytes = 100, .io = 4, .remote_attempts = 4, .recovery_obligations = 4 };
    var lanes: [resources.lane_count]resources.LanePolicy = @splat(.{ .ceiling = total });
    lanes[@intFromEnum(resources.Lane.bounded_read)].floor = .{ .handles = 3, .requests = 1, .runnable = 1, .retained_bytes = 10 };
    lanes[@intFromEnum(resources.Lane.control)].floor = .{ .handles = 3, .requests = 1, .runnable = 1, .retained_bytes = 10 };
    return .{ .total = total, .lanes = lanes };
}

fn testWaiter(ledger: *resources.Ledger, request: *resources.RequestLease, bytes: u64, sequence: u64) !Scheduler.Waiter {
    const options: Options = .{ .io = std.testing.io };
    const now = options.now();
    return .{
        .queue = try ledger.acquire(.queue, request, .{ .queued = 1, .queued_bytes = 1 }),
        .minimum = .{ .runnable = 1, .retained_bytes = bytes },
        .estimate = 1,
        .lane = .general_read,
        .sequence = sequence,
        .started = now,
        .wait_until = now + std.time.ns_per_s,
        .options = options,
    };
}

test "workload admission scheduler backfills only until the oldest large admission barrier" {
    var ledger = try resources.Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var scheduler = try Scheduler.init(&ledger, .{ .max_bypasses = 1, .barrier_age_ms = 60_000 });
    var bulk = try ledger.admit(.general_read, 1);
    defer bulk.release() catch unreachable;
    var blocker = try scheduler.acquire(&bulk, .general_read, .{ .runnable = 1, .retained_bytes = 60 }, 1, 1, .{ .io = std.testing.io });
    defer blocker.release(1);
    var large = try ledger.admit(.general_read, 1);
    defer large.release() catch unreachable;
    var first = try ledger.admit(.general_read, 1);
    defer first.release() catch unreachable;
    var second = try ledger.admit(.general_read, 1);
    defer second.release() catch unreachable;
    var large_waiter = try testWaiter(&ledger, &large, 30, 1);
    var first_waiter = try testWaiter(&ledger, &first, 5, 2);
    var second_waiter = try testWaiter(&ledger, &second, 5, 3);
    scheduler.enqueue(&large_waiter);
    scheduler.enqueue(&first_waiter);
    scheduler.enqueue(&second_waiter);
    scheduler.schedule();
    try std.testing.expect(!large_waiter.finished and first_waiter.finished and !second_waiter.finished);
    try first_waiter.job.?.release();
    scheduler.schedule();
    try std.testing.expect(!second_waiter.finished); // no unbounded small arrivals
    blocker.release(1);
    try std.testing.expect(large_waiter.finished and second_waiter.finished);
    try large_waiter.job.?.release();
    try second_waiter.job.?.release();
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.queued);
}

test "workload admission scheduler preserves continuation state when cancellation wins after resume grant" {
    var ledger = try resources.Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var scheduler = try Scheduler.init(&ledger, .{});
    var request = try ledger.admit(.general_read, 1);
    defer request.release() catch unreachable;
    var job = try scheduler.acquire(&request, .general_read, .{ .runnable = 1, .retained_bytes = 40 }, 1, 1, .{ .io = std.testing.io });
    var continuation = try job.yieldState(40, 1);
    defer continuation.release() catch unreachable;
    var other = try ledger.admit(.general_read, 1);
    defer other.release() catch unreachable;
    var blocker = try scheduler.acquire(&other, .general_read, .{ .runnable = 2 }, 1, 1, .{ .io = std.testing.io });
    defer blocker.release(1);
    const Cancel = struct {
        blocker: *Scheduler.Job,
        calls: usize = 0,
        fn check(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 1) return false;
            self.blocker.release(1);
            return true;
        }
    };
    var cancel = Cancel{ .blocker = &blocker };
    try std.testing.expectError(error.Canceled, scheduler.acquireResume(&request, .general_read, &continuation, .{ .runnable = 1, .retained_bytes = 40 }, 1, 41, .{
        .io = std.testing.io,
        .cancellation = .{ .ptr = &cancel, .is_cancelled_fn = Cancel.check },
    }));
    try std.testing.expectEqual(@as(u64, 42), ledger.snapshot().total.retained_bytes);
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.runnable);
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.queued);
    try continuation.grow(.{ .retained_bytes = 1 }); // the caller still owns its state
}

test "workload admission scheduler closes waiters but cannot revoke active or suspended owners" {
    var ledger = try resources.Ledger.init(std.testing.allocator, testPolicy());
    defer ledger.deinit();
    var scheduler = try Scheduler.init(&ledger, .{});
    var request = try ledger.admit(.general_read, 1);
    defer request.release() catch unreachable;
    var active = try scheduler.acquire(&request, .general_read, .{ .runnable = 2 }, 1, 1, .{ .io = std.testing.io });
    defer active.release(1);
    var queued_request = try ledger.admit(.general_read, 1);
    defer queued_request.release() catch unreachable;
    var waiter = try testWaiter(&ledger, &queued_request, 1, 1);
    scheduler.enqueue(&waiter);
    scheduler.close();
    try std.testing.expectEqual(error.AdmissionClosed, waiter.failure.?);
    try std.testing.expectEqual(@as(u64, 2), ledger.snapshot().total.runnable);
    try std.testing.expectEqual(@as(u64, 2), ledger.snapshot().total.requests);
    try std.testing.expectError(error.AdmissionClosed, scheduler.acquire(&queued_request, .general_read, .{ .runnable = 1 }, 1, 1, .{ .io = std.testing.io }));
}

test "workload admission resumption needs no free start queue or metadata and preserves minimum state" {
    const total: resources.Bundle = .{ .handles = 6, .requests = 3, .queued = 1, .queued_bytes = 1, .runnable = 1, .retained_bytes = 100 };
    var ledger = try resources.Ledger.init(std.testing.allocator, .{ .total = total, .lanes = @splat(.{ .ceiling = total }) });
    defer ledger.deinit();
    var scheduler = try Scheduler.init(&ledger, .{});
    var request = try ledger.admit(.general_read, 1);
    defer request.release() catch unreachable;
    var job = try scheduler.acquire(&request, .general_read, .{ .runnable = 1, .retained_bytes = 40 }, 1, 1, .{ .io = std.testing.io });
    var state = try job.yieldState(40, 1);
    defer state.release() catch unreachable;
    try std.testing.expectError(error.RetainedStateStillLive, scheduler.acquireResume(&request, .general_read, &state, .{ .runnable = 1, .retained_bytes = 39 }, 1, 1, .{ .io = std.testing.io }));
    var other = try ledger.admit(.general_read, 1);
    defer other.release() catch unreachable;
    var blocker = try scheduler.acquire(&other, .general_read, .{ .runnable = 1 }, 1, 1, .{ .io = std.testing.io });
    defer blocker.release(1);
    var newcomer = try ledger.admit(.general_read, 1);
    defer newcomer.release() catch unreachable;
    var start = try testWaiter(&ledger, &newcomer, 1, 1);
    scheduler.enqueue(&start);
    try std.testing.expectEqual(total.handles, ledger.snapshot().total.handles);
    const Wake = struct {
        blocker: *Scheduler.Job,
        checks: usize = 0,
        fn check(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.checks += 1;
            if (self.checks == 2) self.blocker.release(1);
            return false;
        }
    };
    var wake: Wake = .{ .blocker = &blocker };
    var resumed = try scheduler.acquireResume(&request, .general_read, &state, .{ .runnable = 1, .retained_bytes = 40 }, 1, 41, .{
        .io = std.testing.io,
        .cancellation = .{ .ptr = &wake, .is_cancelled_fn = Wake.check },
    });
    try std.testing.expect(!start.finished);
    try std.testing.expectEqual(@as(u64, 1), ledger.snapshot().total.queued);
    resumed.release(1);
    try std.testing.expect(start.finished);
    try start.job.?.release();
}

test "workload admission weighted service is work conserving and does not accrue polling credit" {
    const total: resources.Bundle = .{ .handles = 8, .requests = 3, .queued = 3, .queued_bytes = 10, .runnable = 1, .retained_bytes = 100 };
    var ledger = try resources.Ledger.init(std.testing.allocator, .{ .total = total, .lanes = @splat(.{ .ceiling = total }) });
    defer ledger.deinit();
    var policy: Policy = .{};
    policy.weights[@intFromEnum(resources.Lane.write)] = 3;
    var scheduler = try Scheduler.init(&ledger, policy);
    var reads = try ledger.admit(.general_read, 1);
    defer reads.release() catch unreachable;
    var writes = try ledger.admit(.write, 1);
    defer writes.release() catch unreachable;
    var busy = try ledger.admit(.control, 1);
    defer busy.release() catch unreachable;
    var blocker = try scheduler.acquire(&busy, .control, .{ .runnable = 1 }, 1, 1, .{ .io = std.testing.io });
    var pending = [_]Scheduler.Waiter{ try testWaiter(&ledger, &reads, 1, 1), try testWaiter(&ledger, &writes, 1, 2) };
    pending[1].lane = .write;
    pending[0].estimate = 64;
    pending[1].estimate = 64;
    for (&pending) |*waiter| scheduler.enqueue(waiter);
    for (0..100) |_| scheduler.schedule();
    try std.testing.expectEqual(@as(i128, 0), scheduler.lanes[@intFromEnum(resources.Lane.general_read)].service);
    try std.testing.expectEqual(@as(i128, 0), scheduler.lanes[@intFromEnum(resources.Lane.write)].service);
    blocker.release(1);
    var completions = [_]usize{ 0, 0 };
    for (0..40) |sequence| {
        const selected: usize = if (pending[0].finished) 0 else 1;
        const waiter = &pending[selected];
        try std.testing.expect(waiter.finished);
        completions[selected] += 1;
        try waiter.job.?.release();
        waiter.* = try testWaiter(&ledger, if (selected == 0) &reads else &writes, 1, sequence + 3);
        waiter.lane = if (selected == 0) .general_read else .write;
        waiter.estimate = 64;
        scheduler.enqueue(waiter);
        scheduler.schedule();
    }
    try std.testing.expect(completions[0] >= 9 and completions[0] <= 11);
    try std.testing.expect(completions[1] >= 29 and completions[1] <= 31);
    for (&pending) |*waiter| {
        if (waiter.finished) try waiter.job.?.release() else scheduler.finish(waiter, error.AdmissionClosed);
    }
}
