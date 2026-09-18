// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Transport-independent bounded admission. The runtime owns waiter storage;
//! this controller owns queue membership and reservations until retirement.
//! No allocation, operator execution, or cancellation callback occurs under lock.
const std = @import("std");
const CancellationToken = @import("cancellation.zig").CancellationToken;

pub const Config = struct {
    max_queued_requests: usize = 0,
    max_queued_bytes: usize = 0,
    max_retained_bytes: usize = 0,
    max_wait_ms: u32 = 0,

    pub fn validate(self: Config) !void {
        if (self.max_wait_ms > 60_000) return error.InvalidConfig;
        if (self.max_wait_ms == 0) {
            if (self.max_queued_requests != 0 or self.max_queued_bytes != 0) return error.InvalidConfig;
        } else if (self.max_queued_requests == 0 or self.max_queued_bytes == 0 or self.max_retained_bytes == 0) {
            return error.InvalidConfig;
        }
        if (self.max_queued_bytes > self.max_retained_bytes) return error.InvalidConfig;
    }
};

pub const Options = struct {
    io: std.Io,
    /// The deadline uses clock_io when supplied, otherwise io's awake clock.
    deadline_ns: ?u64 = null,
    clock_io: ?std.Io = null,
    native_now_ns: ?*const fn () u64 = null,
    cancellation: CancellationToken = .none,
    retained_bytes: usize = 0,

    pub fn now(self: Options) u64 {
        if (self.native_now_ns) |native| return native();
        return @intCast(@max(0, std.Io.Clock.now(.awake, self.clock_io orelse self.io).nanoseconds));
    }

    pub fn check(self: Options) !void {
        try self.cancellation.check();
        if (self.deadline_ns) |deadline| if (self.now() >= deadline) return error.DeadlineExceeded;
    }
};

pub const Failure = error{ AdmissionFull, AdmissionQueueFull, AdmissionBytesExhausted, AdmissionRequestTooLarge, AdmissionWaitTimeout, AdmissionClosed, DeadlineExceeded };

pub const wait_bucket_ms = [_]u64{ 1, 5, 10, 25, 50, 100, 250, 1000, 5000, 60000 };
pub const wait_bucket_seconds = [_][]const u8{ "0.001", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "1", "5", "60", "+Inf" };

/// Bounded diagnostic vocabulary. One request rejection increments exactly one
/// reason; allocation denials are separate because an allocator can retry growth.
pub const RejectionReason = enum {
    execution_capacity,
    queue_count,
    queue_bytes,
    retained_bytes,
    request_bytes,
    wait_timeout,
    deadline,
    draining,
    policy_reduction,
};
pub const rejection_reason_count = @typeInfo(RejectionReason).@"enum".fields.len;
pub const AllocationDenial = enum { retained_bytes, allocation_bytes, draining };
pub const allocation_denial_count = @typeInfo(AllocationDenial).@"enum".fields.len;

/// Stable allocation accounting can outlive the embedded admission controller.
/// Its fixed metadata is controller overhead; retained_bytes tracks every
/// allocation charged through it, including allocation-owner metadata.
pub const MemoryAccount = struct {
    allocator: std.mem.Allocator,
    refs: std.atomic.Value(usize) = .init(2), // controller and first owner
    mutex: std.atomic.Mutex = .unlocked,
    controller: ?*Controller,
    retained_bytes: usize = 0,

    fn lock(self: *MemoryAccount) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn retain(self: *MemoryAccount) void {
        const previous = self.refs.fetchAdd(1, .monotonic);
        std.debug.assert(previous > 0 and previous < std.math.maxInt(usize));
    }

    pub fn release(self: *MemoryAccount) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.controller == null and self.retained_bytes == 0);
        self.allocator.destroy(self);
    }

    pub fn reserve(self: *MemoryAccount, bytes: usize) !void {
        self.lock();
        defer self.mutex.unlock();
        const controller = self.controller orelse return error.AdmissionClosed;
        try controller.reserveMemory(bytes);
        self.retained_bytes += bytes;
    }

    pub fn free(self: *MemoryAccount, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.retained_bytes);
        self.retained_bytes -= bytes;
        if (self.controller) |controller| controller.releaseMemory(bytes);
    }

    pub fn retainedBytes(self: *MemoryAccount) usize {
        self.lock();
        defer self.mutex.unlock();
        return self.retained_bytes;
    }
};

pub const Controller = struct {
    capacity: usize,
    config: Config = .{},
    mutex: std.atomic.Mutex = .unlocked,
    active: usize = 0,
    queued: usize = 0,
    queued_bytes: usize = 0,
    retained_bytes: usize = 0,
    peak: usize = 0,
    rejected: u64 = 0,
    waited: u64 = 0,
    wait_ns: u64 = 0,
    wait_completed: u64 = 0,
    wait_buckets: [wait_bucket_ms.len + 1]u64 = @splat(0),
    expired: u64 = 0,
    cancelled: u64 = 0,
    closed: bool = false,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,
    memory_account: ?*MemoryAccount = null,
    policy_generation: u64 = 1,
    rejection_reasons: [rejection_reason_count]u64 = @splat(0),
    allocation_denials: [allocation_denial_count]u64 = @splat(0),

    const Waiter = struct {
        previous: ?*Waiter = null,
        next: ?*Waiter = null,
        options: Options,
        wait_deadline_ns: u64,
        started_ns: u64,
        ready: std.Io.Event = .unset,
        outcome: ?Failure = null,
        finished: bool = false,
    };

    pub const Lease = struct {
        owner: ?*Controller,
        bytes: usize,

        pub fn release(self: *Lease) void {
            const owner = self.owner orelse return;
            self.owner = null;
            owner.releaseBytes(self.bytes);
        }
    };

    pub const Stats = struct {
        capacity: usize,
        in_flight: usize,
        peak_in_flight: usize,
        rejected_total: u64,
        queued: usize = 0,
        queued_bytes: usize = 0,
        retained_bytes: usize = 0,
        waited_total: u64 = 0,
        wait_ns_total: u64 = 0,
        wait_completed_total: u64 = 0,
        wait_buckets: [wait_bucket_ms.len + 1]u64 = @splat(0),
        expired_total: u64 = 0,
        cancelled_total: u64 = 0,
        draining: bool = false,
        max_queued_requests: usize = 0,
        max_queued_bytes: usize = 0,
        max_retained_bytes: usize = 0,
        max_wait_ms: u32 = 0,
        /// Zero denotes an older/partial statistics provider without diagnostics.
        policy_generation: u64 = 0,
        rejection_reasons: [rejection_reason_count]u64 = @splat(0),
        allocation_denials: [allocation_denial_count]u64 = @splat(0),
    };

    pub fn init(capacity: usize) Controller {
        return .{ .capacity = capacity };
    }

    pub fn initConfigured(capacity: usize, config: Config) Controller {
        config.validate() catch @panic("invalid workload admission configuration");
        return .{ .capacity = capacity, .config = config };
    }

    fn lock(self: *Controller) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Configure only before publishing the owner. Runtime reductions use reconfigure.
    pub fn configure(self: *Controller, config: Config) !void {
        try config.validate();
        self.lock();
        defer self.mutex.unlock();
        if (self.active != 0 or self.queued != 0 or self.retained_bytes != 0) return error.AdmissionBusy;
        self.config = config;
    }

    fn hasSlot(self: *const Controller) bool {
        return self.capacity == 0 or self.active < self.capacity;
    }

    fn fitsBytes(self: *const Controller, bytes: usize) bool {
        return bytes <= std.math.maxInt(usize) - self.retained_bytes and
            (self.config.max_retained_bytes == 0 or (self.retained_bytes <= self.config.max_retained_bytes and bytes <= self.config.max_retained_bytes - self.retained_bytes));
    }

    /// Allocation ownership is independent of runnable/request count. A response
    /// may still own bytes after its handler returns. Growth never queues while
    /// holding partially allocated state; callers unwind on failure.
    pub fn reserveMemory(self: *Controller, bytes: usize) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) {
            self.allocation_denials[@intFromEnum(AllocationDenial.draining)] +|= 1;
            return error.AdmissionClosed;
        }
        if (self.config.max_retained_bytes != 0 and bytes > self.config.max_retained_bytes) {
            self.allocation_denials[@intFromEnum(AllocationDenial.allocation_bytes)] +|= 1;
            return error.AdmissionRequestTooLarge;
        }
        if (!self.fitsBytes(bytes)) {
            self.allocation_denials[@intFromEnum(AllocationDenial.retained_bytes)] +|= 1;
            return error.AdmissionBytesExhausted;
        }
        self.retained_bytes += bytes;
    }

    pub fn releaseMemory(self: *Controller, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(bytes <= self.retained_bytes);
        self.retained_bytes -= bytes;
        self.grant();
    }

    pub fn memoryAccount(self: *Controller, allocator: std.mem.Allocator) !*MemoryAccount {
        self.lock();
        if (self.closed) {
            self.mutex.unlock();
            return error.AdmissionClosed;
        }
        if (self.memory_account) |account| {
            account.retain();
            self.mutex.unlock();
            return account;
        }
        self.mutex.unlock();
        const candidate = try allocator.create(MemoryAccount);
        var published = false;
        defer if (!published) allocator.destroy(candidate);
        candidate.* = .{ .allocator = allocator, .controller = self };
        self.lock();
        defer self.mutex.unlock();
        if (self.closed) return error.AdmissionClosed;
        if (self.memory_account) |account| {
            account.retain();
            return account;
        }
        self.memory_account = candidate;
        published = true;
        return candidate;
    }

    /// Call after request execution has drained, before destroying controller
    /// storage. Exported output buffers remain charged and may be freed later.
    /// Never hold the controller lock while taking the account lock: allocation
    /// releases take these locks in the opposite order to wake queued work.
    pub fn deinitMemory(self: *Controller) void {
        self.close();
        self.lock();
        const account = self.memory_account;
        self.memory_account = null;
        self.mutex.unlock();
        if (account) |value| {
            value.lock();
            value.controller = null;
            value.mutex.unlock();
            value.release();
        }
    }

    fn activate(self: *Controller) void {
        self.active += 1;
        self.peak = @max(self.peak, self.active);
    }

    fn recordRejection(self: *Controller, reason: RejectionReason) void {
        self.rejected +|= 1;
        self.rejection_reasons[@intFromEnum(reason)] +|= 1;
    }

    /// Legacy nonwaiting callers share execution capacity and cannot bypass FIFO.
    pub fn tryAcquire(self: *Controller) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.closed or self.head != null or !self.hasSlot() or !self.fitsBytes(0)) {
            self.recordRejection(if (self.closed) .draining else if (!self.fitsBytes(0)) .retained_bytes else .execution_capacity);
            return false;
        }
        self.activate();
        return true;
    }

    pub fn tryAcquireLease(self: *Controller) ?Lease {
        if (!self.tryAcquire()) return null;
        return .{ .owner = self, .bytes = 0 };
    }

    pub fn acquire(self: *Controller, options: Options) !Lease {
        try options.check();
        const started = options.now();
        var waiter: Waiter = .{
            .options = options,
            .started_ns = started,
            .wait_deadline_ns = 0,
        };
        self.lock();
        waiter.wait_deadline_ns = started +| @as(u64, self.config.max_wait_ms) * std.time.ns_per_ms;
        const failure: ?Failure = if (self.closed) error.AdmissionClosed else if (self.config.max_retained_bytes != 0 and options.retained_bytes > self.config.max_retained_bytes) error.AdmissionRequestTooLarge else if (!self.fitsBytes(options.retained_bytes)) error.AdmissionBytesExhausted else null;
        if (failure) |err| {
            self.recordRejection(if (err == error.AdmissionClosed) .draining else if (err == error.AdmissionRequestTooLarge) .request_bytes else .retained_bytes);
            self.mutex.unlock();
            return err;
        }
        if (self.head == null and self.hasSlot()) {
            self.activate();
            self.retained_bytes += options.retained_bytes;
            self.mutex.unlock();
            var lease: Lease = .{ .owner = self, .bytes = options.retained_bytes };
            errdefer lease.release();
            try options.check();
            return lease;
        }
        const queue_failure: ?Failure = if (self.config.max_wait_ms == 0) error.AdmissionFull else if (self.queued >= self.config.max_queued_requests) error.AdmissionQueueFull else if (options.retained_bytes > self.config.max_queued_bytes -| self.queued_bytes) error.AdmissionBytesExhausted else null;
        if (queue_failure) |err| {
            self.recordRejection(if (err == error.AdmissionFull) .execution_capacity else if (err == error.AdmissionQueueFull) .queue_count else .queue_bytes);
            self.mutex.unlock();
            return err;
        }
        self.retained_bytes += options.retained_bytes;
        self.queued_bytes += options.retained_bytes;
        self.queued += 1;
        self.waited +|= 1;
        waiter.previous = self.tail;
        if (self.tail) |tail| tail.next = &waiter else self.head = &waiter;
        self.tail = &waiter;
        self.mutex.unlock();

        // The caller always rejoins the owner lock before reading the outcome or
        // retiring stack storage, including cancellation racing publication.
        while (true) {
            options.check() catch |err| {
                self.cancelWaiter(&waiter, err);
                return err;
            };
            self.lock();
            if (waiter.finished) {
                const outcome = waiter.outcome;
                self.mutex.unlock();
                if (outcome) |err| return err;
                var lease: Lease = .{ .owner = self, .bytes = options.retained_bytes };
                errdefer lease.release();
                try options.check();
                return lease;
            }
            const now = options.now();
            if (now >= waiter.wait_deadline_ns) {
                self.remove(&waiter);
                self.retire(&waiter, error.AdmissionWaitTimeout);
                self.grant();
                self.mutex.unlock();
                return error.AdmissionWaitTimeout;
            }
            const end = @min(waiter.wait_deadline_ns, options.deadline_ns orelse std.math.maxInt(u64));
            const poll_ns = @min(5 * std.time.ns_per_ms, end -| now);
            self.mutex.unlock();
            waiter.ready.waitTimeout(options.io, .{ .duration = .{ .raw = .fromNanoseconds(poll_ns), .clock = .awake } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.cancelWaiter(&waiter, err);
                    return err;
                },
            };
        }
    }

    fn remove(self: *Controller, target: *Waiter) void {
        if (target.previous) |prior| prior.next = target.next else {
            std.debug.assert(self.head == target);
            self.head = target.next;
        }
        if (target.next) |next| next.previous = target.previous else {
            std.debug.assert(self.tail == target);
            self.tail = target.previous;
        }
        target.previous = null;
        target.next = null;
    }

    fn retire(self: *Controller, waiter: *Waiter, outcome: ?Failure) void {
        self.queued -= 1;
        self.queued_bytes -= waiter.options.retained_bytes;
        self.recordWait(waiter.options.now() -| waiter.started_ns);
        if (outcome) |err| {
            self.retained_bytes -= waiter.options.retained_bytes;
            self.recordRejection(switch (err) {
                error.AdmissionWaitTimeout => .wait_timeout,
                error.DeadlineExceeded => .deadline,
                error.AdmissionClosed => .draining,
                // Reconfiguration is the only source of queue-full retirement.
                error.AdmissionQueueFull => .policy_reduction,
                else => unreachable,
            });
            if (err == error.DeadlineExceeded or err == error.AdmissionWaitTimeout) self.expired +|= 1;
        } else self.activate();
        waiter.outcome = outcome;
        waiter.finished = true;
        // Event publication is a runtime primitive, never a user callback. The
        // caller must acquire mutex before freeing waiter storage.
        waiter.ready.set(waiter.options.io);
    }

    fn recordWait(self: *Controller, ns: u64) void {
        self.wait_ns +|= ns;
        self.wait_completed +|= 1;
        inline for (wait_bucket_ms, 0..) |ms, i| {
            if (ns <= ms * std.time.ns_per_ms) self.wait_buckets[i] +|= 1;
        }
        self.wait_buckets[wait_bucket_ms.len] +|= 1;
    }

    fn grant(self: *Controller) void {
        while (self.head) |waiter| {
            const now = waiter.options.now();
            const outcome: ?Failure = if (self.closed) error.AdmissionClosed else if (waiter.options.deadline_ns != null and now >= waiter.options.deadline_ns.?) error.DeadlineExceeded else if (now >= waiter.wait_deadline_ns) error.AdmissionWaitTimeout else null;
            if (outcome == null and (!self.hasSlot() or !self.fitsBytes(0))) return;
            self.remove(waiter);
            self.retire(waiter, outcome);
        }
    }

    fn cancelWaiter(self: *Controller, waiter: *Waiter, cause: anyerror) void {
        self.lock();
        defer self.mutex.unlock();
        if (waiter.finished) {
            if (waiter.outcome == null) {
                self.active -= 1;
                self.retained_bytes -= waiter.options.retained_bytes;
            } else return; // The grant/close path already recorded retirement.
        } else {
            self.remove(waiter);
            self.queued -= 1;
            self.queued_bytes -= waiter.options.retained_bytes;
            self.retained_bytes -= waiter.options.retained_bytes;
            self.recordWait(waiter.options.now() -| waiter.started_ns);
        }
        if (cause == error.DeadlineExceeded) {
            self.expired +|= 1;
            self.recordRejection(.deadline);
        } else self.cancelled +|= 1;
        self.grant();
    }

    pub fn release(self: *Controller) void {
        self.releaseBytes(0);
    }

    fn releaseBytes(self: *Controller, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.active > 0 and self.retained_bytes >= bytes);
        self.active -= 1;
        self.retained_bytes -= bytes;
        self.grant();
    }

    /// Active leases remain owned by callers. Closing only retires waiters.
    pub fn close(self: *Controller) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        self.grant();
    }

    pub fn reconfigure(self: *Controller, capacity: usize, config: Config) !void {
        try config.validate();
        self.lock();
        defer self.mutex.unlock();
        self.capacity = capacity;
        self.config = config;
        self.policy_generation +|= 1;
        // Oldest requests retain priority. Retire excess newest waiters and
        // stop granting while live bytes exceed a reduced process envelope.
        while (self.tail) |waiter| {
            if (config.max_wait_ms != 0 and self.queued <= config.max_queued_requests and self.queued_bytes <= config.max_queued_bytes and (config.max_retained_bytes == 0 or self.retained_bytes <= config.max_retained_bytes)) break;
            self.remove(waiter);
            self.retire(waiter, error.AdmissionQueueFull);
        }
        var cursor = self.head;
        while (cursor) |waiter| : (cursor = waiter.next) {
            waiter.wait_deadline_ns = @min(waiter.wait_deadline_ns, waiter.started_ns +| @as(u64, config.max_wait_ms) * std.time.ns_per_ms);
        }
        self.grant();
    }

    pub fn stats(self: *const Controller) Stats {
        const owner = @constCast(self);
        owner.lock();
        defer owner.mutex.unlock();
        return .{
            .capacity = self.capacity,
            .in_flight = self.active,
            .peak_in_flight = self.peak,
            .rejected_total = self.rejected,
            .queued = self.queued,
            .queued_bytes = self.queued_bytes,
            .retained_bytes = self.retained_bytes,
            .waited_total = self.waited,
            .wait_ns_total = self.wait_ns,
            .wait_completed_total = self.wait_completed,
            .wait_buckets = self.wait_buckets,
            .expired_total = self.expired,
            .cancelled_total = self.cancelled,
            .draining = self.closed,
            .max_queued_requests = self.config.max_queued_requests,
            .max_queued_bytes = self.config.max_queued_bytes,
            .max_retained_bytes = self.config.max_retained_bytes,
            .max_wait_ms = self.config.max_wait_ms,
            .policy_generation = self.policy_generation,
            .rejection_reasons = self.rejection_reasons,
            .allocation_denials = self.allocation_denials,
        };
    }
};

test "workload admission validates queue bounds and preserves zero capacity" {
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_wait_ms = 1 }).validate());
    try std.testing.expectError(error.InvalidConfig, (Config{ .max_queued_requests = 1 }).validate());
    var controller = Controller.init(0);
    var a = try controller.acquire(.{ .io = std.testing.io, .retained_bytes = 100 });
    var b = try controller.acquire(.{ .io = std.testing.io, .retained_bytes = 200 });
    try std.testing.expectEqual(@as(usize, 300), controller.stats().retained_bytes);
    a.release();
    a.release();
    b.release();
    try std.testing.expectEqual(@as(usize, 0), controller.stats().retained_bytes);
}

test "workload admission diagnostics distinguish request rejection from allocation pressure" {
    var gate = Controller.initConfigured(1, .{ .max_retained_bytes = 128 });
    var lease = try gate.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
    defer lease.release();
    try std.testing.expectError(error.AdmissionFull, gate.acquire(.{ .io = std.testing.io, .retained_bytes = 1 }));
    try std.testing.expectError(error.AdmissionRequestTooLarge, gate.acquire(.{ .io = std.testing.io, .retained_bytes = 129 }));
    try std.testing.expectError(error.AdmissionBytesExhausted, gate.acquire(.{ .io = std.testing.io, .retained_bytes = 65 }));
    try std.testing.expectError(error.AdmissionRequestTooLarge, gate.reserveMemory(129));
    try std.testing.expectError(error.AdmissionBytesExhausted, gate.reserveMemory(65));
    const generation = gate.stats().policy_generation;
    try std.testing.expectError(error.InvalidConfig, gate.reconfigure(8, .{ .max_wait_ms = 1 }));
    try std.testing.expectEqual(generation, gate.stats().policy_generation);
    try std.testing.expectEqual(@as(usize, 1), gate.stats().capacity);
    try gate.reconfigure(2, .{ .max_retained_bytes = 32 });
    const reduced = gate.stats();
    try std.testing.expectEqual(generation + 1, reduced.policy_generation);
    try std.testing.expectEqual(@as(usize, 64), reduced.retained_bytes);
    try std.testing.expect(!gate.tryAcquire());
    gate.close();
    try std.testing.expect(!gate.tryAcquire());
    try std.testing.expectError(error.AdmissionClosed, gate.reserveMemory(1));
    const stats = gate.stats();
    try std.testing.expectEqual(@as(u64, 5), stats.rejected_total);
    try std.testing.expectEqual(stats.rejected_total, @reduce(.Add, @as(@Vector(rejection_reason_count, u64), stats.rejection_reasons)));
    try std.testing.expectEqual(@as(u64, 1), stats.rejection_reasons[@intFromEnum(RejectionReason.execution_capacity)]);
    try std.testing.expectEqual(@as(u64, 1), stats.rejection_reasons[@intFromEnum(RejectionReason.request_bytes)]);
    try std.testing.expectEqual(@as(u64, 2), stats.rejection_reasons[@intFromEnum(RejectionReason.retained_bytes)]);
    try std.testing.expectEqual(@as(u64, 1), stats.rejection_reasons[@intFromEnum(RejectionReason.draining)]);
    for (stats.allocation_denials) |count| try std.testing.expectEqual(@as(u64, 1), count);
    lease.release();
    try std.testing.expectEqual(@as(usize, 0), gate.stats().retained_bytes);
}

test "workload admission rejects expired cancelled and oversized work before starting" {
    var controller = Controller.init(1);
    try controller.configure(.{ .max_retained_bytes = 64 });
    try std.testing.expectError(error.AdmissionRequestTooLarge, controller.acquire(.{ .io = std.testing.io, .retained_bytes = 65 }));
    try std.testing.expectError(error.DeadlineExceeded, controller.acquire(.{ .io = std.testing.io, .deadline_ns = 0 }));
    var cancelled = std.atomic.Value(bool).init(true);
    try std.testing.expectError(error.Canceled, controller.acquire(.{ .io = std.testing.io, .cancellation = .fromAtomic(&cancelled) }));
    controller.close();
    try std.testing.expectError(error.AdmissionClosed, controller.acquire(.{ .io = std.testing.io }));
    try std.testing.expectEqual(@as(usize, 0), controller.stats().in_flight);
}

test "workload admission timeout retires queue bytes without releasing running work" {
    var controller = Controller.init(1);
    try controller.configure(.{ .max_wait_ms = 1, .max_queued_requests = 1, .max_queued_bytes = 64, .max_retained_bytes = 128 });
    var blocker = try controller.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
    defer blocker.release();
    try std.testing.expectError(error.AdmissionWaitTimeout, controller.acquire(.{ .io = std.testing.io, .retained_bytes = 64 }));
    const stats = controller.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.queued_bytes);
    try std.testing.expectEqual(@as(usize, 64), stats.retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), stats.in_flight);
    try std.testing.expectEqual(@as(u64, 1), stats.rejection_reasons[@intFromEnum(RejectionReason.wait_timeout)]);
}

test "workload admission cancellation racing grant returns every reservation once" {
    var controller = Controller.init(1);
    try controller.configure(.{ .max_wait_ms = 100, .max_queued_requests = 1, .max_queued_bytes = 64, .max_retained_bytes = 128 });
    var blocker = try controller.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
    defer blocker.release();
    const Race = struct {
        blocker: *Controller.Lease,
        calls: usize = 0,
        fn check(raw: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 1) return false;
            self.blocker.release();
            return true;
        }
    };
    var race: Race = .{ .blocker = &blocker };
    try std.testing.expectError(error.Canceled, controller.acquire(.{ .io = std.testing.io, .retained_bytes = 64, .cancellation = .{ .ptr = &race, .is_cancelled_fn = Race.check } }));
    const stats = controller.stats();
    try std.testing.expectEqual(@as(usize, 0), stats.in_flight);
    try std.testing.expectEqual(@as(usize, 0), stats.queued);
    try std.testing.expectEqual(@as(usize, 0), stats.retained_bytes);
    try std.testing.expectEqual(@as(u64, 1), stats.waited_total);
}

test "workload admission deadline checkpoints retire as expiry before or after grant" {
    const Checkpoint = struct {
        blocker: ?*Controller.Lease,
        calls: usize = 0,
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            self.calls += 1;
            if (self.calls == 1) return;
            if (self.blocker) |lease| lease.release();
            return error.DeadlineExceeded;
        }
    };
    for ([_]bool{ false, true }) |grant_first| {
        var controller = Controller.initConfigured(1, .{ .max_wait_ms = 5000, .max_queued_requests = 1, .max_queued_bytes = 64, .max_retained_bytes = 128 });
        var blocker = try controller.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
        defer blocker.release();
        var checkpoint: Checkpoint = .{ .blocker = if (grant_first) &blocker else null };
        try std.testing.expectError(error.DeadlineExceeded, controller.acquire(.{
            .io = std.testing.io,
            .retained_bytes = 64,
            .cancellation = .{ .ptr = &checkpoint, .check_fn = Checkpoint.check },
        }));
        const stats = controller.stats();
        try std.testing.expectEqual(@as(u64, 1), stats.expired_total);
        try std.testing.expectEqual(@as(u64, 0), stats.cancelled_total);
        try std.testing.expectEqual(@as(u64, 1), stats.rejected_total);
        try std.testing.expectEqual(@as(u64, 1), stats.rejection_reasons[@intFromEnum(RejectionReason.deadline)]);
        try std.testing.expectEqual(@as(usize, 0), stats.queued);
        try std.testing.expectEqual(@as(usize, if (grant_first) 0 else 64), stats.retained_bytes);
        blocker.release();
        try std.testing.expectEqual(@as(usize, 0), controller.stats().in_flight);
    }
}

test "workload admission byte reductions stop grants without revoking live leases" {
    var owner = Controller.init(3);
    try owner.configure(.{ .max_retained_bytes = 128 });
    var a = try owner.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
    defer a.release();
    var b = try owner.acquire(.{ .io = std.testing.io, .retained_bytes = 64 });
    defer b.release();
    try owner.reconfigure(3, .{ .max_retained_bytes = 32 });
    try std.testing.expectEqual(@as(usize, 128), owner.stats().retained_bytes);
    a.release();
    try std.testing.expect(!owner.tryAcquire());
    try std.testing.expectError(error.AdmissionBytesExhausted, owner.acquire(.{ .io = std.testing.io, .retained_bytes = 1 }));
    b.release();
    var next = try owner.acquire(.{ .io = std.testing.io, .retained_bytes = 32 });
    next.release();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().retained_bytes);
}

test "workload admission FIFO pressure reduction and drain preserve ownership" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.io();
    const config: Config = .{ .max_wait_ms = 5000, .max_queued_requests = 2, .max_queued_bytes = 128, .max_retained_bytes = 192 };
    const Worker = struct {
        owner: *Controller,
        io: std.Io,
        granted: std.Io.Event = .unset,
        finish: std.Io.Event = .unset,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            var lease = self.owner.acquire(.{ .io = self.io, .retained_bytes = 64 }) catch |err| {
                self.err = err;
                self.granted.set(self.io);
                return;
            };
            defer lease.release();
            self.granted.set(self.io);
            self.finish.wait(self.io) catch {};
        }
        fn awaitQueued(owner: *Controller, runtime_io: std.Io, count: usize) !void {
            const deadline = (Options{ .io = runtime_io }).now() + 2 * std.time.ns_per_s;
            while (owner.stats().queued != count) {
                if ((Options{ .io = runtime_io }).now() >= deadline) return error.TestUnexpectedResult;
                try runtime_io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    for ([_]bool{ false, true }) |reduce| {
        var controller = Controller.init(1);
        try controller.configure(config);
        var blocker = try controller.acquire(.{ .io = io, .retained_bytes = 64 });
        defer blocker.release();
        var first: Worker = .{ .owner = &controller, .io = io };
        var second: Worker = .{ .owner = &controller, .io = io };
        var group: std.Io.Group = .init;
        defer {
            first.finish.set(io);
            second.finish.set(io);
            group.cancel(io);
        }
        try group.concurrent(io, Worker.run, .{&first});
        try Worker.awaitQueued(&controller, io, 1);
        try std.testing.expect(!controller.tryAcquire());
        try group.concurrent(io, Worker.run, .{&second});
        try Worker.awaitQueued(&controller, io, 2);
        try std.testing.expectError(error.AdmissionBytesExhausted, controller.acquire(.{ .io = io, .retained_bytes = 1 }));
        if (reduce) {
            try controller.reconfigure(1, .{ .max_wait_ms = 5000, .max_queued_requests = 1, .max_queued_bytes = 64, .max_retained_bytes = 128 });
            try second.granted.wait(io);
            try std.testing.expectEqual(error.AdmissionQueueFull, second.err.?);
            try std.testing.expectEqual(@as(u64, 1), controller.stats().rejection_reasons[@intFromEnum(RejectionReason.policy_reduction)]);
        }
        blocker.release();
        try first.granted.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
        try std.testing.expect(first.err == null);
        try std.testing.expectEqual(@as(usize, 1), controller.stats().in_flight);
        controller.close();
        if (!reduce) {
            try second.granted.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
            try std.testing.expectEqual(error.AdmissionClosed, second.err.?);
        }
        // Closing the queue cannot release the still-running first lease.
        try std.testing.expectEqual(@as(usize, 64), controller.stats().retained_bytes);
        first.finish.set(io);
        try group.await(io);
        try std.testing.expectEqual(@as(usize, 0), controller.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), controller.stats().in_flight);
    }
}

test "workload admission bursts through C80 stay bounded and drain without rejection" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.io();
    const Worker = struct {
        owner: *Controller,
        io: std.Io,
        finish: *std.Io.Event,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            var lease = self.owner.acquire(.{ .io = self.io, .retained_bytes = 256 }) catch |err| {
                self.err = err;
                return;
            };
            defer lease.release();
            self.finish.wait(self.io) catch |err| {
                self.err = err;
            };
        }
    };
    for ([_]usize{ 1, 5, 10, 20, 30, 40, 60, 80 }) |concurrency| {
        var controller = Controller.init(32);
        try controller.configure(.{ .max_wait_ms = 5000, .max_queued_requests = 64, .max_queued_bytes = 64 * 256, .max_retained_bytes = 96 * 256 });
        var workers: [80]Worker = undefined;
        var finish: std.Io.Event = .unset;
        var group: std.Io.Group = .init;
        defer {
            finish.set(io);
            group.cancel(io);
        }
        for (workers[0..concurrency]) |*worker| {
            worker.* = .{ .owner = &controller, .io = io, .finish = &finish };
            try group.concurrent(io, Worker.run, .{worker});
        }
        const deadline = (Options{ .io = io }).now() + 2 * std.time.ns_per_s;
        while (true) {
            const snapshot = controller.stats();
            if (snapshot.in_flight + snapshot.queued == concurrency) break;
            if ((Options{ .io = io }).now() >= deadline) return error.TestUnexpectedResult;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@min(concurrency, 32), controller.stats().in_flight);
        try std.testing.expectEqual(concurrency * 256, controller.stats().retained_bytes);
        finish.set(io);
        try group.await(io);
        for (workers[0..concurrency]) |worker| try std.testing.expect(worker.err == null);
        const drained = controller.stats();
        try std.testing.expectEqual(@as(usize, 0), drained.in_flight);
        try std.testing.expectEqual(@as(usize, 0), drained.retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), drained.queued_bytes);
        try std.testing.expectEqual(@as(u64, 0), drained.rejected_total);
        try std.testing.expectEqual(@as(u64, concurrency -| 32), drained.waited_total);
    }
}
