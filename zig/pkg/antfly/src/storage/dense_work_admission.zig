//! FIFO admission for whole rerank callers. Optional read helpers have a
//! separate, nonblocking node budget; a caller never waits for a helper.
const std = @import("std");
const time = @import("antfly_platform").time;
const admission = @import("admission_waiter.zig");
pub const Cancellation = admission.Cancellation;

pub const Queue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    capacity: u32 = 1,
    active: u32 = 0,
    peak: u32 = 0,
    waits: u64 = 0,
    waiters: admission.Fifo(void) = .{},
    const Waiter = admission.Fifo(void).Waiter;

    pub const Lease = struct {
        queue: ?*Queue = null,
        pub fn release(self: *@This()) void {
            const queue = self.queue orelse return;
            self.* = .{};
            queue.lock();
            defer queue.mutex.unlock();
            std.debug.assert(queue.active > 0);
            queue.active -= 1;
            queue.grant();
        }
    };

    fn lock(self: *Queue) void {
        while (!self.mutex.tryLock()) time.yieldBriefly();
    }

    fn grant(self: *Queue) void {
        while (self.active < @max(self.capacity, 1)) {
            const waiter = self.waiters.pop() orelse break;
            self.active += 1;
            self.peak = @max(self.peak, self.active);
            waiter.handoff.publish();
        }
    }

    fn cancel(self: *Queue, target: *Waiter) void {
        self.lock();
        defer self.mutex.unlock();
        if (target.handoff.isAdmitted()) {
            std.debug.assert(self.active > 0);
            self.active -= 1;
        } else {
            const removed = self.waiters.remove(target);
            std.debug.assert(removed);
        }
        self.grant();
    }

    pub fn acquire(self: *Queue, io: ?std.Io, cancellation: ?Cancellation) !Lease {
        if (cancellation) |token| if (token.is_cancelled(token.ptr)) return error.Cancelled;
        self.lock();
        if (self.waiters.head == null and self.active < @max(self.capacity, 1)) {
            self.active += 1;
            self.peak = @max(self.peak, self.active);
            self.mutex.unlock();
            return .{ .queue = self };
        }
        var waiter = Waiter{ .handoff = .{ .io = io }, .payload = {} };
        self.waiters.enqueue(&waiter);
        self.waits +|= 1;
        self.mutex.unlock();
        waiter.handoff.wait(cancellation) catch |err| {
            self.cancel(&waiter);
            return err;
        };
        return .{ .queue = self };
    }

    /// Helpers never wait and never jump ahead of queued query drivers.
    pub fn tryAcquire(self: *Queue) ?Lease {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        if (self.waiters.head != null or self.active >= @max(self.capacity, 1)) return null;
        self.active += 1;
        self.peak = @max(self.peak, self.active);
        return .{ .queue = self };
    }

    pub fn assertIdle(self: *Queue) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.active == 0 and self.waiters.head == null);
    }
};

test "dense rerank admission cancellation and lease lifetime" {
    var queue = Queue{ .capacity = 1 };
    var lease = try queue.acquire(null, null);
    try std.testing.expectEqual(@as(u32, 1), queue.active);
    lease.release();
    lease.release();
    queue.assertIdle();
    const Cancel = struct {
        fn yes(_: *const anyopaque) bool {
            return true;
        }
    };
    try std.testing.expectError(error.Cancelled, queue.acquire(null, .{ .ptr = &queue, .is_cancelled = Cancel.yes }));
    queue.assertIdle();
}

test "dense aggregate nonblocking helpers share capacity with caller leases" {
    var queue = Queue{ .capacity = 2 };
    var caller = try queue.acquire(null, null);
    var helper = queue.tryAcquire().?;
    try std.testing.expect(queue.tryAcquire() == null);
    helper.release();
    var next = queue.tryAcquire().?;
    next.release();
    caller.release();
    queue.assertIdle();
    try std.testing.expectEqual(@as(u32, 2), queue.peak);
}

test "dense rerank cancellation racing a grant returns the permit exactly once" {
    var queue = Queue{ .capacity = 1 };
    var blocker = try queue.acquire(null, null);
    defer blocker.release();
    const Cancel = struct {
        blocker: *Lease,
        calls: usize = 0,
        fn check(ptr: *const anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(@constCast(ptr)));
            self.calls += 1;
            if (self.calls == 1) return false; // preflight
            self.blocker.release(); // grant the enqueued caller before cancellation rejoins
            return true;
        }
        const Lease = Queue.Lease;
    };
    var cancel = Cancel{ .blocker = &blocker };
    try std.testing.expectError(error.Cancelled, queue.acquire(null, .{ .ptr = &cancel, .is_cancelled = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 2), cancel.calls);
    queue.assertIdle();
    var next = queue.tryAcquire().?;
    next.release();
    next.release();
    queue.assertIdle();
}

test "dense rerank callers queue FIFO and cancel without retaining capacity" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.io();
    var queue = Queue{ .capacity = 1 };
    var blocker = try queue.acquire(io, null);
    defer blocker.release();
    const Worker = struct {
        queue: *Queue,
        io: std.Io,
        cancelled: std.atomic.Value(bool) = .init(false),
        acquired: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        release: std.Io.Event = .unset,
        err: ?anyerror = null,
        fn isCancelled(ptr: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(ptr));
            return self.cancelled.load(.acquire);
        }
        fn run(self: *@This()) std.Io.Cancelable!void {
            defer self.done.store(true, .release);
            var lease = self.queue.acquire(self.io, .{ .ptr = self, .is_cancelled = isCancelled }) catch |err| {
                self.err = err;
                return;
            };
            defer lease.release();
            self.acquired.store(true, .release);
            try self.release.wait(self.io);
        }
    };
    var first = Worker{ .queue = &queue, .io = io };
    var second = Worker{ .queue = &queue, .io = io };
    var group = std.Io.Group.init;
    defer group.cancel(io);
    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
    try group.concurrent(io, Worker.run, .{&first});
    while (true) {
        queue.lock();
        const waits = queue.waits;
        queue.mutex.unlock();
        if (waits == 1) break;
        if (time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try group.concurrent(io, Worker.run, .{&second});
    while (true) {
        queue.lock();
        const waits = queue.waits;
        queue.mutex.unlock();
        if (waits == 2) break;
        if (time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    blocker.release();
    while (!first.acquired.load(.acquire)) {
        if (time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!second.acquired.load(.acquire));
    second.cancelled.store(true, .release);
    while (!second.done.load(.acquire)) {
        if (time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(error.Cancelled, second.err.?);
    first.release.set(io);
    try group.await(io);
    queue.assertIdle();
    try std.testing.expectEqual(@as(u32, 1), queue.peak);
}

test "dense rerank Io cancellation retires a queued stack waiter" {
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{});
    defer runtime.deinit();
    const io = runtime.io();
    var queue = Queue{ .capacity = 1 };
    var blocker = try queue.acquire(io, null);
    defer blocker.release();
    const Worker = struct {
        queue: *Queue,
        io: std.Io,
        err: ?anyerror = null,
        fn run(self: *@This()) void {
            var lease = self.queue.acquire(self.io, null) catch |err| {
                self.err = err;
                return;
            };
            lease.release();
        }
    };
    var worker = Worker{ .queue = &queue, .io = io };
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Worker.run, .{&worker});
    const deadline = time.monotonicNs() + 5 * std.time.ns_per_s;
    while (true) {
        queue.lock();
        const pending = queue.waiters.head != null;
        queue.mutex.unlock();
        if (pending) break;
        if (time.monotonicNs() >= deadline) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    group.cancel(io);
    try std.testing.expectEqual(error.Canceled, worker.err.?);
    try std.testing.expect(queue.waiters.head == null and queue.waiters.tail == null);
    try std.testing.expectEqual(@as(u32, 1), queue.active);
    blocker.release();
    queue.assertIdle();
}
