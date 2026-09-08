//! FIFO admission for whole rerank callers. Optional read helpers have a
//! separate, nonblocking node budget; a caller never waits for a helper.
const std = @import("std");
const time = @import("antfly_platform").time;

pub const Cancellation = struct {
    ptr: *const anyopaque,
    is_cancelled: *const fn (*const anyopaque) bool,
};

pub const Queue = struct {
    mutex: std.atomic.Mutex = .unlocked,
    capacity: u32 = 1,
    active: u32 = 0,
    peak: u32 = 0,
    waits: u64 = 0,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,

    const Waiter = struct {
        next: ?*Waiter = null,
        ready: std.Io.Event = .unset,
        admitted: std.atomic.Value(bool) = .init(false),
        io: ?std.Io,
    };

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
            const waiter = self.head orelse break;
            self.head = waiter.next;
            if (self.head == null) self.tail = null;
            self.active += 1;
            self.peak = @max(self.peak, self.active);
            // Signal before publishing admission: after observing admitted,
            // the owner may retire this stack waiter immediately.
            if (waiter.io) |io| waiter.ready.set(io);
            waiter.admitted.store(true, .release);
        }
    }

    fn cancel(self: *Queue, target: *Waiter) void {
        self.lock();
        defer self.mutex.unlock();
        if (target.admitted.load(.acquire)) {
            self.active -= 1;
        } else {
            var previous: ?*Waiter = null;
            var cursor = self.head;
            while (cursor) |waiter| : (cursor = waiter.next) {
                if (waiter == target) {
                    if (previous) |p| p.next = waiter.next else self.head = waiter.next;
                    if (self.tail == waiter) self.tail = previous;
                    break;
                }
                previous = waiter;
            }
        }
        self.grant();
    }

    pub fn acquire(self: *Queue, io: ?std.Io, cancellation: ?Cancellation) !Lease {
        if (cancellation) |token| if (token.is_cancelled(token.ptr)) return error.Cancelled;
        self.lock();
        if (self.head == null and self.active < @max(self.capacity, 1)) {
            self.active += 1;
            self.peak = @max(self.peak, self.active);
            self.mutex.unlock();
            return .{ .queue = self };
        }
        var waiter = Waiter{ .io = io };
        if (self.tail) |tail| tail.next = &waiter else self.head = &waiter;
        self.tail = &waiter;
        self.waits +|= 1;
        self.mutex.unlock();
        while (!waiter.admitted.load(.acquire)) {
            if (cancellation) |token| if (token.is_cancelled(token.ptr)) {
                self.cancel(&waiter);
                return error.Cancelled;
            };
            if (io) |runtime| {
                waiter.ready.waitTimeout(runtime, .{ .duration = .{
                    .raw = std.Io.Duration.fromMilliseconds(5),
                    .clock = .awake,
                } }) catch |err| switch (err) {
                    error.Timeout => {},
                    error.Canceled => {
                        self.cancel(&waiter);
                        return err;
                    },
                };
            } else time.yieldBriefly();
        }
        return .{ .queue = self };
    }

    /// Helpers never wait and never jump ahead of queued query drivers.
    pub fn tryAcquire(self: *Queue) ?Lease {
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock();
        if (self.head != null or self.active >= @max(self.capacity, 1)) return null;
        self.active += 1;
        self.peak = @max(self.peak, self.active);
        return .{ .queue = self };
    }

    pub fn assertIdle(self: *Queue) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.active == 0 and self.head == null);
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
