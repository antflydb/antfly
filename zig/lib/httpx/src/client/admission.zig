//! Bounded outbound attempts, shared across protocols and optionally clients.
//! Owners must join callers before deinit. Waiting happens before transmission.
const std = @import("std");
const Io = std.Io;
const WriteControl = @import("../net/socket.zig").WriteControl;

pub const Config = struct {
    max_active: u32 = 0,
    max_per_endpoint: u32 = 5,
    max_waiters: u16 = 0,
    max_wait_bytes: usize = 64 << 20,
    max_wait_ms: u32 = 5000,
};
pub const Options = struct {
    deadline_ms: ?i64 = null,
    retained_bytes: usize = 0,
    /// Borrowed only for this synchronous acquisition; never retained by a lease.
    execution_wait: ?WriteControl = null,
    context: ?*const anyopaque = null,
    cancelled: ?*const fn (*const anyopaque) bool = null,
    pub fn check(self: Options) !void {
        if (self.execution_wait) |control| try control.check();
    }
    /// Caller holds mutex on entry and exit, including errors. Scheduler
    /// callbacks run without the capacity lock; only the physical condition
    /// wait borrows CPU. A generation recorded under the capacity lock avoids
    /// missing a release between scheduler parking and condition registration.
    /// The original absolute polling deadline is never renewed.
    pub fn waitCapacity(self: Options, io: Io, mutex: *Io.Mutex, changed: *Io.Condition, generation: *const u64, wake_ns: i128) !void {
        const Wait = struct {
            fn run(wait_io: Io, lock: *Io.Mutex, condition: *Io.Condition, deadline_ns: i128) !void {
                try condition.waitTimeout(wait_io, lock, .{ .deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = @intCast(deadline_ns) } } });
            }
            fn unlocked(wait_io: Io, lock: *Io.Mutex, condition: *Io.Condition, current_generation: *const u64, observed_generation: u64, deadline_ns: i128) !void {
                lock.lockUncancelable(wait_io);
                defer lock.unlock(wait_io);
                if (current_generation.* != observed_generation) return;
                run(wait_io, lock, condition, deadline_ns) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
            }
        };
        if (self.execution_wait) |control| {
            const observed_generation = generation.*;
            mutex.unlock(io);
            defer mutex.lockUncancelable(io);
            const remaining_ns = @max(@as(i128, 0), wake_ns - Io.Clock.awake.now(io).nanoseconds);
            const poll_ms: u64 = @intCast(@divTrunc(remaining_ns + std.time.ns_per_ms - 1, std.time.ns_per_ms));
            const ms = (try control.timeout(poll_ms)).?;
            const bounded_wake = @min(wake_ns, Io.Clock.awake.now(io).nanoseconds + @as(i128, ms) * std.time.ns_per_ms);
            try control.withWait(Wait.unlocked, .{ io, mutex, changed, generation, observed_generation, bounded_wake });
        } else Wait.run(io, mutex, changed, wake_ns) catch |err| switch (err) {
            error.Timeout => {},
            else => return err,
        };
    }
    pub fn isCancelled(self: Options) bool {
        return if (self.context) |ptr| if (self.cancelled) |call| call(ptr) else false else false;
    }
};
pub const Snapshot = struct { active: usize, waiting: usize, waiting_bytes: usize, endpoints: usize };
pub const Admission = struct {
    alloc: std.mem.Allocator,
    io: Io,
    config: Config,
    mutex: Io.Mutex = .init,
    changed: Io.Condition = .init,
    changed_generation: u64 = 0,
    endpoints: std.StringHashMapUnmanaged(Endpoint) = .empty,
    active: usize = 0,
    waiting: usize = 0,
    waiting_bytes: usize = 0,
    closed: bool = false,
    const Endpoint = struct { active: usize = 0, owners: usize = 0 };
    pub const Lease = struct {
        owner: ?*Admission = null,
        key: []const u8 = "",
        pub fn release(self: *Lease) void {
            const owner = self.owner orelse return;
            self.owner = null;
            owner.mutex.lockUncancelable(owner.io);
            defer owner.mutex.unlock(owner.io);
            owner.active -= 1;
            owner.endpoints.getPtr(self.key).?.active -= 1;
            owner.dropOwner(self.key);
            owner.changed_generation +%= 1;
            owner.changed.broadcast(owner.io);
        }
    };
    pub fn init(alloc: std.mem.Allocator, io: Io, config: Config) Admission {
        return .{ .alloc = alloc, .io = io, .config = config };
    }
    pub fn close(self: *Admission) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.closed = true;
        self.changed_generation +%= 1;
        self.changed.broadcast(self.io);
    }
    pub fn deinit(self: *Admission) void {
        std.debug.assert(self.active == 0 and self.waiting == 0 and self.endpoints.count() == 0);
        self.endpoints.deinit(self.alloc);
    }
    pub fn snapshot(self: *Admission) Snapshot {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{ .active = self.active, .waiting = self.waiting, .waiting_bytes = self.waiting_bytes, .endpoints = self.endpoints.count() };
    }
    fn dropOwner(self: *Admission, key: []const u8) void {
        const entry = self.endpoints.getPtr(key).?;
        entry.owners -= 1;
        if (entry.owners == 0) {
            const removed = self.endpoints.fetchRemove(key).?;
            self.alloc.free(removed.key);
        }
    }
    pub fn acquire(self: *Admission, host: []const u8, port: u16, options: Options) !Lease {
        try options.check();
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.closed) return error.ClientClosed;
        if (options.isCancelled()) return error.Cancelled;
        const now = nowMs(self.io);
        if (options.deadline_ms) |deadline| if (now >= deadline) return error.Timeout;
        if (self.config.max_active == 0) return .{};
        if (self.config.max_per_endpoint == 0) return error.OutboundCapacityUnavailable;
        var buffer: [280]u8 = undefined;
        const key = try std.fmt.bufPrint(&buffer, "{s}:{d}", .{ host, port });
        const endpoint_active = if (self.endpoints.get(key)) |entry| entry.active else 0;
        const must_wait = self.active >= self.config.max_active or endpoint_active >= self.config.max_per_endpoint;
        if (must_wait and (self.config.max_waiters == 0 or self.waiting >= self.config.max_waiters or
            options.retained_bytes > self.config.max_wait_bytes -| self.waiting_bytes)) return error.OutboundCapacityUnavailable;
        const found = try self.endpoints.getOrPut(self.alloc, key);
        if (!found.found_existing) {
            found.key_ptr.* = self.alloc.dupe(u8, key) catch |err| {
                _ = self.endpoints.remove(key);
                return err;
            };
            found.value_ptr.* = .{};
        }
        const owned_key = found.key_ptr.*;
        found.value_ptr.owners += 1;
        errdefer self.dropOwner(owned_key);
        var queued = false;
        defer if (queued) {
            self.waiting -= 1;
            self.waiting_bytes -= options.retained_bytes;
        };
        const deadline = @min(options.deadline_ms orelse std.math.maxInt(i64), now +| self.config.max_wait_ms);
        while (true) {
            try options.check();
            if (self.closed) return error.ClientClosed;
            if (options.isCancelled()) return error.Cancelled;
            const current = nowMs(self.io);
            if (options.deadline_ms) |original| if (current >= original) return error.Timeout;
            if (queued and current >= deadline) return error.Timeout;
            const entry = self.endpoints.getPtr(owned_key).?;
            if (self.active < self.config.max_active and entry.active < self.config.max_per_endpoint) {
                self.active += 1;
                entry.active += 1;
                return .{ .owner = self, .key = owned_key };
            }
            if (!queued) {
                if (self.config.max_wait_ms == 0) return error.OutboundCapacityUnavailable;
                queued = true;
                self.waiting += 1;
                self.waiting_bytes += options.retained_bytes;
            }
            // Poll borrowed cancellation; release/close wakes capacity waits immediately.
            const wake_ns = @as(i128, @min(deadline, current +| 25)) * std.time.ns_per_ms;
            try options.waitCapacity(self.io, &self.mutex, &self.changed, &self.changed_generation, wake_ns);
        }
    }
};
fn nowMs(io: Io) i64 {
    return @intCast(@divFloor(Io.Clock.awake.now(io).nanoseconds, std.time.ns_per_ms));
}

test "outbound admission shares endpoint and global capacity and reclaims keys" {
    var owner = Admission.init(std.testing.allocator, std.testing.io, .{ .max_active = 2, .max_per_endpoint = 1 });
    defer owner.deinit();
    var a = try owner.acquire("a", 80, .{});
    defer a.release();
    try std.testing.expectError(error.OutboundCapacityUnavailable, owner.acquire("a", 80, .{}));
    var b = try owner.acquire("b", 80, .{});
    defer b.release();
    try std.testing.expectError(error.OutboundCapacityUnavailable, owner.acquire("c", 80, .{}));
    a.release();
    b.release();
    try std.testing.expectEqual(@as(usize, 0), owner.snapshot().endpoints);
    // A second caller/client borrowing this owner gets the same limits.
    var c = try owner.acquire("a", 80, .{});
    c.release();
}

test "outbound admission preserves deadline and rolls back expired wait storage" {
    var owner = Admission.init(std.testing.allocator, std.testing.io, .{ .max_active = 1, .max_waiters = 1, .max_wait_ms = 1000 });
    defer owner.deinit();
    var held = try owner.acquire("a", 80, .{});
    defer held.release();
    const start = nowMs(std.testing.io);
    try std.testing.expectError(error.Timeout, owner.acquire("b", 80, .{ .deadline_ms = start + 10, .retained_bytes = 9 }));
    const snapshot = owner.snapshot();
    try std.testing.expectEqual(@as(usize, 0), snapshot.waiting);
    try std.testing.expectEqual(@as(usize, 0), snapshot.waiting_bytes);
    try std.testing.expectEqual(@as(usize, 1), snapshot.endpoints);
    try std.testing.expect(nowMs(std.testing.io) - start < 1000);
}

const WaitingTest = struct {
    owner: *Admission,
    cancel: std.atomic.Value(bool) = .init(false),
    outcome: std.atomic.Value(u8) = .init(0),
    fn cancelled(raw: *const anyopaque) bool {
        const self: *const WaitingTest = @ptrCast(@alignCast(raw));
        return self.cancel.load(.acquire);
    }
    fn run(self: *WaitingTest) void {
        var lease = self.owner.acquire("waiting", 80, .{ .context = self, .cancelled = cancelled, .retained_bytes = 8 }) catch |err| {
            self.outcome.store(switch (err) {
                error.Cancelled => 1,
                error.ClientClosed => 2,
                else => 3,
            }, .release);
            return;
        };
        lease.release();
        self.outcome.store(4, .release);
    }
};
fn waitForQueue(owner: *Admission) !void {
    for (0..1000) |_| {
        if (owner.snapshot().waiting != 0) return;
        try Io.Clock.Duration.sleep(.{ .clock = .awake, .raw = .fromMilliseconds(1) }, owner.io);
    }
    return error.TestExpectedWaiting;
}

test "outbound admission bounds queued count bytes cancellation and close" {
    for ([_]u8{ 1, 2, 4 }) |expected| {
        var owner = Admission.init(std.testing.allocator, std.testing.io, .{ .max_active = 1, .max_waiters = 1, .max_wait_bytes = 8, .max_wait_ms = 5000 });
        defer owner.deinit();
        var held = try owner.acquire("held", 80, .{});
        defer held.release();
        try std.testing.expectError(error.OutboundCapacityUnavailable, owner.acquire("too-large", 80, .{ .retained_bytes = 9 }));
        var task: WaitingTest = .{ .owner = &owner };
        var running = try owner.io.concurrent(WaitingTest.run, .{&task});
        defer running.await(owner.io);
        defer owner.close();
        try waitForQueue(&owner);
        try std.testing.expectError(error.OutboundCapacityUnavailable, owner.acquire("overflow", 80, .{}));
        try std.testing.expectEqual(@as(usize, 8), owner.snapshot().waiting_bytes);
        switch (expected) {
            1 => task.cancel.store(true, .release),
            2 => owner.close(),
            4 => held.release(),
            else => unreachable,
        }
        running.await(owner.io);
        try std.testing.expectEqual(expected, task.outcome.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), owner.snapshot().waiting_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.snapshot().waiting);
    }
}

test "outbound admission capacity waits loan execution without holding the capacity lock" {
    const Case = enum { success, begin_denied, end_denied, canceled, closed };
    const State = struct {
        owner: *Admission,
        held: *Admission.Lease,
        case: Case,
        parked: bool = false,
        canceled: bool = false,
        begins: usize = 0,
        ends: usize = 0,
        fn check(raw: ?*anyopaque) WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled) return error.Canceled;
        }
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.owner.mutex.tryLock());
            self.owner.mutex.unlock(self.owner.io);
            self.begins += 1;
            try std.testing.expect(!self.parked);
            try std.testing.expectEqual(@as(usize, 1), self.owner.snapshot().waiting);
            try std.testing.expectEqual(@as(usize, 7), self.owner.snapshot().waiting_bytes);
            if (self.case == .begin_denied) return error.SchedulerClosed;
            self.parked = true;
            if (self.case == .closed) self.owner.close() else self.held.release();
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.owner.mutex.tryLock());
            self.owner.mutex.unlock(self.owner.io);
            try std.testing.expect(self.parked);
            self.parked = false;
            self.ends += 1;
            if (self.case == .end_denied) return error.DeadlineExceeded;
            if (self.case == .canceled) self.canceled = true;
        }
    };
    for (std.enums.values(Case)) |case| {
        var owner = Admission.init(std.testing.allocator, std.testing.io, .{ .max_active = 1, .max_per_endpoint = 1, .max_waiters = 1, .max_wait_ms = 1000 });
        defer owner.deinit();
        var held = try owner.acquire("a", 80, .{});
        defer held.release();
        var state: State = .{ .owner = &owner, .held = &held, .case = case };
        const result = owner.acquire("a", 80, .{ .retained_bytes = 7, .execution_wait = .{ .ptr = &state, .check_fn = State.check, .wait_begin_fn = State.begin, .wait_end_fn = State.end } });
        switch (case) {
            .success => {
                var acquired = try result;
                try std.testing.expectEqual(@as(usize, 1), owner.snapshot().active);
                acquired.release();
            },
            .begin_denied => try std.testing.expectError(error.SchedulerClosed, result),
            .end_denied => try std.testing.expectError(error.DeadlineExceeded, result),
            .canceled => try std.testing.expectError(error.Canceled, result),
            .closed => try std.testing.expectError(error.ClientClosed, result),
        }
        try std.testing.expectEqual(@as(usize, 1), state.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_denied) 0 else 1), state.ends);
        try std.testing.expect(!state.parked);
        try std.testing.expectEqual(@as(usize, 0), owner.snapshot().waiting);
        try std.testing.expectEqual(@as(usize, 0), owner.snapshot().waiting_bytes);
        held.release();
        try std.testing.expectEqual(@as(usize, 0), owner.snapshot().active);
        try std.testing.expectEqual(@as(usize, 0), owner.snapshot().endpoints);
    }
}

test "outbound admission capacity release during parking cannot miss the wakeup" {
    const State = struct {
        io: Io,
        mutex: *Io.Mutex,
        changed: *Io.Condition,
        generation: *u64,
        ends: usize = 0,
        fn begin(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.mutex.tryLock());
            defer self.mutex.unlock(self.io);
            self.generation.* +%= 1;
            // There are no registered waiters yet. The generation, rather
            // than a future condition signal, must preserve this release.
            self.changed.broadcast(self.io);
        }
        fn end(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expect(self.mutex.tryLock());
            self.mutex.unlock(self.io);
            self.ends += 1;
        }
    };
    const io = std.testing.io;
    var mutex: Io.Mutex = .init;
    var changed: Io.Condition = .init;
    var generation: u64 = 0;
    var state: State = .{ .io = io, .mutex = &mutex, .changed = &changed, .generation = &generation };
    const options: Options = .{ .execution_wait = .{ .ptr = &state, .wait_begin_fn = State.begin, .wait_end_fn = State.end } };
    const before = Io.Clock.awake.now(io).nanoseconds;
    mutex.lockUncancelable(io);
    defer mutex.unlock(io);
    try options.waitCapacity(io, &mutex, &changed, &generation, before + 5 * std.time.ns_per_s);
    try std.testing.expect(Io.Clock.awake.now(io).nanoseconds - before < std.time.ns_per_s);
    try std.testing.expectEqual(@as(usize, 1), state.ends);
    try std.testing.expectEqual(@as(u64, 1), generation);
}
