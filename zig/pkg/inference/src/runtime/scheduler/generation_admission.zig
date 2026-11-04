// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Queue before reserving transient generation memory. Shared slots permit
//! cooperative execution; exclusive owners fence cached/graph/native routes.
const std = @import("std");
const platform = @import("antfly_platform");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;

pub const Latency = struct {
    pub const bounds_ns = [_]u64{ 1_000_000, 10_000_000, 50_000_000, 100_000_000, 500_000_000, 1_000_000_000, 5_000_000_000, 30_000_000_000, 120_000_000_000, 300_000_000_000 };
    count: u64 = 0,
    total_ns: u64 = 0,
    max_ns: u64 = 0,
    buckets: [bounds_ns.len]u64 = @splat(0),

    pub fn record(self: *Latency, ns: u64) void {
        self.count +|= 1;
        self.total_ns +|= ns;
        self.max_ns = @max(self.max_ns, ns);
        for (bounds_ns, 0..) |bound, i| if (ns <= bound) {
            self.buckets[i] +|= 1;
        };
    }

    pub fn add(self: *Latency, other: Latency) void {
        self.count +|= other.count;
        self.total_ns +|= other.total_ns;
        self.max_ns = @max(self.max_ns, other.max_ns);
        for (&self.buckets, other.buckets) |*dst, src| dst.* +|= src;
    }

    pub fn render(self: Latency, writer: *std.Io.Writer, name: []const u8, help: []const u8) !void {
        try writer.print("# HELP {s} {s}\n# TYPE {s} histogram\n", .{ name, help, name });
        for (bounds_ns, self.buckets) |bound, count| {
            try writer.print("{s}_bucket{{le=\"{d}\"}} {d}\n", .{ name, @as(f64, @floatFromInt(bound)) / 1e9, count });
        }
        try writer.print("{s}_bucket{{le=\"+Inf\"}} {d}\n{s}_count {d}\n{s}_sum {d}\n", .{ name, self.count, name, self.count, name, @as(f64, @floatFromInt(self.total_ns)) / 1e9 });
    }
};

pub const Stats = struct {
    active: usize = 0,
    waiting: usize = 0,
    started: u64 = 0,
    rejected: u64 = 0,
    cancelled: u64 = 0,
    queue_wait: Latency = .{},
    first_content: Latency = .{},
    prefill: Latency = .{},
    decode: Latency = .{},

    pub fn add(self: *Stats, other: Stats) void {
        self.active += other.active;
        self.waiting += other.waiting;
        self.started +|= other.started;
        self.rejected +|= other.rejected;
        self.cancelled +|= other.cancelled;
        self.queue_wait.add(other.queue_wait);
        self.first_content.add(other.first_content);
        self.prefill.add(other.prefill);
        self.decode.add(other.decode);
    }
};

pub const Lane = struct {
    const Waiter = struct {
        next: ?*Waiter = null,
        ready: std.Io.Event = .unset,
        io: ?std.Io = null,
    };
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,
    exclusive: bool = false,
    stats: Stats = .{},

    pub const Lease = struct {
        lane: *Lane,
        exclusive: bool,
        pub fn release(self: Lease) void {
            self.lane.lock();
            defer self.lane.mutex.unlock();
            std.debug.assert(self.lane.stats.active > 0);
            if (self.exclusive) {
                std.debug.assert(self.lane.exclusive and self.lane.stats.active == 1);
                self.lane.exclusive = false;
            }
            self.lane.stats.active -= 1;
            self.lane.wake();
        }
    };

    fn lock(self: *Lane) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn wake(self: *Lane) void {
        if (self.head) |head| if (head.io) |io| head.ready.set(io);
    }

    fn remove(self: *Lane, target: *Waiter) void {
        var previous: ?*Waiter = null;
        var current = self.head;
        while (current) |item| : (current = item.next) {
            if (item == target) {
                if (previous) |p| p.next = item.next else self.head = item.next;
                if (self.tail == item) self.tail = previous;
                self.stats.waiting -= 1;
                return;
            }
            previous = item;
        }
        unreachable;
    }

    pub fn acquire(self: *Lane, control: Control, exclusive: bool, active_limit: usize, queue_limit: usize) !Lease {
        try control.check();
        const started = platform.time.monotonicNs();
        var waiter = Waiter{ .io = control.io };
        self.lock();
        if (self.stats.waiting >= @max(queue_limit, 1)) {
            self.stats.rejected +|= 1;
            self.mutex.unlock();
            return error.GenerationQueueFull;
        }
        if (self.tail) |tail| tail.next = &waiter else self.head = &waiter;
        self.tail = &waiter;
        self.stats.waiting += 1;
        self.mutex.unlock();
        errdefer {
            self.lock();
            self.remove(&waiter);
            self.stats.cancelled +|= 1;
            self.wake();
            self.mutex.unlock();
        }
        while (true) {
            try control.check();
            self.lock();
            const available = !self.exclusive and self.stats.active < @max(active_limit, 1) and (!exclusive or self.stats.active == 0);
            if (self.head == &waiter and available) {
                self.remove(&waiter);
                self.stats.active += 1;
                self.exclusive = exclusive;
                self.stats.started +|= 1;
                self.stats.queue_wait.record(platform.time.monotonicNs() -| started);
                self.wake();
                self.mutex.unlock();
                return .{ .lane = self, .exclusive = exclusive };
            }
            waiter.ready.reset();
            self.mutex.unlock();
            if (control.io) |io| {
                waiter.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromMilliseconds(10), .clock = .awake } }) catch |err| switch (err) {
                    error.Timeout => {},
                    else => return err,
                };
            } else platform.time.yieldNow();
        }
    }

    pub fn snapshot(self: *Lane) Stats {
        self.lock();
        defer self.mutex.unlock();
        return self.stats;
    }

    pub fn notePipeline(self: *Lane, prefill_ms: u64, decode_ms: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.stats.prefill.record(prefill_ms *| std.time.ns_per_ms);
        self.stats.decode.record(decode_ms *| std.time.ns_per_ms);
    }

    pub fn noteFirstContent(self: *Lane, request_started_ns: u64) void {
        self.lock();
        defer self.mutex.unlock();
        self.stats.first_content.record(platform.time.monotonicNs() -| request_started_ns);
    }
};

test "generation admission shares bounded slots and fences exclusive owners" {
    var lane = Lane{};
    const first = try lane.acquire(.{}, false, 2, 4);
    const second = try lane.acquire(.{}, false, 2, 4);
    const State = struct {
        first: Lane.Lease,
        second: Lane.Lease,
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) self.first.release();
            if (self.checks == 3) self.second.release();
        }
    };
    var state = State{ .first = first, .second = second };
    const exclusive = try lane.acquire(.{ .ptr = &state, .check_fn = State.check }, true, 2, 4);
    try std.testing.expectEqual(@as(usize, 3), state.checks);
    try std.testing.expectEqual(@as(usize, 1), lane.snapshot().active);
    try std.testing.expect(lane.exclusive);
    try std.testing.expectError(error.Timeout, lane.acquire(.{ .deadline_ns = 0 }, false, 2, 4));
    exclusive.release();
    const next = try lane.acquire(.{}, false, 2, 4);
    next.release();
    try std.testing.expectEqual(@as(usize, 0), lane.snapshot().active);
    try std.testing.expectEqual(@as(usize, 0), lane.snapshot().waiting);
}

test "generation admission bounds waiting work and removes cancelled queue heads" {
    var lane = Lane{};
    const owner = try lane.acquire(.{}, true, 1, 1);
    defer owner.release();
    const State = struct {
        lane: *Lane,
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) {
                try std.testing.expectError(error.GenerationQueueFull, self.lane.acquire(.{}, false, 2, 1));
                return error.Cancelled;
            }
        }
    };
    var state = State{ .lane = &lane };
    try std.testing.expectError(error.Cancelled, lane.acquire(.{ .ptr = &state, .check_fn = State.check }, false, 2, 1));
    const stats = lane.snapshot();
    try std.testing.expectEqual(@as(usize, 0), stats.waiting);
    try std.testing.expectEqual(@as(u64, 1), stats.rejected);
    try std.testing.expectEqual(@as(u64, 1), stats.cancelled);
    try std.testing.expect(lane.head == null and lane.tail == null);
}

test "generation admission latency histograms retain cumulative buckets when aggregated" {
    var first: Latency = .{};
    first.record(2 * std.time.ns_per_ms);
    var second: Latency = .{};
    second.record(40 * std.time.ns_per_ms);
    first.add(second);
    try std.testing.expectEqual(@as(u64, 2), first.count);
    try std.testing.expectEqual(@as(u64, 42 * std.time.ns_per_ms), first.total_ns);
    try std.testing.expectEqual(@as(u64, 0), first.buckets[0]);
    try std.testing.expectEqual(@as(u64, 1), first.buckets[1]);
    try std.testing.expectEqual(@as(u64, 2), first.buckets[2]);
    var writer: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer writer.deinit();
    try first.render(&writer.writer, "generation_queue_seconds", "Queue wait");
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "# TYPE generation_queue_seconds histogram") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "generation_queue_seconds_bucket{le=\"+Inf\"} 2") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.written(), "generation_queue_seconds_count 2") != null);
}
