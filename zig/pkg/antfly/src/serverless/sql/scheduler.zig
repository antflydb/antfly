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

//! Native delivery is bounded and independent of coordinator metadata turns.
//! Durable dispatch intent makes queue overflow and pod loss replayable.
const std = @import("std");
const runtime = @import("runtime.zig");
const plan = @import("plan.zig");
const state = @import("state.zig");
const Context = @import("antfly_local_sources").serverless_query_lake_read_context.Context;

pub const Turns = struct {
    mutex: std.atomic.Mutex = .unlocked,
    scheduler: ?*Scheduler = null,
    active: [256]?state.Query = @splat(null),
    pub fn port(self: *Turns) runtime.Coordination {
        return .{ .ptr = self, .enter = enter, .leave = leave, .delivery_pending = pending, .delivery_ready = ready, .has_progress = progress, .retry = retry };
    }
    fn pending(raw: *anyopaque, query: state.Query) bool {
        const self: *Turns = @ptrCast(@alignCast(raw));
        return if (self.scheduler) |scheduler| scheduler.hasPending(query) else false;
    }
    fn progress(raw: *anyopaque, query: state.Query) bool {
        const self: *Turns = @ptrCast(@alignCast(raw));
        return if (self.scheduler) |scheduler| scheduler.hasProgress(query) else true;
    }
    fn ready(raw: *anyopaque, query: state.Query) bool {
        const self: *Turns = @ptrCast(@alignCast(raw));
        return if (self.scheduler) |scheduler| scheduler.consumeProgress(query) else true;
    }
    fn retry(raw: *anyopaque, query: state.Query) void {
        const self: *Turns = @ptrCast(@alignCast(raw));
        if (self.scheduler) |scheduler| scheduler.retry(query);
    }
    fn enter(raw: *anyopaque, query: state.Query) bool {
        const self: *Turns = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        var free: ?usize = null;
        for (self.active, 0..) |entry, i| {
            if (entry) |value| {
                if (std.meta.eql(value, query)) return false;
            } else free = i;
        }
        self.active[free orelse return false] = query;
        return true;
    }
    fn leave(raw: *anyopaque, query: state.Query) void {
        const self: *Turns = @ptrCast(@alignCast(raw));
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        for (&self.active) |*entry| if (entry.*) |value| {
            if (std.meta.eql(value, query)) {
                entry.* = null;
                return;
            }
        };
        unreachable;
    }
};

pub const Scheduler = struct {
    const Node = struct { ticket: ?plan.Ticket = null, next: ?u16 = null, slot: u16 = 0, failures: u8 = 0, not_before_ns: u64 = 0 };
    const Slot = struct { query: ?state.Query = null, tenant: state.Digest = undefined, head: ?u16 = null, tail: ?u16 = null, active: u16 = 0, dirty: bool = false };
    io: std.Io,
    clock: runtime.Clock = .{},
    delivery: runtime.Dispatcher,
    notification: ?runtime.Notification = null,
    mutex: std.Io.Mutex = .init,
    available: std.Io.Condition = .init,
    nodes: [1024]Node = @splat(.{}),
    slots: [256]Slot = @splat(.{}),
    cursor: usize = 0,
    group: std.Io.Group = .init,
    started: bool = false,
    stopping: std.atomic.Value(bool) = .init(false),

    fn hasPending(self: *Scheduler, query: state.Query) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.slots) |slot| if (slot.query) |value| {
            if (std.meta.eql(value, query)) return true;
        };
        return false;
    }
    fn hasProgress(self: *Scheduler, query: state.Query) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.slots) |slot| if (slot.query) |value| {
            if (std.meta.eql(value, query)) return slot.dirty;
        };
        return true;
    }
    fn consumeProgress(self: *Scheduler, query: state.Query) bool {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (&self.slots) |*slot| if (slot.query) |value| {
            if (std.meta.eql(value, query)) {
                const ready = slot.dirty;
                slot.dirty = false;
                return ready;
            }
        };
        return true;
    }
    fn retry(self: *Scheduler, query: state.Query) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (&self.slots) |*slot| if (slot.query) |value| {
            if (std.meta.eql(value, query)) slot.dirty = true;
        };
    }
    pub fn dispatcher(self: *Scheduler) runtime.Dispatcher {
        return .{ .ptr = self, .dispatch = enqueue };
    }
    pub fn start(self: *Scheduler, concurrency: u16) !void {
        if (self.started) return;
        if (concurrency == 0 or concurrency > 256) return error.InvalidSqlDeployment;
        self.stopping.store(false, .release);
        errdefer {
            self.stopping.store(true, .release);
            self.group.cancel(self.io);
        }
        for (0..concurrency) |_| try self.group.concurrent(self.io, run, .{self});
        self.started = true;
    }
    pub fn stop(self: *Scheduler) void {
        self.stopping.store(true, .release);
        if (self.started) self.group.cancel(self.io);
        self.started = false;
        // All in-flight deliveries have joined; only durable intent survives.
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.nodes = @splat(.{});
        self.slots = @splat(.{});
    }
    fn enqueue(raw: *anyopaque, tickets: []const plan.Ticket, context: Context) !void {
        const self: *Scheduler = @ptrCast(@alignCast(raw));
        try context.ensureActive();
        try self.mutex.lock(self.io);
        defer self.mutex.unlock(self.io);
        if (self.stopping.load(.acquire)) return error.Canceled;
        self.retireExpired();
        var overflow = false;
        for (tickets) |ticket| {
            if (ticket.query.expires_ms <= self.clock.now()) continue;
            var slot_id: ?usize = null;
            var free_slot: ?usize = null;
            for (self.slots, 0..) |slot, i| {
                if (slot.query) |query| {
                    if (std.meta.eql(query, ticket.query) and std.meta.eql(slot.tenant, ticket.tenant)) {
                        slot_id = i;
                        break;
                    }
                } else free_slot = i;
            }
            var duplicate = false;
            var free_node: ?usize = null;
            for (self.nodes, 0..) |node, i| {
                if (node.ticket) |queued| {
                    if (std.meta.eql(queued, ticket)) {
                        duplicate = true;
                        break;
                    }
                } else free_node = i;
            }
            if (duplicate) continue;
            // Do not block a coordinator on task capacity. Its persisted
            // pending set replays tickets that do not fit this local queue.
            const id = free_node orelse {
                overflow = true;
                continue;
            };
            const slot_index = slot_id orelse free_slot orelse {
                overflow = true;
                continue;
            };
            const slot = &self.slots[slot_index];
            if (slot.query == null) slot.* = .{ .query = ticket.query, .tenant = ticket.tenant };
            self.nodes[id] = .{ .ticket = ticket, .slot = @intCast(slot_index) };
            if (slot.tail) |tail| self.nodes[tail].next = @intCast(id) else slot.head = @intCast(id);
            slot.tail = @intCast(id);
            self.available.signal(self.io);
        }
        if (overflow) return error.SqlWorkerUnavailable;
    }
    // Caller holds mutex. One ticket per query in round-robin order prevents
    // a large query's concurrency window from monopolizing queued delivery.
    fn retireExpired(self: *Scheduler) void {
        const now = self.clock.now();
        for (&self.slots) |*slot| if (slot.query) |query| {
            if (query.expires_ms > now) continue;
            var pending = slot.head;
            while (pending) |id| {
                pending = self.nodes[id].next;
                self.nodes[id] = .{};
            }
            slot.head = null;
            slot.tail = null;
            // In-flight nodes remain owned by their worker until it joins.
            if (slot.active == 0) slot.* = .{};
        };
    }
    fn take(self: *Scheduler) ?u16 {
        self.retireExpired();
        const now = @import("antfly_platform").time.monotonicNs();
        for (0..self.slots.len) |offset| {
            const i = (self.cursor + offset) % self.slots.len;
            const slot = &self.slots[i];
            const id = slot.head orelse continue;
            if (self.nodes[id].not_before_ns > now) continue;
            slot.head = self.nodes[id].next;
            if (slot.head == null) slot.tail = null;
            slot.active += 1;
            self.cursor = (i + 1) % self.slots.len;
            return id;
        }
        return null;
    }
    fn retryDelay(self: *Scheduler) ?u64 {
        var earliest: ?u64 = null;
        for (self.slots) |slot| if (slot.head) |id| {
            const lifetime = slot.query.?.expires_ms -| self.clock.now();
            const expiry = @import("antfly_platform").time.monotonicNs() +| lifetime *| std.time.ns_per_ms;
            const due = @min(self.nodes[id].not_before_ns, expiry);
            earliest = if (earliest) |value| @min(value, due) else due;
        };
        return if (earliest) |value| value -| @import("antfly_platform").time.monotonicNs() else null;
    }
    fn deferDelivery(self: *Scheduler, id: u16) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const node = &self.nodes[id];
        node.failures += 1;
        const delay_ms = @min(@as(u64, 5000), @as(u64, 250) << @as(u6, @intCast(node.failures - 1)));
        const jitter = std.hash.Wyhash.hash(node.failures, &node.ticket.?.query.id) % @max(@as(u64, 1), delay_ms / 4);
        node.not_before_ns = @import("antfly_platform").time.monotonicNs() +| (delay_ms + jitter) * std.time.ns_per_ms;
        node.next = null;
        const slot = &self.slots[node.slot];
        slot.active -= 1;
        if (slot.tail) |tail| self.nodes[tail].next = id else slot.head = id;
        slot.tail = id;
        self.available.broadcast(self.io);
    }
    fn finish(self: *Scheduler, id: u16) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        const slot = &self.slots[self.nodes[id].slot];
        slot.active -= 1;
        slot.dirty = true;
        if (slot.active == 0 and slot.head == null) slot.* = .{};
        self.nodes[id] = .{};
    }
    fn run(self: *Scheduler) void {
        const context: Context = .{ .io = self.io, .cancellation = .{ .ptr = self, .is_cancelled_fn = struct {
            fn canceled(raw: *const anyopaque) bool {
                const scheduler: *const Scheduler = @ptrCast(@alignCast(raw));
                return scheduler.stopping.load(.acquire);
            }
        }.canceled } };
        while (!self.stopping.load(.acquire)) {
            self.mutex.lock(self.io) catch return;
            const id = while (true) {
                if (self.take()) |value| break value;
                const timeout: std.Io.Timeout = if (self.retryDelay()) |delay| .{ .duration = .{ .raw = .fromNanoseconds(@intCast(@min(delay, 6 * std.time.ns_per_s))), .clock = .awake } } else .none;
                self.available.waitTimeout(self.io, &self.mutex, timeout) catch |err| switch (err) {
                    error.Timeout => continue,
                    error.Canceled => {
                        self.mutex.unlock(self.io);
                        return;
                    },
                };
                if (self.stopping.load(.acquire)) {
                    self.mutex.unlock(self.io);
                    return;
                }
            };
            const ticket = self.nodes[id].ticket.?;
            self.mutex.unlock(self.io);
            // Query expiry bounds even a slow/unreachable worker RPC.
            var delivery_context = context;
            const remaining: u64 = @min(@as(u64, 55_000), ticket.query.expires_ms -| self.clock.now());
            delivery_context.deadline_ns = @import("antfly_platform").time.monotonicNs() +| remaining * std.time.ns_per_ms;
            if (remaining == 0) {
                self.finish(id);
                continue;
            }
            // No request-owned context or pointer escapes into delivery.
            self.delivery.dispatch(self.delivery.ptr, &.{ticket}, delivery_context) catch |err| {
                if (!self.stopping.load(.acquire) and self.clock.now() < ticket.query.expires_ms and self.nodes[id].failures < 7) {
                    // Retain identity through backpressure; immediate completion
                    // notifications would turn 503 responses into a hot loop.
                    self.deferDelivery(id);
                    continue;
                }
                if (!self.stopping.load(.acquire)) std.log.warn("distributed SQL delivery deferred: {s}", .{@errorName(err)});
            };
            self.finish(id);
            if (self.notification) |notification| notification.notify(notification.ptr, ticket.tenant, ticket.query);
        }
    }
};

fn testClock(_: ?*anyopaque) u64 {
    return 1000;
}

test "distributed SQL scheduler fairly rotates bounded queues and deduplicates in-flight tickets" {
    var scheduler: Scheduler = .{ .io = std.testing.io, .clock = .{ .read = testClock }, .delivery = undefined };
    defer scheduler.stop();
    const a: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 100000 }, .stage = 0, .task = 0 };
    var a2 = a;
    a2.task = 1;
    var b = a;
    b.query.id = @splat('c');
    var c = b;
    c.query.id = @splat('d');
    const port = scheduler.dispatcher();
    try port.dispatch(port.ptr, &.{ a, a2, b, c }, .{});
    const first = scheduler.take().?;
    const second = scheduler.take().?;
    const third = scheduler.take().?;
    // Query slots are allocated from the end; start order need not be FIFO,
    // but all three queries get a turn before A's second ticket.
    try std.testing.expect(!std.meta.eql(scheduler.nodes[first].ticket.?.query, scheduler.nodes[second].ticket.?.query));
    try std.testing.expect(!std.meta.eql(scheduler.nodes[first].ticket.?.query, scheduler.nodes[third].ticket.?.query));
    try std.testing.expect(!std.meta.eql(scheduler.nodes[second].ticket.?.query, scheduler.nodes[third].ticket.?.query));
    try port.dispatch(port.ptr, &.{ a, a2, b, c }, .{});
    const fourth = scheduler.take().?;
    try std.testing.expect(scheduler.take() == null);
    try std.testing.expect(!scheduler.consumeProgress(a.query));
    const ids = [_]u16{ first, second, third, fourth };
    const completed = for (ids) |id| {
        if (std.meta.eql(scheduler.nodes[id].ticket.?.query, a.query)) break id;
    } else unreachable;
    scheduler.finish(completed);
    // One completion exposes an idle window even while another RPC for this
    // query is still running. A failed coordinator turn preserves that signal.
    try std.testing.expect(scheduler.hasPending(a.query));
    try std.testing.expect(scheduler.consumeProgress(a.query));
    try std.testing.expect(!scheduler.consumeProgress(a.query));
    scheduler.retry(a.query);
    try std.testing.expect(scheduler.consumeProgress(a.query));
    for (ids) |id| if (id != completed) scheduler.finish(id);
    // Capacity is fixed even with more than one query's maximum window.
    for (0..1100) |i| {
        var ticket = a;
        ticket.task = @intCast(i);
        if (i < 1024) try port.dispatch(port.ptr, &.{ticket}, .{}) else try std.testing.expectError(error.SqlWorkerUnavailable, port.dispatch(port.ptr, &.{ticket}, .{}));
    }
    var count: usize = 0;
    while (scheduler.take()) |id| {
        count += 1;
        scheduler.finish(id);
    }
    try std.testing.expectEqual(@as(usize, 1024), count);
}

test "distributed SQL coordinator turns exclude overlapping foreground and recovery work" {
    var turns: Turns = .{};
    const port = turns.port();
    var query: state.Query = .{ .id = @splat('a'), .expires_ms = 100000 };
    try std.testing.expect(port.enter(port.ptr, query));
    try std.testing.expect(!port.enter(port.ptr, query));
    const a = query;
    query.id = @splat('b');
    try std.testing.expect(port.enter(port.ptr, query));
    port.leave(port.ptr, a);
    try std.testing.expect(port.enter(port.ptr, a));
    port.leave(port.ptr, a);
    port.leave(port.ptr, query);
}

test "distributed SQL delivery backpressure retries independently with bounded delay" {
    const Delivery = struct {
        calls: std.atomic.Value(u32) = .init(0),
        accepted: std.Io.Event = .unset,
        fn run(raw: *anyopaque, _: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.calls.fetchAdd(1, .monotonic) == 0) return error.SqlWorkerUnavailable;
            self.accepted.set(context.io.?);
        }
    };
    var delivery: Delivery = .{};
    var scheduler: Scheduler = .{ .io = std.testing.io, .clock = .{ .read = testClock }, .delivery = .{ .ptr = &delivery, .dispatch = Delivery.run } };
    const ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 100000 }, .stage = 0, .task = 0 };
    const port = scheduler.dispatcher();
    try port.dispatch(port.ptr, &.{ticket}, .{});
    const id = scheduler.take().?;
    scheduler.deferDelivery(id);
    try std.testing.expect(scheduler.take() == null);
    try port.dispatch(port.ptr, &.{ticket}, .{});
    // Another query remains runnable while this one is backing off.
    var other = ticket;
    other.query.id = @splat('c');
    try port.dispatch(port.ptr, &.{other}, .{});
    const other_id = scheduler.take().?;
    try std.testing.expect(std.meta.eql(scheduler.nodes[other_id].ticket.?, other));
    scheduler.finish(other_id);
    try std.testing.expect(scheduler.take() == null);
    scheduler.stop();
    // Fresh startup cancels/discards only hints, then supports timed retries.
    try scheduler.start(1);
    defer scheduler.stop();
    try port.dispatch(port.ptr, &.{ticket}, .{});
    try delivery.accepted.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(u32, 2), delivery.calls.load(.acquire));
}

test "distributed SQL scheduler retires expired queues and caps RPC lifetime" {
    const Clock = struct {
        now: u64 = 1000,
        fn read(raw: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.now;
        }
    };
    const Delivery = struct {
        calls: std.atomic.Value(u32) = .init(0),
        received: std.Io.Event = .unset,
        fn run(raw: *anyopaque, _: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const remaining = context.deadline_ns.? -| @import("antfly_platform").time.monotonicNs();
            std.debug.assert(remaining <= 100 * std.time.ns_per_ms);
            _ = self.calls.fetchAdd(1, .monotonic);
            self.received.set(context.io.?);
        }
    };
    var clock: Clock = .{};
    var delivery: Delivery = .{};
    var scheduler: Scheduler = .{ .io = std.testing.io, .clock = .{ .ptr = &clock, .read = Clock.read }, .delivery = .{ .ptr = &delivery, .dispatch = Delivery.run } };
    var ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 2000 }, .stage = 0, .task = 0 };
    const port = scheduler.dispatcher();
    for (0..1024) |i| {
        ticket.task = @intCast(i);
        try port.dispatch(port.ptr, &.{ticket}, .{});
    }
    // Expiry alone frees a saturated queue before workers or retries run.
    clock.now = 3000;
    ticket.query.id = @splat('c');
    ticket.query.expires_ms = 3100;
    ticket.task = 0;
    try port.dispatch(port.ptr, &.{ticket}, .{});
    try scheduler.start(1);
    defer scheduler.stop();
    try delivery.received.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(u32, 1), delivery.calls.load(.acquire));
}

test "distributed SQL expired delivery failure is retired without retry" {
    const Fixture = struct {
        now: u64 = 1000,
        calls: std.atomic.Value(u32) = .init(0),
        expired: std.Io.Event = .unset,
        fn read(raw: ?*anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.now;
        }
        fn retired(raw: *anyopaque, _: state.Digest, _: state.Query) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.expired.set(std.testing.io);
        }
        fn run(raw: *anyopaque, _: []const plan.Ticket, context: Context) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            _ = context;
            self.now = 2000;
            return error.DeadlineExceeded;
        }
    };
    var fixture: Fixture = .{};
    var scheduler: Scheduler = .{ .io = std.testing.io, .clock = .{ .ptr = &fixture, .read = Fixture.read }, .delivery = .{ .ptr = &fixture, .dispatch = Fixture.run } };
    scheduler.notification = .{ .ptr = &fixture, .notify = Fixture.retired };
    const ticket: plan.Ticket = .{ .tenant = @splat('a'), .query = .{ .id = @splat('b'), .expires_ms = 1500 }, .stage = 0, .task = 0 };
    const port = scheduler.dispatcher();
    try port.dispatch(port.ptr, &.{ticket}, .{});
    try scheduler.start(1);
    try fixture.expired.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    scheduler.stop();
    try std.testing.expectEqual(@as(u32, 1), fixture.calls.load(.acquire));
}
