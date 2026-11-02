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

//! Runnable permits are separate from operation admission and retained memory.
//! Entries belong to callers, remain stable until finish, and allocate nothing.
//! Blocking relinquishes execution slots without releasing operation ownership.
const std = @import("std");
const admission = @import("bounded_admission.zig");

pub const Class = enum(u3) { query, write, inference, background, control };
pub const State = enum { fresh, queued, running, resource_blocked, output_blocked, stopped, finished };
pub const Config = struct {
    slots: usize = 8,
    control_slots: usize = 1,
    weights: [4]u16 = .{ 4, 2, 1, 1 },
    max_operations: usize = 128,
    max_bytes: u64 = 8 << 20,
    control_bytes: u64 = 64 << 10,
    max_wait_ms: u32 = 250,
    max_backfills: u16 = 8,
    quantum_ms: u32 = 2,
    checkpoint_budget: u32 = 64,

    pub fn validate(self: Config) !void {
        if (self.slots < 2 or self.slots > 65535 or self.control_slots == 0 or self.control_slots >= self.slots or
            self.max_operations < 2 or self.max_bytes == 0 or self.control_bytes == 0 or self.control_bytes >= self.max_bytes or self.max_wait_ms == 0 or self.max_wait_ms > 60000 or
            self.quantum_ms == 0 or self.quantum_ms > 100 or self.checkpoint_budget == 0 or self.checkpoint_budget > 1048576)
            return error.InvalidRunnablePolicy;
        for (self.weights) |weight| if (weight == 0) return error.InvalidRunnablePolicy;
    }
};

pub const Entry = struct {
    options: admission.Options,
    class: Class,
    slots: usize = 1,
    bytes: u64 = 0,
    state: State = .fresh,
    ready: std.Io.Event = .unset,
    failure: ?anyerror = null,
    scheduler: ?*Scheduler = null,
    next: ?*Entry = null,
    previous: ?*Entry = null,
    all_next: ?*Entry = null,
    all_previous: ?*Entry = null,
    sequence: u64 = 0,
    bypasses: u16 = 0,
    queued_since_ns: u64 = 0,
    waited: bool = false,
    reserved_control: bool = false,
};

const Queue = struct { head: ?*Entry = null, tail: ?*Entry = null };
pub const Scheduler = struct {
    config: Config,
    mutex: std.atomic.Mutex = .unlocked,
    queues: [5]Queue = @splat(.{}),
    entries: ?*Entry = null,
    used: [5]usize = @splat(0),
    credits: [4]usize = @splat(0),
    cursor: usize = 0,
    round_open: bool = false,
    sequence: u64 = 0,
    closed: bool = false,
    metrics: Stats = .{},

    pub const Stats = struct {
        operations: usize = 0,
        retained_bytes: u64 = 0,
        control_operations: usize = 0,
        control_retained_bytes: u64 = 0,
        queued: usize = 0,
        runnable_slots: usize = 0,
        control_slots: usize = 0,
        resource_blocked: usize = 0,
        output_blocked: usize = 0,
        grants: [5]u64 = @splat(0),
        yields: u64 = 0,
        backfills: u64 = 0,
        rejected: u64 = 0,
        wait_ns: u64 = 0,
    };

    pub fn init(config: Config) !Scheduler {
        try config.validate();
        return .{ .config = config };
    }

    fn lock(self: *Scheduler) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn enqueue(self: *Scheduler, e: *Entry, now: u64) void {
        e.queued_since_ns = now;
        e.waited = false;
        const q = &self.queues[@backingInt(e.class)];
        e.previous = q.tail;
        e.next = null;
        if (q.tail) |tail| tail.next = e else q.head = e;
        q.tail = e;
        e.state = .queued;
        e.ready.reset();
        self.metrics.queued += 1;
    }
    fn unlink(self: *Scheduler, e: *Entry) void {
        const q = &self.queues[@backingInt(e.class)];
        if (e.previous) |p| p.next = e.next else q.head = e.next;
        if (e.next) |n| n.previous = e.previous else q.tail = e.previous;
        e.next = null;
        e.previous = null;
        self.metrics.queued -= 1;
    }
    fn ordinaryUsed(self: *Scheduler) usize {
        return self.used[0] + self.used[1] + self.used[2] + self.used[3];
    }
    fn grant(self: *Scheduler, e: *Entry) void {
        self.unlink(e);
        e.state = .running;
        const c = @backingInt(e.class);
        self.used[c] += e.slots;
        self.metrics.grants[c] +|= 1;
        e.ready.set(e.options.io);
    }

    fn choose(self: *Scheduler, free: usize) ?*Entry {
        // Once an older large head has been bypassed enough, let current work
        // drain. New small arrivals cannot repeatedly steal its required slots.
        var barrier: ?*Entry = null;
        for (self.queues[0..4]) |q| if (q.head) |head| {
            if (head.bypasses >= self.config.max_backfills and
                (barrier == null or head.sequence < barrier.?.sequence)) barrier = head;
        };
        if (barrier) |e| return if (e.slots <= free) e else null;
        const eligible = outer: for (self.queues[0..4]) |q| {
            var next = q.head;
            while (next) |e| : (next = e.next) {
                if (e.slots <= free) break :outer true;
            }
        } else false;
        if (!eligible) return null;
        // Deficit round robin charges slot demand, rather than grant count.
        // Idle classes accumulate no credit. Bound rounds even for large jobs.
        const rounds = self.config.slots * 4 + 4;
        for (0..rounds) |_| {
            const c = self.cursor;
            const q = &self.queues[c];
            if (!self.round_open) {
                self.credits[c] = @min(self.config.slots + @as(usize, self.config.weights[c]), self.credits[c] +| self.config.weights[c]);
                self.round_open = true;
            }
            var candidate = q.head;
            while (candidate) |e| {
                if (e.slots <= free) {
                    if (e.slots <= self.credits[c]) {
                        self.credits[c] -= e.slots;
                        return e;
                    }
                    break;
                }
                candidate = e.next;
            }
            if (q.head == null) self.credits[c] = 0;
            self.cursor = (c + 1) % 4;
            self.round_open = false;
        }
        return null;
    }
    fn dispatch(self: *Scheduler) void {
        if (self.closed) return;
        while (self.queues[4].head) |e| {
            if (e.slots > self.config.control_slots - self.used[4]) break;
            self.grant(e);
        }
        const capacity = self.config.slots - self.config.control_slots;
        while (self.ordinaryUsed() < capacity) {
            const free = capacity - self.ordinaryUsed();
            const e = self.choose(free) orelse break;
            for (self.queues[0..4]) |q| if (q.head) |head| {
                if (head != e and head.slots > free) {
                    head.bypasses +|= 1;
                    self.metrics.backfills +|= 1;
                }
            };
            self.grant(e);
        }
    }

    pub fn submit(self: *Scheduler, e: *Entry) !void {
        try e.options.check();
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.state != .fresh or e.scheduler != null) return error.InvalidRunnableTransition;
        const capacity = if (e.class == .control) self.config.control_slots else self.config.slots - self.config.control_slots;
        if (self.closed) return error.SchedulerClosed;
        if (e.slots == 0 or e.slots > capacity) return error.InvalidRunnableDemand;
        const control = e.class == .control;
        const control_count = @min(@as(usize, 4), self.config.max_operations / 2);
        const count_limit = if (control) control_count else self.config.max_operations - control_count;
        const count_used = if (control) self.metrics.control_operations else self.metrics.operations - self.metrics.control_operations;
        const byte_limit = if (control) self.config.control_bytes else self.config.max_bytes - self.config.control_bytes;
        const byte_used = if (control) self.metrics.control_retained_bytes else self.metrics.retained_bytes - self.metrics.control_retained_bytes;
        if (count_used >= count_limit or e.bytes > byte_limit - byte_used) {
            self.metrics.rejected +|= 1;
            return error.SchedulerFull;
        }
        e.reserved_control = control;
        if (control) {
            self.metrics.control_operations += 1;
            self.metrics.control_retained_bytes += e.bytes;
        }
        // Registry/count/bytes cover queued, running AND blocked operations.
        // Resume never needs another reservation or allocation.
        self.metrics.operations += 1;
        self.metrics.retained_bytes += e.bytes;
        e.scheduler = self;
        e.all_next = self.entries;
        if (self.entries) |head| head.all_previous = e;
        self.entries = e;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }

    pub fn state(self: *Scheduler, e: *const Entry) State {
        self.lock();
        defer self.mutex.unlock();
        return e.state;
    }

    /// Initial acquisition retires its unpublished entry on failure.
    pub fn wait(self: *Scheduler, e: *Entry) !void {
        return self.waitImpl(e, false);
    }
    /// A published execution may lend its retained ownership to IO or a child.
    /// Denied reentry stops runnable execution, but only its owning finish may
    /// release operation/byte reservations after those consumers are fenced.
    pub fn waitRetained(self: *Scheduler, e: *Entry) !void {
        return self.waitImpl(e, true);
    }
    fn failWait(self: *Scheduler, e: *Entry, err: anyerror, comptime retain: bool) void {
        if (!retain) return self.finish(e);
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return;
        switch (e.state) {
            .queued => self.unlink(e),
            .running => self.used[@backingInt(e.class)] -= e.slots,
            .resource_blocked => self.metrics.resource_blocked -= 1,
            .output_blocked => self.metrics.output_blocked -= 1,
            .stopped => {},
            .fresh, .finished => unreachable,
        }
        e.state = .stopped;
        e.failure = err;
        e.ready.set(e.options.io);
        self.dispatch();
    }
    fn waitImpl(self: *Scheduler, e: *Entry, comptime retain: bool) !void {
        self.lock();
        const owned = e.scheduler == self;
        self.mutex.unlock();
        if (!owned) return error.InvalidRunnableTransition;
        const options = e.options;
        const start = options.now();
        const until = @min(options.deadline_ns orelse std.math.maxInt(u64), e.queued_since_ns +| @as(u64, self.config.max_wait_ms) * std.time.ns_per_ms);
        defer {
            const elapsed = options.now() -| start;
            self.lock();
            self.metrics.wait_ns +|= elapsed;
            self.mutex.unlock();
        }
        while (true) {
            options.check() catch |err| {
                self.failWait(e, err, retain);
                return err;
            };
            self.lock();
            const still_owned = e.scheduler == self;
            const s = e.state;
            const failure = e.failure;
            const closed = self.closed;
            const waited = e.waited;
            self.mutex.unlock();
            if (!still_owned) return error.InvalidRunnableTransition;
            if (waited and options.now() >= until) {
                self.failWait(e, error.RunnableWaitTimeout, retain);
                return error.RunnableWaitTimeout;
            }
            if (s == .running and !closed) {
                options.check() catch |err| {
                    self.failWait(e, err, retain);
                    return err;
                };
                return;
            }
            if (closed) {
                self.failWait(e, error.SchedulerClosed, retain);
                return error.SchedulerClosed;
            }
            if (s != .queued) {
                const err = failure orelse error.InvalidRunnableTransition;
                self.failWait(e, err, retain);
                return err;
            }
            if (options.now() >= until) {
                self.failWait(e, error.RunnableWaitTimeout, retain);
                return error.RunnableWaitTimeout;
            }
            e.ready.waitTimeout(options.io, .{ .duration = .{
                .raw = .fromNanoseconds(@intCast(@min(until -| options.now(), 5 * std.time.ns_per_ms))),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    self.failWait(e, err, retain);
                    return err;
                },
            };
        }
    }
    pub fn yield(self: *Scheduler, e: *Entry) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running) return error.InvalidRunnableTransition;
        self.used[@backingInt(e.class)] -= e.slots;
        self.metrics.yields +|= 1;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        if (self.closed) {
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            return error.SchedulerClosed;
        }
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    pub fn block(self: *Scheduler, e: *Entry, reason: State) !void {
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or
            (reason != .resource_blocked and reason != .output_blocked)) return error.InvalidRunnableTransition;
        self.used[@backingInt(e.class)] -= e.slots;
        if (self.closed) {
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            e.ready.set(e.options.io);
            return error.SchedulerClosed;
        }
        e.state = reason;
        if (reason == .resource_blocked) self.metrics.resource_blocked += 1 else self.metrics.output_blocked += 1;
        self.dispatch();
    }
    pub fn unblock(self: *Scheduler, e: *Entry) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        if (e.state != .resource_blocked and e.state != .output_blocked) return error.InvalidRunnableTransition;
        if (e.state == .resource_blocked) self.metrics.resource_blocked -= 1 else self.metrics.output_blocked -= 1;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    /// Only trusted adapters may promote a parked read for protected drain.
    /// Classification is never taken from client-provided scheduling labels.
    pub fn reclassifyBlocked(self: *Scheduler, e: *Entry, class: Class) !void {
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        if (e.state != .resource_blocked and e.state != .output_blocked) return error.InvalidRunnableTransition;
        const capacity = if (class == .control) self.config.control_slots else self.config.slots - self.config.control_slots;
        if (e.slots > capacity) return error.InvalidRunnableDemand;
        e.class = class;
    }

    pub fn finish(self: *Scheduler, e: *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler == null) return;
        std.debug.assert(e.scheduler == self);
        switch (e.state) {
            .queued => self.unlink(e),
            .running => self.used[@backingInt(e.class)] -= e.slots,
            .resource_blocked => self.metrics.resource_blocked -= 1,
            .output_blocked => self.metrics.output_blocked -= 1,
            else => {},
        }
        if (e.all_previous) |p| p.all_next = e.all_next else self.entries = e.all_next;
        if (e.all_next) |n| n.all_previous = e.all_previous;
        e.all_next = null;
        e.all_previous = null;
        self.metrics.operations -= 1;
        self.metrics.retained_bytes -= e.bytes;
        if (e.reserved_control) {
            self.metrics.control_operations -= 1;
            self.metrics.control_retained_bytes -= e.bytes;
        }
        e.state = .finished;
        e.scheduler = null;
        self.dispatch();
    }
    pub fn close(self: *Scheduler) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        var next = self.entries;
        while (next) |e| : (next = e.all_next) {
            switch (e.state) {
                .queued => self.unlink(e),
                .resource_blocked => self.metrics.resource_blocked -= 1,
                .output_blocked => self.metrics.output_blocked -= 1,
                // Live execution is joined by its caller, never revoked.
                else => continue,
            }
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            e.ready.set(e.options.io);
        }
    }
    pub fn stats(self: *Scheduler) Stats {
        self.lock();
        defer self.mutex.unlock();
        var result = self.metrics;
        result.runnable_slots = self.ordinaryUsed() + self.used[4];
        result.control_slots = self.used[4];
        return result;
    }
};

/// Stable node lifetime, distinct from the API instances borrowing it. Native
/// archive consumers must use owner callbacks rather than dereference this type.
pub const NodeOwner = struct {
    scheduler: Scheduler,
    mutex: std.atomic.Mutex = .unlocked,
    borrowers: usize = 0,
    closed: bool = false,

    pub fn init(config: Config) !NodeOwner {
        return .{ .scheduler = try Scheduler.init(config) };
    }
    pub fn retain(self: *NodeOwner) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        if (self.closed) return error.SchedulerClosed;
        self.borrowers += 1;
    }
    pub fn release(self: *NodeOwner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        defer self.mutex.unlock();
        std.debug.assert(self.borrowers != 0);
        self.borrowers -= 1;
    }
    pub fn close(self: *NodeOwner) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        self.closed = true;
        self.mutex.unlock();
        self.scheduler.close();
    }
    pub fn deinit(self: *NodeOwner) void {
        self.tryDeinit() catch @panic("runnable node owner still in use");
    }
    pub fn tryDeinit(self: *NodeOwner) !void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
        if (self.borrowers != 0 or self.scheduler.stats().operations != 0) {
            self.mutex.unlock();
            return error.InvalidRunnableTransition;
        }
        self.closed = true;
        self.mutex.unlock();
        self.scheduler.close();
        self.* = undefined;
    }
};

/// Stable, single-worker execution capsule. Parallel children must get their
/// own ownership before sharing this capsule's adapter; it is not a gang lease.
pub const Execution = struct {
    entry: ?Entry = null,
    quantum_start_ns: u64 = 0,
    checkpoints: u32 = 0,
    /// Changes only after a successful grant. Readers use it to invalidate
    /// authority checks across cooperative yields and resource/output waits.
    resume_generation: u64 = 0,

    pub fn start(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class) !void {
        try self.startWithSlots(scheduler, options, class, 1);
    }
    /// One stable capsule can account for a bounded CPU team. Its demand stays
    /// attached across yields and waits, independently of retained memory.
    pub fn startWithSlots(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class, slots: usize) !void {
        std.debug.assert(self.entry == null);
        const owner = scheduler orelse return;
        self.entry = .{ .options = options, .class = class, .slots = slots, .bytes = options.retained_bytes };
        errdefer self.finish();
        try owner.submit(&self.entry.?);
        try owner.wait(&self.entry.?);
        self.resume_generation +%= 1;
        self.quantum_start_ns = options.now();
    }
    fn schedulerFor(e: *Entry, comptime needs_running: bool) !*Scheduler {
        const owner = e.scheduler orelse return error.InvalidRunnableTransition;
        owner.lock();
        defer owner.mutex.unlock();
        // A denied resume is terminal until owning finish. Later phase or
        // cleanup checks must preserve that cause, not attempt to resurrect it.
        if (e.state == .stopped) return e.failure orelse error.InvalidRunnableTransition;
        if (needs_running and e.state != .running) return error.InvalidRunnableTransition;
        return owner;
    }
    pub fn checkpoint(self: *Execution) !void {
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try e.options.check();
        self.checkpoints +|= 1;
        if (self.checkpoints < owner.config.checkpoint_budget and
            e.options.now() -| self.quantum_start_ns < @as(u64, owner.config.quantum_ms) * std.time.ns_per_ms) return;
        try owner.yield(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.checkpoints = 0;
    }
    pub fn park(self: *Execution, reason: State) !void {
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        try owner.block(e, reason);
    }
    pub fn enter(self: *Execution, class: Class) !void {
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        try owner.reclassifyBlocked(e, class);
        try owner.unblock(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.checkpoints = 0;
    }
    pub fn finish(self: *Execution) void {
        if (self.entry) |*e| if (e.scheduler) |owner| owner.finish(e);
        self.entry = null;
    }
};

fn testEntry(class: Class, slots: usize, bytes: u64) Entry {
    return .{ .options = .{ .io = std.testing.io }, .class = class, .slots = slots, .bytes = bytes };
}

test "runnable scheduler weighted execution retains CPU demand across waits and class changes" {
    var scheduler = try Scheduler.init(.{ .slots = 4, .checkpoint_budget = 1 });
    defer scheduler.close();
    var inference: Execution = .{};
    defer inference.finish();
    try inference.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 11 }, .inference, 2);
    var background: Execution = .{};
    defer background.finish();
    try background.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 7 }, .background, 1);
    var control: Execution = .{};
    defer control.finish();
    try control.start(&scheduler, .{ .io = std.testing.io }, .control);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_slots);
    try inference.park(.resource_blocked);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 18), scheduler.stats().retained_bytes);
    try inference.enter(.inference);
    try inference.checkpoint();
    try std.testing.expect(inference.resume_generation > 2);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    background.finish();
    control.finish();
    try inference.park(.output_blocked);
    try inference.enter(.query);
    try std.testing.expectEqual(Class.query, inference.entry.?.class);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    inference.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    var invalid: Execution = .{};
    defer invalid.finish();
    for ([_]usize{ 0, 4 }) |slots| {
        try std.testing.expectError(error.InvalidRunnableDemand, invalid.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 19 }, .query, slots));
        try std.testing.expect(invalid.entry == null);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler node owner outlives independent borrowers and executions" {
    var owner = try NodeOwner.init(.{ .slots = 2, .checkpoint_budget = 1 });
    defer owner.deinit();
    try owner.retain();
    try owner.retain();
    try std.testing.expectError(error.InvalidRunnableTransition, owner.tryDeinit());
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&owner.scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    owner.release();
    try execution.checkpoint();
    try std.testing.expectEqual(@as(usize, 1), owner.borrowers);
    try std.testing.expectEqual(@as(u64, 17), owner.scheduler.stats().retained_bytes);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, owner.retain());
    try std.testing.expectEqual(@as(usize, 1), owner.scheduler.stats().operations);
    owner.release();
    try std.testing.expectError(error.InvalidRunnableTransition, owner.tryDeinit());
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 0), owner.scheduler.stats().retained_bytes);
}

test "runnable scheduler preserves blocked ownership and protected capacity" {
    var s = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .max_operations = 3, .max_bytes = 40, .control_bytes = 10 });
    var read = testEntry(.query, 3, 10);
    var next = testEntry(.write, 3, 10);
    var control = testEntry(.control, 1, 10);
    defer s.finish(&read);
    defer s.finish(&next);
    defer s.finish(&control);
    try s.submit(&read);
    try s.submit(&next);
    try s.submit(&control);
    try std.testing.expectEqual(State.running, s.state(&read));
    try std.testing.expectEqual(State.queued, s.state(&next));
    try std.testing.expectEqual(State.running, s.state(&control));
    try s.block(&read, .output_blocked);
    try std.testing.expectEqual(State.running, s.state(&next));
    try std.testing.expectEqual(@as(usize, 3), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 30), s.stats().retained_bytes);
    var extra = testEntry(.query, 1, 0);
    try std.testing.expectError(error.SchedulerFull, s.submit(&extra));
    // No allocation/new outstanding slot needed even at the hard envelope.
    try s.unblock(&read);
    try std.testing.expectEqual(State.queued, s.state(&read));
    s.finish(&next);
    try s.wait(&read);
    try std.testing.expectEqual(State.running, s.state(&read));
    s.finish(&read);
    s.finish(&read);
    s.finish(&control);
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), s.stats().retained_bytes);
}

test "runnable scheduler bounds backfill and grants a large operation" {
    var s = try Scheduler.init(.{ .slots = 4, .max_backfills = 2 });
    var holder = testEntry(.write, 2, 0);
    var large = testEntry(.query, 3, 0);
    var small: [4]Entry = undefined;
    defer s.finish(&holder);
    defer s.finish(&large);
    defer for (&small) |*e| s.finish(e);
    for (&small) |*e| e.* = testEntry(.query, 1, 0);
    try s.submit(&holder);
    try s.submit(&large);
    for (&small) |*e| try s.submit(e);
    try std.testing.expectEqual(State.running, s.state(&small[0]));
    s.finish(&small[0]);
    try std.testing.expectEqual(State.running, s.state(&small[1]));
    s.finish(&small[1]);
    try std.testing.expectEqual(State.queued, s.state(&small[2]));
    try std.testing.expectEqual(@as(u16, 2), large.bypasses);
    s.finish(&holder);
    try std.testing.expectEqual(State.running, s.state(&large));
    try std.testing.expectEqual(State.queued, s.state(&small[2]));
}

test "runnable scheduler weighted sharing charges slots across busy classes" {
    var s = try Scheduler.init(.{ .slots = 2, .weights = .{ 2, 1, 1, 1 } });
    var holder = testEntry(.background, 1, 0);
    try s.submit(&holder);
    defer s.finish(&holder);
    var reads: [24]Entry = undefined;
    var writes: [24]Entry = undefined;
    for (&reads) |*e| e.* = testEntry(.query, 1, 0);
    for (&writes) |*e| e.* = testEntry(.write, 1, 0);
    defer for (&reads) |*e| s.finish(e);
    defer for (&writes) |*e| s.finish(e);
    for (&reads, &writes) |*r, *w| {
        try s.submit(r);
        try s.submit(w);
    }
    s.finish(&holder);
    for (0..30) |_| {
        var running: ?*Entry = null;
        for (&reads) |*e| if (s.state(e) == .running) {
            running = e;
            break;
        };
        if (running == null) for (&writes) |*e| if (s.state(e) == .running) {
            running = e;
            break;
        };
        s.finish(running orelse return error.NoRunnableWork);
    }
    const counts = s.stats().grants;
    // Includes the next grant after each finish. Both classes remain busy.
    try std.testing.expect(counts[0] >= 20 and counts[0] <= 21);
    try std.testing.expect(counts[1] >= 10 and counts[1] <= 11);
}

test "runnable scheduler cancellation after grant returns every reservation" {
    var s = try Scheduler.init(.{ .slots = 2 });
    var holder = testEntry(.query, 1, 5);
    var next = testEntry(.query, 1, 7);
    try s.submit(&holder);
    try s.submit(&next);
    defer s.finish(&holder);
    defer s.finish(&next);
    const Token = struct {
        fn requested(_: *const anyopaque) bool {
            return true;
        }
    };
    var token: u8 = 0;
    holder.options.cancellation = .{ .ptr = &token, .requested = Token.requested };
    try std.testing.expectError(error.Canceled, s.wait(&holder));
    try std.testing.expectEqual(State.finished, s.state(&holder));
    try std.testing.expectEqual(State.running, s.state(&next));
    try std.testing.expectEqual(@as(u64, 7), s.stats().retained_bytes);
}

test "runnable scheduler deadline and shutdown retire blocked and queued owners" {
    var s = try Scheduler.init(.{ .slots = 2 });
    var holder = testEntry(.query, 1, 4);
    var queued = testEntry(.query, 1, 5);
    try s.submit(&holder);
    try s.submit(&queued);
    queued.options.deadline_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, s.wait(&queued));
    try s.block(&holder, .resource_blocked);
    s.close();
    try std.testing.expectEqual(State.stopped, s.state(&holder));
    try std.testing.expectError(error.SchedulerClosed, s.wait(&holder));
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), s.stats().resource_blocked);
    try std.testing.expectEqual(@as(u64, 0), s.stats().retained_bytes);
}

test "runnable scheduler yield hands execution to another operation" {
    var s = try Scheduler.init(.{ .slots = 2, .weights = .{ 1, 1, 1, 1 } });
    var first = testEntry(.query, 1, 4);
    var second = testEntry(.write, 1, 6);
    try s.submit(&first);
    try s.submit(&second);
    defer s.finish(&first);
    defer s.finish(&second);
    try s.yield(&first);
    try std.testing.expectEqual(State.queued, s.state(&first));
    try std.testing.expectEqual(State.running, s.state(&second));
    try std.testing.expectEqual(@as(u64, 10), s.stats().retained_bytes);
    s.finish(&second);
    try s.wait(&first);
    try std.testing.expectEqual(@as(u64, 1), s.stats().yields);
}

const VirtualClock = struct {
    var value: u64 = 0;
    fn now() u64 {
        return value;
    }
};

test "runnable scheduler queue bound starts at enqueue including grant race" {
    VirtualClock.value = 0;
    var s = try Scheduler.init(.{ .slots = 2, .max_wait_ms = 5 });
    var holder = testEntry(.query, 1, 4);
    var queued = testEntry(.query, 1, 5);
    holder.options.native_now_ns = VirtualClock.now;
    queued.options.native_now_ns = VirtualClock.now;
    queued.options.deadline_ns = 20 * std.time.ns_per_ms;
    try s.submit(&holder);
    try s.submit(&queued);
    defer s.finish(&holder);
    defer s.finish(&queued);
    VirtualClock.value = 6 * std.time.ns_per_ms;
    // A release can win the notification race. Expired work still cannot start.
    s.finish(&holder);
    try std.testing.expectEqual(State.running, s.state(&queued));
    try std.testing.expectError(error.RunnableWaitTimeout, s.wait(&queued));
    try std.testing.expectEqual(@as(usize, 0), s.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
}

test "runnable scheduler real queue timeout releases count and bytes" {
    var s = try Scheduler.init(.{ .slots = 2, .max_wait_ms = 2 });
    var holder = testEntry(.query, 1, 4);
    var queued = testEntry(.query, 1, 5);
    try s.submit(&holder);
    try s.submit(&queued);
    defer s.finish(&holder);
    defer s.finish(&queued);
    try std.testing.expectError(error.RunnableWaitTimeout, s.wait(&queued));
    try std.testing.expectEqual(@as(usize, 1), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 4), s.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), s.stats().queued);
}

test "runnable scheduler rejects invalid policy and oversized demand atomically" {
    for ([_]Config{
        .{ .slots = 0 },                 .{ .slots = 65536 }, .{ .control_slots = 8 },
        .{ .max_operations = 0 },        .{ .max_bytes = 0 }, .{ .max_wait_ms = 0 },
        .{ .weights = .{ 1, 0, 1, 1 } },
    }) |cfg| try std.testing.expectError(error.InvalidRunnablePolicy, Scheduler.init(cfg));
    var s = try Scheduler.init(.{ .slots = 2, .max_bytes = 4, .control_bytes = 1 });
    var bytes = testEntry(.query, 1, 5);
    var slots = testEntry(.query, 2, 4);
    try std.testing.expectError(error.SchedulerFull, s.submit(&bytes));
    try std.testing.expectError(error.InvalidRunnableDemand, s.submit(&slots));
    try std.testing.expectEqual(State.fresh, s.state(&bytes));
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), s.stats().retained_bytes);
}

test "runnable scheduler shutdown racing a new block cannot strand an owner" {
    var s = try Scheduler.init(.{ .slots = 2 });
    var live = testEntry(.query, 1, 8);
    try s.submit(&live);
    s.close();
    try std.testing.expectError(error.SchedulerClosed, s.block(&live, .resource_blocked));
    try std.testing.expectEqual(State.stopped, s.state(&live));
    try std.testing.expectEqual(@as(usize, 0), s.stats().runnable_slots);
    try std.testing.expectError(error.SchedulerClosed, s.unblock(&live));
    s.finish(&live);
    try std.testing.expectEqual(@as(usize, 0), s.stats().operations);
}

test "runnable scheduler execution quanta preserve deadline and parked ownership" {
    VirtualClock.value = 0;
    var s = try Scheduler.init(.{ .slots = 2, .quantum_ms = 2, .checkpoint_budget = 64 });
    var exec: Execution = .{};
    try exec.start(&s, .{ .io = std.testing.io, .native_now_ns = VirtualClock.now, .deadline_ns = 10 * std.time.ns_per_ms, .retained_bytes = 17 }, .query);
    defer exec.finish();
    try std.testing.expectEqual(@as(u64, 1), exec.resume_generation);
    try exec.checkpoint();
    try std.testing.expectEqual(@as(u64, 1), exec.resume_generation);
    try std.testing.expectEqual(@as(u64, 0), s.stats().yields);
    VirtualClock.value = 3 * std.time.ns_per_ms;
    try exec.checkpoint();
    try std.testing.expectEqual(@as(u64, 1), s.stats().yields);
    try std.testing.expectEqual(@as(u64, 2), exec.resume_generation);
    try exec.park(.output_blocked);
    try std.testing.expectEqual(@as(u64, 2), exec.resume_generation);
    try std.testing.expectEqual(@as(usize, 0), s.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 17), s.stats().retained_bytes);
    var other: Execution = .{};
    try other.start(&s, .{ .io = std.testing.io }, .query);
    defer other.finish();
    // Verified completion drain uses reserved control slots at full foreground.
    try exec.enter(.control);
    try std.testing.expectEqual(@as(u64, 3), exec.resume_generation);
    try std.testing.expectEqual(@as(usize, 2), s.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), s.stats().control_slots);
    VirtualClock.value = 11 * std.time.ns_per_ms;
    try std.testing.expectError(error.DeadlineExceeded, exec.checkpoint());
    try std.testing.expectEqual(@as(u64, 3), exec.resume_generation);
    try std.testing.expectEqual(@as(?u64, 10 * std.time.ns_per_ms), exec.entry.?.options.deadline_ns);
}

test "runnable scheduler checkpoint work budget and disabled execution" {
    var disabled: Execution = .{};
    try disabled.start(null, .{ .io = std.testing.io }, .query);
    try disabled.checkpoint();
    try disabled.park(.resource_blocked);
    try disabled.enter(.query);
    disabled.finish();
    try std.testing.expectEqual(@as(u64, 0), disabled.resume_generation);
    var s = try Scheduler.init(.{ .slots = 2, .checkpoint_budget = 2, .quantum_ms = 100 });
    var exec: Execution = .{};
    try exec.start(&s, .{ .io = std.testing.io }, .query);
    defer exec.finish();
    try exec.checkpoint();
    try exec.checkpoint();
    try std.testing.expectEqual(@as(u64, 1), s.stats().yields);
}

test "runnable scheduler reserves control metadata at full ordinary outstanding limits" {
    var s = try Scheduler.init(.{ .slots = 2, .max_operations = 3, .max_bytes = 30, .control_bytes = 10 });
    var first = testEntry(.query, 1, 10);
    var second = testEntry(.query, 1, 10);
    var control = testEntry(.control, 1, 10);
    try s.submit(&first);
    try s.block(&first, .output_blocked);
    try s.submit(&second);
    try s.block(&second, .resource_blocked);
    defer s.finish(&first);
    defer s.finish(&second);
    defer s.finish(&control);
    var excess = testEntry(.query, 1, 0);
    try std.testing.expectError(error.SchedulerFull, s.submit(&excess));
    try s.submit(&control);
    try s.wait(&control);
    try std.testing.expectEqual(@as(usize, 3), s.stats().operations);
    try std.testing.expectEqual(@as(u64, 30), s.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), s.stats().control_operations);
    // Promotion retains the read's original metadata reservation. It needs no
    // new control metadata, even though that partition is completely full.
    try s.reclassifyBlocked(&first, .control);
    try s.unblock(&first);
    s.finish(&control);
    try s.wait(&first);
    try std.testing.expectEqual(@as(usize, 0), s.stats().control_operations);
    try std.testing.expectEqual(@as(usize, 1), s.stats().control_slots);
}

test "runnable scheduler denied reentry retains borrowed reservations until owning finish" {
    const Cause = enum { canceled, deadline, wait_timeout };
    const Flag = struct {
        value: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).value;
        }
    };
    for (std.enums.values(Cause)) |cause| {
        VirtualClock.value = 0;
        var flag: Flag = .{};
        var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 4, .max_wait_ms = 1 });
        defer scheduler.close();
        var borrowed: Execution = .{};
        defer borrowed.finish();
        try borrowed.startWithSlots(&scheduler, .{
            .io = std.testing.io,
            .native_now_ns = if (cause == .wait_timeout) null else VirtualClock.now,
            .deadline_ns = if (cause == .deadline) 10 else null,
            .cancellation = .{ .ptr = &flag, .requested = Flag.requested },
            .retained_bytes = 17,
        }, .inference, 2);
        try borrowed.park(.resource_blocked);
        var peer: Execution = .{};
        defer peer.finish();
        try peer.startWithSlots(&scheduler, .{ .io = std.testing.io }, .query, 2);
        if (cause == .canceled) flag.value = true;
        if (cause == .deadline) VirtualClock.value = 11;
        const expected = switch (cause) {
            .canceled => error.Canceled,
            .deadline => error.DeadlineExceeded,
            .wait_timeout => error.RunnableWaitTimeout,
        };
        try std.testing.expectError(expected, borrowed.enter(.inference));
        const held = scheduler.stats();
        try std.testing.expectEqual(State.stopped, scheduler.state(&borrowed.entry.?));
        try std.testing.expectEqual(@as(usize, 2), held.operations);
        try std.testing.expectEqual(@as(u64, 17), held.retained_bytes);
        try std.testing.expectEqual(@as(usize, 2), held.runnable_slots);
        try std.testing.expectEqual(@as(usize, 0), held.queued);
        // A detached or still-unreaped consumer cannot silently free its
        // outstanding operation reservation for a fresh foreground request.
        var excess = testEntry(.query, 1, 0);
        try std.testing.expectError(error.SchedulerFull, scheduler.submit(&excess));
        // A subsequently cleared cancellation token or shutdown cannot replace
        // the first failure or allow a new runnable interval on this capsule.
        flag.value = false;
        const generation = borrowed.resume_generation;
        try std.testing.expectError(expected, borrowed.checkpoint());
        try std.testing.expectError(expected, borrowed.park(.output_blocked));
        try std.testing.expectError(expected, borrowed.enter(.control));
        scheduler.close();
        try std.testing.expectError(expected, borrowed.checkpoint());
        try std.testing.expectError(expected, borrowed.park(.resource_blocked));
        try std.testing.expectError(expected, borrowed.enter(.inference));
        try std.testing.expectEqual(generation, borrowed.resume_generation);
        try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        peer.finish();
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), scheduler.stats().retained_bytes);
        borrowed.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    }
}

test "runnable scheduler canceled yielded capsule retains ownership while peer computes" {
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 4, .checkpoint_budget = 1 });
    defer scheduler.close();
    var peer = testEntry(.query, 2, 3);
    const CancelAfterYield = struct {
        scheduler: *Scheduler,
        peer: *Entry,
        fn requested(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.scheduler.state(self.peer) == .running;
        }
    };
    const cancellation = CancelAfterYield{ .scheduler = &scheduler, .peer = &peer };
    var borrowed: Execution = .{};
    defer borrowed.finish();
    try borrowed.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17, .cancellation = .{ .ptr = &cancellation, .requested = CancelAfterYield.requested } }, .inference, 2);
    try scheduler.submit(&peer);
    defer scheduler.finish(&peer);
    try std.testing.expectEqual(State.queued, scheduler.state(&peer));
    try std.testing.expectError(error.Canceled, borrowed.checkpoint());
    try std.testing.expectEqual(State.running, scheduler.state(&peer));
    try std.testing.expectEqual(State.stopped, scheduler.state(&borrowed.entry.?));
    const held = scheduler.stats();
    try std.testing.expectEqual(@as(usize, 2), held.operations);
    try std.testing.expectEqual(@as(u64, 20), held.retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), held.runnable_slots);
    try std.testing.expectEqual(@as(u64, 1), held.yields);
    borrowed.finish();
    try std.testing.expectEqual(@as(u64, 3), scheduler.stats().retained_bytes);
    scheduler.finish(&peer);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable fail-fast initial control admission never waits or leaks a reservation" {
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 8, .max_bytes = 128, .control_bytes = 64, .max_wait_ms = 1000 });
    var ordinary: Execution = .{};
    defer ordinary.finish();
    try ordinary.startWithSlots(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query, 2);
    var first: Execution = .{};
    defer first.finish();
    try first.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 31, .allow_wait = false }, .control);
    var rejected: Execution = .{};
    defer rejected.finish();
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, rejected.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 19, .allow_wait = false }, .control));
    try std.testing.expect(rejected.entry == null);
    const active = scheduler.stats();
    try std.testing.expectEqual(@as(usize, 2), active.operations);
    try std.testing.expectEqual(@as(u64, 48), active.retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), active.control_operations);
    try std.testing.expectEqual(@as(usize, 3), active.runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), active.queued);
    first.finish();
    try rejected.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 19, .allow_wait = false }, .control);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_slots);
    rejected.finish();
    ordinary.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable fail-fast acquisition retains fair bounded published reentry" {
    const io = std.testing.io;
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 8, .max_wait_ms = 1000 });
    var first: Execution = .{};
    defer first.finish();
    try first.start(&scheduler, .{ .io = io, .retained_bytes = 31, .allow_wait = false }, .control);
    try first.park(.output_blocked);
    var peer: Execution = .{};
    defer peer.finish();
    try peer.start(&scheduler, .{ .io = io, .retained_bytes = 11, .allow_wait = false }, .control);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().control_operations);
    try std.testing.expectEqual(@as(u64, 42), scheduler.stats().retained_bytes);
    var release = try io.concurrent(struct {
        fn run(value: *Execution, lane: std.Io) !void {
            try lane.sleep(.fromMilliseconds(20), .awake);
            value.finish();
        }
    }.run, .{ &peer, io });
    defer release.cancel(io) catch {};
    try first.enter(.control);
    try release.await(io);
    try std.testing.expectEqual(@as(u64, 2), first.resume_generation);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_operations);
    try std.testing.expectEqual(@as(u64, 31), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().control_slots);
    first.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}
