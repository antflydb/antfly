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
    /// Dedicated cleanup CPU drawn from slots, separate from health controls.
    /// Zero retains the legacy shared-control policy until callers opt in.
    cleanup_slots: usize = 0,
    weights: [4]u16 = .{ 4, 2, 1, 1 },
    max_operations: usize = 128,
    max_bytes: u64 = 8 << 20,
    control_bytes: u64 = 64 << 10,
    max_wait_ms: u32 = 250,
    max_backfills: u16 = 8,
    quantum_ms: u32 = 2,
    checkpoint_budget: u32 = 64,

    pub fn ordinarySlots(self: Config) usize {
        return self.slots - self.control_slots - self.cleanup_slots;
    }

    pub fn validate(self: Config) !void {
        if (self.slots < 2 or self.slots > 65535 or self.control_slots == 0 or self.control_slots >= self.slots or
            self.cleanup_slots > 1 or self.cleanup_slots >= self.slots - self.control_slots or
            self.max_operations < 2 or self.max_bytes == 0 or self.control_bytes == 0 or self.control_bytes >= self.max_bytes or self.max_wait_ms == 0 or self.max_wait_ms > 60000 or
            self.quantum_ms == 0 or self.quantum_ms > 100 or self.checkpoint_budget == 0 or self.checkpoint_budget > 1048576)
            return error.InvalidRunnablePolicy;
        for (self.weights) |weight| if (weight == 0) return error.InvalidRunnablePolicy;
    }
};

pub const Entry = struct {
    options: admission.Options,
    /// Parent and all of its original clock/cancellation owners remain stable
    /// until this entry's owning finish, including stopped and blocked work.
    parent: ?*Entry = null,
    children: usize = 0,
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
    /// Preauthorized before possible durable acceptance. These callbacks and
    /// their independent completion Io must remain valid through all joins.
    completion_options: ?admission.Options = null,
    completion_active: bool = false,
    /// Trusted physical cleanup reserved before publishing an owned resource.
    /// This grants no durable-write authority and never changes CPU partition.
    cleanup_options: ?admission.Options = null,
    /// One-shot physical cleanup duration, consumed at first activation.
    cleanup_duration_ns: ?u64 = null,
    cleanup_active: bool = false,
    protected_cleanup_root: bool = false,

    fn retainedPhase(self: *const Entry) bool {
        return self.completion_active or self.cleanup_active or self.protected_cleanup_root;
    }
};

const Queue = struct { head: ?*Entry = null, tail: ?*Entry = null };
pub const Scheduler = struct {
    config: Config,
    mutex: std.atomic.Mutex = .unlocked,
    queues: [6]Queue = @splat(.{}),
    entries: ?*Entry = null,
    used: [6]usize = @splat(0),
    credits: [4]usize = @splat(0),
    cursor: usize = 0,
    round_open: bool = false,
    sequence: u64 = 0,
    closed: bool = false,
    metrics: Stats = .{},

    pub const Stats = struct {
        operations: usize = 0,
        child_operations: usize = 0,
        retained_bytes: u64 = 0,
        control_operations: usize = 0,
        control_retained_bytes: u64 = 0,
        cleanup_operations: usize = 0,
        cleanup_retained_bytes: u64 = 0,
        queued: usize = 0,
        runnable_slots: usize = 0,
        control_slots: usize = 0,
        cleanup_runnable_slots: usize = 0,
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
    fn cpuIndex(self: *const Scheduler, e: *const Entry) usize {
        return if (e.protected_cleanup_root and self.config.cleanup_slots != 0) 5 else @backingInt(e.class);
    }
    fn capacityFor(self: *const Scheduler, e: *const Entry, class: Class) usize {
        if (e.protected_cleanup_root and self.config.cleanup_slots != 0) return self.config.cleanup_slots;
        return if (class == .control) self.config.control_slots else self.config.ordinarySlots();
    }

    fn enqueue(self: *Scheduler, e: *Entry, now: u64) void {
        e.queued_since_ns = now;
        e.waited = false;
        const q = &self.queues[self.cpuIndex(e)];
        e.previous = q.tail;
        e.next = null;
        if (q.tail) |tail| tail.next = e else q.head = e;
        q.tail = e;
        e.state = .queued;
        e.ready.reset();
        self.metrics.queued += 1;
    }
    fn unlink(self: *Scheduler, e: *Entry) void {
        const q = &self.queues[self.cpuIndex(e)];
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
        self.used[self.cpuIndex(e)] += e.slots;
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
        // Close removes ordinary queued entries, but preauthorized active
        // completion/cleanup retains the same bounded queues and capacity partition.
        while (self.queues[4].head) |e| {
            if (e.slots > self.config.control_slots - self.used[4]) break;
            self.grant(e);
        }
        while (self.queues[5].head) |e| {
            if (e.slots > self.config.cleanup_slots - self.used[5]) break;
            self.grant(e);
        }
        const capacity = self.config.ordinarySlots();
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
        if (e.parent != null) return error.InvalidRunnableTransition;
        try self.submitLocked(e, now);
    }

    /// Reserve a child before publishing its task. Each child uses the same
    /// finite operation/byte registry and fair CPU queue, with independent
    /// park/yield state. Its IO must be an independently owned worker lane;
    /// every deadline, clock and cancellation borrow comes from the parent.
    pub fn submitChild(self: *Scheduler, parent: *Entry, child: *Entry, io: std.Io) !void {
        try self.checkAuthority(parent);
        const now = parent.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (parent == child or parent.scheduler != self or parent.state != .running or parent.retainedPhase() or
            child.state != .fresh or child.scheduler != null or child.parent != null or child.children != 0 or parent.slots != 1) return error.InvalidRunnableTransition;
        try self.checkAncestorsLocked(parent);
        if (child.bytes < @sizeOf(Entry)) return error.InvalidRunnableDemand;
        child.options = parent.options;
        child.options.io = io;
        child.options.retained_bytes = child.bytes;
        child.class = parent.class;
        child.slots = 1;
        try self.submitLocked(child, now);
        child.parent = parent;
        parent.children += 1;
        self.metrics.child_operations += 1;
    }

    fn submitLocked(self: *Scheduler, e: *Entry, now: u64) !void {
        if (e.state != .fresh or e.scheduler != null) return error.InvalidRunnableTransition;
        const capacity = self.capacityFor(e, e.class);
        if (self.closed) return error.SchedulerClosed;
        if (e.slots == 0 or e.slots > capacity) return error.InvalidRunnableDemand;
        const control = e.class == .control;
        const control_count = @min(@as(usize, 4), self.config.max_operations / 2);
        const count_limit = if (control) control_count else self.config.max_operations - control_count;
        const count_used = if (control) self.metrics.control_operations else self.metrics.operations - self.metrics.control_operations;
        const byte_limit = if (control) self.config.control_bytes else self.config.max_bytes - self.config.control_bytes;
        const byte_used = if (control) self.metrics.control_retained_bytes else self.metrics.retained_bytes - self.metrics.control_retained_bytes;
        if (e.protected_cleanup_root) {
            // Cleanup roots share the control partition, but cannot consume
            // its last operation or more than half its retained byte reserve.
            const cleanup_limit = @min(@as(usize, 2), control_count -| 1);
            if (!control or self.metrics.cleanup_operations >= cleanup_limit or
                e.bytes > (self.config.control_bytes / 2) -| self.metrics.cleanup_retained_bytes)
            {
                self.metrics.rejected +|= 1;
                return error.ResourceTemporarilyUnavailable;
            }
        }
        if (count_used >= count_limit or e.bytes > byte_limit - byte_used) {
            self.metrics.rejected +|= 1;
            return error.SchedulerFull;
        }
        e.reserved_control = control;
        if (control) {
            self.metrics.control_operations += 1;
            self.metrics.control_retained_bytes += e.bytes;
        }
        if (e.protected_cleanup_root) {
            self.metrics.cleanup_operations += 1;
            self.metrics.cleanup_retained_bytes += e.bytes;
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

    fn checkAuthority(self: *Scheduler, e: *const Entry) !void {
        e.options.check() catch |err| {
            // A parent worker's IO cancellation may be independent of the
            // explicit token copied into children. Publish its observed cause
            // to the retained family before returning to that worker.
            self.lock();
            const family_owned = e.scheduler == self and (e.children != 0 or e.parent != null);
            self.mutex.unlock();
            if (family_owned) self.failWait(@constCast(e), err, true);
            return err;
        };
        self.lock();
        defer self.mutex.unlock();
        try self.checkAncestorsLocked(e);
    }

    fn checkAncestorsLocked(self: *Scheduler, e: *const Entry) !void {
        var ancestor = e.parent;
        while (ancestor) |parent| : (ancestor = parent.parent) {
            if (parent.scheduler != self) return error.InvalidRunnableTransition;
            if (parent.state == .stopped) return parent.failure orelse error.InvalidRunnableTransition;
        }
    }

    pub fn state(self: *Scheduler, e: *const Entry) State {
        self.lock();
        defer self.mutex.unlock();
        return e.state;
    }

    /// Initial acquisition retires an unpublished entry on failure. A caller
    /// cannot use this API to discard retained completion/cleanup consumers.
    pub fn wait(self: *Scheduler, e: *Entry) !void {
        self.lock();
        const completing = e.scheduler == self and (e.retainedPhase() or e.children != 0);
        self.mutex.unlock();
        if (completing) return self.waitRetained(e);
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
            .running => self.used[self.cpuIndex(e)] -= e.slots,
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
        // Fail-fast initial consumers may run under a transport lock. Never
        // suspend that caller for CPU; unpublished rejection releases its
        // reservation. Published reentry still uses its bounded fair queue.
        if (!retain and !options.allow_wait and e.waited) {
            self.failWait(e, error.ResourceTemporarilyUnavailable, false);
            return error.ResourceTemporarilyUnavailable;
        }
        const start = options.now();
        const until = @min(options.deadline_ns orelse std.math.maxInt(u64), e.queued_since_ns +| @as(u64, self.config.max_wait_ms) * std.time.ns_per_ms);
        defer {
            const elapsed = options.now() -| start;
            self.lock();
            self.metrics.wait_ns +|= elapsed;
            self.mutex.unlock();
        }
        while (true) {
            self.checkAuthority(e) catch |err| {
                self.failWait(e, err, retain);
                return err;
            };
            self.lock();
            const still_owned = e.scheduler == self;
            const s = e.state;
            const failure = e.failure;
            const closed = self.closed;
            const completing = e.retainedPhase();
            const waited = e.waited;
            self.mutex.unlock();
            if (!still_owned) return error.InvalidRunnableTransition;
            if (waited and options.now() >= until) {
                self.failWait(e, error.RunnableWaitTimeout, retain);
                return error.RunnableWaitTimeout;
            }
            if (s == .running and (!closed or completing)) {
                self.checkAuthority(e) catch |err| {
                    self.failWait(e, err, retain);
                    return err;
                };
                return;
            }
            if (closed and !completing) {
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
        self.used[self.cpuIndex(e)] -= e.slots;
        self.metrics.yields +|= 1;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        if (self.closed and !e.retainedPhase()) {
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            self.dispatch();
            return error.SchedulerClosed;
        }
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    /// Change a running capsule's CPU team demand. Call only after joining all
    /// children of the old team. Growth relinquishes the old grant and queues
    /// the full demand fairly; count/bytes stay reserved through denied reentry.
    pub fn resizeTeam(self: *Scheduler, e: *Entry, slots: usize) !void {
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or e.children != 0) return error.InvalidRunnableTransition;
        const capacity = self.capacityFor(e, e.class);
        if (slots == 0 or slots > capacity) return error.InvalidRunnableDemand;
        // Only a separately reserved/activated retained phase may resize
        // after close. Other live work keeps its existing grant until finish.
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
        if (slots <= e.slots) {
            self.used[self.cpuIndex(e)] -= e.slots - slots;
            e.slots = slots;
            // Shrinking retains the current grant; no old queue deadline
            // applies to this synchronous return of CPU capacity.
            e.waited = false;
            self.dispatch();
            return;
        }
        self.used[self.cpuIndex(e)] -= e.slots;
        e.slots = slots;
        e.bypasses = 0;
        self.sequence +|= 1;
        e.sequence = self.sequence;
        self.enqueue(e, now);
        self.dispatch();
        e.waited = e.state == .queued;
    }
    pub fn block(self: *Scheduler, e: *Entry, reason: State) !void {
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or
            (reason != .resource_blocked and reason != .output_blocked)) return error.InvalidRunnableTransition;
        self.used[self.cpuIndex(e)] -= e.slots;
        if (self.closed and !e.retainedPhase()) {
            e.state = .stopped;
            e.failure = error.SchedulerClosed;
            e.ready.set(e.options.io);
            self.dispatch();
            return error.SchedulerClosed;
        }
        e.state = reason;
        if (reason == .resource_blocked) self.metrics.resource_blocked += 1 else self.metrics.output_blocked += 1;
        self.dispatch();
    }
    pub fn unblock(self: *Scheduler, e: *Entry) !void {
        // A blocked operation still owns its reservations, but cancellation,
        // expiry or family failure must be observed before dispatch can grant
        // CPU. Waiting checks authority again after any actual suspension.
        self.checkAuthority(e) catch |err| {
            self.failWait(e, err, true);
            return err;
        };
        const now = e.options.now();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self) return error.InvalidRunnableTransition;
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
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
        if (self.closed and !e.retainedPhase()) return error.SchedulerClosed;
        if (e.state != .resource_blocked and e.state != .output_blocked) return error.InvalidRunnableTransition;
        if ((e.children != 0 or e.parent != null) and class != e.class) return error.InvalidRunnableTransition;
        // Retained completion/cleanup cannot promote its capacity partition.
        if (e.retainedPhase() and class != e.class) return error.InvalidRunnableTransition;
        const capacity = self.capacityFor(e, class);
        if (e.slots > capacity) return error.InvalidRunnableDemand;
        e.class = class;
    }

    /// Reserve completion ownership while the original foreground execution
    /// is still running, before any potentially durable proposal. This uses
    /// the already bounded entry, operation and byte reservation, not a new
    /// foreground or control admission. It does not prove a storage decision.
    pub fn reserveCompletion(self: *Scheduler, e: *Entry, completion: admission.Options) !void {
        if (completion.deadline_ns == null or completion.cancellation != null or !completion.allow_wait)
            return error.InvalidArgument;
        try e.options.check();
        try completion.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or
            (e.class != .write and e.class != .control) or
            e.completion_options != null or e.retainedPhase() or e.parent != null) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        if (completion.retained_bytes > e.bytes) return error.InvalidRunnableDemand;
        var options = completion;
        options.retained_bytes = e.bytes;
        e.completion_options = options;
    }

    /// Reserve cleanup while foreground execution still owns a live grant.
    /// The independent task, callback context, allocator and all physical work
    /// must already be retained by the caller before publishing any resource.
    /// Operation/bytes remain the existing bounded entry; no new admission or
    /// control promotion is available when cleanup eventually activates.
    pub fn reserveCleanup(self: *Scheduler, e: *Entry, cleanup: admission.Options) !void {
        return self.reserveCleanupChecked(e, cleanup, null);
    }

    /// Preauthorize a finite physical cleanup duration before publishing a
    /// resource whose idle lifetime can exceed the foreground query deadline.
    /// This starts exactly once, including fair CPU wait, at activation. It
    /// never refreshes query authority or an already active cleanup deadline.
    pub fn reserveCleanupFor(self: *Scheduler, e: *Entry, cleanup: admission.Options, duration_ns: u64) !void {
        return self.reserveCleanupChecked(e, cleanup, duration_ns);
    }

    fn reserveCleanupChecked(self: *Scheduler, e: *Entry, cleanup: admission.Options, duration_ns: ?u64) !void {
        if (cleanup.cancellation != null or !cleanup.allow_wait) return error.InvalidArgument;
        if (duration_ns) |duration| {
            if (duration == 0 or cleanup.deadline_ns != null) return error.InvalidArgument;
            _ = std.math.add(u64, cleanup.now(), duration) catch return error.InvalidArgument;
        } else if (cleanup.deadline_ns == null) return error.InvalidArgument;
        try e.options.check();
        try cleanup.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or e.cleanup_options != null or e.retainedPhase() or e.parent != null) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        if (cleanup.retained_bytes > e.bytes) return error.InvalidRunnableDemand;
        var options = cleanup;
        options.retained_bytes = e.bytes;
        e.cleanup_options = options;
        e.cleanup_duration_ns = duration_ns;
    }

    /// After fencing foreground children, transfer the retained entry to its
    /// preauthorized cleanup authority. Unlike durable completion, cleanup must
    /// activate after foreground cancellation, denied reentry or owner close.
    /// A stopped foreground is never resumed with its expired query authority.
    /// Fair CPU reentry can fail; then the cleanup owner still retains all bytes
    /// and must join or transfer physical ownership before owning finish.
    pub fn activateCleanup(self: *Scheduler, e: *Entry) !void {
        var cleanup = e.cleanup_options orelse return error.InvalidRunnableTransition;
        const now = cleanup.now();
        if (e.cleanup_duration_ns) |duration| {
            cleanup.deadline_ns = std.math.add(u64, now, duration) catch return error.InvalidArgument;
        }
        try cleanup.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.retainedPhase() or e.children != 0 or e.parent != null) return error.InvalidRunnableTransition;
        switch (e.state) {
            .running => {},
            .queued => self.unlink(e),
            .resource_blocked => self.metrics.resource_blocked -= 1,
            .output_blocked => self.metrics.output_blocked -= 1,
            .stopped => {},
            .fresh, .finished => return error.InvalidRunnableTransition,
        }
        const running = e.state == .running;
        e.options = cleanup;
        e.cleanup_options = cleanup;
        e.cleanup_duration_ns = null;
        e.cleanup_active = true;
        // Inactive write completion was never accepted. Its authorization
        // cannot survive a transfer into physical cleanup.
        e.completion_options = null;
        e.failure = null;
        e.bypasses = 0;
        e.waited = false;
        if (!running) {
            self.sequence +|= 1;
            e.sequence = self.sequence;
            self.enqueue(e, now);
            self.dispatch();
            e.waited = e.state == .queued;
        }
    }

    /// Enter the independent completion phase before possible durable
    /// acceptance. The caller now owns all joins or an authoritative durable
    /// transfer on every exit, including an ambiguous storage outcome.
    pub fn activateCompletion(self: *Scheduler, e: *Entry) !void {
        try e.options.check();
        const completion = e.completion_options orelse return error.InvalidRunnableTransition;
        try completion.check();
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler != self or e.state != .running or e.retainedPhase() or e.children != 0 or e.parent != null) return error.InvalidRunnableTransition;
        if (self.closed) return error.SchedulerClosed;
        const options = e.completion_options orelse return error.InvalidRunnableTransition;
        e.options = options;
        e.completion_active = true;
        // No queue time from the foreground phase governs this retained grant.
        e.waited = false;
    }

    pub fn finish(self: *Scheduler, e: *Entry) void {
        self.lock();
        defer self.mutex.unlock();
        if (e.scheduler == null) return;
        std.debug.assert(e.scheduler == self and e.children == 0);
        switch (e.state) {
            .queued => self.unlink(e),
            .running => self.used[self.cpuIndex(e)] -= e.slots,
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
        if (e.parent) |parent| {
            std.debug.assert(parent.scheduler == self and parent.children != 0);
            parent.children -= 1;
            self.metrics.child_operations -= 1;
            e.parent = null;
        }
        if (e.reserved_control) {
            self.metrics.control_operations -= 1;
            self.metrics.control_retained_bytes -= e.bytes;
        }
        if (e.protected_cleanup_root) {
            self.metrics.cleanup_operations -= 1;
            self.metrics.cleanup_retained_bytes -= e.bytes;
            e.protected_cleanup_root = false;
        }
        e.state = .finished;
        e.scheduler = null;
        e.completion_options = null;
        e.completion_active = false;
        e.cleanup_options = null;
        e.cleanup_duration_ns = null;
        e.cleanup_active = false;
        self.dispatch();
    }
    pub fn close(self: *Scheduler) void {
        self.lock();
        defer self.mutex.unlock();
        self.closed = true;
        var next = self.entries;
        while (next) |e| : (next = e.all_next) {
            if (e.retainedPhase()) continue;
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
        self.dispatch();
    }
    pub fn stats(self: *Scheduler) Stats {
        self.lock();
        defer self.mutex.unlock();
        var result = self.metrics;
        result.runnable_slots = self.ordinaryUsed() + self.used[4] + self.used[5];
        result.control_slots = self.used[4] + self.used[5];
        result.cleanup_runnable_slots = self.used[5];
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
    /// Trusted local adapter to an already admitted opaque owner. The adapter
    /// and original owner outlive every physical borrower and joined child.
    /// A root borrow's finish only retires the adapter; owned child adapters
    /// retire their owner-produced token after the worker has physically joined.
    borrowed: ?Borrow = null,
    child_pending: bool = false,
    quantum_start_ns: u64 = 0,
    checkpoints: u32 = 0,
    /// Changes only after a successful grant. Readers use it to invalidate
    /// authority checks across cooperative yields and resource/output waits.
    resume_generation: u64 = 0,

    pub const Borrow = struct {
        ptr: *anyopaque,
        class: Class,
        current_class: ?*const fn (*anyopaque) anyerror!Class = null,
        checkpoint: *const fn (*anyopaque) anyerror!void,
        check_lifetime: *const fn (*anyopaque) anyerror!void,
        park: *const fn (*anyopaque, State) anyerror!void,
        enter: *const fn (*anyopaque, Class) anyerror!void,
        reserve_child: *const fn (*anyopaque, std.Io, u64, *Execution) anyerror!void,
        activate_child: *const fn (*anyopaque) anyerror!void,
        finish: *const fn (*anyopaque) void,
    };

    pub fn bindBorrowed(self: *Execution, borrow: Borrow) !void {
        if (self.entry != null or self.borrowed != null or self.child_pending) return error.InvalidRunnableTransition;
        self.borrowed = borrow;
    }

    pub fn retainedClass(self: *const Execution) !Class {
        if (self.borrowed) |borrow| return if (borrow.current_class) |current| current(borrow.ptr) else borrow.class;
        const entry = if (self.entry) |*value| value else return error.InvalidRunnableTransition;
        const owner = entry.scheduler orelse return error.InvalidRunnableTransition;
        owner.lock();
        defer owner.mutex.unlock();
        return entry.class;
    }

    pub fn start(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class) !void {
        try self.startWithSlots(scheduler, options, class, 1);
    }
    /// One stable capsule can account for a bounded CPU team. Its demand stays
    /// attached across yields and waits, independently of retained memory.
    pub fn startWithSlots(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class, slots: usize) !void {
        return self.startRoot(scheduler, options, class, slots, false);
    }

    /// Independent finite cleanup authority, never a refreshed read scope.
    /// Admission is fail-fast and preserves global health/control headroom.
    pub fn startCleanupRoot(self: *Execution, scheduler: *Scheduler, options: admission.Options) !void {
        if (options.deadline_ns == null or options.allow_wait) return error.InvalidArgument;
        if (options.deadline_ns.? > options.now() +| (250 * std.time.ns_per_ms)) return error.InvalidArgument;
        return self.startRoot(scheduler, options, .control, 1, true);
    }

    fn startRoot(self: *Execution, scheduler: ?*Scheduler, options: admission.Options, class: Class, slots: usize, protected_cleanup: bool) !void {
        std.debug.assert(self.entry == null and self.borrowed == null);
        const owner = scheduler orelse return;
        self.entry = .{ .options = options, .class = class, .slots = slots, .bytes = options.retained_bytes, .protected_cleanup_root = protected_cleanup };
        errdefer self.finish();
        try owner.submit(&self.entry.?);
        // A new cleanup root has no published consumer yet. Respect its
        // fail-fast acquisition; only subsequent reentry retains ownership.
        if (protected_cleanup) try owner.waitImpl(&self.entry.?, false) else try owner.wait(&self.entry.?);
        self.resume_generation +%= 1;
        self.quantum_start_ns = options.now();
    }
    /// Stable child metadata and the independent worker IO must outlive every
    /// physical task join. Finishing the parent with live children is invalid.
    pub fn startChild(self: *Execution, parent: *Execution, io: std.Io, retained_bytes: u64) !void {
        try self.reserveChild(parent, io, retained_bytes);
        errdefer self.finish();
        try self.activateChild();
    }

    /// Reserve before task publication without waiting for CPU on the parent
    /// coordinator. The independent worker activates after publication.
    pub fn reserveChild(self: *Execution, parent: *Execution, io: std.Io, retained_bytes: u64) !void {
        if (self.entry != null or self.borrowed != null or self == parent or parent.child_pending) return error.InvalidRunnableTransition;
        if (retained_bytes < @sizeOf(Execution)) return error.InvalidRunnableDemand;
        if (parent.borrowed) |borrow| {
            try borrow.reserve_child(borrow.ptr, io, retained_bytes, self);
            self.child_pending = true;
            return;
        }
        const parent_entry = if (parent.entry) |*entry| entry else return error.InvalidRunnableTransition;
        const owner = try schedulerFor(parent_entry, true);
        self.entry = .{ .options = parent_entry.options, .class = parent_entry.class, .bytes = retained_bytes };
        errdefer self.finish();
        try owner.submitChild(parent_entry, &self.entry.?, io);
        self.child_pending = true;
    }

    /// Published task ownership survives rejected activation until physical
    /// join and owning finish. This cannot replay an already activated child.
    pub fn activateChild(self: *Execution) !void {
        if (self.borrowed) |borrow| {
            if (!self.child_pending) return error.InvalidRunnableTransition;
            self.child_pending = false;
            try borrow.activate_child(borrow.ptr);
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*entry| entry else return error.InvalidRunnableTransition;
        if (!self.child_pending or e.parent == null) {
            _ = try schedulerFor(e, false);
            return error.InvalidRunnableTransition;
        }
        self.child_pending = false;
        const owner = try schedulerFor(e, false);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.checkpoints = 0;
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
    /// Read original explicit lifetime and retained family state from another
    /// physical worker. This neither requires CPU nor observes the caller's
    /// thread-local IO cancellation, which must not stop a borrowed parent.
    pub fn checkLifetime(self: *const Execution) !void {
        if (self.borrowed) |borrow| return borrow.check_lifetime(borrow.ptr);
        const e = if (self.entry) |*entry| entry else return error.InvalidRunnableTransition;
        const owner = e.scheduler orelse return error.InvalidRunnableTransition;
        const options = blk: {
            owner.lock();
            defer owner.mutex.unlock();
            if (e.scheduler != owner) return error.InvalidRunnableTransition;
            if (e.state == .stopped) return e.failure orelse error.InvalidRunnableTransition;
            try owner.checkAncestorsLocked(e);
            break :blk e.options;
        };
        if (options.cancellation) |token| if (token.requested(token.ptr)) return error.Canceled;
        if (options.deadline_ns) |deadline| if (options.now() >= deadline) return error.DeadlineExceeded;
    }

    /// Reserve bounded parallel CPU on this same execution before publishing
    /// child work. Children borrow the team grant and must all join before any
    /// resize, park, checkpoint or finish. Physical IO ownership and accepted
    /// durable completion require their separate capabilities.
    pub fn resizeTeam(self: *Execution, slots: usize) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (slots == 0) return error.InvalidRunnableDemand;
        if (self.borrowed != null) return error.UnsupportedOperation;
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try owner.checkAuthority(e);
        try owner.resizeTeam(e, slots);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.checkpoints = 0;
    }
    pub fn checkpoint(self: *Execution) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| {
            try borrow.checkpoint(borrow.ptr);
            // Opaque owners may yield on every checkpoint. Conservatively
            // invalidate authority caches after each successful callback.
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, true);
        try owner.checkAuthority(e);
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
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |borrow| return borrow.park(borrow.ptr, reason);
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        try owner.block(e, reason);
    }
    pub fn enter(self: *Execution, class: Class) !void {
        if (self.child_pending) return error.InvalidRunnableTransition;
        if (self.borrowed) |*borrow| {
            try borrow.enter(borrow.ptr, class);
            borrow.class = class;
            self.resume_generation +%= 1;
            return;
        }
        const e = if (self.entry) |*entry| entry else return;
        const owner = try schedulerFor(e, false);
        owner.checkAuthority(e) catch |err| {
            owner.failWait(e, err, true);
            return err;
        };
        try owner.reclassifyBlocked(e, class);
        try owner.unblock(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.checkpoints = 0;
    }
    /// The independent Io/deadline and all retained resources must be owned
    /// through joined completion. Foreground cancellation is deliberately not
    /// copied into this capability. This is local trusted authority only.
    pub fn reserveCompletion(self: *Execution, completion: admission.Options) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        try owner.checkAuthority(e);
        try owner.reserveCompletion(e, completion);
    }
    pub fn activateCompletion(self: *Execution) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        // Activation must precede acceptance and may not escape a foreground
        // fence which has already failed. Afterwards only completion fences
        // govern the still-retained root.
        try owner.checkAuthority(e);
        const options = e.completion_options orelse return error.InvalidRunnableTransition;
        try options.check();
        try owner.activateCompletion(e);
        self.quantum_start_ns = options.now();
        self.checkpoints = 0;
    }
    /// Read the retained completion fence without issuing a new deadline.
    /// Foreground cancellation and scheduler close do not replace its clock.
    pub fn completionRemainingNs(self: *Execution) !u64 {
        const entry = if (self.entry) |*value| value else return error.ExecutionWaitUnsupported;
        if (!entry.completion_active) return error.ExecutionWaitUnsupported;
        const options = entry.completion_options orelse return error.InvalidRunnableTransition;
        try options.check();
        const deadline = options.deadline_ns orelse return error.InvalidArgument;
        const remaining = deadline -| options.now();
        if (remaining == 0) return error.DeadlineExceeded;
        return remaining;
    }

    pub fn reserveCleanup(self: *Execution, cleanup: admission.Options) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        try owner.reserveCleanup(e, cleanup);
    }
    pub fn reserveCleanupFor(self: *Execution, cleanup: admission.Options, duration_ns: u64) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        const owner = try schedulerFor(e, true);
        try owner.reserveCleanupFor(e, cleanup, duration_ns);
    }
    pub fn activateCleanup(self: *Execution) !void {
        const e = if (self.entry) |*entry| entry else return error.ExecutionWaitUnsupported;
        // A stopped foreground deliberately retains its entry for cleanup.
        // Validate ownership in the scheduler rather than rejecting that state
        // through schedulerFor's foreground transition check.
        const owner = e.scheduler orelse return error.InvalidRunnableTransition;
        try owner.activateCleanup(e);
        try owner.waitRetained(e);
        self.resume_generation +%= 1;
        self.quantum_start_ns = e.options.now();
        self.checkpoints = 0;
    }
    pub fn finish(self: *Execution) void {
        if (self.borrowed) |borrow| borrow.finish(borrow.ptr);
        self.borrowed = null;
        if (self.entry) |*e| if (e.scheduler) |owner| owner.finish(e);
        self.entry = null;
        self.child_pending = false;
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

test "runnable scheduler running write retains completion ownership through cancellation and shutdown" {
    const Flag = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    var flag: Flag = .{};
    var scheduler = try Scheduler.init(.{ .slots = 3, .max_operations = 4 });
    defer scheduler.close();
    var write: Execution = .{};
    defer write.finish();
    try write.startWithSlots(&scheduler, .{
        .io = std.testing.io,
        .retained_bytes = 4096,
        .cancellation = .{ .ptr = &flag, .requested = Flag.requested },
    }, .write, 2);
    const generation = write.resume_generation;
    // The native write has crossed its own acceptance fence. Until that owner
    // reports completion, foreground cancellation must not reclaim its grant.
    // This tests the scheduler side of that contract, not WAL/Raft acceptance.
    flag.canceled = true;
    try std.testing.expectError(error.Canceled, write.checkpoint());
    try std.testing.expectEqual(State.running, scheduler.state(&write.entry.?));
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 4096), scheduler.stats().retained_bytes);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
    try health.checkpoint();
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    health.finish();
    scheduler.close();
    // Close stops admission and blocked reentry; live native execution is
    // joined by its caller and remains charged until that caller finishes.
    try std.testing.expectEqual(State.running, scheduler.state(&write.entry.?));
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 4096), scheduler.stats().retained_bytes);
    var fresh: Execution = .{};
    defer fresh.finish();
    try std.testing.expectError(error.SchedulerClosed, fresh.start(&scheduler, .{ .io = std.testing.io }, .write));
    try std.testing.expectEqual(generation, write.resume_generation);
    write.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable scheduler team growth releases old CPU while preserving fair retained ownership" {
    var scheduler = try Scheduler.init(.{ .slots = 5, .max_operations = 8 });
    defer scheduler.close();
    var parent = testEntry(.write, 1, 19);
    defer scheduler.finish(&parent);
    var peer = testEntry(.query, 3, 11);
    defer scheduler.finish(&peer);
    try scheduler.submit(&parent);
    try scheduler.wait(&parent);
    try scheduler.submit(&peer);
    try scheduler.wait(&peer);
    try scheduler.resizeTeam(&parent, 4);
    try std.testing.expectEqual(State.queued, scheduler.state(&parent));
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 30), scheduler.stats().retained_bytes);
    var health = testEntry(.control, 1, 7);
    defer scheduler.finish(&health);
    try scheduler.submit(&health);
    try scheduler.wait(&health);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    scheduler.finish(&health);
    scheduler.finish(&peer);
    try scheduler.waitRetained(&parent);
    try std.testing.expectEqual(State.running, scheduler.state(&parent));
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 19), scheduler.stats().retained_bytes);
    try scheduler.resizeTeam(&parent, 1);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 19), scheduler.stats().retained_bytes);
}

test "runnable execution team reservations preserve original fencing and denied reentry ownership" {
    const Flag = struct {
        canceled: bool = false,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const @This(), @ptrCast(@alignCast(raw))).canceled;
        }
    };
    var flag: Flag = .{};
    var scheduler = try Scheduler.init(.{ .slots = 5, .max_operations = 8 });
    defer scheduler.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 23, .cancellation = .{ .ptr = &flag, .requested = Flag.requested } }, .write);
    const before = execution.resume_generation;
    try execution.resizeTeam(4);
    try std.testing.expectEqual(before + 1, execution.resume_generation);
    try std.testing.expectEqual(@as(usize, 4), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableDemand, execution.resizeTeam(0));
    try std.testing.expectError(error.InvalidRunnableDemand, execution.resizeTeam(5));
    try std.testing.expectEqual(@as(usize, 4), execution.entry.?.slots);
    try execution.resizeTeam(1);
    var peer: Execution = .{};
    defer peer.finish();
    try peer.startWithSlots(&scheduler, .{ .io = std.testing.io }, .query, 3);
    // Split the queueing step from the wait to inject cancellation deterministically.
    try scheduler.resizeTeam(&execution.entry.?, 4);
    flag.canceled = true;
    try std.testing.expectError(error.Canceled, scheduler.waitRetained(&execution.entry.?));
    try std.testing.expectEqual(State.stopped, scheduler.state(&execution.entry.?));
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    flag.canceled = false;
    try std.testing.expectError(error.Canceled, execution.resizeTeam(1));
    execution.finish();
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}

test "runnable scheduler team rejection and shutdown preserve existing running grant" {
    var scheduler = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .max_operations = 8 });
    defer scheduler.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 31 }, .write);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&scheduler, .{ .io = std.testing.io }, .control);
    try std.testing.expectError(error.InvalidRunnableDemand, health.resizeTeam(2));
    try std.testing.expectEqual(@as(usize, 1), health.entry.?.slots);
    health.finish();
    scheduler.close();
    try std.testing.expectError(error.SchedulerClosed, execution.resizeTeam(3));
    try std.testing.expectEqual(State.running, scheduler.state(&execution.entry.?));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 31), scheduler.stats().retained_bytes);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable execution team shrink does not reuse an expired prior queue wait" {
    VirtualClock.value = 0;
    var scheduler = try Scheduler.init(.{ .slots = 5, .max_operations = 8, .max_wait_ms = 1 });
    defer scheduler.close();
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = VirtualClock.now, .retained_bytes = 13 }, .write);
    var peer = testEntry(.query, 3, 0);
    defer scheduler.finish(&peer);
    try scheduler.submit(&peer);
    try scheduler.wait(&peer);
    try scheduler.resizeTeam(&execution.entry.?, 4);
    try std.testing.expectEqual(State.queued, scheduler.state(&execution.entry.?));
    scheduler.finish(&peer);
    try scheduler.waitRetained(&execution.entry.?);
    // The team has computed for longer than its old queue timeout. Giving CPU
    // back is not a new wait and must not stop an otherwise valid execution.
    VirtualClock.value = 2 * std.time.ns_per_ms;
    try execution.resizeTeam(1);
    try std.testing.expectEqual(State.running, scheduler.state(&execution.entry.?));
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 13), scheduler.stats().retained_bytes);
}

test "runnable execution completion reservation survives foreground cancellation and close" {
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    Clock.now_ns = 0;
    var canceled = std.atomic.Value(bool).init(false);
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .checkpoint_budget = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try execution.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 256, .deadline_ns = 5, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .write, 3);
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
    try execution.activateCompletion();
    canceled.store(true, .release);
    Clock.now_ns = 10;
    try execution.park(.resource_blocked);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().resource_blocked);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), owner.stats().retained_bytes);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io }, .control);
    health.finish();
    owner.close();
    try std.testing.expectEqual(State.resource_blocked, execution.entry.?.state);
    try execution.enter(.write);
    try execution.checkpoint();
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    try execution.park(.output_blocked);
    try execution.enter(.write);
    try execution.resizeTeam(2);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    var fresh: Execution = .{};
    defer fresh.finish();
    try std.testing.expectError(error.SchedulerClosed, fresh.start(&owner, .{ .io = std.testing.io }, .write));
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
}

test "runnable scheduler completion queued before close retains fair bounded ownership" {
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    var completion: Execution = .{};
    defer completion.finish();
    try completion.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .write, 3);
    const deadline = completion.entry.?.options.now() + 10 * std.time.ns_per_s;
    try completion.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline });
    try completion.activateCompletion();
    try completion.park(.resource_blocked);
    var blocker = testEntry(.query, 3, 11);
    defer owner.finish(&blocker);
    try owner.submit(&blocker);
    try owner.unblock(&completion.entry.?);
    try std.testing.expectEqual(State.queued, completion.entry.?.state);
    var unaccepted = testEntry(.write, 1, 5);
    defer owner.finish(&unaccepted);
    try owner.submit(&unaccepted);
    owner.close();
    try std.testing.expectEqual(State.queued, completion.entry.?.state);
    try std.testing.expectEqual(State.stopped, unaccepted.state);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 33), owner.stats().retained_bytes);
    owner.finish(&blocker);
    try owner.waitRetained(&completion.entry.?);
    try std.testing.expectEqual(State.running, completion.entry.?.state);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    owner.finish(&unaccepted);
    completion.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable execution completion requires prior bounded independent authorization" {
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var flag = std.atomic.Value(bool).init(false);
    var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try std.testing.expectError(error.ExecutionWaitUnsupported, execution.reserveCompletion(.{ .io = std.testing.io }));
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 32, .cancellation = .{ .ptr = &flag, .requested = Cancel.requested } }, .write);
    const deadline = execution.entry.?.options.now() + 10 * std.time.ns_per_s;
    try std.testing.expectError(error.InvalidRunnableTransition, execution.activateCompletion());
    try std.testing.expectError(error.InvalidArgument, execution.reserveCompletion(.{ .io = std.testing.io }));
    try std.testing.expectError(error.InvalidArgument, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline, .allow_wait = false }));
    try std.testing.expectError(error.InvalidArgument, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline, .cancellation = .{ .ptr = &flag, .requested = Cancel.requested } }));
    try std.testing.expectError(error.InvalidRunnableDemand, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline, .retained_bytes = 33 }));
    try std.testing.expect(execution.entry.?.completion_options == null);
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline });
    try std.testing.expectError(error.InvalidRunnableTransition, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline }));
    flag.store(true, .release);
    try std.testing.expectError(error.Canceled, execution.activateCompletion());
    try std.testing.expect(!execution.entry.?.completion_active);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    flag.store(false, .release);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, execution.activateCompletion());
    try std.testing.expect(!execution.entry.?.completion_active);
    execution.finish();
    var reader_owner = try Scheduler.init(.{});
    try execution.start(&reader_owner, .{ .io = std.testing.io }, .query);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline }));
}

test "runnable execution expired completion retains consumers until joined finish" {
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    Clock.now_ns = 0;
    var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    var execution: Execution = .{};
    defer execution.finish();
    try execution.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 64, .native_now_ns = Clock.now }, .write, 2);
    try execution.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
    try execution.activateCompletion();
    try std.testing.expectError(error.InvalidRunnableTransition, execution.activateCompletion());
    try execution.park(.resource_blocked);
    try std.testing.expectError(error.InvalidRunnableTransition, execution.enter(.control));
    try std.testing.expectEqual(State.resource_blocked, execution.entry.?.state);
    Clock.now_ns = 100;
    try std.testing.expectError(error.DeadlineExceeded, execution.enter(.write));
    try std.testing.expectEqual(State.stopped, execution.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 64), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    Clock.now_ns = 0;
    try std.testing.expectError(error.DeadlineExceeded, execution.checkpoint());
    try std.testing.expectError(error.DeadlineExceeded, execution.park(.resource_blocked));
    owner.close();
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler closing foreground releases CPU to retained completion before finish" {
    for ([_]bool{ false, true }) |park| {
        var owner = try Scheduler.init(.{ .slots = 4, .control_slots = 1, .checkpoint_budget = 1 });
        var completion: Execution = .{};
        defer completion.finish();
        try completion.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .write, 3);
        const deadline = completion.entry.?.options.now() + 10 * std.time.ns_per_s;
        try completion.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline });
        try completion.activateCompletion();
        try completion.park(.resource_blocked);
        var foreground: Execution = .{};
        defer foreground.finish();
        try foreground.startWithSlots(&owner, .{ .io = std.testing.io, .retained_bytes = 11 }, .query, 3);
        try owner.unblock(&completion.entry.?);
        try std.testing.expectEqual(State.queued, completion.entry.?.state);
        owner.close();
        if (park) {
            try std.testing.expectError(error.SchedulerClosed, foreground.park(.resource_blocked));
        } else {
            try std.testing.expectError(error.SchedulerClosed, foreground.checkpoint());
        }
        // The stopped foreground still owns consumers and retained bytes.
        // Its released CPU must dispatch completion before those joins finish.
        try std.testing.expectEqual(State.stopped, foreground.entry.?.state);
        try std.testing.expectEqual(State.running, completion.entry.?.state);
        try owner.waitRetained(&completion.entry.?);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 28), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
        foreground.finish();
        completion.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    }
}

test "runnable scheduler initial wait cannot retire an active completion consumer" {
    const Clock = struct {
        var now_ns: u64 = 0;
        fn now() u64 {
            return now_ns;
        }
    };
    Clock.now_ns = 0;
    var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    const consumer = try std.testing.allocator.alloc(u8, 64);
    defer std.testing.allocator.free(consumer);
    var execution: Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = consumer.len }, .write);
    try execution.reserveCompletion(.{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 });
    try execution.activateCompletion();
    try execution.park(.resource_blocked);
    try owner.unblock(&execution.entry.?);
    Clock.now_ns = 100;
    // A legacy caller using initial wait still cannot discard a published
    // completion's live buffers. Only a joined owner may finish the root.
    try std.testing.expectError(error.DeadlineExceeded, owner.wait(&execution.entry.?));
    try std.testing.expectEqual(State.stopped, execution.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 64), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    consumer[0] = 17;
    try std.testing.expectEqual(@as(u8, 17), consumer[0]);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable execution independent completion task outlives actual foreground cancellation" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Fixture = struct {
        io: std.Io,
        owner: *Scheduler,
        execution: Execution = .{},
        ready: std.Io.Event = .unset,
        completion_go: std.Io.Event = .unset,
        never: std.Io.Event = .unset,
        completion: ?std.Io.Future(anyerror!void) = null,
        transferred: std.atomic.Value(bool) = .init(false),

        fn foreground(self: *@This()) anyerror!void {
            try self.execution.start(self.owner, .{ .io = self.io, .retained_bytes = 256 }, .write);
            var transferred = false;
            defer if (!transferred) self.execution.finish();
            // Reserve the independent task/resources before entering a phase
            // which might accept durable work. It initially waits for handoff.
            self.completion = try self.io.concurrent(complete, .{self});
            defer if (!transferred) {
                _ = self.completion.?.cancel(self.io) catch {};
            };
            const deadline = self.execution.entry.?.options.now() + 10 * std.time.ns_per_s;
            try self.execution.reserveCompletion(.{ .io = self.io, .deadline_ns = deadline });
            try self.execution.activateCompletion();
            try self.execution.park(.resource_blocked);
            self.transferred.store(true, .release);
            transferred = true;
            self.ready.set(self.io);
            // This is a real task cancellation point, not a mocked boolean.
            try self.never.wait(self.io);
            return error.TestUnexpectedResult;
        }

        fn complete(self: *@This()) anyerror!void {
            // A cancellation before the handoff leaves finish to the parent;
            // afterwards this joined task owns finish even if its wait fails.
            defer if (self.transferred.load(.acquire)) self.execution.finish();
            try self.completion_go.wait(self.io);
            // The original task was canceled and joined; this task has its
            // own cancellation scope on the still-owned executor instance.
            try self.io.checkCancel();
            try self.execution.enter(.write);
            try self.execution.checkpoint();
            try std.testing.expectEqual(@as(usize, 1), self.owner.stats().operations);
            try std.testing.expectEqual(@as(u64, 256), self.owner.stats().retained_bytes);
            try std.testing.expectEqual(@as(usize, 1), self.owner.stats().runnable_slots);
        }
    };
    for ([_]bool{ false, true }) |cancel_completion| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
        defer threaded.deinit();
        const io = threaded.io();
        var owner = try Scheduler.init(.{ .slots = 3, .control_slots = 1, .checkpoint_budget = 1 });
        var fixture = Fixture{ .io = io, .owner = &owner };
        var foreground = try io.concurrent(Fixture.foreground, .{&fixture});
        defer {
            _ = foreground.cancel(io) catch {};
            fixture.completion_go.set(io);
            if (fixture.completion) |*completion| _ = completion.cancel(io) catch {};
        }
        try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        var health: Execution = .{};
        defer health.finish();
        try health.start(&owner, .{ .io = io }, .control);
        health.finish();
        try std.testing.expectError(error.Canceled, foreground.cancel(io));
        owner.close();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().resource_blocked);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        if (cancel_completion) {
            // No storage has been contacted in this fixture. The failed task must
            // join before releasing the handed-off root and retained metadata.
            try std.testing.expectError(error.Canceled, fixture.completion.?.cancel(io));
        } else {
            fixture.completion_go.set(io);
            try fixture.completion.?.await(io);
        }
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    }
}

test "runnable scheduler cleanup reenters retained foreground states after cancellation and close" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    for ([_]State{ .running, .resource_blocked, .output_blocked, .queued, .stopped }) |before| {
        Clock.value = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1, .checkpoint_budget = 1 });
        defer owner.close();
        var query: Execution = .{};
        defer query.finish();
        var blocker: Execution = .{};
        defer blocker.finish();
        try query.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now, .retained_bytes = 32, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
        try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
        switch (before) {
            .running => {},
            .resource_blocked, .output_blocked => try query.park(before),
            .queued => {
                try query.park(.resource_blocked);
                try blocker.start(&owner, .{ .io = std.testing.io }, .query);
                try owner.unblock(&query.entry.?);
            },
            .stopped => owner.failWait(&query.entry.?, error.Canceled, true),
            else => unreachable,
        }
        try std.testing.expectEqual(before, query.entry.?.state);
        canceled = true;
        Clock.value = 10;
        owner.close();
        try owner.activateCleanup(&query.entry.?);
        try std.testing.expect(query.entry.?.cleanup_active);
        try std.testing.expect(!query.entry.?.completion_active);
        try std.testing.expectEqual(Class.query, query.entry.?.class);
        try std.testing.expectEqual(@as(u64, 32), owner.stats().retained_bytes);
        if (before == .queued) {
            // Cleanup does not steal the occupied CPU or enter control capacity.
            try std.testing.expectEqual(State.queued, query.entry.?.state);
            try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
            try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
        }
        blocker.finish();
        try owner.waitRetained(&query.entry.?);
        try query.checkpoint();
        try query.park(.resource_blocked);
        try std.testing.expectError(error.InvalidRunnableTransition, query.enter(.control));
        try query.enter(.query);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().control_slots);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 32), owner.stats().retained_bytes);
        query.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "runnable execution cleanup activation preserves bounded ownership after denied foreground reentry" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now, .retained_bytes = 13 }, .query);
    try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 100, .native_now_ns = Clock.now });
    try query.park(.resource_blocked);
    Clock.value = 10;
    try std.testing.expectError(error.DeadlineExceeded, query.enter(.query));
    try std.testing.expectEqual(State.stopped, query.entry.?.state);
    try std.testing.expectError(error.DeadlineExceeded, query.checkpoint());
    const generation = query.resume_generation;
    try query.activateCleanup();
    try std.testing.expect(query.resume_generation != generation);
    try query.checkpoint();
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, query.activateCleanup());
    Clock.value = 100;
    // Even the public initial-wait API cannot discard live cleanup consumers.
    try std.testing.expectError(error.DeadlineExceeded, owner.wait(&query.entry.?));
    try std.testing.expectEqual(State.stopped, query.entry.?.state);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
}

test "runnable execution cleanup requires prior independent finite authorization" {
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try std.testing.expectError(error.ExecutionWaitUnsupported, query.activateCleanup());
    try query.start(&owner, .{ .io = std.testing.io, .retained_bytes = 8 }, .query);
    const deadline = query.entry.?.options.now() + 10 * std.time.ns_per_s;
    try std.testing.expectError(error.InvalidRunnableTransition, query.activateCleanup());
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanup(.{ .io = std.testing.io }));
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline, .allow_wait = false }));
    try std.testing.expectError(error.InvalidRunnableDemand, query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline, .retained_bytes = 9 }));
    try std.testing.expect(query.entry.?.cleanup_options == null);
    try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline });
    try std.testing.expectEqual(@as(u64, 8), query.entry.?.cleanup_options.?.retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = deadline }));
    try query.activateCleanup();
    try std.testing.expectError(error.InvalidRunnableTransition, query.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = deadline }));
}

test "runnable scheduler cleanup cannot replace active durable completion or authorize expired cleanup" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var write: Execution = .{};
    defer write.finish();
    try write.start(&owner, .{ .io = std.testing.io, .retained_bytes = 8 }, .write);
    try write.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now });
    try write.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now });
    try write.activateCompletion();
    try std.testing.expectError(error.InvalidRunnableTransition, write.activateCleanup());
    try std.testing.expect(write.entry.?.completion_active);
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .retained_bytes = 13 }, .query);
    try query.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now });
    Clock.value = 5;
    owner.close();
    try std.testing.expectError(error.DeadlineExceeded, query.activateCleanup());
    try std.testing.expect(!query.entry.?.cleanup_active);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 21), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup consumes unused write completion and times out fairly under saturation" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    for ([_]bool{ false, true }) |completion_first| {
        Clock.value = 0;
        var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_wait_ms = 1 });
        defer owner.close();
        var write: Execution = .{};
        defer write.finish();
        const options = admission.Options{ .io = std.testing.io, .deadline_ns = 10 * std.time.ns_per_s, .native_now_ns = Clock.now };
        try write.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .retained_bytes = 13, .native_now_ns = Clock.now }, .write);
        if (completion_first) {
            try write.reserveCompletion(options);
            try write.reserveCleanup(options);
        } else {
            try write.reserveCleanup(options);
            try write.reserveCompletion(options);
        }
        try write.park(.resource_blocked);
        var blocker: Execution = .{};
        defer blocker.finish();
        try blocker.start(&owner, .{ .io = std.testing.io }, .query);
        Clock.value = 10;
        try owner.activateCleanup(&write.entry.?);
        try std.testing.expect(write.entry.?.completion_options == null);
        try std.testing.expect(!write.entry.?.completion_active);
        try std.testing.expectEqual(State.queued, write.entry.?.state);
        var control: Execution = .{};
        defer control.finish();
        try control.start(&owner, .{ .io = std.testing.io }, .control);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().control_slots);
        Clock.value = 10 + std.time.ns_per_ms;
        try std.testing.expectError(error.RunnableWaitTimeout, owner.wait(&write.entry.?));
        try std.testing.expectEqual(State.stopped, write.entry.?.state);
        try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
        try std.testing.expectError(error.RunnableWaitTimeout, write.activateCompletion());
        blocker.finish();
        control.finish();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    }
}

test "runnable scheduler cleanup activation budget survives idle expiry and cannot renew" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        requested: bool = false,
        fn check(raw: *const anyopaque) bool {
            const self: *const @This() = @ptrCast(@alignCast(raw));
            return self.requested;
        }
    };
    Clock.value = 0;
    var cancel: Cancel = .{};
    var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .deadline_ns = 5, .native_now_ns = Clock.now, .retained_bytes = 11, .cancellation = .{ .ptr = &cancel, .requested = Cancel.check } }, .query);
    try query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try query.park(.output_blocked);
    Clock.value = 500;
    cancel.requested = true;
    owner.close();
    try query.activateCleanup();
    try std.testing.expectEqual(Class.query, query.entry.?.class);
    try std.testing.expectEqual(@as(?u64, 510), query.entry.?.options.deadline_ns);
    try std.testing.expect(query.entry.?.cleanup_duration_ns == null);
    try std.testing.expectError(error.InvalidRunnableTransition, query.enter(.control));
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 11), owner.stats().retained_bytes);
    Clock.value = 509;
    try query.checkpoint();
    Clock.value = 510;
    try std.testing.expectError(error.DeadlineExceeded, query.checkpoint());
    try std.testing.expectError(error.DeadlineExceeded, query.activateCleanup());
    try std.testing.expectEqual(@as(?u64, 510), query.entry.?.options.deadline_ns);
    // Deadline failure retains the operation and all bytes until physical
    // resource owners have joined and the owning caller explicitly finishes.
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 11), owner.stats().retained_bytes);
    query.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup activation budget includes fair CPU wait and preserves control" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{ .slots = 2, .control_slots = 1, .max_wait_ms = 1000 });
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 13 }, .query);
    try query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try query.park(.resource_blocked);
    var blocker: Execution = .{};
    defer blocker.finish();
    try blocker.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .write);
    Clock.value = 500;
    try owner.activateCleanup(&query.entry.?);
    try std.testing.expectEqual(State.queued, query.entry.?.state);
    try std.testing.expectEqual(@as(?u64, 510), query.entry.?.options.deadline_ns);
    var control: Execution = .{};
    defer control.finish();
    try control.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .control);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().control_slots);
    Clock.value = 510;
    try std.testing.expectError(error.DeadlineExceeded, owner.waitRetained(&query.entry.?));
    try std.testing.expectEqual(State.stopped, query.entry.?.state);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
    blocker.finish();
    control.finish();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 13), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup activation budget rejects invalid and overflowing authority" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var query: Execution = .{};
    defer query.finish();
    try query.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 17 }, .query);
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 0));
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100 }, 10));
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now, .allow_wait = false }, 10));
    try std.testing.expectError(error.InvalidRunnableDemand, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 18 }, 10));
    Clock.value = std.math.maxInt(u64) - 5;
    try std.testing.expectError(error.InvalidArgument, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10));
    try std.testing.expect(query.entry.?.cleanup_options == null);
    Clock.value = 0;
    try query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try std.testing.expectError(error.InvalidRunnableTransition, query.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 20));
    Clock.value = std.math.maxInt(u64) - 5;
    try std.testing.expectError(error.InvalidArgument, query.activateCleanup());
    try std.testing.expect(!query.entry.?.cleanup_active);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
}

test "runnable scheduler cleanup activation budget cannot replace active durable completion" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    Clock.value = 0;
    var owner = try Scheduler.init(.{});
    defer owner.close();
    var write: Execution = .{};
    defer write.finish();
    try write.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 19 }, .write);
    try write.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = 1000, .native_now_ns = Clock.now });
    try write.reserveCleanupFor(.{ .io = std.testing.io, .native_now_ns = Clock.now }, 10);
    try write.activateCompletion();
    Clock.value = 500;
    try std.testing.expectError(error.InvalidRunnableTransition, write.activateCleanup());
    try std.testing.expect(write.entry.?.completion_active);
    try std.testing.expect(!write.entry.?.cleanup_active);
    try std.testing.expectEqual(@as(?u64, 1000), write.entry.?.options.deadline_ns);
    try std.testing.expectEqual(@as(u64, 19), owner.stats().retained_bytes);
}

test "runnable scheduler children independently park yield and retain their parent through nested finish" {
    var owner = try Scheduler.init(.{ .slots = 5, .checkpoint_budget = 1 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
    var first: Execution = .{};
    defer first.finish();
    try first.startChild(&parent, std.testing.io, @sizeOf(Execution));
    var second: Execution = .{};
    defer second.finish();
    try second.startChild(&parent, std.testing.io, @sizeOf(Execution));
    var nested: Execution = .{};
    defer nested.finish();
    try nested.startChild(&first, std.testing.io, @sizeOf(Execution));
    try std.testing.expectEqual(@as(usize, 4), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().child_operations);
    try std.testing.expectEqual(@as(usize, 2), parent.entry.?.children);
    try std.testing.expectEqual(@as(usize, 1), first.entry.?.children);
    try parent.park(.resource_blocked);
    try first.park(.output_blocked);
    const parent_generation = parent.resume_generation;
    try second.checkpoint();
    try nested.checkpoint();
    try std.testing.expectEqual(parent_generation, parent.resume_generation);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 4), owner.stats().operations);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.reclassifyBlocked(&parent.entry.?, .control));
    try parent.enter(.query);
    try first.enter(.query);
    try std.testing.expectError(error.InvalidRunnableTransition, parent.resizeTeam(2));
    nested.finish();
    first.finish();
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    second.finish();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    parent.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler children share bounded reservation and leave control capacity available" {
    var owner = try Scheduler.init(.{ .slots = 4, .max_operations = 6, .max_bytes = 128 * 1024 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 19 }, .inference);
    var first: Execution = .{};
    defer first.finish();
    var second: Execution = .{};
    defer second.finish();
    try first.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try second.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try first.park(.resource_blocked);
    try second.park(.output_blocked);
    const retained = owner.stats().retained_bytes;
    var rejected: Execution = .{};
    defer rejected.finish();
    try std.testing.expectError(error.SchedulerFull, rejected.startChild(&parent, std.testing.io, @sizeOf(Execution)));
    try std.testing.expect(rejected.entry == null);
    try std.testing.expectEqual(@as(usize, 2), parent.entry.?.children);
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false, .retained_bytes = 1 }, .control);
    try health.checkpoint();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().control_slots);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, first.enter(.inference));
    try std.testing.expectEqual(retained + 1, owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), parent.entry.?.children);
    second.finish();
    first.finish();
    parent.finish();
    health.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "runnable scheduler children inherit original deadline cancellation and stopped ancestor cause" {
    const Clock = struct {
        var value: u64 = 0;
        fn now() u64 {
            return value;
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.RunnableWaitTimeout }) |failure| {
        Clock.value = 0;
        var canceled = false;
        var owner = try Scheduler.init(.{ .slots = 4, .checkpoint_budget = 1 });
        defer owner.close();
        var parent: Execution = .{};
        defer parent.finish();
        try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23, .deadline_ns = 100, .native_now_ns = Clock.now, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
        var child: Execution = .{};
        defer child.finish();
        try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
        try std.testing.expectEqual(parent.entry.?.options.deadline_ns, child.entry.?.options.deadline_ns);
        const retained = owner.stats().retained_bytes;
        try child.park(.resource_blocked);
        if (failure == error.Canceled) canceled = true else if (failure == error.DeadlineExceeded) Clock.value = 100 else owner.failWait(&parent.entry.?, failure, true);
        try std.testing.expectError(failure, child.enter(.query));
        try std.testing.expectEqual(State.stopped, child.entry.?.state);
        try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
        try std.testing.expectEqual(retained, owner.stats().retained_bytes);
        try std.testing.expectError(failure, child.checkpoint());
        child.finish();
        try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    }
}

test "runnable scheduler children do not inherit completion cleanup or capacity promotion" {
    var owner = try Scheduler.init(.{ .slots = 4 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    const until = @as(u64, @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds)) + 10 * std.time.ns_per_s;
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 32 }, .write);
    try parent.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = until });
    try parent.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = until });
    var child: Execution = .{};
    defer child.finish();
    try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
    try std.testing.expect(child.entry.?.completion_options == null);
    try std.testing.expect(child.entry.?.cleanup_options == null);
    try std.testing.expectError(error.InvalidRunnableTransition, parent.activateCompletion());
    try std.testing.expectError(error.InvalidRunnableTransition, parent.activateCleanup());
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCompletion(.{ .io = std.testing.io, .deadline_ns = until }));
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCleanup(.{ .io = std.testing.io, .deadline_ns = until }));
    try child.park(.resource_blocked);
    try std.testing.expectError(error.InvalidRunnableTransition, child.enter(.control));
    child.finish();
    try parent.activateCompletion();
    var denied: Execution = .{};
    defer denied.finish();
    try std.testing.expectError(error.InvalidRunnableTransition, denied.startChild(&parent, std.testing.io, @sizeOf(Execution)));
    try std.testing.expect(denied.entry == null);
}

test "runnable scheduler children reject malformed reuse before changing existing authority" {
    var owner = try Scheduler.init(.{ .slots = 4 });
    defer owner.close();
    var root = testEntry(.query, 1, 37);
    defer owner.finish(&root);
    try owner.submit(&root);
    try owner.wait(&root);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.submitChild(&root, &root, std.testing.io));
    try std.testing.expectEqual(@as(u64, 37), root.bytes);
    try std.testing.expectEqual(Class.query, root.class);
    try std.testing.expect(root.parent == null);
    var existing = testEntry(.inference, 1, @sizeOf(Entry));
    defer owner.finish(&existing);
    try owner.submit(&existing);
    try owner.wait(&existing);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.submitChild(&root, &existing, std.testing.io));
    try std.testing.expectEqual(Class.inference, existing.class);
    try std.testing.expect(existing.parent == null);
    try std.testing.expectEqual(@as(usize, 0), root.children);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
}

test "runnable scheduler child fairness preserves an older independent root ahead of later fanout" {
    var owner = try Scheduler.init(.{ .slots = 3 });
    defer owner.close();
    var parent = testEntry(.query, 1, 31);
    defer owner.finish(&parent);
    try owner.submit(&parent);
    try owner.wait(&parent);
    var first = testEntry(.query, 1, @sizeOf(Entry));
    defer owner.finish(&first);
    try owner.submitChild(&parent, &first, std.testing.io);
    try owner.wait(&first);
    var independent = testEntry(.query, 1, 41);
    defer owner.finish(&independent);
    try owner.submit(&independent);
    var later = testEntry(.query, 1, @sizeOf(Entry));
    defer owner.finish(&later);
    try owner.submitChild(&parent, &later, std.testing.io);
    try std.testing.expectEqual(State.queued, owner.state(&independent));
    try std.testing.expectEqual(State.queued, owner.state(&later));
    try owner.block(&first, .resource_blocked);
    try owner.wait(&independent);
    try std.testing.expectEqual(State.queued, owner.state(&later));
    try std.testing.expectEqual(@as(usize, 2), parent.children);
    owner.finish(&independent);
    try owner.wait(&later);
    owner.finish(&later);
    owner.finish(&first);
    try std.testing.expectEqual(@as(usize, 0), parent.children);
}

test "runnable scheduler child physical worker join retains original parent and bytes after cancellation" {
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Worker = struct {
        child: *Execution,
        body: []u8,
        parked: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        freed: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) !void {
            defer {
                std.testing.allocator.free(self.body);
                self.freed.store(true, .release);
            }
            try self.child.park(.resource_blocked);
            self.parked.set(std.testing.io);
            self.go.waitUncancelable(std.testing.io);
            try self.child.enter(.query);
        }
    };
    var canceled: std.atomic.Value(bool) = .init(false);
    var owner = try Scheduler.init(.{ .slots = 3, .checkpoint_budget = 1 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = std.testing.io, .retained_bytes = 43, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
    var child: Execution = .{};
    defer child.finish();
    const child_bytes = @sizeOf(Execution) + 64;
    try child.startChild(&parent, std.testing.io, child_bytes);
    const body = try std.testing.allocator.alloc(u8, 64);
    var body_published = false;
    defer if (!body_published) std.testing.allocator.free(body);
    var worker = Worker{ .child = &child, .body = body };
    var future = try std.testing.io.concurrent(Worker.run, .{&worker});
    body_published = true;
    defer {
        worker.go.set(std.testing.io);
        future.await(std.testing.io) catch {};
    }
    try worker.parked.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try parent.checkpoint();
    canceled.store(true, .release);
    var health: Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
    try health.checkpoint();
    try std.testing.expect(!worker.freed.load(.acquire));
    try std.testing.expectEqual(@as(u64, 43 + child_bytes), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    worker.go.set(std.testing.io);
    try std.testing.expectError(error.Canceled, future.await(std.testing.io));
    try std.testing.expect(worker.freed.load(.acquire));
    // Physical completion is distinct from owning metadata retirement.
    try std.testing.expectEqual(@as(u64, 43 + child_bytes), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().child_operations);
    child.finish();
    try std.testing.expectEqual(@as(u64, 43), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    health.finish();
    parent.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "runnable scheduler child observes original parent worker IO cancellation without a separate token" {
    const Context = struct {
        var cancelled: bool = false;
        fn check(_: ?*anyopaque) std.Io.Cancelable!void {
            if (cancelled) return error.Canceled;
        }
    };
    var vtable = std.testing.io.vtable.*;
    vtable.checkCancel = Context.check;
    const parent_io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &vtable };
    Context.cancelled = false;
    var owner = try Scheduler.init(.{ .slots = 3 });
    defer owner.close();
    var parent: Execution = .{};
    defer parent.finish();
    try parent.start(&owner, .{ .io = parent_io, .retained_bytes = 13 }, .query);
    var child: Execution = .{};
    defer child.finish();
    try child.startChild(&parent, std.testing.io, @sizeOf(Execution));
    Context.cancelled = true;
    try std.testing.expectError(error.Canceled, parent.checkpoint());
    try std.testing.expectEqual(State.stopped, parent.entry.?.state);
    try std.testing.expectError(error.Canceled, child.checkpoint());
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    child.finish();
    parent.finish();
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}
