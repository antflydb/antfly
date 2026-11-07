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

const std = @import("std");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
/// Model-owned FIFO of ready cohorts. Stack-owned waiters allocate no queue
/// storage; a model handle outlives every waiter. Cancellation unlinks in O(1).
/// Only the head acquires the original gate, preserving all existing owners.
pub const Scheduler = struct {
    mutex: std.atomic.Mutex = .unlocked,
    head: ?*Waiter = null,
    tail: ?*Waiter = null,
    pub const Waiter = struct {
        previous: ?*Waiter = null,
        next: ?*Waiter = null,
        owner: ?*Scheduler = null,
        gate: ?*std.atomic.Mutex = null,
        acquired: bool = false,
        ready: bool = false,
    };
    fn lock(self: *Scheduler) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn append(self: *Scheduler, waiter: *Waiter, gate: *std.atomic.Mutex) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(waiter.owner == null);
        waiter.* = .{ .previous = self.tail, .owner = self, .gate = gate };
        if (self.tail) |tail| tail.next = waiter else self.head = waiter;
        self.tail = waiter;
    }
    fn take(self: *Scheduler, waiter: *Waiter) bool {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.head != waiter or !waiter.gate.?.tryLock()) return false;
        waiter.acquired = true;
        return true;
    }
    /// Reserve only the existing FIFO ready turn. No model/provider gate is
    /// held while the selected caller prepares exact resource admission.
    pub fn selectReady(self: *Scheduler, waiter: *Waiter, gate: *std.atomic.Mutex, control: Control) !void {
        try control.check();
        self.append(waiter, gate);
        errdefer leave(waiter);
        while (true) {
            self.lock();
            const ready = self.head == waiter;
            if (ready) waiter.ready = true;
            self.mutex.unlock();
            if (ready) break;
            try control.check();
            if (control.io) |io| try std.Io.sleep(io, .fromMilliseconds(1), .awake) else @import("antfly_platform").time.yieldNow();
        }
        try control.check();
    }
    /// Admission/preparation has finished. Acquire the same original model
    /// gate without holding scheduler metadata exclusion or changing FIFO turn.
    pub fn acquireSelected(waiter: *Waiter, control: Control) !void {
        const self = waiter.owner orelse return error.UnselectedExecutionCohort;
        self.lock();
        const valid = self.head == waiter and waiter.ready and !waiter.acquired;
        const gate = waiter.gate;
        self.mutex.unlock();
        if (!valid or gate == null) return error.UnselectedExecutionCohort;
        errdefer releaseGate(waiter); // Caller retains FIFO ownership through cleanup.
        try control.lock(gate.?);
        self.lock();
        // This caller alone owns its selected stack waiter until leave().
        std.debug.assert(self.head == waiter and waiter.ready and !waiter.acquired);
        waiter.acquired = true;
        self.mutex.unlock();
        try control.check();
    }
    /// End numerical ownership but retain this ready turn while its actual
    /// cleanup epoch seals and supplier refunds finish. A successor cannot
    /// spuriously fail admission against this cohort's just-dead workspace.
    pub fn releaseGate(waiter: *Waiter) void {
        const self = waiter.owner orelse return;
        self.lock();
        defer self.mutex.unlock();
        if (!waiter.acquired) return;
        std.debug.assert(self.head == waiter and waiter.ready);
        waiter.gate.?.unlock();
        waiter.acquired = false;
    }
    pub fn enter(self: *Scheduler, waiter: *Waiter, gate: *std.atomic.Mutex, control: Control) !void {
        try control.check();
        self.append(waiter, gate);
        errdefer leave(waiter);
        while (!self.take(waiter)) {
            try control.check();
            if (control.io) |io| try std.Io.sleep(io, .fromMilliseconds(1), .awake) else @import("antfly_platform").time.yieldNow();
        }
        try control.check();
    }
    /// Called by the current owner at a completed work boundary. A peer that
    /// arrives after this check gets the next bounded wave's handoff.
    pub fn hasPeer(waiter: *const Waiter) bool {
        const self = waiter.owner orelse return true;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        std.debug.assert(waiter.acquired and self.head == waiter);
        return waiter.next != null;
    }

    /// Identity proof under the same metadata exclusion that publishes acquisition.
    /// The caller must keep its final-address waiter alive and must not move it.
    pub fn ownsAcquired(self: *Scheduler, waiter: *const Waiter, gate: *std.atomic.Mutex) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.head == waiter and waiter.owner == self and waiter.gate == gate and waiter.acquired;
    }

    pub fn leave(waiter: *Waiter) void {
        const self = waiter.owner orelse return;
        @import("antfly_platform").sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (waiter.acquired) waiter.gate.?.unlock();
        if (waiter.previous) |previous| previous.next = waiter.next else self.head = waiter.next;
        if (waiter.next) |next| next.previous = waiter.previous else self.tail = waiter.previous;
        waiter.* = .{};
    }
};

test "embeddinggemma2 ready cohort scheduler is FIFO cancels queued work and yields between waves" {
    var scheduler = Scheduler{};
    var gate: std.atomic.Mutex = .unlocked;
    var first = Scheduler.Waiter{};
    var short = Scheduler.Waiter{};
    var canceled = Scheduler.Waiter{};
    scheduler.append(&first, &gate);
    try std.testing.expect(scheduler.take(&first));
    scheduler.append(&canceled, &gate);
    scheduler.append(&short, &gate);
    try std.testing.expect(!scheduler.take(&short));
    Scheduler.leave(&canceled);
    Scheduler.leave(&first);
    scheduler.append(&first, &gate); // Long request's successor goes behind short.
    try std.testing.expect(!scheduler.take(&first));
    try std.testing.expect(scheduler.take(&short));
    Scheduler.leave(&short);
    try std.testing.expect(scheduler.take(&first));
    Scheduler.leave(&first);
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
    const Cancel = struct {
        fn check(_: ?*const anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, scheduler.enter(&canceled, &gate, .{ .check_fn = Cancel.check }));
    try std.testing.expect(scheduler.head == null);
    try std.testing.expect(gate.tryLock());
    const LateCancel = struct {
        fn check(raw: ?*const anyopaque) !void {
            const count: *usize = @ptrCast(@alignCast(@constCast(raw.?)));
            count.* += 1;
            if (count.* >= 2) return error.Cancelled;
        }
    };
    var checks: usize = 0;
    try std.testing.expectError(error.Cancelled, scheduler.enter(&canceled, &gate, .{ .ptr = &checks, .check_fn = LateCancel.check }));
    try std.testing.expect(canceled.owner == null and scheduler.head == null);
    try std.testing.expect(!gate.tryLock()); // Foreign gate owner remains intact.
    gate.unlock();
    try scheduler.enter(&canceled, &gate, .{});
    Scheduler.leave(&canceled);
}

test "embeddinggemma2 ready cohort scheduler hands off across threads and removes canceled peers" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var scheduler = Scheduler{};
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    var foreign = true;
    var counter = std.atomic.Value(usize).init(0);
    const Worker = struct {
        scheduler: *Scheduler,
        gate: *std.atomic.Mutex,
        counter: *std.atomic.Value(usize),
        canceled: std.atomic.Value(bool) = .init(false),
        waiter: Scheduler.Waiter = .{},
        order: usize = std.math.maxInt(usize),
        failure: ?anyerror = null,
        fn cancel(raw: ?*anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            return self.canceled.load(.acquire);
        }
        fn run(self: *@This()) void {
            self.scheduler.enter(&self.waiter, self.gate, .{ .io = std.testing.io, .cancellation = .{ .ptr = self, .is_cancelled_fn = cancel } }) catch |err| {
                self.failure = err;
                return;
            };
            defer Scheduler.leave(&self.waiter);
            self.order = self.counter.fetchAdd(1, .monotonic);
        }
    };
    var workers: [3]Worker = @splat(.{ .scheduler = &scheduler, .gate = &gate, .counter = &counter });
    var threads: [3]?std.Thread = @splat(null);
    defer {
        if (foreign) gate.unlock();
        for (threads) |thread| if (thread) |t| t.join();
    }
    for (&workers, &threads) |*worker, *thread| {
        thread.* = try std.Thread.spawn(.{}, Worker.run, .{worker});
        while (true) {
            @import("antfly_platform").sync.lockYielding(&scheduler.mutex);
            const queued = worker.waiter.owner != null;
            scheduler.mutex.unlock();
            if (queued) break;
            @import("antfly_platform").time.yieldNow();
        }
    }
    workers[1].canceled.store(true, .release);
    threads[1].?.join();
    threads[1] = null;
    try std.testing.expectEqual(@as(?anyerror, error.Cancelled), workers[1].failure);
    gate.unlock();
    foreign = false;
    for (&threads) |*thread| if (thread.*) |t| {
        t.join();
        thread.* = null;
    };
    try std.testing.expectEqual(@as(usize, 0), workers[0].order);
    try std.testing.expectEqual(@as(usize, 1), workers[2].order);
    try std.testing.expectEqual(@as(usize, 2), counter.load(.monotonic));
    try std.testing.expect(scheduler.head == null and scheduler.tail == null);
}

test "ready cohort retains FIFO workspace turn with model gate free throughout producer phase" {
    var scheduler = Scheduler{};
    var gate: std.atomic.Mutex = .unlocked;
    var selected = Scheduler.Waiter{};
    try scheduler.selectReady(&selected, &gate, .{});
    defer Scheduler.leave(&selected);
    try std.testing.expect(selected.ready and !selected.acquired);
    // A real resource supplier can reenter model metadata/gate probes while
    // this one ready cohort owns preparation; no other queued workspace wins.
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    try std.testing.expect(scheduler.mutex.tryLock());
    scheduler.mutex.unlock();
    var peer = Scheduler.Waiter{};
    scheduler.append(&peer, &gate);
    defer Scheduler.leave(&peer);
    try std.testing.expect(!scheduler.take(&peer));
    try Scheduler.acquireSelected(&selected, .{});
    try std.testing.expect(selected.acquired);
    try std.testing.expect(!gate.tryLock());
    Scheduler.leave(&selected);
    try std.testing.expect(scheduler.take(&peer));
    try std.testing.expect(peer.acquired);
}

test "cancelled ready execution retains cleanup turn before successor admission" {
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    var scheduler = Scheduler{};
    var gate: std.atomic.Mutex = .unlocked;
    var selected = Scheduler.Waiter{};
    try scheduler.selectReady(&selected, &gate, .{});
    defer Scheduler.leave(&selected);
    var peer = Scheduler.Waiter{};
    scheduler.append(&peer, &gate);
    defer Scheduler.leave(&peer);
    try std.testing.expectError(error.Cancelled, Scheduler.acquireSelected(&selected, .{ .check_fn = Cancel.check }));
    try std.testing.expect(selected.owner == &scheduler and selected.ready and !selected.acquired);
    try std.testing.expect(gate.tryLock());
    gate.unlock();
    // Real caller cleanup/epoch sealing occurs before leave. A successor must
    // not acquire against quota still owned by the canceled preparation.
    try std.testing.expect(!scheduler.take(&peer));
    Scheduler.leave(&selected);
    try std.testing.expect(scheduler.take(&peer));
}

test "execution cohort authority rejects copied stale and foreign waiters" {
    var scheduler: Scheduler = .{};
    var foreign: Scheduler = .{};
    var gate: std.atomic.Mutex = .unlocked;
    var other_gate: std.atomic.Mutex = .unlocked;
    var waiter: Scheduler.Waiter = .{};
    try scheduler.enter(&waiter, &gate, .{});
    defer Scheduler.leave(&waiter);
    try std.testing.expect(scheduler.ownsAcquired(&waiter, &gate));
    const copied = waiter;
    try std.testing.expect(!scheduler.ownsAcquired(&copied, &gate));
    try std.testing.expect(!foreign.ownsAcquired(&waiter, &gate));
    try std.testing.expect(!scheduler.ownsAcquired(&waiter, &other_gate));
    Scheduler.leave(&waiter);
    try std.testing.expect(!scheduler.ownsAcquired(&waiter, &gate));
    try scheduler.selectReady(&waiter, &gate, .{});
    try std.testing.expect(!scheduler.ownsAcquired(&waiter, &gate));
    try Scheduler.acquireSelected(&waiter, .{});
    try std.testing.expect(scheduler.ownsAcquired(&waiter, &gate));
    Scheduler.releaseGate(&waiter);
    try std.testing.expect(!scheduler.ownsAcquired(&waiter, &gate));
}
