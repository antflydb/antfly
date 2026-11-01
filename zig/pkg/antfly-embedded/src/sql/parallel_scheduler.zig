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

//! Separate CPU/I/O admission under one temporary-work memory budget. Remote
//! reads cannot consume CPU slots or more than half the shared byte allowance.
//! Required work runs inline when
//! saturated; speculative work yields. Result-owning leases last through joining;
//! transient leases end at completion. Both release canceled-before-start slots.
const std = @import("std");
const A = std.mem.Allocator;
pub const LockedAllocator = struct {
    backing: A,
    mutex: std.atomic.Mutex = .unlocked,
    shared_mutex: ?*std.atomic.Mutex = null,
    /// Immutable parent identity for admission through this synchronizing wrapper.
    pub fn parentAllocator(a: std.mem.Allocator) ?std.mem.Allocator {
        if (a.vtable.alloc != allocate) return null;
        return @as(*LockedAllocator, @ptrCast(@alignCast(a.ptr))).backing;
    }
    pub fn allocator(self: *LockedAllocator) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
    }
    fn lock(self: *LockedAllocator) void {
        while (!(self.shared_mutex orelse &self.mutex).tryLock()) std.atomic.spinLoopHint();
    }
    fn unlock(self: *LockedAllocator) void {
        (self.shared_mutex orelse &self.mutex).unlock();
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        @import("spill.zig").Manager.prepareAllocator(self.allocator(), len);
        @import("memory_budget.zig").AllocationGuard.enter(self.shared_mutex orelse &self.mutex);
        defer @import("memory_budget.zig").AllocationGuard.leave();
        self.lock();
        defer self.unlock();
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        @import("spill.zig").Manager.prepareAllocator(self.allocator(), len -| bytes.len);
        @import("memory_budget.zig").AllocationGuard.enter(self.shared_mutex orelse &self.mutex);
        defer @import("memory_budget.zig").AllocationGuard.leave();
        self.lock();
        defer self.unlock();
        return self.backing.rawResize(bytes, alignment, len, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        @import("spill.zig").Manager.prepareAllocator(self.allocator(), len -| bytes.len);
        @import("memory_budget.zig").AllocationGuard.enter(self.shared_mutex orelse &self.mutex);
        defer @import("memory_budget.zig").AllocationGuard.leave();
        self.lock();
        defer self.unlock();
        return self.backing.rawRemap(bytes, alignment, len, ra);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        const Guard = @import("memory_budget.zig").AllocationGuard;
        const mutex = self.shared_mutex orelse &self.mutex;
        const reuse = Guard.reuse(mutex);
        Guard.enter(mutex);
        defer Guard.leave();
        if (!reuse) self.lock();
        defer if (!reuse) self.unlock();
        self.backing.rawFree(bytes, alignment, ra);
    }
};

var shared: Scheduler = .{};
pub fn global() *Scheduler {
    return &shared;
}
const Class = enum { cpu, io };
pub const Scheduler = struct {
    mutex: std.atomic.Mutex = .unlocked,
    max_workers: usize = 8,
    max_io_workers: usize = 32,
    /// Parked pull operators retain independently budgeted statement state.
    /// Bound their task descriptors separately from runnable CPU execution.
    max_pull_workers: usize = 64,
    pull_workers: usize = 0,
    max_pull_bytes: usize = 2 * 1024 * 1024 * 1024,
    pull_bytes: usize = 0,
    max_io_bytes: usize = 32 * 1024 * 1024,
    max_bytes: usize = 64 * 1024 * 1024,
    workers: usize = 0,
    io_workers: usize = 0,
    io_bytes: usize = 0,
    bytes: usize = 0,
    peak_workers: usize = 0,
    peak_io_workers: usize = 0,
    peak_cpu_workers: usize = 0,
    peak_bytes: usize = 0,
    inline_tasks: usize = 0,
    waiters: ?*Waiter = null,
    fn lock(self: *Scheduler) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn reservePull(self: *Scheduler, count: usize, bytes: usize) bool {
        return self.reservePullUpTo(count, bytes, bytes) != null;
    }
    /// Reserve required retained state before choosing optional pipeline buffers.
    /// The returned allowance is atomic with respect to competing publications.
    pub fn reservePullUpTo(self: *Scheduler, count: usize, required: usize, desired: usize) ?usize {
        self.lock();
        defer self.mutex.unlock();
        const bytes = @min(desired, self.max_pull_bytes -| self.pull_bytes);
        if (desired < required or count > self.max_pull_workers -| self.pull_workers or bytes < required) return null;
        self.pull_workers += count;
        self.pull_bytes += bytes;
        return bytes;
    }
    pub fn releasePull(self: *Scheduler, count: usize, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.pull_workers >= count and self.pull_bytes >= bytes);
        self.pull_workers -= count;
        self.pull_bytes -= bytes;
    }
    /// Cap an operator workspace by currently available shared admission.
    /// Larger statement quotas must not increase leases beyond node capacity.
    pub fn workspaceBudget(self: *Scheduler, requested: usize) usize {
        self.lock();
        defer self.mutex.unlock();
        return @min(requested, self.max_bytes -| self.bytes);
    }
    /// CPU pipelines leave half the node allowance for required/speculative
    /// I/O. The caller lane is charged to its statement, like retained results.
    pub fn cpuWorkspaceBudget(self: *Scheduler, requested: usize) usize {
        self.lock();
        defer self.mutex.unlock();
        const reserve = @min(self.max_io_bytes, self.max_bytes / 2);
        const cpu_bytes = self.bytes - self.io_bytes;
        return @min(requested, @min(self.max_bytes -| self.bytes, (self.max_bytes - reserve) -| cpu_bytes));
    }
    /// Advisory fan-out; submit still arbitrates races with other statements.
    /// One caller lane needs no lease. Every concurrent lane has a useful
    /// minimum workspace and shares the operator's total memory allowance.
    pub fn fanout(self: *Scheduler, work: usize, budget: usize, minimum: usize) usize {
        self.lock();
        defer self.mutex.unlock();
        if (work == 0 or minimum == 0) return 1;
        const slots = @min(8, self.max_workers -| (self.workers - self.io_workers));
        const memory = @min(budget, self.max_bytes -| self.bytes) / minimum;
        return @max(1, @min(work, @min(slots, memory)));
    }
    fn acquire(self: *Scheduler, bytes: usize) bool {
        return self.acquireClass(.cpu, bytes);
    }
    fn acquireClass(self: *Scheduler, class: Class, bytes: usize) bool {
        return self.acquireClassReserved(class, bytes, false);
    }
    fn acquireClassReserved(self: *Scheduler, class: Class, bytes: usize, reserve_io: bool) bool {
        self.lock();
        defer self.mutex.unlock();
        return self.acquireLocked(class, bytes, reserve_io, true);
    }
    fn acquireLocked(self: *Scheduler, class: Class, bytes: usize, reserve_io: bool, record_inline: bool) bool {
        const cpu_full = reserve_io and bytes > (self.max_bytes - @min(self.max_io_bytes, self.max_bytes / 2)) -| (self.bytes - self.io_bytes);
        const slots_full = switch (class) {
            .cpu => self.workers - self.io_workers >= self.max_workers,
            .io => self.io_workers >= self.max_io_workers,
        };
        const io_full = class == .io and bytes > @min(self.max_io_bytes, self.max_bytes / 2) -| self.io_bytes;
        if (cpu_full or slots_full or io_full or bytes > self.max_bytes -| self.bytes) {
            if (record_inline) self.inline_tasks += 1;
            return false;
        }
        self.workers += 1;
        if (class == .io) {
            self.io_workers += 1;
            self.io_bytes += bytes;
            self.peak_io_workers = @max(self.peak_io_workers, self.io_workers);
        }
        self.bytes += bytes;
        self.peak_workers = @max(self.peak_workers, self.workers);
        self.peak_cpu_workers = @max(self.peak_cpu_workers, self.workers - self.io_workers);
        self.peak_bytes = @max(self.peak_bytes, self.bytes);
        return true;
    }
    fn release(self: *Scheduler, bytes: usize) void {
        self.releaseClass(.cpu, bytes);
    }
    fn releaseClass(self: *Scheduler, class: Class, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.workers != 0 and self.bytes >= bytes);
        if (class == .io) {
            std.debug.assert(self.io_workers != 0 and self.io_bytes >= bytes);
            self.io_workers -= 1;
            self.io_bytes -= bytes;
        } else std.debug.assert(self.workers > self.io_workers);
        self.workers -= 1;
        self.bytes -= bytes;
        var next = self.waiters;
        while (next) |waiter| : (next = waiter.next) waiter.ready.set(waiter.io);
    }
    /// Speculative tasks retain no result buffers outside independently bounded
    /// caches. Release execution admission at completion, while Task still owns
    /// the join and the descriptor needed for canceled-before-start workers.
    /// A parked coordinator owns bounded retained workspace and one pull slot.
    /// Its child phases separately admit runnable I/O and CPU work. Optional
    /// callers decline when capacity is unavailable instead of queuing inline.
    pub fn submitPullTransient(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        self.lock();
        // Optional coordinators leave half of retained-state capacity for
        // required operators. They never make a required pull queue behind
        // speculative work just to obtain its descriptor or workspace.
        const worker_limit = self.max_pull_workers / 2;
        const byte_limit = self.max_pull_bytes / 2;
        const allowed = self.pull_workers < worker_limit and bytes <= byte_limit -| self.pull_bytes;
        if (allowed) {
            self.pull_workers += 1;
            self.pull_bytes += bytes;
        }
        self.mutex.unlock();
        if (!allowed) return null;
        const admission = std.heap.page_allocator.create(Admission) catch {
            self.releasePull(1, bytes);
            return null;
        };
        admission.* = .{ .scheduler = self, .bytes = bytes, .class = .cpu, .pull = true, .io = io };
        return self.launchTransient(io, admission, function, args);
    }
    pub fn submitTransient(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.cpu, io, bytes, null, false, function, args);
    }
    pub fn submitIoTransient(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.io, io, bytes, null, false, function, args);
    }
    /// Notify only after execution admission is released and readiness published.
    /// A bounded coordinator can drain whichever phase finishes first without
    /// retaining a CPU slot during provider I/O or polling ordered futures.
    pub fn submitNotified(self: *Scheduler, io: std.Io, bytes: usize, done: *std.Io.Event, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.cpu, io, bytes, done, false, function, args);
    }
    pub fn submitIoNotified(self: *Scheduler, io: std.Io, bytes: usize, done: *std.Io.Event, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.io, io, bytes, done, false, function, args);
    }
    pub fn submitNotifiedWithIoReserve(self: *Scheduler, io: std.Io, bytes: usize, done: *std.Io.Event, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.cpu, io, bytes, done, true, function, args);
    }
    fn submitTransientClass(self: *Scheduler, class: Class, io: std.Io, bytes: usize, done: ?*std.Io.Event, reserve_io: bool, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        if (!self.acquireClassReserved(class, bytes, reserve_io)) return null;
        const admission = std.heap.page_allocator.create(Admission) catch {
            self.releaseClass(class, bytes);
            return null;
        };
        admission.* = .{ .scheduler = self, .bytes = bytes, .class = class, .done = done, .io = io };
        return self.launchTransient(io, admission, function, args);
    }
    fn launchTransient(self: *Scheduler, io: std.Io, admission: *Admission, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        const Result = @TypeOf(@call(.auto, function, args));
        const Worker = struct {
            fn run(control: *Admission, arguments: @TypeOf(args)) Result {
                defer {
                    control.release();
                    control.completed.store(true, .release);
                    if (control.done) |event| event.set(control.io);
                }
                return @call(.auto, function, arguments);
            }
        };
        const future = io.concurrent(Worker.run, .{ admission, args }) catch {
            admission.release();
            std.heap.page_allocator.destroy(admission);
            return null;
        };
        return .{ .scheduler = self, .bytes = admission.bytes, .class = admission.class, .future = future, .transient = admission };
    }
    pub fn submit(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitClass(.cpu, io, bytes, function, args);
    }
    /// CPU cohorts share their admitted workspace and leave bounded I/O
    /// headroom, including races with other cohorts using this admission class.
    pub fn submitWithIoReserve(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        if (!self.acquireClassReserved(.cpu, bytes, true)) return null;
        return self.launch(.cpu, io, bytes, function, args);
    }
    pub fn submitIo(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitClass(.io, io, bytes, function, args);
    }
    fn submitClass(self: *Scheduler, class: Class, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        if (!self.acquireClass(class, bytes)) return null;
        return self.launch(class, io, bytes, function, args);
    }
    fn launch(self: *Scheduler, class: Class, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        const future = io.concurrent(function, args) catch {
            self.releaseClass(class, bytes);
            return null;
        };
        return .{ .scheduler = self, .bytes = bytes, .class = class, .future = future };
    }
};
const Waiter = struct {
    io: std.Io,
    ready: std.Io.Event = .unset,
    next: ?*Waiter = null,
};

/// A pull operator's CPU workspace is distinct from its independently admitted
/// retained state. Pause before a producer wait; resume only after dropping any
/// producer locks. Waiting operators consume neither CPU slots nor CPU bytes.
pub const Execution = struct {
    scheduler: *Scheduler,
    bytes: usize,
    active: bool = false,
    checkpoint: ?struct { ptr: *anyopaque, check: *const fn (*anyopaque) anyerror!void } = null,
    pub fn pause(self: *Execution) void {
        if (!self.active) return;
        self.active = false;
        self.scheduler.releaseClass(.cpu, self.bytes);
    }
    /// Optional dependent work must not queue behind CPU admission held by
    /// the producer it will unblock. Retained-state admission is independent.
    pub fn tryEnter(self: *Execution) bool {
        std.debug.assert(!self.active);
        if (!self.scheduler.acquireClassReserved(.cpu, self.bytes, true)) return false;
        self.active = true;
        return true;
    }
    pub fn enter(self: *Execution, io: std.Io) !void {
        std.debug.assert(!self.active);
        try self.waitAdmission(io, .cpu, 1);
        self.active = true;
    }
    /// Publication buffers are covered by retained-state admission. Reserve
    /// only I/O slots while their owner parks, including bounded upload batches.
    pub fn beginIo(self: *Execution, io: std.Io, count: usize) !Io {
        self.pause();
        try self.waitAdmission(io, .io, count);
        return .{ .execution = self, .io = io, .slots = count };
    }
    pub const Io = struct {
        execution: *Execution,
        io: std.Io,
        slots: usize,
        pub fn abort(self: *Io) void {
            for (0..self.slots) |_| self.execution.scheduler.releaseClass(.io, 0);
            self.slots = 0;
        }
        pub fn finish(self: *Io) !void {
            self.abort();
            try self.execution.enter(self.io);
        }
    };
    fn acquire(self: *Execution, class: Class, count: usize) bool {
        const scheduler = self.scheduler;
        if (class == .cpu) return scheduler.acquireLocked(.cpu, self.bytes, true, false);
        if (count > scheduler.max_io_workers -| scheduler.io_workers) return false;
        for (0..count) |_| {
            const acquired = scheduler.acquireLocked(.io, 0, false, false);
            std.debug.assert(acquired);
        }
        return true;
    }
    fn waitAdmission(self: *Execution, io: std.Io, class: Class, count: usize) !void {
        if (class == .io and (count == 0 or count > self.scheduler.max_io_workers)) return error.ConcurrencyUnavailable;
        var waiter: Waiter = .{ .io = io };
        const scheduler = self.scheduler;
        if (self.checkpoint) |hook| try hook.check(hook.ptr);
        scheduler.lock();
        if (self.acquire(class, count)) {
            scheduler.mutex.unlock();
            return;
        }
        waiter.next = scheduler.waiters;
        scheduler.waiters = &waiter;
        scheduler.mutex.unlock();
        defer {
            scheduler.lock();
            defer scheduler.mutex.unlock();
            var link = &scheduler.waiters;
            while (link.*.? != &waiter) link = &link.*.?.next;
            link.* = waiter.next;
        }
        while (true) {
            if (self.checkpoint) |hook| try hook.check(hook.ptr);
            scheduler.lock();
            waiter.ready.reset();
            const acquired = self.acquire(class, count);
            scheduler.mutex.unlock();
            if (acquired) {
                return;
            }
            waiter.ready.waitTimeout(io, .{ .duration = .{ .clock = .awake, .raw = .fromMilliseconds(10) } }) catch |err| switch (err) {
                error.Timeout => {},
                else => return err,
            };
        }
    }
};

const Admission = struct {
    done: ?*std.Io.Event = null,
    io: std.Io,
    scheduler: *Scheduler,
    bytes: usize,
    class: Class,
    pull: bool = false,
    released: std.atomic.Value(bool) = .init(false),
    completed: std.atomic.Value(bool) = .init(false),
    fn release(self: *Admission) void {
        if (!self.released.swap(true, .acq_rel)) {
            if (self.pull) self.scheduler.releasePull(1, self.bytes) else self.scheduler.releaseClass(self.class, self.bytes);
        }
    }
};
pub fn Task(comptime Result: type) type {
    return struct {
        scheduler: *Scheduler,
        bytes: usize,
        class: Class = .cpu,
        future: ?std.Io.Future(Result),
        transient: ?*Admission = null,
        fn release(self: *@This()) void {
            if (self.transient) |admission| {
                admission.release();
                std.heap.page_allocator.destroy(admission);
                self.transient = null;
            } else self.scheduler.releaseClass(self.class, self.bytes);
            self.future = null;
        }
        /// Completion is independent of joining and execution admission.
        /// Only transient tasks expose this nonblocking readiness contract.
        pub fn isComplete(self: *const @This()) bool {
            return if (self.transient) |admission| admission.completed.load(.acquire) else self.future == null;
        }
        pub fn await(self: *@This(), io: std.Io) Result {
            defer self.release();
            return self.future.?.await(io);
        }
        pub fn cancel(self: *@This(), io: std.Io) Result {
            defer self.release();
            return self.future.?.cancel(io);
        }
    };
}
/// Join notified void jobs in completion order. Failures and caller cancellation
/// retire every sibling before the coordinator can release callback storage.
/// Every active task must use submit*Notified with this same readiness event.
pub fn joinNotified(io: std.Io, ready: *std.Io.Event, tasks: []?Task(anyerror!void)) anyerror!void {
    errdefer for (tasks) |*task| if (task.*) |*held| {
        _ = held.cancel(io) catch {};
        task.* = null;
    };
    while (true) {
        ready.reset();
        var pending = false;
        for (tasks) |*task| if (task.*) |*held| {
            std.debug.assert(held.transient != null and held.transient.?.done == ready);
            if (!held.isComplete()) {
                pending = true;
                continue;
            }
            const outcome = held.await(io);
            task.* = null;
            try outcome;
        };
        if (!pending) return;
        try ready.wait(io);
    }
}

test "SQL shared scheduling bounds overlapping operators and releases canceled admissions" {
    const Worker = struct {
        fn run() anyerror!usize {
            return 7;
        }
    };
    var scheduler: Scheduler = .{ .max_workers = 2, .max_bytes = 100 };
    const io = std.testing.io;
    var first = scheduler.submit(io, 40, Worker.run, .{}) orelse return error.TestUnexpectedResult;
    var second = scheduler.submit(io, 60, Worker.run, .{}) orelse return error.TestUnexpectedResult;
    try std.testing.expect(scheduler.submit(io, 1, Worker.run, .{}) == null);
    try std.testing.expectEqual(@as(usize, 7), try first.await(io));
    _ = second.cancel(io) catch {};
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    try std.testing.expectEqual(@as(usize, 2), scheduler.peak_workers);
    try std.testing.expectEqual(@as(usize, 100), scheduler.peak_bytes);
    try std.testing.expect(scheduler.submit(io, 101, Worker.run, .{}) == null);
}

test "SQL completed speculative work releases admission before owner joins" {
    const Worker = struct {
        fn run(io: std.Io, gate: *std.Io.Event) anyerror!void {
            try gate.wait(io);
        }
    };
    var scheduler: Scheduler = .{ .max_workers = 1, .max_bytes = 4 };
    var gate: std.Io.Event = .unset;
    var first = scheduler.submitTransient(std.testing.io, 4, Worker.run, .{ std.testing.io, &gate }).?;
    defer if (first.future != null) {
        _ = first.cancel(std.testing.io) catch {};
    };
    try std.testing.expect(scheduler.submit(std.testing.io, 1, Worker.run, .{ std.testing.io, &gate }) == null);
    gate.set(std.testing.io);
    // The admission is distinct from the future/result owner. Wait on the
    // worker's released control instead of joining the owning task.
    while (!first.transient.?.released.load(.acquire)) std.atomic.spinLoopHint();
    while (true) {
        scheduler.lock();
        const free = scheduler.bytes == 0 and scheduler.workers == 0;
        scheduler.mutex.unlock();
        if (free) break;
        std.atomic.spinLoopHint();
    }
    var second = scheduler.submitTransient(std.testing.io, 4, Worker.run, .{ std.testing.io, &gate }).?;
    try second.await(std.testing.io);
    try first.await(std.testing.io);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
}

test "SQL adaptive fanout respects useful work, occupied leases and workspace" {
    var scheduler: Scheduler = .{ .max_workers = 8, .max_bytes = 1024 };
    try std.testing.expectEqual(@as(usize, 1024), scheduler.workspaceBudget(4096));
    try std.testing.expectEqual(@as(usize, 256), scheduler.workspaceBudget(256));
    try std.testing.expectEqual(@as(usize, 8), scheduler.fanout(16, 1024, 128));
    try std.testing.expectEqual(@as(usize, 3), scheduler.fanout(3, 1024, 128));
    try std.testing.expectEqual(@as(usize, 2), scheduler.fanout(16, 256, 128));
    try std.testing.expect(scheduler.acquire(896));
    try std.testing.expectEqual(@as(usize, 128), scheduler.workspaceBudget(4096));
    try std.testing.expectEqual(@as(usize, 1), scheduler.fanout(16, 1024, 128));
    scheduler.release(896);
    try std.testing.expectEqual(@as(usize, 8), scheduler.fanout(16, 1024, 128));
}

test "SQL remote reads have independent slots and preserve CPU workspace" {
    const Worker = struct {
        fn run() anyerror!usize {
            return 7;
        }
    };
    var scheduler: Scheduler = .{ .max_workers = 1, .max_io_workers = 2, .max_bytes = 100 };
    const io = std.testing.io;
    var cpu = scheduler.submit(io, 50, Worker.run, .{}).?;
    defer if (cpu.future != null) {
        _ = cpu.cancel(io) catch {};
    };
    var first = scheduler.submitIo(io, 20, Worker.run, .{}).?;
    defer if (first.future != null) {
        _ = first.cancel(io) catch {};
    };
    var second = scheduler.submitIo(io, 20, Worker.run, .{}).?;
    defer if (second.future != null) {
        _ = second.cancel(io) catch {};
    };
    try std.testing.expect(scheduler.submitIo(io, 1, Worker.run, .{}) == null);
    try std.testing.expect(scheduler.submit(io, 1, Worker.run, .{}) == null);
    try std.testing.expectEqual(@as(usize, 7), try cpu.await(io));
    // Outstanding remote reads do not occupy the available CPU lane. Their
    // byte reservations still reduce its available workspace.
    try std.testing.expectEqual(@as(usize, 60), scheduler.workspaceBudget(100));
    var next_cpu = scheduler.submit(io, 60, Worker.run, .{}).?;
    try std.testing.expectEqual(@as(usize, 100), scheduler.bytes);
    _ = try next_cpu.await(io);
    _ = first.cancel(io) catch {};
    _ = second.cancel(io) catch {};
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_bytes);
    try std.testing.expectEqual(@as(usize, 2), scheduler.peak_io_workers);
    // Even an oversized configured I/O allowance reserves at least half of
    // the shared budget for CPU work; small node budgets scale automatically.
    try std.testing.expect(scheduler.submitIo(io, 51, Worker.run, .{}) == null);
    var large_cpu = scheduler.submit(io, 71, Worker.run, .{}).?;
    defer if (large_cpu.future != null) {
        _ = large_cpu.cancel(io) catch {};
    };
    try std.testing.expect(scheduler.submitIo(io, 30, Worker.run, .{}) == null);
    var small_io = scheduler.submitIo(io, 29, Worker.run, .{}).?;
    _ = try small_io.await(io);
    _ = try large_cpu.await(io);
}

test "SQL completed remote prefetch releases both admission classes before joining" {
    const Worker = struct {
        fn run() anyerror!void {}
    };
    var scheduler: Scheduler = .{ .max_workers = 1, .max_io_workers = 1, .max_bytes = 100 };
    var task = scheduler.submitIoTransient(std.testing.io, 50, Worker.run, .{}).?;
    defer if (task.future != null) {
        _ = task.cancel(std.testing.io) catch {};
    };
    while (!task.isComplete()) std.atomic.spinLoopHint();
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_bytes);
    try task.await(std.testing.io);
}

test "SQL aggregate CPU work preserves node IO headroom" {
    var scheduler: Scheduler = .{ .max_bytes = 64 * 1024 * 1024, .max_io_bytes = 32 * 1024 * 1024 };
    const budget = scheduler.cpuWorkspaceBudget(128 * 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1024), budget);
    const width = scheduler.fanout(4, budget, 1024 * 1024);
    try std.testing.expectEqual(@as(usize, 4), width);
    for (0..width - 1) |_| try std.testing.expect(scheduler.acquire(budget / width));
    defer for (0..width - 1) |_| scheduler.release(budget / width);
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), scheduler.cpuWorkspaceBudget(128 * 1024 * 1024));
    try std.testing.expect(scheduler.acquireClass(.io, 16 * 1024 * 1024));
    scheduler.releaseClass(.io, 16 * 1024 * 1024);
}

test "SQL suspended pull operators release CPU workspace for producer decoders" {
    const io = std.testing.io;
    var scheduler: Scheduler = .{ .max_workers = 2, .max_bytes = 100, .max_pull_bytes = 100 };
    try std.testing.expect(scheduler.reservePull(2, 100));
    var first: Execution = .{ .scheduler = &scheduler, .bytes = 25 };
    defer first.pause();
    var second: Execution = .{ .scheduler = &scheduler, .bytes = 25 };
    defer second.pause();
    try first.enter(io);
    try second.enter(io);
    try std.testing.expectEqual(@as(usize, 2), scheduler.workers);
    first.pause();
    second.pause();
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    try std.testing.expectEqual(@as(usize, 100), scheduler.pull_bytes);
    try std.testing.expect(!scheduler.reservePull(1, 1));
    const Worker = struct {
        fn decode() anyerror!usize {
            return 42;
        }
    };
    var decode = scheduler.submit(io, 50, Worker.decode, .{}) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 42), try decode.await(io));
    try first.enter(io);
    try second.enter(io);
    first.pause();
    second.pause();
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
    try std.testing.expect(scheduler.waiters == null);
    scheduler.releasePull(2, 100);
    try std.testing.expect(scheduler.reservePull(scheduler.max_pull_workers, scheduler.max_pull_bytes));
    try std.testing.expect(!scheduler.reservePull(1, 1));
    scheduler.releasePull(scheduler.max_pull_workers, scheduler.max_pull_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
    try std.testing.expect(!scheduler.reservePull(1, scheduler.max_pull_bytes + 1));
}

test "SQL cooperative CPU admission wakes waiters and cancels without leaking permits" {
    const io = std.testing.io;
    var scheduler: Scheduler = .{ .max_workers = 1, .max_bytes = 100 };
    var occupied: Execution = .{ .scheduler = &scheduler, .bytes = 50 };
    defer occupied.pause();
    try occupied.enter(io);
    const Worker = struct {
        fn run(execution: *Execution, gate: *std.Io.Event) anyerror!void {
            defer execution.pause();
            gate.set(std.testing.io);
            try execution.enter(std.testing.io);
        }
    };
    for ([_]bool{ true, false }) |cancel| {
        var waiting: Execution = .{ .scheduler = &scheduler, .bytes = 50 };
        var gate: std.Io.Event = .unset;
        var task = try io.concurrent(Worker.run, .{ &waiting, &gate });
        try gate.wait(io);
        while (true) {
            scheduler.lock();
            const registered = scheduler.waiters != null;
            scheduler.mutex.unlock();
            if (registered) break;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        if (cancel) {
            _ = task.cancel(io) catch {};
        } else {
            occupied.pause();
            try task.await(io);
        }
        try std.testing.expect(!waiting.active);
        try std.testing.expect(scheduler.waiters == null);
    }
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
}

test "SQL shared retained admission reserves required work before optional buffers" {
    var scheduler: Scheduler = .{ .max_pull_bytes = 512, .max_pull_workers = 4 };
    try std.testing.expectEqual(@as(?usize, 400), scheduler.reservePullUpTo(2, 256, 400));
    try std.testing.expectEqual(@as(?usize, 112), scheduler.reservePullUpTo(1, 64, 256));
    try std.testing.expectEqual(@as(?usize, null), scheduler.reservePullUpTo(1, 1, 32));
    try std.testing.expectEqual(@as(usize, 512), scheduler.pull_bytes);
    scheduler.releasePull(1, 112);
    try std.testing.expectEqual(@as(?usize, null), scheduler.reservePullUpTo(3, 1, 64));
    scheduler.releasePull(2, 400);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_workers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
}

test "SQL publication IO phases release CPU retain memory and fence canceled resumption" {
    const Check = struct {
        canceled: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.canceled) return error.Canceled;
        }
    };
    const io = std.testing.io;
    var scheduler: Scheduler = .{ .max_workers = 1, .max_io_workers = 2, .max_bytes = 100, .max_io_bytes = 50 };
    try std.testing.expect(scheduler.reservePull(1, 200));
    defer scheduler.releasePull(1, 200);
    var check: Check = .{};
    var first: Execution = .{ .scheduler = &scheduler, .bytes = 50, .checkpoint = .{ .ptr = &check, .check = Check.check } };
    defer first.pause();
    try first.enter(io);
    var pending = try first.beginIo(io, 2);
    defer pending.abort();
    try std.testing.expectEqual(@as(usize, 2), scheduler.io_workers);
    try std.testing.expectEqual(@as(usize, 200), scheduler.pull_bytes);
    // A single CPU lane remains available while the upload batch is blocked.
    var second: Execution = .{ .scheduler = &scheduler, .bytes = 50 };
    try second.enter(io);
    try std.testing.expectEqual(@as(usize, 3), scheduler.workers);
    second.pause();
    try pending.finish();
    try std.testing.expect(first.active);
    try std.testing.expectEqual(@as(usize, 0), scheduler.io_workers);
    pending = try first.beginIo(io, 1);
    check.canceled = true;
    try std.testing.expectError(error.Canceled, pending.finish());
    try std.testing.expect(!first.active);
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
    check.canceled = false;
    try first.enter(io);
    try std.testing.expectError(error.ConcurrencyUnavailable, first.beginIo(io, 3));
    try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
}

test "SQL shared allocator lock protects overlapping parent and child regions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var child: LockedAllocator = .{ .backing = arena.allocator() };
    var parent: LockedAllocator = .{ .backing = arena.allocator(), .shared_mutex = &child.mutex };
    var go: std.Io.Event = .unset;
    var child_values: [64][]u8 = undefined;
    var parent_values: [64][]u8 = undefined;
    const Worker = struct {
        fn run(io: std.Io, gate: *std.Io.Event, a: std.mem.Allocator, output: *[64][]u8) anyerror!void {
            try gate.wait(io);
            for (output) |*value| {
                value.* = try a.alloc(u8, 2048);
                @memset(value.*, 0xaa);
            }
        }
    };
    const io = std.testing.io;
    var future: ?std.Io.Future(anyerror!void) = try io.concurrent(Worker.run, .{ io, &go, child.allocator(), &child_values });
    defer if (future) |*active| {
        active.cancel(io) catch {};
    };
    go.set(io);
    for (&parent_values) |*value| {
        value.* = try parent.allocator().alloc(u8, 2048);
        @memset(value.*, 0xbb);
    }
    const result = future.?.await(io);
    future = null;
    try result;
    for (child_values) |value| for (value) |byte| try std.testing.expectEqual(@as(u8, 0xaa), byte);
    for (parent_values) |value| for (value) |byte| try std.testing.expectEqual(@as(u8, 0xbb), byte);
}

test "external lake Parquet pull coordinators leave execution and required state capacity free" {
    const Worker = struct {
        fn run(io: std.Io, gate: *std.Io.Event) !usize {
            try gate.wait(io);
            return 7;
        }
    };
    const io = std.testing.io;
    for ([_]bool{ false, true }) |cancel| {
        var scheduler: Scheduler = .{ .max_pull_workers = 2, .max_pull_bytes = 200, .max_workers = 1, .max_io_workers = 1 };
        var gate: std.Io.Event = .unset;
        var task = scheduler.submitPullTransient(io, 100, Worker.run, .{ io, &gate }).?;
        defer if (task.future != null) {
            _ = task.cancel(io) catch {};
        };
        try std.testing.expectEqual(@as(usize, 0), scheduler.workers);
        try std.testing.expectEqual(@as(usize, 0), scheduler.bytes);
        try std.testing.expectEqual(@as(usize, 1), scheduler.pull_workers);
        try std.testing.expect(scheduler.submitPullTransient(io, 1, Worker.run, .{ io, &gate }) == null);
        try std.testing.expect(scheduler.reservePull(1, 100));
        scheduler.releasePull(1, 100);
        if (cancel) {
            try std.testing.expectError(error.Canceled, task.cancel(io));
        } else {
            gate.set(io);
            while (!task.isComplete()) try io.sleep(.fromMilliseconds(1), .awake);
            try std.testing.expectEqual(@as(usize, 0), scheduler.pull_workers);
            try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
            try std.testing.expectEqual(@as(usize, 7), try task.await(io));
        }
        try std.testing.expectEqual(@as(usize, 0), scheduler.pull_workers);
        try std.testing.expectEqual(@as(usize, 0), scheduler.pull_bytes);
    }
}

test "SQL shared notified joins cancel parked siblings before callback teardown" {
    const State = struct {
        scheduler: Scheduler = .{ .max_io_workers = 2 },
        gate: std.Io.Event = .unset,
        entered: [2]std.Io.Event = @splat(.unset),
        joining: std.Io.Event = .unset,
        live: std.atomic.Value(usize) = .init(0),
        fn child(self: *@This(), index: usize) anyerror!void {
            _ = self.live.fetchAdd(1, .acq_rel);
            defer _ = self.live.fetchSub(1, .acq_rel);
            self.entered[index].set(std.testing.io);
            try self.gate.wait(std.testing.io);
        }
        fn coordinator(self: *@This()) anyerror!void {
            const io = std.testing.io;
            var ready: std.Io.Event = .unset;
            var tasks: [2]?Task(anyerror!void) = @splat(null);
            defer for (&tasks) |*task| if (task.*) |*held| {
                _ = held.cancel(io) catch {};
            };
            for (&tasks, 0..) |*task, i| task.* = self.scheduler.submitIoNotified(io, 1, &ready, child, .{ self, i }) orelse return error.TestUnexpectedSaturation;
            for (&self.entered) |*event| try event.wait(io);
            self.joining.set(io);
            try joinNotified(io, &ready, &tasks);
        }
    };
    var state: State = .{};
    var parent = try std.testing.io.concurrent(State.coordinator, .{&state});
    try state.joining.wait(std.testing.io);
    try std.testing.expectError(error.Canceled, parent.cancel(std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), state.live.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), state.scheduler.io_workers);
    try std.testing.expectEqual(@as(usize, 0), state.scheduler.io_bytes);
}
