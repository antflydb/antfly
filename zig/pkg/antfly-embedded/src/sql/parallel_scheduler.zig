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
const Runnable = @import("../common/runnable_scheduler.zig");

/// Coordinator-owned authority for CPU kernels over immutable input and private
/// operator files with independent receivers. This does not permit sharing a
/// mutable backend, cursor or policy cache; the manager only retains locked
/// quotas/directory state and the statement allocation owner.
/// Its allocator and original parent contexts outlive every physical join.
pub const ExecutionScope = struct {
    execution: *Runnable.Execution,
    allocator: A,
    io: std.Io,
    wait: @import("execution_wait.zig").Boundary,

    pub fn submit(self: ExecutionScope, bytes: usize, result_allocator: A, comptime function: anytype, args: anytype, comptime discard: anytype) !?Task(@TypeOf(@call(.auto, function, args ++ .{@as(*Runnable.Execution, undefined)}))) {
        return self.submitNotified(bytes, result_allocator, function, args, discard, ignoreRetirement);
    }
    fn ignoreRetirement(_: anytype, _: ?anyerror) void {}
    /// Mandatory notification runs even if activation fails before application
    /// code starts. Streaming consumers can close their queues and then join;
    /// no new CPU grant is required merely to publish terminal ownership.
    pub fn submitNotified(self: ExecutionScope, bytes: usize, result_allocator: A, comptime function: anytype, args: anytype, comptime discard: anytype, comptime retired: anytype) !?Task(@TypeOf(@call(.auto, function, args ++ .{@as(*Runnable.Execution, undefined)}))) {
        const Result = @TypeOf(@call(.auto, function, args ++ .{@as(*Runnable.Execution, undefined)}));
        const retained = std.math.add(u64, bytes, @sizeOf(ScopeControl)) catch return error.InvalidRunnableDemand;
        const control = try self.allocator.create(ScopeControl);
        errdefer self.allocator.destroy(control);
        control.* = .{ .scope = self, .result_allocator = result_allocator };
        control.execution.reserveChild(self.execution, self.io, retained) catch |err| {
            if (err == error.SchedulerFull) {
                self.allocator.destroy(control);
                return null;
            }
            return err;
        };
        errdefer control.execution.finish();
        const Worker = struct {
            fn dispose(allocator: A, result: Result) void {
                if (result) |value| discard(allocator, value) else |_| {}
            }
            fn run(owner: *ScopeControl, arguments: @TypeOf(args)) Result {
                defer owner.completed.store(true, .release);
                const result = execute(owner, arguments);
                retired(arguments, if (result) |_| null else |err| err);
                return result;
            }
            fn execute(owner: *ScopeControl, arguments: @TypeOf(args)) Result {
                try owner.execution.activateChild();
                // Finished CPU work still retains metadata and result ownership
                // until joining, but must not prevent the coordinator resuming.
                defer owner.execution.park(.resource_blocked) catch {};
                const value = try @call(.auto, function, arguments ++ .{&owner.execution});
                errdefer discard(owner.result_allocator, value);
                try owner.execution.checkpoint();
                return value;
            }
        };
        const future = try self.io.concurrent(Worker.run, .{ control, args });
        return .{ .scheduler = undefined, .bytes = bytes, .future = future, .scope = control, .discard_scoped = Worker.dispose, .discard_allocator = result_allocator };
    }
};

const ScopeControl = struct {
    scope: ExecutionScope,
    result_allocator: A,
    execution: Runnable.Execution = .{},
    completed: std.atomic.Value(bool) = .init(false),
};

test "SQL scoped CPU worker joins before retiring original charges and discards denied results" {
    const Fixture = struct {
        execution: *Runnable.Execution,
        rejected: bool,
        waits: usize = 0,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(self.execution.entry.?.children != 0);
            self.waits += 1;
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.query);
            if (self.rejected) return error.Forbidden;
        }
        fn work(a: A, execution: *Runnable.Execution) anyerror!*usize {
            try execution.checkpoint();
            const value = try a.create(usize);
            value.* = 42;
            return value;
        }
        fn discard(a: A, value: *usize) void {
            a.destroy(value);
        }
    };
    for ([_]bool{ false, true }) |rejected| {
        var owner = try Runnable.Scheduler.init(.{ .slots = 2 });
        var parent: Runnable.Execution = .{};
        try parent.start(&owner, .{ .io = std.testing.io }, .query);
        defer parent.finish();
        var fixture = Fixture{ .execution = &parent, .rejected = rejected };
        const scope = ExecutionScope{ .execution = &parent, .allocator = std.testing.allocator, .io = std.testing.io, .wait = .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } };
        var task = (try scope.submit(31, std.testing.allocator, Fixture.work, .{std.testing.allocator}, Fixture.discard)).?;
        try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
        try std.testing.expect(owner.stats().retained_bytes >= 31 + @sizeOf(ScopeControl));
        if (rejected) {
            try std.testing.expectError(error.Forbidden, task.await(std.testing.io));
        } else {
            const value = try task.await(std.testing.io);
            try std.testing.expectEqual(@as(usize, 42), value.*);
            Fixture.discard(std.testing.allocator, value);
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.waits);
        try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "SQL scoped CPU worker denied suspension still physically joins canceled publication" {
    const Fixture = struct {
        execution: *Runnable.Execution,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.checkpoint();
            try self.execution.park(.resource_blocked);
        }
        fn end(_: *anyopaque) !void {
            return error.TestUnexpectedResume;
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn work(execution: *Runnable.Execution) anyerror!usize {
            try execution.checkpoint();
            return 7;
        }
        fn discard(_: A, _: usize) void {}
    };
    var owner = try Runnable.Scheduler.init(.{ .slots = 2 });
    var canceled: std.atomic.Value(bool) = .init(false);
    var parent: Runnable.Execution = .{};
    try parent.start(&owner, .{ .io = std.testing.io, .cancellation = .{ .ptr = &canceled, .requested = Fixture.requested } }, .query);
    defer parent.finish();
    var fixture = Fixture{ .execution = &parent };
    const scope = ExecutionScope{ .execution = &parent, .allocator = std.testing.allocator, .io = std.testing.io, .wait = .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } };
    var task = (try scope.submit(17, std.testing.allocator, Fixture.work, .{}, Fixture.discard)).?;
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, task.await(std.testing.io));
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "SQL scoped CPU metadata allocation and registry rejection roll back without another queue" {
    const Fixture = struct {
        execution: *Runnable.Execution,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.execution.enter(.query);
        }
        fn work(execution: *Runnable.Execution) anyerror!usize {
            try execution.checkpoint();
            return 11;
        }
        fn discard(_: A, _: usize) void {}
        fn scenario(a: A) !void {
            var owner = try Runnable.Scheduler.init(.{ .slots = 2, .max_operations = 3 });
            var parent: Runnable.Execution = .{};
            try parent.start(&owner, .{ .io = std.testing.io }, .query);
            defer parent.finish();
            var fixture = @This(){ .execution = &parent };
            const scope = ExecutionScope{ .execution = &parent, .allocator = a, .io = std.testing.io, .wait = .{ .ptr = &fixture, .begin = begin, .end = end } };
            var first = (scope.submit(19, std.testing.allocator, work, .{}, discard) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
                try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
                return err;
            }).?;
            defer if (first.future != null) {
                _ = first.cancel(std.testing.io) catch {};
            };
            const retained = owner.stats().retained_bytes;
            const denied = scope.submit(23, std.testing.allocator, work, .{}, discard) catch |err| {
                try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
                try std.testing.expectEqual(retained, owner.stats().retained_bytes);
                return err;
            };
            try std.testing.expect(denied == null);
            try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
            try std.testing.expectEqual(retained, owner.stats().retained_bytes);
            try std.testing.expectEqual(@as(usize, 11), try first.await(std.testing.io));
            try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
            try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.scenario, .{});
}
pub const LockedAllocator = struct {
    backing: A,
    mutex: std.atomic.Mutex = .unlocked,
    /// Immutable parent identity for admission through this synchronizing wrapper.
    pub fn parentAllocator(a: std.mem.Allocator) ?std.mem.Allocator {
        if (a.vtable.alloc != allocate) return null;
        return @as(*LockedAllocator, @ptrCast(@alignCast(a.ptr))).backing;
    }
    pub fn allocator(self: *LockedAllocator) A {
        return .{ .ptr = self, .vtable = &.{ .alloc = allocate, .resize = resize, .remap = remap, .free = free } };
    }
    fn lock(self: *LockedAllocator) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn allocate(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        return self.backing.rawAlloc(len, alignment, ra);
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) bool {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        return self.backing.rawResize(bytes, alignment, len, ra);
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ra: usize) ?[*]u8 {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        return self.backing.rawRemap(bytes, alignment, len, ra);
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *LockedAllocator = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
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
    max_io_bytes: usize = 32 * 1024 * 1024,
    max_bytes: usize = 64 * 1024 * 1024,
    workers: usize = 0,
    io_workers: usize = 0,
    io_bytes: usize = 0,
    bytes: usize = 0,
    peak_workers: usize = 0,
    peak_io_workers: usize = 0,
    peak_bytes: usize = 0,
    inline_tasks: usize = 0,
    fn lock(self: *Scheduler) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
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
        self.lock();
        defer self.mutex.unlock();
        const slots_full = switch (class) {
            .cpu => self.workers - self.io_workers >= self.max_workers,
            .io => self.io_workers >= self.max_io_workers,
        };
        const io_full = class == .io and bytes > @min(self.max_io_bytes, self.max_bytes / 2) -| self.io_bytes;
        if (slots_full or io_full or bytes > self.max_bytes -| self.bytes) {
            self.inline_tasks += 1;
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
    }
    /// Speculative tasks retain no result buffers outside independently bounded
    /// caches. Release execution admission at completion, while Task still owns
    /// the join and the descriptor needed for canceled-before-start workers.
    pub fn submitTransient(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.cpu, io, bytes, function, args);
    }
    pub fn submitIoTransient(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitTransientClass(.io, io, bytes, function, args);
    }
    fn submitTransientClass(self: *Scheduler, class: Class, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        const Result = @TypeOf(@call(.auto, function, args));
        if (!self.acquireClass(class, bytes)) return null;
        const admission = std.heap.page_allocator.create(Admission) catch {
            self.releaseClass(class, bytes);
            return null;
        };
        admission.* = .{ .scheduler = self, .bytes = bytes, .class = class };
        const Worker = struct {
            fn run(control: *Admission, arguments: @TypeOf(args)) Result {
                defer {
                    control.release();
                    control.completed.store(true, .release);
                }
                return @call(.auto, function, arguments);
            }
        };
        const future = io.concurrent(Worker.run, .{ admission, args }) catch {
            admission.release();
            std.heap.page_allocator.destroy(admission);
            return null;
        };
        return .{ .scheduler = self, .bytes = bytes, .class = class, .future = future, .transient = admission };
    }
    pub fn submit(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitClass(.cpu, io, bytes, function, args);
    }
    pub fn submitIo(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        return self.submitClass(.io, io, bytes, function, args);
    }
    fn submitClass(self: *Scheduler, class: Class, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        if (!self.acquireClass(class, bytes)) return null;
        const future = io.concurrent(function, args) catch {
            self.releaseClass(class, bytes);
            return null;
        };
        return .{ .scheduler = self, .bytes = bytes, .class = class, .future = future };
    }
};
const Admission = struct {
    scheduler: *Scheduler,
    bytes: usize,
    class: Class,
    released: std.atomic.Value(bool) = .init(false),
    completed: std.atomic.Value(bool) = .init(false),
    fn release(self: *Admission) void {
        if (!self.released.swap(true, .acq_rel)) self.scheduler.releaseClass(self.class, self.bytes);
    }
};
pub fn Task(comptime Result: type) type {
    return struct {
        scheduler: *Scheduler,
        bytes: usize,
        class: Class = .cpu,
        future: ?std.Io.Future(Result),
        transient: ?*Admission = null,
        scope: ?*ScopeControl = null,
        discard_scoped: ?*const fn (A, Result) void = null,
        discard_allocator: A = undefined,
        fn release(self: *@This()) void {
            if (self.scope) |control| {
                const allocator = control.scope.allocator;
                control.execution.finish();
                allocator.destroy(control);
                self.scope = null;
            } else if (self.transient) |admission| {
                admission.release();
                std.heap.page_allocator.destroy(admission);
                self.transient = null;
            } else self.scheduler.releaseClass(self.class, self.bytes);
            self.future = null;
        }
        /// Logical completion permits nonblocking slot recycling. Callers
        /// must still join before retiring charges or borrowed input owners.
        pub fn isComplete(self: *const @This()) bool {
            if (self.scope) |control| return control.completed.load(.acquire);
            return if (self.transient) |admission| admission.completed.load(.acquire) else self.future == null;
        }
        pub fn await(self: *@This(), io: std.Io) Result {
            if (self.scope != null) return self.joinScoped(io, false);
            defer self.release();
            return self.future.?.await(io);
        }
        pub fn cancel(self: *@This(), io: std.Io) Result {
            if (self.scope != null) return self.joinScoped(io, true);
            defer self.release();
            return self.future.?.cancel(io);
        }
        fn joinScoped(self: *@This(), io: std.Io, comptime canceling: bool) Result {
            const scope = self.scope.?.scope;
            const discard = self.discard_scoped.?;
            const allocator = self.discard_allocator;
            scope.wait.begin(scope.wait.ptr) catch |err| {
                // Denied suspension cannot skip joining a published worker or
                // strand a successful owned result behind the original failure.
                const result = self.future.?.cancel(io);
                discard(allocator, result);
                self.release();
                return err;
            };
            const result = if (canceling) self.future.?.cancel(io) else self.future.?.await(io);
            self.release();
            scope.wait.end(scope.wait.ptr) catch |err| {
                discard(allocator, result);
                return err;
            };
            return result;
        }
    };
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

test "SQL scoped streaming retirement notifies before activation failure and preserves the coordinator" {
    const Clock = struct {
        var now_ns: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return now_ns.load(.acquire);
        }
    };
    const Fixture = struct {
        parent: *Runnable.Execution,
        calls: std.atomic.Value(usize) = .init(0),
        retired: std.Io.Event = .unset,
        failure: ?anyerror = null,
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.parent.checkpoint();
            try self.parent.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.parent.enter(.query);
        }
        fn work(self: *@This(), _: *Runnable.Execution) anyerror!void {
            _ = self.calls.fetchAdd(1, .monotonic);
        }
        fn notify(args: anytype, failure: ?anyerror) void {
            const self: *@This() = args[0];
            self.failure = failure;
            self.retired.set(std.testing.io);
        }
        fn discard(_: A, _: void) void {}
    };
    Clock.now_ns.store(0, .release);
    var owner = try Runnable.Scheduler.init(.{ .slots = 2, .max_wait_ms = 10 });
    var parent: Runnable.Execution = .{};
    try parent.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now }, .query);
    defer parent.finish();
    var fixture = Fixture{ .parent = &parent };
    const scope = ExecutionScope{ .execution = &parent, .allocator = std.testing.allocator, .io = std.testing.io, .wait = .{ .ptr = &fixture, .begin = Fixture.begin, .end = Fixture.end } };
    var task = (try scope.submitNotified(8192, std.testing.allocator, Fixture.work, .{&fixture}, Fixture.discard, Fixture.notify)).?;
    defer if (task.future != null) {
        _ = task.cancel(std.testing.io) catch {};
    };
    var queued = false;
    for (0..2000) |_| {
        if (owner.stats().queued != 0) {
            queued = true;
            break;
        }
        try std.Io.sleep(std.testing.io, .fromMilliseconds(1), .awake);
    }
    try std.testing.expect(queued);
    Clock.now_ns.store(10 * std.time.ns_per_ms, .release);
    try fixture.retired.waitTimeout(std.testing.io, .{ .duration = .{ .clock = .awake, .raw = .fromSeconds(2) } });
    try std.testing.expectEqual(@as(usize, 0), fixture.calls.load(.acquire));
    try std.testing.expectEqual(error.RunnableWaitTimeout, fixture.failure.?);
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    try std.testing.expectError(error.RunnableWaitTimeout, task.await(std.testing.io));
    try parent.checkpoint();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test "SQL scoped completion readiness retains child ownership until physical join" {
    const Fixture = struct {
        fn work(a: A, _: *Runnable.Execution) anyerror!*usize {
            const value = try a.create(usize);
            value.* = 31;
            return value;
        }
        fn discard(a: A, value: *usize) void {
            a.destroy(value);
        }
    };
    var scheduler = try Runnable.Scheduler.init(.{ .slots = 2 });
    var parent: Runnable.Execution = .{};
    try parent.start(&scheduler, .{ .io = std.testing.io }, .query);
    defer parent.finish();
    const scope: ExecutionScope = .{ .execution = &parent, .allocator = std.testing.allocator, .io = std.testing.io, .wait = @import("spill.zig").childWait(&parent) };
    var task = (try scope.submit(64, std.testing.allocator, Fixture.work, .{std.testing.allocator}, Fixture.discard)).?;
    defer if (task.future != null) {
        if (task.cancel(std.testing.io)) |value| Fixture.discard(std.testing.allocator, value) else |_| {}
    };
    try parent.park(.resource_blocked);
    while (!task.isComplete()) std.atomic.spinLoopHint();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().child_operations);
    try std.testing.expect(task.future != null and task.scope != null);
    try parent.enter(.query);
    const value = try task.await(std.testing.io);
    defer Fixture.discard(std.testing.allocator, value);
    try std.testing.expectEqual(@as(usize, 31), value.*);
    try std.testing.expect(task.isComplete());
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().child_operations);
    parent.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
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
