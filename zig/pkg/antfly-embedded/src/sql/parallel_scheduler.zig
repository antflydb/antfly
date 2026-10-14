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
