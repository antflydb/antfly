// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Shared admission for native CPU/I/O tasks. Required work runs inline when
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
pub const Scheduler = struct {
    mutex: std.atomic.Mutex = .unlocked,
    max_workers: usize = 8,
    max_bytes: usize = 64 * 1024 * 1024,
    workers: usize = 0,
    bytes: usize = 0,
    peak_workers: usize = 0,
    peak_bytes: usize = 0,
    inline_tasks: usize = 0,
    fn lock(self: *Scheduler) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn acquire(self: *Scheduler, bytes: usize) bool {
        self.lock();
        defer self.mutex.unlock();
        if (self.workers >= self.max_workers or bytes > self.max_bytes -| self.bytes) {
            self.inline_tasks += 1;
            return false;
        }
        self.workers += 1;
        self.bytes += bytes;
        self.peak_workers = @max(self.peak_workers, self.workers);
        self.peak_bytes = @max(self.peak_bytes, self.bytes);
        return true;
    }
    fn release(self: *Scheduler, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.workers != 0 and self.bytes >= bytes);
        self.workers -= 1;
        self.bytes -= bytes;
    }
    /// Speculative tasks retain no result buffers outside independently bounded
    /// caches. Release execution admission at completion, while Task still owns
    /// the join and the descriptor needed for canceled-before-start workers.
    pub fn submitTransient(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        const Result = @TypeOf(@call(.auto, function, args));
        if (!self.acquire(bytes)) return null;
        const admission = std.heap.page_allocator.create(Admission) catch {
            self.release(bytes);
            return null;
        };
        admission.* = .{ .scheduler = self, .bytes = bytes };
        const Worker = struct {
            fn run(control: *Admission, arguments: @TypeOf(args)) Result {
                defer control.release();
                return @call(.auto, function, arguments);
            }
        };
        const future = io.concurrent(Worker.run, .{ admission, args }) catch {
            admission.release();
            std.heap.page_allocator.destroy(admission);
            return null;
        };
        return .{ .scheduler = self, .bytes = bytes, .future = future, .transient = admission };
    }
    pub fn submit(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        if (!self.acquire(bytes)) return null;
        const future = io.concurrent(function, args) catch {
            self.release(bytes);
            return null;
        };
        return .{ .scheduler = self, .bytes = bytes, .future = future };
    }
};
const Admission = struct {
    scheduler: *Scheduler,
    bytes: usize,
    released: std.atomic.Value(bool) = .init(false),
    fn release(self: *Admission) void {
        if (!self.released.swap(true, .acq_rel)) self.scheduler.release(self.bytes);
    }
};
pub fn Task(comptime Result: type) type {
    return struct {
        scheduler: *Scheduler,
        bytes: usize,
        future: ?std.Io.Future(Result),
        transient: ?*Admission = null,
        fn release(self: *@This()) void {
            if (self.transient) |admission| {
                admission.release();
                std.heap.page_allocator.destroy(admission);
                self.transient = null;
            } else self.scheduler.release(self.bytes);
            self.future = null;
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
