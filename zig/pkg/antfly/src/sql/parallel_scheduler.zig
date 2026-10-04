// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Shared admission for native CPU/I/O tasks. Required work runs inline when
//! saturated; speculative work yields. Leases end only after await/cancel joins
//! the worker, so canceled-before-start tasks cannot strand an admission slot.
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
    pub fn submit(self: *Scheduler, io: std.Io, bytes: usize, comptime function: anytype, args: anytype) ?Task(@TypeOf(@call(.auto, function, args))) {
        if (!self.acquire(bytes)) return null;
        const future = io.concurrent(function, args) catch {
            self.release(bytes);
            return null;
        };
        return .{ .scheduler = self, .bytes = bytes, .future = future };
    }
};
pub fn Task(comptime Result: type) type {
    return struct {
        scheduler: *Scheduler,
        bytes: usize,
        future: ?std.Io.Future(Result),
        pub fn await(self: *@This(), io: std.Io) Result {
            defer {
                self.scheduler.release(self.bytes);
                self.future = null;
            }
            return self.future.?.await(io);
        }
        pub fn cancel(self: *@This(), io: std.Io) Result {
            defer {
                self.scheduler.release(self.bytes);
                self.future = null;
            }
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
