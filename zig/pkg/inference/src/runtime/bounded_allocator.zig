// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Request-local live allocation ceiling. The owner must reserve `limit` with
//! process admission first and use a reclaiming backing allocator. Wrapping an
//! arena would count logical frees while retaining physical memory. This owner
//! must outlive every allocation made through it. Accounting is serialized;
//! statistics may be read only once any allocation workers have joined.
const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BoundedAllocator = struct {
    pub const AllocationFailure = struct {
        kind: enum { declared_limit, backing_allocator },
        requested_bytes: usize,
        live_bytes: usize,
        peak_bytes: usize,
        limit_bytes: usize,
    };

    backing: Allocator,
    limit: usize,
    live: usize = 0,
    peak: usize = 0,
    denied: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    /// Optional request-local diagnostic for terminal alloc failures. A
    /// resize/remap miss is not terminal: std.mem.Allocator may recover with
    /// alloc+copy. Called under this allocator's lock; must not allocate or
    /// reenter the allocator. The observer must outlive all allocations.
    failure_context: ?*anyopaque = null,
    allocation_failed: ?*const fn (?*anyopaque, AllocationFailure) void = null,

    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    pub fn allocator(self: *@This()) Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }
    fn permits(self: *@This(), growth: usize) bool {
        if (growth > self.limit -| self.live) {
            self.denied = true;
            return false;
        }
        return true;
    }
    fn record(self: *@This(), old: usize, new: usize) void {
        std.debug.assert(self.live >= old);
        self.live = self.live - old + new;
        self.peak = @max(self.peak, self.live);
    }
    fn failed(self: *@This(), kind: @FieldType(AllocationFailure, "kind"), len: usize) void {
        if (self.allocation_failed) |observe| observe(self.failure_context, .{
            .kind = kind,
            .requested_bytes = len,
            .live_bytes = self.live,
            .peak_bytes = self.peak,
            .limit_bytes = self.limit,
        });
    }
    fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.permits(len)) {
            self.failed(.declared_limit, len);
            return null;
        }
        const ptr = self.backing.rawAlloc(len, alignment, ret) orelse {
            self.failed(.backing_allocator, len);
            return null;
        };
        self.record(0, len);
        return ptr;
    }
    fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.permits(len -| bytes.len)) return false;
        if (!self.backing.rawResize(bytes, alignment, len, ret)) return false;
        self.record(bytes.len, len);
        return true;
    }
    fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, ret: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        if (!self.permits(len -| bytes.len)) return null;
        const ptr = self.backing.rawRemap(bytes, alignment, len, ret) orelse return null;
        self.record(bytes.len, len);
        return ptr;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.lock();
        defer self.mutex.unlock();
        self.backing.rawFree(bytes, alignment, ret);
        self.record(bytes.len, 0);
    }
};

test "request bounded allocator denies growth and reclaims capacity without retaining failed allocations" {
    var budget = BoundedAllocator{ .backing = std.testing.allocator, .limit = 64 };
    const a = budget.allocator();
    var bytes = try a.alloc(u8, 32);
    @memset(bytes, 7);
    try std.testing.expectEqual(@as(usize, 32), budget.live);
    try std.testing.expectError(error.OutOfMemory, a.alloc(u8, 33));
    try std.testing.expect(!a.resize(bytes, 65));
    try std.testing.expectEqual(@as(?[]u8, null), a.remap(bytes, 65));
    try std.testing.expectEqual(@as(usize, 32), budget.live);
    try std.testing.expectEqual(@as(u8, 7), bytes[0]);
    if (a.resize(bytes, 16)) bytes = bytes[0..16];
    a.free(bytes);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    const full = try a.alloc(u8, 64);
    a.free(full);
    try std.testing.expectEqual(@as(usize, 64), budget.peak);
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(budget.denied);
}

test "request bounded allocator backing failure consumes no capacity" {
    var fail = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var budget = BoundedAllocator{ .backing = fail.allocator(), .limit = 64 };
    try std.testing.expectError(error.OutOfMemory, budget.allocator().alloc(u8, 32));
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    try std.testing.expect(!budget.denied);
}
