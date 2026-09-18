// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! Allocator ownership shared by the scheduler's class policy and the existing
//! storage ResourceManager. Both check the same reservation; never sum their
//! byte counters. This owner must outlive every allocation and stay at a stable
//! address. Its pinned completion credits survive execution suspend/resume.
const std = @import("std");
const resources = @import("../common/workload_resources.zig");
const resource_manager = @import("resource_manager.zig");

pub const WorkingMemory = struct {
    mutex: std.atomic.Mutex = .unlocked,
    backing: std.mem.Allocator,
    state: resources.RetainedStateLease,
    reservation: resource_manager.Reservation,
    minimum: u64,
    live_bytes: u64 = 0,
    last_failure: ?anyerror = null,

    pub fn init(manager: *resource_manager.ResourceManager, slice: resource_manager.Slice, ledger: *resources.Ledger, request: *const resources.RequestLease, backing: std.mem.Allocator, minimum: u64) !WorkingMemory {
        var state = try ledger.acquire(.retained_state, request, .{ .retained_bytes = minimum });
        errdefer state.release() catch unreachable;
        // Neither admission waits or calls an operator while a partial bundle
        // is held. A failed second grant returns the first immediately.
        const reservation = try manager.reserveWithoutReclaim(slice, minimum);
        return .{ .backing = backing, .state = state, .reservation = reservation, .minimum = minimum };
    }

    pub fn deinit(self: *WorkingMemory) void {
        std.debug.assert(self.live_bytes == 0);
        self.reservation.release();
        self.state.release() catch unreachable;
        self.* = undefined;
    }

    pub fn allocator(self: *WorkingMemory) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn lock(self: *WorkingMemory) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn grow(self: *WorkingMemory, bytes: usize) !void {
        const next = std.math.add(u64, self.live_bytes, bytes) catch return error.ResourceRequestTooLarge;
        const additional = next -| self.reservation.bytes;
        if (additional > 0) {
            try self.state.grow(.{ .retained_bytes = additional });
            errdefer self.state.shrinkRetained(additional) catch unreachable;
            // No oversized exception: the common policy is a hard envelope.
            try self.reservation.growBoundedOversized(additional, 1);
        }
        self.live_bytes = next;
    }

    fn shrink(self: *WorkingMemory, bytes: usize) void {
        std.debug.assert(bytes <= self.live_bytes);
        self.live_bytes -= bytes;
        const release = self.reservation.bytes - @max(self.live_bytes, self.minimum);
        self.reservation.shrink(release);
        self.state.shrinkRetained(release) catch unreachable;
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *WorkingMemory = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        self.grow(len) catch |err| {
            self.last_failure = err;
            return null;
        };
        return self.backing.rawAlloc(len, alignment, ret_addr) orelse {
            self.shrink(len);
            self.last_failure = error.OutOfMemory;
            return null;
        };
    }

    fn resize(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *WorkingMemory = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        const additional = new_len -| memory.len;
        self.grow(additional) catch |err| {
            self.last_failure = err;
            return false;
        };
        if (!self.backing.rawResize(memory, alignment, new_len, ret_addr)) {
            self.shrink(additional);
            return false;
        }
        if (new_len < memory.len) self.shrink(memory.len - new_len);
        return true;
    }

    fn remap(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *WorkingMemory = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        const additional = new_len -| memory.len;
        self.grow(additional) catch |err| {
            self.last_failure = err;
            return null;
        };
        const result = self.backing.rawRemap(memory, alignment, new_len, ret_addr) orelse {
            self.shrink(additional);
            return null;
        };
        if (new_len < memory.len) self.shrink(memory.len - new_len);
        return result;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *WorkingMemory = @ptrCast(@alignCast(ctx));
        self.lock();
        defer self.mutex.unlock();
        self.backing.rawFree(memory, alignment, ret_addr);
        self.shrink(memory.len);
    }
};

test "workload admission working memory checks class and ResourceManager before allocating" {
    const total: resources.Bundle = .{ .handles = 8, .requests = 2, .runnable = 1, .retained_bytes = 100 };
    var ledger = try resources.Ledger.init(std.testing.allocator, .{ .total = total, .lanes = @splat(.{ .ceiling = total }) });
    defer ledger.deinit();
    var manager = resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 80 } });
    defer manager.deinit(std.testing.allocator);
    var request = try ledger.admit(.general_read, 0);
    defer request.release() catch unreachable;
    var memory = try WorkingMemory.init(&manager, .relational_preparation_working_set, &ledger, &request, std.testing.allocator, 20);
    defer memory.deinit();
    const allocator = memory.allocator();
    const first = try allocator.alloc(u8, 60);
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 30)); // manager limit
    try std.testing.expectEqual(@as(u64, 60), ledger.snapshot().total.retained_bytes);
    try std.testing.expectEqual(@as(u64, 60), manager.snapshot().memory.used_bytes);
    try std.testing.expectError(error.OutOfMemory, allocator.alloc(u8, 50)); // class limit
    allocator.free(first);
    try std.testing.expectEqual(@as(u64, 20), ledger.snapshot().total.retained_bytes);
    try std.testing.expectEqual(@as(u64, 20), manager.snapshot().memory.used_bytes);
}

test "workload admission working memory rollback and completion credits survive suspension" {
    const scheduler_mod = @import("../common/workload_scheduler.zig");
    const total: resources.Bundle = .{ .handles = 8, .requests = 2, .runnable = 1, .retained_bytes = 100 };
    var ledger = try resources.Ledger.init(std.testing.allocator, .{ .total = total, .lanes = @splat(.{ .ceiling = total }) });
    defer ledger.deinit();
    var manager = resource_manager.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 } });
    defer manager.deinit(std.testing.allocator);
    var request = try ledger.admit(.general_read, 0);
    defer request.release() catch unreachable;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    {
        var memory = try WorkingMemory.init(&manager, .relational_preparation_working_set, &ledger, &request, failing.allocator(), 20);
        defer memory.deinit();
        try std.testing.expectError(error.OutOfMemory, memory.allocator().alloc(u8, 40));
        try std.testing.expectEqual(@as(u64, 20), ledger.snapshot().total.retained_bytes);
        try std.testing.expectEqual(@as(u64, 20), manager.snapshot().memory.used_bytes);
    }
    var scheduler = try scheduler_mod.Scheduler.init(&ledger, .{});
    var memory = try WorkingMemory.init(&manager, .relational_preparation_working_set, &ledger, &request, std.testing.allocator, 80);
    defer memory.deinit();
    var job = try scheduler.acquire(&request, .general_read, .{ .runnable = 1 }, 1, 0, .{ .io = std.testing.io });
    const buffer = try memory.allocator().alloc(u8, 70);
    var continuation = try job.yieldState(0, 1); // working memory owns the bytes
    try std.testing.expectEqual(@as(u64, 0), ledger.snapshot().total.runnable);
    try std.testing.expectEqual(@as(u64, 80), ledger.snapshot().total.retained_bytes);
    job = try scheduler.acquireResume(&request, .general_read, &continuation, .{ .runnable = 1 }, 1, 80, .{ .io = std.testing.io });
    memory.allocator().free(buffer);
    job.release(1);
    try std.testing.expectEqual(@as(u64, 80), manager.snapshot().memory.used_bytes);
}
