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
    maximum: ?u64 = null,
    owns_state: bool = true,
    live_bytes: u64 = 0,
    last_failure: ?anyerror = null,

    /// Optional physical allocator for scoped dense state. The driver retains
    /// its request until all allocations are freed and this owner is retired.
    pub fn forDenseDriver(manager: ?*resource_manager.ResourceManager, driver: *const resource_manager.ResourceManager.DenseDriverLease, backing: std.mem.Allocator) !?WorkingMemory {
        const owner = manager orelse return null;
        const runtime = owner.dense_execution orelse return null;
        if (runtime.config.max_working_bytes == 0) return null;
        const scheduled = driver.scheduledLease() orelse return error.InvalidLease;
        const request = if (scheduled.request) |*value| value else return error.InvalidLease;
        return try init(owner, .dense_search_working_set, &runtime.ledger, request, backing, 0);
    }

    /// Scan/output continuations reserve their maximum before acquiring a
    /// snapshot. This allocator checks the same prepaid bytes against the
    /// ResourceManager and never allocates beyond that completion bundle.
    pub fn forReadDriverPrepaid(manager: *resource_manager.ResourceManager, driver: *resource_manager.DenseExecution.Runtime.Lease, backing: std.mem.Allocator) !WorkingMemory {
        const state = driver.prepaid_state orelse return error.InvalidLease;
        if (driver.runtime != manager.dense_execution) return error.InvalidLease;
        return .{
            .backing = backing,
            .state = state,
            .reservation = manager.reserveWithoutReclaim(.relational_preparation_working_set, driver.prepaid_bytes) catch |err| switch (err) {
                error.ResourceBudgetExceeded => return error.AdmissionBytesExhausted,
                else => return err,
            },
            .minimum = driver.prepaid_bytes,
            .maximum = driver.prepaid_bytes,
            .owns_state = false,
        };
    }

    pub fn allocationFailure(self: *const WorkingMemory, err: anyerror) anyerror {
        if (err != error.OutOfMemory) return err;
        return switch (self.last_failure orelse err) {
            error.ResourceRequestTooLarge => error.AdmissionRequestTooLarge,
            error.ResourceTemporarilyUnavailable, error.ResourceBudgetExceeded => error.AdmissionBytesExhausted,
            else => err,
        };
    }

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
        if (self.owns_state) self.state.release() catch unreachable;
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
        if (self.maximum) |maximum| if (next > maximum) return error.ResourceRequestTooLarge;
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

test "workload admission prepaid scan reservation denial preserves caller ownership" {
    const alloc = std.testing.allocator;
    var manager = resource_manager.ResourceManager.init(.{ .identity_allocator = alloc, .memory_budget = .{ .hard_limit_bytes = 80 } });
    defer manager.deinit(alloc);
    try manager.configureReadExecution(.{ .max_runnable_tasks = 1, .max_outstanding_tasks = 2, .max_working_bytes = 100 });
    var driver = (try manager.acquireReadDriverWithState(std.testing.io, .{ .io = std.testing.io }, 90)).?;
    defer driver.release();
    try std.testing.expectError(error.AdmissionBytesExhausted, WorkingMemory.forReadDriverPrepaid(&manager, &driver, alloc));
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.used_bytes);
    try std.testing.expectEqual(@as(u64, 90), manager.denseExecutionStats().working_bytes);
    driver.release();
    try std.testing.expectEqual(@as(u64, 0), manager.denseExecutionStats().working_bytes);
}

test "workload admission prepaid scan buffers retain completion credit across output wait and cancellation" {
    const alloc = std.testing.allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();
    var manager = resource_manager.ResourceManager.init(.{ .identity_allocator = alloc });
    defer manager.deinit(alloc);
    try manager.configureReadExecution(.{ .max_runnable_tasks = 1, .max_outstanding_tasks = 2, .max_queued_tasks = 1, .max_wait_ms = 1000, .max_working_bytes = 100 });
    var cancelled = std.atomic.Value(bool).init(false);
    var driver = (try manager.acquireReadDriverWithState(io, .{ .io = io, .cancellation = @import("../common/cancellation.zig").CancellationToken.fromAtomic(&cancelled) }, 100)).?;
    defer driver.release();
    var memory = try WorkingMemory.forReadDriverPrepaid(&manager, &driver, alloc);
    defer memory.deinit();
    const buffer = try memory.allocator().alloc(u8, 60);
    defer memory.allocator().free(buffer);
    try std.testing.expectError(error.OutOfMemory, memory.allocator().alloc(u8, 41));
    try std.testing.expectEqual(error.AdmissionRequestTooLarge, memory.allocationFailure(error.OutOfMemory));
    try std.testing.expect(try driver.suspendOutput());
    try std.testing.expectEqual(@as(u64, 0), manager.denseExecutionStats().runnable);
    try std.testing.expectEqual(@as(u64, 1), manager.denseExecutionStats().outstanding);
    try std.testing.expectEqual(@as(u64, 100), manager.denseExecutionStats().working_bytes);
    try std.testing.expectEqual(@as(u64, 100), manager.sliceStats(.relational_preparation_working_set).used_bytes);
    var next = (try manager.acquireReadDriver(io, .{ .io = io })).?;
    next.release();
    try driver.resumeOutput();
    try std.testing.expect(try driver.suspendOutput());
    cancelled.store(true, .release);
    try std.testing.expectError(error.Canceled, driver.resumeOutput());
    // Failed resume retains the request and state until the actual buffers and
    // caller's snapshot have unwound; it never fabricates a runnable release.
    try std.testing.expect(driver.job == null and driver.continuation != null);
    try std.testing.expectEqual(@as(u64, 100), manager.denseExecutionStats().working_bytes);
}
