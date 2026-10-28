// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const scheduling = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const admission = @import("antfly_local_sources").common_bounded_admission;
const memory = @import("../common/read_memory.zig");

/// Stable operator owner, retained through all buffers, allocator views and
/// physical joins. Production eligibility additionally requires verified work,
/// continuation, snapshot and backend-resource bounds for that operator.
pub const Owner = struct {
    execution: scheduling.Execution = .{},
    allocations: memory.Group,
    started: bool = false,
    closed: bool = false,

    pub fn init(pool: *memory.Pool, maximum_heap_bytes: usize) !Owner {
        return .{ .allocations = try memory.Group.init(pool, .bounded, maximum_heap_bytes) };
    }

    /// Call at the final address, before obtaining any allocator or publishing
    /// operator resources. Both physical and scheduling transition credits exist
    /// before work starts; failure retires this unpublished owner completely.
    pub fn start(self: *Owner, scheduler: *scheduling.Scheduler, options: admission.Options, budget: scheduling.ReadTransitionBudget, cleanup: admission.Options, cleanup_duration_ns: u64) !void {
        if (self.closed or self.started) return error.InvalidRunnableTransition;
        errdefer {
            self.execution.finish();
            self.allocations.deinit();
            self.closed = true;
        }
        try self.allocations.reserveTransition();
        try self.execution.startDemotableRead(scheduler, options, budget, cleanup, cleanup_duration_ns);
        try scheduler.attachReadResources(&self.execution.entry.?, .{ .context = &self.allocations, .begin = begin, .try_general = tryGeneral });
        self.started = true;
    }

    fn begin(raw: *anyopaque) callconv(.c) bool {
        const group: *memory.Group = @ptrCast(@alignCast(raw));
        group.beginTransition() catch return false;
        return true;
    }
    fn tryGeneral(raw: *anyopaque) callconv(.c) bool {
        const group: *memory.Group = @ptrCast(@alignCast(raw));
        return group.tryEnterGeneral() catch false;
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        std.debug.assert(self.started and !self.closed);
        return self.allocations.allocator();
    }

    /// Every physical consumer and allocation must already be retired. Releasing
    /// the CPU/metadata owner first would permit reuse before the heap joins.
    pub fn deinit(self: *Owner) void {
        if (self.closed) return;
        self.allocations.deinit();
        self.execution.finish();
        self.closed = true;
    }
};

test "runnable read resources require both scheduler and physical destination capacity" {
    const io = std.testing.io;
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true, .max_operations = 12 });
    defer scheduler.close();
    var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 64, 64, 64 }, .groups = .{ 2, 2, 1 } });
    defer pool.deinit();
    var general_heap = try memory.Group.init(&pool, .general, 64);
    defer general_heap.deinit();
    const ordinary_bytes = try general_heap.allocator().alloc(u8, 64 - memory.Group.overhead(.of(u8)));
    var ordinary_live = true;
    defer if (ordinary_live) general_heap.allocator().free(ordinary_bytes);
    var general: [6]scheduling.Entry = undefined;
    for (&general) |*entry| {
        entry.* = .{ .options = .{ .io = io }, .class = .query, .bytes = 100 };
        try scheduler.submit(entry);
    }
    defer for (&general) |*entry| scheduler.finish(entry);
    var read = try Owner.init(&pool, 64);
    defer read.deinit();
    try read.start(&scheduler, .{ .io = io, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 100, .estimated_work_ns = 1 }, .{ .io = io }, 250 * std.time.ns_per_ms);
    const bytes = try read.allocator().dupe(u8, "live immutable continuation");
    defer read.allocator().free(bytes);
    const address = bytes.ptr;
    try scheduler.beginReadDemotion(&read.execution.entry.?, .general);
    try std.testing.expectEqual(memory.Lane.transition, read.allocations.lane);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().bounded_read_slots);
    try std.testing.expect(!try scheduler.tryCompleteReadDemotion(&read.execution.entry.?));
    scheduler.finish(&general[0]);
    // Metadata capacity is now available, but physical general memory is full.
    try std.testing.expect(!try scheduler.tryCompleteReadDemotion(&read.execution.entry.?));
    try std.testing.expectEqual(memory.Lane.transition, read.allocations.lane);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().read_transition_operations);
    general_heap.allocator().free(ordinary_bytes);
    ordinary_live = false;
    try std.testing.expect(try scheduler.tryCompleteReadDemotion(&read.execution.entry.?));
    try std.testing.expectEqual(memory.Lane.general, read.allocations.lane);
    try std.testing.expectEqual(scheduling.ReadLane.general, read.execution.entry.?.read_lane);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().read_transition_reserved_operations);
    try std.testing.expect(bytes.ptr == address);
    try std.testing.expectEqualStrings("live immutable continuation", bytes);
    try scheduler.unblock(&read.execution.entry.?);
    for (&general) |*entry| scheduler.finish(entry);
    try scheduler.waitRetained(&read.execution.entry.?);
}

test "runnable read resources canceled transition retains heap until physical cleanup" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer scheduler.close();
    var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 128, 128, 128 }, .groups = .{ 2, 2, 2 } });
    defer pool.deinit();
    var read = try Owner.init(&pool, 128);
    defer read.deinit();
    try read.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io, .native_now_ns = Clock.now }, std.time.ns_per_s);
    const bytes = try read.allocator().dupe(u8, "preallocated cleanup state");
    var live = true;
    defer if (live) read.allocator().free(bytes);
    const held = pool.stats().total_used;
    try scheduler.beginReadDemotion(&read.execution.entry.?, .general);
    Clock.tick = 100;
    try std.testing.expectError(error.DeadlineExceeded, scheduler.tryCompleteReadDemotion(&read.execution.entry.?));
    try std.testing.expectEqual(held, pool.stats().total_used);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
    try std.testing.expectError(error.OutOfMemory, read.allocator().alloc(u8, 1));
    try read.execution.activateCleanup();
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().cleanup_runnable_slots);
    read.allocator().free(bytes);
    live = false;
    read.deinit();
    try std.testing.expectEqual(@as(usize, 0), pool.stats().total_used);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
}

test "runnable read resources automatic checkpoint demotion carries the physical allocation owner" {
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
    };
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer scheduler.close();
    var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 128, 128, 128 }, .groups = .{ 2, 2, 2 } });
    defer pool.deinit();
    var read = try Owner.init(&pool, 128);
    defer read.deinit();
    try read.start(&scheduler, .{ .io = std.testing.io, .native_now_ns = Clock.now, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = std.time.ns_per_ms }, .{ .io = std.testing.io, .native_now_ns = Clock.now }, std.time.ns_per_s);
    const bytes = try read.allocator().dupe(u8, "same allocation after measured demotion");
    defer read.allocator().free(bytes);
    const address = bytes.ptr;
    const held = pool.stats().total_used;
    Clock.tick = 10 * std.time.ns_per_ms;
    try read.execution.checkpoint();
    try std.testing.expectEqual(memory.Lane.general, read.allocations.lane);
    try std.testing.expectEqual(scheduling.ReadLane.general, read.execution.entry.?.read_lane);
    try std.testing.expectEqual(held, pool.stats().used[@backingInt(memory.Lane.general)]);
    try std.testing.expectEqual(held, pool.stats().total_used);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_bytes);
    try std.testing.expectEqual(@as(u64, 4), scheduler.stats().read_service_debt_paid);
    try std.testing.expect(bytes.ptr == address);
    try std.testing.expectEqualStrings("same allocation after measured demotion", bytes);
}

test "runnable read resources unpublished startup failure retires both reservations" {
    var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 128, 128, 128 }, .groups = .{ 2, 2, 1 } });
    defer pool.deinit();
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 4, .cleanup_slots = 1, .read_isolation = true });
    defer scheduler.close();
    {
        var holder = try memory.Group.init(&pool, .bounded, 128);
        defer holder.deinit();
        try holder.reserveTransition();
        var denied = try Owner.init(&pool, 128);
        defer denied.deinit();
        try std.testing.expectError(error.ReadMemoryCapacityUnavailable, denied.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io }, std.time.ns_per_s));
        try std.testing.expect(denied.closed);
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 1), pool.stats().groups[@backingInt(memory.Lane.bounded)]);
    }
    scheduler.close();
    var closed = try Owner.init(&pool, 128);
    defer closed.deinit();
    try std.testing.expectError(error.SchedulerClosed, closed.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 100 }, .{ .max_retained_bytes = 100, .max_wait_ms = 25, .estimated_work_ns = 1 }, .{ .io = std.testing.io }, std.time.ns_per_s));
    try std.testing.expect(closed.closed);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_groups);
    try std.testing.expectEqual(@as(usize, 0), pool.stats().transition_reserved_bytes);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().read_transition_reserved_operations);
}

test "runnable read resources CPU loan preserves physical general heap limits and buffer ownership" {
    var scheduler = try scheduling.Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2 });
    defer scheduler.close();
    var pool = try memory.Pool.init(std.testing.allocator, null, .{ .bytes = .{ 256, 256, 256 }, .groups = .{ 2, 2, 2 } });
    defer pool.deinit();
    var heap = try memory.Group.init(&pool, .general, 256);
    defer heap.deinit();
    const data = try heap.allocator().alloc(u8, 256 - memory.Group.overhead(.of(u8)));
    defer heap.allocator().free(data);
    @memset(data, 0xa5);
    const address = data.ptr;
    const held = pool.stats();
    var base: scheduling.Entry = .{ .options = .{ .io = std.testing.io }, .class = .background };
    try scheduler.submit(&base);
    defer scheduler.finish(&base);
    var loan: scheduling.Execution = .{};
    defer loan.finish();
    try loan.startBorrowable(&scheduler, .{ .io = std.testing.io, .retained_bytes = 17 }, .query, .{ .max_non_yield_ns = 1000 });
    try std.testing.expectEqual(@as(usize, 1), scheduler.cpuBorrowStats().active);
    try std.testing.expectError(error.OutOfMemory, heap.allocator().alloc(u8, 1));
    try std.testing.expectEqualDeep(held.used, pool.stats().used);
    try std.testing.expectEqualDeep(held.groups, pool.stats().groups);
    var protected: scheduling.Entry = .{ .options = .{ .io = std.testing.io }, .class = .query, .read_lane = .bounded };
    try scheduler.submit(&protected);
    defer scheduler.finish(&protected);
    try std.testing.expectEqual(scheduling.State.running, scheduler.state(&protected));
    try std.testing.expectEqual(@as(usize, 1), scheduler.cpuBorrowStats().active);
    try std.testing.expectEqualDeep(held.used, pool.stats().used);
    try std.testing.expect(data.ptr == address);
    for (data) |byte| try std.testing.expectEqual(@as(u8, 0xa5), byte);
    scheduler.finish(&protected);
    loan.finish();
    scheduler.finish(&base);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    try std.testing.expectEqual(scheduler.cpuBorrowStats().grants, scheduler.cpuBorrowStats().returns);
}
