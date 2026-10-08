// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const allocation = @import("antfly_request_allocation_budget");
const writes = @import("runnable_write_team.zig");

fn checked(status: runtime.error_abi.Status) !void {
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}

fn canceled(raw: *const anyopaque) bool {
    const flag: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw));
    return flag.load(.acquire);
}

const Clock = struct {
    var current: u64 = 0;
    fn now() u64 {
        return current;
    }
};

test "SQL write team uses one retained root and a bounded child allocator" {
    var owner = try scheduler.Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 64 };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 4096 }, .write);
    var team = try writes.init(&execution, memory.allocator());
    const generation = execution.resume_generation;
    try team.reserve(8);
    try std.testing.expectEqual(@as(usize, 3), team.borrow.cpu_slots);
    try std.testing.expectEqual(@as(usize, 2), team.childCapacity());
    try std.testing.expect(execution.resume_generation > generation);
    const admitted = owner.stats();
    try std.testing.expectEqual(@as(usize, 1), admitted.operations);
    try std.testing.expectEqual(@as(u64, 4096), admitted.retained_bytes);
    try std.testing.expectEqual(@as(usize, 3), admitted.runnable_slots);
    const child = try team.allocator.alloc(u8, 64);
    try std.testing.expectEqual(@as(usize, 64), memory.stats().used_bytes);
    try std.testing.expectError(error.OutOfMemory, team.allocator.alloc(u8, 1));
    team.allocator.free(child);
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource))));
    try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.enter(team.borrow.context, @backingInt(runtime.runnable_abi.Class.write))));
    try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.checkpoint(team.borrow.context)));
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    var health: scheduler.Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io }, .control);
    try std.testing.expectEqual(@as(usize, 4), owner.stats().runnable_slots);
    health.finish();
    team.finish();
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, team.reserve(2));
    try std.testing.expect(team.state == .joined);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 4096), owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().runnable_slots);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
}

test "SQL protected commit team cannot widen into health capacity" {
    var owner = try scheduler.Scheduler.init(.{ .slots = 5, .control_slots = 2 });
    var ordinary: scheduler.Execution = .{};
    defer ordinary.finish();
    try ordinary.startWithSlots(&owner, .{ .io = std.testing.io }, .write, 3);
    var commit: scheduler.Execution = .{};
    defer commit.finish();
    try commit.start(&owner, .{ .io = std.testing.io }, .control);
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 64 };
    var team = try writes.init(&commit, memory.allocator());
    try team.reserve(100);
    try std.testing.expectEqual(@as(usize, 0), team.childCapacity());
    try std.testing.expectEqual(@as(usize, 1), team.borrow.cpu_slots);
    try std.testing.expectError(error.InvalidRunnableDemand, team.borrow.resizeTeam(2));
    try std.testing.expectEqual(@as(usize, 1), commit.entry.?.slots);
    var health: scheduler.Execution = .{};
    defer health.finish();
    try health.start(&owner, .{ .io = std.testing.io }, .control);
    try std.testing.expectEqual(@as(usize, 5), owner.stats().runnable_slots);
    team.finish();
    try std.testing.expectEqual(@as(usize, 5), owner.stats().runnable_slots);
}

test "SQL write team retains original cancellation and deadline fences" {
    var owner = try scheduler.Scheduler.init(.{ .slots = 4, .control_slots = 1 });
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 64 };
    var flag = std.atomic.Value(bool).init(false);
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    Clock.current = 0;
    try execution.start(&owner, .{ .io = std.testing.io, .retained_bytes = 256, .native_now_ns = Clock.now, .deadline_ns = 5, .cancellation = .{ .ptr = &flag, .requested = canceled } }, .write);
    var team = try writes.init(&execution, memory.allocator());
    flag.store(true, .release);
    try std.testing.expectError(error.Canceled, team.reserve(2));
    try std.testing.expect(team.state == .fresh);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), owner.stats().retained_bytes);
    flag.store(false, .release);
    Clock.current = 5;
    try std.testing.expectError(error.DeadlineExceeded, team.reserve(2));
    try std.testing.expect(team.state == .fresh);
    try std.testing.expectEqual(@as(usize, 1), team.borrow.cpu_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "SQL write team rejects absent read and wider control roots" {
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 64 };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try std.testing.expectError(error.InvalidRunnableTransition, writes.init(&execution, memory.allocator()));
    var owner = try scheduler.Scheduler.init(.{ .slots = 5, .control_slots = 2 });
    try execution.start(&owner, .{ .io = std.testing.io }, .query);
    try std.testing.expectError(error.InvalidRunnableTransition, writes.init(&execution, memory.allocator()));
    execution.finish();
    try execution.startWithSlots(&owner, .{ .io = std.testing.io }, .control, 2);
    try std.testing.expectError(error.InvalidRunnableDemand, writes.init(&execution, memory.allocator()));
}
