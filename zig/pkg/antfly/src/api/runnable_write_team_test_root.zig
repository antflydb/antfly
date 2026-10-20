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

test "SQL write team local completion bridge retains stable ownership and original fences" {
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    for ([_]bool{ false, true }) |control| {
        Clock.current = 0;
        var owner = try scheduler.Scheduler.init(.{ .slots = 5, .control_slots = 2, .max_operations = 8 });
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var flag = std.atomic.Value(bool).init(false);
        var execution: scheduler.Execution = .{};
        try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &flag, .requested = canceled }, .retained_bytes = 256 }, if (control) .control else .write);
        defer execution.finish();
        // The bridge stays in this scope through the owning finish.
        var bridge = try writes.Completion.init(&execution, memory.allocator());
        var team = try bridge.team();
        try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.checkpoint(team.borrow.context)));
        try team.reserve(8);
        defer team.finish();
        try std.testing.expectEqual(@as(usize, if (control) 1 else 3), team.borrow.cpu_slots);
        try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 });
        const receiver = &bridge.io.?;
        try std.testing.expectError(error.InvalidRunnableTransition, team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 200 }));
        try std.testing.expect(receiver == &bridge.io.?);
        try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource))));
        try std.testing.expectError(error.ExecutionWaitUnsupported, team.borrow.completionRemainingNs());
        try team.borrow.activateCompletion();
        try std.testing.expectEqual(@as(u64, 100), try team.borrow.completionRemainingNs());
        flag.store(true, .release);
        Clock.current = 11;
        try checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
        var health: scheduler.Execution = .{};
        defer health.finish();
        try health.start(&owner, .{ .io = io }, .control);
        health.finish();
        owner.close();
        try std.testing.expectEqual(@as(u64, 89), try team.borrow.completionRemainingNs());
        try checked(team.borrow.enter(team.borrow.context, team.borrow.parent_class));
        try checked(team.borrow.checkpoint(team.borrow.context));
        if (control) try std.testing.expectError(error.InvalidRunnableDemand, team.borrow.resizeTeam(2));
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 256), owner.stats().retained_bytes);
        try checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
        Clock.current = 100;
        try std.testing.expectError(error.DeadlineExceeded, team.borrow.completionRemainingNs());
        Clock.current = 101;
        try std.testing.expectError(error.DeadlineExceeded, checked(team.borrow.enter(team.borrow.context, team.borrow.parent_class)));
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 256), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    }
}

test "SQL write team local completion bridge clears failed reservations and rejects missing native clocks" {
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var owner = try scheduler.Scheduler.init(.{ .slots = 3 });
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    Clock.current = 0;
    try execution.start(&owner, .{ .io = io }, .write);
    var bridge = try writes.Completion.init(&execution, memory.allocator());
    var team = try bridge.team();
    try std.testing.expectError(error.InvalidArgument, team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }));
    try std.testing.expect(bridge.io == null);
    execution.finish();
    try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10 }, .write);
    Clock.current = 11;
    try std.testing.expectError(error.DeadlineExceeded, team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }));
    try std.testing.expect(bridge.io == null and bridge.clock_io == null);
    Clock.current = 0;
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 });
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, team.borrow.activateCompletion());
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
}

test "SQL write team remaining completion budget preserves the independent Io clock" {
    const IoClock = struct {
        var current: i96 = 1000;
        fn now(_: ?*anyopaque, _: std.Io.Clock) std.Io.Timestamp {
            return .{ .nanoseconds = current };
        }
    };
    IoClock.current = 1000;
    Clock.current = 0;
    defer Clock.current = 0;
    var vtable = std.testing.io.vtable.*;
    vtable.now = IoClock.now;
    const clock_io: std.Io = .{ .userdata = null, .vtable = &vtable };
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    const clock_borrow = runtime.native_abi.IoBorrow.init(&clock_io);
    var owner = try scheduler.Scheduler.init(.{ .slots = 3 });
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var flag = std.atomic.Value(bool).init(false);
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &flag, .requested = canceled }, .retained_bytes = 256 }, .write);
    var bridge = try writes.Completion.init(&execution, memory.allocator());
    var team = try bridge.team();
    try std.testing.expectError(error.ExecutionWaitUnsupported, team.borrow.completionRemainingNs());
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .clock_io = &clock_borrow, .deadline_ns = 1100 });
    try std.testing.expectError(error.ExecutionWaitUnsupported, team.borrow.completionRemainingNs());
    try team.borrow.activateCompletion();
    try std.testing.expectEqual(@as(u64, 100), try team.borrow.completionRemainingNs());
    flag.store(true, .release);
    Clock.current = 100000;
    owner.close();
    IoClock.current = 1099;
    try std.testing.expectEqual(@as(u64, 1), try team.borrow.completionRemainingNs());
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 256), owner.stats().retained_bytes);
    IoClock.current = 1100;
    try std.testing.expectError(error.DeadlineExceeded, team.borrow.completionRemainingNs());
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
}

test "SQL write team completion child uses independent bounded receiver and original completion deadline" {
    Clock.current = 0;
    defer Clock.current = 0;
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var owner = try scheduler.Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    defer owner.close();
    var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = 16384 };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 5, .retained_bytes = 256 }, .write);
    var bridge = try writes.Completion.init(&execution, memory.allocator());
    var team = try bridge.team();
    var child: runtime.runnable_abi.Child = undefined;
    const options: runtime.runnable_abi.ChildStart = .{ .io = &io_borrow, .retained_bytes = 17, .out_child = &child };
    try std.testing.expectError(error.ExecutionWaitUnsupported, team.borrow.reserveChild(&options));
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    try team.borrow.reserveCompletion(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 20 });
    try team.borrow.activateCompletion();
    // A whole team must shrink to its coordinator slot before independent
    // children are published. Denial releases the allocated receiver exactly.
    try team.borrow.resizeTeam(2);
    try std.testing.expectError(error.InvalidRunnableTransition, team.borrow.reserveChild(&options));
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    try team.borrow.resizeTeam(1);
    Clock.current = 6;
    owner.close();
    try team.borrow.reserveChild(&options);
    var child_active = true;
    defer if (child_active) child.deinit();
    try std.testing.expect(memory.used.load(.acquire) > 0);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try child.activate();
    try child.borrow.checkLifetime();
    try checked(child.borrow.park(child.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    try checked(child.borrow.enter(child.borrow.context, @backingInt(runtime.runnable_abi.Class.write)));
    Clock.current = 20;
    try std.testing.expectError(error.DeadlineExceeded, child.borrow.checkLifetime());
    child.deinit();
    child_active = false;
    try std.testing.expectEqual(@as(usize, 0), memory.used.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), execution.entry.?.children);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
}

test {
    _ = @import("runnable_participant_window.zig");
}

test "SQL write team producer borrow admits a foreground child without activating completion" {
    var owner = try scheduler.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer owner.close();
    var memory: allocation.Shared = .{ .child = std.testing.allocator, .limit = 64 * 1024 };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .write);
    var completion = try writes.Completion.init(&execution, memory.allocator());
    var team = try completion.team();
    try std.testing.expect(!execution.entry.?.completion_active);
    try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource))));
    const parent = try team.borrow.producerParent();
    const io = std.testing.io;
    const borrowed_io = runtime.native_abi.IoBorrow.init(&io);
    var child: runtime.runnable_abi.Child = undefined;
    try parent.reserveChild(&.{ .io = &borrowed_io, .out_child = &child });
    defer child.deinit();
    try std.testing.expect(memory.stats().used_bytes > 0);
    try checked(parent.park(parent.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    try child.activate();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expect(!execution.entry.?.completion_active);
    try checked(child.borrow.park(child.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
    {
        var peer: scheduler.Execution = .{};
        defer peer.finish();
        try peer.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .query);
        var health: scheduler.Execution = .{};
        defer health.finish();
        try health.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .control);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().runnable_slots);
    }
    child.deinit();
    var marker: u8 = 0;
    const Check = struct {
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return .ok;
        }
    };
    try parent.enterChecked(.write, &.{ .context = &marker, .check = Check.check });
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expect(!execution.entry.?.completion_active);
    try std.testing.expectError(error.ExecutionWaitUnsupported, checked(team.borrow.park(team.borrow.context, @backingInt(runtime.runnable_abi.Wait.resource))));
}
