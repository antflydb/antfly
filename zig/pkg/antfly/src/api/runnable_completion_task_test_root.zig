// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const allocation = @import("antfly_request_allocation_budget");
const owners = @import("runnable_owner_execution.zig");
const completion = @import("runnable_completion_task.zig");

const Clock = struct {
    var current: std.atomic.Value(u64) = .init(0);
    fn now() u64 {
        return current.load(.acquire);
    }
};
fn checked(status: runtime.error_abi.Status) !void {
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}

const Counter = struct {
    count: usize = 0,
    fail: bool = false,
    fn call(raw: *anyopaque, _: runtime.runnable_abi.Borrowed, _: std.Io) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.count += 1;
        if (self.fail) return error.CommitDecisionUnknown;
    }
};

test "completion task reserves bounded metadata and actual concurrency before work" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    for (0..8) |scenario| {
        Clock.current.store(0, .release);
        var handle: ?*anyopaque = null;
        try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.write), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .out_handle = &handle }));
        defer port.finish(handle.?);
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .write);
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = if (scenario == 0) 0 else 16384 };
        var counter: Counter = .{ .fail = scenario == 3 };
        const options: runtime.runnable_abi.Completion = .{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 };
        if (scenario == 0) {
            try std.testing.expectError(error.OutOfMemory, completion.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call));
        } else if (scenario == 1) {
            var no_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(0) });
            defer no_threads.deinit();
            const unavailable = no_threads.io();
            const unavailable_borrow = runtime.native_abi.IoBorrow.init(&unavailable);
            try std.testing.expectError(error.ConcurrencyUnavailable, completion.Task.reserve(memory.allocator(), unavailable, borrow, &.{ .io = &unavailable_borrow, .native_deadline = 1, .deadline_ns = 100 }, &counter, Counter.call));
        } else if (scenario == 4) {
            Clock.current.store(11, .release);
            try std.testing.expectError(error.DeadlineExceeded, completion.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call));
        } else if (scenario == 6) {
            const other_io = std.testing.io;
            const other_borrow = runtime.native_abi.IoBorrow.init(&other_io);
            try std.testing.expectError(error.InvalidArgument, completion.Task.reserve(memory.allocator(), io, borrow, &.{ .io = &other_borrow, .native_deadline = 1, .deadline_ns = 100 }, &counter, Counter.call));
        } else if (scenario == 7) {
            var malformed = options;
            malformed.version = 4;
            try std.testing.expectError(error.UnsupportedVersion, completion.Task.reserve(memory.allocator(), io, borrow, &malformed, &counter, Counter.call));
        } else {
            var task = try completion.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call);
            try std.testing.expect(memory.stats().used_bytes > 0);
            try std.testing.expectEqual(@as(usize, 0), counter.count);
            if (scenario == 5) {
                Clock.current.store(11, .release);
                try std.testing.expectError(error.DeadlineExceeded, task.runAndJoin());
            } else if (scenario == 3) {
                try std.testing.expectError(error.CommitDecisionUnknown, task.runAndJoin());
                try std.testing.expectError(error.InvalidRunnableTransition, task.runAndJoin());
                try std.testing.expectEqual(@as(usize, 1), counter.count);
            }
            task.deinit();
        }
        if (scenario != 3) try std.testing.expectEqual(@as(usize, 0), counter.count);
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    }
}

test "completion task foreground cancellation joins independent accepted work through close" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Fixture = struct {
        io: std.Io,
        owner: *owners.State,
        memory: *allocation.Shared,
        ready: std.Io.Event = .unset,
        calls: usize = 0,
        fn foreground(self: *@This()) anyerror!void {
            const io_borrow = runtime.native_abi.IoBorrow.init(&self.io);
            const port = self.owner.port();
            var handle: ?*anyopaque = null;
            try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.write), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .out_handle = &handle }));
            defer port.finish(handle.?);
            const borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .write);
            var task = try completion.Task.reserve(self.memory.allocator(), self.io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, self, finishWrite);
            defer task.deinit();
            try task.runAndJoin();
            // Protection is restored; foreground still observes its request.
            try self.io.checkCancel();
        }
        fn finishWrite(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, io: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.ready.set(io);
            // A real cancellation point while the foreground joins this task.
            // Without protected join, Future.await propagates cancellation here.
            try io.sleep(.fromMilliseconds(200), .awake);
            try io.checkCancel();
            try checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.write)));
            try checked(borrow.checkpoint(borrow.context));
            try std.testing.expectEqual(@as(usize, 1), self.owner.owner.scheduler.stats().operations);
            try std.testing.expect(self.memory.stats().used_bytes > 0);
            self.calls += 1;
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var fixture: Fixture = .{ .io = io, .owner = &owner, .memory = &memory };
    var foreground = try io.concurrent(Fixture.foreground, .{&fixture});
    defer _ = foreground.cancel(io) catch {};
    try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    const retained = owner.owner.scheduler.stats().retained_bytes;
    try std.testing.expect(retained > 0);
    var health: scheduler.Execution = .{};
    defer health.finish();
    try health.start(&owner.owner.scheduler, .{ .io = io }, .control);
    health.finish();
    Clock.current.store(11, .release);
    owner.owner.close();
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectError(error.Canceled, foreground.cancel(io));
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
}

test "completion task uses local SQL write and protected commit bridge without new admission" {
    const writes = @import("runnable_write_team.zig");
    const Fixture = struct {
        owner: *scheduler.Scheduler,
        execution: *scheduler.Execution,
        flag: *std.atomic.Value(bool),
        calls: usize = 0,
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn complete(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, io: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.flag.store(true, .release);
            Clock.current.store(11, .release);
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            var health: scheduler.Execution = .{};
            defer health.finish();
            try health.start(self.owner, .{ .io = io }, .control);
            health.finish();
            self.owner.close();
            try checked(borrow.enter(borrow.context, borrow.parent_class));
            try checked(borrow.checkpoint(borrow.context));
            try io.checkCancel();
            try std.testing.expectEqual(@as(usize, 1), self.owner.stats().operations);
            try std.testing.expectEqual(@as(u64, 256), self.owner.stats().retained_bytes);
            try std.testing.expect(self.execution.entry.?.completion_active);
            self.calls += 1;
        }
    };
    for ([_]bool{ false, true }) |control| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
        defer threaded.deinit();
        const io = threaded.io();
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        Clock.current.store(0, .release);
        var owner = try scheduler.Scheduler.init(.{ .slots = 5, .control_slots = 2, .max_operations = 8 });
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var flag = std.atomic.Value(bool).init(false);
        var execution: scheduler.Execution = .{};
        defer execution.finish();
        try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &flag, .requested = Fixture.canceled }, .retained_bytes = 256 }, if (control) .control else .write);
        var bridge = try writes.Completion.init(&execution, memory.allocator());
        var team = try bridge.team();
        try team.reserve(8);
        defer team.finish();
        var fixture: Fixture = .{ .owner = &owner, .execution = &execution, .flag = &flag };
        var task = try completion.Task.reserve(memory.allocator(), io, team.borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, &fixture, Fixture.complete);
        var task_live = true;
        defer if (task_live) task.deinit();
        flag.store(true, .release);
        try std.testing.expectError(error.Canceled, task.runAndJoin());
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
        flag.store(false, .release);
        try task.runAndJoin();
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expectEqual(@as(usize, if (control) 1 else 3), owner.stats().runnable_slots);
        task.deinit();
        task_live = false;
        // Retire only after the actual task and allocator consumers joined.
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    }
}
