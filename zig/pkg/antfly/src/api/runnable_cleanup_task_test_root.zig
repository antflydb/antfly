// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const allocation = @import("antfly_request_allocation_budget");
const owners = @import("runnable_owner_execution.zig");
const cleanup = @import("runnable_cleanup_task.zig");

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

test "cleanup task reserves bounded metadata and actual concurrency before work" {
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
        try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .out_handle = &handle }));
        defer port.finish(handle.?);
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .write);
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = if (scenario == 0) 0 else 16384 };
        var counter: Counter = .{ .fail = scenario == 3 };
        const options: runtime.runnable_abi.Cleanup = .{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 };
        if (scenario == 0) {
            try std.testing.expectError(error.OutOfMemory, cleanup.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call));
        } else if (scenario == 1) {
            var no_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(0) });
            defer no_threads.deinit();
            const unavailable = no_threads.io();
            const unavailable_borrow = runtime.native_abi.IoBorrow.init(&unavailable);
            try std.testing.expectError(error.ConcurrencyUnavailable, cleanup.Task.reserve(memory.allocator(), unavailable, borrow, &.{ .io = &unavailable_borrow, .native_deadline = 1, .deadline_ns = 100 }, &counter, Counter.call));
        } else if (scenario == 4) {
            Clock.current.store(11, .release);
            try std.testing.expectError(error.DeadlineExceeded, cleanup.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call));
        } else if (scenario == 6) {
            const other_io = std.testing.io;
            const other_borrow = runtime.native_abi.IoBorrow.init(&other_io);
            try std.testing.expectError(error.InvalidArgument, cleanup.Task.reserve(memory.allocator(), io, borrow, &.{ .io = &other_borrow, .native_deadline = 1, .deadline_ns = 100 }, &counter, Counter.call));
        } else if (scenario == 7) {
            var malformed = options;
            malformed.version = 4;
            try std.testing.expectError(error.UnsupportedVersion, cleanup.Task.reserve(memory.allocator(), io, borrow, &malformed, &counter, Counter.call));
        } else {
            var task = try cleanup.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call);
            try std.testing.expect(memory.stats().used_bytes > 0);
            try std.testing.expectEqual(@as(usize, 0), counter.count);
            if (scenario == 5) {
                Clock.current.store(101, .release);
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

test "cleanup task activates after real foreground cancellation and joins through owner close" {
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
            try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .out_handle = &handle }));
            defer port.finish(handle.?);
            const borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .write);
            var task = try cleanup.Task.reserve(self.memory.allocator(), self.io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, self, finishCleanup);
            defer task.deinit();
            self.ready.set(self.io);
            self.io.sleep(.fromSeconds(60), .awake) catch |err| switch (err) {
                error.Canceled => self.io.recancel(),
            };
            try task.runAndJoin();
            // Protection is restored; foreground still observes its request.
            try self.io.checkCancel();
        }
        fn finishCleanup(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, io: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));

            // A real cancellation point while the foreground joins this task.
            // Without protected join, Future.await propagates cancellation here.
            try io.sleep(.fromMilliseconds(1), .awake);
            try io.checkCancel();
            try checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query)));
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

test "cleanup task ABI rejects old and truncated headers and absent capability before allocation" {
    const abi = runtime.runnable_abi;
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const Header = extern struct { version: u32, struct_size: u32 };
    var old: Header align(8) = .{ .version = 5, .struct_size = @sizeOf(abi.Owner) };
    try std.testing.expectError(error.UnsupportedVersion, abi.Owner.read(@ptrCast(&old)));
    old.version = abi.abi_version;
    old.struct_size = @sizeOf(abi.Owner) - 1;
    try std.testing.expectError(error.UnsupportedVersion, abi.Owner.read(@ptrCast(&old)));
    old.struct_size = @sizeOf(abi.Borrowed) - 1;
    try std.testing.expectError(error.UnsupportedVersion, abi.Borrowed.read(@ptrCast(&old)));
    old.version = 5;
    old.struct_size = @sizeOf(abi.Cleanup);
    try std.testing.expectError(error.UnsupportedVersion, @as(*const abi.Cleanup, @ptrCast(&old)).validate());
    old.version = abi.abi_version;
    old.struct_size = @sizeOf(abi.Cleanup) - 1;
    try std.testing.expectError(error.UnsupportedVersion, @as(*const abi.Cleanup, @ptrCast(&old)).validate());
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var handle: ?*anyopaque = null;
    try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &handle }));
    defer port.finish(handle.?);
    var borrow = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 0 };
    var counter: Counter = .{};
    const options = abi.Cleanup{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 };
    for ([_]bool{ false, true }) |reserve_missing| {
        borrow.reserve_cleanup = if (reserve_missing) null else port.reserve_cleanup;
        borrow.activate_cleanup = if (reserve_missing) port.activate_cleanup else null;
        try std.testing.expectError(error.UnsupportedOperation, cleanup.Task.reserve(memory.allocator(), io, borrow, &options, &counter, Counter.call));
    }
    try std.testing.expectEqual(@as(u64, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), counter.count);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
}

test "cleanup task local query bridge joins retirement after original cancellation and close" {
    const local = @import("runnable_local_cleanup.zig");
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var owner = try scheduler.Scheduler.init(.{ .slots = 3, .control_slots = 1 });
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var cancelled = false;
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .cancellation = .{ .ptr = &cancelled, .requested = Cancel.requested }, .retained_bytes = 64 }, .query);
    var bridge = try local.Cleanup.init(&execution);
    const borrow = try bridge.borrowed();
    try std.testing.expectError(error.ExecutionWaitUnsupported, checked(borrow.checkpoint(borrow.context)));
    const Fixture = struct {
        owner: *scheduler.Scheduler,
        memory: *allocation.Shared,
        body: ?*u8 = null,
        calls: usize = 0,
        fn retire(raw: *anyopaque, value: runtime.runnable_abi.Borrowed, worker_io: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(value.park(value.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            try std.testing.expectEqual(@as(usize, 0), self.owner.stats().runnable_slots);
            var health: scheduler.Execution = .{};
            defer health.finish();
            try health.start(self.owner, .{ .io = worker_io }, .control);
            health.finish();
            self.owner.close();
            try std.testing.expectError(error.InvalidRunnableTransition, checked(value.enter(value.context, @backingInt(runtime.runnable_abi.Class.control))));
            try checked(value.enter(value.context, @backingInt(runtime.runnable_abi.Class.query)));
            try checked(value.checkpoint(value.context));
            try std.testing.expectEqual(@as(usize, 1), self.owner.stats().operations);
            try std.testing.expectEqual(@as(u64, 64), self.owner.stats().retained_bytes);
            self.memory.allocator().destroy(self.body.?);
            self.body = null;
            self.calls += 1;
        }
    };
    var fixture: Fixture = .{ .owner = &owner, .memory = &memory };
    var task = try cleanup.Task.reserve(memory.allocator(), io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, &fixture, Fixture.retire);
    var task_live = true;
    defer if (task_live) task.deinit();
    defer if (fixture.body) |body| memory.allocator().destroy(body);
    // Publish physical ownership only after actual cleanup task admission.
    fixture.body = try memory.allocator().create(u8);
    fixture.body.?.* = 7;
    cancelled = true;
    Clock.current.store(11, .release);
    try task.runAndJoin();
    try std.testing.expectEqual(@as(usize, 1), fixture.calls);
    try std.testing.expect(fixture.body == null and memory.stats().used_bytes > 0);
    task.deinit();
    task_live = false;
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    execution.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "cleanup task local query bridge clears denied receivers before publication" {
    const local = @import("runnable_local_cleanup.zig");
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var owner = try scheduler.Scheduler.init(.{ .slots = 3 });
    var execution: scheduler.Execution = .{};
    defer execution.finish();
    try execution.start(&owner, .{ .io = io }, .query);
    var bridge = try local.Cleanup.init(&execution);
    var borrow = try bridge.borrowed();
    try std.testing.expectError(error.InvalidArgument, borrow.reserveCleanup(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }));
    try std.testing.expect(bridge.io == null and bridge.clock_io == null);
    execution.finish();
    Clock.current.store(0, .release);
    try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10 }, .query);
    Clock.current.store(11, .release);
    try std.testing.expectError(error.DeadlineExceeded, borrow.reserveCleanup(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }));
    try std.testing.expect(bridge.io == null and bridge.clock_io == null);
    Clock.current.store(0, .release);
    try borrow.reserveCleanup(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 });
    try std.testing.expectError(error.InvalidRunnableTransition, borrow.reserveCleanup(&.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 200 }));
    owner.close();
    try borrow.activateCleanup();
    try checked(borrow.checkpoint(borrow.context));
}

test "cleanup task local failed activation and partial retirement retain physical ownership through join" {
    const local = @import("runnable_local_cleanup.zig");
    const Fixture = struct {
        memory: *allocation.Shared,
        first: ?*u8 = null,
        second: ?*u8 = null,
        calls: usize = 0,
        fn retire(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, _: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.memory.allocator().destroy(self.first.?);
            self.first = null;
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            // The first body is actually freed, but the second body is still
            // owned when fair CPU reentry reaches the independent deadline.
            Clock.current.store(100, .release);
            try checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query)));
            self.memory.allocator().destroy(self.second.?);
            self.second = null;
        }
    };
    for ([_]bool{ false, true }) |partial_retirement| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
        defer threaded.deinit();
        const io = threaded.io();
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        Clock.current.store(0, .release);
        var owner = try scheduler.Scheduler.init(.{ .slots = 3, .control_slots = 1 });
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var execution: scheduler.Execution = .{};
        defer execution.finish();
        try execution.start(&owner, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .retained_bytes = 64 }, .query);
        var bridge = try local.Cleanup.init(&execution);
        const borrow = try bridge.borrowed();
        var fixture: Fixture = .{ .memory = &memory };
        defer if (fixture.first) |body| memory.allocator().destroy(body);
        defer if (fixture.second) |body| memory.allocator().destroy(body);
        var task = try cleanup.Task.reserve(memory.allocator(), io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, &fixture, Fixture.retire);
        var task_live = true;
        defer if (task_live) task.deinit();
        // Both physical bodies are published only after concurrent task and
        // cleanup authority admission. The future must join on every error.
        fixture.first = try memory.allocator().create(u8);
        fixture.first.?.* = 7;
        fixture.second = try memory.allocator().create(u8);
        fixture.second.?.* = 9;
        Clock.current.store(if (partial_retirement) 11 else 100, .release);
        owner.close();
        try std.testing.expectError(error.DeadlineExceeded, task.runAndJoin());
        try std.testing.expect(task.joined);
        try std.testing.expectEqual(@as(usize, @intFromBool(partial_retirement)), fixture.calls);
        try std.testing.expectEqual(!partial_retirement, fixture.first != null);
        try std.testing.expect(fixture.second != null);
        try std.testing.expectEqual(@as(u8, 9), fixture.second.?.*);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 64), owner.stats().retained_bytes);
        task.deinit();
        task_live = false;
        // Joining/deleting failed task metadata is not physical retirement.
        // The caller retains the exact unfinished bodies and invocation charge.
        try std.testing.expectEqual(@as(usize, if (partial_retirement) 1 else 2), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        if (fixture.first) |body| {
            memory.allocator().destroy(body);
            fixture.first = null;
        }
        memory.allocator().destroy(fixture.second.?);
        fixture.second = null;
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        execution.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "cleanup task activation budget crosses owner and local bridges after idle and retains partial failure" {
    const local = @import("runnable_local_cleanup.zig");
    const Fixture = struct {
        owner: *scheduler.Scheduler,
        memory: *allocation.Shared,
        first: ?[]u8 = null,
        second: ?[]u8 = null,
        fail: bool,
        calls: usize = 0,
        fn retire(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, _: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(@backingInt(runtime.runnable_abi.Class.query), borrow.parent_class);
            Clock.current.store(1009, .release);
            try checked(borrow.checkpoint(borrow.context));
            self.memory.allocator().free(self.first.?);
            self.first = null;
            if (self.fail) {
                Clock.current.store(1010, .release);
                // Exact activation deadline: leave the second physical buffer
                // and the caller's retained root live on failure.
                try checked(borrow.checkpoint(borrow.context));
                return error.CleanupExceededItsBudget;
            }
            self.memory.allocator().free(self.second.?);
            self.second = null;
        }
    };
    for ([_]bool{ false, true }) |use_local| for ([_]bool{ false, true }) |partial_failure| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
        defer threaded.deinit();
        const io = threaded.io();
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        Clock.current.store(0, .release);
        var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer owner.owner.deinit();
        const port = owner.port();
        var execution: scheduler.Execution = .{};
        defer execution.finish();
        var handle: ?*anyopaque = null;
        defer if (handle) |value| port.finish(value);
        var bridge: local.Cleanup = undefined;
        const borrow = if (use_local) local_borrow: {
            try execution.start(&owner.owner.scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .retained_bytes = 64 }, .query);
            bridge = try local.Cleanup.init(&execution);
            break :local_borrow try bridge.borrowed();
        } else owner_borrow: {
            try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .retained_bytes = 64, .out_handle = &handle }));
            break :owner_borrow try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .query);
        };
        const held_bytes = owner.owner.scheduler.stats().retained_bytes;
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var fixture: Fixture = .{ .owner = &owner.owner.scheduler, .memory = &memory, .fail = partial_failure };
        defer if (fixture.first) |body| memory.allocator().free(body);
        defer if (fixture.second) |body| memory.allocator().free(body);
        var task = try cleanup.Task.reserve(memory.allocator(), io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 10, .activation_budget = 1 }, &fixture, Fixture.retire);
        var task_live = true;
        defer if (task_live) task.deinit();
        fixture.first = try memory.allocator().alloc(u8, 32);
        fixture.second = try memory.allocator().alloc(u8, 32);
        if (use_local) try execution.park(.output_blocked) else try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.output)));
        Clock.current.store(1000, .release);
        owner.owner.scheduler.close();
        if (partial_failure) try std.testing.expectError(error.DeadlineExceeded, task.runAndJoin()) else try task.runAndJoin();
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expect(fixture.first == null);
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(held_bytes, owner.owner.scheduler.stats().retained_bytes);
        try std.testing.expectError(error.InvalidRunnableTransition, task.runAndJoin());
        task.deinit();
        task_live = false;
        if (partial_failure) {
            try std.testing.expect(fixture.second != null);
            try std.testing.expectEqual(@as(usize, 32), memory.stats().used_bytes);
            // Future completion is not physical retirement. The caller still
            // owns the remaining body, allocator chain and charged execution.
            memory.allocator().free(fixture.second.?);
            fixture.second = null;
        }
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        execution.finish();
        if (handle) |value| {
            port.finish(value);
            handle = null;
        }
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    };
}

test "cleanup task bounded observer transfers live ownership after foreground return" {
    const Fixture = struct {
        memory: *allocation.Shared,
        io: std.Io,
        body: ?[]u8 = null,
        release: std.Io.Event = .unset,
        entered: std.Io.Event = .unset,
        calls: std.atomic.Value(usize) = .init(0),
        fail: bool,
        fn retire(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, io: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.entered.set(io);
            try self.release.wait(io);
            try checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query)));
            if (self.fail) return error.CleanupFailed;
            self.memory.allocator().free(self.body.?);
            self.body = null;
        }
        fn foreground(self: *@This(), borrow: runtime.runnable_abi.Borrowed, io_borrow: *const runtime.native_abi.IoBorrow) !cleanup.Task {
            var task = try cleanup.Task.reserve(self.memory.allocator(), self.io, borrow, &.{ .io = io_borrow, .native_deadline = 1, .deadline_ns = 100, .activation_budget = 1 }, self, retire);
            errdefer task.deinit();
            errdefer self.release.set(self.io);
            self.body = try self.memory.allocator().alloc(u8, 128);
            @memset(self.body.?, 42);
            const deadline: std.Io.Clock.Timestamp = .{ .clock = .awake, .raw = std.Io.Clock.awake.now(self.io).addDuration(.fromMilliseconds(20)) };
            try std.testing.expectError(error.InvalidArgument, task.runAndWaitUntil(.{ .clock = .real, .raw = deadline.raw }));
            try std.testing.expect(!task.started);
            try std.testing.expectError(error.Timeout, task.runAndWaitUntil(deadline));
            try std.testing.expect(!task.joined);
            return task; // The supervisor now owns the actual live handle.
        }
    };
    for ([_]bool{ false, true }) |fail| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
        defer threaded.deinit();
        const io = threaded.io();
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        Clock.current.store(0, .release);
        var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer owner.owner.deinit();
        const port = owner.port();
        var handle: ?*anyopaque = null;
        try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .retained_bytes = 128, .out_handle = &handle }));
        defer if (handle) |value| port.finish(value);
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 1, .query);
        const held_bytes = owner.owner.scheduler.stats().retained_bytes;
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        const fixture = try memory.allocator().create(Fixture);
        fixture.* = .{ .memory = &memory, .io = io, .fail = fail };
        defer memory.allocator().destroy(fixture);
        defer if (fixture.body) |body| memory.allocator().free(body);
        var supervisor_task = try fixture.foreground(borrow, &io_borrow);
        var task_live = true;
        defer if (task_live) supervisor_task.deinit();
        defer fixture.release.set(io);
        try fixture.entered.wait(io);
        try std.testing.expect(supervisor_task.outcome() == .pending);
        try std.testing.expect(!supervisor_task.joined);
        try std.testing.expectEqual(@as(u8, 42), fixture.body.?[0]);
        try std.testing.expectEqual(held_bytes, owner.owner.scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
        try std.testing.expectError(error.InvalidRunnableTransition, supervisor_task.start());
        var control: ?*anyopaque = null;
        try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control }));
        port.finish(control.?);
        fixture.release.set(io);
        const until: std.Io.Clock.Timestamp = .{ .clock = .awake, .raw = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5)) };
        if (fail) try std.testing.expectError(error.CleanupFailed, supervisor_task.waitUntil(until)) else try supervisor_task.waitUntil(until);
        try std.testing.expect(!supervisor_task.joined);
        if (fail) {
            try std.testing.expectEqual(@as(anyerror, error.CleanupFailed), supervisor_task.outcome().failed);
            try std.testing.expectError(error.CleanupFailed, supervisor_task.join());
            try std.testing.expect(fixture.body != null);
        } else {
            try std.testing.expect(supervisor_task.outcome() == .succeeded);
            try supervisor_task.join();
            try std.testing.expect(fixture.body == null);
        }
        try std.testing.expect(supervisor_task.joined);
        try std.testing.expectEqual(@as(usize, 1), fixture.calls.load(.acquire));
        try std.testing.expectEqual(held_bytes, owner.owner.scheduler.stats().retained_bytes);
        supervisor_task.deinit();
        task_live = false;
        // Failed callback completion and metadata retirement do not release
        // the remaining physical body or the supervisor's execution charge.
        if (fixture.body) |body| {
            try std.testing.expectEqual(@as(u8, 42), body[0]);
            memory.allocator().free(body);
            fixture.body = null;
        }
        try std.testing.expectEqual(@sizeOf(Fixture), memory.stats().used_bytes);
        port.finish(handle.?);
        handle = null;
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    }
}

const cleanup_owner = @import("runnable_cleanup_owner.zig");
const SupervisedFixture = struct {
    memory: *allocation.Shared,
    owner: *owners.State,
    handle: *anyopaque,
    refs: std.atomic.Value(usize) = .init(1),
    released: *std.atomic.Value(usize),
    ready: std.Io.Event = .unset,
    go: std.Io.Event = .unset,
    bodies: [2]?[]u8 = .{ null, null },
    fail: bool = false,
    retain_failure: bool = false,
    retirement_blocked: bool = false,
    cleanup_calls: usize = 0,
    retirement_calls: usize = 0,
    fn retain(raw: *anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.retain_failure) return error.ParentPinUnavailable;
        _ = self.refs.fetchAdd(1, .monotonic);
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        std.debug.assert(self.bodies[0] == null and self.bodies[1] == null);
        // The final callback parent is the last allocator charge. All task
        // and supervisor metadata must have physically retired before it.
        std.debug.assert(self.memory.stats().used_bytes == @sizeOf(@This()));
        const allocator = self.memory.allocator();
        const port = self.owner.port();
        const handle = self.handle;
        const released = self.released;
        allocator.destroy(self);
        port.finish(handle);
        _ = released.fetchAdd(1, .release);
    }
    fn run(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, io: std.Io) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.cleanup_calls += 1;
        self.memory.allocator().free(self.bodies[0].?);
        self.bodies[0] = null;
        try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
        self.ready.set(io);
        try self.go.wait(io);
        try checked(borrow.enter(borrow.context, @backingInt(runtime.runnable_abi.Class.query)));
        try checked(borrow.checkpoint(borrow.context));
        if (self.fail) return error.CleanupFailed;
        self.memory.allocator().free(self.bodies[1].?);
        self.bodies[1] = null;
    }
    fn retire(raw: *anyopaque) anyerror!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.retirement_calls += 1;
        if (self.retirement_blocked) return error.RetirementBlocked;
        for (&self.bodies) |*body| if (body.*) |bytes| {
            self.memory.allocator().free(bytes);
            body.* = null;
        };
    }
    const callbacks: cleanup_owner.Registry.Callbacks = .{ .retain = retain, .release = release, .cleanup = run, .retire = retire };
};

fn supervisedFixture(memory: *allocation.Shared, owner: *owners.State, io_borrow: *const runtime.native_abi.IoBorrow, released: *std.atomic.Value(usize)) !*SupervisedFixture {
    const port = owner.port();
    var handle: ?*anyopaque = null;
    try checked(port.start(port.context, &.{ .io = io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .out_handle = &handle }));
    errdefer port.finish(handle.?);
    const fixture = try memory.allocator().create(SupervisedFixture);
    fixture.* = .{ .memory = memory, .owner = owner, .handle = handle.?, .released = released };
    return fixture;
}

fn awaitCleanupProgress(registry: *cleanup_owner.Registry, io: std.Io) !cleanup_owner.Registry.Progress {
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (true) {
        const result = registry.pollOne();
        switch (result) {
            .pending => {},
            else => return result,
        }
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
}

test "cleanup task supervisor preadmits ownership and retains partial failures until explicit retirement" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    for ([_]bool{ false, true }) |fail| {
        Clock.current.store(0, .release);
        var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer owner.owner.deinit();
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var released: std.atomic.Value(usize) = .init(0);
        const fixture = try supervisedFixture(&memory, &owner, &io_borrow, &released);
        fixture.fail = fail;
        fixture.retirement_blocked = fail;
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(owner.port(), fixture.handle, 1, .query);
        var registry: cleanup_owner.Registry = .{ .allocator = memory.allocator(), .limit = 1 };
        var reservation = try registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks);
        try std.testing.expectEqual(@as(usize, 2), fixture.refs.load(.acquire));
        const reserved_stats = registry.snapshot();
        try std.testing.expectEqual(@as(usize, 1), reserved_stats.capacity);
        try std.testing.expectEqual(@as(usize, 1), reserved_stats.retained);
        try std.testing.expectEqual(@as(usize, 1), reserved_stats.foreground);
        try std.testing.expectEqual(@as(usize, 0), reserved_stats.in_flight);
        try std.testing.expectError(error.CleanupCapacityUnavailable, registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks));
        try std.testing.expectError(error.InvalidRunnableTransition, reservation.start());
        for (&fixture.bodies) |*body| body.* = try memory.allocator().alloc(u8, 32);
        try reservation.publish();
        try std.testing.expectError(error.InvalidRunnableTransition, reservation.abort());
        try reservation.start();
        try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try std.testing.expectError(error.Timeout, reservation.waitUntil(.{ .clock = .awake, .raw = std.Io.Clock.awake.now(io).addDuration(.fromMilliseconds(20)) }));
        try reservation.transfer();
        // The foreground's final reference disappears while physical cleanup
        // is blocked. Only the prepublished registry pin owns this heap context.
        SupervisedFixture.release(fixture);
        try std.testing.expectEqual(@as(usize, 1), fixture.refs.load(.acquire));
        try std.testing.expectError(error.InvalidRunnableTransition, reservation.transfer());
        const supervised_stats = registry.snapshot();
        try std.testing.expectEqual(@as(usize, 1), supervised_stats.retained);
        try std.testing.expectEqual(@as(usize, 1), supervised_stats.supervised);
        try std.testing.expectEqual(@as(usize, 0), supervised_stats.foreground);
        try std.testing.expect(registry.pollOne() == .pending);
        registry.close();
        try std.testing.expectError(error.CleanupPending, registry.deinit());
        try std.testing.expectEqual(@as(usize, 1), registry.retained());
        try std.testing.expectEqual(@as(usize, 0), released.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
        var control: scheduler.Execution = .{};
        try control.start(&owner.owner.scheduler, .{ .io = io }, .control);
        control.finish();
        fixture.go.set(io);
        const progress = try awaitCleanupProgress(&registry, io);
        if (fail) {
            try std.testing.expect(progress == .failed);
            try std.testing.expectEqual(error.CleanupFailed, progress.failed.cleanup.?);
            const failed_stats = registry.snapshot();
            try std.testing.expect(failed_stats.closing);
            try std.testing.expectEqual(@as(usize, 1), failed_stats.retained);
            try std.testing.expectEqual(@as(usize, 1), failed_stats.failed);
            try std.testing.expectEqual(@as(usize, 0), failed_stats.joining);
            try std.testing.expectEqual(@as(usize, 0), fixture.retirement_calls);
            try std.testing.expect(fixture.bodies[0] == null and fixture.bodies[1] != null);
            try std.testing.expectEqual(@as(usize, 1), registry.retained());
            try std.testing.expect(memory.stats().used_bytes > @sizeOf(SupervisedFixture));
            try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
            try std.testing.expect(registry.pollOne() == .pending);
            const blocked = registry.retireFailedOne();
            try std.testing.expect(blocked == .failed);
            try std.testing.expectEqual(error.CleanupFailed, blocked.failed.cleanup.?);
            try std.testing.expectEqual(error.RetirementBlocked, blocked.failed.retirement.?);
            try std.testing.expectEqual(@as(usize, 1), fixture.cleanup_calls);
            try std.testing.expectError(error.CleanupPending, registry.deinit());
            fixture.retirement_blocked = false;
            try std.testing.expect(registry.retireFailedOne() == .retired);
        } else try std.testing.expect(progress == .retired);
        try std.testing.expectEqual(@as(usize, 1), released.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), registry.retained());
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try registry.deinit();
    }
}

test "cleanup task supervisor rolls back rejected reservations before publication" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    for (0..5) |scenario| {
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var released: std.atomic.Value(usize) = .init(0);
        const fixture = try supervisedFixture(&memory, &owner, &io_borrow, &released);
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(owner.port(), fixture.handle, 1, .query);
        var registry: cleanup_owner.Registry = .{ .allocator = memory.allocator(), .limit = 1 };
        if (scenario == 0) {
            fixture.retain_failure = true;
            try std.testing.expectError(error.ParentPinUnavailable, registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks));
        } else if (scenario == 1) {
            memory.limit = @sizeOf(SupervisedFixture);
            try std.testing.expectError(error.OutOfMemory, registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks));
        } else if (scenario == 2) {
            registry.close();
            try std.testing.expectError(error.CleanupCapacityUnavailable, registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks));
        } else if (scenario == 3) {
            var unavailable = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(0) });
            defer unavailable.deinit();
            const unavailable_io = unavailable.io();
            const unavailable_borrow = runtime.native_abi.IoBorrow.init(&unavailable_io);
            try std.testing.expectError(error.ConcurrencyUnavailable, registry.reserve(unavailable_io, borrow, &.{ .io = &unavailable_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks));
        } else {
            var reservation = try registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks);
            try reservation.abort();
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.refs.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), registry.retained());
        try std.testing.expectEqual(@as(usize, @sizeOf(SupervisedFixture)), memory.stats().used_bytes);
        SupervisedFixture.release(fixture);
        try std.testing.expectEqual(@as(usize, 1), released.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        registry.close();
        try registry.deinit();
    }
}

const read_lifetime = @import("runnable_read_lifetime.zig");
const ReadLifetimeFixture = struct {
    lifetime: read_lifetime.Lifetime,
    memory: *allocation.Shared,
    port: runtime.runnable_abi.Owner,
    handle: *anyopaque,
    stream_body: []u8,
    page_body: []u8,
    signals: *Signals,
    const Signals = struct {
        io: std.Io,
        retiring: std.Io.Event = .unset,
        permit: std.Io.Event = .unset,
        retired: std.atomic.Value(usize) = .init(0),
        destroyed: std.atomic.Value(usize) = .init(0),
        foreground_closed: std.atomic.Value(bool) = .init(false),
        page_released: std.atomic.Value(bool) = .init(false),
    };
    fn retire(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const protection = self.signals.io.swapCancelProtection(.blocked);
        defer _ = self.signals.io.swapCancelProtection(protection);
        self.signals.retiring.set(self.signals.io);
        self.signals.permit.wait(self.signals.io) catch unreachable;
        // The page buffer has physically freed; the root still owns stream
        // resources until this teardown completes outside the metadata lock.
        self.memory.allocator().free(self.stream_body);
        self.port.finish(self.handle);
        _ = self.signals.retired.fetchAdd(1, .release);
    }
    fn destroy(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const signals = self.signals;
        const alloc = self.memory.allocator();
        std.debug.assert(signals.retired.load(.acquire) == 1);
        std.debug.assert(self.memory.stats().used_bytes == @sizeOf(@This()));
        alloc.destroy(self);
        _ = signals.destroyed.fetchAdd(1, .release);
    }
    fn releasePage(self: *@This()) void {
        const signals = self.signals;
        self.memory.allocator().free(self.page_body);
        self.lifetime.releaseResource();
        signals.page_released.store(true, .release);
    }
    fn close(self: *@This()) void {
        const signals = self.signals;
        self.lifetime.close();
        signals.foreground_closed.store(true, .release);
    }
    fn create(memory: *allocation.Shared, owner: *owners.State, io: std.Io, signals: *Signals) !*@This() {
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        const port = owner.port();
        var handle: ?*anyopaque = null;
        try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 100, .retained_bytes = 17, .out_handle = &handle }));
        errdefer port.finish(handle.?);
        const alloc = memory.allocator();
        const self = try alloc.create(@This());
        errdefer alloc.destroy(self);
        self.* = .{ .lifetime = .{ .context = self, .retire = retire, .destroy = destroy }, .memory = memory, .port = port, .handle = handle.?, .stream_body = try alloc.alloc(u8, 32), .page_body = undefined, .signals = signals };
        errdefer alloc.free(self.stream_body);
        self.page_body = try alloc.alloc(u8, 32);
        errdefer alloc.free(self.page_body);
        try self.lifetime.retainResource();
        try checked(port.park(handle.?, @backingInt(runtime.runnable_abi.Wait.output)));
        return self;
    }
};

test "cleanup task read lifetime keeps root and parent through physical page and stream retirement" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 8192 };
    var signals: ReadLifetimeFixture.Signals = .{ .io = io };
    const fixture = try ReadLifetimeFixture.create(&memory, &owner, io, &signals);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    try std.testing.expect(retained >= 17);
    fixture.close();
    try std.testing.expectEqual(@as(usize, 1), fixture.lifetime.references.load(.acquire));
    try std.testing.expectError(error.InvalidRunnableTransition, fixture.lifetime.retainResource());
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, @sizeOf(ReadLifetimeFixture) + 64), memory.stats().used_bytes);
    var page_release = try io.concurrent(ReadLifetimeFixture.releasePage, .{fixture});
    defer {
        signals.permit.set(io);
        page_release.cancel(io);
    }
    try signals.retiring.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(usize, @sizeOf(ReadLifetimeFixture) + 32), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), signals.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), signals.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    var control: scheduler.Execution = .{};
    try control.start(&owner.owner.scheduler, .{ .io = io }, .control);
    control.finish();
    signals.permit.set(io);
    page_release.await(io);
    try std.testing.expectEqual(@as(usize, 1), signals.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), signals.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
}

fn admitReadLifetimeRace(io: std.Io, comptime function: fn (*ReadLifetimeFixture) void, fixture: *ReadLifetimeFixture) !std.Io.Future(void) {
    // Future completion fences user captures; executor bookkeeping may still
    // be retiring its worker slot. Keep one original admission deadline while
    // the fixture retains the root/page/context through transient rejection.
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (true) {
        return io.concurrent(function, .{fixture}) catch |err| {
            if (err != error.ConcurrencyUnavailable) return err;
            if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return err;
            try io.sleep(.fromMilliseconds(1), .awake);
            continue;
        };
    }
}

test "cleanup task read lifetime close races final page release without double retirement" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 8192 };
    for (0..64) |_| {
        var signals: ReadLifetimeFixture.Signals = .{ .io = io };
        signals.permit.set(io);
        const fixture = try ReadLifetimeFixture.create(&memory, &owner, io, &signals);
        defer {
            if (!signals.foreground_closed.load(.acquire)) fixture.close();
            if (!signals.page_released.load(.acquire)) fixture.releasePage();
        }
        var close = try admitReadLifetimeRace(io, ReadLifetimeFixture.close, fixture);
        defer close.cancel(io);
        var page_release = try admitReadLifetimeRace(io, ReadLifetimeFixture.releasePage, fixture);
        defer page_release.cancel(io);
        close.await(io);
        page_release.await(io);
        try std.testing.expectEqual(@as(usize, 1), signals.retired.load(.acquire));
        try std.testing.expectEqual(@as(usize, 1), signals.destroyed.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    }
}

test "cleanup task read lifetime keeps native root until a foreground pull future physically joins" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Pull = struct {
        fixture: *ReadLifetimeFixture,
        io: std.Io,
        page_freed: std.Io.Event = .unset,
        finish: std.Io.Event = .unset,
        fn run(self: *@This()) std.Io.Cancelable!void {
            self.fixture.releasePage();
            self.page_freed.set(self.io);
            try self.finish.wait(self.io);
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 8192 };
    var signals: ReadLifetimeFixture.Signals = .{ .io = io };
    signals.permit.set(io);
    const fixture = try ReadLifetimeFixture.create(&memory, &owner, io, &signals);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    try fixture.lifetime.retainResource();
    var query_owned = true;
    defer if (query_owned) fixture.lifetime.releaseResource();
    fixture.close();
    var pull: Pull = .{ .fixture = fixture, .io = io };
    var future = try io.concurrent(Pull.run, .{&pull});
    defer {
        pull.finish.set(io);
        _ = future.cancel(io) catch {};
    }
    try pull.page_freed.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try std.testing.expectEqual(@as(usize, @sizeOf(ReadLifetimeFixture) + 32), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), signals.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), signals.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    var control: scheduler.Execution = .{};
    try control.start(&owner.owner.scheduler, .{ .io = io }, .control);
    control.finish();
    pull.finish.set(io);
    try future.await(io);
    // The foreground's resource borrow is released only after physical join,
    // not when the last page releases or a callback completion event appears.
    fixture.lifetime.releaseResource();
    query_owned = false;
    try std.testing.expectEqual(@as(usize, 1), signals.retired.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), signals.destroyed.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
}

test "cleanup task runtime independent reaper joins success and explicitly retained failure through shutdown" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var cleanup_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer cleanup_threads.deinit();
    var reaper_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer reaper_threads.deinit();
    const io = cleanup_threads.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    for ([_]bool{ false, true }) |fail| {
        Clock.current.store(0, .release);
        var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer owner.owner.deinit();
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var metadata: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var released: std.atomic.Value(usize) = .init(0);
        const active = try cleanup_owner.Runtime.create(metadata.allocator(), reaper_threads.io(), 2);
        const fixture = try supervisedFixture(&memory, &owner, &io_borrow, &released);
        fixture.fail = fail;
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(owner.port(), fixture.handle, 1, .query);
        var reservation = try active.registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks);
        for (&fixture.bodies) |*body| body.* = try memory.allocator().alloc(u8, 32);
        try reservation.publish();
        try reservation.start();
        try reservation.transfer();
        SupervisedFixture.release(fixture);
        try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        active.close();
        try std.testing.expectError(error.CleanupCapacityUnavailable, active.registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks));
        try std.testing.expectEqual(@as(usize, 1), active.registry.retained());
        try std.testing.expectEqual(@as(usize, 0), released.load(.acquire));
        var control: scheduler.Execution = .{};
        try control.start(&owner.owner.scheduler, .{ .io = io }, .control);
        control.finish();
        fixture.go.set(io);
        const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
        if (fail) {
            while (active.registry.failures() != 1) {
                if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
                try io.sleep(.fromMilliseconds(1), .awake);
            }
            try std.testing.expectEqual(@as(usize, 1), active.registry.retained());
            try std.testing.expectEqual(@as(usize, 0), released.load(.acquire));
            try std.testing.expectEqual(@as(usize, 1), fixture.cleanup_calls);
            try std.testing.expectEqual(@as(usize, 0), fixture.retirement_calls);
            try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
            try std.testing.expect(fixture.bodies[1] != null);
            try active.requestFailureRetirement();
        }
        while (active.registry.retained() != 0) {
            if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(usize, 1), released.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try std.testing.expect(metadata.stats().used_bytes > 0);
        active.join();
        const joined_stats = active.registry.snapshot();
        try std.testing.expect(joined_stats.closing);
        try std.testing.expectEqual(@as(usize, 0), joined_stats.retained);
        // A physically joined owner remains stable for snapshot readers until
        // its publisher unpublishes it and fences those readers.
        try std.testing.expect(metadata.stats().used_bytes > 0);
        active.destroyJoined();
        try std.testing.expectEqual(@as(usize, 0), metadata.stats().used_bytes);
    }
}

test "cleanup task runtime startup allocation and worker failures retire unpublished state" {
    var unavailable = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(0) });
    defer unavailable.deinit();
    var reaper_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer reaper_threads.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var failing = std.testing.FailingAllocator.init(memory.allocator(), .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, cleanup_owner.Runtime.create(failing.allocator(), reaper_threads.io(), 1));
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectError(error.ConcurrencyUnavailable, cleanup_owner.Runtime.create(memory.allocator(), unavailable.io(), 1));
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    const active = try cleanup_owner.Runtime.create(memory.allocator(), reaper_threads.io(), 1);
    try std.testing.expect(memory.stats().used_bytes > 0);
    active.deinit();
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
}

test "cleanup task snapshot retains in-flight reservation through blocked parent pin rollback" {
    const Probe = struct {
        registry: *cleanup_owner.Registry,
        io: std.Io,
        borrow: runtime.runnable_abi.Borrowed,
        io_borrow: *const runtime.native_abi.IoBorrow,
        ready: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        fn retain(raw: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.ready.set(self.io);
            self.go.waitUncancelable(self.io);
            return error.ParentPinUnavailable;
        }
        fn release(_: *anyopaque) void {
            unreachable;
        }
        fn cleanupUnused(_: *anyopaque, _: runtime.runnable_abi.Borrowed, _: std.Io) anyerror!void {
            unreachable;
        }
        fn retireUnused(_: *anyopaque) anyerror!void {
            unreachable;
        }
        fn run(self: *@This()) anyerror!void {
            try std.testing.expectError(error.ParentPinUnavailable, self.registry.reserve(self.io, self.borrow, &.{ .io = self.io_borrow, .native_deadline = 1, .deadline_ns = 100 }, self, .{ .retain = retain, .release = release, .cleanup = cleanupUnused, .retire = retireUnused }));
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer threaded.deinit();
    const io = threaded.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var metadata: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var released: std.atomic.Value(usize) = .init(0);
    const fixture = try supervisedFixture(&memory, &owner, &io_borrow, &released);
    defer SupervisedFixture.release(fixture);
    const borrow = try runtime.runnable_abi.Borrowed.fromOwner(owner.port(), fixture.handle, 1, .query);
    var registry: cleanup_owner.Registry = .{ .allocator = metadata.allocator(), .limit = 1 };
    var probe: Probe = .{ .registry = &registry, .io = io, .borrow = borrow, .io_borrow = &io_borrow };
    var future = try io.concurrent(Probe.run, .{&probe});
    defer {
        probe.go.set(io);
        _ = future.cancel(io) catch {};
    }
    try probe.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    const held = registry.snapshot();
    try std.testing.expectEqual(@as(usize, 1), held.retained);
    try std.testing.expectEqual(@as(usize, 1), held.in_flight);
    try std.testing.expectEqual(@as(usize, 0), held.foreground);
    try std.testing.expectEqual(@as(usize, 0), metadata.stats().used_bytes);
    registry.close();
    try std.testing.expect(registry.snapshot().closing);
    try std.testing.expectError(error.CleanupPending, registry.deinit());
    probe.go.set(io);
    try future.await(io);
    const retired = registry.snapshot();
    try std.testing.expectEqual(@as(usize, 0), retired.retained);
    try std.testing.expectEqual(@as(usize, 0), retired.in_flight);
    try registry.deinit();
}

test "cleanup task retirement requests claim distinct failed slots without coalescing or future authority" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(3) });
    defer threaded.deinit();
    const io = threaded.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memories: [2]allocation.Shared = .{ .{ .child = std.heap.smp_allocator, .limit = 8192 }, .{ .child = std.heap.smp_allocator, .limit = 8192 } };
    var metadata: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 32768 };
    var released: [2]std.atomic.Value(usize) = .{ .init(0), .init(0) };
    var fixtures: [2]*SupervisedFixture = undefined;
    var registry: cleanup_owner.Registry = .{ .allocator = metadata.allocator(), .limit = 2 };
    try std.testing.expectError(error.CleanupNotFailed, registry.requestFailedRetirement());
    for (&fixtures, &memories, &released) |*fixture, *memory, *release_count| {
        fixture.* = try supervisedFixture(memory, &owner, &io_borrow, release_count);
        fixture.*.fail = true;
        fixture.*.retirement_blocked = true;
        const borrow = try runtime.runnable_abi.Borrowed.fromOwner(owner.port(), fixture.*.handle, 1, .query);
        var reservation = try registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture.*, SupervisedFixture.callbacks);
        for (&fixture.*.bodies) |*body| body.* = try memory.allocator().alloc(u8, 32);
        try reservation.publish();
        try reservation.start();
        try fixture.*.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try reservation.transfer();
        SupervisedFixture.release(fixture.*);
    }
    // A request cannot be consumed while cleanup is pending, nor authorize
    // retirement of an unrelated future failure.
    try std.testing.expectError(error.CleanupNotFailed, registry.requestFailedRetirement());
    for (fixtures) |fixture| fixture.go.set(io);
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (registry.failures() != 2) {
        _ = registry.pollOne();
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    registry.close();
    const before = metadata.stats().used_bytes;
    try registry.requestFailedRetirement();
    try registry.requestFailedRetirement();
    try std.testing.expectError(error.CleanupNotFailed, registry.requestFailedRetirement());
    const requested = registry.snapshot();
    try std.testing.expect(requested.closing);
    try std.testing.expectEqual(@as(usize, 2), requested.retained);
    try std.testing.expectEqual(@as(usize, 2), requested.retirement_requested);
    try std.testing.expectEqual(@as(usize, 0), requested.failed);
    try std.testing.expectEqual(@as(usize, 2), registry.failures());
    try std.testing.expectEqual(before, metadata.stats().used_bytes);
    for (0..2) |_| {
        const failed = registry.retireRequestedOne();
        try std.testing.expect(failed == .failed);
        try std.testing.expectEqual(error.CleanupFailed, failed.failed.cleanup.?);
        try std.testing.expectEqual(error.RetirementBlocked, failed.failed.retirement.?);
    }
    try std.testing.expect(registry.retireRequestedOne() == .pending);
    try std.testing.expect(registry.pollOne() == .pending);
    try std.testing.expectEqual(@as(usize, 2), registry.snapshot().failed);
    try std.testing.expectEqual(@as(usize, 0), registry.snapshot().retirement_requested);
    try std.testing.expectEqual(before, metadata.stats().used_bytes);
    for (fixtures, &released) |fixture, *release_count| {
        try std.testing.expectEqual(@as(usize, 1), fixture.cleanup_calls);
        try std.testing.expectEqual(@as(usize, 1), fixture.retirement_calls);
        try std.testing.expect(fixture.bodies[1] != null);
        try std.testing.expectEqual(@as(usize, 0), release_count.load(.acquire));
        fixture.retirement_blocked = false;
    }
    var control: scheduler.Execution = .{};
    try control.start(&owner.owner.scheduler, .{ .io = io }, .control);
    control.finish();
    try registry.requestFailedRetirement();
    try registry.requestFailedRetirement();
    for (0..2) |_| try std.testing.expect(registry.retireRequestedOne() == .retired);
    for (&memories, &released) |*memory, *release_count| {
        try std.testing.expectEqual(@as(usize, 1), release_count.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    }
    try std.testing.expectEqual(@as(usize, 0), metadata.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), registry.retained());
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try registry.deinit();
}

test "cleanup task runtime snapshots and control progress while physical shutdown join is blocked" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    const Joiner = struct {
        active: *cleanup_owner.Runtime,
        done: std.atomic.Value(bool) = .init(false),
        fn run(self: *@This()) void {
            self.active.join();
            self.done.store(true, .release);
        }
    };
    var cleanup_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer cleanup_threads.deinit();
    var reaper_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer reaper_threads.deinit();
    var shutdown_threads = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer shutdown_threads.deinit();
    const io = cleanup_threads.io();
    const shutdown_io = shutdown_threads.io();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    Clock.current.store(0, .release);
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
    defer owner.owner.deinit();
    var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var metadata: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
    var released: std.atomic.Value(usize) = .init(0);
    const active = try cleanup_owner.Runtime.create(metadata.allocator(), reaper_threads.io(), 1);
    var active_live = true;
    var joined = false;
    defer if (active_live) {
        if (!joined) active.join();
        active.destroyJoined();
    };
    const fixture = try supervisedFixture(&memory, &owner, &io_borrow, &released);
    const borrow = try runtime.runnable_abi.Borrowed.fromOwner(owner.port(), fixture.handle, 1, .query);
    var reservation = try active.registry.reserve(io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .deadline_ns = 100 }, fixture, SupervisedFixture.callbacks);
    for (&fixture.bodies) |*body| body.* = try memory.allocator().alloc(u8, 32);
    try reservation.publish();
    try reservation.start();
    try reservation.transfer();
    SupervisedFixture.release(fixture);
    // The callback is physically running, parked on a real resource event,
    // and still owns its remaining body and the native execution root.
    try fixture.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    var joiner: Joiner = .{ .active = active };
    var shutdown = try shutdown_io.concurrent(Joiner.run, .{&joiner});
    var shutdown_joined = false;
    var body_released = false;
    defer {
        if (!body_released) fixture.go.set(io);
        if (!shutdown_joined) shutdown.await(shutdown_io);
        joined = true;
    }
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (!active.registry.snapshot().closing) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) return error.Timeout;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const held_memory = memory.stats().used_bytes;
    const held_metadata = metadata.stats().used_bytes;
    for (0..16) |_| {
        const observed = active.registry.snapshot();
        try std.testing.expect(observed.closing);
        try std.testing.expectEqual(@as(usize, 1), observed.retained);
        try std.testing.expectEqual(@as(usize, 1), observed.supervised);
        try std.testing.expect(!joiner.done.load(.acquire));
        try std.testing.expectEqual(@as(usize, 0), released.load(.acquire));
        try std.testing.expectEqual(held_memory, memory.stats().used_bytes);
        try std.testing.expectEqual(held_metadata, metadata.stats().used_bytes);
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
        var control: scheduler.Execution = .{};
        try control.start(&owner.owner.scheduler, .{ .io = io, .allow_wait = false }, .control);
        control.finish();
    }
    fixture.go.set(io);
    body_released = true;
    // Callback completion alone is insufficient: physically join shutdown
    // before observing parent retirement and destroying the stable owner.
    shutdown.await(shutdown_io);
    shutdown_joined = true;
    joined = true;
    try std.testing.expect(joiner.done.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), released.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), active.registry.snapshot().retained);
    try std.testing.expect(metadata.stats().used_bytes > 0);
    active.destroyJoined();
    active_live = false;
    try std.testing.expectEqual(@as(usize, 0), metadata.stats().used_bytes);
}

test "cleanup task propagates actual admitted width from local and opaque foreground teams" {
    const local = @import("runnable_local_cleanup.zig");
    const Fixture = struct {
        owner: *scheduler.Scheduler,
        expected_slots: usize,
        expected_bytes: u64,
        calls: usize = 0,
        fn retire(raw: *anyopaque, borrow: runtime.runnable_abi.Borrowed, _: std.Io) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            try std.testing.expectEqual(self.expected_slots, borrow.cpu_slots);
            try std.testing.expectEqual(runtime.runnable_abi.Class.inference, try borrow.currentClass());
            try checked(borrow.checkpoint(borrow.context));
            const stats = self.owner.stats();
            try std.testing.expectEqual(self.expected_slots, stats.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), stats.operations);
            try std.testing.expectEqual(self.expected_bytes, stats.retained_bytes);
            if (self.expected_slots == 1) {
                try std.testing.expectEqual(@as(usize, 1), stats.cleanup_runnable_slots);
                if (borrow.resize_team != null) {
                    var team_borrow = borrow;
                    try std.testing.expectError(error.InvalidRunnableDemand, team_borrow.resizeTeam(2));
                }
            }
        }
    };
    for ([_]bool{ false, true }) |use_local| for ([_]bool{ false, true }) |isolated| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
        defer threaded.deinit();
        const io = threaded.io();
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        Clock.current.store(0, .release);
        var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try scheduler.NodeOwner.init(.{ .slots = 6, .control_slots = 1, .cleanup_slots = if (isolated) 1 else 0 }), .native_now_ns = Clock.now };
        defer owner.owner.deinit();
        const port = owner.port();
        var execution: scheduler.Execution = .{};
        defer execution.finish();
        var handle: ?*anyopaque = null;
        defer if (handle) |value| port.finish(value);
        var bridge: local.Cleanup = undefined;
        const borrow = if (use_local) local_borrow: {
            try execution.startWithSlots(&owner.owner.scheduler, .{ .io = io, .native_now_ns = Clock.now, .deadline_ns = 10, .retained_bytes = 57 }, .inference, 3);
            bridge = try local.Cleanup.init(&execution);
            break :local_borrow try bridge.borrowed();
        } else owner_borrow: {
            try checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 3, .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .retained_bytes = 57, .out_handle = &handle }));
            break :owner_borrow try runtime.runnable_abi.Borrowed.fromOwner(port, handle.?, 3, .inference);
        };
        try std.testing.expectEqual(@as(usize, 3), borrow.cpu_slots);
        var memory: allocation.Shared = .{ .child = std.heap.smp_allocator, .limit = 16384 };
        var fixture: Fixture = .{ .owner = &owner.owner.scheduler, .expected_slots = if (isolated) 1 else 3, .expected_bytes = owner.owner.scheduler.stats().retained_bytes };
        var task = try cleanup.Task.reserve(memory.allocator(), io, borrow, &.{ .io = &io_borrow, .native_deadline = 1, .activation_budget = 1, .deadline_ns = 100 }, &fixture, Fixture.retire);
        var task_live = true;
        defer if (task_live) task.deinit();
        try std.testing.expectEqual(@as(usize, 0), fixture.calls);
        try std.testing.expectEqual(@as(usize, 3), owner.owner.scheduler.stats().runnable_slots);
        // Expired foreground and closed policy cannot refresh the cleanup fence
        // or substitute foreground width for the admitted cleanup grant.
        Clock.current.store(1000, .release);
        owner.owner.scheduler.close();
        try task.runAndJoin();
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expectEqual(@as(usize, 3), borrow.cpu_slots);
        task.deinit();
        task_live = false;
        try std.testing.expectEqual(@as(usize, 0), memory.stats().used_bytes);
        execution.finish();
        if (handle) |value| {
            port.finish(value);
            handle = null;
        }
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    };
}
