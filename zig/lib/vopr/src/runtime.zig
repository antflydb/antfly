// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Narrow runtime capabilities shared by production and deterministic worlds.
//!
//! This remains the small stable scheduler kernel beneath `VoprIo`.
//! `std.Io.VTable` includes the entire filesystem, network, process,
//! synchronization, time, entropy, and concurrency surface. Focused components
//! should depend on these narrow capabilities; production components that need
//! the complete contract run through the layered VoprIo adapter.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const transition = @import("transition.zig");

pub const TaskId = ids.StableId;
pub const TimerId = ids.StableId;

/// A type-erased atomic unit of work. Ownership transfers to an Executor only
/// after submit succeeds. The accepting backend must call deinit exactly once,
/// after execution, successful cancellation, or backend teardown.
pub const Task = struct {
    id: TaskId,
    name: []const u8,
    context: *anyopaque,
    run_fn: *const fn (*anyopaque) anyerror!void,
    deinit_fn: *const fn (*anyopaque) void,

    pub fn run(self: Task) !void {
        try self.run_fn(self.context);
    }

    pub fn deinit(self: Task) void {
        self.deinit_fn(self.context);
    }

    pub fn validate(self: Task) !void {
        if (self.id == 0) return error.InvalidTaskId;
        if (self.name.len == 0) return error.EmptyTaskName;
    }
};

/// Application-facing execution capability. A threaded implementation may
/// dispatch immediately; a simulated implementation only makes the task an
/// enabled scheduler transition.
pub const Executor = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        submit: *const fn (*anyopaque, Task) anyerror!void,
        cancel: *const fn (*anyopaque, TaskId) anyerror!bool,
        is_idle: *const fn (*anyopaque) bool,
    };

    pub fn submit(self: Executor, task: Task) !void {
        try task.validate();
        try self.vtable.submit(self.ptr, task);
    }

    pub fn cancel(self: Executor, task_id: TaskId) !bool {
        if (task_id == 0) return error.InvalidTaskId;
        return try self.vtable.cancel(self.ptr, task_id);
    }

    pub fn isIdle(self: Executor) bool {
        return self.vtable.is_idle(self.ptr);
    }
};

pub const Clock = struct {
    ptr: *anyopaque,
    now_ns_fn: *const fn (*anyopaque) u64,

    pub fn nowNs(self: Clock) u64 {
        return self.now_ns_fn(self.ptr);
    }
};

pub const Timer = struct {
    id: TimerId,
    name: []const u8,
    deadline_ns: u64,
    task: Task,

    pub fn validate(self: Timer) !void {
        if (self.id == 0) return error.InvalidTimerId;
        if (self.name.len == 0) return error.EmptyTimerName;
        try self.task.validate();
    }
};

pub const Timers = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        schedule: *const fn (*anyopaque, Timer) anyerror!void,
        cancel: *const fn (*anyopaque, TimerId) anyerror!bool,
    };

    pub fn schedule(self: Timers, timer: Timer) !void {
        try timer.validate();
        try self.vtable.schedule(self.ptr, timer);
    }

    pub fn cancel(self: Timers, timer_id: TimerId) !bool {
        if (timer_id == 0) return error.InvalidTimerId;
        return try self.vtable.cancel(self.ptr, timer_id);
    }
};

/// Capabilities supplied to reusable application code. Entropy remains a
/// ChoiceSource concern so no unrecorded random byte stream can enter replay.
pub const Runtime = struct {
    executor: Executor,
    clock: Clock,
    timers: Timers,
};

/// Scheduler-only view exposed by a future deterministic Runtime backend.
/// Production code never receives this capability.
pub const SchedulerPort = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        enumerate_ready: *const fn (*anyopaque, *transition.List, std.mem.Allocator) anyerror!void,
        execute_ready: *const fn (*anyopaque, ids.StableId, *event.Sink, std.mem.Allocator) anyerror!void,
        quiescent: *const fn (*anyopaque) bool,
    };

    pub fn enumerateReady(self: SchedulerPort, list: *transition.List, allocator: std.mem.Allocator) !void {
        try self.vtable.enumerate_ready(self.ptr, list, allocator);
    }

    pub fn executeReady(self: SchedulerPort, transition_id: ids.StableId, sink: *event.Sink, allocator: std.mem.Allocator) !void {
        if (transition_id == 0) return error.InvalidTransitionId;
        try self.vtable.execute_ready(self.ptr, transition_id, sink, allocator);
    }

    pub fn quiescent(self: SchedulerPort) bool {
        return self.vtable.quiescent(self.ptr);
    }
};

test "executor validates before ownership transfer" {
    const State = struct {
        submits: usize = 0,
        runs: usize = 0,
        deinits: usize = 0,

        fn executor(self: *@This()) Executor {
            return .{ .ptr = self, .vtable = &.{ .submit = submit, .cancel = cancel, .is_idle = isIdle } };
        }
        fn submit(ptr: *anyopaque, task: Task) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.submits += 1;
            defer task.deinit();
            try task.run();
        }
        fn cancel(_: *anyopaque, _: TaskId) !bool {
            return false;
        }
        fn isIdle(_: *anyopaque) bool {
            return true;
        }
        fn run(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.runs += 1;
        }
        fn deinitTask(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
    };
    var state = State{};
    try state.executor().submit(.{
        .id = ids.stable("task", "runtime-test"),
        .name = "runtime-test",
        .context = &state,
        .run_fn = State.run,
        .deinit_fn = State.deinitTask,
    });
    try std.testing.expectEqual(@as(usize, 1), state.submits);
    try std.testing.expectEqual(@as(usize, 1), state.runs);
    try std.testing.expectEqual(@as(usize, 1), state.deinits);

    try std.testing.expectError(error.InvalidTaskId, state.executor().submit(.{
        .id = 0,
        .name = "invalid",
        .context = &state,
        .run_fn = State.run,
        .deinit_fn = State.deinitTask,
    }));
    try std.testing.expectEqual(@as(usize, 1), state.submits);
}

test "timer and scheduler capabilities preserve their boundary" {
    const State = struct {
        now_ns: u64 = 41,
        scheduled: usize = 0,
        executed: usize = 0,
        deinits: usize = 0,

        fn clock(self: *@This()) Clock {
            return .{ .ptr = self, .now_ns_fn = now };
        }
        fn timers(self: *@This()) Timers {
            return .{ .ptr = self, .vtable = &.{ .schedule = schedule, .cancel = cancelTimer } };
        }
        fn scheduler(self: *@This()) SchedulerPort {
            return .{ .ptr = self, .vtable = &.{ .enumerate_ready = enumerateReady, .execute_ready = executeReady, .quiescent = quiescent } };
        }
        fn now(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.now_ns;
        }
        fn schedule(ptr: *anyopaque, timer: Timer) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.scheduled += 1;
            timer.task.deinit();
        }
        fn cancelTimer(_: *anyopaque, _: TimerId) !bool {
            return false;
        }
        fn enumerateReady(_: *anyopaque, list: *transition.List, allocator: std.mem.Allocator) !void {
            try list.append(allocator, .{ .id = ids.stable("transition", "runtime.ready"), .name = "runtime.ready", .kind = .scheduler });
        }
        fn executeReady(ptr: *anyopaque, transition_id: ids.StableId, sink: *event.Sink, allocator: std.mem.Allocator) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (transition_id != ids.stable("transition", "runtime.ready")) return error.UnknownRuntimeTransition;
            self.executed += 1;
            try sink.emitNamed(allocator, .state_change, "runtime.executed", @intCast(self.executed));
        }
        fn quiescent(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.executed > 0;
        }
        fn run(_: *anyopaque) !void {}
        fn deinitTask(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
    };

    var state = State{};
    try std.testing.expectEqual(@as(u64, 41), state.clock().nowNs());
    try state.timers().schedule(.{
        .id = ids.stable("timer", "runtime.timer"),
        .name = "runtime.timer",
        .deadline_ns = 42,
        .task = .{
            .id = ids.stable("task", "runtime.timer-task"),
            .name = "runtime.timer-task",
            .context = &state,
            .run_fn = State.run,
            .deinit_fn = State.deinitTask,
        },
    });
    try std.testing.expectEqual(@as(usize, 1), state.scheduled);
    try std.testing.expectEqual(@as(usize, 1), state.deinits);

    var list = transition.List{};
    defer list.deinit(std.testing.allocator);
    try state.scheduler().enumerateReady(&list, std.testing.allocator);
    try list.canonicalize();
    var sink = event.Sink{};
    defer sink.deinit(std.testing.allocator);
    try state.scheduler().executeReady(list.items.items[0].id, &sink, std.testing.allocator);
    try std.testing.expect(state.scheduler().quiescent());
    try std.testing.expectEqual(@as(usize, 1), sink.events.items.len);
}
