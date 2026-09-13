// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Deterministic implementation of the narrow runtime capabilities.
//!
//! Submitting work never runs it inline. It creates an atomic scheduler
//! transition. Timers become fire transitions only at their deadline; the next
//! deadline is reached through a separate virtual-time transition. All queue
//! identity comes from stable logical IDs plus deterministic creation order.

const std = @import("std");
const event = @import("event.zig");
const ids = @import("id.zig");
const runtime_mod = @import("runtime.zig");
const transition = @import("transition.zig");

const QueuedTask = struct {
    task: runtime_mod.Task,
    sequence: u64,

    fn transitionId(self: QueuedTask) ids.StableId {
        return ids.derive("runtime.task", self.task.id, self.sequence);
    }
};

const QueuedTimer = struct {
    timer: runtime_mod.Timer,
    sequence: u64,

    fn transitionId(self: QueuedTimer) ids.StableId {
        return ids.derive("runtime.timer", self.timer.id, self.sequence);
    }
};

pub const SimRuntime = struct {
    allocator: std.mem.Allocator,
    now_ns: u64 = 0,
    next_sequence: u64 = 1,
    tasks: std.ArrayListUnmanaged(QueuedTask) = .empty,
    timers: std.ArrayListUnmanaged(QueuedTimer) = .empty,

    pub fn init(allocator: std.mem.Allocator, initial_time_ns: u64) SimRuntime {
        return .{ .allocator = allocator, .now_ns = initial_time_ns };
    }

    /// Destroys every task still owned by the backend exactly once.
    pub fn deinit(self: *SimRuntime) void {
        for (self.tasks.items) |queued| queued.task.deinit();
        for (self.timers.items) |queued| queued.timer.task.deinit();
        self.tasks.deinit(self.allocator);
        self.timers.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn runtime(self: *SimRuntime) runtime_mod.Runtime {
        return .{
            .executor = .{ .ptr = self, .vtable = &executor_vtable },
            .clock = .{ .ptr = self, .now_ns_fn = nowNs },
            .timers = .{ .ptr = self, .vtable = &timers_vtable },
        };
    }

    pub fn scheduler(self: *SimRuntime) runtime_mod.SchedulerPort {
        return .{ .ptr = self, .vtable = &scheduler_vtable };
    }

    pub fn pendingTaskCount(self: *const SimRuntime) usize {
        return self.tasks.items.len;
    }

    pub fn pendingTimerCount(self: *const SimRuntime) usize {
        return self.timers.items.len;
    }

    /// Executes a transition owned by this runtime and reports whether the ID
    /// belonged to it. Composite deterministic runtimes use this to merge the
    /// narrow atomic executor with a larger scheduler without treating an ID
    /// owned by another backend as an error.
    pub fn executeReadyIfKnown(
        self: *SimRuntime,
        transition_id: ids.StableId,
        sink: *event.Sink,
        allocator: std.mem.Allocator,
    ) !bool {
        return try self.executeKnown(transition_id, sink, allocator);
    }

    fn allocateSequence(self: *SimRuntime) !u64 {
        if (self.next_sequence == std.math.maxInt(u64)) return error.RuntimeSequenceExhausted;
        const result = self.next_sequence;
        self.next_sequence += 1;
        return result;
    }

    fn containsTaskId(self: *const SimRuntime, task_id: runtime_mod.TaskId) bool {
        for (self.tasks.items) |queued| if (queued.task.id == task_id) return true;
        for (self.timers.items) |queued| if (queued.timer.task.id == task_id) return true;
        return false;
    }

    fn containsTimerId(self: *const SimRuntime, timer_id: runtime_mod.TimerId) bool {
        for (self.timers.items) |queued| if (queued.timer.id == timer_id) return true;
        return false;
    }

    fn submit(ptr: *anyopaque, task: runtime_mod.Task) !void {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        if (self.containsTaskId(task.id)) return error.DuplicateTaskId;
        const sequence = try self.allocateSequence();
        // Ownership transfers only after append succeeds.
        try self.tasks.append(self.allocator, .{ .task = task, .sequence = sequence });
    }

    fn cancelTask(ptr: *anyopaque, task_id: runtime_mod.TaskId) !bool {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        for (self.tasks.items, 0..) |queued, index| {
            if (queued.task.id != task_id) continue;
            const removed = self.tasks.orderedRemove(index);
            removed.task.deinit();
            return true;
        }
        return false;
    }

    fn isIdle(ptr: *anyopaque) bool {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        return self.tasks.items.len == 0 and self.timers.items.len == 0;
    }

    fn nowNs(ptr: *anyopaque) u64 {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        return self.now_ns;
    }

    fn schedule(ptr: *anyopaque, timer: runtime_mod.Timer) !void {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        if (self.containsTimerId(timer.id)) return error.DuplicateTimerId;
        if (self.containsTaskId(timer.task.id)) return error.DuplicateTaskId;
        const sequence = try self.allocateSequence();
        // Ownership transfers only after append succeeds.
        try self.timers.append(self.allocator, .{ .timer = timer, .sequence = sequence });
    }

    fn cancelTimer(ptr: *anyopaque, timer_id: runtime_mod.TimerId) !bool {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        for (self.timers.items, 0..) |queued, index| {
            if (queued.timer.id != timer_id) continue;
            const removed = self.timers.orderedRemove(index);
            removed.timer.task.deinit();
            return true;
        }
        return false;
    }

    fn enumerateReady(ptr: *anyopaque, list: *transition.List, allocator: std.mem.Allocator) !void {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        for (self.tasks.items) |queued| {
            try list.append(allocator, .{
                .id = queued.transitionId(),
                .name = queued.task.name,
                .kind = .scheduler,
                .resource_id = queued.task.id,
            });
        }

        var next_deadline: ?u64 = null;
        for (self.timers.items) |queued| {
            if (queued.timer.deadline_ns <= self.now_ns) {
                try list.append(allocator, .{
                    .id = queued.transitionId(),
                    .name = queued.timer.name,
                    .kind = .scheduler,
                    .resource_id = queued.timer.id,
                    .parameter = @intCast(@min(queued.timer.deadline_ns, @as(u64, std.math.maxInt(i64)))),
                });
            } else if (next_deadline == null or queued.timer.deadline_ns < next_deadline.?) {
                next_deadline = queued.timer.deadline_ns;
            }
        }
        if (next_deadline) |deadline| {
            try list.append(allocator, .{
                .id = ids.derive("runtime.time-advance", deadline, 0),
                .name = "runtime.time_advance",
                .kind = .scheduler,
                .parameter = @intCast(@min(deadline, @as(u64, std.math.maxInt(i64)))),
            });
        }
    }

    fn executeReady(ptr: *anyopaque, transition_id: ids.StableId, sink: *event.Sink, allocator: std.mem.Allocator) !void {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        if (!try self.executeKnown(transition_id, sink, allocator))
            return error.UnknownRuntimeTransition;
    }

    fn executeKnown(self: *SimRuntime, transition_id: ids.StableId, sink: *event.Sink, allocator: std.mem.Allocator) !bool {
        for (self.tasks.items, 0..) |queued, index| {
            if (queued.transitionId() != transition_id) continue;
            const removed = self.tasks.orderedRemove(index);
            defer removed.task.deinit();
            try removed.task.run();
            try sink.emit(allocator, .{
                .id = ids.stable("event", "runtime.task_completed"),
                .name = "runtime.task_completed",
                .kind = .state_change,
                .resource_id = removed.task.id,
            });
            return true;
        }
        for (self.timers.items, 0..) |queued, index| {
            if (queued.transitionId() != transition_id) continue;
            if (queued.timer.deadline_ns > self.now_ns) return error.TimerNotReady;
            const removed = self.timers.orderedRemove(index);
            defer removed.timer.task.deinit();
            try removed.timer.task.run();
            try sink.emit(allocator, .{
                .id = ids.stable("event", "runtime.timer_fired"),
                .name = "runtime.timer_fired",
                .kind = .state_change,
                .resource_id = removed.timer.id,
                .payload_digest = removed.timer.deadline_ns,
            });
            return true;
        }

        const deadline = self.nextDeadline() orelse return false;
        if (transition_id != ids.derive("runtime.time-advance", deadline, 0)) return false;
        if (deadline <= self.now_ns) return error.TimeAdvanceNotReady;
        self.now_ns = deadline;
        try sink.emit(allocator, .{
            .id = ids.stable("event", "runtime.time_advanced"),
            .name = "runtime.time_advanced",
            .kind = .state_change,
            .payload_digest = deadline,
        });
        return true;
    }

    fn nextDeadline(self: *const SimRuntime) ?u64 {
        var result: ?u64 = null;
        for (self.timers.items) |queued| {
            if (queued.timer.deadline_ns <= self.now_ns) continue;
            if (result == null or queued.timer.deadline_ns < result.?) result = queued.timer.deadline_ns;
        }
        return result;
    }

    fn quiescent(ptr: *anyopaque) bool {
        const self: *SimRuntime = @ptrCast(@alignCast(ptr));
        return self.tasks.items.len == 0 and self.timers.items.len == 0;
    }

    const executor_vtable: runtime_mod.Executor.VTable = .{
        .submit = submit,
        .cancel = cancelTask,
        .is_idle = isIdle,
    };
    const timers_vtable: runtime_mod.Timers.VTable = .{
        .schedule = schedule,
        .cancel = cancelTimer,
    };
    const scheduler_vtable: runtime_mod.SchedulerPort.VTable = .{
        .enumerate_ready = enumerateReady,
        .execute_ready = executeReady,
        .quiescent = quiescent,
    };
};

test "sim runtime exposes tasks, time advancement, and timers as atomic transitions" {
    const State = struct {
        const Self = @This();
        runs: std.ArrayListUnmanaged(u8) = .empty,
        deinits: usize = 0,

        const Context = struct { state: *Self, value: u8 };

        fn run(ptr: *anyopaque) !void {
            const context: *Context = @ptrCast(@alignCast(ptr));
            try context.state.runs.append(std.testing.allocator, context.value);
        }
        fn destroy(ptr: *anyopaque) void {
            const context: *Context = @ptrCast(@alignCast(ptr));
            context.state.deinits += 1;
            std.testing.allocator.destroy(context);
        }
        fn task(self: *@This(), id: u64, value: u8) !runtime_mod.Task {
            const context = try std.testing.allocator.create(Context);
            context.* = .{ .state = self, .value = value };
            return .{ .id = id, .name = "test-task", .context = context, .run_fn = run, .deinit_fn = destroy };
        }
    };

    var state = State{};
    defer state.runs.deinit(std.testing.allocator);
    var sim = SimRuntime.init(std.testing.allocator, 10);
    defer sim.deinit();
    const app = sim.runtime();
    const scheduler = sim.scheduler();

    try app.executor.submit(try state.task(101, 1));
    try app.timers.schedule(.{ .id = 201, .name = "test-timer", .deadline_ns = 20, .task = try state.task(102, 2) });
    try std.testing.expectEqual(@as(u64, 10), app.clock.nowNs());

    var enabled = transition.List{};
    defer enabled.deinit(std.testing.allocator);
    try scheduler.enumerateReady(&enabled, std.testing.allocator);
    try enabled.canonicalize();
    try std.testing.expectEqual(@as(usize, 2), enabled.items.items.len);

    const task_transition = ids.derive("runtime.task", 101, 1);
    const advance_transition = ids.derive("runtime.time-advance", 20, 0);
    var sink = event.Sink{};
    defer sink.deinit(std.testing.allocator);
    try scheduler.executeReady(advance_transition, &sink, std.testing.allocator);
    try std.testing.expectEqual(@as(u64, 20), app.clock.nowNs());
    try scheduler.executeReady(task_transition, &sink, std.testing.allocator);

    enabled.items.clearRetainingCapacity();
    try scheduler.enumerateReady(&enabled, std.testing.allocator);
    try enabled.canonicalize();
    try std.testing.expectEqual(@as(usize, 1), enabled.items.items.len);
    try scheduler.executeReady(enabled.items.items[0].id, &sink, std.testing.allocator);
    try std.testing.expectEqualSlices(u8, &.{ 1, 2 }, state.runs.items);
    try std.testing.expectEqual(@as(usize, 2), state.deinits);
    try std.testing.expect(scheduler.quiescent());
}

test "sim runtime cancellation and teardown honor ownership" {
    const State = struct {
        deinits: usize = 0,
        fn run(_: *anyopaque) !void {}
        fn destroy(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.deinits += 1;
        }
        fn task(self: *@This(), id: u64) runtime_mod.Task {
            return .{ .id = id, .name = "owned", .context = self, .run_fn = run, .deinit_fn = destroy };
        }
    };
    var state = State{};
    var sim = SimRuntime.init(std.testing.allocator, 0);
    const app = sim.runtime();
    try app.executor.submit(state.task(1));
    try std.testing.expectError(error.DuplicateTaskId, app.executor.submit(state.task(1)));
    try std.testing.expect(try app.executor.cancel(1));
    try std.testing.expectEqual(@as(usize, 1), state.deinits);
    try app.timers.schedule(.{ .id = 3, .name = "timer", .deadline_ns = 4, .task = state.task(2) });
    try std.testing.expect(try app.timers.cancel(3));
    try std.testing.expectEqual(@as(usize, 2), state.deinits);
    try app.executor.submit(state.task(5));
    sim.deinit();
    try std.testing.expectEqual(@as(usize, 3), state.deinits);
}
