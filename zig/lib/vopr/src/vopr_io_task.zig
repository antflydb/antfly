// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2).

//! Stackful deterministic task kernel used by VoprIo.
//!
//! Fibers never schedule one another directly. Parking switches back to the
//! VOPR runner; resumption, futex wake selection, and virtual-time advancement
//! are explicit stable transitions selected by the ordinary replay kernel.

const builtin = @import("builtin");
const std = @import("std");
const ids = @import("id.zig");
const transition = @import("transition.zig");

comptime {
    if (!std.Io.fiber.supported) @compileError("VoprIo requires std.Io.fiber support");
}

const stack_alignment = builtin.target.stackAlignment();
const storage_alignment = 16;

pub const Config = struct {
    stack_size: usize = 1024 * 1024,
    max_tasks: usize = 4096,
};

pub const Status = enum {
    runnable,
    running,
    waiting_future,
    waiting_group,
    waiting_sleep,
    waiting_futex,
    waiting_external,
    finished,
};

pub const TaskSnapshot = struct {
    id: ids.StableId,
    status: Status,
    sleep_deadline_ns: ?i96,
    waiting_on_futex: bool,
    external_resource_id: ?ids.StableId,
};

pub const Sleep = struct {
    clock: std.Io.Clock,
    deadline_ns: i96,
};

const GroupState = struct {
    public: *std.Io.Group,
    tasks: std.ArrayListUnmanaged(*Task) = .empty,
    awaiter: ?*Task = null,
    cancel_requested: bool = false,
};

const FutexWake = struct {
    ptr: *const u32,
    remaining: u32,
    sequence: u64,
    eligible_through: u64,
};

const FutexIdentity = struct {
    ptr: *const u32,
    id: ids.StableId,
};

const ExternalWake = struct {
    resource_id: ids.StableId,
    remaining: u32,
    sequence: u64,
    eligible_through: u64,
};

const ExternalResourceSequence = struct {
    resource_id: ids.StableId,
    next_wake_sequence: u64 = 1,
    next_waiter_sequence: u64 = 1,
};

const TaskIdentitySequence = struct {
    parent: ids.StableId,
    callsite: u64,
    next_sequence: u64 = 1,
};

const Task = struct {
    kernel: *Kernel,
    id: ids.StableId,
    /// Immutable identity of the logical operation that created resources.
    /// Scheduler identity may later bind to an inbound socket/listener wait,
    /// but an outbound connection must not make its caller the child of the
    /// previous retry's socket and thereby form a replay-fragile chain.
    resource_owner_id: ids.StableId,
    identity_parent: ids.StableId,
    identity_sequence: u64,
    creation_sequence: u64,
    next_futex_sequence: u64 = 1,
    external_identity_bound: bool = false,
    resume_generation: u64 = 1,
    context: std.Io.fiber.Context,
    stack: []align(stack_alignment) u8,
    storage: []align(storage_alignment) u8,
    context_len: usize,
    result_offset: usize,
    result_len: usize,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    status: Status = .runnable,
    awaiter: ?*Task = null,
    waiting_on_future: ?*Task = null,
    group: ?*GroupState = null,
    waiting_on_group: ?*GroupState = null,
    sleep: ?Sleep = null,
    futex_ptr: ?*const u32 = null,
    futex_wait_sequence: u64 = 0,
    external_id: ?ids.StableId = null,
    external_wait_sequence: u64 = 0,
    futex_uncancelable: bool = false,
    cancel_requested: bool = false,
    cancel_acknowledged: bool = false,
    cancel_protection: std.Io.CancelProtection = .unblocked,
    reap_after_switch: bool = false,

    fn transitionId(self: *const Task) ids.StableId {
        return ids.derive("vopr-io.task-resume", self.id, self.resume_generation);
    }

    fn contextBytes(self: *Task) []u8 {
        return self.storage[0..self.context_len];
    }

    fn resultBytes(self: *Task) []u8 {
        return self.storage[self.result_offset..][0..self.result_len];
    }

    fn makeRunnable(self: *Task) void {
        if (self.status == .finished or self.status == .runnable or self.status == .running) return;
        self.status = .runnable;
        self.sleep = null;
        self.futex_ptr = null;
        self.external_id = null;
        self.futex_uncancelable = false;
        self.resume_generation +|= 1;
    }

    fn requestCancel(self: *Task) void {
        self.cancel_requested = true;
        if (self.status == .waiting_group) {
            if (self.waiting_on_group) |group| self.kernel.cancelGroupTasks(group);
        }
        // `Future.await` is not a cancellation point. Keep the parent parked
        // until the target publishes its result; `finishCurrent` will resume
        // it. Waking the parent early would let `await` observe an unfinished
        // target and violate both result ownership and stack lifetime.
        if (self.status != .waiting_future and self.status != .finished and self.status != .running and self.status != .runnable) {
            self.makeRunnable();
        }
    }

    fn checkCancel(self: *Task) std.Io.Cancelable!void {
        if (!self.cancel_requested or self.cancel_protection == .blocked or self.cancel_acknowledged) return;
        self.cancel_requested = false;
        self.cancel_acknowledged = true;
        return error.Canceled;
    }
};

const Entry = struct {
    task: *Task,

    fn entry() callconv(.naked) void {
        switch (builtin.cpu.arch) {
            .aarch64 => asm volatile (
                \\ mov x0, sp
                \\ b %[call]
                :
                : [call] "X" (&call),
            ),
            .x86_64 => asm volatile (
                \\ leaq 8(%%rsp), %%rdi
                \\ jmp %[call:P]
                :
                : [call] "X" (&call),
            ),
            else => |arch| @compileError("unsupported VoprIo fiber architecture: " ++ @tagName(arch)),
        }
    }

    fn call(entry_context: *Entry, _: *const std.Io.fiber.Switch) callconv(.withStackAlign(.c, stack_alignment)) noreturn {
        const task = entry_context.task;
        task.kernel.setCurrent(task);
        task.start(task.contextBytes().ptr, task.resultBytes().ptr);
        task.kernel.finishCurrent(task);
    }
};

fn taskIdentityAnchor(_: *const anyopaque, _: *anyopaque) void {}

fn taskCallsite(start: *const fn (*const anyopaque, *anyopaque) void) u64 {
    return @intFromPtr(start) -% @intFromPtr(&taskIdentityAnchor);
}

fn taskIdentityScope(parent: ids.StableId, start: *const fn (*const anyopaque, *anyopaque) void) ids.StableId {
    return ids.derive("vopr-io.task-callsite", parent, taskCallsite(start));
}

pub const Execution = struct {
    task_id: ids.StableId,
    completed: bool,
    kind: enum { task, futex_wake, external_wake },
};

pub const Kernel = struct {
    allocator: std.mem.Allocator,
    config: Config,
    /// Host-side diagnostic ordinal only. Child replay identities are scoped
    /// to the logical parent task so unrelated owners cannot exchange them when
    /// their internal spawn calls happen in a different wall-memory order.
    next_sequence: u64 = 1,
    next_root_futex_sequence: u64 = 1,
    next_wait_sequence: u64 = 1,
    main_context: std.Io.fiber.Context = undefined,
    current: ?*Task = null,
    execution_thread_id: std.atomic.Value(u64) = .init(0),
    tasks: std.ArrayListUnmanaged(*Task) = .empty,
    groups: std.ArrayListUnmanaged(*GroupState) = .empty,
    futex_wakes: std.ArrayListUnmanaged(FutexWake) = .empty,
    futex_identities: std.ArrayListUnmanaged(FutexIdentity) = .empty,
    external_wakes: std.ArrayListUnmanaged(ExternalWake) = .empty,
    external_resource_sequences: std.ArrayListUnmanaged(ExternalResourceSequence) = .empty,
    task_identity_sequences: std.ArrayListUnmanaged(TaskIdentitySequence) = .empty,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Kernel {
        if (config.stack_size < 64 * 1024) return error.VoprIoStackTooSmall;
        if (config.max_tasks == 0) return error.InvalidVoprIoTaskLimit;
        return .{ .allocator = allocator, .config = config };
    }

    pub fn deinit(self: *Kernel) void {
        std.debug.assert(self.currentTask() == null);
        for (self.tasks.items) |task| self.destroyTaskMemory(task);
        for (self.groups.items) |group| {
            group.tasks.deinit(self.allocator);
            self.allocator.destroy(group);
        }
        self.tasks.deinit(self.allocator);
        self.groups.deinit(self.allocator);
        self.futex_wakes.deinit(self.allocator);
        self.futex_identities.deinit(self.allocator);
        self.external_wakes.deinit(self.allocator);
        self.external_resource_sequences.deinit(self.allocator);
        self.task_identity_sequences.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn activeTaskCount(self: *const Kernel) usize {
        var count: usize = 0;
        for (self.tasks.items) |task| count += @intFromBool(task.status != .finished);
        return count;
    }

    pub fn totalTaskCount(self: *const Kernel) usize {
        return self.tasks.items.len;
    }

    pub fn futureSnapshot(self: *const Kernel, any_future: *std.Io.AnyFuture) ?TaskSnapshot {
        const target: *Task = @ptrCast(@alignCast(any_future));
        for (self.tasks.items) |task| {
            if (task != target) continue;
            return .{
                .id = task.id,
                .status = task.status,
                .sleep_deadline_ns = if (task.sleep) |sleep| sleep.deadline_ns else null,
                .waiting_on_futex = task.futex_ptr != null,
                .external_resource_id = task.external_id,
            };
        }
        return null;
    }

    pub fn isQuiescent(self: *const Kernel) bool {
        for (self.tasks.items) |task| if (task.status != .finished) return false;
        for (self.futex_wakes.items) |wake| if (self.hasFutexWaiter(wake.ptr)) return false;
        for (self.external_wakes.items) |wake| if (self.hasExternalWaiter(wake.resource_id)) return false;
        return true;
    }

    pub fn async(
        self: *Kernel,
        eager_result: []u8,
        result_alignment: std.mem.Alignment,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    ) ?*std.Io.AnyFuture {
        const task = self.createTask(
            eager_result.len,
            result_alignment,
            context,
            context_alignment,
            start,
            null,
        ) catch {
            start(context.ptr, eager_result.ptr);
            return null;
        };
        return @ptrCast(task);
    }

    pub fn concurrent(
        self: *Kernel,
        result_len: usize,
        result_alignment: std.mem.Alignment,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    ) std.Io.ConcurrentError!*std.Io.AnyFuture {
        const task = self.createTask(
            result_len,
            result_alignment,
            context,
            context_alignment,
            start,
            null,
        ) catch return error.ConcurrencyUnavailable;
        return @ptrCast(task);
    }

    pub fn await(self: *Kernel, any_future: *std.Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) !void {
        const target: *Task = @ptrCast(@alignCast(any_future));
        if (target.kernel != self or target.group != null or target.result_len != result.len or
            !result_alignment.compare(.lte, .fromByteUnits(storage_alignment)))
            return error.InvalidVoprIoFuture;
        // Scenario teardown runs on the harness fiber. A task which has
        // already unwound no longer needs an awaiter: copy its result and reap
        // it directly. Unfinished tasks still require an in-runtime awaiter so
        // ordinary scheduling and cancellation remain trace-visible.
        if (target.status == .finished and self.currentTask() == null) {
            @memcpy(result, target.resultBytes());
            self.removeAndDestroyTask(target);
            return;
        }
        const awaiter = self.currentTask() orelse return error.VoprIoAwaitOutsideTask;
        if (target == awaiter or target.awaiter != null) return error.InvalidVoprIoFuture;
        if (target.status != .finished) {
            target.awaiter = awaiter;
            awaiter.waiting_on_future = target;
            awaiter.status = .waiting_future;
            self.yieldCurrent(awaiter);
        }
        std.debug.assert(target.status == .finished);
        @memcpy(result, target.resultBytes());
        awaiter.waiting_on_future = null;
        self.removeAndDestroyTask(target);
    }

    pub fn cancel(self: *Kernel, any_future: *std.Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) !void {
        const target: *Task = @ptrCast(@alignCast(any_future));
        if (target.kernel != self) return error.InvalidVoprIoFuture;
        target.requestCancel();
        try self.await(any_future, result, result_alignment);
    }

    /// Request cooperative cancellation for every unfinished fiber. The
    /// caller must subsequently drive scheduler transitions until
    /// `isQuiescent` becomes true, allowing task defers to release production
    /// ownership instead of discarding fiber stacks at runtime teardown.
    pub fn requestCancelAll(self: *Kernel) void {
        for (self.tasks.items) |task| {
            if (task.status != .finished) task.requestCancel();
        }
    }

    pub fn groupAsync(
        self: *Kernel,
        public_group: *std.Io.Group,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque) void,
    ) void {
        self.groupConcurrentInternal(public_group, context, context_alignment, start) catch {
            start(context.ptr);
        };
    }

    pub fn groupConcurrent(
        self: *Kernel,
        public_group: *std.Io.Group,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque) void,
    ) std.Io.ConcurrentError!void {
        self.groupConcurrentInternal(public_group, context, context_alignment, start) catch
            return error.ConcurrencyUnavailable;
    }

    fn groupConcurrentInternal(
        self: *Kernel,
        public_group: *std.Io.Group,
        context: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque) void,
    ) !void {
        _ = context_alignment;
        const Wrapper = struct {
            original: *const fn (*const anyopaque) void,
            context_len: usize,

            fn run(raw: *const anyopaque, _: *anyopaque) void {
                const header: *const @This() = @ptrCast(@alignCast(raw));
                const bytes: [*]const u8 = @ptrCast(header);
                header.original(bytes[@sizeOf(@This())..][0..header.context_len].ptr);
            }
        };

        const owned_context = try self.allocator.alignedAlloc(
            u8,
            .of(Wrapper),
            @sizeOf(Wrapper) + context.len,
        );
        defer self.allocator.free(owned_context);
        const wrapper: *Wrapper = @ptrCast(owned_context.ptr);
        wrapper.* = .{ .original = start, .context_len = context.len };
        @memcpy(owned_context[@sizeOf(Wrapper)..], context);

        const group = try self.getOrCreateGroup(public_group);
        errdefer self.discardGroupIfEmpty(group);
        const task = try self.createTask(0, .@"1", owned_context, .of(Wrapper), Wrapper.run, group);
        errdefer self.removeAndDestroyTask(task);
        try group.tasks.append(self.allocator, task);
    }

    pub fn groupAwait(self: *Kernel, public_group: *std.Io.Group, token: *anyopaque) !void {
        const group: *GroupState = @ptrCast(@alignCast(token));
        if (group.public != public_group) return error.InvalidVoprIoGroup;
        while (group.tasks.items.len != 0) {
            const awaiter = self.currentTask() orelse return error.VoprIoAwaitOutsideTask;
            if (group.awaiter != null) return error.InvalidVoprIoGroup;
            group.awaiter = awaiter;
            awaiter.waiting_on_group = group;
            awaiter.status = .waiting_group;
            if (awaiter.cancel_requested) self.cancelGroupTasks(group);
            self.yieldCurrent(awaiter);
            awaiter.waiting_on_group = null;
            group.awaiter = null;
            if (group.tasks.items.len != 0 and awaiter.cancel_requested) self.cancelGroupTasks(group);
        }
        if (self.currentTask()) |task| try task.checkCancel();
        self.destroyGroup(group);
    }

    pub fn groupCancel(self: *Kernel, public_group: *std.Io.Group, token: *anyopaque) !void {
        const group: *GroupState = @ptrCast(@alignCast(token));
        if (group.public != public_group) return error.InvalidVoprIoGroup;
        self.cancelGroupTasks(group);
        while (group.tasks.items.len != 0) {
            const awaiter = self.currentTask() orelse return error.VoprIoAwaitOutsideTask;
            if (group.awaiter != null) return error.InvalidVoprIoGroup;
            group.awaiter = awaiter;
            awaiter.waiting_on_group = group;
            awaiter.status = .waiting_group;
            self.yieldCurrent(awaiter);
            awaiter.waiting_on_group = null;
            group.awaiter = null;
        }
        self.destroyGroup(group);
    }

    pub fn recancel(self: *Kernel) !void {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        if (!task.cancel_acknowledged) return error.InvalidVoprIoRecancel;
        task.cancel_acknowledged = false;
        task.cancel_requested = true;
    }

    pub fn swapCancelProtection(self: *Kernel, new: std.Io.CancelProtection) !std.Io.CancelProtection {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        const old = task.cancel_protection;
        task.cancel_protection = new;
        return old;
    }

    pub fn checkCancel(self: *Kernel) std.Io.Cancelable!void {
        const task = self.currentTask() orelse return;
        return task.checkCancel();
    }

    /// Stable logical owner for resources created by the currently executing
    /// fiber. Network identities use the immutable operation origin rather
    /// than either global connect order or the mutable scheduler identity that
    /// may bind to an external wait.
    pub fn currentTaskId(self: *Kernel) ?ids.StableId {
        return if (self.currentTask()) |task| task.resource_owner_id else null;
    }

    pub fn sleepCurrent(self: *Kernel, sleep_info: ?Sleep) !void {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        try task.checkCancel();
        task.sleep = sleep_info;
        task.status = .waiting_sleep;
        self.yieldCurrent(task);
        try task.checkCancel();
    }

    /// Cooperatively expose a stable preemption boundary inside CPU-bound
    /// product code. The current task becomes runnable again, so resumption is
    /// selected and recorded by the ordinary scheduler.
    pub fn yieldCurrentTask(self: *Kernel) !void {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        try task.checkCancel();
        task.status = .runnable;
        task.resume_generation +|= 1;
        self.yieldCurrent(task);
        try task.checkCancel();
    }

    pub fn futexWait(self: *Kernel, ptr: *const u32, expected: u32, sleep_info: ?Sleep, uncancelable: bool) !void {
        if (@atomicLoad(u32, ptr, .seq_cst) != expected) return;
        _ = try self.ensureFutexIdentity(ptr);
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        if (!uncancelable) try task.checkCancel();
        task.futex_ptr = ptr;
        task.futex_wait_sequence = self.allocateWaitSequence();
        task.futex_uncancelable = uncancelable;
        task.sleep = sleep_info;
        task.status = .waiting_futex;
        self.yieldCurrent(task);
        if (!uncancelable) try task.checkCancel();
    }

    pub fn futexWake(self: *Kernel, ptr: *const u32, max_waiters: u32) !void {
        if (max_waiters == 0 or !self.hasFutexWaiter(ptr)) return;
        _ = try self.ensureFutexIdentity(ptr);
        const sequence = try self.allocateSequence();
        try self.futex_wakes.append(self.allocator, .{
            .ptr = ptr,
            .remaining = max_waiters,
            .sequence = sequence,
            .eligible_through = self.next_wait_sequence - 1,
        });
    }

    /// Park the current fiber on a simulator-owned logical resource. The
    /// resource ID must be derived from stable modeled state, never a pointer.
    pub fn waitExternal(self: *Kernel, resource_id: ids.StableId) !void {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        try task.checkCancel();
        try self.bindExternalIdentity(task, resource_id);
        task.external_id = resource_id;
        task.external_wait_sequence = self.allocateWaitSequence();
        task.status = .waiting_external;
        self.yieldCurrent(task);
        try task.checkCancel();
    }

    /// Atomically park the current task and publish one readiness completion.
    /// This is the consumer-after-readiness counterpart to `wakeExternal` and
    /// prevents socket accept/read fast paths from disappearing from the
    /// scheduler merely because production work reached the wait slightly
    /// later within an otherwise atomic transition.
    pub fn waitExternalReady(self: *Kernel, resource_id: ids.StableId) !void {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        try task.checkCancel();
        try self.bindExternalIdentity(task, resource_id);
        const wait_sequence = self.allocateWaitSequence();
        const wake_sequence = try self.allocateExternalWakeSequence(resource_id);
        try self.external_wakes.append(self.allocator, .{
            .resource_id = resource_id,
            .remaining = 1,
            .sequence = wake_sequence,
            .eligible_through = wait_sequence,
        });
        task.external_id = resource_id;
        task.external_wait_sequence = wait_sequence;
        task.status = .waiting_external;
        self.yieldCurrent(task);
        try task.checkCancel();
    }

    /// Make waking an external waiter a scheduler-visible choice. A wake with
    /// no current waiter is intentionally discarded; modeled resources retain
    /// their own readiness state, so a later consumer observes it immediately.
    pub fn wakeExternal(self: *Kernel, resource_id: ids.StableId, max_waiters: u32) !void {
        if (max_waiters == 0 or !self.hasExternalWaiter(resource_id)) return;
        try self.external_wakes.append(self.allocator, .{
            .resource_id = resource_id,
            .remaining = max_waiters,
            .sequence = try self.allocateExternalWakeSequence(resource_id),
            .eligible_through = self.next_wait_sequence - 1,
        });
    }

    /// Move parked ownership from a provisional modeled resource to its
    /// semantic identity before readiness becomes scheduler-visible.
    pub fn rebindExternalResource(self: *Kernel, old_resource_id: ids.StableId, new_resource_id: ids.StableId) !void {
        if (old_resource_id == new_resource_id) return;
        var identity_count: u64 = 0;
        for (self.tasks.items) |task| {
            if (task.external_id == old_resource_id and task.identity_parent == old_resource_id)
                identity_count += 1;
        }
        var next_identity: u64 = 0;
        if (identity_count != 0) {
            const resource = for (self.external_resource_sequences.items) |*candidate| {
                if (candidate.resource_id == new_resource_id) break candidate;
            } else blk: {
                try self.external_resource_sequences.append(self.allocator, .{ .resource_id = new_resource_id });
                break :blk &self.external_resource_sequences.items[self.external_resource_sequences.items.len - 1];
            };
            next_identity = resource.next_waiter_sequence;
            resource.next_waiter_sequence = std.math.add(u64, next_identity, identity_count) catch
                return error.VoprIoSequenceExhausted;
        }
        for (self.external_wakes.items) |*wake| {
            if (wake.resource_id == old_resource_id) wake.resource_id = new_resource_id;
        }
        for (self.tasks.items) |task| {
            if (task.external_id != old_resource_id) continue;
            task.external_id = new_resource_id;
            if (task.identity_parent != old_resource_id) continue;
            const waiter_sequence = next_identity;
            task.id = ids.derive("vopr-io.external-task", new_resource_id, waiter_sequence);
            task.identity_parent = new_resource_id;
            task.identity_sequence = waiter_sequence;
            next_identity += 1;
        }
    }

    pub fn enumerateReady(self: *const Kernel, list: *transition.List, allocator: std.mem.Allocator) !void {
        for (self.tasks.items) |task| {
            if (task.status != .runnable) continue;
            try list.append(allocator, .{
                .id = task.transitionId(),
                .name = "vopr-io.task_resume",
                .kind = .scheduler,
                .actor_id = task.id,
                .resource_id = task.id,
                .parameter = @intCast(task.resume_generation),
            });
        }
        for (self.futex_wakes.items) |wake| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_futex or task.futex_ptr != wake.ptr or
                    task.futex_wait_sequence > wake.eligible_through) continue;
                try list.append(allocator, .{
                    .id = ids.derive("vopr-io.futex-wake", task.id, wake.sequence),
                    .name = "vopr-io.futex_wake",
                    .kind = .scheduler,
                    .actor_id = task.id,
                    .resource_id = self.futexIdentity(wake.ptr).?,
                    .parameter = wake.remaining,
                });
            }
        }
        for (self.external_wakes.items) |wake| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_external or task.external_id != wake.resource_id or
                    task.external_wait_sequence > wake.eligible_through) continue;
                try list.append(allocator, .{
                    .id = ids.derive("vopr-io.external-wake", task.id, wake.sequence),
                    .name = "vopr-io.external_wake",
                    .kind = .scheduler,
                    .actor_id = task.id,
                    .resource_id = wake.resource_id,
                    .parameter = wake.remaining,
                });
            }
        }
    }

    pub fn executeReady(self: *Kernel, transition_id: ids.StableId) !?Execution {
        self.pruneStaleWakes();
        for (self.tasks.items) |task| {
            if (task.status != .runnable or task.transitionId() != transition_id) continue;
            task.status = .running;
            self.switchToTask(task);
            const task_id = task.id;
            const completed = task.status == .finished;
            if (task.reap_after_switch) self.removeAndDestroyTask(task);
            return .{ .task_id = task_id, .completed = completed, .kind = .task };
        }

        for (self.futex_wakes.items, 0..) |*wake, wake_index| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_futex or task.futex_ptr != wake.ptr or
                    task.futex_wait_sequence > wake.eligible_through or
                    ids.derive("vopr-io.futex-wake", task.id, wake.sequence) != transition_id) continue;
                task.makeRunnable();
                const task_id = task.id;
                wake.remaining -= 1;
                if (wake.remaining == 0 or !self.hasEligibleFutexWaiter(wake)) {
                    _ = self.futex_wakes.orderedRemove(wake_index);
                }
                return .{ .task_id = task_id, .completed = false, .kind = .futex_wake };
            }
        }
        for (self.external_wakes.items, 0..) |*wake, wake_index| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_external or task.external_id != wake.resource_id or
                    task.external_wait_sequence > wake.eligible_through or
                    ids.derive("vopr-io.external-wake", task.id, wake.sequence) != transition_id) continue;
                task.makeRunnable();
                const task_id = task.id;
                wake.remaining -= 1;
                if (wake.remaining == 0 or !self.hasEligibleExternalWaiter(wake))
                    _ = self.external_wakes.orderedRemove(wake_index);
                return .{ .task_id = task_id, .completed = false, .kind = .external_wake };
            }
        }
        return null;
    }

    pub fn nextWakeDelta(self: *const Kernel, monotonic_ns: i96, realtime_ns: i96) ?u64 {
        var result: ?u64 = null;
        for (self.tasks.items) |task| {
            const sleep_info = task.sleep orelse continue;
            const current_ns = switch (sleep_info.clock) {
                .real => realtime_ns,
                .awake, .boot => monotonic_ns,
                .cpu_process, .cpu_thread => continue,
            };
            if (sleep_info.deadline_ns <= current_ns) return 0;
            const delta = std.math.cast(u64, sleep_info.deadline_ns - current_ns) orelse continue;
            if (result == null or delta < result.?) result = delta;
        }
        return result;
    }

    pub fn wakeDue(self: *Kernel, monotonic_ns: i96, realtime_ns: i96) void {
        for (self.tasks.items) |task| {
            const sleep_info = task.sleep orelse continue;
            const current_ns = switch (sleep_info.clock) {
                .real => realtime_ns,
                .awake, .boot => monotonic_ns,
                .cpu_process, .cpu_thread => continue,
            };
            if (sleep_info.deadline_ns <= current_ns) task.makeRunnable();
        }
    }

    fn createTask(
        self: *Kernel,
        result_len: usize,
        result_alignment: std.mem.Alignment,
        context_bytes: []const u8,
        context_alignment: std.mem.Alignment,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
        group: ?*GroupState,
    ) !*Task {
        if (self.tasks.items.len >= self.config.max_tasks) return error.VoprIoTaskLimitExceeded;
        if (!result_alignment.compare(.lte, .fromByteUnits(storage_alignment)) or
            !context_alignment.compare(.lte, .fromByteUnits(storage_alignment)))
            return error.VoprIoTaskAlignmentUnsupported;

        const creation_sequence = try self.allocateSequence();
        const identity = try self.allocateTaskIdentity(start);
        const result_offset = result_alignment.forward(context_bytes.len);
        const storage_len = result_offset + @max(result_len, 1);
        const task = try self.allocator.create(Task);
        errdefer self.allocator.destroy(task);
        const stack = try self.allocator.alignedAlloc(u8, .fromByteUnits(stack_alignment), self.config.stack_size);
        errdefer self.allocator.free(stack);
        const storage = try self.allocator.alignedAlloc(u8, .fromByteUnits(storage_alignment), storage_len);
        errdefer self.allocator.free(storage);
        @memcpy(storage[0..context_bytes.len], context_bytes);
        @memset(storage[result_offset..], 0);

        const entry_storage_size = std.mem.alignForward(usize, @sizeOf(Entry), stack_alignment);
        const entry_address = std.mem.alignBackward(
            usize,
            @intFromPtr(stack.ptr) + stack.len - entry_storage_size,
            stack_alignment,
        );
        const entry_context: *Entry = @ptrFromInt(entry_address);
        const task_id = ids.derive("vopr-io.task", identity.parent, identity.sequence);
        task.* = .{
            .kernel = self,
            .id = task_id,
            .resource_owner_id = task_id,
            .identity_parent = identity.parent,
            .identity_sequence = identity.sequence,
            .creation_sequence = creation_sequence,
            .context = switch (builtin.cpu.arch) {
                .aarch64 => .{ .sp = entry_address, .fp = 0, .pc = @intFromPtr(&Entry.entry) },
                .x86_64 => .{ .rsp = entry_address - 8, .rbp = 0, .rip = @intFromPtr(&Entry.entry) },
                else => unreachable,
            },
            .stack = stack,
            .storage = storage,
            .context_len = context_bytes.len,
            .result_offset = result_offset,
            .result_len = result_len,
            .start = start,
            .group = group,
        };
        entry_context.* = .{ .task = task };
        try self.tasks.append(self.allocator, task);
        return task;
    }

    fn allocateSequence(self: *Kernel) !u64 {
        if (self.next_sequence == std.math.maxInt(u64)) return error.VoprIoSequenceExhausted;
        const result = self.next_sequence;
        self.next_sequence += 1;
        return result;
    }

    const ScopedSequence = struct {
        parent: ids.StableId,
        sequence: u64,
    };

    fn allocateTaskIdentity(
        self: *Kernel,
        start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    ) !ScopedSequence {
        const parent = if (self.current) |current| current.id else capability_kernel_id;
        // Both addresses move by the same ASLR slide. The wrapping relative
        // offset is stable for one pinned target/source revision and therefore
        // suitable for replay identity without serializing a process address.
        const callsite = taskCallsite(start);
        for (self.task_identity_sequences.items) |*identity| {
            if (identity.parent != parent or identity.callsite != callsite) continue;
            if (identity.next_sequence == std.math.maxInt(u64))
                return error.VoprIoSequenceExhausted;
            const sequence = identity.next_sequence;
            identity.next_sequence += 1;
            return .{
                .parent = taskIdentityScope(parent, start),
                .sequence = sequence,
            };
        }
        try self.task_identity_sequences.append(self.allocator, .{
            .parent = parent,
            .callsite = callsite,
            .next_sequence = 2,
        });
        return .{
            .parent = taskIdentityScope(parent, start),
            .sequence = 1,
        };
    }

    fn allocateFutexIdentity(self: *Kernel) !ScopedSequence {
        if (self.current) |parent| {
            if (parent.next_futex_sequence == std.math.maxInt(u64))
                return error.VoprIoSequenceExhausted;
            const sequence = parent.next_futex_sequence;
            parent.next_futex_sequence += 1;
            return .{ .parent = parent.id, .sequence = sequence };
        }
        if (self.next_root_futex_sequence == std.math.maxInt(u64))
            return error.VoprIoSequenceExhausted;
        const sequence = self.next_root_futex_sequence;
        self.next_root_futex_sequence += 1;
        return .{ .parent = capability_kernel_id, .sequence = sequence };
    }

    fn finishCurrent(self: *Kernel, task: *Task) noreturn {
        std.debug.assert(self.currentTask() == task);
        task.status = .finished;
        if (task.awaiter) |awaiter| awaiter.makeRunnable();
        if (task.group) |group| {
            for (group.tasks.items, 0..) |candidate, index| {
                if (candidate != task) continue;
                _ = group.tasks.orderedRemove(index);
                break;
            }
            task.reap_after_switch = true;
            if (group.tasks.items.len == 0) {
                if (group.awaiter) |awaiter| awaiter.makeRunnable();
            }
        }
        self.yieldCurrent(task);
        unreachable;
    }

    noinline fn switchToTask(self: *Kernel, task: *Task) void {
        self.execution_thread_id.store(@intCast(std.Thread.getCurrentId()), .seq_cst);
        const message: std.Io.fiber.Switch = .{ .old = &self.main_context, .new = &task.context };
        _ = std.Io.fiber.contextSwitch(&message);
        self.setCurrent(null);
        self.execution_thread_id.store(0, .seq_cst);
    }

    noinline fn yieldCurrent(self: *Kernel, task: *Task) void {
        std.debug.assert(self.currentTask() == task);
        const message: std.Io.fiber.Switch = .{ .old = &task.context, .new = &self.main_context };
        _ = std.Io.fiber.contextSwitch(&message);
        self.setCurrent(task);
    }

    /// Context switching transfers control outside the compiler's ordinary
    /// call/return model. Task fibers publish their identity on entry/resume,
    /// the main fiber clears it after return, and atomic access plus non-inline
    /// switch boundaries prevent either side from moving that state across the
    /// transfer.
    fn currentTask(self: *Kernel) ?*Task {
        const task = @atomicLoad(?*Task, &self.current, .seq_cst);
        if (task != null) {
            const expected = self.execution_thread_id.load(.seq_cst);
            const actual: u64 = @intCast(std.Thread.getCurrentId());
            if (expected != 0 and expected != actual) std.debug.panic(
                "VoprIo accessed from foreign native thread expected_thread={} actual_thread={} task=0x{x} status={s}",
                .{ expected, actual, @intFromPtr(task.?), @tagName(task.?.status) },
            );
        }
        return task;
    }

    fn setCurrent(self: *Kernel, task: ?*Task) void {
        @atomicStore(?*Task, &self.current, task, .seq_cst);
    }

    fn destroyTaskMemory(self: *Kernel, task: *Task) void {
        self.allocator.free(task.storage);
        self.allocator.free(task.stack);
        self.allocator.destroy(task);
    }

    fn removeAndDestroyTask(self: *Kernel, task: *Task) void {
        for (self.tasks.items, 0..) |candidate, index| {
            if (candidate != task) continue;
            _ = self.tasks.orderedRemove(index);
            self.destroyTaskMemory(task);
            return;
        }
        unreachable;
    }

    fn getOrCreateGroup(self: *Kernel, public_group: *std.Io.Group) !*GroupState {
        if (public_group.token.load(.acquire)) |token| return @ptrCast(@alignCast(token));
        const group = try self.allocator.create(GroupState);
        errdefer self.allocator.destroy(group);
        group.* = .{ .public = public_group };
        try self.groups.append(self.allocator, group);
        public_group.token.store(group, .release);
        return group;
    }

    fn discardGroupIfEmpty(self: *Kernel, group: *GroupState) void {
        if (group.tasks.items.len == 0) self.destroyGroup(group);
    }

    fn destroyGroup(self: *Kernel, group: *GroupState) void {
        std.debug.assert(group.tasks.items.len == 0);
        group.public.token.store(null, .release);
        for (self.groups.items, 0..) |candidate, index| {
            if (candidate != group) continue;
            _ = self.groups.orderedRemove(index);
            break;
        }
        group.tasks.deinit(self.allocator);
        self.allocator.destroy(group);
    }

    fn cancelGroupTasks(self: *Kernel, group: *GroupState) void {
        _ = self;
        group.cancel_requested = true;
        for (group.tasks.items) |task| task.requestCancel();
    }

    fn hasFutexWaiter(self: *const Kernel, ptr: *const u32) bool {
        for (self.tasks.items) |task| {
            if (task.status == .waiting_futex and task.futex_ptr == ptr) return true;
        }
        return false;
    }

    fn hasExternalWaiter(self: *const Kernel, resource_id: ids.StableId) bool {
        for (self.tasks.items) |task| {
            if (task.status == .waiting_external and task.external_id == resource_id) return true;
        }
        return false;
    }

    fn hasEligibleFutexWaiter(self: *const Kernel, wake: *const FutexWake) bool {
        for (self.tasks.items) |task| {
            if (task.status == .waiting_futex and task.futex_ptr == wake.ptr and
                task.futex_wait_sequence <= wake.eligible_through) return true;
        }
        return false;
    }

    fn hasEligibleExternalWaiter(self: *const Kernel, wake: *const ExternalWake) bool {
        for (self.tasks.items) |task| {
            if (task.status == .waiting_external and task.external_id == wake.resource_id and
                task.external_wait_sequence <= wake.eligible_through) return true;
        }
        return false;
    }

    fn pruneStaleWakes(self: *Kernel) void {
        var futex_index: usize = 0;
        while (futex_index < self.futex_wakes.items.len) {
            if (!self.hasEligibleFutexWaiter(&self.futex_wakes.items[futex_index])) {
                _ = self.futex_wakes.orderedRemove(futex_index);
            } else futex_index += 1;
        }
        var external_index: usize = 0;
        while (external_index < self.external_wakes.items.len) {
            if (!self.hasEligibleExternalWaiter(&self.external_wakes.items[external_index])) {
                _ = self.external_wakes.orderedRemove(external_index);
            } else external_index += 1;
        }
    }

    fn ensureFutexIdentity(self: *Kernel, ptr: *const u32) !ids.StableId {
        // A futex address is only an identity while its contention epoch is
        // live. Allocators may reuse the same address for an unrelated mutex
        // after every waiter has gone away; retaining the old mapping forever
        // would make later scheduler IDs depend on heap layout. Concurrent
        // waiters still share one identity because the first waiter is already
        // published before another can enter this single-transition kernel.
        for (self.futex_identities.items, 0..) |identity, index| {
            if (identity.ptr != ptr) continue;
            if (self.hasFutexWaiter(ptr)) return identity.id;
            _ = self.futex_identities.orderedRemove(index);
            break;
        }
        const identity = try self.allocateFutexIdentity();
        const logical_id = ids.derive("vopr-io.futex", identity.parent, identity.sequence);
        try self.futex_identities.append(self.allocator, .{ .ptr = ptr, .id = logical_id });
        return logical_id;
    }

    fn allocateWaitSequence(self: *Kernel) u64 {
        const result = self.next_wait_sequence;
        self.next_wait_sequence +|= 1;
        return result;
    }

    fn allocateExternalWakeSequence(self: *Kernel, resource_id: ids.StableId) !u64 {
        for (self.external_resource_sequences.items) |*resource| {
            if (resource.resource_id != resource_id) continue;
            if (resource.next_wake_sequence == std.math.maxInt(u64))
                return error.VoprIoSequenceExhausted;
            const result = resource.next_wake_sequence;
            resource.next_wake_sequence += 1;
            return result;
        }
        try self.external_resource_sequences.append(self.allocator, .{ .resource_id = resource_id });
        self.external_resource_sequences.items[self.external_resource_sequences.items.len - 1].next_wake_sequence = 2;
        return 1;
    }

    fn bindExternalIdentity(self: *Kernel, task: *Task, resource_id: ids.StableId) !void {
        if (task.external_identity_bound) return;
        const waiter_sequence = try self.allocateExternalWaiterIdentity(resource_id);
        task.id = ids.derive("vopr-io.external-task", resource_id, waiter_sequence);
        task.identity_parent = resource_id;
        task.identity_sequence = waiter_sequence;
        task.external_identity_bound = true;
    }

    fn allocateExternalWaiterIdentity(self: *Kernel, resource_id: ids.StableId) !u64 {
        for (self.external_resource_sequences.items) |*resource| {
            if (resource.resource_id != resource_id) continue;
            if (resource.next_waiter_sequence == std.math.maxInt(u64))
                return error.VoprIoSequenceExhausted;
            const waiter_sequence = resource.next_waiter_sequence;
            resource.next_waiter_sequence += 1;
            return waiter_sequence;
        }
        try self.external_resource_sequences.append(self.allocator, .{
            .resource_id = resource_id,
            .next_waiter_sequence = 2,
        });
        return 1;
    }

    fn futexIdentity(self: *const Kernel, ptr: *const u32) ?ids.StableId {
        for (self.futex_identities.items) |identity| {
            if (identity.ptr == ptr) return identity.id;
        }
        return null;
    }

    const capability_kernel_id = ids.stable("vopr-io", "task-kernel-v1");
};

test "inactive futex address reuse starts a new logical contention epoch" {
    var kernel = try Kernel.init(std.testing.allocator, .{});
    defer kernel.deinit();
    var word: u32 = 0;

    const first = try kernel.ensureFutexIdentity(&word);
    const second = try kernel.ensureFutexIdentity(&word);
    try std.testing.expect(first != second);
    try std.testing.expectEqual(@as(usize, 1), kernel.futex_identities.items.len);
    try std.testing.expectEqual(second, kernel.futexIdentity(&word).?);
}

test "task and futex identities follow logical parents instead of global creation order" {
    const start = struct {
        fn call(_: *const anyopaque, _: *anyopaque) void {}
    }.call;

    var first = try Kernel.init(std.testing.allocator, .{});
    defer first.deinit();
    const first_root_a = try first.createTask(0, .@"1", &.{}, .@"1", start, null);
    first.current = first_root_a;
    const first_child_a = try first.createTask(0, .@"1", &.{}, .@"1", start, null);
    first.current = null;
    const first_root_b = try first.createTask(0, .@"1", &.{}, .@"1", start, null);
    first.current = first_root_b;
    const first_child_b = try first.createTask(0, .@"1", &.{}, .@"1", start, null);
    var first_word_a: u32 = 0;
    var first_word_b: u32 = 0;
    const first_futex_b = try first.ensureFutexIdentity(&first_word_b);
    first.current = first_root_a;
    const first_futex_a = try first.ensureFutexIdentity(&first_word_a);
    first.current = null;

    var second = try Kernel.init(std.testing.allocator, .{});
    defer second.deinit();
    const second_root_a = try second.createTask(0, .@"1", &.{}, .@"1", start, null);
    const second_root_b = try second.createTask(0, .@"1", &.{}, .@"1", start, null);
    second.current = second_root_b;
    const second_child_b = try second.createTask(0, .@"1", &.{}, .@"1", start, null);
    var second_word_a: u32 = 0;
    var second_word_b: u32 = 0;
    const second_futex_b = try second.ensureFutexIdentity(&second_word_b);
    second.current = second_root_a;
    const second_child_a = try second.createTask(0, .@"1", &.{}, .@"1", start, null);
    const second_futex_a = try second.ensureFutexIdentity(&second_word_a);
    second.current = null;

    try std.testing.expectEqual(first_root_a.id, second_root_a.id);
    try std.testing.expectEqual(first_root_b.id, second_root_b.id);
    try std.testing.expectEqual(first_child_a.id, second_child_a.id);
    try std.testing.expectEqual(first_child_b.id, second_child_b.id);
    try std.testing.expectEqual(first_futex_a, second_futex_a);
    try std.testing.expectEqual(first_futex_b, second_futex_b);
    try std.testing.expect(first_root_b.creation_sequence != second_root_b.creation_sequence);
    try std.testing.expect(first_child_a.creation_sequence != second_child_a.creation_sequence);
    try std.testing.expectEqual(taskIdentityScope(first_root_a.id, start), first_child_a.identity_parent);
    try std.testing.expectEqual(taskIdentityScope(first_root_b.id, start), first_child_b.identity_parent);
}

test "task identities are scoped by callsite before creation order" {
    const starts = struct {
        noinline fn first(context: *const anyopaque, _: *anyopaque) void {
            // Keep the two code identities distinct under ReleaseSafe. Empty
            // address-taken helpers may be folded into one function, which
            // would invalidate this test's premise rather than the task model.
            std.mem.doNotOptimizeAway(context);
        }
        noinline fn second(_: *const anyopaque, result: *anyopaque) void {
            std.mem.doNotOptimizeAway(result);
        }
    };

    var first = try Kernel.init(std.testing.allocator, .{});
    defer first.deinit();
    const first_a = try first.createTask(0, .@"1", &.{}, .@"1", starts.first, null);
    const first_b = try first.createTask(0, .@"1", &.{}, .@"1", starts.second, null);

    var second = try Kernel.init(std.testing.allocator, .{});
    defer second.deinit();
    const second_b = try second.createTask(0, .@"1", &.{}, .@"1", starts.second, null);
    const second_a = try second.createTask(0, .@"1", &.{}, .@"1", starts.first, null);

    try std.testing.expectEqual(first_a.id, second_a.id);
    try std.testing.expectEqual(first_b.id, second_b.id);
    try std.testing.expect(first_a.creation_sequence != second_a.creation_sequence);
    try std.testing.expect(first_b.creation_sequence != second_b.creation_sequence);
}

test "first external wait binds a task to the logical resource epoch" {
    const start = struct {
        fn call(_: *const anyopaque, _: *anyopaque) void {}
    }.call;
    const listener_resource = ids.stable("test", "listener-accept");

    var first = try Kernel.init(std.testing.allocator, .{});
    defer first.deinit();
    _ = try first.createTask(0, .@"1", &.{}, .@"1", start, null);
    const first_listener = try first.createTask(0, .@"1", &.{}, .@"1", start, null);
    const first_creation_id = first_listener.id;
    try first.bindExternalIdentity(first_listener, listener_resource);

    var second = try Kernel.init(std.testing.allocator, .{});
    defer second.deinit();
    const second_listener = try second.createTask(0, .@"1", &.{}, .@"1", start, null);
    const second_creation_id = second_listener.id;
    _ = try second.createTask(0, .@"1", &.{}, .@"1", start, null);
    try second.bindExternalIdentity(second_listener, listener_resource);

    try std.testing.expect(first_creation_id != second_creation_id);
    try std.testing.expectEqual(first_listener.id, second_listener.id);
    try std.testing.expectEqual(listener_resource, first_listener.identity_parent);
    try std.testing.expectEqual(@as(u64, 1), first_listener.identity_sequence);
    try std.testing.expectEqual(first_creation_id, first_listener.resource_owner_id);
    try std.testing.expectEqual(second_creation_id, second_listener.resource_owner_id);
}

test "parked external task migrates to a semantic resource identity" {
    const start = struct {
        fn call(_: *const anyopaque, _: *anyopaque) void {}
    }.call;
    const provisional = ids.stable("test", "provisional-socket-read");
    const semantic = ids.stable("test", "semantic-socket-read");

    var kernel = try Kernel.init(std.testing.allocator, .{});
    defer kernel.deinit();
    const task = try kernel.createTask(0, .@"1", &.{}, .@"1", start, null);
    try kernel.bindExternalIdentity(task, provisional);
    task.external_id = provisional;
    task.status = .waiting_external;

    try kernel.rebindExternalResource(provisional, semantic);
    try std.testing.expectEqual(semantic, task.external_id.?);
    try std.testing.expectEqual(semantic, task.identity_parent);
    try std.testing.expectEqual(ids.derive("vopr-io.external-task", semantic, 1), task.id);
}

test "task kernel parks future await and exposes each resume" {
    const Adapter = struct {
        fn async(userdata: ?*anyopaque, result: []u8, result_alignment: std.mem.Alignment, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque, *anyopaque) void) ?*std.Io.AnyFuture {
            const kernel: *Kernel = @ptrCast(@alignCast(userdata));
            return kernel.async(result, result_alignment, context, context_alignment, start);
        }
        fn concurrent(userdata: ?*anyopaque, result_len: usize, result_alignment: std.mem.Alignment, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque, *anyopaque) void) std.Io.ConcurrentError!*std.Io.AnyFuture {
            const kernel: *Kernel = @ptrCast(@alignCast(userdata));
            return kernel.concurrent(result_len, result_alignment, context, context_alignment, start);
        }
        fn await(userdata: ?*anyopaque, future: *std.Io.AnyFuture, result: []u8, result_alignment: std.mem.Alignment) void {
            const kernel: *Kernel = @ptrCast(@alignCast(userdata));
            kernel.await(future, result, result_alignment) catch unreachable;
        }
        const vtable: std.Io.VTable = blk: {
            var table = std.Io.failing.vtable.*;
            table.async = async;
            table.concurrent = concurrent;
            table.await = await;
            break :blk table;
        };
    };
    const Shared = struct {
        kernel: *Kernel,
        order: [4]u8 = [_]u8{0} ** 4,
        len: usize = 0,

        fn push(self: *@This(), value: u8) void {
            self.order[self.len] = value;
            self.len += 1;
        }

        fn child(self: *@This()) u32 {
            self.push(2);
            return 42;
        }

        fn parent(self: *@This()) void {
            self.push(1);
            var future = self.kernelIo().async(child, .{self});
            self.push(3);
            const value = future.await(self.kernelIo());
            std.debug.assert(value == 42);
            self.push(4);
        }

        fn kernelIo(self: *@This()) std.Io {
            return .{ .userdata = self.kernel, .vtable = &Adapter.vtable };
        }
    };

    var kernel = try Kernel.init(std.heap.page_allocator, .{});
    defer kernel.deinit();
    var shared = Shared{ .kernel = &kernel };
    _ = shared.kernelIo().async(Shared.parent, .{&shared});

    var enabled: transition.List = .{};
    defer enabled.deinit(std.heap.page_allocator);
    while (!kernel.isQuiescent()) {
        enabled.items.clearRetainingCapacity();
        try kernel.enumerateReady(&enabled, std.heap.page_allocator);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        _ = (try kernel.executeReady(enabled.items.items[0].id)).?;
    }
    try std.testing.expectEqualSlices(u8, &.{ 1, 3, 2, 4 }, shared.order[0..shared.len]);
}

test "futex wake cannot be stolen by a waiter that parks later" {
    const Adapter = struct {
        fn async(userdata: ?*anyopaque, result: []u8, result_alignment: std.mem.Alignment, context: []const u8, context_alignment: std.mem.Alignment, start: *const fn (*const anyopaque, *anyopaque) void) ?*std.Io.AnyFuture {
            const kernel: *Kernel = @ptrCast(@alignCast(userdata));
            return kernel.async(result, result_alignment, context, context_alignment, start);
        }
        const vtable: std.Io.VTable = blk: {
            var table = std.Io.failing.vtable.*;
            table.async = async;
            break :blk table;
        };
    };
    const Shared = struct {
        kernel: *Kernel,
        word: std.atomic.Value(u32) = .init(0),
        first_done: bool = false,
        second_done: bool = false,

        fn first(self: *@This()) void {
            self.kernel.futexWait(&self.word.raw, 0, null, true) catch unreachable;
            self.first_done = true;
        }

        fn second(self: *@This()) void {
            self.kernel.futexWait(&self.word.raw, 0, null, true) catch unreachable;
            self.second_done = true;
        }

        fn io(self: *@This()) std.Io {
            return .{ .userdata = self.kernel, .vtable = &Adapter.vtable };
        }
    };

    var kernel = try Kernel.init(std.heap.page_allocator, .{});
    defer kernel.deinit();
    var shared = Shared{ .kernel = &kernel };
    _ = shared.io().async(Shared.first, .{&shared});

    var ready: transition.List = .{};
    defer ready.deinit(std.heap.page_allocator);
    try kernel.enumerateReady(&ready, std.heap.page_allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    _ = (try kernel.executeReady(ready.items.items[0].id)).?;
    try kernel.futexWake(&shared.word.raw, 1);

    _ = shared.io().async(Shared.second, .{&shared});
    ready.items.clearRetainingCapacity();
    try kernel.enumerateReady(&ready, std.heap.page_allocator);
    const second_resume = for (ready.items.items) |candidate| {
        if (std.mem.eql(u8, candidate.name, "vopr-io.task_resume")) break candidate;
    } else return error.MissingSecondWaiterResume;
    _ = (try kernel.executeReady(second_resume.id)).?;

    ready.items.clearRetainingCapacity();
    try kernel.enumerateReady(&ready, std.heap.page_allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expectEqualStrings("vopr-io.futex_wake", ready.items.items[0].name);
    _ = (try kernel.executeReady(ready.items.items[0].id)).?;

    ready.items.clearRetainingCapacity();
    try kernel.enumerateReady(&ready, std.heap.page_allocator);
    try std.testing.expectEqual(@as(usize, 1), ready.items.items.len);
    try std.testing.expectEqualStrings("vopr-io.task_resume", ready.items.items[0].name);
    _ = (try kernel.executeReady(ready.items.items[0].id)).?;
    try std.testing.expect(shared.first_done);
    try std.testing.expect(!shared.second_done);
}
