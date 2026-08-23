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
};

const FutexIdentity = struct {
    ptr: *const u32,
    id: ids.StableId,
};

const ExternalWake = struct {
    resource_id: ids.StableId,
    remaining: u32,
    sequence: u64,
};

const Task = struct {
    kernel: *Kernel,
    id: ids.StableId,
    creation_sequence: u64,
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
    external_id: ?ids.StableId = null,
    futex_uncancelable: bool = false,
    cancel_requested: bool = false,
    cancel_acknowledged: bool = false,
    cancel_protection: std.Io.CancelProtection = .unblocked,
    reap_after_switch: bool = false,

    fn transitionId(self: *const Task) ids.StableId {
        return ids.derive("sim-io.task-resume", self.id, self.resume_generation);
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
        if (self.status != .finished and self.status != .running and self.status != .runnable) {
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

pub const Execution = struct {
    task_id: ids.StableId,
    completed: bool,
    kind: enum { task, futex_wake, external_wake },
};

pub const Kernel = struct {
    allocator: std.mem.Allocator,
    config: Config,
    next_sequence: u64 = 1,
    main_context: std.Io.fiber.Context = undefined,
    current: ?*Task = null,
    tasks: std.ArrayListUnmanaged(*Task) = .empty,
    groups: std.ArrayListUnmanaged(*GroupState) = .empty,
    futex_wakes: std.ArrayListUnmanaged(FutexWake) = .empty,
    futex_identities: std.ArrayListUnmanaged(FutexIdentity) = .empty,
    external_wakes: std.ArrayListUnmanaged(ExternalWake) = .empty,

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
        if (group.tasks.items.len != 0) {
            const awaiter = self.currentTask() orelse return error.VoprIoAwaitOutsideTask;
            if (group.awaiter != null) return error.InvalidVoprIoGroup;
            group.awaiter = awaiter;
            awaiter.waiting_on_group = group;
            awaiter.status = .waiting_group;
            if (awaiter.cancel_requested) self.cancelGroupTasks(group);
            self.yieldCurrent(awaiter);
            awaiter.waiting_on_group = null;
            try awaiter.checkCancel();
        }
        self.destroyGroup(group);
    }

    pub fn groupCancel(self: *Kernel, public_group: *std.Io.Group, token: *anyopaque) !void {
        const group: *GroupState = @ptrCast(@alignCast(token));
        if (group.public != public_group) return error.InvalidVoprIoGroup;
        self.cancelGroupTasks(group);
        if (group.tasks.items.len != 0) {
            const awaiter = self.currentTask() orelse return error.VoprIoAwaitOutsideTask;
            if (group.awaiter != null) return error.InvalidVoprIoGroup;
            group.awaiter = awaiter;
            awaiter.waiting_on_group = group;
            awaiter.status = .waiting_group;
            self.yieldCurrent(awaiter);
            awaiter.waiting_on_group = null;
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
        });
    }

    /// Park the current fiber on a simulator-owned logical resource. The
    /// resource ID must be derived from stable modeled state, never a pointer.
    pub fn waitExternal(self: *Kernel, resource_id: ids.StableId) !void {
        const task = self.currentTask() orelse return error.VoprIoOperationOutsideTask;
        try task.checkCancel();
        task.external_id = resource_id;
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
            .sequence = try self.allocateSequence(),
        });
    }

    pub fn enumerateReady(self: *const Kernel, list: *transition.List, allocator: std.mem.Allocator) !void {
        for (self.tasks.items) |task| {
            if (task.status != .runnable) continue;
            try list.append(allocator, .{
                .id = task.transitionId(),
                .name = "sim-io.task_resume",
                .kind = .scheduler,
                .actor_id = task.id,
                .resource_id = task.id,
                .parameter = @intCast(task.resume_generation),
            });
        }
        for (self.futex_wakes.items) |wake| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_futex or task.futex_ptr != wake.ptr) continue;
                try list.append(allocator, .{
                    .id = ids.derive("sim-io.futex-wake", task.id, wake.sequence),
                    .name = "sim-io.futex_wake",
                    .kind = .scheduler,
                    .actor_id = task.id,
                    .resource_id = self.futexIdentity(wake.ptr).?,
                    .parameter = wake.remaining,
                });
            }
        }
        for (self.external_wakes.items) |wake| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_external or task.external_id != wake.resource_id) continue;
                try list.append(allocator, .{
                    .id = ids.derive("sim-io.external-wake", task.id, wake.sequence),
                    .name = "sim-io.external_wake",
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
                    ids.derive("sim-io.futex-wake", task.id, wake.sequence) != transition_id) continue;
                task.makeRunnable();
                const task_id = task.id;
                wake.remaining -= 1;
                if (wake.remaining == 0 or !self.hasFutexWaiter(wake.ptr)) {
                    _ = self.futex_wakes.orderedRemove(wake_index);
                }
                return .{ .task_id = task_id, .completed = false, .kind = .futex_wake };
            }
        }
        for (self.external_wakes.items, 0..) |*wake, wake_index| {
            for (self.tasks.items) |task| {
                if (task.status != .waiting_external or task.external_id != wake.resource_id or
                    ids.derive("sim-io.external-wake", task.id, wake.sequence) != transition_id) continue;
                task.makeRunnable();
                const task_id = task.id;
                wake.remaining -= 1;
                if (wake.remaining == 0 or !self.hasExternalWaiter(wake.resource_id))
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

        const sequence = try self.allocateSequence();
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
        task.* = .{
            .kernel = self,
            .id = ids.derive("sim-io.task", capability_kernel_id, sequence),
            .creation_sequence = sequence,
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
        const message: std.Io.fiber.Switch = .{ .old = &self.main_context, .new = &task.context };
        _ = std.Io.fiber.contextSwitch(&message);
        self.setCurrent(null);
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
        return @atomicLoad(?*Task, &self.current, .seq_cst);
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

    fn pruneStaleWakes(self: *Kernel) void {
        var futex_index: usize = 0;
        while (futex_index < self.futex_wakes.items.len) {
            if (!self.hasFutexWaiter(self.futex_wakes.items[futex_index].ptr)) {
                _ = self.futex_wakes.orderedRemove(futex_index);
            } else futex_index += 1;
        }
        var external_index: usize = 0;
        while (external_index < self.external_wakes.items.len) {
            if (!self.hasExternalWaiter(self.external_wakes.items[external_index].resource_id)) {
                _ = self.external_wakes.orderedRemove(external_index);
            } else external_index += 1;
        }
    }

    fn ensureFutexIdentity(self: *Kernel, ptr: *const u32) !ids.StableId {
        if (self.futexIdentity(ptr)) |existing| return existing;
        const sequence = try self.allocateSequence();
        const logical_id = ids.derive("sim-io.futex", capability_kernel_id, sequence);
        try self.futex_identities.append(self.allocator, .{ .ptr = ptr, .id = logical_id });
        return logical_id;
    }

    fn futexIdentity(self: *const Kernel, ptr: *const u32) ?ids.StableId {
        for (self.futex_identities.items) |identity| {
            if (identity.ptr == ptr) return identity.id;
        }
        return null;
    }

    const capability_kernel_id = ids.stable("sim-io", "task-kernel-v1");
};

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
