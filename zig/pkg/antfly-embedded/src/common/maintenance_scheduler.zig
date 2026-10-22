// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Recurring work owns a registration, not a parked thread. Callbacks perform
//! one pass and return a retry delay (null parks until notified). The timer
//! coordinator never performs user work. At most max_active callbacks run on
//! the borrowed I/O lane; its remaining capacity stays available to durable
//! commits and nested storage operations. A handle must be joined before its
//! context is destroyed, and all handles before the scheduler is destroyed.
const std = @import("std");
const Io = std.Io;
const runnable = @import("antfly_runtime_abi").runnable_abi;
const errors = @import("antfly_runtime_abi").error_abi;
const native = @import("antfly_runtime_abi").native_abi;
const callback_bridge = @import("../storage/named_callback.zig");
const RunnableCallbacks = struct { step: *const fn (*anyopaque, runnable.Borrowed) anyerror!?u64 };
const RunnableBoundary = callback_bridge.Boundary(RunnableCallbacks);

pub const Scheduler = struct {
    pub const Class = enum { maintenance, producer, derived, propagation };
    const class_count = @typeInfo(Class).@"enum".field_names.len;

    /// Callbacks may wait for work in another class. Preserve one progress
    /// slot per class even when fractional CPU quota permits no parallel CPU
    /// stages; the runtime's async limit independently bounds those stages.
    pub fn cpuLimitedCapacity(cpu_parallelism: usize, lane_capacity: usize) usize {
        return @min(lane_capacity, @max(class_count, cpu_parallelism));
    }
    /// Each admitted pass has one original finite deadline, including reentry.
    /// Retained bytes cover callback-owned inputs; adapter metadata is added.
    pub const RunnablePass = struct {
        budget_ns: u64,
        retained_bytes: u64 = 0,
        fn validate(self: @This()) !void {
            if (self.budget_ns == 0 or self.budget_ns > 60 * std.time.ns_per_s or self.retained_bytes > std.math.maxInt(u64) - @sizeOf(Task)) return error.InvalidArgument;
        }
    };
    pub const RegistrationSpec = struct {
        class: Class = .maintenance,
        context: *anyopaque,
        step: ?*const fn (*anyopaque) ?u64 = null,
        runnable_step: ?*const fn (*anyopaque, runnable.Borrowed) anyerror!?u64 = null,
        pass: ?RunnablePass = null,
        callback_dispatch: callback_bridge.Dispatch = RunnableBoundary.local_dispatch,
    };
    pub const RemoteHandle = struct {
        context: *anyopaque,
        finish_fn: *const fn (*anyopaque, bool) void,
        stop_fn: *const fn (*anyopaque) void,
        ready_fn: ?*const fn (*anyopaque, *bool) callconv(.c) errors.Status = null,
    };
    pub const RemotePort = struct {
        context: *anyopaque,
        register_fn: *const fn (*anyopaque, RegistrationSpec) anyerror!RemoteHandle,
        wake_fn: *const fn (*anyopaque, *anyopaque) void,
        close_admission_fn: *const fn (*anyopaque) void,
        snapshot_fn: *const fn (*anyopaque) Stats,
    };
    remote: ?RemotePort = null,
    runnable_owner: ?runnable.Lease = null,
    runnable_closing: std.atomic.Value(bool) = .init(false),
    runnable_admissions: std.atomic.Value(u64) = .init(0),
    runnable_rejections: std.atomic.Value(u64) = .init(0),
    runnable_failures: std.atomic.Value(u64) = .init(0),
    alloc: std.mem.Allocator,
    io: Io,
    max_active: usize,
    max_registrations: usize,
    owned_registrations: usize = 0,
    registration_rejections: u64 = 0,
    mutex: Io.Mutex = .init,
    drained: Io.Condition = .init,
    changed: Io.Event = .unset,
    tasks: std.ArrayList(*Task) = .empty,
    coordinator: ?Io.Future(void) = null,
    stopping: bool = false,
    cursor: usize = 0,
    active: usize = 0,
    peak_active: usize = 0,
    dispatches: u64 = 0,
    dispatch_retries: u64 = 0,
    active_by_class: [class_count]usize = @splat(0),

    pub const Stats = struct {
        registrations: usize,
        owned_registrations: usize,
        max_registrations: usize,
        registration_rejections: u64,
        active: usize,
        peak_active: usize,
        active_by_class: [class_count]usize,
        max_active: usize,
        max_active_per_class: usize,
        dispatches: u64,
        dispatch_retries: u64,
        runnable_admissions: u64,
        runnable_rejections: u64,
        runnable_failures: u64,
    };

    pub fn snapshot(self: *Scheduler) Stats {
        if (self.remote) |port| return port.snapshot_fn(port.context);
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{
            .registrations = self.tasks.items.len,
            .owned_registrations = self.owned_registrations,
            .max_registrations = self.max_registrations,
            .registration_rejections = self.registration_rejections,
            .active = self.active,
            .peak_active = self.peak_active,
            .active_by_class = self.active_by_class,
            .max_active = self.max_active,
            .max_active_per_class = @max(1, self.max_active / class_count),
            .dispatches = self.dispatches,
            .dispatch_retries = self.dispatch_retries,
            .runnable_admissions = self.runnable_admissions.load(.acquire),
            .runnable_rejections = self.runnable_rejections.load(.acquire),
            .runnable_failures = self.runnable_failures.load(.acquire),
        };
    }

    const Task = struct {
        scheduler: *Scheduler,
        context: *anyopaque,
        step: ?*const fn (*anyopaque) ?u64 = null,
        runnable_step: ?*const fn (*anyopaque, runnable.Borrowed) anyerror!?u64 = null,
        runnable_pass: ?RunnablePass = null,
        canceled: std.atomic.Value(bool) = .init(false),
        class: Class,
        future: ?Io.Future(void) = null,
        done: std.atomic.Value(bool) = .init(false),
        delay_ms: ?u64 = 0,
        due_ms: u64 = 0,
        notified: bool = false,
    };

    pub const Handle = struct {
        task: *Task = undefined,
        remote: ?RemoteHandle = null,
        remote_scheduler: ?*Scheduler = null,

        pub fn requestStop(self: *Handle) void {
            if (self.remote) |port| {
                port.stop_fn(port.context);
                return;
            }
            const scheduler = self.task.scheduler;
            scheduler.mutex.lockUncancelable(scheduler.io);
            defer scheduler.mutex.unlock(scheduler.io);
            self.task.canceled.store(true, .release);
            scheduler.changed.set(scheduler.io);
        }
        /// Cancellation must fence redispatch before using this readiness
        /// result to join. A true result means user callback work (including
        /// its runnable capsule retirement) has returned, not just stop intent.
        pub fn readyToFinish(self: *const Handle) !bool {
            if (self.remote) |port| {
                const get = port.ready_fn orelse return error.ExecutionWaitUnsupported;
                var ready = false;
                const status = get(port.context, &ready);
                if (!status.isOk()) return errors.errorFromStatus(status);
                return ready;
            }
            const scheduler = self.task.scheduler;
            scheduler.mutex.lockUncancelable(scheduler.io);
            defer scheduler.mutex.unlock(scheduler.io);
            return self.task.future == null or self.task.done.load(.acquire);
        }

        /// Exclusive handle borrow. Failure retains the registration, context
        /// and scheduler lease for mandatory retry. Only genuine callback wait
        /// lends CPU; ready Future/registration metadata retires after reentry.
        pub fn cancelWithAdmission(self: *Handle, io: Io, original: @import("read_admission.zig").Port) !void {
            self.requestStop();
            var trace: @import("read_admission.zig").RejectionTrace = .{};
            const control = trace.bind(original).?;
            const boundary = (try control.executionWaitAfterLifetime()) orelse return error.ExecutionWaitUnsupported;
            _ = (try control.deadlineNs()) orelse return error.ExecutionWaitUnsupported;
            while (true) {
                try control.checkpoint();
                if (try self.readyToFinish()) {
                    self.cancel(io);
                    return;
                }
                try boundary.beginWait();
                var failure: ?anyerror = null;
                while (true) {
                    control.checkLifetime() catch |cause| {
                        failure = trace.failure orelse cause;
                        break;
                    };
                    const ready = self.readyToFinish() catch |cause| {
                        failure = cause;
                        break;
                    };
                    if (ready) break;
                    @import("antfly_platform").time.yieldNow();
                }
                boundary.endWait() catch |cause| {
                    if (failure) |first| return first;
                    return trace.failure orelse cause;
                };
                if (failure) |cause| return cause;
            }
        }

        pub fn await(self: *Handle, _: Io) void {
            self.finish(false);
        }
        pub fn cancel(self: *Handle, _: Io) void {
            self.finish(true);
        }

        fn finish(self: *Handle, cancel_active: bool) void {
            if (self.remote) |port| {
                port.finish_fn(port.context, cancel_active);
                self.remote_scheduler.?.releaseRegistration();
                self.* = undefined;
                return;
            }
            const task = self.task;
            const scheduler = task.scheduler;
            const alloc = scheduler.alloc;
            scheduler.mutex.lockUncancelable(scheduler.io);
            const index = for (scheduler.tasks.items, 0..) |entry, i| {
                if (entry == task) break i;
            } else unreachable;
            _ = scheduler.tasks.orderedRemove(index);
            // Detaching transfers exclusive future ownership to the joiner.
            const running = task.future != null;
            scheduler.mutex.unlock(scheduler.io);
            if (cancel_active) task.canceled.store(true, .release);
            if (task.future) |*future| {
                if (cancel_active) future.cancel(scheduler.io) else future.await(scheduler.io);
            }
            scheduler.mutex.lockUncancelable(scheduler.io);
            if (running) {
                scheduler.active -= 1;
                scheduler.active_by_class[@backingInt(task.class)] -= 1;
            }
            scheduler.changed.set(scheduler.io);
            scheduler.drained.broadcast(scheduler.io);
            scheduler.mutex.unlock(scheduler.io);
            alloc.destroy(task);
            scheduler.releaseRegistration();
            self.* = undefined;
        }
    };

    /// A context-local lifetime wrapper; it owns no executor or coordinator.
    pub fn createRemote(alloc: std.mem.Allocator, io: Io, port: RemotePort) !*Scheduler {
        const stats = port.snapshot_fn(port.context);
        const self = try alloc.create(Scheduler);
        self.* = .{ .alloc = alloc, .io = io, .max_active = stats.max_active, .max_registrations = stats.max_registrations, .remote = port };
        return self;
    }
    fn registerRemote(self: *Scheduler, spec: RegistrationSpec) !Handle {
        try self.reserveRegistration();
        errdefer self.releaseRegistration();
        const port = self.remote.?;
        return .{ .remote = try port.register_fn(port.context, spec), .remote_scheduler = self };
    }
    pub fn create(alloc: std.mem.Allocator, io: Io, max_active: usize) !*Scheduler {
        return createWithRunnableOwner(alloc, io, max_active, null);
    }

    /// Retain the shared node before any pass can be dispatched. The lease
    /// outlives registration detachment, callback joins and coordinator exit.
    pub const default_max_registrations: usize = 4096;
    pub const Config = struct {
        max_active: usize,
        max_registrations: usize = default_max_registrations,
        runnable_owner: ?runnable.Owner = null,
    };

    pub fn createWithRunnableOwner(alloc: std.mem.Allocator, io: Io, max_active: usize, owner: ?runnable.Owner) !*Scheduler {
        return createWithConfig(alloc, io, .{ .max_active = max_active, .runnable_owner = owner });
    }

    pub fn createWithConfig(alloc: std.mem.Allocator, io: Io, config: Config) !*Scheduler {
        const max_active = config.max_active;
        const owner = config.runnable_owner;
        if (max_active == 0 or config.max_registrations == 0) return error.InvalidMaintenanceCapacity;
        var lease = if (owner) |port| try runnable.Lease.init(port) else null;
        errdefer if (lease) |*value| value.deinit();
        const self = try alloc.create(Scheduler);
        errdefer alloc.destroy(self);
        self.* = .{ .alloc = alloc, .io = io, .max_active = max_active, .max_registrations = config.max_registrations, .runnable_owner = lease };
        self.coordinator = try io.concurrent(coordinate, .{self});
        return self;
    }

    /// Deny new registrations and callback dispatch before other runtime
    /// leases drain. Existing handles still own their physical callback joins.
    pub fn closeAdmission(self: *Scheduler) void {
        if (self.remote) |port| {
            self.mutex.lockUncancelable(self.io);
            self.stopping = true;
            self.mutex.unlock(self.io);
            port.close_admission_fn(port.context);
            return;
        }
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.stopping = true;
        self.runnable_closing.store(true, .release);
        self.changed.set(self.io);
    }

    pub fn destroy(self: *Scheduler) void {
        self.closeAdmission();
        self.mutex.lockUncancelable(self.io);
        // Registration handles are lifetime leases, like the runtime's API
        // and inference leases. Enforce their drain in release builds too.
        while (self.owned_registrations != 0 or self.active != 0)
            self.drained.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
        if (self.coordinator) |*future| future.await(self.io);
        self.tasks.deinit(self.alloc);
        if (self.runnable_owner) |*lease| lease.deinit();
        self.alloc.destroy(self);
    }

    pub fn register(self: *Scheduler, context: anytype, comptime step: fn (@TypeOf(context)) ?u64) !Handle {
        return self.registerClass(.maintenance, context, step);
    }

    pub fn registerClass(self: *Scheduler, class: Class, context: anytype, comptime step: fn (@TypeOf(context)) ?u64) !Handle {
        const Adapter = struct {
            fn call(raw: *anyopaque) ?u64 {
                return step(@ptrCast(@alignCast(raw)));
            }
        };
        if (self.remote != null) return self.registerRemote(.{ .class = class, .context = context, .step = Adapter.call });
        try self.reserveRegistration();
        errdefer self.releaseRegistration();
        const task = try self.alloc.create(Task);
        errdefer self.alloc.destroy(task);
        task.* = .{ .scheduler = self, .context = context, .step = Adapter.call, .class = class };
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.stopping) return error.BackendRuntimeShuttingDown;
        try self.tasks.append(self.alloc, task);
        self.changed.set(self.io);
        return .{ .task = task };
    }

    /// Explicit adoption only: each callback must checkpoint CPU work, park
    /// at actual waits, and physically join or transfer every published owner
    /// before returning. Retained service holders require preauthorized
    /// mandatory reentry/cleanup; this wrapper does not supply write authority.
    /// Initial CPU pressure retries a registration without running its callback
    /// or occupying a native runnable queue. Resumption uses the same capsule.
    pub fn registerRunnableClass(self: *Scheduler, class: Class, pass: RunnablePass, context: anytype, comptime step: fn (@TypeOf(context), runnable.Borrowed) anyerror!?u64) !Handle {
        try pass.validate();
        const Adapter = struct {
            fn call(raw: *anyopaque, borrow: runnable.Borrowed) anyerror!?u64 {
                return step(@ptrCast(@alignCast(raw)), borrow);
            }
        };
        if (self.remote != null) return self.registerRemote(.{ .class = class, .context = context, .runnable_step = Adapter.call, .pass = pass });
        if (self.runnable_owner == null) return error.ExecutionWaitUnsupported;
        try self.reserveRegistration();
        errdefer self.releaseRegistration();
        const task = try self.alloc.create(Task);
        errdefer self.alloc.destroy(task);
        task.* = .{ .scheduler = self, .context = context, .runnable_step = Adapter.call, .runnable_pass = pass, .class = class };
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.stopping) return error.BackendRuntimeShuttingDown;
        try self.tasks.append(self.alloc, task);
        self.changed.set(self.io);
        return .{ .task = task };
    }

    /// Reserve before allocating any task or growing the registry. Allocation
    /// runs outside the mutex: reclaimer callbacks cannot reenter admission
    /// under its lock. Pending installers and detached joiners retain capacity.
    fn reserveRegistration(self: *Scheduler) !void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        if (self.stopping) return error.BackendRuntimeShuttingDown;
        if (self.owned_registrations == self.max_registrations) {
            self.registration_rejections += 1;
            return error.ResourceBudgetExceeded;
        }
        self.owned_registrations += 1;
    }

    fn releaseRegistration(self: *Scheduler) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        std.debug.assert(self.owned_registrations != 0);
        self.owned_registrations -= 1;
        self.drained.broadcast(self.io);
    }

    /// Notifications coalesce even during a running pass. Lookup avoids
    /// exposing handle lifetime to concurrent producers and shutdown callers.
    pub fn wake(self: *Scheduler, context: *anyopaque) void {
        if (self.remote) |port| {
            port.wake_fn(port.context, context);
            return;
        }
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.tasks.items) |task| if (task.context == context) {
            task.notified = true;
            self.changed.set(self.io);
        };
    }

    fn now(self: *Scheduler) u64 {
        return @intCast(@max(0, @divTrunc(Io.Clock.awake.now(self.io).nanoseconds, std.time.ns_per_ms)));
    }

    fn run(task: *Task) void {
        const scheduler = task.scheduler;
        task.delay_ms = if (task.runnable_step != null) runRunnable(task) else task.step.?(task.context);
        task.done.store(true, .release);
        scheduler.changed.set(scheduler.io);
    }

    fn checkedRunnable(status: errors.Status) !void {
        if (!status.isOk()) return errors.errorFromStatus(status);
    }

    fn runnableCanceled(raw: ?*const anyopaque) callconv(.c) u8 {
        const task: *const Task = @ptrCast(@alignCast(raw.?));
        return @intFromBool(task.canceled.load(.acquire) or task.scheduler.runnable_closing.load(.acquire));
    }

    fn runRunnable(task: *Task) ?u64 {
        const self = task.scheduler;
        const owner = self.runnable_owner.?.owner;
        const pass = task.runnable_pass.?;
        const io_borrow = native.IoBorrow.init(&self.io);
        const now_ns: u64 = @intCast(@max(0, Io.Clock.awake.now(self.io).nanoseconds));
        var handle: ?*anyopaque = null;
        const bytes = std.math.add(u64, pass.retained_bytes, @sizeOf(Task)) catch {
            _ = self.runnable_failures.fetchAdd(1, .release);
            return null;
        };
        const status = owner.start(owner.context, &.{
            .io = &io_borrow,
            .class = @backingInt(runnable.Class.background),
            .slots = 1,
            .has_deadline = 1,
            .deadline_ns = now_ns +| pass.budget_ns,
            .allow_wait = 0,
            .cancellation = .{ .context = task, .is_cancelled = runnableCanceled },
            .retained_bytes = bytes,
            .out_handle = &handle,
        });
        if (!status.isOk()) {
            _ = self.runnable_rejections.fetchAdd(1, .release);
            const cause = errors.errorFromStatus(status);
            return switch (cause) {
                error.SchedulerClosed, error.Canceled => null,
                else => 10,
            };
        }
        const owned = handle orelse {
            _ = self.runnable_failures.fetchAdd(1, .release);
            return null;
        };
        // Callback completion is not retirement: its return contract requires
        // every child/resource join before this owning finish can execute.
        defer owner.finish(owned);
        _ = self.runnable_admissions.fetchAdd(1, .release);
        const borrow = runnable.Borrowed.fromOwner(owner, owned, 1, .background) catch {
            _ = self.runnable_failures.fetchAdd(1, .release);
            return null;
        };
        checkedRunnable(borrow.checkpoint(borrow.context)) catch {
            _ = self.runnable_failures.fetchAdd(1, .release);
            return if (runnableCanceled(task) != 0) null else 10;
        };
        const delay = task.runnable_step.?(task.context, borrow) catch {
            _ = self.runnable_failures.fetchAdd(1, .release);
            return if (runnableCanceled(task) != 0) null else 10;
        };
        // A parked, expired or canceled capsule cannot publish a successful
        // scheduling result, even when its callback swallowed a wait error.
        checkedRunnable(borrow.checkpoint(borrow.context)) catch {
            _ = self.runnable_failures.fetchAdd(1, .release);
            return if (runnableCanceled(task) != 0) null else 10;
        };
        return delay;
    }

    fn coordinate(self: *Scheduler) void {
        while (true) {
            self.mutex.lockUncancelable(self.io);
            if (self.stopping) {
                self.mutex.unlock(self.io);
                return;
            }
            self.changed.reset();
            const timestamp = self.now();
            var wait_ms: u64 = std.math.maxInt(u32);
            // Reap all finished callbacks before dispatching. Reaping while
            // scanning ready work can leave earlier tasks stranded after the
            // final active callback is joined, with nobody left to wake us.
            for (self.tasks.items) |task| {
                if (task.future != null and task.done.load(.acquire)) {
                    task.future.?.await(self.io);
                    task.future = null;
                    self.active -= 1;
                    self.active_by_class[@backingInt(task.class)] -= 1;
                    task.due_ms = if (task.delay_ms) |delay| timestamp +| delay else std.math.maxInt(u64);
                }
            }
            // Rotate the starting position so a continuously runnable owner
            // cannot starve later registrations when the lane is full.
            const count = self.tasks.items.len;
            const start = if (count == 0) 0 else self.cursor % count;
            for (0..count) |offset| {
                const index = (start + offset) % count;
                const task = self.tasks.items[index];
                if (task.future != null) continue;
                if (task.canceled.load(.acquire)) continue;
                if (task.notified) task.due_ms = 0;
                const class_index = @backingInt(task.class);
                // Producer passes may wait for derived publication. Reserved
                // class capacity prevents every runnable slot being occupied
                // by those waiters while their dependencies sit in the queue.
                if (task.due_ms <= timestamp and self.active < self.max_active and
                    self.active_by_class[class_index] < @max(1, self.max_active / class_count))
                {
                    task.done.store(false, .release);
                    task.future = self.io.concurrent(run, .{task}) catch {
                        // Transient lane saturation leaves the registration
                        // pending; table creation never needs a thread per shard.
                        self.dispatch_retries +|= 1;
                        wait_ms = @min(wait_ms, 10);
                        continue;
                    };
                    task.notified = false;
                    self.active += 1;
                    self.dispatches +|= 1;
                    self.active_by_class[class_index] += 1;
                    self.peak_active = @max(self.peak_active, self.active);
                    self.cursor = index + 1;
                } else if (task.due_ms > timestamp) {
                    wait_ms = @min(wait_ms, task.due_ms - timestamp);
                }
            }
            self.mutex.unlock(self.io);
            self.changed.waitTimeout(self.io, .{ .duration = .{
                .raw = Io.Duration.fromMilliseconds(@intCast(@max(1, wait_ms))),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => {},
                error.Canceled => {
                    // Deployment cancellation may precede owner destruction.
                    // Stop admitting passes while registration owners unwind;
                    // retrying this wait would keep their borrowed lane alive.
                    self.mutex.lockUncancelable(self.io);
                    self.stopping = true;
                    self.mutex.unlock(self.io);
                    return;
                },
            };
        }
    }
};

test "maintenance scheduler cancellation closes admission before owner destruction" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const scheduler = try Scheduler.create(std.testing.allocator, io, 2);
    defer scheduler.destroy();
    scheduler.coordinator.?.cancel(io);
    const Probe = struct {
        fn step(_: *@This()) ?u64 {
            return null;
        }
    };
    var probe: Probe = .{};
    try std.testing.expectError(error.BackendRuntimeShuttingDown, scheduler.register(&probe, Probe.step));
}

test "maintenance scheduler handles hundreds of parked owners with bounded runnable capacity" {
    inline for (.{ 1, 2 }) |capacity| {
        var threaded = Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
        defer threaded.deinit();
        const io = threaded.io();
        const scheduler = try Scheduler.create(std.testing.allocator, io, capacity);
        defer scheduler.destroy();
        const Probe = struct {
            calls: std.atomic.Value(usize) = .init(0),
            fn step(self: *@This()) ?u64 {
                _ = self.calls.fetchAdd(1, .acq_rel);
                return null;
            }
        };
        var probes: [256]Probe = @splat(.{});
        var handles: [256]Scheduler.Handle = undefined;
        var initialized: usize = 0;
        defer for (handles[0..initialized]) |*handle| handle.await(io);
        for (&probes, &handles) |*probe, *handle| {
            handle.* = try scheduler.register(probe, Probe.step);
            initialized += 1;
        }
        const deadline = Io.Clock.awake.now(io).nanoseconds + 10 * std.time.ns_per_s;
        while (true) {
            var complete = true;
            for (&probes) |*probe| complete = complete and probe.calls.load(.acquire) == 1;
            if (complete) break;
            if (Io.Clock.awake.now(io).nanoseconds >= deadline) return error.TestUnexpectedResult;
            try io.sleep(Io.Duration.fromMilliseconds(1), .awake);
        }
        scheduler.wake(&probes[0]);
        while (probes[0].calls.load(.acquire) != 2) {
            if (Io.Clock.awake.now(io).nanoseconds >= deadline) return error.TestUnexpectedResult;
            try io.sleep(Io.Duration.fromMilliseconds(1), .awake);
        }
        scheduler.mutex.lockUncancelable(io);
        try std.testing.expect(scheduler.peak_active <= capacity);
        scheduler.mutex.unlock(io);
    }
}

test "maintenance wake during active pass is retained and cancel joins its invocation" {
    const io = std.testing.io;
    const scheduler = try Scheduler.create(std.testing.allocator, io, 1);
    defer scheduler.destroy();
    const Probe = struct {
        scheduler: *Scheduler,
        calls: usize = 0,
        entered: Io.Event = .unset,
        canceled: bool = false,
        fn step(self: *@This()) ?u64 {
            self.calls += 1;
            if (self.calls == 1) {
                self.scheduler.wake(self);
                return null;
            }
            self.entered.set(self.scheduler.io);
            self.scheduler.io.sleep(Io.Duration.fromSeconds(3600), .awake) catch {
                self.canceled = true;
            };
            return null;
        }
    };
    var probe = Probe{ .scheduler = scheduler };
    var handle = try scheduler.register(&probe, Probe.step);
    errdefer handle.cancel(io);
    try probe.entered.waitTimeout(io, .{ .duration = .{ .raw = Io.Duration.fromSeconds(10), .clock = .awake } });
    handle.cancel(io);
    try std.testing.expect(probe.canceled);
    try std.testing.expectEqual(@as(usize, 2), probe.calls);
    scheduler.wake(&probe); // Unregistered identity is a harmless no-op.
}

test "maintenance producer waiters cannot consume derived publication capacity" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(5) });
    defer threaded.deinit();
    const io = threaded.io();
    const scheduler = try Scheduler.create(std.testing.allocator, io, Scheduler.cpuLimitedCapacity(1, 4));
    defer scheduler.destroy();
    const Probe = struct {
        io: Io,
        entered: Io.Event = .unset,
        published: Io.Event = .unset,
        finished: Io.Event = .unset,
        fn producer(self: *@This()) ?u64 {
            self.entered.set(self.io);
            self.published.wait(self.io) catch return null;
            self.finished.set(self.io);
            return null;
        }
        fn derived(self: *@This()) ?u64 {
            self.published.set(self.io);
            return null;
        }
    };
    var probe = Probe{ .io = io };
    var handles: [3]Scheduler.Handle = undefined;
    var initialized: usize = 0;
    defer for (handles[0..initialized]) |*handle| handle.cancel(io);
    for (&handles) |*handle| {
        handle.* = try scheduler.registerClass(.producer, &probe, Probe.producer);
        initialized += 1;
    }
    const timeout: Io.Timeout = .{ .duration = .{ .raw = Io.Duration.fromSeconds(10), .clock = .awake } };
    try probe.entered.waitTimeout(io, timeout);
    var publisher = try scheduler.registerClass(.derived, &probe, Probe.derived);
    defer publisher.cancel(io);
    try probe.finished.waitTimeout(io, timeout);
}

test "maintenance scheduler shutdown fences registration and drains owner handles" {
    const io = std.testing.io;
    const scheduler = try Scheduler.create(std.testing.allocator, io, 4);
    const Probe = struct {
        fn step(_: *@This()) ?u64 {
            return null;
        }
        fn shutdown(s: *Scheduler) void {
            s.destroy();
        }
    };
    var probe = Probe{};
    var handle = try scheduler.register(&probe, Probe.step);
    var shutdown = io.concurrent(Probe.shutdown, .{scheduler}) catch |err| {
        handle.await(io);
        scheduler.destroy();
        return err;
    };
    defer {
        handle.await(io);
        shutdown.await(io);
    }
    // The handle keeps the scheduler alive while we inspect its close fence.
    while (true) {
        scheduler.mutex.lockUncancelable(io);
        const stopping = scheduler.stopping;
        scheduler.mutex.unlock(io);
        if (stopping) break;
        try io.sleep(Io.Duration.fromMilliseconds(1), .awake);
    }
    try std.testing.expectError(error.BackendRuntimeShuttingDown, scheduler.register(&probe, Probe.step));
}

test "CPU limited maintenance capacity preserves dependency progress slots" {
    try std.testing.expectEqual(@as(usize, 4), Scheduler.cpuLimitedCapacity(1, 16));
    try std.testing.expectEqual(@as(usize, 8), Scheduler.cpuLimitedCapacity(8, 16));
    try std.testing.expectEqual(@as(usize, 4), Scheduler.cpuLimitedCapacity(8, 4));
}

test "maintenance scheduler registration capacity survives detachment until physical join" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    const scheduler = try Scheduler.createWithConfig(std.testing.allocator, io, .{ .max_active = 1, .max_registrations = 2 });
    defer scheduler.destroy();
    const Probe = struct {
        io: Io,
        entered: Io.Event = .unset,
        release: Io.Event = .unset,
        fn step(self: *@This()) ?u64 {
            self.entered.set(self.io);
            self.release.waitUncancelable(self.io);
            return null;
        }
        fn idle(_: *@This()) ?u64 {
            return null;
        }
    };
    var probe: Probe = .{ .io = io };
    var first = try scheduler.register(&probe, Probe.step);
    var first_owned = true;
    defer {
        probe.release.set(io);
        if (first_owned) first.await(io);
    }
    try probe.entered.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    var second = try scheduler.registerClass(.derived, &probe, Probe.idle);
    defer second.await(io);
    try std.testing.expectError(error.ResourceBudgetExceeded, scheduler.register(&probe, Probe.idle));
    var join: ?Io.Future(void) = try io.concurrent(Scheduler.Handle.await, .{ &first, io });
    first_owned = false;
    defer if (join) |*future| {
        probe.release.set(io);
        future.await(io);
    };
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
    while (scheduler.snapshot().registrations != 1 and std.Io.Clock.awake.now(io).nanoseconds < deadline.nanoseconds)
        try io.sleep(.fromMilliseconds(1), .awake);
    const detached = scheduler.snapshot();
    try std.testing.expectEqual(@as(usize, 1), detached.registrations);
    try std.testing.expectEqual(@as(usize, 2), detached.owned_registrations);
    try std.testing.expectEqual(@as(usize, 2), detached.max_registrations);
    try std.testing.expectError(error.ResourceBudgetExceeded, scheduler.registerClass(.producer, &probe, Probe.idle));
    try std.testing.expectEqual(@as(u64, 2), scheduler.snapshot().registration_rejections);
    probe.release.set(io);
    join.?.await(io);
    join = null;
    try std.testing.expectEqual(@as(usize, 1), scheduler.snapshot().owned_registrations);
    var replacement = try scheduler.registerClass(.propagation, &probe, Probe.idle);
    defer replacement.await(io);
    try std.testing.expectEqual(@as(usize, 2), scheduler.snapshot().owned_registrations);
}

test "maintenance scheduler registration allocation rollback restores capacity" {
    if (@import("builtin").single_threaded) return error.SkipZigTest;
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    const Probe = struct {
        fn step(_: *@This()) ?u64 {
            return null;
        }
    };
    var probe: Probe = .{};
    try std.testing.expectError(error.InvalidMaintenanceCapacity, Scheduler.createWithConfig(std.testing.allocator, io, .{ .max_active = 1, .max_registrations = 0 }));
    for (0..2) |failure| {
        const scheduler = try Scheduler.createWithConfig(std.testing.allocator, io, .{ .max_active = 1, .max_registrations = 1 });
        defer scheduler.destroy();
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = failure });
        scheduler.alloc = failing.allocator();
        try std.testing.expectError(error.OutOfMemory, scheduler.register(&probe, Probe.step));
        scheduler.alloc = std.testing.allocator;
        try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
        try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().registrations);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
        var handle = try scheduler.register(&probe, Probe.step);
        handle.await(io);
        scheduler.closeAdmission();
        try std.testing.expectError(error.BackendRuntimeShuttingDown, scheduler.register(&probe, Probe.step));
        try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
    }
}

test "maintenance scheduler execution-aware cancellation lends the sole ordinary slot and retains failed joins" {
    const admission = @import("read_admission.zig");
    const native_scheduler = @import("runnable_scheduler.zig");
    const now = @import("antfly_platform").time.monotonicNs;
    const Case = enum { success, begin_failure, end_failure };
    for (std.enums.values(Case)) |case| {
        const io = std.testing.io;
        var cpu = try native_scheduler.Scheduler.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_wait_ms = 5000 });
        defer cpu.close();
        var producer: native_scheduler.Execution = .{};
        defer producer.finish();
        try producer.start(&cpu, .{ .io = io, .native_now_ns = now, .allow_wait = true }, .background);
        try producer.park(.resource_blocked);
        var closer: native_scheduler.Execution = .{};
        defer closer.finish();
        try closer.start(&cpu, .{ .io = io, .native_now_ns = now, .allow_wait = true, .deadline_ns = now() + 5 * std.time.ns_per_s }, .query);
        var health: native_scheduler.Execution = .{};
        defer health.finish();
        try health.start(&cpu, .{ .io = io, .native_now_ns = now }, .control);
        const scheduler = try Scheduler.create(std.testing.allocator, io, 4);
        defer scheduler.destroy();
        const F = struct {
            io: Io,
            producer: *native_scheduler.Execution,
            entered: Io.Event = .unset,
            ran: bool = false,
            fn step(self: *@This()) ?u64 {
                defer self.producer.finish();
                self.entered.set(self.io);
                self.producer.enter(.background) catch return null;
                self.ran = true;
                return null;
            }
        };
        const Control = struct {
            execution: *native_scheduler.Execution,
            case: Case,
            begins: usize = 0,
            ends: usize = 0,
            fn lifetime(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try self.execution.checkLifetime();
            }
            fn checkpoint(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                try lifetime(raw);
                const entry = &self.execution.entry.?;
                if (entry.scheduler.?.state(entry) != .running) return error.InvalidRunnableTransition;
            }
            fn deadline(raw: *anyopaque) !?u64 {
                const self: *@This() = @ptrCast(@alignCast(raw));
                return self.execution.entry.?.options.deadline_ns;
            }
            fn begin(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.begins += 1;
                if (self.case == .begin_failure) return error.NotFound;
                try self.execution.park(.resource_blocked);
            }
            fn end(raw: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.ends += 1;
                if (self.case == .end_failure) return error.Canceled;
                try self.execution.enter(.query);
            }
        };
        var f: F = .{ .io = io, .producer = &producer };
        var handle = try scheduler.register(&f, F.step);
        var live = true;
        defer if (live) {
            closer.finish();
            handle.cancel(io);
        };
        try f.entered.wait(io);
        var control: Control = .{ .execution = &closer, .case = case };
        var payload: admission.Payload = .{ .ptr = &control, .vtable = &.{ .checkpoint = Control.checkpoint, .check_lifetime = Control.lifetime, .deadline_ns = Control.deadline }, .execution_wait = .{ .ptr = &control, .begin = Control.begin, .end = Control.end } };
        const result = handle.cancelWithAdmission(io, .init(&payload));
        if (case == .success) {
            try result;
            live = false;
        } else {
            try std.testing.expectError(if (case == .begin_failure) error.NotFound else error.Canceled, result);
            try std.testing.expectEqual(@as(usize, 1), scheduler.snapshot().owned_registrations);
            try std.testing.expectEqual(@as(usize, 1), scheduler.snapshot().registrations);
            closer.finish();
            handle.cancel(io);
            live = false;
        }
        try std.testing.expectEqual(@as(usize, 1), control.begins);
        try std.testing.expectEqual(@as(usize, if (case == .begin_failure) 0 else 1), control.ends);
        if (case == .success) {
            try std.testing.expect(f.ran);
            try std.testing.expectEqual(native_scheduler.State.running, cpu.state(&closer.entry.?));
        }
        try std.testing.expectEqual(native_scheduler.State.running, cpu.state(&health.entry.?));
        try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().owned_registrations);
        closer.finish();
        health.finish();
        try std.testing.expectEqual(@as(usize, 0), cpu.stats().operations);
    }
}
