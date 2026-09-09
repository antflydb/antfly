// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: LicenseRef-Elastic-2.0

//! Recurring work owns a registration, not a parked thread. Callbacks perform
//! one pass and return a retry delay (null parks until notified). The timer
//! coordinator never performs user work. At most max_active callbacks run on
//! the borrowed I/O lane; its remaining capacity stays available to durable
//! commits and nested storage operations. A handle must be joined before its
//! context is destroyed, and all handles before the scheduler is destroyed.
const std = @import("std");
const Io = std.Io;

pub const Scheduler = struct {
    pub const Class = enum { maintenance, producer, derived, propagation };
    const class_count = @typeInfo(Class).@"enum".fields.len;
    alloc: std.mem.Allocator,
    io: Io,
    max_active: usize,
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
        active: usize,
        peak_active: usize,
        active_by_class: [class_count]usize,
        max_active: usize,
        max_active_per_class: usize,
        dispatches: u64,
        dispatch_retries: u64,
    };

    pub fn snapshot(self: *Scheduler) Stats {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return .{
            .registrations = self.tasks.items.len,
            .active = self.active,
            .peak_active = self.peak_active,
            .active_by_class = self.active_by_class,
            .max_active = self.max_active,
            .max_active_per_class = @max(1, self.max_active / class_count),
            .dispatches = self.dispatches,
            .dispatch_retries = self.dispatch_retries,
        };
    }

    const Task = struct {
        scheduler: *Scheduler,
        context: *anyopaque,
        step: *const fn (*anyopaque) ?u64,
        class: Class,
        future: ?Io.Future(void) = null,
        done: std.atomic.Value(bool) = .init(false),
        delay_ms: ?u64 = 0,
        due_ms: u64 = 0,
        notified: bool = false,
    };

    pub const Handle = struct {
        task: *Task,

        pub fn await(self: *Handle, _: Io) void {
            self.finish(false);
        }
        pub fn cancel(self: *Handle, _: Io) void {
            self.finish(true);
        }

        fn finish(self: *Handle, cancel_active: bool) void {
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
            if (task.future) |*future| {
                if (cancel_active) future.cancel(scheduler.io) else future.await(scheduler.io);
            }
            scheduler.mutex.lockUncancelable(scheduler.io);
            if (running) {
                scheduler.active -= 1;
                scheduler.active_by_class[@intFromEnum(task.class)] -= 1;
            }
            scheduler.changed.set(scheduler.io);
            scheduler.drained.broadcast(scheduler.io);
            scheduler.mutex.unlock(scheduler.io);
            alloc.destroy(task);
            self.* = undefined;
        }
    };

    pub fn create(alloc: std.mem.Allocator, io: Io, max_active: usize) !*Scheduler {
        if (max_active == 0) return error.InvalidMaintenanceCapacity;
        const self = try alloc.create(Scheduler);
        errdefer alloc.destroy(self);
        self.* = .{ .alloc = alloc, .io = io, .max_active = max_active };
        self.coordinator = try io.concurrent(coordinate, .{self});
        return self;
    }

    pub fn destroy(self: *Scheduler) void {
        self.mutex.lockUncancelable(self.io);
        self.stopping = true;
        self.changed.set(self.io);
        // Registration handles are lifetime leases, like the runtime's API
        // and inference leases. Enforce their drain in release builds too.
        while (self.tasks.items.len != 0 or self.active != 0)
            self.drained.waitUncancelable(self.io, &self.mutex);
        self.mutex.unlock(self.io);
        if (self.coordinator) |*future| future.await(self.io);
        self.tasks.deinit(self.alloc);
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

    /// Notifications coalesce even during a running pass. Lookup avoids
    /// exposing handle lifetime to concurrent producers and shutdown callers.
    pub fn wake(self: *Scheduler, context: *anyopaque) void {
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
        task.delay_ms = task.step(task.context);
        task.done.store(true, .release);
        scheduler.changed.set(scheduler.io);
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
                    self.active_by_class[@intFromEnum(task.class)] -= 1;
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
                if (task.notified) task.due_ms = 0;
                const class_index = @intFromEnum(task.class);
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
            } }) catch {};
        }
    }
};

test "maintenance scheduler handles hundreds of parked owners with bounded runnable capacity" {
    var threaded = Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    const scheduler = try Scheduler.create(std.testing.allocator, io, 2);
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
    try std.testing.expect(scheduler.peak_active <= 2);
    scheduler.mutex.unlock(io);
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
    const scheduler = try Scheduler.create(std.testing.allocator, io, 3);
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
