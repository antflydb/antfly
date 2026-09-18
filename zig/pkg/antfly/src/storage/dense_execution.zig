// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! First operator binding for the common scheduler. Dense drivers retain a
//! coarse runnable lease through scan/rerank and helper join. Opt-in native
//! exact positional reads can suspend only with scoped read arenas and local
//! I/O ownership. Other pooled HBC scratch keeps its existing owner. This
//! binding does not provide general cooperative lane isolation.
const std = @import("std");
const resources = @import("../common/workload_resources.zig");
const scheduling = @import("../common/workload_scheduler.zig");
const admission = @import("../common/workload_admission.zig");

pub const Config = struct {
    max_runnable_tasks: u32 = 0,
    max_outstanding_tasks: u32 = 0,
    max_queued_tasks: u32 = 0,
    max_wait_ms: u32 = 0,
    max_working_bytes: u64 = 0,
    max_suspended_io: u32 = 0,

    pub fn validate(self: Config) !void {
        if (self.max_runnable_tasks == 0) {
            if (self.max_outstanding_tasks != 0 or self.max_queued_tasks != 0 or self.max_wait_ms != 0 or self.max_working_bytes != 0 or self.max_suspended_io != 0) return error.InvalidConfig;
            return;
        }
        if (self.max_outstanding_tasks < self.max_runnable_tasks or self.max_outstanding_tasks > 65_536 or
            self.max_queued_tasks > self.max_outstanding_tasks or self.max_wait_ms > 60_000 or self.max_working_bytes > 1_099_511_627_776 or
            ((self.max_wait_ms == 0) != (self.max_queued_tasks == 0))) return error.InvalidConfig;
        if (self.max_suspended_io > self.max_outstanding_tasks or
            (self.max_suspended_io != 0 and (self.max_working_bytes == 0 or self.max_wait_ms == 0))) return error.InvalidConfig;
    }
};

pub const Stats = struct {
    max_runnable_tasks: u32 = 0,
    max_outstanding_tasks: u32 = 0,
    max_queued_tasks: u32 = 0,
    max_wait_ms: u32 = 0,
    max_working_bytes: u64 = 0,
    working_bytes: u64 = 0,
    max_suspended_io: u32 = 0,
    suspended_io: u64 = 0,
    runnable: u64 = 0,
    outstanding: u64 = 0,
    queued: u64 = 0,
};

pub const Runtime = struct {
    allocator: std.mem.Allocator,
    config: Config,
    ledger: resources.Ledger,
    scheduler: scheduling.Scheduler,

    pub const Lease = struct {
        runtime: ?*Runtime = null,
        options: ?admission.Options = null,
        request: ?resources.RequestLease = null,
        job: ?scheduling.Scheduler.Job = null,
        continuation: ?resources.RetainedStateLease = null,

        pub fn release(self: *Lease) void {
            if (self.job) |*job| job.release(1);
            self.job = null;
            if (self.continuation) |*state| state.release() catch unreachable;
            self.continuation = null;
            if (self.request) |*request| request.release() catch unreachable;
            self.request = null;
        }

        /// Called only by serial physical reads with all destination storage
        /// already owned. An unavailable I/O credit keeps the coarse driver.
        pub fn suspendIo(self: *Lease) !?resources.LocalIoLease {
            const runtime = self.runtime orelse return null;
            if (runtime.effectiveSuspendedIo(self.options.?.io) == 0) return null;
            try self.options.?.check();
            var io_lease = runtime.ledger.acquire(.local_io, &self.request.?, .{ .io = 1 }) catch |err| switch (err) {
                error.ResourceTemporarilyUnavailable => return null,
                else => return mapError(err),
            };
            errdefer io_lease.release() catch unreachable;
            self.continuation = try self.job.?.yieldState(0, 1);
            self.job = null;
            return io_lease;
        }

        pub fn resumeIo(self: *Lease, io_lease: *resources.LocalIoLease) !void {
            io_lease.release() catch unreachable;
            const runtime = self.runtime.?;
            self.job = runtime.scheduler.acquireResume(&self.request.?, .general_read, &self.continuation.?, .{ .runnable = 1 }, 1, 0, self.options.?) catch |err| return mapError(err);
            self.continuation = null;
        }
    };

    pub fn create(allocator: std.mem.Allocator, config: Config) !*Runtime {
        try config.validate();
        if (config.max_runnable_tasks == 0) return error.InvalidConfig;
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        // Request, runnable/queued, working-state and optional I/O per task. Fixed
        // preallocation bounds scheduler metadata before publishing the owner.
        const total: resources.Bundle = .{
            .handles = @as(u64, config.max_outstanding_tasks) * 4,
            .requests = config.max_outstanding_tasks,
            .queued = config.max_queued_tasks,
            .runnable = config.max_runnable_tasks,
            .retained_bytes = config.max_working_bytes,
            .io = config.max_suspended_io,
        };
        self.* = .{
            .allocator = allocator,
            .config = config,
            .ledger = try resources.Ledger.init(allocator, .{ .total = total, .lanes = @splat(.{ .ceiling = total }) }),
            .scheduler = undefined,
        };
        errdefer self.ledger.deinit();
        self.scheduler = try scheduling.Scheduler.init(&self.ledger, .{ .max_wait_ms = config.max_wait_ms });
        return self;
    }

    pub fn destroy(self: *Runtime) void {
        self.scheduler.close();
        self.ledger.deinit();
        self.allocator.destroy(self);
    }

    pub fn stats(self: *Runtime) Stats {
        const snapshot = self.ledger.snapshot();
        return .{
            .max_runnable_tasks = self.config.max_runnable_tasks,
            .max_outstanding_tasks = self.config.max_outstanding_tasks,
            .max_queued_tasks = self.config.max_queued_tasks,
            .max_wait_ms = self.config.max_wait_ms,
            .max_working_bytes = self.config.max_working_bytes,
            .working_bytes = snapshot.total.retained_bytes,
            .max_suspended_io = self.config.max_suspended_io,
            .suspended_io = snapshot.total.io,
            .runnable = snapshot.total.runnable,
            .outstanding = snapshot.total.requests,
            .queued = snapshot.total.queued,
        };
    }

    /// Existing enclosing HBC/source snapshots still have thread-affine scope
    /// bookkeeping. They must be made request-local before migrating providers
    /// can use suspension. The native ABI preserves this originating proof.
    pub fn effectiveSuspendedIo(self: *const Runtime, io: std.Io) u32 {
        return if (@import("../runtime_io_abi.zig").callerThreadPinned(io)) self.config.max_suspended_io else 0;
    }

    pub fn acquire(self: *Runtime, options: admission.Options) !Lease {
        try options.check();
        var request = self.ledger.admit(.general_read, 0) catch |err| return mapError(err);
        errdefer request.release() catch unreachable;
        const job = self.scheduler.acquire(&request, .general_read, .{ .runnable = 1 }, 1, 0, options) catch |err| return mapError(err);
        return .{ .runtime = self, .options = options, .request = request, .job = job };
    }

    pub fn tryAcquire(self: *Runtime) ?Lease {
        var request = self.ledger.admit(.general_read, 0) catch return null;
        const job = self.scheduler.tryAcquire(&request, .general_read, .{ .runnable = 1 }, 1) catch null;
        if (job == null) {
            request.release() catch unreachable;
            return null;
        }
        return .{ .request = request, .job = job };
    }

    fn mapError(err: anyerror) anyerror {
        return switch (err) {
            error.ResourceTemporarilyUnavailable => error.AdmissionFull,
            error.ResourceRequestTooLarge => error.AdmissionRequestTooLarge,
            else => err,
        };
    }
};

test "workload admission dense driver and optional helper share real ownership" {
    const runtime = try Runtime.create(std.testing.allocator, .{ .max_runnable_tasks = 2, .max_outstanding_tasks = 3 });
    defer runtime.destroy();
    var driver = try runtime.acquire(.{ .io = std.testing.io });
    var helper = runtime.tryAcquire().?;
    try std.testing.expect(runtime.tryAcquire() == null);
    try std.testing.expectEqual(@as(u64, 2), runtime.ledger.snapshot().total.runnable);
    // A driver's cancellation/completion cannot retire still-running helpers.
    driver.release();
    try std.testing.expectEqual(@as(u64, 1), runtime.ledger.snapshot().total.runnable);
    helper.release();
    helper.release();
    try std.testing.expectEqual(@as(u64, 0), runtime.ledger.snapshot().total.handles);
}

test "workload admission dense deadline and failed helper grants leave no owners" {
    const runtime = try Runtime.create(std.testing.allocator, .{ .max_runnable_tasks = 1, .max_outstanding_tasks = 2 });
    defer runtime.destroy();
    var driver = try runtime.acquire(.{ .io = std.testing.io });
    defer driver.release();
    try std.testing.expect(runtime.tryAcquire() == null);
    try std.testing.expectError(error.AdmissionFull, runtime.acquire(.{ .io = std.testing.io }));
    try std.testing.expectError(error.DeadlineExceeded, runtime.acquire(.{ .io = std.testing.io, .deadline_ns = 0 }));
    try std.testing.expectEqual(@as(u64, 2), runtime.ledger.snapshot().total.handles);
}

test "workload admission dense real runtime cancels queued caller without helper bypass" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const runtime = try Runtime.create(std.testing.allocator, .{
        .max_runnable_tasks = 1,
        .max_outstanding_tasks = 3,
        .max_queued_tasks = 1,
        .max_wait_ms = 5000,
    });
    defer runtime.destroy();
    var driver = try runtime.acquire(.{ .io = io });
    defer driver.release();
    const Worker = struct {
        runtime: *Runtime,
        io: std.Io,
        cancelled: std.atomic.Value(bool) = .init(false),
        result: ?anyerror = null,
        fn run(self: *@This()) void {
            var lease = self.runtime.acquire(.{
                .io = self.io,
                .cancellation = @import("../common/cancellation.zig").CancellationToken.fromAtomic(&self.cancelled),
            }) catch |err| {
                self.result = err;
                return;
            };
            defer lease.release();
            self.result = error.UnexpectedExecution;
        }
    };
    var worker: Worker = .{ .runtime = runtime, .io = io };
    var group = std.Io.Group.init;
    defer group.cancel(io);
    try group.concurrent(io, Worker.run, .{&worker});
    const until = std.Io.Clock.now(.awake, io).nanoseconds + 5 * std.time.ns_per_s;
    while (runtime.ledger.snapshot().total.queued != 1) {
        if (std.Io.Clock.now(.awake, io).nanoseconds >= until) return error.TestUnexpectedResult;
        try io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    const service = runtime.scheduler.lanes[@intFromEnum(resources.Lane.general_read)].service;
    try std.testing.expect(runtime.tryAcquire() == null);
    try std.testing.expectEqual(service, runtime.scheduler.lanes[@intFromEnum(resources.Lane.general_read)].service);
    worker.cancelled.store(true, .release);
    try group.await(io);
    try std.testing.expectEqual(error.Canceled, worker.result.?);
    try std.testing.expectEqual(@as(u64, 0), runtime.ledger.snapshot().total.queued);
    try std.testing.expectEqual(@as(u64, 1), runtime.ledger.snapshot().total.requests);
    driver.release();
    var next = runtime.tryAcquire().?;
    next.release();
}

test "workload admission ResourceManager activates dense ownership and rolls back denied helpers" {
    const manager_mod = @import("resource_manager.zig");
    var manager = manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator, .dense_read_extra_task_limit = 0 });
    defer manager.deinit(std.testing.allocator);
    try manager.configureDenseExecution(.{ .max_runnable_tasks = 2, .max_outstanding_tasks = 3 });
    var driver = try manager.acquireDenseDriver(std.testing.io, null);
    defer driver.release();
    try std.testing.expect(manager.tryAcquireDenseReadTask() == null); // helper sublimit
    try std.testing.expectEqual(@as(u64, 1), manager.dense_execution.?.ledger.snapshot().total.runnable);
    try std.testing.expectEqual(@as(u64, 1), manager.dense_execution.?.ledger.snapshot().total.requests);
    manager.dense_read_extra_task_limit = 1;
    var helper = manager.tryAcquireDenseReadTask().?;
    try std.testing.expect(manager.tryAcquireDenseReadTask() == null);
    driver.release();
    try std.testing.expectEqual(@as(u64, 1), manager.dense_execution.?.ledger.snapshot().total.runnable);
    helper.release();
    try std.testing.expectEqual(@as(u64, 0), manager.dense_execution.?.ledger.snapshot().total.handles);
}

test "workload admission dense suspension reserves all completion handles without fresh admission" {
    const manager_mod = @import("resource_manager.zig");
    const memory_mod = @import("workload_memory.zig");
    var manager = manager_mod.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    try manager.configureDenseExecution(.{ .max_runnable_tasks = 1, .max_outstanding_tasks = 3, .max_queued_tasks = 1, .max_wait_ms = 5000, .max_working_bytes = 24, .max_suspended_io = 3 });
    const runtime = manager.dense_execution.?;
    var drivers: [3]Runtime.Lease = undefined;
    var memories: [3]memory_mod.WorkingMemory = undefined;
    var buffers: [3][]u8 = undefined;
    var io_leases: [3]resources.LocalIoLease = undefined;
    var count: usize = 0;
    defer for (0..count) |i| {
        memories[i].allocator().free(buffers[i]);
        memories[i].deinit();
        drivers[i].release();
    };
    for (0..3) |i| {
        drivers[i] = try runtime.acquire(.{ .io = std.testing.io });
        memories[i] = try memory_mod.WorkingMemory.init(&manager, .dense_search_working_set, &runtime.ledger, &drivers[i].request.?, std.testing.allocator, 0);
        buffers[i] = try memories[i].allocator().alloc(u8, 8);
        count += 1;
        io_leases[i] = (try drivers[i].suspendIo()).?;
    }
    try std.testing.expectEqual(@as(u64, 12), runtime.ledger.snapshot().total.handles);
    try std.testing.expectEqual(@as(u64, 24), runtime.stats().working_bytes);
    try std.testing.expectError(error.AdmissionFull, runtime.acquire(.{ .io = std.testing.io }));
    for (0..3) |i| {
        try drivers[i].resumeIo(&io_leases[i]);
        try std.testing.expectEqual(@as(u64, 24), runtime.stats().working_bytes);
        drivers[i].job.?.release(1);
        drivers[i].job = null;
    }
    // An unproven provider must keep runnable ownership even when suspended
    // I/O is configured, so enclosing source-session affinity stays intact.
    var unknown_vtable = std.testing.io.vtable.*;
    const unknown_io: std.Io = .{ .userdata = std.testing.io.userdata, .vtable = &unknown_vtable };
    drivers[0].options.?.io = unknown_io;
    drivers[0].job = try runtime.scheduler.acquire(&drivers[0].request.?, .general_read, .{ .runnable = 1 }, 1, 0, drivers[0].options.?);
    try std.testing.expect((try drivers[0].suspendIo()) == null);
    try std.testing.expectEqual(@as(u64, 1), runtime.stats().runnable);
    try std.testing.expectEqual(@as(u64, 0), runtime.stats().suspended_io);
}
