// Copyright 2026 Antfly, Inc.
// Licensed under the Elastic License 2.0 (ELv2); see https://www.antfly.io/licensing/ELv2-license.

//! First operator binding for the common scheduler. Dense drivers retain a
//! coarse runnable lease through scan/rerank and helper join. No cooperative
//! isolation or working-memory coverage is claimed by this binding.
const std = @import("std");
const resources = @import("../common/workload_resources.zig");
const scheduling = @import("../common/workload_scheduler.zig");
const admission = @import("../common/workload_admission.zig");

pub const Config = struct {
    max_runnable_tasks: u32 = 0,
    max_outstanding_tasks: u32 = 0,
    max_queued_tasks: u32 = 0,
    max_wait_ms: u32 = 0,

    pub fn validate(self: Config) !void {
        if (self.max_runnable_tasks == 0) {
            if (self.max_outstanding_tasks != 0 or self.max_queued_tasks != 0 or self.max_wait_ms != 0) return error.InvalidConfig;
            return;
        }
        if (self.max_outstanding_tasks < self.max_runnable_tasks or self.max_outstanding_tasks > 65_536 or
            self.max_queued_tasks > self.max_outstanding_tasks or self.max_wait_ms > 60_000 or
            ((self.max_wait_ms == 0) != (self.max_queued_tasks == 0))) return error.InvalidConfig;
    }
};

pub const Stats = struct {
    max_runnable_tasks: u32 = 0,
    max_outstanding_tasks: u32 = 0,
    max_queued_tasks: u32 = 0,
    max_wait_ms: u32 = 0,
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
        request: ?resources.RequestLease = null,
        job: ?scheduling.Scheduler.Job = null,

        pub fn release(self: *Lease) void {
            if (self.job) |*job| job.release(1);
            self.job = null;
            if (self.request) |*request| request.release() catch unreachable;
            self.request = null;
        }
    };

    pub fn create(allocator: std.mem.Allocator, config: Config) !*Runtime {
        try config.validate();
        if (config.max_runnable_tasks == 0) return error.InvalidConfig;
        const self = try allocator.create(Runtime);
        errdefer allocator.destroy(self);
        // One request handle plus one runnable/queued handle per task. Fixed
        // preallocation bounds scheduler metadata before publishing the owner.
        const total: resources.Bundle = .{
            .handles = @as(u64, config.max_outstanding_tasks) * 2,
            .requests = config.max_outstanding_tasks,
            .queued = config.max_queued_tasks,
            .runnable = config.max_runnable_tasks,
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
            .runnable = snapshot.total.runnable,
            .outstanding = snapshot.total.requests,
            .queued = snapshot.total.queued,
        };
    }

    pub fn acquire(self: *Runtime, options: admission.Options) !Lease {
        try options.check();
        var request = self.ledger.admit(.general_read, 0) catch |err| return mapError(err);
        errdefer request.release() catch unreachable;
        const job = self.scheduler.acquire(&request, .general_read, .{ .runnable = 1 }, 1, 0, options) catch |err| return mapError(err);
        return .{ .request = request, .job = job };
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
