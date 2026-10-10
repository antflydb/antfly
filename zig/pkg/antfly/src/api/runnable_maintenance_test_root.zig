// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Actual recurring passes through the API kernel's opaque execution capsules.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const runnable = runtime.runnable_abi;
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const owners = @import("runnable_owner_execution.zig");
const maintenance = @import("antfly_local_sources").common_maintenance_scheduler.Scheduler;
fn now() u64 {
    return 0;
}
fn checked(status: runtime.error_abi.Status) !void {
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}
fn start(owner: runnable.Owner, io: std.Io, class: runnable.Class) !*anyopaque {
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try checked(owner.start(owner.context, &.{ .io = &io_borrow, .class = @backingInt(class), .retained_bytes = 17, .allow_wait = 0, .out_handle = &handle }));
    return handle orelse error.InvalidRunnableTransition;
}
const Probe = struct {
    io: std.Io,
    owner: *owners.State,
    ready: std.Io.Event = .unset,
    go: std.Io.Event = .unset,
    done: std.Io.Event = .unset,
    borrow: ?runnable.Borrowed = null,
    calls: std.atomic.Value(usize) = .init(0),
    failure: ?anyerror = null,
    reject_authority: bool = false,
    swallow_error: bool = false,
    fn authority(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return if (self.reject_authority) runtime.error_abi.statusFromError(error.Forbidden) else .ok;
    }
    fn step(self: *@This(), borrow: runnable.Borrowed) anyerror!?u64 {
        defer self.done.set(self.io);
        try std.testing.expectEqual(@as(usize, 1), borrow.cpu_slots);
        try std.testing.expectEqual(runnable.Class.background, try borrow.currentClass());
        try std.testing.expectEqual(@as(usize, 1), self.owner.owner.scheduler.stats().runnable_slots);
        _ = self.calls.fetchAdd(1, .release);
        self.borrow = borrow;
        defer self.borrow = null;
        try checked(borrow.park(borrow.context, @backingInt(runnable.Wait.resource)));
        self.ready.set(self.io);
        self.go.waitUncancelable(self.io);
        borrow.enterChecked(.background, &.{ .context = self, .check = authority }) catch |err| {
            self.failure = err;
            if (self.swallow_error) return null;
            return err;
        };
        try std.testing.expectEqual(runnable.Class.background, try borrow.currentClass());
        return null;
    }
};
fn waitEvent(event: *std.Io.Event, io: std.Io) !void {
    try event.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
}

test "background execution recurring pass releases CPU to foreground and retains one original capsule" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2, .max_wait_ms = 500 }), .native_now_ns = now };
    defer owner.owner.deinit();
    const port = owner.port();
    var foreground: ?*anyopaque = try start(port, io, .query);
    defer if (foreground) |handle| port.finish(handle);
    const scheduler = try maintenance.createWithRunnableOwner(std.testing.allocator, io, 4, port);
    defer scheduler.destroy();
    var probe: Probe = .{ .io = io, .owner = &owner };
    var task = try scheduler.registerRunnableClass(.maintenance, .{ .budget_ns = std.time.ns_per_s, .retained_bytes = 37 }, &probe, Probe.step);
    var joined = false;
    defer {
        probe.go.set(io);
        if (!joined) task.await(io);
    }
    const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(1));
    while (scheduler.snapshot().runnable_rejections == 0) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 0), probe.calls.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().queued);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    const health = try start(port, io, .control);
    port.finish(health);
    port.finish(foreground.?);
    foreground = null;
    try waitEvent(&probe.ready, io);
    const blocked = owner.owner.scheduler.stats();
    try std.testing.expectEqual(@as(usize, 1), blocked.operations);
    try std.testing.expectEqual(@as(usize, 0), blocked.runnable_slots);
    try std.testing.expect(blocked.retained_bytes >= 37);
    foreground = try start(port, io, .query);
    probe.go.set(io);
    const reentry_until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(1));
    while (owner.owner.scheduler.stats().queued == 0) {
        if (std.Io.Clock.awake.now(io).nanoseconds >= reentry_until.nanoseconds) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expectEqual(@as(usize, 2), owner.owner.scheduler.stats().operations);
    port.finish(foreground.?);
    foreground = null;
    try waitEvent(&probe.done, io);
    task.await(io);
    joined = true;
    try std.testing.expect(probe.failure == null);
    try std.testing.expectEqual(@as(usize, 1), probe.calls.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), scheduler.snapshot().runnable_admissions);
    try std.testing.expectEqual(@as(u64, 0), scheduler.snapshot().runnable_failures);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
}

test "background execution recurring reentry preserves deadline shutdown and authority failures" {
    const Case = enum { deadline, shutdown, authority };
    for (std.enums.values(Case)) |case| {
        var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
        defer threaded.deinit();
        const io = threaded.io();
        var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = now };
        defer owner.owner.deinit();
        const scheduler = try maintenance.createWithRunnableOwner(std.testing.allocator, io, 4, owner.port());
        defer scheduler.destroy();
        var probe: Probe = .{ .io = io, .owner = &owner, .swallow_error = true };
        var task = try scheduler.registerRunnableClass(.derived, .{ .budget_ns = if (case == .deadline) 100 * std.time.ns_per_ms else std.time.ns_per_s }, &probe, Probe.step);
        var joined = false;
        defer {
            probe.go.set(io);
            if (!joined) task.await(io);
        }
        try waitEvent(&probe.ready, io);
        const expected = switch (case) {
            .deadline => error.DeadlineExceeded,
            .shutdown => error.SchedulerClosed,
            .authority => error.Forbidden,
        };
        switch (case) {
            .deadline => {
                const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(1));
                while (true) {
                    probe.borrow.?.checkLifetime() catch |err| {
                        try std.testing.expectEqual(expected, err);
                        break;
                    };
                    if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.TestUnexpectedResult;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
            },
            .shutdown => owner.owner.close(),
            .authority => probe.reject_authority = true,
        }
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
        probe.go.set(io);
        try waitEvent(&probe.done, io);
        task.await(io);
        joined = true;
        try std.testing.expectEqual(expected, probe.failure.?);
        // The wrapper's final fence catches a callback swallowing its failure.
        try std.testing.expectEqual(@as(u64, 1), scheduler.snapshot().runnable_failures);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    }
}

test "background execution recurring close keeps the original owner through physical callback join" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
    defer threaded.deinit();
    const io = threaded.io();
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = now };
    defer owner.owner.deinit();
    const scheduler = try maintenance.createWithRunnableOwner(std.testing.allocator, io, 4, owner.port());
    defer scheduler.destroy();
    var probe: Probe = .{ .io = io, .owner = &owner };
    var task = try scheduler.registerRunnableClass(.propagation, .{ .budget_ns = std.time.ns_per_s }, &probe, Probe.step);
    var joined = false;
    defer {
        probe.go.set(io);
        if (!joined) task.await(io);
    }
    try waitEvent(&probe.ready, io);
    scheduler.closeAdmission();
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 2), owner.owner.borrowers);
    try std.testing.expectError(error.BackendRuntimeShuttingDown, scheduler.registerRunnableClass(.maintenance, .{ .budget_ns = std.time.ns_per_s }, &probe, Probe.step));
    probe.go.set(io);
    try waitEvent(&probe.done, io);
    task.await(io);
    joined = true;
    try std.testing.expectEqual(error.Canceled, probe.failure.?);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
}

test "background execution recurring registration rejects missing owner and invalid pass without work" {
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    const scheduler = try maintenance.create(std.testing.allocator, io, 1);
    defer scheduler.destroy();
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = now };
    defer owner.owner.deinit();
    var probe: Probe = .{ .io = io, .owner = &owner };
    try std.testing.expectError(error.ExecutionWaitUnsupported, scheduler.registerRunnableClass(.maintenance, .{ .budget_ns = 1 }, &probe, Probe.step));
    try std.testing.expectError(error.InvalidArgument, scheduler.registerRunnableClass(.maintenance, .{ .budget_ns = 0 }, &probe, Probe.step));
    var bad = owner.port();
    bad.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, maintenance.createWithRunnableOwner(std.testing.allocator, io, 1, bad));
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, maintenance.createWithRunnableOwner(failing.allocator(), io, 1, owner.port()));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.borrowers);
    try std.testing.expectEqual(@as(usize, 0), scheduler.snapshot().registrations);
    try std.testing.expectEqual(@as(usize, 0), probe.calls.load(.acquire));
}
