// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! An independently admitted background pass for observations and cold opens.
//! Its caller owns the original native deadline, including any admission wait.
//! It leaves protected health and cleanup capacity available.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const platform = @import("antfly_platform");

pub const budget_ns = 2 * std.time.ns_per_s;

pub const Pass = struct {
    owner: ?abi.Owner = null,
    handle: ?*anyopaque = null,
    borrow: ?abi.Borrowed = null,
    deadline_ns: u64,
    cancellation: runtime.http_abi.CancellationView,
    active: bool = true,

    pub fn init(owner: ?abi.Owner, io: *const std.Io, deadline_ns: u64, cancellation: runtime.http_abi.CancellationView) !Pass {
        return initWithOptions(owner, io, deadline_ns, cancellation, .{});
    }

    pub const Options = struct {
        allow_wait: bool = true,
        retained_bytes: u64 = 0,
    };

    /// Independent producers may refuse queuing so one durable worker can
    /// return to release/close debt when ordinary execution is exhausted.
    pub fn initWithOptions(owner: ?abi.Owner, io: *const std.Io, deadline_ns: u64, cancellation: runtime.http_abi.CancellationView, options: Options) !Pass {
        var self: Pass = .{ .deadline_ns = deadline_ns, .cancellation = cancellation };
        try self.check();
        const port = owner orelse return self;
        if (!port.valid()) return error.UnsupportedVersion;
        const io_borrow = runtime.native_abi.IoBorrow.init(io);
        var handle: ?*anyopaque = null;
        const status = port.start(port.context, &.{
            .io = &io_borrow,
            .class = @backingInt(abi.Class.background),
            .slots = 1,
            .has_deadline = 1,
            .native_deadline = 1,
            .deadline_ns = deadline_ns,
            .allow_wait = @intFromBool(options.allow_wait),
            .cancellation = cancellation,
            .retained_bytes = try std.math.add(u64, @sizeOf(Pass), options.retained_bytes),
            .out_handle = &handle,
        });
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        self.owner = port;
        self.handle = handle orelse return error.InvalidRunnableTransition;
        errdefer self.deinit();
        self.borrow = try abi.Borrowed.fromOwner(port, self.handle.?, 1, .background);
        try self.check();
        return self;
    }

    /// Safe inside a foreign cancellation predicate: never park or reacquire
    /// CPU while a native storage lock might be held.
    pub fn checkLifetime(self: *const Pass) !void {
        if (!self.active) return error.InvalidRunnableTransition;
        if (self.cancellation.requested()) return error.Canceled;
        if (platform.time.monotonicNs() >= self.deadline_ns) return error.Timeout;
        if (self.borrow) |borrow| try borrow.checkLifetime();
    }

    pub fn check(self: *const Pass) !void {
        try self.checkLifetime();
        if (self.borrow) |borrow| {
            const status = borrow.checkpoint(borrow.context);
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        try self.checkLifetime();
    }

    /// Call only after every synchronous observation and its children joined.
    /// Expiry/cancellation never authorize early release of physical ownership.
    pub fn deinit(self: *Pass) void {
        if (self.handle) |handle| self.owner.?.finish(handle);
        self.handle = null;
        self.borrow = null;
        self.owner = null;
        self.active = false;
    }
};

test "runnable status refresh original queued deadline refuses work before allocation" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = platform.time.monotonicNs };
    defer owner.owner.deinit();
    try std.testing.expectError(error.Timeout, Pass.init(owner.port(), &std.testing.io, 0, .{}));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    // Explicitly disabled scheduling still preserves the observational budget.
    try std.testing.expectError(error.Timeout, Pass.init(null, &std.testing.io, 0, .{}));
}

test "runnable status refresh queues fairly behind foreground while health retains its lane" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2, .max_wait_ms = 1000 }), .native_now_ns = platform.time.monotonicNs };
    defer owner.owner.deinit();
    const port = owner.port();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var foreground: ?*anyopaque = null;
    const status = port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &foreground });
    try std.testing.expect(status.isOk());
    defer if (foreground) |handle| port.finish(handle);
    const Probe = struct {
        owner: *owners.State,
        io: std.Io,
        called: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            var pass = Pass.init(self.owner.port(), &self.io, platform.time.monotonicNs() + budget_ns, .{}) catch |err| {
                self.failure = err;
                return;
            };
            defer pass.deinit();
            pass.check() catch |err| {
                self.failure = err;
                return;
            };
            self.called.store(true, .release);
        }
    };
    var probe: Probe = .{ .owner = &owner, .io = io };
    var future = try io.concurrent(Probe.run, .{&probe});
    var joined = false;
    defer {
        if (foreground) |handle| port.finish(handle);
        foreground = null;
        if (!joined) future.await(io);
    }
    const until = platform.time.monotonicNs() + std.time.ns_per_s;
    while (owner.owner.scheduler.stats().queued == 0) {
        if (platform.time.monotonicNs() >= until) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try std.testing.expect(!probe.called.load(.acquire));
    // The other bounded worker must still execute release/drain work while
    // this producer waits for ordinary admission. Neither task needs a new Io.
    const Drain = struct {
        fn run(done: *std.atomic.Value(bool)) void {
            done.store(true, .release);
        }
    };
    var drained = std.atomic.Value(bool).init(false);
    var drain = try io.concurrent(Drain.run, .{&drained});
    drain.await(io);
    try std.testing.expect(drained.load(.acquire));
    try std.testing.expect(!probe.called.load(.acquire));
    var health: ?*anyopaque = null;
    const health_status = port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health });
    try std.testing.expect(health_status.isOk());
    port.finish(health.?);
    port.finish(foreground.?);
    foreground = null;
    future.await(io);
    joined = true;
    if (probe.failure) |err| return err;
    try std.testing.expect(probe.called.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
}

test "runnable status refresh cancellation retains its capsule until physical return" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var owner: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = platform.time.monotonicNs };
    defer owner.owner.deinit();
    const Cancel = struct {
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const value: *const std.atomic.Value(bool) = @ptrCast(@alignCast(raw.?));
            return @intFromBool(value.load(.acquire));
        }
    };
    var canceled = std.atomic.Value(bool).init(false);
    var pass = try Pass.init(owner.port(), &std.testing.io, platform.time.monotonicNs() + budget_ns, .{ .context = &canceled, .is_cancelled = Cancel.requested });
    defer pass.deinit();
    canceled.store(true, .release);
    try std.testing.expectError(error.Canceled, pass.check());
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().runnable_slots);
    pass.deinit();
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectError(error.InvalidRunnableTransition, pass.check());
}

test "runnable status refresh nonwaiting producer preserves health and cleanup capacity" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var state: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = platform.time.monotonicNs };
    defer state.owner.deinit();
    const io = std.testing.io;
    const port = state.port();
    const borrowed_io = runtime.native_abi.IoBorrow.init(&io);
    var foreground: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrowed_io, .class = @backingInt(abi.Class.query), .out_handle = &foreground }).isOk());
    defer port.finish(foreground.?);
    const before = state.owner.scheduler.stats();
    const result = Pass.initWithOptions(port, &io, platform.time.monotonicNs() + budget_ns, .{}, .{ .allow_wait = false, .retained_bytes = 128 });
    if (result) |value| {
        var unexpected = value;
        unexpected.deinit();
        return error.TestUnexpectedResult;
    } else |_| {}
    try std.testing.expectEqual(before.operations, state.owner.scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, state.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().queued);
    var health: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrowed_io, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }).isOk());
    defer port.finish(health.?);
    var cleanup: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrowed_io, .class = @backingInt(abi.Class.control), .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .native_deadline = 1, .deadline_ns = platform.time.monotonicNs() + 250 * std.time.ns_per_ms, .out_handle = &cleanup }).isOk());
    defer port.finish(cleanup.?);
}

test "runnable status refresh producer accounts retained descriptor memory until physical return" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var state: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = platform.time.monotonicNs };
    defer state.owner.deinit();
    var pass = try Pass.initWithOptions(state.port(), &std.testing.io, platform.time.monotonicNs() + budget_ns, .{}, .{ .allow_wait = false, .retained_bytes = 8192 });
    defer pass.deinit();
    try std.testing.expectEqual(abi.Class.background, try pass.borrow.?.currentClass());
    try std.testing.expect(state.owner.scheduler.stats().retained_bytes >= 8192 + @sizeOf(Pass));
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    pass.deadline_ns = 0;
    try std.testing.expectError(error.Timeout, pass.check());
    // An expired logical attempt still owns its resources until physical join.
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    pass.deinit();
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), state.owner.scheduler.stats().retained_bytes);
    try std.testing.expectError(error.Overflow, Pass.initWithOptions(state.port(), &std.testing.io, platform.time.monotonicNs() + budget_ns, .{}, .{ .allow_wait = false, .retained_bytes = std.math.maxInt(u64) }));
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
}

test "runnable status refresh foreign cancellation checks lifetime without CPU reentry" {
    const Probe = struct {
        lifetime_calls: usize = 0,
        cpu_calls: usize = 0,
        closed: bool = false,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lifetime_calls += 1;
            return if (self.closed) runtime.error_abi.statusFromError(error.SchedulerClosed) else .ok;
        }
        fn cpu(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.cpu_calls += 1;
            return .ok;
        }
        fn transition(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("foreign predicate attempted CPU transition");
        }
    };
    var probe: Probe = .{};
    var pass: Pass = .{
        .deadline_ns = platform.time.monotonicNs() + budget_ns,
        .cancellation = .{},
        .borrow = .{ .context = &probe, .cpu_slots = 1, .parent_class = @backingInt(abi.Class.background), .checkpoint = Probe.cpu, .park = Probe.transition, .enter = Probe.transition, .check_lifetime = Probe.lifetime },
    };
    try pass.checkLifetime();
    try std.testing.expectEqual(@as(usize, 0), probe.cpu_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.lifetime_calls);
    probe.closed = true;
    try std.testing.expectError(error.SchedulerClosed, pass.checkLifetime());
    try std.testing.expectEqual(@as(usize, 0), probe.cpu_calls);
}
