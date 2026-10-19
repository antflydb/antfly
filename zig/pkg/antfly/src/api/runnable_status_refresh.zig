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

//! An independently admitted observational pass. Its original native deadline
//! starts before durable-lane queuing; coalesced wakes cannot renew it. It uses
//! ordinary background capacity, leaving the protected control lane available.
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
            .allow_wait = 1,
            .cancellation = cancellation,
            .retained_bytes = @sizeOf(Pass),
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

    pub fn check(self: *const Pass) !void {
        if (!self.active) return error.InvalidRunnableTransition;
        if (self.cancellation.requested()) return error.Canceled;
        if (platform.time.monotonicNs() >= self.deadline_ns) return error.Timeout;
        if (self.borrow) |borrow| {
            const status = borrow.checkpoint(borrow.context);
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
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
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(8) });
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
