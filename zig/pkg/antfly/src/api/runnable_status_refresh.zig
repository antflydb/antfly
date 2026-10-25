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

//! An independently admitted background pass for observations and cold opens.
//! Its caller owns the original native deadline, including any admission wait.
//! It leaves protected health and cleanup capacity available.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const platform = @import("antfly_platform");
const admission = @import("antfly_local_sources").common_read_admission;

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

    /// Borrow only while this exact pass remains alive at its current address.
    /// The scope and its payload stay on the synchronous caller's stack; a
    /// retained storage continuation must never retain either of them.
    pub fn admissionScope(self: *const Pass, io: std.Io) !AdmissionScope {
        try self.check();
        const borrow = self.borrow orelse return error.ExecutionWaitUnsupported;
        try borrow.validate();
        if (borrow.check_lifetime == null or borrow.enter_checked == null or borrow.current_class == null) return error.ExecutionWaitUnsupported;
        _ = try borrow.currentClass();
        return .{ .pass = self, .borrow = borrow, .io = io, .deadline_ns = self.deadline_ns };
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

/// One synchronous storage call under an already admitted background pass.
/// Physical work must join or transfer to its registered owner before the
/// caller retires the pass. Rejection never releases that physical ownership.
pub const AdmissionScope = struct {
    pass: *const Pass,
    borrow: abi.Borrowed,
    io: std.Io,
    deadline_ns: u64,
    rejection: ?anyerror = null,
    parked_class: ?abi.Class = null,

    pub fn payload(self: *AdmissionScope) admission.Payload {
        return .{
            .ptr = self,
            .vtable = &.{ .checkpoint = checkpoint, .check_lifetime = lifetime, .deadline_ns = deadline, .wait_for_progress = wait },
            .execution_wait = .{ .ptr = self, .begin = begin, .end = end },
        };
    }
    fn cast(raw: *anyopaque) *AdmissionScope {
        return @ptrCast(@alignCast(raw));
    }
    fn reject(self: *AdmissionScope, cause: anyerror) anyerror {
        if (self.rejection == null) self.rejection = cause;
        return self.rejection.?;
    }
    fn checkLifetime(self: *AdmissionScope) !void {
        if (self.rejection) |cause| return cause;
        // Check active before touching the copied capsule borrow: finish may
        // already have freed that capsule. All methods serialize with finish.
        self.pass.checkLifetime() catch |cause| return self.reject(cause);
        if (platform.time.monotonicNs() >= self.deadline_ns) return self.reject(error.Timeout);
        if (self.pass.handle != self.borrow.context or self.pass.borrow == null or self.pass.borrow.?.context != self.borrow.context) return self.reject(error.InvalidRunnableTransition);
    }
    fn lifetime(raw: *anyopaque) !void {
        try cast(raw).checkLifetime();
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self = cast(raw);
        try self.checkLifetime();
        // enterChecked invokes its restriction before and after grant. It
        // must inspect lifetime without recursively acquiring or yielding CPU.
        if (self.parked_class == null) {
            checked(self.borrow.checkpoint(self.borrow.context)) catch |cause| return self.reject(cause);
        }
        try self.checkLifetime();
    }
    fn deadline(raw: *anyopaque) !?u64 {
        const self = cast(raw);
        try self.checkLifetime();
        return self.deadline_ns;
    }
    fn begin(raw: *anyopaque) !void {
        const self = cast(raw);
        try checkpoint(raw);
        if (self.parked_class != null) return self.reject(error.InvalidRunnableTransition);
        const phase = self.borrow.currentClass() catch |cause| return self.reject(cause);
        checked(self.borrow.park(self.borrow.context, @backingInt(abi.Wait.resource))) catch |cause| return self.reject(cause);
        self.parked_class = phase;
    }
    fn end(raw: *anyopaque) !void {
        const self = cast(raw);
        try self.checkLifetime();
        const phase = self.parked_class orelse return self.reject(error.InvalidRunnableTransition);
        self.borrow.enterChecked(phase, &.{ .context = self, .check = reentry }) catch |cause| return self.reject(cause);
        self.parked_class = null;
        try checkpoint(raw);
    }
    fn reentry(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        checkpoint(raw) catch |cause| return runtime.error_abi.statusFromError(cause);
        return .ok;
    }
    fn wait(raw: *anyopaque) !void {
        const self = cast(raw);
        const boundary = @import("antfly_local_sources").common_execution_wait.Boundary{ .ptr = self, .begin = begin, .end = end };
        boundary.run(std.Io.sleep, .{ self.io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake }) catch |cause| return self.reject(cause);
    }
    fn checked(status: runtime.error_abi.Status) !void {
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
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

test "runnable status refresh maintenance scope parks one capsule for foreground and protected progress" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var state: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = platform.time.monotonicNs };
    defer state.owner.deinit();
    const io = std.testing.io;
    const owner = state.port();
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    const original_deadline = platform.time.monotonicNs() + budget_ns;
    var pass = try Pass.initWithOptions(owner, &io, original_deadline, .{}, .{ .allow_wait = false, .retained_bytes = 37 });
    defer pass.deinit();
    const identity = pass.handle;
    var scope = try pass.admissionScope(io);
    const payload = scope.payload();
    const original = admission.Port.init(&payload);
    try std.testing.expectEqual(original_deadline, (try original.deadlineNs()).?);
    const wait_boundary = (try original.executionWait()).?;
    try wait_boundary.beginWait();
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    var foreground: ?*anyopaque = null;
    try AdmissionScope.checked(owner.start(owner.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .allow_wait = 0, .out_handle = &foreground }));
    defer if (foreground) |held| owner.finish(held);
    var health: ?*anyopaque = null;
    try AdmissionScope.checked(owner.start(owner.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }));
    defer if (health) |held| owner.finish(held);
    var cleanup: ?*anyopaque = null;
    try AdmissionScope.checked(owner.start(owner.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .native_deadline = 1, .deadline_ns = platform.time.monotonicNs() + 250 * std.time.ns_per_ms, .out_handle = &cleanup }));
    defer if (cleanup) |held| owner.finish(held);
    try original.checkLifetime();
    try std.testing.expectEqual(@as(usize, 4), state.owner.scheduler.stats().operations);
    owner.finish(cleanup.?);
    cleanup = null;
    owner.finish(health.?);
    health = null;
    owner.finish(foreground.?);
    foreground = null;
    try wait_boundary.endWait();
    try std.testing.expectEqual(identity, pass.handle);
    try std.testing.expectEqual(abi.Class.background, try pass.borrow.?.currentClass());
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    try std.testing.expectEqual(original_deadline, (try original.deadlineNs()).?);
    pass.deinit();
    try std.testing.expectError(error.InvalidRunnableTransition, original.checkpoint());
    try std.testing.expectError(error.InvalidRunnableTransition, wait_boundary.beginWait());
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
}

test "runnable status refresh maintenance scope preserves original expiry cancellation and shutdown while parked" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Clock = struct {
        var ns: u64 = 0;
        fn now() u64 {
            return ns;
        }
    };
    const Cancel = struct {
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            return @intFromBool(@as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw.?))).load(.acquire));
        }
    };
    for ([_]anyerror{ error.DeadlineExceeded, error.Canceled, error.SchedulerClosed }) |expected| {
        Clock.ns = platform.time.monotonicNs();
        const original_deadline = Clock.ns + budget_ns;
        var flag = std.atomic.Value(bool).init(false);
        var state: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = Clock.now };
        defer state.owner.deinit();
        var pass = try Pass.init(state.port(), &std.testing.io, original_deadline, .{ .context = &flag, .is_cancelled = Cancel.requested });
        defer pass.deinit();
        var scope = try pass.admissionScope(std.testing.io);
        const payload = scope.payload();
        const original = admission.Port.init(&payload);
        const wait_boundary = (try original.executionWait()).?;
        try wait_boundary.beginWait();
        const retained = state.owner.scheduler.stats().retained_bytes;
        switch (expected) {
            error.DeadlineExceeded => Clock.ns = original_deadline,
            error.Canceled => flag.store(true, .release),
            error.SchedulerClosed => state.owner.close(),
            else => unreachable,
        }
        try std.testing.expectError(expected, wait_boundary.endWait());
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(retained, state.owner.scheduler.stats().retained_bytes);
        Clock.ns = original_deadline - 1;
        flag.store(false, .release);
        // Restoring external conditions cannot renew this failed invocation.
        try std.testing.expectError(expected, original.checkpoint());
        try std.testing.expectError(expected, original.deadlineNs());
        pass.deinit();
        try std.testing.expectError(expected, wait_boundary.endWait());
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), state.owner.scheduler.stats().retained_bytes);
    }
}

test "runnable status refresh maintenance scope requires an admitted checked owner" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    var observational = try Pass.init(null, &std.testing.io, platform.time.monotonicNs() + budget_ns, .{});
    defer observational.deinit();
    try std.testing.expectError(error.ExecutionWaitUnsupported, observational.admissionScope(std.testing.io));
    var state: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = platform.time.monotonicNs };
    defer state.owner.deinit();
    var pass = try Pass.init(state.port(), &std.testing.io, platform.time.monotonicNs() + budget_ns, .{});
    defer pass.deinit();
    pass.borrow.?.enter_checked = null;
    try std.testing.expectError(error.ExecutionWaitUnsupported, pass.admissionScope(std.testing.io));
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    pass.deinit();
    try std.testing.expectError(error.InvalidRunnableTransition, pass.admissionScope(std.testing.io));
}

test "runnable status refresh maintenance scope expires during queued checked reentry" {
    const owners = @import("runnable_owner_execution.zig");
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Clock = struct {
        var ns: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return ns.load(.acquire);
        }
    };
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(4) });
    defer threaded.deinit();
    const io = threaded.io();
    Clock.ns.store(platform.time.monotonicNs(), .release);
    const original_deadline = Clock.now() + budget_ns;
    var state: owners.State = .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_wait_ms = 1000 }), .native_now_ns = Clock.now };
    defer state.owner.deinit();
    const owner = state.port();
    var pass = try Pass.initWithOptions(owner, &io, original_deadline, .{}, .{ .retained_bytes = 37 });
    defer pass.deinit();
    var scope = try pass.admissionScope(io);
    const payload = scope.payload();
    const original = admission.Port.init(&payload);
    const boundary = (try original.executionWait()).?;
    try boundary.beginWait();
    const borrowed_io = runtime.native_abi.IoBorrow.init(&io);
    var foreground: ?*anyopaque = null;
    try AdmissionScope.checked(owner.start(owner.context, &.{ .io = &borrowed_io, .class = @backingInt(abi.Class.query), .allow_wait = 0, .out_handle = &foreground }));
    defer if (foreground) |held| owner.finish(held);
    const Worker = struct {
        boundary: @import("antfly_local_sources").common_execution_wait.Boundary,
        io: std.Io,
        done: std.Io.Event = .unset,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            defer self.done.set(self.io);
            self.boundary.endWait() catch |cause| {
                self.failure = cause;
            };
        }
    };
    var worker: Worker = .{ .boundary = boundary, .io = io };
    var future = try io.concurrent(Worker.run, .{&worker});
    var joined = false;
    defer {
        // Even a failed bounded observation releases the peer before joining.
        if (foreground) |held| owner.finish(held);
        foreground = null;
        if (!joined) future.await(io);
    }
    const observe_until = platform.time.monotonicNs() + std.time.ns_per_s;
    while (state.owner.scheduler.stats().queued == 0) {
        if (platform.time.monotonicNs() >= observe_until) return error.TestUnexpectedResult;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    const held = state.owner.scheduler.stats();
    try std.testing.expectEqual(@as(usize, 2), held.operations);
    try std.testing.expectEqual(@as(usize, 1), held.runnable_slots);
    Clock.ns.store(original_deadline, .release);
    try worker.done.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    future.await(io);
    joined = true;
    try std.testing.expectEqual(error.DeadlineExceeded, worker.failure.?);
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().queued);
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().operations);
    try std.testing.expectEqual(held.retained_bytes, state.owner.scheduler.stats().retained_bytes);
    try std.testing.expectError(error.DeadlineExceeded, original.checkpoint());
    pass.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    owner.finish(foreground.?);
    foreground = null;
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
}
