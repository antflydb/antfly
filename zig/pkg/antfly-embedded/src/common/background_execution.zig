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
const admission = @import("read_admission.zig");

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
        cleanup: bool = false,
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
            .class = @backingInt(if (options.cleanup) abi.Class.control else abi.Class.background),
            .cleanup = @intFromBool(options.cleanup),
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
        self.borrow = try abi.Borrowed.fromOwner(port, self.handle.?, 1, if (options.cleanup) .control else .background);
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

    /// Non-yielding restriction for synchronous storage calls holding guards.
    pub fn lifetime(self: *const Pass) abi.Reentry {
        return .{ .context = @ptrCast(@constCast(self)), .check = checkOriginalLifetime };
    }

    fn checkOriginalLifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self: *const Pass = @ptrCast(@alignCast(raw));
        self.checkLifetime() catch |cause| return runtime.error_abi.statusFromError(cause);
        return .ok;
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
        const boundary = @import("execution_wait.zig").Boundary{ .ptr = self, .begin = begin, .end = end };
        boundary.run(std.Io.sleep, .{ self.io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake }) catch |cause| return self.reject(cause);
    }
    pub fn checked(status: runtime.error_abi.Status) !void {
        if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    }
};
