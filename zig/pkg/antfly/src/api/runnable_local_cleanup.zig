// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const abi = runtime.runnable_abi;

/// Stable local cleanup receiver. The owning ingress must retain this address,
/// execution, allocator and every physical resource through all cleanup joins.
/// Reservation grants no foreground suspension and activation preserves the
/// original scheduling class. Failed activation never retires physical ownership.
pub const Cleanup = struct {
    execution: *scheduler.Execution,
    io: ?runtime.io_abi.Receiver = null,
    clock_io: ?runtime.io_abi.Receiver = null,

    pub fn init(execution: *scheduler.Execution) !Cleanup {
        const entry = execution.entry orelse return error.ExecutionWaitUnsupported;
        if (entry.scheduler == null or entry.state != .running or entry.cleanup_active or entry.completion_active) return error.InvalidRunnableTransition;
        return .{ .execution = execution };
    }

    pub fn borrowed(self: *Cleanup) !abi.Borrowed {
        const entry = self.execution.entry orelse return error.InvalidRunnableTransition;
        return .{ .context = self, .cpu_slots = entry.slots, .parent_class = @intCast(@backingInt(entry.class)), .checkpoint = check, .park = park, .enter = enter, .enter_checked = enterChecked, .reserve_cleanup = reserve, .activate_cleanup = activate, .check_lifetime = lifetime, .current_class = currentClass };
    }

    fn from(raw: *anyopaque) *Cleanup {
        return @ptrCast(@alignCast(raw));
    }
    fn active(self: *Cleanup) !void {
        const entry = self.execution.entry orelse return error.InvalidRunnableTransition;
        if (!entry.cleanup_active) return error.ExecutionWaitUnsupported;
    }
    pub fn reserveChecked(self: *Cleanup, options: *const abi.Cleanup) !void {
        try options.validate();
        if (self.io != null) return error.InvalidRunnableTransition;
        const entry = self.execution.entry orelse return error.InvalidRunnableTransition;
        const native_clock = if (options.native_deadline != 0) entry.options.native_now_ns orelse return error.InvalidArgument else null;
        self.io = try options.io.receive();
        errdefer {
            self.io = null;
            self.clock_io = null;
        }
        self.clock_io = if (options.clock_io) |borrow| try borrow.receive() else null;
        const policy = @import("antfly_local_sources").common_request_admission.bounded_admission.Options{
            .io = self.io.?.io(),
            .clock_io = if (self.clock_io) |*clock| clock.io() else null,
            .native_now_ns = native_clock,
            .deadline_ns = if (options.activation_budget == 0) options.deadline_ns else null,
            .allow_wait = true,
        };
        if (options.activation_budget != 0) try self.execution.reserveCleanupFor(policy, options.deadline_ns) else try self.execution.reserveCleanup(policy);
    }
    fn reserve(raw: *anyopaque, options: *const abi.Cleanup) callconv(.c) runtime.error_abi.Status {
        from(raw).reserveChecked(options) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn activate(raw: *anyopaque, out_slots: *usize) callconv(.c) runtime.error_abi.Status {
        from(raw).execution.activateCleanup() catch |err| return runtime.error_abi.statusFromError(err);
        out_slots.* = from(raw).execution.entry.?.slots;
        return .ok;
    }
    fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        from(raw).execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
        out.* = @intCast(@backingInt(from(raw).execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err)));
        return .ok;
    }
    fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.active() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.active() catch |err| return runtime.error_abi.statusFromError(err);
        const wait = std.enums.fromInt(abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        self.execution.park(if (wait == .resource) .resource_blocked else .output_blocked) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enter(raw: *anyopaque, next: c_int) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.active() catch |err| return runtime.error_abi.statusFromError(err);
        const class = std.enums.fromInt(scheduler.Class, next) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        self.execution.enter(class) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enterChecked(raw: *anyopaque, next: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.active() catch |err| return runtime.error_abi.statusFromError(err);
        const class = std.enums.fromInt(scheduler.Class, next) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        @import("runnable_reentry.zig").enter(self.execution, class, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
};
