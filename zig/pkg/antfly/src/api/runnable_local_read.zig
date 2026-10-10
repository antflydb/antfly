// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Read capability on an ingress-owned execution. Keep this adapter, its
//! allocator and the original execution alive until every physical child joins.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub const Read = struct {
    execution: *native.Execution,
    allocator: std.mem.Allocator,

    pub fn team(self: *Read) !abi.Team {
        const entry = self.execution.entry orelse return error.InvalidRunnableTransition;
        if (entry.state != .running or entry.children != 0) return error.InvalidRunnableTransition;
        return .{
            .borrow = .{
                .context = self,
                .cpu_slots = entry.slots,
                .parent_class = @intCast(@backingInt(try self.execution.retainedClass())),
                .checkpoint = checkpoint,
                .park = park,
                .enter = enter,
                .enter_checked = enterChecked,
                .reserve_child = reserveChild,
                .check_lifetime = lifetime,
                .current_class = currentClass,
            },
            .maximum_slots = entry.slots,
            .allocator = self.allocator,
        };
    }

    fn from(raw: *anyopaque) *Read {
        return @ptrCast(@alignCast(raw));
    }
    fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        from(raw).execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
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
    fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
        const wait = std.enums.fromInt(abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        from(raw).execution.park(if (wait == .resource) .resource_blocked else .output_blocked) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
        const phase = std.enums.fromInt(native.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        from(raw).execution.enter(phase) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enterChecked(raw: *anyopaque, class: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const phase = std.enums.fromInt(native.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        @import("runnable_reentry.zig").enter(from(raw).execution, phase, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn reserveChild(raw: *anyopaque, options: *const abi.ChildStart) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        @import("runnable_local_child.zig").reserve(self.execution, self.allocator, options) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
};
