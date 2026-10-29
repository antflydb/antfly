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

//! Read capability on an ingress-owned execution. Keep this adapter, its
//! allocator and the original execution alive until every physical child joins.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub const Read = struct {
    execution: *native.Execution,
    allocator: std.mem.Allocator,
    validation: ?@import("runnable_read_validation.zig").Hook = null,

    pub fn team(self: *Read) !abi.Team {
        if (self.execution.entry) |entry| {
            if (entry.state != .running or entry.children != 0) return error.InvalidRunnableTransition;
        } else if (self.execution.borrowed != null) {
            try self.execution.checkpoint();
        } else return error.InvalidRunnableTransition;
        return .{ .borrow = try self.borrowed(), .maximum_slots = try self.execution.retainedSlots(), .allocator = self.allocator };
    }
    fn borrowed(self: *Read) !abi.Borrowed {
        return .{
            .context = self,
            .cpu_slots = try self.execution.retainedSlots(),
            .parent_class = @intCast(@backingInt(try self.execution.retainedClass())),
            .checkpoint = checkpoint,
            .park = park,
            .enter = enter,
            .enter_checked = enterChecked,
            .reserve_child = reserveChild,
            .check_lifetime = lifetime,
            .current_class = currentClass,
            .cpu_borrow = cpuBorrow,
            .park_publication = parkPublication,
            .resume_publication = resumePublication,
            .finish_publication = finishPublication,
            .request_resources = requestResources,
            .enter_resources = enterResources,
        };
    }
    pub fn validateCurrent(self: *Read) !void {
        if (self.validation) |*hook| try hook.ensure(self.execution, try self.borrowed());
    }
    fn precheck(self: *Read) !void {
        if (self.validation) |*hook| try hook.precheck();
    }

    fn from(raw: *anyopaque) *Read {
        return @ptrCast(@alignCast(raw));
    }
    fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn parkPublication(raw: *anyopaque, out: *u8) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |cause| return runtime.error_abi.statusFromError(cause);
        return @import("runnable_publication.zig").park(self.execution, out);
    }
    fn resumePublication(raw: *anyopaque, restriction: ?*const abi.Reentry) callconv(.c) runtime.error_abi.Status {
        return @import("runnable_publication.zig").reenter(from(raw).execution, restriction);
    }
    fn finishPublication(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        const generation = self.execution.resume_generation;
        const result = @import("runnable_publication.zig").finish(self.execution);
        if (!result.isOk()) return result;
        if (generation != self.execution.resume_generation) self.validateCurrent() catch |cause| return runtime.error_abi.statusFromError(cause);
        return .ok;
    }

    fn cpuBorrow(raw: *anyopaque, options: *const abi.CpuBorrow) callconv(.c) runtime.error_abi.Status {
        options.validate() catch |err| return runtime.error_abi.statusFromError(err);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        if (options.enabled != 0) {
            self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
            self.execution.enableCpuBorrow(.{ .max_non_yield_ns = options.max_non_yield_ns }) catch |err| return runtime.error_abi.statusFromError(err);
        } else {
            self.execution.disableCpuBorrow() catch |err| return runtime.error_abi.statusFromError(err);
            self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        }
        return .ok;
    }
    fn enterResources(raw: *anyopaque, resources: *const abi.ResourceBundle, restriction: ?*const abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const participant = @import("runnable_resource_bridge.zig").receive(resources) catch |err| return runtime.error_abi.statusFromError(err);
        if (restriction) |check| check.validate() catch |err| return runtime.error_abi.statusFromError(err);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_reentry.zig").enterWithResources(self.execution, participant, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        const generation = self.execution.resume_generation;
        const validation_needed = if (self.validation) |hook| !hook.active and hook.last_generation != generation else false;
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        if (restriction) |check| if (validation_needed or generation != self.execution.resume_generation) check.checkpoint() catch |err| {
            if (self.validation) |*hook| hook.reject(self.execution, err);
            return runtime.error_abi.statusFromError(err);
        };
        return .ok;
    }
    fn requestResources(raw: *anyopaque, resources: *const abi.ResourceBundle) callconv(.c) runtime.error_abi.Status {
        const participant = @import("runnable_resource_bridge.zig").receive(resources) catch |err| return runtime.error_abi.statusFromError(err);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.requestResources(participant) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
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
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.enter(phase) catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enterChecked(raw: *anyopaque, class: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const phase = std.enums.fromInt(native.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_reentry.zig").enter(self.execution, phase, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        const generation = self.execution.resume_generation;
        const validation_needed = if (self.validation) |hook| !hook.active and hook.last_generation != generation else false;
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        // Full policy validation can outlive the caller's restriction even
        // without a scheduling yield or a change in resume generation.
        if (validation_needed or generation != self.execution.resume_generation) restriction.checkpoint() catch |err| {
            if (self.validation) |*hook| hook.reject(self.execution, err);
            return runtime.error_abi.statusFromError(err);
        };
        return .ok;
    }
    fn reserveChild(raw: *anyopaque, options: *const abi.ChildStart) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_local_child.zig").reserveValidated(self.execution, self.allocator, options, self.validation) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
};
