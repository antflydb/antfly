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

//! SQL write capability on one already admitted execution. The caller supplies
//! a stable concurrent bounded allocator, and retains it through all joins.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub fn init(execution: *scheduler.Execution, allocator: std.mem.Allocator) !runtime.runnable_abi.Team {
    return initTeam(execution, allocator);
}

fn initTeam(execution: *scheduler.Execution, allocator: std.mem.Allocator) !runtime.runnable_abi.Team {
    const entry = execution.entry orelse return error.InvalidRunnableTransition;
    const owner = entry.scheduler orelse return error.InvalidRunnableTransition;
    if (entry.state != .running or (entry.class != .write and entry.class != .control)) return error.InvalidRunnableTransition;
    if (entry.class == .control and entry.slots != 1) return error.InvalidRunnableDemand;
    return .{
        .borrow = .{
            .context = execution,
            .cpu_slots = entry.slots,
            .parent_class = @intCast(@backingInt(entry.class)),
            .checkpoint = unsupportedCheck,
            .park = unsupportedPhase,
            .enter = unsupportedPhase,
            .resize_team = resize,
            .check_lifetime = lifetime,
            .current_class = currentClass,
            .park_publication = @import("runnable_publication.zig").park,
            .resume_publication = @import("runnable_publication.zig").reenter,
            .finish_publication = @import("runnable_publication.zig").finish,
        },
        // Protected terminal SQL work must leave the health reserve available.
        .maximum_slots = if (entry.class == .control) 1 else owner.config.generalSlots(),
        .allocator = allocator,
    };
}

fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    const execution: *scheduler.Execution = @ptrCast(@alignCast(raw));
    execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
    const execution: *scheduler.Execution = @ptrCast(@alignCast(raw));
    out.* = @intCast(@backingInt(execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err)));
    return .ok;
}

fn unsupportedCheck(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
    return runtime.error_abi.statusFromError(error.ExecutionWaitUnsupported);
}

fn unsupportedPhase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
    // Accepted writes still need independent completion authority before they
    // can release CPU across waits, cancellation or scheduler shutdown.
    return runtime.error_abi.statusFromError(error.ExecutionWaitUnsupported);
}

fn resize(raw: *anyopaque, slots: usize) callconv(.c) runtime.error_abi.Status {
    const execution: *scheduler.Execution = @ptrCast(@alignCast(raw));
    const entry = execution.entry orelse return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
    if (entry.class == .control and slots != 1) return runtime.error_abi.statusFromError(error.InvalidRunnableDemand);
    execution.resizeTeam(slots) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}

/// Stable ingress-owned bridge for a local write/control Execution. Construct
/// before borrowing team(), and retain this address through the execution's
/// owning finish. All children must be quiescent before whole-team transitions.
/// Reservation alone grants no suspension; only activated completion does.
pub const Completion = struct {
    execution: *scheduler.Execution,
    allocator: std.mem.Allocator,
    io: ?runtime.io_abi.Receiver = null,
    clock_io: ?runtime.io_abi.Receiver = null,

    pub fn init(execution: *scheduler.Execution, allocator: std.mem.Allocator) !Completion {
        _ = try initTeam(execution, allocator);
        return .{ .execution = execution, .allocator = allocator };
    }

    pub fn team(self: *Completion) !runtime.runnable_abi.Team {
        var value = try initTeam(self.execution, self.allocator);
        value.borrow.context = self;
        value.borrow.resize_team = resizeTeam;
        value.borrow.reserve_completion = reserve;
        value.borrow.activate_completion = activate;
        value.borrow.completion_remaining_ns = remaining;
        value.borrow.check_lifetime = @This().lifetime;
        value.borrow.current_class = @This().currentClass;
        value.borrow.checkpoint = check;
        value.borrow.park = park;
        value.borrow.enter = enter;
        value.borrow.enter_checked = enterChecked;
        value.borrow.reserve_child = reserveChild;
        value.borrow.producer_parent = producerParent;
        value.borrow.park_publication = parkPublication;
        value.borrow.resume_publication = resumePublication;
        value.borrow.finish_publication = finishPublication;
        return value;
    }

    /// The storage producer stage calls this only after physical service
    /// retirement. Ordinary completion callbacks keep their activation guard.
    fn producerParent(raw: *anyopaque, out: *runtime.runnable_abi.Borrowed) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        const base = initTeam(self.execution, self.allocator) catch |err| return runtime.error_abi.statusFromError(err);
        out.* = base.borrow;
        out.context = self;
        out.resize_team = null;
        out.checkpoint = Producer.check;
        out.park = Producer.park;
        out.enter = Producer.enter;
        out.enter_checked = Producer.enterChecked;
        out.reserve_child = Producer.reserve;
        out.check_lifetime = Completion.lifetime;
        out.current_class = Completion.currentClass;
        out.park_publication = Completion.parkPublication;
        out.resume_publication = Completion.resumePublication;
        out.finish_publication = Completion.finishPublication;
        return .ok;
    }
    const Producer = struct {
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            from(raw).execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
            const wait = std.enums.fromInt(runtime.runnable_abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
            from(raw).execution.park(if (wait == .resource) .resource_blocked else .output_blocked) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn enter(raw: *anyopaque, next: c_int) callconv(.c) runtime.error_abi.Status {
            const class = std.enums.fromInt(scheduler.Class, next) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
            from(raw).execution.enter(class) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn enterChecked(raw: *anyopaque, next: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
            const class = std.enums.fromInt(scheduler.Class, next) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
            @import("runnable_reentry.zig").enter(from(raw).execution, class, restriction) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        fn reserve(raw: *anyopaque, options: *const runtime.runnable_abi.ChildStart) callconv(.c) runtime.error_abi.Status {
            const self = from(raw);
            @import("runnable_local_child.zig").reserve(self.execution, self.allocator, options) catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
    };

    fn parkPublication(raw: *anyopaque, out: *u8) callconv(.c) runtime.error_abi.Status {
        return @import("runnable_publication.zig").park(from(raw).execution, out);
    }
    fn resumePublication(raw: *anyopaque, restriction: ?*const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
        return @import("runnable_publication.zig").reenter(from(raw).execution, restriction);
    }
    fn finishPublication(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        return @import("runnable_publication.zig").finish(from(raw).execution);
    }

    fn reserveChild(raw: *anyopaque, options: *const runtime.runnable_abi.ChildStart) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.active() catch |err| return runtime.error_abi.statusFromError(err);
        // Accepted completion publishes independent workers from its one-slot
        // coordinator. Ordinary producer reservations retain their team path.
        const slots = self.execution.retainedSlots() catch |err| return runtime.error_abi.statusFromError(err);
        if (slots != 1) return runtime.error_abi.statusFromError(error.InvalidRunnableTransition);
        @import("runnable_local_child.zig").reserve(self.execution, self.allocator, options) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }

    fn from(raw: *anyopaque) *Completion {
        return @ptrCast(@alignCast(raw));
    }
    fn active(self: *Completion) !void {
        const entry = self.execution.entry orelse return error.InvalidRunnableTransition;
        if (!entry.completion_active) return error.ExecutionWaitUnsupported;
    }
    fn reserve(raw: *anyopaque, options: *const runtime.runnable_abi.Completion) callconv(.c) runtime.error_abi.Status {
        reserveChecked(from(raw), options) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn reserveChecked(self: *Completion, options: *const runtime.runnable_abi.Completion) !void {
        try options.validate();
        if (self.io != null) return error.InvalidRunnableTransition;
        const entry = self.execution.entry orelse return error.InvalidRunnableTransition;
        const native_clock = if (options.native_deadline != 0) entry.options.native_now_ns orelse return error.InvalidArgument else null;
        const io = try options.io.receive();
        const clock_io = if (options.clock_io) |borrow| try borrow.receive() else null;
        self.io = io;
        self.clock_io = clock_io;
        errdefer {
            self.io = null;
            self.clock_io = null;
        }
        try self.execution.reserveCompletion(.{
            .io = self.io.?.io(),
            .clock_io = if (self.clock_io) |*clock| clock.io() else null,
            .native_now_ns = native_clock,
            .deadline_ns = options.deadline_ns,
        });
    }
    fn activate(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        from(raw).execution.activateCompletion() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn remaining(raw: *anyopaque, out: *u64) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.active() catch |err| return runtime.error_abi.statusFromError(err);
        out.* = self.execution.completionRemainingNs() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn resizeTeam(raw: *anyopaque, slots: usize) callconv(.c) runtime.error_abi.Status {
        return resize(from(raw).execution, slots);
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
        const wait = std.enums.fromInt(runtime.runnable_abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        self.execution.park(switch (wait) {
            .resource => .resource_blocked,
            .output => .output_blocked,
        }) catch |err| return runtime.error_abi.statusFromError(err);
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
