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

const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const scheduler = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const abi = runtime.runnable_abi;

/// Child receiver for ingress-owned executions. The allocator must be bounded,
/// concurrent and retained through physical joins. Parent execution and original
/// clock/cancellation contexts stay alive until every child packet is retired.
/// No callback touches a mutable SQL Adapter or its read-fence cache. SQL worker
/// adapters must supply their own policy fences before enabling operator fanout.
pub fn reserve(parent: *scheduler.Execution, allocator: std.mem.Allocator, options: *const abi.ChildStart) !void {
    return reserveValidated(parent, allocator, options, null);
}
pub fn reserveValidated(parent: *scheduler.Execution, allocator: std.mem.Allocator, options: *const abi.ChildStart, validation: ?@import("runnable_read_validation.zig").Hook) !void {
    try options.validate();
    const bytes = std.math.add(u64, options.retained_bytes, @sizeOf(Receiver)) catch return error.InvalidRunnableDemand;
    const receiver = try allocator.create(Receiver);
    errdefer allocator.destroy(receiver);
    receiver.* = .{ .allocator = allocator, .io = try options.io.receive(), .validation = if (validation) |hook| if (hook.active) null else hook.fresh() else null };
    errdefer receiver.execution.finish();
    if (options.control != 0) try receiver.execution.reserveControlChild(parent, receiver.io.io(), bytes) else try receiver.execution.reserveChild(parent, receiver.io.io(), bytes);
    options.out_child.* = .{ .borrow = receiver.borrowed(), .finish = Receiver.finish };
}

const Receiver = struct {
    allocator: std.mem.Allocator,
    io: runtime.io_abi.Receiver,
    execution: scheduler.Execution = .{},
    validation: ?@import("runnable_read_validation.zig").Hook = null,

    fn precheck(self: *Receiver) !void {
        if (self.validation) |*hook| try hook.precheck();
    }
    fn validateCurrent(self: *Receiver) !void {
        if (self.validation) |*hook| try hook.ensure(&self.execution, self.borrowed());
    }

    fn from(raw: *anyopaque) *Receiver {
        return @ptrCast(@alignCast(raw));
    }
    fn borrowed(self: *Receiver) abi.Borrowed {
        return .{
            .context = self,
            .cpu_slots = 1,
            .parent_class = @intCast(@backingInt(self.execution.entry.?.class)),
            .checkpoint = check,
            .park = park,
            .enter = enter,
            .enter_checked = enterChecked,
            .reserve_child = reserveNested,
            .activate_child = activate,
            .activate_child_checked = activateChecked,
            .check_lifetime = lifetime,
            .current_class = currentClass,
            .completion_remaining_ns = remaining,
            .request_resources = requestResources,
            .enter_resources = enterResources,
        };
    }
    fn enterResources(raw: *anyopaque, resources: *const abi.ResourceBundle, restriction: ?*const abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const participant = @import("runnable_resource_bridge.zig").receive(resources) catch |err| return runtime.error_abi.statusFromError(err);
        if (restriction) |fence| fence.validate() catch |err| return runtime.error_abi.statusFromError(err);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_reentry.zig").enterWithResources(&self.execution, participant, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        const generation = self.execution.resume_generation;
        const validation_needed = if (self.validation) |hook| !hook.active and hook.last_generation != generation else false;
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        if (restriction) |fence| if (validation_needed or generation != self.execution.resume_generation) fence.checkpoint() catch |err| {
            if (self.validation) |*hook| hook.reject(&self.execution, err);
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
    fn remaining(raw: *anyopaque, out: *u64) callconv(.c) runtime.error_abi.Status {
        out.* = from(raw).execution.completionRemainingNs() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
        out.* = @intCast(@backingInt(from(raw).execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err)));
        return .ok;
    }
    fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn activate(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.activateChild() catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn activateChecked(raw: *anyopaque, restriction: *const abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_reentry.zig").activateChild(&self.execution, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        const generation = self.execution.resume_generation;
        const validation_needed = if (self.validation) |hook| !hook.active and hook.last_generation != generation else false;
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        if (validation_needed or generation != self.execution.resume_generation) restriction.checkpoint() catch |err| {
            if (self.validation) |*hook| hook.reject(&self.execution, err);
            return runtime.error_abi.statusFromError(err);
        };
        return .ok;
    }
    fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
        const wait = std.enums.fromInt(abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        from(raw).execution.park(if (wait == .resource) .resource_blocked else .output_blocked) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enter(raw: *anyopaque, next: c_int) callconv(.c) runtime.error_abi.Status {
        const class = std.enums.fromInt(scheduler.Class, next) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.execution.enter(class) catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enterChecked(raw: *anyopaque, next: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const class = std.enums.fromInt(scheduler.Class, next) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_reentry.zig").enter(&self.execution, class, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        const generation = self.execution.resume_generation;
        const validation_needed = if (self.validation) |hook| !hook.active and hook.last_generation != generation else false;
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        // Full policy validation can outlive the caller's restriction even
        // without a scheduling yield or a change in resume generation.
        if (validation_needed or generation != self.execution.resume_generation) restriction.checkpoint() catch |err| {
            if (self.validation) |*hook| hook.reject(&self.execution, err);
            return runtime.error_abi.statusFromError(err);
        };
        return .ok;
    }
    fn reserveNested(raw: *anyopaque, options: *const abi.ChildStart) callconv(.c) runtime.error_abi.Status {
        const self = from(raw);
        self.precheck() catch |err| return runtime.error_abi.statusFromError(err);
        self.validateCurrent() catch |err| return runtime.error_abi.statusFromError(err);
        reserveValidated(&self.execution, self.allocator, options, self.validation) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn finish(raw: *anyopaque) callconv(.c) void {
        const self = from(raw);
        const allocator = self.allocator;
        self.execution.finish();
        allocator.destroy(self);
    }
};

fn checked(status: runtime.error_abi.Status) !void {
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}

test "local child receiver owns independent worker state until physical join" {
    const Worker = struct {
        child: *abi.Child,
        ready: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        fn run(self: *@This()) !void {
            try self.child.activate();
            try checked(self.child.borrow.park(self.child.borrow.context, @backingInt(abi.Wait.resource)));
            self.ready.set(std.testing.io);
            self.go.waitUncancelable(std.testing.io);
            try checked(self.child.borrow.enter(self.child.borrow.context, @backingInt(abi.Class.query)));
        }
    };
    var owner = try scheduler.Scheduler.init(.{ .slots = 2 });
    defer std.debug.assert(owner.stats().operations == 0);
    var canceled: std.atomic.Value(bool) = .init(false);
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var parent: scheduler.Execution = .{};
    try parent.start(&owner, .{ .io = std.testing.io, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
    defer parent.finish();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var child: abi.Child = undefined;
    try reserve(&parent, std.testing.allocator, &.{ .io = &io_borrow, .retained_bytes = 31, .out_child = &child });
    defer child.deinit();
    const retained = owner.stats().retained_bytes;
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    try std.testing.expectError(error.InvalidRunnableTransition, checked(child.borrow.checkpoint(child.borrow.context)));
    try parent.park(.resource_blocked);
    var worker = Worker{ .child = &child };
    var future = try io.concurrent(Worker.run, .{&worker});
    defer {
        worker.go.set(io);
        future.await(io) catch {};
    }
    try worker.ready.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
    try parent.enter(.query);
    var health: scheduler.Execution = .{};
    try health.start(&owner, .{ .io = io, .allow_wait = false }, .control);
    try health.checkpoint();
    health.finish();
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    try parent.park(.resource_blocked);
    canceled.store(true, .release);
    worker.go.set(io);
    try std.testing.expectError(error.Canceled, future.await(io));
    try std.testing.expectError(error.Canceled, child.activate());
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    child.deinit();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().child_operations);
}

test "local child receiver rollback balances parent pins and bounded allocation" {
    const Scenario = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var owner = try scheduler.Scheduler.init(.{ .slots = 3 });
            defer std.debug.assert(owner.stats().operations == 0);
            var parent: scheduler.Execution = .{};
            try parent.start(&owner, .{ .io = std.testing.io }, .query);
            defer parent.finish();
            const io = std.testing.io;
            const io_borrow = runtime.native_abi.IoBorrow.init(&io);
            var child: abi.Child = undefined;
            reserve(&parent, allocator, &.{ .io = &io_borrow, .out_child = &child }) catch |err| {
                try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
                try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
                return err;
            };
            child.deinit();
            try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Scenario.run, .{});
}

test "local child receiver rejects overflow and retains stopped nested scopes" {
    var owner = try scheduler.Scheduler.init(.{ .slots = 4 });
    defer std.debug.assert(owner.stats().operations == 0);
    var parent: scheduler.Execution = .{};
    try parent.start(&owner, .{ .io = std.testing.io }, .query);
    defer parent.finish();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var child: abi.Child = undefined;
    try std.testing.expectError(error.InvalidRunnableDemand, reserve(&parent, std.testing.allocator, &.{ .io = &io_borrow, .retained_bytes = std.math.maxInt(u64), .out_child = &child }));
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
    try reserve(&parent, std.testing.allocator, &.{ .io = &io_borrow, .out_child = &child });
    defer child.deinit();
    try child.activate();
    var grandchild: abi.Child = undefined;
    try child.borrow.reserveChild(&.{ .io = &io_borrow, .out_child = &grandchild });
    defer grandchild.deinit();
    const retained = owner.stats().retained_bytes;
    owner.close();
    try std.testing.expectError(error.SchedulerClosed, grandchild.activate());
    try std.testing.expectError(error.SchedulerClosed, grandchild.activate());
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), owner.stats().child_operations);
    grandchild.deinit();
    child.deinit();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
}

test "local child receiver enforces shared operation bounds with exact rollback" {
    // Two foreground registry positions and one protected control position.
    var owner = try scheduler.Scheduler.init(.{ .slots = 2, .max_operations = 3 });
    defer std.debug.assert(owner.stats().operations == 0);
    var parent: scheduler.Execution = .{};
    try parent.start(&owner, .{ .io = std.testing.io }, .query);
    defer parent.finish();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var child: abi.Child = undefined;
    try reserve(&parent, std.testing.allocator, &.{ .io = &io_borrow, .out_child = &child });
    defer child.deinit();
    const retained = owner.stats().retained_bytes;
    var denied: abi.Child = undefined;
    try std.testing.expectError(error.SchedulerFull, reserve(&parent, std.testing.allocator, &.{ .io = &io_borrow, .out_child = &denied }));
    try std.testing.expectEqual(@as(usize, 1), parent.entry.?.children);
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    var health: scheduler.Execution = .{};
    try health.start(&owner, .{ .io = io, .allow_wait = false }, .control);
    health.finish();
    child.deinit();
    try reserve(&parent, std.testing.allocator, &.{ .io = &io_borrow, .out_child = &child });
    child.deinit();
    try std.testing.expectEqual(@as(usize, 0), parent.entry.?.children);
}
