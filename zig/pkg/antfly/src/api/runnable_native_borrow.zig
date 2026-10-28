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

//! Stable local adapter for native read kernels borrowing an opaque admitted
//! execution. Root adapters never finish the ingress owner's operation. Child
//! adapters retain their owner-produced token and IO receiver until physical
//! joining; timeout, readiness and CPU parking never retire that ownership.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub const Bridge = struct {
    borrow: abi.Borrowed,
    allocator: std.mem.Allocator,
    owned_child: ?*ChildReceiver = null,
    resource_rejection: ?anyerror = null,

    pub fn init(raw: *const abi.Borrowed, allocator: std.mem.Allocator) !Bridge {
        const borrow = try abi.Borrowed.read(raw);
        // A native physical read polls lifetime while parked. Substituting its
        // CPU checkpoint would reject valid parked reads or reacquire CPU.
        if (borrow.check_lifetime == null or borrow.current_class == null) return error.UnsupportedOperation;
        return .{ .borrow = borrow, .allocator = allocator };
    }

    /// Bind only after this bridge has reached its stable retained address.
    pub fn bind(self: *Bridge, execution: *native.Execution) !void {
        try execution.bindBorrowed(.{
            .ptr = self,
            .class = try currentClass(self),
            .current_class = currentClass,
            .cpu_borrow = if (self.borrow.cpu_borrow != null) cpuBorrow else null,
            .park_publication = if (self.borrow.park_publication != null) parkPublication else null,
            .resume_publication = if (self.borrow.resume_publication != null) resumePublication else null,
            .finish_publication = if (self.borrow.finish_publication != null) finishPublication else null,
            .request_resources = if (self.borrow.request_resources != null) requestResources else null,
            .enter_resources = if (self.borrow.enter_resources != null) enterResources else null,
            .checkpoint = checkpoint,
            .check_lifetime = lifetime,
            .park = park,
            .enter = enter,
            .enter_checked = if (self.borrow.enter_checked != null) enterChecked else null,
            .reserve_child = reserveChild,
            .activate_child = activateChild,
            .activate_child_checked = if (self.borrow.activate_child_checked != null) activateChildChecked else null,
            .finish = finish,
        });
    }

    fn enterResources(raw: *anyopaque, resources: native.GrantResources, restriction: ?native.AdmissionCheck) !void {
        const self = from(raw);
        if (self.resource_rejection) |cause| return cause;
        const participant = @import("runnable_resource_bridge.zig").send(resources);
        const Forward = struct {
            restriction: native.AdmissionCheck,
            rejection: ?anyerror = null,
            fn check(context: *anyopaque) callconv(.c) runtime.error_abi.Status {
                const value: *@This() = @ptrCast(@alignCast(context));
                if (value.rejection != null) return runtime.error_abi.statusFromError(error.Canceled);
                value.restriction.checkpoint() catch |err| {
                    value.rejection = err;
                    return runtime.error_abi.statusFromError(error.Canceled);
                };
                return .ok;
            }
        };
        var forwarded: ?Forward = if (restriction) |value| .{ .restriction = value } else null;
        var reentry: ?abi.Reentry = if (forwarded) |*value| .{ .context = value, .check = Forward.check } else null;
        self.borrow.enterWithResources(&participant, if (reentry) |*value| value else null) catch |err| {
            if (forwarded) |value| if (value.rejection) |cause| {
                self.resource_rejection = cause;
                return cause;
            };
            return err;
        };
    }
    fn requestResources(raw: *anyopaque, resources: native.GrantResources) !void {
        const self = from(raw);
        const participant = @import("runnable_resource_bridge.zig").send(resources);
        try self.borrow.requestResources(&participant);
    }
    fn parkPublication(raw: *anyopaque) !bool {
        return from(raw).borrow.parkPublication();
    }
    fn resumePublication(raw: *anyopaque, restriction: ?native.AdmissionCheck) !void {
        const Check = struct {
            original: native.AdmissionCheck,
            fn call(context: *anyopaque) callconv(.c) runtime.error_abi.Status {
                const self: *@This() = @ptrCast(@alignCast(context));
                self.original.checkpoint() catch |cause| return runtime.error_abi.statusFromError(cause);
                return .ok;
            }
        };
        var check: ?Check = if (restriction) |value| .{ .original = value } else null;
        try from(raw).borrow.resumePublication(if (check) |*fence| &.{ .context = fence, .check = Check.call } else null);
    }
    fn finishPublication(raw: *anyopaque) !void {
        try from(raw).borrow.finishPublication();
    }

    fn cpuBorrow(raw: *anyopaque, budget: ?native.CpuBorrowBudget) !void {
        const self = from(raw);
        try self.borrow.setCpuBorrow(&.{ .enabled = @intFromBool(budget != null), .max_non_yield_ns = if (budget) |value| value.max_non_yield_ns else 0 });
    }
    fn from(raw: *anyopaque) *Bridge {
        return @ptrCast(@alignCast(raw));
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self = from(raw);
        if (self.resource_rejection) |cause| return cause;
        try checked(self.borrow.checkpoint(self.borrow.context));
    }
    fn lifetime(raw: *anyopaque) !void {
        const self = from(raw);
        if (self.resource_rejection) |cause| return cause;
        try self.borrow.checkLifetime();
    }
    fn currentClass(raw: *anyopaque) !native.Class {
        return classFromAbi(@backingInt(try from(raw).borrow.currentClass()));
    }
    fn park(raw: *anyopaque, state: native.State) !void {
        const wait: abi.Wait = switch (state) {
            .resource_blocked => .resource,
            .output_blocked => .output,
            else => return error.InvalidRunnableTransition,
        };
        const self = from(raw);
        try checked(self.borrow.park(self.borrow.context, @backingInt(wait)));
    }
    fn enter(raw: *anyopaque, class: native.Class) !void {
        const self = from(raw);
        const next: abi.Class = switch (class) {
            inline else => |value| @field(abi.Class, @tagName(value)),
        };
        try checked(self.borrow.enter(self.borrow.context, @backingInt(next)));
    }
    fn enterChecked(raw: *anyopaque, class: native.Class, restriction: native.AdmissionCheck) !void {
        const self = from(raw);
        const Forward = struct {
            restriction: native.AdmissionCheck,
            fn check(context: *anyopaque) callconv(.c) runtime.error_abi.Status {
                const value: *@This() = @ptrCast(@alignCast(context));
                value.restriction.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
                return .ok;
            }
        };
        var forwarded: Forward = .{ .restriction = restriction };
        const next: abi.Class = switch (class) {
            inline else => |value| @field(abi.Class, @tagName(value)),
        };
        try self.borrow.enterChecked(next, &.{ .context = &forwarded, .check = Forward.check });
    }
    fn reserveChild(raw: *anyopaque, io: std.Io, retained_bytes: u64, execution: *native.Execution) !void {
        const self = from(raw);
        const bytes = std.math.add(u64, retained_bytes, @sizeOf(ChildReceiver)) catch return error.InvalidRunnableDemand;
        const receiver = try self.allocator.create(ChildReceiver);
        errdefer self.allocator.destroy(receiver);
        receiver.* = .{ .io = io };
        // Keep the IO value in the retained packet. Its underlying executor
        // remains borrowed from the caller through the child's physical join.
        const io_borrow = runtime.native_abi.IoBorrow.init(&receiver.io);
        try self.borrow.reserveChild(&.{ .io = &io_borrow, .retained_bytes = bytes, .out_child = &receiver.child });
        errdefer receiver.child.deinit();
        try receiver.child.validate();
        receiver.bridge = try init(&receiver.child.borrow, self.allocator);
        receiver.bridge.owned_child = receiver;
        try receiver.bridge.bind(execution);
    }
    fn activateChild(raw: *anyopaque) !void {
        const self = from(raw);
        const receiver = self.owned_child orelse return error.InvalidRunnableTransition;
        try receiver.child.activate();
    }
    fn activateChildChecked(raw: *anyopaque, restriction: native.AdmissionCheck) !void {
        const self = from(raw);
        if (self.resource_rejection) |cause| return cause;
        const receiver = self.owned_child orelse return error.InvalidRunnableTransition;
        const Forward = struct {
            restriction: native.AdmissionCheck,
            rejection: ?anyerror = null,
            fn check(context: *anyopaque) callconv(.c) runtime.error_abi.Status {
                const value: *@This() = @ptrCast(@alignCast(context));
                if (value.rejection) |cause| return runtime.error_abi.statusFromError(cause);
                value.restriction.checkpoint() catch |err| {
                    value.rejection = err;
                    return runtime.error_abi.statusFromError(err);
                };
                return .ok;
            }
        };
        var forwarded: Forward = .{ .restriction = restriction };
        receiver.child.activateChecked(&.{ .context = &forwarded, .check = Forward.check }) catch |err| {
            if (forwarded.rejection) |cause| {
                self.resource_rejection = cause;
                return cause;
            }
            return err;
        };
    }
    fn finish(raw: *anyopaque) void {
        const self = from(raw);
        if (self.owned_child) |receiver| {
            const allocator = self.allocator;
            receiver.child.deinit();
            allocator.destroy(receiver);
        }
    }
};

const ChildReceiver = struct {
    io: std.Io,
    child: abi.Child = undefined,
    bridge: Bridge = undefined,
};

fn checked(status: runtime.error_abi.Status) !void {
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}
fn classFromAbi(raw: c_int) !native.Class {
    const class = std.enums.fromInt(abi.Class, raw) orelse return error.InvalidArgument;
    return switch (class) {
        inline else => |value| @field(native.Class, @tagName(value)),
    };
}

const TestAuthority = struct {
    execution: *native.Execution,
    allocator: std.mem.Allocator = std.testing.allocator,
    const Dispatch = @FieldType(runtime.native_abi.IoBorrow, "dispatch");
    const P = @typeInfo(@typeInfo(Dispatch).pointer.child).@"fn".param_types;
    var original_dispatch: Dispatch = undefined;
    var foreign_calls: std.atomic.Value(usize) = .init(0);
    fn dispatch(a: P[0].?, b: P[1].?, c: P[2].?, d: P[3].?, e: P[4].?, f: P[5].?) callconv(.c) void {
        _ = foreign_calls.fetchAdd(1, .release);
        original_dispatch(a, b, c, d, e, f);
    }
    fn borrowed(self: *@This()) abi.Borrowed {
        return .{ .context = self, .cpu_slots = 1, .parent_class = @backingInt(abi.Class.query), .checkpoint = check, .park = park, .enter = enter, .enter_checked = enterChecked, .reserve_child = reserve, .check_lifetime = lifetime, .current_class = currentClass };
    }
    fn owner(raw: *anyopaque) *@This() {
        return @ptrCast(@alignCast(raw));
    }
    fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        owner(raw).execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
        owner(raw).execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
        out.* = @intCast(@backingInt(owner(raw).execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err)));
        return .ok;
    }
    fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
        const wait = std.enums.fromInt(abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
        owner(raw).execution.park(if (wait == .resource) .resource_blocked else .output_blocked) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enter(raw: *anyopaque, next: c_int) callconv(.c) runtime.error_abi.Status {
        const class = classFromAbi(next) catch |err| return runtime.error_abi.statusFromError(err);
        owner(raw).execution.enter(class) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn enterChecked(raw: *anyopaque, class: c_int, restriction: *const abi.Reentry) callconv(.c) runtime.error_abi.Status {
        const phase = classFromAbi(class) catch |err| return runtime.error_abi.statusFromError(err);
        @import("runnable_reentry.zig").enter(owner(raw).execution, phase, restriction) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
    fn reserve(raw: *anyopaque, options: *const abi.ChildStart) callconv(.c) runtime.error_abi.Status {
        const self = owner(raw);
        // Exercise retained IO callback contexts even in this one-codegen test.
        var io = options.io.*;
        original_dispatch = io.dispatch;
        io.dispatch = dispatch;
        var forwarded = options.*;
        forwarded.io = &io;
        @import("runnable_local_child.zig").reserve(self.execution, self.allocator, &forwarded) catch |err| return runtime.error_abi.statusFromError(err);
        return .ok;
    }
};

test "native execution borrow preserves physical child lifetime and protected control" {
    const Case = enum { success, canceled, expired, shutdown };
    const Clock = struct {
        var time: std.atomic.Value(u64) = .init(10);
        fn now() u64 {
            return time.load(.acquire);
        }
    };
    const Cancel = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    const Worker = struct {
        execution: *native.Execution,
        file: std.Io.File,
        ready: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        fn run(self: *@This()) anyerror!u8 {
            try self.execution.activateChild();
            defer self.execution.park(.resource_blocked) catch {};
            try self.execution.checkpoint();
            try self.execution.park(.resource_blocked);
            self.ready.set(std.testing.io);
            self.go.waitUncancelable(std.testing.io);
            try self.execution.checkLifetime();
            var bytes: [1]u8 = undefined;
            try std.testing.expectEqual(@as(usize, 1), try self.file.readPositionalAll(std.testing.io, &bytes, 0));
            try self.execution.enter(try self.execution.retainedClass());
            try self.execution.checkpoint();
            return bytes[0];
        }
    };
    for (std.enums.values(Case)) |case| {
        Clock.time.store(10, .release);
        TestAuthority.foreign_calls.store(0, .release);
        var canceled: std.atomic.Value(bool) = .init(false);
        var owner = try native.Scheduler.init(.{ .slots = 2 });
        var original: native.Execution = .{};
        try original.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 19, .cancellation = .{ .ptr = &canceled, .requested = Cancel.requested } }, .query);
        defer original.finish();
        var authority: TestAuthority = .{ .execution = &original };
        var port = authority.borrowed();
        var bridge = try Bridge.init(&port, std.testing.allocator);
        var parent: native.Execution = .{};
        try bridge.bind(&parent);
        defer parent.finish();
        var child: native.Execution = .{};
        try child.reserveChild(&parent, std.testing.io, @sizeOf(native.Execution) + 31);
        defer child.finish();
        try std.testing.expectError(error.InvalidRunnableTransition, child.checkpoint());
        try std.testing.expectEqual(@as(usize, 1), original.entry.?.children);
        const retained = owner.stats().retained_bytes;
        try parent.park(.resource_blocked);
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "value", .data = &.{42} });
        const file = try tmp.dir.openFile(std.testing.io, "value", .{});
        defer file.close(std.testing.io);
        var worker: Worker = .{ .execution = &child, .file = file };
        var future = try std.testing.io.concurrent(Worker.run, .{&worker});
        defer {
            worker.go.set(std.testing.io);
            _ = future.cancel(std.testing.io) catch {};
        }
        try worker.ready.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
        try parent.checkLifetime();
        try std.testing.expectError(error.InvalidRunnableTransition, parent.checkpoint());
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        var control: native.Execution = .{};
        try control.start(&owner, .{ .io = std.testing.io }, .control);
        try control.checkpoint();
        control.finish();
        switch (case) {
            .success => {},
            .canceled => canceled.store(true, .release),
            .expired => Clock.time.store(100, .release),
            .shutdown => owner.close(),
        }
        try std.testing.expectEqual(retained, owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
        worker.go.set(std.testing.io);
        const result = future.await(std.testing.io);
        switch (case) {
            .success => try std.testing.expectEqual(@as(u8, 42), try result),
            .canceled => try std.testing.expectError(error.Canceled, result),
            .expired => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, result),
        }
        // Logical completion is insufficient: ownership remains through the
        // physical join and is retired only by the owned child adapter finish.
        try std.testing.expectEqual(retained, owner.stats().retained_bytes);
        child.finish();
        try std.testing.expect(TestAuthority.foreign_calls.load(.acquire) != 0);
        try std.testing.expectEqual(@as(usize, 0), original.entry.?.children);
        try std.testing.expectEqual(@as(u64, 19), owner.stats().retained_bytes);
        parent.finish();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        original.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "native execution borrow retires every failed receiver allocation before publication" {
    const Fixture = struct {
        fn open(allocator: std.mem.Allocator) !void {
            var owner = try native.Scheduler.init(.{ .slots = 2 });
            var original: native.Execution = .{};
            try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
            defer original.finish();
            var authority: TestAuthority = .{ .execution = &original, .allocator = allocator };
            var port = authority.borrowed();
            var bridge = try Bridge.init(&port, allocator);
            var parent: native.Execution = .{};
            try bridge.bind(&parent);
            defer parent.finish();
            var child: native.Execution = .{};
            defer {
                child.finish();
                std.debug.assert(original.entry.?.children == 0);
                std.debug.assert(owner.stats().operations == 1);
                std.debug.assert(owner.stats().retained_bytes == 17);
            }
            try child.reserveChild(&parent, std.testing.io, @sizeOf(native.Execution));
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.open, .{});
}

test "native execution borrow refuses legacy lifetime and duplicate root binding" {
    var owner = try native.Scheduler.init(.{ .slots = 2 });
    var original: native.Execution = .{};
    try original.start(&owner, .{ .io = std.testing.io }, .query);
    defer original.finish();
    var authority: TestAuthority = .{ .execution = &original };
    var port = authority.borrowed();
    port.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, Bridge.init(&port, std.testing.allocator));
    port.version = abi.abi_version;
    port.check_lifetime = null;
    try std.testing.expectError(error.UnsupportedOperation, Bridge.init(&port, std.testing.allocator));
    port.check_lifetime = TestAuthority.lifetime;
    port.current_class = null;
    try std.testing.expectError(error.UnsupportedOperation, Bridge.init(&port, std.testing.allocator));
    port.current_class = TestAuthority.currentClass;
    var bridge = try Bridge.init(&port, std.testing.allocator);
    var parent: native.Execution = .{};
    try bridge.bind(&parent);
    defer parent.finish();
    try std.testing.expectError(error.InvalidRunnableTransition, bridge.bind(&parent));
    try std.testing.expectError(error.InvalidRunnableTransition, parent.activateChild());
    try std.testing.expectError(error.UnsupportedOperation, parent.resizeTeam(2));
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
}

test "native execution borrow resumes the current phase without promoting saved parent class" {
    for ([_]native.Class{ .inference, .background }) |phase| {
        var owner = try native.Scheduler.init(.{ .slots = 2 });
        var original: native.Execution = .{};
        try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 17 }, .query);
        defer original.finish();
        var authority: TestAuthority = .{ .execution = &original };
        var port = authority.borrowed();
        var bridge = try Bridge.init(&port, std.testing.allocator);
        var execution: native.Execution = .{};
        try bridge.bind(&execution);
        defer execution.finish();
        const query_grants = owner.stats().grants[@backingInt(native.Class.query)];
        try original.park(.resource_blocked);
        try original.enter(phase);
        try std.testing.expectEqual(@backingInt(abi.Class.query), port.parent_class);
        try std.testing.expectEqual(phase, try execution.retainedClass());
        try execution.park(.resource_blocked);
        try execution.checkLifetime();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        try execution.enter(try execution.retainedClass());
        try std.testing.expectEqual(phase, try original.retainedClass());
        try std.testing.expectEqual(query_grants, owner.stats().grants[@backingInt(native.Class.query)]);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 17), owner.stats().retained_bytes);
    }
}

test "native execution borrow carries production HTTP read capability through a physical child" {
    const Worker = struct {
        execution: *native.Execution,
        file: std.Io.File,
        ready: std.Io.Event = .unset,
        go: std.Io.Event = .unset,
        fn run(self: *@This()) anyerror!u8 {
            try self.execution.activateChild();
            defer self.execution.park(.resource_blocked) catch {};
            try self.execution.park(.resource_blocked);
            self.ready.set(std.testing.io);
            self.go.waitUncancelable(std.testing.io);
            try self.execution.checkLifetime();
            var bytes: [1]u8 = undefined;
            try std.testing.expectEqual(@as(usize, 1), try self.file.readPositionalAll(std.testing.io, &bytes, 0));
            try self.execution.enter(try self.execution.retainedClass());
            try self.execution.checkpoint();
            return bytes[0];
        }
    };
    var owner = try native.Scheduler.init(.{ .slots = 2 });
    var original: native.Execution = .{};
    try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
    defer original.finish();
    var read: @import("runnable_local_read.zig").Read = .{ .execution = &original, .allocator = std.testing.allocator };
    var team = try read.team();
    var bridge = try Bridge.init(&team.borrow, std.testing.allocator);
    var parent: native.Execution = .{};
    try bridge.bind(&parent);
    defer parent.finish();
    var child: native.Execution = .{};
    try child.reserveChild(&parent, std.testing.io, @sizeOf(native.Execution) + 31);
    defer child.finish();
    const retained = owner.stats().retained_bytes;
    try std.testing.expectError(error.InvalidRunnableTransition, read.team());
    try parent.park(.resource_blocked);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "value", .data = &.{42} });
    const file = try tmp.dir.openFile(std.testing.io, "value", .{});
    defer file.close(std.testing.io);
    var worker: Worker = .{ .execution = &child, .file = file };
    var future = try std.testing.io.concurrent(Worker.run, .{&worker});
    defer {
        worker.go.set(std.testing.io);
        _ = future.cancel(std.testing.io) catch {};
    }
    try worker.ready.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } });
    try team.borrow.checkLifetime();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
    var control: native.Execution = .{};
    try control.start(&owner, .{ .io = std.testing.io }, .control);
    try control.checkpoint();
    control.finish();
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    worker.go.set(std.testing.io);
    try std.testing.expectEqual(@as(u8, 42), try future.await(std.testing.io));
    // Readiness and completed IO cannot retire the child receiver before join.
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    child.finish();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try parent.enter(try parent.retainedClass());
    parent.finish();
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
    original.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
}

test "native execution borrow CPU loan return revalidates production read authority without retiring root" {
    const io = std.testing.io;
    const Policy = struct {
        execution: *native.Execution,
        revoked: std.atomic.Value(bool) = .init(false),
        checks: std.atomic.Value(usize) = .init(0),
        fn check(raw: *anyopaque, _: abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.execution.entry.?.borrowed_read_cpu or self.execution.entry.?.cpu_borrow_budget != null) return error.InvalidRunnableTransition;
            _ = self.checks.fetchAdd(1, .acq_rel);
            if (self.revoked.load(.acquire)) return error.Forbidden;
        }
    };
    const Worker = struct {
        fn run(execution: *native.Execution) !void {
            try execution.checkpoint();
        }
    };
    inline for (.{ false, true }) |revoke| {
        var owner = try native.Scheduler.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true, .bounded_read_slots = 2, .checkpoint_budget = 1, .max_wait_ms = 5000 });
        defer owner.close();
        var original: native.Execution = .{};
        defer original.finish();
        const deadline = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 5 * std.time.ns_per_s;
        try original.start(&owner, .{ .io = io, .deadline_ns = deadline, .retained_bytes = 23 }, .query);
        var policy: Policy = .{ .execution = &original };
        var read: @import("runnable_local_read.zig").Read = .{ .execution = &original, .allocator = std.testing.allocator, .validation = .{ .context = &policy, .check = Policy.check } };
        var team = try read.team();
        var bridge = try Bridge.init(&team.borrow, std.testing.allocator);
        var adapter: native.Execution = .{};
        try bridge.bind(&adapter);
        defer adapter.finish();
        try adapter.enableCpuBorrow(.{ .max_non_yield_ns = 1000 });
        var peer: native.Entry = .{ .options = .{ .io = io }, .class = .background, .cpu_borrow_budget = .{ .max_non_yield_ns = 1000 } };
        try owner.submit(&peer);
        defer owner.finish(&peer);
        // Returning the base CPU converts the peer's loan into base capacity;
        // the same checked root then receives the idle reserved CPU instead.
        try original.checkpoint();
        try std.testing.expect(original.entry.?.borrowed_read_cpu);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
        var floor: native.Entry = .{ .options = .{ .io = io }, .class = .query, .read_lane = .bounded };
        try owner.submit(&floor);
        defer owner.finish(&floor);
        try std.testing.expectEqual(native.State.running, owner.state(&floor));
        const before_checks = policy.checks.load(.acquire);
        var future = try io.concurrent(Worker.run, .{&adapter});
        defer future.cancel(io) catch {};
        const until = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(2));
        while (owner.state(&original.entry.?) != .queued) {
            if (owner.state(&original.entry.?) == .stopped) return future.await(io);
            if (std.Io.Clock.awake.now(io).nanoseconds >= until.nanoseconds) return error.CheckedCpuLoanDidNotWait;
            try io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(before_checks, policy.checks.load(.acquire));
        try std.testing.expectEqual(native.State.running, owner.state(&floor));
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        policy.revoked.store(revoke, .release);
        owner.finish(&peer);
        if (revoke) try std.testing.expectError(error.Forbidden, future.await(io)) else try future.await(io);
        owner.finish(&floor);
        try std.testing.expectEqual(deadline, original.entry.?.options.deadline_ns.?);
        try std.testing.expectEqual(@as(usize, 0), owner.cpuBorrowStats().active);
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
        try std.testing.expectEqual(if (revoke) native.State.stopped else native.State.running, owner.state(&original.entry.?));
        try std.testing.expect(policy.checks.load(.acquire) == before_checks + 1);
        adapter.finish();
        // A root adapter never owns its ingress root's finish.
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        original.finish();
        try std.testing.expectEqual(@as(u64, 0), owner.stats().retained_bytes);
    }
}

test "native execution borrow absent CPU loan capability refuses before changing root ownership" {
    var owner = try native.Scheduler.init(.{ .slots = 2 });
    defer owner.close();
    var original: native.Execution = .{};
    defer original.finish();
    try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
    var authority: TestAuthority = .{ .execution = &original };
    var port = authority.borrowed();
    var bridge = try Bridge.init(&port, std.testing.allocator);
    var adapter: native.Execution = .{};
    try bridge.bind(&adapter);
    defer adapter.finish();
    try std.testing.expectError(error.UnsupportedOperation, adapter.enableCpuBorrow(.{ .max_non_yield_ns = 1000 }));
    try std.testing.expectError(error.UnsupportedOperation, adapter.disableCpuBorrow());
    try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
}

test "native execution borrow validation fences stopped roots through cached and reentrant policy checks" {
    const Mode = enum { expired, canceled, closed };
    const Phase = enum { callback, cached, reentrant };
    const Read = @import("runnable_local_read.zig").Read;
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    const Policy = struct {
        owner: *native.Scheduler,
        canceled: *bool,
        mode: Mode,
        armed: bool = false,
        reentrant: bool = false,
        read: ?*Read = null,
        checks: usize = 0,
        nested_failure: ?anyerror = null,
        fn stop(self: *@This()) void {
            switch (self.mode) {
                .expired => Clock.tick = 100,
                .canceled => self.canceled.* = true,
                .closed => self.owner.close(),
            }
        }
        fn check(raw: *anyopaque, _: abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (!self.armed) return;
            self.stop();
            if (self.reentrant) {
                self.read.?.validateCurrent() catch |err| {
                    self.nested_failure = err;
                    return error.Forbidden;
                };
                return error.InvalidRunnableTransition;
            }
        }
    };
    inline for (.{ Phase.callback, Phase.cached, Phase.reentrant }) |phase| {
        inline for (.{ Mode.expired, Mode.canceled, Mode.closed }) |mode| {
            Clock.tick = 0;
            var canceled = false;
            var owner = try native.Scheduler.init(.{ .slots = 2 });
            defer owner.close();
            var original: native.Execution = .{};
            defer original.finish();
            try original.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Clock.canceled } }, .query);
            var policy: Policy = .{ .owner = &owner, .canceled = &canceled, .mode = mode, .reentrant = phase == .reentrant };
            var read: Read = .{ .execution = &original, .allocator = std.testing.allocator, .validation = .{ .context = &policy, .check = Policy.check } };
            policy.read = &read;
            if (phase == .cached) {
                try read.validateCurrent();
                policy.stop();
            } else policy.armed = true;
            const expected = switch (mode) {
                .expired => error.DeadlineExceeded,
                .canceled => error.Canceled,
                .closed => error.SchedulerClosed,
            };
            try std.testing.expectError(expected, read.validateCurrent());
            try std.testing.expect(read.validation.?.failure.? == expected);
            try std.testing.expect(read.validation.?.last_generation == null);
            try std.testing.expect(!read.validation.?.active);
            if (phase == .reentrant) try std.testing.expect(policy.nested_failure.? == expected);
            try std.testing.expectEqual(@as(usize, 1), policy.checks);
            try std.testing.expectEqual(native.State.stopped, owner.state(&original.entry.?));
            try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
            try std.testing.expectEqual(@as(u64, 100), original.entry.?.options.deadline_ns.?);
            Clock.tick = 0;
            canceled = false;
            try std.testing.expectError(expected, read.validateCurrent());
            try std.testing.expectEqual(@as(usize, 1), policy.checks);
            original.finish();
            try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        }
    }
}

test "native execution borrow child activation rejects lifetime stopped during policy without retiring receivers" {
    const Mode = enum { expired, canceled, closed };
    const Clock = struct {
        var tick: u64 = 0;
        fn now() u64 {
            return tick;
        }
        fn canceled(raw: *const anyopaque) bool {
            return @as(*const bool, @ptrCast(@alignCast(raw))).*;
        }
    };
    const Policy = struct {
        owner: *native.Scheduler,
        canceled: *bool,
        mode: Mode,
        checks: usize = 0,
        armed: bool = false,
        fn check(raw: *anyopaque, _: abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (!self.armed) return;
            switch (self.mode) {
                .expired => Clock.tick = 100,
                .canceled => self.canceled.* = true,
                .closed => self.owner.close(),
            }
        }
    };
    inline for (.{ Mode.expired, Mode.canceled, Mode.closed }) |mode| {
        Clock.tick = 0;
        var canceled = false;
        var owner = try native.Scheduler.init(.{ .slots = 2 });
        defer owner.close();
        var original: native.Execution = .{};
        defer original.finish();
        try original.start(&owner, .{ .io = std.testing.io, .native_now_ns = Clock.now, .deadline_ns = 100, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Clock.canceled } }, .query);
        var policy: Policy = .{ .owner = &owner, .canceled = &canceled, .mode = mode };
        var read: @import("runnable_local_read.zig").Read = .{ .execution = &original, .allocator = std.testing.allocator, .validation = .{ .context = &policy, .check = Policy.check } };
        var team = try read.team();
        var bridge = try Bridge.init(&team.borrow, std.testing.allocator);
        var parent: native.Execution = .{};
        try bridge.bind(&parent);
        defer parent.finish();
        var child: native.Execution = .{};
        defer child.finish();
        try child.reserveChild(&parent, std.testing.io, @sizeOf(native.Execution) + 31);
        const retained = owner.stats().retained_bytes;
        policy.armed = true;
        try parent.park(.resource_blocked);
        const expected = switch (mode) {
            .expired => error.DeadlineExceeded,
            .canceled => error.Canceled,
            .closed => error.SchedulerClosed,
        };
        try std.testing.expectError(expected, child.activateChild());
        try std.testing.expectError(expected, child.checkLifetime());
        try std.testing.expectEqual(@as(usize, 2), policy.checks);
        try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
        try std.testing.expectEqual(retained, owner.stats().retained_bytes);
        child.finish();
        parent.finish();
        try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
        try std.testing.expectEqual(@as(u64, 100), original.entry.?.options.deadline_ns.?);
        original.finish();
        try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
    }
}

test "native execution borrow checked reentry retains caller restrictions across nonyielding policy validation" {
    const Policy = struct {
        stopped: bool = false,
        armed: bool = false,
        checks: usize = 0,
        cause: anyerror,
        fn validate(raw: *anyopaque, _: abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.armed) self.stopped = true;
        }
        fn restrict(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.stopped) return self.cause;
        }
    };
    inline for (.{ false, true }) |use_child| {
        inline for (.{ error.CatalogRoutingSnapshotTimeout, error.Canceled, error.PreDecisionDeadlineExceeded }) |cause| {
            var owner = try native.Scheduler.init(.{ .slots = 2 });
            defer owner.close();
            var original: native.Execution = .{};
            defer original.finish();
            try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
            var policy: Policy = .{ .cause = cause };
            var read: @import("runnable_local_read.zig").Read = .{ .execution = &original, .allocator = std.testing.allocator, .validation = .{ .context = &policy, .check = Policy.validate } };
            var team = try read.team();
            var bridge = try Bridge.init(&team.borrow, std.testing.allocator);
            var parent: native.Execution = .{};
            try bridge.bind(&parent);
            defer parent.finish();
            var child: native.Execution = .{};
            defer child.finish();
            if (use_child) try child.reserveChild(&parent, std.testing.io, @sizeOf(native.Execution) + 31);
            try parent.park(.resource_blocked);
            if (use_child) {
                try child.activateChild();
                try child.park(.resource_blocked);
            }
            const retained = owner.stats().retained_bytes;
            const before = policy.checks;
            policy.armed = true;
            const execution = if (use_child) &child else &parent;
            try std.testing.expectError(cause, execution.enterWithCheck(.query, .{ .context = &policy, .check = Policy.restrict }));
            try std.testing.expectEqual(before + 1, policy.checks);
            try std.testing.expectEqual(@as(usize, 0), owner.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, if (use_child) 2 else 1), owner.stats().operations);
            try std.testing.expectEqual(retained, owner.stats().retained_bytes);
            policy.stopped = false;
            try std.testing.expectError(cause, execution.checkLifetime());
            if (use_child) try original.checkLifetime() else try std.testing.expect(read.validation.?.last_generation == null);
            child.finish();
            parent.finish();
            try std.testing.expectEqual(@as(usize, 1), owner.stats().operations);
            try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
            original.finish();
            try std.testing.expectEqual(@as(usize, 0), owner.stats().operations);
        }
    }
}

test "native execution borrow checked child activation rejects queued private authority and retains ownership" {
    const Scope = struct {
        owner: *native.Scheduler,
        checks: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            // stats takes the owner lock: restrictions must run outside it.
            const stats = self.owner.stats();
            self.checks += 1;
            if (stats.queued != 0) return error.OriginalProducerActivationRejected;
        }
    };
    var owner = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer owner.close();
    var original: native.Execution = .{};
    defer original.finish();
    try original.start(&owner, .{ .io = std.testing.io, .retained_bytes = 23 }, .query);
    var authority: TestAuthority = .{ .execution = &original };
    var bridge = try Bridge.init(&authority.borrowed(), std.testing.allocator);
    var parent: native.Execution = .{};
    try bridge.bind(&parent);
    defer parent.finish();
    var child: native.Execution = .{};
    defer child.finish();
    try child.reserveChild(&parent, std.testing.io, @sizeOf(native.Execution) + 31);
    try parent.park(.resource_blocked);
    var peer: native.Execution = .{};
    defer peer.finish();
    try peer.start(&owner, .{ .io = std.testing.io, .allow_wait = false }, .query);
    const retained = owner.stats().retained_bytes;
    var scope: Scope = .{ .owner = &owner };
    const restriction: native.AdmissionCheck = .{ .context = &scope, .check = Scope.check };
    try std.testing.expectError(error.OriginalProducerActivationRejected, child.activateChildWithCheck(restriction));
    try std.testing.expectEqual(@as(usize, 2), scope.checks);
    try std.testing.expectError(error.OriginalProducerActivationRejected, child.activateChildWithCheck(restriction));
    try std.testing.expectError(error.OriginalProducerActivationRejected, child.checkLifetime());
    try std.testing.expectEqual(@as(usize, 2), scope.checks);
    try std.testing.expectEqual(@as(usize, 3), owner.stats().operations);
    try std.testing.expectEqual(retained, owner.stats().retained_bytes);
    try std.testing.expectEqual(native.State.running, peer.entry.?.state);
    child.finish();
    try std.testing.expectEqual(@as(usize, 2), owner.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), owner.stats().retained_bytes);
}
