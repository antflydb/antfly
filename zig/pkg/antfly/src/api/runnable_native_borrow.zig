// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

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
            .checkpoint = checkpoint,
            .check_lifetime = lifetime,
            .park = park,
            .enter = enter,
            .reserve_child = reserveChild,
            .activate_child = activateChild,
            .finish = finish,
        });
    }

    fn from(raw: *anyopaque) *Bridge {
        return @ptrCast(@alignCast(raw));
    }
    fn checkpoint(raw: *anyopaque) !void {
        const self = from(raw);
        try checked(self.borrow.checkpoint(self.borrow.context));
    }
    fn lifetime(raw: *anyopaque) !void {
        try from(raw).borrow.checkLifetime();
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
        return .{ .context = self, .cpu_slots = 1, .parent_class = @backingInt(abi.Class.query), .checkpoint = check, .park = park, .enter = enter, .reserve_child = reserve, .check_lifetime = lifetime, .current_class = currentClass };
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
