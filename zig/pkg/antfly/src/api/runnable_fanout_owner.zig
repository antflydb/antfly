// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;

/// One stable fanout result slot. Reservation precedes publication; retirement
/// follows physical joining and destruction of the slot's result arena.
pub const Owner = struct {
    io: std.Io = undefined,
    child: ?abi.Child = null,
    running: bool = false,
    started: bool = false,
    can_activate: bool = false,

    pub fn reserve(self: *Owner, parent: ?abi.Borrowed, io: std.Io, retained_bytes: u64) !void {
        if (self.child != null) return error.InvalidRunnableTransition;
        const borrowed = parent orelse return;
        self.io = io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&self.io);
        var child: abi.Child = undefined;
        try borrowed.reserveChild(&.{ .io = &io_borrow, .retained_bytes = retained_bytes, .out_child = &child });
        self.child = child;
        errdefer self.retire();
        try self.child.?.validate();
        self.can_activate = true;
    }

    pub fn activate(self: *Owner) !?abi.Borrowed {
        if (self.running) return error.InvalidRunnableTransition;
        const child = if (self.child) |*value| value else return null;
        if (!self.can_activate) return error.InvalidRunnableTransition;
        self.can_activate = false;
        if (self.started) {
            const phase = try child.borrow.currentClass();
            try checked(child.borrow.enter(child.borrow.context, @backingInt(phase)));
        } else {
            try child.activate();
            self.started = true;
        }
        self.running = true;
        return child.borrow;
    }

    /// Completed CPU work still owns its results until the coordinator joins.
    pub fn park(self: *Owner) void {
        if (!self.running) return;
        self.running = false;
        const borrow = self.child.?.borrow;
        _ = borrow.park(borrow.context, @backingInt(abi.Wait.resource));
    }

    /// A reusable packet may move to another physical worker only after the
    /// coordinator has joined the previous task and destroyed its results.
    pub fn joined(self: *Owner) void {
        std.debug.assert(!self.running and self.child != null);
        self.can_activate = true;
    }

    pub fn retire(self: *Owner) void {
        if (self.child) |*child| child.deinit();
        self.child = null;
        self.running = false;
        self.started = false;
        self.can_activate = false;
    }
};

pub fn parkCoordinator(parent: ?abi.Borrowed) !?abi.Class {
    const borrow = parent orelse return null;
    try checked(borrow.checkpoint(borrow.context));
    const class = try borrow.currentClass();
    try checked(borrow.park(borrow.context, @backingInt(abi.Wait.resource)));
    return class;
}

pub fn resumeCoordinator(parent: ?abi.Borrowed, original_class: ?abi.Class) !void {
    const class = original_class orelse return;
    const borrow = parent orelse return error.InvalidRunnableTransition;
    try checked(borrow.enter(borrow.context, @backingInt(class)));
}

pub const RemoteWait = struct {
    borrow: ?@import("antfly_runtime_abi").runnable_abi.Borrowed,
    parked_class: ?@import("antfly_runtime_abi").runnable_abi.Class = null,
    /// A rejected ownership transition is terminal for this request view.
    /// Preserve its original cause even if a transport adapter tries again.
    rejection: ?anyerror = null,

    pub fn boundary(self: *@This()) ?@import("antfly_local_sources").common_execution_wait.Boundary {
        if (self.borrow == null) return null;
        return .{ .ptr = self, .begin = begin, .end = end };
    }
    fn begin(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.rejection) |cause| return cause;
        if (self.parked_class != null) {
            self.rejection = error.InvalidRunnableTransition;
            return error.InvalidRunnableTransition;
        }
        self.parked_class = parkCoordinator(self.borrow) catch |err| {
            self.rejection = err;
            return err;
        };
    }
    fn end(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.rejection) |cause| return cause;
        const phase = self.parked_class orelse {
            self.rejection = error.InvalidRunnableTransition;
            return error.InvalidRunnableTransition;
        };
        resumeCoordinator(self.borrow, phase) catch |err| {
            self.rejection = err;
            return err;
        };
        self.parked_class = null;
    }
};

/// A bounded owner-transition retry is a resource wait, not CPU work. Retain
/// the original operation while releasing its runnable slot for the sleep.
pub fn sleepForProgress(parent: ?abi.Borrowed, io: std.Io, duration: std.Io.Duration) !void {
    var wait = RemoteWait{ .borrow = parent };
    if (wait.boundary()) |boundary| {
        try boundary.run(std.Io.sleep, .{ io, duration, std.Io.Clock.awake });
    } else try io.sleep(duration, .awake);
}

pub fn checked(status: runtime.error_abi.Status) !void {
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
}

test "fanout owner retains joined result slots and original phase through denied resumption" {
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const a = std.testing.allocator;
    const Case = enum { success, cancel, expire, shutdown };
    const Worker = struct {
        fn run(owner: *Owner, root_context: *anyopaque, result: *?[]u8, failure: *?anyerror) void {
            execute(owner, root_context, result) catch |err| {
                failure.* = err;
            };
        }
        fn execute(owner: *Owner, root_context: *anyopaque, result: *?[]u8) !void {
            const borrow = (try owner.activate()).?;
            defer owner.park();
            try std.testing.expect(borrow.context != root_context);
            try borrow.checkLifetime();
            try checked(borrow.checkpoint(borrow.context));
            result.* = try std.testing.allocator.dupe(u8, "retained child result");
        }
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var root: native.Execution = .{};
        defer root.finish();
        const deadline = @as(u64, @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds)) + std.time.ns_per_s;
        try root.start(&scheduler, .{
            .io = std.testing.io,
            .deadline_ns = deadline,
            .retained_bytes = 23,
            .cancellation = .{ .ptr = &canceled, .requested = Worker.requested },
        }, phase);
        var read: @import("runnable_local_read.zig").Read = .{ .execution = &root, .allocator = a };
        const team = try read.team();
        var owners: [2]Owner = @splat(.{});
        defer for (&owners) |*owner| owner.retire();
        for (&owners) |*owner| try owner.reserve(team.borrow, std.testing.io, 1024);
        const original_class = try parkCoordinator(team.borrow);
        try std.testing.expectEqual(@as(usize, 3), scheduler.stats().operations);
        var results: [2]?[]u8 = @splat(null);
        defer for (results) |result| if (result) |bytes| a.free(bytes);
        var failures: [2]?anyerror = @splat(null);
        var group: std.Io.Group = .init;
        for (&owners, &results, &failures) |*owner, *result, *failure| group.async(std.testing.io, Worker.run, .{ owner, team.borrow.context, result, failure });
        try group.await(std.testing.io);
        for (failures) |failure| try std.testing.expect(failure == null);
        for (results) |result| try std.testing.expectEqualStrings("retained child result", result.?);
        const retained = scheduler.stats();
        try std.testing.expectEqual(@as(usize, 3), retained.operations);
        try std.testing.expectEqual(@as(usize, 0), retained.runnable_slots);
        var control: native.Execution = .{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = std.testing.io, .retained_bytes = 7 }, .control);
        try control.checkpoint();
        control.finish();
        switch (case) {
            .success => {},
            .cancel => canceled.store(true, .release),
            .expire => while (@as(u64, @intCast(std.Io.Clock.now(.awake, std.testing.io).nanoseconds)) < deadline) {
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            },
            .shutdown => scheduler.close(),
        }
        const grants_before = scheduler.stats().grants;
        const resumption = resumeCoordinator(team.borrow, original_class);
        switch (case) {
            .success => {
                try resumption;
                try std.testing.expectEqual(phase, try root.retainedClass());
            },
            .cancel => try std.testing.expectError(error.Canceled, resumption),
            .expire => try std.testing.expectError(error.DeadlineExceeded, resumption),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, resumption),
        }
        if (case != .success) try std.testing.expectEqual(grants_before, scheduler.stats().grants);
        try std.testing.expectEqual(retained.operations, scheduler.stats().operations);
        try std.testing.expectEqual(retained.retained_bytes, scheduler.stats().retained_bytes);
        // Result destruction precedes child retirement, after actual joining.
        for (&owners, &results) |*owner, *result| {
            a.free(result.*.?);
            result.* = null;
            owner.retire();
        }
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        root.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "remote wait restores the original request phase and rejects stopped authority" {
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Case = enum { success, cancel, expire, shutdown };
    const Signal = struct {
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
    };
    var absent = RemoteWait{ .borrow = null };
    try std.testing.expect(absent.boundary() == null);
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        const io = std.testing.io;
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var root = native.Execution{};
        defer root.finish();
        const deadline = @as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) + 100 * std.time.ns_per_ms;
        try root.start(&scheduler, .{ .io = io, .retained_bytes = 23, .deadline_ns = deadline, .cancellation = .{ .ptr = &canceled, .requested = Signal.requested } }, phase);
        var read = @import("runnable_local_read.zig").Read{ .execution = &root, .allocator = std.testing.allocator };
        const team = try read.team();
        var wait = RemoteWait{ .borrow = team.borrow };
        const boundary = wait.boundary().?;
        try boundary.beginWait();
        try std.testing.expectEqual(phase, try root.retainedClass());
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().resource_blocked);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        // Physical remote waits release capacity while retaining the original
        // operation. Protected control must still execute at C1.
        var control = native.Execution{};
        defer control.finish();
        try control.start(&scheduler, .{ .io = io, .retained_bytes = 7 }, .control);
        try control.checkpoint();
        control.finish();
        switch (case) {
            .success => {},
            .cancel => canceled.store(true, .release),
            .expire => while (@as(u64, @intCast(std.Io.Clock.awake.now(io).nanoseconds)) < deadline) try io.sleep(.fromMilliseconds(1), .awake),
            .shutdown => scheduler.close(),
        }
        const grants = scheduler.stats().grants;
        const resumption = boundary.endWait();
        switch (case) {
            .success => {
                try resumption;
                try std.testing.expect(wait.parked_class == null);
                try std.testing.expectEqual(phase, try root.retainedClass());
                // Reuse is synchronous and preserves the same request scope.
                try boundary.beginWait();
                try boundary.endWait();
            },
            .cancel => try std.testing.expectError(error.Canceled, resumption),
            .expire => try std.testing.expectError(error.DeadlineExceeded, resumption),
            .shutdown => try std.testing.expectError(error.SchedulerClosed, resumption),
        }
        if (case != .success) {
            try std.testing.expect(wait.parked_class != null);
            const cause = wait.rejection orelse return error.TestExpectedAuthorityRejection;
            try std.testing.expectError(cause, boundary.beginWait());
            try std.testing.expectError(cause, boundary.endWait());
            try std.testing.expectEqual(grants, scheduler.stats().grants);
        }
        try std.testing.expectEqual(@as(?u64, deadline), root.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        root.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test {
    _ = @import("algebraic_partial_ownership.zig");
}

test "owner transition sleep releases CPU and preserves original authority before retry" {
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const Case = enum { success, cancel, expire, shutdown, sleep_failure, shutdown_and_sleep_failure };
    const Fixture = struct {
        scheduler: *native.Scheduler,
        canceled: *std.atomic.Value(bool),
        case: Case,
        calls: usize = 0,
        fn requested(raw: *const anyopaque) bool {
            return @as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw))).load(.acquire);
        }
        fn sleep(raw: ?*anyopaque, _: std.Io.Timeout) std.Io.Cancelable!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            const stats = self.scheduler.stats();
            std.debug.assert(stats.runnable_slots == 0 and stats.resource_blocked == 1);
            std.debug.assert(stats.operations == 1 and stats.retained_bytes == 23);
            var control: native.Execution = .{};
            defer control.finish();
            control.start(self.scheduler, .{ .io = std.testing.io }, .control) catch unreachable;
            control.checkpoint() catch unreachable;
            control.finish();
            switch (self.case) {
                .success => try std.testing.io.sleep(.fromMilliseconds(1), .awake),
                .cancel => self.canceled.store(true, .release),
                .expire => try std.testing.io.sleep(.fromMilliseconds(100), .awake),
                .shutdown => self.scheduler.close(),
                .sleep_failure => return error.Canceled,
                .shutdown_and_sleep_failure => {
                    self.scheduler.close();
                    return error.Canceled;
                },
            }
        }
    };
    for ([_]native.Class{ .query, .inference }) |phase| for (std.enums.values(Case)) |case| {
        var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
        defer scheduler.close();
        var canceled = std.atomic.Value(bool).init(false);
        var root: native.Execution = .{};
        defer root.finish();
        const deadline = @as(u64, @intCast(std.Io.Clock.awake.now(std.testing.io).nanoseconds)) + 50 * std.time.ns_per_ms;
        try root.start(&scheduler, .{ .io = std.testing.io, .deadline_ns = deadline, .retained_bytes = 23, .cancellation = .{ .ptr = &canceled, .requested = Fixture.requested } }, phase);
        var read = @import("runnable_local_read.zig").Read{ .execution = &root, .allocator = std.testing.allocator };
        const team = try read.team();
        var fixture: Fixture = .{ .scheduler = &scheduler, .canceled = &canceled, .case = case };
        var vtable = std.testing.io.vtable.*;
        vtable.sleep = Fixture.sleep;
        const io: std.Io = .{ .userdata = &fixture, .vtable = &vtable };
        const result = sleepForProgress(team.borrow, io, .fromMilliseconds(1));
        switch (case) {
            .success => try result,
            .cancel, .sleep_failure => try std.testing.expectError(error.Canceled, result),
            .expire => try std.testing.expectError(error.DeadlineExceeded, result),
            .shutdown, .shutdown_and_sleep_failure => try std.testing.expectError(error.SchedulerClosed, result),
        }
        try std.testing.expectEqual(@as(usize, 1), fixture.calls);
        try std.testing.expectEqual(phase, try root.retainedClass());
        try std.testing.expectEqual(@as(?u64, deadline), root.entry.?.options.deadline_ns);
        try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
        root.finish();
        try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
    };
}

test "remote wait restores the original request phase with overlapping protected control" {
    const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
    const io = std.testing.io;
    var scheduler = try native.Scheduler.init(.{ .slots = 2, .control_slots = 1 });
    defer scheduler.close();
    var root: native.Execution = .{};
    defer root.finish();
    try root.start(&scheduler, .{ .io = io, .retained_bytes = 23 }, .query);
    var read = @import("runnable_local_read.zig").Read{ .execution = &root, .allocator = std.testing.allocator };
    const team = try read.team();
    var children: [2]Owner = @splat(.{});
    defer for (&children) |*child| child.retire();
    try children[0].reserve(team.borrow, io, 5);
    try children[1].reserve(team.borrow, io, 11);
    const retained_fanout_bytes = scheduler.stats().retained_bytes;
    try std.testing.expect(retained_fanout_bytes >= 23 + 5 + 11);
    const phase = try parkCoordinator(team.borrow);
    var first = RemoteWait{ .borrow = try children[0].activate() };
    const first_wait = first.boundary().?;
    try first_wait.beginWait();
    var second = RemoteWait{ .borrow = try children[1].activate() };
    const second_wait = second.boundary().?;
    var control: native.Execution = .{};
    defer control.finish();
    try control.start(&scheduler, .{ .io = io, .retained_bytes = 7, .allow_wait = false }, .control);
    try control.checkpoint();

    // Force the grant overlap that can occur between two real transport
    // callbacks. One ordinary CPU and one control CPU are both legal at C1.
    const overlap = scheduler.stats();
    try std.testing.expectEqual(@as(usize, 4), overlap.operations);
    try std.testing.expectEqual(@as(usize, 2), overlap.child_operations);
    try std.testing.expectEqual(retained_fanout_bytes + 7, overlap.retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), overlap.runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), overlap.control_slots);
    try std.testing.expectEqual(@as(usize, 1), overlap.runnable_slots - overlap.control_slots);
    try std.testing.expect(overlap.runnable_slots - overlap.control_slots <= scheduler.config.slots - scheduler.config.control_slots);
    try std.testing.expectEqual(@as(usize, 2), overlap.resource_blocked);

    try second_wait.beginWait();
    try first_wait.endWait();
    const resumed = scheduler.stats();
    try std.testing.expectEqual(@as(usize, 2), resumed.runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), resumed.control_slots);
    try std.testing.expectEqual(@as(usize, 2), resumed.resource_blocked);
    try std.testing.expectEqual(abi.Class.query, try first.borrow.?.currentClass());
    children[0].park();
    try second_wait.endWait();
    try std.testing.expectEqual(abi.Class.query, try second.borrow.?.currentClass());
    children[1].park();
    control.finish();
    // Result capsules remain charged through joining, even after their CPU
    // and transport work complete. Retire only after the coordinator joins.
    try std.testing.expectEqual(@as(usize, 3), scheduler.stats().operations);
    try std.testing.expectEqual(retained_fanout_bytes, scheduler.stats().retained_bytes);
    for (&children) |*child| child.retire();
    try resumeCoordinator(team.borrow, phase);
    try std.testing.expectEqual(@as(usize, 1), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 23), scheduler.stats().retained_bytes);
    root.finish();
    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), scheduler.stats().retained_bytes);
}
