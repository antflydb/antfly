// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! API-kernel-owned execution capsules. Foreign archives see only checked
//! C callbacks, stable statuses and opaque handles.
const std = @import("std");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;

pub const State = struct {
    owner_alloc: std.mem.Allocator,
    owner: runnable.NodeOwner,
    native_now_ns: *const fn () u64,

    pub fn port(self: *State) abi.Owner {
        return .{ .context = &self.owner, .retain = retain, .release = release, .start = start, .checkpoint = checkpoint, .park = park, .enter = enter, .finish = finish };
    }
};

const Capsule = struct {
    state: *State,
    io: runtime.io_abi.Receiver,
    clock_io: ?runtime.io_abi.Receiver,
    cancellation: runtime.http_abi.CancellationView,
    execution: runnable.Execution = .{},

    fn cancelled(raw: *const anyopaque) bool {
        const self: *const Capsule = @ptrCast(@alignCast(raw));
        return self.cancellation.requested();
    }
};

fn state(raw: *anyopaque) *State {
    const owner: *runnable.NodeOwner = @ptrCast(@alignCast(raw));
    return @fieldParentPtr("owner", owner);
}
fn capsule(raw: *anyopaque) *Capsule {
    return @ptrCast(@alignCast(raw));
}
fn retain(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    state(raw).owner.retain() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn release(raw: *anyopaque) callconv(.c) void {
    state(raw).owner.release();
}
fn start(raw: *anyopaque, options: *const abi.Start) callconv(.c) runtime.error_abi.Status {
    startChecked(raw, options) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn startChecked(raw: *anyopaque, options: *const abi.Start) !void {
    try options.validate();
    const owner = state(raw);
    try owner.owner.retain();
    errdefer owner.owner.release();
    const value = try owner.owner_alloc.create(Capsule);
    errdefer owner.owner_alloc.destroy(value);
    value.* = .{
        .state = owner,
        .io = try options.io.receive(),
        .clock_io = if (options.clock_io) |borrow| try borrow.receive() else null,
        .cancellation = options.cancellation,
    };
    errdefer value.execution.finish();
    try value.execution.startWithSlots(&owner.owner.scheduler, .{
        .io = value.io.io(),
        .clock_io = if (value.clock_io) |*clock| clock.io() else null,
        .native_now_ns = if (options.native_deadline != 0) owner.native_now_ns else null,
        .deadline_ns = if (options.has_deadline != 0) options.deadline_ns else null,
        .cancellation = .{ .ptr = value, .requested = Capsule.cancelled },
        .retained_bytes = options.retained_bytes +| @sizeOf(Capsule),
        .allow_wait = options.allow_wait != 0,
    }, std.enums.fromInt(runnable.Class, options.class) orelse return error.InvalidArgument, options.slots);
    options.out_handle.* = value;
}
fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    capsule(raw).execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn park(raw: *anyopaque, reason: c_int) callconv(.c) runtime.error_abi.Status {
    const wait = std.enums.fromInt(abi.Wait, reason) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
    capsule(raw).execution.park(switch (wait) {
        .resource => .resource_blocked,
        .output => .output_blocked,
    }) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn enter(raw: *anyopaque, class: c_int) callconv(.c) runtime.error_abi.Status {
    const next = std.enums.fromInt(runnable.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
    capsule(raw).execution.enter(next) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn finish(raw: *anyopaque) callconv(.c) void {
    const value = capsule(raw);
    const owner = value.state;
    value.execution.finish();
    owner.owner_alloc.destroy(value);
    owner.owner.release();
}

const TestClock = struct {
    var now_ns: u64 = 0;
    fn now() u64 {
        return now_ns;
    }
};

test "runnable execution port retains parked weighted capsules and protects control progress" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    const port = owner.port();
    try std.testing.expect(port.retain(port.context).isOk());
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.inference), .slots = 2, .retained_bytes = 17, .out_handle = &handle }).isOk());
    try std.testing.expectEqual(@as(usize, 2), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(u64, 17 + @sizeOf(Capsule)), owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.owner.tryDeinit());
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
    // A retained capsule still prevents destruction after the consumer releases.
    port.release(port.context);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.owner.tryDeinit());
    var control: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .out_handle = &control }).isOk());
    try std.testing.expect(port.enter(handle.?, @backingInt(abi.Class.background)).isOk());
    try std.testing.expectEqual(@as(usize, 3), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().control_slots);
    try std.testing.expect(port.checkpoint(handle.?).isOk());
    port.finish(control.?);
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.output)).isOk());
    owner.owner.close();
    try std.testing.expectEqual(error.SchedulerClosed, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.background))));
    port.finish(handle.?);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    try owner.owner.tryDeinit();
}

test "runnable execution port resumes with original deadline and cancellation" {
    TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    defer if (handle) |live| port.finish(live);
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.inference), .native_deadline = 1, .has_deadline = 1, .deadline_ns = 10, .out_handle = &handle }).isOk());
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    TestClock.now_ns = 11;
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.inference))));
    port.finish(handle.?);
    handle = null;

    const Cancel = struct {
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const flag: *const bool = @ptrCast(@alignCast(raw.?));
            return @intFromBool(flag.*);
        }
    };
    var cancelled = false;
    handle = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.background), .cancellation = .{ .context = &cancelled, .is_cancelled = Cancel.requested }, .out_handle = &handle }).isOk());
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.output)).isOk());
    cancelled = true;
    try std.testing.expectEqual(error.Canceled, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.background))));
    port.finish(handle.?);
    handle = null;
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
}

test "runnable execution port rejects malformed borrows and impossible demand without leaking ownership" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    const port = owner.port();
    const io = std.testing.io;
    var borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    var options: abi.Start = .{ .io = &borrow, .class = @backingInt(abi.Class.inference), .slots = 3, .out_handle = &handle };
    try std.testing.expectEqual(error.InvalidRunnableDemand, runtime.error_abi.errorFromStatus(port.start(port.context, &options)));
    options.slots = 1;
    borrow.contract.size += 1;
    try std.testing.expectEqual(error.InvalidArgument, runtime.error_abi.errorFromStatus(port.start(port.context, &options)));
    try std.testing.expect(handle == null);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    try owner.owner.tryDeinit();
}

test "runnable execution port allocation failure releases its owner lease" {
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var owner: State = .{ .owner_alloc = failing.allocator(), .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(error.OutOfMemory, runtime.error_abi.errorFromStatus(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.background), .out_handle = &handle })));
    try std.testing.expect(handle == null);
    try std.testing.expect(failing.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try owner.owner.tryDeinit();
}

test "runnable execution consumer lease validates before retaining and refuses shutdown" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    var port = owner.port();
    port.version += 1;
    try std.testing.expectError(error.UnsupportedVersion, abi.Lease.init(port));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.borrowers);
    port = owner.port();
    var lease = try abi.Lease.init(port);
    try std.testing.expectError(error.InvalidRunnableTransition, owner.owner.tryDeinit());
    owner.owner.close();
    try std.testing.expectError(error.SchedulerClosed, abi.Lease.init(port));
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
    lease.deinit();
    try owner.owner.tryDeinit();
}

test "runnable execution borrowed child transfers one weighted operation without nested admission" {
    TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.query), .slots = 2, .retained_bytes = 17, .out_handle = &handle }).isOk());
    defer port.finish(handle.?);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    const parent = try abi.Borrowed.fromOwner(port, handle.?, 2, .query);
    var child = try abi.Transfer.init(parent, .inference);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 2), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 2), child.borrow.cpu_slots);
    try std.testing.expectEqual(@as(u64, 1), owner.owner.scheduler.stats().grants[2]);
    var control: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .out_handle = &control }).isOk());
    defer port.finish(control.?);
    try child.checkpoint();
    try child.returnToParent();
    try std.testing.expectEqual(@as(u64, 2), owner.owner.scheduler.stats().grants[0]);
    try std.testing.expectError(error.InvalidRunnableTransition, child.returnToParent());
    try std.testing.expectError(error.InvalidRunnableTransition, child.checkpoint());
}

test "runnable execution borrowed child rejects stale deadlines headers and shutdown without retiring its parent" {
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    for (0..4) |scenario| {
        TestClock.now_ns = 0;
        var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
        var handle: ?*anyopaque = null;
        const port = owner.port();
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .out_handle = &handle }).isOk());
        errdefer {
            port.finish(handle.?);
            owner.owner.deinit();
        }
        var parent = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
        if (scenario == 0) {
            parent.version += 1;
            try std.testing.expectError(error.UnsupportedVersion, abi.Transfer.init(parent, .inference));
            try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().grants[2]);
        } else if (scenario == 1) {
            TestClock.now_ns = 11;
            try std.testing.expectError(error.DeadlineExceeded, abi.Transfer.init(parent, .inference));
        } else {
            var child = try abi.Transfer.init(parent, .inference);
            if (scenario == 2) {
                TestClock.now_ns = 11;
                try std.testing.expectError(error.DeadlineExceeded, child.returnToParent());
            } else {
                owner.owner.close();
                try std.testing.expectError(error.SchedulerClosed, child.returnToParent());
            }
            try std.testing.expectError(error.InvalidRunnableTransition, child.returnToParent());
        }
        // A failed resumed acquisition may retire its scheduler entry. The
        // capsule and owner lease still belong to the original caller.
        try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
        try std.testing.expectError(error.InvalidRunnableTransition, owner.owner.tryDeinit());
        port.finish(handle.?);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
        try owner.owner.tryDeinit();
    }
}

test "runnable execution port forwards fail-fast control admission without publishing rejected capsules" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8, .max_wait_ms = 1000 }), .native_now_ns = TestClock.now };
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var first: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .retained_bytes = 31, .out_handle = &first }).isOk());
    defer if (first) |handle| port.finish(handle);
    const before = owner.owner.scheduler.stats();
    var rejected: ?*anyopaque = null;
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, runtime.error_abi.errorFromStatus(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .retained_bytes = 19, .out_handle = &rejected })));
    try std.testing.expect(rejected == null);
    try std.testing.expectEqual(before.operations, owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().queued);
    port.finish(first.?);
    first = null;
    try std.testing.expectEqual(@as(usize, 0), owner.owner.borrowers);
    try owner.owner.tryDeinit();
}
