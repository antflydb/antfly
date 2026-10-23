// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

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
    cleanup_in_use: std.atomic.Value(bool) = .init(false),

    pub fn port(self: *State) abi.Owner {
        return .{ .context = &self.owner, .retain = retain, .release = release, .start = start, .reserve_cleanup_root = reserveCleanupRoot, .enter_cleanup_root = enterCleanupRoot, .checkpoint = checkpoint, .park = park, .enter = enter, .enter_checked = enterChecked, .finish = finish, .resize_team = resizeTeam, .reserve_completion = reserveCompletion, .activate_completion = activateCompletion, .reserve_cleanup = reserveCleanup, .activate_cleanup = activateCleanup, .completion_remaining_ns = completionRemaining, .reserve_child = reserveChild, .activate_child = activateChild, .activate_child_checked = activateChildChecked, .check_lifetime = checkLifetime, .current_class = currentClass, .cpu_borrow = cpuBorrow, .request_resources = requestResources, .enter_resources = enterResources };
    }
};

const Capsule = struct {
    state: *State,
    io: runtime.io_abi.Receiver,
    clock_io: ?runtime.io_abi.Receiver,
    cancellation: runtime.http_abi.CancellationView,
    execution: runnable.Execution = .{},
    cleanup: bool = false,
    // Foreign Receiver.io() may point back into the receiver. Keep these at
    // stable capsule addresses until every consumer is joined and finish runs.
    completion_io: ?runtime.io_abi.Receiver = null,
    completion_clock_io: ?runtime.io_abi.Receiver = null,
    cleanup_phase_io: ?runtime.io_abi.Receiver = null,
    cleanup_phase_clock_io: ?runtime.io_abi.Receiver = null,

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
    const resource = if (options.resources) |value| try @import("runnable_resource_bridge.zig").receive(value) else null;
    const owner = state(raw);
    const cleanup = options.cleanup != 0;
    if (cleanup) try owner.owner.retainCleanup() else try owner.owner.retain();
    errdefer owner.owner.release();
    if (cleanup) {
        const config = owner.owner.scheduler.config;
        // One cleanup root may retain at most half the control byte reserve.
        // Small policies with only one control operation reserve it for health.
        if (@min(@as(usize, 4), config.max_operations / 2) < 2) return error.ResourceTemporarilyUnavailable;
        if (options.retained_bytes +| @sizeOf(Capsule) > config.control_bytes / 2) return error.SchedulerFull;
        if (owner.cleanup_in_use.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return error.ResourceTemporarilyUnavailable;
    }
    errdefer if (cleanup) owner.cleanup_in_use.store(false, .release);
    const value = try owner.owner_alloc.create(Capsule);
    errdefer owner.owner_alloc.destroy(value);
    value.* = .{
        .state = owner,
        .io = try options.io.receive(),
        .clock_io = if (options.clock_io) |borrow| try borrow.receive() else null,
        .cancellation = options.cancellation,
        .cleanup = cleanup,
    };
    errdefer value.execution.finish();
    if (cleanup) {
        const clock = if (value.clock_io) |*borrow| borrow.io() else value.io.io();
        const now: u64 = if (options.native_deadline != 0) owner.native_now_ns() else @intCast(@max(0, std.Io.Clock.now(.awake, clock).nanoseconds));
        if (options.deadline_ns > now +| (250 * std.time.ns_per_ms)) return error.InvalidArgument;
    }
    const execution_options: @import("antfly_local_sources").common_bounded_admission.Options = .{
        .io = value.io.io(),
        .clock_io = if (value.clock_io) |*clock| clock.io() else null,
        .native_now_ns = if (options.native_deadline != 0) owner.native_now_ns else null,
        .deadline_ns = if (options.has_deadline != 0) options.deadline_ns else null,
        .cancellation = .{ .ptr = value, .requested = Capsule.cancelled },
        .retained_bytes = options.retained_bytes +| @sizeOf(Capsule),
        .allow_wait = options.allow_wait != 0,
    };
    if (cleanup) {
        if (options.class != @backingInt(abi.Class.control) or options.slots != 1) return error.InvalidArgument;
        try value.execution.startCleanupRoot(&owner.owner.scheduler, execution_options);
    } else if (resource) |participant| {
        try value.execution.startWithResources(&owner.owner.scheduler, execution_options, std.enums.fromInt(runnable.Class, options.class) orelse return error.InvalidArgument, options.slots, participant);
    } else try value.execution.startWithSlots(&owner.owner.scheduler, execution_options, std.enums.fromInt(runnable.Class, options.class) orelse return error.InvalidArgument, options.slots);
    options.out_handle.* = value;
}
fn reserveCleanupRoot(raw: *anyopaque, options: *const abi.CleanupReservation) callconv(.c) runtime.error_abi.Status {
    reserveCleanupRootChecked(raw, options) catch |cause| return runtime.error_abi.statusFromError(cause);
    return .ok;
}
fn reserveCleanupRootChecked(raw: *anyopaque, options: *const abi.CleanupReservation) !void {
    try options.validate();
    const owner = state(raw);
    try owner.owner.retain();
    errdefer owner.owner.release();
    const value = try owner.owner_alloc.create(Capsule);
    errdefer owner.owner_alloc.destroy(value);
    value.* = .{ .state = owner, .io = try options.io.receive(), .clock_io = if (options.clock_io) |clock| try clock.receive() else null, .cancellation = .{} };
    errdefer value.execution.finish();
    const bytes = std.math.add(u64, options.retained_bytes, @sizeOf(Capsule)) catch return error.InvalidRunnableDemand;
    try value.execution.reserveCleanupRoot(&owner.owner.scheduler, .{
        .io = value.io.io(),
        .clock_io = if (value.clock_io) |*clock| clock.io() else null,
        .native_now_ns = if (options.native_clock != 0) owner.native_now_ns else null,
        .retained_bytes = bytes,
        .allow_wait = true,
    }, options.budget_ns);
    options.out_handle.* = value;
}

fn requestResources(raw: *anyopaque, resources: *const abi.ResourceBundle) callconv(.c) runtime.error_abi.Status {
    const participant = @import("runnable_resource_bridge.zig").receive(resources) catch |err| return runtime.error_abi.statusFromError(err);
    capsule(raw).execution.requestResources(participant) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn enterResources(raw: *anyopaque, resources: *const abi.ResourceBundle, restriction: ?*const abi.Reentry) callconv(.c) runtime.error_abi.Status {
    const participant = @import("runnable_resource_bridge.zig").receive(resources) catch |err| return runtime.error_abi.statusFromError(err);
    @import("runnable_reentry.zig").enterWithResources(&capsule(raw).execution, participant, restriction) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn cpuBorrow(raw: *anyopaque, options: *const abi.CpuBorrow) callconv(.c) runtime.error_abi.Status {
    options.validate() catch |err| return runtime.error_abi.statusFromError(err);
    const value = capsule(raw);
    if (options.enabled != 0) {
        value.execution.enableCpuBorrow(.{ .max_non_yield_ns = options.max_non_yield_ns }) catch |err| return runtime.error_abi.statusFromError(err);
    } else value.execution.disableCpuBorrow() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}

fn reserveChild(raw: *anyopaque, options: *const abi.ChildStart) callconv(.c) runtime.error_abi.Status {
    reserveChildChecked(raw, options) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn reserveChildChecked(raw: *anyopaque, options: *const abi.ChildStart) !void {
    try options.validate();
    const parent = capsule(raw);
    if (parent.cleanup) return error.UnsupportedOperation;
    const owner = parent.state;
    try owner.owner.retain();
    errdefer owner.owner.release();
    const value = try owner.owner_alloc.create(Capsule);
    errdefer owner.owner_alloc.destroy(value);
    value.* = .{ .state = owner, .io = try options.io.receive(), .clock_io = null, .cancellation = .{} };
    errdefer value.execution.finish();
    const bytes = std.math.add(u64, options.retained_bytes, @sizeOf(Capsule)) catch return error.InvalidRunnableDemand;
    try value.execution.reserveChild(&parent.execution, value.io.io(), bytes);
    options.out_child.* = .{
        .borrow = try abi.Borrowed.fromOwner(owner.port(), value, 1, @fromBackingInt(@intCast(@backingInt(value.execution.entry.?.class)))),
        .finish = finish,
    };
}
fn activateChild(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    capsule(raw).execution.activateChild() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}

fn activateChildChecked(raw: *anyopaque, restriction: *const abi.Reentry) callconv(.c) runtime.error_abi.Status {
    @import("runnable_reentry.zig").activateChild(&capsule(raw).execution, restriction) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}

fn checkLifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    capsule(raw).execution.checkLifetime() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn currentClass(raw: *anyopaque, out: *c_int) callconv(.c) runtime.error_abi.Status {
    out.* = @intCast(@backingInt(capsule(raw).execution.retainedClass() catch |err| return runtime.error_abi.statusFromError(err)));
    return .ok;
}
fn checkpoint(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    capsule(raw).execution.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn reserveCompletion(raw: *anyopaque, options: *const abi.Completion) callconv(.c) runtime.error_abi.Status {
    reserveCompletionChecked(raw, options) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn reserveCompletionChecked(raw: *anyopaque, options: *const abi.Completion) !void {
    try options.validate();
    const value = capsule(raw);
    if (value.cleanup) return error.UnsupportedOperation;
    // Never replace an executor already retained by the execution.
    if (value.completion_io != null) return error.InvalidRunnableTransition;
    const io = try options.io.receive();
    const clock_io = if (options.clock_io) |borrow| try borrow.receive() else null;
    value.completion_io = io;
    value.completion_clock_io = clock_io;
    errdefer {
        value.completion_io = null;
        value.completion_clock_io = null;
    }
    try value.execution.reserveCompletion(.{
        .io = value.completion_io.?.io(),
        .clock_io = if (value.completion_clock_io) |*clock| clock.io() else null,
        .native_now_ns = if (options.native_deadline != 0) value.state.native_now_ns else null,
        .deadline_ns = options.deadline_ns,
        .allow_wait = true,
    });
}
fn completionRemaining(raw: *anyopaque, out: *u64) callconv(.c) runtime.error_abi.Status {
    const value = capsule(raw);
    if (value.cleanup) return runtime.error_abi.statusFromError(error.UnsupportedOperation);
    out.* = value.execution.completionRemainingNs() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn activateCompletion(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
    const value = capsule(raw);
    if (value.cleanup) return runtime.error_abi.statusFromError(error.UnsupportedOperation);
    value.execution.activateCompletion() catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn reserveCleanup(raw: *anyopaque, options: *const abi.Cleanup) callconv(.c) runtime.error_abi.Status {
    reserveCleanupChecked(raw, options) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn reserveCleanupChecked(raw: *anyopaque, options: *const abi.Cleanup) !void {
    try options.validate();
    const value = capsule(raw);
    if (value.cleanup) return error.UnsupportedOperation;
    // Never replace an executor already retained by the execution.
    if (value.cleanup_phase_io != null) return error.InvalidRunnableTransition;
    const io = try options.io.receive();
    const clock_io = if (options.clock_io) |borrow| try borrow.receive() else null;
    value.cleanup_phase_io = io;
    value.cleanup_phase_clock_io = clock_io;
    errdefer {
        value.cleanup_phase_io = null;
        value.cleanup_phase_clock_io = null;
    }
    const policy = @import("antfly_local_sources").common_request_admission.bounded_admission.Options{
        .io = value.cleanup_phase_io.?.io(),
        .clock_io = if (value.cleanup_phase_clock_io) |*clock| clock.io() else null,
        .native_now_ns = if (options.native_deadline != 0) value.state.native_now_ns else null,
        .deadline_ns = if (options.activation_budget == 0) options.deadline_ns else null,
        .allow_wait = true,
    };
    if (options.activation_budget != 0) try value.execution.reserveCleanupFor(policy, options.deadline_ns) else try value.execution.reserveCleanup(policy);
}
fn activateCleanup(raw: *anyopaque, out_slots: *usize) callconv(.c) runtime.error_abi.Status {
    const value = capsule(raw);
    if (value.cleanup) return runtime.error_abi.statusFromError(error.UnsupportedOperation);
    value.execution.activateCleanup() catch |err| return runtime.error_abi.statusFromError(err);
    out_slots.* = value.execution.entry.?.slots;
    return .ok;
}
fn enterCleanupRoot(raw: *anyopaque, restriction: ?*const abi.Reentry, out_slots: *usize, out_deadline: *u64) callconv(.c) runtime.error_abi.Status {
    const value = capsule(raw);
    const deadline = @import("runnable_reentry.zig").enterReservedCleanup(&value.execution, restriction) catch |cause| return runtime.error_abi.statusFromError(cause);
    out_slots.* = value.execution.entry.?.slots;
    out_deadline.* = deadline;
    return .ok;
}
fn resizeTeam(raw: *anyopaque, slots: usize) callconv(.c) runtime.error_abi.Status {
    const value = capsule(raw);
    // Cleanup's independent subquota is deliberately single-slot even when
    // the shared policy has multiple reserved control slots.
    if (value.cleanup and slots != 1) return runtime.error_abi.statusFromError(error.InvalidRunnableDemand);
    value.execution.resizeTeam(slots) catch |err| return runtime.error_abi.statusFromError(err);
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
fn enterChecked(raw: *anyopaque, class: c_int, restriction: *const runtime.runnable_abi.Reentry) callconv(.c) runtime.error_abi.Status {
    const next = std.enums.fromInt(runnable.Class, class) orelse return runtime.error_abi.statusFromError(error.InvalidArgument);
    @import("runnable_reentry.zig").enter(&capsule(raw).execution, next, restriction) catch |err| return runtime.error_abi.statusFromError(err);
    return .ok;
}
fn finish(raw: *anyopaque) callconv(.c) void {
    const value = capsule(raw);
    const owner = value.state;
    value.execution.finish();
    const cleanup = value.cleanup;
    owner.owner_alloc.destroy(value);
    if (cleanup) owner.cleanup_in_use.store(false, .release);
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

fn cleanupOptions(borrow: *const runtime.native_abi.IoBorrow, handle: *?*anyopaque, bytes: u64) abi.Start {
    return .{ .io = borrow, .class = @backingInt(abi.Class.control), .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .native_deadline = 1, .deadline_ns = 100 * std.time.ns_per_ms, .retained_bytes = bytes, .out_handle = handle };
}

test "runnable execution cleanup subquota preserves health headroom with all ordinary operations retained" {
    TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var ordinary: [4]?*anyopaque = @splat(null);
    defer for (ordinary) |handle| if (handle) |value| port.finish(value);
    for (&ordinary) |*handle| {
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.query), .slots = 2, .retained_bytes = 17, .out_handle = handle }).isOk());
        try std.testing.expect(port.park(handle.*.?, @backingInt(abi.Wait.output)).isOk());
    }
    var cleanup: ?*anyopaque = null;
    defer if (cleanup) |handle| port.finish(handle);
    try std.testing.expect(port.start(port.context, &cleanupOptions(&borrow, &cleanup, 31)).isOk());
    try std.testing.expect(port.park(cleanup.?, @backingInt(abi.Wait.output)).isOk());
    const retained = owner.owner.scheduler.stats();
    var denied: ?*anyopaque = null;
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, runtime.error_abi.errorFromStatus(port.start(port.context, &cleanupOptions(&borrow, &denied, 31))));
    try std.testing.expect(denied == null);
    try std.testing.expectEqual(retained.operations, owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained.retained_bytes, owner.owner.scheduler.stats().retained_bytes);
    var health: [3]?*anyopaque = @splat(null);
    defer for (health) |handle| if (handle) |value| port.finish(value);
    for (&health) |*handle| {
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .retained_bytes = 11, .out_handle = handle }).isOk());
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().control_slots);
        try std.testing.expect(port.park(handle.*.?, @backingInt(abi.Wait.output)).isOk());
    }
    try std.testing.expectEqual(@as(usize, 8), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 4), owner.owner.scheduler.stats().control_operations);
    for (&health) |*handle| {
        port.finish(handle.*.?);
        handle.* = null;
    }
    port.finish(cleanup.?);
    cleanup = null;
    try std.testing.expect(!owner.cleanup_in_use.load(.acquire));
    for (&ordinary) |*handle| {
        port.finish(handle.*.?);
        handle.* = null;
    }
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.borrowers);
}

test "runnable execution cleanup rejects oversized unbounded and failed allocations without consuming its subquota" {
    TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expectEqual(error.SchedulerFull, runtime.error_abi.errorFromStatus(port.start(port.context, &cleanupOptions(&borrow, &handle, owner.owner.scheduler.config.control_bytes / 2))));
    var options = cleanupOptions(&borrow, &handle, 31);
    options.deadline_ns = 251 * std.time.ns_per_ms;
    try std.testing.expectEqual(error.InvalidArgument, runtime.error_abi.errorFromStatus(port.start(port.context, &options)));
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    owner.owner_alloc = failing.allocator();
    try std.testing.expectEqual(error.OutOfMemory, runtime.error_abi.errorFromStatus(port.start(port.context, &cleanupOptions(&borrow, &handle, 31))));
    try std.testing.expect(failing.has_induced_failure);
    owner.owner_alloc = std.testing.allocator;
    try std.testing.expect(handle == null);
    try std.testing.expect(!owner.cleanup_in_use.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.borrowers);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expect(port.start(port.context, &cleanupOptions(&borrow, &handle, 31)).isOk());
    port.finish(handle.?);
    try std.testing.expect(!owner.cleanup_in_use.load(.acquire));
    var small: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 2 }), .native_now_ns = TestClock.now };
    defer small.owner.tryDeinit() catch @panic("small owner leaked");
    const small_port = small.port();
    handle = null;
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, runtime.error_abi.errorFromStatus(small_port.start(small_port.context, &cleanupOptions(&borrow, &handle, 31))));
    try std.testing.expect(!small.cleanup_in_use.load(.acquire));
    try std.testing.expect(small_port.start(small_port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &handle }).isOk());
    small_port.finish(handle.?);
}

test "runnable execution cleanup keeps its gate and bytes through denied reentry until owning finish" {
    TestClock.now_ns = 0;
    defer TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    defer if (handle) |value| port.finish(value);
    try std.testing.expect(port.start(port.context, &cleanupOptions(&borrow, &handle, 31)).isOk());
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.output)).isOk());
    const retained = owner.owner.scheduler.stats().retained_bytes;
    TestClock.now_ns = 100 * std.time.ns_per_ms;
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.control))));
    TestClock.now_ns = 0;
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(port.checkpoint(handle.?)));
    try std.testing.expect(owner.cleanup_in_use.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    var denied: ?*anyopaque = null;
    try std.testing.expectEqual(error.ResourceTemporarilyUnavailable, runtime.error_abi.errorFromStatus(port.start(port.context, &cleanupOptions(&borrow, &denied, 31))));
    try std.testing.expect(denied == null);
    var health: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }).isOk());
    port.finish(health.?);
    port.finish(handle.?);
    handle = null;
    try std.testing.expect(!owner.cleanup_in_use.load(.acquire));
    try std.testing.expect(port.start(port.context, &cleanupOptions(&borrow, &handle, 31)).isOk());
    port.finish(handle.?);
    handle = null;
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
}

test "runnable execution HTTP cleanup callbacks borrow the real shared owner until writer finish" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    var adapter = try runtime.http_cleanup.Owner.init(port);
    defer adapter.deinit();
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var ordinary: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.query), .slots = 2, .out_handle = &ordinary }).isOk());
    defer port.finish(ordinary.?);
    const view = adapter.view();
    const deadline: u64 = @intCast(@max(0, std.Io.Clock.now(.awake, io).nanoseconds) + 249 * std.time.ns_per_ms);
    const handle = try view.start(view.context, io, deadline, 4096);
    defer view.finish(view.context, handle);
    try std.testing.expect(owner.cleanup_in_use.load(.acquire));
    try std.testing.expectEqual(@as(usize, 3), owner.owner.scheduler.stats().runnable_slots);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    try view.park(view.context, handle);
    try std.testing.expectEqual(@as(usize, 2), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, view.start(view.context, io, deadline, 4096));
    var health: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }).isOk());
    port.finish(health.?);
    try view.enter(view.context, handle);
    try std.testing.expectEqual(@as(usize, 3), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
}

test "runnable execution checked team resize retains the same capsule and memory owner" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 5, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.write), .allow_wait = 0, .retained_bytes = 47, .out_handle = &handle }).isOk());
    defer port.finish(handle.?);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    var loan = try abi.Borrowed.fromOwner(port, handle.?, 1, .write);
    try loan.resizeTeam(4);
    try std.testing.expectEqual(@as(usize, 4), loan.cpu_slots);
    try std.testing.expectEqual(@as(usize, 4), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectError(error.InvalidRunnableDemand, loan.resizeTeam(5));
    try std.testing.expectEqual(@as(usize, 4), loan.cpu_slots);
    try loan.resizeTeam(1);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().runnable_slots);
    var health: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .out_handle = &health }).isOk());
    port.finish(health.?);
    owner.owner.close();
    try std.testing.expectError(error.SchedulerClosed, loan.resizeTeam(3));
    try std.testing.expectEqual(@as(usize, 1), loan.cpu_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
}

test "runnable execution checked team resize cannot expand protected cleanup" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 5, .control_slots = 2, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &cleanupOptions(&borrow, &handle, 31)).isOk());
    defer port.finish(handle.?);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    try std.testing.expectError(error.InvalidRunnableDemand, port.resizeTeam(handle.?, 2));
    try std.testing.expect(owner.cleanup_in_use.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
}

test "runnable execution fanout team reserves coordinator and children without a new root" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 5, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.write), .retained_bytes = 61, .out_handle = &handle }).isOk());
    defer if (handle) |active| port.finish(active);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    var team: abi.Team = .{ .borrow = try abi.Borrowed.fromOwner(port, handle.?, 1, .write), .maximum_slots = 4, .allocator = std.testing.allocator };
    try team.reserve(8);
    try std.testing.expectEqual(@as(usize, 3), team.childCapacity());
    try std.testing.expectEqual(@as(usize, 4), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    const allocation = try team.allocator.alloc(u8, 32);
    team.allocator.free(allocation);
    try std.testing.expectError(error.InvalidRunnableTransition, team.reserve(1));
    var health: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }).isOk());
    port.finish(health.?);
    owner.owner.close();
    team.finish();
    try std.testing.expectError(error.SchedulerClosed, team.reserve(8));
    // Joining fanout does not abandon an accepted write or attempt fresh
    // foreground admission after shutdown. Only owning finish reclaims it.
    try std.testing.expectEqual(@as(usize, 4), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    port.finish(handle.?);
    handle = null;
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
}

test "runnable execution fanout team admission rejects unsupported and excessive demand before activation" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.write), .out_handle = &handle }).isOk());
    defer port.finish(handle.?);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    var team: abi.Team = .{ .borrow = try abi.Borrowed.fromOwner(port, handle.?, 1, .write), .maximum_slots = 4, .allocator = std.testing.allocator };
    try std.testing.expectError(error.InvalidRunnableDemand, team.reserve(3));
    try std.testing.expectEqual(@TypeOf(team.state).fresh, team.state);
    try std.testing.expectEqual(@as(usize, 1), team.borrow.cpu_slots);
    team.borrow.resize_team = null;
    try std.testing.expectError(error.UnsupportedOperation, team.reserve(1));
    try std.testing.expectEqual(@TypeOf(team.state).fresh, team.state);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    team.borrow.resize_team = port.resize_team;
    team.maximum_slots = 1;
    try team.reserve(8);
    try std.testing.expectEqual(@as(usize, 0), team.childCapacity());
    team.finish();
    try team.reserve(1);
    try std.testing.expectEqual(@as(usize, 0), team.childCapacity());
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    team.finish();
}

test "runnable execution completion ABI retains one capsule through foreground cancellation and close" {
    TestClock.now_ns = 0;
    defer TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    const Cancel = struct {
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            return @intFromBool(@as(*const bool, @ptrCast(@alignCast(raw.?))).*);
        }
    };
    var cancelled = false;
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.write), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .retained_bytes = 64, .cancellation = .{ .context = &cancelled, .is_cancelled = Cancel.requested }, .out_handle = &handle }).isOk());
    defer port.finish(handle.?);
    const retained = owner.owner.scheduler.stats().retained_bytes;
    const child = try abi.Borrowed.fromOwner(port, handle.?, 1, .write);
    try child.reserveCompletion(&.{ .io = &borrow, .native_deadline = 1, .deadline_ns = 100 });
    try std.testing.expectError(error.InvalidRunnableTransition, child.reserveCompletion(&.{ .io = &borrow, .native_deadline = 1, .deadline_ns = 200 }));
    try std.testing.expectError(error.ExecutionWaitUnsupported, child.completionRemainingNs());
    try port.activateCompletion(handle.?);
    try std.testing.expectEqual(@as(u64, 100), try port.completionRemainingNs(handle.?));
    try std.testing.expectEqual(@as(u64, 100), try child.completionRemainingNs());
    try std.testing.expectError(error.InvalidRunnableTransition, child.activateCompletion());
    cancelled = true;
    TestClock.now_ns = 11;
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    var health: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }).isOk());
    port.finish(health.?);
    owner.owner.close();
    try std.testing.expectEqual(@as(u64, 89), try child.completionRemainingNs());
    try std.testing.expect(port.enter(handle.?, @backingInt(abi.Class.write)).isOk());
    try std.testing.expect(port.checkpoint(handle.?).isOk());
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    TestClock.now_ns = 100;
    try std.testing.expectError(error.DeadlineExceeded, port.completionRemainingNs(handle.?));
    TestClock.now_ns = 101;
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.write))));
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(retained, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
}

test "runnable execution completion ABI checks foreground fences and denies cleanup authority" {
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    for (0..3) |scenario| {
        TestClock.now_ns = 0;
        defer TestClock.now_ns = 0;
        var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .max_operations = 8 }), .native_now_ns = TestClock.now };
        defer owner.owner.deinit();
        const port = owner.port();
        var handle: ?*anyopaque = null;
        const cleanup = scenario == 2;
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(if (cleanup) abi.Class.control else abi.Class.write), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .cleanup = @intFromBool(cleanup), .allow_wait = @intFromBool(!cleanup), .out_handle = &handle }).isOk());
        defer port.finish(handle.?);
        if (cleanup) {
            try std.testing.expectError(error.UnsupportedOperation, port.reserveCompletion(handle.?, &.{ .io = &borrow, .native_deadline = 1, .deadline_ns = 100 }));
            try std.testing.expectError(error.UnsupportedOperation, port.activateCompletion(handle.?));
        } else if (scenario == 0) {
            TestClock.now_ns = 11;
            try std.testing.expectError(error.DeadlineExceeded, port.reserveCompletion(handle.?, &.{ .io = &borrow, .native_deadline = 1, .deadline_ns = 100 }));
            try std.testing.expect(capsule(handle.?).completion_io == null);
        } else {
            try port.reserveCompletion(handle.?, &.{ .io = &borrow, .native_deadline = 1, .deadline_ns = 100 });
            TestClock.now_ns = 11;
            try std.testing.expectError(error.DeadlineExceeded, port.activateCompletion(handle.?));
        }
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    }
}

test "runnable execution port child reservation publishes before worker CPU and retains original authority through physical join" {
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
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            return @intFromBool(@as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw.?))).load(.acquire));
        }
    };
    for ([_]?anyerror{ null, error.Canceled, error.DeadlineExceeded }) |failure| {
        TestClock.now_ns = 0;
        var canceled: std.atomic.Value(bool) = .init(false);
        var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = TestClock.now };
        defer owner.owner.tryDeinit() catch @panic("owner leaked");
        const port = owner.port();
        const io = std.testing.io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        var parent: ?*anyopaque = null;
        try Worker.checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 10, .cancellation = .{ .context = &canceled, .is_cancelled = Worker.requested }, .out_handle = &parent }));
        defer port.finish(parent.?);
        const parent_borrow = try abi.Borrowed.fromOwner(port, parent.?, 1, .query);
        var child: abi.Child = undefined;
        try parent_borrow.reserveChild(&.{ .io = &io_borrow, .retained_bytes = 41, .out_child = &child });
        defer child.deinit();
        try std.testing.expect(capsule(child.borrow.context).execution.child_pending);
        // Reservation owns the child before a worker exists. CPU queueing
        // starts at activation; leaving a reservation queued can consume CPU
        // or delay runnable peers before the task is physically started.
        try std.testing.expectEqual(runnable.State.resource_blocked, capsule(child.borrow.context).execution.entry.?.state);
        const reserved = owner.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), reserved.queued);
        try std.testing.expectEqual(@as(usize, 1), reserved.resource_blocked);
        try std.testing.expectEqual(@as(usize, 1), capsule(parent.?).execution.entry.?.children);
        try std.testing.expectError(error.InvalidRunnableTransition, Worker.checked(child.borrow.checkpoint(child.borrow.context)));
        const charged = owner.owner.scheduler.stats().retained_bytes;
        try Worker.checked(port.park(parent.?, @backingInt(abi.Wait.resource)));
        var worker = Worker{ .child = &child };
        var future = try std.testing.io.concurrent(Worker.run, .{&worker});
        defer {
            worker.go.set(std.testing.io);
            future.await(std.testing.io) catch {};
        }
        try worker.ready.waitTimeout(std.testing.io, .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } });
        try Worker.checked(port.enter(parent.?, @backingInt(abi.Class.query)));
        try std.testing.expectEqual(charged, owner.owner.scheduler.stats().retained_bytes);
        var health: ?*anyopaque = null;
        try Worker.checked(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }));
        defer port.finish(health.?);
        try Worker.checked(port.checkpoint(health.?));
        try Worker.checked(port.park(parent.?, @backingInt(abi.Wait.resource)));
        if (failure) |err| {
            if (err == error.Canceled) canceled.store(true, .release) else TestClock.now_ns = 10;
        }
        worker.go.set(std.testing.io);
        if (failure) |err| try std.testing.expectError(err, future.await(std.testing.io)) else try future.await(std.testing.io);
        try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().child_operations);
        try std.testing.expectEqual(@as(usize, 1), capsule(parent.?).execution.entry.?.children);
        if (failure) |err| try std.testing.expectError(err, child.activate()) else try std.testing.expectError(error.InvalidRunnableTransition, child.activate());
        child.deinit();
        try std.testing.expectEqual(@as(usize, 0), capsule(parent.?).execution.entry.?.children);
        try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().child_operations);
    }
}

test "runnable execution port child allocation failures preserve parent and exact owner leases" {
    const Fixture = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var owner: State = .{ .owner_alloc = alloc, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
            defer owner.owner.tryDeinit() catch @panic("owner leaked");
            const port = owner.port();
            const io = std.testing.io;
            const io_borrow = runtime.native_abi.IoBorrow.init(&io);
            var parent: ?*anyopaque = null;
            const status = port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &parent });
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            defer port.finish(parent.?);
            const before = owner.owner.scheduler.stats();
            var child: abi.Child = undefined;
            port.reserveChild(parent.?, &.{ .io = &io_borrow, .out_child = &child }) catch |err| {
                try std.testing.expectEqual(before.retained_bytes, owner.owner.scheduler.stats().retained_bytes);
                try std.testing.expectEqual(@as(usize, 0), capsule(parent.?).execution.entry.?.children);
                try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
                return err;
            };
            defer child.deinit();
            try child.activate();
            try std.testing.expectEqual(@as(usize, 2), owner.owner.borrowers);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "runnable execution port pending child shutdown retains stopped cause and publication ownership" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var parent: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &parent }).isOk());
    defer port.finish(parent.?);
    var child: abi.Child = undefined;
    try port.reserveChild(parent.?, &.{ .io = &io_borrow, .out_child = &child });
    defer child.deinit();
    const bytes = owner.owner.scheduler.stats().retained_bytes;
    owner.owner.close();
    try std.testing.expectError(error.SchedulerClosed, child.activate());
    try std.testing.expectError(error.SchedulerClosed, child.activate());
    const check = child.borrow.checkpoint(child.borrow.context);
    try std.testing.expectEqual(error.SchedulerClosed, runtime.error_abi.errorFromStatus(check));
    try std.testing.expectEqual(bytes, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), capsule(parent.?).execution.entry.?.children);
    try std.testing.expectEqual(@as(usize, 2), owner.owner.borrowers);
    child.deinit();
    try std.testing.expectEqual(@as(usize, 0), capsule(parent.?).execution.entry.?.children);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
}

test "runnable execution port child invalid prefix and overflowing ownership leave parent unchanged" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
    defer owner.owner.tryDeinit() catch @panic("owner leaked");
    const port = owner.port();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var parent: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &parent }).isOk());
    defer port.finish(parent.?);
    const before = owner.owner.scheduler.stats();
    var child: abi.Child = undefined;
    var request: abi.ChildStart = .{ .io = &io_borrow, .out_child = &child, .version = 8 };
    try std.testing.expectError(error.UnsupportedVersion, port.reserveChild(parent.?, &request));
    request.version = abi.abi_version;
    request.retained_bytes = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidRunnableDemand, port.reserveChild(parent.?, &request));
    try std.testing.expectEqual(before.retained_bytes, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(before.operations, owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), capsule(parent.?).execution.entry.?.children);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
}

test "runnable execution port checks parked lifetime without CPU grants or ownership retirement" {
    const Signal = struct {
        fn canceled(raw: ?*const anyopaque) callconv(.c) u8 {
            return @intFromBool(@as(*const std.atomic.Value(bool), @ptrCast(@alignCast(raw.?))).load(.acquire));
        }
    };
    for ([_]?anyerror{ null, error.Canceled, error.DeadlineExceeded, error.SchedulerClosed }) |failure| {
        TestClock.now_ns = 0;
        var flag = std.atomic.Value(bool).init(false);
        var owner_state: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2, .max_operations = 4 }), .native_now_ns = TestClock.now };
        defer owner_state.owner.deinit();
        const port = owner_state.port();
        const io = std.testing.io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        var handle: ?*anyopaque = null;
        const status = port.start(port.context, &.{ .io = &io_borrow, .native_deadline = 1, .has_deadline = 1, .deadline_ns = 10, .cancellation = .{ .context = &flag, .is_cancelled = Signal.canceled }, .class = @backingInt(abi.Class.query), .retained_bytes = 37, .out_handle = &handle });
        try std.testing.expect(status.isOk());
        defer port.finish(handle.?);
        const borrowed = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
        try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
        const before = owner_state.owner.scheduler.stats();
        if (failure) |expected| switch (expected) {
            error.Canceled => flag.store(true, .release),
            error.DeadlineExceeded => TestClock.now_ns = 10,
            error.SchedulerClosed => owner_state.owner.close(),
            else => unreachable,
        };
        if (failure) |expected| {
            try std.testing.expectError(expected, port.checkLifetime(handle.?));
            try std.testing.expectError(expected, borrowed.checkLifetime());
        } else {
            try port.checkLifetime(handle.?);
            try borrowed.checkLifetime();
            var health: ?*anyopaque = null;
            try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .allow_wait = 0, .out_handle = &health }).isOk());
            try port.checkLifetime(handle.?);
            port.finish(health.?);
        }
        const after = owner_state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), after.runnable_slots);
        try std.testing.expectEqual(before.operations, after.operations);
        try std.testing.expectEqual(before.retained_bytes, after.retained_bytes);
        try std.testing.expectEqual(before.grants[0], after.grants[0]);
    }
}

test "runnable execution retained consumer admits protected cleanup after ordinary closure" {
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1 }), .native_now_ns = TestClock.now };
    const port = owner.port();
    var lease = try abi.Lease.init(port);
    owner.owner.close();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    const options: abi.Start = .{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .slots = 1, .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .native_deadline = 1, .deadline_ns = TestClock.now() + 100 * std.time.ns_per_ms, .retained_bytes = 23, .out_handle = &handle };
    try std.testing.expect(port.start(port.context, &options).isOk());
    try std.testing.expect(handle != null);
    try std.testing.expectEqual(@as(usize, 2), owner.owner.borrowers);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    const charged = owner.owner.scheduler.stats().retained_bytes;
    try std.testing.expect(charged >= 23);
    var other: ?*anyopaque = null;
    var ordinary = options;
    ordinary.cleanup = 0;
    ordinary.class = @backingInt(abi.Class.query);
    ordinary.out_handle = &other;
    try std.testing.expectEqual(error.SchedulerClosed, runtime.error_abi.errorFromStatus(port.start(port.context, &ordinary)));
    try std.testing.expect(other == null);
    port.finish(handle.?);
    handle = null;
    try std.testing.expectEqual(@as(usize, 1), owner.owner.borrowers);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    lease.deinit();
    try std.testing.expectEqual(error.SchedulerClosed, runtime.error_abi.errorFromStatus(port.start(port.context, &options)));
    try std.testing.expect(handle == null);
    try owner.owner.tryDeinit();
}

test "runnable execution CPU borrow port preserves capsule ownership and returns checked errors" {
    var value: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 5, .cleanup_slots = 1, .read_isolation = true }), .native_now_ns = TestClock.now };
    defer value.owner.tryDeinit() catch @panic("owner leaked");
    const port = value.port();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .retained_bytes = 17, .out_handle = &handle }).isOk());
    defer if (handle) |active| port.finish(active);
    const retained = value.owner.scheduler.stats().retained_bytes;
    const borrow = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
    try borrow.setCpuBorrow(&.{ .enabled = 1, .max_non_yield_ns = 1000 });
    try std.testing.expectError(error.InvalidRunnableTransition, port.resizeTeam(handle.?, 2));
    var malformed: abi.CpuBorrow = .{ .enabled = 1, .max_non_yield_ns = 1000 };
    malformed.struct_size -= 1;
    try std.testing.expectEqual(error.UnsupportedVersion, runtime.error_abi.errorFromStatus(port.cpu_borrow.?(handle.?, &malformed)));
    try std.testing.expectEqual(retained, value.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 1), value.owner.scheduler.stats().operations);
    var stale = port;
    stale.version -= 1;
    try std.testing.expectError(error.UnsupportedVersion, stale.setCpuBorrow(handle.?, &.{ .enabled = 0 }));
    stale = port;
    stale.struct_size -= 1;
    try std.testing.expectError(error.UnsupportedVersion, stale.setCpuBorrow(handle.?, &.{ .enabled = 0 }));
    stale = port;
    stale.cpu_borrow = null;
    try std.testing.expectError(error.UnsupportedOperation, stale.setCpuBorrow(handle.?, &.{ .enabled = 0 }));
    try port.setCpuBorrow(handle.?, &.{ .enabled = 0 });
    try std.testing.expectEqual(@as(?runnable.CpuBorrowBudget, null), capsule(handle.?).execution.entry.?.cpu_borrow_budget);
    try std.testing.expectEqual(@as(usize, 1), value.owner.borrowers);
    port.finish(handle.?);
    handle = null;
    try std.testing.expectEqual(@as(u64, 0), value.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), value.owner.borrowers);
}

test "runnable execution checked child activation preserves refusal before queue during queue and after grant" {
    const Scope = struct {
        state: *State,
        execution: *runnable.Execution,
        reject_at: runnable.State,
        checks: usize = 0,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.state.owner.scheduler.stats();
            self.checks += 1;
            if (self.execution.entry.?.state == self.reject_at)
                return runtime.error_abi.statusFromError(error.DeadlineExceeded);
            return .ok;
        }
    };
    for ([_]runnable.State{ .resource_blocked, .queued, .running }) |reject_at| {
        var owner_state: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2, .control_slots = 1 }), .native_now_ns = TestClock.now };
        defer owner_state.owner.tryDeinit() catch @panic("owner leaked");
        const port = owner_state.port();
        const io = std.testing.io;
        const io_borrow = runtime.native_abi.IoBorrow.init(&io);
        var parent: ?*anyopaque = null;
        try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &parent }).isOk());
        defer port.finish(parent.?);
        const parent_borrow = try abi.Borrowed.fromOwner(port, parent.?, 1, .query);
        var child: abi.Child = undefined;
        try parent_borrow.reserveChild(&.{ .io = &io_borrow, .out_child = &child });
        defer child.deinit();
        try std.testing.expect(parent_borrow.park(parent_borrow.context, @backingInt(abi.Wait.resource)).isOk());
        var peer: runnable.Execution = .{};
        defer peer.finish();
        if (reject_at == .queued)
            try peer.start(&owner_state.owner.scheduler, .{ .io = io, .allow_wait = false }, .query);
        var scope: Scope = .{ .state = &owner_state, .execution = &capsule(child.borrow.context).execution, .reject_at = reject_at };
        const restriction: abi.Reentry = .{ .context = &scope, .check = Scope.check };
        const retained = owner_state.owner.scheduler.stats().retained_bytes;
        try std.testing.expectError(error.DeadlineExceeded, child.activateChecked(&restriction));
        try std.testing.expectEqual(runnable.State.stopped, scope.execution.entry.?.state);
        const checks = scope.checks;
        try std.testing.expectError(error.DeadlineExceeded, child.activateChecked(&restriction));
        try std.testing.expectEqual(checks, scope.checks);
        try std.testing.expectEqual(retained, owner_state.owner.scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, if (reject_at == .queued) 3 else 2), owner_state.owner.scheduler.stats().operations);
        child.deinit();
        try std.testing.expectEqual(@as(usize, if (reject_at == .queued) 2 else 1), owner_state.owner.scheduler.stats().operations);
    }
}

test "runnable execution dormant cleanup retains funding without CPU through close" {
    TestClock.now_ns = 0;
    defer TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try port.reserveCleanupRoot(&.{ .io = &borrow, .native_clock = 1, .budget_ns = 100, .retained_bytes = 64, .out_handle = &handle });
    var reserved: abi.ReservedCleanup = .{ .owner = port, .handle = handle };
    defer reserved.deinit();
    const charged = owner.owner.scheduler.stats().retained_bytes;
    try std.testing.expect(charged >= 64);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(error.InvalidRunnableTransition, runtime.error_abi.errorFromStatus(port.checkpoint(handle.?)));
    try std.testing.expectEqual(error.InvalidRunnableTransition, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.write))));
    TestClock.now_ns = 1000;
    owner.owner.close();
    _ = try reserved.activate();
    try std.testing.expect(port.checkpoint(handle.?).isOk());
    try std.testing.expectError(error.InvalidRunnableTransition, reserved.activate());
    try std.testing.expectEqual(charged, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    TestClock.now_ns = 1101;
    try std.testing.expectEqual(error.DeadlineExceeded, runtime.error_abi.errorFromStatus(port.enter(handle.?, @backingInt(abi.Class.control))));
    try std.testing.expectEqual(charged, owner.owner.scheduler.stats().retained_bytes);
    reserved.deinit();
    reserved.deinit();
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(@as(u64, 0), owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(usize, 0), owner.owner.borrowers);
}

test "runnable execution dormant cleanup allocation failures release admission and owner leases" {
    const Check = struct {
        fn run(alloc: std.mem.Allocator) !void {
            var owner: State = .{ .owner_alloc = alloc, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = TestClock.now };
            defer owner.owner.deinit();
            defer std.debug.assert(owner.owner.borrowers == 0);
            const io = std.testing.io;
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            var reserved = try abi.ReservedCleanup.reserve(owner.port(), &borrow, 100, 32);
            reserved.deinit();
            try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().operations);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Check.run, .{});
}

test "runnable execution reserved cleanup resumes its original deadline and charge after close" {
    TestClock.now_ns = 0;
    defer TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    try port.reserveCleanupRoot(&.{ .io = &io_borrow, .native_clock = 1, .budget_ns = 100, .out_handle = &handle });
    var reserved: abi.ReservedCleanup = .{ .owner = port, .handle = handle };
    defer reserved.deinit();
    const charged = owner.owner.scheduler.stats().retained_bytes;
    TestClock.now_ns = 1000;
    const grant = try reserved.enter();
    try std.testing.expectEqual(@as(u64, 1100), grant.deadline_ns);
    try std.testing.expectEqual(abi.Class.write, try grant.borrow.currentClass());
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    const Reject = struct {
        fn check(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.NotFound);
        }
    };
    try std.testing.expectError(error.NotFound, grant.borrow.enterChecked(.write, &.{ .context = &owner, .check = Reject.check }));
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
    owner.owner.close();
    TestClock.now_ns = 1050;
    const resumed = try reserved.enter();
    try std.testing.expectEqual(grant.deadline_ns, resumed.deadline_ns);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
    try std.testing.expectEqual(charged, owner.owner.scheduler.stats().retained_bytes);
    try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
    TestClock.now_ns = 1100;
    try std.testing.expectError(error.DeadlineExceeded, reserved.enter());
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(charged, owner.owner.scheduler.stats().retained_bytes);
}

test "runnable execution budget delegates CPU and reenters with the original deadline" {
    const sources = @import("antfly_local_sources");
    const Controls = struct {
        execution_runnable: ?abi.Borrowed,
        fn check(self: @This()) !void {
            if (self.execution_runnable) |borrow| {
                const status = borrow.checkpoint(borrow.context);
                if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            }
        }
    };
    TestClock.now_ns = 0;
    defer TestClock.now_ns = 0;
    var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 }), .native_now_ns = TestClock.now };
    defer owner.owner.deinit();
    const port = owner.port();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var caller: ?*anyopaque = null;
    const status = port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .has_deadline = 1, .native_deadline = 1, .deadline_ns = 100, .out_handle = &caller });
    if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
    defer port.finish(caller.?);
    const Budget = sources.common_read_execution_budget.Budget(Controls, Controls.check);
    var budget: Budget = .{ .controls = .{ .execution_runnable = try abi.Borrowed.fromOwner(port, caller.?, 1, .query) }, .io = io };
    var adapter: sources.common_read_admission.Adapter(*Budget) = .{ .budget = &budget };
    var payload = adapter.payload();
    const original = sources.common_read_admission.Port.init(&payload);
    const delegation = (try original.executionDelegation()).?;
    try delegation.beginDelegation();
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
    try delegation.endDelegation();
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().runnable_slots);
    try delegation.beginDelegation();
    TestClock.now_ns = 100;
    try std.testing.expectError(error.DeadlineExceeded, delegation.endDelegation());
    try std.testing.expectEqual(@as(usize, 0), owner.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
}

test "runnable execution reserved activation observes queued caller rejection without renewing cleanup" {
    const Check = struct {
        calls: usize = 0,
        cause: anyerror,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.calls >= 3) return runtime.error_abi.statusFromError(self.cause);
            return .ok;
        }
    };
    for ([_]bool{ false, true }) |occupied| {
        for ([_]anyerror{ error.NotFound, error.Canceled, error.DeadlineExceeded }) |cause| {
            TestClock.now_ns = 0;
            defer TestClock.now_ns = 0;
            var owner: State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3, .control_slots = 1, .cleanup_slots = 1, .max_operations = 8 }), .native_now_ns = TestClock.now };
            defer owner.owner.deinit();
            const port = owner.port();
            const io = std.testing.io;
            const io_borrow = runtime.native_abi.IoBorrow.init(&io);
            var handle: ?*anyopaque = null;
            try port.reserveCleanupRoot(&.{ .io = &io_borrow, .native_clock = 1, .budget_ns = 100, .out_handle = &handle });
            var reserved: abi.ReservedCleanup = .{ .owner = port, .handle = handle };
            defer reserved.deinit();
            var holder: ?*anyopaque = null;
            defer if (holder) |value| port.finish(value);
            if (occupied) {
                const status = port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.control), .cleanup = 1, .allow_wait = 0, .has_deadline = 1, .native_deadline = 1, .deadline_ns = 100, .out_handle = &holder });
                if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            }
            const charged = owner.owner.scheduler.stats().retained_bytes;
            var check: Check = .{ .cause = cause };
            try std.testing.expectError(cause, reserved.enterChecked(&.{ .context = &check, .check = Check.check }));
            try std.testing.expectEqual(@as(usize, @intFromBool(occupied)), owner.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(charged, owner.owner.scheduler.stats().retained_bytes);
            if (holder) |value| port.finish(value);
            holder = null;
            TestClock.now_ns = 10;
            const grant = try reserved.enter();
            try std.testing.expectEqual(@as(u64, 100), grant.deadline_ns);
            try std.testing.expectEqual(@as(usize, 1), owner.owner.scheduler.stats().operations);
            try std.testing.expect(port.park(handle.?, @backingInt(abi.Wait.resource)).isOk());
            TestClock.now_ns = 100;
            try std.testing.expectError(error.DeadlineExceeded, reserved.enter());
        }
    }
}
