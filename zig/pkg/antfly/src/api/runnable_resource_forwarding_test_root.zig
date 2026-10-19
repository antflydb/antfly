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

const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const abi = runtime.runnable_abi;
const sources = @import("antfly_local_sources");
const native = sources.common_request_admission.runnable_scheduler;
const memory = sources.storage_resource_manager;
const Bundle = sources.storage_runnable_dense_bundle.Bundle;
const Owner = @import("runnable_owner_execution.zig").State;
const Bridge = @import("runnable_native_borrow.zig").Bridge;
const wire = @import("runnable_resource_bridge.zig");
const Clock = struct {
    var ns: u64 = 0;
    fn now() u64 {
        return ns;
    }
};
fn manager() memory.ResourceManager {
    return memory.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 100 }, .dense_search_bandwidth_capacity_bytes = 10, .identity_allocator = std.testing.allocator });
}
fn prepare(resources: *memory.ResourceManager, bytes: u64, io_bytes: u64) !Bundle {
    return Bundle.init(resources, &.{.{ .slice = .lsm_read_working_set, .bytes = bytes }}, io_bytes);
}
fn owner() !Owner {
    Clock.ns = 0;
    return .{ .owner_alloc = std.testing.allocator, .owner = try native.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
}
fn usage(resources: *memory.ResourceManager, bytes: u64, io_bytes: u64) !void {
    try std.testing.expectEqual(bytes, resources.snapshot().memory.used_bytes);
    try std.testing.expectEqual(io_bytes, resources.denseSearchAdmissionStats().active_bytes);
}

test "runnable resource forwarding owns initial and incremental bundles through the actual owner and native bridge" {
    var resources = manager();
    defer resources.deinit(std.testing.allocator);
    var state = try owner();
    defer state.owner.deinit();
    const port = state.port();
    var base = try prepare(&resources, 60, 4);
    defer base.deinitAfterJoin();
    var increment = try prepare(&resources, 20, 3);
    defer increment.deinitAfterJoin();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    const initial = wire.send(base.port());
    var handle: ?*anyopaque = null;
    defer if (handle) |value| port.finish(value);
    try port.startWithResources(&.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .resources = &initial, .native_deadline = 1, .has_deadline = 1, .deadline_ns = 1000, .retained_bytes = 2 * @sizeOf(Bundle), .out_handle = &handle });
    const original = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
    var bridge = try Bridge.init(&original, std.testing.allocator);
    var execution: native.Execution = .{};
    defer execution.finish();
    try bridge.bind(&execution);
    var view = base.allocationView(.lsm_read_working_set, std.testing.allocator);
    const bytes = try view.allocator().alloc(u8, 32);
    var live = true;
    defer if (live) view.allocator().free(bytes);
    const before = state.owner.scheduler.stats();
    try execution.park(.resource_blocked);
    try execution.requestResources(increment.port());
    try execution.enter(.query);
    try usage(&resources, 80, 7);
    try std.testing.expectEqual(before.operations, state.owner.scheduler.stats().operations);
    try std.testing.expectEqual(before.retained_bytes, state.owner.scheduler.stats().retained_bytes);
    try std.testing.expectEqual(@as(?u64, 1000), state.owner.scheduler.entries.?.options.deadline_ns);
    execution.finish(); // A bridge is not the original operation's owner.
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    port.finish(handle.?);
    handle = null;
    try std.testing.expectError(error.ResourcesStillLive, base.tryDeinitAfterJoin());
    try usage(&resources, 80, 7);
    view.allocator().free(bytes);
    live = false;
    try base.tryDeinitAfterJoin();
    try increment.tryDeinitAfterJoin();
    try usage(&resources, 0, 0);
}

test "runnable resource forwarding snapshots caller frames and refuses replacing a pending bundle" {
    const Trap = struct {
        var calls: usize = 0;
        fn call(_: *anyopaque) callconv(.c) c_int {
            calls += 1;
            return 99;
        }
    };
    Trap.calls = 0;
    var resources = manager();
    defer resources.deinit(std.testing.allocator);
    var state = try owner();
    defer state.owner.deinit();
    const port = state.port();
    var first = try prepare(&resources, 60, 4);
    defer first.deinitAfterJoin();
    var second = try prepare(&resources, 20, 3);
    defer second.deinitAfterJoin();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    defer if (handle) |value| port.finish(value);
    try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &handle }).isOk());
    const borrowed = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
    try std.testing.expect(borrowed.park(borrowed.context, @backingInt(abi.Wait.resource)).isOk());
    var frame = wire.send(first.port());
    try borrowed.requestResources(&frame);
    frame = .{ .version = 99, .context = @ptrFromInt(8), .probe = Trap.call, .acquire = Trap.call };
    const replacement = wire.send(second.port());
    try std.testing.expectError(error.InvalidRunnableTransition, borrowed.requestResources(&replacement));
    try usage(&resources, 0, 0);
    try std.testing.expect(borrowed.enter(borrowed.context, @backingInt(abi.Class.query)).isOk());
    try usage(&resources, 60, 4);
    try std.testing.expectEqual(@as(usize, 0), Trap.calls);
    port.finish(handle.?);
    handle = null;
}

test "runnable resource forwarding reserves a child bundle before first CPU activation" {
    var resources = manager();
    defer resources.deinit(std.testing.allocator);
    var state = try owner();
    defer state.owner.deinit();
    const port = state.port();
    var bundle = try prepare(&resources, 60, 4);
    defer bundle.deinitAfterJoin();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    defer if (handle) |value| port.finish(value);
    try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .native_deadline = 1, .has_deadline = 1, .deadline_ns = 1000, .out_handle = &handle }).isOk());
    const borrowed = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
    var child: abi.Child = undefined;
    try borrowed.reserveChild(&.{ .io = &io_borrow, .retained_bytes = @sizeOf(Bundle), .out_child = &child });
    defer child.deinit();
    const participant = wire.send(bundle.port());
    try child.borrow.requestResources(&participant);
    try usage(&resources, 0, 0);
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
    try child.activate();
    try usage(&resources, 60, 4);
    try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
    child.deinit();
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
    port.finish(handle.?);
    handle = null;
}

test "runnable resource forwarding local read and child adapters retain native grant ownership" {
    var resources = manager();
    defer resources.deinit(std.testing.allocator);
    var node = try native.NodeOwner.init(.{ .slots = 3 });
    defer node.deinit();
    var bundle = try prepare(&resources, 60, 4);
    defer bundle.deinitAfterJoin();
    var parent: native.Execution = .{};
    defer parent.finish();
    try parent.start(&node.scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(native.Execution) }, .query);
    var local = @import("runnable_local_read.zig").Read{ .execution = &parent, .allocator = std.testing.allocator };
    var team = try local.team();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var child: abi.Child = undefined;
    try team.borrow.reserveChild(&.{ .io = &io_borrow, .retained_bytes = @sizeOf(Bundle), .out_child = &child });
    defer child.deinit();
    const participant = wire.send(bundle.port());
    try child.borrow.requestResources(&participant);
    try child.activate();
    try usage(&resources, 60, 4);
    child.deinit();
    parent.finish();
    try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
}

test "runnable resource forwarding missing capability fails before start and preserves original closed authority" {
    var resources = manager();
    defer resources.deinit(std.testing.allocator);
    var state = try owner();
    defer state.owner.deinit();
    var port = state.port();
    var bundle = try prepare(&resources, 60, 4);
    defer bundle.deinitAfterJoin();
    const participant = wire.send(bundle.port());
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    var handle: ?*anyopaque = null;
    defer if (handle) |value| state.port().finish(value);
    port.request_resources = null;
    try std.testing.expectError(error.UnsupportedOperation, port.startWithResources(&.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .resources = &participant, .out_handle = &handle }));
    try std.testing.expect(handle == null);
    try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
    port = state.port();
    try std.testing.expect(port.start(port.context, &.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .out_handle = &handle }).isOk());
    var borrowed = try abi.Borrowed.fromOwner(port, handle.?, 1, .query);
    borrowed.request_resources = null;
    var bridge = try Bridge.init(&borrowed, std.testing.allocator);
    var execution: native.Execution = .{};
    defer execution.finish();
    try bridge.bind(&execution);
    try execution.park(.resource_blocked);
    try std.testing.expectError(error.UnsupportedOperation, execution.requestResources(bundle.port()));
    state.owner.close();
    try std.testing.expectError(error.SchedulerClosed, execution.requestResources(bundle.port()));
    try usage(&resources, 0, 0);
}

test "runnable resource forwarding rejects unknown callback result integers without publishing a handle" {
    const Invalid = struct {
        var bad_probe: bool = false;
        var acquires: usize = 0;
        fn probe(_: *anyopaque) callconv(.c) c_int {
            return if (bad_probe) 99 else @backingInt(abi.ResourceBundle.Probe.ready);
        }
        fn acquire(_: *anyopaque) callconv(.c) c_int {
            acquires += 1;
            return 99;
        }
    };
    var state = try owner();
    defer state.owner.deinit();
    const port = state.port();
    const io = std.testing.io;
    const io_borrow = runtime.native_abi.IoBorrow.init(&io);
    for ([_]bool{ false, true }) |bad| {
        Invalid.bad_probe = bad;
        Invalid.acquires = 0;
        var handle: ?*anyopaque = null;
        const participant: abi.ResourceBundle = .{ .context = @ptrFromInt(8), .probe = Invalid.probe, .acquire = Invalid.acquire };
        try std.testing.expectError(error.ResourceAccountingMismatch, port.startWithResources(&.{ .io = &io_borrow, .class = @backingInt(abi.Class.query), .resources = &participant, .out_handle = &handle }));
        try std.testing.expect(handle == null);
        try std.testing.expectEqual(@as(usize, if (bad) 0 else 1), Invalid.acquires);
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(@as(usize, 0), state.owner.scheduler.stats().runnable_slots);
    }
}

test "runnable resource forwarding validates short and older ABI prefixes before reading callback or IO tails" {
    const Header = extern struct { version: u32, struct_size: u32 };
    var header: Header align(@alignOf(abi.Start)) = .{ .version = 0, .struct_size = @sizeOf(Header) };
    try std.testing.expectError(error.UnsupportedVersion, abi.ResourceBundle.read(@ptrCast(&header)));
    header.version = abi.ResourceBundle.version_number;
    try std.testing.expectError(error.UnsupportedVersion, abi.ResourceBundle.read(@ptrCast(&header)));
    header.version = abi.abi_version - 1;
    try std.testing.expectError(error.UnsupportedVersion, abi.Owner.read(@ptrCast(&header)));
    try std.testing.expectError(error.UnsupportedVersion, abi.Borrowed.read(@ptrCast(&header)));
    try std.testing.expectError(error.UnsupportedVersion, @as(*const abi.Start, @ptrCast(&header)).validate());
    header.version = abi.abi_version;
    try std.testing.expectError(error.UnsupportedVersion, abi.Owner.read(@ptrCast(&header)));
    try std.testing.expectError(error.UnsupportedVersion, abi.Borrowed.read(@ptrCast(&header)));
    try std.testing.expectError(error.UnsupportedVersion, @as(*const abi.Start, @ptrCast(&header)).validate());
    var handle: ?*anyopaque = null;
    var control: abi.Start = .{ .io = @ptrFromInt(8), .class = @backingInt(abi.Class.control), .resources = @ptrFromInt(8), .out_handle = &handle };
    try std.testing.expectError(error.InvalidArgument, control.validate());
}

test "runnable resource forwarding local read root checks retained policy before attaching and validates after grant" {
    const Policy = struct {
        calls: usize = 0,
        fn check(raw: *anyopaque, _: abi.Borrowed) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
        }
    };
    for ([_]bool{ false, true }) |rejected| {
        Clock.ns = 0;
        var resources = manager();
        defer resources.deinit(std.testing.allocator);
        var node = try native.NodeOwner.init(.{ .slots = 3 });
        defer node.deinit();
        var bundle = try prepare(&resources, 60, 4);
        defer bundle.deinitAfterJoin();
        var parent: native.Execution = .{};
        defer parent.finish();
        try parent.start(&node.scheduler, .{ .io = std.testing.io, .retained_bytes = @sizeOf(Bundle), .native_now_ns = Clock.now, .deadline_ns = 1000 }, .query);
        var policy: Policy = .{};
        var local = @import("runnable_local_read.zig").Read{
            .execution = &parent,
            .allocator = std.testing.allocator,
            .validation = .{ .context = &policy, .check = Policy.check, .failure = if (rejected) error.ReadSafetyBarrierUnavailable else null },
        };
        const team = try local.team();
        var bridge = try Bridge.init(&team.borrow, std.testing.allocator);
        var execution: native.Execution = .{};
        defer execution.finish();
        try bridge.bind(&execution);
        try execution.park(.resource_blocked);
        const before = node.scheduler.stats();
        if (rejected) {
            try std.testing.expectError(error.ReadSafetyBarrierUnavailable, execution.requestResources(bundle.port()));
            try std.testing.expect(parent.entry.?.pending_resources == null);
            try usage(&resources, 0, 0);
            try std.testing.expectEqual(@as(usize, 0), policy.calls);
        } else {
            try execution.requestResources(bundle.port());
            try std.testing.expectEqual(@as(usize, 0), policy.calls);
            try execution.enter(.query);
            try usage(&resources, 60, 4);
            try std.testing.expectEqual(@as(usize, 1), policy.calls);
            try std.testing.expectEqual(@as(?u64, 1000), parent.entry.?.options.deadline_ns);
        }
        try std.testing.expectEqual(before.operations, node.scheduler.stats().operations);
        try std.testing.expectEqual(before.retained_bytes, node.scheduler.stats().retained_bytes);
        execution.finish();
        parent.finish();
        try std.testing.expectEqual(@as(usize, 0), node.scheduler.stats().operations);
    }
}
