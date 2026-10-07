// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Focused integration gate: real API owner capsules and inference CPU scopes.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const owner_module = @import("runnable_owner_execution.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const inference = @import("inference_runnable_execution");

test "inference admission uses shared node capacity after parent parking and preserves control" {
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = Clock.now };
    defer state.owner.tryDeinit() catch @panic("owner leaked");
    const port = state.port();
    var lease = try runtime.runnable_abi.Lease.init(port);
    defer lease.deinit();
    const io = std.testing.io;
    const borrow = runtime.native_abi.IoBorrow.init(&io);
    var parent: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &parent }).isOk());
    defer port.finish(parent.?);
    try std.testing.expect(port.park(parent.?, @backingInt(runtime.runnable_abi.Wait.resource)).isOk());

    var provider = try inference.Provider.init(std.testing.allocator, port);
    const admission = provider.admission();
    const microbatch = inference.microbatch;
    var first = std.atomic.Value(bool).init(false);
    var second = std.atomic.Value(bool).init(false);
    const payload: usize = 1;
    var outputs: [2]usize = undefined;
    var slots = [_]microbatch.ResultSlot{ .{ .output = &outputs[0] }, .{ .output = &outputs[1] } };
    const items = [_]microbatch.ExecuteItem{
        .{ .allocator = std.testing.allocator, .identity = .{}, .payload = &payload, .slot = &slots[0], .control = .{ .cancellation = microbatch.Cancellation.fromAtomic(&first) } },
        .{ .allocator = std.testing.allocator, .identity = .{}, .payload = &payload, .slot = &slots[1], .control = .{ .cancellation = microbatch.Cancellation.fromAtomic(&second) } },
    };
    const key = microbatch.Key{ .model = "shared", .task = .read, .resource_class = .cpu };
    const handle = try admission.begin(admission.context, io, key, 17, .{ .items = &items });
    var finished = false;
    defer if (!finished) admission.finish(admission.context, handle);
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().resource_blocked);
    first.store(true, .release);
    var collective = microbatch.ExecutionControl{ .items = &items };
    try microbatch.ExecutionControl.check(&collective);
    var control: ?*anyopaque = null;
    try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control }).isOk());
    try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
    port.finish(control.?);
    admission.finish(admission.context, handle);
    finished = true;
    try std.testing.expect(port.enter(parent.?, @backingInt(runtime.runnable_abi.Class.query)).isOk());
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
    second.store(true, .release);
    try std.testing.expectError(error.Canceled, admission.begin(admission.context, io, key, 17, .{ .items = &items }));
    try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
}
