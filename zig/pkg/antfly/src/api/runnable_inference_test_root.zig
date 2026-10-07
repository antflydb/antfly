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

test "inference model gate wait preserves real owner bytes and control progress" {
    const control_module = @import("inference_execution_control");
    try control_module.testNestedSynchronousResourceWait();
    const Mode = enum { success, canceled, deadline };
    const Clock = struct {
        var current: u64 = 0;
        fn now() u64 {
            return current;
        }
    };
    const Probe = struct {
        owner: *owner_module.State,
        port: runtime.runnable_abi.Owner,
        parent: *anyopaque,
        io: *const runtime.native_abi.IoBorrow,
        gate: std.atomic.Mutex = .unlocked,
        mode: Mode,
        canceled: std.atomic.Value(bool) = .init(false),
        retained_bytes: u64,
        waits: usize = 0,
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.checkpoint(self.parent));
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.park(self.parent, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.waits += 1;
            const parked = self.owner.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), parked.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), parked.resource_blocked);
            try std.testing.expectEqual(self.retained_bytes, parked.retained_bytes);
            var peer: ?*anyopaque = null;
            try checked(self.port.start(self.port.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &peer }));
            defer self.port.finish(peer.?);
            var control: ?*anyopaque = null;
            try checked(self.port.start(self.port.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control }));
            defer self.port.finish(control.?);
            try std.testing.expectEqual(@as(usize, 2), self.owner.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), self.owner.owner.scheduler.stats().control_slots);
            self.canceled.store(self.mode == .canceled, .release);
            if (self.mode == .deadline) Clock.current = 6;
            self.gate.unlock();
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.enter(self.parent, @backingInt(runtime.runnable_abi.Class.inference)));
        }
    };
    for ([_]Mode{ .success, .canceled, .deadline }) |mode| {
        Clock.current = 0;
        var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer state.owner.tryDeinit() catch @panic("owner leaked");
        const port = state.port();
        var lease = try runtime.runnable_abi.Lease.init(port);
        defer lease.deinit();
        const io = std.testing.io;
        const borrow = runtime.native_abi.IoBorrow.init(&io);
        var probe = Probe{ .owner = &state, .port = port, .parent = undefined, .io = &borrow, .mode = mode, .retained_bytes = 0 };
        var parent: ?*anyopaque = null;
        try Probe.checked(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 123, .has_deadline = 1, .native_deadline = 1, .deadline_ns = 5, .cancellation = .{ .context = &probe, .is_cancelled = Probe.requested }, .out_handle = &parent }));
        var finished = false;
        defer if (!finished) port.finish(parent.?);
        probe.parent = parent.?;
        probe.retained_bytes = state.owner.scheduler.stats().retained_bytes;
        var scope = control_module.CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check, .wait_begin_fn = Probe.begin, .wait_end_fn = Probe.end });
        defer if (scope.active) scope.deinit();
        try std.testing.expect(probe.gate.tryLock());
        const control = control_module.InferenceExecutionControl{};
        if (mode == .canceled) {
            try std.testing.expectError(error.Canceled, control.lock(&probe.gate));
        } else if (mode == .deadline) {
            try std.testing.expectError(error.DeadlineExceeded, control.lock(&probe.gate));
        } else {
            try control.lock(&probe.gate);
            probe.gate.unlock();
            try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(probe.retained_bytes, state.owner.scheduler.stats().retained_bytes);
        }
        try std.testing.expectEqual(@as(usize, 1), probe.waits);
        try std.testing.expect(probe.gate.tryLock());
        probe.gate.unlock();
        scope.deinit();
        port.finish(parent.?);
        finished = true;
        const drained = state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), drained.operations);
        try std.testing.expectEqual(@as(usize, 0), drained.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), drained.retained_bytes);
    }
}
