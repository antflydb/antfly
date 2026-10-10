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

test "broker provider wait releases real capacity and fences swallowed reacquisition errors" {
    const controls = @import("inference_execution_control");
    const Mode = enum { success, canceled, deadline };
    const Clock = struct {
        fn now() u64 {
            return 0;
        }
    };
    for ([_]Mode{ .success, .canceled, .deadline }) |mode| {
        var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = Clock.now };
        defer state.owner.tryDeinit() catch @panic("owner leaked");
        const port = state.port();
        var lease = try runtime.runnable_abi.Lease.init(port);
        defer lease.deinit();
        const io = std.testing.io;
        const borrow = runtime.native_abi.IoBorrow.init(&io);
        var provider = try inference.Provider.init(std.testing.allocator, port);
        const admission = provider.admission();
        var canceled = std.atomic.Value(bool).init(false);
        var internal_cancel = std.atomic.Value(bool).init(false);
        const payload: usize = 1;
        var output: usize = undefined;
        var slot: inference.microbatch.ResultSlot = .{ .output = &output };
        var items = [_]inference.microbatch.ExecuteItem{.{
            .allocator = std.testing.allocator,
            .identity = .{},
            .payload = &payload,
            .slot = &slot,
            .control = .{ .io = io, .cancellation = inference.microbatch.Cancellation.fromAtomic(&canceled), .cancel_requested = &internal_cancel },
        }};
        const handle = try admission.begin(admission.context, io, .{ .model = "model", .task = .embed, .resource_class = .cpu }, 123, .{ .items = &items });
        var finished = false;
        defer if (!finished) admission.finish(admission.context, handle);
        const retained = state.owner.scheduler.stats().retained_bytes;
        try (controls.InferenceExecutionControl{}).check();
        var wait = try controls.ResourceWait.init();
        defer wait.finish() catch {};
        const parked = state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), parked.runnable_slots);
        try std.testing.expectEqual(@as(usize, 1), parked.resource_blocked);
        try std.testing.expectEqual(retained, parked.retained_bytes);
        var peer: ?*anyopaque = null;
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &peer }).isOk());
        var control: ?*anyopaque = null;
        try std.testing.expect(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control }).isOk());
        try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
        port.finish(control.?);
        port.finish(peer.?);
        if (mode == .canceled) canceled.store(true, .release);
        if (mode == .deadline) items[0].control.deadline = .{ .clock = .awake, .raw = .{ .nanoseconds = 0 } };
        if (mode == .success) {
            try wait.finish();
            try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().runnable_slots);
            internal_cancel.store(true, .release);
            try admission.checkpoint.?(admission.context, handle);
        } else {
            const expected = if (mode == .canceled) error.Canceled else error.DeadlineExceeded;
            try std.testing.expectError(expected, wait.finish());
            canceled.store(false, .release);
            items[0].control.deadline = null;
            // A backend swallowing and clearing the error cannot revive a grant.
            try std.testing.expectError(expected, admission.checkpoint.?(admission.context, handle));
        }
        admission.finish(admission.context, handle);
        finished = true;
        const drained = state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), drained.operations);
        try std.testing.expectEqual(@as(usize, 0), drained.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), drained.retained_bytes);
    }
}

test "broker submissions park their parent before independent admission and clean rejected outputs" {
    const controls = @import("inference_execution_control");
    const Probe = struct {
        state: *owner_module.State,
        port: runtime.runnable_abi.Owner,
        parent: *anyopaque,
        reject: bool,
        canceled: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn now() u64 {
            return 0;
        }
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.checkpoint(self.parent));
        }
        fn park(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.park(self.parent, @backingInt(runtime.runnable_abi.Wait.resource)));
        }
        fn reenter(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.enter(self.parent, @backingInt(runtime.runnable_abi.Class.query)));
        }
        fn release(allocator: std.mem.Allocator, value: []u8) void {
            allocator.free(value);
        }
        fn execute(raw: *anyopaque, items: []const inference.microbatch.ExecuteItem) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            const stats = self.state.owner.scheduler.stats();
            std.debug.assert(stats.runnable_slots == 1 and stats.resource_blocked == 1 and stats.operations == 2);
            (controls.InferenceExecutionControl{}).check() catch @panic("independent child checkpoint failed");
            for (items) |item| {
                const value = item.allocator.dupe(u8, "owned") catch |err| {
                    item.slot.fail(err);
                    continue;
                };
                item.slot.setOwnedValue([]u8, value, .native_batch, item.allocator, release);
            }
            self.canceled.store(self.reject, .release);
        }
    };
    for ([_]bool{ false, true }) |reject| for ([_]bool{ false, true }) |native| {
        var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 2 }), .native_now_ns = Probe.now };
        defer state.owner.tryDeinit() catch @panic("owner leaked");
        const port = state.port();
        var lease = try runtime.runnable_abi.Lease.init(port);
        defer lease.deinit();
        const io = std.testing.io;
        const borrow = runtime.native_abi.IoBorrow.init(&io);
        var probe = Probe{ .state = &state, .port = port, .parent = undefined, .reject = reject };
        var parent: ?*anyopaque = null;
        try Probe.checked(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .retained_bytes = 71, .cancellation = .{ .context = &probe, .is_cancelled = Probe.requested }, .out_handle = &parent }));
        defer port.finish(parent.?);
        probe.parent = parent.?;
        const parent_retained = state.owner.scheduler.stats().retained_bytes;
        var scope = controls.CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check, .wait_begin_fn = Probe.park, .wait_end_fn = Probe.reenter });
        defer scope.deinit();
        var provider = try inference.Provider.init(std.testing.allocator, port);
        var broker = inference.microbatch.Broker.initWithAdmission(std.testing.allocator, provider.admission());
        defer broker.deinit();
        const key = inference.microbatch.Key{ .model = "owned", .task = .embed, .resource_class = .cpu };
        if (native) {
            const values = [_]usize{ 1, 2 };
            const results = try broker.submitBatchControlled(usize, []u8, io, std.testing.allocator, key, .{ .mode = .native, .preferred_items = 2, .max_items = 2 }, &.{ .{}, .{} }, &.{ .{}, .{} }, null, .{}, &values, &probe, Probe.execute);
            defer std.testing.allocator.free(results);
            for (results) |result| {
                if (reject) try std.testing.expectEqual(error.Canceled, result.result.item_error.cause) else {
                    try std.testing.expectEqualStrings("owned", result.result.value);
                    std.testing.allocator.free(result.result.value);
                }
            }
        } else {
            const payload: usize = 1;
            const result = try broker.submit(usize, []u8, io, std.testing.allocator, key, .{ .mode = .serial_compatibility, .preferred_items = 1, .max_items = 1 }, .{}, .{}, null, null, &payload, &probe, Probe.execute);
            if (reject) try std.testing.expectEqual(error.Canceled, result.result.item_error.cause) else {
                try std.testing.expectEqualStrings("owned", result.result.value);
                std.testing.allocator.free(result.result.value);
            }
        }
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        // Failed reacquisition stops runnable execution. The parent capsule
        // and its reservation remain owned until the original caller finishes.
        try std.testing.expectEqual(@as(usize, 1), state.owner.scheduler.stats().operations);
        try std.testing.expectEqual(parent_retained, state.owner.scheduler.stats().retained_bytes);
        try std.testing.expectEqual(@as(usize, if (reject) 0 else 1), state.owner.scheduler.stats().runnable_slots);
    };
}

test "coalesced broker callers park separate real parents and share one execution grant" {
    const controls = @import("inference_execution_control");
    const Microbatch = inference.microbatch;
    const Mode = enum { success, canceled, deadline };
    const Clock = struct {
        var current: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return current.load(.acquire);
        }
    };
    const Executor = struct {
        state: *owner_module.State,
        port: runtime.runnable_abi.Owner,
        mode: Mode,
        canceled_parent: ?*std.atomic.Value(bool) = null,
        calls: std.atomic.Value(usize) = .init(0),
        members: std.atomic.Value(usize) = .init(0),
        fn observe(self: *@This(), items: []const Microbatch.ExecuteItem) !void {
            const before = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 1), before.runnable_slots);
            try std.testing.expectEqual(@as(usize, 2), before.resource_blocked);
            try std.testing.expectEqual(@as(usize, 3), before.operations);
            try std.testing.expect(before.retained_bytes >= 142);
            try (controls.InferenceExecutionControl{}).check();
            const io = std.testing.io;
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            var control: ?*anyopaque = null;
            const status = self.port.start(self.port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control });
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
            defer self.port.finish(control.?);
            try std.testing.expectEqual(@as(usize, 1), self.state.owner.scheduler.stats().control_slots);
            try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
            for (items) |item| item.slot.setValue(usize, item.payloadAs(usize).* * 2, .native_batch);
            if (self.mode == .canceled) self.canceled_parent.?.store(true, .release);
            if (self.mode == .deadline) Clock.current.store(6, .release);
        }
        fn run(raw: *anyopaque, items: []const Microbatch.ExecuteItem) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.calls.fetchAdd(1, .monotonic);
            _ = self.members.fetchAdd(items.len, .monotonic);
            self.observe(items) catch |err| for (items) |item| item.slot.fail(err);
        }
    };
    const Caller = struct {
        port: runtime.runnable_abi.Owner,
        broker: *Microbatch.Broker,
        executor: *Executor,
        payload: usize,
        canceled: std.atomic.Value(bool) = .init(false),
        deadline: bool = false,
        parent: ?*anyopaque = null,
        output: ?Microbatch.ItemResult(usize) = null,
        err: ?anyerror = null,
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.checkpoint(self.parent.?));
        }
        fn park(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.park(self.parent.?, @backingInt(runtime.runnable_abi.Wait.resource)));
        }
        fn reenter(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.enter(self.parent.?, @backingInt(runtime.runnable_abi.Class.query)));
        }
        fn submit(self: *@This()) !void {
            const io = std.testing.io;
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            try checked(self.port.start(self.port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .retained_bytes = 71, .deadline_ns = 5, .has_deadline = @intFromBool(self.deadline), .native_deadline = @intFromBool(self.deadline), .cancellation = .{ .context = self, .is_cancelled = requested }, .out_handle = &self.parent }));
            defer self.port.finish(self.parent.?);
            var scope = controls.CheckpointScope.init(.{ .context = self, .check_fn = check, .wait_begin_fn = park, .wait_end_fn = reenter });
            defer scope.deinit();
            self.output = try self.broker.submit(usize, usize, io, std.testing.allocator, .{ .model = "cohort", .task = .embed, .resource_class = .cpu }, .{ .mode = .native, .preferred_items = 2, .max_items = 2, .max_wait_us = 2_000_000 }, .{}, .{}, null, null, &self.payload, self.executor, Executor.run);
        }
        fn run(self: *@This()) std.Io.Cancelable!void {
            self.submit() catch |err| {
                self.err = err;
            };
        }
    };
    for ([_]Mode{ .success, .canceled, .deadline }) |mode| {
        Clock.current.store(0, .release);
        var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer state.owner.tryDeinit() catch @panic("owner leaked");
        const port = state.port();
        var lease = try runtime.runnable_abi.Lease.init(port);
        defer lease.deinit();
        var provider = try inference.Provider.init(std.testing.allocator, port);
        var broker = Microbatch.Broker.initWithAdmission(std.testing.allocator, provider.admission());
        defer broker.deinit();
        var executor = Executor{ .state = &state, .port = port, .mode = mode };
        var callers = [_]Caller{
            .{ .port = port, .broker = &broker, .executor = &executor, .payload = 3, .deadline = mode == .deadline },
            .{ .port = port, .broker = &broker, .executor = &executor, .payload = 7 },
        };
        executor.canceled_parent = &callers[0].canceled;
        var group: std.Io.Group = .init;
        defer group.cancel(std.testing.io);
        for (&callers) |*caller| try group.concurrent(std.testing.io, Caller.run, .{caller});
        try group.await(std.testing.io);
        for (&callers, 0..) |*caller, index| {
            if (caller.err) |err| return err;
            const result = caller.output orelse return error.MissingMicrobatchResult;
            if (index == 0 and mode != .success) {
                try std.testing.expectEqual(if (mode == .canceled) error.Canceled else error.DeadlineExceeded, result.result.item_error.cause);
            } else switch (result.result) {
                .item_error => |err| return err.cause,
                .value => |value| try std.testing.expectEqual(caller.payload * 2, value),
            }
        }
        try std.testing.expectEqual(@as(usize, 1), executor.calls.load(.monotonic));
        try std.testing.expectEqual(@as(usize, 2), executor.members.load(.monotonic));
        try std.testing.expectEqual(callers[0].output.?.execution_id, callers[1].output.?.execution_id);
        const drained = state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), drained.operations);
        try std.testing.expectEqual(@as(usize, 0), drained.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), drained.retained_bytes);
    }
}

test "inference Io mutex wait preserves real owner bytes and control progress" {
    const control_module = @import("inference_execution_control");
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
        gate: std.Io.Mutex = .init,
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
            self.gate.unlock(std.testing.io);
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
            try std.testing.expectError(error.Canceled, control.lockIo(std.testing.io, &probe.gate));
        } else if (mode == .deadline) {
            try std.testing.expectError(error.DeadlineExceeded, control.lockIo(std.testing.io, &probe.gate));
        } else {
            try control.lockIo(std.testing.io, &probe.gate);
            probe.gate.unlock(std.testing.io);
            try std.testing.expectEqual(@as(usize, 2), state.owner.scheduler.stats().runnable_slots);
            try std.testing.expectEqual(probe.retained_bytes, state.owner.scheduler.stats().retained_bytes);
        }
        try std.testing.expectEqual(@as(usize, 1), probe.waits);
        try std.testing.expect(probe.gate.tryLock());
        probe.gate.unlock(std.testing.io);
        scope.deinit();
        port.finish(parent.?);
        finished = true;
        const drained = state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), drained.operations);
        try std.testing.expectEqual(@as(usize, 0), drained.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), drained.retained_bytes);
    }
}
