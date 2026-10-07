// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Real scheduler capsules, production phase helpers, and actual worker pipes.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const owner_module = @import("runnable_owner_execution.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const controls = @import("inference_execution_control");
const rpc = @import("inference_worker_rpc");
const phases = @import("inference_worker_phases").For(controls, rpc);

test "inference worker physical handoff releases weighted real capacity and fences rejected resumes" {
    const Mode = enum { success, canceled, deadline, quantum };
    const Clock = struct {
        var current: std.atomic.Value(u64) = .init(0);
        fn now() u64 {
            return current.load(.acquire);
        }
    };
    const Probe = struct {
        state: *owner_module.State,
        port: runtime.runnable_abi.Owner,
        capsule: *anyopaque,
        io: *const runtime.native_abi.IoBorrow,
        pair: *rpc.TestPair,
        mode: Mode,
        gate: std.atomic.Mutex = .unlocked,
        canceled: std.atomic.Value(bool) = .init(false),
        computed: std.atomic.Value(bool) = .init(false),
        child_joined: std.Io.Event = .unset,
        parent_phase: phases.Parent = .{},
        retained_bytes: u64 = 0,
        handler_started: std.atomic.Value(bool) = .init(false),
        parks: usize = 0,
        reentries: usize = 0,
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.checkpoint(self.capsule));
        }
        fn park(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try checked(self.port.park(self.capsule, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.parks += 1;
            const parked = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), parked.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), parked.resource_blocked);
            try std.testing.expectEqual(self.retained_bytes, parked.retained_bytes);
            var query: ?*anyopaque = null;
            try checked(self.port.start(self.port.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &query }));
            var control: ?*anyopaque = null;
            try checked(self.port.start(self.port.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control }));
            try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
            self.port.finish(control.?);
            self.port.finish(query.?);
            if (self.mode == .canceled) self.canceled.store(true, .release);
            if (self.mode == .deadline) Clock.current.store(6, .release);
            self.gate.unlock();
        }
        fn reenter(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reentries += 1;
            try checked(self.port.enter(self.capsule, @backingInt(runtime.runnable_abi.Class.inference)));
            const resumed = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 2), resumed.runnable_slots);
            try std.testing.expectEqual(self.retained_bytes, resumed.retained_bytes);
        }
        fn phase(raw: ?*anyopaque, phase_value: rpc.Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try self.parent_phase.transition(phase_value);
        }
        fn rpcCheck(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try self.parent_phase.poll(runtime.http_abi.CancellationView{ .context = self, .is_cancelled = requested }, null, Clock.now());
        }
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.pair.child.fail();
            // The fixture's reaping fence joins the actual child handler before
            // the caller can retire its original scheduler capsule.
            if (self.handler_started.load(.acquire)) self.child_joined.waitUncancelable(std.testing.io);
        }
        fn handle(raw: *anyopaque, request: *rpc.Request) !rpc.OwnedPayload {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.handler_started.store(true, .release);
            defer self.child_joined.set(std.testing.io);
            var child_phase = phases.Child{ .request = request };
            var scope = controls.CheckpointScope.init(.{ .context = &child_phase, .check_fn = phases.Child.check, .wait_begin_fn = phases.Child.park, .wait_end_fn = phases.Child.reenter });
            defer scope.deinit();
            try (controls.InferenceExecutionControl{}).lock(&self.gate);
            defer self.gate.unlock();
            if (self.mode == .quantum) {
                // Model a native primitive which cannot yield midway. Advance
                // past the scheduler quantum and let the caller poll repeatedly.
                // Its capacity must stay charged until this child checkpoints.
                Clock.current.store(3 * std.time.ns_per_ms, .release);
                for (0..15) |_| {
                    try std.testing.io.sleep(.fromMilliseconds(2), .awake);
                    try std.testing.expectEqual(@as(u64, 0), self.state.owner.scheduler.stats().yields);
                    try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
                }
                try (controls.InferenceExecutionControl{}).check();
                try std.testing.expectEqual(@as(u64, 1), self.state.owner.scheduler.stats().yields);
            }
            if (child_phase.failure) |err| return err;
            self.computed.store(true, .release);
            return request.endpoint.copyPayload(.{ .body = "computed" });
        }
        fn call(self: *@This()) !rpc.OwnedPayload {
            const result = self.pair.parent.call(.{ .body = "work" }, .{ .ptr = self, .check = rpcCheck, .phase = phase, .fence_on_error = true }) catch |err| {
                try self.parent_phase.finish();
                return err;
            };
            errdefer result.deinit();
            if (self.parent_phase.wait != null) {
                try self.parent_phase.finish();
                return error.InvalidWorkerPhase;
            }
            return result;
        }
    };
    for ([_]Mode{ .success, .canceled, .deadline, .quantum }) |mode| {
        Clock.current.store(0, .release);
        var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Clock.now };
        defer state.owner.tryDeinit() catch @panic("owner leaked");
        const port = state.port();
        var lease = try runtime.runnable_abi.Lease.init(port);
        defer lease.deinit();
        const io = std.testing.io;
        const borrow = runtime.native_abi.IoBorrow.init(&io);
        var pair: rpc.TestPair = undefined;
        try pair.init();
        var probe = Probe{ .state = &state, .port = port, .capsule = undefined, .io = &borrow, .pair = &pair, .mode = mode };
        // The pair must retire before the stack-owned callback context.
        defer pair.deinit();
        pair.parent.mutex.lockUncancelable(io);
        pair.parent.context = &probe;
        pair.parent.on_closed = Probe.closed;
        pair.parent.mutex.unlock(io);
        pair.child.mutex.lockUncancelable(io);
        pair.child.context = &probe;
        pair.child.handler = Probe.handle;
        pair.child.mutex.unlock(io);
        var capsule: ?*anyopaque = null;
        try Probe.checked(port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 123, .has_deadline = 1, .native_deadline = 1, .deadline_ns = if (mode == .quantum) 10 * std.time.ns_per_ms else 5, .cancellation = .{ .context = &probe, .is_cancelled = Probe.requested }, .out_handle = &capsule }));
        probe.capsule = capsule.?;
        probe.retained_bytes = state.owner.scheduler.stats().retained_bytes;
        var finished = false;
        defer if (!finished) port.finish(capsule.?);
        var scope = controls.CheckpointScope.init(.{ .context = &probe, .check_fn = Probe.check, .wait_begin_fn = Probe.park, .wait_end_fn = Probe.reenter });
        defer if (scope.active) scope.deinit();
        try std.testing.expect(probe.gate.tryLock());
        if (mode == .success or mode == .quantum) {
            const response = try probe.call();
            defer response.deinit();
            try std.testing.expectEqualStrings("computed", response.view().body);
            try std.testing.expect(probe.computed.load(.acquire));
        } else {
            try std.testing.expectError(if (mode == .canceled) error.Canceled else error.DeadlineExceeded, probe.call());
            try std.testing.expect(!probe.computed.load(.acquire));
            try std.testing.expect(pair.parent.closed_fence.isSet());
        }
        try std.testing.expectEqual(@as(usize, 1), probe.parks);
        try std.testing.expectEqual(@as(usize, 1), probe.reentries);
        try std.testing.expect(probe.parent_phase.wait == null);
        scope.deinit();
        port.finish(capsule.?);
        finished = true;
        const drained = state.owner.scheduler.stats();
        try std.testing.expectEqual(@as(usize, 0), drained.operations);
        try std.testing.expectEqual(@as(usize, 0), drained.runnable_slots);
        try std.testing.expectEqual(@as(u64, 0), drained.retained_bytes);
    }
}
