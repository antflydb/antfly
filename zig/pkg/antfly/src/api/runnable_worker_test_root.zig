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
    const Mode = enum { success, canceled, deadline, quantum, output, output_canceled, output_deadline, output_lost_ack };
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
        writes: usize = 0,
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
            try self.parent_phase.poll(runtime.http_abi.CancellationView{ .context = self, .is_cancelled = requested }, if (self.mode == .quantum) 10 * std.time.ns_per_ms else 5, Clock.now());
        }
        fn event(raw: ?*anyopaque, payload: rpc.Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            // Formatter CPU starts charged. Only the sink's physical wait
            // releases the real API capsule while child output is acknowledged.
            const running = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 2), running.runnable_slots);
            try std.testing.expectEqual(@as(usize, 0), running.resource_blocked);
            try self.parent_phase.transition(.resource_wait);
            try outputWait(raw, payload);
            try self.parent_phase.transition(.runnable);
            try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
        }
        fn outputWait(raw: ?*anyopaque, payload: rpc.Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings("chunk", payload.body);
            try std.testing.expect(!self.computed.load(.acquire));
            const parked = self.state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), parked.runnable_slots);
            try std.testing.expectEqual(@as(usize, 1), parked.resource_blocked);
            try std.testing.expectEqual(self.retained_bytes, parked.retained_bytes);
            // Execute both peer classes while the real output callback is
            // stalled. The child's acknowledged call cannot resume yet.
            var query: ?*anyopaque = null;
            try checked(self.port.start(self.port.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &query }));
            defer self.port.finish(query.?);
            var control: ?*anyopaque = null;
            try checked(self.port.start(self.port.context, &.{ .io = self.io, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control }));
            defer self.port.finish(control.?);
            for (0..15) |_| {
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                try std.testing.expect(!self.computed.load(.acquire));
                try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
            }
            self.writes += 1;
            switch (self.mode) {
                .output_canceled => self.canceled.store(true, .release),
                .output_deadline => Clock.current.store(6, .release),
                .output_lost_ack => self.pair.parent.fail(),
                .output => {},
                else => return error.InvalidTestMode,
            }
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
            if (self.mode == .output or self.mode == .output_canceled or self.mode == .output_deadline or self.mode == .output_lost_ack) {
                try request.eventAcknowledged(.{ .body = "chunk" });
                self.computed.store(true, .release);
                return request.endpoint.copyPayload(.{ .body = "computed" });
            }
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
            const result = self.pair.parent.call(.{ .body = "work" }, .{ .ptr = self, .check = rpcCheck, .event = event, .phase = phase, .fence_on_error = true, .event_owns_waits = true }) catch |err| {
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
    for (std.enums.values(Mode)) |mode| {
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
        if (mode == .success or mode == .quantum or mode == .output) {
            const response = try probe.call();
            defer response.deinit();
            try std.testing.expectEqualStrings("computed", response.view().body);
            try std.testing.expect(probe.computed.load(.acquire));
        } else {
            try std.testing.expectError(switch (mode) {
                .canceled, .output_canceled => error.Canceled,
                .output_lost_ack => error.InferenceWorkerUnavailable,
                else => error.DeadlineExceeded,
            }, probe.call());
            try std.testing.expect(!probe.computed.load(.acquire));
            try std.testing.expect(pair.parent.closed_fence.isSet());
        }
        try std.testing.expectEqual(@as(usize, if (mode == .output or mode == .output_canceled or mode == .output_deadline or mode == .output_lost_ack) 1 else 0), probe.writes);
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

test "inference worker physical handoff carries API ownership through checked streaming callbacks to a stalled TCP peer" {
    const httpx = @import("httpx");
    const io = std.testing.io;
    const Fixture = struct {
        state: *owner_module.State,
        port: runtime.runnable_abi.Owner,
        capsule: *anyopaque,
        transport: *runtime.http_bridge.Outbound,
        payload: []const u8,
        io: std.Io,
        deadline_ns: u64,
        h2: ?*httpx.H2Connection = null,
        canceled: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),
        waiting: std.Io.Event = .unset,
        body_started: bool = false,
        write_epoch: std.atomic.Value(usize) = .init(0),
        in_write: std.atomic.Value(bool) = .init(false),
        begins: usize = 0,
        ends: usize = 0,
        fn now(io_value: std.Io) u64 {
            return @intCast(@max(0, std.Io.Clock.now(.awake, io_value).nanoseconds));
        }
        fn unusedNativeNow() u64 {
            @panic("original IO clock must own this deadline");
        }
        fn requested(raw: ?*const anyopaque) callconv(.c) u8 {
            const self: *const @This() = @ptrCast(@alignCast(raw.?));
            return @intFromBool(self.canceled.load(.acquire));
        }
        fn check(raw: ?*anyopaque) httpx.socket.WriteControl.Error!void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.canceled.load(.acquire)) return error.Canceled;
            if (now(self.io) >= self.deadline_ns) return error.Timeout;
        }
        fn remaining(raw: ?*anyopaque) httpx.socket.WriteControl.Error!?u64 {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try check(raw);
            const ns = self.deadline_ns -| now(self.io);
            return @divTrunc(ns, std.time.ns_per_ms) + @intFromBool(ns % std.time.ns_per_ms != 0);
        }
        fn status(value: runtime.error_abi.Status) runtime.http_abi.CallbackStatus {
            if (value.isOk()) return .ok;
            return switch (runtime.error_abi.errorFromStatus(value)) {
                error.Canceled, error.Cancelled => .canceled,
                error.Timeout, error.DeadlineExceeded => .timeout,
                else => .failed,
            };
        }
        fn begin(raw: ?*anyopaque) callconv(.c) runtime.http_abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            const result = status(self.port.park(self.capsule, @backingInt(runtime.runnable_abi.Wait.resource)));
            if (result != .ok) return result;
            self.begins += 1;
            self.write_epoch.store(self.begins, .release);
            self.in_write.store(true, .release);
            // Header and chunk-prefix writes complete first. Signal while a
            // large body is being sent to a peer which never reads it.
            if (self.body_started and self.begins >= 4) self.waiting.set(self.io);
            return .ok;
        }
        fn end(raw: ?*anyopaque) callconv(.c) runtime.http_abi.CallbackStatus {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.ends += 1;
            self.in_write.store(false, .release);
            if (self.h2) |connection| {
                // Weighted reentry can queue. It must not retain the frame
                // mutex and obstruct connection control/state progress.
                if (!connection.write_mutex.tryLock()) return .failed;
                connection.write_mutex.unlock(self.io);
            }
            return status(self.port.enter(self.capsule, @backingInt(runtime.runnable_abi.Class.inference)));
        }
        fn run(self: *@This()) !runtime.http_abi.CallbackStatus {
            defer self.done.store(true, .release);
            const sink = self.transport.stream();
            const installed = sink.set_execution_wait.?(sink.context, &.{ .context = self, .begin = begin, .end = end });
            if (installed != .ok) return installed;
            defer _ = sink.set_execution_wait.?(sink.context, null);
            const started = sink.start.?(sink.context, 200, .init("application/x-ndjson"), .{});
            if (started != .ok) return started;
            try std.testing.expectEqual(@as(usize, 2), self.state.owner.scheduler.stats().runnable_slots);
            self.body_started = true;
            for (0..64) |_| {
                const result = sink.write.?(sink.context, .init(self.payload));
                if (result != .ok) return result;
            }
            return error.FixtureBufferDidNotFill;
        }
    };
    const payload = try std.testing.allocator.alloc(u8, 4 * 1024 * 1024);
    defer std.testing.allocator.free(payload);
    @memset(payload, 'x');
    for ([_]bool{ false, true }) |use_h2| {
        for ([_]bool{ false, true }) |cancel| {
            std.debug.print("REAL_SINK transport={s} cancel={} start\n", .{ if (use_h2) "h2" else "h1", cancel });
            var state: owner_module.State = .{ .owner_alloc = std.testing.allocator, .owner = try runnable.NodeOwner.init(.{ .slots = 3 }), .native_now_ns = Fixture.unusedNativeNow };
            defer state.owner.tryDeinit() catch @panic("socket owner leaked");
            const port = state.port();
            var lease = try runtime.runnable_abi.Lease.init(port);
            defer lease.deinit();
            const borrow = runtime.native_abi.IoBorrow.init(&io);
            var listener = try httpx.socket.TcpListener.init(.{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } }, io);
            defer listener.deinit();
            var sender = try httpx.socket.Socket.connect(listener.getLocalAddress(), io);
            defer sender.close();
            sender.enableNativeTimeouts();
            var peer = try listener.accept();
            defer peer.socket.close();
            peer.socket.enableNativeTimeouts();
            var request = try httpx.Request.init(std.testing.allocator, .POST, "/stream");
            defer request.deinit();
            var context = httpx.Context.init(std.testing.allocator, io, &request);
            defer context.deinit();
            context.h1_sock = &sender;
            var h2 = httpx.H2Connection.initServer(std.testing.allocator, io);
            defer h2.deinit();
            if (use_h2) {
                const stream = try h2.stream_manager.getOrCreateStream(1);
                stream.state = .open;
                // Give DATA enough peer-advertised credit to reach actual TCP
                // backpressure rather than the independent flow-credit fixture.
                stream.send_window = std.math.maxInt(i32);
                h2.stream_manager.connection_send_window = std.math.maxInt(i32);
                context.h2 = &h2;
                context.h2_sock = &sender;
                context.h2_stream_id = 1;
            }
            var outbound = runtime.http_bridge.Outbound{ .context = &context };
            var fixture = Fixture{ .state = &state, .port = port, .capsule = undefined, .transport = &outbound, .payload = payload, .io = io, .deadline_ns = Fixture.now(io) + 750 * std.time.ns_per_ms, .h2 = if (use_h2) &h2 else null };
            outbound.write_control = .{ .ptr = &fixture, .check_fn = Fixture.check, .remaining_ms_fn = Fixture.remaining };
            var capsule: ?*anyopaque = null;
            const start = port.start(port.context, &.{ .io = &borrow, .clock_io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = payload.len, .deadline_ns = fixture.deadline_ns, .has_deadline = 1, .cancellation = .{ .context = &fixture, .is_cancelled = Fixture.requested }, .out_handle = &capsule });
            if (!start.isOk()) return runtime.error_abi.errorFromStatus(start);
            fixture.capsule = capsule.?;
            var finished = false;
            defer if (!finished) port.finish(capsule.?);
            const retained = state.owner.scheduler.stats().retained_bytes;
            var task = try io.concurrent(Fixture.run, .{&fixture});
            defer _ = task.cancel(io) catch .failed;
            std.debug.print("REAL_SINK await backpressure\n", .{});
            try fixture.waiting.waitTimeout(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
            var query: ?*anyopaque = null;
            const query_start = port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.query), .out_handle = &query });
            if (!query_start.isOk()) return runtime.error_abi.errorFromStatus(query_start);
            var query_finished = false;
            defer if (!query_finished) port.finish(query.?);
            var control: ?*anyopaque = null;
            const control_start = port.start(port.context, &.{ .io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.control), .out_handle = &control });
            if (!control_start.isOk()) return runtime.error_abi.errorFromStatus(control_start);
            var control_finished = false;
            defer if (!control_finished) port.finish(control.?);
            try std.testing.expect(!fixture.done.load(.acquire));
            const waiting = state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 1), waiting.resource_blocked + waiting.queued);
            try io.sleep(.fromMilliseconds(15), .awake);
            port.finish(control.?);
            control_finished = true;
            port.finish(query.?);
            query_finished = true;
            try std.testing.expectEqual(retained, state.owner.scheduler.stats().retained_bytes);
            if (cancel) {
                // Cancellation between complete H2 frames preserves the
                // connection. Require a stable in-progress kernel write,
                // after releasing peer grants, to exercise uncertain output.
                var epoch: usize = 0;
                var stable_since = Fixture.now(io);
                while (true) {
                    const current = fixture.write_epoch.load(.acquire);
                    const now = Fixture.now(io);
                    if (!fixture.in_write.load(.acquire) or current != epoch) {
                        epoch = current;
                        stable_since = now;
                    } else if (now - stable_since >= 40 * std.time.ns_per_ms) break;
                    if (fixture.done.load(.acquire) or now >= fixture.deadline_ns) return error.FixtureDidNotStall;
                    try io.sleep(.fromMilliseconds(1), .awake);
                }
                fixture.canceled.store(true, .release);
            }
            const output_status = try task.await(io);
            std.debug.print("REAL_SINK output status={s} begins={d} ends={d}\n", .{ @tagName(output_status), fixture.begins, fixture.ends });
            try std.testing.expectEqual(if (cancel) runtime.http_abi.CallbackStatus.canceled else .timeout, output_status);
            try std.testing.expect(fixture.begins >= 4);
            try std.testing.expectEqual(fixture.begins, fixture.ends);
            try std.testing.expect(outbound.execution_wait.begin == null);
            // A partly delivered chunk cannot be reused as another response. The
            // controlled writer retires uncertainty; drain queued TCP bytes and
            // require EOF rather than a live, idle connection after the failure.
            try peer.socket.setRecvTimeout(1000);
            var bytes: [64 * 1024]u8 = undefined;
            var drained_bytes: usize = 0;
            std.debug.print("REAL_SINK await peer EOF\n", .{});
            while (true) {
                const count = peer.socket.recv(&bytes) catch |err| {
                    std.debug.print("REAL_SINK peer err={} drained={d}\n", .{ err, drained_bytes });
                    return err;
                };
                if (count == 0) break;
                drained_bytes += count;
                if (drained_bytes > 64 * 1024 * 1024) return error.FixtureDrainExceeded;
            }
            try std.testing.expect(drained_bytes > 0);
            port.finish(capsule.?);
            finished = true;
            const drained = state.owner.scheduler.stats();
            try std.testing.expectEqual(@as(usize, 0), drained.operations);
            try std.testing.expectEqual(@as(usize, 0), drained.runnable_slots);
            try std.testing.expectEqual(@as(u64, 0), drained.retained_bytes);
        }
    }
}
