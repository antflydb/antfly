// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Actual child process, acknowledged RPC output, API owner and physical TCP waits.
const std = @import("std");
const runtime = @import("antfly_runtime_abi");
const owner_module = @import("runnable_owner_execution.zig");
const runnable = @import("antfly_local_sources").common_request_admission.runnable_scheduler;
const rpc = @import("inference_worker_rpc");
const chunk_size = 1024 * 1024;

test "inference worker subprocess output retains borrowed capacity through physical waits and actual reap" {
    const httpx = @import("httpx");
    const io = std.testing.io;
    const executable = std.testing.environ.getPosix("ANTFLY_TEST_WORKER_OUTPUT_FIXTURE") orelse return error.WorkerFixturePathMissing;
    const Fixture = struct {
        state: *owner_module.State,
        port: runtime.runnable_abi.Owner,
        capsule: *anyopaque,
        transport: *runtime.http_bridge.Outbound,
        endpoint: *rpc.Endpoint = undefined,
        child: *std.process.Child = undefined,
        reaped: std.atomic.Value(bool) = .init(false),
        held_through_reap: std.atomic.Value(bool) = .init(false),
        retained_bytes: u64 = 0,
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
        fn closed(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const before = self.state.owner.scheduler.stats();
            self.child.kill(self.io);
            const after = self.state.owner.scheduler.stats();
            self.held_through_reap.store(before.operations == 1 and after.operations == 1 and before.retained_bytes == self.retained_bytes and after.retained_bytes == self.retained_bytes, .release);
            self.reaped.store(self.child.id == null, .release);
        }
        fn reject(_: *anyopaque, _: *rpc.Request) !rpc.OwnedPayload {
            return error.UnsupportedOperation;
        }
        fn rpcCheck(raw: ?*anyopaque) !void {
            try check(raw);
        }
        fn phase(raw: ?*anyopaque, value: rpc.Phase) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (value != .checkpoint) return error.InvalidWorkerPhase;
            const result = self.port.checkpoint(self.capsule);
            if (!result.isOk()) return runtime.error_abi.errorFromStatus(result);
        }
        fn event(raw: ?*anyopaque, payload: rpc.Payload) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            try std.testing.expectEqualStrings("chunk", payload.metadata);
            try std.testing.expectEqual(chunk_size, payload.body.len);
            const active = self.state.owner.scheduler.stats();
            if (active.runnable_slots - active.control_slots != 2) return error.FormatterHasNoOrdinaryGrant;
            const sink = self.transport.stream();
            const result = sink.write.?(sink.context, .init(payload.body));
            if (result != .ok) return switch (result) {
                .canceled => error.Canceled,
                .timeout => error.Timeout,
                else => error.OutputFailed,
            };
        }
        fn run(self: *@This()) !runtime.http_abi.CallbackStatus {
            defer self.done.store(true, .release);
            const sink = self.transport.stream();
            const installed = sink.set_execution_wait.?(sink.context, &.{ .context = self, .begin = begin, .end = end });
            if (installed != .ok) return installed;
            defer _ = sink.set_execution_wait.?(sink.context, null);
            const started = sink.start.?(sink.context, 200, .init("application/x-ndjson"), .{});
            if (started != .ok) return started;
            const active = self.state.owner.scheduler.stats();
            if (active.runnable_slots - active.control_slots != 2) return error.FormatterHasNoOrdinaryGrant;
            self.body_started = true;
            const response = self.endpoint.call(.{ .body = "stream" }, .{ .ptr = self, .check = rpcCheck, .event = event, .phase = phase, .fence_on_error = true, .event_owns_waits = true }) catch |err| {
                std.debug.print("PROCESS_RPC cause={} fenced={} reaped={} held={}\n", .{ err, self.endpoint.closed_fence.isSet(), self.reaped.load(.acquire), self.held_through_reap.load(.acquire) });
                if (!self.endpoint.closed_fence.isSet()) return error.BorrowedPeerNotFenced;
                if (!self.reaped.load(.acquire)) return error.BorrowedPeerNotReaped;
                if (!self.held_through_reap.load(.acquire)) return error.BorrowedOwnerRetiredEarly;
                return switch (err) {
                    error.Canceled, error.Cancelled => .canceled,
                    error.Timeout, error.DeadlineExceeded => .timeout,
                    else => return err,
                };
            };
            defer response.deinit();
            return error.FixtureBufferDidNotFill;
        }
    };
    for ([_]bool{ false, true }) |use_h2| {
        for ([_]bool{ false, true }) |cancel| {
            std.debug.print("PROCESS_SINK transport={s} cancel={} start\n", .{ if (use_h2) "h2" else "h1", cancel });
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
            var fixture = Fixture{ .state = &state, .port = port, .capsule = undefined, .transport = &outbound, .io = io, .deadline_ns = Fixture.now(io) + 750 * std.time.ns_per_ms, .h2 = if (use_h2) &h2 else null };
            outbound.write_control = .{ .ptr = &fixture, .check_fn = Fixture.check, .remaining_ms_fn = Fixture.remaining };
            var capsule: ?*anyopaque = null;
            const start = port.start(port.context, &.{ .io = &borrow, .clock_io = &borrow, .class = @backingInt(runtime.runnable_abi.Class.inference), .slots = 2, .retained_bytes = 2 * chunk_size, .deadline_ns = fixture.deadline_ns, .has_deadline = 1, .cancellation = .{ .context = &fixture, .is_cancelled = Fixture.requested }, .out_handle = &capsule });
            if (!start.isOk()) return runtime.error_abi.errorFromStatus(start);
            fixture.capsule = capsule.?;
            var finished = false;
            defer if (!finished) port.finish(capsule.?);
            const retained = state.owner.scheduler.stats().retained_bytes;
            fixture.retained_bytes = retained;
            var child = try std.process.spawn(io, .{ .argv = &.{executable}, .stdin = .pipe, .stdout = .pipe, .stderr = .inherit });
            errdefer child.kill(io);
            fixture.child = &child;
            var endpoint = rpc.Endpoint{ .alloc = std.testing.allocator, .io = io, .input = child.stdout.?, .output = child.stdin.?, .context = &fixture, .handler = Fixture.reject, .on_closed = Fixture.closed, .next_id = 1 };
            fixture.endpoint = &endpoint;
            try endpoint.start();
            defer endpoint.deinit();
            var task = try io.concurrent(Fixture.run, .{&fixture});
            defer _ = task.cancel(io) catch .failed;
            std.debug.print("PROCESS_SINK await backpressure\n", .{});
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
            std.debug.print("PROCESS_SINK output status={s} begins={d} ends={d}\n", .{ @tagName(output_status), fixture.begins, fixture.ends });
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
            std.debug.print("PROCESS_SINK await peer EOF\n", .{});
            while (true) {
                const count = peer.socket.recv(&bytes) catch |err| {
                    std.debug.print("PROCESS_SINK peer err={} drained={d}\n", .{ err, drained_bytes });
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
