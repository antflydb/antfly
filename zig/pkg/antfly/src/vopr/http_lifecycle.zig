// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production HTTP/1 lifecycle coverage on VoprIo. The server and handlers are
//! the real httpx implementation; this module owns only deterministic driving
//! and assertions.

const std = @import("std");
const httpx = @import("httpx");
const vopr = @import("vopr");
const http_disconnect = @import("http_disconnect.zig");

const VoprTestAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

const HandlerState = struct {
    echo_calls: usize = 0,
    stream_calls: usize = 0,
    saw_cancel: bool = false,

    fn echo(self: *HandlerState, ctx: *httpx.Context) anyerror!httpx.Response {
        self.echo_calls += 1;
        self.saw_cancel = self.saw_cancel or ctx.isCancellationRequested();
        const body = (try ctx.body()) orelse "";
        return ctx.text(body);
    }

    fn stream(self: *HandlerState, ctx: *httpx.Context) anyerror!httpx.Response {
        self.stream_calls += 1;
        self.saw_cancel = self.saw_cancel or ctx.isCancellationRequested();
        var writer = try ctx.streamResponse(200);
        try writer.write("alpha");
        try ctx.io.sleep(.zero, .awake);
        try writer.write("beta");
        try writer.close();
        return ctx.response.build();
    }
};

fn driveUntilQuiescent(
    sim: *vopr.vopr_io.VoprIo,
    alloc: std.mem.Allocator,
    transition_budget: usize,
) !void {
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    const scheduler = sim.scheduler();
    var transitions: usize = 0;
    while (!scheduler.quiescent()) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        if (enabled.items.items.len == 0) return error.VoprHttpLifecycleDeadlock;
        var selected = enabled.items.items[0];
        for (enabled.items.items) |candidate| {
            // Prefer concrete work over a timeout while a request can still
            // advance; time remains enabled and is selected when it is the
            // only progress transition.
            if (!std.mem.eql(u8, candidate.name, "vopr-io.time_advance")) {
                selected = candidate;
                break;
            }
        }
        try scheduler.executeReady(selected.id, &events, alloc);
        transitions += 1;
        if (transitions > transition_budget) return error.VoprHttpLifecycleTransitionBudgetExceeded;
    }
}

fn driveUntilListenerStarted(
    sim: *vopr.vopr_io.VoprIo,
    alloc: std.mem.Allocator,
    server: *httpx.Server,
) !void {
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    const scheduler = sim.scheduler();
    var transitions: usize = 0;
    while (!server.listen_started.load(.acquire)) {
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        if (enabled.items.items.len == 0) return error.VoprHttpLifecycleListenerDeadlock;
        try scheduler.executeReady(enabled.items.items[0].id, &events, alloc);
        transitions += 1;
        if (transitions > 100) return error.VoprHttpLifecycleListenerStartBudgetExceeded;
    }
}

test "production HTTP lifecycle runs chunked keep-alive pipeline and stream on VoprIo" {
    var alloc_state: VoprTestAllocator = .init;
    defer _ = alloc_state.deinit();
    const alloc = alloc_state.allocator();
    var sim = try vopr.vopr_io.VoprIo.init(.{
        .tasks = .{ .stack_size = 4 * 1024 * 1024 },
        .network = .{ .max_sockets = 8 },
        .required = .of(&.{ .clock_read, .sockets, .task_scheduling, .synchronization, .sleep }),
    });
    defer sim.deinit();
    const io = sim.io();

    var http_runtime = httpx.HttpRuntime.init(alloc, .{
        .max_active_h1_requests = 0,
        .max_active_connections = 2,
        .max_active_requests = 2,
        .max_listeners = 1,
        .borrowed_io = .{ .listener = io, .connection = io, .request = io },
    });
    defer http_runtime.deinit();
    var server = httpx.Server.initWithConfig(alloc, io, .{
        .host = "127.0.0.1",
        .port = 0,
        .h1_disconnect_cancellation = .disabled,
        .max_connections = 2,
        .max_request_tasks = 2,
        .http_runtime = &http_runtime,
    });
    defer server.deinit();
    var handlers = HandlerState{};
    try server.post("/echo", httpx.Handler.bind(&handlers, HandlerState.echo));
    try server.get("/stream", httpx.Handler.bind(&handlers, HandlerState.stream));

    var listener = httpx.Server.ListenerTask.init(&server);
    try listener.start();
    // `start` publishes the future after binding. Let the listener fiber enter
    // accept before the client becomes eligible; a real kernel also makes a
    // bound listener connectable independently of accept scheduling.
    try driveUntilListenerStarted(&sim, alloc, &server);
    try std.testing.expect(server.listen_started.load(.acquire));
    const address = server.boundAddress() orelse return error.VoprHttpLifecycleListenerNotBound;

    const Shared = struct {
        io: std.Io,
        address: httpx.socket.Address,
        listener: *httpx.Server.ListenerTask,
        response: [4096]u8 = undefined,
        response_len: usize = 0,
        failure: ?anyerror = null,
        done: bool = false,

        fn fail(self: *@This(), err: anyerror) void {
            self.failure = err;
            self.listener.requestStop();
            self.listener.join() catch {};
            self.done = true;
        }

        fn run(self: *@This()) void {
            var socket = httpx.Socket.connect(self.address, self.io) catch |err| return self.fail(err);
            defer socket.close();

            // Deliberately split the chunked body across writes, then pipeline
            // a second request before reading either response. The half-close
            // proves that already-buffered request bytes are not mistaken for
            // cancellation of the first handler.
            socket.sendAll(
                "POST /echo HTTP/1.1\r\n" ++
                    "Host: vopr\r\n" ++
                    "Transfer-Encoding: chunked\r\n" ++
                    "Connection: keep-alive\r\n\r\n" ++
                    "5\r\nhello\r\n",
            ) catch |err| return self.fail(err);
            socket.sendAll(
                "6\r\n world\r\n0\r\n\r\n" ++
                    "GET /stream HTTP/1.1\r\n" ++
                    "Host: vopr\r\n" ++
                    "Connection: close\r\n\r\n",
            ) catch |err| return self.fail(err);
            socket.shutdownWrite() catch |err| return self.fail(err);

            while (self.response_len < self.response.len) {
                const count = socket.recv(self.response[self.response_len..]) catch |err| return self.fail(err);
                if (count == 0) break;
                self.response_len += count;
            }
            if (self.response_len == self.response.len) return self.fail(error.VoprHttpLifecycleResponseTooLarge);
            self.listener.requestStop();
            self.listener.join() catch |err| return self.fail(err);
            self.done = true;
        }
    };

    var shared = Shared{ .io = io, .address = address, .listener = &listener };
    _ = io.async(Shared.run, .{&shared});
    try driveUntilQuiescent(&sim, alloc, 20_000);

    try std.testing.expect(shared.done);
    if (shared.failure) |err| {
        std.debug.print("VOPR HTTP lifecycle client failed: {s}\n", .{@errorName(err)});
        return err;
    }
    const response = shared.response[0..shared.response_len];
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, response, "HTTP/1.1 200"));
    try std.testing.expect(std.mem.indexOf(u8, response, "\r\n\r\nhello world") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "5\r\nalpha\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, response, "4\r\nbeta\r\n0\r\n\r\n") != null);
    try std.testing.expectEqual(@as(usize, 1), handlers.echo_calls);
    try std.testing.expectEqual(@as(usize, 1), handlers.stream_calls);
    try std.testing.expect(!handlers.saw_cancel);
    try std.testing.expectEqual(@as(usize, 0), server.runtimeStats().active_connections);
    try std.testing.expectEqual(@as(usize, 0), server.runtimeStats().active_requests);
    try sim.ensureNoCapabilityViolation();
}

test "production HTTP lifecycle observes VoprIo hard disconnect behind pipelined input" {
    var alloc_state: VoprTestAllocator = .init;
    defer _ = alloc_state.deinit();
    const alloc = alloc_state.allocator();
    var sim = try vopr.vopr_io.VoprIo.init(.{
        .tasks = .{ .stack_size = 4 * 1024 * 1024 },
        .network = .{ .max_sockets = 4 },
        .required = .of(&.{ .clock_read, .sockets, .task_scheduling, .synchronization, .sleep }),
    });
    defer sim.deinit();
    const io = sim.io();
    var disconnect_probe = http_disconnect.Probe{ .vopr_io = &sim };

    var http_runtime = httpx.HttpRuntime.init(alloc, .{
        .max_active_h1_requests = 0,
        .max_active_connections = 1,
        .max_active_requests = 1,
        .max_listeners = 1,
        .borrowed_io = .{ .listener = io, .connection = io, .request = io },
    });
    defer http_runtime.deinit();
    var server = httpx.Server.initWithConfig(alloc, io, .{
        .host = "127.0.0.1",
        .port = 0,
        .h1_disconnect_cancellation = .required,
        .h1_disconnect_probe = disconnect_probe.iface(),
        .max_connections = 1,
        .max_request_tasks = 1,
        .http_runtime = &http_runtime,
    });
    defer server.deinit();

    const State = struct {
        started: bool = false,
        canceled: bool = false,

        fn slow(self: *@This(), ctx: *httpx.Context) anyerror!httpx.Response {
            self.started = true;
            while (!ctx.isCancellationRequested()) try ctx.io.sleep(.zero, .awake);
            self.canceled = true;
            return error.Canceled;
        }
    };
    var state = State{};
    try server.get("/slow", httpx.Handler.bind(&state, State.slow));
    var listener = httpx.Server.ListenerTask.init(&server);
    try listener.start();
    try driveUntilListenerStarted(&sim, alloc, &server);
    const address = server.boundAddress() orelse return error.VoprHttpLifecycleListenerNotBound;

    const Shared = struct {
        io: std.Io,
        sim: *vopr.vopr_io.VoprIo,
        address: httpx.socket.Address,
        listener: *httpx.Server.ListenerTask,
        state: *State,
        failure: ?anyerror = null,
        done: bool = false,

        fn run(self: *@This()) void {
            var socket = httpx.Socket.connect(self.address, self.io) catch |err| return self.fail(err);
            socket.sendAll("GET /slow HTTP/1.1\r\nHost: vopr\r\n\r\nG") catch |err| return self.fail(err);
            while (!self.state.started) self.io.sleep(.zero, .awake) catch {};
            self.sim.abortNetworkSocket(socket.handle) catch |err| return self.fail(err);
            while (!self.state.canceled) self.io.sleep(.zero, .awake) catch {};
            self.listener.requestStop();
            self.listener.join() catch |err| return self.fail(err);
            self.done = true;
        }

        fn fail(self: *@This(), err: anyerror) void {
            self.failure = err;
            self.listener.requestStop();
            self.listener.join() catch {};
            self.done = true;
        }
    };
    var shared = Shared{
        .io = io,
        .sim = &sim,
        .address = address,
        .listener = &listener,
        .state = &state,
    };
    _ = io.async(Shared.run, .{&shared});
    try driveUntilQuiescent(&sim, alloc, 20_000);

    try std.testing.expect(shared.done);
    if (shared.failure) |err| return err;
    try std.testing.expect(state.canceled);
    try std.testing.expectEqual(@as(u64, 1), server.httpRuntimeStats().h1_hard_disconnect_cancellations_total);
    try std.testing.expectEqual(@as(usize, 0), server.runtimeStats().active_connections);
    try std.testing.expectEqual(@as(usize, 0), server.runtimeStats().active_requests);
    try sim.ensureNoCapabilityViolation();
}

test "production HTTP TLS termination boundary fails closed before bind" {
    var server = httpx.Server.initWithConfig(std.testing.allocator, std.testing.io, .{ // vopr-audit: allow(host_filesystem) TLS fails closed during construction before host I/O starts
        .host = "127.0.0.1",
        .port = 0,
        .h1_disconnect_cancellation = .disabled,
        .tls_cert_path = "/operator/tls.crt",
        .tls_key_path = "/operator/tls.key",
    });
    defer server.deinit();
    try std.testing.expectError(error.ServerTlsUnsupported, server.bind());

    var incomplete = httpx.Server.initWithConfig(std.testing.allocator, std.testing.io, .{ // vopr-audit: allow(host_filesystem) TLS fails closed during construction before host I/O starts
        .host = "127.0.0.1",
        .port = 0,
        .h1_disconnect_cancellation = .disabled,
        .tls_cert_path = "/operator/tls.crt",
    });
    defer incomplete.deinit();
    try std.testing.expectError(error.InvalidServerTlsConfiguration, incomplete.bind());
}
