// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Actual HTTP/1.1 loopback qualification, with no public listener or runtime
//! capability override. Successful extraction uses the generated Node routes.
//! The separate cancellation route is a test-owned synchronization boundary;
//! it does not alter a production route, Node field, or request option.
const std = @import("std");
const builtin = @import("builtin");
const httpx = @import("httpx");
const platform = @import("antfly_platform");
const Node = @import("server.zig").Node;
const shared = @import("gliner_boundary_service_test.zig");
const pipeline = @import("../pipelines/gliner_boundary_pipeline.zig");
const fixtures = @import("../architectures/gliner_boundary_parity_test.zig");
const model = @import("../models/gliner_boundary.zig");
const BoundedAllocator = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;
const Io = std.Io;
const clock = platform.time.monotonicNs;
const short_timeout_ms = 5_000;
const request_timeout_ms = 180_000;

/// Stable addresses for Server, its ListenerTask, executors and allocator.
/// Node and its watchdog borrow std.testing.io, whose lifetime extends beyond
/// this transport owner. Per-request controls borrow server_io only until the
/// listener joins. Model weights and request scratch retain Node admission.
pub const Loopback = struct {
    allocator: Allocator,
    budget: BoundedAllocator,
    server_io: Io.Threaded,
    client_io: Io.Threaded,
    server: httpx.Server,
    client: httpx.Client,
    listener: httpx.Server.ListenerTask,
    address: ?Io.net.IpAddress = null,

    pub fn init(a: Allocator, node: *Node) !*Loopback {
        // Caller-owned listeners need the same exclusive startup precondition
        // as Node.serve. Publish the resource owner before request workers can
        // race its lazy creation; this neither loads a model nor reserves work.
        try node.model_manager.ensureResourceOwnerReady();
        const self = try a.create(Loopback);
        errdefer a.destroy(self);
        self.allocator = a;
        self.budget = .{ .backing = a, .limit = 16 * 1024 * 1024 };
        const transport_allocator = self.budget.allocator();
        self.server_io = Io.Threaded.init(transport_allocator, .{ .concurrent_limit = .limited(4) });
        errdefer self.server_io.deinit();
        self.client_io = Io.Threaded.init(transport_allocator, .{ .concurrent_limit = .limited(4) });
        errdefer self.client_io.deinit();
        self.server = httpx.Server.initWithConfig(transport_allocator, self.server_io.io(), .{
            .host = "127.0.0.1",
            .port = 0,
            .max_body_size = 64 * 1024,
            .max_headers = 32,
            .max_connections = 2,
            .max_request_tasks = 2,
            .max_h1_inflight_bodies = 1,
            .header_read_timeout_ms = short_timeout_ms,
            .body_read_timeout_ms = short_timeout_ms,
            .response_write_timeout_ms = short_timeout_ms,
            .keep_alive_timeout_ms = short_timeout_ms,
            .keep_alive = false,
            .max_requests_per_connection = 1,
            .request_body_buffer_budget_bytes = 128 * 1024,
        });
        errdefer self.server.deinit();
        // The ingress deadline is fixed by this test owner, never by request
        // contents. Socket timeouts separately bound connect and delivery.
        try self.server.preRoute(deadline);
        try node.registerHttpRoutes(&self.server);
        self.client = httpx.Client.initWithConfig(transport_allocator, self.client_io.io(), .{
            .timeouts = .{
                .connect_ms = short_timeout_ms,
                .read_ms = request_timeout_ms,
                .write_ms = short_timeout_ms,
                .request_ms = request_timeout_ms,
            },
            .retry_policy = .noRetry(),
            .redirect_policy = .{ .follow_redirects = false, .max_redirects = 0 },
            .max_response_size = 1024 * 1024,
            .max_response_headers = 64,
            .keep_alive = false,
            .pool_max_connections = 1,
            .pool_max_per_host = 1,
            .cookies_enabled = false,
            .cancel_in_flight_on_shutdown = true,
        });
        self.listener = .init(&self.server);
        self.address = null;
        return self;
    }

    fn deadline(ctx: *httpx.Context) !void {
        ctx.application_deadline_ns = clock() + request_timeout_ms * std.time.ns_per_ms;
    }

    pub fn start(self: *Loopback) !void {
        // Read the bound address before publishing any listener task. There is
        // no free-port probe/rebind race and no concurrent listener-field read.
        try self.server.bind();
        self.address = self.server.boundAddress() orelse return error.MissingLoopbackAddress;
        const address = self.address.?;
        try std.testing.expect(address == .ip4);
        try std.testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &address.ip4.bytes);
        try std.testing.expect(address.ip4.port != 0);
        try self.listener.start();
        const until = clock() + short_timeout_ms * std.time.ns_per_ms;
        while (!self.server.listen_started.load(.acquire)) {
            if (self.listener.runtimeFailure()) |err| return err;
            if (clock() >= until) return error.LoopbackStartTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
    }

    pub fn request(self: *Loopback, method: httpx.types.Method, path: []const u8, bytes: ?[]const u8) !httpx.Response {
        const a = self.budget.allocator();
        const url = try std.fmt.allocPrint(a, "http://127.0.0.1:{d}{s}", .{ self.address.?.ip4.port, path });
        defer a.free(url);
        return self.client.request(method, url, .{
            .borrowed_body = bytes,
            .headers = &.{.{ "Content-Type", "application/json" }},
            .timeout_ms = if (method == .GET) short_timeout_ms else request_timeout_ms,
        });
    }

    pub fn post(self: *Loopback, bytes: []const u8) !httpx.Response {
        return self.request(.POST, "/ai/v1/extract", bytes);
    }

    pub fn idle(self: *Loopback) !void {
        const until = clock() + short_timeout_ms * std.time.ns_per_ms;
        while (true) {
            const stats = self.server.runtimeStats();
            if (stats.active_connections == 0 and stats.active_requests == 0 and
                self.server.httpRuntimeStats().active_h1_cancellation_observers == 0) break;
            if (self.listener.runtimeFailure()) |err| return err;
            if (clock() >= until) return error.LoopbackDrainTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
        try std.testing.expectEqual(@as(usize, 0), self.server.runtimeStats().body_buffer_in_use_bytes);
    }

    pub fn finish(self: *Loopback) !void {
        const started = clock();
        self.listener.shutdown(25);
        try self.listener.join();
        try std.testing.expect(clock() - started < short_timeout_ms * std.time.ns_per_ms);
        try std.testing.expectEqual(httpx.Server.ListenerTask.RuntimeState.stopped, self.listener.runtimeState());
        try self.idle();
        const stats = self.server.httpRuntimeStats();
        try std.testing.expectEqual(@as(usize, 0), stats.active_listener_leases);
        try std.testing.expectEqual(@as(usize, 0), stats.reserved_connection_capacity);
        try std.testing.expectEqual(@as(usize, 0), stats.reserved_request_capacity);
        try std.testing.expectEqual(@as(usize, 0), stats.reserved_h1_request_capacity);
    }

    pub fn deinit(self: *Loopback) void {
        // Also runs after a failed assertion or response parse. join consumes
        // its Future on errors; never destroy routes/Node under live handlers.
        self.listener.requestStop();
        self.listener.join() catch {};
        self.client.deinit();
        self.server.deinit();
        self.client_io.deinit();
        self.server_io.deinit();
        std.debug.assert(self.budget.live == 0);
        self.allocator.destroy(self);
    }

    fn connect(self: *Loopback) !httpx.Socket {
        // Zig 0.16 Threaded.netConnectIpPosix panics for a non-none native
        // timeout. Use the same structured race as the HTTP client's deadline:
        // cancel and join the loser, closing any late successful connection.
        const Outcome = union(enum) { connected: anyerror!httpx.Socket, deadline: Io.Cancelable!void };
        const Tasks = struct {
            fn expire(io: Io) Io.Cancelable!void {
                try io.sleep(.fromMilliseconds(short_timeout_ms), .awake);
            }
            fn discard(outcome: Outcome) void {
                switch (outcome) {
                    .connected => |result| if (result) |value| {
                        var socket = value;
                        socket.close();
                    } else |_| {},
                    .deadline => {},
                }
            }
        };
        const io = self.client_io.io();
        var storage: [2]Outcome = undefined;
        var select = Io.Select(Outcome).init(io, &storage);
        try select.concurrent(.connected, httpx.Socket.connect, .{ self.address.?, io });
        errdefer while (select.cancel()) |late| Tasks.discard(late);
        try select.concurrent(.deadline, Tasks.expire, .{io});
        switch (try select.await()) {
            .connected => |result| {
                select.cancelDiscard();
                return try result;
            },
            .deadline => |result| {
                while (select.cancel()) |late| Tasks.discard(late);
                try result;
                return error.Timeout;
            },
        }
    }

    fn barrierSocket(self: *Loopback, body: []const u8) !httpx.Socket {
        var socket = try self.connect();
        errdefer socket.close();
        try socket.setRecvTimeout(short_timeout_ms);
        try socket.setSendTimeout(short_timeout_ms);
        var header: [256]u8 = undefined;
        const bytes = try std.fmt.bufPrint(&header, "POST /test/cancellation HTTP/1.1\r\nHost: 127.0.0.1\r\nContent-Type: application/json\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n", .{body.len});
        try socket.sendAll(bytes);
        try socket.sendAll(body);
        return socket;
    }
};

pub fn metric(response: *const httpx.Response, name: []const u8, expected: u64) !void {
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
    // metricsHandler returns ctx.text(), whose established text helper sets
    // the final MIME value after the handler's earlier header assignment.
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", response.header("Content-Type").?);
    const body = response.body orelse return error.MissingMetricsBody;
    var lines = std.mem.splitScalar(u8, body, '\n');
    var found: usize = 0;
    while (lines.next()) |line| {
        if (line.len <= name.len or line[name.len] != ' ' or !std.mem.startsWith(u8, line, name)) continue;
        found += 1;
        try std.testing.expectEqual(expected, try std.fmt.parseInt(u64, line[name.len + 1 ..], 10));
    }
    try std.testing.expectEqual(@as(usize, 1), found);
}

const CancellationBarrier = struct {
    node: *Node,
    entered: std.atomic.Value(u32) = .init(0),
    confirmed: std.atomic.Value(u32) = .init(0),
    completed: std.atomic.Value(u32) = .init(0),

    fn handle(self: *CancellationBarrier, ctx: *httpx.Context) !httpx.Response {
        defer _ = self.completed.fetchAdd(1, .release);
        _ = self.entered.fetchAdd(1, .release);
        const until = clock() + short_timeout_ms * std.time.ns_per_ms;
        while (!ctx.isCancellationRequested()) {
            if (clock() >= until) return error.TransportCancellationNotObserved;
            ctx.io.sleep(.fromMilliseconds(1), .awake) catch |err| {
                if (!ctx.isCancellationRequested()) return err;
            };
        }
        // The signal belongs to the real HTTP transport. Call the Node with
        // that Context and verify rejection before parsing/admission/loading.
        var response = try self.node.extractJSON(ctx);
        defer response.deinit();
        try std.testing.expectEqual(@as(u16, 408), response.status.code);
        var parsed = try std.json.parseFromSlice(std.json.Value, ctx.allocator, response.body.?, .{});
        defer parsed.deinit();
        try std.testing.expectEqualStrings("INFERENCE_CANCELLED", parsed.value.object.get("error").?.string);
        _ = self.confirmed.fetchAdd(1, .release);
        // A disconnected peer must not receive a manufactured HTTP response.
        return error.Canceled;
    }

    fn waitEntered(self: *CancellationBarrier, count: u32) !void {
        const until = clock() + short_timeout_ms * std.time.ns_per_ms;
        while (self.entered.load(.acquire) != count) {
            if (clock() >= until) return error.CancellationBarrierStartTimeout;
            try std.testing.io.sleep(.fromMilliseconds(1), .awake);
        }
    }
};

test "gliner boundary socket transport gate disconnect retry metrics and shutdown without model" {
    if (builtin.os.tag != .macos and builtin.os.tag != .linux) return error.SkipZigTest;
    const a = std.testing.allocator;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const parent = try temporary.dir.realPathFileAlloc(std.testing.io, ".", a);
    defer a.free(parent);
    const config = try fixtures.fixtureBytes(a, "models/small/config.json");
    defer a.free(config);
    const encoder = try fixtures.fixtureBytes(a, "models/small/encoder_config.json");
    defer a.free(encoder);
    try temporary.dir.createDirPath(std.testing.io, "socket-gate/encoder_config");
    try temporary.dir.writeFile(std.testing.io, .{ .sub_path = "socket-gate/config.json", .data = config });
    try temporary.dir.writeFile(std.testing.io, .{ .sub_path = "socket-gate/encoder_config/config.json", .data = encoder });
    try temporary.dir.writeFile(std.testing.io, .{ .sub_path = "socket-gate/model.safetensors", .data = "never loaded" });
    var node = try Node.init(a, .{
        .models_dir = parent,
        .max_concurrent_requests = 1,
        .process_termination_available = true,
        .generation_budget_overrides = .{ .host_limit_bytes = 32 * 1024 * 1024, .scratch_limit_bytes = 128 * 1024 },
    });
    defer node.deinit();
    shared.useNativeBackend(&node);
    try node.attachIo(std.testing.io);
    var barrier = CancellationBarrier{ .node = &node };
    const transport = try Loopback.init(a, &node);
    defer transport.deinit();
    try transport.server.post("/test/cancellation", httpx.Handler.bind(&barrier, CancellationBarrier.handle));
    try transport.start();
    var schema = try std.json.parseFromSlice(std.json.Value, a, "{\"entities\":[\"person\"]}", .{});
    defer schema.deinit();
    const raw = try shared.requestBytes(a, "socket-gate", schema.value, &.{.{ .content = "Ada" }});
    defer a.free(raw);
    {
        var response = try transport.post(raw);
        defer response.deinit();
        try shared.errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
    }
    try transport.idle();
    _ = try shared.idle(&node);
    {
        var socket = try transport.barrierSocket(raw);
        var open = true;
        defer if (open) socket.close();
        try barrier.waitEntered(1);
        // A hard reset, unlike a legal HTTP half-close, must cancel the active
        // handler. The socket is still owned here; no stale descriptor is used.
        var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
        try std.posix.setsockopt(socket.handle, std.posix.SOL.SOCKET, std.posix.SO.LINGER, std.mem.asBytes(&linger));
        socket.close();
        open = false;
        try transport.idle();
        try std.testing.expectEqual(@as(u32, 1), barrier.confirmed.load(.acquire));
        try std.testing.expectEqual(@as(u32, 1), barrier.completed.load(.acquire));
        try std.testing.expectEqual(@as(u64, 1), transport.server.httpRuntimeStats().h1_hard_disconnect_cancellations_total);
        _ = try shared.idle(&node);
    }
    // The same generated route and listener remain usable after disconnect.
    {
        var response = try transport.post(raw);
        defer response.deinit();
        try shared.errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
    }
    try transport.idle();
    {
        var response = try transport.request(.GET, "/ml/v1/metrics", null);
        defer response.deinit();
        try metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"http\"}", 2);
        try metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"unsupported\"}", 2);
        try metric(&response, "antfly_inference_extract_v2_active", 0);
        try metric(&response, "antfly_inference_extract_v2_decoded_items_total", 0);
    }
    try transport.idle();
    var held = try transport.barrierSocket(raw);
    defer held.close();
    try barrier.waitEntered(2);
    // Unlike the first request, the client stays connected. Expiring the
    // caller-owned graceful-drain period must cancel and join this handler.
    try transport.finish();
    try std.testing.expectEqual(@as(u32, 2), barrier.confirmed.load(.acquire));
    try std.testing.expectEqual(@as(u32, 2), barrier.completed.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), transport.server.runtimeStats().request_cancellations_total);
    try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded.count());
    try std.testing.expectEqual(@as(usize, 0), (try shared.idle(&node)).hostTotalBytes());
    try std.testing.expect(!model.runtime_available);
}

test "gliner boundary socket pinned small real HTTP success atomic recovery and metrics" {
    if (builtin.os.tag != .macos and builtin.os.tag != .linux) return error.SkipZigTest;
    const requested = platform.env.getenv("ANTFLY_GLINER25_SMALL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var path: [Io.Dir.max_path_bytes]u8 = undefined;
    const length = if (std.fs.path.isAbsolute(requested))
        try Io.Dir.realPathFileAbsolute(std.testing.io, requested, &path)
    else
        try Io.Dir.cwd().realPathFile(std.testing.io, requested, &path);
    const directory = path[0..length];
    const name = std.fs.path.basename(directory);
    const bytes = try fixtures.fixtureBytes(a, "pipeline_cases.json");
    defer a.free(bytes);
    var fixture = try std.json.parseFromSlice(pipeline.ReferenceFixture, a, bytes, .{});
    defer fixture.deinit();
    const pins = fixture.value.model_files;
    try shared.verifyFiles(a, directory, pins);
    const case = fixture.value.cases[0];
    try std.testing.expectEqualStrings("mixed_tasks", case.id);
    const raw = try shared.requestBytes(a, name, case.schema, &.{.{ .id = case.id, .content = case.text }});
    defer a.free(raw);
    {
        var node = try Node.init(a, .{
            .models_dir = std.fs.path.dirname(directory) orelse return error.InvalidModelPath,
            .max_loaded_models = 1,
            .max_concurrent_requests = 1,
            .keep_alive_ms = 30 * 60 * 1000,
            .process_termination_available = true,
            .generation_budget_overrides = .{ .host_limit_bytes = 1024 * 1024 * 1024, .scratch_limit_bytes = 128 * 1024 * 1024 },
        });
        defer node.deinit();
        shared.useNativeBackend(&node);
        try node.attachIo(std.testing.io);
        const transport = try Loopback.init(a, &node);
        defer transport.deinit();
        try transport.start();
        try std.testing.expect(!node.test_allow_unqualified_gliner_boundary);
        {
            var response = try transport.post(raw);
            defer response.deinit();
            try shared.errorResponse(a, &response, 400, "UNSUPPORTED_EXTRACTION_FEATURE", "model", null);
        }
        try transport.idle();
        try std.testing.expectEqual(@as(usize, 0), node.model_manager.loaded.count());
        try std.testing.expectEqual(@as(usize, 0), (try shared.idle(&node)).hostTotalBytes());
        // No concurrent request reads this test-only field. The production
        // representation is void and the global published gate stays false.
        node.test_allow_unqualified_gliner_boundary = true;
        {
            const control = Control{ .io = std.testing.io, .hard_cancellation = node.hard_cancellation_watchdog.?.boundary(), .deadline_ns = clock() + request_timeout_ms * std.time.ns_per_ms };
            var lease = try control.enterUninterruptible(.process_required);
            lease.deinit();
            _ = try shared.idle(&node);
        }
        var prompt_tokens: i64 = 0;
        {
            var response = try transport.post(raw);
            defer response.deinit();
            prompt_tokens = try shared.successfulResponse(a, &response, name, case.id, case.text, case.expected);
        }
        try transport.idle();
        const cached = try shared.cachedModel(&node, directory, pins);
        const resident = try shared.idle(&node);
        try std.testing.expect(resident.host_weight_bytes >= pins.@"model.safetensors".size_bytes);
        try std.testing.expectEqual(@as(usize, 0), resident.backend_weight_bytes);
        {
            const batch = try shared.requestBytes(a, name, case.schema, &.{
                .{ .id = "decoded-first", .content = case.text },
                .{ .id = "rejected-second", .content = "x " ** 4097 },
            });
            defer a.free(batch);
            var response = try transport.post(batch);
            defer response.deinit();
            try shared.errorResponse(a, &response, 413, "EXTRACTION_LIMIT_EXCEEDED", "tokenizing", 1);
        }
        try transport.idle();
        try std.testing.expectEqual(resident, try shared.idle(&node));
        try std.testing.expectEqual(cached, try shared.cachedModel(&node, directory, pins));
        {
            const retry = try shared.requestBytes(a, name, case.schema, &.{.{ .content = case.text }});
            defer a.free(retry);
            var response = try transport.post(retry);
            defer response.deinit();
            try std.testing.expectEqual(prompt_tokens, try shared.successfulResponse(a, &response, name, null, case.text, case.expected));
        }
        try transport.idle();
        try std.testing.expectEqual(resident, try shared.idle(&node));
        try std.testing.expectEqual(cached, try shared.cachedModel(&node, directory, pins));
        {
            var response = try transport.request(.GET, "/ml/v1/metrics", null);
            defer response.deinit();
            try metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"http\"}", 4);
            try metric(&response, "antfly_inference_extract_v2_requests_total{transport=\"direct\"}", 0);
            try metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"success\"}", 2);
            try metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"unsupported\"}", 1);
            try metric(&response, "antfly_inference_extract_v2_outcomes_total{outcome=\"resource_limit\"}", 1);
            try metric(&response, "antfly_inference_extract_v2_failures_total{stage=\"model\"}", 1);
            try metric(&response, "antfly_inference_extract_v2_failures_total{stage=\"tokenizing\"}", 1);
            try metric(&response, "antfly_inference_extract_v2_parsed_items_total", 5);
            try metric(&response, "antfly_inference_extract_v2_decoded_items_total", 3);
            try metric(&response, "antfly_inference_extract_v2_returned_items_total", 2);
            try metric(&response, "antfly_inference_extract_v2_decoded_prompt_tokens_total", @intCast(3 * prompt_tokens));
            try metric(&response, "antfly_inference_extract_v2_decoded_output_values_total", 15);
            try metric(&response, "antfly_inference_extract_v2_active", 0);
        }
        try transport.idle();
        try std.testing.expectEqual(resident, try shared.idle(&node));
        try std.testing.expectEqual(cached, try shared.cachedModel(&node, directory, pins));
        try transport.finish();
        _ = try shared.idle(&node);
        try std.testing.expect(!model.runtime_available);
    }
    // Both network executors, listener, request owners and managed model are
    // destroyed before rehashing all five immutable source artifacts.
    try shared.verifyFiles(a, directory, pins);
}
