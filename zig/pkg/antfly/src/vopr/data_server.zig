// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0

//! Production DataServer composition on VoprIo. Production modules expose only
//! std.Io and transport-neutral executor seams; VOPR ownership stays here.

const std = @import("std");
const httpx = @import("httpx");
const vopr = @import("vopr");
const data_runtime = @import("../data/runtime.zig");
const background_runtime = @import("../storage/background_runtime.zig");
const http_common = @import("../common/http/http_common.zig");
const request_lifecycle = @import("request_lifecycle.zig");
const VoprTestAllocator = std.heap.DebugAllocator(.{ .stack_trace_frames = 0 });

const StubMetadataExecutor = struct {
    calls: usize = 0,

    fn executor(self: *StubMetadataExecutor) http_common.RequestExecutor {
        return .{ .ptr = self, .vtable = &.{ .execute = execute } };
    }

    fn execute(
        ptr: *anyopaque,
        _: std.mem.Allocator,
        _: http_common.HttpRequest,
    ) !http_common.HttpResponse {
        const self: *StubMetadataExecutor = @ptrCast(@alignCast(ptr));
        self.calls += 1;
        return .{ .status = 503 };
    }
};

const ScenarioOptions = struct {
    partial_write_limit: ?usize = null,
    prioritize_time: bool = false,
    half_close_request: bool = false,
    max_sockets: usize = 4096,
};

fn runProductionDataServerScenario(options: ScenarioOptions) !void {
    var alloc_state: VoprTestAllocator = .init;
    defer _ = alloc_state.deinit();
    const alloc = alloc_state.allocator();
    var vopr_io = try vopr.vopr_io.VoprIo.init(.{
        // The production HTTP stack intentionally uses large fixed parser and
        // formatting buffers. Match a production worker thread's stack rather
        // than the compact default used by protocol-level VOPR scenarios.
        .tasks = .{ .stack_size = 8 * 1024 * 1024 },
        .network = .{ .max_sockets = options.max_sockets },
        .instrumentation = .{ .enabled = false, .map_digest = 0x44535652 },
    });
    defer vopr_io.deinit();
    const io = vopr_io.io();

    var backend_runtime = try background_runtime.BackendRuntime.init(alloc, .{
        .backend = .manual,
        .borrowed_io = .{
            .general = io,
            .raft_inbound = io,
            .raft_outbound = io,
            .api = io,
            .inference = io,
            .control = io,
        },
    });
    defer backend_runtime.deinit();

    var lifecycle = request_lifecycle.Hook{ .vopr_io = &vopr_io };
    var metadata = StubMetadataExecutor{};
    const metadata_executors = [_]http_common.RequestExecutor{metadata.executor()};
    var server = try data_runtime.DataServer.initFromMetadataApiUrls(alloc, .{
        .replica_root_dir = "/vopr/data-server",
        .enable_data_raft = false,
        .backend_runtime = &backend_runtime,
        .metadata_request_executors = &metadata_executors,
        .api_server_cfg = .{
            .deployment_mode = .serverless,
            .request_lifecycle_hook = lifecycle.lifecycle(),
        },
    }, &.{"http://metadata.vopr"});
    defer server.deinit();

    try server.startPublicHttp();
    try std.testing.expect(server.query_io_impl == null);
    try std.testing.expect(server.read_source.io_impl != null);
    try std.testing.expect(server.read_source.io_impl.?.backend.vtable == io.vtable);

    const base_uri = try server.baseUri(alloc);
    defer alloc.free(base_uri);
    const request_uri = try std.fmt.allocPrint(alloc, "{s}/healthz", .{base_uri});
    defer alloc.free(request_uri);

    const Shared = struct {
        alloc: std.mem.Allocator,
        io: std.Io,
        server: *data_runtime.DataServer,
        request_uri: []const u8,
        half_close_request: bool,
        response_status: ?u16 = null,
        request_error: ?anyerror = null,
        request_done: bool = false,
        shutdown_done: bool = false,

        fn request(self: *@This()) void {
            if (self.half_close_request) return self.requestWithHalfClose();
            var client = httpx.Client.initWithConfig(self.alloc, self.io, .{
                .keep_alive = false,
                .retry_policy = .{ .max_retries = 0 },
            });
            defer client.deinit();
            var response = client.get(self.request_uri, .{}) catch |err| {
                self.request_error = err;
                self.request_done = true;
                return;
            };
            defer response.deinit();
            self.response_status = response.status.code;
            self.request_done = true;
        }

        fn requestWithHalfClose(self: *@This()) void {
            const uri = httpx.Uri.parse(self.request_uri) catch |err| {
                self.finishWithError(err);
                return;
            };
            var socket = httpx.Socket.connectHost(uri.host orelse {
                self.finishWithError(error.InvalidUri);
                return;
            }, uri.effectivePort(), self.io) catch |err| {
                self.finishWithError(err);
                return;
            };
            defer socket.close();
            socket.sendAll("GET /healthz HTTP/1.1\r\nHost: vopr\r\nConnection: close\r\n\r\n") catch |err| {
                self.finishWithError(err);
                return;
            };
            socket.shutdownWrite() catch |err| {
                self.finishWithError(err);
                return;
            };
            var response: [4096]u8 = undefined;
            var used: usize = 0;
            while (used < response.len) {
                const count = socket.recv(response[used..]) catch |err| {
                    self.finishWithError(err);
                    return;
                };
                if (count == 0) break;
                used += count;
            }
            if (!std.mem.startsWith(u8, response[0..used], "HTTP/1.1 200")) {
                self.finishWithError(error.UnexpectedHttpStatus);
                return;
            }
            self.response_status = 200;
            self.request_done = true;
        }

        fn finishWithError(self: *@This(), err: anyerror) void {
            self.request_error = err;
            self.request_done = true;
        }

        fn shutdown(self: *@This()) void {
            self.server.quiesceBackgroundWork();
            self.shutdown_done = true;
        }
    };

    var shared = Shared{
        .alloc = alloc,
        .io = io,
        .server = &server,
        .request_uri = request_uri,
        .half_close_request = options.half_close_request,
    };
    if (options.partial_write_limit) |limit| try vopr_io.limitNextNetworkWrite(limit);
    _ = io.async(Shared.request, .{&shared});

    var shutdown_started = false;
    var enabled: vopr.transition.List = .{};
    defer enabled.deinit(alloc);
    var events: vopr.event.Sink = .{};
    defer events.deinit(alloc);
    const scheduler = vopr_io.scheduler();
    var transitions: usize = 0;
    while (!scheduler.quiescent()) {
        if (shared.request_done and !shutdown_started) {
            shutdown_started = true;
            _ = io.async(Shared.shutdown, .{&shared});
        }
        enabled.items.clearRetainingCapacity();
        try scheduler.enumerateReady(&enabled, alloc);
        try enabled.canonicalize();
        try std.testing.expect(enabled.items.items.len != 0);
        var selected = enabled.items.items[0];
        if (options.prioritize_time) {
            for (enabled.items.items) |candidate| {
                if (std.mem.eql(u8, candidate.name, "sim-io.time_advance")) {
                    selected = candidate;
                    break;
                }
            }
        } else {
            for (enabled.items.items) |candidate| {
                if (!std.mem.eql(u8, candidate.name, "sim-io.time_advance")) {
                    selected = candidate;
                    break;
                }
            }
        }
        try scheduler.executeReady(selected.id, &events, alloc);
        transitions += 1;
        if (transitions > 10_000) return error.VoprDataServerTransitionBudgetExceeded;
    }

    if (options.prioritize_time) {
        try std.testing.expect(shared.request_error != null);
        try std.testing.expectEqual(@as(?u16, null), shared.response_status);
    } else {
        try std.testing.expect(shared.request_error == null);
        try std.testing.expectEqual(@as(?u16, 200), shared.response_status);
    }
    try std.testing.expect(shared.shutdown_done);
    try std.testing.expectEqual(@as(usize, 0), metadata.calls);
    const expected_lifecycle_count: u64 = if (options.prioritize_time) 0 else 1;
    try std.testing.expectEqual(expected_lifecycle_count, vopr_io.instrumentation.count(
        request_lifecycle.Hook.stableId(.{ .phase = .ingress }),
    ));
    try std.testing.expectEqual(expected_lifecycle_count, vopr_io.instrumentation.count(
        request_lifecycle.Hook.stableId(.{ .phase = .response_ready }),
    ));
    try vopr_io.ensureNoCapabilityViolation();
}

test "production DataServer public HTTP runs on borrowed VoprIo" {
    try runProductionDataServerScenario(.{});
}

test "production DataServer public HTTP survives partial VoprIo writes" {
    try runProductionDataServerScenario(.{ .partial_write_limit = 1 });
}

test "production DataServer public HTTP deadline cancels and shuts down on VoprIo" {
    try runProductionDataServerScenario(.{ .prioritize_time = true });
}

test "production DataServer public HTTP preserves request bytes before half close on VoprIo" {
    try runProductionDataServerScenario(.{ .half_close_request = true });
}

test "production DataServer public HTTP recovers minimum socket capacity for shutdown" {
    // One listener plus one client/server connection pair exactly fills this
    // quota. Closing the request pair must return both reservations so the
    // listener's shutdown wake connection can be created.
    try runProductionDataServerScenario(.{ .max_sockets = 3 });
}
