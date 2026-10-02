// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: LicenseRef-Elastic-2.0

//! A wake barrier must execute the selected query, not just fetch the catalog.
const std = @import("std");
const client_mod = @import("antfly-client");

fn retryableStatus(status: i32) bool {
    return switch (status) {
        408, 425, 429, 502, 503, 504 => true,
        else => false,
    };
}

fn retryableTransport(err: anyerror) bool {
    return switch (err) {
        error.Timeout, error.ConnectionRefused, error.ConnectionReset, error.ConnectionResetByPeer, error.ConnectionClosed, error.ConnectionAborted, error.ConnectionTimedOut, error.ConnectionTimeout, error.BrokenPipe => true,
        else => false,
    };
}

const Disposition = enum { ready, retry, failed };

fn disposition(status: u16, data: ?client_mod.types.QueryResponses) !Disposition {
    if (status < 200 or status >= 300) return if (retryableStatus(status)) .retry else .failed;
    const payload = data orelse return error.InvalidApiResponse;
    const results = payload.responses orelse return error.InvalidApiResponse;
    if (results.len != 1) return error.InvalidApiResponse;
    const result = results[0];
    if (result.status >= 200 and result.status < 300 and result.@"error" == null) return .ready;
    return if (retryableStatus(result.status)) .retry else .failed;
}

pub fn wait(io: std.Io, client: *client_mod.AntflyClient, table: []const u8, body: client_mod.types.QueryRequest, timeout_ms: u64) !client_mod.openapi.ApiResponse(client_mod.types.QueryResponses) {
    const deadline = std.Io.Clock.awake.now(io).nanoseconds +| @as(i128, timeout_ms) * std.time.ns_per_ms;
    var backoff_ns: i128 = 100 * std.time.ns_per_ms;
    while (true) {
        const remaining = deadline - std.Io.Clock.awake.now(io).nanoseconds;
        if (remaining <= 0) return error.ServingReadinessTimeout;
        const remaining_ms: u64 = @intCast(@divFloor(remaining + std.time.ns_per_ms - 1, std.time.ns_per_ms));
        if (client.queryTableResponseWithTimeout(table, body, remaining_ms)) |value| {
            var response = value;
            errdefer response.deinit();
            const action = try disposition(response.status_code, if (response.data) |data| data.value else null);
            switch (action) {
                .ready => {
                    // A response received after the deadline is not readiness
                    // within the caller's declared wake budget.
                    if (std.Io.Clock.awake.now(io).nanoseconds >= deadline) return error.ServingReadinessTimeout;
                    return response;
                },
                .failed => {
                    std.debug.print("Serving query failed (HTTP {d}): {s}\n", .{ response.status_code, response.err_body orelse "query operation failed" });
                    return error.ApiError;
                },
                .retry => response.deinit(),
            }
        } else |err| {
            if (!retryableTransport(err)) return err;
        }
        const delay = @min(backoff_ns, deadline - std.Io.Clock.awake.now(io).nanoseconds);
        if (delay <= 0) return error.ServingReadinessTimeout;
        try io.sleep(.fromNanoseconds(@intCast(delay)), .awake);
        backoff_ns = @min(backoff_ns * 2, std.time.ns_per_s);
    }
}

test "query wake barrier requires successful data results and rejects permanent errors" {
    try std.testing.expectEqual(Disposition.retry, try disposition(503, null));
    inline for (.{ 400, 401, 403, 404, 409, 500 }) |status|
        try std.testing.expectEqual(Disposition.failed, try disposition(status, null));
    try std.testing.expectError(error.InvalidApiResponse, disposition(200, .{ .responses = &.{} }));
    try std.testing.expectEqual(Disposition.ready, try disposition(200, .{ .responses = &.{.{ .status = 200, .took = 0 }} }));
    try std.testing.expectEqual(Disposition.retry, try disposition(200, .{ .responses = &.{.{ .status = 503, .took = 0, .@"error" = "warming" }} }));
    try std.testing.expectEqual(Disposition.failed, try disposition(200, .{ .responses = &.{.{ .status = 200, .took = 0, .@"error" = "partial failure" }} }));
    try std.testing.expect(!retryableTransport(error.InvalidApiResponse));
    try std.testing.expect(!retryableTransport(error.OutOfMemory));
}

test "query wake barrier retries HTTP and operation admission before returning data" {
    const httpx = client_mod.httpx;
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Check = struct {
        fn request(info: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer wake-test", info.header("Authorization").?);
            try std.testing.expect(std.mem.indexOf(u8, info.body, "alpha") != null);
        }
    };
    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/db/v1/tables/docs/query", .max_uses = 1, .respond = .{ .status = 503, .body = "warming" }, .assert_request = Check.request },
        .{ .method = .POST, .path = "/db/v1/tables/docs/query", .max_uses = 1, .respond = .{ .body = "{\"responses\":[{\"status\":503,\"took\":0,\"error\":\"busy\"}]}" }, .assert_request = Check.request },
        .{ .method = .POST, .path = "/db/v1/tables/docs/query", .respond = .{ .body = "{\"responses\":[{\"status\":200,\"took\":0}]}" }, .assert_request = Check.request },
    });
    defer server.deinit();
    var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
    defer http.deinit();
    var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
    defer client.deinit();
    try client.setBearer("wake-test");
    const Task = struct {
        fn serve(test_server: *httpx.TestServer) std.Io.Cancelable!void {
            for (0..3) |_| test_server.handleOne() catch return;
        }
    };
    var group: std.Io.Group = .init;
    try group.concurrent(io, Task.serve, .{&server});
    defer group.cancel(io);
    var response = try wait(io, &client, "docs", .{ .semantic_search = "alpha" }, 3000);
    defer response.deinit();
    try std.testing.expectEqual(@as(u16, 200), response.status_code);
    try std.testing.expectEqualSlices(usize, &.{ 1, 1, 1 }, server.route_hits);
}

test "query wake barrier bounds a stalled first data request" {
    const httpx = client_mod.httpx;
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    var server = try httpx.TestServer.start(alloc, io, &.{
        .{ .method = .POST, .path = "/db/v1/tables/docs/query", .respond = .{ .delay_ns = 2000 * std.time.ns_per_ms, .body = "{\"responses\":[{\"status\":200,\"took\":0}]}" } },
    });
    defer server.deinit();
    var http = httpx.Client.initWithConfig(alloc, io, .{ .keep_alive = false, .retry_policy = .{ .max_retries = 0 } });
    defer http.deinit();
    var client = try client_mod.AntflyClient.init(alloc, &http, server.baseUrl());
    defer client.deinit();
    const Task = struct {
        fn serve(test_server: *httpx.TestServer) std.Io.Cancelable!void {
            test_server.handleOne() catch return;
        }
    };
    var group: std.Io.Group = .init;
    try group.concurrent(io, Task.serve, .{&server});
    defer group.cancel(io);
    const start = std.Io.Clock.awake.now(io).nanoseconds;
    try std.testing.expectError(error.ServingReadinessTimeout, wait(io, &client, "docs", .{}, 50));
    try std.testing.expect(std.Io.Clock.awake.now(io).nanoseconds - start < 1000 * std.time.ns_per_ms);
}
