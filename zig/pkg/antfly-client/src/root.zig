// Copyright 2026 Antfly, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");
pub const openapi = @import("antfly_client_openapi");
pub const httpx = @import("httpx");

pub const AntflyClient = @import("client.zig").AntflyClient;
pub const ApiError = @import("client.zig").ApiError;

/// Re-export generated types for convenience.
pub const types = openapi.types;

test "antfly client pkg compiles" {
    _ = AntflyClient;
    _ = ApiError;
    _ = types;
    _ = @import("client.zig");
}

test "SQL client preserves typed parameters receipts and forbids replay" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Assert = struct {
        fn request(info: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Bearer secret", info.header("Authorization").?);
            var body = try std.json.parseFromSlice(types.SQLRequest, std.testing.allocator, info.body, .{ .parse_numbers = false });
            defer body.deinit();
            try std.testing.expectEqualStrings("SELECT id FROM things WHERE id=$1", body.value.statement);
            try std.testing.expectEqualStrings("9007199254740993", body.value.parameters.?[0].number_string);
            try std.testing.expectEqualStrings("explicit", body.value.database.?);
            try std.testing.expectEqualStrings("app", body.value.namespace.?);
        }
    };
    const cases = [_]struct { status: u16, body: []const u8, malformed: bool = false }{
        .{ .status = 200, .body = "{\"columns\":[{\"name\":\"id\",\"type\":\"integer\"}],\"rows\":[[\"9007199254740993\"]],\"rows_affected\":0,\"command_tag\":\"SELECT\"}" },
        .{ .status = 503, .body = "{\"code\":\"40003\",\"message\":\"unknown commit\",\"transaction_id\":\"0123456789abcdef0123456789abcdef\",\"retryable\":false}" },
        .{ .status = 200, .body = "{\"columns\":[],\"rows\":[[1]],\"rows_affected\":0,\"command_tag\":\"SELECT\"}", .malformed = true },
    };
    for (cases) |case| for ([_]bool{ false, true }) |generated| {
        // The generated type decoder intentionally leaves ordinal semantic
        // validation to the convenience method, but replay policy is shared.
        if (generated and case.malformed) continue;
        var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = .POST, .path = "/db/v1/sql", .respond = .{ .status = case.status, .body = case.body }, .assert_request = Assert.request }});
        defer server.deinit();
        var serving = try io.concurrent(httpx.TestServer.handleOne, .{&server});
        defer serving.cancel(io) catch {};
        var http = httpx.Client.initWithConfig(alloc, io, .{
            .retry_policy = .{ .retry_only_idempotent = false, .max_retries = 3, .initial_delay_ms = 0 },
            .timeouts = .{ .request_ms = 1000 },
        });
        defer http.deinit();
        var client = try AntflyClient.init(alloc, &http, server.baseUrl());
        defer client.deinit();
        try client.setBearer("secret");
        client.catalog_scope = .{ .database = "fallback", .namespace = "app" };
        const request: types.SQLRequest = .{ .statement = "SELECT id FROM things WHERE id=$1", .parameters = &.{.{ .number_string = "9007199254740993" }}, .database = "explicit" };
        if (case.malformed) {
            try std.testing.expectError(error.InvalidApiResponse, client.executeSQL(request));
        } else {
            var raw_request = request;
            raw_request.namespace = "app";
            var response = try if (generated) client.inner.executeSQL(raw_request) else client.executeSQL(request);
            defer response.deinit();
            try std.testing.expectEqual(case.status, response.status_code);
            if (case.status == 200) {
                try std.testing.expectEqualStrings("9007199254740993", response.data.?.value.rows[0][0].string);
            } else {
                try std.testing.expectEqualStrings(case.body, response.err_body.?);
            }
        }
        try serving.await(io);
        try std.testing.expectEqual(@as(usize, 1), server.route_hits[0]);
    };
}

test "get index response timeout bounds the complete HTTP request" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const Assert = struct {
        var test_io: std.Io = undefined;

        fn request(info: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqual(httpx.Method.GET, info.method);
            try std.testing.expectEqualStrings("/db/v1/tables/docs/indexes/dense", info.path);
            try std.testing.expectEqualStrings("Bearer test-token", info.header("Authorization").?);
            // Keep the server response beyond the per-request budget. The
            // client watchdog runs on the concurrent I/O worker.
            try test_io.sleep(std.Io.Duration.fromMilliseconds(200), .awake);
        }
    };
    Assert.test_io = io;

    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .GET,
        .path = "/db/v1/tables/docs/indexes/dense",
        .respond = .{ .body = "{}" },
        .assert_request = Assert.request,
    }});
    defer server.deinit();

    var http = httpx.Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .retry_policy = .{ .max_retries = 0 },
    });
    defer http.deinit();
    var client = try AntflyClient.init(allocator, &http, server.baseUrl());
    defer client.deinit();
    try client.setBearer("test-token");

    var timed_out = std.atomic.Value(bool).init(false);
    var unexpected = std.atomic.Value(bool).init(false);
    var succeeded = std.atomic.Value(bool).init(false);
    var group = std.Io.Group.init;
    const ClientTask = struct {
        fn run(
            _: std.Io,
            c: *AntflyClient,
            timeout: *std.atomic.Value(bool),
            other_error: *std.atomic.Value(bool),
            success: *std.atomic.Value(bool),
        ) std.Io.Cancelable!void {
            var resp = c.getIndexResponseWithTimeout("docs", "dense", 50) catch |err| {
                if (err == error.Timeout) {
                    timeout.store(true, .release);
                } else {
                    other_error.store(true, .release);
                }
                return;
            };
            defer resp.deinit();
            success.store(true, .release);
        }
    };
    try group.concurrent(io, ClientTask.run, .{ io, &client, &timed_out, &unexpected, &succeeded });
    server.handleOne() catch |err| {
        // A timed-out client may close before the delayed response is written.
        // Preserve request assertion failures, which happen before the delay.
        if (!timed_out.load(.acquire)) return err;
    };
    try group.await(io);
    try std.testing.expect(timed_out.load(.acquire));
    try std.testing.expect(!unexpected.load(.acquire));
    try std.testing.expect(!succeeded.load(.acquire));
}

test "SQL prepared client forbids replay and preserves exact owner identity" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const id = "0123456789abcdef0123456789abcdef";
    inline for (.{ "prepare", "execute", "close" }) |operation| {
        const path = if (comptime std.mem.eql(u8, operation, "prepare")) "/db/v1/sql/prepared" else if (comptime std.mem.eql(u8, operation, "execute")) "/db/v1/sql/prepared/" ++ id ++ "/execute" else "/db/v1/sql/prepared/" ++ id;
        const method: httpx.Method = if (comptime std.mem.eql(u8, operation, "close")) .DELETE else .POST;
        for ([_]u16{ 200, 503 }) |status| {
            const success = if (comptime std.mem.eql(u8, operation, "prepare")) "{\"prepared_id\":\"" ++ id ++ "\",\"owner_node_id\":\"9007199254740993\",\"expires_at_ms\":123,\"columns\":[],\"parameter_types\":[]}" else if (comptime std.mem.eql(u8, operation, "execute")) "{\"columns\":[],\"rows\":[],\"rows_affected\":0,\"command_tag\":\"SELECT 0\"}" else "{}";
            const body = if (status == 200) success else "{\"code\":\"40003\",\"message\":\"do not replay\",\"retryable\":false}";
            var server = try httpx.TestServer.start(alloc, io, &.{.{ .method = method, .path = path, .respond = .{ .status = status, .body = body } }});
            defer server.deinit();
            var serving = try io.concurrent(httpx.TestServer.handleOne, .{&server});
            defer serving.cancel(io) catch {};
            var http = httpx.Client.initWithConfig(alloc, io, .{ .retry_policy = .{ .retry_only_idempotent = false, .max_retries = 3, .initial_delay_ms = 0 }, .timeouts = .{ .request_ms = 1000 } });
            defer http.deinit();
            var client = try AntflyClient.init(alloc, &http, server.baseUrl());
            defer client.deinit();
            var response = try if (comptime std.mem.eql(u8, operation, "prepare")) client.prepareSQL(.{ .statement = "SELECT 1" }) else if (comptime std.mem.eql(u8, operation, "execute")) client.executePreparedSQL(id, .{}) else client.closePreparedSQL(id);
            defer response.deinit();
            try std.testing.expectEqual(status, response.status_code);
            if (comptime std.mem.eql(u8, operation, "prepare")) {
                if (status == 200) try std.testing.expectEqualStrings("9007199254740993", response.data.?.value.owner_node_id);
            }
            try serving.await(io);
            try std.testing.expectEqual(@as(usize, 1), server.route_hits[0]);
        }
    }
}

test "list indexes response timeout bounds readiness preflight" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const Assert = struct {
        var test_io: std.Io = undefined;

        fn request(info: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqual(httpx.Method.GET, info.method);
            try std.testing.expectEqualStrings("/db/v1/tables/docs/indexes", info.path);
            try std.testing.expectEqualStrings("Bearer test-token", info.header("Authorization").?);
            try test_io.sleep(std.Io.Duration.fromMilliseconds(200), .awake);
        }
    };
    Assert.test_io = io;

    var server = try httpx.TestServer.start(allocator, io, &.{.{
        .method = .GET,
        .path = "/db/v1/tables/docs/indexes",
        .respond = .{ .body = "[]" },
        .assert_request = Assert.request,
    }});
    defer server.deinit();

    var http = httpx.Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .retry_policy = .{ .max_retries = 0 },
    });
    defer http.deinit();
    var client = try AntflyClient.init(allocator, &http, server.baseUrl());
    defer client.deinit();
    try client.setBearer("test-token");

    var timed_out = std.atomic.Value(bool).init(false);
    var unexpected = std.atomic.Value(bool).init(false);
    var succeeded = std.atomic.Value(bool).init(false);
    var group = std.Io.Group.init;
    const ClientTask = struct {
        fn run(
            _: std.Io,
            c: *AntflyClient,
            timeout: *std.atomic.Value(bool),
            other_error: *std.atomic.Value(bool),
            success: *std.atomic.Value(bool),
        ) std.Io.Cancelable!void {
            var resp = c.listIndexesResponseWithTimeout("docs", 50) catch |err| {
                if (err == error.Timeout) {
                    timeout.store(true, .release);
                } else {
                    other_error.store(true, .release);
                }
                return;
            };
            defer resp.deinit();
            success.store(true, .release);
        }
    };
    try group.concurrent(io, ClientTask.run, .{ io, &client, &timed_out, &unexpected, &succeeded });
    server.handleOne() catch |err| {
        if (!timed_out.load(.acquire)) return err;
    };
    try group.await(io);
    try std.testing.expect(timed_out.load(.acquire));
    try std.testing.expect(!unexpected.load(.acquire));
    try std.testing.expect(!succeeded.load(.acquire));
}

test "personal connections polling accommodates slow status and explicit deadlines" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Assert = struct {
        var test_io: std.Io = undefined;
        var delay_ms: i64 = 0;
        fn request(_: httpx.testing_mod.RequestInfo) !void {
            try test_io.sleep(.fromMilliseconds(delay_ms), .awake);
        }
    };
    Assert.test_io = io;
    for ([_]i64{ 16_000, 200 }) |delay| {
        Assert.delay_ms = delay;
        var server = try httpx.TestServer.start(a, io, &.{.{
            .method = .GET,
            .path = "/db/v1/connections/chatgpt/attempts/attempt-1",
            .respond = .{ .body = "{\"status\":\"connected\",\"connection_id\":\"one\"}" },
            .assert_request = Assert.request,
        }});
        defer server.deinit();
        var serving = try io.concurrent(httpx.TestServer.handleOne, .{&server});
        defer serving.cancel(io) catch {};
        var http = httpx.Client.initWithConfig(a, io, .{ .keep_alive = false });
        defer http.deinit();
        var client = try AntflyClient.init(a, &http, server.baseUrl());
        defer client.deinit();
        if (delay == 16_000) {
            var response = try client.getChatGPTAttempt("attempt-1");
            defer response.deinit();
            try std.testing.expectEqualStrings("connected", response.data.?.value.status);
            try serving.await(io);
        } else {
            try std.testing.expectError(error.Timeout, client.getChatGPTAttemptWithTimeout("attempt-1", 50));
        }
        try std.testing.expectEqual(@as(usize, 1), server.route_hits[0]);
    }
}

test "personal connections preserve owner auth and forbid replay" {
    const alloc = std.testing.allocator;
    const io = std.testing.io;
    const Assert = struct {
        fn request(info: httpx.testing_mod.RequestInfo) !void {
            try std.testing.expectEqualStrings("Basic dXNlcjpwYXNz", info.header("Authorization").?);
            try std.testing.expectEqualStrings("{\"connection_id\":\"account-1\"}", info.body);
        }
    };
    for ([_]u16{ 200, 503 }) |status| {
        var server = try httpx.TestServer.start(alloc, io, &.{.{
            .method = .POST,
            .path = "/db/v1/connections/chatgpt/authorize",
            .respond = .{ .status = status, .body = "{\"attempt_id\":\"attempt-1\",\"authorization_url\":\"https://auth.openai.com/api/accounts/authorize?state=test\",\"expires_at\":123}" },
            .assert_request = Assert.request,
        }});
        defer server.deinit();
        var serving = try io.concurrent(httpx.TestServer.handleOne, .{&server});
        defer serving.cancel(io) catch {};
        var http = httpx.Client.initWithConfig(alloc, io, .{ .retry_policy = .{ .retry_only_idempotent = false, .max_retries = 3, .initial_delay_ms = 0 }, .timeouts = .{ .request_ms = 1000 } });
        defer http.deinit();
        var client = try AntflyClient.init(alloc, &http, server.baseUrl());
        defer client.deinit();
        try client.setBearer("previous-owner");
        try client.setBasicAuth("user", "pass");
        var response = try client.authorizeChatGPT(.{ .connection_id = "account-1" });
        defer response.deinit();
        try std.testing.expectEqual(status, response.status_code);
        if (status == 200) try std.testing.expectEqualStrings("attempt-1", response.data.?.value.attempt_id);
        try serving.await(io);
        try std.testing.expectEqual(@as(usize, 1), server.route_hits[0]);
        try std.testing.expectError(error.InvalidConnectionId, client.getChatGPTAttempt("../other"));
        try std.testing.expectError(error.InvalidConnectionId, client.disconnectChatGPT("account?redirect=other"));
    }
}
