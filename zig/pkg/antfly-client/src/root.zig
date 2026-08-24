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
