// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the Elastic License 2.0 is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
// the Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Client for the HA HTTP admin adapter.

const std = @import("std");
const Allocator = std.mem.Allocator;
const http_common = @import("../../common/http/http_common.zig");
const routes = @import("../../raft/transport/routes.zig");
const fencing = @import("fencing.zig");
const http_admin = @import("http_admin.zig");
const primary_mod = @import("primary.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const RenderedOutput = struct {
    content_type: []u8,
    body: []u8,

    pub fn deinit(self: *RenderedOutput, alloc: Allocator) void {
        alloc.free(self.content_type);
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub const Client = struct {
    alloc: Allocator,
    executor: http_common.RequestExecutor,

    pub fn init(alloc: Allocator, executor: http_common.RequestExecutor) Client {
        return .{
            .alloc = alloc,
            .executor = executor,
        };
    }

    pub fn checkHealth(self: *Client, base_uri: []const u8) !void {
        const uri = try join(self.alloc, base_uri, http_admin.Routes.health);
        defer self.alloc.free(uri);

        var resp = try self.executeWithRetry(.{ .method = .GET, .uri = uri });
        defer resp.deinit(self.alloc);
        try mapStatus(resp.status);
    }

    pub fn executeCommand(self: *Client, base_uri: []const u8, argv: []const []const u8) !RenderedOutput {
        const uri = try join(self.alloc, base_uri, http_admin.Routes.command);
        defer self.alloc.free(uri);

        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            struct { argv: []const []const u8 }{ .argv = argv },
            .{},
        );
        defer self.alloc.free(body);

        var resp = try self.executeWithRetry(.{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
        errdefer resp.deinit(self.alloc);
        try mapStatus(resp.status);

        for (resp.headers) |*header| header.deinit(self.alloc);
        if (resp.headers.len > 0) self.alloc.free(resp.headers);

        return .{
            .content_type = resp.content_type orelse try self.alloc.dupe(u8, "application/octet-stream"),
            .body = resp.body,
        };
    }

    fn executeWithRetry(self: *Client, req: http_common.HttpRequest) !http_common.HttpResponse {
        var attempt: usize = 0;
        while (true) {
            return self.executor.execute(self.alloc, req) catch |err| switch (err) {
                error.HttpConnectionClosing,
                error.ConnectionResetByPeer,
                error.ConnectionRefused,
                error.BrokenPipe,
                error.EndOfStream,
                => {
                    if (attempt >= 1) return err;
                    attempt += 1;
                    continue;
                },
                else => return err,
            };
        }
    }

    fn mapStatus(status: u16) !void {
        if (status >= 200 and status < 300) return;
        if (status == 400) return error.InvalidHaCommand;
        if (status == 404) return error.HaEndpointNotFound;
        if (status == 405) return error.UnsupportedOperation;
        if (status == 409) return error.HaCommandConflict;
        return error.UnexpectedHttpStatus;
    }
};

fn join(alloc: Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    return try routes.Routes.join(alloc, base_uri, path);
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
    }
};

fn testPaths(alloc: Allocator, comptime name: []const u8) !TestPaths {
    const nonce = @atomicRmw(u64, &test_path_counter, .Add, 1, .seq_cst);
    const primary_log = try allocPrintPath(alloc, name, "primary-log", nonce);
    defer alloc.free(primary_log);
    const primary_slots = try allocPrintPath(alloc, name, "primary-slots", nonce);
    defer alloc.free(primary_slots);
    const standby_log = try allocPrintPath(alloc, name, "standby-log", nonce);
    defer alloc.free(standby_log);
    const standby_progress = try allocPrintPath(alloc, name, "standby-progress", nonce);
    defer alloc.free(standby_progress);
    const fence_wal = try allocPrintPath(alloc, name, "fence-wal", nonce);
    defer alloc.free(fence_wal);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
        .fence_wal = try alloc.dupeZ(u8, fence_wal),
    };
}

fn allocPrintPath(alloc: Allocator, comptime name: []const u8, comptime part: []const u8, nonce: u64) ![]u8 {
    return try std.fmt.allocPrint(
        alloc,
        ".zig-cache/tmp/ha-http-client-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
        .{ std.testing.random_seed, nonce },
    );
}

fn testIdentity() standby_mod.Identity {
    return .{
        .cluster_id = 100,
        .shard_id = 10,
        .table_id = 20,
        .timeline_id = 1,
        .epoch = 1,
    };
}

test "storage.ha http client round trips admin commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "round-trip");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = http_admin.Server.init(alloc, .{ .primary = &primary, .standby = &standby, .fence_store = &fence_store });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    try client.checkHealth("http://ha-admin.test");

    var created = try client.executeCommand("http://ha-admin.test/", &.{ "slot", "create", "standby-a", "--initial-lsn", "0" });
    defer created.deinit(alloc);
    try std.testing.expectEqualStrings("application/json", created.content_type);
    try expectContains(created.body, "\"slot_name\":\"standby-a\"");

    var appended = try client.executeCommand("http://ha-admin.test", &.{
        "--table",
        "commit",
        "append",
        "--payload",
        "one",
        "--sync-mode",
        "async",
    });
    defer appended.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", appended.content_type);
    try expectContains(appended.body, "result=commit_append\n");
    try expectContains(appended.body, "lsn=1\n");
    try expectContains(appended.body, "action=acknowledge\n");

    var streamed = try client.executeCommand("http://ha-admin.test", &.{ "--table", "stream", "once", "--slot", "standby-a" });
    defer streamed.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", streamed.content_type);
    try expectContains(streamed.body, "result=stream_once\n");
    try expectContains(streamed.body, "applied_lsn=1\n");

    var fenced = try client.executeCommand("http://ha-admin.test", &.{
        "--table",
        "fence",
        "acquire",
        "--cluster-id",
        "100",
        "--shard-id",
        "10",
        "--table-id",
        "20",
        "--timeline-id",
        "1",
        "--epoch",
        "1",
        "--old-primary-id",
        "primary-a",
        "--promoted-node-id",
        "standby-a",
        "--new-timeline-id",
        "2",
        "--new-epoch",
        "2",
        "--required-lsn",
        "1",
        "--observed-lsn",
        "1",
        "--reason",
        "http-client-test",
    });
    defer fenced.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", fenced.content_type);
    try expectContains(fenced.body, "result=fence_acquire\n");
    try expectContains(fenced.body, "promoted_node_id=standby-a\n");

    var current_fence = try client.executeCommand("http://ha-admin.test", &.{ "--table", "fence", "current" });
    defer current_fence.deinit(alloc);
    try expectContains(current_fence.body, "result=fence_current\n");
    try expectContains(current_fence.body, "held=true\n");

    var promoted = try client.executeCommand("http://ha-admin.test", &.{ "--table", "promote", "--current-fence" });
    defer promoted.deinit(alloc);
    try expectContains(promoted.body, "result=promote_current_fence\n");
    try expectContains(promoted.body, "promotion.new_identity.timeline_id=2\n");
}

test "storage.ha http client maps admin errors" {
    const alloc = std.testing.allocator;
    var server = http_admin.Server.init(alloc, .{});
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    try std.testing.expectError(error.HaCommandConflict, client.executeCommand("http://ha-admin.test", &.{"identify"}));
    try std.testing.expectError(error.InvalidHaCommand, client.executeCommand("http://ha-admin.test", &.{"unknown"}));
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
