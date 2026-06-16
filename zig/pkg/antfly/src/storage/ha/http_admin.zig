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

//! HTTP adapter for HA admin commands.
//!
//! The storage HA command vocabulary lives in `admin_cli.zig`, and execution
//! lives in `admin_exec.zig`. This adapter gives operators and future node
//! servers a small HTTP surface without creating a second command contract.

const std = @import("std");
const Allocator = std.mem.Allocator;
const http_common = @import("../../common/http/http_common.zig");
const admin_cli = @import("admin_cli.zig");
const admin_exec = @import("admin_exec.zig");
const fencing = @import("fencing.zig");
const primary_mod = @import("primary.zig");
const standby_mod = @import("standby.zig");

var test_path_counter: u64 = 0;

pub const Routes = struct {
    pub const health = "/ha/v1/health";
    pub const ready = "/ha/v1/ready";
    pub const command = "/ha/v1/admin/command";
};

pub const CommandRequest = struct {
    argv: []const []const u8,
};

pub const Server = struct {
    alloc: Allocator,
    ctx: admin_exec.Context,

    pub fn init(alloc: Allocator, ctx: admin_exec.Context) Server {
        return .{
            .alloc = alloc,
            .ctx = ctx,
        };
    }

    pub fn executor(self: *Server) http_common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    pub fn deinit(self: *Server) void {
        self.* = undefined;
    }

    pub fn handle(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        const path = requestPath(req.uri);
        switch (req.method) {
            .GET => {
                if (std.mem.eql(u8, path, Routes.health)) {
                    return try textResponse(self.alloc, 200, "ok");
                }
                if (std.mem.eql(u8, path, Routes.ready)) {
                    if (self.ready()) return try textResponse(self.alloc, 200, "ready");
                    return try textResponse(self.alloc, 503, "not ready");
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            .POST => {
                if (std.mem.eql(u8, path, Routes.command)) {
                    return try self.handleCommand(req);
                }
                return try textResponse(self.alloc, 404, "not found");
            },
            else => return try textResponse(self.alloc, 405, "method not allowed"),
        }
    }

    fn handleCommand(self: *Server, req: http_common.HttpRequest) !http_common.HttpResponse {
        if (req.body.len == 0) return try textResponse(self.alloc, 400, "empty HA command request");

        var parsed = std.json.parseFromSlice(
            CommandRequest,
            self.alloc,
            req.body,
            .{ .ignore_unknown_fields = true },
        ) catch return try textResponse(self.alloc, 400, "invalid HA command request");
        defer parsed.deinit();

        var plan = admin_cli.parse(self.alloc, parsed.value.argv) catch return try textResponse(self.alloc, 400, "invalid HA command argv");
        defer plan.deinit(self.alloc);

        var rendered = admin_exec.executeAndRenderAlloc(self.alloc, self.ctx, plan) catch |err| {
            return try textResponse(self.alloc, commandErrorStatus(err), @errorName(err));
        };
        errdefer rendered.deinit(self.alloc);

        return .{
            .status = 200,
            .content_type = try self.alloc.dupe(u8, rendered.content_type),
            .body = rendered.body,
        };
    }

    fn execute(ptr: *anyopaque, _: Allocator, req: http_common.HttpRequest) !http_common.HttpResponse {
        const self: *Server = @ptrCast(@alignCast(ptr));
        return try self.handle(req);
    }

    fn ready(self: *const Server) bool {
        return self.ctx.primary != null or self.ctx.standby != null or self.ctx.fence_store != null;
    }
};

fn requestPath(uri: []const u8) []const u8 {
    const scheme_index = std.mem.indexOf(u8, uri, "://") orelse return uri;
    const authority_start = scheme_index + 3;
    const path_index = std.mem.indexOfScalarPos(u8, uri, authority_start, '/') orelse return "/";
    return uri[path_index..];
}

fn commandErrorStatus(err: anyerror) u16 {
    return switch (err) {
        error.PrimaryUnavailable,
        error.StandbyUnavailable,
        error.FenceStoreUnavailable,
        error.FenceAlreadyHeld,
        error.FenceReceiptMissing,
        error.BaseBackupSlotInUse,
        error.SlotAlreadyExists,
        error.SlotInactive,
        error.SlotRequiresReseed,
        error.WalNoLongerRetained,
        error.MissingReceivedRecord,
        error.RecordAlreadyReceived,
        error.StandbyAlreadyBootstrapped,
        error.SyncPolicyUnsatisfied,
        => 409,
        error.SlotNotFound,
        error.BackupStartNotFound,
        => 404,
        error.PrometheusUnsupportedForResult,
        error.InvalidSlotName,
        error.InvalidSlotProgress,
        error.InvalidReplicationError,
        error.InvalidReplicationStartLsn,
        error.InvalidCheckpointLsn,
        error.InvalidBackupLsn,
        error.InvalidManifestId,
        error.ManifestIdTooLong,
        error.EmptyManifest,
        error.TooManyManifestFiles,
        error.InvalidManifestPath,
        error.ManifestPathTooLong,
        error.DuplicateManifestPath,
        error.ManifestFileSetMismatch,
        error.BackupStartNotDurable,
        error.InitialLsnAheadOfPrimary,
        error.ManifestPathMissing,
        error.ManifestFileTooLarge,
        error.ManifestFileMissing,
        error.ManifestFileCrcMismatch,
        error.ManifestFileSizeMismatch,
        error.InvalidOldPrimaryId,
        error.InvalidPromotedNodeId,
        error.InvalidTimelineSwitch,
        error.InvalidFenceLsn,
        error.FenceRequiresForce,
        error.FenceFieldTooLong,
        error.FencingRequired,
        error.PromotionRequiresForce,
        error.PromotionNotAllowed,
        error.StandbyAheadOfPrimary,
        error.TargetAheadOfPrimary,
        error.InvalidSyncPolicy,
        error.WrongCluster,
        error.WrongShard,
        error.WrongTable,
        error.WrongTimeline,
        error.WrongEpoch,
        => 400,
        else => 500,
    };
}

fn textResponse(alloc: Allocator, status: u16, body: []const u8) !http_common.HttpResponse {
    return .{
        .status = status,
        .content_type = try alloc.dupe(u8, "text/plain"),
        .body = try alloc.dupe(u8, body),
    };
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
        ".zig-cache/tmp/ha-http-admin-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

test "storage.ha http admin serves health and command endpoint" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "command");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    var server = Server.init(alloc, .{ .primary = &primary, .standby = &standby, .fence_store = &fence_store });
    defer server.deinit();

    var health = try server.handle(.{ .method = .GET, .uri = Routes.health });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);
    try std.testing.expectEqualStrings("ok", health.body);

    var ready = try server.handle(.{ .method = .GET, .uri = Routes.ready });
    defer ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), ready.status);
    try std.testing.expectEqualStrings("ready", ready.body);

    var create = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"slot\",\"create\",\"standby-a\",\"--initial-lsn\",\"0\"]}",
    });
    defer create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), create.status);
    try std.testing.expectEqualStrings("application/json", create.content_type.?);
    try expectContains(create.body, "\"slot_name\":\"standby-a\"");

    var duplicate_create = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"slot\",\"create\",\"standby-a\",\"--initial-lsn\",\"0\"]}",
    });
    defer duplicate_create.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), duplicate_create.status);
    try expectContains(duplicate_create.body, "SlotAlreadyExists");

    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));
    var stream = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"stream\",\"once\",\"--slot\",\"standby-a\"]}",
    });
    defer stream.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), stream.status);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", stream.content_type.?);
    try expectContains(stream.body, "result=stream_once\n");
    try expectContains(stream.body, "received_count=1\n");
    try expectContains(stream.body, "applied_lsn=1\n");

    var invalid_progress = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"standby\",\"ack\",\"--slot\",\"standby-a\",\"--timeline-id\",\"1\",\"--received-lsn\",\"1\",\"--applied-lsn\",\"2\"]}",
    });
    defer invalid_progress.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_progress.status);
    try expectContains(invalid_progress.body, "InvalidSlotProgress");

    var invalid_seed = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"seed\",\"begin\",\"--slot\",\"standby-a\",\"--manifest-id\",\"\"]}",
    });
    defer invalid_seed.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), invalid_seed.status);
    try expectContains(invalid_seed.body, "InvalidManifestId");

    try primary.pauseSlot("standby-a");
    var inactive_stream = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"stream\",\"--slot\",\"standby-a\",\"--from-lsn\",\"1\"]}",
    });
    defer inactive_stream.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), inactive_stream.status);
    try expectContains(inactive_stream.body, "SlotInactive");
    try primary.resumeSlot("standby-a");

    var fail_closed_append = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"commit\",\"append\",\"--payload\",\"two\",\"--sync-mode\",\"remote-write\",\"--sync-standby\",\"standby-a\",\"--sync-failure\",\"fail-closed\"]}",
    });
    defer fail_closed_append.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), fail_closed_append.status);
    try expectContains(fail_closed_append.body, "SyncPolicyUnsatisfied");
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var fence = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"fence\",\"acquire\",\"--cluster-id\",\"100\",\"--shard-id\",\"10\",\"--table-id\",\"20\",\"--timeline-id\",\"1\",\"--epoch\",\"1\",\"--old-primary-id\",\"primary-a\",\"--promoted-node-id\",\"standby-a\",\"--new-timeline-id\",\"2\",\"--new-epoch\",\"2\",\"--required-lsn\",\"1\",\"--observed-lsn\",\"1\",\"--reason\",\"http-admin-test\"]}",
    });
    defer fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), fence.status);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", fence.content_type.?);
    try expectContains(fence.body, "result=fence_acquire\n");
    try expectContains(fence.body, "promoted_node_id=standby-a\n");

    var current_fence = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"fence\",\"current\"]}",
    });
    defer current_fence.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), current_fence.status);
    try expectContains(current_fence.body, "result=fence_current\n");
    try expectContains(current_fence.body, "held=true\n");

    var promote_assess = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"promote\",\"assess\",\"--current-fence\"]}",
    });
    defer promote_assess.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), promote_assess.status);
    try expectContains(promote_assess.body, "result=promote_assess\n");
    try expectContains(promote_assess.body, "assessment.can_promote=true\n");

    var promote = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"--table\",\"promote\",\"--current-fence\"]}",
    });
    defer promote.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), promote.status);
    try expectContains(promote.body, "result=promote_current_fence\n");
    try expectContains(promote.body, "promotion.new_identity.timeline_id=2\n");
}

test "storage.ha http admin returns route method and command errors" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var missing = try server.handle(.{ .method = .GET, .uri = "/ha/v1/missing" });
    defer missing.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 404), missing.status);

    var not_ready = try server.handle(.{ .method = .GET, .uri = Routes.ready });
    defer not_ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), not_ready.status);
    try std.testing.expectEqualStrings("not ready", not_ready.body);

    var wrong_method = try server.handle(.{ .method = .PUT, .uri = Routes.command });
    defer wrong_method.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 405), wrong_method.status);

    var bad_json = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{",
    });
    defer bad_json.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 400), bad_json.status);

    var unavailable = try server.handle(.{
        .method = .POST,
        .uri = Routes.command,
        .content_type = "application/json",
        .body = "{\"argv\":[\"identify\"]}",
    });
    defer unavailable.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 409), unavailable.status);
    try expectContains(unavailable.body, "PrimaryUnavailable");
}

test "storage.ha http admin exposes request executor" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();
    const executor = server.executor();
    var health = try executor.execute(alloc, .{ .method = .GET, .uri = Routes.health });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);
}

test "storage.ha http admin accepts absolute URIs" {
    const alloc = std.testing.allocator;
    var server = Server.init(alloc, .{});
    defer server.deinit();

    var health = try server.handle(.{ .method = .GET, .uri = "http://ha-admin.test/ha/v1/health" });
    defer health.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 200), health.status);

    var ready = try server.handle(.{ .method = .GET, .uri = "http://ha-admin.test/ha/v1/ready" });
    defer ready.deinit(alloc);
    try std.testing.expectEqual(@as(u16, 503), ready.status);
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
