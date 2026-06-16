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
const admin_api = @import("../../admin/mod.zig");
const http_common = @import("../../common/http/http_common.zig");
const routes = @import("../../raft/transport/routes.zig");
const backup_manifest = @import("backup_manifest.zig");
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

pub fn ParsedOutput(comptime T: type) type {
    return struct {
        body: []u8,
        parsed: std.json.Parsed(T),

        pub fn deinit(self: *@This(), alloc: Allocator) void {
            self.parsed.deinit();
            alloc.free(self.body);
            self.* = undefined;
        }
    };
}

pub const PrimaryStatusOptions = struct {
    max_lag_lsn: ?u64 = null,
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

    pub fn checkReady(self: *Client, base_uri: []const u8) !void {
        const uri = try join(self.alloc, base_uri, http_admin.Routes.ready);
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

    pub fn getPrimaryStatus(
        self: *Client,
        base_uri: []const u8,
        options: PrimaryStatusOptions,
    ) !ParsedOutput(admin_api.openapi.HAPrimaryStatusResponse) {
        var uri = try join(self.alloc, base_uri, admin_api.routes.ha_primary_status);
        defer self.alloc.free(uri);
        if (options.max_lag_lsn) |max_lag_lsn| {
            uri = try appendQueryU64(self.alloc, uri, "max_lag_lsn", max_lag_lsn);
        }

        return try self.executeJson(admin_api.openapi.HAPrimaryStatusResponse, .{
            .method = .GET,
            .uri = uri,
        });
    }

    pub fn getStandbyStatus(
        self: *Client,
        base_uri: []const u8,
        upstream_lsn: ?u64,
    ) !ParsedOutput(admin_api.openapi.HAStandbyStatusResponse) {
        var uri = try join(self.alloc, base_uri, admin_api.routes.ha_standby_status);
        defer self.alloc.free(uri);
        if (upstream_lsn) |lsn| {
            uri = try appendQueryU64(self.alloc, uri, "upstream_lsn", lsn);
        }

        return try self.executeJson(admin_api.openapi.HAStandbyStatusResponse, .{
            .method = .GET,
            .uri = uri,
        });
    }

    pub fn listReplicationSlots(
        self: *Client,
        base_uri: []const u8,
    ) !ParsedOutput(admin_api.openapi.HAPrimaryStatusResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_replication_slots);
        defer self.alloc.free(uri);
        return try self.executeJson(admin_api.openapi.HAPrimaryStatusResponse, .{
            .method = .GET,
            .uri = uri,
        });
    }

    pub fn createReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        initial_lsn: ?u64,
    ) !ParsedOutput(admin_api.openapi.HAReplicationSlotActionResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_replication_slots);
        defer self.alloc.free(uri);
        const body = try std.json.Stringify.valueAlloc(
            self.alloc,
            admin_api.openapi.ReplicationSlotCreateRequest{
                .slot_name = slot_name,
                .initial_lsn = if (initial_lsn) |lsn| try i64FromU64(lsn) else null,
            },
            .{},
        );
        defer self.alloc.free(body);

        return try self.executeJson(admin_api.openapi.HAReplicationSlotActionResponse, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
    }

    pub fn pauseReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
    ) !ParsedOutput(admin_api.openapi.HAReplicationSlotActionResponse) {
        return try self.replicationSlotLifecycle(base_uri, slot_name, .pause);
    }

    pub fn resumeReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
    ) !ParsedOutput(admin_api.openapi.HAReplicationSlotActionResponse) {
        return try self.replicationSlotLifecycle(base_uri, slot_name, .@"resume");
    }

    pub fn dropReplicationSlot(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
    ) !ParsedOutput(admin_api.openapi.HAReplicationSlotActionResponse) {
        return try self.replicationSlotLifecycle(base_uri, slot_name, .drop);
    }

    pub fn beginBaseBackup(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.BaseBackupStartRequest,
    ) !ParsedOutput(admin_api.openapi.HABaseBackupBeginResponse) {
        return try self.postJson(
            admin_api.openapi.HABaseBackupBeginResponse,
            base_uri,
            admin_api.routes.ha_base_backups,
            request,
        );
    }

    pub fn finishBaseBackup(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.BaseBackupManifestPathRequest,
    ) !ParsedOutput(admin_api.openapi.HABaseBackupFinishResponse) {
        return try self.postJson(
            admin_api.openapi.HABaseBackupFinishResponse,
            base_uri,
            admin_api.routes.ha_base_backups_finish,
            request,
        );
    }

    pub fn bootstrapStandby(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.StandbyBootstrapRequest,
    ) !ParsedOutput(admin_api.openapi.HAStandbyBootstrapResponse) {
        return try self.postJson(
            admin_api.openapi.HAStandbyBootstrapResponse,
            base_uri,
            admin_api.routes.ha_standby_bootstrap,
            request,
        );
    }

    pub fn acquireFence(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.FenceAcquireRequest,
    ) !ParsedOutput(admin_api.openapi.HAFenceResponse) {
        return try self.postJson(
            admin_api.openapi.HAFenceResponse,
            base_uri,
            admin_api.routes.ha_fence,
            request,
        );
    }

    pub fn currentFence(
        self: *Client,
        base_uri: []const u8,
    ) !ParsedOutput(admin_api.openapi.HACurrentFenceResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_fence_current);
        defer self.alloc.free(uri);
        return try self.executeJson(admin_api.openapi.HACurrentFenceResponse, .{
            .method = .GET,
            .uri = uri,
        });
    }

    pub fn assessPromotion(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.PromotionAssessRequest,
    ) !ParsedOutput(admin_api.openapi.HAPromotionAssessResponse) {
        return try self.postJson(
            admin_api.openapi.HAPromotionAssessResponse,
            base_uri,
            admin_api.routes.ha_promotion_assess,
            request,
        );
    }

    pub fn promoteWithCurrentFence(
        self: *Client,
        base_uri: []const u8,
    ) !ParsedOutput(admin_api.openapi.HAPromotionResponse) {
        const uri = try join(self.alloc, base_uri, admin_api.routes.ha_promotion_current_fence);
        defer self.alloc.free(uri);
        return try self.executeJson(admin_api.openapi.HAPromotionResponse, .{
            .method = .POST,
            .uri = uri,
        });
    }

    pub fn promote(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.FenceAcquireRequest,
    ) !ParsedOutput(admin_api.openapi.HAPromotionResponse) {
        return try self.postJson(
            admin_api.openapi.HAPromotionResponse,
            base_uri,
            admin_api.routes.ha_promotion,
            request,
        );
    }

    pub fn assessRejoin(
        self: *Client,
        base_uri: []const u8,
        request: admin_api.openapi.RejoinAssessRequest,
    ) !ParsedOutput(admin_api.openapi.HARejoinAssessResponse) {
        return try self.postJson(
            admin_api.openapi.HARejoinAssessResponse,
            base_uri,
            admin_api.routes.ha_rejoin_assess,
            request,
        );
    }

    fn replicationSlotLifecycle(
        self: *Client,
        base_uri: []const u8,
        slot_name: []const u8,
        action: enum { pause, @"resume", drop },
    ) !ParsedOutput(admin_api.openapi.HAReplicationSlotActionResponse) {
        const path = switch (action) {
            .pause => try admin_api.routes.replicationSlotPausePathAlloc(self.alloc, slot_name),
            .@"resume" => try admin_api.routes.replicationSlotResumePathAlloc(self.alloc, slot_name),
            .drop => try admin_api.routes.replicationSlotPathAlloc(self.alloc, slot_name),
        };
        defer self.alloc.free(path);
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);

        return try self.executeJson(admin_api.openapi.HAReplicationSlotActionResponse, .{
            .method = switch (action) {
                .drop => .DELETE,
                .pause, .@"resume" => .PUT,
            },
            .uri = uri,
        });
    }

    fn executeJson(
        self: *Client,
        comptime T: type,
        req: http_common.HttpRequest,
    ) !ParsedOutput(T) {
        var resp = try self.executeWithRetry(req);
        errdefer resp.deinit(self.alloc);
        try mapStatus(resp.status);

        if (resp.content_type) |content_type| self.alloc.free(content_type);
        resp.content_type = null;
        for (resp.headers) |*header| header.deinit(self.alloc);
        if (resp.headers.len > 0) self.alloc.free(resp.headers);
        resp.headers = &.{};

        const body = resp.body;
        resp.body = &.{};
        errdefer self.alloc.free(body);
        const parsed = try std.json.parseFromSlice(
            T,
            self.alloc,
            body,
            .{ .ignore_unknown_fields = true },
        );
        return .{
            .body = body,
            .parsed = parsed,
        };
    }

    fn postJson(
        self: *Client,
        comptime T: type,
        base_uri: []const u8,
        path: []const u8,
        value: anytype,
    ) !ParsedOutput(T) {
        const uri = try join(self.alloc, base_uri, path);
        defer self.alloc.free(uri);
        const body = try std.json.Stringify.valueAlloc(self.alloc, value, .{});
        defer self.alloc.free(body);
        return try self.executeJson(T, .{
            .method = .POST,
            .uri = uri,
            .content_type = "application/json",
            .body = body,
        });
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
        if (status == 503) return error.HaEndpointNotReady;
        return error.UnexpectedHttpStatus;
    }
};

fn join(alloc: Allocator, base_uri: []const u8, path: []const u8) ![]u8 {
    return try routes.Routes.join(alloc, base_uri, path);
}

fn appendQueryU64(alloc: Allocator, old_uri: []u8, key: []const u8, value: u64) ![]u8 {
    const separator: []const u8 = if (std.mem.indexOfScalar(u8, old_uri, '?') == null) "?" else "&";
    const next = try std.fmt.allocPrint(alloc, "{s}{s}{s}={d}", .{ old_uri, separator, key, value });
    alloc.free(old_uri);
    return next;
}

fn i64FromU64(value: u64) !i64 {
    if (value > @as(u64, @intCast(std.math.maxInt(i64)))) return error.InvalidHaCommand;
    return @intCast(value);
}

const TestPaths = struct {
    primary_log: [:0]u8,
    primary_slots: [:0]u8,
    standby_log: [:0]u8,
    standby_progress: [:0]u8,
    fence_wal: [:0]u8,
    backup_root: [:0]u8,

    fn deinit(self: TestPaths, alloc: Allocator) void {
        alloc.free(self.primary_log);
        alloc.free(self.primary_slots);
        alloc.free(self.standby_log);
        alloc.free(self.standby_progress);
        alloc.free(self.fence_wal);
        alloc.free(self.backup_root);
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
    const backup_root = try allocPrintPath(alloc, name, "backup-root", nonce);
    defer alloc.free(backup_root);

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), primary_slots) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_log) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), standby_progress) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), fence_wal) catch {};
    std.Io.Dir.cwd().deleteTree(io_impl.io(), backup_root) catch {};

    return .{
        .primary_log = try alloc.dupeZ(u8, primary_log),
        .primary_slots = try alloc.dupeZ(u8, primary_slots),
        .standby_log = try alloc.dupeZ(u8, standby_log),
        .standby_progress = try alloc.dupeZ(u8, standby_progress),
        .fence_wal = try alloc.dupeZ(u8, fence_wal),
        .backup_root = try alloc.dupeZ(u8, backup_root),
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

fn testAdminIdentity() admin_api.openapi.HAIdentity {
    const identity = testIdentity();
    return .{
        .cluster_id = @intCast(identity.cluster_id),
        .shard_id = @intCast(identity.shard_id),
        .table_id = @intCast(identity.table_id),
        .timeline_id = @intCast(identity.timeline_id),
        .epoch = @intCast(identity.epoch),
    };
}

fn seedFiles() [2]backup_manifest.FileEntry {
    return .{
        .{ .path = "manifest", .kind = .manifest, .size_bytes = 8, .crc32 = backup_manifest.crc32("manifest") },
        .{ .path = "sst/0001", .kind = .sstable, .size_bytes = 7, .crc32 = backup_manifest.crc32("sstable") },
    };
}

fn writeTestFile(path: []const u8, bytes: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    if (std.fs.path.dirname(path)) |parent| try std.Io.Dir.cwd().createDirPath(io_impl.io(), parent);
    try std.Io.Dir.cwd().writeFile(io_impl.io(), .{
        .sub_path = path,
        .data = bytes,
    });
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
    try client.checkReady("http://ha-admin.test");

    var typed_primary_status = try client.getPrimaryStatus("http://ha-admin.test", .{ .max_lag_lsn = 1 });
    defer typed_primary_status.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), typed_primary_status.parsed.value.schema_version);
    try std.testing.expectEqualStrings("primary", typed_primary_status.parsed.value.snapshot.role);
    try std.testing.expectEqual(@as(i64, 0), typed_primary_status.parsed.value.snapshot.current_lsn);

    var typed_created = try client.createReplicationSlot("http://ha-admin.test", "standby-typed", 0);
    defer typed_created.deinit(alloc);
    try std.testing.expectEqualStrings("create", typed_created.parsed.value.slot_action);
    try std.testing.expectEqualStrings("standby-typed", typed_created.parsed.value.slot.slot_name);
    try std.testing.expect(typed_created.parsed.value.slot.active);

    var typed_slots = try client.listReplicationSlots("http://ha-admin.test");
    defer typed_slots.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), typed_slots.parsed.value.snapshot.slots.len);
    try std.testing.expectEqualStrings("standby-typed", typed_slots.parsed.value.snapshot.slots[0].name);

    var typed_paused = try client.pauseReplicationSlot("http://ha-admin.test", "standby-typed");
    defer typed_paused.deinit(alloc);
    try std.testing.expectEqualStrings("pause", typed_paused.parsed.value.slot_action);
    try std.testing.expect(!typed_paused.parsed.value.slot.active);

    var typed_resumed = try client.resumeReplicationSlot("http://ha-admin.test", "standby-typed");
    defer typed_resumed.deinit(alloc);
    try std.testing.expectEqualStrings("resume", typed_resumed.parsed.value.slot_action);
    try std.testing.expect(typed_resumed.parsed.value.slot.active);

    var typed_dropped = try client.dropReplicationSlot("http://ha-admin.test", "standby-typed");
    defer typed_dropped.deinit(alloc);
    try std.testing.expectEqualStrings("drop", typed_dropped.parsed.value.slot_action);
    try std.testing.expectEqual(@as(?bool, true), typed_dropped.parsed.value.slot.dropped);

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

    var typed_standby_status = try client.getStandbyStatus("http://ha-admin.test", 2);
    defer typed_standby_status.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), typed_standby_status.parsed.value.schema_version);
    try std.testing.expectEqualStrings("standby", typed_standby_status.parsed.value.snapshot.role);
    try std.testing.expectEqual(@as(i64, 1), typed_standby_status.parsed.value.snapshot.applied_lsn);
    try std.testing.expectEqual(@as(?i64, 2), typed_standby_status.parsed.value.snapshot.upstream_lsn);
    try std.testing.expectEqual(@as(?i64, 1), typed_standby_status.parsed.value.snapshot.write_lag_lsn);

    var operator_plan = try client.executeCommand("http://ha-admin.test", &.{
        "operator",
        "plan",
        "--standby",
        "standby-a",
        "--sync-mode",
        "remote-apply",
        "--sync-standby",
        "standby-a",
        "--auto-failover",
        "--fencing-authority",
        "kubernetes-lease",
        "--current-primary-id",
        "primary-a",
        "--primary-admin-unavailable",
        "--fence-authority",
        "kubernetes-lease",
        "--fence-ready",
        "--fence-holder",
        "standby-a",
        "--fence-generation",
        "4",
        "--fence-reason",
        "LeaseAcquired",
    });
    defer operator_plan.deinit(alloc);
    try std.testing.expectEqualStrings("application/json", operator_plan.content_type);
    try expectContains(operator_plan.body, "\"operator_plan\"");
    try expectContains(operator_plan.body, "\"automatic_promotion_allowed\":true");
    try expectContains(operator_plan.body, "\"fencing_precondition\"");
    try expectContains(operator_plan.body, "\"generation\":4");

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

    var rejoin_plan = try client.executeCommand("http://ha-admin.test", &.{
        "--table",
        "operator",
        "plan",
        "--former-primary-id",
        "primary-a",
        "--former-cluster-id",
        "100",
        "--former-shard-id",
        "10",
        "--former-table-id",
        "20",
        "--former-timeline-id",
        "1",
        "--former-epoch",
        "1",
        "--former-last-lsn",
        "1",
        "--retained-from-lsn",
        "1",
        "--receipt-old-primary-id",
        "primary-a",
        "--receipt-promoted-node-id",
        "standby-a",
        "--receipt-parent-timeline-id",
        "1",
        "--receipt-parent-epoch",
        "1",
        "--receipt-new-timeline-id",
        "2",
        "--receipt-new-epoch",
        "2",
        "--receipt-required-lsn",
        "1",
        "--receipt-observed-lsn",
        "1",
        "--receipt-generation",
        "1",
        "--receipt-token",
        "token",
        "--receipt-reason",
        "http-client-test",
    });
    defer rejoin_plan.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", rejoin_plan.content_type);
    try expectContains(rejoin_plan.body, "result=operator_plan\n");
    try expectContains(rejoin_plan.body, "former_primary.action=rewind\n");
    try expectContains(rejoin_plan.body, "former_primary.fork_lsn=1\n");
}

test "storage.ha http client round trips typed seed operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "typed-seed");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();

    var server = http_admin.Server.init(alloc, .{ .primary = &primary, .standby = &standby });
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    var begin = try client.beginBaseBackup("http://ha-admin.test", .{
        .slot_name = "standby-seed",
        .manifest_id = "base-http-client",
    });
    defer begin.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), begin.parsed.value.schema_version);
    try std.testing.expectEqual(@as(i64, 1), begin.parsed.value.backup_lsn);
    try std.testing.expectEqual(@as(i64, 1), begin.parsed.value.start_record_lsn);

    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "during-copy" }));

    const files = seedFiles();
    const encoded_manifest = try backup_manifest.encodeAlloc(alloc, .{
        .identity = identity,
        .manifest_id = "base-http-client",
        .backup_lsn = 1,
        .checkpoint_lsn = 2,
        .files = &files,
    });
    defer alloc.free(encoded_manifest);

    const manifest_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "backup.afha" });
    defer alloc.free(manifest_path);
    const manifest_file_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "manifest" });
    defer alloc.free(manifest_file_path);
    const sstable_path = try std.fs.path.join(alloc, &.{ paths.backup_root, "sst/0001" });
    defer alloc.free(sstable_path);
    try writeTestFile(manifest_path, encoded_manifest);
    try writeTestFile(manifest_file_path, "manifest");
    try writeTestFile(sstable_path, "sstable");

    var finish = try client.finishBaseBackup("http://ha-admin.test", .{
        .manifest_path = manifest_path,
    });
    defer finish.deinit(alloc);
    try std.testing.expectEqualStrings("base-http-client", finish.parsed.value.manifest_id);
    try std.testing.expectEqual(@as(i64, 1), finish.parsed.value.backup_lsn);
    try std.testing.expectEqual(@as(i64, 3), finish.parsed.value.end_record_lsn);

    var bootstrap = try client.bootstrapStandby("http://ha-admin.test", .{
        .manifest_path = manifest_path,
        .content_root = paths.backup_root,
    });
    defer bootstrap.deinit(alloc);
    try std.testing.expectEqualStrings("base-http-client", bootstrap.parsed.value.manifest_id);
    try std.testing.expectEqual(@as(i64, 2), bootstrap.parsed.value.checkpoint_lsn);
    try std.testing.expectEqual(@as(u64, 3), standby.nextReceiveLsn());
}

test "storage.ha http client round trips typed safety operations" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "typed-safety");
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

    var slot = try client.createReplicationSlot("http://ha-admin.test", "standby-a", 0);
    defer slot.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), try primary.append(.{ .payload = "one" }));

    var streamed = try client.executeCommand("http://ha-admin.test", &.{ "--table", "stream", "once", "--slot", "standby-a" });
    defer streamed.deinit(alloc);
    try expectContains(streamed.body, "applied_lsn=1\n");

    const fence_request = admin_api.openapi.FenceAcquireRequest{
        .identity = testAdminIdentity(),
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 1,
        .observed_lsn = 1,
        .reason = "typed-client-test",
    };
    var fence = try client.acquireFence("http://ha-admin.test", fence_request);
    defer fence.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 1), fence.parsed.value.schema_version);
    try std.testing.expectEqualStrings("standby-a", fence.parsed.value.receipt.promoted_node_id);
    try std.testing.expectEqual(@as(i64, 2), fence.parsed.value.receipt.new_timeline_id);

    var current = try client.currentFence("http://ha-admin.test");
    defer current.deinit(alloc);
    try std.testing.expect(current.parsed.value.held);
    try std.testing.expectEqualStrings("primary-a", current.parsed.value.receipt.?.old_primary_id);

    var assessment = try client.assessPromotion("http://ha-admin.test", .{
        .use_current_fence = true,
    });
    defer assessment.deinit(alloc);
    try std.testing.expect(assessment.parsed.value.assessment.can_promote);
    try std.testing.expect(assessment.parsed.value.assessment.fencing_confirmed);

    var promoted = try client.promoteWithCurrentFence("http://ha-admin.test");
    defer promoted.deinit(alloc);
    try std.testing.expectEqual(@as(i64, 2), promoted.parsed.value.promotion.new_identity.timeline_id);
    try std.testing.expectEqual(@as(i64, 1), promoted.parsed.value.fence_generation);

    var rejoin = try client.assessRejoin("http://ha-admin.test", .{
        .node_id = "primary-a",
        .identity = testAdminIdentity(),
        .last_lsn = 2,
        .retained_from_lsn = 0,
        .receipt = fence.parsed.value.receipt,
    });
    defer rejoin.deinit(alloc);
    try std.testing.expectEqualStrings("rewind", rejoin.parsed.value.assessment.action);
    try std.testing.expectEqual(@as(i64, 2), rejoin.parsed.value.assessment.target_timeline_id);
}

test "storage.ha http client maps admin errors" {
    const alloc = std.testing.allocator;
    var server = http_admin.Server.init(alloc, .{});
    defer server.deinit();
    var client = Client.init(alloc, server.executor());

    try std.testing.expectError(error.HaEndpointNotReady, client.checkReady("http://ha-admin.test"));
    try std.testing.expectError(error.HaCommandConflict, client.executeCommand("http://ha-admin.test", &.{"identify"}));
    try std.testing.expectError(error.InvalidHaCommand, client.executeCommand("http://ha-admin.test", &.{"unknown"}));
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
