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

//! Shared HA admin command executor.
//!
//! `admin_cli.zig` owns parsing and stable command vocabulary. `admin.zig`
//! owns individual storage operations. This module binds the two so CLI, HTTP,
//! and operator integration layers can execute the same command contract without
//! recreating dispatch, handle checks, status/metrics selection, or result
//! cleanup.

const std = @import("std");
const Allocator = std.mem.Allocator;
const admin = @import("admin.zig");
const admin_cli = @import("admin_cli.zig");
const commit_gate = @import("commit_gate.zig");
const fencing = @import("fencing.zig");
const metrics = @import("metrics.zig");
const primary_mod = @import("primary.zig");
const read_gate = @import("read_gate.zig");
const rejoin = @import("rejoin.zig");
const replication_api = @import("replication_api.zig");
const replication_record = @import("replication_record.zig");
const standby_mod = @import("standby.zig");
const status = @import("status.zig");

var test_path_counter: u64 = 0;

pub const Context = struct {
    primary: ?*primary_mod.Primary = null,
    standby: ?*standby_mod.Standby = null,
    fence_store: ?*fencing.Store = null,
};

pub const Result = union(enum) {
    identify_system: replication_api.IdentifySystemResponse,
    slot: admin.SlotResult,
    slot_list: status.PrimarySnapshot,
    seed_begin: primary_mod.BaseBackupStartResult,
    start_replication: replication_api.StartReplicationResponse,
    standby_status_update: replication_api.StandbyStatusUpdateResponse,
    primary_status: status.PrimarySnapshot,
    standby_status: status.StandbySnapshot,
    primary_metrics: metrics.PrimaryMetrics,
    standby_metrics: metrics.StandbyMetrics,
    commit_check: commit_gate.GateResult,
    read_check: read_gate.Decision,
    promote: admin.FencedPromotionResult,
    rejoin_assess: rejoin.Assessment,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        switch (self.*) {
            .start_replication => |*result| result.deinit(alloc),
            .slot_list => |*snapshot| snapshot.deinit(alloc),
            .primary_status => |*snapshot| snapshot.deinit(alloc),
            .primary_metrics => |*snapshot| snapshot.deinit(alloc),
            .promote => |*result| result.deinit(alloc),
            else => {},
        }
        self.* = undefined;
    }
};

pub const ResultDocument = struct {
    schema_version: u32 = 1,
    result: Result,
};

pub const RenderedOutput = struct {
    content_type: []const u8,
    body: []u8,

    pub fn deinit(self: *RenderedOutput, alloc: Allocator) void {
        alloc.free(self.body);
        self.* = undefined;
    }
};

pub fn resultDocument(result: Result) ResultDocument {
    return .{ .result = result };
}

pub fn renderJsonAlloc(alloc: Allocator, result: Result) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, resultDocument(result), .{});
}

pub fn renderPrometheusAlloc(alloc: Allocator, result: Result) ![]u8 {
    return switch (result) {
        .slot_list => |snapshot| blk: {
            var metric_snapshot = try metrics.fromPrimarySnapshot(alloc, snapshot);
            defer metric_snapshot.deinit(alloc);
            break :blk try metrics.renderPrimaryPrometheusAlloc(alloc, metric_snapshot);
        },
        .primary_status => |snapshot| blk: {
            var metric_snapshot = try metrics.fromPrimarySnapshot(alloc, snapshot);
            defer metric_snapshot.deinit(alloc);
            break :blk try metrics.renderPrimaryPrometheusAlloc(alloc, metric_snapshot);
        },
        .standby_status => |snapshot| try metrics.renderStandbyPrometheusAlloc(
            alloc,
            metrics.fromStandbySnapshot(snapshot),
        ),
        .primary_metrics => |metric_snapshot| try metrics.renderPrimaryPrometheusAlloc(alloc, metric_snapshot),
        .standby_metrics => |metric_snapshot| try metrics.renderStandbyPrometheusAlloc(alloc, metric_snapshot),
        else => error.PrometheusUnsupportedForResult,
    };
}

pub fn renderOutputAlloc(alloc: Allocator, result: Result, output: admin_cli.OutputFormat) !RenderedOutput {
    return switch (output) {
        .json => .{
            .content_type = "application/json",
            .body = try renderJsonAlloc(alloc, result),
        },
        .prometheus => .{
            .content_type = "text/plain; version=0.0.4",
            .body = try renderPrometheusAlloc(alloc, result),
        },
        .table => error.TableOutputRequiresIntegration,
    };
}

pub fn executeAndRenderAlloc(alloc: Allocator, ctx: Context, plan: admin_cli.Plan) !RenderedOutput {
    var result = try execute(alloc, ctx, plan);
    defer result.deinit(alloc);
    return try renderOutputAlloc(alloc, result, plan.output);
}

pub fn execute(alloc: Allocator, ctx: Context, plan: admin_cli.Plan) !Result {
    return switch (plan.command) {
        .identify_system => .{ .identify_system = admin.identifyPrimary(try requirePrimary(ctx)) },
        .slot => |command| .{
            .slot = try admin.applySlotAction(try requirePrimary(ctx), command.action, command.request),
        },
        .slot_list => |command| .{
            .slot_list = try admin.primaryStatus(alloc, try requirePrimary(ctx), command.retention_policy, null),
        },
        .seed => |command| try executeSeed(try requirePrimary(ctx), command),
        .start_replication => |request| .{
            .start_replication = try replication_api.startReplication(alloc, try requirePrimary(ctx), request),
        },
        .standby_status_update => |request| .{
            .standby_status_update = try admin.updateStandbyProgress(try requirePrimary(ctx), request),
        },
        .primary_status => |command| try executePrimaryStatus(alloc, try requirePrimary(ctx), command),
        .standby_status => |command| executeStandbyStatus(try requireStandby(ctx), command),
        .commit_check => |command| .{
            .commit_check = try admin.evaluateCommit(try requirePrimary(ctx), command.target_lsn, command.policy),
        },
        .read_check => |request| .{
            .read_check = try admin.evaluateStandbyRead(try requireStandby(ctx), request),
        },
        .promote => |request| .{
            .promote = try admin.promoteWithFence(alloc, try requireFenceStore(ctx), try requireStandby(ctx), request),
        },
        .rejoin_assess => |command| .{
            .rejoin_assess = admin.assessFormerPrimaryRejoin(command.former, command.receipt, command.policy),
        },
    };
}

fn executeSeed(primary: *primary_mod.Primary, command: admin_cli.SeedCommand) !Result {
    return switch (command) {
        .begin => |request| .{ .seed_begin = try admin.beginBaseBackup(primary, request) },
        .finish => error.ManifestFileExecutionRequiresIntegration,
        .bootstrap => error.ManifestFileExecutionRequiresIntegration,
    };
}

fn executePrimaryStatus(
    alloc: Allocator,
    primary: *primary_mod.Primary,
    command: admin_cli.PrimaryStatusCommand,
) !Result {
    var snapshot = try admin.primaryStatus(alloc, primary, command.retention_policy, command.sync_policy);
    errdefer snapshot.deinit(alloc);

    return switch (command.view) {
        .status => .{ .primary_status = snapshot },
        .metrics => blk: {
            var metric_snapshot = try metrics.fromPrimarySnapshot(alloc, snapshot);
            errdefer metric_snapshot.deinit(alloc);
            snapshot.deinit(alloc);
            break :blk .{ .primary_metrics = metric_snapshot };
        },
    };
}

fn executeStandbyStatus(standby: *const standby_mod.Standby, command: admin_cli.StandbyStatusCommand) Result {
    const snapshot = admin.standbyStatus(standby, command.upstream_lsn);
    return switch (command.view) {
        .status => .{ .standby_status = snapshot },
        .metrics => .{ .standby_metrics = metrics.fromStandbySnapshot(snapshot) },
    };
}

fn requirePrimary(ctx: Context) !*primary_mod.Primary {
    return ctx.primary orelse error.PrimaryUnavailable;
}

fn requireStandby(ctx: Context) !*standby_mod.Standby {
    return ctx.standby orelse error.StandbyUnavailable;
}

fn requireFenceStore(ctx: Context) !*fencing.Store {
    return ctx.fence_store orelse error.FenceStoreUnavailable;
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
        ".zig-cache/tmp/ha-admin-exec-" ++ name ++ "-" ++ part ++ "-{d}-{d}",
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

fn baseRecord(identity: standby_mod.Identity, lsn: u64, payload: []const u8) replication_record.Record {
    return .{
        .kind = .batch_mutation,
        .payload_codec = .raw,
        .cluster_id = identity.cluster_id,
        .shard_id = identity.shard_id,
        .table_id = identity.table_id,
        .timeline_id = identity.timeline_id,
        .epoch = identity.epoch,
        .lsn = lsn,
        .previous_lsn = lsn - 1,
        .payload = payload,
    };
}

const ApplyCounter = struct {
    count: usize = 0,

    fn apply(ctx: *anyopaque, _: replication_record.RecordView) !void {
        const self: *ApplyCounter = @ptrCast(@alignCast(ctx));
        self.count += 1;
    }
};

test "storage.ha admin exec runs slot lifecycle and status commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "status");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "one" });
    _ = try primary.append(.{ .payload = "two" });

    var create_plan = try admin_cli.parse(alloc, &.{ "slot", "create", "standby-a", "--initial-lsn", "1" });
    defer create_plan.deinit(alloc);
    var created = try execute(alloc, .{ .primary = &primary }, create_plan);
    defer created.deinit(alloc);
    try std.testing.expectEqualStrings("standby-a", created.slot.create.slot_name);
    try std.testing.expectEqual(@as(u64, 1), created.slot.create.restart_lsn);

    var ack_plan = try admin_cli.parse(alloc, &.{ "standby", "ack", "--slot", "standby-a", "--timeline-id", "1", "--received-lsn", "2", "--applied-lsn", "1" });
    defer ack_plan.deinit(alloc);
    var acked = try execute(alloc, .{ .primary = &primary }, ack_plan);
    defer acked.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), acked.standby_status_update.received_lsn);

    var status_plan = try admin_cli.parse(alloc, &.{ "status", "primary", "--view", "metrics", "--max-lag-lsn", "10" });
    defer status_plan.deinit(alloc);
    var primary_metrics = try execute(alloc, .{ .primary = &primary }, status_plan);
    defer primary_metrics.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 2), primary_metrics.primary_metrics.current_lsn);
    try std.testing.expectEqual(@as(u64, 1), primary_metrics.primary_metrics.slot_count);
    try std.testing.expectEqual(@as(u64, 1), primary_metrics.primary_metrics.max_apply_lag_lsn);

    const json_body = try renderJsonAlloc(alloc, primary_metrics);
    defer alloc.free(json_body);
    try expectContains(json_body, "\"schema_version\":1");
    try expectContains(json_body, "\"primary_metrics\"");
    try expectContains(json_body, "\"current_lsn\":2");

    const prometheus_body = try renderPrometheusAlloc(alloc, primary_metrics);
    defer alloc.free(prometheus_body);
    try expectContains(prometheus_body, "antfly_ha_primary_current_lsn 2\n");
    try expectContains(prometheus_body, "antfly_ha_slot_apply_lag_lsn{slot=\"standby-a\"} 1\n");

    var rendered_plan = try admin_cli.parse(alloc, &.{ "--prometheus", "status", "primary", "--view", "metrics", "--max-lag-lsn", "10" });
    defer rendered_plan.deinit(alloc);
    var rendered = try executeAndRenderAlloc(alloc, .{ .primary = &primary }, rendered_plan);
    defer rendered.deinit(alloc);
    try std.testing.expectEqualStrings("text/plain; version=0.0.4", rendered.content_type);
    try expectContains(rendered.body, "antfly_ha_primary_current_lsn 2\n");
}

test "storage.ha admin exec runs read commit promote and rejoin commands" {
    const alloc = std.testing.allocator;
    const paths = try testPaths(alloc, "promote");
    defer paths.deinit(alloc);
    const identity = testIdentity();

    var primary = try primary_mod.Primary.open(alloc, paths.primary_log.ptr, paths.primary_slots.ptr, identity, .{});
    defer primary.close();
    var standby = try standby_mod.Standby.open(alloc, paths.standby_log.ptr, paths.standby_progress.ptr, identity, .{});
    defer standby.close();
    var fence_store = try fencing.Store.open(alloc, paths.fence_wal.ptr, .{});
    defer fence_store.close();

    try primary.createSlot("standby-a", 0);
    _ = try primary.append(.{ .payload = "one" });
    try primary.standbyStatusUpdate("standby-a", identity.timeline_id, 1, 1);
    _ = try standby.receive(baseRecord(identity, 1, "one"));
    var apply_counter = ApplyCounter{};
    try std.testing.expectEqual(@as(usize, 1), try standby.applyAvailable(&apply_counter, ApplyCounter.apply));

    var commit_plan = try admin_cli.parse(alloc, &.{ "commit", "check", "--target-lsn", "1", "--sync-mode", "remote-apply", "--sync-standby", "standby-a" });
    defer commit_plan.deinit(alloc);
    var commit = try execute(alloc, .{ .primary = &primary }, commit_plan);
    defer commit.deinit(alloc);
    try std.testing.expectEqual(commit_gate.Action.acknowledge, commit.commit_check.action);

    var read_plan = try admin_cli.parse(alloc, &.{ "read", "check", "--at-least-lsn", "1" });
    defer read_plan.deinit(alloc);
    var read = try execute(alloc, .{ .standby = &standby }, read_plan);
    defer read.deinit(alloc);
    try std.testing.expectEqual(read_gate.Action.serve_standby, read.read_check.action);

    var promote_plan = try admin_cli.parse(alloc, &.{
        "promote",
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
        "admin-exec-test",
    });
    defer promote_plan.deinit(alloc);
    var promoted = try execute(alloc, .{ .standby = &standby, .fence_store = &fence_store }, promote_plan);
    defer promoted.deinit(alloc);
    try std.testing.expect(promoted.promote.assessment.can_promote);
    try std.testing.expectEqual(@as(u64, 2), standby.identity.timeline_id);

    var rejoin_plan = try admin_cli.parse(alloc, &.{
        "rejoin",              "assess",
        "--node-id",           "primary-a",
        "--cluster-id",        "100",
        "--shard-id",          "10",
        "--table-id",          "20",
        "--timeline-id",       "1",
        "--epoch",             "1",
        "--last-lsn",          "1",
        "--retained-from-lsn", "1",
    });
    defer rejoin_plan.deinit(alloc);
    var rejoin_result = try execute(alloc, .{}, rejoin_plan);
    defer rejoin_result.deinit(alloc);
    try std.testing.expectEqual(rejoin.Action.reject_unfenced, rejoin_result.rejoin_assess.action);

    const read_json = try renderJsonAlloc(alloc, read);
    defer alloc.free(read_json);
    try expectContains(read_json, "\"schema_version\":1");
    try expectContains(read_json, "\"read_check\"");
    try expectContains(read_json, "\"action\":\"serve_standby\"");

    try std.testing.expectError(error.PrometheusUnsupportedForResult, renderPrometheusAlloc(alloc, read));

    var json_plan = try admin_cli.parse(alloc, &.{ "read", "check", "--at-least-lsn", "1" });
    defer json_plan.deinit(alloc);
    var rendered_json = try executeAndRenderAlloc(alloc, .{ .standby = &standby }, json_plan);
    defer rendered_json.deinit(alloc);
    try std.testing.expectEqualStrings("application/json", rendered_json.content_type);
    try expectContains(rendered_json.body, "\"read_check\"");

    var table_plan = try admin_cli.parse(alloc, &.{ "--table", "read", "check", "--at-least-lsn", "1" });
    defer table_plan.deinit(alloc);
    try std.testing.expectError(error.TableOutputRequiresIntegration, executeAndRenderAlloc(alloc, .{ .standby = &standby }, table_plan));
}

fn expectContains(haystack: []const u8, needle: []const u8) !void {
    try std.testing.expect(std.mem.indexOf(u8, haystack, needle) != null);
}
