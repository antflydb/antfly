// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");

const ha_commit_gate_mod = @import("../ha/commit_gate.zig");
const ha_effects_mod = @import("../ha/effects.zig");
const ha_fencing_mod = @import("../ha/fencing.zig");
const ha_primary_mod = @import("../ha/primary.zig");
const ha_session_mod = @import("../ha/session.zig");
const ha_standby_mod = @import("../ha/standby.zig");
const ha_write_gate_mod = @import("../ha/write_gate.zig");
const change_journal_mod = @import("derived/change_journal.zig");
const db_internal = @import("internal.zig");
const doc_identity = @import("doc_identity.zig");
const internal_keys = @import("../internal_keys.zig");
const ha_replication_record_mod = @import("../ha/replication_record.zig");
const ha_types = @import("ha_types.zig");
const replay_stream_mod = @import("derived/replay_stream.zig");
const schema_mod = @import("../schema.zig");
const transactions_mod = @import("../transactions.zig");
const db_config = @import("config.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};

fn tempPath(buf: []u8) [*:0]const u8 {
    return TestHelpers.tempPath(buf);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    TestHelpers.cleanupTempDir(path);
}

pub const AsyncEffectMirror = ha_types.AsyncEffectMirror;
pub const AsyncBatchMirror = ha_types.AsyncBatchMirror;
pub const AsyncMetadataMirror = ha_types.AsyncMetadataMirror;
pub const SyncWaitFn = ha_types.SyncWaitFn;
pub const ProgressPollFn = ha_types.ProgressPollFn;
pub const PrimaryProgressSyncWait = ha_types.PrimaryProgressSyncWait;
pub const SessionSyncWait = ha_types.SessionSyncWait;
pub const WriteGate = ha_types.WriteGate;
pub const applied_lsn_value_len = ha_types.applied_lsn_value_len;
pub const appliedReplicationLsnWrite = ha_types.appliedReplicationLsnWrite;
pub const readAppliedReplicationLsn = ha_types.readAppliedReplicationLsn;
pub const writeGateIsStandby = ha_types.writeGateIsStandby;

test "storage.ha db mirrors appended derived replay records into HA stream" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 200,
        .shard_id = 30,
        .table_id = 90,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{
            .identity_namespace = .{ .shard_id = 3, .table_id = 9 },
            .enrichment = .{ .enable_without_producers = true },
            .ha_async_effect_mirror = .{
                .primary = &primary,
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            },
        });
        defer db.close();

        const enrichment_ctx = db.enrichment_append_context orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 3), enrichment_ctx.identity_namespace.shard_id);
        try std.testing.expectEqual(@as(u64, 9), enrichment_ctx.identity_namespace.table_id);

        const changed_artifact_keys = [_][]const u8{artifact_key};
        const sequence = try db.derivedAsyncAppendDerivedBatchRecord(.{
            .changed_artifact_keys = changed_artifact_keys[0..],
        });
        try std.testing.expectEqual(@as(u64, 1), sequence);
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .derived_effect), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 200), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 3), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 9), entry.record.table_id);

    var decoded = try ha_effects_mod.decodeDerivedChangeRecord(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 1), decoded.record.sequence);
    try std.testing.expectEqualStrings(artifact_key, decoded.record.changed_artifact_keys[0]);
    try std.testing.expectEqual(change_journal_mod.TargetHint.graph, decoded.record.target_hints[0]);
}

test "storage.ha db mirrors committed batch mutations into HA stream for standby apply" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var primary_db_path_buf: [256]u8 = undefined;
    const primary_db_path = TestHelpers.tempPath(&primary_db_path_buf);
    defer TestHelpers.cleanupTempDir(primary_db_path);
    var standby_db_path_buf: [256]u8 = undefined;
    const standby_db_path = TestHelpers.tempPath(&standby_db_path_buf);
    defer TestHelpers.cleanupTempDir(standby_db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 250,
        .shard_id = 40,
        .table_id = 100,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();

    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, .{
        .cluster_id = 250,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    {
        var db = try DB.open(alloc, std.mem.span(primary_db_path), .{
            .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
            .ha_async_batch_mirror = .{
                .primary = &primary,
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            },
            .start_index_workers = false,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
            .deletes = &.{"doc:old"},
            .timestamp_ns = 123,
            .sync_level = .write,
        });
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .batch_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 250), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 4), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 10), entry.record.table_id);

    var decoded = try ha_effects_mod.decodeBatchMutationRequest(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.writes.len);
    try std.testing.expectEqualStrings("doc:a", decoded.value.request.writes[0].key);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", decoded.value.request.writes[0].value);
    try std.testing.expectEqual(@as(usize, 1), decoded.value.request.deletes.len);
    try std.testing.expectEqualStrings("doc:old", decoded.value.request.deletes[0]);
    try std.testing.expectEqual(@as(u64, 123), decoded.value.request.timestamp_ns);

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .ha_write_gate = .{ .standby = &standby },
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .last_lsn = &last_lsn,
            .failure_count = &failures,
        },
    });
    defer standby_db.close();

    try standby_db.batchReplicatedApply(decoded.value.request);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    var found = (try standby_db.lookup(alloc, "doc:a", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"alpha\"}", found.json);
}

test "storage.ha db evaluates sync commit gate for mirrored batch mutations" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 253,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var last_lsn = std.atomic.Value(u64).init(0);
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var degraded = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{
            .ha_async_batch_mirror = .{
                .primary = &primary,
                .last_lsn = &last_lsn,
                .sync_policy = .{
                    .mode = .remote_write,
                    .standby_names = &standby_names,
                    .failure_policy = .degrade_to_async,
                },
                .last_gate_lsn = &gate_lsn,
                .last_gate_action = &gate_action,
                .sync_degraded_count = &degraded,
            },
            .start_index_workers = false,
        });
        defer db.close();

        try db.batch(.{
            .writes = &.{.{ .key = "doc:sync", .value = "{\"title\":\"sync\"}" }},
            .sync_level = .write,
        });
    }

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.acknowledge_degraded), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), degraded.load(.acquire));
}

test "storage.ha db block sync policy waits for standby acknowledgement" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 255,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const SyncWait = struct {
        calls: u64 = 0,

        fn wait(ctx: *anyopaque, primary_arg: *ha_primary_mod.Primary, target_lsn: u64, policy: ha_primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try std.testing.expectEqual(ha_primary_mod.DurabilityMode.remote_write, policy.mode);
            try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, 0);
        }
    };

    var wait_state = SyncWait{};
    var last_lsn = std.atomic.Value(u64).init(0);
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:block", .value = "{\"title\":\"block\"}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(@as(u64, 1), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    var found = (try db.lookup(alloc, "doc:block", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"block\"}", found.json);
}

test "storage.ha db session sync wait satisfies remote apply through standby DB apply" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var primary_db_path_buf: [256]u8 = undefined;
    const primary_db_path = TestHelpers.tempPath(&primary_db_path_buf);
    defer TestHelpers.cleanupTempDir(primary_db_path);
    var standby_db_path_buf: [256]u8 = undefined;
    const standby_db_path = TestHelpers.tempPath(&standby_db_path_buf);
    defer TestHelpers.cleanupTempDir(standby_db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 257,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = false,
    });
    defer standby_db.close();

    var wait_state = SessionSyncWait{
        .alloc = alloc,
        .slot_name = "standby-a",
        .standby = &standby,
        .apply_ctx = &standby_db,
        .apply_fn = DB.applyHAReplicationRecordCallback,
    };
    var last_lsn = std.atomic.Value(u64).init(0);
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var primary_db = try DB.open(alloc, std.mem.span(primary_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SessionSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        },
        .start_index_workers = false,
    });
    defer primary_db.close();

    try primary_db.batch(.{
        .writes = &.{.{ .key = "doc:remote-apply", .value = "{\"title\":\"remote-apply\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());

    var found = (try standby_db.lookup(alloc, "doc:remote-apply", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"remote-apply\"}", found.json);
}

test "storage.ha db session sync wait remote write acknowledges durable receive despite apply failure" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var primary_db_path_buf: [256]u8 = undefined;
    const primary_db_path = TestHelpers.tempPath(&primary_db_path_buf);
    defer TestHelpers.cleanupTempDir(primary_db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 258,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, identity, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    const ApplyFailure = struct {
        calls: u64 = 0,

        fn apply(ctx: *anyopaque, _: ha_replication_record_mod.RecordView) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.IntentionalApplyFailure;
        }
    };

    var apply_failure = ApplyFailure{};
    var wait_state = SessionSyncWait{
        .alloc = alloc,
        .slot_name = "standby-a",
        .standby = &standby,
        .apply_ctx = &apply_failure,
        .apply_fn = ApplyFailure.apply,
    };
    var last_lsn = std.atomic.Value(u64).init(0);
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var primary_db = try DB.open(alloc, std.mem.span(primary_db_path), .{
        .identity_namespace = .{ .shard_id = 4, .table_id = 10 },
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .last_lsn = &last_lsn,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SessionSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        },
        .start_index_workers = false,
    });
    defer primary_db.close();

    try primary_db.batch(.{
        .writes = &.{.{ .key = "doc:remote-write", .value = "{\"title\":\"remote-write\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), apply_failure.calls);
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);
    try std.testing.expectEqualStrings("IntentionalApplyFailure", slot.last_error.?);

    var found = (try primary_db.lookup(alloc, "doc:remote-write", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"remote-write\"}", found.json);
}

test "storage.ha db primary progress sync wait observes reported remote apply ack" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 259,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const RemoteAck = struct {
        calls: usize = 0,

        fn poll(ctx: *anyopaque, primary_arg: *ha_primary_mod.Primary, target_lsn: u64, policy: ha_primary_mod.SyncPolicy, round: usize) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            try std.testing.expectEqual(ha_primary_mod.DurabilityMode.remote_apply, policy.mode);
            try std.testing.expectEqual(self.calls - 1, round);
            if (self.calls == 1) {
                try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, 0);
            } else {
                try primary_arg.standbyStatusUpdate("standby-a", primary_arg.identity.timeline_id, target_lsn, target_lsn);
            }
        }
    };

    var remote_ack = RemoteAck{};
    var wait_state = PrimaryProgressSyncWait{
        .max_rounds = 3,
        .poll_ctx = &remote_ack,
        .poll_fn = RemoteAck.poll,
    };
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = PrimaryProgressSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        },
        .start_index_workers = false,
    });
    defer db.close();

    try db.batch(.{
        .writes = &.{.{ .key = "doc:progress-wait", .value = "{\"title\":\"progress-wait\"}" }},
        .sync_level = .write,
    });

    try std.testing.expectEqual(@as(usize, 2), remote_ack.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.acknowledge), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 1), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 1), slot.applied_lsn);
}

test "storage.ha db primary progress sync wait returns would block without reported ack" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 260,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var wait_state = PrimaryProgressSyncWait{ .max_rounds = 1 };
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = PrimaryProgressSyncWait.wait,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HASyncCommitWouldBlock, db.batch(.{
        .writes = &.{.{ .key = "doc:progress-timeout", .value = "{\"title\":\"progress-timeout\"}" }},
        .sync_level = .write,
    }));

    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.wait_for_standby), gate_action.load(.acquire));
    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);
}

test "db transaction HA retry drains durable mirror outbox" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;
    var db_path_buf: [256]u8 = undefined;
    const db_path = tempPath(&db_path_buf);
    defer cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = tempPath(&ha_log_path_buf);
    defer cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = tempPath(&ha_slots_path_buf);
    defer cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 263,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);
    const AckOnRetry = struct {
        calls: usize = 0,
        fn wait(ctx: *anyopaque, active_primary: *ha_primary_mod.Primary, target_lsn: u64, _: ha_primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedMirrorWaitFailure;
            try active_primary.standbyStatusUpdate("standby-a", 1, target_lsn, target_lsn);
        }
    };
    var ack = AckOnRetry{};
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &ack,
            .sync_wait_fn = AckOnRetry.wait,
        },
        .start_index_workers = false,
    });
    defer db.close();

    const txn_id = try db.beginTransaction(20_000);
    try db.writeTransaction(txn_id, .{ .writes = &.{.{
        .key = "doc:ha-txn",
        .value = "{\"title\":\"committed\"}",
    }} });
    try std.testing.expectError(error.InjectedMirrorWaitFailure, db.commitTransaction(txn_id, 20_001));
    try std.testing.expectEqual(transactions_mod.TxnStatus.committed, try db.getTransactionStatus(txn_id));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    const configured_mirror = db.ha_async_batch_mirror;
    db.ha_async_batch_mirror = null;
    try std.testing.expectError(error.HAMirrorUnavailable, db.commitTransaction(txn_id, 20_001));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    db.ha_async_batch_mirror = configured_mirror;

    try db.commitTransaction(txn_id, 20_001);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
    try db.commitTransaction(txn_id, 20_001);
    try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
}

test "storage.ha db primary progress sync wait survives primary restart before ack" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    const identity = ha_primary_mod.Identity{
        .cluster_id = 261,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    const standby_names = [_][]const u8{"standby-a"};
    const policy = ha_primary_mod.SyncPolicy{
        .mode = .remote_apply,
        .standby_names = &standby_names,
        .failure_policy = .block,
    };

    var target_lsn: u64 = 0;
    {
        var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, identity, .{});
        defer primary.close();
        try primary.createSlot("standby-a", 0);

        var wait_state = PrimaryProgressSyncWait{ .max_rounds = 1 };
        var gate_lsn = std.atomic.Value(u64).init(0);
        var gate_action = std.atomic.Value(u8).init(255);
        var waits = std.atomic.Value(u64).init(0);
        var db = try DB.open(alloc, std.mem.span(db_path), .{
            .ha_async_batch_mirror = .{
                .primary = &primary,
                .sync_policy = policy,
                .sync_wait_ctx = &wait_state,
                .sync_wait_fn = PrimaryProgressSyncWait.wait,
                .last_gate_lsn = &gate_lsn,
                .last_gate_action = &gate_action,
                .sync_wait_count = &waits,
            },
            .start_index_workers = false,
        });
        defer db.close();

        try std.testing.expectError(error.HASyncCommitWouldBlock, db.batch(.{
            .writes = &.{.{ .key = "doc:restart-before-ack", .value = "{\"title\":\"restart-before-ack\"}" }},
            .sync_level = .write,
        }));
        target_lsn = primary.lastLsn();
        try std.testing.expectEqual(@as(u64, 1), target_lsn);
        try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
        try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.wait_for_standby), gate_action.load(.acquire));

        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(@as(u64, 0), slot.received_lsn);
        try std.testing.expectEqual(@as(u64, 0), slot.applied_lsn);
    }

    {
        var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, identity, .{});
        defer primary.close();
        try std.testing.expectEqual(target_lsn, primary.lastLsn());

        var wait_state = PrimaryProgressSyncWait{ .max_rounds = 1 };
        try std.testing.expectError(
            error.HASyncCommitWouldBlock,
            PrimaryProgressSyncWait.wait(&wait_state, &primary, target_lsn, policy),
        );

        try primary.standbyStatusUpdate("standby-a", identity.timeline_id, target_lsn, target_lsn);
        try PrimaryProgressSyncWait.wait(&wait_state, &primary, target_lsn, policy);

        const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
        try std.testing.expectEqual(target_lsn, slot.received_lsn);
        try std.testing.expectEqual(target_lsn, slot.applied_lsn);
    }
}

test "storage.ha db block sync policy surfaces wait provider errors" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 256,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    const SyncWait = struct {
        calls: u64 = 0,

        fn timeout(ctx: *anyopaque, _: *ha_primary_mod.Primary, _: u64, _: ha_primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.calls += 1;
            return error.HASyncCommitWaitTimeout;
        }
    };

    var wait_state = SyncWait{};
    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var waits = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{
                .mode = .remote_apply,
                .standby_names = &standby_names,
                .failure_policy = .block,
            },
            .sync_wait_ctx = &wait_state,
            .sync_wait_fn = SyncWait.timeout,
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_wait_count = &waits,
        },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HASyncCommitWaitTimeout, db.batch(.{
        .writes = &.{.{ .key = "doc:timeout", .value = "{\"title\":\"timeout\"}" }},
        .sync_level = .write,
    }));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), wait_state.calls);
    try std.testing.expectEqual(@as(u64, 1), waits.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.wait_for_standby), gate_action.load(.acquire));
}

test "storage.ha db fail-closed sync policy rejects before local batch commit" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 254,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var rejected = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .fail_closed,
            },
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_reject_count = &rejected,
        },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.SyncPolicyUnsatisfied, db.batch(.{
        .writes = &.{.{ .key = "doc:rejected", .value = "{\"title\":\"rejected\"}" }},
        .sync_level = .write,
    }));
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.reject), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), rejected.load(.acquire));
    try std.testing.expect((try db.lookup(alloc, "doc:rejected", .{})) == null);
}

test "storage.ha db fail-closed metadata sync policy rejects before local schema metadata commits" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);

    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, .{
        .cluster_id = 253,
        .shard_id = 5,
        .table_id = 11,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer primary.close();
    try primary.createSlot("standby-a", 0);

    var gate_lsn = std.atomic.Value(u64).init(0);
    var gate_action = std.atomic.Value(u8).init(255);
    var rejected = std.atomic.Value(u64).init(0);
    const standby_names = [_][]const u8{"standby-a"};
    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_async_metadata_mirror = .{
            .primary = &primary,
            .sync_policy = .{
                .mode = .remote_write,
                .standby_names = &standby_names,
                .failure_policy = .fail_closed,
            },
            .last_gate_lsn = &gate_lsn,
            .last_gate_action = &gate_action,
            .sync_reject_count = &rejected,
        },
        .start_index_workers = false,
    });
    defer db.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    try std.testing.expectError(error.SyncPolicyUnsatisfied, db.applyTableSchemaJson(alloc, schema_json, .{}));
    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.reject), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), rejected.load(.acquire));
    try std.testing.expect((try db.getSchemaJson(alloc)) == null);
    var durable_schema = try schema_mod.loadSchema(db.core.store, alloc);
    defer if (durable_schema) |loaded| schema_mod.freeSchema(alloc, loaded);
    try std.testing.expect(durable_schema == null);

    try std.testing.expectError(error.SyncPolicyUnsatisfied, db.applyLiteSqlTableRecord(alloc, .{
        .table_id = 42,
        .name = "usage_records",
        .database_name = "tenant_db",
        .namespace_name = "billing",
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
        .indexes_json = "{\"algebraic_index_v0\":{\"kind\":\"algebraic\",\"source\":\"lite_sql\"}}",
    }));

    try std.testing.expectEqual(@as(u64, 0), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 1), gate_lsn.load(.acquire));
    try std.testing.expectEqual(@intFromEnum(ha_commit_gate_mod.Action.reject), gate_action.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), rejected.load(.acquire));
    try std.testing.expect((try db.getSchemaJson(alloc)) == null);
    try std.testing.expect((try db.getLiteSqlTableRecordAlloc(alloc)) == null);

    durable_schema = try schema_mod.loadSchema(db.core.store, alloc);
    try std.testing.expect(durable_schema == null);
}

test "storage.ha db mirrors and applies schema metadata mutation records" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var primary_db_path_buf: [256]u8 = undefined;
    const primary_db_path = TestHelpers.tempPath(&primary_db_path_buf);
    defer TestHelpers.cleanupTempDir(primary_db_path);
    var standby_db_path_buf: [256]u8 = undefined;
    const standby_db_path = TestHelpers.tempPath(&standby_db_path_buf);
    defer TestHelpers.cleanupTempDir(standby_db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 252,
        .shard_id = 5,
        .table_id = 11,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary_identity = identity;
    primary_identity.shard_id = 50;
    primary_identity.table_id = 110;
    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, primary_identity, .{});
    defer primary.close();
    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    {
        var db = try DB.open(alloc, std.mem.span(primary_db_path), .{
            .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
            .ha_async_metadata_mirror = .{
                .primary = &primary,
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            },
            .start_index_workers = false,
        });
        defer db.close();

        const relational_columns = [_]schema_mod.RelationalColumn{.{
            .name = "id",
            .path = "id",
            .field_type = .keyword,
            .nullable = false,
        }};
        try db.setSchema(.{
            .version = 12,
            .default_type = "row",
            .ttl_duration_ns = 456,
            .ttl_field = "expires_at",
            .storage_mode = .relational,
            .relational_columns = relational_columns[0..],
        });
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .metadata_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 252), entry.record.cluster_id);
    try std.testing.expectEqual(@as(u64, 5), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 11), entry.record.table_id);

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = false,
    });
    defer standby_db.close();

    try std.testing.expectError(error.HAReadOnlyStandby, standby_db.setSchema(.{ .version = 99 }));
    try standby_db.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());

    const replicated_schema = (try schema_mod.loadSchema(standby_db.core.store, alloc)).?;
    defer schema_mod.freeSchema(alloc, replicated_schema);
    try std.testing.expectEqual(@as(u32, 12), replicated_schema.version);
    try std.testing.expectEqualStrings("row", replicated_schema.default_type);
    try std.testing.expectEqual(@as(u64, 456), replicated_schema.ttl_duration_ns);
    try std.testing.expectEqualStrings("expires_at", replicated_schema.ttl_field);
    try std.testing.expectEqual(schema_mod.StorageMode.relational, replicated_schema.storage_mode);
    try std.testing.expectEqual(@as(usize, 1), replicated_schema.relational_columns.len);
    try std.testing.expectEqualStrings("id", replicated_schema.relational_columns[0].name);
    try std.testing.expect(standby_db.async_context.relational_base_rows);

    try standby_db.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());
}

test "storage.ha db mirrors and applies local schema json metadata mutation records" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var primary_db_path_buf: [256]u8 = undefined;
    const primary_db_path = TestHelpers.tempPath(&primary_db_path_buf);
    defer TestHelpers.cleanupTempDir(primary_db_path);
    var standby_db_path_buf: [256]u8 = undefined;
    const standby_db_path = TestHelpers.tempPath(&standby_db_path_buf);
    defer TestHelpers.cleanupTempDir(standby_db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 253,
        .shard_id = 5,
        .table_id = 11,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary_identity = identity;
    primary_identity.shard_id = 50;
    primary_identity.table_id = 110;
    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, primary_identity, .{});
    defer primary.close();
    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;

    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    {
        var db = try DB.open(alloc, std.mem.span(primary_db_path), .{
            .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
            .ha_async_metadata_mirror = .{
                .primary = &primary,
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            },
            .start_index_workers = false,
        });
        defer db.close();

        try db.applyTableSchemaJson(alloc, schema_json, .{});
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .metadata_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 5), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 11), entry.record.table_id);

    var decoded = try ha_effects_mod.decodeMetadataMutation(alloc, entry.record);
    defer decoded.deinit();
    try std.testing.expectEqual(ha_effects_mod.MetadataMutationKind.schema, decoded.value.kind);
    try std.testing.expectEqualStrings(schema_json, decoded.value.local_schema_json orelse return error.TestUnexpectedResult);
    try std.testing.expect(decoded.value.lite_sql_table_record_json == null);

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = false,
    });
    defer standby_db.close();

    try standby_db.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());

    const replicated_schema = (try schema_mod.loadSchema(standby_db.core.store, alloc)).?;
    defer schema_mod.freeSchema(alloc, replicated_schema);
    try std.testing.expectEqual(@as(u32, 1), replicated_schema.version);
    try std.testing.expectEqualStrings("row", replicated_schema.default_type);

    const local_schema_json = (try standby_db.getSchemaJson(alloc)) orelse return error.TestUnexpectedResult;
    defer alloc.free(local_schema_json);
    try std.testing.expectEqualStrings(schema_json, local_schema_json);

    try standby_db.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());
}

test "storage.ha db mirrors and applies lite sql table metadata mutation records" {
    const DB = @import("mod.zig").DB;
    const metadata_table_manager = @import("../../metadata/table_manager.zig");
    const alloc = std.testing.allocator;

    var primary_db_path_buf: [256]u8 = undefined;
    const primary_db_path = TestHelpers.tempPath(&primary_db_path_buf);
    defer TestHelpers.cleanupTempDir(primary_db_path);
    var standby_db_path_buf: [256]u8 = undefined;
    const standby_db_path = TestHelpers.tempPath(&standby_db_path_buf);
    defer TestHelpers.cleanupTempDir(standby_db_path);
    var ha_log_path_buf: [256]u8 = undefined;
    const ha_log_path = TestHelpers.tempPath(&ha_log_path_buf);
    defer TestHelpers.cleanupTempDir(ha_log_path);
    var ha_slots_path_buf: [256]u8 = undefined;
    const ha_slots_path = TestHelpers.tempPath(&ha_slots_path_buf);
    defer TestHelpers.cleanupTempDir(ha_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 251,
        .shard_id = 5,
        .table_id = 11,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary_identity = identity;
    primary_identity.shard_id = 50;
    primary_identity.table_id = 110;
    var primary = try ha_primary_mod.Primary.open(alloc, ha_log_path, ha_slots_path, primary_identity, .{});
    defer primary.close();
    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();

    const schema_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","enforce_types":true,"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"keyword"},"status":{"type":"keyword"}},"required":["id"],"additionalProperties":false}}},"primary_key":{"columns":["id"]}}
    ;
    const indexes_json =
        \\{"algebraic_index_v0":{"kind":"algebraic","source":"lite_sql"}}
    ;

    var last_lsn = std.atomic.Value(u64).init(0);
    var failures = std.atomic.Value(u64).init(0);
    {
        var db = try DB.open(alloc, std.mem.span(primary_db_path), .{
            .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
            .ha_async_metadata_mirror = .{
                .primary = &primary,
                .last_lsn = &last_lsn,
                .failure_count = &failures,
            },
            .start_index_workers = false,
        });
        defer db.close();

        try db.applyLiteSqlTableRecord(alloc, .{
            .table_id = 42,
            .name = "usage_records",
            .database_name = "tenant_db",
            .namespace_name = "billing",
            .placement_role = "data",
            .desired_replica_count = 1,
            .schema_json = schema_json,
            .indexes_json = indexes_json,
        });
    }

    try std.testing.expectEqual(@as(u64, 1), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());

    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);
    try std.testing.expectEqual(@as(@TypeOf(entry.record.kind), .metadata_mutation), entry.record.kind);
    try std.testing.expectEqual(@as(u64, 5), entry.record.shard_id);
    try std.testing.expectEqual(@as(u64, 11), entry.record.table_id);

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .identity_namespace = .{ .shard_id = 5, .table_id = 11 },
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = false,
    });
    defer standby_db.close();

    try std.testing.expectError(error.HAReadOnlyStandby, standby_db.applyLiteSqlTableRecord(alloc, .{
        .table_id = 43,
        .name = "denied_records",
        .database_name = "tenant_db",
        .namespace_name = "billing",
        .placement_role = "data",
        .desired_replica_count = 1,
        .schema_json = schema_json,
        .indexes_json = indexes_json,
    }));

    try standby_db.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());

    const replicated_schema = (try schema_mod.loadSchema(standby_db.core.store, alloc)).?;
    defer schema_mod.freeSchema(alloc, replicated_schema);
    try std.testing.expectEqual(@as(u32, 1), replicated_schema.version);
    try std.testing.expectEqualStrings("row", replicated_schema.default_type);

    const local_schema_json = (try standby_db.getSchemaJson(alloc)) orelse return error.TestUnexpectedResult;
    defer alloc.free(local_schema_json);
    try std.testing.expectEqualStrings(schema_json, local_schema_json);

    const table_record = (try standby_db.getLiteSqlTableRecordAlloc(alloc)) orelse return error.TestUnexpectedResult;
    defer metadata_table_manager.freeTable(alloc, table_record);
    try std.testing.expectEqual(@as(u64, 42), table_record.table_id);
    try std.testing.expectEqualStrings("usage_records", table_record.name);
    try std.testing.expectEqualStrings("tenant_db", table_record.database_name);
    try std.testing.expectEqualStrings("billing", table_record.namespace_name);
    try std.testing.expectEqualStrings(schema_json, table_record.schema_json);
    try std.testing.expectEqualStrings(indexes_json, table_record.indexes_json);

    try standby_db.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try standby_db.haAppliedReplicationLsn());
}

test "storage.ha db applies batch mutation records through replication session callback" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var standby_db_path_buf: [256]u8 = undefined;
    const standby_db_path = TestHelpers.tempPath(&standby_db_path_buf);
    defer TestHelpers.cleanupTempDir(standby_db_path);
    var primary_log_path_buf: [256]u8 = undefined;
    const primary_log_path = TestHelpers.tempPath(&primary_log_path_buf);
    defer TestHelpers.cleanupTempDir(primary_log_path);
    var primary_slots_path_buf: [256]u8 = undefined;
    const primary_slots_path = TestHelpers.tempPath(&primary_slots_path_buf);
    defer TestHelpers.cleanupTempDir(primary_slots_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 251,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try ha_primary_mod.Primary.open(alloc, primary_log_path, primary_slots_path, identity, .{});
    defer primary.close();
    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, identity, .{});
    defer standby.close();
    try primary.createSlot("standby-a", 0);

    _ = try ha_effects_mod.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"replicated-session\"}" }},
        .sync_level = .full_index,
    }, .{});
    _ = try primary.append(.{
        .kind = .backup_start,
        .payload_codec = .json,
        .payload = "{\"manifest_id\":\"base-session\"}",
    });
    _ = try ha_effects_mod.appendDerivedChangeRecord(alloc, &primary, .{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    }, .{});

    var standby_db = try DB.open(alloc, std.mem.span(standby_db_path), .{
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = false,
    });
    defer standby_db.close();

    const result = try ha_session_mod.replicateAvailable(
        alloc,
        &primary,
        "standby-a",
        &standby,
        &standby_db,
        DB.applyHAReplicationRecordCallback,
    );
    try std.testing.expectEqual(@as(usize, 3), result.received_count);
    try std.testing.expectEqual(@as(usize, 3), result.applied_count);
    try std.testing.expectEqual(@as(u64, 3), result.progress.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), result.progress.applied_lsn);

    const slot = primary.slot("standby-a") orelse return error.TestExpectedEqual;
    try std.testing.expectEqual(@as(u64, 3), slot.received_lsn);
    try std.testing.expectEqual(@as(u64, 3), slot.applied_lsn);
    try std.testing.expectEqual(@as(u64, 3), try standby_db.haAppliedReplicationLsn());

    var found = (try standby_db.lookup(alloc, "doc:a", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"replicated-session\"}", found.json);

    const replay_entries = try replay_stream_mod.iterateFrom(alloc, standby_db.core.store, 1);
    defer {
        for (replay_entries) |*entry| entry.deinit(alloc);
        alloc.free(replay_entries);
    }
    try std.testing.expectEqual(@as(usize, 2), replay_entries.len);
    try std.testing.expectEqual(@as(u64, 1), replay_entries[0].sequence);
    try std.testing.expectEqual(@as(u64, 2), replay_entries[1].sequence);

    var replicated_effect_record = try change_journal_mod.decodeRecord(alloc, replay_entries[1].payload);
    defer replicated_effect_record.deinit();
    try std.testing.expectEqual(@as(u64, 2), replicated_effect_record.record.sequence);
    try std.testing.expectEqualStrings("doc:a", replicated_effect_record.record.changed_doc_keys[0]);
    try std.testing.expectEqual(@as(usize, 1), replicated_effect_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.full_text, replicated_effect_record.record.target_hints[0]);

    var duplicate_batch = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer duplicate_batch.deinit(alloc);
    try standby_db.applyHAReplicationRecord(duplicate_batch.record);
    var duplicate_derived = (try primary.log.entryAt(alloc, 3)) orelse return error.TestExpectedEqual;
    defer duplicate_derived.deinit(alloc);
    try standby_db.applyHAReplicationRecord(duplicate_derived.record);
    try std.testing.expectEqual(@as(u64, 3), try standby_db.haAppliedReplicationLsn());

    const replay_after_duplicates = try replay_stream_mod.iterateFrom(alloc, standby_db.core.store, 1);
    defer {
        for (replay_after_duplicates) |*entry| entry.deinit(alloc);
        alloc.free(replay_after_duplicates);
    }
    try std.testing.expectEqual(@as(usize, 2), replay_after_duplicates.len);
}

test "storage.ha db persists applied replication marker across reopen" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var primary_log_path_buf: [256]u8 = undefined;
    const primary_log_path = TestHelpers.tempPath(&primary_log_path_buf);
    defer TestHelpers.cleanupTempDir(primary_log_path);
    var primary_slots_path_buf: [256]u8 = undefined;
    const primary_slots_path = TestHelpers.tempPath(&primary_slots_path_buf);
    defer TestHelpers.cleanupTempDir(primary_slots_path);

    const identity = ha_standby_mod.Identity{
        .cluster_id = 252,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try ha_primary_mod.Primary.open(alloc, primary_log_path, primary_slots_path, identity, .{});
    defer primary.close();

    _ = try ha_effects_mod.appendBatchMutationRequest(alloc, &primary, .{
        .writes = &.{.{ .key = "doc:persisted-marker", .value = "{\"title\":\"persisted\"}" }},
        .sync_level = .write,
    }, .{});
    var entry = (try primary.log.entryAt(alloc, 1)) orelse return error.TestExpectedEqual;
    defer entry.deinit(alloc);

    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{ .start_index_workers = false });
        defer db.close();
        try db.applyHAReplicationRecord(entry.record);
        try std.testing.expectEqual(@as(u64, 1), try db.haAppliedReplicationLsn());
    }

    var reopened = try DB.open(alloc, std.mem.span(db_path), .{ .start_index_workers = false });
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 1), try reopened.haAppliedReplicationLsn());
    try reopened.applyHAReplicationRecord(entry.record);
    try std.testing.expectEqual(@as(u64, 1), try reopened.haAppliedReplicationLsn());

    const replay_entries = try replay_stream_mod.iterateFrom(alloc, reopened.core.store, 1);
    defer {
        for (replay_entries) |*replay_entry| replay_entry.deinit(alloc);
        alloc.free(replay_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), replay_entries.len);
    try std.testing.expectEqual(@as(u64, 1), replay_entries[0].sequence);
}

test "storage.ha db applies timeline switch as durable replication boundary" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);

    const switch_record = ha_replication_record_mod.RecordView{
        .kind = .timeline_switch,
        .payload_codec = .json,
        .cluster_id = 252,
        .shard_id = 4,
        .table_id = 10,
        .timeline_id = 2,
        .epoch = 2,
        .lsn = 7,
        .previous_lsn = 6,
        .payload =
        \\{"parent_timeline_id":1,"new_timeline_id":2,"parent_epoch":1,"new_epoch":2,"required_lsn":6,"forced":false,"data_loss_possible":false}
        ,
    };

    {
        var db = try DB.open(alloc, std.mem.span(db_path), .{ .start_index_workers = false });
        defer db.close();
        try db.applyHAReplicationRecord(switch_record);
        try std.testing.expectEqual(@as(u64, 7), try db.haAppliedReplicationLsn());
    }

    var reopened = try DB.open(alloc, std.mem.span(db_path), .{ .start_index_workers = false });
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 7), try reopened.haAppliedReplicationLsn());
    try reopened.applyHAReplicationRecord(switch_record);
    try std.testing.expectEqual(@as(u64, 7), try reopened.haAppliedReplicationLsn());
}

test "storage.ha db write gate rejects client writes on standby but allows replicated apply" {
    const db_mod = @import("mod.zig");
    const DB = db_mod.DB;
    const BatchProfile = db_mod.BatchProfile;
    const DocumentArtifactChildRangeDispatch = db_mod.DocumentArtifactChildRangeDispatch;
    const DocumentArtifactChildRangeDispatcher = db_mod.DocumentArtifactChildRangeDispatcher;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, .{
        .cluster_id = 300,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = false,
    });
    defer db.close();

    const NoopDocumentArtifactChildRangeDispatcher = struct {
        fn apply(_: *anyopaque, _: Allocator, _: DocumentArtifactChildRangeDispatch) anyerror!void {}
    };
    var noop_dispatcher_state: u8 = 0;
    const noop_dispatcher = DocumentArtifactChildRangeDispatcher{
        .ptr = &noop_dispatcher_state,
        .apply = NoopDocumentArtifactChildRangeDispatcher.apply,
    };
    var blocked_profile = BatchProfile{};

    try std.testing.expectError(error.HAReadOnlyStandby, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"client\"}" }},
    }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.batchProfiled(.{
        .writes = &.{.{ .key = "doc:profiled", .value = "{\"title\":\"client\"}" }},
    }, &blocked_profile));
    try std.testing.expectError(error.HAReadOnlyStandby, db.batchWithDocumentArtifactChildRangeDispatcher(.{
        .writes = &.{.{ .key = "doc:dispatch", .value = "{\"title\":\"client\"}" }},
    }, noop_dispatcher));
    try std.testing.expectError(error.HAReadOnlyStandby, db.batchWithoutRangeValidation(.{
        .writes = &.{.{ .key = "doc:norange", .value = "{\"title\":\"client\"}" }},
    }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.applyDocumentArtifactChildRangeBatch(.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginBulkIngestSession());
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginDenseAutoBulkIngestSession());
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginPrimaryStoreAutoBulkIngestSession());
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishDenseAutoBulkIngestSessionWithOptionsAndNotifyExecutor(.{}, false),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishDenseAutoBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.rollDenseAutoBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.finishPrimaryStoreAutoBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.rollPrimaryStoreAutoBulkIngestSessionWithOptions(.{}),
    );
    try std.testing.expectError(error.HAReadOnlyStandby, db.drainDocumentArtifactChildRangeOutbox(noop_dispatcher, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.updateDocumentArtifactChildRangePlacement(alloc, "doc:a", "asset", @as(types.DocumentArtifactChildRangePlacementUpdate, undefined)));
    try std.testing.expectError(error.HAReadOnlyStandby, db.reprocessDocumentArtifact(alloc, "doc:a", "asset"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.reprocessDocumentArtifactRange(alloc, "asset", @as(types.DocumentArtifactTableReprocessRequest, undefined)));
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.updateRange(.{ .start = "doc:a", .end = "doc:z" }),
    );
    try std.testing.expectError(error.HAReadOnlyStandby, db.setSplitState(null));
    try std.testing.expectError(error.HAReadOnlyStandby, db.clearSplitState());
    try std.testing.expectError(error.HAReadOnlyStandby, db.setSplitDeltaFinalSeq(1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.clearSplitDeltaFinalSeq());
    try std.testing.expectError(error.HAReadOnlyStandby, db.clearSplitDeltaEntries());
    try std.testing.expectError(error.HAReadOnlyStandby, db.createShadowIndexManager("doc:m", "doc:z"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.closeShadowIndexManager());
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.split(.{ .start = "doc:a", .end = "doc:z" }, "doc:m", "dest1", "dest2", true),
    );
    try std.testing.expectError(error.HAReadOnlyStandby, db.finalizeSplit(.{ .start = "doc:a", .end = "doc:m" }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.snapshot("blocked"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.sync(false));
    try std.testing.expectError(error.HAReadOnlyStandby, db.syncIndexes(false));
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairRestoreRuntimeStateStepIfNeeded(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairRestoreRuntimeStateIfNeeded(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricMaintenanceForIdle());
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.refreshGraphMetric(alloc, "graph_idx", "manual_degree"),
    );
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedMaintenanceForIdle(.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricServiceMaintenanceJsonAlloc(alloc, "{}"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildGraphMetric(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.deleteGraphMetricMaterialization(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.pauseGraphMetricMaintenance(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.resumeGraphMetricMaintenance(alloc, "graph_idx", "manual_degree"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.ensureGraphMetricPlannedBuild(alloc, "graph_idx", "manual_degree", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedWorkerPageStep("graph_idx", "manual_degree", "worker-a"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedWorkerPageStepAt("graph_idx", "manual_degree", "worker-a", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedCoordinatorStep("graph_idx", "manual_degree"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedCoordinatorStepAt("graph_idx", "manual_degree", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.failGraphMetricPlannedBuild(alloc, "graph_idx", "manual_degree", error.TestExpectedError));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedDrain(alloc, "graph_idx", "manual_degree", 1, .{ .worker_ids = &.{"worker-a"} }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedCoordinatorSweep(.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runGraphMetricPlannedWorkerSweep(.{ .worker_id = "worker-a" }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runLsmMaintenanceStep());
    try std.testing.expectError(error.HAReadOnlyStandby, db.retryQuarantinedIndexLoads(true));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runUntilIdle());
    try std.testing.expectError(error.HAReadOnlyStandby, db.evaluateAlgebraicAdaptiveCandidates());
    try std.testing.expectError(error.HAReadOnlyStandby, db.setSchema(.{ .version = 99 }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.applyTableSchemaJson(alloc, "{\"version\":99}", .{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.setSchemaJson(alloc, "{\"version\":99}"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.reloadAlgebraicSchemaConfigs("{\"version\":99}"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.schemaRuntimeStageAlgebraicSchemaConfigsPending("{\"version\":99}"));
    const metadata_table_manager = @import("../../metadata/table_manager.zig");
    try std.testing.expectError(error.HAReadOnlyStandby, db.applyLiteSqlTableRecord(alloc, .{
        .table_id = 99,
        .name = "blocked",
        .schema_json = "{\"version\":99}",
    }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.executeClaimedSchemaRewriteJob(alloc, @as(metadata_table_manager.SchemaRewriteJobRecord, undefined)));
    try std.testing.expectError(error.HAReadOnlyStandby, db.addIndex(@as(types.IndexConfig, undefined)));
    try std.testing.expectError(error.HAReadOnlyStandby, db.addEnrichment(@as(types.EnrichmentConfig, undefined)));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertEnrichment(@as(types.EnrichmentConfig, undefined)));
    try std.testing.expectError(error.HAReadOnlyStandby, db.drainResolverBackfill());
    try std.testing.expectError(error.HAReadOnlyStandby, db.compactTextIndexes());
    try std.testing.expectError(error.HAReadOnlyStandby, db.drainScheduledTextMerges());
    try std.testing.expectError(error.HAReadOnlyStandby, db.forceCompactTextIndexes());
    try std.testing.expectError(error.HAReadOnlyStandby, db.bestEffortForceCompactTextIndexes());
    try std.testing.expectError(error.HAReadOnlyStandby, db.deleteIndex("missing"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.deleteEnrichment(.asset, "missing"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.removeResolver("missing"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rewriteEntityEdges(alloc, "missing", "a", "b"));
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.rebuildRelationalSecondaryIndexInRange("missing", 0, "doc:a", "doc:z"),
    );
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairForeignKeyRefsInRange("doc:a", "doc:z"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairForeignKeyRefsInRangeForConstraint("fk_parent", "doc:a", "doc:z"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairUniqueConstraintRowsInRange("doc:a", "doc:z"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairForeignKeyRefOwnerForParent("fk_parent", "parent", "p1"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.repairForeignKeyRefOwnerRange("fk_parent", "parent", "p1", "p9"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimForeignKeyIntegrityWorkUnit("claim-a", "worker-a", 1, "scan", "repair", null, "doc:a", "doc:z", 1000));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimForeignKeyIntegrityWorkUnitAt("claim-at", "worker-a", 1, "scan", "repair", null, "doc:a", "doc:z", 1000, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertForeignKeyIntegrityJobRecord("job-a", "child", "repair", "worker-a", null, "doc:a", "doc:z", 1000, 10, "running"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertForeignKeyIntegrityJobRecordAt("job-at", "child", "repair", "worker-a", null, "doc:a", "doc:z", 1000, 10, "running", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertRelationalIndexRepairJobRecord("repair-a", "default", "public", "docs", "worker-a", "doc:a", "doc:z", 1000, 10, "running"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertRelationalIndexRepairJobRecordAt("repair-at", "default", "public", "docs", "worker-a", "doc:a", "doc:z", 1000, 10, "running", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.recordRelationalIndexRepairJobPass("repair-a", "running", false, 1, 1, 0, "doc:m", .{}, null));
    try std.testing.expectError(error.HAReadOnlyStandby, db.recordRelationalIndexRepairJobPassAt("repair-at", "complete", true, 1, 1, 0, "", .{}, null, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertRelationalIndexRepairJobTargetAt("repair-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", "", 1000, 10, "running", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.recordRelationalIndexRepairJobTargetPassAt("repair-target", "complete", true, "", 1, 1, 0, 1, null, false, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runRelationalIndexRepairJobPageAt("repair-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", 1000, 10, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleRelationalIndexRepairJobPageAt("repair-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", 1000, 10, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.upsertRelationalIndexDropJobRecordAt("drop-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", "", 1000, 10, "running", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.recordRelationalIndexDropJobPassAt("drop-target", "worker-a", 1, "complete", true, "", 1, 1, 0, 1, null, false, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleRelationalIndexDropJob("drop-target", "default", "public", "docs", "text_search", "idx", 1, "worker-a", 1000, 10));
    try std.testing.expectError(error.HAReadOnlyStandby, db.discoverRelationalIndexDropJobs("docs"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.completeForeignKeyIntegrityJobRecord("job-a", "complete", true, .{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.completeForeignKeyIntegrityJobRecordWithDiagnostics("job-a", "complete", true, .{}, "[]", 0, false));
    try std.testing.expectError(error.HAReadOnlyStandby, db.completeForeignKeyIntegrityJobRecordAt("job-at", "complete", true, .{}, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.completeForeignKeyIntegrityJobRecordWithDiagnosticsAt("job-at", "complete", true, .{}, "[]", 0, false, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.updateForeignKeyIntegrityJobDiagnostics("job-a", "[]", 0, false));
    try std.testing.expectError(error.HAReadOnlyStandby, db.updateForeignKeyIntegrityJobDiagnosticsWithReport("job-a", .{}, "[]", 0, false));
    try std.testing.expectError(error.HAReadOnlyStandby, db.updateForeignKeyIntegrityJobDiagnosticsAt("job-at", "[]", 0, false, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.updateForeignKeyIntegrityJobDiagnosticsWithReportAt("job-at", .{}, "[]", 0, false, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleForeignKeyActionJob("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleForeignKeyActionJobWithUpdatedParentKey("action-b", "cascade", "worker-a", "fk_parent", "parent", "p1", "p2", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleForeignKeyActionJobAt("action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.requeueForeignKeyActionJob("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.requeueForeignKeyActionJobWithUpdatedParentKey("action-b", "cascade", "worker-a", "fk_parent", "parent", "p1", "p2", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.requeueForeignKeyActionJobAt("action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleForeignKeyActionSchedule("schedule-a", "action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleForeignKeyActionScheduleWithUpdatedParentKey("schedule-b", "action-b", "cascade", "worker-a", "fk_parent", "parent", "p1", "p2", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.scheduleForeignKeyActionScheduleAt("schedule-c", "action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.requeueForeignKeyActionSchedule("schedule-a", "action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.requeueForeignKeyActionScheduleAt("schedule-c", "action-c", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.markForeignKeyActionScheduleSeeded("schedule-a", 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.markForeignKeyActionScheduleSeededAt("schedule-a", 1, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimAndRunForeignKeyActionJobPage("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimAndRunForeignKeyActionJobPageAt("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimForeignKeyActionJobPage("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimForeignKeyActionJobPageAt("action-a", "cascade", "worker-a", "fk_parent", "parent", "p1", 16, 1000, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.finishClaimedForeignKeyActionJobPage(@as(DB.ForeignKeyActionJobRecord, undefined), 0, false, null, null, null));
    try std.testing.expectError(error.HAReadOnlyStandby, db.finishClaimedForeignKeyActionJobPageAt(@as(DB.ForeignKeyActionJobRecord, undefined), 0, false, null, null, null, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimAndRunForeignKeyIntegrityWorkUnit("claim-a", "worker-a", 1, "scan", .repair, null, "doc:a", "doc:z", 1000));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimAndRunForeignKeyIntegrityWorkUnitAt("claim-at", "worker-a", 1, "scan", .repair, null, "doc:a", "doc:z", 1000, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.catchUpPendingDerivedReplay());
    const NoopReplayProgress = struct {
        fn hook(_: *anyopaque, _: []const u8, _: db_mod.ReplayProgress) anyerror!void {}
    };
    try std.testing.expectError(error.HAReadOnlyStandby, db.catchUpPendingDerivedReplayWithProgress(&noop_dispatcher_state, NoopReplayProgress.hook));
    try std.testing.expectError(error.HAReadOnlyStandby, db.derivedAsyncAppendDerivedBatchRecord(.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildDenseIndexesForTargetCoverage(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildSparseIndexesForTargetCoverage(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildGraphIndexesForTargetCoverage(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runDensePostingMaintenanceForIdle());
    try std.testing.expectError(error.HAReadOnlyStandby, db.runDensePostingMaintenanceForIdleBestEffort());
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildDenseIndexesFromStoredEmbeddingArtifacts(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeededWithProgress(alloc, null, null));
    try std.testing.expectError(error.HAReadOnlyStandby, db.derivedAsyncRebuildDenseIndexesFromStoredEmbeddingArtifactsResumeWithProgress(alloc, null, null, null, null, null, null, null, 16, 16));
    try std.testing.expectError(error.HAReadOnlyStandby, db.replayGeneratedEnrichmentsFromStoredDocs(alloc));
    try std.testing.expectError(error.HAReadOnlyStandby, db.ensureGroupCreatedAtMillis(alloc, 42, 1234));
    try std.testing.expectError(error.HAReadOnlyStandby, db.runMaintenanceUntilTargets(1, &.{"idx"}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.mutateRelationalRowsFromSource(alloc, .{}, .{ .kind = .update }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.stagePlannedRelationalRowsMutationSourceAlloc(alloc, .{}, .{ .kind = .update }, 0, &.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.mutateRelationalRowsJoinedSourceAlloc(alloc, .{}, .{ .kind = .update }));
    try std.testing.expectError(error.HAReadOnlyStandby, db.stagePlannedRelationalRowsJoinedMutationSourceAlloc(alloc, .{}, .{ .kind = .update }, 0, &.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.stagePlannedRelationalRowsJoinedMutationSourceWithSourceSchemaAlloc(alloc, .{}, .{}, .{ .kind = .update }, 0, &.{}));
    const blocked_txn: transactions_mod.TxnId = .{ 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42, 42 };
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginTransaction(1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginTransactionWithId(blocked_txn, 1));
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginTransactionWithParticipants(1, &.{"remote"}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.beginTransactionWithIdAndParticipants(blocked_txn, 1, &.{"remote"}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.writeIntents(blocked_txn, &.{}, &.{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.writeTransaction(blocked_txn, .{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.claimRowsForTransaction(blocked_txn, &.{"row:a"}, .{}));
    try std.testing.expectError(error.HAReadOnlyStandby, db.commitTransaction(blocked_txn, 2));
    try std.testing.expectError(error.HAReadOnlyStandby, db.resolveTransactionIntents(blocked_txn, .committed, 3));
    try std.testing.expectError(error.HAReadOnlyStandby, db.abortTransaction(blocked_txn, 4));
    try std.testing.expectError(error.HAReadOnlyStandby, db.markTransactionParticipantResolved(blocked_txn, "remote"));
    try std.testing.expectError(error.HAReadOnlyStandby, db.recoverTransactions(5, 6));
    try std.testing.expectError(
        error.HAReadOnlyStandby,
        db.reassignIdentityNamespaceForInternalTransition(.{ .table_id = 300, .shard_id = 1, .range_id = 1 }),
    );

    try db.batchReplicatedApply(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"replicated\"}" }},
    });
    var found = (try db.lookup(alloc, "doc:a", .{})) orelse return error.TestExpectedEqual;
    defer found.deinit(alloc);
    try std.testing.expectEqualStrings("{\"title\":\"replicated\"}", found.json);
}

test "storage.ha db write gate rejects fenced former primary writes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var primary_log_path_buf: [256]u8 = undefined;
    const primary_log_path = TestHelpers.tempPath(&primary_log_path_buf);
    defer TestHelpers.cleanupTempDir(primary_log_path);
    var primary_slots_path_buf: [256]u8 = undefined;
    const primary_slots_path = TestHelpers.tempPath(&primary_slots_path_buf);
    defer TestHelpers.cleanupTempDir(primary_slots_path);
    var fence_path_buf: [256]u8 = undefined;
    const fence_path = TestHelpers.tempPath(&fence_path_buf);
    defer TestHelpers.cleanupTempDir(fence_path);

    const identity = ha_primary_mod.Identity{
        .cluster_id = 301,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    };
    var primary = try ha_primary_mod.Primary.open(alloc, primary_log_path, primary_slots_path, identity, .{});
    defer primary.close();
    _ = try primary.append(.{ .payload = "before-fence" });

    var fence_store = try ha_fencing_mod.Store.open(alloc, fence_path, .{});
    defer fence_store.close();
    const receipt = try fence_store.acquirePromotionFence(.{
        .identity = identity,
        .old_primary_id = "primary-a",
        .promoted_node_id = "standby-a",
        .new_timeline_id = 2,
        .new_epoch = 2,
        .required_lsn = 1,
        .observed_lsn = 1,
        .reason = "db-write-gate-test",
    });
    defer ha_fencing_mod.freeReceipt(alloc, receipt);

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_write_gate = .{ .fenced_primary = .{
            .primary = &primary,
            .fence_store = &fence_store,
            .node_id = "primary-a",
        } },
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expectError(error.HAFencedPrimary, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"blocked\"}" }},
    }));

    const gate = db.ha_write_gate orelse return error.TestExpectedEqual;
    switch (gate) {
        .fenced_primary => |fenced| {
            const decision = try ha_write_gate_mod.evaluateFencedPrimary(fenced, .{});
            try std.testing.expectEqual(ha_write_gate_mod.Action.reject_fenced_primary, decision.action);
        },
        else => return error.TestExpectedEqual,
    }
}

test "storage.ha db standby role suppresses mutating background runtimes" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var db_path_buf: [256]u8 = undefined;
    const db_path = TestHelpers.tempPath(&db_path_buf);
    defer TestHelpers.cleanupTempDir(db_path);
    var standby_log_path_buf: [256]u8 = undefined;
    const standby_log_path = TestHelpers.tempPath(&standby_log_path_buf);
    defer TestHelpers.cleanupTempDir(standby_log_path);
    var standby_progress_path_buf: [256]u8 = undefined;
    const standby_progress_path = TestHelpers.tempPath(&standby_progress_path_buf);
    defer TestHelpers.cleanupTempDir(standby_progress_path);

    var standby = try ha_standby_mod.Standby.open(alloc, standby_log_path, standby_progress_path, .{
        .cluster_id = 301,
        .shard_id = 0,
        .table_id = 0,
        .timeline_id = 1,
        .epoch = 1,
    }, .{});
    defer standby.close();

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .ha_write_gate = .{ .standby = &standby },
        .start_index_workers = true,
        .start_optional_runtimes = true,
        .enrichment = .{ .enable_without_producers = true },
        .ttl_cleanup = .{ .enabled = true },
        .transaction_recovery = .{ .enabled = true },
        .text_merge = .{ .enabled = true },
        .sparse_compaction = .{ .enabled = true },
    });
    defer db.close();

    try std.testing.expect(!db.start_index_workers);
    try std.testing.expect(!db.executor.hasWorkers());
    try std.testing.expect(db.enrichment_runtime == null);
    try std.testing.expect(db.resolution_runtime == null);
    try std.testing.expect(db.promotion_runtime == null);
    try std.testing.expect(db.ttl_runtime == null);
    try std.testing.expect(db.transaction_runtime == null);
    try std.testing.expect(db.text_merge_runtime == null);
    try std.testing.expect(db.sparse_compaction_runtime == null);
}

pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn enforceDurableMutationGate(self: *DB) !void {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            try enforceDBWriteGate(self);
        }

        pub fn enforceDBWriteGate(self: *DB) !void {
            try enforceWriteGateOptional(self.ha_write_gate);
        }

        pub fn mirrorDBReplayPayloadBestEffort(self: *DB, payload: []const u8) void {
            const resources = self.core.batchExecutionResources();
            mirrorReplayPayloadBestEffort(resources.log_mutex, resources.identity_namespace, self.ha_async_effect_mirror, payload);
        }

        pub fn mirrorDBBatchMutationBestEffort(self: *DB, request: types.BatchRequest) void {
            const resources = self.core.batchExecutionResources();
            mirrorBatchMutationBestEffort(self.alloc, resources.log_mutex, resources.identity_namespace, self.ha_async_batch_mirror, request);
        }

        pub fn mirrorDBSchemaMetadataBestEffort(self: *DB, table_schema: schema_mod.TableSchema) void {
            const resources = self.core.batchExecutionResources();
            mirrorSchemaMetadataBestEffort(self.alloc, resources.log_mutex, resources.identity_namespace, self.ha_async_metadata_mirror, table_schema);
        }

        pub fn preflightDBBatchSyncCommit(self: *DB) !void {
            const resources = self.core.batchExecutionResources();
            try preflightMirrorSyncCommit(resources.log_mutex, self.ha_async_batch_mirror);
            try preflightMirrorSyncCommit(resources.log_mutex, self.ha_async_effect_mirror);
        }

        pub fn preflightDBMetadataSyncCommit(self: *DB) !void {
            const resources = self.core.batchExecutionResources();
            try preflightMirrorSyncCommit(resources.log_mutex, self.ha_async_metadata_mirror);
        }

        pub fn mirrorDBBatchMutationCommit(self: *DB, request: types.BatchRequest) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorBatchMutationCommit(self.alloc, resources.log_mutex, resources.identity_namespace, self.ha_async_batch_mirror, request);
        }

        pub fn mirrorHAEncodedBatchMutationCommit(self: *DB, payload: []const u8) !void {
            var ctx = self.batchContext();
            try mirrorHAEncodedBatchMutationCommitContext(&ctx, payload);
        }

        pub fn mirrorDBReplayPayloadCommit(self: *DB, payload: []const u8) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorReplayPayloadCommit(resources.log_mutex, resources.identity_namespace, self.ha_async_effect_mirror, payload);
        }

        pub fn flushTransactionHAOutbox(self: *DB, txn_id: transactions_mod.TxnId) !void {
            var outbox = try self.core.loadTransactionHAOutbox(self.alloc, txn_id);
            defer outbox.deinit(self.alloc);
            if (outbox.batch_payload == null and outbox.replay_payload == null) return;

            // An outbox entry is a durable synchronous-replication obligation. Do
            // not silently discard it if a restart temporarily removes or
            // downgrades the corresponding mirror configuration.
            if (outbox.batch_payload != null) {
                const mirror = self.ha_async_batch_mirror orelse return error.HAMirrorUnavailable;
                if (!haMirrorSyncEnabled(mirror)) return error.HAMirrorUnavailable;
            }
            if (outbox.replay_payload != null) {
                const mirror = self.ha_async_effect_mirror orelse return error.HAMirrorUnavailable;
                if (!haMirrorSyncEnabled(mirror)) return error.HAMirrorUnavailable;
            }
            try Self.enforceDBWriteGate(self);
            const ctx = self.batchContext();
            if (outbox.batch_payload != null) try preflightMirrorSyncCommit(ctx.log_mutex, ctx.ha_async_batch_mirror);
            if (outbox.replay_payload != null) try preflightMirrorSyncCommit(ctx.log_mutex, ctx.ha_async_effect_mirror);

            if (outbox.batch_payload) |payload| {
                try Self.mirrorHAEncodedBatchMutationCommit(self, payload);
                self.core.lockApply();
                defer self.core.unlockApply();
                try self.core.clearTransactionHAOutbox(txn_id, .batch);
            }
            if (outbox.replay_payload) |payload| {
                try Self.mirrorDBReplayPayloadCommit(self, payload);
                self.core.lockApply();
                defer self.core.unlockApply();
                try self.core.clearTransactionHAOutbox(txn_id, .replay);
            }
        }

        pub fn mirrorDBSchemaMetadataCommit(self: *DB, table_schema: schema_mod.TableSchema) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorSchemaMetadataCommit(self.alloc, resources.log_mutex, resources.identity_namespace, self.ha_async_metadata_mirror, table_schema);
        }

        pub fn mirrorDBSchemaJsonMetadataCommit(
            self: *DB,
            table_schema: schema_mod.TableSchema,
            schema_json: []const u8,
        ) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorSchemaJsonMetadataCommit(self.alloc, resources.log_mutex, resources.identity_namespace, self.ha_async_metadata_mirror, table_schema, schema_json);
        }

        pub fn mirrorDBLiteSqlTableMetadataCommit(
            self: *DB,
            table_schema: schema_mod.TableSchema,
            schema_json: []const u8,
            table_record_json: []const u8,
        ) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorLiteSqlTableMetadataCommit(
                self.alloc,
                resources.log_mutex,
                resources.identity_namespace,
                self.ha_async_metadata_mirror,
                table_schema,
                schema_json,
                table_record_json,
            );
        }

        pub fn appliedReplicationLsn(self: *DB) anyerror!u64 {
            return try readAppliedReplicationLsn(self.alloc, self.core.store);
        }

        pub fn replicationRecordAlreadyApplied(self: *DB, record: ha_replication_record_mod.RecordView) !bool {
            if (record.lsn == 0) return false;
            return (try Self.appliedReplicationLsn(self)) >= record.lsn;
        }

        pub fn markReplicationRecordApplied(self: *DB, lsn: u64) !void {
            if (lsn == 0) return;
            const current = try Self.appliedReplicationLsn(self);
            if (current >= lsn) return;
            var value_buf: [applied_lsn_value_len]u8 = undefined;
            const marker = appliedReplicationLsnWrite(lsn, &value_buf);
            try self.core.store.putBatch(&.{marker}, &.{});
        }

        pub fn setSchemaReplicatedApplyWithMarker(self: *DB, table_schema: schema_mod.TableSchema, applied_lsn_marker: ?u64) anyerror!void {
            try DB.HAReplicationCallbacks.set_schema_replicated_apply(self, table_schema);
            if (applied_lsn_marker) |lsn| try Self.markReplicationRecordApplied(self, lsn);
        }

        pub fn applyDerivedEffectRecord(self: *DB, record: ha_replication_record_mod.RecordView) anyerror!u64 {
            var ctx = self.batchContext();
            return try DB.HAReplicationCallbacks.append_replicated_ha_derived_effect_context(&ctx, record);
        }

        pub fn applyReplicationRecord(self: *DB, record: ha_replication_record_mod.RecordView) anyerror!void {
            if (try Self.replicationRecordAlreadyApplied(self, record)) return;

            switch (record.kind) {
                .batch_mutation => {
                    var decoded = try ha_effects_mod.decodeBatchMutationRequest(self.alloc, record);
                    defer decoded.deinit();
                    try DB.HAReplicationCallbacks.batch_replicated_apply_with_marker(self, decoded.value.request, record.lsn);
                },
                .metadata_mutation => {
                    var decoded = try ha_effects_mod.decodeMetadataMutation(self.alloc, record);
                    defer decoded.deinit();
                    if (decoded.value.kind != .schema) return error.UnsupportedMetadataMutationKind;
                    const decoded_schema = try schema_mod.deserializeSchema(self.alloc, decoded.value.schema_bytes);
                    defer schema_mod.freeSchema(self.alloc, decoded_schema);
                    if (decoded.value.lite_sql_table_record_json) |table_record_json| {
                        const schema_json = decoded.value.local_schema_json orelse return error.InvalidMetadataMutationPayload;
                        try DB.HAReplicationCallbacks.set_schema_with_local_lite_sql_table_record_json_replicated_apply(
                            self,
                            decoded_schema,
                            schema_json,
                            table_record_json,
                        );
                        try Self.markReplicationRecordApplied(self, record.lsn);
                    } else if (decoded.value.local_schema_json) |schema_json| {
                        try DB.HAReplicationCallbacks.set_schema_with_local_schema_json_replicated_apply(
                            self,
                            decoded_schema,
                            schema_json,
                        );
                        try Self.markReplicationRecordApplied(self, record.lsn);
                    } else {
                        try Self.setSchemaReplicatedApplyWithMarker(self, decoded_schema, record.lsn);
                    }
                },
                .derived_effect => {
                    _ = try Self.applyDerivedEffectRecord(self, record);
                    try Self.markReplicationRecordApplied(self, record.lsn);
                },
                .backup_start,
                .backup_end,
                .checkpoint,
                .manifest,
                .truncate,
                .timeline_switch,
                => try Self.markReplicationRecordApplied(self, record.lsn),
                _ => return error.HAReplicationRecordApplyUnsupported,
            }
        }

        pub fn applyReplicationRecordCallback(ctx: *anyopaque, record: ha_replication_record_mod.RecordView) anyerror!void {
            const self: *DB = @ptrCast(@alignCast(ctx));
            try Self.applyReplicationRecord(self, record);
        }

        const Self = @This();
    };
}

pub fn haMirrorSyncEnabled(mirror: ha_types.AsyncEffectMirror) bool {
    return mirror.sync_policy.mode != .async;
}

pub fn enforceWriteGateOptional(gate: ?WriteGate) !void {
    const configured = gate orelse return;
    try configured.check();
}

pub fn preflightMirrorSyncCommit(log_mutex: *std.atomic.Mutex, mirror: ?AsyncEffectMirror) !void {
    const configured = mirror orelse return;
    if (configured.sync_policy.mode == .async) return;
    if (configured.sync_policy.failure_policy != .fail_closed) return;

    _ = platform.sync.lockAtomic(log_mutex);
    defer log_mutex.*.unlock();

    const target_lsn = configured.primary.nextLsn();
    const decision = try configured.primary.evaluateAppendDurability(target_lsn, configured.sync_policy);
    const gate = commitGateResultFromDecision(target_lsn, decision);
    if (gate.action == .reject) {
        recordMirrorGate(configured, gate);
        return error.SyncPolicyUnsatisfied;
    }
}

pub fn mirrorReplayPayloadBestEffort(log_mutex: *std.atomic.Mutex, identity_namespace: doc_identity.Namespace, mirror: ?AsyncEffectMirror, payload: []const u8) void {
    const configured = mirror orelse return;
    _ = platform.sync.lockAtomic(log_mutex);
    defer log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendEncodedDerivedChangeRecord(configured.primary, payload, .{
        .shard_id = identity_namespace.shard_id,
        .table_id = identity_namespace.table_id,
    }) catch |err| {
        if (configured.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB derived effect into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorReplayPayloadCommit(log_mutex: *std.atomic.Mutex, identity_namespace: doc_identity.Namespace, mirror: ?AsyncEffectMirror, payload: []const u8) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        _ = platform.sync.lockAtomic(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendEncodedDerivedChangeRecord(configured.primary, payload, .{
            .shard_id = identity_namespace.shard_id,
            .table_id = identity_namespace.table_id,
        }) catch |err| {
            noteMirrorFailure(configured, "derived effect", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

pub fn mirrorBatchMutationBestEffort(alloc: Allocator, log_mutex: *std.atomic.Mutex, identity_namespace: doc_identity.Namespace, mirror: ?AsyncBatchMirror, request: types.BatchRequest) void {
    const configured = mirror orelse return;
    _ = platform.sync.lockAtomic(log_mutex);
    defer log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendBatchMutationRequest(alloc, configured.primary, request, .{
        .shard_id = identity_namespace.shard_id,
        .table_id = identity_namespace.table_id,
    }) catch |err| {
        if (configured.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB batch mutation into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorBatchMutationCommit(alloc: Allocator, log_mutex: *std.atomic.Mutex, identity_namespace: doc_identity.Namespace, mirror: ?AsyncBatchMirror, request: types.BatchRequest) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        _ = platform.sync.lockAtomic(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendBatchMutationRequest(alloc, configured.primary, request, .{
            .shard_id = identity_namespace.shard_id,
            .table_id = identity_namespace.table_id,
        }) catch |err| {
            noteMirrorFailure(configured, "batch mutation", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

fn mirrorHAEncodedBatchMutationCommitContext(ctx: anytype, payload: []const u8) !void {
    const mirror = ctx.ha_async_batch_mirror orelse return;
    const lsn = blk: {
        db_internal.lockAtomicWithBackoff(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendEncodedBatchMutationRequest(mirror.primary, payload, .{
            .shard_id = ctx.identity_namespace.shard_id,
            .table_id = ctx.identity_namespace.table_id,
        }) catch |err| {
            noteMirrorFailure(mirror, "batch mutation", err);
            if (haMirrorSyncEnabled(mirror)) return err;
            return;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(mirror, lsn);
}

pub fn mirrorSchemaMetadataBestEffort(alloc: Allocator, log_mutex: *std.atomic.Mutex, identity_namespace: doc_identity.Namespace, mirror: ?AsyncMetadataMirror, table_schema: schema_mod.TableSchema) void {
    const configured = mirror orelse return;
    _ = platform.sync.lockAtomic(log_mutex);
    defer log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendSchemaMetadataMutation(alloc, configured.primary, table_schema, .{
        .shard_id = identity_namespace.shard_id,
        .table_id = identity_namespace.table_id,
    }) catch |err| {
        if (configured.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB schema metadata into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorSchemaMetadataCommit(alloc: Allocator, log_mutex: *std.atomic.Mutex, identity_namespace: doc_identity.Namespace, mirror: ?AsyncMetadataMirror, table_schema: schema_mod.TableSchema) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        _ = platform.sync.lockAtomic(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendSchemaMetadataMutation(alloc, configured.primary, table_schema, .{
            .shard_id = identity_namespace.shard_id,
            .table_id = identity_namespace.table_id,
        }) catch |err| {
            noteMirrorFailure(configured, "metadata mutation", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

pub fn mirrorSchemaJsonMetadataCommit(
    alloc: Allocator,
    log_mutex: *std.atomic.Mutex,
    identity_namespace: doc_identity.Namespace,
    mirror: ?AsyncMetadataMirror,
    table_schema: schema_mod.TableSchema,
    schema_json: []const u8,
) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        _ = platform.sync.lockAtomic(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendSchemaMetadataMutationWithPayloadOptions(
            alloc,
            configured.primary,
            table_schema,
            .{ .local_schema_json = schema_json },
            .{
                .shard_id = identity_namespace.shard_id,
                .table_id = identity_namespace.table_id,
            },
        ) catch |err| {
            noteMirrorFailure(configured, "schema json metadata mutation", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

pub fn mirrorLiteSqlTableMetadataCommit(
    alloc: Allocator,
    log_mutex: *std.atomic.Mutex,
    identity_namespace: doc_identity.Namespace,
    mirror: ?AsyncMetadataMirror,
    table_schema: schema_mod.TableSchema,
    schema_json: []const u8,
    table_record_json: []const u8,
) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        _ = platform.sync.lockAtomic(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendSchemaMetadataMutationWithPayloadOptions(
            alloc,
            configured.primary,
            table_schema,
            .{
                .local_schema_json = schema_json,
                .lite_sql_table_record_json = table_record_json,
            },
            .{
                .shard_id = identity_namespace.shard_id,
                .table_id = identity_namespace.table_id,
            },
        ) catch |err| {
            noteMirrorFailure(configured, "lite sql metadata mutation", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

fn evaluateMirrorCommitGate(mirror: AsyncEffectMirror, lsn: u64) !void {
    if (mirror.sync_policy.mode == .async) return;
    var gate = try ha_commit_gate_mod.evaluate(mirror.primary, lsn, mirror.sync_policy);
    recordMirrorGate(mirror, gate);
    switch (gate.action) {
        .acknowledge => return,
        .acknowledge_degraded => return,
        .reject => return error.SyncPolicyUnsatisfied,
        .wait_for_standby => {
            const wait_fn = mirror.sync_wait_fn orelse return error.HASyncCommitWouldBlock;
            const wait_ctx = mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
            try wait_fn(wait_ctx, mirror.primary, lsn, mirror.sync_policy);
            gate = try ha_commit_gate_mod.evaluate(mirror.primary, lsn, mirror.sync_policy);
            recordMirrorGate(mirror, gate);
            switch (gate.action) {
                .acknowledge => return,
                .acknowledge_degraded => return,
                .reject => return error.SyncPolicyUnsatisfied,
                .wait_for_standby => return error.HASyncCommitWouldBlock,
            }
        },
    }
}

fn mirrorSyncEnabled(mirror: AsyncEffectMirror) bool {
    return mirror.sync_policy.mode != .async;
}

fn recordMirrorGate(mirror: AsyncEffectMirror, gate: ha_commit_gate_mod.GateResult) void {
    if (mirror.last_gate_lsn) |last_lsn| last_lsn.store(gate.target_lsn, .release);
    if (mirror.last_gate_action) |last_action| last_action.store(@intFromEnum(gate.action), .release);
    switch (gate.action) {
        .acknowledge => {},
        .acknowledge_degraded => {
            if (mirror.sync_degraded_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
        .reject => {
            if (mirror.sync_reject_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
        .wait_for_standby => {
            if (mirror.sync_wait_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
    }
}

fn commitGateResultFromDecision(target_lsn: u64, decision: ha_primary_mod.DurabilityDecision) ha_commit_gate_mod.GateResult {
    return .{
        .target_lsn = target_lsn,
        .action = switch (decision.status) {
            .satisfied => .acknowledge,
            .would_block => .wait_for_standby,
            .fail_closed => .reject,
            .degraded_to_async => .acknowledge_degraded,
        },
        .decision = decision,
    };
}

fn noteMirrorFailure(mirror: AsyncEffectMirror, comptime label: []const u8, err: anyerror) void {
    if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
    std.log.warn("failed to mirror DB " ++ label ++ " into HA stream: {s}", .{@errorName(err)});
}
