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

//! Durable authority for an unpublished restore owner. Logical primary rows
//! and native generated-artifact caches cross the source/target boundary;
//! identities and constraints are allocated by the target schema/row pipeline.
const std = @import("std");
const identity = @import("doc_identity.zig");
const activation = @import("relational_integrity_activation.zig");
const catalog_mod = @import("relational_integrity_catalog.zig");
const Allocator = std.mem.Allocator;
pub const key = @import("restore_staging_contract.zig").key;
pub const bootstrap_key = @import("restore_staging_contract.zig").bootstrap_key;
pub const OwnerBootstrap = @import("restore_staging_contract.zig").OwnerBootstrap;
pub const Digest = @import("restore_staging_contract.zig").Digest;
pub const Phase = @import("restore_staging_contract.zig").Phase;
pub const Timestamp = @import("restore_staging_contract.zig").Timestamp;
pub const Artifact = @import("restore_staging_contract.zig").Artifact;
pub const ImportPage = @import("restore_staging_contract.zig").ImportPage;
pub const Control = @import("restore_staging_contract.zig").Control;
pub const PreparedPage = struct {
    arena: std.heap.ArenaAllocator,
    phase: Phase,
    batch: ?@import("types.zig").BatchRequest,
    pub fn deinit(self: *@This()) void {
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Scope = @import("restore_staging_contract.zig").Scope;

pub const Progress = @import("restore_staging_contract.zig").Progress;

pub const digest = @import("restore_staging_contract.zig").digest;

test "restore empty generation proves pristine owner and rejects source import" {
    for ([_][]const u8{ "{}", "{\"version\":1,\"storage_mode\":\"relational\",\"default_type\":\"row\",\"document_schemas\":{\"row\":{\"schema\":{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"integer\"}},\"additionalProperties\":false}}}}" }) |definition| {
        const db = @import("db.zig");
        const alloc = std.testing.allocator;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/empty-owner", .{tmp.sub_path});
        defer alloc.free(path);
        const options: db.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var target = try db.DB.open(alloc, path, options);
        defer target.close();
        try target.setSchemaJson(alloc, definition);
        const schema = try @import("../schema.zig").serializeSchema(alloc, target.core.schema orelse .{});
        defer alloc.free(schema);
        const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(0), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = options.identity_namespace.?, .target_schema_digest = digest(schema), .empty_generation = true };
        try target.reserveRestoreStagingScoped(alloc, scope);
        const count_key = &@import("../internal_keys.zig").range_document_count_key;
        var count: [8]u8 = undefined;
        std.mem.writeInt(u64, &count, 1, .little);
        try target.core.store.putBatch(&.{.{ .key = count_key, .value = &count }}, &.{});
        try std.testing.expectError(error.RestoreStagingTargetNotEmpty, target.beginRestoreStaging(alloc, scope));
        {
            var state = (try target.restoreStagingStatus(alloc)).?;
            defer state.deinit();
            try std.testing.expectEqual(Phase.reserved, state.value.phase);
        }
        try target.core.store.putBatch(&.{}, &.{count_key});
        const orphan = @import("relational_index_records.zig").forward_namespace ++ "orphan";
        try target.core.store.putBatch(&.{.{ .key = orphan, .value = "" }}, &.{});
        try std.testing.expectError(error.RestoreStagingTargetNotEmpty, target.beginRestoreStaging(alloc, scope));
        try target.core.store.putBatch(&.{}, &.{orphan});
        try target.beginRestoreStaging(alloc, scope);
        try target.beginRestoreStaging(alloc, scope);
        {
            var state = (try target.restoreStagingStatus(alloc)).?;
            defer state.deinit();
            try std.testing.expectEqual(Phase.imported, state.value.phase);
            try std.testing.expectEqual(@as(u64, 0), state.value.rows);
            try std.testing.expectEqual(@as(u64, 0), (try identity.visibilitySummaryFromStore(target.core.store)).?.live_ordinals);
        }
        try std.testing.expectError(error.InvalidRestoreStagingCommand, target.prepareRestoreStagingPage(alloc, scope, &target, 128, .none));
        _ = try target.finishRestoreStaging(alloc, scope.digest(), .validated);
        _ = try target.finishRestoreStaging(alloc, scope.digest(), .published);
    }
}

test "restore graph empty generation rejects physical artifacts before staging" {
    const db = @import("db.zig");
    const alloc = std.testing.allocator;
    for ([_]enum { clean, edge, primary_artifact }{ .clean, .edge, .primary_artifact }) |case| {
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/graph-empty-owner", .{tmp.sub_path});
        defer alloc.free(path);
        const namespace: identity.Namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 };
        var target = try db.DB.open(alloc, path, .{ .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false });
        defer target.close();
        try target.setSchemaJson(alloc, "{}");
        try target.addIndex(.{ .name = "links", .kind = .graph, .config_json = "{}" });
        const schema = try @import("../schema.zig").serializeSchema(alloc, target.core.schema orelse .{});
        defer alloc.free(schema);
        const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(0), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = namespace, .target_schema_digest = digest(schema), .empty_generation = true, .graph_retirement_digest = @splat(7) };
        try target.reserveRestoreStagingScoped(alloc, scope);
        if (case == .edge) try target.core.index_manager.graphIndex("links").?.index.batchApply(&.{.{ .source = "a", .target = "b", .edge_type = "link" }}, &.{});
        if (case == .primary_artifact) {
            const keys = @import("../internal_keys.zig");
            var state = std.ArrayListUnmanaged(u8).empty;
            defer state.deinit(alloc);
            try keys.appendDocumentPrefix(&state, alloc, "orphan");
            try state.append(alloc, keys.graph_asset_state_kind);
            try keys.appendEncodedComponent(&state, alloc, "links");
            try keys.appendEncodedComponent(&state, alloc, "source");
            try target.core.store.putBatch(&.{.{ .key = state.items, .value = "orphan" }}, &.{});
        }
        if (case != .clean) {
            try std.testing.expectError(error.RestoreStagingTargetNotEmpty, target.beginRestoreStaging(alloc, scope));
        } else {
            try target.beginRestoreStaging(alloc, scope);
            var state = (try target.restoreStagingStatus(alloc)).?;
            defer state.deinit();
            try std.testing.expectEqual(Phase.imported, state.value.phase);
        }
    }
}

pub fn optional(txn: anytype) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}
pub fn requireScope(alloc: Allocator, txn: anytype, expected: ?Digest, allow_importing: bool) !void {
    const raw = (try optional(txn)) orelse {
        if (expected != null) return error.RestoreStagingScopeChanged;
        return;
    };
    var progress = try Progress.decode(alloc, raw);
    defer progress.deinit();
    if (progress.value.phase == .published) {
        if (expected != null) return error.RestoreStagingScopeChanged;
        return;
    }
    if (progress.value.phase == .canceled) return error.RestoreStagingCanceled;
    if (progress.value.phase == .reserved) return error.RestoreStagingInProgress;
    if (!std.mem.eql(u8, &(expected orelse return error.RestoreStagingInProgress), &progress.value.scope.digest())) return error.RestoreStagingScopeChanged;
    if (!allow_importing and progress.value.phase == .importing) return error.RestoreStagingInProgress;
}

/// Once a validated receipt is durable, neither stale workers nor an already
/// captured private routing view may change its claims/checkpoint underneath it.
pub fn requireMutableScope(alloc: Allocator, txn: anytype, expected: ?Digest) !void {
    try requireScope(alloc, txn, expected, false);
    if (expected == null) return;
    var progress = try Progress.decode(alloc, (try optional(txn)) orelse return error.RestoreStagingScopeChanged);
    defer progress.deinit();
    if (progress.value.phase != .imported) return error.RestoreStagingScopeChanged;
}

/// Internal PreparedRow import admission, consumed under the DB apply fence.
pub const BatchAdmission = struct { expected: Digest, next: []const u8, scope: Digest, rewrite: bool = false, source_effects: u32 = 0, artifact_page: bool = false, projection_page: bool = false, source_generation_proof_page: bool = false };
pub fn validateImport(alloc: Allocator, txn: anytype, admission: BatchAdmission, row_count: usize, delete_count: usize) !void {
    const raw = (try optional(txn)) orelse return error.RestoreStagingScopeChanged;
    if (!std.mem.eql(u8, &digest(raw), &admission.expected)) return error.RestoreStagingProgressChanged;
    var before = try Progress.decode(alloc, raw);
    defer before.deinit();
    if (before.value.scope.empty_generation) return error.InvalidRestoreStagingCommand;
    var after = Progress.decode(alloc, admission.next) catch |err| {
        if (err == error.OutOfMemory) return err;
        return error.InvalidRestoreStagingCommand;
    };
    defer after.deinit();
    if (before.value.phase != .importing or (after.value.phase != .importing and after.value.phase != .imported) or
        !std.mem.eql(u8, &before.value.scope.digest(), &admission.scope) or !std.mem.eql(u8, &after.value.scope.digest(), &admission.scope) or
        after.value.rows != std.math.add(u64, before.value.rows, row_count +| delete_count) catch return error.InvalidRestoreStagingCommand)
        return error.InvalidRestoreStagingCommand;
    if (admission.rewrite != (before.value.scope.rewrite != null)) return error.InvalidRestoreStagingCommand;
    if (admission.source_generation_proof_page) {
        if (admission.rewrite or admission.artifact_page or admission.projection_page or
            before.value.source_generation_proofs_complete or !after.value.source_generation_proofs_complete or
            before.value.phase != .importing or after.value.phase != .importing or
            row_count != 0 or delete_count != 0 or admission.source_effects != 0 or
            before.value.rows != 0 or before.value.cursor.len != 0 or before.value.artifact_cursor.len != 0 or
            before.value.projection_cursor.len != 0 or before.value.artifacts_complete or before.value.rows_complete or
            before.value.rows != after.value.rows or
            !std.mem.eql(u8, before.value.cursor, after.value.cursor) or
            !std.mem.eql(u8, before.value.artifact_cursor, after.value.artifact_cursor) or
            !std.mem.eql(u8, before.value.projection_cursor, after.value.projection_cursor) or
            before.value.artifacts_complete != after.value.artifacts_complete or
            before.value.rows_complete != after.value.rows_complete or
            !std.mem.eql(u8, &before.value.logical_digest, &after.value.logical_digest) or
            !std.meta.eql(before.value.rewrite, after.value.rewrite)) return error.InvalidRestoreStagingCommand;
        return;
    }
    if (!before.value.source_generation_proofs_complete) return error.InvalidRestoreStagingCommand;
    if (before.value.source_generation_proofs_complete != after.value.source_generation_proofs_complete)
        return error.InvalidRestoreStagingCommand;
    if (admission.projection_page) {
        if (admission.artifact_page or admission.rewrite or !before.value.scope.preserve_artifacts or !before.value.artifacts_complete or
            !before.value.rows_complete or !after.value.rows_complete or !after.value.artifacts_complete or
            row_count != 0 or delete_count != 0 or admission.source_effects != 0 or
            !std.mem.eql(u8, before.value.cursor, after.value.cursor) or
            !std.mem.eql(u8, before.value.artifact_cursor, after.value.artifact_cursor) or
            !std.mem.eql(u8, &before.value.logical_digest, &after.value.logical_digest) or
            (after.value.phase == .imported and after.value.projection_cursor.len != 0) or
            (after.value.phase != .imported and std.mem.order(u8, after.value.projection_cursor, before.value.projection_cursor) != .gt)) return error.InvalidRestoreStagingCommand;
        return;
    }
    if (!std.mem.eql(u8, before.value.projection_cursor, after.value.projection_cursor)) return error.InvalidRestoreStagingCommand;
    if (admission.artifact_page) {
        if (admission.rewrite or !before.value.scope.preserve_artifacts or before.value.artifacts_complete or
            before.value.rows_complete or after.value.rows_complete or
            row_count != 0 or delete_count != 0 or admission.source_effects != 0 or after.value.phase != .importing or
            !std.mem.eql(u8, before.value.cursor, after.value.cursor) or
            !std.mem.eql(u8, &before.value.logical_digest, &after.value.logical_digest) or
            (after.value.artifacts_complete and after.value.artifact_cursor.len != 0) or
            (!after.value.artifacts_complete and std.mem.order(u8, after.value.artifact_cursor, before.value.artifact_cursor) != .gt))
            return error.InvalidRestoreStagingCommand;
        return;
    }
    if (before.value.artifacts_complete != after.value.artifacts_complete or
        !std.mem.eql(u8, before.value.artifact_cursor, after.value.artifact_cursor) or
        (before.value.scope.preserve_artifacts and !before.value.artifacts_complete)) return error.InvalidRestoreStagingCommand;
    if (before.value.scope.preserve_artifacts) {
        if (before.value.rows_complete or after.value.phase != .importing or
            (after.value.rows_complete and after.value.cursor.len != 0)) return error.InvalidRestoreStagingCommand;
    } else if (before.value.rows_complete or after.value.rows_complete) return error.InvalidRestoreStagingCommand;
    if (!admission.rewrite) {
        if (delete_count != 0 or admission.source_effects != 0 or (after.value.phase == .importing and !after.value.rows_complete and std.mem.order(u8, after.value.cursor, before.value.cursor) != .gt)) return error.InvalidRestoreStagingCommand;
        return;
    }
    const previous = before.value.rewrite orelse return error.InvalidRestoreStagingCommand;
    const next = after.value.rewrite orelse return error.InvalidRestoreStagingCommand;
    if (previous.final_cut != null) return error.InvalidRestoreStagingCommand;
    if (!previous.snapshot_complete) {
        if (delete_count != 0 or admission.source_effects != 0 or next.sequence != previous.sequence or next.frame_offset != 0 or next.final_cut != null or
            (row_count != 0 and std.mem.order(u8, after.value.cursor, before.value.cursor) != .gt) or
            (!next.snapshot_complete and std.mem.order(u8, after.value.cursor, before.value.cursor) != .gt)) return error.InvalidRestoreStagingCommand;
    } else {
        if (!next.snapshot_complete or !std.mem.eql(u8, before.value.cursor, after.value.cursor)) return error.InvalidRestoreStagingCommand;
        const effects = admission.source_effects;
        if (effects < row_count +| delete_count or effects > 1024) return error.InvalidRestoreStagingCommand;
        if (next.final_cut != null) {
            if (effects != 0 or next.sequence != previous.sequence or previous.frame_offset != 0 or after.value.phase != .imported) return error.InvalidRestoreStagingCommand;
        } else if (next.sequence == previous.sequence) {
            if (effects == 0 or next.frame_offset <= previous.frame_offset or next.frame_remaining == 0) return error.InvalidRestoreStagingCommand;
            if (previous.frame_offset != 0 and (!std.mem.eql(u8, &previous.frame_digest, &next.frame_digest) or
                previous.frame_remaining != next.frame_remaining +| effects)) return error.InvalidRestoreStagingCommand;
        } else {
            if (next.sequence != (std.math.add(u64, previous.sequence, 1) catch return error.InvalidRestoreStagingCommand) or
                effects == 0 or next.frame_offset != 0 or (previous.frame_offset != 0 and effects != previous.frame_remaining)) return error.InvalidRestoreStagingCommand;
        }
    }
}

pub fn initialCoverage(alloc: Allocator, txn: anytype, catalog: catalog_mod.Catalog) !?[]u8 {
    if (!activation.hasActive(catalog)) return null;
    var progress = try activation.status(txn, catalog);
    progress.state = .validating;
    progress.cursor = "";
    progress.rows_scanned = 0;
    progress.failure = "";
    progress.phase = @import("relational_integrity_activation_contract.zig").firstPhase(catalog);
    return try progress.encode(alloc);
}

test "restore staging owner scope and checksummed continuation exclude source identities" {
    const alloc = std.testing.allocator;
    const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = .{ .table_id = 4, .shard_id = 5, .range_id = 5 }, .target_namespace = .{ .table_id = 6, .shard_id = 7, .range_id = 7 }, .target_schema_digest = @splat(8) };
    const bytes = try (Progress{ .scope = scope }).encode(alloc);
    defer alloc.free(bytes);
    var decoded = try Progress.decode(alloc, bytes);
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &scope.digest(), &decoded.value.scope.digest());
    bytes[5] ^= 1;
    try std.testing.expectError(error.InvalidRestoreStagingRecord, Progress.decode(alloc, bytes));
}

test "native restore artifact phase cannot skip row fencing or mutate logical progress" {
    const alloc = std.testing.allocator;
    const scope: Scope = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .source_artifact_digest = @splat(3),
        .source_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 },
        .target_namespace = .{ .table_id = 3, .shard_id = 4, .range_id = 4 },
        .target_schema_digest = @splat(5),
        .preserve_artifacts = true,
    };
    const before: Progress = .{ .scope = scope };
    const raw = try before.encode(alloc);
    defer alloc.free(raw);
    const Read = struct {
        value: []const u8,
        fn get(self: *@This(), requested: []const u8) anyerror![]const u8 {
            if (!std.mem.eql(u8, requested, key)) return error.NotFound;
            return self.value;
        }
    };
    var read: Read = .{ .value = raw };
    var next = before;
    next.artifact_cursor = "artifact";
    const encoded = try next.encode(alloc);
    defer alloc.free(encoded);
    var admission: BatchAdmission = .{ .expected = digest(raw), .next = encoded, .scope = scope.digest(), .artifact_page = true };
    try validateImport(alloc, &read, admission, 0, 0);
    try std.testing.expectError(error.InvalidRestoreStagingCommand, validateImport(alloc, &read, admission, 1, 0));
    admission.artifact_page = false;
    try std.testing.expectError(error.InvalidRestoreStagingCommand, validateImport(alloc, &read, admission, 0, 0));
    next.artifacts_complete = true;
    next.artifact_cursor = "";
    const completed = try next.encode(alloc);
    defer alloc.free(completed);
    admission.next = completed;
    admission.artifact_page = true;
    try validateImport(alloc, &read, admission, 0, 0);
    read.value = completed;
    admission.expected = digest(completed);
    try std.testing.expectError(error.InvalidRestoreStagingCommand, validateImport(alloc, &read, admission, 0, 0));
}

fn applyTestPage(alloc: Allocator, db: *@import("db.zig").DB, req: @import("types.zig").BatchRequest, index: u64, ha: bool) !void {
    if (!ha) return db.batchRaftReplicatedApply(req, .{ .term = 1, .index = index });
    const payload = try @import("../hot_standby/effects.zig").encodeBatchMutationRequestAlloc(alloc, req);
    defer alloc.free(payload);
    try db.applyHAReplicationRecord(.{ .kind = .batch_mutation, .payload_codec = .json, .cluster_id = 1, .timeline_id = 1, .epoch = 1, .lsn = index, .previous_lsn = index - 1, .payload = payload });
    try std.testing.expectEqual(index, try db.haAppliedReplicationLsn());
}

test "relational integrity restore staging Raft controls retain HA append obligations and replay original import timestamps" {
    const db_mod = @import("db.zig");
    const primary_mod = @import("../hot_standby/primary.zig");
    const effects = @import("../hot_standby/effects.zig");
    const types = @import("types.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const owned = arena.allocator();
    const source_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/source-ha", .{tmp.sub_path});
    var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    {
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        try source.setSchemaJson(alloc, "{}");
        try source.batch(.{ .timestamp_ns = 1234, .writes = &.{.{ .key = "row", .value = "{\"id\":1}" }} });
    }
    source_options.open_mode = .query_readonly;
    var source = try db_mod.DB.open(alloc, source_path, source_options);
    defer source.close();
    for ([_]bool{ false, true }, 0..) |synchronous, trial| {
        const path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/primary-{d}", .{ tmp.sub_path, trial });
        const replica_path = try std.fmt.allocPrint(owned, ".zig-cache/tmp/{s}/standby-{d}", .{ tmp.sub_path, trial });
        const log_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/log-{d}", .{ tmp.sub_path, trial }, 0);
        const slots_path = try std.fmt.allocPrintSentinel(owned, ".zig-cache/tmp/{s}/slots-{d}", .{ tmp.sub_path, trial }, 0);
        var primary = try primary_mod.Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 10, .shard_id = 11 }, .{});
        defer primary.close();
        try primary.createSlot("standby", 0);
        const Ack = struct {
            calls: usize = 0,
            fn wait(ptr: *anyopaque, stream: *primary_mod.Primary, lsn: u64, _: primary_mod.SyncPolicy) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.calls += 1;
                if (self.calls == 1) return error.InjectedRestoreMirrorWaitFailure;
                try stream.standbyStatusUpdate("standby", 1, lsn, lsn);
            }
        };
        var ack: Ack = .{};
        const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
        var target = try db_mod.DB.open(alloc, path, target_options);
        defer target.close();
        var replica = try db_mod.DB.open(alloc, replica_path, target_options);
        defer replica.close();
        try target.setSchemaJson(alloc, "{}");
        try replica.setSchemaJson(alloc, "{}");
        const schema = try @import("../schema.zig").serializeSchema(owned, target.core.schema orelse .{});
        const scope: Scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = digest(schema) };
        try target.reserveRestoreStagingScoped(alloc, scope);
        try replica.reserveRestoreStagingScoped(alloc, scope);
        const bootstrap: OwnerBootstrap = .{ .scope = scope, .table_name = "docs", .schema_json = "{}", .indexes_json = "{}", .byte_range = .{ .start = "", .end = "" } };
        try target.installRestoreStagingBootstrap(alloc, bootstrap);
        try target.installRestoreStagingBootstrap(alloc, bootstrap);
        var changed_bootstrap = bootstrap;
        changed_bootstrap.table_name = "another-table";
        try std.testing.expectError(error.RestoreStagingScopeChanged, target.installRestoreStagingBootstrap(alloc, changed_bootstrap));
        {
            var stored_bootstrap = (try target.readRestoreStagingBootstrap(alloc)) orelse return error.TestUnexpectedResult;
            defer stored_bootstrap.deinit();
            try std.testing.expectEqualStrings("docs", stored_bootstrap.value.table_name);
        }
        target.ha_async_batch_mirror = .{
            .primary = &primary,
            .sync_policy = .{ .mode = if (synchronous) .remote_write else .async, .standby_names = &.{"standby"}, .failure_policy = .block },
            .sync_wait_ctx = &ack,
            .sync_wait_fn = Ack.wait,
        };
        const begin: types.BatchRequest = .{ .restore_staging = .{ .begin = scope } };
        if (synchronous) {
            try std.testing.expectError(error.InjectedRestoreMirrorWaitFailure, target.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 }));
            try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        }
        try target.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 });
        try target.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 });
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        var raft_index: u64 = 2;
        while (true) : (raft_index += 1) {
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 128, .none);
            defer page.deinit();
            if (page.batch) |batch| {
                try target.batchRaftReplicatedApply(batch, .{ .term = 1, .index = raft_index });
                try target.batchRaftReplicatedApply(batch, .{ .term = 1, .index = raft_index });
            }
            if (page.phase == .imported) break;
        }
        for ([_]Phase{ .validated, .published }) |phase| {
            raft_index += 1;
            try target.batchRaftReplicatedApply(.{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = phase } } }, .{ .term = 1, .index = raft_index });
        }
        var saw_import = false;
        for (1..primary.lastLsn() + 1) |lsn| {
            var entry = (try primary.log.entryAt(alloc, lsn)) orelse return error.TestUnexpectedResult;
            defer entry.deinit(alloc);
            var decoded = try effects.decodeBatchMutationRequest(alloc, entry.record);
            defer decoded.deinit();
            if (decoded.value.request.restore_staging.? == .begin) {
                const restored_bootstrap = decoded.value.restore_staging_bootstrap orelse return error.TestUnexpectedResult;
                try std.testing.expectEqualStrings("docs", restored_bootstrap.table_name);
                try std.testing.expectEqualSlices(u8, &scope.digest(), &restored_bootstrap.scope.digest());
                try replica.installRestoreStagingBootstrap(alloc, restored_bootstrap);
            } else try std.testing.expect(decoded.value.restore_staging_bootstrap == null);
            if (decoded.value.request.restore_staging.? == .import_page and decoded.value.request.writes.len != 0) {
                saw_import = true;
                try std.testing.expectEqual(@as(u64, 1234), decoded.value.request.restore_staging.?.import_page.timestamps[0].timestamp);
            }
            try replica.applyHAReplicationRecord(entry.record);
        }
        try std.testing.expect(saw_import);
        var found = (try replica.lookup(alloc, "row", .{})) orelse return error.TestUnexpectedResult;
        defer found.deinit(alloc);
        try std.testing.expectEqualStrings("{\"id\":1}", found.json);
    }
}

test "relational integrity restore staging imports typed and document rows with restart and exact timestamps" {
    const db_mod = @import("db.zig");
    const types = @import("types.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    for ([_][]const u8{
        "{}",
        \\{"version":1,"storage_mode":"relational","default_type":"row","document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"name":{"type":"string"}},"additionalProperties":false}}}}
    }, 0..) |schema_json, index| {
        const source_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/source-{d}", .{ tmp.sub_path, index });
        defer alloc.free(source_path);
        const target_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/target-{d}", .{ tmp.sub_path, index });
        defer alloc.free(target_path);
        var source_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 10, .shard_id = 11, .range_id = 11 }, .start_index_workers = false, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
        const target_options: db_mod.OpenOptions = .{ .identity_namespace = .{ .table_id = 20, .shard_id = 21, .range_id = 21 }, .start_index_workers = false, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
        {
            var source = try db_mod.DB.open(alloc, source_path, source_options);
            defer source.close();
            try source.setSchemaJson(alloc, schema_json);
            try source.batch(.{ .writes = &.{.{ .key = "a", .value = "{\"id\":1,\"name\":\"alpha\"}" }}, .timestamp_ns = 123 });
            try source.batch(.{ .writes = &.{.{ .key = "b", .value = "{\"id\":2,\"name\":\"beta\"}" }}, .timestamp_ns = 456 });
        }
        source_options.open_mode = .query_readonly;
        var source = try db_mod.DB.open(alloc, source_path, source_options);
        defer source.close();
        var scope: Scope = undefined;
        var raft_index: u64 = 0;
        {
            var target = try db_mod.DB.open(alloc, target_path, target_options);
            defer target.close();
            try target.setSchemaJson(alloc, schema_json);
            const serialized = try @import("../schema.zig").serializeSchema(alloc, target.core.schema orelse .{});
            defer alloc.free(serialized);
            scope = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .source_artifact_digest = @splat(3), .source_namespace = source_options.identity_namespace.?, .target_namespace = target_options.identity_namespace.?, .target_schema_digest = digest(serialized) };
            try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, scope.target_namespace);
            try target.reserveRestoreStaging(alloc, scope.plan_id, scope.plan_digest, scope.target_namespace);
            try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{ .restore_staging_scope = scope.digest() }));
            try std.testing.expectError(error.RestoreStagingInProgress, target.beginTransaction(1));
            var wrong_plan = scope;
            wrong_plan.plan_id[0] ^= 1;
            try std.testing.expectError(error.RestoreStagingScopeChanged, target.beginRestoreStaging(alloc, wrong_plan));
            raft_index += 1;
            try applyTestPage(alloc, &target, .{ .restore_staging = .{ .begin = scope } }, raft_index, index == 1);
            try target.beginRestoreStaging(alloc, scope);
            try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{}));
            try std.testing.expectError(error.RestoreStagingInProgress, target.batch(.{ .writes = &.{.{ .key = "evil", .value = "{}" }} }));
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 1, .none);
            defer page.deinit();
            var corrupt = page.batch.?;
            const corrupted_progress = try alloc.dupe(u8, corrupt.restore_staging.?.import_page.next);
            defer alloc.free(corrupted_progress);
            corrupted_progress[4] ^= 1;
            corrupt.restore_staging.?.import_page.next = corrupted_progress;
            try std.testing.expectError(error.InvalidRestoreStagingCommand, target.batchReplicatedApply(corrupt));
            var unchanged = (try target.restoreStagingStatus(alloc)).?;
            defer unchanged.deinit();
            try std.testing.expectEqual(@as(u64, 0), unchanged.value.rows);
            raft_index += 1;
            try applyTestPage(alloc, &target, page.batch.?, raft_index, index == 1);
            try applyTestPage(alloc, &target, page.batch.?, raft_index, index == 1);
        }
        var target = try db_mod.DB.open(alloc, target_path, target_options);
        defer target.close();
        try std.testing.expectError(error.RestoreStagingInProgress, target.lookup(alloc, "a", .{}));
        for (0..20) |_| {
            var page = try target.prepareRestoreStagingPage(alloc, scope, &source, 1, .none);
            defer page.deinit();
            if (page.batch) |req| {
                raft_index += 1;
                try applyTestPage(alloc, &target, req, raft_index, index == 1);
                try applyTestPage(alloc, &target, req, raft_index, index == 1);
            }
            if (page.phase == .imported) break;
        } else return error.TestUnexpectedResult;
        var status = (try target.restoreStagingStatus(alloc)).?;
        defer status.deinit();
        try std.testing.expectEqual(@as(u64, 2), status.value.rows);
        try std.testing.expectEqual(@as(u64, 123), try target.getTimestamp(alloc, "a"));
        try std.testing.expectEqual(@as(u64, 456), try target.getTimestamp(alloc, "b"));
        raft_index += 1;
        try applyTestPage(alloc, &target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .validated } } }, raft_index, index == 1);
        const receipt = try target.finishRestoreStaging(alloc, scope.digest(), .validated);
        try std.testing.expectEqualSlices(u8, &receipt, &try target.finishRestoreStaging(alloc, scope.digest(), .validated));
        raft_index += 1;
        try applyTestPage(alloc, &target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } }, raft_index, index == 1);
        try applyTestPage(alloc, &target, .{ .restore_staging = .{ .finish = .{ .scope = scope.digest(), .phase = .published } } }, raft_index, index == 1);
        var row = (try target.lookup(alloc, "b", .{ .include_all_fields = true })).?;
        defer row.deinit(alloc);
        try std.testing.expect(std.mem.indexOf(u8, row.json, "beta") != null);
        try std.testing.expectError(error.RestoreStagingScopeChanged, target.lookup(alloc, "b", .{ .restore_staging_scope = scope.digest() }));
        _ = types;
    }
}
