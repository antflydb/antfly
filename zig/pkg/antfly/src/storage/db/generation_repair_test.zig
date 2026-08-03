// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0.

const std = @import("std");

const DB = @import("mod.zig").DB;
const artifact_repair_mod = @import("artifact_repair.zig");
const ArtifactRepair = DB.ArtifactRepairCallbacks;
const DerivedAsync = DB.DerivedAsyncCallbacks;
const Lifecycle = DB.LifecycleCallbacks;
const SchemaRuntime = DB.SchemaRuntimeCallbacks;
const TestHelpers = @import("test_support.zig");
const apply_state = @import("derived/apply_state.zig");
const db_internal = @import("internal.zig");
const doc_identity = @import("doc_identity.zig");
const docstore_mod = @import("../docstore.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const generation_lifecycle = @import("generation_lifecycle.zig");
const graph_mod = @import("../../graph/graph.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const internal_keys = @import("../internal_keys.zig");
const range_cardinality = @import("range_cardinality.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;
const CountingDenseEmbedder = TestHelpers.CountingDenseEmbedder;
const QueryVisibilityChange = db_internal.QueryVisibilityChange;
const tempPath = TestHelpers.tempPath;
const cleanupTempDir = TestHelpers.cleanupTempDir;
const monotonicTimeNs = @import("antfly_platform").time.monotonicNs;
const threadedIo = db_internal.threadedIo;

fn ensureDirPath(path: []const u8) !void {
    if (path.len == 0) return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), path);
}

fn writeRawProjectionCheckpointSidecarForTest(path: []const u8, raw: []const u8) !void {
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = path,
        .data = raw,
    });
}

test "db generation repair artifact dense target prefers current incarnation outcomes over stale name counter" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var counting = CountingDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = counting.interface(),
        },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"body\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dense_idx\"}}",
        .coverage_generation = 42,
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    try db.runUntilIdle();

    // Same-name recreation can leave this legacy counter stale while the
    // current generation's outcome accounting is complete.
    const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, config.name);
    defer alloc.free(counter_key);
    var stale_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &stale_value, 1, .little);
    try db.core.store.put(counter_key, &stale_value);

    try std.testing.expectEqual(
        @as(?u64, 2),
        try DerivedAsync.dense_target_count_for_index_context(db.async_context, config.name),
    );
}

test "db generation repair asynchronous dense replay lag is not classified as repair debt" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .write,
    });

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
    const target_sequence = try Lifecycle.probe_derived_replay_target_sequence(
        &db,
        alloc,
        db.core.replaySource(),
        .{ .name = "dense_idx", .kind = .dense_vector },
        checkpoint.applied_sequence,
    );
    try std.testing.expect(checkpoint.applied_sequence < target_sequence);
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, db.core.store, "dense_idx"),
    );
    try std.testing.expectEqual(
        @as(u64, 0),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    try std.testing.expect(!(try ArtifactRepair.index_generation_repair_required(&db, alloc, "dense_idx")));
    try std.testing.expect(!(try db.hasPendingDenseArtifactRebuild(alloc)));
}

test "db generation repair source artifact debt does not synthesize index generation repair debt" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .embedding,
        .index_name = try alloc.dupe(u8, "dense_idx"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "dense_idx"),
        .artifact_key = try alloc.dupe(u8, "corrupt-artifact"),
        .reason = .corrupt_artifact,
        .sequence = db.core.nextDerivedSequence(),
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const artifact_issues = try db.listArtifactRepairIssues(alloc, .embedding, "dense_idx", 0);
    defer types.freeArtifactRepairIssues(alloc, artifact_issues);
    try std.testing.expectEqual(@as(usize, 1), artifact_issues.len);
    try std.testing.expect(!(try ArtifactRepair.index_generation_repair_required(&db, alloc, "dense_idx")));

    var generation_issues = try db.listArtifactRepairIssuesPage(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
    });
    defer generation_issues.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), generation_issues.issues.len);
}

test "db generation repair automatic dense repair bootstraps missing coverage metadata" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
        const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, "dense_idx");
        defer alloc.free(counter_key);
        try db.core.store.delete(counter_key);
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeZ(u8, dense_path);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const result = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(result.attempted);
    try std.testing.expect(!result.terminal);
    try std.testing.expect(result.repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, reopened.core.store, "dense_idx"),
    );
    try std.testing.expect(try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, "dense_idx") == null);

    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);

    var search_result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer search_result.deinit();
    try std.testing.expectEqual(@as(u32, 1), search_result.total_hits);
    try std.testing.expectEqualStrings("doc:a", search_result.hits[0].id);
}

test "db generation repair corrupt projection sidecar degrades non-dense checkpoint and quarantines writes" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const text_cfg: types.IndexConfig = .{
        .name = "ft_idx",
        .kind = .full_text,
        .config_json = "{}",
    };
    var text_config_hash: u64 = 0;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();

        try db.addIndex(text_cfg);
        const stored_text_cfg = db.core.index_manager.get("ft_idx") orelse return error.TestUnexpectedResult;
        text_config_hash = types.indexConfigHash(stored_text_cfg.*);
        try db.core.saveProjectionCheckpoint("ft_idx", .{
            .applied_sequence = 9,
            .status = .clean,
            .generation = 4,
            .config_hash = text_config_hash,
        });

        const checkpoint_path = db.core.applied_sequence_checkpoint_path orelse return error.TestUnexpectedResult;
        try writeRawProjectionCheckpointSidecarForTest(checkpoint_path, "not-a-derived-apply-checkpoint");
    }

    {
        var status_only = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status_only.close();

        try std.testing.expectEqual(@as(u64, 0), try status_only.core.loadAppliedSequence(alloc, "ft_idx"));
        const degraded_checkpoint = try status_only.core.loadProjectionCheckpoint(alloc, "ft_idx");
        try std.testing.expectEqual(apply_state.ProjectionStatus.repair_required, degraded_checkpoint.status);
        try std.testing.expectEqual(@as(u64, 0), degraded_checkpoint.applied_sequence);
        try std.testing.expectEqual(text_config_hash, degraded_checkpoint.config_hash);

        const degraded_stats = try status_only.stats(alloc);
        defer types.freeDBStats(alloc, degraded_stats);
        try std.testing.expectEqual(@as(usize, 1), degraded_stats.indexes.len);
        try std.testing.expectEqualStrings("repair_required", degraded_stats.indexes[0].projection_checkpoint_status);
        try std.testing.expect(degraded_stats.indexes[0].repair_degraded);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expectEqual(@as(u64, 0), try reopened.core.loadAppliedSequence(alloc, "ft_idx"));
    const degraded_by_open_checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.repair_required, degraded_by_open_checkpoint.status);
    try std.testing.expectEqual(@as(u64, 0), degraded_by_open_checkpoint.applied_sequence);
    try std.testing.expectEqual(text_config_hash, degraded_by_open_checkpoint.config_hash);
    try std.testing.expectEqualStrings(
        "InvalidDerivedApplyState",
        reopened.core.index_manager.loadFailure("ft_idx") orelse return error.TestUnexpectedResult,
    );

    try std.testing.expectError(
        error.IndexNotFound,
        reopened.core.saveAppliedSequence("ft_idx", 7),
    );
    const still_degraded_checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.repair_required, still_degraded_checkpoint.status);
    try std.testing.expectEqual(@as(u64, 0), still_degraded_checkpoint.applied_sequence);
    try std.testing.expectEqual(text_config_hash, still_degraded_checkpoint.config_hash);
}

test "db generation repair corrupt repair checkpoint preserves primary availability and fails indexes closed" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    var repair_checkpoint_path: []u8 = undefined;

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
        repair_checkpoint_path = try alloc.dupe(u8, db.core.index_repair_checkpoint.?.path);
    }
    defer alloc.free(repair_checkpoint_path);
    try writeRawProjectionCheckpointSidecarForTest(repair_checkpoint_path, "corrupt-index-repair-state");

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.async_context.index_repair_state_corrupt.load(.acquire));
    try std.testing.expect(reopened.async_context.index_repair_replay_pinned.load(.acquire));
    try std.testing.expectError(error.InvalidIndexRepairState, reopened.hasPendingIndexRepairIntents(alloc));
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.repair_degraded);
    try std.testing.expect(stats.indexes[0].repair_degraded);
    try std.testing.expectEqualStrings("corrupt_local_repair_state", stats.indexes[0].index_repair_trigger);

    const primary = (try reopened.get(alloc, "doc:a")) orelse return error.TestUnexpectedResult;
    defer alloc.free(primary);
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\"}" }},
        .sync_level = .write,
    });
    const appended = (try reopened.get(alloc, "doc:b")) orelse return error.TestUnexpectedResult;
    defer alloc.free(appended);
}

test "db generation repair coverage status reports partial counter tuples without scanning markers" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{
        .name = "external_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}",
    });
    const generation = db.core.index_manager.coverageGenerationForIndex("external_v1") orelse return error.TestUnexpectedResult;
    const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, "external_v1", generation, "produced");
    defer alloc.free(counter_key);
    var encoded_count: [8]u8 = undefined;
    try db.core.store.put(counter_key, internal_keys.encodeDerivedCoverageOutcomeCount(&encoded_count, 7));

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    for (stats.indexes) |index_stats| {
        if (!std.mem.eql(u8, index_stats.name, "external_v1")) continue;
        try std.testing.expectEqual(@as(u64, 7), index_stats.coverage_produced_count);
        try std.testing.expect(!index_stats.coverage_summary_ready);
        try std.testing.expect(index_stats.repair_degraded);
        return;
    }
    return error.IndexNotFound;
}

test "db generation repair dense artifact coverage finalizes a completed rebuilding checkpoint" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    try db.addIndex(dense_cfg);
    const stored_dense_cfg = &(db.core.index_manager.denseIndex("dense_idx") orelse return error.TestUnexpectedResult).config;
    const dense_config_hash = types.indexConfigHash(stored_dense_cfg.*);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":\"AACAPwAAAAAAAAAA\"}}" }},
        .sync_level = .full_index,
    });

    const target_sequence = db.core.nextDerivedSequence();
    try db.core.saveAppliedSequence("dense_idx", target_sequence -| 1);
    try db.core.saveProjectionCheckpoint("dense_idx", .{
        .applied_sequence = target_sequence -| 1,
        .status = .rebuilding,
        .generation = 7,
        .config_hash = dense_config_hash,
    });

    try std.testing.expectEqual(@as(usize, 1), try db.rebuildDenseIndexesFromStoredEmbeddingArtifactsIfNeeded(alloc));
    try std.testing.expectEqual(target_sequence, try db.core.loadAppliedSequence(alloc, "dense_idx"));
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(target_sequence, checkpoint.applied_sequence);
    try std.testing.expectEqual(@as(u64, 8), checkpoint.generation);
    try std.testing.expectEqual(dense_config_hash, checkpoint.config_hash);
}

test "db generation repair dense finalization owner drains requests queued during publication" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });

    const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, config.name);
    defer alloc.free(counter_key);
    var counter_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter_value, 1, .little);
    try db.core.store.put(counter_key, &counter_value);
    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.core.saveProjectionCheckpoint(config.name, .{
        .applied_sequence = applied,
        .status = .rebuilding,
        .generation = 17,
        .config_hash = types.indexConfigHash(config),
    });

    db.async_context.apply_mutex.lockShared();
    defer db.async_context.apply_mutex.unlockShared();
    db.async_context.dense_projection_finalizing.store(true, .release);
    try std.testing.expect(!try DerivedAsync.finalize_covered_dense_projection_checkpoint(db.async_context, config.name, applied));
    try std.testing.expect(db.async_context.dense_projection_finalization_requested);
    try std.testing.expect(try DerivedAsync.drain_claimed_dense_projection_finalizations(db.async_context));
    try std.testing.expect(!db.async_context.dense_projection_finalizing.load(.acquire));

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 18), checkpoint.generation);
}

test "db generation repair dense repair defers before candidate creation when node admission is exhausted" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeZ(u8, dense_path);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.dense_repair_working_set)] = .{
        .soft_limit_bytes = 2 * 1024 * 1024,
        .hard_limit_bytes = 4 * 1024 * 1024,
    };
    var resources = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    defer resources.deinit(alloc);
    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .resource_manager = &resources,
    });
    defer reopened.close();
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    const deferred = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(deferred.attempted);
    try std.testing.expect(deferred.deferred);
    try std.testing.expect(!deferred.repaired);

    var entry = try ArtifactRepair.load_index_repair_entry_by_id(&reopened, alloc, repair_id);
    defer entry.deinit(alloc);
    try std.testing.expect(entry.intent.candidate_relative_path == null);
    try std.testing.expectEqualStrings("RepairResourceUnavailable", entry.intent.last_error.?);
    try std.testing.expectEqual(@as(u32, 1), entry.intent.attempt_count);
    try std.testing.expectEqual(@as(u32, 1), entry.intent.failure_streak);
    const first_retry_at_ms = entry.intent.next_retry_at_ms;
    const admission = resources.sliceStats(.dense_repair_working_set);
    try std.testing.expectEqual(@as(u64, 0), admission.used_bytes);
    try std.testing.expectEqual(@as(u64, 1), admission.hard_limit_rejections);
    try std.testing.expectError(error.IncompleteBulkPublish, hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
        .dims = 2,
        .storage_backend = .lsm,
    }, .{}));

    // A persisted retry does not reset its consecutive-failure backoff across
    // executor passes or process restart.
    try ArtifactRepair.update_index_repair_intent(&reopened, alloc, repair_id, .{ .next_retry_at_ms = 0 });
    const second = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(second.attempted);
    try std.testing.expect(second.deferred);
    var retried = try ArtifactRepair.load_index_repair_entry_by_id(&reopened, alloc, repair_id);
    defer retried.deinit(alloc);
    try std.testing.expectEqual(@as(u32, 2), retried.intent.attempt_count);
    try std.testing.expectEqual(@as(u32, 2), retried.intent.failure_streak);
    try std.testing.expect(retried.intent.next_retry_at_ms > first_retry_at_ms);
}

test "db generation repair dense repair durably yields and resumes a reopenable building candidate" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    artifact_repair_mod.test_dense_repair_rebuild_batch_size = 1;
    defer artifact_repair_mod.test_dense_repair_rebuild_batch_size = null;
    artifact_repair_mod.test_index_repair_catch_up_max_records_per_window = 1;
    defer artifact_repair_mod.test_index_repair_catch_up_max_records_per_window = null;

    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"bravo\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
                .{ .key = "doc:c", .value = "{\"title\":\"charlie\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" },
                .{ .key = "doc:d", .value = "{\"title\":\"delta\",\"_embeddings\":{\"dense_idx\":[1,1,0]}}" },
            },
            .sync_level = .full_index,
        });
    }

    const active_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ std.mem.span(path), cfg.name });
    defer alloc.free(active_path);
    const active_path_z = try alloc.dupeZ(u8, active_path);
    defer alloc.free(active_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, active_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    const Yield = struct {
        fn requested(_: *anyopaque) bool {
            return true;
        }
    };
    var yield_token: u8 = 0;
    const options = types.ArtifactRepairRunOptions{
        .yield_check = .{ .ptr = &yield_token, .is_requested = Yield.requested },
        // This test covers durable yield/resume checkpoints, not the default
        // activation pause SLA. Leave enough headroom for contended CI hosts.
        .max_activation_pause_ms = 5_000,
    };
    var repair_id: u128 = 0;
    var candidate_path: []u8 = undefined;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexRepairIdForIndex(alloc, cfg.name)) orelse return error.TestUnexpectedResult;
        const first = try db.advanceIndexRepairIntent(alloc, repair_id, options);
        try std.testing.expect(first.attempted);
        try std.testing.expect(first.busy);
        try std.testing.expect(!first.repaired);
        try std.testing.expectEqual(@as(u64, 1), first.documents_reprocessed);

        var state = try db.loadIndexRepairState(alloc);
        defer state.deinit(alloc);
        const intent = state.entries.items[0].intent;
        try std.testing.expectEqual(index_repair_state.Phase.building, intent.phase);
        try std.testing.expect(intent.build_resume_key != null);
        try std.testing.expectEqual(@as(u64, 1), intent.build_reprocessed);
        try std.testing.expectEqual(@as(u32, 0), intent.failure_streak);
        candidate_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ std.mem.span(path), intent.candidate_relative_path.? });

        // Mutate both sides of the saved source cursor. The new key sorts
        // before the first artifact slice and must arrive through replay; the
        // deleted key sorts after it and may disappear from a later snapshot.
        // Final fenced coverage must still converge without restarting the
        // candidate from zero.
        try db.batch(.{
            .writes = &.{.{ .key = "doc:0", .value = "{\"title\":\"zero\",\"_embeddings\":{\"dense_idx\":[1,0,1]}}" }},
            .sync_level = .write,
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"bravo-updated\",\"_embeddings\":{\"dense_idx\":[0,1,1]}}" }},
            .sync_level = .write,
        });
        try db.batch(.{ .deletes = &.{"doc:d"}, .sync_level = .write });
    }
    defer alloc.free(candidate_path);

    // A yielded building candidate has no incomplete-publication marker and is
    // independently reopenable before the next process resumes its cursor.
    const candidate_path_z = try alloc.dupeZ(u8, candidate_path);
    defer alloc.free(candidate_path_z);
    {
        var candidate = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, candidate_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        candidate.close();
    }

    var documents_reprocessed: u64 = 1;
    var observed_catch_up_yield = false;
    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();
        for (0..16) |_| {
            const step = try reopened.advanceIndexRepairIntent(alloc, repair_id, options);
            documents_reprocessed += step.documents_reprocessed;
            try std.testing.expect(!step.repaired);
            try std.testing.expect(step.busy);
            try std.testing.expect(!step.terminal);
            var state = try ArtifactRepair.load_index_repair_entry_by_id(&reopened, alloc, repair_id);
            defer state.deinit(alloc);
            if (state.intent.phase == .catching_up) {
                try std.testing.expect(state.intent.candidate_applied_sequence > state.intent.build_floor_sequence);
                observed_catch_up_yield = true;
                break;
            }
        }
    }
    try std.testing.expect(observed_catch_up_yield);

    // Reopen from the durable replay checkpoint, proving a yielded catch-up
    // turn is independently restartable just like a yielded source scan.
    var final_reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer final_reopened.close();
    var repaired = false;
    for (0..16) |_| {
        const step = try final_reopened.advanceIndexRepairIntent(alloc, repair_id, options);
        documents_reprocessed += step.documents_reprocessed;
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(step.busy);
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    // Three source artifacts were scanned (the fourth was concurrently
    // deleted); doc:0 arrived through replay and is intentionally not counted
    // as snapshot-reprocessed work.
    try std.testing.expectEqual(@as(u64, 3), documents_reprocessed);
    try std.testing.expect(!try final_reopened.hasPendingIndexRepairIntents(alloc));

    var result = try final_reopened.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 4 } },
        .limit = 4,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 4), result.total_hits);
    var found_replayed_insert = false;
    for (result.hits) |hit| {
        found_replayed_insert = found_replayed_insert or std.mem.eql(u8, hit.id, "doc:0");
        try std.testing.expect(!std.mem.eql(u8, hit.id, "doc:d"));
    }
    try std.testing.expect(found_replayed_insert);
}

test "db generation repair dense repair uses resource manager capacity admission before building" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try ArtifactRepair.create_operator_generation_repair_intent(&db, alloc, cfg.*, 0, 0);
    const deferred = try db.advanceIndexRepairIntent(alloc, repair_id, .{
        .capacity_domain_id = 77,
        .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
    });
    try std.testing.expect(deferred.deferred);
    try std.testing.expect(deferred.disk_wait);
    try std.testing.expect(!deferred.attempted);

    var entry = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    try std.testing.expect(entry.intent.candidate_relative_path == null);
    try std.testing.expectEqualStrings("disk_admission_unavailable", entry.intent.last_error.?);
    const first_estimate = entry.intent.estimated_candidate_bytes;
    const first_reservation = entry.intent.planned_disk_bytes;
    entry.deinit(alloc);
    const admission = db.core.index_manager.resource_manager.?.capacityStats();
    try std.testing.expectEqual(@as(u64, 0), admission.reserved_bytes);
    try std.testing.expectEqual(@as(u64, 1), admission.denials);

    // A durable intent must refresh upward when the live generation or the
    // scheduler's estimate grows between retries.
    try ArtifactRepair.update_index_repair_intent(&db, alloc, repair_id, .{ .next_retry_at_ms = 0 });
    const larger_estimate = first_estimate +| 1024 * 1024 * 1024;
    const retried = try db.advanceIndexRepairIntent(alloc, repair_id, .{
        .estimated_candidate_bytes = larger_estimate,
        .capacity_domain_id = 77,
        .capacity_observation = .{ .available_bytes = 1, .capacity_bytes = 1 },
    });
    try std.testing.expect(retried.disk_wait);
    var refreshed = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    defer refreshed.deinit(alloc);
    try std.testing.expectEqual(larger_estimate, refreshed.intent.estimated_candidate_bytes);
    try std.testing.expect(refreshed.intent.planned_disk_bytes > first_reservation);
}

test "db generation repair dense shadow activation rejects surplus candidate coverage" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" },
        },
        .sync_level = .full_index,
    });
    const active_pointer_before = try db.core.index_manager.captureActiveIndexRootPointer("dense_idx");
    defer if (active_pointer_before) |pointer| alloc.free(pointer);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;

    const CoverageHook = struct {
        fn afterSnapshot(_: *anyopaque, hooked_db: *DB, index_name: []const u8, _: u64) !void {
            const key = try DerivedAsync.dense_artifact_target_counter_key_alloc(hooked_db.alloc, index_name);
            defer hooked_db.alloc.free(key);
            var reduced: [8]u8 = undefined;
            std.mem.writeInt(u64, &reduced, 1, .little);
            try hooked_db.core.store.put(key, &reduced);
        }
    };
    var hook_context: u8 = 0;
    db.shadow_index_repair_hook = .{
        .ptr = &hook_context,
        .after_snapshot_build = CoverageHook.afterSnapshot,
    };
    const outcome = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    db.shadow_index_repair_hook = null;
    try std.testing.expect(outcome.attempted);
    try std.testing.expect(outcome.deferred);
    try std.testing.expect(!outcome.terminal);
    try std.testing.expect(!outcome.repaired);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));

    // Restore the deliberately altered source metadata. The failed candidate
    // must not have replaced the previous healthy two-vector generation.
    const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    var restored: [8]u8 = undefined;
    std.mem.writeInt(u64, &restored, 2, .little);
    try db.core.store.put(counter_key, &restored);
    try std.testing.expectEqual(
        @as(u64, 2),
        db.core.index_manager.denseIndex("dense_idx").?.index.stats().active_count,
    );
    const active_pointer_after = try db.core.index_manager.captureActiveIndexRootPointer("dense_idx");
    defer if (active_pointer_after) |pointer| alloc.free(pointer);
    try std.testing.expectEqual(active_pointer_before == null, active_pointer_after == null);
    if (active_pointer_before) |before| {
        try std.testing.expectEqualStrings(before, active_pointer_after.?);
    }

    // Coverage failure makes this snapshot candidate insufficient. The next
    // turn discards it; the following turn rebuilds from authoritative source.
    const reset = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reset.attempted);
    try std.testing.expect(reset.deferred);
    var reset_entry = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Phase.preflight, reset_entry.intent.phase);
    try std.testing.expect(reset_entry.intent.candidate_relative_path == null);
    try std.testing.expect(reset_entry.intent.last_error == null);
    try std.testing.expectEqual(@as(u32, 0), reset_entry.intent.failure_streak);
    reset_entry.deinit(alloc);

    const recovered = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(recovered.attempted);
    try std.testing.expect(recovered.repaired);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair durable root incarnation follows the physical root rather than visibility generations" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var first_incarnation: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        first_incarnation = try db.durableRootIncarnation();
        try std.testing.expect(first_incarnation != 0);
    }
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try std.testing.expectEqual(first_incarnation, try db.durableRootIncarnation());
    }
    const repair_checkpoint_path = try std.fmt.allocPrint(alloc, "{s}/index_repair.checkpoint", .{std.mem.span(path)});
    defer alloc.free(repair_checkpoint_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = repair_checkpoint_path, .data = "corrupt-derived-state" });
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try std.testing.expectEqual(first_incarnation, try db.durableRootIncarnation());
        try std.testing.expect(db.async_context.index_repair_state_corrupt.load(.acquire));
    }
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 2,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try std.testing.expectEqual(first_incarnation, try db.durableRootIncarnation());
    }

    var transition = try generation_lifecycle.beginProcessExclusive(std.mem.span(path));
    defer transition.deinit();
    var staged = try transition.beginStaging();
    defer staged.deinit();
    const retired_path = try alloc.dupeZ(u8, staged.path());
    defer {
        cleanupTempDir(retired_path);
        alloc.free(retired_path);
    }
    var staged_incarnation: u128 = 0;
    {
        var db = try DB.open(alloc, staged.path(), .{
            .lsm_root_generation = 2,
            .staged_generation = &staged,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        staged_incarnation = try db.durableRootIncarnation();
        try std.testing.expect(staged_incarnation != first_incarnation);
    }
    _ = try staged.publish();
    transition.deinit();
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 3,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try std.testing.expectEqual(staged_incarnation, try db.durableRootIncarnation());
    }
}

test "db generation repair empty inline dense generation finalizes without scanning primary documents" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    try db.addIndex(config);
    try db.core.saveProjectionCheckpoint(config.name, .{
        .status = .rebuilding,
        .generation = 3,
        .config_hash = types.indexConfigHash(config),
    });

    try std.testing.expect(try DerivedAsync.finalize_covered_dense_projection_checkpoints_if_idle(db.async_context));
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 4), checkpoint.generation);
}

test "db generation repair external dense coverage tracks exact writes deletes and reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const ExpectCoverage = struct {
        fn run(db_value: *DB, source: u64, produced: u64) !void {
            const stats = try db_value.stats(std.testing.allocator);
            defer types.freeDBStats(std.testing.allocator, stats);
            try std.testing.expectEqual(source, stats.source_doc_count);
            for (stats.indexes) |index_stats| {
                if (!std.mem.eql(u8, index_stats.name, "external_v1")) continue;
                try std.testing.expectEqual(produced, index_stats.coverage_produced_count);
                try std.testing.expectEqual(@as(u64, 0), index_stats.coverage_skipped_count);
                return;
            }
            return error.IndexNotFound;
        }
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{
            .name = "external_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try std.testing.expect(db.core.index_manager.denseIndexUsesExternalCoverage("external_v1"));

        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:covered", .value = "{\"title\":\"covered\",\"_embeddings\":{\"external_v1\":[1,0,0]}}" },
                .{ .key = "doc:pending", .value = "{\"title\":\"pending\"}" },
            },
            .sync_level = .full_index,
        });
        try ExpectCoverage.run(&db, 2, 1);

        try db.batch(.{ .writes = &.{.{ .key = "doc:covered", .value = "{\"title\":\"vector removed\"}" }}, .sync_level = .full_index });
        try ExpectCoverage.run(&db, 2, 0);
        try db.batch(.{ .writes = &.{.{ .key = "doc:pending", .value = "{\"_embeddings\":{\"external_v1\":[0,1,0]}}" }}, .sync_level = .full_index });
        try ExpectCoverage.run(&db, 2, 1);
    }

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{});
        defer reopened.close();
        try ExpectCoverage.run(&reopened, 2, 1);
        try reopened.batch(.{ .deletes = &.{"doc:pending"}, .sync_level = .full_index });
        try ExpectCoverage.run(&reopened, 1, 0);
    }
}

test "db generation repair external dense ingest finalizes an exactly covered rebuilding checkpoint" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const dense_cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        .coverage_generation = 42,
    };
    try db.addIndex(dense_cfg);
    try db.core.saveProjectionCheckpoint("dense_idx", .{
        .status = .rebuilding,
        .config_hash = types.indexConfigHash(dense_cfg),
    });

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
        .sync_level = .full_index,
    });

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expect(checkpoint.applied_sequence > 0);
    try std.testing.expectEqual(@as(u64, 1), checkpoint.generation);
}

test "db generation repair failed activated dense generation rolls back to retained predecessor" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var repair_id: u128 = 0;
    var candidate_relative_path: ?[]u8 = null;
    defer if (candidate_relative_path) |value| alloc.free(value);
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });

        // Activate one healthy shadow first so the fault below proves that a
        // later failed generation can roll back to a non-canonical predecessor
        // pointer, not only to the original canonical root.
        var first_generation = try db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
            .force = true,
        });
        first_generation.deinit(alloc);
        const first_generation_pointer = (try db.core.index_manager.captureActiveIndexRootPointer("dense_idx")) orelse
            return error.TestUnexpectedResult;
        defer alloc.free(first_generation_pointer);

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashBeforeReplacementValidation;
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(error.TestCrashBeforeReplacementValidation, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
            .force = true,
        }));
        db.shadow_index_repair_hook = null;
        repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var interrupted = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Phase.activating, interrupted.intent.phase);
        try std.testing.expect(interrupted.intent.previous_pointer_captured);
        try std.testing.expectEqualStrings(first_generation_pointer, interrupted.intent.previous_active_relative_path.?);
        candidate_relative_path = try alloc.dupe(u8, interrupted.intent.candidate_relative_path.?);
    }

    // Remove only the newly active candidate. A pointer-selected root without
    // its ready manifest must fail closed instead of being recreated empty.
    const candidate_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ std.mem.span(path), candidate_relative_path.? });
    defer alloc.free(candidate_path);
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteTree(io_impl.io(), candidate_path);
    try ensureDirPath(candidate_path);

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") != null);
    try std.testing.expect(try ArtifactRepair.rollback_unavailable_activated_index_repair(&reopened, alloc, repair_id));
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);

    // The previous generation is immediately serviceable after rollback,
    // before another corpus rebuild is attempted.
    var previous = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer previous.deinit();
    try std.testing.expectEqual(@as(u32, 1), previous.total_hits);
    try std.testing.expectEqualStrings("doc:a", previous.hits[0].id);

    const rebuilt = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(rebuilt.attempted);
    try std.testing.expect(rebuilt.repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair forced dense repair keeps structurally invalid generation fail closed" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true,\"leaf_size\":2}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}" },
            .{ .key = "doc:b", .value = "{\"_embeddings\":{\"dense_idx\":[0,1]}}" },
            .{ .key = "doc:c", .value = "{\"_embeddings\":{\"dense_idx\":[0.9,0.1]}}" },
            .{ .key = "doc:d", .value = "{\"_embeddings\":{\"dense_idx\":[0.1,0.9]}}" },
        },
        .sync_level = .full_index,
    });
    const dense_entry = db.core.index_manager.denseIndex("dense_idx").?;
    var read_txn = try dense_entry.index.beginReadTxn();
    var root = try dense_entry.index.loadNode(&read_txn, dense_entry.index.metadata.root_node);
    read_txn.abort();
    defer root.deinit(alloc);
    try std.testing.expect(!root.is_leaf);
    try dense_entry.index.deleteNodeHeaderForTest(root.children[0]);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;

    const AfterInvalidSnapshot = struct {
        fn run(_: *anyopaque, hook_db: *DB, _: []const u8, _: u64) !void {
            const current_id = (try hook_db.indexRepairIdForIndex(hook_db.alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
            var current = try ArtifactRepair.load_index_repair_entry_by_id(hook_db, hook_db.alloc, current_id);
            defer current.deinit(hook_db.alloc);
            try std.testing.expectEqual(index_repair_state.Trigger.projection_generation_invalid, current.intent.trigger);
            try std.testing.expectError(error.IndexRebuilding, hook_db.search(hook_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            }));
            return error.TestStopAfterInvalidOperatorGenerationValidation;
        }
    };
    var hook_ctx: u8 = 0;
    db.shadow_index_repair_hook = .{ .ptr = &hook_ctx, .after_snapshot_build = AfterInvalidSnapshot.run };
    try std.testing.expectError(
        error.TestStopAfterInvalidOperatorGenerationValidation,
        db.advanceIndexRepairIntent(alloc, repair_id, .{}),
    );
    db.shadow_index_repair_hook = null;
}

test "db generation repair forced dense repair stays fail closed until background health proof" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
        .sync_level = .full_index,
    });

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var pending = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.operator_generation_validation, pending.intent.trigger);
    pending.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
    }));

    const AfterValidatedSnapshot = struct {
        fn run(_: *anyopaque, hook_db: *DB, _: []const u8, _: u64) !void {
            const current_id = (try hook_db.indexRepairIdForIndex(hook_db.alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
            var current = try ArtifactRepair.load_index_repair_entry_by_id(hook_db, hook_db.alloc, current_id);
            defer current.deinit(hook_db.alloc);
            try std.testing.expectEqual(index_repair_state.Trigger.operator_generation_rebuild, current.intent.trigger);
            var result = try hook_db.search(hook_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            });
            defer result.deinit();
            try std.testing.expectEqual(@as(u32, 1), result.total_hits);
            return error.TestStopAfterOperatorGenerationValidation;
        }
    };
    var hook_ctx: u8 = 0;
    db.shadow_index_repair_hook = .{ .ptr = &hook_ctx, .after_snapshot_build = AfterValidatedSnapshot.run };
    try std.testing.expectError(
        error.TestStopAfterOperatorGenerationValidation,
        db.advanceIndexRepairIntent(alloc, repair_id, .{}),
    );
    db.shadow_index_repair_hook = null;
}

test "db generation repair forced generation repair completion is crash idempotent before api acknowledgement" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try ArtifactRepair.create_operator_generation_repair_intent(&db, alloc, cfg.*, 42, 1_234_567);
    inline for ([_]index_repair_state.Phase{
        .preflight,
        .building,
        .catching_up,
        .ready,
        .activating,
        .validating,
        .cleanup,
    }) |phase| try ArtifactRepair.update_index_repair_intent(&db, alloc, repair_id, .{ .phase = phase });
    try SchemaRuntime.remove_index_repair_intent_and_pin(&db, alloc, repair_id);

    // Simulate an API process crash after the group completed but before its
    // repair job recorded the pass. Redispatch of the stable job identity must
    // observe completion instead of creating a second full generation.
    const replayed = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 42,
        .repair_job_created_at_ms = 1_234_567,
    });
    try std.testing.expectEqual(@as(u64, 1), replayed.repaired);
    try std.testing.expectEqual(@as(u64, 1), replayed.indexes_rebuilt);
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_idx")) == null);
}

test "db generation repair forced repair attaches to automatic generation intent idempotently" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    const cfg = db.core.index_manager.get("dense_idx") orelse return error.TestUnexpectedResult;
    const repair_id = try ArtifactRepair.ensure_automatic_dense_generation_repair_intent(
        &db,
        alloc,
        cfg.*,
        .artifact_coverage_mismatch,
        "dense_artifact_coverage_surplus",
    );

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 77,
        .repair_job_created_at_ms = 8_765_432,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);

    var attached = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.artifact_coverage_mismatch, attached.intent.trigger);
    try std.testing.expectEqual(@as(u64, 77), attached.intent.operator_job_id);
    try std.testing.expectEqual(@as(u64, 8_765_432), attached.intent.operator_job_created_at_ms);
    attached.deinit(alloc);

    try std.testing.expectEqual(repair_id, try ArtifactRepair.ensure_automatic_dense_generation_repair_intent(
        &db,
        alloc,
        cfg.*,
        .projection_generation_invalid,
        "dense_projection_structure_invalid",
    ));
    var promoted = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.projection_generation_invalid, promoted.intent.trigger);
    try std.testing.expectEqual(@as(u64, 77), promoted.intent.operator_job_id);
    promoted.deinit(alloc);
    try std.testing.expect(db.core.index_manager.repairUnavailable("dense_idx"));

    inline for ([_]index_repair_state.Phase{
        .preflight,
        .building,
        .catching_up,
        .ready,
        .activating,
        .validating,
        .cleanup,
    }) |phase| try ArtifactRepair.update_index_repair_intent(&db, alloc, repair_id, .{ .phase = phase });
    try SchemaRuntime.remove_index_repair_intent_and_pin(&db, alloc, repair_id);

    const replayed = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 77,
        .repair_job_created_at_ms = 8_765_432,
    });
    try std.testing.expectEqual(@as(u64, 1), replayed.repaired);
    try std.testing.expectEqual(@as(u64, 1), replayed.indexes_rebuilt);
    try std.testing.expect((try db.indexRepairIdForIndex(alloc, "dense_idx")) == null);
}

test "db generation repair forced repair preserves missing-counter fail-closed classification" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0]}}",
        }},
        .sync_level = .full_index,
    });
    const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, "dense_idx");
    defer alloc.free(counter_key);
    try db.core.store.delete(counter_key);

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
        .repair_job_id = 88,
        .repair_job_created_at_ms = 9_876_543,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);

    const repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var intent = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.artifact_counter_missing, intent.intent.trigger);
    try std.testing.expectEqual(@as(u64, 88), intent.intent.operator_job_id);
    intent.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
}

test "db generation repair generated artifact cleanup retries beyond the transient burst" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    const cfg: types.IndexConfig = .{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(cfg);

    index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(12, .release);
    defer index_manager_mod.test_generated_artifact_cleanup_failures_remaining.store(0, .release);
    try std.testing.expect(try db.deleteIndex(cfg.name));
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    try std.testing.expectEqual(
        @as(u32, 0),
        index_manager_mod.test_generated_artifact_cleanup_failures_remaining.load(.acquire),
    );
    try db.addIndex(cfg);
}

test "db generation repair generated artifact finalization releases page arbitration and preserves admission fence" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    const first: types.IndexConfig = .{
        .name = "a_full_text",
        .kind = .full_text,
        .config_json = "{}",
    };
    const second: types.IndexConfig = .{
        .name = "b_full_text",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(first);
    try db.addIndex(second);

    ArtifactRepair.test_generated_artifact_finalization_entered.store(false, .release);
    ArtifactRepair.test_release_generated_artifact_finalization.store(false, .release);
    ArtifactRepair.test_block_generated_artifact_finalization.store(true, .release);
    defer {
        ArtifactRepair.test_release_generated_artifact_finalization.store(true, .release);
        ArtifactRepair.test_block_generated_artifact_finalization.store(false, .release);
    }
    try std.testing.expect(try db.deleteIndex(first.name));
    var wait_attempts: usize = 0;
    while (!ArtifactRepair.test_generated_artifact_finalization_entered.load(.acquire)) : (wait_attempts += 1) {
        try std.testing.expect(wait_attempts < 100_000);
        std.Thread.yield() catch {};
    }

    try std.testing.expect(db.async_context.index_artifact_cleanup_mutex.tryLock());
    db.async_context.index_artifact_cleanup_mutex.unlock();
    try std.testing.expectError(error.IndexArtifactCleanupPending, db.addIndex(first));

    try std.testing.expect(try db.deleteIndex(second.name));
    try std.testing.expectEqual(
        DB.GeneratedArtifactCleanupAdvanceResult.progressed,
        try db.advanceGeneratedArtifactCleanupPage(second.name),
    );

    ArtifactRepair.test_release_generated_artifact_finalization.store(true, .release);
    db.backend_runtime.durable_jobs.drainOwner(db.repair_cleanup_owner_id);

    var second_drained = false;
    for (0..1_000) |_| {
        switch (try db.advanceGeneratedArtifactCleanupPage(second.name)) {
            .idle => {
                second_drained = true;
                break;
            },
            .progressed => {},
            .busy => std.Thread.yield() catch {},
        }
    }
    try std.testing.expect(second_drained);
    try db.addIndex(first);
    try db.addIndex(second);
}

test "db generation repair healthy dense generation remains searchable until replacement activation" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1]}}" },
        },
        .sync_level = .full_index,
    });

    const StopBeforeActivation = struct {
        fn afterSnapshot(_: *anyopaque, hook_db: *DB, _: []const u8, _: u64) !void {
            var result = try hook_db.search(hook_db.alloc, .{
                .index_name = "dense_idx",
                .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
                .limit = 2,
            });
            defer result.deinit();
            try std.testing.expectEqual(@as(u32, 2), result.total_hits);
            return error.TestStopGenerationBeforeActivation;
        }
    };
    var hook_ctx: u8 = 0;
    db.shadow_index_repair_hook = .{
        .ptr = &hook_ctx,
        .after_snapshot_build = StopBeforeActivation.afterSnapshot,
    };
    try std.testing.expectError(error.TestStopGenerationBeforeActivation, db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    }));
    db.shadow_index_repair_hook = null;

    const generation_repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
    var generation_state = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, generation_repair_id);
    try std.testing.expectEqual(index_repair_state.Trigger.operator_generation_rebuild, generation_state.intent.trigger);
    try std.testing.expectEqual(index_repair_state.Phase.catching_up, generation_state.intent.phase);
    try std.testing.expect(generation_state.pin != null);
    try std.testing.expect(generation_state.intent.candidate_relative_path != null);
    generation_state.deinit(alloc);
    {
        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        const dense_stats = for (stats.indexes) |item| {
            if (std.mem.eql(u8, item.name, "dense_idx")) break item;
        } else return error.TestUnexpectedResult;
        try std.testing.expect(dense_stats.backfill_active);
        try std.testing.expect(!dense_stats.repair_degraded);
    }

    {
        var result = try db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
            .limit = 2,
        });
        defer result.deinit();
        try std.testing.expectEqual(@as(u32, 2), result.total_hits);
        try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
    }

    var rebuilt = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
        .force = true,
    });
    defer rebuilt.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), rebuilt.indexes_rebuilt);
    // The interrupted explicit generation is durable and resumable; the
    // second request catches up and activates it without rescanning the corpus.
    try std.testing.expectEqual(@as(u64, 0), rebuilt.reprocessed);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db generation repair inline dense coverage counters rebase with range ownership" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0]}" },
            .{ .key = "doc:z", .value = "{\"embedding\":[0,1,0]}" },
        },
        .sync_level = .full_index,
    });

    try std.testing.expectEqual(@as(?u64, 2), try DerivedAsync.dense_target_count_for_index_context(db.async_context, config.name));
    try db.updateRange(.{ .start = "", .end = "doc:m" });
    try std.testing.expectEqual(@as(?u64, 1), try DerivedAsync.dense_target_count_for_index_context(db.async_context, config.name));
    try std.testing.expectEqual(@as(?u64, 1), try range_cardinality.load(alloc, db.core.store));
    try std.testing.expectEqual(@as(u64, 2), (try doc_identity.fastStatsFromStore(db.core.store)).live_ordinals);

    // Expanding ownership is the merge-side topology transition. Rebase sees
    // already-present donor rows and republishes the compact local summaries
    // in the same durable batch as the new range descriptor.
    try db.updateRange(.{ .start = "", .end = "" });
    try std.testing.expectEqual(@as(?u64, 2), try DerivedAsync.dense_target_count_for_index_context(db.async_context, config.name));
    try std.testing.expectEqual(@as(?u64, 2), try range_cardinality.load(alloc, db.core.store));
}

test "db generation repair inline dense generation remains rebuilding until outcomes cover the live corpus" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\"}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"embedding\":[1,0,0]}" },
            .{ .key = "doc:b", .value = "{\"title\":\"no vector\"}" },
        },
        .sync_level = .full_index,
    });

    const generation = db.core.index_manager.coverageGenerationForIndex(config.name) orelse
        return error.TestUnexpectedResult;
    const CounterFixture = struct {
        fn write(
            fixture_alloc: Allocator,
            store: *docstore_mod.DocStore,
            index_name: []const u8,
            coverage_generation: u64,
            counts: [3]u64,
        ) !void {
            const outcome_names = [_][]const u8{ "produced", "skipped", "terminal_failed" };
            var keys: [outcome_names.len][]u8 = undefined;
            var initialized: usize = 0;
            defer for (keys[0..initialized]) |key| fixture_alloc.free(key);
            var values: [outcome_names.len][8]u8 = undefined;
            var writes: [outcome_names.len]docstore_mod.KVPair = undefined;
            for (outcome_names, counts, 0..) |outcome, count, i| {
                keys[i] = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(
                    fixture_alloc,
                    index_name,
                    coverage_generation,
                    outcome,
                );
                initialized += 1;
                writes[i] = .{
                    .key = keys[i],
                    .value = internal_keys.encodeDerivedCoverageOutcomeCount(&values[i], count),
                };
            }
            try store.putBatch(&writes, &.{});
        }
    };

    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.core.saveProjectionCheckpoint(config.name, .{
        .applied_sequence = applied,
        .status = .rebuilding,
        .generation = 9,
        .config_hash = types.indexConfigHash(config),
    });

    // One indexed source and one unaccounted source is a normal bounded-replay
    // intermediate state. It must not be confused with complete coverage.
    try CounterFixture.write(alloc, db.core.store, config.name, generation, .{ 1, 0, 0 });
    try std.testing.expect(!try DerivedAsync.finalize_covered_dense_projection_checkpoints_if_idle(db.async_context));
    var checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.rebuilding, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 9), checkpoint.generation);

    // Corrupt counters fail explicitly instead of wrapping into a false proof.
    try CounterFixture.write(alloc, db.core.store, config.name, generation, .{ std.math.maxInt(u64), 1, 0 });
    try std.testing.expectError(
        error.InvalidDerivedCoverageCounter,
        DerivedAsync.dense_target_count_for_index_context(db.async_context, config.name),
    );

    // Once every live source has a terminal outcome, the maintained summaries
    // prove coverage without a primary-document scan.
    try CounterFixture.write(alloc, db.core.store, config.name, generation, .{ 1, 1, 0 });
    try std.testing.expect(try DerivedAsync.finalize_covered_dense_projection_checkpoints_if_idle(db.async_context));
    checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 10), checkpoint.generation);
}

test "db generation repair last dense catch-up lease finalizes every covered rebuilding generation" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const configs = [_]types.IndexConfig{
        .{
            .name = "dense_a",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        },
        .{
            .name = "dense_b",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
        },
    };
    for (configs) |config| try db.addIndex(config);
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_a\":[1,0,0],\"dense_b\":[0,1,0]}}",
        }},
        .sync_level = .full_index,
    });

    for (configs) |config| {
        const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, config.name);
        defer alloc.free(counter_key);
        var counter_value: [8]u8 = undefined;
        std.mem.writeInt(u64, &counter_value, 1, .little);
        try db.core.store.put(counter_key, &counter_value);

        const applied = try db.core.loadAppliedSequence(alloc, config.name);
        try db.core.saveProjectionCheckpoint(config.name, .{
            .applied_sequence = applied,
            .status = .rebuilding,
            .generation = 7,
            .config_hash = types.indexConfigHash(config),
        });
        try db_internal.beginDenseCatchUpSessionTracked(db.async_context, config.name);
    }

    db_internal.finishDenseCatchUpSessionTracked(db.async_context, configs[0].name);
    try std.testing.expect(!try DerivedAsync.finalize_covered_dense_projection_checkpoints_if_idle(db.async_context));
    try std.testing.expect(try DerivedAsync.finish_dense_catch_up_session_tracked_and_finalize(db.async_context, configs[1].name));

    for (configs) |config| {
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
        try std.testing.expectEqual(@as(u64, 8), checkpoint.generation);
    }
}

test "db generation repair last external dense bulk lease finalizes covered rebuilding generations" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const config: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };
    try db.addIndex(config);
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });

    const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, config.name);
    defer alloc.free(counter_key);
    var counter_value: [8]u8 = undefined;
    std.mem.writeInt(u64, &counter_value, 1, .little);
    try db.core.store.put(counter_key, &counter_value);
    const applied = try db.core.loadAppliedSequence(alloc, config.name);
    try db.core.saveProjectionCheckpoint(config.name, .{
        .applied_sequence = applied,
        .status = .rebuilding,
        .generation = 11,
        .config_hash = types.indexConfigHash(config),
    });

    try db_internal.beginExternalDenseBulkSessionTracked(db.async_context);
    try std.testing.expect(try DerivedAsync.finish_external_dense_bulk_session_tracked_and_finalize(db.async_context));

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, config.name);
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(@as(u64, 12), checkpoint.generation);
}

test "db generation repair managed admission drain preserves a generation requested during the pass" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const Hook = struct {
        passes: usize = 0,

        fn afterPass(ptr: *anyopaque, hooked_db: *DB) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.passes += 1;
            if (self.passes == 1) SchemaRuntime.request_managed_admission_materialization(hooked_db);
        }
    };
    var hook = Hook{};
    ArtifactRepair.test_managed_admission_materialization_hook.* = .{
        .ptr = &hook,
        .after_pass = Hook.afterPass,
    };
    defer ArtifactRepair.test_managed_admission_materialization_hook.* = null;

    SchemaRuntime.request_managed_admission_materialization(&db);
    try SchemaRuntime.drain_managed_index_admissions(&db, alloc);
    try std.testing.expectEqual(@as(usize, 2), hook.passes);
    try std.testing.expect(!ArtifactRepair.managed_admission_materialization_pending(&db));
}

test "db generation repair managed admission ignores stale zero identity cache" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    // Model a primary commit followed by an HA mirror failure before runtime
    // cache publication. Admission authority must remain the primary store.
    db.identity_visibility_summary_cache = .{};
    try std.testing.expect((try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) != null);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair managed admission materialization never infers debt from replay lag" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "ft", .kind = .full_text, .config_json = "{}" });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    try std.testing.expect((try db.materializeManagedIndexAdmission(alloc, "ft")) == null);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair managed admission materialization serializes with index deletion" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    _ = try SchemaRuntime.install_index_while_enrichment_quiesced(&db, .{
        .name = index_name,
        .kind = .full_text,
        .config_json = "{}",
    }, .managed_full_text);

    const Race = struct {
        db: *DB,
        entered: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),
        delete_started: std.atomic.Value(bool) = .init(false),
        delete_completed: std.atomic.Value(bool) = .init(false),
        materialize_result: ?u128 = null,
        materialize_err: ?anyerror = null,
        delete_result: bool = false,
        delete_err: ?anyerror = null,

        fn afterConfigLookup(ptr: *anyopaque, _: *DB, _: []const u8) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) std.Thread.yield() catch {};
        }

        fn materialize(self: *@This()) void {
            self.materialize_result = self.db.materializeManagedIndexAdmission(self.db.alloc, index_name) catch |err| {
                self.materialize_err = err;
                return;
            };
        }

        fn delete(self: *@This()) void {
            self.delete_started.store(true, .release);
            self.delete_result = self.db.deleteIndex(index_name) catch |err| {
                self.delete_err = err;
                self.delete_completed.store(true, .release);
                return;
            };
            self.delete_completed.store(true, .release);
        }
    };

    var race = Race{ .db = &db };
    ArtifactRepair.test_managed_admission_materialization_hook.* = .{
        .ptr = &race,
        .after_config_lookup = Race.afterConfigLookup,
    };
    defer ArtifactRepair.test_managed_admission_materialization_hook.* = null;

    var materialize_thread = try std.Thread.spawn(.{}, Race.materialize, .{&race});
    var entered = false;
    for (0..100_000) |_| {
        if (race.entered.load(.acquire)) {
            entered = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    if (!entered) {
        race.release.store(true, .release);
        materialize_thread.join();
        return error.TestTimeout;
    }

    var delete_thread = std.Thread.spawn(.{}, Race.delete, .{&race}) catch |err| {
        race.release.store(true, .release);
        materialize_thread.join();
        return err;
    };
    var delete_started = false;
    for (0..100_000) |_| {
        if (race.delete_started.load(.acquire)) {
            delete_started = true;
            break;
        }
        std.Thread.yield() catch {};
    }
    if (!delete_started) {
        race.release.store(true, .release);
        materialize_thread.join();
        delete_thread.join();
        return error.TestTimeout;
    }
    for (0..256) |_| std.Thread.yield() catch {};
    const deletion_crossed_materialization = race.delete_completed.load(.acquire);
    race.release.store(true, .release);
    materialize_thread.join();
    delete_thread.join();

    try std.testing.expect(!deletion_crossed_materialization);
    try std.testing.expect(race.materialize_err == null);
    try std.testing.expect(race.materialize_result != null);
    try std.testing.expect(race.delete_err == null);
    try std.testing.expect(race.delete_result);
    try std.testing.expect(db.core.index_manager.get(index_name) == null);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair managed admission reconciliation retains same-name catalog mismatches" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    const index_name = "dense_idx";
    try db.addIndex(.{
        .name = index_name,
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":2,\"external\":true}",
    });

    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
    defer alloc.free(admission_key);
    const marker = ArtifactRepair.encode_managed_index_admission_marker(.{
        .config_hash = 1,
        .source_doc_count = 1,
        .identity_generation = 0,
        .replay_target_sequence = db.core.nextDerivedSequence(),
    });
    try db.core.store.put(admission_key, &marker);
    SchemaRuntime.request_managed_admission_materialization(&db);

    try std.testing.expectError(error.InvalidManagedIndexAdmission, SchemaRuntime.drain_managed_index_admissions(&db, alloc));
    const retained = try db.core.store.get(alloc, admission_key);
    defer alloc.free(retained);
    try std.testing.expectEqualSlices(u8, &marker, retained);
    try std.testing.expect(ArtifactRepair.managed_admission_materialization_pending(&db));
}

test "db generation repair managed admission rejects regressed identity evidence" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });
    const cfg = types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    _ = (try db.admitManagedFullTextIndex(cfg)) orelse return error.TestUnexpectedResult;

    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    const raw = try db.core.store.get(alloc, admission_key);
    defer alloc.free(raw);
    var marker = try ArtifactRepair.decode_managed_index_admission_marker(raw);
    marker.identity_generation += 1;
    const regressed_marker = ArtifactRepair.encode_managed_index_admission_marker(marker);
    try db.core.store.put(admission_key, &regressed_marker);

    try std.testing.expectError(
        error.InvalidDocIdentity,
        db.materializeManagedIndexAdmission(alloc, cfg.name),
    );
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair managed full text admission avoids debt for an empty source" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    index_manager_mod.test_text_backfill_invocations = 0;
    defer index_manager_mod.test_text_backfill_invocations = 0;
    try std.testing.expect((try db.admitManagedFullTextIndex(.{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    })) == null);
    try std.testing.expectEqual(@as(usize, 0), index_manager_mod.test_text_backfill_invocations);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
}

test "db generation repair managed full text admission survives restart without in-place backfill" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const cfg = types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    var repair_checkpoint_path: ?[]u8 = null;
    defer if (repair_checkpoint_path) |value| alloc.free(value);
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });

        const repair_id = (try db.admitManagedFullTextIndex(cfg)) orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(repair_id, (try db.materializeManagedIndexAdmission(alloc, cfg.name)).?);
        try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
        repair_checkpoint_path = try alloc.dupe(u8, db.core.index_repair_checkpoint.?.path);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        const target = for (stats.indexes) |item| {
            if (std.mem.eql(u8, item.name, cfg.name)) break item;
        } else return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 0), target.doc_count);
    }

    // Model a crash after the atomic catalog/outbox commit but before the
    // repair checkpoint becomes durable. Reopen must reconstruct the intent
    // from the primary-store marker without running an in-place backfill.
    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try std.Io.Dir.cwd().deleteFile(io_impl.io(), repair_checkpoint_path.?);

    // A read-only/status process cannot materialize the checkpoint, but the
    // primary marker must still close service during this crash window.
    var readonly = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer readonly.close();
    try std.testing.expectError(error.IndexRebuilding, readonly.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const reopened_stats = try reopened.stats(alloc);
    defer types.freeDBStats(alloc, reopened_stats);
    const reopened_target = for (reopened_stats.indexes) |item| {
        if (std.mem.eql(u8, item.name, cfg.name)) break item;
    } else return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 0), reopened_target.doc_count);
    const repair_id = (try reopened.materializeManagedIndexAdmission(alloc, cfg.name)) orelse
        return error.TestUnexpectedResult;
    const summary = try reopened.indexRepairIntentSummary(alloc);
    try std.testing.expectEqual(@as(usize, 1), summary.runnable);

    var repaired = false;
    for (0..16) |_| {
        const step = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
        if (step.repaired) {
            repaired = true;
            break;
        }
        try std.testing.expect(!step.terminal);
    }
    try std.testing.expect(repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, cfg.name);
    defer alloc.free(admission_key);
    try std.testing.expectError(error.NotFound, reopened.core.store.get(alloc, admission_key));

    // This reader still owns the pre-activation root. Sidecar completion is
    // not permission to expose it; cache retirement and a fresh open publish
    // the repaired generation to subsequent requests.
    try std.testing.expectError(error.IndexRebuilding, readonly.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));

    var result = try reopened.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db generation repair managed visibility hook rehydrates durable repair debt once" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    const index_name = "full_text_index_v1";

    const repair_id = repair: {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
            .sync_level = .write,
        });

        // Admission happens before the managed owner installs its hook. Its
        // process-local edge is deliberately lost across this close.
        break :repair (try db.admitManagedFullTextIndex(.{
            .name = index_name,
            .kind = .full_text,
            .config_json = "{}",
        })) orelse return error.TestUnexpectedResult;
    };

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const Hook = struct {
        pending: usize = 0,

        fn onChange(
            ptr: *anyopaque,
            _: []const u8,
            _: u64,
            _: ?*DB,
            change: QueryVisibilityChange,
        ) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (change == .index_repair_pending) self.pending += 1;
        }
    };
    var hook = Hook{};
    // A clear for one intent cannot prove that all durable repair debt is
    // gone, so it must not consume a queued scheduler wakeup.
    DB.notifyQueryVisibilityHook(db.async_context, .index_repair_cleared);
    db.setQueryVisibilityHook(.{
        .ptr = &hook,
        .table_name = "docs",
        .group_id = 7001,
        .db = &db,
        .on_change = Hook.onChange,
    });
    try std.testing.expectEqual(@as(usize, 1), hook.pending);

    try std.testing.expectEqual(
        repair_id,
        (try db.materializeManagedIndexAdmission(alloc, index_name)) orelse
            return error.TestUnexpectedResult,
    );
    // Reconciliation adopts the same durable intent without creating a hot
    // loop of cache invalidations and duplicate scheduler notifications.
    try std.testing.expectEqual(@as(usize, 1), hook.pending);
}

test "db generation repair named repair advances managed full text admission without force" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    const cfg = types.IndexConfig{
        .name = "full_text_index_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    try std.testing.expect((try db.admitManagedFullTextIndex(cfg)) != null);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = cfg.name,
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

    var result = try db.search(alloc, .{
        .index_name = cfg.name,
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
}

test "db generation repair ordinary index admission remains fail closed after post-commit activation failure" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"alpha\"}" }},
        .sync_level = .write,
    });

    SchemaRuntime.test_fail_index_activation_after_catalog_commit.* = true;
    defer SchemaRuntime.test_fail_index_activation_after_catalog_commit.* = false;
    try db.addIndex(.{
        .name = "ft",
        .kind = .full_text,
        .config_json = "{}",
    });
    SchemaRuntime.test_fail_index_activation_after_catalog_commit.* = false;

    try std.testing.expect(db.core.index_manager.get("ft") != null);
    try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
    const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, "ft");
    defer alloc.free(admission_key);
    const marker_raw = try db.core.store.get(alloc, admission_key);
    defer alloc.free(marker_raw);
    const marker = try ArtifactRepair.decode_managed_index_admission_marker(marker_raw);
    try std.testing.expectEqual(ArtifactRepair.IndexAdmissionDisposition.activation_fence, marker.disposition);
    try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
        .index_name = "ft",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    }));
}

test "db generation repair paused dense repair resumes its durable candidate after restart" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeZ(u8, dense_path);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;

        const PauseHook = struct {
            fn afterSnapshot(_: *anyopaque, hook_db: *DB, index_name: []const u8, _: u64) !void {
                var control = try hook_db.repairArtifactIssuesWithRequest(hook_db.alloc, .{
                    .target = .index,
                    .index_name = index_name,
                    .control = .pause_automatic,
                });
                defer control.deinit(hook_db.alloc);
                try std.testing.expectEqual(@as(u64, 1), control.controls_applied);
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = PauseHook.afterSnapshot,
        };
        const paused = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
        db.shadow_index_repair_hook = null;
        try std.testing.expect(paused.attempted);
        try std.testing.expect(paused.deferred);
        try std.testing.expect(!paused.repaired);

        var state = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
        defer state.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Automation.paused, state.intent.automation);
        try std.testing.expectEqual(index_repair_state.Phase.catching_up, state.intent.phase);
        try std.testing.expect(state.intent.candidate_relative_path != null);
        try std.testing.expect(state.intent.estimated_candidate_bytes > 0);
        try std.testing.expect(state.intent.planned_disk_bytes >= state.intent.estimated_candidate_bytes);
        try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    var restarted_state = try ArtifactRepair.load_index_repair_entry_by_id(&reopened, alloc, repair_id);
    try std.testing.expect(restarted_state.intent.planned_disk_bytes >= restarted_state.intent.estimated_candidate_bytes);
    try std.testing.expect(restarted_state.intent.planned_disk_bytes > 0);
    restarted_state.deinit(alloc);
    var resume_control = try reopened.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "dense_idx",
        .control = .resume_automatic,
    });
    defer resume_control.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), resume_control.controls_applied);
    const resumed = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(resumed.attempted);
    try std.testing.expect(resumed.repaired);
    try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db generation repair quarantined dense bootstrap tracks concurrent insert update and delete" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);
    const cfg: types.IndexConfig = .{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(cfg);
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}" }},
            .sync_level = .full_index,
        });
        const counter_key = try DerivedAsync.dense_artifact_target_counter_key_alloc(alloc, cfg.name);
        defer alloc.free(counter_key);
        try db.core.store.delete(counter_key);
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/{s}", .{ std.mem.span(path), cfg.name });
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeZ(u8, dense_path);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{});
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectEqualStrings("IncompleteBulkPublish", reopened.core.index_manager.loadFailure(cfg.name).?);
    try std.testing.expect(reopened.core.index_manager.denseIndex(cfg.name) == null);
    _ = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    const repair_id = (try reopened.indexRepairIdForIndex(alloc, cfg.name)) orelse return error.TestUnexpectedResult;

    var bootstrap_snapshot = try ArtifactRepair.begin_dense_artifact_counter_bootstrap_snapshot(&reopened, alloc, cfg.name, repair_id);
    defer bootstrap_snapshot.deinit();

    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1,0]}}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(i64, 1),
        (try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name)).?.delta,
    );
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha-2\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" }},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(i64, 1),
        (try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name)).?.delta,
    );
    try reopened.batch(.{
        .deletes = &.{"doc:a"},
        .sync_level = .write,
    });
    try std.testing.expectEqual(
        @as(i64, 0),
        (try DB.loadDenseArtifactCounterBootstrap(alloc, reopened.core.store, cfg.name)).?.delta,
    );

    const snapshot_count = try ArtifactRepair.count_dense_artifacts_for_config_from_read_txn(
        &reopened,
        alloc,
        cfg,
        &bootstrap_snapshot.txn,
        null,
    );
    try std.testing.expectEqual(@as(u64, 1), snapshot_count);
    try ArtifactRepair.finish_dense_artifact_counter_bootstrap(
        &reopened,
        alloc,
        cfg.name,
        repair_id,
        bootstrap_snapshot.attempt_id,
        snapshot_count,
    );
    try std.testing.expectEqual(
        @as(?u64, 1),
        try DB.loadDenseArtifactTargetCounter(alloc, reopened.core.store, cfg.name),
    );

    const repaired = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(repaired.repaired);
    var result = try reopened.search(alloc, .{
        .index_name = cfg.name,
        .query = .{ .dense_knn = .{ .vector = &.{ 0, 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:b", result.hits[0].id);
}

test "db generation repair removing one repair pin preserves pressure gate for another index" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    for ([_][]const u8{ "dense_a", "dense_b" }) |name| {
        try db.addIndex(.{
            .name = name,
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
    }
    const cfg_a = db.core.index_manager.get("dense_a") orelse return error.TestUnexpectedResult;
    const cfg_b = db.core.index_manager.get("dense_b") orelse return error.TestUnexpectedResult;
    const repair_a = try ArtifactRepair.create_operator_generation_repair_intent(&db, alloc, cfg_a.*, 0, 0);
    const repair_b = try ArtifactRepair.create_operator_generation_repair_intent(&db, alloc, cfg_b.*, 0, 0);
    var snapshot_a = try ArtifactRepair.begin_pinned_index_repair_snapshot(&db, alloc, repair_a);
    snapshot_a.deinit();
    var snapshot_b = try ArtifactRepair.begin_pinned_index_repair_snapshot(&db, alloc, repair_b);
    snapshot_b.deinit();

    try SchemaRuntime.remove_index_repair_intent_and_pin(&db, alloc, repair_a);
    try std.testing.expect(db.async_context.index_repair_replay_pinned.load(.acquire));
    var remaining = try db.loadIndexRepairState(alloc);
    try std.testing.expect(remaining.minimumRetainAfterSequence() != null);
    remaining.deinit(alloc);

    try SchemaRuntime.remove_index_repair_intent_and_pin(&db, alloc, repair_b);
    try std.testing.expect(!db.async_context.index_repair_replay_pinned.load(.acquire));
}

test "db generation repair repair activation admission is time and sequence bounded" {
    try std.testing.expect(artifact_repair_mod.repairActivationAdmissible(0, 0, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(!artifact_repair_mod.repairActivationAdmissible(201, std.time.ns_per_ms, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(!artifact_repair_mod.repairActivationAdmissible(1, 0, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(artifact_repair_mod.repairActivationAdmissible(100, std.time.ns_per_ms, 200, 250 * std.time.ns_per_ms));
    try std.testing.expect(!artifact_repair_mod.repairActivationAdmissible(100, 3 * std.time.ns_per_ms, 200, 250 * std.time.ns_per_ms));

    try std.testing.expectEqual(@as(u64, 10), artifact_repair_mod.observeRepairCatchUpCost(0, 10, 20, 100));
    try std.testing.expectEqual(@as(u64, 12), artifact_repair_mod.observeRepairCatchUpCost(12, 20, 30, 100));
    try std.testing.expectEqual(@as(u64, 12), artifact_repair_mod.observeRepairCatchUpCost(12, 30, 30, 999));

    const deadline = 1_000 * std.time.ns_per_ms;
    try std.testing.expectEqual(
        @as(?u64, deadline - 50 * std.time.ns_per_ms),
        artifact_repair_mod.repairActivationReplayDeadline(700 * std.time.ns_per_ms, deadline, 250 * std.time.ns_per_ms),
    );
    try std.testing.expectEqual(
        @as(?u64, null),
        artifact_repair_mod.repairActivationReplayDeadline(998 * std.time.ns_per_ms, deadline, 10 * std.time.ns_per_ms),
    );
}

test "db generation repair repair capacity converts materialized shadow bytes into consumed reservation" {
    const alloc = std.testing.allocator;
    var db_path_buf: [256]u8 = undefined;
    const db_path = tempPath(&db_path_buf);
    defer cleanupTempDir(db_path);
    var candidate_path_buf: [256]u8 = undefined;
    const candidate_path = tempPath(&candidate_path_buf);
    defer cleanupTempDir(candidate_path);
    try ensureDirPath(std.mem.span(candidate_path));

    var db = try DB.open(alloc, std.mem.span(db_path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    const Capacity = struct {
        available: u64,

        fn observe(ptr: *anyopaque) anyerror!resource_manager_mod.CapacityObservation {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return .{
                .available_bytes = self.available,
                .capacity_bytes = 100,
                .observed_at_ns = monotonicTimeNs(),
                .valid_for_ns = 5 * std.time.ns_per_s,
            };
        }
    };
    var capacity = Capacity{ .available = 100 };
    var manager = resource_manager_mod.ResourceManager.init(.{
        .disk_safety_floor_bytes = 0,
        .disk_safety_floor_divisor = 0,
    });
    defer manager.deinit(alloc);
    var reservation = try manager.reserveCapacity(alloc, 9, 80, try Capacity.observe(&capacity), monotonicTimeNs());
    defer reservation.release();
    var guard = ArtifactRepair.RepairCapacityGuard{
        .reservation = &reservation,
        .db = &db,
        .alloc = alloc,
        .repair_id = 0,
        .options = .{ .capacity_source = .{ .ptr = &capacity, .domain_id = 9, .observe = Capacity.observe } },
        .admitted_total_bytes = 80,
        .headroom_bytes = 0,
    };
    try ArtifactRepair.RepairCapacityGuard.bindCandidateRoot(&guard, std.mem.span(candidate_path));

    const file_path = try std.fmt.allocPrint(alloc, "{s}/materialized", .{std.mem.span(candidate_path)});
    defer alloc.free(file_path);
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{
        .sub_path = file_path,
        .data = "0123456789012345678901234567890123456789",
    });
    capacity.available = 60;
    try ArtifactRepair.RepairCapacityGuard.revalidate(&guard);
    try std.testing.expectEqual(@as(u64, 40), manager.capacityStats().reserved_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.capacityStats().denials);
}

test "db generation repair repair replay pin applies hard-pressure write backpressure" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lsm_wal_retention)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var resources = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .resource_manager = &resources,
    });
    defer db.close();

    var tracked: u64 = 0;
    resources.observeUsage(.lsm_wal_retention, &tracked, 2);
    defer resources.observeUsage(.lsm_wal_retention, &tracked, 0);
    db.async_context.index_repair_replay_pinned.store(true, .release);
    try std.testing.expectError(error.DenseRepairBackpressure, db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    }));

    db.async_context.index_repair_replay_pinned.store(false, .release);
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" }},
    });
}

test "db generation repair restart automatically resumes pinned explicit dense generation rebuild" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" },
                .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"_embeddings\":{\"dense_idx\":[0,1]}}" },
            },
            .sync_level = .full_index,
        });

        const StopAtReady = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}

            fn afterPhase(_: *anyopaque, _: *DB, _: u128, phase: index_repair_state.Phase) !void {
                if (phase == .ready) return error.TestStopExplicitGenerationAtReady;
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = StopAtReady.afterSnapshot,
            .after_phase_persisted = StopAtReady.afterPhase,
        };
        try std.testing.expectError(error.TestStopExplicitGenerationAtReady, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
            .force = true,
        }));
        db.shadow_index_repair_hook = null;
        try std.testing.expect(try db.hasPendingIndexRepairIntents(alloc));
        const repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var ready = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
        defer ready.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Phase.ready, ready.intent.phase);

        // The active generation remains serviceable before restart.
        var active = try db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
            .limit = 2,
        });
        active.deinit();
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    // Reloading the durable intent must not quarantine the prior healthy
    // generation while the candidate is still in catching_up.
    var before_resume = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    before_resume.deinit();

    const resumed = try reopened.repairRecoverableStartupIndexFailures(alloc, 1, .{});
    try std.testing.expectEqual(@as(usize, 1), resumed.attempted);
    try std.testing.expectEqual(@as(usize, 1), resumed.repaired);
    try std.testing.expectEqual(@as(u64, 0), resumed.documents_reprocessed);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 2 } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db generation repair restart clears stale dense generation intent after clean checkpoint" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterCleanCheckpoint(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterCleanCheckpoint;
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_clean_checkpoint = CrashHook.afterCleanCheckpoint,
        };
        try std.testing.expectError(error.TestCrashAfterCleanCheckpoint, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
            .force = true,
        }));
        db.shadow_index_repair_hook = null;

        repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;
        var interrupted = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Phase.validating, interrupted.intent.phase);
        try std.testing.expect(interrupted.pin != null);
        try std.testing.expect(interrupted.intent.previous_pointer_captured);
        try std.testing.expect(interrupted.intent.previous_active_relative_path == null);
        try std.testing.expect(try db.core.index_manager.isRepairCandidateActive(
            "dense_idx",
            interrupted.intent.candidate_relative_path.?,
        ));
        const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "dense_idx");
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
        try std.testing.expectEqual(interrupted.intent.config_hash, checkpoint.config_hash);
        try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const reconciled = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reconciled.attempted);
    try std.testing.expect(reconciled.repaired);
    try std.testing.expectEqual(@as(u64, 0), reconciled.documents_reprocessed);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db generation repair restart reconciles activated dense repair without rebuilding" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeZ(u8, dense_path);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{});
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse return error.TestUnexpectedResult;

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterPointerActivation;
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(error.TestCrashAfterPointerActivation, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .embedding,
            .index_name = "dense_idx",
            .limit = 1,
        }));
        db.shadow_index_repair_hook = null;
        var interrupted = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Phase.activating, interrupted.intent.phase);
        try std.testing.expect(interrupted.pin != null);
        try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("dense_idx", interrupted.intent.candidate_relative_path.?));
        try std.testing.expectError(error.IndexRebuilding, db.search(alloc, .{
            .index_name = "dense_idx",
            .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
            .limit = 1,
        }));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const reconciled = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reconciled.attempted);
    try std.testing.expect(reconciled.repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db generation repair restart reconciles an activated graph repair without rebuilding" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

        const edge_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "mentions", "doc:b");
        defer alloc.free(edge_key);
        const edge_value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 0.75, 0, 0, "");
        defer alloc.free(edge_value);
        try db.core.store.put(edge_key, edge_value);

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}
            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterPointerActivation;
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(error.TestCrashAfterPointerActivation, db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .graph,
            .index_name = "graph_v1",
            .limit = 1,
            .force = true,
        }));
        db.shadow_index_repair_hook = null;
        repair_id = (try db.indexRepairIdForIndex(alloc, "graph_v1")) orelse return error.TestUnexpectedResult;
        var interrupted = try ArtifactRepair.load_index_repair_entry_by_id(&db, alloc, repair_id);
        defer interrupted.deinit(alloc);
        try std.testing.expectEqual(index_repair_state.Phase.activating, interrupted.intent.phase);
        try std.testing.expect(try db.core.index_manager.isRepairCandidateActive("graph_v1", interrupted.intent.candidate_relative_path.?));
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();
    const reconciled = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reconciled.attempted);
    try std.testing.expect(reconciled.repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    const edges = try reopened.getEdges(alloc, "graph_v1", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings("doc:b", edges[0].target);
}

test "db generation repair root generation rollover preserves activated repair debt fail closed" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var old_repair_id: u128 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        try db.addIndex(.{
            .name = "dense_idx",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":2,\"metric\":\"l2_squared\",\"external\":true}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0]}}" }},
            .sync_level = .full_index,
        });
    }

    const dense_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_path);
    const dense_path_z = try alloc.dupeZ(u8, dense_path);
    defer alloc.free(dense_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_path_z, .{
            .dims = 2,
            .storage_backend = .lsm,
        }, .{ .root_generation = 1 });
        try hbc.beginBulkIngestSession();
        hbc.close();
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{
            .lsm_root_generation = 1,
            .open_mode = .writer_no_replay,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer db.close();
        _ = try db.discoverRecoverableStartupIndexFailures(alloc, 1);
        old_repair_id = (try db.indexRepairIdForIndex(alloc, "dense_idx")) orelse
            return error.TestUnexpectedResult;

        const CrashHook = struct {
            fn afterSnapshot(_: *anyopaque, _: *DB, _: []const u8, _: u64) !void {}

            fn afterActivation(_: *anyopaque, _: *DB, _: []const u8) !void {
                return error.TestCrashAfterPointerActivation;
            }
        };
        var hook_ctx: u8 = 0;
        db.shadow_index_repair_hook = .{
            .ptr = &hook_ctx,
            .after_snapshot_build = CrashHook.afterSnapshot,
            .after_pointer_activation = CrashHook.afterActivation,
        };
        try std.testing.expectError(
            error.TestCrashAfterPointerActivation,
            db.repairArtifactIssuesWithRequest(alloc, .{
                .target = .index,
                .artifact_kind = .embedding,
                .index_name = "dense_idx",
                .limit = 1,
            }),
        );
        db.shadow_index_repair_hook = null;
    }

    var reopened = try DB.open(alloc, std.mem.span(path), .{
        .lsm_root_generation = 2,
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    }));
    const new_repair_id = (try reopened.indexRepairIdForIndex(alloc, "dense_idx")) orelse
        return error.TestUnexpectedResult;
    try std.testing.expect(new_repair_id != old_repair_id);
    var replacement = try ArtifactRepair.load_index_repair_entry_by_id(&reopened, alloc, new_repair_id);
    defer replacement.deinit(alloc);
    try std.testing.expectEqual(index_repair_state.Trigger.root_generation_rebuild, replacement.intent.trigger);
    try std.testing.expectEqual(@as(u64, 2), replacement.intent.root_generation);
    try std.testing.expect(replacement.intent.candidate_relative_path == null);

    const repaired = try reopened.advanceIndexRepairIntent(alloc, new_repair_id, .{});
    try std.testing.expect(repaired.attempted);
    try std.testing.expect(repaired.repaired);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0 }, .k = 1 } },
        .limit = 1,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 1), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db generation repair runtime status overlay refreshes identity totals with coverage counters" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "dense_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"l2_squared\",\"external\":true}",
    });
    var stale_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stale_stats);
    try std.testing.expectEqual(@as(u64, 0), stale_stats.source_doc_count);

    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"title\":\"alpha\",\"_embeddings\":{\"dense_idx\":[1,0,0]}}",
        }},
        .sync_level = .full_index,
    });
    try db.overlayRuntimeStatusConsistent(alloc, &stale_stats);
    try std.testing.expectEqual(@as(u64, 1), stale_stats.source_doc_count);
    try std.testing.expectEqual(@as(u64, 1), stale_stats.doc_identity.live_ordinals);

    const generation = stale_stats.indexes[0].coverage_generation;
    const produced_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(
        alloc,
        "dense_idx",
        generation,
        "produced",
    );
    defer alloc.free(produced_key);
    try db.core.store.put(produced_key, "invalid");

    try std.testing.expectError(
        error.InvalidDerivedCoverageOutcomeCount,
        db.overlayRuntimeStatusConsistent(alloc, &stale_stats),
    );
    db.overlayRuntimeStatusBestEffort(alloc, &stale_stats);
    try std.testing.expect(!stale_stats.indexes[0].coverage_identity_ready);
    try std.testing.expect(!stale_stats.indexes[0].coverage_summary_ready);
    try std.testing.expect(stale_stats.indexes[0].repair_degraded);
}

test "db generation repair derived coverage stats require validated config identity" {
    var unavailable = types.DBIndexStats{
        .name = "dense_v1",
        .kind = .dense_vector,
    };
    Lifecycle.initialize_derived_coverage_identity(.{
        .name = "dense_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
        .coverage_generation = 42,
    }, &unavailable);
    try std.testing.expect(!unavailable.coverage_identity_ready);
    try std.testing.expect(!unavailable.coverage_summary_ready);
    try std.testing.expect(unavailable.repair_degraded);

    var ready = types.DBIndexStats{
        .name = "dense_v1",
        .kind = .dense_vector,
    };
    Lifecycle.initialize_derived_coverage_identity(.{
        .name = "dense_v1",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
        .coverage_generation = 42,
        .coverage_config_fingerprint = 0,
    }, &ready);
    try std.testing.expect(ready.coverage_identity_ready);
    try std.testing.expect(ready.coverage_summary_ready);
    try std.testing.expectEqual(@as(u64, 42), ready.coverage_generation);
    try std.testing.expectEqual(@as(u64, 0), ready.coverage_config_hash);
}

test "db generation repair runtime status never regresses below durable projection checkpoint" {
    var item = types.DBIndexStats{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .replay_applied_sequence = 7,
        .replay_target_sequence = 8,
        .replay_catch_up_required = true,
        .catch_up_applied_sequence = 7,
        .catch_up_target_sequence = 8,
        .backfill_active = true,
        .backfill_progress = 0.875,
        .projection_checkpoint_applied_sequence = 8,
    };

    Lifecycle.normalize_replay_status_from_durable_checkpoint(&item);

    try std.testing.expectEqual(@as(u64, 8), item.replay_applied_sequence);
    try std.testing.expectEqual(@as(u64, 8), item.replay_target_sequence);
    try std.testing.expectEqual(@as(u64, 8), item.catch_up_applied_sequence);
    try std.testing.expect(!item.replay_catch_up_required);
    try std.testing.expect(!item.backfill_active);
    try std.testing.expectEqual(@as(f64, 1.0), item.backfill_progress);
}

test "db generation repair runtime status overlay hydrates cached derived coverage identity" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    const config_json = "{\"field\":\"embedding\",\"dims\":3,\"external\":true}";
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{
        .name = "external_v1",
        .kind = .dense_vector,
        .config_json = config_json,
    });

    var indexes = [_]types.DBIndexStats{.{
        .name = "external_v1",
        .kind = .dense_vector,
    }};
    var cached_stats = types.DBStats{
        .index_count = 1,
        .indexes = &indexes,
    };
    try db.overlayRuntimeStatusConsistent(alloc, &cached_stats);

    try std.testing.expect(indexes[0].coverage_identity_ready);
    try std.testing.expect(indexes[0].coverage_summary_ready);
    try std.testing.expectEqual(
        try internal_keys.derivedCoverageConfigFingerprint(alloc, config_json),
        indexes[0].coverage_config_hash,
    );
}
