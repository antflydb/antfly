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

const Allocator = std.mem.Allocator;
const apply_state = @import("derived/apply_state.zig");
const artifact_ids = @import("artifact_ids.zig");
const db_internal = @import("internal.zig");
const derived_types = @import("derived/derived_types.zig");
const docstore_mod = @import("../docstore.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const graph_mod = @import("../../graph/graph.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const types = @import("types.zig");

const artifact_repair_summary_dirty_marker = "dirty";
const index_load_failure_prefix = "\x00\x00__metadata__:index_load_failure:";
const graph_repair_rebuild_batch_size: usize = 2048;
const threadedIo = db_internal.threadedIo;
const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};
var test_graph_repair_stream_flushes: std.atomic.Value(u64) = .init(0);
var repair_shadow_nonce: std.atomic.Value(u64) = .init(0);

fn currentTimeNs() u64 {
    return db_internal.currentTimeNs();
}

fn ensureDirPath(path: []const u8) !void {
    if (path.len == 0) return;
    var io_impl = threadedIo();
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), path);
}

fn createUniqueRepairShadowBase(alloc: Allocator, base_path: []const u8) ![]u8 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    const io = io_impl.io();
    try fs_paths.createDirPathPortable(io, base_path);
    for (0..64) |_| {
        const candidate = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-{d}-{x}", .{
            base_path,
            currentTimeNs(),
            repair_shadow_nonce.fetchAdd(1, .monotonic),
        });
        errdefer alloc.free(candidate);
        std.Io.Dir.cwd().createDir(io, candidate, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {
                alloc.free(candidate);
                continue;
            },
            else => return err,
        };
        errdefer std.Io.Dir.cwd().deleteTree(io, candidate) catch {};
        try fs_paths.syncDirPortable(io, base_path);
        return candidate;
    }
    return error.PathAlreadyExists;
}

fn checkArtifactRepairCancelled(options: types.ArtifactRepairRunOptions) !void {
    return db_internal.checkArtifactRepairCancelled(options);
}

fn bytesToHexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn hexToBytesAlloc(alloc: Allocator, hex: []const u8) ![]u8 {
    if (hex.len % 2 != 0) return error.InvalidArtifactPayload;
    const out = try alloc.alloc(u8, hex.len / 2);
    errdefer alloc.free(out);
    for (out, 0..) |*byte, i| {
        byte.* = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
    }
    return out;
}

fn artifactRepairKindHasAutomatedReprocessor(kind: types.ArtifactRepairKind) bool {
    return switch (kind) {
        .embedding, .asset => true,
        .chunk, .graph, .full_text, .algebraic => false,
    };
}

fn repairKindFromArtifactKind(kind: types.ArtifactKind) types.ArtifactRepairKind {
    return switch (kind) {
        .asset => .asset,
        .chunk => .chunk,
        .embedding => .embedding,
    };
}

test "db artifact repair summary is persisted for status-only reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 7,
        };
        defer issue.deinit(alloc);
        try db.recordArtifactRepairIssue(alloc, issue);

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expect(stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, 1), stats.repair_issue_count);
    }

    var status_db = try DB.open(alloc, std.mem.span(path), .{
        .open_mode = .status_only,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer status_db.close();

    const status_stats = try status_db.stats(alloc);
    defer types.freeDBStats(alloc, status_stats);
    try std.testing.expect(status_stats.repair_degraded);
    try std.testing.expect(!status_stats.repair_summary_ready);
    try std.testing.expect(status_stats.repair_issue_count_estimated);
    try std.testing.expectEqual(@as(u64, 1), status_stats.repair_issue_count);
}

test "db artifact repair list cursor pages durable repair debt" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var first_issue = types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "document_units_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_key = try alloc.dupe(u8, "asset:0001"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer first_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, first_issue);

    var second_issue = types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "document_units_v1"),
        .doc_key = try alloc.dupe(u8, "doc:b"),
        .artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_key = try alloc.dupe(u8, "asset:0002"),
        .reason = .corrupt_artifact,
        .sequence = 2,
    };
    defer second_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, second_issue);

    var first_page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .asset, .limit = 1 });
    defer first_page.deinit(alloc);
    try std.testing.expect(first_page.has_more);
    try std.testing.expect(first_page.next_cursor != null);
    try std.testing.expectEqual(@as(usize, 1), first_page.issues.len);
    try std.testing.expectEqualStrings("doc:a", first_page.issues[0].doc_key);

    var second_page = try db.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = .asset,
        .limit = 1,
        .cursor = first_page.next_cursor.?,
    });
    defer second_page.deinit(alloc);
    try std.testing.expect(!second_page.has_more);
    try std.testing.expect(second_page.next_cursor == null);
    try std.testing.expectEqual(@as(usize, 1), second_page.issues.len);
    try std.testing.expectEqualStrings("doc:b", second_page.issues[0].doc_key);
}

test "db artifact repair fallback issue ids do not collide on delimiter-bearing fields" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var first_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a\x1fbody"),
        .artifact_name = try alloc.dupe(u8, "chunks"),
        .unit_id = try alloc.dupe(u8, "page:1"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer first_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, first_issue);

    var second_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "body\x1fchunks"),
        .unit_id = try alloc.dupe(u8, "page:1"),
        .reason = .corrupt_artifact,
        .sequence = 2,
    };
    defer second_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, second_issue);

    const issues = try db.listArtifactRepairIssues(alloc, .chunk, "body_chunks_v1", 10);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 2), issues.len);
}

test "db artifact repair kind filter uses selective index after reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var asset_issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:a"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 1,
        };
        defer asset_issue.deinit(alloc);
        const asset_key = try artifactRepairIssueKeyForIssueAlloc(alloc, asset_issue);
        defer alloc.free(asset_key);
        const asset_value = try encodeArtifactRepairIssueValueAlloc(alloc, asset_issue);
        defer alloc.free(asset_value);
        try db.core.store.put(asset_key, asset_value);

        var graph_issue = types.ArtifactRepairIssue{
            .artifact_kind = .graph,
            .index_name = try alloc.dupe(u8, "entity_graph_v1"),
            .doc_key = try alloc.dupe(u8, "doc:g"),
            .artifact_name = try alloc.dupe(u8, "entity_graph_v1"),
            .reason = .corrupt_artifact,
            .sequence = 2,
        };
        defer graph_issue.deinit(alloc);
        const graph_key = try artifactRepairIssueKeyForIssueAlloc(alloc, graph_issue);
        defer alloc.free(graph_key);
        const graph_value = try encodeArtifactRepairIssueValueAlloc(alloc, graph_issue);
        defer alloc.free(graph_value);
        try db.core.store.put(graph_key, graph_value);

        const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        try db.core.store.putBatch(&.{}, &.{ready_key});
    }

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.runArtifactRepairMetadataMaintenanceUntilIdle();

    var page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .graph, .limit = 1 });
    defer page.deinit(alloc);
    try std.testing.expect(!page.has_more);
    try std.testing.expectEqual(@as(u64, 1), page.scanned);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(.graph, page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:g", page.issues[0].doc_key);
}

test "db artifact repair kind fallback scan is bounded when kind index is rebuilding" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const fallback_scan_budget: u64 = 256;
    var i: usize = 0;
    while (i < fallback_scan_budget + 8) : (i += 1) {
        const doc_key = try std.fmt.allocPrint(alloc, "doc:embedding:{d:0>4}", .{i});
        defer alloc.free(doc_key);
        const artifact_key = try std.fmt.allocPrint(alloc, "embedding:{d:0>4}", .{i});
        defer alloc.free(artifact_key);
        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .embedding,
            .index_name = try alloc.dupe(u8, "idx"),
            .doc_key = try alloc.dupe(u8, doc_key),
            .artifact_name = try alloc.dupe(u8, "idx"),
            .artifact_key = try alloc.dupe(u8, artifact_key),
            .reason = .missing_artifact,
            .sequence = @intCast(i + 1),
        };
        defer issue.deinit(alloc);
        const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
        defer alloc.free(key);
        const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
        defer alloc.free(value);
        try db.core.store.put(key, value);
    }

    var graph_issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "idx"),
        .doc_key = try alloc.dupe(u8, "doc:graph"),
        .artifact_name = try alloc.dupe(u8, "idx"),
        .artifact_key = try alloc.dupe(u8, "graph:0001"),
        .reason = .corrupt_artifact,
        .sequence = 1000,
    };
    defer graph_issue.deinit(alloc);
    const graph_key = try artifactRepairIssueKeyForIssueAlloc(alloc, graph_issue);
    defer alloc.free(graph_key);
    const graph_value = try encodeArtifactRepairIssueValueAlloc(alloc, graph_issue);
    defer alloc.free(graph_value);
    try db.core.store.put(graph_key, graph_value);

    const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
    defer alloc.free(ready_key);
    try db.core.store.putBatch(&.{}, &.{ready_key});

    var first_page = try db.listArtifactRepairIssuesPage(alloc, .{ .artifact_kind = .graph, .index_name = "idx", .limit = 1 });
    defer first_page.deinit(alloc);
    try std.testing.expect(first_page.has_more);
    try std.testing.expect(first_page.next_cursor != null);
    try std.testing.expectEqual(fallback_scan_budget, first_page.scanned);
    try std.testing.expectEqual(@as(usize, 0), first_page.issues.len);

    var second_page = try db.listArtifactRepairIssuesPage(alloc, .{
        .artifact_kind = .graph,
        .index_name = "idx",
        .limit = 1,
        .cursor = first_page.next_cursor,
    });
    defer second_page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), second_page.issues.len);
    try std.testing.expectEqual(.graph, second_page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("doc:graph", second_page.issues[0].doc_key);
}

test "db artifact repair reports unsupported artifact kinds without clearing debt" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "entity_graph_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "entity_graph_v1"),
        .reason = .corrupt_artifact,
        .sequence = 9,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{ .artifact_kind = .graph, .limit = 10 });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expectEqual(@as(u64, 1), repair.unsupported);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(!repair.has_more);
    try std.testing.expect(repair.next_cursor == null);
    try std.testing.expect(repair.debt_remaining);

    const issues = try db.listArtifactRepairIssues(alloc, .graph, "entity_graph_v1", 0);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(@as(u64, 1), issues[0].attempts);
    try std.testing.expect(!issues[0].repairable);
    try std.testing.expectEqualStrings("graph_reprocessor_unavailable", issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("graph_reprocessor_unavailable", issues[0].last_error);
}

test "db index repair requires explicit index and force for healthy rebuild" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

    try std.testing.expectError(error.InvalidArgument, db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .limit = 1,
    }));

    var skipped = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
    });
    defer skipped.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 0), skipped.scanned);
    try std.testing.expectEqual(@as(u64, 0), skipped.indexes_rebuilt);
    try std.testing.expect(!skipped.debt_remaining);

    var forced = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    });
    defer forced.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), forced.scanned);
    try std.testing.expectEqual(@as(u64, 1), forced.indexes_rebuilt);
    try std.testing.expect(!forced.debt_remaining);

    try std.testing.expectError(error.InvalidArgument, db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .cursor = "unexpected-cursor",
    }));
}

test "db index repair targets one graph index per selected config" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_a", .kind = .graph, .config_json = "{}" });
    try db.addIndex(.{ .name = "graph_b", .kind = .graph, .config_json = "{}" });

    const key_a = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_a", "mentions", "doc:b");
    defer alloc.free(key_a);
    const value_a = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 0.7, 0, 0, "");
    defer alloc.free(value_a);
    const key_b = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_b", "mentions", "doc:c");
    defer alloc.free(key_b);
    const value_b = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 0.9, 0, 0, "");
    defer alloc.free(value_b);
    try db.core.store.putBatch(&.{
        .{ .key = key_a, .value = value_a },
        .{ .key = key_b, .value = value_b },
    }, &.{});
    const graph_entry = db.core.index_manager.graphIndex("graph_a") orelse return error.TestUnexpectedResult;
    try graph_entry.index.batchApply(&.{.{
        .source = "doc:stale-source",
        .target = "doc:stale",
        .edge_type = "mentions",
        .weight = 1.0,
    }}, &.{});

    {
        const before_edges = try graph_entry.index.getEdges(alloc, "doc:stale-source", "mentions", .out);
        defer graph_mod.GraphIndex.freeEdges(alloc, before_edges);
        try std.testing.expectEqual(@as(usize, 1), before_edges.len);
    }

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_a",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);

    const repaired_graph_entry = db.core.index_manager.graphIndex("graph_a") orelse return error.TestUnexpectedResult;
    const raw_edges_a = try repaired_graph_entry.index.getEdges(alloc, "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, raw_edges_a);
    try std.testing.expectEqual(@as(usize, 1), raw_edges_a.len);
    try std.testing.expectEqualStrings("doc:b", raw_edges_a[0].target);
    const raw_stale_edges = try repaired_graph_entry.index.getEdges(alloc, "doc:stale-source", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, raw_stale_edges);
    try std.testing.expectEqual(@as(usize, 0), raw_stale_edges.len);

    const edges_a = try db.getEdges(alloc, "graph_a", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges_a);
    try std.testing.expectEqual(@as(usize, 1), edges_a.len);
    try std.testing.expectEqualStrings("doc:b", edges_a[0].target);

    const edges_b = try db.getEdges(alloc, "graph_b", "doc:a", "mentions", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges_b);
    try std.testing.expectEqual(@as(usize, 0), edges_b.len);
}

test "db index repair resets sparse index before rebuilding from artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "sp_v1",
        .kind = .sparse_vector,
        .config_json = "{\"field\":\"sparse\"}",
    });
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\"}" },
        },
        .sync_level = .write,
    });

    const key_a = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "sp_v1");
    defer alloc.free(key_a);
    const value_a = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, null, &.{1}, &.{1.0});
    defer alloc.free(value_a);
    const key_b = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:b", "sp_v1");
    defer alloc.free(key_b);
    const value_b = try enrichment_artifact_codec.encodeSparseEmbeddingAlloc(alloc, null, &.{2}, &.{1.0});
    defer alloc.free(value_b);
    try db.core.store.putBatch(&.{
        .{ .key = key_a, .value = value_a },
        .{ .key = key_b, .value = value_b },
    }, &.{});

    const sparse_entry = db.core.index_manager.sparseIndex("sp_v1") orelse return error.TestUnexpectedResult;
    try sparse_entry.index.batch(&.{
        .{ .doc_id = "doc:a", .vec = .{ .indices = &.{1}, .values = &.{1.0} } },
        .{ .doc_id = "doc:b", .vec = .{ .indices = &.{2}, .values = &.{1.0} } },
        .{ .doc_id = "doc:stale", .vec = .{ .indices = &.{3}, .values = &.{1.0} } },
    }, &.{});
    try std.testing.expect((try sparse_entry.index.debugDocNumForDocId("doc:stale")) != null);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "sp_v1",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 2), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);

    const repaired_entry = db.core.index_manager.sparseIndex("sp_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expect((try repaired_entry.index.debugDocNumForDocId("doc:a")) != null);
    try std.testing.expect((try repaired_entry.index.debugDocNumForDocId("doc:b")) != null);
    try std.testing.expectEqual(@as(?u32, null), try repaired_entry.index.debugDocNumForDocId("doc:stale"));
}

test "db index repair rebuilds full text index from stored documents" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const text_cfg: types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };
    try db.addIndex(text_cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha winner\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta winner\"}" },
        },
        .sync_level = .full_index,
    });

    var before = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer before.deinit();
    try std.testing.expectEqual(@as(u32, 1), before.total_hits);

    try db.core.saveProjectionCheckpoint("ft_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = types.indexConfigHash(text_cfg),
    });

    var page = try db.listArtifactRepairIssuesPage(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expect(page.issues[0].repairable);
    try std.testing.expectEqual(types.ArtifactRepairKind.full_text, page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("ft_v1", page.issues[0].index_name);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 2), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);

    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "ft_v1");
    const stored_text_cfg = db.core.index_manager.get("ft_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expectEqual(types.indexConfigHash(stored_text_cfg.*), checkpoint.config_hash);

    var after = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer after.deinit();
    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db generic artifact repair queue reprocesses document asset artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "document_units_v1",
        .kind = .asset,
        .field = "url",
        .content_type = "application/json",
        .producer_json = "{\"type\":\"document_extraction\",\"config\":{}}",
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            .value = "{\"url\":\"data:text/plain;base64,YWxwaGE=\"}",
        }},
        .sync_level = .full_index,
    });

    var before = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer before.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), before.generation);

    const manifest_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "document_units_v1");
    defer alloc.free(manifest_key);
    const artifact_key_hex = try bytesToHexAlloc(alloc, manifest_key);
    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .asset,
        .index_name = try alloc.dupe(u8, "document_units_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_key = artifact_key_hex,
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer issue.deinit(alloc);

    try db.recordArtifactRepairIssue(alloc, issue);

    {
        const issues = try db.listArtifactRepairIssues(alloc, .asset, "document_units_v1", 0);
        defer types.freeArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqual(.asset, issues[0].artifact_kind);
        try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
        try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
        try std.testing.expectEqualStrings(artifact_key_hex, issues[0].artifact_key);
    }

    const degraded_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, degraded_stats);
    try std.testing.expect(degraded_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), degraded_stats.repair_issue_count);
    try std.testing.expectEqual(@as(u32, 0), degraded_stats.index_count);

    try db.core.store.put(manifest_key, "bad-artifact");

    const repair = try db.repairArtifactIssues(alloc, .asset, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);

    const issues_after = try db.listArtifactRepairIssues(alloc, .asset, "document_units_v1", 0);
    defer types.freeArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);

    const repaired_stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, repaired_stats);
    try std.testing.expect(!repaired_stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 0), repaired_stats.repair_issue_count);

    var after = (try db.getDocumentArtifactManifest(alloc, "doc:a", "document_units_v1")) orelse return error.TestUnexpectedResult;
    defer after.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), after.generation);
    try std.testing.expect(after.manifest_json.len > 0);
}

test "db index repair rebuilds dense index quarantined by incomplete bulk publish" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
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
    }

    const dense_index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/dense_idx", .{std.mem.span(path)});
    defer alloc.free(dense_index_path);
    const dense_index_path_z = try alloc.dupeZ(u8, dense_index_path);
    defer alloc.free(dense_index_path_z);
    {
        var hbc = try hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_index_path_z, .{
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

    const recorded = reopened.core.index_manager.loadFailure("dense_idx") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("IncompleteBulkPublish", recorded);
    {
        const status_key = try db_internal.indexStatusKeyAlloc(alloc, "dense_idx");
        defer alloc.free(status_key);
        var stale_status: [64]u8 = undefined;
        db_internal.encodeIndexStatusSnapshot(.{
            .kind = .dense_vector,
            .doc_count = 99,
            .node_count = 99,
            .root_node = 99,
        }, &stale_status);
        try reopened.core.store.put(status_key, &stale_status);
    }
    {
        const stats = try reopened.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
    }

    var repair = try reopened.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "dense_idx",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 2), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);
    {
        const persisted_status = (try db_internal.loadIndexStatusSnapshot(alloc, reopened.core.store, "dense_idx")).?;
        try std.testing.expectEqual(@as(u64, 2), persisted_status.doc_count);
    }

    var result = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 1.0, 0.0, 0.0 },
            .k = 2,
        } },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
}

test "db repair queue reprocesses corrupt generated dense embedding artifacts" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const OpenOptions = @import("mod.zig").OpenOptions;
    const opts: OpenOptions = .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "repair-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), opts);
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dv_v1\"}}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"repair target text\"}" }},
            .sync_level = .full_index,
        });

        const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = "doc:a",
                    .artifact_key = artifact_key,
                    .vector = &.{},
                },
            },
        };
        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), opts);
    defer reopened.close();

    {
        const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
        defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
        try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
        try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    }

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);

    const issues_after = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(dense_applied >= appended_sequence);

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, artifact_value, enrichment_artifact_codec.hashSource("repair target text"), 3);
}

test "db repair queue reprocesses corrupt chunk generated dense embedding artifacts from parent document" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    const OpenOptions = @import("mod.zig").OpenOptions;
    const opts: OpenOptions = .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
        .enrichment = .{
            .owner_id = "repair-worker",
            .dense_embedder = deterministic.interface(),
        },
    };

    var appended_sequence: u64 = 0;
    {
        var db = try DB.open(alloc, std.mem.span(path), opts);
        defer db.close();

        try db.addIndex(.{
            .name = "dv_v1",
            .kind = .dense_vector,
            .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"artifact_name\":\"body_chunks_v1\",\"chunk_size\":8,\"chunk_overlap\":2,\"embedding_name\":\"chunk_dense_v1\"}}",
        });
        try db.batch(.{
            .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"abcdefghijklmno\"}" }},
            .sync_level = .full_index,
        });

        const chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
        defer alloc.free(chunk_key);
        const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "chunk_dense_v1");
        defer alloc.free(artifact_key);
        try db.core.store.put(artifact_key, "bad-artifact");

        const derived_batch = derived_types.DerivedBatch{
            .dense_embeddings = &.{
                .{
                    .index_name = "dv_v1",
                    .doc_key = chunk_key,
                    .artifact_key = artifact_key,
                    .vector = &.{},
                },
            },
        };
        appended_sequence = try db.derivedAsyncAppendDerivedBatchRecord(derived_batch);
    }

    var reopened = try DB.open(alloc, std.mem.span(path), opts);
    defer reopened.close();

    {
        const issues = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
        defer types.freeEmbeddingArtifactRepairIssues(alloc, issues);
        try std.testing.expectEqual(@as(usize, 1), issues.len);
        try std.testing.expectEqual(.corrupt_embedding_artifact, issues[0].reason);
        try std.testing.expectEqual(.embedding, issues[0].artifact_kind);
        try std.testing.expectEqualStrings("doc:a", issues[0].parent_doc_key);
        try std.testing.expectEqualStrings("body_chunks_v1", issues[0].source_artifact_name);
        try std.testing.expectEqual(@as(?u32, 0), issues[0].chunk_id);
        try std.testing.expectEqualStrings("chunk_dense_v1", issues[0].artifact_name);
    }

    const repair = try reopened.repairEmbeddingArtifactIssues(alloc, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);

    const issues_after = try reopened.listEmbeddingArtifactRepairIssues(alloc, "dv_v1", 0);
    defer types.freeEmbeddingArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);

    const dense_applied = try reopened.core.loadAppliedSequence(alloc, "dv_v1");
    try std.testing.expect(dense_applied >= appended_sequence);

    const chunk_key = try internal_keys.chunkArtifactKeyAlloc(alloc, "doc:a", "body_chunks_v1", 0);
    defer alloc.free(chunk_key);
    const artifact_key = try internal_keys.derivedEmbeddingArtifactKeyAlloc(alloc, chunk_key, "chunk_dense_v1");
    defer alloc.free(artifact_key);
    const artifact_value = try reopened.core.store.get(alloc, artifact_key);
    defer alloc.free(artifact_value);
    try enrichment_artifact_codec.expectDenseEmbeddingValue(alloc, artifact_value, enrichment_artifact_codec.hashSource("abcdefgh"), 3);
}

test "db repair issue list reports index repair candidates" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "graph_v1"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    var page = try db.listArtifactRepairIssuesPage(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .limit = 10,
    });
    defer page.deinit(alloc);

    try std.testing.expect(!page.has_more);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(.graph, page.issues[0].artifact_kind);
    try std.testing.expectEqualStrings("graph_v1", page.issues[0].index_name);
    try std.testing.expectEqualStrings("index_repair_required", page.issues[0].last_error);
}

test "db index repair reports remaining artifact debt after rebuild" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "graph_v1",
        .kind = .graph,
        .config_json = "{}",
    });

    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .graph,
        .index_name = try alloc.dupe(u8, "graph_v1"),
        .doc_key = try alloc.dupe(u8, "doc:missing"),
        .artifact_name = try alloc.dupe(u8, "graph_v1"),
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), stats.repair_issue_count);
}

test "db repair issue list reports algebraic index debt as unsupported" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    const cfg: types.IndexConfig = .{
        .name = "alg_v1",
        .kind = .algebraic,
        .config_json =
        \\{
        \\  "table": "docs",
        \\  "schema_version": 1,
        \\  "capability_fingerprint": "test-capability"
        \\}
        ,
    };
    try db.addIndex(cfg);
    try db.core.saveProjectionCheckpoint("alg_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = types.indexConfigHash(cfg),
    });

    var page = try db.listArtifactRepairIssuesPage(alloc, .{
        .target = .index,
        .artifact_kind = .algebraic,
        .index_name = "alg_v1",
        .limit = 1,
    });
    defer page.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(types.ArtifactRepairKind.algebraic, page.issues[0].artifact_kind);
    try std.testing.expect(!page.issues[0].repairable);
    try std.testing.expectEqualStrings("algebraic_index_rebuild_unavailable", page.issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("algebraic_index_rebuild_unavailable", page.issues[0].last_error);

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .algebraic,
        .index_name = "alg_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.unsupported);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);
}

test "db index repair serializes duplicate repairs for one index" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });
    db.beginIndexRepairBarrier();
    defer db.endIndexRepairBarrier();

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.in_progress);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
}

test "db graph index repair records corrupt artifact debt during shadow rebuild" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "relations_graph",
        .kind = .graph,
        .config_json = "{}",
    });

    const artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "relations_graph", "mentions", "doc:b");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-graph-artifact");

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "relations_graph",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);

    const issues = try db.listArtifactRepairIssues(alloc, .graph, "relations_graph", 10);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 1), issues.len);
    try std.testing.expectEqual(.corrupt_artifact, issues[0].reason);
    try std.testing.expectEqualStrings("doc:a", issues[0].doc_key);
    try std.testing.expectEqualStrings("mentions:doc:b", issues[0].artifact_name);

    const raw_artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(raw_artifact);
    try std.testing.expectEqualStrings("bad-graph-artifact", raw_artifact);
}

test "db index repair shadow swap survives reopen" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const text_cfg: types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        try db.addIndex(text_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha durable\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta durable\"}" },
            },
            .sync_level = .full_index,
        });
        try db.core.saveProjectionCheckpoint("ft_v1", .{
            .applied_sequence = 0,
            .status = .repair_required,
            .config_hash = types.indexConfigHash(text_cfg),
        });

        const stale_canonical_file = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/stale-before-repair", .{std.mem.span(path)});
        defer alloc.free(stale_canonical_file);
        try std.Io.Dir.cwd().writeFile(std.testing.io, .{
            .sub_path = stale_canonical_file,
            .data = "stale",
        });

        var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .artifact_kind = .full_text,
            .index_name = "ft_v1",
            .limit = 1,
        });
        defer repair.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    }

    const abandoned_shadow = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-abandoned/indexes/ft_v1", .{std.mem.span(path)});
    defer alloc.free(abandoned_shadow);
    try ensureDirPath(abandoned_shadow);
    const in_progress_shadow_root = try std.fmt.allocPrint(alloc, "{s}/.repair-shadow-live-build", .{std.mem.span(path)});
    defer alloc.free(in_progress_shadow_root);
    const in_progress_shadow = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1", .{in_progress_shadow_root});
    defer alloc.free(in_progress_shadow);
    try ensureDirPath(in_progress_shadow);
    try index_manager_mod.IndexManager.writeRepairShadowInProgressMarker(alloc, in_progress_shadow_root);

    {
        var reopened = try DB.open(alloc, std.mem.span(path), .{
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer reopened.close();

        const checkpoint = try reopened.core.loadProjectionCheckpoint(alloc, "ft_v1");
        try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);

        var after = try reopened.search(alloc, .{
            .index_name = "ft_v1",
            .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
        });
        defer after.deinit();
        try std.testing.expectEqual(@as(u32, 1), after.total_hits);
        try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, abandoned_shadow, .{}));
        try std.Io.Dir.cwd().access(std.testing.io, in_progress_shadow, .{});
        const stale_canonical_file = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_v1/stale-before-repair", .{std.mem.span(path)});
        defer alloc.free(stale_canonical_file);
        try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, stale_canonical_file, .{}));
    }
}

test "db index repair shadow roots are allocated uniquely" {
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const first = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(first);
    const second = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(second);
    const third = try createUniqueRepairShadowBase(alloc, std.mem.span(path));
    defer alloc.free(third);

    try std.testing.expect(!std.mem.eql(u8, first, second));
    try std.testing.expect(!std.mem.eql(u8, first, third));
    try std.testing.expect(!std.mem.eql(u8, second, third));
    try std.testing.expect(std.mem.indexOf(u8, first, "/.repair-shadow-") != null);
    try std.testing.expect(std.mem.indexOf(u8, second, "/.repair-shadow-") != null);
    try std.testing.expect(std.mem.indexOf(u8, third, "/.repair-shadow-") != null);

    try std.Io.Dir.cwd().access(std.testing.io, first, .{});
    try std.Io.Dir.cwd().access(std.testing.io, second, .{});
    try std.Io.Dir.cwd().access(std.testing.io, third, .{});
}

test "db index repair shadow swap preserves post snapshot mutations" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const text_cfg: types.IndexConfig = .{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    };

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(text_cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"body\":\"alpha before\"}" },
            .{ .key = "doc:b", .value = "{\"body\":\"beta before\"}" },
        },
        .sync_level = .full_index,
    });
    try db.core.saveProjectionCheckpoint("ft_v1", .{
        .applied_sequence = 0,
        .status = .repair_required,
        .config_hash = types.indexConfigHash(text_cfg),
    });

    const HookContext = struct {
        fired: bool = false,
        observed_floor: u64 = 0,
    };
    const Hook = struct {
        fn afterSnapshotBuild(ptr: *anyopaque, hook_db: *DB, index_name: []const u8, build_floor_sequence: u64) anyerror!void {
            const ctx: *HookContext = @ptrCast(@alignCast(ptr));
            try std.testing.expectEqualStrings("ft_v1", index_name);
            try std.testing.expect(build_floor_sequence > 0);
            try std.testing.expect(!ctx.fired);
            ctx.fired = true;
            ctx.observed_floor = build_floor_sequence;

            try hook_db.batch(.{
                .writes = &.{
                    .{ .key = "doc:b", .value = "{\"body\":\"gamma after\"}" },
                    .{ .key = "doc:c", .value = "{\"body\":\"alpha after\"}" },
                },
                .deletes = &.{"doc:a"},
                .sync_level = .write,
            });
        }
    };
    var hook_ctx = HookContext{};
    db.shadow_index_repair_hook = .{
        .ptr = &hook_ctx,
        .after_snapshot_build = Hook.afterSnapshotBuild,
    };
    defer db.shadow_index_repair_hook = null;

    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expect(hook_ctx.fired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expect(!repair.debt_remaining);
    const checkpoint = try db.core.loadProjectionCheckpoint(alloc, "ft_v1");
    try std.testing.expectEqual(apply_state.ProjectionStatus.clean, checkpoint.status);
    try std.testing.expect(checkpoint.applied_sequence > hook_ctx.observed_floor);

    var alpha = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer alpha.deinit();
    try std.testing.expectEqual(@as(u32, 1), alpha.total_hits);
    try std.testing.expectEqualStrings("doc:c", alpha.hits[0].id);

    var beta = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "beta" } },
    });
    defer beta.deinit();
    try std.testing.expectEqual(@as(u32, 0), beta.total_hits);

    var gamma = try db.search(alloc, .{
        .index_name = "ft_v1",
        .full_text = .{ .match = .{ .field = "body", .text = "gamma" } },
    });
    defer gamma.deinit();
    try std.testing.expectEqual(@as(u32, 1), gamma.total_hits);
    try std.testing.expectEqualStrings("doc:b", gamma.hits[0].id);
}

test "db index repair recovers quarantined index load before rebuild" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }

    index_manager_mod.test_inject_index_open_error = error.FileNotFound;
    defer index_manager_mod.test_inject_index_open_error = null;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        const recorded = db.core.index_manager.loadFailure("ft_v1") orelse return error.TestUnexpectedResult;
        try std.testing.expectEqualStrings("FileNotFound", recorded);

        index_manager_mod.test_inject_index_open_error = null;
        var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
            .target = .index,
            .index_name = "ft_v1",
            .limit = 1,
        });
        defer repair.deinit(alloc);
        try std.testing.expectEqual(@as(u64, 1), repair.scanned);
        try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded);
        try std.testing.expectEqual(@as(u64, 1), repair.repaired);
        try std.testing.expectEqual(@as(u64, 0), repair.failed);
        try std.testing.expectEqual(@as(u64, 0), repair.unresolved);
        try std.testing.expect(!repair.debt_remaining);
        try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
        try std.testing.expect(db.core.textIndexEntry("ft_v1") != null);
    }
}

test "db index repair rebuilds full text after quarantined root recreation" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);
    const path_slice = std.mem.span(path);

    const text_cfg: types.IndexConfig = .{
        .name = "ft_recreate",
        .kind = .full_text,
        .config_json = "{}",
    };

    {
        var db = try DB.open(alloc, path_slice, .{});
        defer db.close();

        try db.addIndex(text_cfg);
        try db.batch(.{
            .writes = &.{
                .{ .key = "doc:a", .value = "{\"body\":\"alpha durable\"}" },
                .{ .key = "doc:b", .value = "{\"body\":\"beta durable\"}" },
            },
            .sync_level = .full_index,
        });
        try db.core.saveProjectionCheckpoint("ft_recreate", .{
            .applied_sequence = db.core.nextDerivedSequence(),
            .status = .clean,
            .generation = 2,
            .config_hash = types.indexConfigHash(text_cfg),
        });
    }

    const index_path = try std.fmt.allocPrint(alloc, "{s}/indexes/ft_recreate", .{path_slice});
    defer alloc.free(index_path);
    try std.testing.expect((try TestHelpers.corruptNonEmptyFilesUnderDir(alloc, index_path)) > 0);

    var reopened = try DB.open(alloc, path_slice, .{
        .open_mode = .writer_no_replay,
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer reopened.close();

    const recorded = reopened.core.index_manager.loadFailure("ft_recreate") orelse return error.TestUnexpectedResult;
    try std.testing.expect(recorded.len > 0);

    var repair = try reopened.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .full_text,
        .index_name = "ft_recreate",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expect(!repair.debt_remaining);
    try std.testing.expect(reopened.core.index_manager.loadFailure("ft_recreate") == null);

    var after = try reopened.search(alloc, .{
        .index_name = "ft_recreate",
        .full_text = .{ .match = .{ .field = "body", .text = "alpha" } },
    });
    defer after.deinit();
    try std.testing.expectEqual(@as(u32, 1), after.total_hits);
    try std.testing.expectEqualStrings("doc:a", after.hits[0].id);
}

test "db index repair streams graph artifact rebuild in batches" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_stream", .kind = .graph, .config_json = "{}" });

    const total_edges = graph_repair_rebuild_batch_size + 3;
    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |write| {
            alloc.free(@constCast(write.key));
            alloc.free(@constCast(write.value));
        }
        writes.deinit(alloc);
    }
    for (0..total_edges) |i| {
        const source = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{i});
        defer alloc.free(source);
        const target = try std.fmt.allocPrint(alloc, "target:{d:0>5}", .{i});
        defer alloc.free(target);
        const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, source, "graph_stream", "links", target);
        errdefer alloc.free(key);
        const value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, null, 1.0, 0, 0, "");
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    try db.core.store.putBatch(writes.items, &.{});

    test_graph_repair_stream_flushes.store(0, .monotonic);
    var repair = try db.repairArtifactIssuesWithRequest(alloc, .{
        .target = .index,
        .artifact_kind = .graph,
        .index_name = "graph_stream",
        .limit = 1,
        .force = true,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, @intCast(total_edges)), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 2), test_graph_repair_stream_flushes.load(.monotonic));

    const last_source = try std.fmt.allocPrint(alloc, "doc:{d:0>5}", .{total_edges - 1});
    defer alloc.free(last_source);
    const last_target = try std.fmt.allocPrint(alloc, "target:{d:0>5}", .{total_edges - 1});
    defer alloc.free(last_target);
    const edges = try db.getEdges(alloc, "graph_stream", last_source, "links", .out);
    defer graph_mod.GraphIndex.freeEdges(alloc, edges);
    try std.testing.expectEqual(@as(usize, 1), edges.len);
    try std.testing.expectEqualStrings(last_target, edges[0].target);
}

test "db index repair barrier disables published dense fast path" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try std.testing.expect(db.beginPublishedDenseSearch());
    db.endPublishedDenseSearch();

    db.beginIndexRepairBarrier();
    defer db.endIndexRepairBarrier();
    try std.testing.expect(!db.beginPublishedDenseSearch());
}

fn seedRawAssetRepairIssues(alloc: Allocator, db: anytype, count: usize) !void {
    for (0..count) |i| {
        const doc_key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
        defer alloc.free(doc_key);
        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, doc_key),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = @intCast(i + 1),
        };
        defer issue.deinit(alloc);
        const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
        defer alloc.free(key);
        const value = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
        defer alloc.free(value);
        try db.core.store.put(key, value);
    }
    const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    defer alloc.free(ready_key);
    const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
    defer alloc.free(progress_key);
    const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
    defer alloc.free(root_summary_key);
    try db.core.store.putBatch(&.{}, &.{ ready_key, progress_key, root_summary_key });
}

test "db artifact repair summary rebuild is bounded and exact until ready" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const seeded_count: usize = 1030;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try seedRawAssetRepairIssues(alloc, &db, seeded_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);

        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, ready_key));

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expect(stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
        defer alloc.free(ready_key);
        const ready = try db.core.store.get(alloc, ready_key);
        defer alloc.free(ready);

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, progress_key));

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(stats.repair_summary_ready);
        try std.testing.expect(!stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, seeded_count), stats.repair_issue_count);
    }
}

test "db artifact repair summary rebuild invalidates partial counters on mutation" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try seedRawAssetRepairIssues(alloc, &db, 1030);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);

        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:9999"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 9999,
        };
        defer issue.deinit(alloc);
        try db.recordArtifactRepairIssue(alloc, issue);

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);
        try std.testing.expectEqualStrings(artifact_repair_summary_dirty_marker, progress);

        const dirty_stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, dirty_stats);
        try std.testing.expect(dirty_stats.repair_degraded);
        try std.testing.expect(!dirty_stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), dirty_stats.repair_issue_count);
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status_db.close();

        const stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(stats.repair_summary_ready);
        try std.testing.expect(!stats.repair_issue_count_estimated);
        try std.testing.expectEqual(@as(u64, 1031), stats.repair_issue_count);
    }
}

test "db artifact repair summary store writer invalidates partial rebuild" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try seedRawAssetRepairIssues(alloc, &db, 1030);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);

        var issue = types.ArtifactRepairIssue{
            .artifact_kind = .asset,
            .index_name = try alloc.dupe(u8, "document_units_v1"),
            .doc_key = try alloc.dupe(u8, "doc:0001-extra"),
            .artifact_name = try alloc.dupe(u8, "document_units_v1"),
            .reason = .corrupt_artifact,
            .sequence = 9999,
        };
        defer issue.deinit(alloc);
        try db.recordArtifactRepairIssue(alloc, issue);

        const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
        defer alloc.free(progress_key);
        const progress = try db.core.store.get(alloc, progress_key);
        defer alloc.free(progress);
        try std.testing.expectEqualStrings(artifact_repair_summary_dirty_marker, progress);

        const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
        defer alloc.free(root_summary_key);
        try std.testing.expectError(error.NotFound, db.core.store.get(alloc, root_summary_key));
    }

    {
        var status_db = try DB.open(alloc, std.mem.span(path), .{
            .open_mode = .status_only,
            .start_index_workers = false,
            .ttl_cleanup = .{ .enabled = false },
        });
        defer status_db.close();

        const stats = try status_db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(!stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1024), stats.repair_issue_count);
    }

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        const stats = try db.stats(alloc);
        defer types.freeDBStats(alloc, stats);
        try std.testing.expect(stats.repair_degraded);
        try std.testing.expect(stats.repair_summary_ready);
        try std.testing.expectEqual(@as(u64, 1031), stats.repair_issue_count);
    }
}

test "db artifact repair metadata maintenance drains summary rebuild without restart" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    const seeded_count: usize = 2050;
    {
        var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
        defer db.close();
        try seedRawAssetRepairIssues(alloc, &db, seeded_count);
    }

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();

    const partial = try db.stats(alloc);
    defer types.freeDBStats(alloc, partial);
    try std.testing.expect(partial.repair_degraded);
    try std.testing.expect(!partial.repair_summary_ready);
    try std.testing.expectEqual(@as(u64, 1024), partial.repair_issue_count);
    try std.testing.expect(db.pendingWorkStats().repair_metadata_rebuild_pending);

    try db.runUntilIdle();

    const drained = try db.stats(alloc);
    defer types.freeDBStats(alloc, drained);
    try std.testing.expect(drained.repair_degraded);
    try std.testing.expect(drained.repair_summary_ready);
    try std.testing.expectEqual(@as(u64, seeded_count), drained.repair_issue_count);
    try std.testing.expect(!db.pendingWorkStats().repair_metadata_rebuild_pending);
}

test "db artifact repair queue keeps distinct unit artifacts for same document and name" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    var first_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .parent_doc_key = try alloc.dupe(u8, "doc:a"),
        .unit_id = try alloc.dupe(u8, "page:000001"),
        .source_artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
        .artifact_key = try alloc.dupe(u8, "aaaa"),
        .chunk_id = 0,
        .reason = .corrupt_artifact,
        .sequence = 1,
    };
    defer first_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, first_issue);

    var second_issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .index_name = try alloc.dupe(u8, "body_chunks_v1"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .parent_doc_key = try alloc.dupe(u8, "doc:a"),
        .unit_id = try alloc.dupe(u8, "page:000002"),
        .source_artifact_name = try alloc.dupe(u8, "document_units_v1"),
        .artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
        .artifact_key = try alloc.dupe(u8, "bbbb"),
        .chunk_id = 0,
        .reason = .corrupt_artifact,
        .sequence = 2,
    };
    defer second_issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, second_issue);

    const issues = try db.listArtifactRepairIssues(alloc, .chunk, "body_chunks_v1", 10);
    defer types.freeArtifactRepairIssues(alloc, issues);
    try std.testing.expectEqual(@as(usize, 2), issues.len);
    try std.testing.expect(!issues[0].repairable);
    try std.testing.expectEqualStrings("chunk_reprocessor_unavailable", issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("page:000001", issues[0].unit_id);
    try std.testing.expectEqualStrings("page:000002", issues[1].unit_id);
}

fn artifactRepairUnsupportedReason(kind: types.ArtifactRepairKind) []const u8 {
    return switch (kind) {
        .embedding, .asset => "",
        .chunk => "chunk_reprocessor_unavailable",
        .graph => "graph_reprocessor_unavailable",
        .full_text => "full_text_reprocessor_unavailable",
        .algebraic => "algebraic_index_rebuild_unavailable",
    };
}

fn artifactRepairIssueKeyForIssueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    const issue_id = try artifactRepairIssueIdAlloc(alloc, issue);
    defer alloc.free(issue_id);
    return try internal_keys.artifactRepairIssueKeyAlloc(alloc, issue.index_name, @tagName(issue.artifact_kind), issue_id);
}

fn artifactRepairIssueKindKeyForIssueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    const issue_id = try artifactRepairIssueIdAlloc(alloc, issue);
    defer alloc.free(issue_id);
    return try internal_keys.artifactRepairIssueKindKeyAlloc(alloc, @tagName(issue.artifact_kind), issue.index_name, issue_id);
}

fn artifactRepairIssueIdAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    if (issue.artifact_key.len > 0) return try alloc.dupe(u8, issue.artifact_key);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    artifactRepairIssueIdHashString(&hasher, issue.doc_key);
    artifactRepairIssueIdHashString(&hasher, issue.parent_doc_key);
    artifactRepairIssueIdHashString(&hasher, issue.source_artifact_name);
    artifactRepairIssueIdHashString(&hasher, issue.artifact_name);
    artifactRepairIssueIdHashString(&hasher, issue.unit_id);
    artifactRepairIssueIdHashOptionalU64(&hasher, if (issue.chunk_id) |chunk_id| @as(u64, chunk_id) else null);
    artifactRepairIssueIdHashString(&hasher, @tagName(issue.artifact_kind));
    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&digest);
    const hex = try bytesToHexAlloc(alloc, &digest);
    defer alloc.free(hex);
    return try std.fmt.allocPrint(alloc, "tuple-sha256:{s}", .{hex});
}

fn artifactRepairIssueIdHashString(hasher: *std.crypto.hash.sha2.Sha256, value: []const u8) void {
    var len_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &len_buf, value.len, .little);
    hasher.update(&len_buf);
    hasher.update(value);
}

fn artifactRepairIssueIdHashOptionalU64(hasher: *std.crypto.hash.sha2.Sha256, value: ?u64) void {
    hasher.update(if (value == null) "\x00" else "\x01");
    if (value) |raw| {
        var buf: [8]u8 = undefined;
        std.mem.writeInt(u64, &buf, raw, .little);
        hasher.update(&buf);
    }
}

fn encodeArtifactRepairIssueValueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
    return try std.json.Stringify.valueAlloc(alloc, issue, .{ .emit_null_optional_fields = false });
}

fn decodeArtifactRepairIssueValueAlloc(alloc: Allocator, raw: []const u8) !types.ArtifactRepairIssue {
    var parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();
    if (parsed.value != .object) return error.InvalidArtifactPayload;
    const obj = parsed.value.object;
    const Fields = struct {
        fn string(object: std.json.ObjectMap, name: []const u8) []const u8 {
            const value = object.get(name) orelse return "";
            if (value != .string) return "";
            return value.string;
        }
        fn u64Value(object: std.json.ObjectMap, name: []const u8) u64 {
            const value = object.get(name) orelse return 0;
            if (value != .integer or value.integer < 0) return 0;
            return std.math.cast(u64, value.integer) orelse 0;
        }
        fn optionalU32(object: std.json.ObjectMap, name: []const u8) ?u32 {
            const value = object.get(name) orelse return null;
            if (value == .null) return null;
            if (value != .integer or value.integer < 0) return null;
            return std.math.cast(u32, value.integer);
        }
        fn boolValue(object: std.json.ObjectMap, name: []const u8, default: bool) bool {
            const value = object.get(name) orelse return default;
            if (value != .bool) return default;
            return value.bool;
        }
    };
    const kind = std.meta.stringToEnum(types.ArtifactRepairKind, Fields.string(obj, "artifact_kind")) orelse .embedding;
    const reason = std.meta.stringToEnum(types.ArtifactRepairReason, Fields.string(obj, "reason")) orelse .missing_artifact;
    const default_repairable = artifactRepairKindHasAutomatedReprocessor(kind);
    return .{
        .artifact_kind = kind,
        .index_name = try alloc.dupe(u8, Fields.string(obj, "index_name")),
        .doc_key = try alloc.dupe(u8, Fields.string(obj, "doc_key")),
        .parent_doc_key = try alloc.dupe(u8, Fields.string(obj, "parent_doc_key")),
        .unit_id = try alloc.dupe(u8, Fields.string(obj, "unit_id")),
        .source_artifact_name = try alloc.dupe(u8, Fields.string(obj, "source_artifact_name")),
        .artifact_name = try alloc.dupe(u8, Fields.string(obj, "artifact_name")),
        .artifact_key = try alloc.dupe(u8, Fields.string(obj, "artifact_key")),
        .chunk_id = Fields.optionalU32(obj, "chunk_id"),
        .repairable = Fields.boolValue(obj, "repairable", default_repairable),
        .unsupported_reason = try alloc.dupe(u8, Fields.string(obj, "unsupported_reason")),
        .sequence = Fields.u64Value(obj, "sequence"),
        .reason = reason,
        .attempts = Fields.u64Value(obj, "attempts"),
        .first_seen_ns = Fields.u64Value(obj, "first_seen_ns"),
        .last_seen_ns = Fields.u64Value(obj, "last_seen_ns"),
        .last_error = try alloc.dupe(u8, Fields.string(obj, "last_error")),
    };
}

fn loadArtifactRepairIssueFromStoreByKey(alloc: Allocator, store: *docstore_mod.DocStore, key: []const u8) !?types.ArtifactRepairIssue {
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try decodeArtifactRepairIssueValueAlloc(alloc, raw);
}

fn indexLoadFailureKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ index_load_failure_prefix, index_name });
}

fn cloneArtifactRepairIssueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) !types.ArtifactRepairIssue {
    var out = types.ArtifactRepairIssue{
        .artifact_kind = issue.artifact_kind,
        .chunk_id = issue.chunk_id,
        .repairable = issue.repairable,
        .sequence = issue.sequence,
        .reason = issue.reason,
        .attempts = issue.attempts,
        .first_seen_ns = issue.first_seen_ns,
        .last_seen_ns = issue.last_seen_ns,
    };
    errdefer out.deinit(alloc);
    out.index_name = try alloc.dupe(u8, issue.index_name);
    out.doc_key = try alloc.dupe(u8, issue.doc_key);
    out.parent_doc_key = try alloc.dupe(u8, issue.parent_doc_key);
    out.unit_id = try alloc.dupe(u8, issue.unit_id);
    out.source_artifact_name = try alloc.dupe(u8, issue.source_artifact_name);
    out.artifact_name = try alloc.dupe(u8, issue.artifact_name);
    out.artifact_key = try alloc.dupe(u8, issue.artifact_key);
    out.unsupported_reason = try alloc.dupe(u8, issue.unsupported_reason);
    out.last_error = try alloc.dupe(u8, issue.last_error);
    return out;
}

fn replaceRepairIssueLastError(alloc: Allocator, issue: *types.ArtifactRepairIssue, value: []const u8) !void {
    const owned = try alloc.dupe(u8, value);
    if (issue.last_error.len > 0) alloc.free(@constCast(issue.last_error));
    issue.last_error = owned;
}

fn saveArtifactRepairIssueToStore(alloc: Allocator, store: *docstore_mod.DocStore, issue: types.ArtifactRepairIssue) !void {
    const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
    defer alloc.free(key);
    const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
    defer alloc.free(kind_key);
    const encoded = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
    defer alloc.free(encoded);
    const writes = [_]docstore_mod.KVPair{
        .{ .key = key, .value = encoded },
        .{ .key = kind_key, .value = encoded },
    };
    try store.putBatch(writes[0..], &.{});
}

fn appendKeysForPrefixDeleteInStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]const u8),
    prefix: []const u8,
) !void {
    const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
    defer if (upper) |buf| alloc.free(buf);
    const ScanState = struct {
        alloc: Allocator,
        deletes: *std.ArrayListUnmanaged([]const u8),
        owned_keys: *std.ArrayListUnmanaged([]const u8),

        fn scanEntry(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
            const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
            const key_copy = try state.alloc.dupe(u8, key);
            errdefer state.alloc.free(key_copy);
            const owned_len = state.owned_keys.items.len;
            try state.owned_keys.append(state.alloc, key_copy);
            errdefer state.owned_keys.shrinkRetainingCapacity(owned_len);
            try state.deletes.append(state.alloc, key_copy);
            return .@"continue";
        }
    };
    var state = ScanState{ .alloc = alloc, .deletes = deletes, .owned_keys = owned_keys };
    try store.scanWithContext(prefix, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);
}

fn appendArtifactRepairSummaryRebuildInvalidationForStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: ?*std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
    errdefer alloc.free(progress_key);
    const owned_len = owned_keys.items.len;
    try owned_keys.append(alloc, progress_key);
    errdefer owned_keys.shrinkRetainingCapacity(owned_len);
    if (writes) |out| {
        const dirty_value = try alloc.dupe(u8, artifact_repair_summary_dirty_marker);
        errdefer alloc.free(dirty_value);
        try out.append(alloc, .{ .key = progress_key, .value = dirty_value });
    } else {
        try deletes.append(alloc, progress_key);
    }

    const rebuild_prefix = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
    defer alloc.free(rebuild_prefix);
    try appendKeysForPrefixDeleteInStore(alloc, store, deletes, owned_keys, rebuild_prefix);
}

fn appendArtifactRepairSummaryDirtyForStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
    deletes: *std.ArrayListUnmanaged([]const u8),
    owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
) !void {
    const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
    errdefer alloc.free(ready_key);
    const owned_len = owned_delete_keys.items.len;
    try owned_delete_keys.append(alloc, ready_key);
    errdefer owned_delete_keys.shrinkRetainingCapacity(owned_len);
    try deletes.append(alloc, ready_key);
    try appendArtifactRepairSummaryRebuildInvalidationForStore(alloc, store, writes, deletes, owned_delete_keys);
}

fn saveArtifactRepairIssueToStoreWithSummary(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    key: []const u8,
    issue: types.ArtifactRepairIssue,
    new_issue: bool,
) !void {
    const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
    defer alloc.free(kind_key);
    const encoded = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
    defer alloc.free(encoded);
    if (!new_issue) {
        const writes = [_]docstore_mod.KVPair{
            .{ .key = key, .value = encoded },
            .{ .key = kind_key, .value = encoded },
        };
        try store.putBatch(writes[0..], &.{});
        return;
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    defer {
        for (writes.items) |item| {
            if (item.value.ptr != encoded.ptr) alloc.free(@constCast(item.value));
        }
        writes.deinit(alloc);
    }
    var deletes = std.ArrayListUnmanaged([]const u8).empty;
    defer deletes.deinit(alloc);
    var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
        owned_delete_keys.deinit(alloc);
    }

    try writes.append(alloc, .{ .key = key, .value = encoded });
    try writes.append(alloc, .{ .key = kind_key, .value = encoded });
    try appendArtifactRepairSummaryDirtyForStore(alloc, store, &writes, &deletes, &owned_delete_keys);
    try store.putBatch(writes.items, deletes.items);
}

pub fn recordEmbeddingArtifactRepairIssueForReplay(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    artifact_key: []const u8,
    sequence: u64,
    reason: types.ArtifactRepairReason,
) !void {
    var identity = (try artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key)) orelse return;
    defer identity.deinit(alloc);

    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(alloc, index_name, "embedding", artifact_key_hex);
    defer alloc.free(issue_key);

    const now_ns = currentTimeNs();
    const existing = try loadArtifactRepairIssueFromStoreByKey(alloc, store, issue_key);
    var issue = if (existing) |loaded|
        loaded
    else
        types.ArtifactRepairIssue{
            .artifact_kind = .embedding,
            .index_name = try alloc.dupe(u8, index_name),
            .doc_key = try alloc.dupe(u8, identity.doc_key),
            .parent_doc_key = try alloc.dupe(u8, identity.parent_doc_key orelse ""),
            .unit_id = try alloc.dupe(u8, identity.unit_id orelse ""),
            .source_artifact_name = try alloc.dupe(u8, identity.source_artifact_name orelse ""),
            .artifact_name = try alloc.dupe(u8, identity.embedding_name),
            .artifact_key = try alloc.dupe(u8, artifact_key_hex),
            .chunk_id = identity.chunk_id,
            .repairable = true,
            .first_seen_ns = now_ns,
        };
    defer issue.deinit(alloc);

    issue.sequence = sequence;
    issue.reason = reason;
    issue.chunk_id = identity.chunk_id;
    issue.repairable = true;
    issue.last_seen_ns = now_ns;
    if (issue.artifact_key.len == 0) issue.artifact_key = try alloc.dupe(u8, artifact_key_hex);
    if (issue.parent_doc_key.len == 0) issue.parent_doc_key = try alloc.dupe(u8, identity.parent_doc_key orelse "");
    if (issue.unit_id.len == 0) issue.unit_id = try alloc.dupe(u8, identity.unit_id orelse "");
    if (issue.source_artifact_name.len == 0) issue.source_artifact_name = try alloc.dupe(u8, identity.source_artifact_name orelse "");

    try saveArtifactRepairIssueToStoreWithSummary(alloc, store, issue_key, issue, existing == null);
}

pub fn recordArtifactRepairIssueForReplay(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    artifact_kind: types.ArtifactRepairKind,
    index_name: []const u8,
    doc_key: []const u8,
    parent_doc_key: []const u8,
    unit_id: []const u8,
    source_artifact_name: []const u8,
    artifact_name: []const u8,
    artifact_key: []const u8,
    chunk_id: ?u32,
    sequence: u64,
    reason: types.ArtifactRepairReason,
) !void {
    const kind_name = @tagName(artifact_kind);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(alloc, index_name, kind_name, artifact_key_hex);
    defer alloc.free(issue_key);

    const now_ns = currentTimeNs();
    const existing = try loadArtifactRepairIssueFromStoreByKey(alloc, store, issue_key);
    var issue = if (existing) |loaded|
        loaded
    else
        types.ArtifactRepairIssue{
            .artifact_kind = artifact_kind,
            .index_name = try alloc.dupe(u8, index_name),
            .doc_key = try alloc.dupe(u8, doc_key),
            .parent_doc_key = try alloc.dupe(u8, parent_doc_key),
            .unit_id = try alloc.dupe(u8, unit_id),
            .source_artifact_name = try alloc.dupe(u8, source_artifact_name),
            .artifact_name = try alloc.dupe(u8, artifact_name),
            .artifact_key = try alloc.dupe(u8, artifact_key_hex),
            .chunk_id = chunk_id,
            .repairable = artifactRepairKindHasAutomatedReprocessor(artifact_kind),
            .unsupported_reason = try alloc.dupe(u8, artifactRepairUnsupportedReason(artifact_kind)),
            .first_seen_ns = now_ns,
        };
    defer issue.deinit(alloc);

    issue.artifact_kind = artifact_kind;
    issue.sequence = sequence;
    issue.reason = reason;
    issue.chunk_id = chunk_id;
    issue.repairable = artifactRepairKindHasAutomatedReprocessor(artifact_kind);
    issue.last_seen_ns = now_ns;
    if (issue.artifact_key.len == 0) issue.artifact_key = try alloc.dupe(u8, artifact_key_hex);
    if (issue.parent_doc_key.len == 0 and parent_doc_key.len > 0) issue.parent_doc_key = try alloc.dupe(u8, parent_doc_key);
    if (issue.unit_id.len == 0 and unit_id.len > 0) issue.unit_id = try alloc.dupe(u8, unit_id);
    if (issue.source_artifact_name.len == 0 and source_artifact_name.len > 0) issue.source_artifact_name = try alloc.dupe(u8, source_artifact_name);
    if (issue.unsupported_reason.len == 0 and !issue.repairable) issue.unsupported_reason = try alloc.dupe(u8, artifactRepairUnsupportedReason(artifact_kind));

    try saveArtifactRepairIssueToStoreWithSummary(alloc, store, issue_key, issue, existing == null);
}

pub fn recordArtifactRepairIssueForRefReplay(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    artifact_ref: types.ArtifactRef,
    artifact_key: []const u8,
    sequence: u64,
    reason: types.ArtifactRepairReason,
) !void {
    const unit_id = artifact_ref.unit_id orelse if (artifact_ref.source) |source| source.unit_id orelse "" else "";
    const parent_doc_key = if (unit_id.len > 0) artifact_ref.document_id else "";
    const source_artifact_name = if (artifact_ref.source) |source| source.name else "";
    try recordArtifactRepairIssueForReplay(
        alloc,
        store,
        repairKindFromArtifactKind(artifact_ref.kind),
        index_name,
        artifact_ref.document_id,
        parent_doc_key,
        unit_id,
        source_artifact_name,
        artifact_ref.name,
        artifact_key,
        artifact_ref.chunk_id,
        sequence,
        reason,
    );
}

pub fn Impl(comptime DB: type) type {
    return struct {
        fn loadArtifactRepairIssueByKey(self: *DB, alloc: Allocator, key: []const u8) !?types.ArtifactRepairIssue {
            return try loadArtifactRepairIssueFromStoreByKey(alloc, self.core.store, key);
        }

        pub fn loadPersistedIndexLoadFailure(self: *DB, alloc: Allocator, index_name: []const u8) !?[]u8 {
            const key = try indexLoadFailureKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            return raw;
        }

        pub fn persistIndexLoadFailuresFromManager(self: *DB, alloc: Allocator) !void {
            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            var failure_batch = try self.core.store.beginWriteBatchWithOptions(.{ .defer_commit_flush = true });
            errdefer failure_batch.abort();
            var wrote = false;
            for (configs) |cfg| {
                const key = try indexLoadFailureKeyAlloc(alloc, cfg.name);
                defer alloc.free(key);
                if (self.core.index_manager.loadFailure(cfg.name)) |err_name| {
                    try failure_batch.put(key, err_name);
                    wrote = true;
                } else {
                    const persisted = self.core.store.get(alloc, key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                    if (persisted) |value| {
                        alloc.free(value);
                        failure_batch.delete(key) catch {};
                        wrote = true;
                    }
                }
            }
            if (wrote) try failure_batch.commit() else failure_batch.abort();
        }

        fn loadArtifactRepairSummaryCountByKey(self: *DB, alloc: Allocator, key: []const u8) !?u64 {
            const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            if (raw.len != @sizeOf(u64)) return error.InvalidArtifactPayload;
            return std.mem.readInt(u64, raw[0..8], .little);
        }

        fn artifactRepairSummaryReady(self: *DB, alloc: Allocator) !bool {
            const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
            defer alloc.free(ready_key);
            const ready = self.core.store.get(alloc, ready_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            alloc.free(ready);
            return true;
        }

        fn scanArtifactRepairIssueCountBounded(self: *DB, alloc: Allocator, index_name: ?[]const u8) !u64 {
            const prefix = try internal_keys.artifactRepairIssueRootPrefixAlloc(alloc);
            defer alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);

            const ScanState = struct {
                const scan_limit: usize = 1024;
                alloc: Allocator,
                index_name: ?[]const u8,
                count: u64 = 0,
                scanned: usize = 0,

                fn scanEntry(ctx: ?*anyopaque, _: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    var issue = try decodeArtifactRepairIssueValueAlloc(state.alloc, value);
                    defer issue.deinit(state.alloc);
                    state.scanned += 1;
                    if (state.index_name == null or std.mem.eql(u8, issue.index_name, state.index_name.?)) state.count += 1;
                    if (state.scanned >= scan_limit) return .stop;
                    return .@"continue";
                }
            };

            var state = ScanState{ .alloc = alloc, .index_name = index_name };
            try self.core.store.scanWithContext(prefix, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);
            return state.count;
        }

        pub const ArtifactRepairSummarySnapshot = struct {
            ready: bool,
            count: u64,
            repair_scan_count: u64 = 0,
        };

        pub fn artifactRepairSummaryRootSnapshot(self: *DB, alloc: Allocator) !ArtifactRepairSummarySnapshot {
            const ready = try artifactRepairSummaryReady(self, alloc);
            const key = if (ready)
                try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc)
            else
                try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
            defer alloc.free(key);
            return .{
                .ready = ready,
                .count = (try loadArtifactRepairSummaryCountByKey(self, alloc, key)) orelse if (ready) 0 else try scanArtifactRepairIssueCountBounded(self, alloc, null),
            };
        }

        fn artifactRepairSummaryIndexSnapshot(self: *DB, alloc: Allocator, index_name: []const u8, ready: bool) !ArtifactRepairSummarySnapshot {
            const key = if (ready)
                try internal_keys.artifactRepairSummaryIndexKeyAlloc(alloc, index_name)
            else
                try internal_keys.artifactRepairSummaryRebuildIndexKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            if (try loadArtifactRepairSummaryCountByKey(self, alloc, key)) |count| {
                return .{ .ready = ready, .count = count };
            }
            if (ready) return .{ .ready = true, .count = 0 };
            const scanned_count = try scanArtifactRepairIssueCountBounded(self, alloc, index_name);
            return .{ .ready = false, .count = scanned_count, .repair_scan_count = scanned_count };
        }

        pub const ArtifactRepairIndexFallbackCounts = struct {
            alloc: Allocator,
            counts: std.StringHashMapUnmanaged(u64) = .empty,
            loaded: bool = false,

            pub fn deinit(self: *@This()) void {
                var it = self.counts.iterator();
                while (it.next()) |entry| self.alloc.free(entry.key_ptr.*);
                self.counts.deinit(self.alloc);
            }
        };

        fn ensureArtifactRepairIndexFallbackCounts(self: *DB, alloc: Allocator, fallback: *ArtifactRepairIndexFallbackCounts) !void {
            if (fallback.loaded) return;
            fallback.loaded = true;

            const prefix = try internal_keys.artifactRepairIssueRootPrefixAlloc(alloc);
            defer alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);

            const ScanState = struct {
                const scan_limit: usize = 1024;

                alloc: Allocator,
                counts: *std.StringHashMapUnmanaged(u64),
                scanned: usize = 0,

                fn scanEntry(ctx: ?*anyopaque, _: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    var issue = try decodeArtifactRepairIssueValueAlloc(state.alloc, value);
                    defer issue.deinit(state.alloc);
                    state.scanned += 1;
                    const result = try state.counts.getOrPut(state.alloc, issue.index_name);
                    if (!result.found_existing) {
                        result.key_ptr.* = try state.alloc.dupe(u8, issue.index_name);
                        result.value_ptr.* = 0;
                    }
                    result.value_ptr.* += 1;
                    if (state.scanned >= scan_limit) return .stop;
                    return .@"continue";
                }
            };

            var state = ScanState{ .alloc = alloc, .counts = &fallback.counts };
            try self.core.store.scanWithContext(prefix, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);
        }

        pub fn artifactRepairSummaryIndexSnapshotForStats(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            ready: bool,
            fallback: *ArtifactRepairIndexFallbackCounts,
        ) !ArtifactRepairSummarySnapshot {
            if (ready) return try artifactRepairSummaryIndexSnapshot(self, alloc, index_name, true);

            const key = try internal_keys.artifactRepairSummaryRebuildIndexKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            if (try loadArtifactRepairSummaryCountByKey(self, alloc, key)) |count| {
                return .{ .ready = false, .count = count };
            }

            try ensureArtifactRepairIndexFallbackCounts(self, alloc, fallback);
            const scanned_count = fallback.counts.get(index_name) orelse 0;
            return .{ .ready = false, .count = scanned_count, .repair_scan_count = scanned_count };
        }

        fn artifactRepairSummaryIndexCount(self: *DB, alloc: Allocator, index_name: []const u8) !u64 {
            return (try artifactRepairSummaryIndexSnapshot(self, alloc, index_name, try artifactRepairSummaryReady(self, alloc))).count;
        }

        fn appendArtifactRepairSummaryWrite(
            self: *DB,
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            deletes: *std.ArrayListUnmanaged([]const u8),
            key: []const u8,
            delta: i64,
        ) !void {
            const existing = (try loadArtifactRepairSummaryCountByKey(self, alloc, key)) orelse 0;
            const updated = if (delta >= 0) existing +| @as(u64, @intCast(delta)) else existing -| @as(u64, @intCast(-delta));
            if (updated == 0) {
                try deletes.append(alloc, key);
                return;
            }
            const value = try alloc.alloc(u8, @sizeOf(u64));
            std.mem.writeInt(u64, value[0..8], updated, .little);
            try writes.append(alloc, .{ .key = key, .value = value });
        }

        pub fn artifactRepairSummaryReadyForStats(self: *DB, alloc: Allocator) !bool {
            return try artifactRepairSummaryReady(self, alloc);
        }

        pub fn artifactRepairSummaryRootCountForStats(self: *DB, alloc: Allocator) !u64 {
            return (try artifactRepairSummaryRootSnapshot(self, alloc)).count;
        }

        pub fn artifactRepairSummaryIndexCountForStats(self: *DB, alloc: Allocator, index_name: []const u8) !u64 {
            return try artifactRepairSummaryIndexCount(self, alloc, index_name);
        }

        fn appendKeysForPrefixDelete(
            self: *DB,
            alloc: Allocator,
            deletes: *std.ArrayListUnmanaged([]const u8),
            owned_keys: *std.ArrayListUnmanaged([]const u8),
            prefix: []const u8,
        ) !void {
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);
            const ScanState = struct {
                alloc: Allocator,
                deletes: *std.ArrayListUnmanaged([]const u8),
                owned_keys: *std.ArrayListUnmanaged([]const u8),

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, _: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    const key_copy = try state.alloc.dupe(u8, key);
                    errdefer state.alloc.free(key_copy);
                    const owned_len = state.owned_keys.items.len;
                    try state.owned_keys.append(state.alloc, key_copy);
                    errdefer state.owned_keys.shrinkRetainingCapacity(owned_len);
                    try state.deletes.append(state.alloc, key_copy);
                    return .@"continue";
                }
            };
            var state = ScanState{ .alloc = alloc, .deletes = deletes, .owned_keys = owned_keys };
            try self.core.store.scanWithContext(prefix, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);
        }

        fn appendArtifactRepairSummaryRebuildInvalidation(
            self: *DB,
            alloc: Allocator,
            writes: ?*std.ArrayListUnmanaged(docstore_mod.KVPair),
            deletes: *std.ArrayListUnmanaged([]const u8),
            owned_keys: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
            errdefer alloc.free(progress_key);
            const owned_len = owned_keys.items.len;
            try owned_keys.append(alloc, progress_key);
            errdefer owned_keys.shrinkRetainingCapacity(owned_len);
            if (writes) |out| {
                const dirty_value = try alloc.dupe(u8, artifact_repair_summary_dirty_marker);
                errdefer alloc.free(dirty_value);
                try out.append(alloc, .{ .key = progress_key, .value = dirty_value });
            } else {
                try deletes.append(alloc, progress_key);
            }

            const rebuild_prefix = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
            defer alloc.free(rebuild_prefix);
            try appendKeysForPrefixDelete(self, alloc, deletes, owned_keys, rebuild_prefix);
        }

        fn appendArtifactRepairSummaryDirty(
            self: *DB,
            alloc: Allocator,
            writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
            deletes: *std.ArrayListUnmanaged([]const u8),
            owned_delete_keys: *std.ArrayListUnmanaged([]const u8),
        ) !void {
            const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
            errdefer alloc.free(ready_key);
            const owned_len = owned_delete_keys.items.len;
            try owned_delete_keys.append(alloc, ready_key);
            errdefer owned_delete_keys.shrinkRetainingCapacity(owned_len);
            try deletes.append(alloc, ready_key);
            try appendArtifactRepairSummaryRebuildInvalidation(self, alloc, writes, deletes, owned_delete_keys);
        }

        fn saveArtifactRepairIssueWithSummary(self: *DB, alloc: Allocator, key: []const u8, issue: types.ArtifactRepairIssue, new_issue: bool) !void {
            const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
            defer alloc.free(kind_key);
            const encoded = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(encoded);
            if (!new_issue) {
                const writes = [_]docstore_mod.KVPair{
                    .{ .key = key, .value = encoded },
                    .{ .key = kind_key, .value = encoded },
                };
                try self.core.store.putBatch(writes[0..], &.{});
                return;
            }

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (writes.items) |item| {
                    if (item.value.ptr != encoded.ptr) alloc.free(@constCast(item.value));
                }
                writes.deinit(alloc);
            }
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(alloc);
            var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
                owned_delete_keys.deinit(alloc);
            }

            try writes.append(alloc, .{ .key = key, .value = encoded });
            try writes.append(alloc, .{ .key = kind_key, .value = encoded });
            try appendArtifactRepairSummaryDirty(self, alloc, &writes, &deletes, &owned_delete_keys);
            try self.core.store.putBatch(writes.items, deletes.items);
        }

        fn clearArtifactRepairIssueWithSummary(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);

            const existing = (try loadArtifactRepairIssueByKey(self, alloc, key)) orelse {
                const stale_kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
                defer alloc.free(stale_kind_key);
                try self.core.store.putBatch(&.{}, &.{stale_kind_key});
                return;
            };
            var existing_issue = existing;
            defer existing_issue.deinit(alloc);

            const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, existing_issue);
            defer alloc.free(kind_key);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (writes.items) |item| alloc.free(@constCast(item.value));
                writes.deinit(alloc);
            }
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(alloc);
            var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
                owned_delete_keys.deinit(alloc);
            }

            try deletes.append(alloc, key);
            try deletes.append(alloc, kind_key);
            try appendArtifactRepairSummaryDirty(self, alloc, &writes, &deletes, &owned_delete_keys);
            try self.core.store.putBatch(writes.items, deletes.items);
        }

        pub fn recordArtifactRepairIssue(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);

            const now_ns = currentTimeNs();
            const existing = try loadArtifactRepairIssueByKey(self, alloc, key);
            var stored = if (existing) |loaded| loaded else try cloneArtifactRepairIssueAlloc(alloc, issue);
            defer stored.deinit(alloc);

            stored.artifact_kind = issue.artifact_kind;
            stored.repairable = artifactRepairKindHasAutomatedReprocessor(issue.artifact_kind);
            stored.sequence = issue.sequence;
            stored.reason = issue.reason;
            if (stored.first_seen_ns == 0) stored.first_seen_ns = now_ns;
            stored.last_seen_ns = now_ns;
            if (stored.artifact_key.len == 0 and issue.artifact_key.len > 0) stored.artifact_key = try alloc.dupe(u8, issue.artifact_key);
            if (stored.parent_doc_key.len == 0 and issue.parent_doc_key.len > 0) stored.parent_doc_key = try alloc.dupe(u8, issue.parent_doc_key);
            if (stored.unit_id.len == 0 and issue.unit_id.len > 0) stored.unit_id = try alloc.dupe(u8, issue.unit_id);
            if (stored.source_artifact_name.len == 0 and issue.source_artifact_name.len > 0) stored.source_artifact_name = try alloc.dupe(u8, issue.source_artifact_name);
            if (stored.unsupported_reason.len == 0 and !stored.repairable) stored.unsupported_reason = try alloc.dupe(u8, artifactRepairUnsupportedReason(stored.artifact_kind));

            try saveArtifactRepairIssueWithSummary(self, alloc, key, stored, existing == null);
        }

        fn rebuildArtifactRepairSummaryIfMissing(self: *DB, alloc: Allocator) !bool {
            if (try artifactRepairSummaryReady(self, alloc)) return false;

            const root_summary_key = try internal_keys.artifactRepairSummaryRootKeyAlloc(alloc);
            defer alloc.free(root_summary_key);
            const rebuild_root_summary_key = try internal_keys.artifactRepairSummaryRebuildRootKeyAlloc(alloc);
            defer alloc.free(rebuild_root_summary_key);
            const progress_key = try internal_keys.artifactRepairSummaryProgressKeyAlloc(alloc);
            defer alloc.free(progress_key);
            const raw_progress = self.core.store.get(alloc, progress_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (raw_progress) |value| alloc.free(value);

            if (raw_progress == null and (try loadArtifactRepairSummaryCountByKey(self, alloc, root_summary_key)) != null) {
                const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
                defer alloc.free(ready_key);
                var deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer deletes.deinit(alloc);
                var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
                defer {
                    for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
                    owned_delete_keys.deinit(alloc);
                }
                try appendArtifactRepairSummaryRebuildInvalidation(self, alloc, null, &deletes, &owned_delete_keys);
                const writes = [_]docstore_mod.KVPair{.{ .key = ready_key, .value = "1" }};
                try self.core.store.putBatchWithReplayWithOptions(null, writes[0..], deletes.items, null, .{ .defer_commit_flush = true });
                return false;
            }
            if (raw_progress == null) {
                var deletes = std.ArrayListUnmanaged([]const u8).empty;
                defer deletes.deinit(alloc);
                var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
                defer {
                    for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
                    owned_delete_keys.deinit(alloc);
                }
                try appendArtifactRepairSummaryRebuildInvalidation(self, alloc, null, &deletes, &owned_delete_keys);
                if (deletes.items.len != 0) {
                    try self.core.store.putBatchWithReplayWithOptions(null, &.{}, deletes.items, null, .{ .defer_commit_flush = true });
                }
            }

            const prefix = try internal_keys.artifactRepairIssueRootPrefixAlloc(alloc);
            defer alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);

            const lower = lower: {
                const progress = raw_progress orelse break :lower try alloc.dupe(u8, prefix);
                if (!std.mem.startsWith(u8, progress, prefix)) break :lower try alloc.dupe(u8, prefix);
                var lower = try alloc.alloc(u8, progress.len + 1);
                @memcpy(lower[0..progress.len], progress);
                lower[progress.len] = 0;
                break :lower lower;
            };
            defer alloc.free(lower);

            const ScanState = struct {
                const batch_limit: usize = 1024;

                alloc: Allocator,
                root_count: u64 = 0,
                per_index: std.StringHashMapUnmanaged(u64) = .empty,
                rows: usize = 0,
                last_key: ?[]u8 = null,

                fn deinit(state: *@This()) void {
                    var it = state.per_index.iterator();
                    while (it.next()) |entry| state.alloc.free(entry.key_ptr.*);
                    state.per_index.deinit(state.alloc);
                    if (state.last_key) |key| state.alloc.free(key);
                }

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    var issue = try decodeArtifactRepairIssueValueAlloc(state.alloc, value);
                    defer issue.deinit(state.alloc);
                    state.root_count += 1;
                    state.rows += 1;
                    const result = try state.per_index.getOrPut(state.alloc, issue.index_name);
                    if (!result.found_existing) {
                        result.key_ptr.* = try state.alloc.dupe(u8, issue.index_name);
                        result.value_ptr.* = 0;
                    }
                    result.value_ptr.* += 1;
                    if (state.last_key) |existing| state.alloc.free(existing);
                    state.last_key = try state.alloc.dupe(u8, key);
                    if (state.rows >= batch_limit) return .stop;
                    return .@"continue";
                }
            };

            var state = ScanState{ .alloc = alloc };
            defer state.deinit();
            try self.core.store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            var owned_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (writes.items) |item| alloc.free(@constCast(item.value));
                for (owned_keys.items) |owned_key| alloc.free(@constCast(owned_key));
                owned_keys.deinit(alloc);
                writes.deinit(alloc);
            }
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(alloc);

            if (state.rows < ScanState.batch_limit) {
                var publish_counts = std.StringHashMapUnmanaged(u64).empty;
                defer publish_counts.deinit(alloc);

                const PublishState = struct {
                    alloc: Allocator,
                    counts: *std.StringHashMapUnmanaged(u64),
                    deletes: *std.ArrayListUnmanaged([]const u8),
                    owned_keys: *std.ArrayListUnmanaged([]const u8),

                    fn addCount(ctx: *@This(), live_key: []u8, value: u64) !void {
                        errdefer ctx.alloc.free(live_key);
                        const result = try ctx.counts.getOrPut(ctx.alloc, live_key);
                        if (result.found_existing) {
                            ctx.alloc.free(live_key);
                            result.value_ptr.* += value;
                        } else {
                            try ctx.owned_keys.append(ctx.alloc, live_key);
                            result.key_ptr.* = live_key;
                            result.value_ptr.* = value;
                        }
                    }

                    fn scanEntry(ctx_raw: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                        const ctx: *@This() = @ptrCast(@alignCast(ctx_raw orelse return error.InvalidArgument));
                        if (value.len != @sizeOf(u64)) return error.InvalidArtifactPayload;
                        const shadow_key = try ctx.alloc.dupe(u8, key);
                        errdefer ctx.alloc.free(shadow_key);
                        const owned_len = ctx.owned_keys.items.len;
                        try ctx.owned_keys.append(ctx.alloc, shadow_key);
                        errdefer ctx.owned_keys.shrinkRetainingCapacity(owned_len);
                        try ctx.deletes.append(ctx.alloc, shadow_key);
                        const count = std.mem.readInt(u64, value[0..8], .little);
                        var live_key = try ctx.alloc.dupe(u8, key);
                        live_key[2] = internal_keys.artifact_repair_summary_kind;
                        try ctx.addCount(live_key, count);
                        return .@"continue";
                    }
                };

                try appendKeysForPrefixDelete(self, alloc, &deletes, &owned_keys, root_summary_key);
                var publish_state = PublishState{
                    .alloc = alloc,
                    .counts = &publish_counts,
                    .deletes = &deletes,
                    .owned_keys = &owned_keys,
                };
                const rebuild_upper = try internal_keys.nextPrefixAlloc(alloc, rebuild_root_summary_key);
                defer if (rebuild_upper) |buf| alloc.free(buf);
                try self.core.store.scanWithContext(
                    rebuild_root_summary_key,
                    if (rebuild_upper) |buf| buf else "",
                    .{},
                    &publish_state,
                    PublishState.scanEntry,
                );

                if (state.root_count != 0) {
                    const live_key = try alloc.dupe(u8, root_summary_key);
                    try PublishState.addCount(&publish_state, live_key, state.root_count);
                }

                var it = state.per_index.iterator();
                while (it.next()) |entry| {
                    const live_key = try internal_keys.artifactRepairSummaryIndexKeyAlloc(alloc, entry.key_ptr.*);
                    try PublishState.addCount(&publish_state, live_key, entry.value_ptr.*);
                }

                var count_it = publish_counts.iterator();
                while (count_it.next()) |entry| {
                    if (entry.value_ptr.* == 0) continue;
                    const value = try alloc.alloc(u8, @sizeOf(u64));
                    std.mem.writeInt(u64, value[0..8], entry.value_ptr.*, .little);
                    try writes.append(alloc, .{ .key = entry.key_ptr.*, .value = value });
                }

                const ready_key = try internal_keys.artifactRepairSummaryReadyKeyAlloc(alloc);
                try owned_keys.append(alloc, ready_key);
                const ready_value = try alloc.dupe(u8, "1");
                try writes.append(alloc, .{ .key = ready_key, .value = ready_value });
                try deletes.append(alloc, progress_key);
            } else if (state.last_key) |last_key| {
                if (state.root_count != 0) {
                    try appendArtifactRepairSummaryWrite(self, alloc, &writes, &deletes, rebuild_root_summary_key, @intCast(state.root_count));
                }

                var it = state.per_index.iterator();
                while (it.next()) |entry| {
                    const key = try internal_keys.artifactRepairSummaryRebuildIndexKeyAlloc(alloc, entry.key_ptr.*);
                    try owned_keys.append(alloc, key);
                    try appendArtifactRepairSummaryWrite(self, alloc, &writes, &deletes, key, @intCast(entry.value_ptr.*));
                }

                const progress_key_copy = try alloc.dupe(u8, progress_key);
                try owned_keys.append(alloc, progress_key_copy);
                const progress_value = try alloc.dupe(u8, last_key);
                try writes.append(alloc, .{ .key = progress_key_copy, .value = progress_value });
                try self.core.store.putBatchWithReplayWithOptions(null, writes.items, deletes.items, null, .{ .defer_commit_flush = true });
                return true;
            }
            try self.core.store.putBatchWithReplayWithOptions(null, writes.items, deletes.items, null, .{ .defer_commit_flush = true });
            return false;
        }

        fn rebuildArtifactRepairKindIndexIfMissing(self: *DB, alloc: Allocator) !bool {
            if (try artifactRepairKindIndexReady(self, alloc)) return false;

            const prefix = try internal_keys.artifactRepairIssueRootPrefixAlloc(alloc);
            defer alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);

            const progress_key = try internal_keys.artifactRepairKindIndexProgressKeyAlloc(alloc);
            defer alloc.free(progress_key);
            const raw_progress = self.core.store.get(alloc, progress_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            defer if (raw_progress) |value| alloc.free(value);

            const lower = lower: {
                const progress = raw_progress orelse break :lower try alloc.dupe(u8, prefix);
                if (!std.mem.startsWith(u8, progress, prefix)) break :lower try alloc.dupe(u8, prefix);
                var lower = try alloc.alloc(u8, progress.len + 1);
                @memcpy(lower[0..progress.len], progress);
                lower[progress.len] = 0;
                break :lower lower;
            };
            defer alloc.free(lower);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer {
                for (writes.items) |item| {
                    alloc.free(@constCast(item.key));
                    alloc.free(@constCast(item.value));
                }
                writes.deinit(alloc);
            }

            const ScanState = struct {
                const batch_limit: usize = 1024;

                alloc: Allocator,
                writes: *std.ArrayListUnmanaged(docstore_mod.KVPair),
                rows: usize = 0,
                last_key: ?[]u8 = null,

                fn deinit(state: *@This()) void {
                    if (state.last_key) |key| state.alloc.free(key);
                }

                fn scanEntry(ctx: ?*anyopaque, _: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    var issue = try decodeArtifactRepairIssueValueAlloc(state.alloc, value);
                    defer issue.deinit(state.alloc);

                    const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(state.alloc, issue);
                    errdefer state.alloc.free(kind_key);
                    const value_copy = try state.alloc.dupe(u8, value);
                    errdefer state.alloc.free(value_copy);
                    try state.writes.append(state.alloc, .{ .key = kind_key, .value = value_copy });
                    state.rows += 1;
                    if (state.last_key) |key| state.alloc.free(key);
                    state.last_key = try artifactRepairIssueKeyForIssueAlloc(state.alloc, issue);
                    if (state.rows >= batch_limit) return .stop;
                    return .@"continue";
                }
            };
            var state = ScanState{
                .alloc = alloc,
                .writes = &writes,
            };
            defer state.deinit();
            try self.core.store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);

            if (state.rows < ScanState.batch_limit) {
                const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
                errdefer alloc.free(ready_key);
                const ready_value = try alloc.dupe(u8, "1");
                errdefer alloc.free(ready_value);
                try writes.append(alloc, .{ .key = ready_key, .value = ready_value });
                const deletes = [_][]const u8{progress_key};
                try self.core.store.putBatchWithReplayWithOptions(null, writes.items, deletes[0..], null, .{ .defer_commit_flush = true });
                return false;
            }

            if (state.last_key) |last_key| {
                const progress_key_copy = try alloc.dupe(u8, progress_key);
                errdefer alloc.free(progress_key_copy);
                const progress_value = try alloc.dupe(u8, last_key);
                errdefer alloc.free(progress_value);
                try writes.append(alloc, .{ .key = progress_key_copy, .value = progress_value });
            }

            try self.core.store.putBatchWithReplayWithOptions(null, writes.items, &.{}, null, .{ .defer_commit_flush = true });
            return true;
        }

        fn artifactRepairScanLowerBoundAlloc(self: *DB, alloc: Allocator, prefix: []const u8, cursor: ?[]const u8) ![]u8 {
            _ = self;
            const raw_cursor = cursor orelse return try alloc.dupe(u8, prefix);
            if (raw_cursor.len == 0) return try alloc.dupe(u8, prefix);
            const key = hexToBytesAlloc(alloc, raw_cursor) catch return error.InvalidArgument;
            defer alloc.free(key);
            if (!std.mem.startsWith(u8, key, prefix)) return error.InvalidArgument;
            var lower = try alloc.alloc(u8, key.len + 1);
            @memcpy(lower[0..key.len], key);
            lower[key.len] = 0;
            return lower;
        }

        fn artifactRepairCursorMatchesPrefix(self: *DB, alloc: Allocator, cursor: ?[]const u8, prefix: []const u8) !bool {
            _ = self;
            const raw_cursor = cursor orelse return true;
            if (raw_cursor.len == 0) return true;
            const key = hexToBytesAlloc(alloc, raw_cursor) catch return false;
            defer alloc.free(key);
            return std.mem.startsWith(u8, key, prefix);
        }

        fn artifactRepairPrimaryScanPrefixAlloc(self: *DB, alloc: Allocator, req: types.ArtifactRepairListRequest) ![]u8 {
            _ = self;
            if (req.index_name) |name| return try internal_keys.artifactRepairIssueIndexPrefixAlloc(alloc, name);
            return try internal_keys.artifactRepairIssueRootPrefixAlloc(alloc);
        }

        fn artifactRepairKindScanPrefixAlloc(self: *DB, alloc: Allocator, req: types.ArtifactRepairListRequest, kind: types.ArtifactRepairKind) ![]u8 {
            _ = self;
            if (req.index_name) |name| return try internal_keys.artifactRepairIssueKindIndexPrefixAlloc(alloc, @tagName(kind), name);
            return try internal_keys.artifactRepairIssueKindRootPrefixAlloc(alloc, @tagName(kind));
        }

        fn artifactRepairKindIndexReady(self: *DB, alloc: Allocator) !bool {
            const ready_key = try internal_keys.artifactRepairKindIndexReadyKeyAlloc(alloc);
            defer alloc.free(ready_key);
            const ready = self.core.store.get(alloc, ready_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            alloc.free(ready);
            return true;
        }

        fn artifactRepairFallbackScanBudget(limit: u32) u64 {
            const requested: u64 = if (limit == 0) 4096 else @as(u64, limit) * 32;
            return @min(@max(requested, 256), 4096);
        }

        fn artifactRepairKindForIndexKind(kind: types.IndexKind) ?types.ArtifactRepairKind {
            return switch (kind) {
                .dense_vector, .sparse_vector => .embedding,
                .graph => .graph,
                .full_text => .full_text,
                .algebraic => .algebraic,
            };
        }

        fn denseCoverageRegressionRepairRequired(self: *DB, alloc: Allocator, index_name: []const u8) !bool {
            const entry = self.core.index_manager.denseIndex(index_name) orelse return false;
            const status_snapshot = (try db_internal.loadIndexStatusSnapshot(alloc, self.core.store, index_name)) orelse return false;
            if (status_snapshot.kind != .dense_vector) return false;
            return status_snapshot.doc_count > entry.index.stats().active_count;
        }

        fn indexRepairRequired(self: *DB, alloc: Allocator, index_name: []const u8) !bool {
            if (self.core.index_manager.loadFailure(index_name) != null) return true;
            if (try denseCoverageRegressionRepairRequired(self, alloc, index_name)) return true;
            const projection_checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
            if (projection_checkpoint.status == .degraded or projection_checkpoint.status == .repair_required) return true;
            return try artifactRepairSummaryIndexCount(self, alloc, index_name) != 0;
        }

        fn listIndexRepairIssuesPage(self: *DB, alloc: Allocator, req: types.ArtifactRepairListRequest) !types.ArtifactRepairListResult {
            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            std.mem.sort(types.IndexConfig, configs, {}, struct {
                fn lessThan(_: void, lhs: types.IndexConfig, rhs: types.IndexConfig) bool {
                    return std.mem.order(u8, lhs.name, rhs.name) == .lt;
                }
            }.lessThan);

            var issues = std.ArrayListUnmanaged(types.ArtifactRepairIssue).empty;
            errdefer {
                for (issues.items) |*issue| issue.deinit(alloc);
                issues.deinit(alloc);
            }

            const cursor = req.cursor orelse "";
            var last_returned: ?[]const u8 = null;
            var has_more = false;
            var scanned: u64 = 0;
            for (configs) |cfg| {
                if (cursor.len != 0 and std.mem.order(u8, cfg.name, cursor) != .gt) continue;
                if (req.index_name) |requested| if (!std.mem.eql(u8, requested, cfg.name)) continue;
                const artifact_kind = artifactRepairKindForIndexKind(cfg.kind) orelse continue;
                if (req.artifact_kind) |requested_kind| if (requested_kind != artifact_kind) continue;
                if (!(try indexRepairRequired(self, alloc, cfg.name))) continue;
                scanned += 1;
                if (req.limit != 0 and issues.items.len >= req.limit) {
                    has_more = true;
                    break;
                }

                const load_error = self.core.index_manager.loadFailure(cfg.name);
                try issues.append(alloc, .{
                    .artifact_kind = artifact_kind,
                    .index_name = try alloc.dupe(u8, cfg.name),
                    .artifact_name = try alloc.dupe(u8, cfg.name),
                    .repairable = artifact_kind != .algebraic,
                    .unsupported_reason = if (artifact_kind == .algebraic) try alloc.dupe(u8, artifactRepairUnsupportedReason(.algebraic)) else "",
                    .reason = if (load_error != null) .unreadable_artifact else .missing_artifact,
                    .last_error = if (load_error) |err_name| try alloc.dupe(u8, err_name) else try alloc.dupe(u8, if (artifact_kind == .algebraic) artifactRepairUnsupportedReason(.algebraic) else "index_repair_required"),
                });
                last_returned = cfg.name;
            }

            return .{
                .issues = try issues.toOwnedSlice(alloc),
                .limit = req.limit,
                .scanned = scanned,
                .next_cursor = if (has_more and last_returned != null) try alloc.dupe(u8, last_returned.?) else null,
                .has_more = has_more,
            };
        }

        pub fn listArtifactRepairIssuesPage(self: *DB, alloc: Allocator, req: types.ArtifactRepairListRequest) !types.ArtifactRepairListResult {
            if (req.target == .index) return try listIndexRepairIssuesPage(self, alloc, req);

            var filtering_without_kind_index = false;
            const prefix = if (req.artifact_kind) |kind| prefix_blk: {
                if (try artifactRepairKindIndexReady(self, alloc)) {
                    const kind_prefix = try artifactRepairKindScanPrefixAlloc(self, alloc, req, kind);
                    if (try artifactRepairCursorMatchesPrefix(self, alloc, req.cursor, kind_prefix)) break :prefix_blk kind_prefix;
                    alloc.free(kind_prefix);
                }
                filtering_without_kind_index = true;
                break :prefix_blk try artifactRepairPrimaryScanPrefixAlloc(self, alloc, req);
            } else try artifactRepairPrimaryScanPrefixAlloc(self, alloc, req);
            defer alloc.free(prefix);
            const upper = try internal_keys.nextPrefixAlloc(alloc, prefix);
            defer if (upper) |buf| alloc.free(buf);
            const lower = try artifactRepairScanLowerBoundAlloc(self, alloc, prefix, req.cursor);
            defer alloc.free(lower);

            const ScanState = struct {
                alloc: Allocator,
                db: *DB,
                artifact_kind: ?types.ArtifactRepairKind,
                limit: u32,
                scan_budget: u64,
                scanned: u64 = 0,
                has_more: bool = false,
                last_cursor: ?[]u8 = null,
                issues: std.ArrayListUnmanaged(types.ArtifactRepairIssue) = .empty,

                fn deinitPartial(state: *@This()) void {
                    for (state.issues.items) |*issue| issue.deinit(state.alloc);
                    state.issues.deinit(state.alloc);
                    if (state.last_cursor) |cursor| state.alloc.free(cursor);
                }

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    if (state.limit != 0 and state.issues.items.len >= state.limit) {
                        state.has_more = true;
                        return .stop;
                    }
                    state.scanned += 1;
                    if (state.last_cursor) |cursor| state.alloc.free(cursor);
                    state.last_cursor = try bytesToHexAlloc(state.alloc, key);
                    var issue = try decodeArtifactRepairIssueValueAlloc(state.alloc, value);
                    errdefer issue.deinit(state.alloc);
                    if (state.artifact_kind) |kind| {
                        if (issue.artifact_kind != kind) {
                            issue.deinit(state.alloc);
                            if (state.scan_budget != 0 and state.scanned >= state.scan_budget) {
                                state.has_more = true;
                                return .stop;
                            }
                            return .@"continue";
                        }
                    }
                    try state.issues.append(state.alloc, issue);
                    if (state.scan_budget != 0 and state.scanned >= state.scan_budget) {
                        state.has_more = true;
                        return .stop;
                    }
                    return .@"continue";
                }
            };

            var state = ScanState{
                .alloc = alloc,
                .db = self,
                .artifact_kind = req.artifact_kind,
                .limit = req.limit,
                .scan_budget = if (filtering_without_kind_index) artifactRepairFallbackScanBudget(req.limit) else 0,
            };
            errdefer state.deinitPartial();
            try self.core.store.scanWithContext(lower, if (upper) |buf| buf else "", .{}, &state, ScanState.scanEntry);
            const issues = try state.issues.toOwnedSlice(alloc);
            state.issues = .empty;
            const next_cursor = if (state.has_more) state.last_cursor else null;
            if (!state.has_more) if (state.last_cursor) |cursor| alloc.free(cursor);
            state.last_cursor = null;
            return .{
                .issues = issues,
                .limit = req.limit,
                .scanned = state.scanned,
                .next_cursor = next_cursor,
                .has_more = state.has_more,
            };
        }

        pub fn listArtifactRepairIssues(self: *DB, alloc: Allocator, artifact_kind: ?types.ArtifactRepairKind, index_name: ?[]const u8, limit: usize) ![]types.ArtifactRepairIssue {
            const page = try self.listArtifactRepairIssuesPage(alloc, .{
                .artifact_kind = artifact_kind,
                .index_name = index_name,
                .limit = @intCast(@min(limit, std.math.maxInt(u32))),
            });
            if (page.next_cursor) |cursor| alloc.free(cursor);
            return page.issues;
        }

        pub fn listEmbeddingArtifactRepairIssues(self: *DB, alloc: Allocator, index_name: ?[]const u8, limit: usize) ![]types.EmbeddingArtifactRepairIssue {
            const generic = try self.listArtifactRepairIssues(alloc, .embedding, index_name, limit);
            defer types.freeArtifactRepairIssues(alloc, generic);

            var issues = try alloc.alloc(types.EmbeddingArtifactRepairIssue, generic.len);
            var count: usize = 0;
            errdefer {
                for (issues[0..count]) |*issue| issue.deinit(alloc);
                alloc.free(issues);
            }
            for (generic) |issue| {
                issues[count] = try types.embeddingArtifactRepairIssueFromArtifactAlloc(alloc, issue);
                count += 1;
            }
            return issues;
        }

        fn documentAssetArtifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            const doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
            if (doc_key.len == 0 or issue.artifact_name.len == 0) return false;
            if (issue.artifact_key.len > 0) {
                const actual_key = try hexToBytesAlloc(alloc, issue.artifact_key);
                defer alloc.free(actual_key);
                const expected_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", issue.artifact_name);
                defer alloc.free(expected_key);
                if (!std.mem.eql(u8, actual_key, expected_key)) return false;
            }
            var manifest = (self.getDocumentArtifactManifest(alloc, doc_key, issue.artifact_name) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return false,
            }) orelse return false;
            defer manifest.deinit(alloc);
            return true;
        }

        fn isRecoverableEmbeddingArtifactError(err: anyerror) bool {
            return switch (err) {
                error.InvalidArtifactHeader,
                error.InvalidArtifactMagic,
                error.UnsupportedArtifactCodecVersion,
                error.InvalidArtifactKind,
                error.InvalidArtifactPayload,
                error.InvalidVectorDimensions,
                error.InvalidSparseEmbedding,
                error.UnsupportedArtifactEncoding,
                error.InvalidEmbeddingVector,
                => true,
                else => false,
            };
        }

        fn embeddingArtifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            const artifact_key = if (issue.artifact_key.len > 0)
                try hexToBytesAlloc(alloc, issue.artifact_key)
            else
                try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, issue.doc_key, issue.artifact_name);
            defer alloc.free(artifact_key);

            const raw = self.core.store.get(alloc, artifact_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            defer alloc.free(raw);

            if (self.core.index_manager.denseIndex(issue.index_name)) |entry| {
                const view = enrichment_artifact_codec.denseEmbeddingVectorView(raw) catch |err| {
                    if (isRecoverableEmbeddingArtifactError(err)) return false;
                    return err;
                };
                if (view) |vector| return vector.len == entry.dims;
                const decoded = enrichment_artifact_codec.decodeDenseEmbeddingAlloc(alloc, raw) catch |err| {
                    if (isRecoverableEmbeddingArtifactError(err)) return false;
                    return err;
                };
                defer alloc.free(decoded);
                return decoded.len == entry.dims;
            }

            if (self.core.index_manager.sparseIndex(issue.index_name) != null) {
                if (enrichment_artifact_codec.sparseEmbeddingVectorView(raw)) |maybe_view| {
                    if (maybe_view != null) return true;
                } else |err| {
                    if (isRecoverableEmbeddingArtifactError(err)) return false;
                    return err;
                }
                var decoded = enrichment_artifact_codec.decodeSparseEmbeddingAlloc(alloc, raw) catch |err| {
                    if (isRecoverableEmbeddingArtifactError(err)) return false;
                    return err;
                };
                decoded.deinit(alloc);
                return true;
            }
            return false;
        }

        fn artifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            return switch (issue.artifact_kind) {
                .embedding => try embeddingArtifactNowReadable(self, alloc, issue),
                .asset => try documentAssetArtifactNowReadable(self, alloc, issue),
                .chunk, .graph, .full_text, .algebraic => false,
            };
        }

        fn deleteStoreKeyIfPresent(self: *DB, key: []const u8) !void {
            self.core.store.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }

        fn deleteAssetRepairArtifactIfPresent(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
            if (issue.artifact_key.len == 0) return;
            const doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
            if (doc_key.len == 0 or issue.artifact_name.len == 0) return error.InvalidArtifactPayload;
            const actual_key = try hexToBytesAlloc(alloc, issue.artifact_key);
            defer alloc.free(actual_key);
            const expected_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", issue.artifact_name);
            defer alloc.free(expected_key);
            if (!std.mem.eql(u8, actual_key, expected_key)) return error.InvalidArtifactPayload;
            try deleteStoreKeyIfPresent(self, actual_key);
        }

        fn embeddingRepairArtifactKeyAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) !?[]u8 {
            if (issue.artifact_key.len > 0) return try hexToBytesAlloc(alloc, issue.artifact_key);
            if (issue.doc_key.len == 0 or issue.artifact_name.len == 0) return null;
            return try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, issue.doc_key, issue.artifact_name);
        }

        fn deleteEmbeddingRepairArtifactIfPresent(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
            const artifact_key = (try embeddingRepairArtifactKeyAlloc(alloc, issue)) orelse return;
            defer alloc.free(artifact_key);
            if (issue.artifact_key.len > 0) {
                var identity = (try artifact_ids.decodeEmbeddingArtifactIdentityAlloc(alloc, artifact_key)) orelse return error.InvalidArtifactPayload;
                defer identity.deinit(alloc);
                if (!std.mem.eql(u8, identity.embedding_name, issue.artifact_name)) return error.InvalidArtifactPayload;
                if (!std.mem.eql(u8, identity.doc_key, issue.doc_key)) return error.InvalidArtifactPayload;
                if (issue.parent_doc_key.len > 0) {
                    const identity_parent = identity.parent_doc_key orelse return error.InvalidArtifactPayload;
                    if (!std.mem.eql(u8, identity_parent, issue.parent_doc_key)) return error.InvalidArtifactPayload;
                }
            }
            try deleteStoreKeyIfPresent(self, artifact_key);
        }

        fn reprocessEmbeddingArtifactIssue(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            var cfg = (try self.getEnrichment(alloc, .embedding, issue.artifact_name)) orelse return false;
            defer cfg.deinit(alloc);

            const source_doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
            const value = try self.get(alloc, source_doc_key) orelse return error.NotFound;
            defer alloc.free(value);

            try deleteEmbeddingRepairArtifactIfPresent(self, alloc, issue);
            const writes = [_]types.BatchWrite{.{ .key = source_doc_key, .value = value }};
            const force_artifacts = [_][]const u8{issue.artifact_name};
            try DB.WritePathCallbacks.batch_internal(self, .{
                .writes = &writes,
                .sync_level = .full_index,
            }, null, .{
                .force_generated_artifact_names = &force_artifacts,
                .admission_prechecked = true,
            });
            const sequence = self.core.nextDerivedSequence();
            try self.runEnrichmentUntil(sequence);
            self.runDerivedUntil(self.core.nextDerivedSequence()) catch |err| switch (err) {
                error.ArtifactRepairRequired => {},
                else => return err,
            };
            return true;
        }

        pub fn reprocessDocumentEmbeddingArtifact(
            self: *DB,
            alloc: Allocator,
            doc_key: []const u8,
            artifact_name: []const u8,
        ) !bool {
            const issue = types.ArtifactRepairIssue{
                .artifact_kind = .embedding,
                .doc_key = doc_key,
                .artifact_name = artifact_name,
            };
            return reprocessEmbeddingArtifactIssue(self, alloc, issue) catch |err| switch (err) {
                error.NotFound => false,
                else => return err,
            };
        }

        fn reprocessArtifactIssue(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            return switch (issue.artifact_kind) {
                .embedding => try reprocessEmbeddingArtifactIssue(self, alloc, issue),
                .asset => blk: {
                    try deleteAssetRepairArtifactIfPresent(self, alloc, issue);
                    break :blk try self.reprocessDocumentArtifact(alloc, if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key, issue.artifact_name);
                },
                .chunk, .graph, .full_text, .algebraic => false,
            };
        }

        pub fn repairArtifactIssuesWithRequest(self: *DB, alloc: Allocator, req: types.ArtifactRepairRunRequest) !types.ArtifactRepairResult {
            return try self.repairArtifactIssuesWithRequestOptions(alloc, req, .{});
        }

        pub fn repairArtifactIssuesWithRequestOptions(self: *DB, alloc: Allocator, req: types.ArtifactRepairRunRequest, options: types.ArtifactRepairRunOptions) !types.ArtifactRepairResult {
            try checkArtifactRepairCancelled(options);
            if (req.target == .index) return try repairIndexIssuesWithRequest(self, alloc, req, options);

            const page = try self.listArtifactRepairIssuesPage(alloc, .{
                .artifact_kind = req.artifact_kind,
                .index_name = req.index_name,
                .limit = req.limit,
                .cursor = req.cursor,
            });
            defer {
                types.freeArtifactRepairIssues(alloc, page.issues);
                if (page.next_cursor) |cursor| alloc.free(cursor);
            }

            var result: types.ArtifactRepairResult = .{
                .limit = req.limit,
                .has_more = page.has_more,
                .debt_remaining = page.has_more,
            };
            if (page.next_cursor) |cursor| result.next_cursor = try alloc.dupe(u8, cursor);
            for (page.issues) |*issue| {
                if (options.cancelled()) {
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    return result;
                }
                result.scanned += 1;
                issue.attempts += 1;
                issue.last_seen_ns = currentTimeNs();

                if (!artifactRepairKindHasAutomatedReprocessor(issue.artifact_kind)) {
                    const unsupported_reason = artifactRepairUnsupportedReason(issue.artifact_kind);
                    issue.repairable = false;
                    if (issue.unsupported_reason.len == 0) issue.unsupported_reason = try alloc.dupe(u8, unsupported_reason);
                    try replaceRepairIssueLastError(alloc, issue, unsupported_reason);
                    try saveArtifactRepairIssueToStore(alloc, self.core.store, issue.*);
                    result.unsupported += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    continue;
                }

                const reprocessed = reprocessArtifactIssue(self, alloc, issue.*) catch |err| switch (err) {
                    error.NotFound => {
                        try replaceRepairIssueLastError(alloc, issue, "source_document_missing");
                        try saveArtifactRepairIssueToStore(alloc, self.core.store, issue.*);
                        result.missing_source_docs += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        continue;
                    },
                    else => {
                        try replaceRepairIssueLastError(alloc, issue, @errorName(err));
                        try saveArtifactRepairIssueToStore(alloc, self.core.store, issue.*);
                        result.failed += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        continue;
                    },
                };
                if (!reprocessed) {
                    const unavailable_error = switch (issue.artifact_kind) {
                        .embedding => "embedding_enrichment_unavailable",
                        else => "artifact_reprocessor_unavailable",
                    };
                    try replaceRepairIssueLastError(alloc, issue, unavailable_error);
                    try saveArtifactRepairIssueToStore(alloc, self.core.store, issue.*);
                    result.failed += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    continue;
                }
                result.reprocessed += 1;
                if (try artifactNowReadable(self, alloc, issue.*)) {
                    try clearArtifactRepairIssueWithSummary(self, alloc, issue.*);
                    result.repaired += 1;
                } else {
                    try replaceRepairIssueLastError(alloc, issue, "artifact_still_unreadable");
                    try saveArtifactRepairIssueToStore(alloc, self.core.store, issue.*);
                    result.failed += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                }
            }
            if (result.repaired != 0) {
                self.runDerivedUntil(self.core.nextDerivedSequence()) catch |err| switch (err) {
                    error.ArtifactRepairRequired => {},
                    else => return err,
                };
                try runArtifactRepairMetadataMaintenanceUntilIdle(self);
            }
            return result;
        }

        pub fn clearActiveIndexRepairsLocked(self: *DB) void {
            var it = self.active_index_repairs.keyIterator();
            while (it.next()) |key_ptr| self.alloc.free(@constCast(key_ptr.*));
            self.active_index_repairs.clearRetainingCapacity();
        }

        fn beginIndexRepairLease(self: *DB, index_name: []const u8) !bool {
            db_internal.lockAtomicWithBackoff(&self.index_repair_mutex);
            defer self.index_repair_mutex.unlock();
            if (self.index_repair_barriers.load(.acquire) != 0) return false;
            if (self.active_index_repairs.contains(index_name)) return false;
            const owned = try self.alloc.dupe(u8, index_name);
            errdefer self.alloc.free(owned);
            try self.active_index_repairs.put(self.alloc, owned, {});
            return true;
        }

        fn endIndexRepairLease(self: *DB, index_name: []const u8) void {
            db_internal.lockAtomicWithBackoff(&self.index_repair_mutex);
            defer self.index_repair_mutex.unlock();
            if (self.active_index_repairs.fetchRemove(index_name)) |removed| {
                self.alloc.free(@constCast(removed.key));
            }
        }

        pub fn beginIndexRepairBarrier(self: *DB) void {
            _ = self.index_repair_barriers.fetchAdd(1, .acq_rel);
            var spins: usize = 0;
            while (self.published_dense_searches.load(.acquire) != 0) : (spins += 1) {
                if (spins < 64) {
                    std.atomic.spinLoopHint();
                } else {
                    std.Thread.yield() catch {};
                }
            }
        }

        pub fn endIndexRepairBarrier(self: *DB) void {
            _ = self.index_repair_barriers.fetchSub(1, .acq_rel);
        }

        const ShadowIndexReplacementResult = struct {
            reprocessed: u64 = 0,
            applied_sequence: u64 = 0,
        };

        fn saveShadowReplacementAppliedSequence(
            self: *DB,
            alloc: Allocator,
            shadow_manager: *index_manager_mod.IndexManager,
            shadow_checkpoint_path: []const u8,
            index_ref: index_manager_mod.ManagedIndexRef,
            sequence: u64,
        ) !void {
            try shadow_manager.checkpointLsmWalForManagedIndex(index_ref);
            const update = apply_state.AppliedSequenceUpdate{
                .index_name = index_ref.name,
                .sequence = sequence,
            };
            try apply_state.saveAppliedSequenceUpdateWithCheckpoint(alloc, self.core.store, shadow_checkpoint_path, update);
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(alloc, self.core.store, shadow_manager, &[_]apply_state.AppliedSequenceUpdate{update});
        }

        fn scanStoreForRebuildContext(
            ctx: *db_internal.AsyncContext(DB),
            lower: []const u8,
            upper: []const u8,
            options: docstore_mod.DocStore.ScanOptions,
            scan_ctx: ?*anyopaque,
            callback: docstore_mod.DocStore.ScanWithContextCallback,
        ) !void {
            if (ctx.snapshot_read_txn) |txn| {
                try ctx.store.scanReadTxnWithContext(txn, lower, upper, options, scan_ctx, callback);
                return;
            }
            try ctx.store.scanWithContext(lower, upper, options, scan_ctx, callback);
        }

        fn freeGraphRepairWrites(alloc: Allocator, writes: []types.GraphEdgeWrite) void {
            for (writes) |write| {
                alloc.free(@constCast(write.index_name));
                alloc.free(@constCast(write.source));
                alloc.free(@constCast(write.target));
                alloc.free(@constCast(write.edge_type));
                if (write.metadata_json.len > 0) alloc.free(@constCast(write.metadata_json));
            }
        }

        fn applySplitGraphArtifactsForIndexStreamingContext(
            self: *DB,
            ctx: *db_internal.AsyncContext(DB),
            index_name: []const u8,
            batch_size: usize,
        ) !usize {
            _ = ctx.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
            const store_lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(store_lower);
            const effective_batch_size = @max(batch_size, 1);

            const ScanState = struct {
                db: *DB,
                ctx: *db_internal.AsyncContext(DB),
                index_name: []const u8,
                batch_size: usize,
                writes: std.ArrayListUnmanaged(types.GraphEdgeWrite) = .empty,
                mutation_count: usize = 0,

                fn clearWrites(state: *@This()) void {
                    freeGraphRepairWrites(state.ctx.alloc, state.writes.items);
                    state.writes.clearRetainingCapacity();
                }

                fn deinit(state: *@This()) void {
                    state.clearWrites();
                    state.writes.deinit(state.ctx.alloc);
                }

                fn flush(state: *@This()) !void {
                    try db_internal.checkAsyncRepairCancelled(state.ctx);
                    if (state.writes.items.len == 0) return;
                    if (builtin.is_test) {
                        _ = test_graph_repair_stream_flushes.fetchAdd(1, .monotonic);
                    }
                    try state.ctx.index_manager.applyGraphMutationsByName(state.index_name, state.writes.items, &.{});
                    state.mutation_count += state.writes.items.len;
                    state.clearWrites();
                }

                fn scanEntry(scan_ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(scan_ctx orelse return error.InvalidArgument));
                    if (!internal_keys.isGraphEdgeArtifactKey(key)) return .@"continue";
                    try db_internal.checkAsyncRepairCancelled(state.ctx);
                    const parsed = (try internal_keys.parseGraphEdgeArtifactKeyAlloc(state.ctx.alloc, key)) orelse return .@"continue";
                    defer {
                        state.ctx.alloc.free(parsed.doc_key);
                        state.ctx.alloc.free(parsed.index_name);
                        state.ctx.alloc.free(parsed.edge_type);
                        state.ctx.alloc.free(parsed.target_doc_key);
                    }
                    if (!std.mem.eql(u8, parsed.index_name, state.index_name)) return .@"continue";

                    var decoded = enrichment_artifact_codec.decodeGraphEdgeAlloc(state.ctx.alloc, value) catch |err| switch (err) {
                        error.OutOfMemory => return err,
                        else => {
                            const artifact_name = try std.fmt.allocPrint(state.ctx.alloc, "{s}:{s}", .{ parsed.edge_type, parsed.target_doc_key });
                            defer state.ctx.alloc.free(artifact_name);
                            try recordArtifactRepairIssue(state.db, state.ctx.alloc, .{
                                .artifact_kind = .graph,
                                .index_name = parsed.index_name,
                                .doc_key = parsed.doc_key,
                                .artifact_name = artifact_name,
                                .artifact_key = key,
                                .sequence = state.ctx.repair_sequence,
                                .reason = .corrupt_artifact,
                            });
                            return .@"continue";
                        },
                    };
                    errdefer decoded.deinit(state.ctx.alloc);
                    try state.writes.append(state.ctx.alloc, .{
                        .index_name = try state.ctx.alloc.dupe(u8, parsed.index_name),
                        .source = try state.ctx.alloc.dupe(u8, parsed.doc_key),
                        .target = try state.ctx.alloc.dupe(u8, parsed.target_doc_key),
                        .edge_type = try state.ctx.alloc.dupe(u8, parsed.edge_type),
                        .weight = decoded.weight,
                        .created_at = decoded.created_at,
                        .updated_at = decoded.updated_at,
                        .metadata_json = decoded.metadata_json,
                    });
                    decoded.metadata_json = &.{};
                    decoded.deinit(state.ctx.alloc);
                    if (state.writes.items.len >= state.batch_size) try state.flush();
                    return .@"continue";
                }
            };

            var state = ScanState{
                .db = self,
                .ctx = ctx,
                .index_name = index_name,
                .batch_size = effective_batch_size,
            };
            defer state.deinit();

            try scanStoreForRebuildContext(ctx, store_lower, "", .{}, &state, ScanState.scanEntry);
            try state.flush();
            return state.mutation_count;
        }

        fn catchUpShadowReplacementUntil(
            self: *DB,
            alloc: Allocator,
            shadow_manager: *index_manager_mod.IndexManager,
            shadow_checkpoint_path: []const u8,
            index_ref: index_manager_mod.ManagedIndexRef,
            target_sequence: u64,
            options: types.ArtifactRepairRunOptions,
        ) !u64 {
            if (target_sequence == 0) return 0;
            var batch_ctx = self.batchContext();
            batch_ctx.index_manager = shadow_manager;
            batch_ctx.applied_sequence_checkpoint_path = shadow_checkpoint_path;
            batch_ctx.async_context = null;
            batch_ctx.dense_bulk_session_scope = .external;

            var applied = try apply_state.loadAppliedSequenceWithCheckpoint(alloc, self.core.store, shadow_checkpoint_path, index_ref.name);
            while (applied < target_sequence) {
                try checkArtifactRepairCancelled(options);
                const stats = try DB.DerivedAsyncCallbacks.catch_up_managed_index_with_batch_context(&batch_ctx, index_ref, applied, target_sequence);
                const advanced = stats.appliedSequenceAdvance(applied) orelse blk: {
                    if (stats.shouldTryTargetAdvance(applied, target_sequence) and
                        try DB.DerivedAsyncCallbacks.can_advance_derived_replay_target_for_batch_context(&batch_ctx, index_ref, applied, target_sequence))
                    {
                        break :blk target_sequence;
                    }
                    break :blk applied;
                };
                if (advanced <= applied) return applied;
                try saveShadowReplacementAppliedSequence(self, alloc, shadow_manager, shadow_checkpoint_path, index_ref, advanced);
                applied = advanced;
            }
            try checkArtifactRepairCancelled(options);
            return applied;
        }

        fn rebuildIndexWithShadowReplacement(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            options: types.ArtifactRepairRunOptions,
        ) !ShadowIndexReplacementResult {
            try checkArtifactRepairCancelled(options);
            const shadow_base = try createUniqueRepairShadowBase(alloc, self.core.path);
            try index_manager_mod.IndexManager.writeRepairShadowInProgressMarker(alloc, shadow_base);
            var shadow_installed = false;
            defer {
                var io_impl = threadedIo();
                defer io_impl.deinit();
                if (!shadow_installed) {
                    index_manager_mod.IndexManager.clearRepairShadowInProgressMarker(alloc, shadow_base) catch {};
                    std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_base) catch {};
                }
                alloc.free(shadow_base);
            }

            const shadow_indexes_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{shadow_base});
            defer alloc.free(shadow_indexes_path);
            try ensureDirPath(shadow_indexes_path);
            const shadow_index_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ shadow_indexes_path, cfg.name });
            defer alloc.free(shadow_index_path);
            const shadow_checkpoint_path = try std.fmt.allocPrint(alloc, "{s}/applied-sequences", .{shadow_base});
            defer alloc.free(shadow_checkpoint_path);

            var shadow_manager = try index_manager_mod.IndexManager.initWithOptions(alloc, shadow_base, self.index_backends);
            var shadow_manager_open = true;
            defer if (shadow_manager_open) shadow_manager.deinit();
            shadow_manager.setAppliedSequenceCheckpointPath(shadow_checkpoint_path);
            try shadow_manager.registerReplacementIndex(self.core.store, cfg);

            const AsyncContext = db_internal.AsyncContext(DB);
            var shadow_ctx = AsyncContext{
                .alloc = alloc,
                .io = self.backend_runtime.io(),
                .store = self.core.store,
                .applied_sequence_checkpoint_path = shadow_checkpoint_path,
                .index_manager = &shadow_manager,
                .apply_mutex = self.async_context.apply_mutex,
                .dense_bulk_session_scope = .external,
                .resolution_runtime = self.resolution_runtime,
                .promotion_runtime = self.promotion_runtime,
                .repair_options = options,
            };
            defer shadow_ctx.deinit(alloc);

            var build_floor_sequence: u64 = 0;
            const rebuilt: u64 = switch (cfg.kind) {
                .dense_vector, .sparse_vector, .graph, .full_text => rebuilt_blk: {
                    try checkArtifactRepairCancelled(options);
                    var snapshot_txn = try self.core.store.beginReadTxn();
                    var snapshot_open = true;
                    defer if (snapshot_open) snapshot_txn.abort();
                    build_floor_sequence = try self.core.store.lastReplaySequenceFromTxn(&snapshot_txn, 0);
                    shadow_ctx.repair_sequence = build_floor_sequence;
                    shadow_ctx.snapshot_read_txn = &snapshot_txn;
                    defer shadow_ctx.snapshot_read_txn = null;
                    const count: u64 = switch (cfg.kind) {
                        .dense_vector => @intCast(try DB.DerivedAsyncCallbacks.rebuild_dense_index_for_target_coverage_context(&shadow_ctx, cfg.name, 2048)),
                        .sparse_vector => @intCast(try DB.DerivedAsyncCallbacks.rebuild_sparse_index_from_stored_embedding_artifacts_context(&shadow_ctx, cfg.name, 2048)),
                        .graph => @intCast(try applySplitGraphArtifactsForIndexStreamingContext(self, &shadow_ctx, cfg.name, graph_repair_rebuild_batch_size)),
                        .full_text => try shadow_manager.resetFullTextIndexForArtifactRebuildFromReadTxn(self.core.store, &snapshot_txn, cfg.name, options.cancel_check),
                        else => unreachable,
                    };
                    snapshot_txn.abort();
                    snapshot_open = false;
                    break :rebuilt_blk count;
                },
                .algebraic => return error.UnsupportedOperation,
            };

            const index_ref = index_manager_mod.ManagedIndexRef{ .name = cfg.name, .kind = cfg.kind };
            try saveShadowReplacementAppliedSequence(self, alloc, &shadow_manager, shadow_checkpoint_path, index_ref, build_floor_sequence);
            if (self.shadow_index_repair_hook) |hook| {
                try hook.after_snapshot_build(hook.ptr, self, cfg.name, build_floor_sequence);
            }
            _ = try catchUpShadowReplacementUntil(self, alloc, &shadow_manager, shadow_checkpoint_path, index_ref, self.core.nextDerivedSequence(), options);

            const use_dense_search_barrier = cfg.kind == .dense_vector;
            if (use_dense_search_barrier) beginIndexRepairBarrier(self);
            defer if (use_dense_search_barrier) endIndexRepairBarrier(self);

            try checkArtifactRepairCancelled(options);
            self.core.lockApply();
            defer self.core.unlockApply();

            const final_target = self.core.nextDerivedSequence();
            const reached_target = try catchUpShadowReplacementUntil(self, alloc, &shadow_manager, shadow_checkpoint_path, index_ref, final_target, options);
            if (reached_target < final_target) return error.ShadowIndexCatchUpIncomplete;
            try checkArtifactRepairCancelled(options);

            shadow_manager.deinit();
            shadow_manager_open = false;
            try self.core.index_manager.installBuiltReplacementIndex(self.core.store, cfg, shadow_index_path);
            shadow_installed = true;
            index_manager_mod.IndexManager.clearRepairShadowInProgressMarker(alloc, shadow_base) catch |err| {
                std.log.warn("failed to clear repair shadow in-progress marker index={s} err={s}", .{ cfg.name, @errorName(err) });
            };

            const prior_checkpoint = try apply_state.loadProjectionCheckpointWithSidecar(
                alloc,
                self.core.store,
                self.core.applied_sequence_checkpoint_path,
                cfg.name,
            );
            const final_update = apply_state.AppliedSequenceUpdate{
                .index_name = cfg.name,
                .sequence = final_target,
                .status = .clean,
                .generation = prior_checkpoint.generation +| 1,
                .config_hash = types.indexConfigHash(cfg),
            };
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(alloc, self.core.store, self.core.index_manager, &[_]apply_state.AppliedSequenceUpdate{final_update});
            try self.core.saveAppliedSequence(cfg.name, final_target);
            try apply_state.saveProjectionCheckpointWithSidecar(alloc, self.core.store, self.core.applied_sequence_checkpoint_path, cfg.name, .{
                .applied_sequence = final_update.sequence,
                .status = final_update.status,
                .generation = final_update.generation,
                .config_hash = final_update.config_hash,
            });

            return .{
                .reprocessed = rebuilt,
                .applied_sequence = final_target,
            };
        }

        fn repairIndexIssuesWithRequest(self: *DB, alloc: Allocator, req: types.ArtifactRepairRunRequest, options: types.ArtifactRepairRunOptions) !types.ArtifactRepairResult {
            try checkArtifactRepairCancelled(options);
            const requested_index = req.index_name orelse return error.InvalidArgument;
            if (req.cursor != null and req.cursor.?.len != 0) return error.InvalidArgument;
            const limit = if (req.limit == 0) @as(u32, 100) else req.limit;
            var result = types.ArtifactRepairResult{ .limit = limit };
            if (limit == 0) return result;

            const cfg_ptr = self.core.index_manager.get(requested_index) orelse return error.NotFound;
            var cfg = try types.IndexConfig.clone(alloc, cfg_ptr.*);
            defer cfg.deinit(alloc);

            if (req.artifact_kind) |kind| {
                const matches_kind = switch (cfg.kind) {
                    .dense_vector, .sparse_vector => kind == .embedding,
                    .graph => kind == .graph,
                    .full_text => kind == .full_text,
                    .algebraic => kind == .algebraic,
                };
                if (!matches_kind) return error.NotFound;
            }

            const repair_required = try indexRepairRequired(self, alloc, cfg.name);
            if (!repair_required and !req.force) return result;

            if (!(try beginIndexRepairLease(self, cfg.name))) {
                result.scanned += 1;
                result.in_progress += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                return result;
            }
            defer endIndexRepairLease(self, cfg.name);

            result.scanned += 1;
            if (self.core.index_manager.loadFailure(cfg.name) != null) {
                result.indexes_degraded += 1;
                _ = try self.retryQuarantinedIndexLoads(true);
                if (self.core.index_manager.loadFailure(cfg.name) != null) {
                    switch (cfg.kind) {
                        .dense_vector, .sparse_vector, .graph, .full_text => {
                            _ = self.core.index_manager.reopenQuarantinedIndexForArtifactRebuild(self.core.store, cfg.name) catch {
                                result.failed += 1;
                                result.unresolved += 1;
                                result.debt_remaining = true;
                                return result;
                            };
                        },
                        .algebraic => {
                            result.failed += 1;
                            result.unresolved += 1;
                            result.debt_remaining = true;
                            return result;
                        },
                    }
                }
            }

            if (cfg.kind == .algebraic) {
                result.unsupported += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                return result;
            }
            const rebuilt = rebuildIndexWithShadowReplacement(self, alloc, cfg, options) catch |err| switch (err) {
                error.Canceled => {
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    return result;
                },
                error.ShadowIndexCatchUpIncomplete => {
                    result.failed += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    return result;
                },
                else => return err,
            };
            result.reprocessed += rebuilt.reprocessed;
            result.indexes_rebuilt += 1;
            if (try indexRepairRequired(self, alloc, cfg.name)) {
                result.unresolved += 1;
                result.debt_remaining = true;
            } else {
                result.repaired += 1;
            }
            return result;
        }

        pub fn artifactRepairMetadataRebuildPending(self: *DB) bool {
            const summary_ready = artifactRepairSummaryReady(self, self.alloc) catch return true;
            if (!summary_ready) return true;
            const kind_index_ready = artifactRepairKindIndexReady(self, self.alloc) catch return true;
            return !kind_index_ready;
        }

        pub fn runArtifactRepairMetadataMaintenancePass(self: *DB) !bool {
            if (DB.LifecycleCallbacks.open_mode_requires_read_only_backends(self.open_mode)) return false;
            self.core.lockApply();
            defer self.core.unlockApply();

            var rebuilt = false;
            rebuilt = (try rebuildArtifactRepairSummaryIfMissing(self, self.alloc)) or rebuilt;
            rebuilt = (try rebuildArtifactRepairKindIndexIfMissing(self, self.alloc)) or rebuilt;
            return rebuilt;
        }

        pub fn runArtifactRepairMetadataMaintenanceUntilIdle(self: *DB) !void {
            while (try runArtifactRepairMetadataMaintenancePass(self)) {}
        }

        const artifact_repair_metadata_poll_ns: u64 = 5 * std.time.ns_per_s;
        const artifact_repair_metadata_active_poll_ns: u64 = 100 * std.time.ns_per_ms;
        const artifact_repair_metadata_sleep_slice_ns: u64 = 25 * std.time.ns_per_ms;

        pub fn startArtifactRepairMetadataWorkerIfNeeded(self: *DB) void {
            if (comptime builtin.single_threaded or builtin.os.tag == .freestanding) return;
            if (comptime builtin.is_test) return;
            if (!self.start_index_workers) return;
            if (DB.LifecycleCallbacks.open_mode_requires_read_only_backends(self.open_mode)) return;
            if (self.artifact_repair_metadata_future != null) return;
            const io_impl = self.backend_runtime.io_impl orelse return;
            self.artifact_repair_metadata_stop.store(false, .release);
            self.artifact_repair_metadata_future = io_impl.io().concurrent(artifactRepairMetadataWorkerMain, .{self}) catch |err| {
                std.log.warn("artifact repair metadata worker spawn failed: {}", .{err});
                return;
            };
        }

        pub fn stopArtifactRepairMetadataWorker(self: *DB) void {
            self.artifact_repair_metadata_stop.store(true, .release);
            if (self.artifact_repair_metadata_future) |*future| {
                if (self.backend_runtime.io_impl) |io_impl| {
                    _ = future.await(io_impl.io());
                }
                self.artifact_repair_metadata_future = null;
            }
        }

        fn sleepArtifactRepairMetadataWorker(self: *DB, target_ns: u64) bool {
            var slept: u64 = 0;
            while (slept < target_ns) : (slept += artifact_repair_metadata_sleep_slice_ns) {
                if (self.artifact_repair_metadata_stop.load(.acquire)) return false;
                db_internal.sleepNs(artifact_repair_metadata_sleep_slice_ns);
            }
            return !self.artifact_repair_metadata_stop.load(.acquire);
        }

        fn artifactRepairMetadataWorkerMain(self: *DB) void {
            while (true) {
                const active = artifactRepairMetadataRebuildPending(self);
                if (!sleepArtifactRepairMetadataWorker(self, if (active) artifact_repair_metadata_active_poll_ns else artifact_repair_metadata_poll_ns)) return;
                if (self.artifact_repair_metadata_stop.load(.acquire)) return;
                _ = runArtifactRepairMetadataMaintenancePass(self) catch |err| {
                    std.log.warn("artifact repair metadata maintenance pass failed: {}", .{err});
                    continue;
                };
            }
        }

        pub fn repairArtifactIssues(self: *DB, alloc: Allocator, artifact_kind: ?types.ArtifactRepairKind, limit: usize) !types.ArtifactRepairResult {
            return try self.repairArtifactIssuesWithRequest(alloc, .{
                .artifact_kind = artifact_kind,
                .limit = @intCast(@min(limit, std.math.maxInt(u32))),
            });
        }

        pub fn repairEmbeddingArtifactIssues(self: *DB, alloc: Allocator, limit: usize) !types.EmbeddingArtifactRepairResult {
            return try self.repairArtifactIssues(alloc, .embedding, limit);
        }
    };
}
