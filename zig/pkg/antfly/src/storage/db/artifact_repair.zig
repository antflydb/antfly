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
const background_runtime_mod = @import("../background_runtime.zig");
const db_config = @import("config.zig");
const db_internal = @import("internal.zig");
const derived_worker = @import("derived/derived_worker.zig");
const index_generation_manifest = @import("derived/index_generation_manifest.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const range_cardinality = @import("range_cardinality.zig");
const derived_types = @import("derived/derived_types.zig");
const docstore_mod = @import("../docstore.zig");
const embedder_mod = @import("enrichment/embedder.zig");
const asset_producer_mod = @import("enrichment/asset_producer.zig");
const enrichment_runtime_mod = @import("enrichment/enrichment_runtime.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");
const fs_paths = @import("../../common/fs_paths.zig");
const graph_mod = @import("../../graph/graph.zig");
const hbc_mod = @import("../hbc_adapter.zig");
const index_manager_mod = @import("catalog/index_manager.zig");
const internal_keys = @import("../internal_keys.zig");
const resource_manager_mod = @import("../resource_manager.zig");
const types = @import("types.zig");

const artifact_repair_summary_dirty_marker = "dirty";
const index_load_failure_prefix = "\x00\x00__metadata__:index_load_failure:";
const graph_repair_rebuild_batch_size: usize = 2048;
const threadedIo = db_internal.threadedIo;
const TestHelpers = if (builtin.is_test) @import("test_support.zig") else struct {};
const lockAtomicWithBackoff = db_internal.lockAtomicWithBackoff;
var test_graph_repair_stream_flushes: std.atomic.Value(u64) = .init(0);
var repair_shadow_nonce: std.atomic.Value(u64) = .init(0);
pub var test_dense_repair_rebuild_batch_size: ?usize = null;
pub var test_index_repair_catch_up_max_records_per_window: ?usize = null;
pub var test_quarantine_publication_fence_entered: std.atomic.Value(bool) = .init(false);

fn tempPath(buf: []u8) [*:0]const u8 {
    return TestHelpers.tempPath(buf);
}

fn cleanupTempDir(path: [*:0]const u8) void {
    TestHelpers.cleanupTempDir(path);
}

fn currentTimeNs() u64 {
    return db_internal.currentTimeNs();
}

const DerivedCoverageOutcome = enum { produced, skipped, terminal_failed };

fn loadDerivedCoverageOutcomeCounterFromStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    generation: u64,
    outcome: []const u8,
) !?u64 {
    const counter_key = try internal_keys.derivedCoverageOutcomeCountKeyAlloc(alloc, index_name, generation, outcome);
    defer alloc.free(counter_key);
    const raw = store.get(alloc, counter_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    return try internal_keys.decodeDerivedCoverageOutcomeCount(raw);
}

fn monotonicTimeNs() u64 {
    return @import("antfly_platform").time.monotonicNs();
}

fn elapsedSince(start_ns: u64) u64 {
    return monotonicTimeNs() - start_ns;
}

pub fn observeRepairCatchUpCost(previous_ns_per_sequence: u64, from_sequence: u64, to_sequence: u64, elapsed_ns: u64) u64 {
    const advanced = to_sequence -| from_sequence;
    if (advanced == 0) return previous_ns_per_sequence;
    const observed = @max(@as(u64, 1), (elapsed_ns +| advanced -| 1) / advanced);
    // Retain the slower observed rate. Activation admission should be
    // conservative when foreground pressure makes recent replay slower.
    return @max(previous_ns_per_sequence, observed);
}

pub fn repairActivationReplayDeadline(now_ns: u64, activation_deadline_ns: u64, pause_budget_ns: u64) ?u64 {
    if (now_ns >= activation_deadline_ns) return null;
    // Reserve a bounded tail for manifest validation, pointer publication,
    // and clean-checkpoint durability. Five milliseconds is sufficient for
    // small budgets; large budgets reserve at most fifty milliseconds so
    // replay still receives the majority of the fence interval.
    const reserve_ns = @max(
        5 * std.time.ns_per_ms,
        @min(50 * std.time.ns_per_ms, pause_budget_ns / 5),
    );
    if (activation_deadline_ns - now_ns <= reserve_ns) return null;
    return activation_deadline_ns - reserve_ns;
}

pub fn repairActivationAdmissible(
    gap_sequences: u64,
    observed_ns_per_sequence: u64,
    max_gap_sequences: u64,
    max_pause_ns: u64,
) bool {
    if (gap_sequences == 0) return true;
    if (gap_sequences > max_gap_sequences or observed_ns_per_sequence == 0) return false;
    const estimated_pause_ns = std.math.mul(u64, gap_sequences, observed_ns_per_sequence) catch std.math.maxInt(u64);
    return estimated_pause_ns <= max_pause_ns;
}

fn ensureRepairActivationDeadline(deadline_ns: u64) !void {
    if (monotonicTimeNs() >= deadline_ns) return error.ShadowIndexCatchUpIncomplete;
}

fn directoryUsageBytes(alloc: Allocator, path: []const u8) !u64 {
    var io_impl = threadedIo();
    defer io_impl.deinit();
    var dir = std.Io.Dir.cwd().openDir(io_impl.io(), path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io_impl.io());
    var total: u64 = 0;
    var walker = try dir.walk(alloc);
    defer walker.deinit();
    while (try walker.next(io_impl.io())) |entry| {
        if (entry.kind != .file) continue;
        const stat = try dir.statFile(io_impl.io(), entry.path, .{});
        total +|= stat.size;
    }
    return total;
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

pub fn bytesToHexAlloc(alloc: Allocator, bytes: []const u8) ![]u8 {
    const out = try alloc.alloc(u8, bytes.len * 2);
    for (bytes, 0..) |byte, idx| {
        out[idx * 2] = std.fmt.digitToChar(byte >> 4, .lower);
        out[idx * 2 + 1] = std.fmt.digitToChar(byte & 0x0f, .lower);
    }
    return out;
}

fn nextArtifactRepairIssueTimestamp(previous: u64, observed_now: u64) u64 {
    // The realtime clock may have coarse resolution. Keep this revision field
    // strictly monotonic per issue so concurrent publication can never compare
    // equal to the stale revision a repair captured before provider work.
    return @max(observed_now, previous +| 1);
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
        .embedding, .asset, .chunk => true,
        .graph, .full_text, .algebraic => false,
    };
}

fn repairKindFromArtifactKind(kind: types.ArtifactKind) types.ArtifactRepairKind {
    return switch (kind) {
        .asset => .asset,
        .chunk => .chunk,
        .embedding => .embedding,
    };
}

test "db generic artifact repair queue regenerates set-valued chunk artifacts" {
    const DB = @import("mod.zig").DB;
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addEnrichment(.{
        .name = "body_chunks_v1",
        .kind = .chunk,
        .field = "body",
        .chunk_size = 8,
    });
    try db.batch(.{
        .writes = &.{.{
            .key = "doc:a",
            // Empty input intentionally produces no physical chunk. Successful
            // regeneration of this empty set must still retire durable debt.
            .value = "{\"body\":\"\"}",
        }},
        .sync_level = .full_index,
    });

    const chunk_prefix = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "chunk", "body_chunks_v1");
    defer alloc.free(chunk_prefix);
    const artifact_key_hex = try bytesToHexAlloc(alloc, chunk_prefix);
    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .chunk,
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "body_chunks_v1"),
        .artifact_key = artifact_key_hex,
        .reason = .enrichment_failed,
        .sequence = 1,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const repair = try db.repairArtifactIssues(alloc, .chunk, 10);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.failed);
    try std.testing.expect(!repair.debt_remaining);

    const issues_after = try db.listArtifactRepairIssues(alloc, .chunk, null, 0);
    defer types.freeArtifactRepairIssues(alloc, issues_after);
    try std.testing.expectEqual(@as(usize, 0), issues_after.len);
}

test "db repair completion cannot clear debt behind terminal coverage" {
    const DB = @import("mod.zig").DB;
    const Repair = Impl(DB);
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3}",
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic");
    defer alloc.free(artifact_key);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .embedding,
        .index_name = try alloc.dupe(u8, "semantic"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "semantic"),
        .artifact_key = artifact_key_hex,
        .reason = .enrichment_failed,
        .sequence = 7,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const pending = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    const revision = Repair.ArtifactRepairIssueRevision.capture(pending[0]);
    const completion_key = try Repair.artifactRepairCompletionKeyForIssueAlloc(&db, alloc, pending[0]);
    defer alloc.free(completion_key);
    const completion = try Repair.artifactRepairCompletionSnapshot(&db, alloc, completion_key);

    const generation = db.core.index_manager.coverageGenerationForIndex("semantic") orelse return error.TestUnexpectedResult;
    const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "semantic", generation, "doc:a");
    defer alloc.free(marker_key);
    try db.core.store.put(marker_key, "terminal_failed");
    try std.testing.expectEqual(
        Repair.ArtifactRepairCompletionDisposition.stale,
        try Repair.completeArtifactRepairIssueIfCurrent(
            &db,
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch +% 1,
            false,
        ),
    );
    try std.testing.expectEqual(
        Repair.ArtifactRepairCompletionDisposition.coverage_incomplete,
        try Repair.completeArtifactRepairIssueIfCurrent(
            &db,
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch,
            false,
        ),
    );

    try db.core.store.put(marker_key, "produced");
    try std.testing.expectEqual(
        Repair.ArtifactRepairCompletionDisposition.completed,
        try Repair.completeArtifactRepairIssueIfCurrent(
            &db,
            alloc,
            pending[0],
            revision,
            completion_key,
            completion.epoch,
            false,
        ),
    );
    const remaining = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "db shared repair completion reprocesses consumer with terminal coverage" {
    const DB = @import("mod.zig").DB;
    const Repair = Impl(DB);
    const alloc = std.testing.allocator;

    var path_buf: [256]u8 = undefined;
    const path = tempPath(&path_buf);
    defer cleanupTempDir(path);

    var deterministic = embedder_mod.DeterministicDenseEmbedder{};
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_optional_runtime_workers = false,
        .enrichment = .{
            .owner_id = "worker-a",
            .dense_embedder = deterministic.interface(),
        },
    });
    defer db.close();
    try db.addIndex(.{
        .name = "semantic",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"dense_v1\"}}",
    });
    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"body\":\"repair me\"}" }},
        .sync_level = .full_index,
    });

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dense_v1");
    defer alloc.free(artifact_key);
    var issue = types.ArtifactRepairIssue{
        .artifact_kind = .embedding,
        .index_name = try alloc.dupe(u8, "semantic"),
        .doc_key = try alloc.dupe(u8, "doc:a"),
        .artifact_name = try alloc.dupe(u8, "dense_v1"),
        .artifact_key = try bytesToHexAlloc(alloc, artifact_key),
        .reason = .enrichment_failed,
        .sequence = 7,
    };
    defer issue.deinit(alloc);
    try db.recordArtifactRepairIssue(alloc, issue);

    const pending = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    const completion_key = try Repair.artifactRepairCompletionKeyForIssueAlloc(&db, alloc, pending[0]);
    defer alloc.free(completion_key);
    var completion = try Repair.artifactRepairCompletionSnapshot(&db, alloc, completion_key);
    completion.completed_sequence = pending[0].sequence;
    var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
    encodeArtifactRepairCompletionState(&encoded_completion, completion);
    try db.core.store.put(completion_key, &encoded_completion);

    const generation = db.core.index_manager.coverageGenerationForIndex("semantic") orelse return error.TestUnexpectedResult;
    const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, "semantic", generation, "doc:a");
    defer alloc.free(marker_key);
    try DB.DerivedAsyncCallbacks.set_derived_coverage_outcomes(alloc, db.core.store, db.core.index_manager, "semantic", &.{.{
        .doc_key = "doc:a",
        .outcome = .terminal_failed,
    }});
    var repair = try db.repairArtifactIssues(alloc, .embedding, 1);
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 1), repair.reprocessed);
    try std.testing.expectEqual(@as(u64, 1), repair.repaired);
    try std.testing.expectEqual(@as(u64, 0), repair.unresolved);
    try std.testing.expect(!repair.debt_remaining);

    try std.testing.expect(try Repair.repairCoverageMarkerComplete(&db, alloc, marker_key));
    const remaining = try db.listArtifactRepairIssues(alloc, .embedding, "semantic", 1);
    defer types.freeArtifactRepairIssues(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
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
    const discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    try std.testing.expectEqual(@as(usize, 1), discovery.discovered);
    try std.testing.expect(try reopened.hasPendingIndexRepairIntents(alloc));
    const duplicate_discovery = try reopened.discoverRecoverableStartupIndexFailures(alloc, 1);
    try std.testing.expectEqual(@as(usize, 0), duplicate_discovery.discovered);
    try std.testing.expectEqual(@as(usize, 1), duplicate_discovery.already_pending);
    var discovered_state = try reopened.loadIndexRepairState(alloc);
    const repair_id = discovered_state.entries.items[0].intent.repair_id;
    try std.testing.expectEqual(index_repair_state.Phase.detected, discovered_state.entries.items[0].intent.phase);
    try std.testing.expect(discovered_state.entries.items[0].pin == null);
    discovered_state.deinit(alloc);

    var pinned_snapshot = try Impl(DB).beginPinnedIndexRepairSnapshot(&reopened, alloc, repair_id);
    const pinned_floor = pinned_snapshot.build_floor_sequence;
    pinned_snapshot.deinit();
    var pinned_state = try reopened.loadIndexRepairState(alloc);
    try std.testing.expectEqual(@as(?u64, pinned_floor), pinned_state.minimumRetainAfterSequence());
    pinned_state.deinit(alloc);

    // Discovery and pin acquisition must not delete or reopen the poisoned
    // root merely to make rebuilding convenient.
    try std.testing.expectError(
        error.IncompleteBulkPublish,
        hbc_mod.HBCIndex.openWithLsmOptions(alloc, dense_index_path_z, .{
            .dims = 3,
            .storage_backend = .lsm,
        }, .{}),
    );
    {
        const status_key = try db_internal.indexStatusKeyAlloc(alloc, "dense_idx");
        defer alloc.free(status_key);
        var stale_status: [db_internal.index_status_encoded_len]u8 = undefined;
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

    const OwnershipFence = struct {
        lost: bool = false,

        fn current(ptr: *anyopaque) anyerror!bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return !self.lost;
        }

        fn afterSnapshot(ptr: *anyopaque, _: *DB, _: []const u8, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.lost = true;
        }
    };
    var ownership_fence = OwnershipFence{};
    reopened.shadow_index_repair_hook = .{
        .ptr = &ownership_fence,
        .after_snapshot_build = OwnershipFence.afterSnapshot,
    };
    const ownership_lost = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{
        .owner_epoch = 17,
        .activation_check = .{
            .ptr = &ownership_fence,
            .is_current_owner = OwnershipFence.current,
        },
    });
    reopened.shadow_index_repair_hook = null;
    try std.testing.expect(ownership_lost.attempted);
    try std.testing.expect(ownership_lost.deferred);
    try std.testing.expect(!ownership_lost.repaired);
    try std.testing.expect(try reopened.hasPendingIndexRepairIntents(alloc));
    var reserved_state = try reopened.loadIndexRepairState(alloc);
    try std.testing.expect(reserved_state.entries.items[0].intent.estimated_candidate_bytes > 0);
    try std.testing.expectEqual(@as(u64, 17), reserved_state.entries.items[0].intent.owner_epoch);
    try std.testing.expect(
        reserved_state.entries.items[0].intent.planned_disk_bytes >=
            reserved_state.entries.items[0].intent.estimated_candidate_bytes,
    );
    reserved_state.deinit(alloc);
    try std.testing.expectError(error.IndexRebuilding, reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{ .vector = &.{ 1, 0, 0 }, .k = 1 } },
        .limit = 1,
    }));
    try std.testing.expectError(
        error.StaleIndexRepairControl,
        Impl(DB).pauseAutomaticIndexRepair(&reopened, alloc, "dense_idx", repair_id + 1),
    );
    try std.testing.expect(try Impl(DB).resumeAutomaticIndexRepair(&reopened, alloc, "dense_idx", null));

    const repair = try reopened.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(repair.attempted);
    try std.testing.expect(repair.repaired);
    // The first attempt reached a durable candidate before ownership loss;
    // retry reopens and catches up that generation instead of rescanning the
    // primary corpus.
    try std.testing.expectEqual(@as(u64, 0), repair.documents_reprocessed);
    try std.testing.expect(reopened.core.index_manager.loadFailure("dense_idx") == null);
    try std.testing.expect(!try reopened.hasPendingIndexRepairIntents(alloc));
    var completed_state = try reopened.loadIndexRepairState(alloc);
    defer completed_state.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 0), completed_state.entries.items.len);
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

    // The adopted shadow handle must be a normal active runtime, including
    // loader callbacks and subsequent foreground apply; it must not retain a
    // dependency on the destroyed shadow manager.
    try reopened.batch(.{
        .writes = &.{.{ .key = "doc:c", .value = "{\"title\":\"gamma\",\"_embeddings\":{\"dense_idx\":[0,0,1]}}" }},
        .sync_level = .full_index,
    });
    var after_adoption = try reopened.search(alloc, .{
        .index_name = "dense_idx",
        .query = .{ .dense_knn = .{
            .vector = &.{ 0.0, 0.0, 1.0 },
            .k = 1,
        } },
        .limit = 1,
    });
    defer after_adoption.deinit();
    try std.testing.expectEqual(@as(u32, 1), after_adoption.total_hits);
    try std.testing.expectEqualStrings("doc:c", after_adoption.hits[0].id);
}

test "db artifact repair dense repair reprocesses corrupt managed source before rebuilding" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var counting = TestHelpers.CountingDenseEmbedder{};
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

    const cfg: types.IndexConfig = .{
        .name = "semantic_idx",
        .kind = .dense_vector,
        .config_json = "{\"field\":\"embedding\",\"dims\":3,\"metric\":\"cosine\",\"generator\":{\"kind\":\"dense_embedding\",\"source_field\":\"body\",\"embedding_name\":\"semantic_idx\"}}",
    };
    try db.addIndex(cfg);
    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:a", .value = "{\"title\":\"alpha\",\"body\":\"alpha concept overview\"}" },
            .{ .key = "doc:b", .value = "{\"title\":\"beta\",\"body\":\"beta architecture notes\"}" },
        },
        .sync_level = .write,
    });
    // The bounded workerless repair owner drains the re-armed producer
    // synchronously before yielding its turn.
    try db.runUntilIdle();

    const artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "semantic_idx");
    defer alloc.free(artifact_key);
    try db.core.store.put(artifact_key, "bad-artifact");

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .artifact_kind = .embedding,
        .index_name = "semantic_idx",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    const repair_id = (try db.indexRepairIdForIndex(alloc, "semantic_idx")) orelse return error.TestUnexpectedResult;

    const incomplete = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(incomplete.attempted);
    try std.testing.expect(incomplete.deferred);
    try std.testing.expect(!incomplete.repaired);
    try std.testing.expect(db.enrichment_runtime != null);
    try std.testing.expect(!db.enrichment_runtime.?.isStarted());

    try db.runUntilIdle();
    const repaired_artifact = try db.core.store.get(alloc, artifact_key);
    defer alloc.free(repaired_artifact);
    try TestHelpers.expectDenseEmbeddingArtifactValue(
        alloc,
        repaired_artifact,
        enrichment_artifact_codec.hashSource("alpha concept overview"),
        3,
    );

    const reset = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(reset.attempted);
    try std.testing.expect(reset.deferred);
    const recovered = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(recovered.has_repair_outcome);
    try std.testing.expectEqual(@as(u64, 1), recovered.indexes_rebuilt);
    try std.testing.expect(!recovered.repaired);
    try std.testing.expect(recovered.debt_remaining);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));

    const query_vec = try counting.interface().embedDense(alloc, "semantic_idx", "alpha concept overview", 3);
    defer alloc.free(query_vec);
    var result = try db.search(alloc, .{
        .index_name = "semantic_idx",
        .dense = .{ .vector = query_vec, .k = 2 },
        .limit = 2,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 2), result.total_hits);
    try std.testing.expectEqualStrings("doc:a", result.hits[0].id);
}

test "db artifact repair managed operator repair persists intent without running reconstruction inline" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;
    const Repair = Impl(DB);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

    var queued = try db.repairArtifactIssuesWithRequestOptions(alloc, .{
        .target = .index,
        .index_name = "graph_v1",
        .limit = 1,
        .force = true,
    }, .{ .defer_durable_index_repair_execution = true });
    defer queued.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), queued.scanned);
    try std.testing.expectEqual(@as(u64, 1), queued.in_progress);
    try std.testing.expectEqual(@as(u64, 0), queued.indexes_rebuilt);
    try std.testing.expect(queued.debt_remaining);

    const repair_id = (try db.indexRepairIdForIndex(alloc, "graph_v1")) orelse
        return error.TestUnexpectedResult;
    var pending = try Repair.loadIndexRepairEntryById(&db, alloc, repair_id);
    defer pending.deinit(alloc);
    try std.testing.expectEqual(index_repair_state.Phase.detected, pending.intent.phase);
    try std.testing.expect(pending.intent.candidate_relative_path == null);

    const completed = try db.advanceIndexRepairIntent(alloc, repair_id, .{});
    try std.testing.expect(completed.attempted);
    try std.testing.expect(completed.repaired);
    try std.testing.expect(!try db.hasPendingIndexRepairIntents(alloc));
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

test "db artifact repair issue list reports recorded graph issue" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

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
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 10,
    });
    defer page.deinit(alloc);

    try std.testing.expect(!page.has_more);
    try std.testing.expectEqual(@as(usize, 1), page.issues.len);
    try std.testing.expectEqual(.graph, page.issues[0].artifact_kind);
    try std.testing.expectEqual(.corrupt_artifact, page.issues[0].reason);
    try std.testing.expectEqualStrings("graph_v1", page.issues[0].index_name);
    try std.testing.expectEqualStrings("", page.issues[0].last_error);
}

test "db artifact repair reports remaining debt when source is missing" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });

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
        .artifact_kind = .graph,
        .index_name = "graph_v1",
        .limit = 1,
    });
    defer repair.deinit(alloc);
    try std.testing.expectEqual(@as(u64, 1), repair.scanned);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_rebuilt);
    try std.testing.expectEqual(@as(u64, 0), repair.repaired);
    try std.testing.expectEqual(@as(u64, 1), repair.unresolved);
    try std.testing.expect(repair.debt_remaining);

    const stats = try db.stats(alloc);
    defer types.freeDBStats(alloc, stats);
    try std.testing.expect(stats.repair_degraded);
    try std.testing.expectEqual(@as(u64, 1), stats.repair_issue_count);
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
    const Repair = Impl(DB);

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{ .name = "graph_v1", .kind = .graph, .config_json = "{}" });
    try std.testing.expect(try Repair.beginIndexRepairLease(&db, "graph_v1"));
    defer Repair.endIndexRepairLease(&db, "graph_v1");

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
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_after);
}

test "db graph index repair records corrupt artifact debt during shadow rebuild" {
    const alloc = std.testing.allocator;
    const DB = @import("mod.zig").DB;

    var path_buf: [256]u8 = undefined;
    const path = TestHelpers.tempPath(&path_buf);
    defer TestHelpers.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
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
    // The generation rebuild completed, but the corrupt source artifact remains
    // durable debt. `repaired` is the stronger healthy outcome, not a duplicate
    // count of completed rebuild work.
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
        reopened.backend_runtime.durable_jobs.drainOwner(reopened.repair_cleanup_owner_id);
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
        try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_before);
        try std.testing.expectEqual(@as(u64, 0), repair.indexes_degraded_after);
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
    try std.testing.expectEqual(@as(u64, 1), repair.indexes_degraded_before);
    try std.testing.expectEqual(@as(u64, 0), repair.indexes_degraded_after);
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
    try std.testing.expect(issues[0].repairable);
    try std.testing.expectEqualStrings("", issues[0].unsupported_reason);
    try std.testing.expectEqualStrings("page:000001", issues[0].unit_id);
    try std.testing.expectEqualStrings("page:000002", issues[1].unit_id);
}

fn artifactRepairUnsupportedReason(kind: types.ArtifactRepairKind) []const u8 {
    return switch (kind) {
        .embedding, .asset, .chunk => "",
        .graph => "graph_reprocessor_unavailable",
        .full_text => "full_text_reprocessor_unavailable",
        .algebraic => "algebraic_index_rebuild_unavailable",
    };
}

pub fn artifactRepairIssueKeyForIssueAlloc(alloc: Allocator, issue: types.ArtifactRepairIssue) ![]u8 {
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
        .generation_attempts = Fields.u64Value(obj, "generation_attempts"),
        .generation_error = try alloc.dupe(u8, Fields.string(obj, "generation_error")),
        .attempts = Fields.u64Value(obj, "attempts"),
        .first_seen_ns = Fields.u64Value(obj, "first_seen_ns"),
        .last_seen_ns = Fields.u64Value(obj, "last_seen_ns"),
        .last_error = try alloc.dupe(u8, Fields.string(obj, "last_error")),
    };
}

pub fn loadArtifactRepairIssueFromStoreByKey(alloc: Allocator, store: *docstore_mod.DocStore, key: []const u8) !?types.ArtifactRepairIssue {
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
        .generation_attempts = issue.generation_attempts,
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
    out.generation_error = try alloc.dupe(u8, issue.generation_error);
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

const artifact_repair_completion_state_len = 24;

const ArtifactRepairCompletionState = struct {
    /// Incremented for every failure publication for this physical artifact.
    /// A repair may commit only if the epoch observed before provider work is
    /// still current afterward.
    epoch: u64 = 0,
    completed_sequence: u64 = 0,
    pending_issues: u64 = 0,
};

fn encodeArtifactRepairCompletionState(
    out: *[artifact_repair_completion_state_len]u8,
    state: ArtifactRepairCompletionState,
) void {
    std.mem.writeInt(u64, out[0..8], state.epoch, .little);
    std.mem.writeInt(u64, out[8..16], state.completed_sequence, .little);
    std.mem.writeInt(u64, out[16..24], state.pending_issues, .little);
}

fn loadArtifactRepairCompletionStateFromStore(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    key: []const u8,
) !?ArtifactRepairCompletionState {
    const raw = store.get(alloc, key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    defer alloc.free(raw);
    // This fence is an optimization, never authoritative repair debt. Treat a
    // malformed value as absent so metadata corruption cannot wedge repair;
    // the next issue transition overwrites or deletes it atomically.
    if (raw.len != artifact_repair_completion_state_len) return null;
    return .{
        .epoch = std.mem.readInt(u64, raw[0..8], .little),
        .completed_sequence = std.mem.readInt(u64, raw[8..16], .little),
        .pending_issues = std.mem.readInt(u64, raw[16..24], .little),
    };
}

fn saveArtifactRepairIssueToStoreWithSummary(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    key: []const u8,
    issue: types.ArtifactRepairIssue,
    new_issue: bool,
    extra_writes: []const docstore_mod.KVPair,
    extra_deletes: []const []const u8,
) !void {
    const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
    defer alloc.free(kind_key);
    const encoded = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
    defer alloc.free(encoded);
    if (!new_issue) {
        var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
        defer writes.deinit(alloc);
        try writes.append(alloc, .{ .key = key, .value = encoded });
        try writes.append(alloc, .{ .key = kind_key, .value = encoded });
        try writes.appendSlice(alloc, extra_writes);
        try store.putBatch(writes.items, extra_deletes);
        return;
    }

    var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
    var borrowed_write_count: usize = 0;
    defer {
        for (writes.items[borrowed_write_count..]) |item| alloc.free(@constCast(item.value));
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
    borrowed_write_count = writes.items.len;
    try writes.appendSlice(alloc, extra_writes);
    borrowed_write_count = writes.items.len;
    try deletes.appendSlice(alloc, extra_deletes);
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
    issue.last_seen_ns = nextArtifactRepairIssueTimestamp(issue.last_seen_ns, now_ns);
    if (issue.artifact_key.len == 0) issue.artifact_key = try alloc.dupe(u8, artifact_key_hex);
    if (issue.parent_doc_key.len == 0) issue.parent_doc_key = try alloc.dupe(u8, identity.parent_doc_key orelse "");
    if (issue.unit_id.len == 0) issue.unit_id = try alloc.dupe(u8, identity.unit_id orelse "");
    if (issue.source_artifact_name.len == 0) issue.source_artifact_name = try alloc.dupe(u8, identity.source_artifact_name orelse "");

    const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(alloc, "embedding", artifact_key_hex);
    defer alloc.free(completion_key);
    try saveArtifactRepairIssueToStoreWithSummary(
        alloc,
        store,
        issue_key,
        issue,
        existing == null,
        &.{},
        &.{completion_key},
    );
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
    issue_mutex: ?*std.atomic.Mutex,
) !void {
    if (issue_mutex) |mutex| lockAtomicWithBackoff(mutex);
    defer if (issue_mutex) |mutex| mutex.unlock();

    const kind_name = @tagName(artifact_kind);
    const artifact_key_hex = try bytesToHexAlloc(alloc, artifact_key);
    defer alloc.free(artifact_key_hex);
    const issue_id = try artifactRepairIssueIdAlloc(alloc, .{
        .artifact_kind = artifact_kind,
        .index_name = index_name,
        .doc_key = doc_key,
        .parent_doc_key = parent_doc_key,
        .unit_id = unit_id,
        .source_artifact_name = source_artifact_name,
        .artifact_name = artifact_name,
        .artifact_key = artifact_key_hex,
        .chunk_id = chunk_id,
    });
    defer alloc.free(issue_id);
    const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(alloc, index_name, kind_name, issue_id);
    defer alloc.free(issue_key);

    const now_ns = currentTimeNs();
    const existing = try loadArtifactRepairIssueFromStoreByKey(alloc, store, issue_key);
    const existing_was_pending = if (existing) |loaded| loaded.reason == .enrichment_failed else false;
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
    issue.last_seen_ns = nextArtifactRepairIssueTimestamp(issue.last_seen_ns, now_ns);
    if (issue.artifact_key.len == 0) issue.artifact_key = try alloc.dupe(u8, artifact_key_hex);
    if (issue.parent_doc_key.len == 0 and parent_doc_key.len > 0) issue.parent_doc_key = try alloc.dupe(u8, parent_doc_key);
    if (issue.unit_id.len == 0 and unit_id.len > 0) issue.unit_id = try alloc.dupe(u8, unit_id);
    if (issue.source_artifact_name.len == 0 and source_artifact_name.len > 0) issue.source_artifact_name = try alloc.dupe(u8, source_artifact_name);
    if (issue.unsupported_reason.len == 0 and !issue.repairable) issue.unsupported_reason = try alloc.dupe(u8, artifactRepairUnsupportedReason(artifact_kind));

    const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(alloc, kind_name, issue_id);
    defer alloc.free(completion_key);
    if (reason == .enrichment_failed) {
        var completion = (try loadArtifactRepairCompletionStateFromStore(alloc, store, completion_key)) orelse ArtifactRepairCompletionState{
            .pending_issues = @intFromBool(existing_was_pending),
        };
        if (existing_was_pending) completion.pending_issues = @max(completion.pending_issues, 1);
        completion.epoch +%= 1;
        if (completion.epoch == 0) completion.epoch = 1;
        completion.completed_sequence = 0;
        if (!existing_was_pending) completion.pending_issues +|= 1;
        var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
        encodeArtifactRepairCompletionState(&encoded_completion, completion);
        try saveArtifactRepairIssueToStoreWithSummary(
            alloc,
            store,
            issue_key,
            issue,
            existing == null,
            &.{.{ .key = completion_key, .value = &encoded_completion }},
            &.{},
        );
    } else {
        try saveArtifactRepairIssueToStoreWithSummary(alloc, store, issue_key, issue, existing == null, &.{}, &.{completion_key});
    }
}

pub fn recordArtifactRepairIssueForRefReplay(
    alloc: Allocator,
    store: *docstore_mod.DocStore,
    index_name: []const u8,
    artifact_ref: types.ArtifactRef,
    artifact_key: []const u8,
    sequence: u64,
    reason: types.ArtifactRepairReason,
    issue_mutex: ?*std.atomic.Mutex,
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
        issue_mutex,
    );
}

pub fn Impl(comptime DB: type) type {
    return struct {
        fn applyCommittedRepairOutcomeToAdvance(
            result: *IndexRepairAdvanceResult,
            repair: types.ArtifactRepairResult,
        ) void {
            result.has_repair_outcome = true;
            result.artifacts_repaired = repair.repaired;
            result.missing_source_docs = repair.missing_source_docs;
            result.failed = repair.failed;
            result.unsupported = repair.unsupported;
            result.unresolved = repair.unresolved;
            result.in_progress = repair.in_progress;
            result.indexes_rebuilt = repair.indexes_rebuilt;
            result.indexes_degraded_before = repair.indexes_degraded_before;
            result.indexes_degraded_after = repair.indexes_degraded_after;
            result.debt_remaining = repair.debt_remaining;
        }

        const MaintenanceRuntimeKind = enum {
            text_merge,
            sparse_compaction,
        };

        fn maintenanceRestartState(ctx: *AsyncContext, kind: MaintenanceRuntimeKind) *std.atomic.Value(u8) {
            return switch (kind) {
                .text_merge => &ctx.text_merge_restart_state,
                .sparse_compaction => &ctx.sparse_compaction_restart_state,
            };
        }

        fn ensureMaintenanceRuntimeRunning(ctx: *AsyncContext, kind: MaintenanceRuntimeKind) !bool {
            return switch (kind) {
                .text_merge => if (ctx.text_merge_runtime) |runtime| try runtime.ensureRunning() else true,
                .sparse_compaction => if (ctx.sparse_compaction_runtime) |runtime| try runtime.ensureRunning() else true,
            };
        }

        const MaintenanceRestartWork = struct {
            ctx: *AsyncContext,
            lane: background_runtime_mod.DurableJobLane,
            owner_id: u64,
            kind: MaintenanceRuntimeKind,

            const retry_initial_ms: i64 = 25;
            const retry_max_ms: i64 = 1000;
            const retries_per_job: usize = 8;

            fn run(ptr: *anyopaque) anyerror!void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                const restart_state = maintenanceRestartState(work.ctx, work.kind);
                var retries: usize = 0;
                while (true) {
                    if (work.ctx.background_closing.load(.acquire)) {
                        restart_state.store(0, .release);
                        return;
                    }

                    var start_error: ?anyerror = null;
                    const running = ensureMaintenanceRuntimeRunning(work.ctx, work.kind) catch |err| blk: {
                        start_error = err;
                        break :blk false;
                    };
                    if (running) {
                        if (restart_state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return;
                        if (restart_state.cmpxchgStrong(2, 1, .acq_rel, .acquire) == null) {
                            retries = 0;
                            continue;
                        }
                        return;
                    }

                    retries += 1;
                    if (start_error) |err| {
                        if (retries == 1 or std.math.isPowerOfTwo(retries)) {
                            std.log.warn("{s} runtime restart retry attempt={} err={s}", .{
                                @tagName(work.kind),
                                retries,
                                @errorName(err),
                            });
                        }
                    }
                    // A paused runtime is owned by a subsequent structural
                    // mutation. Wait without starting it behind that mutation.
                    if (work.ctx.io) |io| {
                        const shift: u6 = @intCast(@min(retries - 1, 5));
                        const delay_ms = if (builtin.is_test) 1 else @min(retry_initial_ms << shift, retry_max_ms);
                        io.sleep(std.Io.Duration.fromMilliseconds(delay_ms), .awake) catch {};
                    } else {
                        restart_state.store(0, .release);
                        return error.MaintenanceRuntimeUnavailable;
                    }
                    if (retries >= retries_per_job) {
                        // Yield the shared durable lane during persistent failure;
                        // the desired-running bit makes resubmission idempotent.
                        restart_state.store(0, .release);
                        scheduleMaintenanceRestartContext(work.ctx, work.lane, work.owner_id, work.kind);
                        return;
                    }
                }
            }

            fn deinit(ptr: *anyopaque) void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                std.heap.page_allocator.destroy(work);
            }
        };

        fn scheduleMaintenanceRestartContext(
            ctx: *AsyncContext,
            lane: background_runtime_mod.DurableJobLane,
            owner_id: u64,
            kind: MaintenanceRuntimeKind,
        ) void {
            if (ctx.background_closing.load(.acquire)) return;
            const restart_state = maintenanceRestartState(ctx, kind);
            if (restart_state.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) {
                _ = restart_state.cmpxchgStrong(1, 2, .acq_rel, .acquire);
                return;
            }
            const work = std.heap.page_allocator.create(MaintenanceRestartWork) catch {
                restart_state.store(0, .release);
                return;
            };
            work.* = .{ .ctx = ctx, .lane = lane, .owner_id = owner_id, .kind = kind };
            lane.submit(.{
                .owner_id = owner_id,
                .class = .maintenance,
                .ptr = work,
                .run = MaintenanceRestartWork.run,
                .deinit = MaintenanceRestartWork.deinit,
            }) catch |err| {
                std.heap.page_allocator.destroy(work);
                restart_state.store(0, .release);
                std.log.warn("{s} runtime restart was not scheduled err={s}", .{ @tagName(kind), @errorName(err) });
            };
        }

        pub fn quiesceTextMergeForStructuralMutation(self: *DB) bool {
            const runtime = self.text_merge_runtime orelse return false;
            return runtime.pause();
        }

        pub fn quiesceSparseCompactionForStructuralMutation(self: *DB) bool {
            const runtime = self.sparse_compaction_runtime orelse return false;
            return runtime.pause();
        }

        pub fn restartTextMergeAfterStructuralMutation(self: *DB, operation: []const u8, index_name: []const u8) void {
            const runtime = self.text_merge_runtime orelse return;
            runtime.resumeAfterPause() catch |err| {
                // Index catalog durability is already decided at this point.
                // Queries and foreground indexing remain available; surface the
                // maintenance degradation without misreporting the mutation.
                std.log.err("failed to restart text merge runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
                scheduleMaintenanceRestartContext(
                    self.async_context,
                    self.backend_runtime.durable_jobs,
                    self.repair_cleanup_owner_id,
                    .text_merge,
                );
            };
        }

        pub fn restartSparseCompactionAfterStructuralMutation(self: *DB, operation: []const u8, index_name: []const u8) void {
            const runtime = self.sparse_compaction_runtime orelse return;
            runtime.resumeAfterPause() catch |err| {
                std.log.err("failed to restart sparse compaction runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
                scheduleMaintenanceRestartContext(
                    self.async_context,
                    self.backend_runtime.durable_jobs,
                    self.repair_cleanup_owner_id,
                    .sparse_compaction,
                );
            };
        }

        pub const IndexStructuralMutationGuard = struct {
            db: *DB,
            operation: []const u8,
            index_name: []const u8,
            restart_text_merge: bool,
            restart_sparse_compaction: bool,
            catalog_barrier_held: bool = false,
            active: bool = true,

            fn acquireCatalogBarrierUntil(self: *@This(), deadline_ns: u64) bool {
                std.debug.assert(self.active and !self.catalog_barrier_held);
                if (!self.db.beginIndexCatalogBarrierUntil(deadline_ns)) return false;
                self.catalog_barrier_held = true;
                return true;
            }

            fn releaseCatalogBarrier(self: *@This()) void {
                if (!self.catalog_barrier_held) return;
                self.db.endIndexCatalogBarrier();
                self.catalog_barrier_held = false;
            }

            pub fn release(self: *@This()) void {
                if (!self.active) return;
                // The catalog is stable once the caller releases apply exclusive;
                // let lock-free dense searches resume before runtime startup work.
                self.releaseCatalogBarrier();
                // Restart while serialization is still held: another structural
                // mutation must not stop a runtime between this restart and the
                // corresponding unlock.
                if (self.restart_sparse_compaction) {
                    self.db.restartSparseCompactionAfterStructuralMutation(self.operation, self.index_name);
                }
                if (self.restart_text_merge) {
                    self.db.restartTextMergeAfterStructuralMutation(self.operation, self.index_name);
                }
                self.db.index_structural_mutation_mutex.unlock();
                self.active = false;
            }

            pub fn deinit(self: *@This()) void {
                self.release();
            }
        };

        const QuarantineIndexPublicationFence = struct {
            db: *DB,
            structural_guard: ?IndexStructuralMutationGuard = null,
            apply_locked: bool = false,

            fn iface(self: *@This()) index_manager_mod.IndexManager.CatalogPublicationFence {
                return .{
                    .ptr = self,
                    .lock_fn = lockOpaque,
                    .unlock_fn = unlockOpaque,
                };
            }

            fn lockOpaque(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                std.debug.assert(self.structural_guard == null and !self.apply_locked);
                self.structural_guard = self.db.beginIndexStructuralMutation("quarantined index publication", "*");
                if (builtin.is_test) test_quarantine_publication_fence_entered.store(true, .release);
                self.db.core.lockApply();
                self.apply_locked = true;
            }

            fn unlockOpaque(ptr: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                std.debug.assert(self.apply_locked and self.structural_guard != null);
                self.db.core.unlockApply();
                self.apply_locked = false;
                self.structural_guard.?.release();
                self.structural_guard = null;
            }
        };

        pub fn retryQuarantinedIndexLoads(self: *DB, force: bool) !index_manager_mod.IndexManager.QuarantineRetryResult {
            var publication_fence = QuarantineIndexPublicationFence{ .db = self };
            return try self.core.index_manager.retryFailedIndexLoads(
                self.core.store,
                monotonicTimeNs(),
                force,
                publication_fence.iface(),
            );
        }

        pub fn beginIndexStructuralMutation(
            self: *DB,
            operation: []const u8,
            index_name: []const u8,
        ) IndexStructuralMutationGuard {
            var guard = self.beginDrainedIndexStructuralMutation(operation, index_name);
            if (!guard.acquireCatalogBarrierUntil(std.math.maxInt(u64))) unreachable;
            return guard;
        }

        pub fn beginDrainedIndexStructuralMutation(
            self: *DB,
            operation: []const u8,
            index_name: []const u8,
        ) IndexStructuralMutationGuard {
            db_internal.lockAtomicWithBackoff(&self.index_structural_mutation_mutex);
            return .{
                .db = self,
                .operation = operation,
                .index_name = index_name,
                .restart_text_merge = self.quiesceTextMergeForStructuralMutation(),
                .restart_sparse_compaction = self.quiesceSparseCompactionForStructuralMutation(),
            };
        }

        const GeneratedArtifactCleanupWork = struct {
            ctx: *AsyncContext,
            lane: background_runtime_mod.DurableJobLane,
            owner_id: u64,

            const pages_per_job: usize = 8;
            const retries_per_job: usize = 8;
            const retry_initial_ms: i64 = 25;
            const retry_max_ms: i64 = 1000;

            fn drainOnePage(work: *@This()) !GeneratedArtifactCleanupAdvanceResult {
                return try advanceGeneratedArtifactCleanupContext(work.ctx, null);
            }

            fn run(ptr: *anyopaque) anyerror!void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                var pages: usize = 0;
                var retries: usize = 0;
                var contention_retries: usize = 0;
                while (true) {
                    if (work.ctx.background_closing.load(.acquire)) {
                        work.ctx.index_manager.artifact_cleanup_state.store(0, .release);
                        return;
                    }
                    const advance = work.drainOnePage() catch |err| {
                        retries += 1;
                        if (retries == 1 or std.math.isPowerOfTwo(retries))
                            std.log.warn("generated-artifact cleanup retry attempt={} err={s}", .{ retries, @errorName(err) });
                        if (work.ctx.io) |io| {
                            const shift: u6 = @intCast(@min(retries - 1, 5));
                            const delay_ms = if (builtin.is_test) 1 else @min(retry_initial_ms << shift, retry_max_ms);
                            io.sleep(std.Io.Duration.fromMilliseconds(delay_ms), .awake) catch {};
                        } else {
                            // Manual/freestanding lanes have no autonomous timer.
                            // Leave the durable marker pending for the next explicit
                            // maintenance poll or reopen instead of monopolizing the
                            // caller that is executing the lane synchronously.
                            work.ctx.index_manager.artifact_cleanup_state.store(0, .release);
                            return err;
                        }
                        if (retries >= retries_per_job) {
                            // A persistent failure must not monopolize a shared
                            // durable-job worker. Requeue at the tail while the
                            // durable tombstone remains the source of truth.
                            work.ctx.index_manager.artifact_cleanup_state.store(0, .release);
                            scheduleGeneratedArtifactCleanupContext(work.ctx, work.lane, work.owner_id);
                            return;
                        }
                        continue;
                    };
                    retries = 0;

                    if (advance == .busy) {
                        // Another caller owns a bounded page or terminal filesystem
                        // finalization. Yield without turning expected contention
                        // into an error or spinning on the shared durable-job lane.
                        const io = work.ctx.io orelse {
                            work.ctx.index_manager.artifact_cleanup_state.store(0, .release);
                            return;
                        };
                        contention_retries += 1;
                        if (contention_retries < retries_per_job) {
                            const shift: u6 = @intCast(@min(contention_retries - 1, 5));
                            const delay_ms = if (builtin.is_test) 1 else @min(retry_initial_ms << shift, retry_max_ms);
                            io.sleep(std.Io.Duration.fromMilliseconds(delay_ms), .awake) catch {};
                            continue;
                        }
                        work.ctx.index_manager.artifact_cleanup_state.store(0, .release);
                        scheduleGeneratedArtifactCleanupContext(work.ctx, work.lane, work.owner_id);
                        return;
                    }
                    contention_retries = 0;

                    if (advance == .idle) {
                        if (work.ctx.index_manager.artifact_cleanup_state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return;
                        if (work.ctx.index_manager.artifact_cleanup_state.cmpxchgStrong(2, 1, .acq_rel, .acquire) == null) {
                            pages = 0;
                            continue;
                        }
                        return;
                    }

                    pages += 1;
                    if (pages < pages_per_job) continue;

                    // Yield the durable-job lane between bounded slices. The
                    // outbox cursor makes resubmission idempotent across failures.
                    work.ctx.index_manager.artifact_cleanup_state.store(0, .release);
                    scheduleGeneratedArtifactCleanupContext(work.ctx, work.lane, work.owner_id);
                    return;
                }
            }

            fn deinit(ptr: *anyopaque) void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                std.heap.page_allocator.destroy(work);
            }
        };

        const EnrichmentRestartWork = struct {
            ctx: *AsyncContext,
            lane: background_runtime_mod.DurableJobLane,
            owner_id: u64,

            const retry_initial_ms: i64 = 25;
            const retry_max_ms: i64 = 1000;
            const retries_per_job: usize = 8;

            fn run(ptr: *anyopaque) anyerror!void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                var retries: usize = 0;
                while (true) {
                    if (work.ctx.background_closing.load(.acquire)) {
                        work.ctx.enrichment_restart_state.store(0, .release);
                        return;
                    }
                    if (!work.ctx.enrichment_desired_running.load(.acquire)) {
                        if (work.ctx.enrichment_restart_state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return;
                        if (work.ctx.enrichment_restart_state.cmpxchgStrong(2, 1, .acq_rel, .acquire) == null) {
                            retries = 0;
                            continue;
                        }
                        return;
                    }

                    db_internal.lockAtomicWithBackoff(&work.ctx.enrichment_lifecycle_mutex);
                    const runtime = work.ctx.enrichment_runtime;
                    const should_start = runtime != null and
                        work.ctx.enrichment_desired_running.load(.acquire) and
                        !work.ctx.background_closing.load(.acquire) and
                        !runtime.?.isStarted();
                    const start_result: ?anyerror = if (should_start) blk: {
                        startEnrichmentRuntimeForLifecycle(runtime.?) catch |err| break :blk err;
                        break :blk null;
                    } else null;
                    const started = runtime == null or runtime.?.isStarted();
                    work.ctx.enrichment_lifecycle_mutex.unlock();

                    if (started) {
                        DB.notifyQueryVisibilityHook(work.ctx, .status);
                        if (work.ctx.enrichment_restart_state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return;
                        if (work.ctx.enrichment_restart_state.cmpxchgStrong(2, 1, .acq_rel, .acquire) == null) {
                            retries = 0;
                            continue;
                        }
                        return;
                    }

                    retries += 1;
                    if (retries == 1 or std.math.isPowerOfTwo(retries)) {
                        std.log.warn("enrichment runtime restart retry attempt={} err={s}", .{
                            retries,
                            if (start_result) |err| @errorName(err) else "RuntimeNotStarted",
                        });
                    }
                    if (work.ctx.io) |io| {
                        const shift: u6 = @intCast(@min(retries - 1, 5));
                        const delay_ms = if (builtin.is_test) 1 else @min(retry_initial_ms << shift, retry_max_ms);
                        io.sleep(std.Io.Duration.fromMilliseconds(delay_ms), .awake) catch {};
                    } else {
                        work.ctx.enrichment_restart_state.store(0, .release);
                        return start_result orelse error.EnrichmentRuntimeUnavailable;
                    }
                    if (retries >= retries_per_job) {
                        // Preserve autonomous recovery while allowing unrelated
                        // durable work to run on a persistently failing runtime.
                        work.ctx.enrichment_restart_state.store(0, .release);
                        scheduleEnrichmentRestartContext(work.ctx, work.lane, work.owner_id);
                        return;
                    }
                }
            }

            fn deinit(ptr: *anyopaque) void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                std.heap.page_allocator.destroy(work);
            }
        };

        var test_enrichment_restart_failures_remaining: std.atomic.Value(u32) = .init(0);
        pub var test_block_generated_artifact_finalization: std.atomic.Value(bool) = .init(false);
        pub var test_generated_artifact_finalization_entered: std.atomic.Value(bool) = .init(false);
        pub var test_release_generated_artifact_finalization: std.atomic.Value(bool) = .init(false);

        pub fn setTestEnrichmentRestartFailuresRemaining(value: u32) void {
            test_enrichment_restart_failures_remaining.store(value, .release);
        }

        pub fn testEnrichmentRestartFailuresRemaining() u32 {
            return test_enrichment_restart_failures_remaining.load(.acquire);
        }

        pub fn startEnrichmentRuntimeForLifecycle(runtime: *enrichment_runtime_mod.EnrichmentRuntime) !void {
            if (builtin.is_test) {
                var remaining = test_enrichment_restart_failures_remaining.load(.acquire);
                while (remaining != 0) {
                    if (test_enrichment_restart_failures_remaining.cmpxchgWeak(
                        remaining,
                        remaining - 1,
                        .acq_rel,
                        .acquire,
                    )) |actual| {
                        remaining = actual;
                        continue;
                    }
                    return error.TestTransientEnrichmentRestart;
                }
            }
            try runtime.start();
        }

        pub fn scheduleEnrichmentRestartContext(
            ctx: *AsyncContext,
            lane: background_runtime_mod.DurableJobLane,
            owner_id: u64,
        ) void {
            if (ctx.background_closing.load(.acquire) or
                !ctx.enrichment_desired_running.load(.acquire)) return;
            if (ctx.enrichment_restart_state.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) {
                _ = ctx.enrichment_restart_state.cmpxchgStrong(1, 2, .acq_rel, .acquire);
                return;
            }
            const work = std.heap.page_allocator.create(EnrichmentRestartWork) catch {
                ctx.enrichment_restart_state.store(0, .release);
                return;
            };
            work.* = .{ .ctx = ctx, .lane = lane, .owner_id = owner_id };
            lane.submit(.{
                .owner_id = owner_id,
                .class = .maintenance,
                .ptr = work,
                .run = EnrichmentRestartWork.run,
                .deinit = EnrichmentRestartWork.deinit,
            }) catch |err| {
                std.heap.page_allocator.destroy(work);
                ctx.enrichment_restart_state.store(0, .release);
                std.log.warn("enrichment runtime restart was not scheduled err={s}", .{@errorName(err)});
            };
        }

        pub fn quiesceEnrichmentForStructuralMutation(self: *DB) bool {
            const desired = self.async_context.enrichment_desired_running.swap(false, .acq_rel);
            db_internal.lockAtomicWithBackoff(&self.async_context.enrichment_lifecycle_mutex);
            defer self.async_context.enrichment_lifecycle_mutex.unlock();
            const runtime = self.async_context.enrichment_runtime orelse return desired;
            const started = runtime.isStarted();
            if (started) runtime.stop();
            return desired or started;
        }

        pub fn scheduleGeneratedArtifactCleanup(self: *DB) void {
            scheduleGeneratedArtifactCleanupContext(
                self.async_context,
                self.backend_runtime.durable_jobs,
                self.repair_cleanup_owner_id,
            );
        }

        pub const GeneratedArtifactCleanupAdvanceResult = enum {
            idle,
            progressed,
            busy,
        };

        pub fn advanceGeneratedArtifactCleanupPage(self: *DB, index_name: ?[]const u8) !GeneratedArtifactCleanupAdvanceResult {
            return try advanceGeneratedArtifactCleanupContext(self.async_context, index_name);
        }

        fn advanceGeneratedArtifactCleanupContext(
            ctx: *AsyncContext,
            index_name: ?[]const u8,
        ) !GeneratedArtifactCleanupAdvanceResult {
            if (!ctx.index_artifact_cleanup_mutex.tryLock()) return .busy;
            var cleanup_locked = true;
            defer if (cleanup_locked) ctx.index_artifact_cleanup_mutex.unlock();

            if (index_name) |name| {
                var targeted = try ctx.index_manager.drainGeneratedArtifactCleanupForIndexPage(
                    ctx.store,
                    name,
                );
                defer targeted.deinit();
                if (targeted.found) {
                    if (!targeted.completed) return .progressed;
                    return try finalizeClaimedRetiredIndexCleanup(ctx, &cleanup_locked, targeted.index_name, targeted.key);
                }
            }

            var result = try ctx.index_manager.drainGeneratedArtifactCleanupOutboxPage(ctx.store);
            defer result.deinit();
            if (!result.found) return .idle;
            if (!result.completed) return .progressed;
            return try finalizeClaimedRetiredIndexCleanup(ctx, &cleanup_locked, result.index_name, result.key);
        }

        fn finalizeClaimedRetiredIndexCleanup(
            ctx: *AsyncContext,
            cleanup_locked: *bool,
            index_name: []const u8,
            cleanup_key: []const u8,
        ) !GeneratedArtifactCleanupAdvanceResult {
            if (!ctx.index_artifact_finalization_mutex.tryLock()) return .busy;
            defer ctx.index_artifact_finalization_mutex.unlock();

            // The durable tombstone remains visible while finalization runs, so
            // namespace admission stays fenced. Release page arbitration before
            // checkpoint and filesystem I/O; other indexes can continue draining.
            ctx.index_artifact_cleanup_mutex.unlock();
            cleanup_locked.* = false;
            if (builtin.is_test and test_block_generated_artifact_finalization.load(.acquire)) {
                test_generated_artifact_finalization_entered.store(true, .release);
                while (!test_release_generated_artifact_finalization.load(.acquire)) {
                    std.Thread.yield() catch {};
                }
            }
            try finalizeRetiredIndexCleanupContext(ctx, index_name, cleanup_key);
            return .progressed;
        }

        fn scheduleGeneratedArtifactCleanupContext(
            ctx: *AsyncContext,
            lane: background_runtime_mod.DurableJobLane,
            owner_id: u64,
        ) void {
            if (ctx.background_closing.load(.acquire)) return;
            if (ctx.index_manager.artifact_cleanup_state.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) {
                _ = ctx.index_manager.artifact_cleanup_state.cmpxchgStrong(1, 2, .acq_rel, .acquire);
                return;
            }
            const work = std.heap.page_allocator.create(GeneratedArtifactCleanupWork) catch {
                ctx.index_manager.artifact_cleanup_state.store(0, .release);
                return;
            };
            work.* = .{ .ctx = ctx, .lane = lane, .owner_id = owner_id };
            lane.submit(.{
                .owner_id = owner_id,
                .class = .cleanup,
                .ptr = work,
                .run = GeneratedArtifactCleanupWork.run,
                .deinit = GeneratedArtifactCleanupWork.deinit,
            }) catch |err| {
                std.heap.page_allocator.destroy(work);
                ctx.index_manager.artifact_cleanup_state.store(0, .release);
                std.log.warn("generated-artifact cleanup was not scheduled err={s}", .{@errorName(err)});
            };
        }

        fn finalizeRetiredIndexCleanupContext(ctx: *AsyncContext, index_name: []const u8, cleanup_key: []const u8) !void {
            // Filesystem/cache cleanup is intentionally outside the DB apply lock.
            // The durable tombstone blocks overlapping artifact namespaces while
            // this runs.
            try ctx.index_manager.finalizeRetiredIndexStorage(ctx.store, index_name);

            ctx.apply_mutex.lockExclusive();
            defer ctx.apply_mutex.unlockExclusive();
            if (ctx.index_manager.get(index_name) != null) return error.IndexGenerationStillActive;

            const storage_alloc = ctx.index_manager.alloc;
            if (try indexRepairIdForIndexContext(ctx, storage_alloc, index_name)) |repair_id| {
                removeIndexRepairIntentAndPinContext(ctx, storage_alloc, repair_id) catch |err| switch (err) {
                    error.IndexRepairIntentNotFound, error.FileNotFound => {},
                    else => return err,
                };
            }
            try DB.ArtifactRepairCallbacks.delete_dense_artifact_counter_metadata_context(storage_alloc, ctx.store, index_name);
            ctx.store.delete(cleanup_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }

        pub fn rejectConflictingRetiredIndexCleanupForAdmission(self: *DB, cfg: types.IndexConfig) !void {
            db_internal.lockAtomicWithBackoff(&self.async_context.index_artifact_cleanup_mutex);
            defer self.async_context.index_artifact_cleanup_mutex.unlock();
            const retired_name = (try self.core.index_manager.pendingGeneratedArtifactCleanupIndexForConfigAlloc(
                self.core.store,
                cfg,
            )) orelse return;
            defer self.alloc.free(retired_name);
            Self.scheduleGeneratedArtifactCleanup(self);
            return error.IndexArtifactCleanupPending;
        }

        const Self = @This();
        const AsyncContext = db_internal.AsyncContext(DB);

        const managed_index_admission_magic: u64 = 0x324d444154584449;
        const managed_index_admission_encoded_len = 48;

        pub const IndexAdmissionDisposition = enum(u64) {
            activation_fence = 1,
            managed_rebuild = 2,
        };

        pub const ManagedIndexAdmissionMarker = struct {
            config_hash: u64,
            source_doc_count: u64,
            identity_generation: u64,
            replay_target_sequence: u64,
            disposition: IndexAdmissionDisposition = .managed_rebuild,
        };

        pub const ManagedAdmissionMaterializationTestHook = struct {
            ptr: *anyopaque,
            after_config_lookup: ?*const fn (*anyopaque, *DB, []const u8) void = null,
            after_pass: ?*const fn (*anyopaque, *DB) void = null,
        };

        pub var test_managed_admission_materialization_hook: ?ManagedAdmissionMaterializationTestHook = null;

        pub fn encodeManagedIndexAdmissionMarker(
            marker: ManagedIndexAdmissionMarker,
        ) [managed_index_admission_encoded_len]u8 {
            var out: [managed_index_admission_encoded_len]u8 = undefined;
            std.mem.writeInt(u64, out[0..8], managed_index_admission_magic, .little);
            std.mem.writeInt(u64, out[8..16], marker.config_hash, .little);
            std.mem.writeInt(u64, out[16..24], marker.source_doc_count, .little);
            std.mem.writeInt(u64, out[24..32], marker.identity_generation, .little);
            std.mem.writeInt(u64, out[32..40], marker.replay_target_sequence, .little);
            std.mem.writeInt(u64, out[40..48], @intFromEnum(marker.disposition), .little);
            return out;
        }

        pub fn decodeManagedIndexAdmissionMarker(raw: []const u8) !ManagedIndexAdmissionMarker {
            if (raw.len != managed_index_admission_encoded_len) return error.InvalidManagedIndexAdmission;
            if (std.mem.readInt(u64, raw[0..8], .little) != managed_index_admission_magic)
                return error.InvalidManagedIndexAdmission;
            const disposition = std.enums.fromInt(
                IndexAdmissionDisposition,
                std.mem.readInt(u64, raw[40..48], .little),
            ) orelse return error.InvalidManagedIndexAdmission;
            return .{
                .config_hash = std.mem.readInt(u64, raw[8..16], .little),
                .source_doc_count = std.mem.readInt(u64, raw[16..24], .little),
                .identity_generation = std.mem.readInt(u64, raw[24..32], .little),
                .replay_target_sequence = std.mem.readInt(u64, raw[32..40], .little),
                .disposition = disposition,
            };
        }

        pub fn encodeManagedIndexAdmissionValue(
            config_hash: u64,
            source_doc_count: u64,
            identity_generation: u64,
            replay_target_sequence: u64,
            managed_rebuild: bool,
        ) [managed_index_admission_encoded_len]u8 {
            return encodeManagedIndexAdmissionMarker(.{
                .config_hash = config_hash,
                .source_doc_count = source_doc_count,
                .identity_generation = identity_generation,
                .replay_target_sequence = replay_target_sequence,
                .disposition = if (managed_rebuild) .managed_rebuild else .activation_fence,
            });
        }

        pub const StartupIndexRepairDiscovery = struct {
            discovered: usize = 0,
            already_pending: usize = 0,
            terminal: usize = 0,
            existing_terminal: usize = 0,
        };

        fn indexRepairStateLocation(self: *const DB) !index_repair_state.Location {
            return try indexRepairStateLocationContext(self.async_context);
        }

        fn indexRepairStateLocationContext(ctx: *const AsyncContext) !index_repair_state.Location {
            return ctx.index_repair_checkpoint orelse error.DurableIndexRepairStateUnavailable;
        }

        fn localRepairGroupId(self: *const DB) u64 {
            const namespace = self.core.identity_namespace;
            if (namespace.range_id != 0) return namespace.range_id;
            return namespace.shard_id;
        }

        pub fn hasPendingIndexRepairIntents(self: *const DB, alloc: Allocator) !bool {
            const location = try Self.indexRepairStateLocation(self);
            var state = index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
                error.FileNotFound => return Self.hasManagedIndexAdmissionMarker(self, alloc),
                else => return err,
            };
            defer state.deinit(alloc);
            if (state.entries.items.len != 0) return true;
            return Self.hasManagedIndexAdmissionMarker(self, alloc);
        }

        fn hasManagedIndexAdmissionMarker(self: *const DB, alloc: Allocator) !bool {
            const admission_prefix = try internal_keys.managedIndexAdmissionRootPrefixAlloc(alloc);
            defer alloc.free(admission_prefix);
            const admissions = try self.core.store.scanPrefixPage(alloc, admission_prefix, null, 1);
            defer docstore_mod.DocStore.freeResults(alloc, admissions);
            return admissions.len != 0;
        }

        pub const IndexRepairIntentSummary = struct {
            runnable: usize = 0,
            paused: usize = 0,
            terminal: usize = 0,
            earliest_retry_at_ms: u64 = 0,
        };

        pub fn indexRepairIntentSummary(self: *DB, alloc: Allocator) !IndexRepairIntentSummary {
            if (Self.managedAdmissionMaterializationPending(self)) try Self.drainManagedIndexAdmissions(self, alloc);
            var state = Self.loadIndexRepairState(self, alloc) catch |err| switch (err) {
                error.FileNotFound => return .{},
                else => return err,
            };
            defer state.deinit(alloc);
            var summary: IndexRepairIntentSummary = .{};
            for (state.entries.items) |entry| {
                if (entry.intent.phase == .terminal and retryableIndexRepairTerminalPhase(
                    entry.intent.last_error,
                    entry.intent.trigger,
                ) == null) {
                    summary.terminal += 1;
                } else if (entry.intent.automation == .paused) {
                    summary.paused += 1;
                } else {
                    summary.runnable += 1;
                    if (entry.intent.next_retry_at_ms != 0 and
                        (summary.earliest_retry_at_ms == 0 or entry.intent.next_retry_at_ms < summary.earliest_retry_at_ms))
                    {
                        summary.earliest_retry_at_ms = entry.intent.next_retry_at_ms;
                    }
                }
            }
            return summary;
        }

        pub fn loadIndexRepairState(self: *const DB, alloc: Allocator) !index_repair_state.State {
            return try index_repair_state.loadAt(alloc, try Self.indexRepairStateLocation(self));
        }

        fn refreshIndexRepairAvailabilityGate(self: *DB, alloc: Allocator) !void {
            var state = Self.loadIndexRepairState(self, alloc) catch |err| switch (err) {
                error.FileNotFound, error.DurableIndexRepairStateUnavailable => return,
                error.InvalidIndexRepairState => {
                    self.async_context.index_repair_state_corrupt.store(true, .release);
                    self.async_context.index_repair_replay_pinned.store(true, .release);
                    const configs = try self.core.index_manager.listIndexesPublic(alloc);
                    defer {
                        for (configs) |*cfg| cfg.deinit(alloc);
                        alloc.free(configs);
                    }
                    for (configs) |cfg| try self.core.index_manager.markRepairUnavailable(cfg.name);
                    return;
                },
                else => return err,
            };
            defer state.deinit(alloc);
            for (state.entries.items) |entry| {
                var unavailable = indexRepairIntentBlocksService(entry.intent);
                if (!unavailable) {
                    if (entry.intent.trigger == .incomplete_bulk_publish) {
                        const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.intent.index_name);
                        unavailable = checkpoint.status != .clean or checkpoint.config_hash != entry.intent.config_hash;
                    }
                }
                if (unavailable) {
                    try self.core.index_manager.markRepairUnavailable(entry.intent.index_name);
                } else {
                    self.core.index_manager.clearRepairUnavailable(entry.intent.index_name);
                }
            }
        }

        pub fn refreshIndexRepairAvailabilityForIndex(self: *DB, alloc: Allocator, index_name: []const u8) !void {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return;
            var state = Self.loadIndexRepairState(self, alloc) catch |err| switch (err) {
                error.FileNotFound => {
                    try Self.clearRepairGateIfAdmissionCompleted(self, alloc, index_name);
                    return;
                },
                error.DurableIndexRepairStateUnavailable => return,
                error.InvalidIndexRepairState => {
                    self.async_context.index_repair_state_corrupt.store(true, .release);
                    self.async_context.index_repair_replay_pinned.store(true, .release);
                    return;
                },
                else => return err,
            };
            defer state.deinit(alloc);

            // A checkpoint from another root generation is not authority to clear
            // this generation's gate. Startup/root-transition repair owns it.
            if (state.identity.root_generation != self.core.root_generation) return;

            const entry_index = state.findIndex(index_name) orelse {
                try Self.clearRepairGateIfAdmissionCompleted(self, alloc, index_name);
                return;
            };
            const intent = state.entries.items[entry_index].intent;
            var unavailable = indexRepairIntentBlocksService(intent);
            if (!unavailable and intent.trigger == .incomplete_bulk_publish) {
                const checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
                unavailable = checkpoint.status != .clean or checkpoint.config_hash != intent.config_hash;
            }
            if (!unavailable) self.core.index_manager.clearRepairUnavailable(index_name);
        }

        fn clearRepairGateIfAdmissionCompleted(self: *DB, alloc: Allocator, index_name: []const u8) !void {
            const key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const marker = self.core.store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => {
                    self.core.index_manager.clearManagedAdmissionSnapshotForIndex(index_name);
                    self.core.index_manager.clearRepairUnavailable(index_name);
                    return;
                },
                else => return err,
            };
            alloc.free(marker);
        }

        fn indexRepairIntentBlocksService(intent: index_repair_state.IndexRepairIntent) bool {
            return switch (intent.trigger) {
                .incomplete_bulk_publish => intent.phase != .cleanup,
                // Only an operator-requested rebuild starts from a generation that
                // is still proven healthy. Coverage mismatch or missing proof is
                // fail-closed until replacement validation publishes cleanup.
                .operator_generation_rebuild => intent.phase == .activating or intent.phase == .validating,
                .artifact_coverage_mismatch,
                .artifact_counter_missing,
                .root_generation_rebuild,
                .projection_generation_invalid,
                .operator_generation_validation,
                => intent.phase != .cleanup,
            };
        }

        fn loadOrCreateCurrentIndexRepairState(self: *DB, alloc: Allocator) !index_repair_state.State {
            const location = try Self.indexRepairStateLocation(self);
            var state = try index_repair_state.loadOrCreateAt(alloc, location, self.core.root_generation);
            var state_owned = true;
            defer if (state_owned) state.deinit(alloc);
            if (state.identity.root_generation == self.core.root_generation) {
                state_owned = false;
                return state;
            }

            // A root-generation transition fences every old repair identity. An
            // inactive candidate can be deleted. A pointer-selected candidate must
            // never become silently serviceable merely because its old intent is
            // discarded: persist fresh, new-root reconstruction debt first and
            // keep that index fail-closed until a new generation is validated.
            var replacement_intents = std.ArrayListUnmanaged(index_repair_state.IndexRepairIntent).empty;
            defer {
                for (replacement_intents.items) |*intent| intent.deinit(alloc);
                replacement_intents.deinit(alloc);
            }
            for (state.entries.items) |entry| {
                const candidate = entry.intent.candidate_relative_path orelse continue;
                try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
                if (try self.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) {
                    const cfg = self.core.index_manager.get(entry.intent.index_name) orelse
                        return error.InvalidIndexRepairState;
                    const now_ms = currentTimeNs() / std.time.ns_per_ms;
                    const target_sequence = self.core.nextDerivedSequence();
                    var replacement_intent = index_repair_state.IndexRepairIntent{
                        .repair_id = try index_repair_state.newRepairId(alloc),
                        .db_identity = state.identity.db_identity,
                        .group_id = Self.localRepairGroupId(self),
                        .replica_id = state.identity.replica_id,
                        .root_generation = self.core.root_generation,
                        .index_name = try alloc.dupe(u8, entry.intent.index_name),
                        .kind = cfg.kind,
                        .config_hash = types.indexConfigHash(cfg.*),
                        .trigger = .root_generation_rebuild,
                        .detected_sequence = target_sequence,
                        .target_sequence = target_sequence,
                        .started_at_ms = now_ms,
                        .updated_at_ms = now_ms,
                        .owner_epoch = 0,
                        .last_error = try alloc.dupe(u8, "root_generation_changed_during_activation"),
                    };
                    var replacement_intent_owned = true;
                    errdefer if (replacement_intent_owned) replacement_intent.deinit(alloc);
                    try replacement_intents.append(alloc, replacement_intent);
                    replacement_intent_owned = false;
                    try self.core.index_manager.markRepairUnavailable(entry.intent.index_name);
                    continue;
                }
                self.core.index_manager.clearRepairUnavailable(entry.intent.index_name);
                const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
                const shadow_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate[0..separator] });
                defer alloc.free(shadow_root);
                var io_impl = threadedIo();
                defer io_impl.deinit();
                try std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_root);
            }
            const old_identity = state.identity;
            state.deinit(alloc);
            state_owned = false;
            return try index_repair_state.resetForRootGenerationWithIntentsAt(
                alloc,
                location,
                old_identity,
                self.core.root_generation,
                replacement_intents.items,
            );
        }

        pub fn loadIndexRepairEntryById(
            self: *const DB,
            alloc: Allocator,
            repair_id: u128,
        ) !index_repair_state.Entry {
            var state = try Self.loadIndexRepairState(self, alloc);
            defer state.deinit(alloc);
            for (state.entries.items) |entry| {
                if (entry.intent.repair_id == repair_id) return try entry.clone(alloc);
            }
            return error.IndexRepairIntentNotFound;
        }

        pub const PinnedIndexRepairSnapshot = struct {
            txn: docstore_mod.DocStore.Txn,
            repair_id: u128,
            build_floor_sequence: u64,

            pub fn deinit(self: *@This()) void {
                self.txn.abort();
                self.* = undefined;
            }
        };

        pub fn beginPinnedIndexRepairSnapshot(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
        ) !PinnedIndexRepairSnapshot {
            db_internal.lockAtomicWithBackoff(self.core.repair_replay_mutex);
            defer self.core.repair_replay_mutex.unlock();
            const location = try Self.indexRepairStateLocation(self);
            var state = try index_repair_state.loadAt(alloc, location);
            defer state.deinit(alloc);
            const entry_index = blk: {
                for (state.entries.items, 0..) |entry, i| {
                    if (entry.intent.repair_id == repair_id) break :blk i;
                }
                return error.IndexRepairIntentNotFound;
            };
            var entry = try state.entries.items[entry_index].clone(alloc);
            defer entry.deinit(alloc);
            if (entry.intent.phase == .terminal or entry.intent.automation == .paused) return error.IndexRepairNotRunnable;
            var expected = index_repair_state.ExpectedTransition{
                .repair_id = entry.intent.repair_id,
                .revision = entry.intent.revision,
                .phase = entry.intent.phase,
                .config_hash = entry.intent.config_hash,
                .root_generation = entry.intent.root_generation,
                .owner_epoch = entry.intent.owner_epoch,
            };
            if (entry.pin == null) {
                entry.pin = .{
                    .repair_id = entry.intent.repair_id,
                    .db_identity = entry.intent.db_identity,
                    .replica_id = entry.intent.replica_id,
                    .root_generation = entry.intent.root_generation,
                    .index_name = try alloc.dupe(u8, entry.intent.index_name),
                    .retain_after_sequence = 0,
                };
                try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
            } else {
                // Re-entering snapshot acquisition must become conservative before
                // replacing a previously finalized build floor.
                entry.pin.?.retain_after_sequence = 0;
                try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
            }
            expected.revision +|= 1;
            self.async_context.index_repair_replay_pinned.store(true, .release);

            var txn = try self.core.store.beginReadTxn();
            errdefer txn.abort();
            const build_floor = try self.core.store.lastReplaySequenceFromTxn(&txn, 0);
            entry.intent.build_floor_sequence = build_floor;
            entry.intent.updated_at_ms = currentTimeNs() / std.time.ns_per_ms;
            entry.pin.?.retain_after_sequence = build_floor;
            try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
            return .{
                .txn = txn,
                .repair_id = repair_id,
                .build_floor_sequence = build_floor,
            };
        }

        const IndexRepairIntentUpdate = struct {
            trigger: ?index_repair_state.Trigger = null,
            operator_job_id: ?u64 = null,
            operator_job_created_at_ms: ?u64 = null,
            phase: ?index_repair_state.Phase = null,
            candidate_relative_path: ?[]const u8 = null,
            replace_candidate_path: bool = false,
            previous_pointer_captured: ?bool = null,
            previous_active_relative_path: ?[]const u8 = null,
            replace_previous_active_path: bool = false,
            candidate_applied_sequence: ?u64 = null,
            build_resume_key: ?[]const u8 = null,
            replace_build_resume_key: bool = false,
            build_reprocessed: ?u64 = null,
            estimated_candidate_bytes: ?u64 = null,
            planned_disk_bytes: ?u64 = null,
            target_sequence: ?u64 = null,
            attempt_count: ?u32 = null,
            failure_streak: ?u32 = null,
            next_retry_at_ms: ?u64 = null,
            automation: ?index_repair_state.Automation = null,
            owner_epoch: ?u64 = null,
            last_error: ?[]const u8 = null,
            replace_last_error: bool = false,
        };

        fn indexRepairRetryDelayMs(repair_id: u128, attempt_count: u32) u64 {
            const exponent: u6 = @intCast(@min(attempt_count -| 1, 5));
            const base_ms: u64 = 30 * std.time.ms_per_s;
            const maximum_ms: u64 = 10 * std.time.ms_per_min;
            const nominal = @min(base_ms << exponent, maximum_ms);
            // Stable per-attempt +/-10% jitter prevents a node restart from
            // synchronizing every pending repair while keeping the retry time
            // reconstructible and bounded.
            var seed: [20]u8 = undefined;
            std.mem.writeInt(u128, seed[0..16], repair_id, .little);
            std.mem.writeInt(u32, seed[16..20], attempt_count, .little);
            const spread = @max(@as(u64, 1), nominal / 5);
            const jitter = std.hash.Wyhash.hash(0x4944585250525954, &seed) % spread;
            return nominal - nominal / 10 + jitter;
        }

        pub fn indexRepairIdForIndex(self: *const DB, alloc: Allocator, index_name: []const u8) !?u128 {
            return try indexRepairIdForIndexContext(self.async_context, alloc, index_name);
        }

        fn indexRepairIdForIndexContext(ctx: *const AsyncContext, alloc: Allocator, index_name: []const u8) !?u128 {
            const location = indexRepairStateLocationContext(ctx) catch {
                // Backends without a replica-local durable checkpoint cannot own
                // repair intents, so read-side discovery is equivalent to empty.
                // Intent creation remains fail-closed through the same capability.
                return null;
            };
            var state = index_repair_state.loadAt(alloc, location) catch |err| switch (err) {
                error.FileNotFound => return null,
                else => return err,
            };
            defer state.deinit(alloc);
            const i = state.findIndex(index_name) orelse return null;
            return state.entries.items[i].intent.repair_id;
        }

        fn validateIndexRepairActivationState(self: *DB, alloc: Allocator, repair_id: u128, expected_owner_epoch: u64) !void {
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);
            if (entry.intent.root_generation != self.core.root_generation or
                entry.intent.group_id != Self.localRepairGroupId(self))
            {
                return error.RepairOwnershipLost;
            }
            if (entry.intent.phase == .terminal or entry.intent.automation == .paused) return error.IndexRepairNotRunnable;
            if (expected_owner_epoch != 0 and entry.intent.owner_epoch != expected_owner_epoch) {
                return error.RepairOwnershipLost;
            }
            const cfg = self.core.index_manager.get(entry.intent.index_name) orelse return error.IndexRepairConfigurationChanged;
            if (cfg.kind != entry.intent.kind or types.indexConfigHash(cfg.*) != entry.intent.config_hash) {
                return error.IndexRepairConfigurationChanged;
            }
        }

        pub fn updateIndexRepairIntent(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
            update: IndexRepairIntentUpdate,
        ) !void {
            const location = try Self.indexRepairStateLocation(self);
            var state = try index_repair_state.loadAt(alloc, location);
            defer state.deinit(alloc);
            const i = blk: {
                for (state.entries.items, 0..) |entry, entry_i| {
                    if (entry.intent.repair_id == repair_id) break :blk entry_i;
                }
                return error.IndexRepairIntentNotFound;
            };
            var entry = try state.entries.items[i].clone(alloc);
            defer entry.deinit(alloc);
            const expected = index_repair_state.ExpectedTransition{
                .repair_id = entry.intent.repair_id,
                .revision = entry.intent.revision,
                .phase = entry.intent.phase,
                .config_hash = entry.intent.config_hash,
                .root_generation = entry.intent.root_generation,
                .owner_epoch = entry.intent.owner_epoch,
            };
            if (update.trigger) |value| entry.intent.trigger = value;
            if (update.operator_job_id) |value| entry.intent.operator_job_id = value;
            if (update.operator_job_created_at_ms) |value| entry.intent.operator_job_created_at_ms = value;
            if (update.phase) |value| entry.intent.phase = value;
            if (update.replace_candidate_path) {
                if (entry.intent.candidate_relative_path) |value| alloc.free(value);
                entry.intent.candidate_relative_path = if (update.candidate_relative_path) |value| try alloc.dupe(u8, value) else null;
            }
            if (update.previous_pointer_captured) |value| entry.intent.previous_pointer_captured = value;
            if (update.replace_previous_active_path) {
                if (entry.intent.previous_active_relative_path) |value| alloc.free(value);
                entry.intent.previous_active_relative_path = if (update.previous_active_relative_path) |value| try alloc.dupe(u8, value) else null;
            }
            if (update.candidate_applied_sequence) |value| entry.intent.candidate_applied_sequence = value;
            if (update.replace_build_resume_key) {
                if (entry.intent.build_resume_key) |value| alloc.free(value);
                entry.intent.build_resume_key = if (update.build_resume_key) |value| try alloc.dupe(u8, value) else null;
            }
            if (update.build_reprocessed) |value| entry.intent.build_reprocessed = value;
            if (update.estimated_candidate_bytes) |value| entry.intent.estimated_candidate_bytes = value;
            if (update.planned_disk_bytes) |value| entry.intent.planned_disk_bytes = value;
            if (update.target_sequence) |value| entry.intent.target_sequence = value;
            if (update.attempt_count) |value| entry.intent.attempt_count = value;
            if (update.failure_streak) |value| entry.intent.failure_streak = value;
            if (update.next_retry_at_ms) |value| entry.intent.next_retry_at_ms = value;
            if (update.automation) |value| entry.intent.automation = value;
            if (update.owner_epoch) |value| entry.intent.owner_epoch = value;
            if (update.replace_last_error) {
                if (entry.intent.last_error) |value| alloc.free(value);
                entry.intent.last_error = if (update.last_error) |value| try alloc.dupe(u8, value) else null;
            }
            entry.intent.updated_at_ms = currentTimeNs() / std.time.ns_per_ms;
            try index_repair_state.putEntryAt(alloc, location, state.identity, expected, entry);
            if (indexRepairIntentBlocksService(entry.intent)) {
                try self.core.index_manager.markRepairUnavailable(entry.intent.index_name);
            } else {
                self.core.index_manager.clearRepairUnavailable(entry.intent.index_name);
            }
            if (update.automation) |automation| {
                DB.notifyQueryVisibilityHook(
                    self.async_context,
                    if (automation == .enabled) .index_repair_pending else .index_repair_cleared,
                );
            }
            if (update.phase) |phase| {
                if (self.shadow_index_repair_hook) |hook| {
                    if (hook.after_phase_persisted) |after_phase| {
                        try after_phase(hook.ptr, self, repair_id, phase);
                    }
                }
            }
        }

        const operator_repair_completion_prefix = "\x00\x00__metadata__:index_generation_repair_completion:";

        fn operatorRepairCompletionKeyAlloc(
            alloc: Allocator,
            index_name: []const u8,
        ) ![]u8 {
            return try std.fmt.allocPrint(alloc, "{s}{s}", .{ operator_repair_completion_prefix, index_name });
        }

        fn operatorRepairAlreadyCompleted(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            job_id: u64,
            job_created_at_ms: u64,
        ) !bool {
            if (job_id == 0 or job_created_at_ms == 0) return false;
            const key = try operatorRepairCompletionKeyAlloc(alloc, cfg.name);
            defer alloc.free(key);
            const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            defer alloc.free(raw);
            if (raw.len != 48) return error.InvalidIndexRepairState;
            var state = try Self.loadOrCreateCurrentIndexRepairState(self, alloc);
            defer state.deinit(alloc);
            const completed_job_id = std.mem.readInt(u64, raw[32..40], .little);
            const completed_created_at_ms = std.mem.readInt(u64, raw[40..48], .little);
            const completion_covers_request = completed_created_at_ms > job_created_at_ms or
                (completed_created_at_ms == job_created_at_ms and completed_job_id >= job_id);
            return completion_covers_request and
                std.mem.readInt(u64, raw[0..8], .little) == types.indexConfigHash(cfg) and
                std.mem.readInt(u128, raw[8..24], .little) == state.identity.db_identity and
                std.mem.readInt(u64, raw[24..32], .little) == state.identity.root_generation;
        }

        fn persistOperatorRepairCompletion(self: *DB, alloc: Allocator, intent: index_repair_state.IndexRepairIntent) !void {
            return try persistOperatorRepairCompletionContext(self.async_context, alloc, intent);
        }

        fn persistOperatorRepairCompletionContext(ctx: *AsyncContext, alloc: Allocator, intent: index_repair_state.IndexRepairIntent) !void {
            if (intent.operator_job_id == 0 or intent.operator_job_created_at_ms == 0) return;
            const key = try operatorRepairCompletionKeyAlloc(alloc, intent.index_name);
            defer alloc.free(key);
            var raw: [48]u8 = undefined;
            std.mem.writeInt(u64, raw[0..8], intent.config_hash, .little);
            std.mem.writeInt(u128, raw[8..24], intent.db_identity, .little);
            std.mem.writeInt(u64, raw[24..32], intent.root_generation, .little);
            std.mem.writeInt(u64, raw[32..40], intent.operator_job_id, .little);
            std.mem.writeInt(u64, raw[40..48], intent.operator_job_created_at_ms, .little);
            try ctx.store.put(key, &raw);
        }

        const AutomaticDenseGenerationRepairClassification = struct {
            trigger: index_repair_state.Trigger,
            last_error: []const u8,
        };

        const OperatorDenseGenerationValidation = union(enum) {
            online_safe,
            replay_pending,
            fail_closed: AutomaticDenseGenerationRepairClassification,
        };

        fn classifyCurrentDenseGenerationRepair(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
        ) !?AutomaticDenseGenerationRepairClassification {
            if (cfg.kind != .dense_vector) return null;
            const entry = self.core.index_manager.denseIndex(cfg.name) orelse return null;
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
            if (checkpoint.config_hash != types.indexConfigHash(cfg)) return .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_config_mismatch",
            };
            if (checkpoint.status == .repair_required) return .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_checkpoint_repair_required",
            };
            if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return null;
            const expected = (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse return .{
                .trigger = .artifact_counter_missing,
                .last_error = "dense_artifact_counter_missing",
            };
            if (entry.index.stats().active_count <= expected) return null;
            const target_sequence = try DB.LifecycleCallbacks.probe_derived_replay_target_sequence(
                self,
                alloc,
                self.core.replaySource(),
                .{ .name = cfg.name, .kind = .dense_vector },
                checkpoint.applied_sequence,
            );
            if (checkpoint.applied_sequence < target_sequence) return null;
            return .{
                .trigger = .artifact_coverage_mismatch,
                .last_error = "dense_artifact_coverage_surplus",
            };
        }

        fn validateOperatorDenseGenerationForOnlineRebuild(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            options: types.ArtifactRepairRunOptions,
        ) !OperatorDenseGenerationValidation {
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
            if (checkpoint.config_hash != types.indexConfigHash(cfg)) return .{ .fail_closed = .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_config_mismatch",
            } };
            if (checkpoint.status != .clean) return .{ .fail_closed = .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_checkpoint_not_clean",
            } };
            const target_sequence = try DB.LifecycleCallbacks.probe_derived_replay_target_sequence(
                self,
                alloc,
                self.core.replaySource(),
                .{ .name = cfg.name, .kind = .dense_vector },
                checkpoint.applied_sequence,
            );
            if (checkpoint.applied_sequence < target_sequence) return .replay_pending;

            // Pin both the catalog entry and its generation while inspecting the
            // HBC metadata/tree. The read transaction stabilizes backend pages;
            // this guard additionally prevents an apply or delete/recreate from
            // changing the in-memory generation around that snapshot.
            var apply_guard = self.core.index_manager.lockManagedIndexApply(.{
                .name = cfg.name,
                .kind = .dense_vector,
            }) catch return .{ .fail_closed = .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_generation_unavailable",
            } };
            defer apply_guard.unlock();

            const entry = self.core.index_manager.denseIndex(cfg.name) orelse return .{ .fail_closed = .{
                .trigger = .projection_generation_invalid,
                .last_error = "dense_projection_generation_unavailable",
            } };

            if (try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) {
                const expected = (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse
                    return .{ .fail_closed = .{
                        .trigger = .artifact_counter_missing,
                        .last_error = "dense_artifact_counter_missing",
                    } };
                if (!DB.DerivedAsyncCallbacks.dense_coverage_matches_target(entry.index.stats().active_count, expected)) {
                    return .{ .fail_closed = .{
                        .trigger = .artifact_coverage_mismatch,
                        .last_error = "dense_artifact_coverage_mismatch",
                    } };
                }
            }

            const cancel_ctx = if (options.cancel_check) |check| check.ptr else null;
            const cancel_fn = if (options.cancel_check) |check| check.is_requested else null;
            entry.index.validateStoredStructureWithCancellation(alloc, cancel_ctx, cancel_fn) catch |err| switch (err) {
                error.NotFound, error.FileNotFound, error.Corrupted => return .{ .fail_closed = .{
                    .trigger = .projection_generation_invalid,
                    .last_error = "dense_projection_structure_invalid",
                } },
                else => return err,
            };
            return .online_safe;
        }

        fn createGenerationRepairIntent(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            trigger: index_repair_state.Trigger,
            operator_job_id: u64,
            operator_job_created_at_ms: u64,
            last_error: ?[]const u8,
        ) !u128 {
            return try Self.createGenerationRepairIntentAtTarget(
                self,
                alloc,
                cfg,
                trigger,
                operator_job_id,
                operator_job_created_at_ms,
                last_error,
                null,
            );
        }

        fn createGenerationRepairIntentAtTarget(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            trigger: index_repair_state.Trigger,
            operator_job_id: u64,
            operator_job_created_at_ms: u64,
            last_error: ?[]const u8,
            minimum_target_sequence: ?u64,
        ) !u128 {
            var state = try Self.loadOrCreateCurrentIndexRepairState(self, alloc);
            defer state.deinit(alloc);
            if (state.findIndex(cfg.name)) |i| return state.entries.items[i].intent.repair_id;
            const location = try Self.indexRepairStateLocation(self);
            const now_ms = currentTimeNs() / std.time.ns_per_ms;
            const target_sequence = @max(
                self.core.nextDerivedSequence(),
                minimum_target_sequence orelse 0,
            );
            const repair_id = try index_repair_state.newRepairId(alloc);
            const index_name = try alloc.dupe(u8, cfg.name);
            var intent_allocations_transferred = false;
            errdefer if (!intent_allocations_transferred) alloc.free(index_name);
            const owned_last_error = if (last_error) |value| try alloc.dupe(u8, value) else null;
            errdefer if (!intent_allocations_transferred) if (owned_last_error) |value| alloc.free(value);
            var intent = index_repair_state.IndexRepairIntent{
                .repair_id = repair_id,
                .db_identity = state.identity.db_identity,
                .group_id = Self.localRepairGroupId(self),
                .replica_id = state.identity.replica_id,
                .root_generation = state.identity.root_generation,
                .index_name = index_name,
                .kind = cfg.kind,
                .config_hash = types.indexConfigHash(cfg),
                .trigger = trigger,
                .operator_job_id = operator_job_id,
                .operator_job_created_at_ms = operator_job_created_at_ms,
                .detected_sequence = target_sequence,
                .target_sequence = target_sequence,
                .started_at_ms = now_ms,
                .updated_at_ms = now_ms,
                .owner_epoch = 0,
                .last_error = owned_last_error,
            };
            intent_allocations_transferred = true;
            defer intent.deinit(alloc);
            index_repair_state.putEntryAt(alloc, location, state.identity, null, .{ .intent = intent }) catch |err| switch (err) {
                // Another operator/maintenance caller may win creation between the
                // optimistic state read and the checkpoint CAS. Adopt that durable
                // intent; callers subsequently attach their job identity or
                // promote its safety classification with revision-fenced retries.
                error.RepairTransitionConflict => return (try Self.indexRepairIdForIndex(self, alloc, cfg.name)) orelse return err,
                else => return err,
            };
            if (indexRepairIntentBlocksService(intent)) {
                try self.core.index_manager.markRepairUnavailable(intent.index_name);
            }
            DB.notifyQueryVisibilityHook(self.async_context, .index_repair_pending);
            return repair_id;
        }

        pub fn createOperatorGenerationRepairIntent(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            operator_job_id: u64,
            operator_job_created_at_ms: u64,
        ) !u128 {
            const repair_id = try Self.createGenerationRepairIntent(
                self,
                alloc,
                cfg,
                if (cfg.kind == .dense_vector) .operator_generation_validation else .operator_generation_rebuild,
                operator_job_id,
                operator_job_created_at_ms,
                null,
            );
            try Self.attachOperatorRepairRequest(
                self,
                alloc,
                repair_id,
                operator_job_id,
                operator_job_created_at_ms,
            );
            return repair_id;
        }

        pub fn materializeManagedIndexAdmission(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
        ) !?u128 {
            if (db_config.openModeRequiresReadOnlyBackends(self.open_mode)) return error.ReadOnly;
            // Catalog entries are owned by the apply lifecycle. Hold its exclusive
            // lock through marker validation and checkpoint publication so deletion or
            // replacement cannot free the config or commit absence in between.
            DB.LifecycleCallbacks.lock_apply(self);
            defer self.core.unlockApply();
            const key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, index_name);
            defer alloc.free(key);
            const raw = self.core.store.get(alloc, key) catch |err| switch (err) {
                error.NotFound => return null,
                else => return err,
            };
            defer alloc.free(raw);
            return try Self.materializeManagedIndexAdmissionValueLocked(self, alloc, index_name, key, raw);
        }

        fn materializeManagedIndexAdmissionValueLocked(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            key: []const u8,
            raw: []const u8,
        ) !?u128 {
            const marker = try decodeManagedIndexAdmissionMarker(raw);
            const cfg = self.core.index_manager.get(index_name) orelse {
                try self.core.store.delete(key);
                return null;
            };
            if (builtin.is_test) {
                if (test_managed_admission_materialization_hook) |hook| {
                    if (hook.after_config_lookup) |after_lookup| after_lookup(hook.ptr, self, index_name);
                }
            }
            if (types.indexAdmissionConfigHash(cfg.*) != marker.config_hash)
                return error.InvalidManagedIndexAdmission;
            if (marker.disposition == .managed_rebuild and
                (cfg.kind != .full_text or marker.source_doc_count == 0))
                return error.InvalidManagedIndexAdmission;
            const summary = try DB.ArtifactRepairCallbacks.managed_admission_visibility_summary(self);
            const current_identity_generation = @max(
                summary.max_created_generation,
                summary.max_deleted_generation,
            );
            if (current_identity_generation < marker.identity_generation)
                return error.InvalidDocIdentity;

            return try Self.createGenerationRepairIntentAtTarget(
                self,
                alloc,
                cfg.*,
                .projection_generation_invalid,
                0,
                0,
                "managed_catalog_admission_rebuild",
                marker.replay_target_sequence,
            );
        }

        pub fn requestManagedAdmissionMaterialization(self: *DB) void {
            _ = self.managed_admission_materialization_requested.fetchAdd(1, .release);
        }

        pub fn managedAdmissionMaterializationPending(self: *const DB) bool {
            return self.managed_admission_materialization_completed.load(.acquire) !=
                self.managed_admission_materialization_requested.load(.acquire);
        }

        pub fn drainManagedIndexAdmissions(self: *DB, alloc: Allocator) !void {
            db_internal.lockAtomicWithBackoff(&self.managed_admission_materialization_mutex);
            defer self.managed_admission_materialization_mutex.unlock();

            while (true) {
                const target_generation = self.managed_admission_materialization_requested.load(.acquire);
                if (self.managed_admission_materialization_completed.load(.acquire) == target_generation) return;
                try Self.materializeManagedIndexAdmissionsOnce(self, alloc);
                self.managed_admission_materialization_completed.store(target_generation, .release);
            }
        }

        pub fn loadManagedAdmissionSnapshotForOpen(self: *DB, alloc: Allocator) !void {
            const admission_prefix = try internal_keys.managedIndexAdmissionRootPrefixAlloc(alloc);
            defer alloc.free(admission_prefix);
            const managed_admissions = try self.core.store.scanPrefix(alloc, admission_prefix);
            defer docstore_mod.DocStore.freeResults(alloc, managed_admissions);
            try self.core.index_manager.replaceManagedAdmissionSnapshot(managed_admissions);
            if (managed_admissions.len != 0 and !db_config.openModeRequiresReadOnlyBackends(self.open_mode)) {
                Self.requestManagedAdmissionMaterialization(self);
            }
        }

        pub fn initializeIndexRepairStateForOpen(self: *DB, alloc: Allocator) !void {
            if (!db_config.openModeRequiresReadOnlyBackends(self.open_mode)) {
                try Self.drainManagedIndexAdmissions(self, alloc);
                var repair_state: ?index_repair_state.State = Self.loadOrCreateCurrentIndexRepairState(self, alloc) catch |err| switch (err) {
                    error.DurableIndexRepairStateUnavailable => null,
                    error.InvalidIndexRepairState => invalid_state_blk: {
                        self.async_context.index_repair_state_corrupt.store(true, .release);
                        self.async_context.index_repair_replay_pinned.store(true, .release);
                        break :invalid_state_blk null;
                    },
                    else => return err,
                };
                if (repair_state) |*state| {
                    state.deinit(alloc);
                    repair_state = null;
                    try Self.removeOrphanedIndexRepairIntents(self, alloc);
                    repair_state = Self.loadIndexRepairState(self, alloc) catch |err| switch (err) {
                        error.FileNotFound => null,
                        else => return err,
                    };
                }
                if (repair_state) |*state| {
                    for (state.entries.items) |entry| {
                        if (entry.intent.phase != .terminal and entry.intent.automation == .enabled) {
                            self.async_context.index_repair_notification_pending = true;
                            break;
                        }
                    }
                    self.async_context.index_repair_replay_pinned.store(state.minimumRetainAfterSequence() != null, .release);
                    state.deinit(alloc);
                }
            }

            try self.core.index_manager.markManagedAdmissionsUnavailable();
            try Self.refreshIndexRepairAvailabilityGate(self, alloc);
            if (!db_config.openModeRequiresReadOnlyBackends(self.open_mode)) {
                const has_durable_repair_debt = Self.hasPendingIndexRepairIntents(self, alloc) catch true;
                if (!has_durable_repair_debt) Self.scheduleInactiveRepairShadowCleanup(self);
            }
        }

        fn materializeManagedIndexAdmissionsOnce(self: *DB, alloc: Allocator) !void {
            // This is intentionally a structural lock, not a general repair lock:
            // the primary marker, catalog membership, and borrowed config must
            // remain one lifecycle observation through checkpoint publication.
            DB.LifecycleCallbacks.lock_apply(self);
            defer self.core.unlockApply();
            const prefix = try internal_keys.managedIndexAdmissionRootPrefixAlloc(alloc);
            defer alloc.free(prefix);
            const admissions = try self.core.store.scanPrefix(alloc, prefix);
            defer docstore_mod.DocStore.freeResults(alloc, admissions);
            try Self.materializeManagedIndexAdmissionRowsLocked(self, alloc, admissions);
            if (builtin.is_test) {
                if (test_managed_admission_materialization_hook) |hook| {
                    if (hook.after_pass) |after_pass| after_pass(hook.ptr, self);
                }
            }
        }

        fn materializeManagedIndexAdmissionRowsLocked(
            self: *DB,
            alloc: Allocator,
            admissions: []const docstore_mod.OwnedKVPair,
        ) !void {
            if (admissions.len == 0) return;

            const configs = try self.core.listIndexes(alloc);
            defer types.freeIndexConfigs(alloc, configs);
            var configs_by_name = std.StringHashMapUnmanaged(*const types.IndexConfig).empty;
            defer configs_by_name.deinit(alloc);
            try configs_by_name.ensureTotalCapacity(alloc, @intCast(configs.len));
            for (configs) |*cfg| {
                configs_by_name.putAssumeCapacity(cfg.name, cfg);
            }

            for (admissions) |admission| {
                const index_name = try internal_keys.managedIndexAdmissionNameAlloc(alloc, admission.key);
                defer alloc.free(index_name);
                if (configs_by_name.get(index_name) == null) {
                    // Catalog deletion wins over an orphaned outbox row. Only
                    // catalog absence proves orphanhood; a same-name kind/hash
                    // mismatch is corruption and retains its fail-closed marker.
                    try self.core.store.delete(admission.key);
                    self.core.index_manager.clearManagedAdmissionSnapshotForIndex(index_name);
                    self.core.index_manager.clearRepairUnavailable(index_name);
                    continue;
                }
                _ = try Self.materializeManagedIndexAdmissionValueLocked(
                    self,
                    alloc,
                    index_name,
                    admission.key,
                    admission.value,
                );
            }
        }

        fn attachOperatorRepairRequest(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
            operator_job_id: u64,
            operator_job_created_at_ms: u64,
        ) !void {
            if (operator_job_id == 0 or operator_job_created_at_ms == 0) return;
            var attempts: usize = 0;
            while (attempts < 4) : (attempts += 1) {
                var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                defer entry.deinit(alloc);
                const existing_is_newer = entry.intent.operator_job_created_at_ms > operator_job_created_at_ms or
                    (entry.intent.operator_job_created_at_ms == operator_job_created_at_ms and
                        entry.intent.operator_job_id >= operator_job_id);
                if (existing_is_newer) return;
                Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .operator_job_id = operator_job_id,
                    .operator_job_created_at_ms = operator_job_created_at_ms,
                }) catch |err| switch (err) {
                    error.RepairTransitionConflict => continue,
                    else => return err,
                };
                return;
            }
            return error.RepairTransitionConflict;
        }

        fn automaticDenseGenerationRepairTriggerPriority(trigger: index_repair_state.Trigger) u8 {
            return switch (trigger) {
                .operator_generation_rebuild, .operator_generation_validation => 0,
                .artifact_coverage_mismatch => 1,
                .artifact_counter_missing => 2,
                .incomplete_bulk_publish, .root_generation_rebuild, .projection_generation_invalid => 3,
            };
        }

        pub fn ensureAutomaticDenseGenerationRepairIntent(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            trigger: index_repair_state.Trigger,
            last_error: []const u8,
        ) !u128 {
            std.debug.assert(trigger == .artifact_coverage_mismatch or
                trigger == .artifact_counter_missing or
                trigger == .projection_generation_invalid);

            // Every automatic generation repair represents missing correctness
            // proof. Close the query gate before persistence so an I/O failure
            // cannot leave a known-unverified index available.
            try self.core.index_manager.markRepairUnavailable(cfg.name);

            const repair_id = (try Self.indexRepairIdForIndex(self, alloc, cfg.name)) orelse
                try Self.createGenerationRepairIntent(
                    self,
                    alloc,
                    cfg,
                    trigger,
                    0,
                    0,
                    last_error,
                );

            // Creation races with an operator request are harmless: re-read the
            // winning intent and promote its safety classification when needed.
            var attempts: usize = 0;
            while (attempts < 4) : (attempts += 1) {
                var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                defer entry.deinit(alloc);
                if (automaticDenseGenerationRepairTriggerPriority(trigger) <=
                    automaticDenseGenerationRepairTriggerPriority(entry.intent.trigger)) return repair_id;
                Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .trigger = trigger,
                    .replace_last_error = true,
                    .last_error = last_error,
                }) catch |err| switch (err) {
                    error.RepairTransitionConflict => continue,
                    else => return err,
                };
                return repair_id;
            }
            return error.RepairTransitionConflict;
        }

        pub const DenseArtifactCounterBootstrapSnapshot = struct {
            txn: docstore_mod.DocStore.Txn,
            attempt_id: u128,

            pub fn deinit(self: *@This()) void {
                self.txn.abort();
                self.* = undefined;
            }
        };

        pub fn beginDenseArtifactCounterBootstrapSnapshot(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            repair_id: u128,
        ) !DenseArtifactCounterBootstrapSnapshot {
            DB.LifecycleCallbacks.lock_apply(self);
            defer self.core.unlockApply();

            if (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, index_name) != null) {
                return error.DenseArtifactCounterAlreadyInitialized;
            }
            const attempt_id = try index_repair_state.newRepairId(alloc);
            const marker_key = try DB.DerivedAsyncCallbacks.dense_artifact_counter_bootstrap_key_alloc(alloc, index_name);
            defer alloc.free(marker_key);
            var marker_value: [DB.DerivedAsyncCallbacks.dense_artifact_counter_bootstrap_encoded_len]u8 = undefined;
            DB.DerivedAsyncCallbacks.encode_dense_artifact_counter_bootstrap(.{
                .repair_id = repair_id,
                .attempt_id = attempt_id,
            }, &marker_value);
            try self.core.store.putBatch(&.{.{ .key = marker_key, .value = &marker_value }}, &.{});

            // The apply lock orders marker publication before this snapshot and
            // before every later artifact mutation. Once it is released, those
            // mutations update the marker's signed delta in their own atomic store
            // batch while this stable snapshot is counted without blocking writes.
            return .{
                .txn = try self.core.store.beginReadTxn(),
                .attempt_id = attempt_id,
            };
        }

        pub fn finishDenseArtifactCounterBootstrap(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            repair_id: u128,
            attempt_id: u128,
            snapshot_count: u64,
        ) !void {
            DB.LifecycleCallbacks.lock_apply(self);
            defer self.core.unlockApply();

            if (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, index_name) != null) return;
            const marker = (try DB.DerivedAsyncCallbacks.load_dense_artifact_counter_bootstrap(alloc, self.core.store, index_name)) orelse
                return error.DenseArtifactCounterBootstrapMissing;
            if (marker.repair_id != repair_id or marker.attempt_id != attempt_id) {
                return error.StaleDenseArtifactCounterBootstrap;
            }

            const final_count_i128 = @as(i128, @intCast(snapshot_count)) + @as(i128, marker.delta);
            if (final_count_i128 < 0 or final_count_i128 > std.math.maxInt(u64)) {
                return error.InvalidDenseArtifactCounterBootstrapDelta;
            }
            const counter_key = try DB.DerivedAsyncCallbacks.dense_artifact_target_counter_key_alloc(alloc, index_name);
            defer alloc.free(counter_key);
            const marker_key = try DB.DerivedAsyncCallbacks.dense_artifact_counter_bootstrap_key_alloc(alloc, index_name);
            defer alloc.free(marker_key);
            var counter_value: [8]u8 = undefined;
            std.mem.writeInt(u64, &counter_value, @intCast(final_count_i128), .little);
            try self.core.store.putBatch(
                &.{.{ .key = counter_key, .value = &counter_value }},
                &.{marker_key},
            );
        }

        pub fn countDenseArtifactsForConfigFromReadTxn(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            read_txn: *docstore_mod.DocStore.Txn,
            cancel_check: ?types.RepairCancelCheck,
        ) !u64 {
            const artifact_name = try index_manager_mod.denseConfigArtifactNameAlloc(alloc, cfg);
            defer alloc.free(artifact_name);
            const dims = try index_manager_mod.denseConfigDimensions(alloc, cfg);
            const lower = try self.core.documentRangeLowerAlloc("");
            defer self.core.alloc.free(lower);

            const ScanState = struct {
                alloc: Allocator,
                artifact_name: []const u8,
                dims: u32,
                cancel_check: ?types.RepairCancelCheck,
                scanned: usize = 0,
                count: u64 = 0,

                fn scanEntry(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!docstore_mod.DocStore.ScanAction {
                    const state: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                    state.scanned +|= 1;
                    if ((state.scanned & 1023) == 0) {
                        if (state.cancel_check) |check| if (check.requested()) return error.Canceled;
                    }
                    if (!internal_keys.isInternalUserKey(key)) return .@"continue";
                    var artifact_ref = (try db_internal.decodeArtifactRefIfKnownAlloc(state.alloc, key)) orelse return .@"continue";
                    defer artifact_ref.deinit(state.alloc);
                    if (artifact_ref.kind != .embedding or
                        !std.mem.eql(u8, artifact_ref.name, state.artifact_name))
                    {
                        return .@"continue";
                    }
                    const artifact_dims = enrichment_artifact_codec.decodeDenseEmbeddingDims(value) catch |err| {
                        if (isRecoverableEmbeddingArtifactError(err)) return .@"continue";
                        return err;
                    };
                    if (artifact_dims == state.dims) state.count +|= 1;
                    return .@"continue";
                }
            };

            var scan_state = ScanState{
                .alloc = alloc,
                .artifact_name = artifact_name,
                .dims = dims,
                .cancel_check = cancel_check,
            };
            try self.core.store.scanReadTxnWithContext(read_txn, lower, "", .{}, &scan_state, ScanState.scanEntry);
            return scan_state.count;
        }

        pub fn ensureDenseArtifactTargetCounterForRepair(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            repair_id: u128,
            cancel_check: ?types.RepairCancelCheck,
        ) !void {
            if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return;
            if (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name) != null) return;

            // Counter bootstrap is a background repair phase in its own right. It
            // can hold a stable store snapshot for a full artifact scan, so admit
            // it before publishing the marker rather than doing unaccounted work
            // that a later shadow-build admission would reject.
            var bootstrap_admission: ?resource_manager_mod.Reservation = if (self.core.index_manager.resource_manager) |manager|
                try reserveDenseCounterBootstrapWorkingSet(manager)
            else
                null;
            defer if (bootstrap_admission) |*reservation| reservation.release();

            var bootstrap_snapshot = Self.beginDenseArtifactCounterBootstrapSnapshot(self, alloc, cfg.name, repair_id) catch |err| switch (err) {
                error.DenseArtifactCounterAlreadyInitialized => return,
                else => return err,
            };
            defer bootstrap_snapshot.deinit();

            const snapshot_count = try Self.countDenseArtifactsForConfigFromReadTxn(
                self,
                alloc,
                cfg,
                &bootstrap_snapshot.txn,
                cancel_check,
            );
            if (cancel_check) |check| if (check.requested()) return error.Canceled;
            try Self.finishDenseArtifactCounterBootstrap(
                self,
                alloc,
                cfg.name,
                repair_id,
                bootstrap_snapshot.attempt_id,
                snapshot_count,
            );
        }

        fn validateIndexRepairSourcePreflight(self: *DB, alloc: Allocator, cfg: types.IndexConfig) !void {
            if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return;
            const target_count = try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name);
            if (target_count == null) return error.RepairSourceCoverageIncomplete;
        }

        fn currentIndexRepairIdMatches(self: *const DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !?u128 {
            const repair_id = (try Self.indexRepairIdForIndex(self, alloc, index_name)) orelse {
                if (expected_repair_id != null) return error.StaleIndexRepairControl;
                return null;
            };
            if (expected_repair_id) |expected| {
                if (repair_id != expected) return error.StaleIndexRepairControl;
            }
            return repair_id;
        }

        pub fn pauseAutomaticIndexRepair(self: *DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
            const repair_id = (try Self.currentIndexRepairIdMatches(self, alloc, index_name, expected_repair_id)) orelse return false;
            try Self.updateIndexRepairIntent(self, alloc, repair_id, .{ .automation = .paused });
            _ = Self.requestActiveIndexRepairCancellation(self, index_name);
            return true;
        }

        pub fn cancelCurrentIndexRepairAttempt(self: *DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
            _ = (try Self.currentIndexRepairIdMatches(self, alloc, index_name, expected_repair_id)) orelse return false;
            return Self.requestActiveIndexRepairCancellation(self, index_name);
        }

        pub fn resumeAutomaticIndexRepair(self: *DB, alloc: Allocator, index_name: []const u8, expected_repair_id: ?u128) !bool {
            const repair_id = (try Self.currentIndexRepairIdMatches(self, alloc, index_name, expected_repair_id)) orelse return false;
            try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .automation = .enabled,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
            return true;
        }

        pub fn prepareIndexRepairForDeletion(self: *DB, alloc: Allocator, index_name: []const u8) !?u128 {
            const repair_id = (try Self.indexRepairIdForIndex(self, alloc, index_name)) orelse return null;
            if (!(try Self.beginIndexRepairLease(self, index_name))) return error.IndexRepairInProgress;
            errdefer Self.endIndexRepairLease(self, index_name);
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);
            if (entry.intent.candidate_relative_path) |candidate| {
                if (!try self.core.index_manager.isRepairCandidateActive(index_name, candidate)) {
                    try Self.discardInactiveIndexRepairCandidate(self, alloc, repair_id);
                }
            }
            return repair_id;
        }

        fn removeOrphanedIndexRepairIntents(self: *DB, alloc: Allocator) !void {
            var state = Self.loadIndexRepairState(self, alloc) catch |err| switch (err) {
                error.FileNotFound => return,
                else => return err,
            };
            defer state.deinit(alloc);
            var orphan_ids = std.ArrayListUnmanaged(u128).empty;
            defer orphan_ids.deinit(alloc);
            for (state.entries.items) |entry| {
                if (self.core.index_manager.get(entry.intent.index_name) == null) {
                    try orphan_ids.append(alloc, entry.intent.repair_id);
                }
            }
            for (orphan_ids.items) |repair_id| try Self.removeIndexRepairIntentAndPin(self, alloc, repair_id);
        }

        pub fn removeOrphanedIndexRepairIntentForFreshAdmission(self: *DB, alloc: Allocator, index_name: []const u8) !void {
            if (self.core.index_manager.get(index_name) != null) return;
            const repair_id = (try Self.indexRepairIdForIndex(self, alloc, index_name)) orelse return;
            try Self.removeIndexRepairIntentAndPin(self, alloc, repair_id);
        }

        fn recordIndexRepairAttemptFailure(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
            err_name: []const u8,
            terminal: bool,
        ) !void {
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);
            const now_ms = currentTimeNs() / std.time.ns_per_ms;
            const attempt_count = @max(entry.intent.attempt_count, 1);
            const failure_streak = entry.intent.failure_streak +| 1;
            try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .phase = if (terminal) .terminal else entry.intent.phase,
                .attempt_count = attempt_count,
                .failure_streak = failure_streak,
                .next_retry_at_ms = if (terminal) 0 else now_ms +| indexRepairRetryDelayMs(repair_id, failure_streak),
                .last_error = err_name,
                .replace_last_error = true,
            });
        }

        fn indexRepairFailureIsTerminal(err: anyerror) bool {
            return switch (err) {
                error.InvalidArgument,
                error.NotFound,
                error.UnsupportedOperation,
                error.InvalidIndexRepairState,
                error.InvalidRepairCandidatePath,
                error.ReplicaIdentityMismatch,
                error.DurableIndexRepairStateUnavailable,
                error.RepairSourceCoverageIncomplete,
                error.IndexRepairConfigurationChanged,
                => true,
                else => false,
            };
        }

        fn retryableIndexRepairTerminalPhase(
            last_error: ?[]const u8,
            trigger: index_repair_state.Trigger,
        ) ?index_repair_state.Phase {
            const reason = last_error orelse return null;
            if (!std.mem.eql(u8, reason, @errorName(error.RepairSourceCoverageIncomplete))) return null;
            // Only managed replacement/artifact generations can become complete
            // after source artifacts are reprocessed. Structural-invalid and
            // externally supplied generations retain fail-closed classification.
            if (trigger != .operator_generation_rebuild and
                trigger != .artifact_coverage_mismatch)
            {
                return null;
            }
            // Durable transitions permit terminal -> detected, after which the
            // ordinary state machine revalidates current-generation coverage.
            return .detected;
        }

        test "index repair treats source coverage lag as retryable" {
            try std.testing.expect(indexRepairFailureIsTerminal(error.RepairSourceCoverageIncomplete));
            try std.testing.expect(indexRepairFailureIsTerminal(error.IndexRepairConfigurationChanged));
            try std.testing.expectEqual(
                index_repair_state.Phase.detected,
                retryableIndexRepairTerminalPhase("RepairSourceCoverageIncomplete", .operator_generation_rebuild).?,
            );
            try std.testing.expect(
                retryableIndexRepairTerminalPhase("RepairSourceCoverageIncomplete", .projection_generation_invalid) == null,
            );
            try std.testing.expectEqual(
                index_repair_state.Phase.detected,
                retryableIndexRepairTerminalPhase("RepairSourceCoverageIncomplete", .artifact_coverage_mismatch).?,
            );
            try std.testing.expect(
                retryableIndexRepairTerminalPhase("IndexRepairConfigurationChanged", .operator_generation_rebuild) == null,
            );
        }

        pub fn removeIndexRepairIntentAndPin(self: *DB, alloc: Allocator, repair_id: u128) !void {
            return try removeIndexRepairIntentAndPinContext(self.async_context, alloc, repair_id);
        }

        fn removeIndexRepairIntentAndPinContext(ctx: *AsyncContext, alloc: Allocator, repair_id: u128) !void {
            // Pin creation, pin removal, the in-memory pressure gate, and replay
            // truncation must observe one serialization order. Per-index repair
            // leases are insufficient because two different indexes may complete
            // concurrently.
            const repair_replay_mutex = ctx.repair_replay_mutex orelse return error.DurableIndexRepairStateUnavailable;
            db_internal.lockAtomicWithBackoff(repair_replay_mutex);
            defer repair_replay_mutex.unlock();
            const location = try indexRepairStateLocationContext(ctx);
            var state = try index_repair_state.loadAt(alloc, location);
            defer state.deinit(alloc);
            const entry = blk: {
                for (state.entries.items) |candidate| {
                    if (candidate.intent.repair_id == repair_id) break :blk candidate;
                }
                return error.IndexRepairIntentNotFound;
            };
            if (entry.intent.phase == .cleanup) try persistOperatorRepairCompletionContext(ctx, alloc, entry.intent);
            var remaining_replay_pin = false;
            for (state.entries.items) |candidate| {
                if (candidate.intent.repair_id != repair_id and candidate.pin != null) {
                    remaining_replay_pin = true;
                    break;
                }
            }
            // For managed admission, clear the primary-store lifecycle marker only
            // after activation published a clean generation. Do this before
            // removing the checkpoint intent: a crash between the two leaves
            // harmless resumable checkpoint debt, never an admitted generation
            // with no proof.
            const admission_key = try internal_keys.managedIndexAdmissionKeyAlloc(alloc, entry.intent.index_name);
            defer alloc.free(admission_key);
            ctx.store.delete(admission_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try index_repair_state.removeEntryAndPinAt(alloc, location, state.identity, .{
                .repair_id = repair_id,
                .revision = entry.intent.revision,
                .phase = entry.intent.phase,
                .config_hash = entry.intent.config_hash,
                .root_generation = entry.intent.root_generation,
                .owner_epoch = entry.intent.owner_epoch,
            });
            ctx.index_repair_replay_pinned.store(remaining_replay_pin, .release);
            ctx.index_manager.clearRepairUnavailable(entry.intent.index_name);
            DB.notifyQueryVisibilityHook(
                ctx,
                if (remaining_replay_pin) .index_repair_pending else .index_repair_cleared,
            );
        }

        pub fn denseRepairWriteBackpressured(self: *const DB) bool {
            if (!self.async_context.index_repair_replay_pinned.load(.acquire)) return false;
            const manager = self.core.index_manager.resource_manager orelse return false;
            return manager.denseRepairReplayPressureIsHard();
        }

        fn discardInactiveIndexRepairCandidate(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
        ) !void {
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);
            const candidate = entry.intent.candidate_relative_path orelse return;
            try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
            if (try self.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) {
                return error.RepairCandidateAlreadyActive;
            }
            const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
            const shadow_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate[0..separator] });
            defer alloc.free(shadow_root);
            var io_impl = threadedIo();
            defer io_impl.deinit();
            // `deleteTree` is idempotent for an absent root. A cancelled
            // pre-checkpoint build may already have removed its non-reopenable
            // shadow in the unwind path.
            try std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_root);
            try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .phase = .preflight,
                .replace_candidate_path = true,
                .replace_build_resume_key = true,
                .build_reprocessed = 0,
                .candidate_applied_sequence = 0,
                .target_sequence = self.core.nextDerivedSequence(),
                .failure_streak = 0,
                .next_retry_at_ms = 0,
                .replace_last_error = true,
            });
        }

        fn reconcileActivatedIndexRepair(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
        ) !bool {
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);
            const candidate = entry.intent.candidate_relative_path orelse return false;
            try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
            if (!try self.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) return false;
            if (!self.core.index_manager.has(entry.intent.index_name) or
                self.core.index_manager.loadFailure(entry.intent.index_name) != null)
            {
                return false;
            }

            try self.core.index_manager.syncIndexByName(entry.intent.index_name, true);
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, entry.intent.index_name);
            const applied_sequence = @max(checkpoint.applied_sequence, entry.intent.candidate_applied_sequence);
            if (entry.intent.phase != .cleanup or checkpoint.status != .clean or checkpoint.config_hash != entry.intent.config_hash) {
                const update = apply_state.AppliedSequenceUpdate{
                    .index_name = entry.intent.index_name,
                    .sequence = applied_sequence,
                    .status = .clean,
                    .generation = checkpoint.generation +| 1,
                    .config_hash = entry.intent.config_hash,
                };
                try DB.DerivedAsyncCallbacks.save_index_status_snapshots(alloc, self.core.store, self.core.index_manager, &[_]apply_state.AppliedSequenceUpdate{update});
                try self.core.saveAppliedSequence(entry.intent.index_name, applied_sequence);
                try self.core.saveProjectionCheckpoint(entry.intent.index_name, .{
                    .applied_sequence = update.sequence,
                    .status = update.status,
                    .generation = update.generation,
                    .config_hash = update.config_hash,
                });
                try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .cleanup,
                    .candidate_applied_sequence = applied_sequence,
                    .target_sequence = applied_sequence,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
            }
            try Self.removeIndexRepairIntentAndPin(self, alloc, repair_id);
            Self.scheduleInactiveRepairShadowCleanup(self);
            return true;
        }

        pub fn rollbackUnavailableActivatedIndexRepair(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
        ) !bool {
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);
            if (!entry.intent.previous_pointer_captured) return false;
            const candidate = entry.intent.candidate_relative_path orelse return false;
            if (!try self.core.index_manager.isRepairCandidateActive(entry.intent.index_name, candidate)) return false;
            {
                var structural_guard = self.beginIndexStructuralMutation("index repair rollback", entry.intent.index_name);
                defer structural_guard.deinit();
                self.core.lockApply();
                defer self.core.unlockApply();
                try self.core.index_manager.restoreActiveIndexRootPointer(
                    entry.intent.index_name,
                    candidate,
                    entry.intent.previous_active_relative_path,
                );
            }
            _ = try self.retryQuarantinedIndexLoads(true);
            try Self.discardInactiveIndexRepairCandidate(self, alloc, repair_id);
            return true;
        }

        pub fn discoverRecoverableStartupIndexFailures(
            self: *DB,
            alloc: Allocator,
            limit: usize,
        ) !StartupIndexRepairDiscovery {
            var result: StartupIndexRepairDiscovery = .{};
            var state = try Self.loadOrCreateCurrentIndexRepairState(self, alloc);
            defer state.deinit(alloc);
            const location = try Self.indexRepairStateLocation(self);

            const configs = try self.core.index_manager.listIndexesPublic(alloc);
            defer {
                for (configs) |*cfg| cfg.deinit(alloc);
                alloc.free(configs);
            }
            for (configs) |cfg| {
                const action = self.core.index_manager.recoveryActionForIndex(cfg.name) orelse continue;
                if (state.findIndex(cfg.name)) |i| {
                    if (state.entries.items[i].intent.phase == .terminal) {
                        result.terminal += 1;
                        result.existing_terminal += 1;
                    } else {
                        result.already_pending += 1;
                    }
                    continue;
                }
                switch (action) {
                    .retry_open => continue,
                    .manual_intervention => {
                        result.terminal += 1;
                        continue;
                    },
                    .rebuild_from_artifacts => {},
                }
                // IncompleteBulkPublish is currently an HBC dense-index failure.
                // Do not let a future reuse of the error name silently broaden the
                // destructive automatic-reconstruction allowlist.
                if (cfg.kind != .dense_vector) {
                    result.terminal += 1;
                    continue;
                }
                if (result.discovered >= limit) continue;

                const checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
                const now_ms = currentTimeNs() / std.time.ns_per_ms;
                const target_sequence = self.core.nextDerivedSequence();
                const repair_id = try index_repair_state.newRepairId(alloc);
                const index_name = try alloc.dupe(u8, cfg.name);
                var intent = index_repair_state.IndexRepairIntent{
                    .repair_id = repair_id,
                    .db_identity = state.identity.db_identity,
                    .group_id = Self.localRepairGroupId(self),
                    .replica_id = state.identity.replica_id,
                    .root_generation = state.identity.root_generation,
                    .index_name = index_name,
                    .kind = cfg.kind,
                    .config_hash = types.indexConfigHash(cfg),
                    .detected_sequence = target_sequence,
                    .target_sequence = target_sequence,
                    .started_at_ms = now_ms,
                    .updated_at_ms = now_ms,
                    .owner_epoch = 0,
                };
                defer intent.deinit(alloc);
                try index_repair_state.putEntryAt(alloc, location, state.identity, null, .{ .intent = intent });
                try self.core.index_manager.markRepairUnavailable(cfg.name);
                DB.notifyQueryVisibilityHook(self.async_context, .index_repair_pending);
                result.discovered += 1;

                // Intent durability precedes the serviceability marker. If this
                // checkpoint write fails, restart still rediscovers the intent and
                // completes the transition without deleting the quarantined root.
                try self.core.saveProjectionCheckpoint(cfg.name, .{
                    .applied_sequence = checkpoint.applied_sequence,
                    .status = .repair_required,
                    .generation = checkpoint.generation,
                    .config_hash = types.indexConfigHash(cfg),
                });
            }
            return result;
        }

        pub const IndexRepairAdvanceResult = struct {
            repair_id: u128,
            attempted: bool = false,
            repaired: bool = false,
            busy: bool = false,
            deferred: bool = false,
            disk_wait: bool = false,
            terminal: bool = false,
            documents_reprocessed: u64 = 0,
            next_retry_at_ms: u64 = 0,
            has_repair_outcome: bool = false,
            artifacts_repaired: u64 = 0,
            missing_source_docs: u64 = 0,
            failed: u64 = 0,
            unsupported: u64 = 0,
            unresolved: u64 = 0,
            in_progress: u64 = 0,
            indexes_rebuilt: u64 = 0,
            indexes_degraded_before: u64 = 0,
            indexes_degraded_after: u64 = 0,
            debt_remaining: bool = false,
        };

        const DurableIndexRepairRunControl = struct {
            db: *DB,
            index_name: []const u8,
            upstream: ?types.RepairCancelCheck,
            ownership: ?types.RepairActivationCheck,

            fn cancelled(ptr: *anyopaque) bool {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                if (self.upstream) |check| {
                    if (check.requested()) return true;
                }
                if (self.ownership) |check| {
                    if (!(check.current() catch return true)) return true;
                }
                return Self.activeIndexRepairCancellationRequested(self.db, self.index_name);
            }
        };

        pub const RepairCapacityGuard = struct {
            reservation: *resource_manager_mod.CapacityReservation,
            db: *DB,
            alloc: Allocator,
            repair_id: u128,
            options: types.ArtifactRepairRunOptions,
            admitted_total_bytes: u64,
            headroom_bytes: u64,
            candidate_root: ?[]const u8 = null,

            pub fn bindCandidateRoot(ptr: *anyopaque, candidate_root: []const u8) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                self.candidate_root = candidate_root;
            }

            fn reconcile(ptr: *anyopaque, candidate_bytes: u64) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const required = candidate_bytes +| self.headroom_bytes;
                const next_plan = @max(required, self.admitted_total_bytes);
                const desired_future_claim = next_plan -| candidate_bytes;
                const observation = try repairCapacityObservation(self.options);
                const now_ns = monotonicTimeNs();
                try self.reservation.resize(desired_future_claim, observation, now_ns);
                // Shrinking a claim is admission-free, but this boundary follows
                // materialized writes. Revalidate the remaining aggregate claim
                // against the same fresh observation before advancing state.
                try self.reservation.revalidate(observation, now_ns);
                if (next_plan != self.admitted_total_bytes) {
                    try Self.updateIndexRepairIntent(self.db, self.alloc, self.repair_id, .{ .planned_disk_bytes = next_plan });
                    self.admitted_total_bytes = next_plan;
                }
            }

            pub fn revalidate(ptr: *anyopaque) !void {
                const self: *@This() = @ptrCast(@alignCast(ptr));
                const observation = try repairCapacityObservation(self.options);
                // Availability already includes materialized candidate bytes,
                // while the reservation describes future growth. Stay on the O(1)
                // path while the conservative claim fits. A failed probe is not a
                // denial yet: reconcile exact candidate bytes first, then let the
                // final revalidation record a real denial only if it still fails.
                if (try self.reservation.fits(observation, monotonicTimeNs())) return;
                if (self.candidate_root) |candidate_root| {
                    return try RepairCapacityGuard.reconcile(self, try directoryUsageBytes(self.alloc, candidate_root));
                }
                try self.reservation.revalidate(observation, monotonicTimeNs());
            }
        };

        fn repairCapacityDomain(options: types.ArtifactRepairRunOptions) resource_manager_mod.CapacityDomainId {
            if (options.capacity_source) |source| return source.domain_id;
            return options.capacity_domain_id;
        }

        pub fn repairCapacityObservation(options: types.ArtifactRepairRunOptions) !resource_manager_mod.CapacityObservation {
            const observed = if (options.capacity_source) |source|
                source.current() catch |err| switch (err) {
                    // Capacity observation is an optional backend capability.
                    // Unsupported backends still participate in node-wide byte
                    // accounting; transient failures from a supported source
                    // remain retryable and fail admission closed.
                    error.UnsupportedPlatform => resource_manager_mod.CapacityObservation{},
                    else => return err,
                }
            else
                options.capacity_observation;
            return .{
                .available_bytes = observed.available_bytes,
                .capacity_bytes = observed.capacity_bytes,
                .observed_at_ns = observed.observed_at_ns,
                .valid_for_ns = observed.valid_for_ns,
            };
        }

        fn artifactBackedDenseRepairEstimate(self: *DB, alloc: Allocator, cfg: types.IndexConfig) !u64 {
            if (!try index_manager_mod.denseConfigRequiresArtifactCoverage(alloc, cfg)) return 0;
            const target_count = (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, cfg.name)) orelse return 0;
            const dims = try index_manager_mod.denseConfigDimensions(alloc, cfg);
            // Candidate storage contains the source vector, HBC node/posting
            // state, quantized payloads, keys, and LSM metadata. Two times the raw
            // vector payload plus a per-vector structural allowance is a
            // deliberately conservative preflight; live boundary checks below
            // remain authoritative as bytes materialize.
            const per_vector = std.math.add(u64, @as(u64, dims) *| @sizeOf(f32), 512) catch std.math.maxInt(u64);
            const logical = std.math.mul(u64, target_count, per_vector) catch std.math.maxInt(u64);
            return std.math.mul(u64, logical, 2) catch std.math.maxInt(u64);
        }

        pub const RepairWorkingSetPlan = struct {
            reservation_bytes: u64,
            dense_rebuild_batch_items: usize = 2048,
        };

        pub const dense_counter_bootstrap_working_set_bytes: u64 = 8 * 1024 * 1024;

        pub fn reserveDenseCounterBootstrapWorkingSet(
            manager: *resource_manager_mod.ResourceManager,
        ) !resource_manager_mod.Reservation {
            const slice_stats = manager.sliceStats(.dense_repair_working_set);
            const hard_available = if (slice_stats.hard_limit_bytes == 0)
                std.math.maxInt(u64)
            else
                slice_stats.hard_limit_bytes -| slice_stats.used_bytes;
            // Let ResourceManager perform an authoritative hard rejection so the
            // shared rejection metric remains accurate under pressure and races.
            if (hard_available < dense_counter_bootstrap_working_set_bytes) {
                return manager.reserve(.dense_repair_working_set, dense_counter_bootstrap_working_set_bytes) catch
                    return error.RepairResourceUnavailable;
            }
            const soft_available = if (slice_stats.soft_limit_bytes == 0)
                hard_available
            else
                slice_stats.soft_limit_bytes -| slice_stats.used_bytes;
            if (soft_available < dense_counter_bootstrap_working_set_bytes) {
                return error.RepairResourceUnavailable;
            }
            return manager.reserve(.dense_repair_working_set, dense_counter_bootstrap_working_set_bytes) catch
                return error.RepairResourceUnavailable;
        }

        pub fn repairWorkingSetPlan(
            alloc: Allocator,
            manager: *resource_manager_mod.ResourceManager,
            cfg: types.IndexConfig,
        ) !RepairWorkingSetPlan {
            const base_bytes: u64 = 8 * 1024 * 1024;
            if (cfg.kind != .dense_vector) return .{ .reservation_bytes = base_bytes };

            const dims = try index_manager_mod.denseConfigDimensions(alloc, cfg);
            // The snapshot scanner owns one decoded vector while the mapper/HBC
            // boundary may transiently own a second copy. Include keys, artifact
            // identity, list capacity, and allocator overhead per item. HBC/LSM
            // caches and routing work remain independently accounted by their own
            // ResourceManager slices.
            const raw_vector_bytes = std.math.mul(u64, dims, @sizeOf(f32)) catch std.math.maxInt(u64);
            const per_item_bytes = (std.math.mul(u64, raw_vector_bytes, 2) catch std.math.maxInt(u64)) +| 2048;
            const desired_items: u64 = 2048;
            const slice_stats = manager.sliceStats(.dense_repair_working_set);
            const hard_available = if (slice_stats.hard_limit_bytes == 0)
                std.math.maxInt(u64)
            else
                slice_stats.hard_limit_bytes -| slice_stats.used_bytes;
            const minimum_reservation = base_bytes +| per_item_bytes;
            // Preserve ResourceManager's authoritative hard-limit rejection and
            // metric. For admissible work, however, size background batches to the
            // soft budget: reaching the hard limit is a safety failure, not a
            // throughput target.
            if (hard_available < minimum_reservation) {
                return .{
                    .reservation_bytes = minimum_reservation,
                    .dense_rebuild_batch_items = 1,
                };
            }
            const available = if (slice_stats.soft_limit_bytes == 0)
                hard_available
            else
                slice_stats.soft_limit_bytes -| slice_stats.used_bytes;
            if (available < minimum_reservation) return error.RepairResourceUnavailable;
            const fitting_items = if (available <= base_bytes)
                0
            else
                (available - base_bytes) / @max(@as(u64, 1), per_item_bytes);
            const batch_items = @min(desired_items, fitting_items);
            return .{
                .reservation_bytes = base_bytes +| (per_item_bytes *| batch_items),
                .dense_rebuild_batch_items = if (comptime builtin.is_test)
                    test_dense_repair_rebuild_batch_size orelse @as(usize, @intCast(batch_items))
                else
                    @intCast(batch_items),
            };
        }

        pub fn advanceIndexRepairIntent(
            self: *DB,
            alloc: Allocator,
            repair_id: u128,
            options: types.ArtifactRepairRunOptions,
        ) anyerror!IndexRepairAdvanceResult {
            var result = IndexRepairAdvanceResult{ .repair_id = repair_id };
            var effective_options = options;
            if (effective_options.capacity_source == null) effective_options.capacity_source = self.capacity_source;
            var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer entry.deinit(alloc);

            if (entry.intent.root_generation != self.core.root_generation or
                entry.intent.group_id != Self.localRepairGroupId(self))
            {
                try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "repair_identity_mismatch", true);
                result.terminal = true;
                return result;
            }
            if (entry.intent.phase == .terminal) {
                if (retryableIndexRepairTerminalPhase(
                    entry.intent.last_error,
                    entry.intent.trigger,
                )) |retry_phase| {
                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .phase = retry_phase,
                        .next_retry_at_ms = 0,
                        .replace_last_error = true,
                    });
                    entry.intent.phase = retry_phase;
                } else {
                    result.terminal = true;
                    return result;
                }
            }
            // A candidate which already failed coverage cannot manufacture an
            // artifact missing below its snapshot floor. Discard only that
            // inactive candidate and rebuild from a current pinned snapshot.
            if (entry.intent.candidate_relative_path != null and
                retryableIndexRepairTerminalPhase(
                    entry.intent.last_error,
                    entry.intent.trigger,
                ) != null)
            {
                try Self.discardInactiveIndexRepairCandidate(self, alloc, repair_id);
                result.attempted = true;
                result.deferred = true;
                return result;
            }
            if (entry.intent.automation == .paused) {
                result.deferred = true;
                return result;
            }
            const now_ms = currentTimeNs() / std.time.ns_per_ms;
            if (entry.intent.next_retry_at_ms > now_ms) {
                result.deferred = true;
                result.next_retry_at_ms = entry.intent.next_retry_at_ms;
                return result;
            }
            db_internal.checkArtifactRepairActivationOwner(options) catch |err| switch (err) {
                error.RepairOwnershipLost => {
                    result.deferred = true;
                    return result;
                },
                else => return err,
            };

            // Claim the durable state machine with the same epoch captured by the
            // owner-side activation fence. A later term/root owner may safely
            // reclaim the intent, but every subsequent transition and the final
            // activation in this attempt are CAS-fenced by the new epoch.
            if (options.owner_epoch != 0 and entry.intent.owner_epoch != options.owner_epoch) {
                try Self.updateIndexRepairIntent(self, alloc, repair_id, .{ .owner_epoch = options.owner_epoch });
                entry.intent.owner_epoch = options.owner_epoch;
            }

            const cfg_ptr = self.core.index_manager.get(entry.intent.index_name) orelse {
                try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "index_configuration_missing", true);
                result.terminal = true;
                return result;
            };
            if (cfg_ptr.kind != entry.intent.kind or types.indexConfigHash(cfg_ptr.*) != entry.intent.config_hash) {
                try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "index_configuration_changed", true);
                result.terminal = true;
                return result;
            }

            // Missing-counter projections bootstrap coverage from a stable primary
            // snapshot plus an atomically maintained concurrent-write delta. The
            // durable repair intent authorizes retry after cancellation or restart;
            // no candidate directory is created until authoritative coverage exists.
            Self.ensureDenseArtifactTargetCounterForRepair(
                self,
                alloc,
                cfg_ptr.*,
                repair_id,
                effective_options.cancel_check,
            ) catch |err| switch (err) {
                error.Canceled, error.StaleDenseArtifactCounterBootstrap => {
                    result.attempted = true;
                    result.deferred = true;
                    return result;
                },
                error.RepairResourceUnavailable => {
                    const attempt_count = entry.intent.attempt_count +| 1;
                    const failure_streak = entry.intent.failure_streak +| 1;
                    const next_retry_at_ms = now_ms +| indexRepairRetryDelayMs(repair_id, failure_streak);
                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .attempt_count = attempt_count,
                        .failure_streak = failure_streak,
                        .next_retry_at_ms = next_retry_at_ms,
                        .last_error = "RepairResourceUnavailable",
                        .replace_last_error = true,
                    });
                    result.attempted = true;
                    result.deferred = true;
                    result.next_retry_at_ms = next_retry_at_ms;
                    return result;
                },
                else => return err,
            };

            // Automatic reconstruction is destructive only at pointer activation,
            // but creating an apparently valid empty candidate from missing source
            // metadata is still unsafe. Full per-artifact validation remains part
            // of the shadow build.
            Self.validateIndexRepairSourcePreflight(self, alloc, cfg_ptr.*) catch |err| {
                const err_name = if (err == error.RepairSourceCoverageIncomplete)
                    "dense_artifact_coverage_missing"
                else
                    @errorName(err);
                try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, err_name, true);
                result.attempted = true;
                result.terminal = true;
                return result;
            };

            const active_index_bytes = self.core.index_manager.activeIndexStorageBytes(entry.intent.index_name) catch 0;
            const source_estimate = Self.artifactBackedDenseRepairEstimate(self, alloc, cfg_ptr.*) catch 0;
            var estimated_candidate_bytes = @max(
                entry.intent.estimated_candidate_bytes,
                @max(options.estimated_candidate_bytes, @max(active_index_bytes, source_estimate)),
            );
            if (estimated_candidate_bytes == 0) {
                estimated_candidate_bytes = 64 * 1024 * 1024;
            }
            const cleanup_and_replay_headroom = @max(estimated_candidate_bytes / 4, 256 * 1024 * 1024);
            const required_disk_bytes = estimated_candidate_bytes +| cleanup_and_replay_headroom;
            const planned_disk_bytes = @max(
                entry.intent.planned_disk_bytes,
                @max(options.planned_disk_bytes, required_disk_bytes),
            );
            if (entry.intent.estimated_candidate_bytes != estimated_candidate_bytes or
                entry.intent.planned_disk_bytes != planned_disk_bytes)
            {
                try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .estimated_candidate_bytes = estimated_candidate_bytes,
                    .planned_disk_bytes = planned_disk_bytes,
                });
                entry.intent.estimated_candidate_bytes = estimated_candidate_bytes;
                entry.intent.planned_disk_bytes = planned_disk_bytes;
            }

            // A process may stop after pointer activation but before publishing a
            // clean checkpoint or deleting the intent. Never rebuild in that case:
            // prove that the recorded candidate is active and loaded, finish the
            // validation checkpoint, then atomically release intent and replay pin.
            if (entry.intent.phase == .activating or
                entry.intent.phase == .validating or
                entry.intent.phase == .cleanup)
            {
                if (entry.intent.candidate_relative_path == null) {
                    try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "activated_candidate_missing", true);
                    result.terminal = true;
                    return result;
                }
                if (try Self.reconcileActivatedIndexRepair(self, alloc, repair_id)) {
                    result.attempted = true;
                    var repair = types.ArtifactRepairResult{ .indexes_rebuilt = 1 };
                    try Self.finalizeCommittedIndexRepairOutcome(self, alloc, entry.intent.index_name, 0, &repair);
                    applyCommittedRepairOutcomeToAdvance(&result, repair);
                    result.repaired = repair.indexes_degraded_after == 0 and !repair.debt_remaining;
                    return result;
                }
                const rolled_back = try Self.rollbackUnavailableActivatedIndexRepair(self, alloc, repair_id);
                // `activating` was persisted before the pointer write. If the
                // candidate is not active, retain the ready generation and resume
                // convergence. This transition is authorized only after the durable
                // pointer was checked above; an ambiguous or failed rollback leaves
                // the intent in `activating` and fails closed.
                if (!rolled_back) {
                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .phase = .waiting_for_convergence,
                    });
                    entry.intent.phase = .waiting_for_convergence;
                }
            }

            // Capacity is checked and claimed through the shared ResourceManager
            // so every DB on the node participates in one admission domain. Existing
            // candidate bytes are already reflected in filesystem availability;
            // reserve only the remaining growth and cleanup headroom on resume.
            // Reconciliation above is intentionally admission-free because it
            // releases space and must remain possible on an already-full volume.
            var candidate_bytes: u64 = 0;
            if (entry.intent.candidate_relative_path) |candidate| {
                try index_repair_state.validateCandidateRelativePath(entry.intent.index_name, candidate);
                const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
                const candidate_root = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate[0..separator] });
                defer alloc.free(candidate_root);
                candidate_bytes = directoryUsageBytes(alloc, candidate_root) catch 0;
            }
            const remaining_disk_claim = planned_disk_bytes -| @min(planned_disk_bytes, candidate_bytes);
            const manager = self.core.index_manager.resource_manager orelse return error.ResourceManagerUnavailable;
            const first_capacity = repairCapacityObservation(effective_options) catch |err| {
                try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, @errorName(err), false);
                result.deferred = true;
                result.disk_wait = true;
                return result;
            };
            var disk_reservation = manager.reserveCapacity(
                alloc,
                repairCapacityDomain(effective_options),
                remaining_disk_claim,
                first_capacity,
                monotonicTimeNs(),
            ) catch |err| switch (err) {
                error.CapacityUnavailable, error.CapacityObservationStale => {
                    try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "disk_admission_unavailable", false);
                    result.deferred = true;
                    result.disk_wait = true;
                    var updated = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                    defer updated.deinit(alloc);
                    result.next_retry_at_ms = updated.intent.next_retry_at_ms;
                    return result;
                },
                else => return err,
            };
            defer disk_reservation.release();
            var capacity_guard = RepairCapacityGuard{
                .reservation = &disk_reservation,
                .db = self,
                .alloc = alloc,
                .repair_id = repair_id,
                .options = effective_options,
                .admitted_total_bytes = planned_disk_bytes,
                .headroom_bytes = cleanup_and_replay_headroom,
            };

            const attempt_count = entry.intent.attempt_count +| 1;
            try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .attempt_count = attempt_count,
                .next_retry_at_ms = 0,
            });
            entry.intent.attempt_count = attempt_count;
            result.attempted = true;
            var run_control = DurableIndexRepairRunControl{
                .db = self,
                .index_name = entry.intent.index_name,
                .upstream = options.cancel_check,
                .ownership = options.activation_check,
            };
            var run_options = effective_options;
            run_options.cancel_check = .{
                .ptr = &run_control,
                .is_requested = DurableIndexRepairRunControl.cancelled,
            };
            run_options.capacity_check = .{
                .ptr = &capacity_guard,
                .reconcile = RepairCapacityGuard.reconcile,
                .revalidate = RepairCapacityGuard.revalidate,
                .bind_candidate_root = RepairCapacityGuard.bindCandidateRoot,
            };
            run_options.executing_durable_index_repair = true;
            const artifact_kind = artifactRepairKindForIndexKind(entry.intent.kind) orelse {
                try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "unsupported_index_kind", true);
                result.terminal = true;
                return result;
            };
            const repair = Self.repairArtifactIssuesWithRequestOptions(self, alloc, .{
                .target = .index,
                .artifact_kind = artifact_kind,
                .index_name = entry.intent.index_name,
                .limit = 1,
                .force = true,
            }, run_options) catch |err| {
                // Fault-injection hooks model process loss, not an ordinary
                // returned repair failure. Let the test harness observe the
                // injected stop without persisting retry/backoff state that a
                // crashed process could never have written.
                if (comptime builtin.is_test) {
                    if (self.shadow_index_repair_hook != null and std.mem.startsWith(u8, @errorName(err), "Test")) {
                        return err;
                    }
                }
                if (err == error.Canceled and run_options.cancelled()) {
                    result.deferred = true;
                    return result;
                }
                var current_trigger = entry.intent.trigger;
                if (err == error.RepairSourceCoverageIncomplete) {
                    var current = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                    defer current.deinit(alloc);
                    current_trigger = current.intent.trigger;
                }
                if (err == error.RepairSourceCoverageIncomplete and
                    entry.intent.kind == .dense_vector and
                    self.enrichment_runtime != null and
                    (current_trigger == .operator_generation_rebuild or
                        current_trigger == .artifact_coverage_mismatch))
                {
                    // A shadow rebuild consumes stored embedding artifacts and
                    // cannot repair one missing below its snapshot floor. Re-arm
                    // the managed producer from primary documents first.
                    const artifact_name = try index_manager_mod.denseConfigArtifactNameAlloc(alloc, cfg_ptr.*);
                    defer alloc.free(artifact_name);
                    const queued_refs = self.reprocessGeneratedEnrichmentFromStoredDocs(alloc, artifact_name) catch |reprocess_err| blk: {
                        std.log.warn(
                            "dense repair source recovery deferred index={s} err={s}",
                            .{ entry.intent.index_name, @errorName(reprocess_err) },
                        );
                        break :blk 0;
                    };
                    if (queued_refs != 0) {
                        const recovery_target = self.core.nextDerivedSequence();
                        const runtime = self.enrichment_runtime.?;
                        if (runtime.isStarted()) {
                            runtime.notifySequence(recovery_target);
                            runtime.waitForApplied(recovery_target) catch |drain_err| {
                                std.log.warn(
                                    "dense repair source worker recovery deferred index={s} err={s}",
                                    .{ entry.intent.index_name, @errorName(drain_err) },
                                );
                            };
                        } else {
                            // Bounded startup/repair owners must not start a
                            // long-lived worker which can outlive and pin them.
                            runtime.catchUpUntil(recovery_target) catch |drain_err| {
                                std.log.warn(
                                    "dense repair source foreground recovery deferred index={s} err={s}",
                                    .{ entry.intent.index_name, @errorName(drain_err) },
                                );
                            };
                        }
                    }
                }
                const capacity_wait = err == error.CapacityUnavailable or err == error.CapacityObservationStale;
                const retryable_replacement_coverage =
                    err == error.RepairSourceCoverageIncomplete and
                    (current_trigger == .operator_generation_rebuild or
                        current_trigger == .artifact_coverage_mismatch);
                const terminal_failure =
                    !retryable_replacement_coverage and indexRepairFailureIsTerminal(err);
                try Self.recordIndexRepairAttemptFailure(
                    self,
                    alloc,
                    repair_id,
                    if (capacity_wait) "disk_admission_unavailable" else @errorName(err),
                    terminal_failure,
                );
                result.disk_wait = capacity_wait;
                if (terminal_failure) {
                    result.terminal = true;
                } else {
                    result.deferred = true;
                    var updated = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                    defer updated.deinit(alloc);
                    result.next_retry_at_ms = updated.intent.next_retry_at_ms;
                }
                return result;
            };
            result.documents_reprocessed = repair.reprocessed;
            applyCommittedRepairOutcomeToAdvance(&result, repair);

            // A committed replacement wins a cancellation observed after the
            // final pointer/checkpoint publication. Artifact debt discovered
            // during the rebuild remains in its own durable queue and must not
            // resurrect the already-completed generation intent.
            if (repair.indexes_rebuilt != 0 or repair.repaired != 0) {
                result.repaired = repair.indexes_degraded_after == 0 and !repair.debt_remaining;
                return result;
            }

            // Cooperative owner shutdown is not a failed repair attempt. Durable
            // snapshot/catch-up boundaries have already left the candidate in a
            // resumable phase, so preserve its retry schedule and let the next
            // owner continue immediately.
            if (run_options.cancelled()) {
                result.deferred = true;
                return result;
            }
            if (repair.in_progress != 0) {
                try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .failure_streak = 0,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
                result.busy = true;
                return result;
            }
            try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, "repair_attempt_incomplete", false);
            result.deferred = true;
            var updated = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
            defer updated.deinit(alloc);
            result.next_retry_at_ms = updated.intent.next_retry_at_ms;
            return result;
        }

        pub const StartupIndexRepairResult = struct {
            discovered: usize = 0,
            attempted: usize = 0,
            repaired: usize = 0,
            degraded: usize = 0,
            remaining: usize = 0,
            terminal: usize = 0,
            deferred: usize = 0,
            busy: usize = 0,
            disk_waits: usize = 0,
            documents_reprocessed: u64 = 0,
            next_retry_at_ms: u64 = 0,
        };

        pub fn repairRecoverableStartupIndexFailures(
            self: *DB,
            alloc: Allocator,
            limit: usize,
            options: types.ArtifactRepairRunOptions,
        ) !StartupIndexRepairResult {
            var result: StartupIndexRepairResult = .{};
            const discovery = try Self.discoverRecoverableStartupIndexFailures(self, alloc, limit);
            result.discovered = discovery.discovered;
            // Existing terminal intents are counted from the durable state below.
            // Discovery contributes only terminal load failures for which no intent
            // exists, avoiding double-counting checkpointed failures.
            result.terminal = discovery.terminal - discovery.existing_terminal;

            var state = try Self.loadIndexRepairState(self, alloc);
            defer state.deinit(alloc);
            const Candidate = struct {
                repair_id: u128,
                started_at_ms: u64,
            };
            var candidates = std.ArrayListUnmanaged(Candidate).empty;
            defer candidates.deinit(alloc);
            const now_ms = currentTimeNs() / std.time.ns_per_ms;
            for (state.entries.items) |entry| {
                if (entry.intent.phase == .terminal and retryableIndexRepairTerminalPhase(
                    entry.intent.last_error,
                    entry.intent.trigger,
                ) == null) {
                    result.terminal += 1;
                    continue;
                }
                if (entry.intent.automation == .paused or entry.intent.next_retry_at_ms > now_ms) {
                    result.deferred += 1;
                    if (entry.intent.next_retry_at_ms != 0 and
                        (result.next_retry_at_ms == 0 or entry.intent.next_retry_at_ms < result.next_retry_at_ms))
                    {
                        result.next_retry_at_ms = entry.intent.next_retry_at_ms;
                    }
                    continue;
                }
                try candidates.append(alloc, .{
                    .repair_id = entry.intent.repair_id,
                    .started_at_ms = entry.intent.started_at_ms,
                });
            }
            std.mem.sort(Candidate, candidates.items, {}, struct {
                fn lessThan(_: void, a: Candidate, b: Candidate) bool {
                    if (a.started_at_ms != b.started_at_ms) return a.started_at_ms < b.started_at_ms;
                    return a.repair_id < b.repair_id;
                }
            }.lessThan);
            for (candidates.items[0..@min(limit, candidates.items.len)]) |candidate| {
                const advanced = try Self.advanceIndexRepairIntent(self, alloc, candidate.repair_id, options);
                result.attempted += @intFromBool(advanced.attempted);
                result.repaired += @intFromBool(advanced.repaired);
                result.degraded += @intFromBool(advanced.indexes_degraded_after != 0);
                result.deferred += @intFromBool(advanced.deferred);
                result.disk_waits += @intFromBool(advanced.disk_wait);
                result.busy += @intFromBool(advanced.busy);
                result.documents_reprocessed += advanced.documents_reprocessed;
                if (advanced.next_retry_at_ms != 0 and
                    (result.next_retry_at_ms == 0 or advanced.next_retry_at_ms < result.next_retry_at_ms))
                {
                    result.next_retry_at_ms = advanced.next_retry_at_ms;
                }
            }
            var after = try Self.loadIndexRepairState(self, alloc);
            defer after.deinit(alloc);
            result.remaining = after.entries.items.len;
            return result;
        }

        fn reprocessChunkArtifactIssue(
            self: *DB,
            alloc: Allocator,
            issue: types.ArtifactRepairIssue,
        ) !bool {
            var cfg = (try self.getEnrichment(alloc, .chunk, issue.artifact_name)) orelse return false;
            defer cfg.deinit(alloc);

            const source_doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
            const value = try self.get(alloc, source_doc_key) orelse return error.NotFound;
            defer alloc.free(value);

            const writes = [_]types.BatchWrite{.{ .key = source_doc_key, .value = value }};
            const force_artifacts = [_][]const u8{issue.artifact_name};
            try DB.WritePathCallbacks.batch_internal(self, .{
                .writes = &writes,
                .sync_level = .full_index,
            }, null, .{
                .force_generated_artifact_names = &force_artifacts,
                .admission_prechecked = true,
            });
            return true;
        }

        pub fn artifactRepairCompletionKeyForIssueAlloc(
            self: *DB,
            alloc: Allocator,
            issue: types.ArtifactRepairIssue,
        ) ![]u8 {
            _ = self;
            const issue_id = try artifactRepairIssueIdAlloc(alloc, issue);
            defer alloc.free(issue_id);
            return try internal_keys.artifactRepairCompletionKeyAlloc(alloc, @tagName(issue.artifact_kind), issue_id);
        }

        fn loadArtifactRepairCompletionState(
            self: *DB,
            alloc: Allocator,
            completion_key: []const u8,
        ) !?ArtifactRepairCompletionState {
            return try loadArtifactRepairCompletionStateFromStore(alloc, self.core.store, completion_key);
        }

        pub const ArtifactRepairIssueRevision = struct {
            sequence: u64,
            reason: types.ArtifactRepairReason,
            generation_attempts: u64,
            generation_error_hash: u64,
            last_seen_ns: u64,

            pub fn capture(issue: types.ArtifactRepairIssue) @This() {
                return .{
                    .sequence = issue.sequence,
                    .reason = issue.reason,
                    .generation_attempts = issue.generation_attempts,
                    .generation_error_hash = std.hash.Wyhash.hash(0x6172_745f_7265_7676, issue.generation_error),
                    .last_seen_ns = issue.last_seen_ns,
                };
            }

            fn matches(self: @This(), issue: types.ArtifactRepairIssue) bool {
                return self.sequence == issue.sequence and
                    self.reason == issue.reason and
                    self.generation_attempts == issue.generation_attempts and
                    self.last_seen_ns == issue.last_seen_ns and
                    self.generation_error_hash == std.hash.Wyhash.hash(0x6172_745f_7265_7676, issue.generation_error);
            }
        };

        pub const ArtifactRepairCompletionDisposition = enum {
            completed,
            stale,
            coverage_incomplete,
        };

        pub fn artifactRepairCompletionSnapshot(
            self: *DB,
            alloc: Allocator,
            completion_key: []const u8,
        ) !ArtifactRepairCompletionState {
            lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
            defer self.async_context.artifact_repair_issue_mutex.unlock();
            return (try Self.loadArtifactRepairCompletionState(self, alloc, completion_key)) orelse .{};
        }

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

        fn saveArtifactRepairIssueWithSummary(
            self: *DB,
            alloc: Allocator,
            key: []const u8,
            issue: types.ArtifactRepairIssue,
            new_issue: bool,
            extra_writes: []const docstore_mod.KVPair,
            extra_deletes: []const []const u8,
        ) !void {
            const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, issue);
            defer alloc.free(kind_key);
            const encoded = try encodeArtifactRepairIssueValueAlloc(alloc, issue);
            defer alloc.free(encoded);
            if (!new_issue) {
                var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
                defer writes.deinit(alloc);
                try writes.append(alloc, .{ .key = key, .value = encoded });
                try writes.append(alloc, .{ .key = kind_key, .value = encoded });
                try writes.appendSlice(alloc, extra_writes);
                try self.core.store.putBatch(writes.items, extra_deletes);
                return;
            }

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            var borrowed_write_count: usize = 0;
            defer {
                for (writes.items[borrowed_write_count..]) |item| alloc.free(@constCast(item.value));
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
            borrowed_write_count = writes.items.len;
            try writes.appendSlice(alloc, extra_writes);
            borrowed_write_count = writes.items.len;
            try deletes.appendSlice(alloc, extra_deletes);
            try appendArtifactRepairSummaryDirty(self, alloc, &writes, &deletes, &owned_delete_keys);
            try self.core.store.putBatch(writes.items, deletes.items);
        }

        fn deleteArtifactRepairIssueWithSummary(
            self: *DB,
            alloc: Allocator,
            key: []const u8,
            existing_issue: types.ArtifactRepairIssue,
            extra_writes: []const docstore_mod.KVPair,
            extra_deletes: []const []const u8,
        ) !void {
            const kind_key = try artifactRepairIssueKindKeyForIssueAlloc(alloc, existing_issue);
            defer alloc.free(kind_key);

            var writes = std.ArrayListUnmanaged(docstore_mod.KVPair).empty;
            defer writes.deinit(alloc);
            try writes.appendSlice(alloc, extra_writes);
            const borrowed_write_count = writes.items.len;
            defer for (writes.items[borrowed_write_count..]) |item| alloc.free(@constCast(item.value));
            var deletes = std.ArrayListUnmanaged([]const u8).empty;
            defer deletes.deinit(alloc);
            var owned_delete_keys = std.ArrayListUnmanaged([]const u8).empty;
            defer {
                for (owned_delete_keys.items) |owned_key| alloc.free(@constCast(owned_key));
                owned_delete_keys.deinit(alloc);
            }

            try deletes.append(alloc, key);
            try deletes.append(alloc, kind_key);
            try deletes.appendSlice(alloc, extra_deletes);
            try Self.appendArtifactRepairSummaryDirty(self, alloc, &writes, &deletes, &owned_delete_keys);
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
            try Self.deleteArtifactRepairIssueWithSummary(self, alloc, key, existing_issue, &.{}, &.{});
        }

        pub fn clearArtifactRepairIssue(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
            try clearArtifactRepairIssueWithSummary(self, alloc, issue);
        }

        pub fn recordArtifactRepairIssue(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !void {
            lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
            defer self.async_context.artifact_repair_issue_mutex.unlock();
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);

            const now_ns = currentTimeNs();
            const existing = try loadArtifactRepairIssueByKey(self, alloc, key);
            const existing_was_pending = if (existing) |loaded| loaded.reason == .enrichment_failed else false;
            var stored = if (existing) |loaded| loaded else try cloneArtifactRepairIssueAlloc(alloc, issue);
            defer stored.deinit(alloc);

            stored.artifact_kind = issue.artifact_kind;
            stored.repairable = artifactRepairKindHasAutomatedReprocessor(issue.artifact_kind);
            stored.sequence = issue.sequence;
            stored.reason = issue.reason;
            if (stored.first_seen_ns == 0) stored.first_seen_ns = now_ns;
            stored.last_seen_ns = nextArtifactRepairIssueTimestamp(stored.last_seen_ns, now_ns);
            if (stored.artifact_key.len == 0 and issue.artifact_key.len > 0) stored.artifact_key = try alloc.dupe(u8, issue.artifact_key);
            if (stored.parent_doc_key.len == 0 and issue.parent_doc_key.len > 0) stored.parent_doc_key = try alloc.dupe(u8, issue.parent_doc_key);
            if (stored.unit_id.len == 0 and issue.unit_id.len > 0) stored.unit_id = try alloc.dupe(u8, issue.unit_id);
            if (stored.source_artifact_name.len == 0 and issue.source_artifact_name.len > 0) stored.source_artifact_name = try alloc.dupe(u8, issue.source_artifact_name);
            if (stored.unsupported_reason.len == 0 and !stored.repairable) stored.unsupported_reason = try alloc.dupe(u8, artifactRepairUnsupportedReason(stored.artifact_kind));

            const completion_key = try Self.artifactRepairCompletionKeyForIssueAlloc(self, alloc, stored);
            defer alloc.free(completion_key);
            var completion = (try Self.loadArtifactRepairCompletionState(self, alloc, completion_key)) orelse ArtifactRepairCompletionState{
                .pending_issues = @intFromBool(existing_was_pending),
            };
            if (existing_was_pending) completion.pending_issues = @max(completion.pending_issues, 1);
            if (stored.reason == .enrichment_failed) {
                completion.epoch +%= 1;
                if (completion.epoch == 0) completion.epoch = 1;
                completion.completed_sequence = 0;
                if (!existing_was_pending) completion.pending_issues +|= 1;
                var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
                encodeArtifactRepairCompletionState(&encoded_completion, completion);
                try Self.saveArtifactRepairIssueWithSummary(
                    self,
                    alloc,
                    key,
                    stored,
                    existing == null,
                    &.{.{ .key = completion_key, .value = &encoded_completion }},
                    &.{},
                );
            } else {
                try Self.saveArtifactRepairIssueWithSummary(self, alloc, key, stored, existing == null, &.{}, &.{completion_key});
            }
        }

        pub fn clearArtifactRepairIssueIfCurrent(
            self: *DB,
            alloc: Allocator,
            issue: types.ArtifactRepairIssue,
            revision: ArtifactRepairIssueRevision,
        ) !bool {
            lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
            defer self.async_context.artifact_repair_issue_mutex.unlock();

            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const existing = (try Self.loadArtifactRepairIssueByKey(self, alloc, key)) orelse return false;
            var current = existing;
            defer current.deinit(alloc);
            if (!revision.matches(current)) return false;

            try Self.deleteArtifactRepairIssueWithSummary(self, alloc, key, current, &.{}, &.{});
            return true;
        }

        fn enrichmentFailureCoverageMarkerKeyAlloc(
            self: *DB,
            alloc: Allocator,
            issue: types.ArtifactRepairIssue,
        ) !?[]u8 {
            if (issue.reason != .enrichment_failed or issue.index_name.len == 0) return null;
            if (self.core.index_manager.denseIndex(issue.index_name) == null and
                self.core.index_manager.sparseIndex(issue.index_name) == null)
            {
                return null;
            }
            const generation = self.core.index_manager.coverageGenerationForIndex(issue.index_name) orelse return null;
            return try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, issue.index_name, generation, issue.doc_key);
        }

        fn repairCoverageMarkerComplete(self: *DB, alloc: Allocator, marker_key: []const u8) !bool {
            const raw_outcome = self.core.store.get(alloc, marker_key) catch |err| switch (err) {
                error.NotFound => return false,
                else => return err,
            };
            defer alloc.free(raw_outcome);
            const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, raw_outcome) orelse
                return error.InvalidDerivedCoverageOutcome;
            return outcome == .produced or outcome == .skipped;
        }

        pub fn completeArtifactRepairIssueIfCurrent(
            self: *DB,
            alloc: Allocator,
            issue: types.ArtifactRepairIssue,
            revision: ArtifactRepairIssueRevision,
            completion_key: []const u8,
            expected_epoch: u64,
            require_completed_fence: bool,
        ) !ArtifactRepairCompletionDisposition {
            const coverage_marker_key = try Self.enrichmentFailureCoverageMarkerKeyAlloc(self, alloc, issue);
            defer if (coverage_marker_key) |key| alloc.free(key);

            lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
            defer self.async_context.artifact_repair_issue_mutex.unlock();

            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const existing = (try Self.loadArtifactRepairIssueByKey(self, alloc, key)) orelse return .stale;
            var current = existing;
            defer current.deinit(alloc);
            if (!revision.matches(current)) return .stale;

            // Validate the shared execution fence before interpreting this
            // consumer's coverage marker. A sibling failure can advance the epoch
            // without changing this issue revision; treating that stale snapshot
            // as coverage debt would pay for provider work that cannot commit.
            var completion = (try Self.loadArtifactRepairCompletionState(self, alloc, completion_key)) orelse ArtifactRepairCompletionState{};
            if (completion.epoch != expected_epoch) return .stale;
            if (require_completed_fence and completion.completed_sequence < revision.sequence) return .stale;

            // Coverage transitions and repair-ledger completion share this mutex.
            // Re-read the repaired generation while holding it so either repair
            // clears debt first (and stale terminal transitions are rejected) or a
            // terminal transition lands first (and debt remains queued). There is
            // no interleaving that can leave terminal coverage without repair debt.
            if (coverage_marker_key) |marker_key| {
                if (!try Self.repairCoverageMarkerComplete(self, alloc, marker_key)) return .coverage_incomplete;
            }

            completion.completed_sequence = @max(completion.completed_sequence, revision.sequence);
            completion.pending_issues -|= 1;

            var encoded: [artifact_repair_completion_state_len]u8 = undefined;
            const completion_writes: []const docstore_mod.KVPair = if (completion.pending_issues == 0)
                &.{}
            else blk: {
                encodeArtifactRepairCompletionState(&encoded, completion);
                break :blk &.{.{ .key = completion_key, .value = &encoded }};
            };
            const completion_deletes: []const []const u8 = if (completion.pending_issues == 0)
                &.{completion_key}
            else
                &.{};
            try Self.deleteArtifactRepairIssueWithSummary(
                self,
                alloc,
                key,
                current,
                completion_writes,
                completion_deletes,
            );
            return .completed;
        }

        fn saveArtifactRepairAttemptIfCurrent(
            self: *DB,
            alloc: Allocator,
            issue: types.ArtifactRepairIssue,
            revision: ArtifactRepairIssueRevision,
        ) !bool {
            lockAtomicWithBackoff(&self.async_context.artifact_repair_issue_mutex);
            defer self.async_context.artifact_repair_issue_mutex.unlock();
            const key = try artifactRepairIssueKeyForIssueAlloc(alloc, issue);
            defer alloc.free(key);
            const existing = (try Self.loadArtifactRepairIssueByKey(self, alloc, key)) orelse return false;
            var current = existing;
            defer current.deinit(alloc);
            if (!revision.matches(current)) return false;
            try Self.saveArtifactRepairIssueWithSummary(self, alloc, key, issue, false, &.{}, &.{});
            return true;
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

        const CommittedCoverageDisposition = enum {
            complete,
            degraded,
            indeterminate,
        };

        const IndexDerivedCoverageOwnership = union(enum) {
            external,
            managed: types.DerivedCoveragePolicy,
        };

        fn indexDerivedCoverageOwnership(alloc: Allocator, cfg: types.IndexConfig) ?IndexDerivedCoverageOwnership {
            var parsed = std.json.parseFromSlice(std.json.Value, alloc, cfg.config_json, .{}) catch return null;
            defer parsed.deinit();
            if (parsed.value != .object) return null;
            if (parsed.value.object.get("external")) |external| {
                if (external != .bool) return null;
                if (external.bool) return .external;
            }
            const configured = parsed.value.object.get("coverage_policy") orelse return .{ .managed = .strict };
            if (configured != .string) return null;
            if (std.mem.eql(u8, configured.string, "strict")) return .{ .managed = .strict };
            if (std.mem.eql(u8, configured.string, "partial")) return .{ .managed = .partial };
            if (std.mem.eql(u8, configured.string, "best_effort")) return .{ .managed = .best_effort };
            return null;
        }

        fn committedIndexCoverageDisposition(self: *DB, alloc: Allocator, index_name: []const u8) !CommittedCoverageDisposition {
            const cfg = self.core.index_manager.get(index_name) orelse return .indeterminate;
            if (cfg.kind != .dense_vector and cfg.kind != .sparse_vector) return .complete;
            const managed_direct_field = switch (cfg.kind) {
                .dense_vector => self.core.index_manager.denseIndexUsesManagedDirectField(index_name),
                .sparse_vector => self.core.index_manager.sparseIndexUsesManagedDirectField(index_name),
                else => unreachable,
            };
            // Direct-field vectors are rebuilt from primary rows and do not emit
            // enrichment outcome counters. Their replay/generation health is
            // already covered by indexGenerationRepairRequired above.
            if (managed_direct_field) return .complete;
            const ownership = indexDerivedCoverageOwnership(alloc, cfg.*) orelse return .indeterminate;
            const policy = switch (ownership) {
                .external => return .complete,
                .managed => |value| value,
            };
            const generation = self.core.index_manager.coverageGenerationForIndex(index_name) orelse return .indeterminate;
            const produced = try loadDerivedCoverageOutcomeCounterFromStore(alloc, self.core.store, index_name, generation, "produced");
            const skipped = try loadDerivedCoverageOutcomeCounterFromStore(alloc, self.core.store, index_name, generation, "skipped");
            const terminal_failed = try loadDerivedCoverageOutcomeCounterFromStore(alloc, self.core.store, index_name, generation, "terminal_failed");

            const source_total = try range_cardinality.loadOrCount(alloc, self.core.store, self.core.index_manager.byte_range);
            const applied_sequence = try DB.ArtifactRepairCallbacks.managed_index_applied_sequence(self, alloc, index_name);
            const target_sequence = try DB.ArtifactRepairCallbacks.projection_stats_target_sequence(self, alloc, cfg.*, applied_sequence);
            const replay_current = applied_sequence >= target_sequence;
            if (produced == null or skipped == null or terminal_failed == null) {
                // An empty source can legitimately predate creation of the compact
                // counter tuple. Any non-empty or still-replaying source remains
                // unknown and must not be reported repaired.
                return if (source_total == 0 and replay_current) .complete else .indeterminate;
            }
            const assessment = types.evaluateDerivedCoverageAssessment(
                policy,
                source_total,
                produced.?,
                skipped.?,
                terminal_failed.?,
                true,
                replay_current,
            );
            if (!assessment.health.counters_valid or !assessment.complete) {
                return if (assessment.degraded) .degraded else .indeterminate;
            }
            return if (assessment.degraded) .degraded else .complete;
        }

        /// Classify a generation after its replacement is durably committed.
        /// Both the initial response and crash-idempotent job replay enter here so
        /// a completion marker can never be mistaken for proof of healthy coverage.
        fn finalizeCommittedIndexRepairOutcome(
            self: *DB,
            alloc: Allocator,
            index_name: []const u8,
            observed_unresolved_artifacts: u64,
            result: *types.ArtifactRepairResult,
        ) !void {
            const repair_summary_ready = try Self.artifactRepairSummaryReady(self, alloc);
            const observed_or_summarized_unresolved = if (repair_summary_ready)
                @max(
                    observed_unresolved_artifacts,
                    (try Self.artifactRepairSummaryIndexSnapshot(self, alloc, index_name, true)).count,
                )
            else
                observed_unresolved_artifacts;
            const unresolved_artifacts = if (!repair_summary_ready and observed_or_summarized_unresolved == 0)
                @as(u64, 1)
            else
                observed_or_summarized_unresolved;
            result.unresolved += unresolved_artifacts;

            // A rebuilding summary cannot prove absence. Preserve debt without a
            // corpus scan on this latency-sensitive completion path; background
            // summary maintenance will publish the exact count.
            result.debt_remaining = result.debt_remaining or !repair_summary_ready or unresolved_artifacts != 0;

            const generation_repair_required = try Self.indexGenerationRepairRequired(self, alloc, index_name);
            const coverage = try Self.committedIndexCoverageDisposition(self, alloc, index_name);
            if (generation_repair_required or coverage != .complete) {
                result.unresolved += 1;
                result.debt_remaining = true;
                result.indexes_degraded_after = 1;
            }

            // "Rebuilt" describes completed work. "Repaired" is the stronger
            // operator promise that no known or indeterminate debt remains.
            result.indexes_degraded_after = @intFromBool(result.debt_remaining);
            if (!result.debt_remaining) {
                result.repaired += 1;
            }
        }

        fn denseCoverageRegressionRepairRequired(self: *DB, alloc: Allocator, index_name: []const u8) !bool {
            const entry = self.core.index_manager.denseIndex(index_name) orelse return false;
            if (!DB.DerivedAsyncCallbacks.dense_index_is_artifact_backed(entry)) return false;
            const expected = (try DB.loadDenseArtifactTargetCounter(alloc, self.core.store, index_name)) orelse
                return true;
            if (DB.DerivedAsyncCallbacks.dense_coverage_matches_target(entry.index.stats().active_count, expected)) return false;

            // Source counters advance atomically with primary writes, before the
            // asynchronous projection applies the corresponding replay sequence.
            // A cardinality difference is repair debt only after replay converges;
            // otherwise every healthy asynchronous write would be misclassified.
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
            if (checkpoint.status == .rebuilding) return false;
            const target_sequence = try DB.LifecycleCallbacks.probe_derived_replay_target_sequence(
                self,
                alloc,
                self.core.replaySource(),
                .{ .name = index_name, .kind = .dense_vector },
                checkpoint.applied_sequence,
            );
            return checkpoint.applied_sequence >= target_sequence;
        }

        pub fn indexGenerationRepairRequired(self: *DB, alloc: Allocator, index_name: []const u8) !bool {
            if (self.core.index_manager.loadFailure(index_name) != null) return true;
            // A durable generation intent is itself authoritative repair debt.
            // Managed catalog admission can intentionally leave the active
            // checkpoint clean while a shadow replacement is pending, so looking
            // only at checkpoint/artifact state would make the ordinary named
            // repair entry point skip work that has already been admitted and is
            // fail-closing service. Healthy indexes still require `force` because
            // they have neither an intent nor any of the conditions below.
            if (try Self.indexRepairIdForIndex(self, alloc, index_name) != null) return true;
            const checkpoint = try self.core.loadProjectionCheckpoint(alloc, index_name);
            switch (checkpoint.status) {
                .degraded, .repair_required => return true,
                .clean, .rebuilding => {},
            }
            if (try Self.denseCoverageRegressionRepairRequired(self, alloc, index_name)) return true;
            // Source artifacts have their own durable repair queue. Treating that
            // queue as generation debt recursively starts another shadow rebuild
            // after a repaired generation activates but before its source artifact
            // is regenerated. Managed catch-up repairs source debt, advances replay,
            // and rebuilds a generation only when one of the structural predicates
            // above still requires it.
            return false;
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
                if (!(try Self.indexGenerationRepairRequired(self, alloc, cfg.name))) continue;
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
            const page = try Self.listArtifactRepairIssuesPage(self, alloc, .{
                .artifact_kind = artifact_kind,
                .index_name = index_name,
                .limit = @intCast(@min(limit, std.math.maxInt(u32))),
            });
            if (page.next_cursor) |cursor| alloc.free(cursor);
            return page.issues;
        }

        pub fn listEmbeddingArtifactRepairIssues(self: *DB, alloc: Allocator, index_name: ?[]const u8, limit: usize) ![]types.EmbeddingArtifactRepairIssue {
            const generic = try Self.listArtifactRepairIssues(self, alloc, .embedding, index_name, limit);
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

        fn assetArtifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            const doc_key = if (issue.parent_doc_key.len > 0) issue.parent_doc_key else issue.doc_key;
            if (doc_key.len == 0 or issue.artifact_name.len == 0) return false;
            var cfg = (try self.getEnrichment(alloc, .asset, issue.artifact_name)) orelse return false;
            defer cfg.deinit(alloc);
            var producer_cfg = try asset_producer_mod.parseProducerConfig(alloc, cfg.producer_json);
            defer producer_cfg.deinit(alloc);

            const expected_key = try internal_keys.artifactNamedPrefixAlloc(alloc, doc_key, "asset", issue.artifact_name);
            defer alloc.free(expected_key);
            if (issue.artifact_key.len > 0) {
                const actual_key = try hexToBytesAlloc(alloc, issue.artifact_key);
                defer alloc.free(actual_key);
                if (!std.mem.eql(u8, actual_key, expected_key)) return false;
            }
            if (producer_cfg.type != .document_extraction) {
                const raw = self.core.store.get(alloc, expected_key) catch |err| switch (err) {
                    error.NotFound => return false,
                    else => return err,
                };
                defer alloc.free(raw);
                return true;
            }
            var manifest = (self.getDocumentArtifactManifest(alloc, doc_key, issue.artifact_name) catch |err| switch (err) {
                error.OutOfMemory => return err,
                else => return false,
            }) orelse return false;
            defer manifest.deinit(alloc);
            return true;
        }

        fn embeddingArtifactNowReadable(self: *DB, alloc: Allocator, issue: types.ArtifactRepairIssue) !bool {
            if (issue.reason == .enrichment_failed and issue.source_artifact_name.len > 0 and issue.chunk_id == null) {
                // Request-scoped chunk generation may legitimately produce zero,
                // one, or many physical artifacts. The generation-scoped coverage
                // outcome is the authoritative completion fence after full_index
                // repair; probing one synthetic document key can never prove this
                // set healthy.
                const generation = self.core.index_manager.coverageGenerationForIndex(issue.index_name) orelse return false;
                const marker_key = try internal_keys.derivedCoverageOutcomeKeyAlloc(alloc, issue.index_name, generation, issue.doc_key);
                defer alloc.free(marker_key);
                const raw_outcome = self.core.store.get(alloc, marker_key) catch |err| switch (err) {
                    error.NotFound => return false,
                    else => return err,
                };
                defer alloc.free(raw_outcome);
                const outcome = std.meta.stringToEnum(DerivedCoverageOutcome, raw_outcome) orelse
                    return error.InvalidDerivedCoverageOutcome;
                return outcome == .produced or outcome == .skipped;
            }

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
                .asset => try assetArtifactNowReadable(self, alloc, issue),
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
                .chunk => try reprocessChunkArtifactIssue(self, alloc, issue),
                .graph, .full_text, .algebraic => false,
            };
        }

        pub fn repairArtifactIssuesWithRequest(self: *DB, alloc: Allocator, req: types.ArtifactRepairRunRequest) !types.ArtifactRepairResult {
            return try Self.repairArtifactIssuesWithRequestOptions(self, alloc, req, .{});
        }

        pub fn repairArtifactIssuesWithRequestOptions(self: *DB, alloc: Allocator, req: types.ArtifactRepairRunRequest, options: types.ArtifactRepairRunOptions) !types.ArtifactRepairResult {
            try checkArtifactRepairCancelled(options);
            if (req.target == .index) return try repairIndexIssuesWithRequest(self, alloc, req, options);

            const page = try Self.listArtifactRepairIssuesPage(self, alloc, .{
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
                const issue_revision = ArtifactRepairIssueRevision.capture(issue.*);
                issue.attempts += 1;
                issue.last_seen_ns = currentTimeNs();

                if (!artifactRepairKindHasAutomatedReprocessor(issue.artifact_kind)) {
                    const unsupported_reason = artifactRepairUnsupportedReason(issue.artifact_kind);
                    issue.repairable = false;
                    if (issue.unsupported_reason.len == 0) issue.unsupported_reason = try alloc.dupe(u8, unsupported_reason);
                    try replaceRepairIssueLastError(alloc, issue, unsupported_reason);
                    _ = try Self.saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
                    result.unsupported += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    continue;
                }

                // Consumer-specific debt records for one physical artifact share
                // a single local execution lease and a durable completion fence.
                const completion_key = try Self.artifactRepairCompletionKeyForIssueAlloc(self, alloc, issue.*);
                defer alloc.free(completion_key);
                if (!(try beginIndexRepairLease(self, completion_key))) {
                    result.in_progress += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    continue;
                }
                defer endIndexRepairLease(self, completion_key);

                const completion_snapshot = if (issue.reason == .enrichment_failed)
                    try Self.artifactRepairCompletionSnapshot(self, alloc, completion_key)
                else
                    ArtifactRepairCompletionState{};
                if (issue.reason == .enrichment_failed and
                    completion_snapshot.completed_sequence >= issue.sequence)
                {
                    // Chunk regeneration is set-valued. A successful full_index
                    // completion fence, rather than one physical key, proves it.
                    if (issue.artifact_kind == .chunk or try artifactNowReadable(self, alloc, issue.*)) {
                        switch (try Self.completeArtifactRepairIssueIfCurrent(
                            self,
                            alloc,
                            issue.*,
                            issue_revision,
                            completion_key,
                            completion_snapshot.epoch,
                            true,
                        )) {
                            .completed => {
                                result.repaired += 1;
                                continue;
                            },
                            .stale => {
                                result.unresolved += 1;
                                result.debt_remaining = true;
                                continue;
                            },
                            .coverage_incomplete => {},
                        }
                    }
                }

                const reprocessed = reprocessArtifactIssue(self, alloc, issue.*) catch |err| switch (err) {
                    error.NotFound => {
                        try replaceRepairIssueLastError(alloc, issue, "source_document_missing");
                        _ = try Self.saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
                        result.missing_source_docs += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        continue;
                    },
                    else => {
                        try replaceRepairIssueLastError(alloc, issue, @errorName(err));
                        _ = try Self.saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
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
                    _ = try Self.saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
                    result.failed += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    continue;
                }
                result.reprocessed += 1;
                if (issue.artifact_kind == .chunk or try artifactNowReadable(self, alloc, issue.*)) {
                    if (issue.reason == .enrichment_failed) {
                        switch (try Self.completeArtifactRepairIssueIfCurrent(
                            self,
                            alloc,
                            issue.*,
                            issue_revision,
                            completion_key,
                            completion_snapshot.epoch,
                            false,
                        )) {
                            .completed => result.repaired += 1,
                            .stale, .coverage_incomplete => {
                                result.unresolved += 1;
                                result.debt_remaining = true;
                            },
                        }
                    } else if (try Self.clearArtifactRepairIssueIfCurrent(self, alloc, issue.*, issue_revision)) {
                        result.repaired += 1;
                    } else {
                        result.unresolved += 1;
                        result.debt_remaining = true;
                    }
                } else {
                    try replaceRepairIssueLastError(alloc, issue, "artifact_still_unreadable");
                    _ = try Self.saveArtifactRepairAttemptIfCurrent(self, alloc, issue.*, issue_revision);
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

        const RepairShadowCleanupWork = struct {
            manager: *index_manager_mod.IndexManager,

            fn run(ptr: *anyopaque) anyerror!void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                while (true) {
                    work.manager.cleanupInactiveRepairShadowRoots();
                    if (work.manager.repair_cleanup_state.cmpxchgStrong(1, 0, .acq_rel, .acquire) == null) return;
                    if (work.manager.repair_cleanup_state.cmpxchgStrong(2, 1, .acq_rel, .acquire) == null) continue;
                }
            }

            fn deinit(ptr: *anyopaque) void {
                const work: *@This() = @ptrCast(@alignCast(ptr));
                std.heap.page_allocator.destroy(work);
            }
        };

        fn scheduleInactiveRepairShadowCleanup(self: *DB) void {
            if (self.core.index_manager.repair_cleanup_state.cmpxchgStrong(0, 1, .acq_rel, .acquire) != null) {
                _ = self.core.index_manager.repair_cleanup_state.cmpxchgStrong(1, 2, .acq_rel, .acquire);
                return;
            }
            const work = std.heap.page_allocator.create(RepairShadowCleanupWork) catch {
                self.core.index_manager.repair_cleanup_state.store(0, .release);
                return;
            };
            work.* = .{ .manager = self.core.index_manager };
            self.backend_runtime.durable_jobs.submit(.{
                .owner_id = self.repair_cleanup_owner_id,
                .class = .cleanup,
                .ptr = work,
                .run = RepairShadowCleanupWork.run,
                .deinit = RepairShadowCleanupWork.deinit,
            }) catch |err| {
                std.heap.page_allocator.destroy(work);
                self.core.index_manager.repair_cleanup_state.store(0, .release);
                std.log.warn("deferred index-generation cleanup was not scheduled err={s}", .{@errorName(err)});
            };
        }

        fn beginIndexRepairLease(self: *DB, index_name: []const u8) !bool {
            db_internal.lockAtomicWithBackoff(&self.index_repair_mutex);
            defer self.index_repair_mutex.unlock();
            if (self.active_index_repairs.contains(index_name)) return false;
            const owned = try self.alloc.dupe(u8, index_name);
            errdefer self.alloc.free(owned);
            try self.active_index_repairs.put(self.alloc, owned, false);
            return true;
        }

        fn activeIndexRepairCancellationRequested(self: *DB, index_name: []const u8) bool {
            db_internal.lockAtomicWithBackoff(&self.index_repair_mutex);
            defer self.index_repair_mutex.unlock();
            return self.active_index_repairs.get(index_name) orelse false;
        }

        fn requestActiveIndexRepairCancellation(self: *DB, index_name: []const u8) bool {
            db_internal.lockAtomicWithBackoff(&self.index_repair_mutex);
            defer self.index_repair_mutex.unlock();
            const requested = self.active_index_repairs.getPtr(index_name) orelse return false;
            requested.* = true;
            return true;
        }

        pub fn endIndexRepairLease(self: *DB, index_name: []const u8) void {
            db_internal.lockAtomicWithBackoff(&self.index_repair_mutex);
            defer self.index_repair_mutex.unlock();
            if (self.active_index_repairs.fetchRemove(index_name)) |removed| {
                self.alloc.free(@constCast(removed.key));
            }
        }

        fn lockApplyUntil(self: *DB, deadline_ns: u64) bool {
            var spins: usize = 0;
            while (!self.core.tryLockApplyExclusive()) : (spins += 1) {
                if (monotonicTimeNs() >= deadline_ns) return false;
                if (spins < 64) {
                    std.atomic.spinLoopHint();
                } else {
                    std.Thread.yield() catch {};
                }
            }
            return true;
        }

        const ShadowIndexReplacementResult = struct {
            reprocessed: u64 = 0,
            applied_sequence: u64 = 0,
            unresolved_artifacts: u64 = 0,
            yielded: bool = false,
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
                .config_hash = if (shadow_manager.get(index_ref.name)) |value| types.indexConfigHash(value.*) else 0,
            };
            const updates = [_]apply_state.AppliedSequenceUpdate{update};
            try DB.DerivedAsyncCallbacks.save_dense_projection_metadata_for_applied_sequence_updates(shadow_manager, &updates);
            try DB.DerivedAsyncCallbacks.checkpoint_managed_projection_effects_for_applied_sequence_updates(shadow_manager, &updates);
            try apply_state.saveAppliedSequenceUpdateWithCheckpoint(
                alloc,
                shadow_manager.checkpointIo(),
                self.core.store,
                shadow_checkpoint_path,
                update,
            );
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

        pub fn recordArtifactRepairIssueContextDetailed(
            ctx: *const AsyncContext,
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
            generation_attempts: u64,
            generation_error: []const u8,
        ) !void {
            const kind_name = @tagName(artifact_kind);
            const artifact_key_hex = if (artifact_key.len > 0)
                try bytesToHexAlloc(ctx.alloc, artifact_key)
            else
                try ctx.alloc.dupe(u8, "");
            defer ctx.alloc.free(artifact_key_hex);
            const issue_id = try artifactRepairIssueIdAlloc(ctx.alloc, .{
                .artifact_kind = artifact_kind,
                .index_name = index_name,
                .doc_key = doc_key,
                .parent_doc_key = parent_doc_key,
                .unit_id = unit_id,
                .source_artifact_name = source_artifact_name,
                .artifact_name = artifact_name,
                .artifact_key = artifact_key_hex,
                .chunk_id = chunk_id,
            });
            defer ctx.alloc.free(issue_id);
            const issue_key = try internal_keys.artifactRepairIssueKeyAlloc(ctx.alloc, index_name, kind_name, issue_id);
            defer ctx.alloc.free(issue_key);

            const mutable_ctx = @constCast(ctx);
            lockAtomicWithBackoff(&mutable_ctx.artifact_repair_issue_mutex);
            defer mutable_ctx.artifact_repair_issue_mutex.unlock();

            const now_ns = currentTimeNs();
            const existing = try loadArtifactRepairIssueFromStoreByKey(ctx.alloc, ctx.store, issue_key);
            const existing_was_pending = if (existing) |loaded| loaded.reason == .enrichment_failed else false;
            var issue = if (existing) |loaded|
                loaded
            else
                types.ArtifactRepairIssue{
                    .artifact_kind = artifact_kind,
                    .index_name = try ctx.alloc.dupe(u8, index_name),
                    .doc_key = try ctx.alloc.dupe(u8, doc_key),
                    .parent_doc_key = try ctx.alloc.dupe(u8, parent_doc_key),
                    .unit_id = try ctx.alloc.dupe(u8, unit_id),
                    .source_artifact_name = try ctx.alloc.dupe(u8, source_artifact_name),
                    .artifact_name = try ctx.alloc.dupe(u8, artifact_name),
                    .artifact_key = if (artifact_key_hex.len > 0) try ctx.alloc.dupe(u8, artifact_key_hex) else "",
                    .chunk_id = chunk_id,
                    .repairable = artifactRepairKindHasAutomatedReprocessor(artifact_kind),
                    .unsupported_reason = try ctx.alloc.dupe(u8, artifactRepairUnsupportedReason(artifact_kind)),
                    .first_seen_ns = now_ns,
                };
            defer issue.deinit(ctx.alloc);

            const merged_generation_attempts = mergeGenerationFailureAttempts(
                issue.reason,
                issue.sequence,
                issue.generation_error,
                issue.generation_attempts,
                reason,
                sequence,
                generation_error,
                generation_attempts,
            );
            issue.artifact_kind = artifact_kind;
            issue.sequence = sequence;
            issue.reason = reason;
            issue.generation_attempts = merged_generation_attempts;
            issue.chunk_id = chunk_id;
            issue.repairable = artifactRepairKindHasAutomatedReprocessor(artifact_kind);
            issue.last_seen_ns = nextArtifactRepairIssueTimestamp(issue.last_seen_ns, now_ns);
            if (issue.artifact_key.len == 0 and artifact_key_hex.len > 0) {
                issue.artifact_key = try ctx.alloc.dupe(u8, artifact_key_hex);
            }
            if (issue.parent_doc_key.len == 0 and parent_doc_key.len > 0) {
                issue.parent_doc_key = try ctx.alloc.dupe(u8, parent_doc_key);
            }
            if (issue.unit_id.len == 0 and unit_id.len > 0) {
                issue.unit_id = try ctx.alloc.dupe(u8, unit_id);
            }
            if (issue.source_artifact_name.len == 0 and source_artifact_name.len > 0) {
                issue.source_artifact_name = try ctx.alloc.dupe(u8, source_artifact_name);
            }
            if (issue.unsupported_reason.len == 0 and !issue.repairable) {
                issue.unsupported_reason = try ctx.alloc.dupe(u8, artifactRepairUnsupportedReason(artifact_kind));
            }
            if (!std.mem.eql(u8, issue.generation_error, generation_error)) {
                const owned_generation_error = if (generation_error.len > 0)
                    try ctx.alloc.dupe(u8, generation_error)
                else
                    "";
                if (issue.generation_error.len > 0) ctx.alloc.free(@constCast(issue.generation_error));
                issue.generation_error = owned_generation_error;
            }

            const completion_key = try internal_keys.artifactRepairCompletionKeyAlloc(ctx.alloc, kind_name, issue_id);
            defer ctx.alloc.free(completion_key);
            if (reason == .enrichment_failed) {
                var completion = (try loadArtifactRepairCompletionStateFromStore(ctx.alloc, ctx.store, completion_key)) orelse ArtifactRepairCompletionState{
                    .pending_issues = @intFromBool(existing_was_pending),
                };
                if (existing_was_pending) completion.pending_issues = @max(completion.pending_issues, 1);
                completion.epoch +%= 1;
                if (completion.epoch == 0) completion.epoch = 1;
                completion.completed_sequence = 0;
                if (!existing_was_pending) completion.pending_issues +|= 1;
                var encoded_completion: [artifact_repair_completion_state_len]u8 = undefined;
                encodeArtifactRepairCompletionState(&encoded_completion, completion);
                try saveArtifactRepairIssueToStoreWithSummary(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    issue,
                    existing == null,
                    &.{.{ .key = completion_key, .value = &encoded_completion }},
                    &.{},
                );
            } else {
                // A non-enrichment diagnosis supersedes the shared success fence. It
                // must be invalidated in the same metadata transaction as the issue so
                // neither crashes nor concurrent repair can leave a stale marker.
                try saveArtifactRepairIssueToStoreWithSummary(
                    ctx.alloc,
                    ctx.store,
                    issue_key,
                    issue,
                    existing == null,
                    &.{},
                    &.{completion_key},
                );
            }
            if (ctx.repair_issue_counter) |counter| _ = counter.fetchAdd(1, .monotonic);
        }

        fn mergeGenerationFailureAttempts(
            previous_reason: types.ArtifactRepairReason,
            previous_sequence: u64,
            previous_error: []const u8,
            previous_attempts: u64,
            reason: types.ArtifactRepairReason,
            sequence: u64,
            generation_error: []const u8,
            generation_attempts: u64,
        ) u64 {
            const same_window = previous_reason == .enrichment_failed and
                reason == .enrichment_failed and
                previous_sequence == sequence and
                std.mem.eql(u8, previous_error, generation_error);
            return if (same_window) @max(previous_attempts, generation_attempts) else generation_attempts;
        }

        test "artifact repair enrichment generation attempts are exact per failure window" {
            try std.testing.expectEqual(@as(u64, 6), mergeGenerationFailureAttempts(
                .enrichment_failed,
                41,
                "EmbedRateLimited",
                6,
                .enrichment_failed,
                41,
                "EmbedRateLimited",
                4,
            ));
            try std.testing.expectEqual(@as(u64, 1), mergeGenerationFailureAttempts(
                .enrichment_failed,
                41,
                "EmbedRateLimited",
                6,
                .enrichment_failed,
                42,
                "EmbedRateLimited",
                1,
            ));
            try std.testing.expectEqual(@as(u64, 2), mergeGenerationFailureAttempts(
                .enrichment_failed,
                42,
                "EmbedRateLimited",
                6,
                .enrichment_failed,
                42,
                "ProviderUnavailable",
                2,
            ));
        }

        fn recordArtifactRepairIssueContext(
            ctx: *const AsyncContext,
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
            try Self.recordArtifactRepairIssueContextDetailed(
                ctx,
                artifact_kind,
                index_name,
                doc_key,
                parent_doc_key,
                unit_id,
                source_artifact_name,
                artifact_name,
                artifact_key,
                chunk_id,
                sequence,
                reason,
                0,
                "",
            );
        }

        fn applySplitGraphArtifactsForIndexStreamingContext(
            ctx: *db_internal.AsyncContext(DB),
            index_name: []const u8,
            batch_size: usize,
        ) !usize {
            _ = ctx.index_manager.graphIndex(index_name) orelse return error.IndexNotFound;
            const store_lower = try internal_keys.documentRangeLowerAlloc(ctx.alloc, "");
            defer ctx.alloc.free(store_lower);
            const effective_batch_size = @max(batch_size, 1);

            const ScanState = struct {
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
                    try db_internal.checkAsyncRepairCapacityBoundary(state.ctx);
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
                            try recordArtifactRepairIssueContext(
                                state.ctx,
                                .graph,
                                parsed.index_name,
                                parsed.doc_key,
                                "",
                                "",
                                "",
                                artifact_name,
                                key,
                                null,
                                state.ctx.repair_sequence,
                                .corrupt_artifact,
                            );
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
                .ctx = ctx,
                .index_name = index_name,
                .batch_size = effective_batch_size,
            };
            defer state.deinit();

            try scanStoreForRebuildContext(ctx, store_lower, "", .{}, &state, ScanState.scanEntry);
            try state.flush();
            return state.mutation_count;
        }

        const ShadowCatchUpResult = struct {
            applied_sequence: u64,
            yielded: bool = false,
        };

        fn catchUpShadowReplacementUntil(
            self: *DB,
            alloc: Allocator,
            shadow_manager: *index_manager_mod.IndexManager,
            shadow_checkpoint_path: []const u8,
            index_ref: index_manager_mod.ManagedIndexRef,
            target_sequence: u64,
            options: types.ArtifactRepairRunOptions,
            deadline_ns: ?u64,
            observed_ns_per_sequence: u64,
        ) !ShadowCatchUpResult {
            if (target_sequence == 0) return .{ .applied_sequence = 0 };
            // Cooperative yielding is only valid before activation fencing. Final
            // replay has its own hard deadline and must either reach that target or
            // abort activation while the write/search barriers remain held.
            const cooperative = deadline_ns == null and options.yield_check != null;
            var batch_ctx = self.batchContext();
            batch_ctx.index_manager = shadow_manager;
            batch_ctx.applied_sequence_checkpoint_path = shadow_checkpoint_path;
            batch_ctx.async_context = null;
            batch_ctx.dense_bulk_session_scope = .external;

            var applied = try apply_state.loadAppliedSequenceWithCheckpoint(
                alloc,
                shadow_manager.checkpointIo(),
                self.core.store,
                shadow_checkpoint_path,
                index_ref.name,
            );
            while (applied < target_sequence) {
                try checkArtifactRepairCancelled(options);
                if (deadline_ns) |deadline| {
                    if (monotonicTimeNs() >= deadline) return error.ShadowIndexCatchUpIncomplete;
                }
                const remaining_sequences = target_sequence -| applied;
                var max_records_per_window = if (comptime builtin.is_test)
                    test_index_repair_catch_up_max_records_per_window orelse derived_worker.catch_up_max_records_per_window_default
                else
                    derived_worker.catch_up_max_records_per_window_default;
                var max_items_per_window: usize = 0;
                if (deadline_ns) |deadline| {
                    const now = monotonicTimeNs();
                    if (now >= deadline) return error.CatchUpDeadlineExceeded;
                    const per_sequence_ns = @max(@as(u64, 1), observed_ns_per_sequence);
                    const affordable_sequences = (deadline - now) / per_sequence_ns;
                    if (affordable_sequences == 0) return error.CatchUpDeadlineExceeded;
                    const bounded_sequences = @min(remaining_sequences, affordable_sequences);
                    max_records_per_window = @max(@as(usize, 1), @min(
                        derived_worker.catch_up_max_records_per_window_default,
                        std.math.cast(usize, bounded_sequences) orelse std.math.maxInt(usize),
                    ));
                    max_items_per_window = max_records_per_window;
                }
                const catch_up_stats = try DB.DerivedAsyncCallbacks.catch_up_managed_index_with_batch_context_options(
                    &batch_ctx,
                    index_ref,
                    applied,
                    target_sequence,
                    max_records_per_window,
                    max_items_per_window,
                    if (cooperative) 1 else 0,
                    deadline_ns,
                );
                const advanced = catch_up_stats.appliedSequenceAdvance(applied) orelse blk: {
                    if (catch_up_stats.shouldTryTargetAdvance(applied, target_sequence) and
                        try DB.DerivedAsyncCallbacks.can_advance_derived_replay_target_for_batch_context(&batch_ctx, index_ref, applied, target_sequence))
                    {
                        break :blk target_sequence;
                    }
                    break :blk applied;
                };
                if (advanced <= applied) return .{ .applied_sequence = applied };
                try Self.saveShadowReplacementAppliedSequence(self, alloc, shadow_manager, shadow_checkpoint_path, index_ref, advanced);
                applied = advanced;
                if (cooperative and applied < target_sequence and options.yield_check.?.requested()) {
                    return .{ .applied_sequence = applied, .yielded = true };
                }
                if (deadline_ns) |deadline| {
                    if (applied < target_sequence and monotonicTimeNs() >= deadline) {
                        return error.ShadowIndexCatchUpIncomplete;
                    }
                }
            }
            try checkArtifactRepairCancelled(options);
            return .{ .applied_sequence = applied };
        }

        pub fn rebuildIndexWithShadowReplacement(
            self: *DB,
            alloc: Allocator,
            cfg: types.IndexConfig,
            options: types.ArtifactRepairRunOptions,
            durable_repair_id: ?u128,
        ) !ShadowIndexReplacementResult {
            try checkArtifactRepairCancelled(options);
            const working_set_plan = if (self.core.index_manager.resource_manager) |manager|
                try repairWorkingSetPlan(alloc, manager, cfg)
            else
                RepairWorkingSetPlan{ .reservation_bytes = 0 };
            var repair_admission: ?resource_manager_mod.Reservation = if (self.core.index_manager.resource_manager) |manager|
                manager.reserve(.dense_repair_working_set, working_set_plan.reservation_bytes) catch
                    return error.RepairResourceUnavailable
            else
                null;
            defer if (repair_admission) |*reservation| reservation.release();

            // A forced dense rebuild is initially fail-closed. Only the admitted
            // background worker may prove the current generation safe enough to
            // remain online during shadow construction. Automatic anomaly
            // classifications stay fail-closed and reuse the same reconstruction.
            if (durable_repair_id) |repair_id| {
                var durable_entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                defer durable_entry.deinit(alloc);
                if (durable_entry.intent.trigger == .operator_generation_validation) {
                    switch (try Self.validateOperatorDenseGenerationForOnlineRebuild(self, alloc, cfg, options)) {
                        .online_safe => try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                            .trigger = .operator_generation_rebuild,
                            .replace_last_error = true,
                        }),
                        .replay_pending => return error.OperatorGenerationValidationPending,
                        .fail_closed => |classification| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                            .trigger = classification.trigger,
                            .last_error = classification.last_error,
                            .replace_last_error = true,
                        }),
                    }
                }
            }
            var resume_candidate = false;
            var resume_building = false;
            var resume_requires_ready_manifest = false;
            var persisted_build_floor_sequence: u64 = 0;
            var persisted_build_reprocessed: u64 = 0;
            var build_resume_key: ?[]u8 = null;
            defer if (build_resume_key) |key| alloc.free(key);
            const generation_id = durable_repair_id orelse try index_repair_state.newRepairId(alloc);
            var shadow_base: []u8 = undefined;
            if (durable_repair_id) |repair_id| {
                var entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                defer entry.deinit(alloc);
                const resumable_phase = switch (entry.intent.phase) {
                    .building => entry.intent.build_resume_key != null and cfg.kind == .dense_vector,
                    .catching_up, .ready, .waiting_for_convergence => true,
                    else => false,
                };
                if (resumable_phase and entry.intent.candidate_relative_path != null) {
                    const candidate = entry.intent.candidate_relative_path.?;
                    try index_repair_state.validateCandidateRelativePath(cfg.name, candidate);
                    const separator = std.mem.indexOfScalar(u8, candidate, '/') orelse return error.InvalidRepairCandidatePath;
                    shadow_base = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ self.core.path, candidate[0..separator] });
                    resume_candidate = true;
                    resume_building = entry.intent.phase == .building;
                    resume_requires_ready_manifest = entry.intent.phase == .ready or entry.intent.phase == .waiting_for_convergence;
                    persisted_build_floor_sequence = entry.intent.build_floor_sequence;
                    persisted_build_reprocessed = entry.intent.build_reprocessed;
                    build_resume_key = if (entry.intent.build_resume_key) |key| try alloc.dupe(u8, key) else null;
                } else {
                    try Self.discardInactiveIndexRepairCandidate(self, alloc, repair_id);
                    shadow_base = try createUniqueRepairShadowBase(alloc, self.core.path);
                }
            } else {
                shadow_base = try createUniqueRepairShadowBase(alloc, self.core.path);
            }
            if (!resume_candidate) try index_manager_mod.IndexManager.writeRepairShadowInProgressMarker(alloc, shadow_base);
            var shadow_installed = false;
            var candidate_reopenable = resume_candidate;
            defer {
                var io_impl = threadedIo();
                defer io_impl.deinit();
                if (!shadow_installed and !candidate_reopenable) {
                    index_manager_mod.IndexManager.clearRepairShadowInProgressMarker(alloc, shadow_base) catch {};
                    std.Io.Dir.cwd().deleteTree(io_impl.io(), shadow_base) catch {};
                }
                alloc.free(shadow_base);
            }
            const shadow_indexes_path = try std.fmt.allocPrint(alloc, "{s}/indexes", .{shadow_base});
            defer alloc.free(shadow_indexes_path);
            if (!resume_candidate) try ensureDirPath(shadow_indexes_path);

            const shadow_index_path = try std.fmt.allocPrint(alloc, "{s}/{s}", .{ shadow_indexes_path, cfg.name });
            defer alloc.free(shadow_index_path);
            const shadow_checkpoint_path = try std.fmt.allocPrint(alloc, "{s}/applied-sequences", .{shadow_base});
            defer alloc.free(shadow_checkpoint_path);
            if (options.capacity_check) |check| try check.bindCandidateRoot(shadow_base);
            if (resume_requires_ready_manifest) {
                _ = index_generation_manifest.validateReady(
                    alloc,
                    shadow_index_path,
                    generation_id,
                    cfg.name,
                    types.indexConfigHash(cfg),
                ) catch {
                    candidate_reopenable = false;
                    if (durable_repair_id) |repair_id| Self.discardInactiveIndexRepairCandidate(self, alloc, repair_id) catch {};
                    return error.RepairCandidateResumeInvalid;
                };
            }
            if (!resume_candidate) {
                if (durable_repair_id) |repair_id| {
                    const candidate_relative_path = try std.fmt.allocPrint(
                        alloc,
                        "{s}/indexes/{s}",
                        .{ std.fs.path.basename(shadow_base), cfg.name },
                    );
                    defer alloc.free(candidate_relative_path);
                    try index_repair_state.validateCandidateRelativePath(cfg.name, candidate_relative_path);
                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .phase = .building,
                        .candidate_relative_path = candidate_relative_path,
                        .replace_candidate_path = true,
                    });
                }
            }

            var shadow_manager = try index_manager_mod.IndexManager.initWithOptions(
                alloc,
                shadow_base,
                self.index_backends,
            );
            shadow_manager.setIo(self.backend_runtime.io());
            shadow_manager.setAppliedSequenceCheckpointPath(shadow_checkpoint_path);
            shadow_manager.registerReplacementIndex(self.core.store, cfg) catch |err| {
                shadow_manager.deinit();
                if (resume_candidate) {
                    candidate_reopenable = false;
                    if (durable_repair_id) |repair_id| Self.discardInactiveIndexRepairCandidate(self, alloc, repair_id) catch {};
                    return error.RepairCandidateResumeInvalid;
                }
                return err;
            };
            var shadow_manager_open = true;
            defer if (shadow_manager_open) shadow_manager.deinit();

            // A scan cursor is useful only when there is a durable intent to own
            // it. Non-durable/internal one-shot rebuild callers retain the existing
            // complete bulk behavior even if they forward a scheduler policy by
            // mistake; otherwise they could repeatedly discard partial shadows.
            var effective_options = options;
            if (durable_repair_id == null) effective_options.yield_check = null;
            const cooperative_dense_build = cfg.kind == .dense_vector and effective_options.yield_check != null;
            var repair_issue_counter: std.atomic.Value(u64) = .init(0);
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
                .repair_options = effective_options,
                .repair_issue_counter = &repair_issue_counter,
            };
            defer shadow_ctx.deinit(alloc);

            var build_floor_sequence: u64 = if (resume_building)
                persisted_build_floor_sequence
            else if (resume_candidate)
                try apply_state.loadAppliedSequenceWithCheckpoint(
                    alloc,
                    shadow_manager.checkpointIo(),
                    self.core.store,
                    shadow_checkpoint_path,
                    cfg.name,
                )
            else
                0;
            var expected_snapshot_coverage: ?u64 = null;
            const completed_snapshot_build = !resume_candidate or resume_building;
            const rebuilt: u64 = if (resume_candidate and !resume_building) 0 else switch (cfg.kind) {
                .dense_vector, .sparse_vector, .graph, .full_text => rebuilt_blk: {
                    try checkArtifactRepairCancelled(options);
                    var pinned_snapshot: ?PinnedIndexRepairSnapshot = if (!resume_building) if (durable_repair_id) |repair_id|
                        try Self.beginPinnedIndexRepairSnapshot(self, alloc, repair_id)
                    else
                        null else null;
                    defer if (pinned_snapshot) |*pinned| pinned.deinit();
                    var ordinary_snapshot: ?docstore_mod.DocStore.Txn = if (pinned_snapshot == null)
                        try self.core.store.beginReadTxn()
                    else
                        null;
                    defer if (ordinary_snapshot) |*ordinary| ordinary.abort();
                    const snapshot_txn = if (pinned_snapshot) |*pinned|
                        &pinned.txn
                    else
                        &ordinary_snapshot.?;
                    build_floor_sequence = if (resume_building)
                        persisted_build_floor_sequence
                    else if (pinned_snapshot) |pinned|
                        pinned.build_floor_sequence
                    else
                        try self.core.store.lastReplaySequenceFromTxn(snapshot_txn, 0);
                    shadow_ctx.repair_sequence = build_floor_sequence;
                    shadow_ctx.snapshot_read_txn = snapshot_txn;
                    defer shadow_ctx.snapshot_read_txn = null;
                    const count: u64 = switch (cfg.kind) {
                        .dense_vector => count_blk: {
                            var slice = try DB.DerivedAsyncCallbacks.rebuild_dense_index_for_target_coverage_slice_context(
                                &shadow_ctx,
                                cfg.name,
                                working_set_plan.dense_rebuild_batch_items,
                                build_resume_key,
                            );
                            defer slice.deinit(alloc);
                            const count: u64 = persisted_build_reprocessed +| @as(u64, @intCast(slice.rebuilt));
                            if (!slice.complete()) {
                                if (options.capacity_check) |check| {
                                    try check.current(try directoryUsageBytes(alloc, shadow_base));
                                }
                                if (durable_repair_id) |repair_id| {
                                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                                        .phase = .building,
                                        .build_resume_key = slice.resume_key.?,
                                        .replace_build_resume_key = true,
                                        .build_reprocessed = count,
                                        .failure_streak = 0,
                                        .next_retry_at_ms = 0,
                                        .replace_last_error = true,
                                    });
                                }
                                candidate_reopenable = durable_repair_id != null;
                                return .{ .reprocessed = @intCast(slice.rebuilt), .yielded = true };
                            }
                            if (cooperative_dense_build) {
                                // A count observed between slices is not a stable
                                // coverage assertion. Require the durable,
                                // generation-scoped outcome tuple when available;
                                // validate its exact fenced value after final
                                // replay below.
                                const dense_entry = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
                                expected_snapshot_coverage = if (DB.DerivedAsyncCallbacks.dense_index_is_artifact_backed(dense_entry))
                                    try DB.DerivedAsyncCallbacks.dense_target_count_for_index_context(&shadow_ctx, cfg.name)
                                else
                                    null;
                            } else {
                                expected_snapshot_coverage = try DB.DerivedAsyncCallbacks.dense_target_count_for_index_context(&shadow_ctx, cfg.name);
                            }
                            break :count_blk count;
                        },
                        .sparse_vector => @intCast(try DB.DerivedAsyncCallbacks.rebuild_sparse_index_from_stored_embedding_artifacts_context(&shadow_ctx, cfg.name, 2048)),
                        .graph => @intCast(try Self.applySplitGraphArtifactsForIndexStreamingContext(
                            &shadow_ctx,
                            cfg.name,
                            graph_repair_rebuild_batch_size,
                        )),
                        .full_text => try shadow_manager.resetFullTextIndexForArtifactRebuildFromReadTxn(
                            self.core.store,
                            snapshot_txn,
                            cfg.name,
                            options.cancel_check,
                            options.capacity_check,
                        ),
                        else => unreachable,
                    };
                    break :rebuilt_blk count;
                },
                .algebraic => return error.UnsupportedOperation,
            };
            // Durable intent progress is cumulative, while the public/run result
            // reports work performed by this scheduler turn. Keeping those two
            // meanings separate prevents the final resumed slice from recounting
            // every vector processed by earlier turns.
            const reprocessed_this_pass = if (resume_building and cfg.kind == .dense_vector)
                rebuilt -| persisted_build_reprocessed
            else
                rebuilt;

            const index_ref = index_manager_mod.ManagedIndexRef{
                .name = cfg.name,
                .kind = cfg.kind,
            };
            if (cfg.kind == .dense_vector) {
                const built_entry = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
                if (completed_snapshot_build and DB.DerivedAsyncCallbacks.dense_index_is_artifact_backed(built_entry) and expected_snapshot_coverage == null) {
                    return error.RepairSourceCoverageIncomplete;
                }
                if (!cooperative_dense_build) {
                    if (expected_snapshot_coverage) |expected| {
                        if (!DB.DerivedAsyncCallbacks.dense_coverage_matches_target(built_entry.index.stats().active_count, expected)) {
                            return error.RepairSourceCoverageIncomplete;
                        }
                    }
                }
            }
            if (completed_snapshot_build) {
                try checkArtifactRepairCancelled(options);
                try Self.saveShadowReplacementAppliedSequence(self, alloc, &shadow_manager, shadow_checkpoint_path, index_ref, build_floor_sequence);
                if (options.capacity_check) |check| {
                    try check.current(try directoryUsageBytes(alloc, shadow_base));
                }
                if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .catching_up,
                    .replace_build_resume_key = true,
                    .build_reprocessed = rebuilt,
                    .candidate_applied_sequence = build_floor_sequence,
                    .failure_streak = 0,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
                // From this transition onward the candidate and its applied
                // checkpoint are independently reopenable. Cancellation, owner
                // loss, or restart preserves it for bounded catch-up resumption.
                candidate_reopenable = durable_repair_id != null;
                if (self.shadow_index_repair_hook) |hook| {
                    try hook.after_snapshot_build(hook.ptr, self, cfg.name, build_floor_sequence);
                }
            }
            var observed_ns_per_sequence: u64 = 0;
            var catch_up_start_sequence = build_floor_sequence;
            var catch_up_start_ns = monotonicTimeNs();
            const initial_catch_up_target = self.core.nextDerivedSequence();
            const initial_catch_up = try Self.catchUpShadowReplacementUntil(
                self,
                alloc,
                &shadow_manager,
                shadow_checkpoint_path,
                index_ref,
                initial_catch_up_target,
                options,
                null,
                0,
            );
            var converged_sequence = initial_catch_up.applied_sequence;
            if (options.capacity_check) |check| {
                try check.current(try directoryUsageBytes(alloc, shadow_base));
            }
            observed_ns_per_sequence = observeRepairCatchUpCost(
                observed_ns_per_sequence,
                catch_up_start_sequence,
                converged_sequence,
                elapsedSince(catch_up_start_ns),
            );
            if (initial_catch_up.yielded) {
                if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .catching_up,
                    .candidate_applied_sequence = converged_sequence,
                    .target_sequence = initial_catch_up_target,
                    .failure_streak = 0,
                    .next_retry_at_ms = 0,
                    .replace_last_error = true,
                });
                candidate_reopenable = durable_repair_id != null;
                return .{ .reprocessed = reprocessed_this_pass, .yielded = true };
            }
            try index_generation_manifest.writeReady(
                alloc,
                shadow_index_path,
                generation_id,
                cfg.name,
                types.indexConfigHash(cfg),
                converged_sequence,
            );
            if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .phase = .ready,
                .candidate_applied_sequence = converged_sequence,
                .target_sequence = self.core.nextDerivedSequence(),
            });

            if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .phase = .waiting_for_convergence,
                .candidate_applied_sequence = converged_sequence,
                .target_sequence = self.core.nextDerivedSequence(),
            });
            const max_activation_gap_sequences = @max(@as(u64, 1), options.max_activation_gap_sequences);
            const max_convergence_rounds: usize = @max(@as(usize, 1), options.max_convergence_rounds);
            const max_activation_pause_ns = std.math.mul(
                u64,
                @max(@as(u64, 1), options.max_activation_pause_ms),
                std.time.ns_per_ms,
            ) catch std.math.maxInt(u64);
            var convergence_round: usize = 0;
            while (!repairActivationAdmissible(
                self.core.nextDerivedSequence() -| converged_sequence,
                observed_ns_per_sequence,
                max_activation_gap_sequences,
                max_activation_pause_ns,
            ) and
                convergence_round < max_convergence_rounds) : (convergence_round += 1)
            {
                const convergence_target = self.core.nextDerivedSequence();
                if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .catching_up,
                    .candidate_applied_sequence = converged_sequence,
                    .target_sequence = convergence_target,
                });
                catch_up_start_sequence = converged_sequence;
                catch_up_start_ns = monotonicTimeNs();
                const convergence_catch_up = try Self.catchUpShadowReplacementUntil(
                    self,
                    alloc,
                    &shadow_manager,
                    shadow_checkpoint_path,
                    index_ref,
                    convergence_target,
                    options,
                    null,
                    0,
                );
                converged_sequence = convergence_catch_up.applied_sequence;
                if (options.capacity_check) |check| {
                    try check.current(try directoryUsageBytes(alloc, shadow_base));
                }
                observed_ns_per_sequence = observeRepairCatchUpCost(
                    observed_ns_per_sequence,
                    catch_up_start_sequence,
                    converged_sequence,
                    elapsedSince(catch_up_start_ns),
                );
                if (convergence_catch_up.yielded) {
                    if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .phase = .catching_up,
                        .candidate_applied_sequence = converged_sequence,
                        .target_sequence = convergence_target,
                        .failure_streak = 0,
                        .next_retry_at_ms = 0,
                        .replace_last_error = true,
                    });
                    candidate_reopenable = durable_repair_id != null;
                    return .{ .reprocessed = reprocessed_this_pass, .yielded = true };
                }
                try index_generation_manifest.writeReady(
                    alloc,
                    shadow_index_path,
                    generation_id,
                    cfg.name,
                    types.indexConfigHash(cfg),
                    converged_sequence,
                );
                if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .ready,
                    .candidate_applied_sequence = converged_sequence,
                    .target_sequence = convergence_target,
                });
                if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .waiting_for_convergence,
                    .candidate_applied_sequence = converged_sequence,
                    .target_sequence = self.core.nextDerivedSequence(),
                });
            }
            if (!repairActivationAdmissible(
                self.core.nextDerivedSequence() -| converged_sequence,
                observed_ns_per_sequence,
                max_activation_gap_sequences,
                max_activation_pause_ns,
            )) {
                return error.ShadowIndexCatchUpIncomplete;
            }
            const ready_sequence = try index_generation_manifest.validateReady(
                alloc,
                shadow_index_path,
                generation_id,
                cfg.name,
                types.indexConfigHash(cfg),
            );
            if (ready_sequence != converged_sequence) return error.IndexGenerationManifestMismatch;
            try db_internal.checkArtifactRepairActivationOwner(options);
            // Drain catalog-borrowing maintenance before the bounded reader
            // pause; slow merge/compaction shutdown is not query downtime.
            var structural_guard = Self.beginDrainedIndexStructuralMutation(
                self,
                "index repair activation",
                cfg.name,
            );
            defer structural_guard.deinit();
            const activation_started_ns = monotonicTimeNs();
            const activation_deadline_ns = activation_started_ns +| max_activation_pause_ns;
            if (!structural_guard.acquireCatalogBarrierUntil(activation_deadline_ns)) {
                return error.ShadowIndexCatchUpIncomplete;
            }
            var unpublished_replacement: ?index_manager_mod.IndexManager.DetachedIndex = null;
            defer if (unpublished_replacement) |*replacement| {
                shadow_manager.destroyDetachedReplacementIndex(replacement);
            };
            var retired_generation: ?index_manager_mod.IndexManager.DetachedIndex = null;
            defer if (retired_generation) |*retired| {
                self.core.index_manager.destroyDetachedReplacementIndex(retired);
            };
            var activation_metric_recorded = false;
            defer if (!activation_metric_recorded) {
                if (self.core.index_manager.resource_manager) |manager| {
                    manager.recordIndexRepairActivation(monotonicTimeNs() -| activation_started_ns, max_activation_pause_ns);
                }
            };
            try checkArtifactRepairCancelled(options);
            if (!Self.lockApplyUntil(self, activation_deadline_ns)) return error.ShadowIndexCatchUpIncomplete;
            var apply_lock_held = true;
            defer if (apply_lock_held) self.core.unlockApply();

            const final_target = self.core.nextDerivedSequence();
            if (!repairActivationAdmissible(
                final_target -| converged_sequence,
                observed_ns_per_sequence,
                max_activation_gap_sequences,
                max_activation_pause_ns,
            )) {
                return error.ShadowIndexCatchUpIncomplete;
            }
            const activation_replay_deadline_ns = repairActivationReplayDeadline(
                monotonicTimeNs(),
                activation_deadline_ns,
                max_activation_pause_ns,
            ) orelse return error.ShadowIndexCatchUpIncomplete;
            const activation_catch_up = Self.catchUpShadowReplacementUntil(
                self,
                alloc,
                &shadow_manager,
                shadow_checkpoint_path,
                index_ref,
                final_target,
                options,
                activation_replay_deadline_ns,
                observed_ns_per_sequence,
            ) catch |err| switch (err) {
                error.CatchUpDeadlineExceeded => return error.ShadowIndexCatchUpIncomplete,
                else => return err,
            };
            std.debug.assert(!activation_catch_up.yielded);
            const reached_target = activation_catch_up.applied_sequence;
            if (reached_target < final_target) return error.ShadowIndexCatchUpIncomplete;
            // The generation-scoped coverage tuple is maintained in the same
            // fenced primary write stream and is O(1) to read.
            if (cfg.kind == .dense_vector) {
                const dense_entry = shadow_manager.denseIndex(cfg.name) orelse return error.IndexNotFound;
                if (DB.DerivedAsyncCallbacks.dense_index_is_artifact_backed(dense_entry)) {
                    const expected = (try DB.DerivedAsyncCallbacks.dense_target_count_for_index_context(self.async_context, cfg.name)) orelse
                        return error.RepairSourceCoverageIncomplete;
                    if (!DB.DerivedAsyncCallbacks.dense_coverage_matches_target(dense_entry.index.stats().active_count, expected)) {
                        return error.RepairSourceCoverageIncomplete;
                    }
                }
            }
            try checkArtifactRepairCancelled(options);
            try db_internal.checkArtifactRepairActivationOwner(options);
            try ensureRepairActivationDeadline(activation_deadline_ns);
            if (durable_repair_id) |repair_id| try Self.validateIndexRepairActivationState(self, alloc, repair_id, options.owner_epoch);
            try ensureRepairActivationDeadline(activation_deadline_ns);
            const previous_active_pointer = try self.core.index_manager.captureActiveIndexRootPointer(cfg.name);
            defer if (previous_active_pointer) |value| alloc.free(value);
            try ensureRepairActivationDeadline(activation_deadline_ns);
            if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .phase = .activating,
                .candidate_applied_sequence = reached_target,
                .target_sequence = final_target,
                .previous_pointer_captured = true,
                .previous_active_relative_path = previous_active_pointer,
                .replace_previous_active_path = true,
            });
            try ensureRepairActivationDeadline(activation_deadline_ns);

            // Final replay may advance beyond the preliminary ready marker. Make
            // readiness describe the exact durable generation installed by the
            // pointer swap, not merely the pre-barrier candidate.
            try index_generation_manifest.writeReady(
                alloc,
                shadow_index_path,
                generation_id,
                cfg.name,
                types.indexConfigHash(cfg),
                reached_target,
            );
            try ensureRepairActivationDeadline(activation_deadline_ns);
            const manifest_sequence = try index_generation_manifest.validateReady(
                alloc,
                shadow_index_path,
                generation_id,
                cfg.name,
                types.indexConfigHash(cfg),
            );
            if (manifest_sequence != reached_target) return error.IndexGenerationManifestMismatch;
            try ensureRepairActivationDeadline(activation_deadline_ns);

            // Transfer the already-open runtime for every index family. Pointer
            // publication and in-memory handoff are allocation-free, and the
            // predecessor is retired only after service fences are released.
            unpublished_replacement = try shadow_manager.detachReplacementIndex(cfg.name);
            retired_generation = try self.core.index_manager.installAdoptedReplacementIndex(
                self.core.store,
                cfg,
                shadow_index_path,
                previous_active_pointer,
                &unpublished_replacement.?,
            );
            unpublished_replacement = null;
            shadow_installed = true;
            if (self.shadow_index_repair_hook) |hook| {
                if (hook.after_pointer_activation) |after_activation| try after_activation(hook.ptr, self, cfg.name);
            }
            if (durable_repair_id) |repair_id| try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                .phase = .validating,
                .candidate_applied_sequence = final_target,
                .target_sequence = final_target,
            });
            index_manager_mod.IndexManager.clearRepairShadowInProgressMarker(alloc, shadow_base) catch |err| {
                std.log.warn("failed to clear repair shadow in-progress marker index={s} err={s}", .{ cfg.name, @errorName(err) });
            };
            const prior_checkpoint = try self.core.loadProjectionCheckpoint(alloc, cfg.name);
            const final_update = apply_state.AppliedSequenceUpdate{
                .index_name = cfg.name,
                .sequence = final_target,
                .status = .clean,
                .generation = prior_checkpoint.generation +| 1,
                .config_hash = types.indexConfigHash(cfg),
            };
            try DB.DerivedAsyncCallbacks.save_index_status_snapshots(alloc, self.core.store, self.core.index_manager, &[_]apply_state.AppliedSequenceUpdate{final_update});
            try self.core.saveAppliedSequence(cfg.name, final_target);
            try self.core.saveProjectionCheckpoint(cfg.name, .{
                .applied_sequence = final_update.sequence,
                .status = final_update.status,
                .generation = final_update.generation,
                .config_hash = final_update.config_hash,
            });

            // The pointer and clean checkpoint are now ordered before any later
            // foreground apply. Release the write and catalog fences before
            // operator hooks, intent cleanup, and generation garbage collection.
            // Generation teardown and other post-commit work are measured
            // separately and cannot produce false pause-budget alarms.
            const activation_pause_ns = monotonicTimeNs() -| activation_started_ns;
            self.core.unlockApply();
            apply_lock_held = false;
            structural_guard.releaseCatalogBarrier();
            if (retired_generation) |*retired| {
                self.core.index_manager.destroyDetachedReplacementIndex(retired);
                retired_generation = null;
            }
            if (shadow_manager_open) {
                shadow_manager.deinit();
                shadow_manager_open = false;
            }
            structural_guard.release();
            if (self.core.index_manager.resource_manager) |manager| {
                manager.recordIndexRepairActivation(activation_pause_ns, max_activation_pause_ns);
            }
            activation_metric_recorded = true;
            if (activation_pause_ns > max_activation_pause_ns) {
                std.log.warn(
                    "index repair activation exceeded pause budget index={s} elapsed_ms={} budget_ms={}",
                    .{
                        cfg.name,
                        @divTrunc(activation_pause_ns, std.time.ns_per_ms),
                        @divTrunc(max_activation_pause_ns, std.time.ns_per_ms),
                    },
                );
            }
            if (self.shadow_index_repair_hook) |hook| {
                if (hook.after_clean_checkpoint) |after_checkpoint| try after_checkpoint(hook.ptr, self, cfg.name);
            }
            if (durable_repair_id) |repair_id| {
                try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                    .phase = .cleanup,
                    .candidate_applied_sequence = final_target,
                    .target_sequence = final_target,
                });
                // The clean checkpoint and replacement are durable before this
                // single local transition releases replay retention and removes
                // the intent.
                try Self.removeIndexRepairIntentAndPin(self, alloc, repair_id);
                Self.scheduleInactiveRepairShadowCleanup(self);
            }

            return .{
                .reprocessed = reprocessed_this_pass,
                .applied_sequence = final_target,
                .unresolved_artifacts = repair_issue_counter.load(.monotonic),
            };
        }

        fn repairIndexIssuesWithRequest(
            self: *DB,
            alloc: Allocator,
            req: types.ArtifactRepairRunRequest,
            options: types.ArtifactRepairRunOptions,
        ) !types.ArtifactRepairResult {
            try checkArtifactRepairCancelled(options);
            const requested_index = req.index_name orelse return error.InvalidArgument;
            if (req.cursor != null and req.cursor.?.len != 0) return error.InvalidArgument;
            const limit = if (req.limit == 0) @as(u32, 100) else req.limit;
            var result = types.ArtifactRepairResult{ .limit = limit };
            if (limit == 0) return result;
            if (req.control == null and req.repair_id != null) return error.InvalidArgument;

            const cfg_ptr = self.core.index_manager.get(requested_index) orelse return error.NotFound;
            var cfg = try types.IndexConfig.clone(alloc, cfg_ptr.*);
            defer cfg.deinit(alloc);

            if (req.control) |control| {
                if (req.force or req.cursor != null or req.artifact_kind != null) return error.InvalidArgument;
                const applied = switch (control) {
                    .pause_automatic => try Self.pauseAutomaticIndexRepair(self, alloc, requested_index, req.repair_id),
                    .resume_automatic => try Self.resumeAutomaticIndexRepair(self, alloc, requested_index, req.repair_id),
                    .cancel_current_attempt => try Self.cancelCurrentIndexRepairAttempt(self, alloc, requested_index, req.repair_id),
                };
                result.scanned = 1;
                result.controls_applied = @intFromBool(applied);
                result.debt_remaining = applied;
                result.indexes_degraded_before = @intFromBool(applied);
                result.indexes_degraded_after = @intFromBool(applied);
                return result;
            }

            if (req.artifact_kind) |kind| {
                const matches_kind = switch (cfg.kind) {
                    .dense_vector, .sparse_vector => kind == .embedding,
                    .graph => kind == .graph,
                    .full_text => kind == .full_text,
                    .algebraic => kind == .algebraic,
                };
                if (!matches_kind) return error.NotFound;
            }

            const repair_required = try Self.indexGenerationRepairRequired(self, alloc, cfg.name);
            result.indexes_degraded_before = @intFromBool(repair_required);
            if (!repair_required and !req.force) return result;
            if ((req.repair_job_id == null) != (req.repair_job_created_at_ms == null)) return error.InvalidArgument;
            const repair_job_created_at_ms = req.repair_job_created_at_ms orelse 0;
            if (req.force and req.repair_job_id != null and try Self.operatorRepairAlreadyCompleted(
                self,
                alloc,
                cfg,
                req.repair_job_id.?,
                repair_job_created_at_ms,
            )) {
                result.scanned = 1;
                result.indexes_rebuilt = 1;
                try Self.finalizeCommittedIndexRepairOutcome(self, alloc, cfg.name, 0, &result);
                return result;
            }

            const had_load_failure = self.core.index_manager.loadFailure(cfg.name) != null;
            var durable_repair_id = try Self.indexRepairIdForIndex(self, alloc, cfg.name);
            if (had_load_failure and self.core.index_manager.recoveryActionForIndex(cfg.name) == .rebuild_from_artifacts) {
                _ = try Self.discoverRecoverableStartupIndexFailures(self, alloc, 1);
                durable_repair_id = try Self.indexRepairIdForIndex(self, alloc, cfg.name);
            }
            if (try Self.classifyCurrentDenseGenerationRepair(self, alloc, cfg)) |classification| {
                durable_repair_id = try Self.ensureAutomaticDenseGenerationRepairIntent(
                    self,
                    alloc,
                    cfg,
                    classification.trigger,
                    classification.last_error,
                );
            } else if (durable_repair_id == null and cfg.kind != .algebraic) {
                durable_repair_id = try Self.createOperatorGenerationRepairIntent(
                    self,
                    alloc,
                    cfg,
                    req.repair_job_id orelse 0,
                    repair_job_created_at_ms,
                );
            }
            if (durable_repair_id) |repair_id| {
                if (req.force and req.repair_job_id != null) {
                    try Self.attachOperatorRepairRequest(
                        self,
                        alloc,
                        repair_id,
                        req.repair_job_id.?,
                        repair_job_created_at_ms,
                    );
                }
            }

            // Public/operator entry points never execute a durable generation
            // rebuild through this lower-level function. Managed callers enqueue
            // the exact group; standalone callers synchronously enter the same
            // admitted state machine used by the owner-side scheduler.
            if (durable_repair_id) |repair_id| if (!options.executing_durable_index_repair) {
                result.scanned = 1;
                result.indexes_degraded_before = @intFromBool(repair_required);
                result.indexes_degraded_after = 1;
                if (req.force) {
                    var current = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                    defer current.deinit(alloc);
                    if (current.intent.phase == .terminal) {
                        try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                            .phase = .detected,
                            .attempt_count = current.intent.attempt_count +| 1,
                            .failure_streak = 0,
                            .next_retry_at_ms = 0,
                            .replace_last_error = true,
                        });
                    }
                }
                if (options.defer_durable_index_repair_execution) {
                    result.in_progress = 1;
                    result.unresolved = 1;
                    result.debt_remaining = true;
                    return result;
                }
                const advanced = try Self.advanceIndexRepairIntent(self, alloc, repair_id, options);
                result.reprocessed = advanced.documents_reprocessed;
                if (advanced.has_repair_outcome) {
                    result.repaired = advanced.artifacts_repaired;
                    result.missing_source_docs = advanced.missing_source_docs;
                    result.failed = advanced.failed;
                    result.unsupported = advanced.unsupported;
                    result.unresolved = advanced.unresolved;
                    result.in_progress = advanced.in_progress;
                    result.indexes_rebuilt = advanced.indexes_rebuilt;
                    result.indexes_degraded_before = advanced.indexes_degraded_before;
                    result.indexes_degraded_after = advanced.indexes_degraded_after;
                    result.debt_remaining = advanced.debt_remaining;
                } else if (advanced.repaired) {
                    result.repaired = 1;
                    result.indexes_rebuilt = 1;
                } else if (advanced.terminal) {
                    result.failed = 1;
                    result.unresolved = 1;
                    result.debt_remaining = true;
                } else {
                    result.in_progress = 1;
                    result.unresolved = 1;
                    result.debt_remaining = true;
                }
                return result;
            };

            if (!(try Self.beginIndexRepairLease(self, cfg.name))) {
                result.scanned += 1;
                result.in_progress += 1;
                result.unresolved += 1;
                result.indexes_degraded_after = 1;
                result.debt_remaining = true;
                return result;
            }
            defer Self.endIndexRepairLease(self, cfg.name);

            result.scanned += 1;
            if (durable_repair_id) |repair_id| {
                if (try Self.reconcileActivatedIndexRepair(self, alloc, repair_id)) {
                    result.indexes_rebuilt += 1;
                    try Self.finalizeCommittedIndexRepairOutcome(self, alloc, cfg.name, 0, &result);
                    return result;
                }
                _ = try Self.rollbackUnavailableActivatedIndexRepair(self, alloc, repair_id);
                var durable_entry = try Self.loadIndexRepairEntryById(self, alloc, repair_id);
                defer durable_entry.deinit(alloc);
                const resumable = durable_entry.intent.candidate_relative_path != null and switch (durable_entry.intent.phase) {
                    .building => durable_entry.intent.build_resume_key != null and cfg.kind == .dense_vector,
                    .catching_up, .ready, .waiting_for_convergence => true,
                    else => false,
                };
                if (durable_entry.intent.phase == .terminal) {
                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .phase = .detected,
                        .attempt_count = durable_entry.intent.attempt_count +| 1,
                        .failure_streak = 0,
                        .next_retry_at_ms = 0,
                        .replace_last_error = true,
                    });
                    durable_entry.intent.phase = .detected;
                }
                if (!resumable and durable_entry.intent.phase != .preflight) {
                    try Self.updateIndexRepairIntent(self, alloc, repair_id, .{
                        .phase = .preflight,
                        .attempt_count = @max(@as(u32, 1), durable_entry.intent.attempt_count),
                        .failure_streak = 0,
                        .next_retry_at_ms = 0,
                        .replace_last_error = true,
                    });
                }
                Self.ensureDenseArtifactTargetCounterForRepair(
                    self,
                    alloc,
                    cfg,
                    repair_id,
                    options.cancel_check,
                ) catch |err| switch (err) {
                    error.Canceled, error.StaleDenseArtifactCounterBootstrap => {
                        result.in_progress += 1;
                        result.unresolved += 1;
                        result.debt_remaining = true;
                        return result;
                    },
                    else => return err,
                };
                Self.validateIndexRepairSourcePreflight(self, alloc, cfg) catch |err| {
                    const err_name = if (err == error.RepairSourceCoverageIncomplete)
                        "dense_artifact_coverage_missing"
                    else
                        @errorName(err);
                    try Self.recordIndexRepairAttemptFailure(self, alloc, repair_id, err_name, true);
                    result.failed += 1;
                    result.unresolved += 1;
                    result.debt_remaining = true;
                    return result;
                };
            }
            result.indexes_degraded_before = @intFromBool(repair_required);
            result.indexes_degraded_after = 1;
            if (self.core.index_manager.loadFailure(cfg.name) != null) {
                const recovery_action = self.core.index_manager.recoveryActionForIndex(cfg.name).?;
                switch (recovery_action) {
                    .retry_open, .manual_intervention => {
                        // Explicit named operator repair may first retry any
                        // quarantined open. This does not broaden the automatic
                        // destructive-rebuild allowlist.
                        _ = try self.retryQuarantinedIndexLoads(true);
                        if (self.core.index_manager.loadFailure(cfg.name) != null and recovery_action == .retry_open) {
                            result.failed += 1;
                            result.unresolved += 1;
                            result.debt_remaining = true;
                            return result;
                        }
                    },
                    .rebuild_from_artifacts => {
                        // Build directly from the retained status-only config. The
                        // quarantined root remains untouched until the replacement
                        // is durable and activated by pointer swap.
                    },
                }
            }
            if (had_load_failure and self.core.index_manager.loadFailure(cfg.name) == null and (cfg.kind == .full_text or cfg.kind == .algebraic) and !try Self.indexGenerationRepairRequired(self, alloc, cfg.name)) {
                result.indexes_degraded_after = 0;
                result.repaired += 1;
                return result;
            }

            if (cfg.kind == .algebraic) {
                result.unsupported += 1;
                result.unresolved += 1;
                result.debt_remaining = true;
                return result;
            }
            const rebuilt = Self.rebuildIndexWithShadowReplacement(self, alloc, cfg, options, durable_repair_id) catch |err| switch (err) {
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
            if (rebuilt.yielded) {
                const repair_summary_ready = try Self.artifactRepairSummaryReady(self, alloc);
                const unresolved_artifacts = if (repair_summary_ready)
                    @max(
                        rebuilt.unresolved_artifacts,
                        (try Self.artifactRepairSummaryIndexSnapshot(self, alloc, cfg.name, true)).count,
                    )
                else
                    rebuilt.unresolved_artifacts;
                result.unresolved += unresolved_artifacts;
                result.in_progress += 1;
                result.unresolved += 1;
                result.indexes_degraded_after = 1;
                result.debt_remaining = true;
                return result;
            }
            result.indexes_rebuilt += 1;
            try Self.finalizeCommittedIndexRepairOutcome(self, alloc, cfg.name, rebuilt.unresolved_artifacts, &result);
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
            return try Self.repairArtifactIssuesWithRequest(self, alloc, .{
                .artifact_kind = artifact_kind,
                .limit = @intCast(@min(limit, std.math.maxInt(u32))),
            });
        }

        pub fn repairEmbeddingArtifactIssues(self: *DB, alloc: Allocator, limit: usize) !types.EmbeddingArtifactRepairResult {
            return try Self.repairArtifactIssues(self, alloc, .embedding, limit);
        }
    };
}
