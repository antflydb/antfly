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
const Allocator = std.mem.Allocator;
const hbc_mod = @import("../hbc_adapter.zig");
const apply_state = @import("derived/apply_state.zig");
const index_repair_state = @import("derived/index_repair_state.zig");
const types = @import("types.zig");
const graph_mod = @import("../../graph/graph.zig");
pub fn freeDBIndexStatsItem(alloc: Allocator, item: types.DBIndexStats) void {
    alloc.free(item.name);
    for (item.source_replay) |source| alloc.free(source.artifact_name);
    if (item.source_replay.len > 0) alloc.free(item.source_replay);
    if (item.load_error) |value| alloc.free(value);
    if (item.index_repair_last_error) |value| alloc.free(value);
    if (item.algebraic_last_error_doc_key) |value| alloc.free(value);
    if (item.algebraic_last_error_reason) |value| alloc.free(value);
    if (item.algebraic_capability_fingerprint) |value| alloc.free(value);
    if (item.algebraic_capability_lifecycle_status) |value| alloc.free(value);
    if (item.algebraic_planner_last_decision) |value| alloc.free(value);
    if (item.algebraic_planner_last_fallback_reason) |value| alloc.free(value);
    if (item.algebraic_planner_lifecycle_blocking_reason) |value| alloc.free(value);
    if (item.algebraic_last_observed_query_shape) |value| alloc.free(value);
    if (item.algebraic_last_recommended_materialization) |value| alloc.free(value);
    types.freeGraphMetricStatuses(alloc, @constCast(item.graph_metric_status));
    if (item.algebraic_top_candidate) |candidate| {
        alloc.free(candidate.recommendation);
        alloc.free(candidate.materialization_id);
        alloc.free(candidate.lifecycle);
        alloc.free(candidate.decision);
    }
    if (item.algebraic_active_progress) |progress| {
        alloc.free(progress.recommendation);
        alloc.free(progress.materialization_id);
        alloc.free(progress.lifecycle);
    }
    for (item.algebraic_candidates) |candidate| {
        alloc.free(candidate.recommendation);
        alloc.free(candidate.materialization_id);
        alloc.free(candidate.lifecycle);
        alloc.free(candidate.decision);
    }
    if (item.algebraic_candidates.len > 0) alloc.free(item.algebraic_candidates);
    for (item.algebraic_candidate_decision_history) |entry| {
        alloc.free(entry.recommendation);
        alloc.free(entry.materialization_id);
        alloc.free(entry.lifecycle);
        alloc.free(entry.previous_decision);
        alloc.free(entry.decision);
    }
    if (item.algebraic_candidate_decision_history.len > 0) alloc.free(item.algebraic_candidate_decision_history);
    for (item.algebraic_progress) |progress| {
        alloc.free(progress.recommendation);
        alloc.free(progress.materialization_id);
        alloc.free(progress.lifecycle);
    }
    if (item.algebraic_progress.len > 0) alloc.free(item.algebraic_progress);
}

pub fn cloneAlgebraicCandidateStatusAlloc(
    alloc: Allocator,
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    decision: []const u8,
    observation_count: u64,
    estimated_scan_rows_saved: u64,
    estimated_write_cost: u64,
    estimated_tensor_rows: u64,
    estimated_storage_bytes: u64,
    estimated_write_amplification: u64,
    score: i128,
    idle_miss_count: u64,
    generation: u64,
) !types.AlgebraicCandidateStatus {
    const owned_recommendation = try alloc.dupe(u8, recommendation);
    errdefer alloc.free(owned_recommendation);
    const owned_materialization_id = try alloc.dupe(u8, materialization_id);
    errdefer alloc.free(owned_materialization_id);
    const owned_lifecycle = try alloc.dupe(u8, lifecycle);
    errdefer alloc.free(owned_lifecycle);
    const owned_decision = try alloc.dupe(u8, decision);
    errdefer alloc.free(owned_decision);
    return .{
        .recommendation = owned_recommendation,
        .materialization_id = owned_materialization_id,
        .lifecycle = owned_lifecycle,
        .decision = owned_decision,
        .observation_count = observation_count,
        .estimated_scan_rows_saved = estimated_scan_rows_saved,
        .estimated_write_cost = estimated_write_cost,
        .estimated_tensor_rows = estimated_tensor_rows,
        .estimated_storage_bytes = estimated_storage_bytes,
        .estimated_write_amplification = estimated_write_amplification,
        .score = score,
        .idle_miss_count = idle_miss_count,
        .generation = generation,
    };
}

pub fn cloneAlgebraicCandidateDecisionStatusAlloc(
    alloc: Allocator,
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    previous_decision: []const u8,
    decision: []const u8,
    observation_count: u64,
    estimated_scan_rows_saved: u64,
    estimated_write_cost: u64,
    score: i128,
    score_delta: i128,
    idle_miss_count: u64,
    generation: u64,
) !types.AlgebraicCandidateDecisionStatus {
    const owned_recommendation = try alloc.dupe(u8, recommendation);
    errdefer alloc.free(owned_recommendation);
    const owned_materialization_id = try alloc.dupe(u8, materialization_id);
    errdefer alloc.free(owned_materialization_id);
    const owned_lifecycle = try alloc.dupe(u8, lifecycle);
    errdefer alloc.free(owned_lifecycle);
    const owned_previous_decision = try alloc.dupe(u8, previous_decision);
    errdefer alloc.free(owned_previous_decision);
    const owned_decision = try alloc.dupe(u8, decision);
    errdefer alloc.free(owned_decision);
    return .{
        .recommendation = owned_recommendation,
        .materialization_id = owned_materialization_id,
        .lifecycle = owned_lifecycle,
        .previous_decision = owned_previous_decision,
        .decision = owned_decision,
        .observation_count = observation_count,
        .estimated_scan_rows_saved = estimated_scan_rows_saved,
        .estimated_write_cost = estimated_write_cost,
        .score = score,
        .score_delta = score_delta,
        .idle_miss_count = idle_miss_count,
        .generation = generation,
    };
}

pub fn cloneAlgebraicProgressStatusAlloc(
    alloc: Allocator,
    recommendation: []const u8,
    materialization_id: []const u8,
    lifecycle: []const u8,
    target_sequence: u64,
    applied_sequence: u64,
    rows_processed: u64,
    target_rows: u64,
) !types.AlgebraicProgressStatus {
    const owned_recommendation = try alloc.dupe(u8, recommendation);
    errdefer alloc.free(owned_recommendation);
    const owned_materialization_id = try alloc.dupe(u8, materialization_id);
    errdefer alloc.free(owned_materialization_id);
    const owned_lifecycle = try alloc.dupe(u8, lifecycle);
    errdefer alloc.free(owned_lifecycle);
    return .{
        .recommendation = owned_recommendation,
        .materialization_id = owned_materialization_id,
        .lifecycle = owned_lifecycle,
        .target_sequence = target_sequence,
        .applied_sequence = applied_sequence,
        .rows_processed = rows_processed,
        .target_rows = target_rows,
    };
}

pub fn indexStatusKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ index_status_prefix, index_name });
}

pub fn encodeIndexStatusSnapshot(status_snapshot: IndexStatusSnapshot, out: *[index_status_encoded_len]u8) void {
    var offset: usize = 0;
    inline for (.{
        index_status_magic,
        @as(u64, @backingInt(status_snapshot.kind)),
        status_snapshot.doc_count,
        status_snapshot.term_count,
        status_snapshot.edge_count,
        status_snapshot.node_count,
        status_snapshot.root_node,
        status_snapshot.updated_at_ns,
        @as(u64, @intFromBool(status_snapshot.graph_counts_pending)),
    }) |value| {
        std.mem.writeInt(u64, out[offset..][0..8], value, .little);
        offset += 8;
    }
}

pub fn decodeIndexStatusSnapshot(raw: []const u8) !IndexStatusSnapshot {
    if (raw.len != index_status_encoded_len and raw.len != 64) return error.InvalidIndexStatusSnapshot;
    var offset: usize = 0;
    const magic = std.mem.readInt(u64, raw[offset..][0..8], .little);
    offset += 8;
    if (!((magic == index_status_magic and raw.len == index_status_encoded_len) or
        (magic == index_status_magic_v1 and raw.len == 64))) return error.InvalidIndexStatusSnapshot;
    const counts_pending = if (raw.len == 64) 0 else std.mem.readInt(u64, raw[64..72], .little);
    if (counts_pending > 1) return error.InvalidIndexStatusSnapshot;
    const kind_raw = std.mem.readInt(u64, raw[offset..][0..8], .little);
    offset += 8;
    const kind: types.IndexKind = switch (kind_raw) {
        @backingInt(types.IndexKind.full_text) => .full_text,
        @backingInt(types.IndexKind.dense_vector) => .dense_vector,
        @backingInt(types.IndexKind.sparse_vector) => .sparse_vector,
        @backingInt(types.IndexKind.graph) => .graph,
        @backingInt(types.IndexKind.algebraic) => .algebraic,
        else => return error.InvalidIndexStatusSnapshot,
    };
    return .{
        .kind = kind,
        .graph_counts_pending = counts_pending != 0,
        .doc_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .term_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .edge_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .node_count = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .root_node = blk: {
            const value = std.mem.readInt(u64, raw[offset..][0..8], .little);
            offset += 8;
            break :blk value;
        },
        .updated_at_ns = std.mem.readInt(u64, raw[offset..][0..8], .little),
    };
}

pub fn applyIndexStatusSnapshot(item: *types.DBIndexStats, status_snapshot: IndexStatusSnapshot) void {
    if (status_snapshot.kind != item.kind) return;
    item.doc_count = status_snapshot.doc_count;
    item.term_count = status_snapshot.term_count;
    item.edge_count = status_snapshot.edge_count;
    item.graph_counts_pending = status_snapshot.graph_counts_pending;
    item.node_count = status_snapshot.node_count;
    item.root_node = status_snapshot.root_node;
}

pub fn indexLoadFailureKeyAlloc(alloc: Allocator, index_name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{s}{s}", .{ index_load_failure_prefix, index_name });
}

pub fn applyTerminalLoadFailureStatus(item: *types.DBIndexStats) void {
    item.replay_catch_up_required = false;
    item.catch_up_active = false;
    item.backfill_active = false;
    item.repair_degraded = true;
}

pub fn applyGraphAlgebraicRuntimeStats(item: *types.DBIndexStats, graph_index: *const graph_mod.GraphIndex) void {
    const algebraic_graph = graph_index.algebraicTraversalRuntimeStats();
    item.algebraic_graph_traversal_attempt_count = algebraic_graph.attempt_count;
    item.algebraic_graph_traversal_proven_count = algebraic_graph.proven_count;
    item.algebraic_graph_traversal_rejected_count = algebraic_graph.rejected_count;
    item.algebraic_graph_traversal_fallback_count = algebraic_graph.fallback_count;
    item.algebraic_graph_traversal_result_node_count = algebraic_graph.result_node_count;
}

fn cloneGraphMetricBuildPageStatusesFromGraph(
    alloc: Allocator,
    source: []const graph_mod.GraphIndex.GraphMetricBuildPageStatus,
) ![]types.GraphMetricBuildPageStatus {
    if (source.len == 0) return &.{};
    const out = try alloc.alloc(types.GraphMetricBuildPageStatus, source.len);
    var initialized: usize = 0;
    errdefer {
        for (out[0..initialized]) |*page| page.deinit(alloc);
        alloc.free(out);
    }
    for (source, 0..) |page, i| {
        const worker_id = if (page.worker_id.len > 0) try alloc.dupe(u8, page.worker_id) else "";
        errdefer if (worker_id.len > 0) alloc.free(worker_id);
        const cursor = if (page.cursor.len > 0) try alloc.dupe(u8, page.cursor) else "";
        errdefer if (cursor.len > 0) alloc.free(cursor);
        const last_error = if (page.last_error.len > 0) try alloc.dupe(u8, page.last_error) else "";
        errdefer if (last_error.len > 0) alloc.free(last_error);
        out[i] = .{
            .phase = page.phase,
            .iteration = page.iteration,
            .page_id = page.page_id,
            .state = page.state,
            .range_kind = page.range_kind,
            .worker_id = worker_id,
            .lease_expires_at_ms = page.lease_expires_at_ms,
            .attempt = page.attempt,
            .cursor = cursor,
            .completed_units = page.completed_units,
            .total_units = page.total_units,
            .last_error = last_error,
        };
        initialized += 1;
    }
    return out;
}

pub fn cloneGraphMetricStatusFromGraph(
    alloc: Allocator,
    source: graph_mod.GraphIndex.GraphMetricStatus,
) !types.GraphMetricStatus {
    var out = types.GraphMetricStatus{
        .name = try alloc.dupe(u8, source.name),
        .state = source.state,
        .phase = source.phase,
        .metadata_version = source.metadata_version,
        .config_fingerprint = source.config_fingerprint,
        .maintenance_paused = source.maintenance_paused,
        .build_queued = source.build_queued,
        .published_generation = source.published_edge_generation,
        .edge_generation = source.edge_generation,
        .target_edge_generation = source.target_edge_generation,
        .queued_generation = source.queued_generation,
        .building_generation = source.building_generation,
        .build_job_id = source.build_job_id,
        .build_started_at_ms = source.build_started_at_ms,
        .build_iteration = source.build_iteration,
        .build_lease_expires_at_ms = source.build_lease_expires_at_ms,
        .build_completed_units = source.build_completed_units,
        .build_total_units = source.build_total_units,
        .build_pages_truncated = source.build_pages_truncated,
        .retry_count = source.retry_count,
        .progress = source.progress,
        .converged = source.converged,
        .iterations_completed = source.iterations_completed,
        .delta = source.delta,
        .computed_at_ms = source.computed_at_ms,
        .last_event = source.last_event,
    };
    errdefer out.deinit(alloc);
    out.edge_filter = try source.edge_filter.cloneAlloc(alloc);
    out.build_worker_id = if (source.build_worker_id.len > 0) try alloc.dupe(u8, source.build_worker_id) else "";
    out.build_cursor = if (source.build_cursor.len > 0) try alloc.dupe(u8, source.build_cursor) else "";
    out.last_error = if (source.last_error.len > 0) try alloc.dupe(u8, source.last_error) else "";
    out.recent_events = if (source.recent_events.len > 0)
        try alloc.dupe(graph_mod.GraphIndex.GraphMetricEvent, source.recent_events)
    else
        &.{};
    out.build_pages = try cloneGraphMetricBuildPageStatusesFromGraph(alloc, source.build_pages);
    return out;
}

pub const IndexStatusSnapshot = struct {
    kind: types.IndexKind,
    doc_count: u64 = 0,
    term_count: u64 = 0,
    edge_count: u64 = 0,
    graph_counts_pending: bool = false,
    node_count: u64 = 0,
    root_node: u64 = 0,
    updated_at_ns: u64 = 0,
};

const index_status_prefix = "\x00\x00__metadata__:index_status:";
pub const index_status_magic_v1: u64 = 0x3153544154584449; // "IDXTATS1" little-endian
pub const index_status_magic: u64 = 0x3253544154584449; // "IDXTATS2" little-endian
pub const index_status_encoded_len = 9 * 8;
const index_load_failure_prefix = "\x00\x00__metadata__:index_load_failure:";

test "status projection preserves current and legacy persisted snapshots" {
    var raw: [index_status_encoded_len]u8 = undefined;
    const snapshot: IndexStatusSnapshot = .{ .kind = .graph, .doc_count = 7, .edge_count = 11, .graph_counts_pending = true, .updated_at_ns = 23 };
    encodeIndexStatusSnapshot(snapshot, &raw);
    try std.testing.expectEqualDeep(snapshot, try decodeIndexStatusSnapshot(&raw));
    std.mem.writeInt(u64, raw[0..8], index_status_magic_v1, .little);
    var legacy = snapshot;
    legacy.graph_counts_pending = false;
    try std.testing.expectEqualDeep(legacy, try decodeIndexStatusSnapshot(raw[0..64]));
    try std.testing.expectError(error.InvalidIndexStatusSnapshot, decodeIndexStatusSnapshot(&raw));
    std.mem.writeInt(u64, raw[0..8], index_status_magic, .little);
    std.mem.writeInt(u64, raw[64..72], 2, .little);
    try std.testing.expectError(error.InvalidIndexStatusSnapshot, decodeIndexStatusSnapshot(&raw));
}

test "status projection releases candidate progress and metric clones across allocation failures" {
    const F = struct {
        fn run(alloc: Allocator) !void {
            var item: types.DBIndexStats = .{ .name = try alloc.dupe(u8, "graph"), .kind = .graph };
            defer freeDBIndexStatsItem(alloc, item);
            item.algebraic_top_candidate = try cloneAlgebraicCandidateStatusAlloc(alloc, "recommendation", "materialization", "lifecycle", "decision", 1, 2, 3, 4, 5, 6, 7, 8, 9);
            item.algebraic_active_progress = try cloneAlgebraicProgressStatusAlloc(alloc, "recommendation", "materialization", "lifecycle", 9, 8, 7, 10);
            var metric = try cloneGraphMetricStatusFromGraph(alloc, .{ .name = @constCast("metric"), .build_worker_id = "worker", .build_cursor = "cursor", .last_error = "failure" });
            defer metric.deinit(alloc);
            try std.testing.expectEqualStrings("cursor", metric.build_cursor);
            const decision = try cloneAlgebraicCandidateDecisionStatusAlloc(alloc, "recommendation", "materialization", "lifecycle", "previous", "decision", 1, 2, 3, 4, 5, 6, 7);
            defer {
                alloc.free(decision.recommendation);
                alloc.free(decision.materialization_id);
                alloc.free(decision.lifecycle);
                alloc.free(decision.previous_decision);
                alloc.free(decision.decision);
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, F.run, .{});
}

fn dbHbcCacheKindStats(cache_stats: anytype) types.HbcCacheKindStats {
    return .{
        .used_bytes = cache_stats.used_bytes,
        .peak_bytes = cache_stats.peak_bytes,
        .hits = cache_stats.hits,
        .misses = cache_stats.misses,
        .insertions = cache_stats.insertions,
        .replacements = cache_stats.replacements,
        .sampled_admissions = cache_stats.sampled_admissions,
        .admission_skips = cache_stats.admission_skips,
        .evictions = cache_stats.evictions,
    };
}

pub fn dbHbcCacheStats(cache_stats: anytype) types.HbcCacheStats {
    return .{
        .total_bytes = cache_stats.total_bytes,
        .accounted_bytes = cache_stats.accounted_bytes,
        .pinned_bytes = cache_stats.pinned_bytes,
        .node = dbHbcCacheKindStats(cache_stats.node),
        .quantized = dbHbcCacheKindStats(cache_stats.quantized),
        .vector = dbHbcCacheKindStats(cache_stats.vector),
        .metadata = dbHbcCacheKindStats(cache_stats.metadata),
    };
}

pub fn dbHbcPostingStats(backlog: hbc_mod.PostingBacklogStats, profile: hbc_mod.WriteProfile) types.HbcPostingStats {
    return .{
        .scanned_nodes = backlog.scanned_nodes,
        .scanned_postings = backlog.scanned_postings,
        .dirty_postings = backlog.dirty_postings,
        .centroid_dirty_postings = backlog.centroid_dirty_postings,
        .payload_dirty_postings = backlog.payload_dirty_postings,
        .max_centroid_version_lag = backlog.max_centroid_version_lag,
        .max_payload_version_lag = backlog.max_payload_version_lag,
        .max_mutation_version = backlog.max_mutation_version,
        .skipped_missing = backlog.skipped_missing,
        .maintenance_scanned_nodes = profile.posting_maintenance_scanned_nodes,
        .maintenance_scanned_postings = profile.posting_maintenance_scanned_postings,
        .maintenance_dirty_postings = profile.posting_maintenance_dirty_postings,
        .maintenance_repaired_postings = profile.posting_maintenance_repaired_postings,
        .maintenance_centroid_refreshed = profile.posting_maintenance_centroid_refreshed,
        .maintenance_payload_refreshed = profile.posting_maintenance_payload_refreshed,
        .maintenance_ancestor_refresh_roots = profile.posting_maintenance_ancestor_refresh_roots,
        .maintenance_split_postings = profile.posting_maintenance_split_postings,
        .maintenance_merged_postings = profile.posting_maintenance_merged_postings,
        .maintenance_boundary_reassigned_vectors = profile.posting_maintenance_boundary_reassigned_vectors,
        .lazy_centroid_deferrals = profile.posting_lazy_centroid_deferrals,
        .lazy_payload_deferrals = profile.posting_lazy_payload_deferrals,
        .lazy_ancestor_deferrals = profile.posting_lazy_ancestor_deferrals,
    };
}

pub fn projectionCheckpointStatusName(status: apply_state.ProjectionStatus) []const u8 {
    return switch (status) {
        .clean => "clean",
        .rebuilding => "rebuilding",
        .degraded => "degraded",
        .repair_required => "repair_required",
    };
}

pub fn durableGenerationBuildActive(item: *const types.DBIndexStats) bool {
    // A retained status snapshot may predate durable repair-state
    // materialization. The checkpoint is itself durable proof that a
    // non-replay generation build is active and must not be erased by a
    // live replay-counter overlay.
    if (std.mem.eql(u8, item.projection_checkpoint_status, "rebuilding")) return true;
    if (item.index_repair_id != null and
        !std.mem.eql(u8, item.index_repair_phase, @tagName(index_repair_state.Phase.terminal)) and
        !std.mem.eql(u8, item.index_repair_automation, "paused"))
    {
        return true;
    }
    const generation_build = std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.operator_generation_rebuild)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.artifact_baseline_adoption)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.storage_format_migration)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.operator_generation_validation)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.artifact_coverage_mismatch)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.artifact_counter_missing)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.replay_artifact_unavailable)) or
        std.mem.eql(u8, item.index_repair_trigger, @tagName(index_repair_state.Trigger.projection_generation_invalid));
    return generation_build and
        !std.mem.eql(u8, item.index_repair_phase, @tagName(index_repair_state.Phase.terminal));
}
