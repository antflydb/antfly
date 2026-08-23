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

//! Query-time access to immutable graph metric vectors. Every read verifies
//! that the metric was derived from the graph artifact pinned by this session,
//! preventing silent cross-generation score mixing.

const std = @import("std");
const Allocator = std.mem.Allocator;
const metric_segment = @import("../graph_metric_segment/mod.zig");
const graph_mod = @import("../../graph/graph.zig");
const graph_metric_config = @import("../build/graph_metric_config.zig");
const lake_graph_metric = @import("../build/lake_graph_metric.zig");
const graph_metric_policy = @import("../build/graph_metric_policy.zig");
const cache_mod = @import("cache.zig");
const runtime_mod = @import("runtime.zig");
const artifacts_mod = @import("../artifacts/mod.zig");
const manifest_mod = @import("../manifest/mod.zig");

pub const Limits = struct {
    // Keep this aligned with the public query contract. v6 serves the common
    // bounded prefix from its footer tier and retains an exact authenticated
    // scan fallback for unusually large result sets.
    max_top_k: usize = 10_000,
    max_point_scores: usize = 100_000,
    max_scores_scanned: usize = 1_000_000,
    max_result_bytes: usize = 64 * 1024 * 1024,
};

pub const RejectionDiagnostic = struct {
    graph_index_name: [256]u8 = undefined,
    graph_index_name_len: u16 = 0,
    metric_name: [128]u8 = undefined,
    metric_name_len: u8 = 0,
    materializer_fingerprint: u64 = 0,
    reason: metric_segment.RejectionReason = .none,

    pub fn graphIndexName(self: *const RejectionDiagnostic) []const u8 {
        return self.graph_index_name[0..self.graph_index_name_len];
    }

    pub fn metricName(self: *const RejectionDiagnostic) []const u8 {
        return self.metric_name[0..self.metric_name_len];
    }
};

threadlocal var last_rejection: ?RejectionDiagnostic = null;

pub fn clearLastRejectionDiagnostic() void {
    last_rejection = null;
}

pub fn peekLastRejectionDiagnostic() ?RejectionDiagnostic {
    return last_rejection;
}

fn recordRejectionDiagnostic(segment: anytype) void {
    var diagnostic = RejectionDiagnostic{
        .materializer_fingerprint = segment.materializer_fingerprint,
        .reason = segment.rejection_reason,
    };
    const graph_len = @min(segment.graph_index_name.len, diagnostic.graph_index_name.len);
    @memcpy(diagnostic.graph_index_name[0..graph_len], segment.graph_index_name[0..graph_len]);
    diagnostic.graph_index_name_len = @intCast(graph_len);
    const metric_len = @min(segment.metric_name.len, diagnostic.metric_name.len);
    @memcpy(diagnostic.metric_name[0..metric_len], segment.metric_name[0..metric_len]);
    diagnostic.metric_name_len = @intCast(metric_len);
    last_rejection = diagnostic;
}

pub const Score = struct {
    node_id: []u8,
    value: f64,

    pub fn deinit(self: *Score, alloc: Allocator) void {
        alloc.free(self.node_id);
        self.* = undefined;
    }
};

pub const Result = struct {
    scores: []Score,
    config_fingerprint: u64,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
    metadata_version: u16,
    published_generation: u64,
    edge_generation: u64,
    computed_at_ms: u64,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        for (self.scores) |*score| score.deinit(alloc);
        alloc.free(self.scores);
        self.edge_filter.deinit(alloc);
        self.* = undefined;
    }
};

pub const PointScoresResult = struct {
    scores: []?f64,
    config_fingerprint: u64,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
    metadata_version: u16,
    published_generation: u64,
    edge_generation: u64,
    computed_at_ms: u64,

    pub fn deinit(self: *PointScoresResult, alloc: Allocator) void {
        alloc.free(self.scores);
        self.edge_filter.deinit(alloc);
        self.* = undefined;
    }
};

const BorrowedScore = struct { node_id: []const u8, value: f64 };
const TopQueue = std.PriorityQueue(BorrowedScore, void, compareWorstFirst);
const OwnedTopQueue = std.PriorityQueue(Score, void, compareOwnedWorstFirst);
const ScoreFetchRange = struct { first_block: usize, last_block: usize, offset: u64, len: usize };
const max_score_range_requests: usize = 32;
const max_top_score_range_requests: usize = 64;
const coalesced_score_window_bytes: u64 = 8 * 1024 * 1024;

pub fn scoreAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, node_id: []const u8) !?Score {
    const node_ids = [_][]const u8{node_id};
    var loaded = try scoresAlloc(alloc, session, graph_index_name, metric_name, &node_ids);
    defer loaded.deinit(alloc);
    const value = loaded.scores[0] orelse return null;
    return .{ .node_id = try alloc.dupe(u8, node_id), .value = value };
}

/// Resolves many exact node scores with one control/footer read and at most one
/// range fetch per touched score block. Output positions match `node_ids`.
pub fn scoresAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    graph_index_name: []const u8,
    metric_name: []const u8,
    node_ids: []const []const u8,
) !PointScoresResult {
    clearLastRejectionDiagnostic();
    try session.checkCancellation();
    if (node_ids.len > (Limits{}).max_point_scores) return error.GraphMetricQueryBudgetExceeded;
    for (node_ids) |node_id| {
        if (node_id.len == 0 or node_id.len > metric_segment.codec.max_score_node_id_bytes) return error.InvalidGraphMetricNodeId;
    }

    const specs = try graph_metric_config.parseIndexSpecsAlloc(alloc, session.manifest.stats.indexes_json);
    defer graph_metric_config.freeIndexSpecs(alloc, specs);
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const artifact_name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(artifact_name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, artifact_name) orelse return error.MetricNotReady;
    const metric_artifact = session.artifactRef(metric_index) orelse return error.InvalidGraphMetricSegment;

    const control_len = try metric_segment.controlProbeLen(
        metric_artifact.byte_len,
        graph_index_name,
        metric_name,
        graph_artifact.artifact_id,
        graph_artifact.checksum,
        config.edge_filter,
    );
    const control_bytes = try fetchControlAlloc(session, metric_index, metric_artifact, control_len);
    defer alloc.free(control_bytes);
    const control = try metric_segment.decodeControl(control_bytes, config.edge_filter);
    try validateControl(control.header, graph_index_name, metric_name, graph_artifact, config);
    if (control.header.materialization_state == .rejected) {
        recordRejectionDiagnostic(control.header);
        return error.GraphMetricMaterializationRejected;
    }

    // Version 1-3 artifacts remain readable during rolling upgrades. New
    // publications use authenticated v5 routing and never materialize the
    // complete vector for point/rerank requests.
    if (control.header.version < 4) {
        var segment = try loadVerifiedAlloc(alloc, session, graph_index_name, metric_name);
        defer segment.deinit(alloc);
        const values = try alloc.alloc(?f64, node_ids.len);
        errdefer alloc.free(values);
        for (node_ids, 0..) |node_id, i| {
            if (i % 4096 == 0) try session.checkCancellation();
            values[i] = segment.score(node_id);
        }
        var edge_filter = try segment.edge_filter.cloneAlloc(alloc);
        errdefer edge_filter.deinit(alloc);
        return .{
            .scores = values,
            .config_fingerprint = segment.config_fingerprint,
            .converged = segment.converged,
            .iterations_completed = segment.iterations_completed,
            .delta = segment.delta,
            .edge_filter = edge_filter,
            .metadata_version = control.header.version,
            .published_generation = segment.published_generation,
            .edge_generation = segment.edge_generation,
            .computed_at_ms = segment.computed_at_ms,
        };
    }

    const footer_len = try routingFooterLenAlloc(session, metric_index, metric_artifact, control.header.version);
    const footer_offset = metric_artifact.byte_len - footer_len;
    const footer = try fetchRoutingFooterAlloc(session, metric_index, metric_artifact, control.header.version, footer_offset, footer_len);
    defer alloc.free(footer);
    var routing = try metric_segment.decodeRoutingIndexForVersionWithCancellationAlloc(alloc, footer, metric_artifact.byte_len, control.header.version, session.cancellation);
    defer routing.deinit(alloc);
    const expected_blocks = @as(usize, control.score_count) / metric_segment.score_block_entries +
        @intFromBool(@as(usize, control.score_count) % metric_segment.score_block_entries != 0);
    if (routing.entries.len != expected_blocks or routing.footer_offset != footer_offset or
        (routing.entries.len == 0 and routing.footer_offset != control.score_data_offset) or
        (routing.entries.len > 0 and routing.entries[0].offset != control.score_data_offset))
    {
        return error.InvalidGraphMetricSegment;
    }

    const values = try alloc.alloc(?f64, node_ids.len);
    errdefer alloc.free(values);
    @memset(values, null);
    const block_counts = try alloc.alloc(usize, routing.entries.len);
    defer alloc.free(block_counts);
    @memset(block_counts, 0);
    var grouped_count: usize = 0;
    for (node_ids) |node_id| if (routing.findIndex(node_id)) |block_index| {
        block_counts[block_index] += 1;
        grouped_count += 1;
    };
    const block_offsets = try alloc.alloc(usize, routing.entries.len + 1);
    defer alloc.free(block_offsets);
    block_offsets[0] = 0;
    for (block_counts, 0..) |count, block_index| block_offsets[block_index + 1] = block_offsets[block_index] + count;
    const grouped_node_indexes = try alloc.alloc(usize, grouped_count);
    defer alloc.free(grouped_node_indexes);
    const cursors = try alloc.dupe(usize, block_offsets[0..routing.entries.len]);
    defer alloc.free(cursors);
    for (node_ids, 0..) |node_id, node_index| if (routing.findIndex(node_id)) |block_index| {
        grouped_node_indexes[cursors[block_index]] = node_index;
        cursors[block_index] += 1;
    };

    const fetch_ranges = try planScoreFetchRangesAlloc(alloc, routing.entries, block_counts, control.score_data_offset);
    defer alloc.free(fetch_ranges);

    for (fetch_ranges) |range| {
        try session.checkCancellation();
        const payload = try fetchScoreRangeAlloc(alloc, session, metric_index, control.header.version, routing.entries, range);
        defer alloc.free(payload);
        for (range.first_block..range.last_block + 1) |block_index| {
            if (block_counts[block_index] == 0) continue;
            const entry = routing.entries[block_index];
            if (entry.offset < range.offset) return error.InvalidGraphMetricSegment;
            const relative_offset = std.math.cast(usize, entry.offset - range.offset) orelse return error.InvalidGraphMetricSegment;
            const relative_end = std.math.add(usize, relative_offset, entry.len) catch return error.InvalidGraphMetricSegment;
            if (relative_end > payload.len) return error.InvalidGraphMetricSegment;
            const decoded_block = try metric_segment.decodeScoreBlockWithCancellation(payload[relative_offset..relative_end], session.cancellation);
            const expected_score_count = if (block_index + 1 < routing.entries.len)
                metric_segment.score_block_entries
            else
                @as(usize, control.score_count) - block_index * metric_segment.score_block_entries;
            if (decoded_block.len != expected_score_count or
                !std.mem.eql(u8, decoded_block.scores[0].node_id, entry.first_node_id))
            {
                return error.InvalidGraphMetricSegment;
            }
            for (grouped_node_indexes[block_offsets[block_index]..block_offsets[block_index + 1]]) |node_index| {
                values[node_index] = decoded_block.score(node_ids[node_index]);
            }
        }
    }
    var edge_filter = try config.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    return .{
        .scores = values,
        .config_fingerprint = control.header.config_fingerprint,
        .converged = control.header.converged,
        .iterations_completed = control.header.iterations_completed,
        .delta = control.header.delta,
        .edge_filter = edge_filter,
        .metadata_version = control.header.version,
        .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
        .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
        .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
    };
}

fn planScoreFetchRangesAlloc(
    alloc: Allocator,
    entries: []const metric_segment.codec.RoutingEntry,
    block_counts: []const usize,
    score_data_offset: u64,
) ![]ScoreFetchRange {
    if (entries.len != block_counts.len) return error.InvalidGraphMetricSegment;
    var ranges = std.ArrayListUnmanaged(ScoreFetchRange).empty;
    errdefer ranges.deinit(alloc);
    var touched_blocks: usize = 0;
    for (block_counts) |count| touched_blocks += @intFromBool(count != 0);
    if (touched_blocks <= max_score_range_requests) {
        for (entries, block_counts, 0..) |entry, count, block_index| {
            if (count == 0) continue;
            try ranges.append(alloc, .{ .first_block = block_index, .last_block = block_index, .offset = entry.offset, .len = entry.len });
        }
    } else {
        var active_window: ?u64 = null;
        for (entries, block_counts, 0..) |entry, count, block_index| {
            if (count == 0) continue;
            if (entry.offset < score_data_offset) return error.InvalidGraphMetricSegment;
            const window = (entry.offset - score_data_offset) / coalesced_score_window_bytes;
            if (active_window == null or active_window.? != window) {
                try ranges.append(alloc, .{ .first_block = block_index, .last_block = block_index, .offset = entry.offset, .len = entry.len });
                active_window = window;
            } else {
                const range = &ranges.items[ranges.items.len - 1];
                const entry_end = std.math.add(u64, entry.offset, entry.len) catch return error.InvalidGraphMetricSegment;
                range.last_block = block_index;
                range.len = std.math.cast(usize, entry_end - range.offset) orelse return error.GraphMetricQueryBudgetExceeded;
            }
        }
    }
    if (ranges.items.len > max_score_range_requests) return error.GraphMetricQueryBudgetExceeded;
    return try ranges.toOwnedSlice(alloc);
}

fn fetchControlAlloc(
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    artifact: manifest_mod.ArtifactRef,
    expected_len: usize,
) ![]u8 {
    if (artifact.metadata_version >= 5) {
        if (artifact.graph_metric_control_len != expected_len) return error.InvalidGraphMetricSegment;
        const subranges = [_]runtime_mod.AuthenticatedSubrange{.{
            .relative_offset = 0,
            .len = expected_len,
            .checksum = artifact.graph_metric_control_checksum,
        }};
        return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, 0, expected_len, &subranges) catch |err| switch (err) {
            error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
            else => |other| other,
        };
    }
    try session.verifyArtifact(metric_index);
    return try session.fetchArtifactBlockRangeAlloc(metric_index, cache_mod.graph_metric_control_block_id, 0, expected_len);
}

fn routingFooterLenAlloc(
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    artifact: manifest_mod.ArtifactRef,
    segment_version: u16,
) !usize {
    if (segment_version >= 5) {
        if (artifact.metadata_version != segment_version or artifact.graph_metric_routing_footer_len == 0 or
            artifact.graph_metric_routing_footer_len > metric_segment.codec.max_routing_bytes or
            artifact.graph_metric_routing_footer_len > artifact.byte_len)
        {
            return error.InvalidGraphMetricSegment;
        }
        return artifact.graph_metric_routing_footer_len;
    }
    if (artifact.byte_len < metric_segment.routing_trailer_len) return error.InvalidGraphMetricSegment;
    const trailer_offset = artifact.byte_len - metric_segment.routing_trailer_len;
    const trailer = try session.fetchArtifactBlockRangeAlloc(
        metric_index,
        cache_mod.graph_metric_routing_block_id,
        trailer_offset,
        metric_segment.routing_trailer_len,
    );
    defer session.alloc.free(trailer);
    return try metric_segment.routingFooterLenFromTrailer(artifact.byte_len, trailer);
}

fn fetchRoutingFooterAlloc(
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    artifact: manifest_mod.ArtifactRef,
    segment_version: u16,
    footer_offset: u64,
    footer_len: usize,
) ![]u8 {
    if (segment_version >= 5) {
        const subranges = [_]runtime_mod.AuthenticatedSubrange{.{
            .relative_offset = 0,
            .len = footer_len,
            .checksum = artifact.graph_metric_routing_checksum,
        }};
        return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, footer_offset, footer_len, &subranges) catch |err| switch (err) {
            error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
            else => |other| other,
        };
    }
    return try session.fetchArtifactBlockRangeAlloc(metric_index, cache_mod.graph_metric_routing_block_id, footer_offset, footer_len);
}

fn fetchScoreRangeAlloc(
    alloc: Allocator,
    session: *runtime_mod.QuerySession,
    metric_index: usize,
    segment_version: u16,
    entries: []const metric_segment.codec.RoutingEntry,
    range: ScoreFetchRange,
) ![]u8 {
    if (segment_version < 5) {
        return try session.fetchArtifactBlockRangeAlloc(metric_index, cache_mod.graph_metric_score_block_id, range.offset, range.len);
    }
    if (range.first_block > range.last_block or range.last_block >= entries.len) return error.InvalidGraphMetricSegment;
    const subranges = try alloc.alloc(runtime_mod.AuthenticatedSubrange, range.last_block - range.first_block + 1);
    defer alloc.free(subranges);
    var covered: usize = 0;
    for (entries[range.first_block .. range.last_block + 1], 0..) |entry, index| {
        if (entry.offset < range.offset) return error.InvalidGraphMetricSegment;
        const relative_offset = std.math.cast(usize, entry.offset - range.offset) orelse return error.InvalidGraphMetricSegment;
        if (relative_offset != covered) return error.InvalidGraphMetricSegment;
        subranges[index] = .{
            .relative_offset = relative_offset,
            .len = entry.len,
            .checksum = entry.checksum,
        };
        covered = std.math.add(usize, covered, entry.len) catch return error.InvalidGraphMetricSegment;
    }
    if (covered != range.len) return error.InvalidGraphMetricSegment;
    return session.fetchArtifactAuthenticatedRangeAlloc(metric_index, range.offset, range.len, subranges) catch |err| switch (err) {
        error.ArtifactIntegrityMismatch => error.InvalidGraphMetricSegment,
        else => |other| other,
    };
}

fn planAllScoreFetchRangesAlloc(
    alloc: Allocator,
    entries: []const metric_segment.codec.RoutingEntry,
) ![]ScoreFetchRange {
    var ranges = std.ArrayListUnmanaged(ScoreFetchRange).empty;
    errdefer ranges.deinit(alloc);
    for (entries, 0..) |entry, block_index| {
        const entry_end = std.math.add(u64, entry.offset, entry.len) catch return error.InvalidGraphMetricSegment;
        if (ranges.items.len == 0) {
            try ranges.append(alloc, .{
                .first_block = block_index,
                .last_block = block_index,
                .offset = entry.offset,
                .len = entry.len,
            });
            continue;
        }
        const range = &ranges.items[ranges.items.len - 1];
        const range_end = std.math.add(u64, range.offset, range.len) catch return error.InvalidGraphMetricSegment;
        const combined_len = entry_end - range.offset;
        if (entry.offset != range_end or combined_len > coalesced_score_window_bytes) {
            try ranges.append(alloc, .{
                .first_block = block_index,
                .last_block = block_index,
                .offset = entry.offset,
                .len = entry.len,
            });
        } else {
            range.last_block = block_index;
            range.len = std.math.cast(usize, combined_len) orelse return error.GraphMetricQueryBudgetExceeded;
        }
    }
    if (ranges.items.len > max_top_score_range_requests) return error.GraphMetricQueryBudgetExceeded;
    return try ranges.toOwnedSlice(alloc);
}

pub fn openAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8) !metric_segment.Segment {
    return try loadVerifiedAlloc(alloc, session, graph_index_name, metric_name);
}

pub fn topAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize) !Result {
    return try topWithLimitsAlloc(alloc, session, graph_index_name, metric_name, top_k, .{});
}

pub fn topWithLimitsAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize, limits: Limits) !Result {
    if (top_k > limits.max_top_k or limits.max_top_k == 0 or limits.max_result_bytes == 0) return error.GraphMetricQueryBudgetExceeded;

    clearLastRejectionDiagnostic();
    try session.checkCancellation();
    const specs = try graph_metric_config.parseIndexSpecsAlloc(alloc, session.manifest.stats.indexes_json);
    defer graph_metric_config.freeIndexSpecs(alloc, specs);
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const artifact_name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(artifact_name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, artifact_name) orelse return error.MetricNotReady;
    const metric_artifact = session.artifactRef(metric_index) orelse return error.InvalidGraphMetricSegment;
    const control_len = try metric_segment.controlProbeLen(
        metric_artifact.byte_len,
        graph_index_name,
        metric_name,
        graph_artifact.artifact_id,
        graph_artifact.checksum,
        config.edge_filter,
    );
    const control_bytes = try fetchControlAlloc(session, metric_index, metric_artifact, control_len);
    defer alloc.free(control_bytes);
    const control = try metric_segment.decodeControl(control_bytes, config.edge_filter);
    try validateControl(control.header, graph_index_name, metric_name, graph_artifact, config);
    if (control.header.materialization_state == .rejected) {
        recordRejectionDiagnostic(control.header);
        return error.GraphMetricMaterializationRejected;
    }
    // Preserve rolling-upgrade support for the old contiguous vector layout.
    // Current v6 artifacts serve bounded top-k reads from an authenticated
    // footer tier without touching the score vector.
    if (control.header.version < 4) {
        if (control.score_count > limits.max_scores_scanned) return error.GraphMetricQueryBudgetExceeded;
        return try topLegacyWithLimitsAlloc(alloc, session, graph_index_name, metric_name, top_k, limits);
    }
    const footer_len = try routingFooterLenAlloc(session, metric_index, metric_artifact, control.header.version);
    const footer_offset = metric_artifact.byte_len - footer_len;
    const footer = try fetchRoutingFooterAlloc(session, metric_index, metric_artifact, control.header.version, footer_offset, footer_len);
    defer alloc.free(footer);
    var routing = try metric_segment.decodeRoutingIndexForVersionWithCancellationAlloc(alloc, footer, metric_artifact.byte_len, control.header.version, session.cancellation);
    defer routing.deinit(alloc);
    const expected_blocks = @as(usize, control.score_count) / metric_segment.score_block_entries +
        @intFromBool(@as(usize, control.score_count) % metric_segment.score_block_entries != 0);
    if (routing.entries.len != expected_blocks or routing.footer_offset != footer_offset or
        (routing.entries.len == 0 and routing.footer_offset != control.score_data_offset) or
        (routing.entries.len > 0 and routing.entries[0].offset != control.score_data_offset))
    {
        return error.InvalidGraphMetricSegment;
    }

    if (control.header.version >= 6) {
        const expected_top_count = @min(@as(usize, control.score_count), metric_segment.codec.max_persisted_top_entries);
        if (routing.top_scores.len != expected_top_count) return error.InvalidGraphMetricSegment;
        const result_count = @min(top_k, @as(usize, control.score_count));
        if (result_count <= routing.top_scores.len) {
            var result_bytes = std.math.mul(usize, result_count, @sizeOf(Score)) catch return error.GraphMetricQueryBudgetExceeded;
            for (routing.top_scores[0..result_count]) |score| {
                result_bytes = std.math.add(usize, result_bytes, score.node_id.len) catch return error.GraphMetricQueryBudgetExceeded;
            }
            if (result_bytes > limits.max_result_bytes) return error.GraphMetricQueryBudgetExceeded;
            const scores = try alloc.alloc(Score, result_count);
            var initialized: usize = 0;
            errdefer {
                for (scores[0..initialized]) |*score| score.deinit(alloc);
                alloc.free(scores);
            }
            for (routing.top_scores[0..result_count], 0..) |score, index| {
                if (index % 256 == 0) try session.checkCancellation();
                scores[index] = .{ .node_id = try alloc.dupe(u8, score.node_id), .value = score.value };
                initialized += 1;
            }
            var edge_filter = try config.edge_filter.cloneAlloc(alloc);
            errdefer edge_filter.deinit(alloc);
            return .{
                .scores = scores,
                .config_fingerprint = control.header.config_fingerprint,
                .converged = control.header.converged,
                .iterations_completed = control.header.iterations_completed,
                .delta = control.header.delta,
                .edge_filter = edge_filter,
                .metadata_version = control.header.version,
                .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
                .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
                .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
            };
        }
    }

    if (control.score_count > limits.max_scores_scanned) return error.GraphMetricQueryBudgetExceeded;
    var selected = OwnedTopQueue.empty;
    defer selected.deinit(alloc);
    var selected_owned = true;
    defer if (selected_owned) for (selected.items) |*item| item.deinit(alloc);
    try selected.ensureTotalCapacityPrecise(alloc, @min(top_k, @as(usize, control.score_count)));
    var selected_bytes: usize = 0;
    const fetch_ranges = try planAllScoreFetchRangesAlloc(alloc, routing.entries);
    defer alloc.free(fetch_ranges);
    for (fetch_ranges) |range| {
        try session.checkCancellation();
        const payload = try fetchScoreRangeAlloc(alloc, session, metric_index, control.header.version, routing.entries, range);
        defer alloc.free(payload);
        for (range.first_block..range.last_block + 1) |block_index| {
            const entry = routing.entries[block_index];
            if (entry.offset < range.offset) return error.InvalidGraphMetricSegment;
            const relative_offset = std.math.cast(usize, entry.offset - range.offset) orelse return error.InvalidGraphMetricSegment;
            const relative_end = std.math.add(usize, relative_offset, entry.len) catch return error.InvalidGraphMetricSegment;
            if (relative_end > payload.len) return error.InvalidGraphMetricSegment;
            const decoded_block = try metric_segment.decodeScoreBlockWithCancellation(payload[relative_offset..relative_end], session.cancellation);
            const expected_score_count = if (block_index + 1 < routing.entries.len)
                metric_segment.score_block_entries
            else
                @as(usize, control.score_count) - block_index * metric_segment.score_block_entries;
            if (decoded_block.len != expected_score_count or
                (decoded_block.len > 0 and !std.mem.eql(u8, decoded_block.scores[0].node_id, entry.first_node_id)))
            {
                return error.InvalidGraphMetricSegment;
            }
            for (decoded_block.scores) |candidate| {
                const borrowed = BorrowedScore{ .node_id = candidate.node_id, .value = candidate.value };
                const should_insert = selected.count() < top_k or
                    (top_k > 0 and scoreOrder(borrowed, borrowedFromOwned(selected.peek().?)) == .lt);
                if (!should_insert) continue;
                const owned_node_id = try alloc.dupe(u8, candidate.node_id);
                errdefer alloc.free(owned_node_id);
                if (selected.count() == top_k) {
                    var removed = selected.pop().?;
                    selected_bytes -= @sizeOf(Score) + removed.node_id.len;
                    removed.deinit(alloc);
                }
                selected_bytes = std.math.add(usize, selected_bytes, @sizeOf(Score) + owned_node_id.len) catch return error.GraphMetricQueryBudgetExceeded;
                if (selected_bytes > limits.max_result_bytes) return error.GraphMetricQueryBudgetExceeded;
                try selected.push(alloc, .{ .node_id = owned_node_id, .value = candidate.value });
            }
        }
    }
    std.mem.sort(Score, selected.items, {}, lessOwnedScore);
    var edge_filter = try config.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    const scores = try alloc.dupe(Score, selected.items);
    selected_owned = false;
    return .{
        .scores = scores,
        .config_fingerprint = control.header.config_fingerprint,
        .converged = control.header.converged,
        .iterations_completed = control.header.iterations_completed,
        .delta = control.header.delta,
        .edge_filter = edge_filter,
        .metadata_version = control.header.version,
        .published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version,
        .edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version,
        .computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms),
    };
}

fn topLegacyWithLimitsAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize, limits: Limits) !Result {
    var loaded = try loadVerifiedAlloc(alloc, session, graph_index_name, metric_name);
    defer loaded.deinit(alloc);
    if (loaded.scores.len > limits.max_scores_scanned) return error.GraphMetricQueryBudgetExceeded;

    var selected = TopQueue.empty;
    defer selected.deinit(alloc);
    try selected.ensureTotalCapacityPrecise(alloc, @min(top_k, loaded.scores.len));
    for (loaded.scores, 0..) |candidate, i| {
        if (i % 4096 == 0) try session.checkCancellation();
        const borrowed = BorrowedScore{ .node_id = candidate.node_id, .value = candidate.value };
        if (selected.count() < top_k) {
            try selected.push(alloc, borrowed);
        } else if (top_k > 0 and scoreOrder(borrowed, selected.peek().?) == .lt) {
            _ = selected.pop();
            try selected.push(alloc, borrowed);
        }
    }
    std.mem.sort(BorrowedScore, selected.items, {}, lessScore);
    var result_bytes = std.math.mul(usize, selected.items.len, @sizeOf(Score)) catch return error.GraphMetricQueryBudgetExceeded;
    for (selected.items) |item| result_bytes = std.math.add(usize, result_bytes, item.node_id.len) catch return error.GraphMetricQueryBudgetExceeded;
    if (result_bytes > limits.max_result_bytes) return error.GraphMetricQueryBudgetExceeded;
    var edge_filter = try loaded.edge_filter.cloneAlloc(alloc);
    errdefer edge_filter.deinit(alloc);
    const scores = try alloc.alloc(Score, selected.items.len);
    errdefer alloc.free(scores);
    var initialized: usize = 0;
    errdefer for (scores[0..initialized]) |*item| item.deinit(alloc);
    for (selected.items, 0..) |item, i| {
        scores[i] = .{ .node_id = try alloc.dupe(u8, item.node_id), .value = item.value };
        initialized += 1;
    }
    return .{ .scores = scores, .config_fingerprint = loaded.config_fingerprint, .converged = loaded.converged, .iterations_completed = loaded.iterations_completed, .delta = loaded.delta, .edge_filter = edge_filter, .metadata_version = loaded.metadata_version, .published_generation = loaded.published_generation, .edge_generation = loaded.edge_generation, .computed_at_ms = loaded.computed_at_ms };
}

fn findConfig(
    specs: []graph_metric_config.IndexSpec,
    graph_index_name: []const u8,
    metric_name: []const u8,
) ?graph_mod.GraphMetricConfig {
    for (specs) |spec| {
        if (!std.mem.eql(u8, spec.index_name, graph_index_name)) continue;
        for (spec.configs) |config| if (std.mem.eql(u8, config.name, metric_name)) return config;
        return null;
    }
    return null;
}

fn validateControl(
    header: metric_segment.codec.Header,
    graph_index_name: []const u8,
    metric_name: []const u8,
    graph_artifact: anytype,
    config: graph_mod.GraphMetricConfig,
) !void {
    if (!std.mem.eql(u8, header.graph_index_name, graph_index_name) or
        !std.mem.eql(u8, header.metric_name, metric_name)) return error.InvalidGraphMetricSegment;
    if (header.kind != config.kind or header.config_fingerprint != lake_graph_metric.configFingerprint(config)) {
        return error.MetricStale;
    }
    if (!std.mem.eql(u8, header.source_graph_artifact_id, graph_artifact.artifact_id) or
        !std.mem.eql(u8, header.source_graph_checksum, graph_artifact.checksum)) return error.MetricStale;
    if (header.materializer_fingerprint != graph_metric_policy.materializerFingerprint(.{})) {
        return error.GraphMetricPolicyStale;
    }
}

fn loadVerifiedAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8) !metric_segment.Segment {
    clearLastRejectionDiagnostic();
    try session.checkCancellation();
    const specs = try graph_metric_config.parseIndexSpecsAlloc(alloc, session.manifest.stats.indexes_json);
    defer graph_metric_config.freeIndexSpecs(alloc, specs);
    const config = findConfig(specs, graph_index_name, metric_name) orelse return error.MetricNotConfigured;
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, name) orelse return error.MetricNotReady;
    const payload = try session.fetchArtifactAlloc(metric_index);
    defer alloc.free(payload);
    var segment = try metric_segment.decodeAllocWithCancellation(alloc, payload, session.cancellation);
    errdefer segment.deinit(alloc);
    const metric_artifact = session.artifactRef(metric_index) orelse return error.InvalidGraphMetricSegment;
    // The authenticated payload owns its schema version. Manifest provenance
    // is optional and must not override the decoded wire contract.
    segment.published_generation = if (metric_artifact.published_generation != 0) metric_artifact.published_generation else session.manifest.version;
    segment.edge_generation = if (metric_artifact.edge_generation != 0) metric_artifact.edge_generation else session.manifest.version;
    segment.computed_at_ms = if (metric_artifact.computed_at_ms != 0) metric_artifact.computed_at_ms else @divTrunc(session.manifest.built_at_ns, std.time.ns_per_ms);
    if (!std.mem.eql(u8, segment.graph_index_name, graph_index_name) or !std.mem.eql(u8, segment.metric_name, metric_name)) return error.InvalidGraphMetricSegment;
    if (segment.kind != config.kind or segment.config_fingerprint != lake_graph_metric.configFingerprint(config) or
        !segment.edge_filter.equivalent(config.edge_filter)) return error.MetricStale;
    if (!std.mem.eql(u8, segment.source_graph_artifact_id, graph_artifact.artifact_id) or !std.mem.eql(u8, segment.source_graph_checksum, graph_artifact.checksum)) return error.MetricStale;
    if (segment.materializer_fingerprint != graph_metric_policy.materializerFingerprint(.{})) return error.GraphMetricPolicyStale;
    if (segment.materialization_state == .rejected) return switch (segment.rejection_reason) {
        .build_budget_exceeded => {
            recordRejectionDiagnostic(segment);
            return error.GraphMetricMaterializationRejected;
        },
        .none => error.InvalidGraphMetricSegment,
    };
    return segment;
}

test "serverless graph metric range planning caps broad point batches" {
    const alloc = std.testing.allocator;
    var entries: [64]metric_segment.codec.RoutingEntry = undefined;
    var counts: [64]usize = @splat(1);
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * (4 * 1024 * 1024),
        .len = 1024,
    };
    const broad = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0);
    defer alloc.free(broad);
    try std.testing.expectEqual(max_score_range_requests, broad.len);
    for (broad) |range| try std.testing.expectEqual(@as(usize, 2), range.last_block - range.first_block + 1);

    @memset(&counts, 0);
    counts[0] = 1;
    counts[counts.len - 1] = 1;
    const sparse = try planScoreFetchRangesAlloc(alloc, &entries, &counts, 0);
    defer alloc.free(sparse);
    try std.testing.expectEqual(@as(usize, 2), sparse.len);
    try std.testing.expectEqual(@as(usize, 1024), sparse[0].len);
    try std.testing.expectEqual(@as(usize, 1024), sparse[1].len);
}

test "serverless graph metric top range planning coalesces contiguous blocks" {
    const alloc = std.testing.allocator;
    var entries: [977]metric_segment.codec.RoutingEntry = undefined;
    for (&entries, 0..) |*entry, index| entry.* = .{
        .first_node_id = "node",
        .offset = index * 256 * 1024,
        .len = 256 * 1024,
    };
    const ranges = try planAllScoreFetchRangesAlloc(alloc, &entries);
    defer alloc.free(ranges);
    try std.testing.expectEqual(@as(usize, 31), ranges.len);
    for (ranges) |range| try std.testing.expect(range.len <= coalesced_score_window_bytes);
}

test "serverless graph metric point reads authenticate before fetching ranges" {
    const alloc = std.testing.allocator;
    const State = struct {
        verify_calls: usize = 0,
        range_calls: usize = 0,

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedPut;
        }
        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedFullRead;
        }
        fn getRangeAlloc(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.range_calls += 1;
            return error.UnexpectedRangeRead;
        }
        fn stat(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedStat;
        }
        fn verifyContent(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: []const u8, _: @import("../../common/cancellation.zig").CancellationToken) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.verify_calls += 1;
            return error.ArtifactIntegrityMismatch;
        }
        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedDelete;
        }

        const vtable = artifacts_mod.ArtifactStore.VTable{
            .deinit = deinit,
            .put = put,
            .get_alloc = getAlloc,
            .get_range_alloc = getRangeAlloc,
            .stat = stat,
            .verify_content = verifyContent,
            .delete = delete,
        };
    };
    const checksum = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    const artifact_id = "sha256:" ++ checksum;
    var state = State{};
    var artifacts = artifacts_mod.ArtifactStore{ .allocator = alloc, .ptr = &state, .vtable = &State.vtable };
    defer artifacts.deinit();
    var refs = [_]manifest_mod.ArtifactRef{
        .{ .kind = .graph_segment, .name = "graph_idx", .artifact_id = artifact_id, .byte_len = 1, .checksum = checksum },
        .{ .kind = .graph_metric_segment, .name = "9:graph_idx4:rank", .artifact_id = artifact_id, .byte_len = 100, .checksum = checksum },
    };
    var session = runtime_mod.QuerySession{
        .alloc = alloc,
        .artifacts = &artifacts,
        .manifest = .{
            .namespace = "docs",
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 1,
            .wal_end_lsn = 1,
            .stats = .{ .indexes_json = @constCast("{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}") },
            .artifacts = &refs,
        },
    };
    const node_ids = [_][]const u8{"node"};
    try std.testing.expectError(error.ArtifactIntegrityMismatch, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    try std.testing.expectEqual(@as(usize, 1), state.verify_calls);
    try std.testing.expectEqual(@as(usize, 0), state.range_calls);
}

test "serverless graph metric v6 point and top reads authenticate bounded ranges without full scans" {
    const alloc = std.testing.allocator;
    const source_checksum = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const source_artifact_id = "sha256:" ++ source_checksum;
    const config = graph_mod.GraphMetricConfig{ .name = "rank" };
    var metric = metric_segment.Segment{
        .graph_index_name = try alloc.dupe(u8, "graph_idx"),
        .metric_name = try alloc.dupe(u8, "rank"),
        .kind = .pagerank,
        .source_graph_artifact_id = try alloc.dupe(u8, source_artifact_id),
        .source_graph_checksum = try alloc.dupe(u8, source_checksum),
        .config_fingerprint = lake_graph_metric.configFingerprint(config),
        .materializer_fingerprint = graph_metric_policy.materializerFingerprint(.{}),
        .edge_filter = .{},
        .converged = true,
        .iterations_completed = 2,
        .delta = 0.001,
        .scores = try alloc.alloc(metric_segment.Score, 2),
    };
    metric.scores[0] = .{ .node_id = try alloc.dupe(u8, "a"), .value = 0.25 };
    metric.scores[1] = .{ .node_id = try alloc.dupe(u8, "b"), .value = 0.75 };
    defer metric.deinit(alloc);
    const payload = try metric_segment.encodeAlloc(alloc, metric);
    defer alloc.free(payload);
    const integrity = try metric_segment.artifactIntegrity(metric, payload);
    var metric_digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(payload, &metric_digest, .{});
    const metric_checksum = std.fmt.bytesToHex(metric_digest, .lower);
    var metric_artifact_id: [artifacts_mod.store.sha256_artifact_id_prefix.len + metric_checksum.len]u8 = undefined;
    @memcpy(metric_artifact_id[0..artifacts_mod.store.sha256_artifact_id_prefix.len], artifacts_mod.store.sha256_artifact_id_prefix);
    @memcpy(metric_artifact_id[artifacts_mod.store.sha256_artifact_id_prefix.len..], &metric_checksum);

    const State = struct {
        payload: []const u8,
        control_len: usize,
        footer_offset: usize,
        range_calls: usize = 0,
        verify_calls: usize = 0,
        corrupt_score_reads: bool = false,

        fn deinit(_: Allocator, _: *anyopaque) void {}
        fn put(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedPut;
        }
        fn getAlloc(_: *anyopaque, _: Allocator, _: []const u8) ![]u8 {
            return error.UnexpectedFullRead;
        }
        fn getRangeAlloc(ptr: *anyopaque, result_alloc: Allocator, _: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.range_calls += 1;
            const start = std.math.cast(usize, offset) orelse return error.InvalidRange;
            if (start > self.payload.len or len > self.payload.len - start) return error.InvalidRange;
            const out = try result_alloc.dupe(u8, self.payload[start..][0..len]);
            if (self.corrupt_score_reads and start >= self.control_len and start < self.footer_offset and out.len > @sizeOf(u32)) {
                out[@sizeOf(u32)] ^= 0x01;
            }
            return out;
        }
        fn stat(_: *anyopaque, _: Allocator, _: []const u8) !artifacts_mod.ArtifactMetadata {
            return error.UnexpectedStat;
        }
        fn verifyContent(ptr: *anyopaque, _: Allocator, _: []const u8, _: u64, _: []const u8, _: @import("../../common/cancellation.zig").CancellationToken) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.verify_calls += 1;
            return error.UnexpectedFullVerification;
        }
        fn delete(_: *anyopaque, _: []const u8) !void {
            return error.UnexpectedDelete;
        }

        const vtable = artifacts_mod.ArtifactStore.VTable{
            .deinit = deinit,
            .put = put,
            .get_alloc = getAlloc,
            .get_range_alloc = getRangeAlloc,
            .stat = stat,
            .verify_content = verifyContent,
            .delete = delete,
        };
    };
    var state = State{
        .payload = payload,
        .control_len = integrity.control_len,
        .footer_offset = payload.len - integrity.routing_footer_len,
    };
    var artifacts = artifacts_mod.ArtifactStore{ .allocator = alloc, .ptr = &state, .vtable = &State.vtable };
    defer artifacts.deinit();
    var refs = [_]manifest_mod.ArtifactRef{
        .{ .kind = .graph_segment, .name = "graph_idx", .artifact_id = source_artifact_id, .byte_len = 1, .checksum = source_checksum },
        .{
            .kind = .graph_metric_segment,
            .name = "9:graph_idx4:rank",
            .artifact_id = &metric_artifact_id,
            .byte_len = payload.len,
            .checksum = &metric_checksum,
            .metadata_version = metric_segment.wire_version,
            .materializer_fingerprint = metric.materializer_fingerprint,
            .graph_metric_control_len = integrity.control_len,
            .graph_metric_routing_footer_len = integrity.routing_footer_len,
            .graph_metric_control_checksum = integrity.control_checksum,
            .graph_metric_routing_checksum = integrity.routing_checksum,
            .graph_metric_config_fingerprint = metric.config_fingerprint,
            .graph_metric_source_checksum = @splat(0xaa),
        },
    };
    var session = runtime_mod.QuerySession{
        .alloc = alloc,
        .artifacts = &artifacts,
        .manifest = .{
            .namespace = "docs",
            .version = 1,
            .built_at_ns = 1,
            .wal_start_lsn = 1,
            .wal_end_lsn = 1,
            .stats = .{ .indexes_json = @constCast("{\"graph_idx\":{\"type\":\"graph\",\"metrics\":{\"rank\":{\"kind\":\"pagerank\"}}}}") },
            .artifacts = &refs,
        },
    };
    const node_ids = [_][]const u8{"b"};
    var result = try scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids);
    defer result.deinit(alloc);
    try std.testing.expectEqual(@as(?f64, 0.75), result.scores[0]);
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls);
    try std.testing.expectEqual(@as(usize, 3), state.range_calls);

    state.range_calls = 0;
    var top = try topWithLimitsAlloc(alloc, &session, "graph_idx", "rank", 2, .{ .max_scores_scanned = 0 });
    defer top.deinit(alloc);
    try std.testing.expectEqual(@as(usize, 2), top.scores.len);
    try std.testing.expectEqualStrings("b", top.scores[0].node_id);
    try std.testing.expectEqual(@as(f64, 0.75), top.scores[0].value);
    try std.testing.expectEqualStrings("a", top.scores[1].node_id);
    try std.testing.expectEqual(@as(usize, 2), state.range_calls);
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls);

    state.corrupt_score_reads = true;
    try std.testing.expectError(error.InvalidGraphMetricSegment, scoresAlloc(alloc, &session, "graph_idx", "rank", &node_ids));
    try std.testing.expectEqual(@as(usize, 0), state.verify_calls);
}

fn scoreOrder(a: BorrowedScore, b: BorrowedScore) std.math.Order {
    // Descending score, then ascending node id for deterministic ties.
    const value_order = std.math.order(b.value, a.value);
    if (value_order != .eq) return value_order;
    return std.mem.order(u8, a.node_id, b.node_id);
}
fn compareWorstFirst(_: void, a: BorrowedScore, b: BorrowedScore) std.math.Order {
    return scoreOrder(b, a);
}
fn lessScore(_: void, a: BorrowedScore, b: BorrowedScore) bool {
    return scoreOrder(a, b) == .lt;
}

fn borrowedFromOwned(score: Score) BorrowedScore {
    return .{ .node_id = score.node_id, .value = score.value };
}

fn compareOwnedWorstFirst(_: void, a: Score, b: Score) std.math.Order {
    return scoreOrder(borrowedFromOwned(b), borrowedFromOwned(a));
}

fn lessOwnedScore(_: void, a: Score, b: Score) bool {
    return scoreOrder(borrowedFromOwned(a), borrowedFromOwned(b)) == .lt;
}
