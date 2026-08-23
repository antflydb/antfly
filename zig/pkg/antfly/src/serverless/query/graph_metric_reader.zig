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

pub const Limits = struct {
    max_top_k: usize = 100_000,
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

    pub fn deinit(self: *PointScoresResult, alloc: Allocator) void {
        alloc.free(self.scores);
        self.edge_filter.deinit(alloc);
        self.* = undefined;
    }
};

const BorrowedScore = struct { node_id: []const u8, value: f64 };
const TopQueue = std.PriorityQueue(BorrowedScore, void, compareWorstFirst);

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
    const control_bytes = try session.fetchArtifactBlockRangeAlloc(metric_index, cache_mod.graph_metric_control_block_id, 0, control_len);
    defer alloc.free(control_bytes);
    const control = try metric_segment.decodeControl(control_bytes, config.edge_filter);
    try validateControl(control.header, graph_index_name, metric_name, graph_artifact, config);
    if (control.header.materialization_state == .rejected) {
        recordRejectionDiagnostic(control.header);
        return error.GraphMetricMaterializationRejected;
    }

    // Version 1-3 artifacts remain readable during rolling upgrades. New
    // publications use v4 routing and never materialize the complete vector for
    // point/rerank requests.
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
        };
    }

    if (metric_artifact.byte_len < metric_segment.routing_trailer_len) return error.InvalidGraphMetricSegment;
    const trailer_offset = metric_artifact.byte_len - metric_segment.routing_trailer_len;
    const trailer = try session.fetchArtifactBlockRangeAlloc(
        metric_index,
        cache_mod.graph_metric_routing_block_id,
        trailer_offset,
        metric_segment.routing_trailer_len,
    );
    defer alloc.free(trailer);
    const footer_len = try metric_segment.routingFooterLenFromTrailer(metric_artifact.byte_len, trailer);
    const footer_offset = metric_artifact.byte_len - footer_len;
    const footer = try session.fetchArtifactBlockRangeAlloc(
        metric_index,
        cache_mod.graph_metric_routing_block_id,
        footer_offset,
        footer_len,
    );
    defer alloc.free(footer);
    var routing = try metric_segment.decodeRoutingIndexWithCancellationAlloc(alloc, footer, metric_artifact.byte_len, session.cancellation);
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
    const block_for_node = try alloc.alloc(?usize, node_ids.len);
    defer alloc.free(block_for_node);
    const needed = try alloc.alloc(bool, routing.entries.len);
    defer alloc.free(needed);
    @memset(needed, false);
    for (node_ids, 0..) |node_id, i| {
        const block_index = routing.findIndex(node_id);
        block_for_node[i] = block_index;
        if (block_index) |index| needed[index] = true;
    }
    for (routing.entries, 0..) |entry, block_index| {
        if (!needed[block_index]) continue;
        try session.checkCancellation();
        const block = try session.fetchArtifactBlockRangeAlloc(
            metric_index,
            cache_mod.graph_metric_score_block_id,
            entry.offset,
            entry.len,
        );
        defer alloc.free(block);
        const decoded_block = try metric_segment.decodeScoreBlockWithCancellation(block, session.cancellation);
        const expected_score_count = if (block_index + 1 < routing.entries.len)
            metric_segment.score_block_entries
        else
            @as(usize, control.score_count) - block_index * metric_segment.score_block_entries;
        if (decoded_block.len != expected_score_count or
            !std.mem.eql(u8, decoded_block.scores[0].node_id, entry.first_node_id))
        {
            return error.InvalidGraphMetricSegment;
        }
        for (node_ids, block_for_node, 0..) |node_id, maybe_block_index, node_index| {
            if (maybe_block_index != block_index) continue;
            values[node_index] = decoded_block.score(node_id);
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
    };
}

pub fn openAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8) !metric_segment.Segment {
    return try loadVerifiedAlloc(alloc, session, graph_index_name, metric_name);
}

pub fn topAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize) !Result {
    return try topWithLimitsAlloc(alloc, session, graph_index_name, metric_name, top_k, .{});
}

pub fn topWithLimitsAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, top_k: usize, limits: Limits) !Result {
    if (top_k > limits.max_top_k or limits.max_top_k == 0 or limits.max_scores_scanned == 0 or limits.max_result_bytes == 0) return error.GraphMetricQueryBudgetExceeded;
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
    return .{ .scores = scores, .config_fingerprint = loaded.config_fingerprint, .converged = loaded.converged, .iterations_completed = loaded.iterations_completed, .delta = loaded.delta, .edge_filter = edge_filter };
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
