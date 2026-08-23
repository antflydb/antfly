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
const graph_metric_policy = @import("../build/graph_metric_policy.zig");
const runtime_mod = @import("runtime.zig");

pub const Limits = struct {
    max_top_k: usize = 100_000,
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

fn recordRejectionDiagnostic(segment: metric_segment.Segment) void {
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

const BorrowedScore = struct { node_id: []const u8, value: f64 };
const TopQueue = std.PriorityQueue(BorrowedScore, void, compareWorstFirst);

pub fn scoreAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8, node_id: []const u8) !?Score {
    var loaded = try loadVerifiedAlloc(alloc, session, graph_index_name, metric_name);
    defer loaded.deinit(alloc);
    const value = loaded.score(node_id) orelse return null;
    return .{ .node_id = try alloc.dupe(u8, node_id), .value = value };
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

fn loadVerifiedAlloc(alloc: Allocator, session: *runtime_mod.QuerySession, graph_index_name: []const u8, metric_name: []const u8) !metric_segment.Segment {
    clearLastRejectionDiagnostic();
    try session.checkCancellation();
    const graph_index = session.findNamedArtifactIndex(.graph_segment, graph_index_name) orelse return error.GraphSegmentNotFound;
    const graph_artifact = session.artifactRef(graph_index).?;
    const name = try metric_segment.artifactNameAlloc(alloc, graph_index_name, metric_name);
    defer alloc.free(name);
    const metric_index = session.findNamedArtifactIndex(.graph_metric_segment, name) orelse return error.MetricNotReady;
    const payload = try session.fetchArtifactAlloc(metric_index);
    defer alloc.free(payload);
    var segment = try metric_segment.decodeAlloc(alloc, payload);
    errdefer segment.deinit(alloc);
    if (!std.mem.eql(u8, segment.graph_index_name, graph_index_name) or !std.mem.eql(u8, segment.metric_name, metric_name)) return error.InvalidGraphMetricSegment;
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
