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

const std = @import("std");
const Allocator = std.mem.Allocator;
const graph_mod = @import("../../graph/graph.zig");

pub const Score = struct {
    node_id: []u8,
    value: f64,

    pub fn deinit(self: *Score, alloc: Allocator) void {
        alloc.free(self.node_id);
        self.* = undefined;
    }
};

pub const MaterializationState = enum(u8) {
    ready = 0,
    rejected = 1,
};

pub const RejectionReason = enum(u8) {
    none = 0,
    build_budget_exceeded = 1,
};

/// An immutable metric vector tied to the exact graph artifact from which it
/// was computed. Scores are sorted by node id for deterministic encoding and
/// binary-search point lookups without a per-request hash table.
pub const Segment = struct {
    graph_index_name: []u8,
    metric_name: []u8,
    kind: graph_mod.GraphMetricKind,
    source_graph_artifact_id: []u8,
    source_graph_checksum: []u8,
    config_fingerprint: u64,
    /// Identifies the implementation and admission policy that produced this
    /// artifact. A changed runtime policy invalidates terminal rejections and
    /// safely causes a rebuild without user configuration churn.
    materializer_fingerprint: u64 = 0,
    materialization_state: MaterializationState = .ready,
    rejection_reason: RejectionReason = .none,
    edge_filter: graph_mod.GraphMetricEdgeFilter,
    converged: bool,
    iterations_completed: u32,
    delta: f64,
    scores: []Score,

    pub fn deinit(self: *Segment, alloc: Allocator) void {
        alloc.free(self.graph_index_name);
        alloc.free(self.metric_name);
        alloc.free(self.source_graph_artifact_id);
        alloc.free(self.source_graph_checksum);
        self.edge_filter.deinit(alloc);
        for (self.scores) |*item| item.deinit(alloc);
        alloc.free(self.scores);
        self.* = undefined;
    }

    pub fn score(self: Segment, node_id: []const u8) ?f64 {
        var low: usize = 0;
        var high = self.scores.len;
        while (low < high) {
            const mid = low + (high - low) / 2;
            switch (std.mem.order(u8, self.scores[mid].node_id, node_id)) {
                .lt => low = mid + 1,
                .gt => high = mid,
                .eq => return self.scores[mid].value,
            }
        }
        return null;
    }
};

pub fn freeSegment(alloc: Allocator, segment: *Segment) void {
    segment.deinit(alloc);
}

/// Length-prefix both components so index and metric names cannot alias even
/// when they contain separators used by human-readable artifact names.
pub fn artifactNameAlloc(alloc: Allocator, graph_index_name: []const u8, metric_name: []const u8) ![]u8 {
    if (graph_index_name.len == 0 or metric_name.len == 0) return error.InvalidGraphMetricArtifactName;
    return try std.fmt.allocPrint(alloc, "{d}:{s}{d}:{s}", .{ graph_index_name.len, graph_index_name, metric_name.len, metric_name });
}
