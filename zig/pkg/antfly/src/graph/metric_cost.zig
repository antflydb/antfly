// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy at
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Shared graph-metric kernel cost model. Admission and execution must use the
//! same model so a configured work ceiling remains a real isolation boundary.

const std = @import("std");

pub const Kind = enum {
    degree,
    pagerank,
    eigenvector,
    hits,
};

fn scaled(count: usize, passes: u64) !u64 {
    return std.math.mul(u64, @intCast(count), passes) catch
        error.GraphMetricBuildBudgetExceeded;
}

fn add(left: u64, right: u64) !u64 {
    return std.math.add(u64, left, right) catch
        error.GraphMetricBuildBudgetExceeded;
}

/// Conservative logical work performed by a kernel, including dense-vector
/// setup. A work item is one visited vertex or edge in a full kernel pass.
pub fn kernelWorkItems(kind: Kind, node_count: usize, edge_count: usize, iterations: u32) !u64 {
    const setup_node_passes: u64 = switch (kind) {
        .degree => 0,
        .pagerank => 2, // reciprocal out-degree and initial rank
        .eigenvector => 1,
        .hits => 2,
    };
    const iteration_node_passes: u64 = switch (kind) {
        .degree => 1,
        .pagerank => 2, // sink mass, fused adjacency fill + delta
        .eigenvector => 3, // adjacency fill, norm, fused scale + delta
        .hits => 5, // two fills, two norms, fused paired scale/delta/copy
    };
    const iteration_edge_passes: u64 = switch (kind) {
        .degree => 0,
        .pagerank, .eigenvector => 1,
        .hits => 2,
    };
    const effective_iterations: u64 = if (kind == .degree) 1 else iterations;
    const setup = try scaled(node_count, setup_node_passes);
    const nodes_per_iteration = try scaled(node_count, iteration_node_passes);
    const edges_per_iteration = try scaled(edge_count, iteration_edge_passes);
    const per_iteration = try add(nodes_per_iteration, edges_per_iteration);
    return try add(setup, std.math.mul(u64, per_iteration, effective_iterations) catch
        return error.GraphMetricBuildBudgetExceeded);
}

test "kernel work model accounts for algorithm-specific vector and edge passes" {
    try std.testing.expectEqual(@as(u64, 10), try kernelWorkItems(.degree, 10, 20, 50));
    try std.testing.expectEqual(@as(u64, 100), try kernelWorkItems(.pagerank, 10, 20, 2));
    try std.testing.expectEqual(@as(u64, 110), try kernelWorkItems(.eigenvector, 10, 20, 2));
    try std.testing.expectEqual(@as(u64, 200), try kernelWorkItems(.hits, 10, 20, 2));
}

test "kernel work model rejects overflow" {
    try std.testing.expectError(
        error.GraphMetricBuildBudgetExceeded,
        kernelWorkItems(.hits, std.math.maxInt(usize), std.math.maxInt(usize), 1_000),
    );
}
