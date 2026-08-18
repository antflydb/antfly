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

//! Storage-independent graph metric kernels.
//!
//! This module deliberately knows nothing about graph indexes, maintenance
//! jobs, HTTP contracts, or persisted generations. Runtime code can map its
//! stable node identifiers to dense ordinals, run a kernel, and persist the
//! returned scores separately.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const WeightedEdge = struct {
    source: usize,
    target: usize,
    weight: f64 = 1.0,
};

pub const PageRankOptions = struct {
    damping: f64 = 0.85,
    tolerance: f64 = 1e-9,
    max_iterations: u32 = 200,
};

pub const PageRankResult = struct {
    scores: []f64,
    iterations: u32,
    converged: bool,

    pub fn deinit(self: *PageRankResult, alloc: Allocator) void {
        alloc.free(self.scores);
        self.* = undefined;
    }
};

pub const PageRankError = error{
    InvalidOptions,
    InvalidEdge,
};

/// Compute weighted PageRank over dense node ordinals in `[0, node_count)`.
///
/// Dangling-node mass is redistributed uniformly, so each completed iteration
/// preserves a probability distribution. Edge order does not affect the
/// result beyond normal floating-point summation noise.
pub fn pageRankAlloc(
    alloc: Allocator,
    node_count: usize,
    edges: []const WeightedEdge,
    options: PageRankOptions,
) (Allocator.Error || PageRankError)!PageRankResult {
    if (!std.math.isFinite(options.damping) or
        options.damping < 0 or
        options.damping >= 1 or
        !std.math.isFinite(options.tolerance) or
        options.tolerance < 0 or
        options.max_iterations == 0)
    {
        return error.InvalidOptions;
    }

    if (node_count == 0) {
        if (edges.len != 0) return error.InvalidEdge;
        return .{
            .scores = try alloc.alloc(f64, 0),
            .iterations = 0,
            .converged = true,
        };
    }

    const outgoing_weight = try alloc.alloc(f64, node_count);
    defer alloc.free(outgoing_weight);
    @memset(outgoing_weight, 0);

    for (edges) |edge| {
        if (edge.source >= node_count or
            edge.target >= node_count or
            !std.math.isFinite(edge.weight) or
            edge.weight <= 0)
        {
            return error.InvalidEdge;
        }
        outgoing_weight[edge.source] += edge.weight;
        if (!std.math.isFinite(outgoing_weight[edge.source])) return error.InvalidEdge;
    }

    var scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    var next = try alloc.alloc(f64, node_count);
    defer alloc.free(next);

    const initial = 1.0 / @as(f64, @floatFromInt(node_count));
    @memset(scores, initial);

    var iteration: u32 = 0;
    while (iteration < options.max_iterations) {
        iteration += 1;

        var dangling_mass: f64 = 0;
        for (scores, outgoing_weight) |score, weight| {
            if (weight == 0) dangling_mass += score;
        }

        const node_count_f: f64 = @floatFromInt(node_count);
        const base = ((1.0 - options.damping) + options.damping * dangling_mass) / node_count_f;
        @memset(next, base);

        for (edges) |edge| {
            next[edge.target] += options.damping * scores[edge.source] * edge.weight / outgoing_weight[edge.source];
        }

        var max_delta: f64 = 0;
        for (scores, next) |score, next_score| {
            max_delta = @max(max_delta, @abs(next_score - score));
        }

        const old_scores = scores;
        scores = next;
        next = old_scores;

        if (max_delta <= options.tolerance) {
            return .{
                .scores = scores,
                .iterations = iteration,
                .converged = true,
            };
        }
    }

    return .{
        .scores = scores,
        .iterations = iteration,
        .converged = false,
    };
}

fn expectApprox(expected: f64, actual: f64, tolerance: f64) !void {
    try std.testing.expect(@abs(expected - actual) <= tolerance);
}

fn scoreSum(scores: []const f64) f64 {
    var sum: f64 = 0;
    for (scores) |score| sum += score;
    return sum;
}

test "pagerank keeps a directed cycle uniform" {
    const edges = [_]WeightedEdge{
        .{ .source = 0, .target = 1 },
        .{ .source = 1, .target = 2 },
        .{ .source = 2, .target = 0 },
    };
    var result = try pageRankAlloc(std.testing.allocator, 3, &edges, .{});
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.converged);
    try std.testing.expectEqual(@as(u32, 1), result.iterations);
    for (result.scores) |score| try expectApprox(1.0 / 3.0, score, 1e-12);
}

test "pagerank ranks a multiply referenced center above leaves" {
    const edges = [_]WeightedEdge{
        .{ .source = 0, .target = 1 },
        .{ .source = 2, .target = 1 },
        .{ .source = 1, .target = 0 },
        .{ .source = 1, .target = 2 },
    };
    var result = try pageRankAlloc(std.testing.allocator, 3, &edges, .{});
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.converged);
    try std.testing.expect(result.scores[1] > result.scores[0]);
    try expectApprox(result.scores[0], result.scores[2], 1e-12);
    try expectApprox(1.0, scoreSum(result.scores), 1e-12);
}

test "pagerank redistributes dangling mass" {
    const edges = [_]WeightedEdge{
        .{ .source = 0, .target = 1 },
    };
    var result = try pageRankAlloc(std.testing.allocator, 3, &edges, .{});
    defer result.deinit(std.testing.allocator);

    try std.testing.expect(result.converged);
    try expectApprox(1.0, scoreSum(result.scores), 1e-12);
    for (result.scores) |score| try std.testing.expect(score > 0);
}

test "pagerank rejects invalid edges and options" {
    const out_of_range = [_]WeightedEdge{.{ .source = 0, .target = 2 }};
    try std.testing.expectError(error.InvalidEdge, pageRankAlloc(std.testing.allocator, 2, &out_of_range, .{}));

    const bad_weight = [_]WeightedEdge{.{ .source = 0, .target = 1, .weight = 0 }};
    try std.testing.expectError(error.InvalidEdge, pageRankAlloc(std.testing.allocator, 2, &bad_weight, .{}));

    try std.testing.expectError(error.InvalidOptions, pageRankAlloc(std.testing.allocator, 2, &.{}, .{ .damping = 1 }));
}
