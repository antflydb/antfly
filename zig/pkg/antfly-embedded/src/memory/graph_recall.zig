// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Bounded weighted personalized PageRank over the authorized induced graph.
//! graph.metrics accepts unweighted topology; memory's relation/provisional
//! policy needs weighted transitions and a std-only portable policy kernel.
const std = @import("std");
const t = @import("types.zig");
pub const Edge = struct { from: usize, to: usize, weight: f64 };
pub fn validate(options: t.GraphRecall) !void {
    if (options.entity_seeds.len > 32 or options.max_hops == 0 or options.max_hops > 4 or options.max_nodes < 2 or options.max_nodes > 512 or options.max_edges == 0 or options.max_edges > 4096 or options.iterations == 0 or options.iterations > 64) return error.InvalidMemoryRequest;
    inline for (.{ options.restart_probability, options.relation_weight, options.provisional_weight }) |weight| if (!std.math.isFinite(weight) or weight <= 0 or weight > 1) return error.InvalidMemoryRequest;
}
pub fn rank(a: std.mem.Allocator, seeds: []const f64, edges: []const Edge, options: t.GraphRecall) ![]f64 {
    try validate(options);
    if (seeds.len > options.max_nodes or edges.len > options.max_edges) return error.MemoryWorkLimit;
    const result = try a.alloc(f64, seeds.len);
    errdefer a.free(result);
    if (seeds.len == 0) return result;
    const mass = try a.alloc(f64, seeds.len);
    defer a.free(mass);
    const outgoing = try a.alloc(f64, seeds.len);
    defer a.free(outgoing);
    const next = try a.alloc(f64, seeds.len);
    defer a.free(next);
    var total: f64 = 0;
    for (seeds) |seed| {
        if (!std.math.isFinite(seed) or seed < 0) return error.InvalidMemoryRequest;
        total += seed;
    }
    for (seeds, mass) |seed, *value| value.* = if (total > 0) seed / total else 1.0 / @as(f64, @floatFromInt(seeds.len));
    @memcpy(result, mass);
    @memset(outgoing, 0);
    for (edges) |edge| {
        if (edge.from >= seeds.len or edge.to >= seeds.len or !std.math.isFinite(edge.weight) or edge.weight <= 0) return error.InvalidMemoryRequest;
        outgoing[edge.from] += edge.weight;
        outgoing[edge.to] += edge.weight;
    }
    for (0..options.iterations) |_| {
        var dangling: f64 = 0;
        for (outgoing, result) |degree, value| if (degree == 0) {
            dangling += value;
        };
        for (mass, next) |seed, *value| value.* = (options.restart_probability + (1 - options.restart_probability) * dangling) * seed;
        for (edges) |edge| {
            const weight = (1 - options.restart_probability) * edge.weight;
            next[edge.to] += weight * result[edge.from] / outgoing[edge.from];
            next[edge.from] += weight * result[edge.to] / outgoing[edge.to];
        }
        @memcpy(result, next);
    }
    return result;
}
test "personalized rank conserves mass, discounts weak links, and is deterministic" {
    const a = std.testing.allocator;
    const options: t.GraphRecall = .{};
    const edges = [_]Edge{ .{ .from = 0, .to = 1, .weight = 1 }, .{ .from = 0, .to = 2, .weight = 0.5 } };
    const scores = try rank(a, &.{ 1, 0, 0, 0 }, &edges, options);
    defer a.free(scores);
    var total: f64 = 0;
    for (scores) |score| total += score;
    try std.testing.expectApproxEqAbs(@as(f64, 1), total, 1e-12);
    try std.testing.expect(scores[1] > scores[2]);
    try std.testing.expectEqual(@as(f64, 0), scores[3]);
    const again = try rank(a, &.{ 1, 0, 0, 0 }, &edges, options);
    defer a.free(again);
    try std.testing.expectEqualSlices(f64, scores, again);
    try std.testing.expectError(error.InvalidMemoryRequest, rank(a, &.{1}, &.{}, .{ .restart_probability = std.math.nan(f64) }));
}
