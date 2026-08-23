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

//! Storage-independent, bounded graph metric kernels. Persistence and work
//! scheduling deliberately live outside this module so the same algorithms can
//! be used by embedded and immutable lake-native graph implementations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const CancellationToken = @import("../common/cancellation.zig").CancellationToken;

pub const Edge = struct {
    source: usize,
    target: usize,
};

pub const Options = struct {
    damping: f64 = 0.85,
    tolerance: f64 = 0.000001,
    max_iterations: u32 = 50,
    max_nodes: usize = 1_000_000,
    max_edges: usize = 10_000_000,
    max_work_items: u64 = 500_000_000,
    cancellation: CancellationToken = .none,
};

pub const Result = struct {
    scores: []f64,
    iterations_completed: u32,
    converged: bool,
    delta: f64,

    pub fn deinit(self: *Result, alloc: Allocator) void {
        alloc.free(self.scores);
        self.* = undefined;
    }
};

pub const HitsResult = struct {
    authorities: []f64,
    hubs: []f64,
    iterations_completed: u32,
    converged: bool,
    delta: f64,

    pub fn deinit(self: *HitsResult, alloc: Allocator) void {
        alloc.free(self.authorities);
        alloc.free(self.hubs);
        self.* = undefined;
    }
};

fn validate(node_count: usize, edges: []const Edge, options: Options) !u64 {
    if (node_count > options.max_nodes or edges.len > options.max_edges) return error.GraphMetricBuildBudgetExceeded;
    if (!std.math.isFinite(options.damping) or options.damping < 0 or options.damping >= 1 or
        !std.math.isFinite(options.tolerance) or options.tolerance < 0 or
        options.max_iterations == 0 or options.max_iterations > 1_000 or
        options.max_nodes == 0 or options.max_edges == 0 or options.max_work_items == 0)
    {
        return error.InvalidGraphMetricOptions;
    }
    for (edges, 0..) |edge, i| {
        if (i % 4096 == 0) try options.cancellation.check();
        if (edge.source >= node_count or edge.target >= node_count) return error.InvalidGraphMetricEdge;
    }
    const per_iteration = std.math.add(u64, @intCast(node_count), @intCast(edges.len)) catch
        return error.GraphMetricBuildBudgetExceeded;
    return per_iteration;
}

fn admitWork(per_iteration: u64, iterations: u32, passes: u64, max_work_items: u64) !void {
    const iteration_work = std.math.mul(u64, per_iteration, passes) catch
        return error.GraphMetricBuildBudgetExceeded;
    const work = std.math.mul(u64, iteration_work, iterations) catch
        return error.GraphMetricBuildBudgetExceeded;
    if (work > max_work_items) return error.GraphMetricBuildBudgetExceeded;
}

pub fn degreeAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !Result {
    const per_iteration = try validate(node_count, edges, options);
    try admitWork(per_iteration, 1, 1, options.max_work_items);
    const scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    @memset(scores, 0);
    for (edges, 0..) |edge, i| {
        if (i % 4096 == 0) try options.cancellation.check();
        scores[edge.source] += 1;
        scores[edge.target] += 1;
    }
    return .{ .scores = scores, .iterations_completed = 1, .converged = true, .delta = 0 };
}

pub fn pageRankAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !Result {
    const per_iteration = try validate(node_count, edges, options);
    try admitWork(per_iteration, options.max_iterations, 2, options.max_work_items);
    var scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    if (node_count == 0) return .{ .scores = scores, .iterations_completed = 0, .converged = true, .delta = 0 };
    var next = try alloc.alloc(f64, node_count);
    defer alloc.free(next);
    const out_degree = try alloc.alloc(u64, node_count);
    defer alloc.free(out_degree);
    @memset(out_degree, 0);
    for (edges, 0..) |edge, i| {
        if (i % 4096 == 0) try options.cancellation.check();
        out_degree[edge.source] += 1;
    }
    const count: f64 = @floatFromInt(node_count);
    @memset(scores, 1.0 / count);

    var iteration: u32 = 0;
    var delta: f64 = 0;
    while (iteration < options.max_iterations) {
        try options.cancellation.check();
        iteration += 1;
        var sink_mass: f64 = 0;
        for (scores, out_degree, 0..) |score, degree, i| {
            if (i % 4096 == 0) try options.cancellation.check();
            if (degree == 0) sink_mass += score;
        }
        const base = (1.0 - options.damping + options.damping * sink_mass) / count;
        @memset(next, base);
        for (edges, 0..) |edge, i| {
            if (i % 4096 == 0) try options.cancellation.check();
            next[edge.target] += options.damping * scores[edge.source] / @as(f64, @floatFromInt(out_degree[edge.source]));
        }
        delta = try swapAndDelta(&scores, &next, options.cancellation);
        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        if (delta <= options.tolerance) return .{ .scores = scores, .iterations_completed = iteration, .converged = true, .delta = delta };
    }
    return .{ .scores = scores, .iterations_completed = iteration, .converged = false, .delta = delta };
}

pub fn eigenvectorAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !Result {
    const per_iteration = try validate(node_count, edges, options);
    try admitWork(per_iteration, options.max_iterations, 2, options.max_work_items);
    var scores = try alloc.alloc(f64, node_count);
    errdefer alloc.free(scores);
    if (node_count == 0) return .{ .scores = scores, .iterations_completed = 0, .converged = true, .delta = 0 };
    var next = try alloc.alloc(f64, node_count);
    defer alloc.free(next);
    @memset(scores, 1.0 / @sqrt(@as(f64, @floatFromInt(node_count))));
    var iteration: u32 = 0;
    var delta: f64 = 0;
    while (iteration < options.max_iterations) {
        try options.cancellation.check();
        iteration += 1;
        @memset(next, 0);
        for (edges, 0..) |edge, i| {
            if (i % 4096 == 0) try options.cancellation.check();
            next[edge.target] += scores[edge.source];
        }
        try normalize(next, options.cancellation);
        delta = try swapAndDelta(&scores, &next, options.cancellation);
        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        if (delta <= options.tolerance) return .{ .scores = scores, .iterations_completed = iteration, .converged = true, .delta = delta };
    }
    return .{ .scores = scores, .iterations_completed = iteration, .converged = false, .delta = delta };
}

pub fn hitsAlloc(alloc: Allocator, node_count: usize, edges: []const Edge, options: Options) !HitsResult {
    const per_iteration = try validate(node_count, edges, options);
    try admitWork(per_iteration, options.max_iterations, 2, options.max_work_items);
    const authorities = try alloc.alloc(f64, node_count);
    errdefer alloc.free(authorities);
    const hubs = try alloc.alloc(f64, node_count);
    errdefer alloc.free(hubs);
    if (node_count == 0) return .{ .authorities = authorities, .hubs = hubs, .iterations_completed = 0, .converged = true, .delta = 0 };
    var next_authorities = try alloc.alloc(f64, node_count);
    defer alloc.free(next_authorities);
    var next_hubs = try alloc.alloc(f64, node_count);
    defer alloc.free(next_hubs);
    const initial = 1.0 / @sqrt(@as(f64, @floatFromInt(node_count)));
    @memset(authorities, initial);
    @memset(hubs, initial);
    var iteration: u32 = 0;
    var delta: f64 = 0;
    while (iteration < options.max_iterations) {
        try options.cancellation.check();
        iteration += 1;
        @memset(next_authorities, 0);
        @memset(next_hubs, 0);
        for (edges, 0..) |edge, i| {
            if (i % 4096 == 0) try options.cancellation.check();
            next_authorities[edge.target] += hubs[edge.source];
        }
        try normalize(next_authorities, options.cancellation);
        for (edges, 0..) |edge, i| {
            if (i % 4096 == 0) try options.cancellation.check();
            next_hubs[edge.source] += next_authorities[edge.target];
        }
        try normalize(next_hubs, options.cancellation);
        delta = 0;
        for (0..node_count) |i| {
            if (i % 4096 == 0) try options.cancellation.check();
            delta += @abs(next_authorities[i] - authorities[i]);
            delta += @abs(next_hubs[i] - hubs[i]);
            authorities[i] = next_authorities[i];
            hubs[i] = next_hubs[i];
        }
        if (!std.math.isFinite(delta)) return error.InvalidGraphMetricScore;
        if (delta <= options.tolerance) return .{ .authorities = authorities, .hubs = hubs, .iterations_completed = iteration, .converged = true, .delta = delta };
    }
    return .{ .authorities = authorities, .hubs = hubs, .iterations_completed = iteration, .converged = false, .delta = delta };
}

fn normalize(values: []f64, cancellation: CancellationToken) !void {
    var norm_sq: f64 = 0;
    for (values, 0..) |value, i| {
        if (i % 4096 == 0) try cancellation.check();
        norm_sq += value * value;
    }
    const norm = @sqrt(norm_sq);
    if (norm > 0) {
        for (values, 0..) |*value, i| {
            if (i % 4096 == 0) try cancellation.check();
            value.* /= norm;
        }
    }
}

fn swapAndDelta(current: *[]f64, next: *[]f64, cancellation: CancellationToken) !f64 {
    var delta: f64 = 0;
    for (current.*, next.*, 0..) |old, new, i| {
        if (i % 4096 == 0) try cancellation.check();
        delta += @abs(new - old);
    }
    const previous = current.*;
    current.* = next.*;
    next.* = previous;
    return delta;
}

test "serverless bounded graph metric kernels compute all supported metrics" {
    const edges = [_]Edge{ .{ .source = 0, .target = 1 }, .{ .source = 2, .target = 1 }, .{ .source = 1, .target = 0 } };
    var degree = try degreeAlloc(std.testing.allocator, 3, &edges, .{});
    defer degree.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(f64, 3), degree.scores[1]);
    var pagerank = try pageRankAlloc(std.testing.allocator, 3, &edges, .{});
    defer pagerank.deinit(std.testing.allocator);
    try std.testing.expect(pagerank.scores[1] > pagerank.scores[2]);
    var eigenvector = try eigenvectorAlloc(std.testing.allocator, 3, &edges, .{});
    defer eigenvector.deinit(std.testing.allocator);
    try std.testing.expect(eigenvector.iterations_completed > 0);
    var hits = try hitsAlloc(std.testing.allocator, 3, &edges, .{});
    defer hits.deinit(std.testing.allocator);
    try std.testing.expect(hits.authorities[1] > hits.authorities[2]);
}

test "serverless graph metric kernels reject unbounded work before allocating" {
    try std.testing.expectError(error.GraphMetricBuildBudgetExceeded, pageRankAlloc(std.testing.allocator, 2, &.{}, .{ .max_nodes = 1 }));
    try std.testing.expectError(error.InvalidGraphMetricEdge, degreeAlloc(std.testing.allocator, 1, &.{.{ .source = 0, .target = 1 }}, .{}));
}
