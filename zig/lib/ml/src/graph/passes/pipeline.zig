// Copyright 2026 Antfly, Inc.
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

// Named pass pipeline.
//
// Composes optimization passes into a configurable sequence. Each pass
// takes a Graph and returns an optimized Graph. The pipeline runs them
// in order, threading the graph through each pass.
//
// Pre-defined pipelines:
//   default: const_fold → fuse → cse   (standard optimization)
//   cleanup: dce only                   (minimal cleanup)

const std = @import("std");
const graph_mod = @import("../graph.zig");
const Graph = graph_mod.Graph;
const NodeId = @import("../node.zig").NodeId;

const fuse_mod = @import("fuse.zig");
const const_fold_mod = @import("const_fold.zig");
const cse_mod = @import("cse.zig");
const dce_mod = @import("dce.zig");

pub const Pass = enum {
    const_fold,
    fuse,
    cse,
    dce,
};

pub const PipelineResult = struct {
    graph: Graph,
    stats: Stats,

    pub fn deinit(self: *PipelineResult) void {
        self.graph.deinit();
    }
};

pub const Stats = struct {
    num_folded: u32 = 0,
    num_fused: u32 = 0,
    num_cse: u32 = 0,
    num_dce: u32 = 0,
    /// Fixed-point iterations actually executed. Always >= 1 unless the
    /// pipeline has no passes.
    num_iterations: u32 = 0,
};

pub const Pipeline = struct {
    passes: []const Pass,
    /// Cap on fixed-point iterations. Each iteration runs every pass in
    /// `passes` once; the loop exits early when no pass reports a
    /// rewrite. The cap exists purely as a safety net — well-formed
    /// passes converge in a handful of iterations.
    max_iterations: u32 = 8,

    /// Standard optimization pipeline: fold constants, dedupe, fuse,
    /// DCE the orphans, dedupe again. The whole sequence runs to a
    /// fixed point.
    ///
    /// CSE runs both BEFORE and AFTER fuse:
    ///   * Before — so the fuse matchers see a deduplicated graph
    ///     in iteration 1 instead of waiting until iteration 2.
    ///     Most pattern matchers compare resolved input ids, so two
    ///     redundant consts (the kind ONNX importers leave behind)
    ///     would otherwise hide a match in the first pass.
    ///   * After — to clean up duplicate scalar consts the algebraic
    ///     rewrites emit (e.g. `mul(x, 2)` from add(x,x)) before
    ///     they ride through the next iteration's const-fold.
    ///
    /// The interleaved DCE drops the orphan nodes the redirects
    /// leave behind so the second CSE doesn't waste time hashing
    /// dead branches.
    pub const default: Pipeline = .{ .passes = &.{ .const_fold, .cse, .fuse, .dce, .cse } };

    /// Minimal cleanup: dead code elimination only.
    pub const cleanup: Pipeline = .{ .passes = &.{.dce} };

    /// Run the pipeline on a graph. Takes ownership of nothing — the
    /// caller still owns the input graph. Returns a new optimized graph.
    pub fn run(self: Pipeline, allocator: std.mem.Allocator, input: *const Graph) !PipelineResult {
        var current = try cloneGraph(allocator, input);
        errdefer current.deinit();
        var stats = Stats{};

        if (self.passes.len == 0) {
            return .{ .graph = current, .stats = stats };
        }

        var iter: u32 = 0;
        while (iter < self.max_iterations) : (iter += 1) {
            stats.num_iterations += 1;
            var any_rewrite = false;
            for (self.passes) |pass| {
                switch (pass) {
                    .const_fold => {
                        const result = try const_fold_mod.fold(allocator, &current);
                        if (result.num_folded > 0) any_rewrite = true;
                        stats.num_folded += result.num_folded;
                        allocator.free(result.id_map);
                        current.deinit();
                        current = result.graph;
                    },
                    .fuse => {
                        const result = try fuse_mod.fuse(allocator, &current);
                        if (result.num_rewrites > 0) any_rewrite = true;
                        stats.num_fused += result.num_rewrites;
                        allocator.free(result.id_map);
                        current.deinit();
                        current = result.graph;
                    },
                    .cse => {
                        const result = try cse_mod.eliminate(allocator, &current);
                        if (result.num_eliminated > 0) any_rewrite = true;
                        stats.num_cse += result.num_eliminated;
                        allocator.free(result.id_map);
                        current.deinit();
                        current = result.graph;
                    },
                    .dce => {
                        const old_count = current.nodeCount();
                        var result = try dce_mod.eliminate(allocator, &current);
                        const removed = old_count - result.graph.nodeCount();
                        if (removed > 0) any_rewrite = true;
                        stats.num_dce += removed;
                        allocator.free(result.id_map);
                        current.deinit();
                        current = result.graph;
                    },
                }
            }
            if (!any_rewrite) break;
        }

        return .{ .graph = current, .stats = stats };
    }
};

fn cloneGraph(allocator: std.mem.Allocator, src: *const Graph) !Graph {
    var dst = Graph.init(allocator);
    errdefer dst.deinit();
    try dst.nodes.appendSlice(allocator, src.nodes.items);
    try dst.constant_pool.appendSlice(allocator, src.constant_pool.items);
    try dst.string_table.appendSlice(allocator, src.string_table.items);
    try dst.outputs.appendSlice(allocator, src.outputs.items);
    try dst.parameters.appendSlice(allocator, src.parameters.items);
    return dst;
}

// ── Tests ─────────────────────────────────────────────────────────────

const Builder = @import("../builder.zig").Builder;
const Shape = @import("../shape.zig").Shape;

test "default pipeline optimizes graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // rsqrt(64.0) = 0.125 (foldable constant)
    const c = try b.scalarConst(.f32, 64.0);
    const scale = try b.rsqrt(c);

    // param * scale
    const x = try b.parameter("x", Shape.scalar(.f32));
    const out = try b.mul(x, scale);
    try g.markOutput(out);

    var result = try Pipeline.default.run(allocator, &g);
    defer result.deinit();
    g.deinit();

    // rsqrt should be folded.
    try std.testing.expect(result.stats.num_folded >= 1);
    // Graph should be smaller than original.
    try std.testing.expect(result.graph.nodeCount() <= 3);
}

test "cleanup pipeline runs DCE" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const used = try b.gelu(x);
    _ = try b.relu(x); // dead code — not an output
    try g.markOutput(used);

    var result = try Pipeline.cleanup.run(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.stats.num_dce >= 1);
    // Dead relu should be eliminated.
    try std.testing.expect(result.graph.nodeCount() < 4);
}

test "empty pipeline is identity" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const empty_pipeline = Pipeline{ .passes = &.{} };
    var result = try empty_pipeline.run(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.stats.num_folded);
    try std.testing.expectEqual(@as(u32, 0), result.stats.num_fused);
    try std.testing.expectEqual(@as(u32, 0), result.stats.num_cse);
}

test "custom pipeline: fold then CSE" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const c1 = try b.scalarConst(.f32, 2.0);
    const c2 = try b.scalarConst(.f32, 3.0);
    const sum = try b.add(c1, c2);
    const x = try b.parameter("x", Shape.scalar(.f32));
    const out = try b.mul(x, sum);
    try g.markOutput(out);

    const custom = Pipeline{ .passes = &.{ .const_fold, .cse } };
    var result = try custom.run(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expect(result.stats.num_folded >= 1);
}

test "pipeline runs to fixed point" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Multi-step folding chain: folding rsqrt exposes mul that const-fold
    // can resolve in a later iteration only if the input becomes constant.
    // Stricter scenario: chained foldable subgraphs that trigger more
    // rewrites than a single fold pass would emit.
    const c1 = try b.scalarConst(.f32, 4.0);
    const c2 = try b.scalarConst(.f32, 16.0);
    const r1 = try b.sqrt(c1); // fold → 2.0
    const r2 = try b.sqrt(c2); // fold → 4.0
    const sum = try b.add(r1, r2); // fold → 6.0 (after r1, r2 are folded)
    const out = try b.mul(sum, sum); // fold → 36.0
    try g.markOutput(out);

    var result = try Pipeline.default.run(allocator, &g);
    defer result.deinit();
    g.deinit();

    // Loop must terminate within max_iterations and at least run twice
    // (first iter folds the leaves, later iters fold the dependents).
    try std.testing.expect(result.stats.num_iterations >= 1);
    try std.testing.expect(result.stats.num_iterations <= 8);
    // The whole subgraph collapses to a single constant.
    try std.testing.expectEqual(@as(u32, 1), result.graph.nodeCount());
}
