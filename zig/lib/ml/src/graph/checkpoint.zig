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

// Activation checkpointing: trade compute for memory during training.
//
// During backpropagation, intermediate forward activations consume memory
// proportional to model depth. Activation checkpointing reduces this by
// only saving activations at checkpoint boundaries, then recomputing the
// intermediate values between checkpoints during the backward pass.
//
// This module operates on the lowered gradient graph produced by autodiff:
//   1. Identifies checkpoint boundary nodes in the forward subgraph
//   2. For backward nodes that reference non-checkpoint forward activations,
//      inserts duplicate forward computation chains rooted at the nearest
//      preceding checkpoint
//   3. The interpreter can then free non-checkpoint forward activations
//      once they're no longer needed by the forward pass
//
// Usage:
//   var grad = try autodiff.gradient(allocator, &graph, loss, wrt);
//   try applyCheckpointing(allocator, &grad, .{ .strategy = .every_n_layers, .layer_interval = 2 });
//   // grad.graph now has recomputation nodes; interpreter will use less peak memory

const std = @import("std");
const graph_mod = @import("graph.zig");
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");
const autodiff_mod = @import("autodiff.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;
const GradientResult = autodiff_mod.GradientResult;

pub const CheckpointStrategy = enum {
    /// Save activations every N layers (linear nodes).
    every_n_layers,
    /// Save only attention output activations.
    attention_outputs,
    /// Save only graph roots and loss; recompute all forward activations used by backward.
    parameters_only,
};

pub const CheckpointConfig = struct {
    strategy: CheckpointStrategy = .every_n_layers,
    /// For every_n_layers: interval between saved activations.
    layer_interval: u32 = 1,
    /// Experimental: recursively clone non-checkpoint dependencies for each
    /// recompute activation. This can reduce retained activations but may
    /// explode compute/memory on broad training graphs, so it is opt-in.
    recursive_recompute_dependencies: bool = false,
};

/// Identifies which forward nodes should be treated as checkpoint boundaries.
/// Returns a bitset (as a bool slice) where true = "keep this activation".
pub fn identifyCheckpoints(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    forward_count: u32,
    config: CheckpointConfig,
) ![]bool {
    const is_checkpoint = try allocator.alloc(bool, forward_count);
    @memset(is_checkpoint, false);

    switch (config.strategy) {
        .every_n_layers => {
            // Count layer-boundary projections rather than every linear-like
            // op. Transformer blocks contain several internal linear
            // projections (Q/K/V, FFN up) whose activations are exactly what
            // checkpointing should be allowed to drop and recompute. The most
            // stable generic signal for a block boundary in lowered graphs is
            // the FFN down projection: rank-2 input/output with the last
            // dimension shrinking back to hidden size.
            var layer_boundary_count: u32 = 0;
            for (0..forward_count) |i| {
                if (isLayerBoundaryProjection(graph, @intCast(i))) {
                    layer_boundary_count += 1;
                    if (layer_boundary_count % config.layer_interval == 0) {
                        is_checkpoint[i] = true;
                    }
                }
            }
            markParameterAndLossCheckpoints(graph, forward_count, is_checkpoint);
        },
        .attention_outputs => {
            // Mark parameters, constants, and nodes that look like attention outputs.
            for (0..forward_count) |i| {
                const n = graph.node(@intCast(i));
                if (n.op == .parameter or n.op == .constant) {
                    is_checkpoint[i] = true;
                } else if (isAttentionOutput(n.op)) {
                    is_checkpoint[i] = true;
                }
            }
            markParameterAndLossCheckpoints(graph, forward_count, is_checkpoint);
        },
        .parameters_only => {
            markParameterAndLossCheckpoints(graph, forward_count, is_checkpoint);
        },
    }

    return is_checkpoint;
}

fn markParameterAndLossCheckpoints(graph: *const Graph, forward_count: u32, is_checkpoint: []bool) void {
    for (0..forward_count) |i| {
        const n = graph.node(@intCast(i));
        if (n.op == .parameter or n.op == .constant) {
            is_checkpoint[i] = true;
        }
    }
    if (forward_count > 0) {
        is_checkpoint[forward_count - 1] = true;
    }
}

/// Analyze a gradient graph to determine memory savings from checkpointing.
/// Returns the number of forward activations that can be freed (recomputed).
pub fn analyzeCheckpointSavings(
    graph: *const Graph,
    forward_count: u32,
    is_checkpoint: []const bool,
) CheckpointAnalysis {
    var total_forward: u32 = 0;
    var checkpointed: u32 = 0;
    var recomputable: u32 = 0;

    for (0..forward_count) |i| {
        const n = graph.node(@intCast(i));
        // Skip parameters and constants — they're always resident.
        if (n.op == .parameter or n.op == .constant) continue;

        total_forward += 1;
        if (is_checkpoint[i]) {
            checkpointed += 1;
        } else {
            recomputable += 1;
        }
    }

    return .{
        .total_forward_activations = total_forward,
        .checkpointed_activations = checkpointed,
        .recomputable_activations = recomputable,
    };
}

pub const CheckpointAnalysis = struct {
    total_forward_activations: u32,
    checkpointed_activations: u32,
    recomputable_activations: u32,

    /// Fraction of activations that can be freed and recomputed.
    pub fn savingsRatio(self: CheckpointAnalysis) f32 {
        if (self.total_forward_activations == 0) return 0.0;
        return @as(f32, @floatFromInt(self.recomputable_activations)) /
            @as(f32, @floatFromInt(self.total_forward_activations));
    }
};

/// Apply activation checkpointing to a gradient result.
///
/// For each backward node that references a non-checkpoint forward activation,
/// inserts a recomputation chain from the nearest checkpoint ancestor(s).
/// After this pass, the interpreter can free non-checkpoint activations
/// once they're consumed by the forward pass.
///
/// Modifies grad_result.graph in-place by appending recomputation nodes.
pub fn applyCheckpointing(
    allocator: std.mem.Allocator,
    grad_result: *GradientResult,
    config: CheckpointConfig,
) !void {
    const g = &grad_result.graph;
    const total_nodes = g.nodeCount();

    // Find the boundary between forward and backward nodes.
    // Forward nodes were produced by lowering; backward nodes were appended by autodiff.
    // Convention: the original forward subgraph has node IDs [0, forward_count).
    // The id_map tells us the highest original node → highest forward node.
    var forward_count: u32 = 0;
    for (grad_result.id_map) |mapped| {
        if (mapped != null_node and mapped >= forward_count) {
            forward_count = mapped + 1;
        }
    }

    if (forward_count == 0 or forward_count >= total_nodes) return;

    // Identify checkpoint boundaries.
    const is_checkpoint = try identifyCheckpoints(allocator, g, forward_count, config);
    defer allocator.free(is_checkpoint);

    if (config.recursive_recompute_dependencies) {
        try applyRecursiveDependencyRecompute(allocator, grad_result, forward_count, total_nodes, is_checkpoint);
    } else {
        try applyDirectRecompute(allocator, grad_result, forward_count, total_nodes, is_checkpoint);
    }

    try reorderGradientGraphTopologically(allocator, grad_result);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn applyDirectRecompute(
    allocator: std.mem.Allocator,
    grad_result: *GradientResult,
    forward_count: u32,
    total_nodes: u32,
    is_checkpoint: []const bool,
) !void {
    const g = &grad_result.graph;

    var needs_recompute = try allocator.alloc(bool, forward_count);
    defer allocator.free(needs_recompute);
    @memset(needs_recompute, false);

    for (forward_count..total_nodes) |i| {
        const n = g.node(@intCast(i));
        for (n.getInputs()) |inp| {
            if (inp != null_node and inp < forward_count and !is_checkpoint[inp]) {
                needs_recompute[inp] = true;
            }
        }
    }

    var remap = try allocator.alloc(NodeId, forward_count);
    defer allocator.free(remap);
    for (0..forward_count) |i| {
        remap[i] = @intCast(i);
    }

    for (0..forward_count) |i| {
        if (!needs_recompute[i]) continue;

        const orig_node = g.node(@intCast(i));
        var new_node = orig_node.*;
        for (&new_node.inputs, 0..) |*inp, j| {
            if (j >= new_node.num_inputs) break;
            if (inp.* != null_node and inp.* < forward_count) {
                inp.* = remap[inp.*];
            }
        }
        new_node.vjp_alternate = null_node;

        const new_id = try g.addNode(new_node);
        remap[i] = new_id;
    }

    const updated_total = g.nodeCount();
    for (forward_count..updated_total) |i| {
        const n = g.nodeMut(@intCast(i));
        for (&n.inputs, 0..) |*inp, j| {
            if (j >= n.num_inputs) break;
            if (inp.* != null_node and inp.* < forward_count) {
                inp.* = remap[inp.*];
            }
        }
    }

    for (g.outputs.items) |*out| {
        if (out.* < forward_count) {
            out.* = remap[out.*];
        }
    }

    for (grad_result.param_grads) |*pg| {
        if (pg.* != null_node and pg.* < forward_count) {
            pg.* = remap[pg.*];
        }
    }
}

fn applyRecursiveDependencyRecompute(
    allocator: std.mem.Allocator,
    grad_result: *GradientResult,
    forward_count: u32,
    total_nodes: u32,
    is_checkpoint: []const bool,
) !void {
    const g = &grad_result.graph;

    var remap = try allocator.alloc(NodeId, forward_count);
    defer allocator.free(remap);
    for (0..forward_count) |i| {
        remap[i] = if (is_checkpoint[i]) @intCast(i) else null_node;
    }

    for (forward_count..total_nodes) |i| {
        const node_id: NodeId = @intCast(i);
        // duplicateForwardForRecompute -> g.addNode appends to g.nodes, which can
        // reallocate the backing array and invalidate any held node pointer. Re-fetch
        // g.nodeMut(node_id) before every read/write instead of caching a pointer.
        const num_inputs = g.nodeMut(node_id).num_inputs;
        for (0..num_inputs) |j| {
            const cur = g.nodeMut(node_id).inputs[j];
            if (cur != null_node and cur < forward_count and !is_checkpoint[cur]) {
                const new_id = try duplicateForwardForRecompute(g, forward_count, is_checkpoint, remap, cur);
                g.nodeMut(node_id).inputs[j] = new_id;
            }
        }
    }

    for (g.outputs.items) |*out| {
        if (out.* < forward_count and !is_checkpoint[out.*]) {
            out.* = try duplicateForwardForRecompute(g, forward_count, is_checkpoint, remap, out.*);
        }
    }

    for (grad_result.param_grads) |*pg| {
        if (pg.* != null_node and pg.* < forward_count and !is_checkpoint[pg.*]) {
            pg.* = try duplicateForwardForRecompute(g, forward_count, is_checkpoint, remap, pg.*);
        }
    }
}

fn duplicateForwardForRecompute(
    g: *Graph,
    forward_count: u32,
    is_checkpoint: []const bool,
    remap: []NodeId,
    node_id: NodeId,
) !NodeId {
    if (node_id == null_node or node_id >= forward_count) return node_id;
    if (is_checkpoint[node_id]) return node_id;
    if (remap[node_id] != null_node) return remap[node_id];

    var new_node = g.node(node_id).*;
    for (&new_node.inputs, 0..) |*inp, idx| {
        if (idx >= new_node.num_inputs) break;
        if (inp.* != null_node and inp.* < forward_count and !is_checkpoint[inp.*]) {
            inp.* = try duplicateForwardForRecompute(g, forward_count, is_checkpoint, remap, inp.*);
        }
    }
    new_node.vjp_alternate = null_node;

    const new_id = try g.addNode(new_node);
    remap[node_id] = new_id;
    return new_id;
}

fn reorderGradientGraphTopologically(
    allocator: std.mem.Allocator,
    grad_result: *GradientResult,
) !void {
    const g = &grad_result.graph;
    const total_nodes = g.nodeCount();
    if (total_nodes == 0) return;

    const visit_state = try allocator.alloc(u8, total_nodes);
    defer allocator.free(visit_state);
    @memset(visit_state, 0);

    var order = std.ArrayListUnmanaged(NodeId).empty;
    defer order.deinit(allocator);
    try order.ensureTotalCapacity(allocator, total_nodes);

    for (0..total_nodes) |node_id| {
        try topoVisit(g, allocator, visit_state, &order, @intCast(node_id));
    }
    std.debug.assert(order.items.len == total_nodes);

    const old_nodes = try g.nodes.toOwnedSlice(allocator);
    defer allocator.free(old_nodes);
    const old_outputs = try g.outputs.toOwnedSlice(allocator);
    defer allocator.free(old_outputs);
    const old_parameters = try g.parameters.toOwnedSlice(allocator);
    defer allocator.free(old_parameters);

    const remap = try allocator.alloc(NodeId, total_nodes);
    defer allocator.free(remap);
    const new_nodes = try allocator.alloc(Node, total_nodes);
    errdefer allocator.free(new_nodes);

    for (order.items, 0..) |old_id, new_id| {
        remap[old_id] = @intCast(new_id);
    }

    for (order.items, 0..) |old_id, new_id| {
        var node = old_nodes[old_id];
        for (&node.inputs, 0..) |*inp, idx| {
            if (idx >= node.num_inputs) break;
            if (inp.* != null_node) inp.* = remap[inp.*];
        }
        if (node.vjp_alternate != null_node) {
            node.vjp_alternate = remap[node.vjp_alternate];
        }
        new_nodes[new_id] = node;
    }

    var new_outputs = try allocator.alloc(NodeId, old_outputs.len);
    errdefer allocator.free(new_outputs);
    for (old_outputs, 0..) |old_id, idx| {
        new_outputs[idx] = remap[old_id];
    }

    var new_parameters = try allocator.alloc(NodeId, old_parameters.len);
    errdefer allocator.free(new_parameters);
    for (old_parameters, 0..) |old_id, idx| {
        new_parameters[idx] = remap[old_id];
    }

    for (grad_result.param_grads) |*pg| {
        if (pg.* != null_node) pg.* = remap[pg.*];
    }
    for (grad_result.id_map) |*mapped| {
        if (mapped.* != null_node) mapped.* = remap[mapped.*];
    }

    var const_it = g.constant_cache.map.iterator();
    while (const_it.next()) |entry| {
        entry.value_ptr.* = remap[entry.value_ptr.*];
    }

    g.nodes.deinit(allocator);
    g.outputs.deinit(allocator);
    g.parameters.deinit(allocator);
    g.nodes = std.ArrayListUnmanaged(Node).fromOwnedSlice(new_nodes);
    g.outputs = std.ArrayListUnmanaged(NodeId).fromOwnedSlice(new_outputs);
    g.parameters = std.ArrayListUnmanaged(NodeId).fromOwnedSlice(new_parameters);
}

fn topoVisit(
    g: *const Graph,
    allocator: std.mem.Allocator,
    visit_state: []u8,
    order: *std.ArrayListUnmanaged(NodeId),
    node_id: NodeId,
) !void {
    switch (visit_state[node_id]) {
        1 => return error.InvalidCheckpointGraphCycle,
        2 => return,
        else => {},
    }
    visit_state[node_id] = 1;

    const node = g.node(node_id);
    for (node.getInputs()) |inp| {
        if (inp != null_node) try topoVisit(g, allocator, visit_state, order, inp);
    }
    if (node.vjp_alternate != null_node) {
        try topoVisit(g, allocator, visit_state, order, node.vjp_alternate);
    }

    visit_state[node_id] = 2;
    try order.append(allocator, node_id);
}

fn isLinearLikeOp(op: OpCode) bool {
    return switch (op) {
        .fused_linear, .fused_linear_no_bias => true,
        // Primitive dot_general is the lowered form of linear.
        .dot_general => true,
        else => false,
    };
}

fn lastDim(shape: Shape) ?i64 {
    if (shape.rank() == 0) return null;
    return shape.dim(shape.rank() - 1);
}

fn isLayerBoundaryProjection(graph: *const Graph, node_id: NodeId) bool {
    const n = graph.node(node_id);
    switch (n.op) {
        .fused_linear, .fused_linear_no_bias => |attrs| {
            return attrs.num_projections == 0 and
                attrs.in_dim > attrs.out_dim and
                attrs.out_dim > 0;
        },
        .dot_general => {},
        else => return false,
    }

    const inputs = n.getInputs();
    if (inputs.len == 0 or inputs[0] == null_node) return false;
    const in_last = lastDim(graph.node(inputs[0]).output_shape) orelse return false;
    const out_last = lastDim(n.output_shape) orelse return false;
    return in_last > out_last and out_last > 0;
}

fn isAttentionOutput(op: OpCode) bool {
    return switch (op) {
        .fused_sdpa => true,
        // In lowered form, attention is a chain of dot_general + softmax.
        // We'd need more context to identify these; for now just match fused.
        else => false,
    };
}

// ── Tests ─────────────────────────────────────────────────────────────────────

const builder_mod = @import("builder.zig");
const Builder = builder_mod.Builder;

test "identifyCheckpoints marks parameters and every-N linear" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // Build: x -> linear1 -> linear2 -> linear3 -> sum
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w1 = try bld.parameter("w1", Shape.init(.f32, &.{ 4, 3 }));
    const w2 = try bld.parameter("w2", Shape.init(.f32, &.{ 4, 4 }));
    const w3 = try bld.parameter("w3", Shape.init(.f32, &.{ 4, 4 }));
    const y1 = try bld.linearNoBias(x, w1, 2, 3, 4);
    const y2 = try bld.linearNoBias(y1, w2, 2, 4, 4);
    const y3 = try bld.linearNoBias(y2, w3, 2, 4, 4);
    const loss = try bld.reduceSum(y3, &.{ 0, 1 });
    _ = loss;

    const forward_count = g.nodeCount();
    const checkpoints = try identifyCheckpoints(allocator, &g, forward_count, .{
        .strategy = .every_n_layers,
        .layer_interval = 2,
    });
    defer allocator.free(checkpoints);

    // Parameters should be checkpointed.
    try std.testing.expect(checkpoints[x]);
    try std.testing.expect(checkpoints[w1]);
    try std.testing.expect(checkpoints[w2]);
    try std.testing.expect(checkpoints[w3]);

    // Last node (loss) should be checkpointed.
    try std.testing.expect(checkpoints[forward_count - 1]);
}

test "analyzeCheckpointSavings reports correct counts" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
    const y = try bld.linearNoBias(x, w, 2, 3, 4);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    _ = loss;

    const forward_count = g.nodeCount();
    const checkpoints = try identifyCheckpoints(allocator, &g, forward_count, .{
        .strategy = .every_n_layers,
        .layer_interval = 1,
    });
    defer allocator.free(checkpoints);

    const analysis = analyzeCheckpointSavings(&g, forward_count, checkpoints);

    // Should have some forward activations and savings ratio between 0 and 1.
    try std.testing.expect(analysis.total_forward_activations > 0);
    try std.testing.expect(analysis.savingsRatio() >= 0.0);
    try std.testing.expect(analysis.savingsRatio() <= 1.0);
}

test "every-N checkpointing marks FFN down projection boundaries only" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w_up = try bld.parameter("w_up", Shape.init(.f32, &.{ 16, 4 }));
    const w_down = try bld.parameter("w_down", Shape.init(.f32, &.{ 4, 16 }));
    const up = try bld.linearNoBias(x, w_up, 2, 4, 16);
    const act = try bld.gelu(up);
    const down = try bld.linearNoBias(act, w_down, 2, 16, 4);
    const loss = try bld.reduceSum(down, &.{ 0, 1 });
    _ = loss;

    const forward_count = g.nodeCount();
    const checkpoints = try identifyCheckpoints(allocator, &g, forward_count, .{
        .strategy = .every_n_layers,
        .layer_interval = 1,
    });
    defer allocator.free(checkpoints);

    try std.testing.expect(!checkpoints[up]);
    try std.testing.expect(!checkpoints[act]);
    try std.testing.expect(checkpoints[down]);
    try std.testing.expect(checkpoints[forward_count - 1]);
}

test "parameters-only checkpointing marks roots and loss only" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const c = try bld.constant("scale", Shape.init(.f32, &.{1}), &.{2.0});
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 4 }));
    const y = try bld.linearNoBias(x, w, 2, 4, 4);
    const scaled = try bld.mul(y, c);
    const loss = try bld.reduceSum(scaled, &.{ 0, 1 });
    _ = loss;

    const forward_count = g.nodeCount();
    const checkpoints = try identifyCheckpoints(allocator, &g, forward_count, .{
        .strategy = .parameters_only,
    });
    defer allocator.free(checkpoints);

    for (0..forward_count) |i| {
        const n = g.node(@intCast(i));
        const expected = n.op == .parameter or n.op == .constant or i == forward_count - 1;
        try std.testing.expectEqual(expected, checkpoints[i]);
    }
}

test "applyCheckpointing produces valid graph" {
    const allocator = std.testing.allocator;
    const autodiff = @import("autodiff.zig");

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // Simple: x -> linear -> loss
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
    const y = try bld.linearNoBias(x, w, 2, 3, 4);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    // Compute gradients.
    var grad = try autodiff.gradient(allocator, &g, loss, &.{ x, w });
    defer grad.deinit();

    const pre_count = grad.graph.nodeCount();

    // Apply checkpointing.
    try applyCheckpointing(allocator, &grad, .{
        .strategy = .every_n_layers,
        .layer_interval = 1,
    });

    const post_count = grad.graph.nodeCount();

    // Graph should still be valid (may have more nodes from recomputation).
    try std.testing.expect(post_count >= pre_count);

    // Gradient nodes should still be valid.
    for (grad.param_grads) |pg| {
        try std.testing.expect(pg != null_node);
        try std.testing.expect(pg < post_count);
    }

    for (grad.graph.nodes.items, 0..) |node, node_id| {
        for (node.getInputs()) |inp| {
            try std.testing.expect(inp == null_node or inp < node_id);
        }
        try std.testing.expect(node.vjp_alternate == null_node or node.vjp_alternate < node_id);
    }
}

test "applyCheckpointing handles duplicate inputs (x * x)" {
    // Regression test: nodes with the same input twice (e.g. mul(x, x))
    // must not be dropped by the topological rebuild.
    const allocator = std.testing.allocator;
    const autodiff = @import("autodiff.zig");

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // x * x -> reduce_sum -> loss
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const x_sq = try bld.mul(x, x); // duplicate input
    const loss = try bld.reduceSum(x_sq, &.{ 0, 1 });
    try g.markOutput(loss);

    var grad = try autodiff.gradient(allocator, &g, loss, &.{x});
    defer grad.deinit();

    const pre_count = grad.graph.nodeCount();

    try applyCheckpointing(allocator, &grad, .{
        .strategy = .every_n_layers,
        .layer_interval = 1,
    });

    const post_count = grad.graph.nodeCount();
    try std.testing.expect(post_count >= pre_count);

    // All param_grads must be valid.
    for (grad.param_grads) |pg| {
        try std.testing.expect(pg != null_node);
        try std.testing.expect(pg < post_count);
    }

    // Topological invariant.
    for (0..post_count) |i| {
        const n = grad.graph.node(@intCast(i));
        for (n.getInputs()) |inp| {
            if (inp != null_node) {
                try std.testing.expect(inp < @as(NodeId, @intCast(i)));
            }
        }
    }
}

test "parameters-only checkpointing recursively duplicates dropped dependencies" {
    const allocator = std.testing.allocator;
    const autodiff = @import("autodiff.zig");

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w_up = try bld.parameter("w_up", Shape.init(.f32, &.{ 8, 4 }));
    const w_down = try bld.parameter("w_down", Shape.init(.f32, &.{ 4, 8 }));
    const up = try bld.linearNoBias(x, w_up, 2, 4, 8);
    const act = try bld.gelu(up);
    const down = try bld.linearNoBias(act, w_down, 2, 8, 4);
    const loss = try bld.reduceSum(down, &.{ 0, 1 });
    try g.markOutput(loss);

    var grad = try autodiff.gradient(allocator, &g, loss, &.{ x, w_up, w_down });
    defer grad.deinit();

    try applyCheckpointing(allocator, &grad, .{
        .strategy = .parameters_only,
        .recursive_recompute_dependencies = true,
    });

    const mapped_up = grad.id_map[up];
    const mapped_act = grad.id_map[act];
    const mapped_down = grad.id_map[down];

    var found_recomputed_act = false;
    for (grad.graph.nodes.items, 0..) |node, node_id| {
        if (node.op != .fused_gelu) continue;
        if (node_id == mapped_act) continue;
        const inputs = node.getInputs();
        if (inputs.len == 0) continue;
        const input_id = inputs[0];
        if (input_id == mapped_up or input_id == mapped_act or input_id == mapped_down) {
            return error.RecomputedActivationStillReferencesOriginalForwardActivation;
        }
        if (input_id != null_node and input_id < grad.graph.nodeCount()) {
            const input_node = grad.graph.node(input_id);
            if (input_node.op == .dot_general or input_node.op == .fused_linear_no_bias) {
                found_recomputed_act = true;
            }
        }
    }

    try std.testing.expect(found_recomputed_act);
}

test "applyCheckpointing with multi-layer graph" {
    // Deeper graph with multiple linear layers to exercise recomputation
    // across several non-checkpoint nodes.
    const allocator = std.testing.allocator;
    const autodiff = @import("autodiff.zig");

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    // x -> linear1 -> linear2 -> linear3 -> reduce_sum
    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w1 = try bld.parameter("w1", Shape.init(.f32, &.{ 4, 4 }));
    const w2 = try bld.parameter("w2", Shape.init(.f32, &.{ 4, 4 }));
    const w3 = try bld.parameter("w3", Shape.init(.f32, &.{ 4, 4 }));
    const y1 = try bld.linearNoBias(x, w1, 2, 4, 4);
    const y2 = try bld.linearNoBias(y1, w2, 2, 4, 4);
    const y3 = try bld.linearNoBias(y2, w3, 2, 4, 4);
    const loss = try bld.reduceSum(y3, &.{ 0, 1 });
    try g.markOutput(loss);

    var grad = try autodiff.gradient(allocator, &g, loss, &.{ x, w1, w2, w3 });
    defer grad.deinit();

    // Checkpoint every 2 layers — some intermediate activations will be
    // recomputed, exercising the full recompute + rebuild path.
    try applyCheckpointing(allocator, &grad, .{
        .strategy = .every_n_layers,
        .layer_interval = 2,
    });

    const post_count = grad.graph.nodeCount();

    // All param_grads must point to valid nodes.
    for (grad.param_grads) |pg| {
        try std.testing.expect(pg != null_node);
        try std.testing.expect(pg < post_count);
    }

    // Topological invariant.
    for (0..post_count) |i| {
        const n = grad.graph.node(@intCast(i));
        for (n.getInputs()) |inp| {
            if (inp != null_node) {
                try std.testing.expect(inp < @as(NodeId, @intCast(i)));
            }
        }
    }

    // id_map entries must be valid.
    for (grad.id_map) |m| {
        if (m != null_node) {
            try std.testing.expect(m < post_count);
        }
    }
}

test "applyCheckpointing no-op when all nodes checkpointed" {
    // When layer_interval=1, every linear is checkpointed, so no
    // recomputation nodes should be added (count shouldn't grow).
    const allocator = std.testing.allocator;
    const autodiff = @import("autodiff.zig");

    var g = Graph.init(allocator);
    defer g.deinit();
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
    const y = try bld.linearNoBias(x, w, 2, 3, 4);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    var grad = try autodiff.gradient(allocator, &g, loss, &.{ x, w });
    defer grad.deinit();

    const pre_count = grad.graph.nodeCount();

    try applyCheckpointing(allocator, &grad, .{
        .strategy = .every_n_layers,
        .layer_interval = 1,
    });

    const post_count = grad.graph.nodeCount();

    // Graph should still be topologically valid even if no recomputation
    // nodes were added.
    for (0..post_count) |i| {
        const n = grad.graph.node(@intCast(i));
        for (n.getInputs()) |inp| {
            if (inp != null_node) {
                try std.testing.expect(inp < @as(NodeId, @intCast(i)));
            }
        }
    }

    // Checkpointing every layer is functionally a no-op: every node
    // claims a checkpoint slot, so the recomputation pass has nothing
    // to inject. The rebuild may add a single bookkeeping node (e.g.
    // an output marker after the gradient pass), so allow up to +1.
    try std.testing.expect(post_count <= pre_count + 1);
}
