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

// Fused-to-primitive lowering pass.
//
// Replaces fused ops with their decomposed primitive subgraphs (already
// present in the graph via vjp_alternate pointers). Some fused ops are
// deliberately preserved because they have custom VJPs or backend kernels.
//
// Fused ops WITHOUT a vjp_alternate are passed through unchanged (they
// need hand-written VJPs or are not differentiable).

const std = @import("std");
const graph_mod = @import("graph.zig");
const node_mod = @import("node.zig");
const shape_mod = @import("shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;

pub const LowerResult = struct {
    graph: Graph,
    /// old_id → new_id mapping. Caller must free.
    id_map: []NodeId,

    pub fn deinit(self: *LowerResult) void {
        const allocator = self.graph.allocator;
        self.graph.deinit();
        allocator.free(self.id_map);
    }
};

/// Lower a graph by replacing fused ops (that have vjp_alternate) with
/// their decomposed primitive subgraphs. Returns a new graph where lowerable
/// fused ops are replaced by their primitive equivalents.
///
/// The returned id_map translates old node IDs to new node IDs.
pub fn lower(allocator: std.mem.Allocator, graph: *const Graph) !LowerResult {
    const count = graph.nodeCount();

    // Step 1: Build a "redirect" map. For each fused node with vjp_alternate,
    // redirect its consumers to the vjp_alternate output instead.
    const redirect = try allocator.alloc(NodeId, count);
    defer allocator.free(redirect);
    for (0..count) |i| {
        redirect[i] = @intCast(i); // identity by default
    }
    for (0..count) |i| {
        const n = graph.node(@intCast(i));
        if (n.num_inputs > n.inputs.len) return error.InvalidGraphDependency;
        for (n.getInputs()) |input| if (input != null_node and input >= count) return error.InvalidGraphDependency;
        if (n.vjp_alternate != null_node and n.vjp_alternate >= count) return error.InvalidGraphDependency;
        if (n.op == .fused_gelu or n.op == .fused_gelu_exact or n.op == .fused_softmax) continue;
        // Fused disentangled attention keeps its fused forward kernel and is
        // differentiated by a custom VJP rule (not vjp_alternate lowering).
        if (n.op == .fused_disentangled_attention or n.op == .fused_disentangled_attention_backward) continue;
        // A materialized alternate would discard replay/control semantics and
        // reintroduce quadratic owners. These versioned kernels have a VJP.
        if (n.op == .fused_deberta_training_attention_v1 or n.op == .fused_deberta_training_attention_backward_v1) continue;
        if (n.op.isFused() and n.vjp_alternate != null_node) {
            if (n.vjp_alternate == i) return error.CyclicGraph;
            redirect[i] = n.vjp_alternate;
        }
    }

    // Resolve complete alternate chains once. A public id_map must name the
    // final semantic value regardless of the original ID order; one-hop maps
    // can otherwise leave a live fused output unmapped after adapter rewrites.
    const redirect_state = try allocator.alloc(u2, count);
    defer allocator.free(redirect_state);
    @memset(redirect_state, 0);
    var chain = std.ArrayListUnmanaged(NodeId).empty;
    defer chain.deinit(allocator);
    for (0..count) |start| {
        if (redirect_state[start] == 2) continue;
        chain.clearRetainingCapacity();
        var current: NodeId = @intCast(start);
        while (redirect[current] != current and redirect_state[current] != 2) {
            if (redirect_state[current] == 1) return error.CyclicGraph;
            redirect_state[current] = 1;
            try chain.append(allocator, current);
            current = redirect[current];
        }
        const target = redirect[current];
        redirect_state[current] = 2;
        for (chain.items) |id| {
            redirect[id] = target;
            redirect_state[id] = 2;
        }
    }

    // Step 2: Mark reachable nodes from redirected outputs.
    const reachable = try allocator.alloc(bool, count);
    defer allocator.free(reachable);
    @memset(reachable, false);

    var stack = std.ArrayListUnmanaged(NodeId).empty;
    defer stack.deinit(allocator);
    for (graph.outputs.items) |out_id| {
        if (out_id >= count) return error.InvalidGraphDependency;
        const id = redirect[out_id];
        if (reachable[id]) continue;
        reachable[id] = true;
        try stack.append(allocator, id);
    }
    // Iterative reachability bounds call-stack use for long unrolled training
    // graphs. Each semantic node is pushed at most once, including shared uses.
    while (stack.pop()) |id| for (graph.node(id).getInputs()) |input| {
        if (input == null_node) continue;
        const target = redirect[input];
        if (reachable[target]) continue;
        reachable[target] = true;
        try stack.append(allocator, target);
    };
    for (graph.parameters.items) |id| if (id >= count) return error.InvalidGraphDependency;

    // Step 3: Collect reachable node IDs and compute their redirected
    // dependencies so we can topologically sort them. LoRA injection may
    // cause decomposed subgraph nodes to reference LoRA combined outputs
    // at higher original indices, breaking the sequential ordering.
    var reachable_ids = std.ArrayListUnmanaged(NodeId).empty;
    defer reachable_ids.deinit(allocator);
    for (0..count) |i| {
        if (reachable[i]) {
            try reachable_ids.append(allocator, @intCast(i));
        }
    }
    const num_reachable: u32 = @intCast(reachable_ids.items.len);

    // Build a temporary index: old_id → position in reachable_ids.
    const tmp_idx = try allocator.alloc(NodeId, count);
    defer allocator.free(tmp_idx);
    @memset(tmp_idx, null_node);
    for (reachable_ids.items, 0..) |old_id, pos| {
        tmp_idx[old_id] = @intCast(pos);
    }

    // Kahn's algorithm with compact successor lists. Scanning every node for
    // each popped dependency made large training graphs quadratic to lower.
    // Successors retain original-node order, preserving deterministic ties.
    const in_degree = try allocator.alloc(u32, num_reachable);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);
    const offsets = try allocator.alloc(usize, @as(usize, num_reachable) + 1);
    defer allocator.free(offsets);
    @memset(offsets, 0);
    for (reachable_ids.items, 0..) |old_id, pos| {
        for (graph.node(old_id).getInputs()) |input| {
            if (input == null_node) continue;
            const redirected = redirect[input];
            if (redirected >= count or !reachable[redirected]) return error.InvalidGraphDependency;
            in_degree[pos] += 1;
            offsets[@as(usize, tmp_idx[redirected]) + 1] += 1;
        }
    }
    for (1..offsets.len) |i| offsets[i] = try std.math.add(usize, offsets[i], offsets[i - 1]);
    const successors = try allocator.alloc(u32, offsets[num_reachable]);
    defer allocator.free(successors);
    const next = try allocator.dupe(usize, offsets[0..num_reachable]);
    defer allocator.free(next);
    for (reachable_ids.items, 0..) |old_id, pos| {
        for (graph.node(old_id).getInputs()) |input| {
            if (input == null_node) continue;
            const dep = tmp_idx[redirect[input]];
            successors[next[dep]] = @intCast(pos);
            next[dep] += 1;
        }
    }
    var queue = std.ArrayListUnmanaged(u32).empty;
    defer queue.deinit(allocator);
    for (0..num_reachable) |pos| if (in_degree[pos] == 0) try queue.append(allocator, @intCast(pos));
    const topo_order = try allocator.alloc(NodeId, num_reachable);
    defer allocator.free(topo_order);
    var topo_count: u32 = 0;
    var q_head: usize = 0;
    while (q_head < queue.items.len) {
        const pos = queue.items[q_head];
        q_head += 1;
        topo_order[topo_count] = reachable_ids.items[pos];
        topo_count += 1;
        for (successors[offsets[pos]..offsets[pos + 1]]) |successor| {
            in_degree[successor] -= 1;
            if (in_degree[successor] == 0) try queue.append(allocator, successor);
        }
    }
    if (topo_count != num_reachable) return error.CyclicGraph;

    // Step 4: Build old→new ID mapping using topological order.
    const id_map = try allocator.alloc(NodeId, count);
    errdefer allocator.free(id_map);
    @memset(id_map, null_node);
    for (0..topo_count) |i| {
        id_map[topo_order[i]] = @intCast(i);
    }

    // Step 5: Build new graph with nodes in topological order.
    var new_graph = Graph.init(allocator);
    errdefer new_graph.deinit();

    try new_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try new_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);

    for (0..topo_count) |i| {
        const old_id = topo_order[i];
        const old_node = graph.node(old_id);
        var new_node = old_node.*;

        for (0..new_node.num_inputs) |j| {
            const old_input = new_node.inputs[j];
            if (old_input != null_node) {
                const redirected = redirect[old_input];
                new_node.inputs[j] = id_map[redirected];
            }
        }

        new_node.vjp_alternate = null_node;
        _ = try new_graph.addNode(new_node);
    }

    // Remap outputs through redirect then id_map.
    for (graph.outputs.items) |old_out| {
        const redirected = redirect[old_out];
        try new_graph.outputs.append(allocator, id_map[redirected]);
    }

    // Remap parameters (only those that survived).
    for (graph.parameters.items) |old_param| {
        if (id_map[old_param] != null_node) {
            try new_graph.parameters.append(allocator, id_map[old_param]);
        }
    }

    // The public map describes original graph values, including a fused value
    // replaced by its alternate. Leaving those entries null loses vector
    // seed roots and captured intermediate identities even though consumers
    // and graph outputs were already redirected correctly.
    for (redirect, 0..) |target, original| {
        if (target != original) id_map[original] = id_map[target];
    }

    return .{ .graph = new_graph, .id_map = id_map };
}

// ── Tests ──────────────────────────────────────────────────────────────

const Builder = @import("builder.zig").Builder;

test "lower replaces fused linear with primitives" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try b.parameter("b", Shape.init(.f32, &.{3}));
    const result = try b.linear(x, w, bias, 2, 4, 3);
    try g.markOutput(result);

    // Before lowering: has fused_linear
    const fused_node = g.node(result);
    try std.testing.expect(fused_node.op.isFused());

    var lowered = try lower(allocator, &g);
    defer lowered.deinit();

    // After lowering: no fused ops
    for (0..lowered.graph.nodeCount()) |i| {
        const n = lowered.graph.node(@intCast(i));
        try std.testing.expect(n.op.isPrimitive());
    }

    // Should have: 3 params + transpose + matmul + add = 6 nodes
    try std.testing.expectEqual(@as(u32, 6), lowered.graph.nodeCount());

    // Output should be the add node
    try std.testing.expectEqual(@as(usize, 1), lowered.graph.outputs.items.len);
    try std.testing.expectEqual(lowered.graph.outputs.items[0], lowered.id_map[result]);
}

test "lower preserves pure primitive graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const y = try b.parameter("y", Shape.init(.f32, &.{ 2, 4 }));
    const sum = try b.add(x, y);
    const product = try b.mul(sum, x);
    try g.markOutput(product);

    var lowered = try lower(allocator, &g);
    defer lowered.deinit();

    // Same number of nodes (all were already primitive)
    try std.testing.expectEqual(g.nodeCount(), lowered.graph.nodeCount());
}

test "lower chains of fused ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{8}));

    // Chain: rmsNorm -> gelu -> silu (all fused with decomposed shadows)
    const normed = try b.rmsNorm(x, w, 8, 1e-5);
    const activated = try b.gelu(normed);
    const out = try b.silu(activated);
    try g.markOutput(out);

    var lowered = try lower(allocator, &g);
    defer lowered.deinit();

    // Lowerable fused nodes should become primitives; activation kernels with
    // custom differentiation are intentionally preserved.
    for (0..lowered.graph.nodeCount()) |i| {
        const n = lowered.graph.node(@intCast(i));
        try std.testing.expect(n.op.isPrimitive() or n.op == .fused_gelu or n.op == .fused_gelu_exact or n.op == .fused_softmax);
    }

    // Should have more nodes than the fused version (decomposed)
    try std.testing.expect(lowered.graph.nodeCount() > 5);
}

test "lower preserves parameter names" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("input", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", Shape.init(.f32, &.{4}));
    const out = try b.rmsNorm(x, w, 4, 1e-5);
    try g.markOutput(out);

    var lowered = try lower(allocator, &g);
    defer lowered.deinit();

    // Find parameter nodes and check names
    var found_input = false;
    var found_weight = false;
    for (0..lowered.graph.nodeCount()) |i| {
        const n = lowered.graph.node(@intCast(i));
        switch (n.op) {
            .parameter => {
                const name = lowered.graph.parameterName(n);
                if (std.mem.eql(u8, name, "input")) found_input = true;
                if (std.mem.eql(u8, name, "weight")) found_weight = true;
            },
            else => {},
        }
    }
    try std.testing.expect(found_input);
    try std.testing.expect(found_weight);
}

test "lower resolves transitive fused value identities and rejects cyclic or invalid dependencies" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{2}));
    const primitive = try b.mul(x, x);
    const inner = try g.addNode(.{ .op = .fused_elem_multiply, .output_shape = g.node(x).output_shape, .inputs = .{ x, x, null_node, null_node }, .num_inputs = 2, .vjp_alternate = primitive });
    const outer = try g.addNode(.{ .op = .fused_elem_multiply, .output_shape = g.node(x).output_shape, .inputs = .{ x, x, null_node, null_node }, .num_inputs = 2, .vjp_alternate = inner });
    try g.markOutput(try b.add(inner, outer));
    var lowered = try lower(a, &g);
    defer lowered.deinit();
    try std.testing.expect(lowered.id_map[outer] != null_node);
    try std.testing.expectEqual(lowered.id_map[primitive], lowered.id_map[inner]);
    try std.testing.expectEqual(lowered.id_map[primitive], lowered.id_map[outer]);
    const output = lowered.graph.node(lowered.graph.outputs.items[0]);
    try std.testing.expectEqual(output.inputs[0], output.inputs[1]);
    g.nodes.items[inner].vjp_alternate = outer;
    try std.testing.expectError(error.CyclicGraph, lower(a, &g));
    g.nodes.items[inner].vjp_alternate = primitive;
    g.nodes.items[primitive].inputs[0] = @intCast(g.nodes.items.len);
    try std.testing.expectError(error.InvalidGraphDependency, lower(a, &g));
    g.nodes.items[primitive].inputs[0] = primitive;
    try std.testing.expectError(error.CyclicGraph, lower(a, &g));
}

test "lower traverses deep shared graphs without recursive call stack growth" {
    const a = std.testing.allocator;
    var g = Graph.init(a);
    defer g.deinit();
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{1}));
    var value = x;
    for (0..32768) |_| value = try b.add(value, x);
    try g.markOutput(value);
    var lowered = try lower(a, &g);
    defer lowered.deinit();
    try std.testing.expectEqual(g.nodeCount(), lowered.graph.nodeCount());
    try std.testing.expectEqual(@as(usize, 1), lowered.graph.parameters.items.len);
}
