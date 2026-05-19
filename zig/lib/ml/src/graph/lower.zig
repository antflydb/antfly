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
// present in the graph via vjp_alternate pointers). Produces a new graph
// containing only primitive ops, suitable for autograd differentiation.
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
/// their decomposed primitive subgraphs. Returns a new graph where all
/// such fused ops are replaced by their primitive equivalents.
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
        if (n.op.isFused() and n.vjp_alternate != null_node) {
            redirect[i] = n.vjp_alternate;
        }
    }

    // Step 2: Mark reachable nodes from redirected outputs.
    const reachable = try allocator.alloc(bool, count);
    defer allocator.free(reachable);
    @memset(reachable, false);

    for (graph.outputs.items) |out_id| {
        markReachable(graph, reachable, redirect, redirect[out_id]);
    }

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

    // Kahn's algorithm: compute in-degree per reachable node (using
    // redirected inputs), then BFS from zero-indegree nodes.
    const in_degree = try allocator.alloc(u32, num_reachable);
    defer allocator.free(in_degree);
    @memset(in_degree, 0);

    for (reachable_ids.items, 0..) |old_id, pos| {
        const n = graph.node(old_id);
        for (n.getInputs()) |inp| {
            if (inp != null_node) {
                const redir = redirect[inp];
                if (redir < count and reachable[redir]) {
                    _ = pos; // suppress unused
                    in_degree[tmp_idx[redir]] +%= 0; // ensure redir is counted
                }
            }
        }
        _ = pos;
    }
    // Recompute properly: for each edge (dep → node), increment in_degree[node].
    @memset(in_degree, 0);
    for (reachable_ids.items) |old_id| {
        const n = graph.node(old_id);
        for (n.getInputs()) |inp| {
            if (inp != null_node) {
                const redir = redirect[inp];
                if (redir < count and reachable[redir]) {
                    // Edge: redir → old_id, so old_id has one more dependency.
                    in_degree[tmp_idx[old_id]] += 1;
                }
            }
        }
    }

    var queue = std.ArrayListUnmanaged(u32).empty;
    defer queue.deinit(allocator);
    for (0..num_reachable) |pos| {
        if (in_degree[pos] == 0) {
            try queue.append(allocator, @intCast(pos));
        }
    }

    var topo_order = try allocator.alloc(NodeId, num_reachable);
    defer allocator.free(topo_order);
    var topo_count: u32 = 0;
    var q_head: usize = 0;
    while (q_head < queue.items.len) {
        const pos = queue.items[q_head];
        q_head += 1;
        topo_order[topo_count] = reachable_ids.items[pos];
        topo_count += 1;

        // Find successors: nodes that list reachable_ids[pos] as a
        // (redirected) dependency.
        const old_id = reachable_ids.items[pos];
        for (reachable_ids.items, 0..) |succ_old, succ_pos| {
            const n = graph.node(succ_old);
            for (n.getInputs()) |inp| {
                if (inp != null_node) {
                    const redir = redirect[inp];
                    if (redir == old_id) {
                        in_degree[succ_pos] -= 1;
                        if (in_degree[succ_pos] == 0) {
                            try queue.append(allocator, @intCast(succ_pos));
                        }
                    }
                }
            }
        }
    }

    // Step 4: Build old→new ID mapping using topological order.
    const id_map = try allocator.alloc(NodeId, count);
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

    return .{ .graph = new_graph, .id_map = id_map };
}

fn markReachable(graph: *const Graph, reachable: []bool, redirect: []const NodeId, id: NodeId) void {
    if (id == null_node or id >= reachable.len) return;
    if (reachable[id]) return;

    reachable[id] = true;

    const n = graph.node(id);
    for (n.getInputs()) |input_id| {
        if (input_id != null_node) {
            // Follow redirects for inputs too.
            markReachable(graph, reachable, redirect, redirect[input_id]);
        }
    }
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

    // All nodes should be primitive
    for (0..lowered.graph.nodeCount()) |i| {
        const n = lowered.graph.node(@intCast(i));
        try std.testing.expect(n.op.isPrimitive());
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
