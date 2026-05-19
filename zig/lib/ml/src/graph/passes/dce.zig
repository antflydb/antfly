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

// Dead Code Elimination pass.
//
// Produces a compacted graph containing only nodes reachable from outputs.
// Unreachable nodes (e.g. decomposed primitive subgraphs referenced only
// via vjp_alternate) are removed, and node IDs are renumbered to be
// contiguous.

const std = @import("std");
const graph_mod = @import("../graph.zig");
const node_mod = @import("../node.zig");
const shape_mod = @import("../shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;

/// Mark nodes reachable from outputs by walking backward through inputs.
/// Does NOT follow vjp_alternate links — those are for autograd only.
pub fn computeReachable(allocator: std.mem.Allocator, graph: *const Graph) ![]bool {
    const count = graph.nodeCount();
    const reachable = try allocator.alloc(bool, count);
    @memset(reachable, false);

    for (graph.outputs.items) |out_id| {
        markReachable(graph, reachable, out_id);
    }

    return reachable;
}

fn markReachable(graph: *const Graph, reachable: []bool, id: NodeId) void {
    if (id == null_node or id >= reachable.len) return;
    if (reachable[id]) return;

    reachable[id] = true;

    const n = graph.node(id);
    for (n.getInputs()) |input_id| {
        markReachable(graph, reachable, input_id);
    }
}

/// Produce a compacted graph with dead nodes removed and IDs renumbered.
/// The returned graph is a new allocation; the original is not modified.
///
/// The old-to-new ID mapping is returned so callers can translate
/// external references (e.g. RuntimeInput node_ids).
pub fn eliminate(allocator: std.mem.Allocator, graph: *const Graph) !EliminateResult {
    const count = graph.nodeCount();
    const reachable = try computeReachable(allocator, graph);
    defer allocator.free(reachable);

    // Build old→new ID mapping
    const id_map = try allocator.alloc(NodeId, count);
    @memset(id_map, null_node);

    var new_count: u32 = 0;
    for (0..count) |i| {
        if (reachable[i]) {
            id_map[i] = new_count;
            new_count += 1;
        }
    }

    // Build compacted graph
    var new_graph = Graph.init(allocator);
    errdefer new_graph.deinit();

    // Copy string table and constant pool verbatim (offsets remain valid)
    try new_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try new_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);

    // Copy reachable nodes with remapped inputs
    for (0..count) |i| {
        if (!reachable[i]) continue;

        const old_node = graph.node(@intCast(i));
        var new_node = old_node.*;

        // Remap inputs
        for (0..new_node.num_inputs) |j| {
            const old_input = new_node.inputs[j];
            if (old_input != null_node) {
                new_node.inputs[j] = id_map[old_input];
            }
        }

        // Remap vjp_alternate (keep it if the target was also reachable)
        if (new_node.vjp_alternate != null_node and
            new_node.vjp_alternate < count and
            reachable[new_node.vjp_alternate])
        {
            new_node.vjp_alternate = id_map[new_node.vjp_alternate];
        } else {
            new_node.vjp_alternate = null_node;
        }

        _ = try new_graph.addNode(new_node);
    }

    // Remap outputs
    for (graph.outputs.items) |old_out| {
        try new_graph.outputs.append(allocator, id_map[old_out]);
    }

    // Remap parameters (only those that survived)
    for (graph.parameters.items) |old_param| {
        if (id_map[old_param] != null_node) {
            try new_graph.parameters.append(allocator, id_map[old_param]);
        }
    }

    return .{ .graph = new_graph, .id_map = id_map };
}

pub const EliminateResult = struct {
    graph: Graph,
    /// old_id → new_id mapping. Caller must free.
    id_map: []NodeId,

    pub fn deinit(self: *EliminateResult) void {
        const allocator = self.graph.allocator;
        self.graph.deinit();
        allocator.free(self.id_map);
    }
};

// ── Tests ──────────────────────────────────────────────────────────────

const Builder = @import("../builder.zig").Builder;
const Shape = shape_mod.Shape;

test "DCE removes vjp_alternate subgraph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try b.parameter("b", Shape.init(.f32, &.{3}));

    // linear emits ~3 decomposed + 1 fused = 7 total nodes (3 params + 3 prim + 1 fused)
    const result = try b.linear(x, w, bias, 2, 4, 3);
    try g.markOutput(result);

    const total_before = g.nodeCount();
    try std.testing.expect(total_before > 4); // has decomposed nodes

    var dce = try eliminate(allocator, &g);
    defer dce.deinit();

    // After DCE: only 3 params + 1 fused = 4 nodes
    try std.testing.expectEqual(@as(u32, 4), dce.graph.nodeCount());
    try std.testing.expectEqual(@as(usize, 1), dce.graph.outputs.items.len);
    try std.testing.expectEqual(@as(usize, 3), dce.graph.parameters.items.len);
}

test "DCE preserves chained fused ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 8 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{8}));

    const normed = try b.rmsNorm(x, w, 8, 1e-5);
    const activated = try b.gelu(normed);
    const out = try b.silu(activated);
    try g.markOutput(out);

    var dce = try eliminate(allocator, &g);
    defer dce.deinit();

    // 2 params + 3 fused ops = 5 nodes
    try std.testing.expectEqual(@as(u32, 5), dce.graph.nodeCount());
}

test "DCE id_map is correct" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{ 3, 4 }));
    const bias = try b.parameter("b", Shape.init(.f32, &.{3}));
    const result = try b.linear(x, w, bias, 2, 4, 3);
    try g.markOutput(result);

    var dce = try eliminate(allocator, &g);
    defer dce.deinit();

    // Parameters should be renumbered 0, 1, 2
    try std.testing.expectEqual(@as(NodeId, 0), dce.id_map[x]);
    try std.testing.expectEqual(@as(NodeId, 1), dce.id_map[w]);
    try std.testing.expectEqual(@as(NodeId, 2), dce.id_map[bias]);

    // Fused node should be 3
    try std.testing.expectEqual(@as(NodeId, 3), dce.id_map[result]);

    // Output should point to new fused node
    try std.testing.expectEqual(@as(NodeId, 3), dce.graph.outputs.items[0]);
}
