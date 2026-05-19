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

// Common Subexpression Elimination (CSE) pass.
//
// Walks the graph in topological order. For each node, computes a hash
// of (opcode + attributes, resolved inputs). If an identical node was
// already seen, redirects the duplicate to the original.
//
// Does NOT deduplicate parameter or constant nodes (parameters are
// unique by name, constants may have been intentionally duplicated for
// readability).
//
// Run after constant folding and fusion for best results — those passes
// may create new duplicate patterns.

const std = @import("std");
const graph_mod = @import("../graph.zig");
const node_mod = @import("../node.zig");
const shape_mod = @import("../shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const OpCode = node_mod.OpCode;
const Shape = shape_mod.Shape;

pub const CseResult = struct {
    graph: Graph,
    /// old_id -> new_id mapping. Caller must free.
    id_map: []NodeId,
    /// Number of nodes eliminated.
    num_eliminated: u32,

    pub fn deinit(self: *CseResult) void {
        const allocator = self.graph.allocator;
        self.graph.deinit();
        allocator.free(self.id_map);
    }
};

/// Apply CSE to the graph.
/// Returns a new graph with duplicate subexpressions eliminated.
pub fn eliminate(allocator: std.mem.Allocator, graph: *const Graph) !CseResult {
    const count = graph.nodeCount();
    if (count == 0) return .{
        .graph = Graph.init(allocator),
        .id_map = try allocator.alloc(NodeId, 0),
        .num_eliminated = 0,
    };

    const redirect = try allocator.alloc(NodeId, count);
    defer allocator.free(redirect);
    for (0..count) |i| redirect[i] = @intCast(i);

    var num_eliminated: u32 = 0;

    // Hash map: node_hash -> first node with that hash.
    var seen = std.AutoHashMapUnmanaged(u64, NodeId).empty;
    defer seen.deinit(allocator);

    for (0..count) |i| {
        const id: NodeId = @intCast(i);
        const n = graph.node(id);

        // Don't deduplicate parameters or constants.
        switch (n.op) {
            .parameter, .constant => continue,
            else => {},
        }

        // Hash: opcode bytes + resolved input IDs.
        const h = nodeHash(n, redirect);

        const entry = try seen.getOrPut(allocator, h);
        if (entry.found_existing) {
            // Verify it's a true match (not just a hash collision).
            const existing_id = entry.value_ptr.*;
            if (nodesEqual(graph, existing_id, id, redirect)) {
                redirect[i] = existing_id;
                num_eliminated += 1;
                continue;
            }
            // Hash collision — keep both. The first one wins the slot;
            // the second is not deduplicated. This is conservative but
            // correct, and collisions are rare with 64-bit hashes.
        } else {
            entry.value_ptr.* = id;
        }
    }

    const result = try rebuild(allocator, graph, redirect, num_eliminated);
    return result;
}

// ── Hashing ───────────────────────────────────────────────────────────

fn nodeHash(n: *const Node, redirect: []const NodeId) u64 {
    var h = std.hash.Wyhash.init(0xa7b3c1d9);

    // Hash the opcode (tag + payload bytes).
    h.update(std.mem.asBytes(&n.op));

    // Hash resolved inputs.
    for (n.getInputs()) |inp| {
        const resolved = resolve(redirect, inp);
        h.update(std.mem.asBytes(&resolved));
    }

    return h.final();
}

fn nodesEqual(graph: *const Graph, a_id: NodeId, b_id: NodeId, redirect: []const NodeId) bool {
    const a = graph.node(a_id);
    const b = graph.node(b_id);

    // Same opcode (tag + attributes)?
    if (!std.mem.eql(u8, std.mem.asBytes(&a.op), std.mem.asBytes(&b.op))) return false;

    // Same number of inputs?
    if (a.num_inputs != b.num_inputs) return false;

    // Same resolved inputs?
    for (0..a.num_inputs) |i| {
        if (resolve(redirect, a.inputs[i]) != resolve(redirect, b.inputs[i])) return false;
    }

    return true;
}

// ── Helpers ───────────────────────────────────────────────────────────

fn resolve(redirect: []const NodeId, id: NodeId) NodeId {
    if (id == null_node) return id;
    if (id >= redirect.len) return id;
    var cur = id;
    var depth: u32 = 0;
    while (cur < redirect.len and redirect[cur] != cur and depth < 100) : (depth += 1) {
        cur = redirect[cur];
    }
    return cur;
}

// ── Rebuild ───────────────────────────────────────────────────────────

fn rebuild(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    redirect: []const NodeId,
    num_eliminated: u32,
) !CseResult {
    const count = graph.nodeCount();

    // Mark reachable from outputs.
    const reachable = try allocator.alloc(bool, count);
    defer allocator.free(reachable);
    @memset(reachable, false);

    for (graph.outputs.items) |out_id| {
        markReachable(graph, reachable, redirect, resolve(redirect, out_id));
    }

    // Build new graph with only reachable nodes.
    const id_map = try allocator.alloc(NodeId, count);
    @memset(id_map, null_node);

    var new_graph = Graph.init(allocator);
    errdefer {
        new_graph.deinit();
        allocator.free(id_map);
    }

    try new_graph.string_table.appendSlice(allocator, graph.string_table.items);
    try new_graph.constant_pool.appendSlice(allocator, graph.constant_pool.items);

    for (0..count) |i| {
        if (!reachable[i]) continue;

        const old_node = graph.node(@intCast(i));
        var new_node = old_node.*;

        for (0..new_node.num_inputs) |j| {
            const old_input = new_node.inputs[j];
            if (old_input != null_node) {
                const redir = resolve(redirect, old_input);
                new_node.inputs[j] = id_map[redir];
            }
        }

        if (new_node.vjp_alternate != null_node) {
            const redir = resolve(redirect, new_node.vjp_alternate);
            if (redir < count and reachable[redir]) {
                new_node.vjp_alternate = id_map[redir];
            } else {
                new_node.vjp_alternate = null_node;
            }
        }

        id_map[i] = try new_graph.addNode(new_node);
    }

    for (graph.outputs.items) |old_out| {
        const redir = resolve(redirect, old_out);
        try new_graph.outputs.append(allocator, id_map[redir]);
    }

    for (graph.parameters.items) |old_param| {
        const redir = resolve(redirect, old_param);
        if (id_map[redir] != null_node) {
            try new_graph.parameters.append(allocator, id_map[redir]);
        }
    }

    return .{ .graph = new_graph, .id_map = id_map, .num_eliminated = num_eliminated };
}

fn markReachable(graph: *const Graph, reachable: []bool, redirect: []const NodeId, id: NodeId) void {
    if (id == null_node or id >= reachable.len) return;
    if (reachable[id]) return;

    reachable[id] = true;

    const n = graph.node(id);
    for (n.getInputs()) |input_id| {
        if (input_id != null_node) {
            markReachable(graph, reachable, redirect, resolve(redirect, input_id));
        }
    }

    if (n.vjp_alternate != null_node) {
        markReachable(graph, reachable, redirect, resolve(redirect, n.vjp_alternate));
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

const Builder = @import("../builder.zig").Builder;

test "eliminate duplicate unary ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: gelu(x) twice — should be deduplicated.
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const g1 = try b.gelu(x);
    const g2 = try b.gelu(x);
    // Use both as outputs to keep them alive.
    try g.markOutput(g1);
    try g.markOutput(g2);

    var result = try eliminate(allocator, &g);
    defer result.deinit();
    g.deinit();

    // Two gelu calls duplicate (a) the outer fused_gelu node and (b) the
    // ~9 nodes in each gelu's vjp_alternate primitive shadow that CSE
    // also dedupes transitively. Assert the floor: at least the fused
    // op itself collapses and the two outputs end up identical.
    try std.testing.expect(result.num_eliminated >= 1);
    try std.testing.expectEqual(result.graph.outputs.items[0], result.graph.outputs.items[1]);
}

test "does not eliminate distinct ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Build: gelu(x) and relu(x) — different ops, keep both.
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const g1 = try b.gelu(x);
    const r1 = try b.relu(x);
    try g.markOutput(g1);
    try g.markOutput(r1);

    var result = try eliminate(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_eliminated);
    try std.testing.expect(result.graph.outputs.items[0] != result.graph.outputs.items[1]);
}

test "eliminate duplicate binary ops" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));

    // Two identical adds of the same inputs.
    const a1 = try b.add(x, y);
    const a2 = try b.add(x, y);
    try g.markOutput(a1);
    try g.markOutput(a2);

    var result = try eliminate(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 1), result.num_eliminated);
    try std.testing.expectEqual(result.graph.outputs.items[0], result.graph.outputs.items[1]);
}

test "does not eliminate parameters" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    // Two parameters with different names — should NOT be merged.
    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    const y = try b.parameter("y", Shape.init(.f32, &.{4}));
    try g.markOutput(x);
    try g.markOutput(y);

    var result = try eliminate(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_eliminated);
}

test "transitive dedup: same subexpression tree" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{4}));
    // Build neg(gelu(x)) twice.
    const g1 = try b.gelu(x);
    const n1 = try b.neg(g1);
    const g2 = try b.gelu(x);
    const n2 = try b.neg(g2);
    try g.markOutput(n1);
    try g.markOutput(n2);

    var result = try eliminate(allocator, &g);
    defer result.deinit();
    g.deinit();

    // gelu and neg dedupe at minimum; gelu's primitive vjp_alternate
    // also has identical structure between the two copies and CSE
    // collapses those transitively.
    try std.testing.expect(result.num_eliminated >= 2);
    try std.testing.expectEqual(result.graph.outputs.items[0], result.graph.outputs.items[1]);
}

test "empty graph" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);

    var result = try eliminate(allocator, &g);
    defer result.deinit();
    g.deinit();

    try std.testing.expectEqual(@as(u32, 0), result.num_eliminated);
}
