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

// Memory planning pass.
//
// Analyzes tensor liveness in a graph to produce a buffer reuse plan.
// Instead of allocating/freeing per-op, the execution engine can
// allocate a fixed pool and reuse buffers once their last consumer
// has executed. This eliminates allocator overhead in the hot decode loop.

const std = @import("std");
const graph_mod = @import("../graph.zig");
const node_mod = @import("../node.zig");
const shape_mod = @import("../shape.zig");

const Graph = graph_mod.Graph;
const Node = node_mod.Node;
const NodeId = node_mod.NodeId;
const null_node = node_mod.null_node;
const Shape = shape_mod.Shape;

/// Liveness interval for a single node's output tensor.
pub const LiveInterval = struct {
    /// Node that produces this tensor.
    node_id: NodeId,
    /// First node index that can read this tensor (= node_id).
    start: u32,
    /// Last node index that reads this tensor as input.
    /// maxInt(u32) for graph outputs (live beyond the graph).
    end: u32,
    /// Size of the tensor in bytes (null if dynamic shape).
    byte_size: ?usize,
    /// Assigned buffer slot (filled by planBuffers).
    buffer_slot: u32 = 0,
};

/// Compute liveness intervals for all reachable nodes.
pub fn computeLiveness(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    reachable: []const bool,
) ![]LiveInterval {
    const count = graph.nodeCount();

    // Compute last-use for each node
    const last_use = try allocator.alloc(u32, count);
    defer allocator.free(last_use);
    @memset(last_use, std.math.maxInt(u32));

    for (0..count) |i| {
        if (!reachable[i]) continue;
        const n = graph.node(@intCast(i));
        for (n.getInputs()) |input_id| {
            if (input_id != null_node and input_id < count) {
                last_use[input_id] = @intCast(i);
            }
        }
    }

    // Mark output nodes as live forever
    for (graph.outputs.items) |out_id| {
        last_use[out_id] = std.math.maxInt(u32);
    }

    // Build intervals for reachable nodes
    var intervals = std.ArrayListUnmanaged(LiveInterval).empty;
    for (0..count) |i| {
        if (!reachable[i]) continue;
        const node_id: NodeId = @intCast(i);
        const n = graph.node(node_id);
        const byte_size = if (n.output_shape.numElements()) |elems|
            @as(usize, @intCast(elems)) * n.output_shape.dtype.byteSize()
        else
            null;

        try intervals.append(allocator, .{
            .node_id = node_id,
            .start = node_id,
            .end = last_use[i],
            .byte_size = byte_size,
        });
    }

    return intervals.toOwnedSlice(allocator);
}

/// Buffer reuse plan: maps each node's output to a reusable buffer slot.
pub const BufferPlan = struct {
    /// Assigned buffer slot per reachable node (indexed by position in
    /// the intervals array, NOT by NodeId).
    intervals: []LiveInterval,
    /// Total number of buffer slots needed.
    num_slots: u32,
    /// Size of each buffer slot in bytes.
    slot_sizes: []usize,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *BufferPlan) void {
        self.allocator.free(self.intervals);
        self.allocator.free(self.slot_sizes);
    }

    /// Look up the buffer slot for a given node ID.
    pub fn slotForNode(self: *const BufferPlan, node_id: NodeId) ?u32 {
        for (self.intervals) |iv| {
            if (iv.node_id == node_id) return iv.buffer_slot;
        }
        return null;
    }
};

/// Greedy interval-coloring algorithm: assign buffer slots to liveness
/// intervals, reusing slots whose previous occupant has expired.
///
/// Parameters and graph outputs get unique slots (not reused) since
/// parameters are borrowed and outputs must survive beyond execution.
pub fn planBuffers(
    allocator: std.mem.Allocator,
    graph: *const Graph,
    reachable: []const bool,
) !BufferPlan {
    const intervals = try computeLiveness(allocator, graph, reachable);

    // Track which slots are free (end time of current occupant)
    var slot_ends = std.ArrayListUnmanaged(u32).empty;
    defer slot_ends.deinit(allocator);

    var slot_sizes = std.ArrayListUnmanaged(usize).empty;
    defer slot_sizes.deinit(allocator);

    for (intervals) |*iv| {
        const n = graph.node(iv.node_id);
        const is_param = switch (n.op) {
            .parameter => true,
            else => false,
        };
        const is_output = iv.end == std.math.maxInt(u32);
        const size = iv.byte_size orelse 0;

        if (is_param or is_output) {
            // Unique slot — never reused
            const slot: u32 = @intCast(slot_ends.items.len);
            try slot_ends.append(allocator, std.math.maxInt(u32));
            try slot_sizes.append(allocator, size);
            iv.buffer_slot = slot;
        } else {
            // Try to reuse an expired slot with sufficient size
            var best_slot: ?u32 = null;
            for (slot_ends.items, 0..) |end, idx| {
                if (end < iv.start and slot_sizes.items[idx] >= size) {
                    best_slot = @intCast(idx);
                    break;
                }
            }

            if (best_slot) |slot| {
                iv.buffer_slot = slot;
                slot_ends.items[slot] = iv.end;
                // Grow slot if this tensor is larger
                if (size > slot_sizes.items[slot]) {
                    slot_sizes.items[slot] = size;
                }
            } else {
                // Allocate new slot
                const slot: u32 = @intCast(slot_ends.items.len);
                try slot_ends.append(allocator, iv.end);
                try slot_sizes.append(allocator, size);
                iv.buffer_slot = slot;
            }
        }
    }

    return .{
        .intervals = intervals,
        .num_slots = @intCast(slot_ends.items.len),
        .slot_sizes = try slot_sizes.toOwnedSlice(allocator),
        .allocator = allocator,
    };
}

// ── Tests ──────────────────────────────────────────────────────────────

const dce = @import("dce.zig");
const Builder = @import("../builder.zig").Builder;

test "computeLiveness basic intervals" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{4}));
    const normed = try b.rmsNorm(x, w, 4, 1e-5);
    const out = try b.gelu(normed);
    try g.markOutput(out);

    const reachable = try dce.computeReachable(allocator, &g);
    defer allocator.free(reachable);

    const intervals = try computeLiveness(allocator, &g, reachable);
    defer allocator.free(intervals);

    // Should have intervals for reachable nodes only
    var reachable_count: usize = 0;
    for (reachable) |r| {
        if (r) reachable_count += 1;
    }
    try std.testing.expectEqual(reachable_count, intervals.len);

    // Output node should have end = maxInt (live forever)
    for (intervals) |iv| {
        if (iv.node_id == out) {
            try std.testing.expectEqual(std.math.maxInt(u32), iv.end);
        }
    }
}

test "planBuffers reuses expired slots" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("w", Shape.init(.f32, &.{4}));

    // Chain: x -> rmsNorm -> gelu -> silu -> output
    // After rmsNorm is consumed by gelu, its buffer can be reused by silu
    const normed = try b.rmsNorm(x, w, 4, 1e-5);
    const activated = try b.gelu(normed);
    const out = try b.silu(activated);
    try g.markOutput(out);

    const reachable = try dce.computeReachable(allocator, &g);
    defer allocator.free(reachable);

    var plan = try planBuffers(allocator, &g, reachable);
    defer plan.deinit();

    // Should need fewer slots than nodes due to reuse
    try std.testing.expect(plan.num_slots <= plan.intervals.len);

    // Parameters get unique slots
    const x_slot = plan.slotForNode(x);
    const w_slot = plan.slotForNode(w);
    try std.testing.expect(x_slot != null);
    try std.testing.expect(w_slot != null);
    try std.testing.expect(x_slot.? != w_slot.?);
}

test "planBuffers output gets unique slot" {
    const allocator = std.testing.allocator;
    var g = Graph.init(allocator);
    defer g.deinit();
    var b = Builder.init(&g);

    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const reachable = try dce.computeReachable(allocator, &g);
    defer allocator.free(reachable);

    var plan = try planBuffers(allocator, &g, reachable);
    defer plan.deinit();

    // Output slot should differ from parameter slot
    const x_slot = plan.slotForNode(x).?;
    const out_slot = plan.slotForNode(out).?;
    try std.testing.expect(x_slot != out_slot);
}
