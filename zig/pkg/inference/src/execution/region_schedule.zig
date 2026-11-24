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

//! Verify frozen region boundaries against the actual graph, before slot
//! lifetimes/coloring. A fusion's node count never establishes coverage.
const std = @import("std");
const ml = @import("ml");
const region = @import("region_route.zig");
const NodeId = ml.graph.NodeId;
pub const DeferredProducer = struct { producer: NodeId, consumer: NodeId };
const none = std.math.maxInt(usize);

/// Keep deferred producer legality shared by the legacy executor and cold
/// region selection. Last use is expressed in graph NodeIds in this proof.
pub fn deferredAddConsumer(graph: *const ml.graph.Graph, node_id: NodeId, reachable: []const bool, last_use: []const u32, use_counts: ?[]const u8) ?NodeId {
    const node_index: usize = @intCast(node_id);
    if (node_index >= graph.nodeCount() or node_index >= reachable.len or !reachable[node_index]) return null;
    switch (graph.node(node_id).op) {
        .mul, .fused_elem_multiply => {},
        else => return null,
    }
    if (node_index >= last_use.len) return null;
    const consumer_id: NodeId = @intCast(last_use[node_index]);
    if (consumer_id == ml.graph.null_node or consumer_id >= graph.nodeCount()) return null;
    const consumer_index: usize = @intCast(consumer_id);
    if (consumer_index >= reachable.len or !reachable[consumer_index]) return null;
    const consumer = graph.node(consumer_id);
    switch (consumer.op) {
        .add, .fused_elem_add => {},
        else => return null,
    }
    if (consumer.num_inputs < 2 or (consumer.inputs[0] != node_id and consumer.inputs[1] != node_id)) return null;
    if (std.mem.indexOfScalar(NodeId, graph.outputs.items, node_id) != null) return null;
    if (!hasSingleReachableConsumer(graph, node_id, reachable, use_counts)) return null;
    return consumer_id;
}

pub fn deferredProducerInputsLiveUntilConsumer(
    graph: *const ml.graph.Graph,
    producer_id: NodeId,
    consumer_id: NodeId,
    last_use: []const u32,
) bool {
    if (producer_id == ml.graph.null_node or producer_id >= graph.nodeCount()) return false;
    const producer_index: usize = @intCast(producer_id);
    const consumer_index: usize = @intCast(consumer_id);
    const producer = graph.node(producer_id);
    for (producer.getInputs()) |input_id| {
        if (input_id == ml.graph.null_node or input_id >= graph.nodeCount()) return false;
        const input_index: usize = @intCast(input_id);
        if (input_index >= last_use.len) return false;
        // Pre-materialized constants (scalar/tensor consts, zero tensors) are
        // re-materialized every execution and never recycled by buffer reuse, so
        // their last-use position does not constrain deferral — a deferred mul can
        // safely read them at the consumer even if a sibling consumed them earlier.
        // Exempting them recovers fusion for the common `(x*s) + (y*s)` pattern
        // where a shared scalar's last use is the sibling multiply.
        switch (graph.node(input_id).op) {
            .constant, .fused_zero_tensor => continue,
            else => {},
        }
        const input_last_use = last_use[input_index];
        if (input_last_use == std.math.maxInt(u32)) continue;
        const input_last_use_index: usize = @intCast(input_last_use);
        if (input_last_use_index == producer_index) continue;
        if (input_last_use_index >= consumer_index) continue;
        return false;
    }
    return true;
}

pub fn reachableUseCount(graph: *const ml.graph.Graph, node_id: NodeId, reachable: []const bool, stop_after: usize) usize {
    var count: usize = 0;
    var candidate: NodeId = 0;
    while (candidate < graph.nodeCount()) : (candidate += 1) {
        const candidate_index: usize = @intCast(candidate);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        const node = graph.node(candidate);
        for (node.getInputs()) |input_id| {
            if (input_id != node_id) continue;
            count += 1;
            if (count >= stop_after) return count;
        }
    }
    return count;
}

/// Per-node count of reachable consumers, capped at 2 (callers only need to tell
/// "exactly one consumer" from "more than one"). Built once in O(edges) so the
/// per-node execution loop can answer the deferral use-count query in O(1)
/// instead of an O(nodes) `reachableUseCount` scan per candidate — the fix for
/// the O(nodes^2) term on the hot path. Caller owns the returned slice.
pub fn computeReachableUseCountsCapped(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, reachable: []const bool) ![]u8 {
    const counts = try allocator.alloc(u8, graph.nodeCount());
    @memset(counts, 0);
    var candidate: NodeId = 0;
    while (candidate < graph.nodeCount()) : (candidate += 1) {
        const candidate_index: usize = @intCast(candidate);
        if (candidate_index >= reachable.len or !reachable[candidate_index]) continue;
        for (graph.node(candidate).getInputs()) |input_id| {
            if (input_id == ml.graph.null_node or input_id >= graph.nodeCount()) continue;
            const input_index: usize = @intCast(input_id);
            if (counts[input_index] < 2) counts[input_index] += 1;
        }
    }
    return counts;
}

/// True when `node_id` has exactly one reachable consumer. Uses the precomputed
/// capped use-count array when supplied (O(1)); otherwise falls back to the
/// O(nodes) scan (fine for the small graphs in unit tests).
fn hasSingleReachableConsumer(graph: *const ml.graph.Graph, node_id: NodeId, reachable: []const bool, use_counts: ?[]const u8) bool {
    if (use_counts) |counts| {
        const idx: usize = @intCast(node_id);
        if (idx < counts.len) return counts[idx] == 1;
    }
    return reachableUseCount(graph, node_id, reachable, 2) == 1;
}

pub fn validateBoundaries(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, owners: []const *const region.Owned) !void {
    const reachable = try @import("../graph/interpreter.zig").computeReachable(allocator, graph);
    defer allocator.free(reachable);
    return validateReachableBoundaries(allocator, graph, reachable, owners);
}

/// Serving preparation reuses the pinned program's canonical reachability;
/// dead graph consumers do not invent extra live region publications.
pub fn validateReachableBoundaries(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, reachable: []const bool, owners: []const *const region.Owned) !void {
    if (reachable.len != graph.nodeCount()) return error.InvalidRegionBoundary;
    const owner_of = try allocator.alloc(usize, graph.nodeCount());
    defer allocator.free(owner_of);
    @memset(owner_of, none);
    for (owners, 0..) |owner, index| {
        if (owner.covered_nodes.len == 0 or std.mem.indexOfScalar(NodeId, owner.covered_nodes, owner.entry_node) == null) return error.InvalidRegionBoundary;
        for (owner.covered_nodes) |node| {
            if (node >= owner_of.len or !reachable[node] or owner_of[node] != none) return error.OverlappingRegionBoundary;
            owner_of[node] = index;
        }
        for (owner.requirements.outputs, 0..) |output, output_index| {
            if (output.node >= owner_of.len or std.mem.indexOfScalar(NodeId, owner.covered_nodes, output.node) == null) return error.InvalidRegionBoundary;
            for (owner.requirements.outputs[0..output_index]) |prior| if (prior.node == output.node) return error.InvalidRegionBoundary;
            const shape = graph.node(output.node).output_shape;
            if (shape.dtype != output.logical.dtype or shape.rank() != output.logical.rank() or !std.mem.eql(i64, shape.dims[0..shape.rank()], output.logical.dims[0..output.logical.rank()])) return error.InvalidRegionOutputShape;
        }
        for (owner.input_nodes, 0..) |input, input_index| {
            if (input >= owner_of.len or std.mem.indexOfScalar(NodeId, owner.covered_nodes, input) != null or std.mem.indexOfScalar(NodeId, owner.input_nodes[0..input_index], input) != null) return error.InvalidRegionBoundary;
        }
    }
    for (owners) |owner| {
        for (owner.literal_nodes, 0..) |literal, index| {
            if (literal >= owner_of.len or !reachable[literal] or owner_of[literal] != none or
                graph.node(literal).op != .constant or std.mem.indexOfScalar(NodeId, owner.input_nodes, literal) != null or
                std.mem.indexOfScalar(NodeId, owner.literal_nodes[0..index], literal) != null) return error.InvalidRegionBoundary;
        }
    }
    // This is one graph-edge pass, not a whole-graph scan per region. Every
    // edge crossing a frozen boundary must have a declared publication/input.
    for (0..graph.nodeCount()) |raw| {
        const node: NodeId = @intCast(raw);
        if (!reachable[node]) continue;
        for (graph.node(node).getInputs()) |input| {
            if (input == ml.graph.null_node) continue;
            if (input >= owner_of.len) return error.InvalidRegionBoundary;
            const source = owner_of[input];
            const target = owner_of[node];
            if (source == target) continue;
            if (source != none and !publishes(owners[source], input)) return error.MissingRegionBoundaryOutput;
            if (target != none and
                std.mem.indexOfScalar(NodeId, owners[target].input_nodes, input) == null and
                std.mem.indexOfScalar(NodeId, owners[target].literal_nodes, input) == null) return error.MissingRegionBoundaryInput;
        }
    }
    for (graph.outputs.items) |output| {
        if (output >= owner_of.len) return error.InvalidRegionBoundary;
        const owner = owner_of[output];
        if (owner != none and !publishes(owners[owner], output)) return error.MissingRegionBoundaryOutput;
    }
}

fn publishes(owner: *const region.Owned, node: NodeId) bool {
    for (owner.requirements.outputs) |output| if (output.node == node) return true;
    return false;
}

/// Feed these overrides into the existing BufferPlan builder BEFORE allocation
/// coloring. Later members of a packed region are already live at entry.
pub fn firstUseOverrides(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, node_order: []const NodeId, owners: []const *const region.Owned) ![]u32 {
    try validateBoundaries(allocator, graph, owners);
    const positions = try allocator.alloc(u32, graph.nodeCount());
    defer allocator.free(positions);
    @memset(positions, std.math.maxInt(u32));
    for (node_order, 0..) |node, index| {
        if (node >= positions.len or positions[node] != std.math.maxInt(u32)) return error.InvalidRegionOrder;
        positions[node] = @intCast(index);
    }
    const overrides = try allocator.alloc(u32, graph.nodeCount());
    errdefer allocator.free(overrides);
    @memset(overrides, std.math.maxInt(u32));
    for (owners) |owner| {
        const entry = positions[owner.entry_node];
        if (entry == std.math.maxInt(u32)) return error.InvalidRegionOrder;
        // Deferred metadata producers (e.g. a parameter transpose) can occur
        // before actual dispatch. They are owned/elided by the region; only
        // publications may not precede the numerical entry point.
        for (owner.covered_nodes) |node| if (positions[node] == std.math.maxInt(u32)) return error.InvalidRegionOrder;
        for (owner.requirements.outputs) |output| {
            if (positions[output.node] < entry) return error.InvalidRegionOrder;
            overrides[output.node] = @min(overrides[output.node], entry);
        }
    }
    return overrides;
}

const BoundaryFixture = struct {
    fn matches(_: *anyopaque, _: region.Validate) bool {
        return true;
    }
    fn validate(_: *anyopaque, _: region.Validate) !void {}
    fn dispatch(_: *anyopaque, _: region.Invocation) !region.Outcome {
        return error.UnexpectedNumericalDispatch;
    }
    fn deinit(_: *anyopaque) void {}
    const table = region.Owned.VTable{ .matches = matches, .validate = validate, .dispatch = dispatch, .deinit = deinit };

    fn check(allocator: std.mem.Allocator, graph: *const ml.graph.Graph, order: []const NodeId, owner: *const region.Owned) !void {
        const first = try firstUseOverrides(allocator, graph, order, &.{owner});
        defer allocator.free(first);
        try std.testing.expectEqual(@as(u32, 1), first[owner.requirements.outputs[0].node]);
        try std.testing.expectEqual(@as(u32, 1), first[owner.requirements.outputs[1].node]);
    }
};

test "frozen region boundary proof preserves independent packed outputs and advances both lifetimes before coloring" {
    const a = std.testing.allocator;
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const input = try builder.parameter("input", shape);
    const left = try builder.neg(input);
    const right = try builder.neg(input);
    try graph.markOutput(left);
    try graph.markOutput(right);
    var marker: u8 = 0;
    const outputs = [_]region.Output{ .{ .node = left, .logical = shape, .bytes = 16 }, .{ .node = right, .logical = shape, .bytes = 16 } };
    var owner = region.Owned{ .context = &marker, .implementation = .{ .context = &marker, .table = &marker }, .key = 1, .policy_key = 2, .covered_nodes = &.{ left, right }, .input_nodes = &.{input}, .entry_node = left, .requirements = .{ .outputs = &outputs, .uncovered = .{ .dispatch_workspace = true } }, .vtable = &BoundaryFixture.table };
    const order = [_]NodeId{ input, left, right };
    try BoundaryFixture.check(a, &graph, &order, &owner);
    try std.testing.checkAllAllocationFailures(a, BoundaryFixture.check, .{ &graph, &order, &owner });
    // Knowing exact output extents still does not prove the fused backend's
    // hidden scratch, cold uploads or fallback resource requirements.
    try std.testing.expect(!owner.requirements.replacementEligible());
    owner.requirements.outputs = outputs[0..1];
    try std.testing.expectError(error.MissingRegionBoundaryOutput, validateBoundaries(a, &graph, &.{&owner}));
    owner.requirements.outputs = &outputs;
    owner.input_nodes = &.{};
    try std.testing.expectError(error.MissingRegionBoundaryInput, validateBoundaries(a, &graph, &.{&owner}));
    owner.literal_nodes = &.{input};
    try std.testing.expectError(error.InvalidRegionBoundary, validateBoundaries(a, &graph, &.{&owner}));
    owner.literal_nodes = &.{};
    owner.input_nodes = &.{input};
    try std.testing.expectError(error.OverlappingRegionBoundary, validateBoundaries(a, &graph, &.{ &owner, &owner }));
    try std.testing.expectError(error.InvalidRegionOrder, firstUseOverrides(a, &graph, &.{ input, right, left }, &.{&owner}));
}
