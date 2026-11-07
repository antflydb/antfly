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
const none = std.math.maxInt(usize);

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
            if (target != none and std.mem.indexOfScalar(NodeId, owners[target].input_nodes, input) == null) return error.MissingRegionBoundaryInput;
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
    owner.input_nodes = &.{input};
    try std.testing.expectError(error.OverlappingRegionBoundary, validateBoundaries(a, &graph, &.{ &owner, &owner }));
    try std.testing.expectError(error.InvalidRegionOrder, firstUseOverrides(a, &graph, &.{ input, right, left }, &.{&owner}));
}
