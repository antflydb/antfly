// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Frozen dispatch changes producer/consumer times. Keep physical positions
//! separate from the NodeIds used by interpreter value-handle reclamation.
const std = @import("std");
const ml = @import("ml");
const buffers = @import("../graph/buffer_plan.zig");
const region = @import("region_route.zig");
const interpreter = @import("../graph/interpreter.zig");
const none = std.math.maxInt(u32);

pub const Plan = struct {
    allocator: std.mem.Allocator,
    first_use: []u32,
    input_last_use: []u32,
    /// NodeIds/sentinel, never physical submission positions.
    runtime_last_use: []u32,
    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.first_use);
        self.allocator.free(self.input_last_use);
        self.allocator.free(self.runtime_last_use);
    }
};

pub fn create(a: std.mem.Allocator, graph: *const ml.graph.Graph, reachable: []const bool, physical: *const buffers.BufferPlan, owners: []const *const region.Owned) !Plan {
    const n = graph.nodeCount();
    if (reachable.len != n or physical.node_positions.len != n) return error.InvalidRegionOrder;
    const order = try a.alloc(ml.graph.NodeId, n);
    defer a.free(order);
    var live: usize = 0;
    for (reachable, 0..) |active, id| if (active) {
        order[live] = @intCast(id);
        live += 1;
    };
    std.mem.sort(ml.graph.NodeId, order[0..live], physical, struct {
        fn less(plan: *const buffers.BufferPlan, left: ml.graph.NodeId, right: ml.graph.NodeId) bool {
            return plan.positionForNode(left) < plan.positionForNode(right);
        }
    }.less);
    try @import("region_schedule.zig").validateReachableBoundaries(a, graph, reachable, owners);
    const first = try a.alloc(u32, n);
    errdefer a.free(first);
    @memset(first, none);
    const input_last = try a.alloc(u32, n);
    errdefer a.free(input_last);
    @memset(input_last, none);
    const producers = try a.dupe(u32, physical.node_positions);
    defer a.free(producers);
    // A publication supplied by an earlier region is available at that actual
    // region entry, even when its original unfused node appeared later.
    for (owners) |owner| {
        const entry = physical.positionForNode(owner.entry_node);
        if (entry == none) return error.InvalidRegionOrder;
        for (owner.covered_nodes) |node| if (physical.positionForNode(node) == none) return error.InvalidRegionOrder;
        for (owner.requirements.outputs) |output| {
            if (physical.positionForNode(output.node) < entry) return error.InvalidRegionOrder;
            first[output.node] = @min(first[output.node], entry);
            producers[output.node] = entry;
        }
    }
    const runtime = try interpreter.computeLastUse(a, graph, reachable);
    errdefer a.free(runtime);
    const scheduled = try a.alloc(u32, n);
    defer a.free(scheduled);
    @memset(scheduled, none);
    for (order[0..live]) |node| for (graph.node(node).getInputs()) |input| {
        if (input != ml.graph.null_node) scheduled[input] = node;
    };
    // Match the existing partition executor's conservative treatment of a
    // graph whose actual submission order differs from canonical NodeId order.
    for (runtime, scheduled) |*value, actual| if (value.* != actual) {
        value.* = none;
    };
    for (owners) |owner| {
        const entry = physical.positionForNode(owner.entry_node);
        for (owner.input_nodes) |input| {
            if (input >= n or !reachable[input] or producers[input] == none or producers[input] >= entry) return error.RegionInputUnavailableAtEntry;
            input_last[input] = if (input_last[input] == none) entry else @max(input_last[input], entry);
            // Unknown/output sentinel lifetimes stay conservative. An earlier
            // elided consumer no longer authorizes freeing this handle before
            // the actual frozen numerical entry reads it.
            if (runtime[input] != none and physical.positionForNode(runtime[input]) < entry) runtime[input] = owner.entry_node;
        }
    }
    return .{ .allocator = a, .first_use = first, .input_last_use = input_last, .runtime_last_use = runtime };
}

const Fixture = struct {
    fn matches(_: *anyopaque, _: region.Validate) bool {
        return true;
    }
    fn validate(_: *anyopaque, _: region.Validate) !void {}
    fn dispatch(_: *anyopaque, _: region.Invocation) !region.Outcome {
        return error.UnexpectedNumericalDispatch;
    }
    fn deinit(_: *anyopaque) void {}
    const table = region.Owned.VTable{ .matches = matches, .validate = validate, .dispatch = dispatch, .deinit = deinit };
    fn check(a: std.mem.Allocator) !void {
        const partition = @import("../graph/partition.zig");
        var graph = ml.graph.Graph.init(a);
        defer graph.deinit();
        var builder = ml.graph.Builder.init(&graph);
        const shape = ml.graph.Shape.init(.f32, &.{4});
        const input = try builder.parameter("x", shape);
        const elided = try builder.neg(input);
        const entry = try builder.neg(elided);
        _ = try builder.neg(input); // unreachable consumer cannot extend proof
        try graph.markOutput(entry);
        const capabilities = [_]partition.Capability{.{ .backend = .native, .priority = 0, .supports = &partition.supportsAll }};
        var schedule = try partition.partition(a, &graph, &capabilities);
        defer schedule.deinit();
        var physical = try buffers.build(a, &graph, &schedule, .{});
        defer physical.deinit();
        const reachable = try interpreter.computeReachable(a, &graph);
        defer a.free(reachable);
        var marker: u8 = 0;
        const outputs = [_]region.Output{.{ .node = entry, .logical = shape, .bytes = 16 }};
        const owner = region.Owned{ .context = &marker, .implementation = .{ .context = &marker, .table = &marker }, .key = 1, .policy_key = 2, .covered_nodes = &.{ elided, entry }, .input_nodes = &.{input}, .entry_node = entry, .requirements = .{ .outputs = &outputs, .uncovered = .{} }, .vtable = &table };
        var lifetimes = try create(a, &graph, reachable, &physical, &.{&owner});
        defer lifetimes.deinit();
        try std.testing.expectEqual(entry, lifetimes.runtime_last_use[input]);
        try std.testing.expectEqual(physical.positionForNode(entry), lifetimes.input_last_use[input]);
        const original_last_use = physical.slotForNode(input).?.last_use;
        try @import("region_recolor.zig").applyWithLastUse(&physical, lifetimes.first_use, lifetimes.input_last_use);
        // A preexisting conservative bound may already extend past entry.
        // Recoloring must preserve it, never shorten it to the new minimum.
        try std.testing.expect(physical.slotForNode(input).?.last_use >= physical.positionForNode(entry));
        try std.testing.expect(physical.slotForNode(input).?.last_use >= original_last_use);
        try physical.validate(&graph, &schedule);
    }
};

test "frozen region retains an activation past elided consumer using separate runtime and physical times" {
    try Fixture.check(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "frozen region cannot consume an external activation before its producer is scheduled" {
    const a = std.testing.allocator;
    const partition = @import("../graph/partition.zig");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var builder = ml.graph.Builder.init(&graph);
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const input = try builder.parameter("x", shape);
    const entry = try builder.neg(input);
    const late = try builder.neg(input);
    const output = try builder.add(entry, late);
    try graph.markOutput(output);
    const capabilities = [_]partition.Capability{.{ .backend = .native, .priority = 0, .supports = &partition.supportsAll }};
    var schedule = try partition.partition(a, &graph, &capabilities);
    defer schedule.deinit();
    var physical = try buffers.build(a, &graph, &schedule, .{});
    defer physical.deinit();
    const reachable = try interpreter.computeReachable(a, &graph);
    defer a.free(reachable);
    var marker: u8 = 0;
    const outputs = [_]region.Output{.{ .node = output, .logical = shape, .bytes = 16 }};
    const owner = region.Owned{ .context = &marker, .implementation = .{ .context = &marker, .table = &marker }, .key = 1, .policy_key = 2, .covered_nodes = &.{ entry, output }, .input_nodes = &.{ input, late }, .entry_node = entry, .requirements = .{ .outputs = &outputs, .uncovered = .{} }, .vtable = &Fixture.table };
    try std.testing.expectError(error.RegionInputUnavailableAtEntry, create(a, &graph, reachable, &physical, &.{&owner}));
}
