// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Semantic encoder regions and an independently owned head graph. This cut
//! happens before constructing any head differentiation or retained tape.
//! The original, once-injected graph remains the canonical optimizer namespace.
const std = @import("std");
const ml = @import("ml").graph;
const encoder = @import("gliner_boundary_encoder_graph.zig");
const recomputed = @import("../graph/recomputed_training.zig");
const staged = @import("../graph/multi_stage_training.zig");
const Allocator = std.mem.Allocator;
const Id = ml.NodeId;
const nil = ml.null_node;

pub const HeadSpec = struct {
    outputs: []const Id,
    boundaries: []const staged.Boundary = &.{},
};

pub const Built = struct {
    allocator: Allocator,
    regional: recomputed.Plan,
    head: ml.lower.LowerResult,
    /// Cut-head IDs to the original graph. Seed leaves are deliberately absent.
    original_ids: []Id,
    final_hidden: Id,
    seeds: []ml.autodiff.Seed,
    parameters: []Id,
    boundaries: []staged.Boundary,

    pub fn deinit(self: *Built) void {
        for (self.boundaries) |boundary| {
            self.allocator.free(boundary.outputs);
            self.allocator.free(boundary.deferred_parameters);
        }
        self.allocator.free(self.boundaries);
        self.allocator.free(self.parameters);
        self.allocator.free(self.seeds);
        self.allocator.free(self.original_ids);
        self.head.deinit();
        self.regional.deinit();
        self.* = undefined;
    }

    pub fn headId(self: *const Built, original: Id) Id {
        return if (original < self.head.id_map.len) self.head.id_map[original] else nil;
    }

    pub fn originalParameter(self: *const Built, local: Id) !Id {
        if (local == self.final_hidden or local >= self.original_ids.len) return error.InvalidRecomputeGradient;
        const original = self.original_ids[local];
        if (original == nil or self.regional.source.node(original).op != .parameter)
            return error.InvalidRecomputeGradient;
        return original;
    }
};

fn validateRegions(graph: *const ml.Graph, regions: encoder.EncoderRegions) !void {
    if (regions.layers.len == 0 or regions.layers.len > 255 or regions.embedding_output >= graph.nodeCount() or
        regions.normalized_relative >= graph.nodeCount() or regions.output >= graph.nodeCount())
        return error.InvalidBoundaryEncoderRegion;
    var previous = regions.embedding_output;
    const hidden_shape = graph.node(previous).output_shape;
    if (hidden_shape.dtype != .f32 or hidden_shape.rank_ != 2) return error.InvalidBoundaryEncoderRegion;
    const relative_shape = graph.node(regions.normalized_relative).output_shape;
    if (relative_shape.dtype != .f32 or relative_shape.rank_ != 2 or relative_shape.dims[1] != hidden_shape.dims[1])
        return error.InvalidBoundaryEncoderRegion;
    for (regions.layers, 0..) |layer, ordinal| {
        if (layer.ordinal != ordinal or layer.input != previous or layer.output >= graph.nodeCount() or
            !graph.node(layer.output).output_shape.eq(hidden_shape)) return error.InvalidBoundaryEncoderRegion;
        previous = layer.output;
    }
    if (regions.output != previous) return error.InvalidBoundaryEncoderRegion;
}

fn mappedSlice(a: Allocator, ids: []const Id, mapping: []const Id) ![]Id {
    const output = try a.alloc(Id, ids.len);
    errdefer a.free(output);
    for (ids, output) |id, *local| {
        if (id >= mapping.len or mapping[id] == nil) return error.InvalidTrainingStage;
        local.* = mapping[id];
    }
    return output;
}

/// Compile regional programs, then cut the whole encoder out of the head.
/// The result is intentionally unsealed: the caller must construct its direct
/// or staged head Session, then supply complete enclosing execution admission.
pub fn build(a: Allocator, graph: *const ml.Graph, regions: encoder.EncoderRegions, selected: []const Id, replay_inputs: []const recomputed.ReplayInput, spec: HeadSpec, options: recomputed.Options) !Built {
    try validateRegions(graph, regions);
    if (spec.outputs.len == 0 or spec.boundaries.len >= staged.max_stages) return error.InvalidTrainingStage;
    var scratch_arena = std.heap.ArenaAllocator.init(a);
    defer scratch_arena.deinit();
    const scratch = scratch_arena.allocator();
    const specs = try scratch.alloc(recomputed.RegionSpec, regions.layers.len + 1);
    const pairs = try scratch.alloc([2]Id, regions.layers.len + 1);
    const outputs = try scratch.alloc([1]Id, regions.layers.len);
    pairs[0] = .{ regions.embedding_output, regions.normalized_relative };
    specs[0] = .{ .key = 0, .inputs = &.{}, .outputs = &pairs[0] };
    for (regions.layers, 0..) |layer, i| {
        pairs[i + 1] = .{ layer.input, regions.normalized_relative };
        outputs[i] = .{layer.output};
        specs[i + 1] = .{ .key = @as(u64, layer.ordinal) + 1, .inputs = &pairs[i + 1], .outputs = &outputs[i] };
    }
    var managed = std.ArrayListUnmanaged(Id).empty;
    for (graph.parameters.items) |id| {
        const node = graph.node(id);
        if (!std.mem.startsWith(u8, graph.parameterName(node), "__")) try managed.append(scratch, id);
    }
    var regional = try recomputed.Plan.init(a, graph, specs, regions.output, selected, .{ .managed_parameters = managed.items, .replay_inputs = replay_inputs }, options);
    errdefer regional.deinit();
    var head = try regional.cutHead(a, spec.outputs);
    errdefer head.deinit();
    const final_hidden = head.id_map[regions.output];
    if (final_hidden == nil or head.graph.node(final_hidden).op != .parameter) return error.InvalidTrainingCut;
    const original_ids = try a.alloc(Id, head.graph.nodeCount());
    errdefer a.free(original_ids);
    @memset(original_ids, nil);
    for (head.id_map, 0..) |local, original| if (local != nil) {
        // Equivalent non-parameter views may share a lowered ID. Only named
        // parameter slots and the explicit final cut define this reverse map.
        if (original != regions.output and graph.node(@intCast(original)).op != .parameter) continue;
        if (original_ids[local] != nil or head.graph.node(local).op != .parameter)
            return error.InvalidTrainingCut;
        original_ids[local] = @intCast(original);
    };
    var selected_head = std.ArrayListUnmanaged(Id).empty;
    defer selected_head.deinit(a);
    for (selected) |id| if (head.id_map[id] != nil) try selected_head.append(a, head.id_map[id]);
    if (regional.needsBackward()) try selected_head.append(a, final_hidden);
    const parameters = try selected_head.toOwnedSlice(a);
    errdefer a.free(parameters);
    const seeds = try a.alloc(ml.autodiff.Seed, spec.outputs.len);
    errdefer a.free(seeds);
    var builder = ml.Builder.init(&head.graph);
    for (spec.outputs, seeds, 0..) |output, *seed, i| {
        const local = head.id_map[output];
        if (local == nil) return error.InvalidTrainingCut;
        var buffer: [96]u8 = undefined;
        const name = try std.fmt.bufPrint(&buffer, "__gliner25.recomputed_head.seed.{d}", .{i});
        seed.* = .{ .output = local, .cotangent = try builder.parameter(name, head.graph.node(local).output_shape) };
    }
    const boundaries = try a.alloc(staged.Boundary, spec.boundaries.len);
    errdefer a.free(boundaries);
    var initialized: usize = 0;
    errdefer for (boundaries[0..initialized]) |boundary| {
        a.free(boundary.outputs);
        a.free(boundary.deferred_parameters);
    };
    for (spec.boundaries, boundaries) |boundary, *local| {
        const exposed = try mappedSlice(a, boundary.outputs, head.id_map);
        errdefer a.free(exposed);
        const deferred = try mappedSlice(a, boundary.deferred_parameters, head.id_map);
        local.* = .{ .outputs = exposed, .deferred_parameters = deferred };
        initialized += 1;
    }
    return .{ .allocator = a, .regional = regional, .head = head, .original_ids = original_ids, .final_hidden = final_hidden, .seeds = seeds, .parameters = parameters, .boundaries = boundaries };
}

fn cutFixture(a: Allocator) !void {
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const shape = ml.Shape.init(.f32, &.{ 2, 2 });
    const input = try builder.parameter("__input", shape);
    const raw_relative = try builder.parameter("encoder.relative", shape);
    const embedding_weight = try builder.parameter("encoder.embedding", shape);
    const relative_weight = try builder.parameter("encoder.relative_norm", shape);
    const layer0_weight = try builder.parameter("encoder.layer.0", shape);
    const layer1_weight = try builder.parameter("encoder.layer.1", shape);
    const head_weight = try builder.parameter("classifier.weight", shape);
    const deferred = try builder.parameter("__head.detached_candidates", shape);
    const embedded = try builder.mul(input, embedding_weight);
    const relative = try builder.mul(raw_relative, relative_weight);
    const first = try builder.mul(try builder.add(embedded, relative), layer0_weight);
    const final = try builder.mul(try builder.add(first, relative), layer1_weight);
    const exposed = try builder.mul(final, head_weight);
    const output = try builder.add(exposed, deferred);
    var layers = [_]encoder.LayerRegion{
        .{ .ordinal = 0, .input = embedded, .output = first },
        .{ .ordinal = 1, .input = first, .output = final },
    };
    var built = try build(a, &graph, .{ .embedding_output = embedded, .normalized_relative = relative, .layers = &layers, .output = final }, &.{ embedding_weight, relative_weight, layer0_weight, layer1_weight, head_weight }, &.{}, .{ .outputs = &.{output}, .boundaries = &.{.{ .outputs = &.{exposed}, .deferred_parameters = &.{deferred} }} }, .{});
    defer built.deinit();
    try std.testing.expect(built.regional.needsBackward());
    try std.testing.expect(built.regional.admission == null);
    try std.testing.expectEqual(@as(usize, 2), built.parameters.len);
    try std.testing.expectEqual(built.final_hidden, built.parameters[1]);
    try std.testing.expectEqual(head_weight, try built.originalParameter(built.parameters[0]));
    try std.testing.expectError(error.InvalidRecomputeGradient, built.originalParameter(built.final_hidden));
    for ([_]Id{ embedded, relative, first, embedding_weight, relative_weight, layer0_weight, layer1_weight }) |id|
        try std.testing.expectEqual(nil, built.headId(id));
    try std.testing.expectEqual(built.headId(exposed), built.boundaries[0].outputs[0]);
    try std.testing.expectEqual(built.headId(deferred), built.boundaries[0].deferred_parameters[0]);
    // This differentiates only the compact head. Encoder gradients are built
    // and owned by the separate regional sessions already compiled above.
    var session = try staged.Session.init(a, &built.head.graph, built.seeds, built.parameters, built.boundaries, .{});
    defer session.deinit();
    try std.testing.expectEqual(@as(usize, 2), session.base.gradient_parameters.len);
    for (session.base.differentiated.graph.parameters.items) |id| {
        const name = session.base.differentiated.graph.parameterName(session.base.differentiated.graph.node(id));
        try std.testing.expect(!std.mem.startsWith(u8, name, "encoder."));
    }
}

test "boundary recomputed graph cuts before staged head differentiation and preserves original optimizer IDs" {
    try cutFixture(std.testing.allocator);
}

test "boundary recomputed graph regional and staged head ownership releases every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, cutFixture, .{});
}
