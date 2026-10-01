// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const graph_mod = @import("boundary_graph.zig");
const candidate_graph = @import("boundary_graph_candidates.zig");
const task_graph = @import("boundary_graph_tasks.zig");
const model = @import("../../models/gliner_boundary.zig");
const native = @import("../../ops/native_compute.zig");
const ops = @import("../../ops/ops.zig");
const interpreter = @import("../../graph/interpreter.zig");
const seeded = @import("../../graph/seeded_training.zig");
const staged = @import("../../graph/staged_training.zig");
const Allocator = std.mem.Allocator;

fn config() model.Config {
    return .{ .version = model.config_version, .architecture_version = model.architecture_version, .max_len = 4096, .backbone = .small, .encoder = .{ .hidden_size = 4, .intermediate_size = 8, .num_hidden_layers = 1, .num_attention_heads = 2, .vocab_size = 8, .max_position_embeddings = 512, .position_buckets = 256, .layer_norm_eps = 1e-7, .hidden_dropout_prob = 0.1, .attention_probs_dropout_prob = 0.1, .pad_token_id = 0 }, .head = .{ .boundary_dim = 4, .pair_dim = 4, .boundary_attention_heads = 2, .boundary_attention_layers = 1, .boundary_attention_window = 1, .boundary_refinement_layers = 1, .boundary_ffn_multiplier = 2, .content_dim = 2, .multihead_pair_compat_heads = 2, .candidate_pool = .shared, .candidate_attention_layers = 0, .query_attention_layers = 0, .record_dim = 4 } };
}
const Output = struct { name: []const u8, node: ml.NodeId };
const Built = struct { inputs: [3]ml.NodeId, outputs: [9]Output, proposals: graph_mod.Proposals };
fn build(g: *graph_mod.GraphBuilder) !Built {
    const text = try g.input("__gliner25.input.text", ml.Shape.init(.f32, &.{ 6, 4 }), .values, .proposals, 0);
    const query = try g.input("__gliner25.input.queries", ml.Shape.init(.f32, &.{ 4, 4 }), .values, .proposals, 0);
    const choices = try g.input("__gliner25.input.classification", ml.Shape.init(.f32, &.{ 3, 4 }), .values, .proposals, 0);
    const text_mask = try g.input("__gliner25.input.text_mask", ml.Shape.init(.f32, &.{ 2, 3 }), .binary_mask, .proposals, 0);
    const query_mask = try g.input("__gliner25.input.query_mask", ml.Shape.init(.f32, &.{ 2, 2 }), .binary_mask, .proposals, 0);
    const proposal = try g.buildProposals(.{ .text = text, .queries = query, .text_mask = text_mask, .query_mask = query_mask });
    const classified = try g.buildClassification(choices, 3);
    try g.check();
    return .{ .inputs = .{ text, query, choices }, .proposals = proposal, .outputs = .{
        .{ .name = "boundary_states", .node = proposal.boundary_states },
        .{ .name = "start_logits", .node = proposal.start_logits },
        .{ .name = "end_logits", .node = proposal.end_logits },
        .{ .name = "inside_logits", .node = proposal.inside_logits },
        .{ .name = "pool_start", .node = proposal.pool_start },
        .{ .name = "pool_end", .node = proposal.pool_end },
        .{ .name = "null_logits", .node = proposal.null_logits.? },
        .{ .name = "count_logits", .node = proposal.count_logits.? },
        .{ .name = "classification_logits", .node = classified },
    } };
}
fn field(value: std.json.Value, name: []const u8) !std.json.Value {
    if (value != .object) return error.InvalidGraphOracle;
    return value.object.get(name) orelse error.InvalidGraphOracle;
}
fn fixtureCotangent(fixture: std.json.Value, case: std.json.Value, name: []const u8) !std.json.Value {
    if (case != .object) return error.InvalidGraphOracle;
    // An explicit case replaces the entire shared seed set, including zeros.
    return field(case.object.get("cotangents") orelse try field(fixture, "cotangents"), name);
}

test "boundary training graph shared cotangents retain explicit zero and reject incomplete overrides" {
    const a = std.testing.allocator;
    var parsed = try std.json.parseFromSlice(std.json.Value, a,
        \\{"cotangents":{"logits":1,"auxiliary":2},"cases":[{},
        \\{"cotangents":{"logits":0}},{"cotangents":null}]}
    , .{});
    defer parsed.deinit();
    const fixture = parsed.value;
    const cases = (try field(fixture, "cases")).array.items;
    try std.testing.expectEqual(@as(i64, 1), (try fixtureCotangent(fixture, cases[0], "logits")).integer);
    try std.testing.expectEqual(@as(i64, 0), (try fixtureCotangent(fixture, cases[1], "logits")).integer);
    try std.testing.expectError(error.InvalidGraphOracle, fixtureCotangent(fixture, cases[1], "auxiliary"));
    try std.testing.expectError(error.InvalidGraphOracle, fixtureCotangent(fixture, cases[2], "logits"));
    try std.testing.expectError(error.InvalidGraphOracle, fixtureCotangent(cases[0], cases[0], "logits"));
}

fn allocationLifecycle(a: Allocator) !void {
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var g = try graph_mod.GraphBuilder.init(a, &builder, config(), .{ .batch = 2, .words = 3, .queries = 2, .classifications = 3 }, .training, .{});
    defer g.deinit();
    _ = try build(&g);
}
test "boundary training graph releases descriptors and graph on allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationLifecycle, .{});
}

test "boundary training input gradients preserve independent shift accumulation" {
    const a = std.testing.allocator;
    for ([_]graph_mod.InputGradientProfile{ .grouped_v1, .pytorch_v2 }) |profile| {
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var g = try graph_mod.GraphBuilder.init(a, &builder, config(), .{ .batch = 2, .words = 3, .queries = 2, .classifications = 3 }, .training, .{});
        defer g.deinit();
        g.input_gradient_profile = profile;
        const built = try build(&g);
        var shifted: [2]ml.NodeId = .{ ml.null_node, ml.null_node };
        for (graph.nodes.items) |node| {
            if (node.op != .fused_linear) continue;
            const weight = graph.node(node.inputs[1]);
            if (weight.op != .parameter) continue;
            const name = graph.parameterName(weight);
            if (std.mem.eql(u8, name, "boundary_head.boundary_encoder.left_projection.weight")) shifted[0] = node.inputs[0];
            if (std.mem.eql(u8, name, "boundary_head.boundary_encoder.right_projection.weight")) shifted[1] = node.inputs[0];
        }
        for (shifted) |node| try std.testing.expect(node != ml.null_node);
        // Seed the real shifted views, plus a later text consumer. Independent
        // PyTorch FP32 views yield (-2^24 + 2^24) + 1 == 1; a shared shift view
        // instead yields -2^24 + (2^24 + 1) == 0. This observes VJP arithmetic,
        // including BOS/EOS routing, rather than asserting graph node counts.
        const side = try builder.mul(built.inputs[0], try builder.scalarConst(.f32, 1));
        var seeds: [3]ml.autodiff.Seed = undefined;
        for (shifted ++ .{side}, &seeds, 0..) |output, *seed, i| {
            var name: [32]u8 = undefined;
            seed.* = .{ .output = output, .cotangent = try builder.parameter(try std.fmt.bufPrint(&name, "shift_seed_{d}", .{i}), graph.node(output).output_shape) };
        }
        var ad = try ml.autodiff.gradientWithSeeds(a, &graph, &seeds, &.{built.inputs[0]}, .{ .require_all_gradients = true });
        defer ad.deinit();
        ad.graph.outputs.clearRetainingCapacity();
        try ad.graph.markOutput(ad.param_grads[0]);
        var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
        var compute = native.NativeCompute.init(a, &store, null);
        defer compute.deinit();
        var cb = compute.computeBackend();
        var bindings: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
        defer {
            for (bindings.items) |binding| cb.free(binding.value);
            bindings.deinit(a);
        }
        for (graph.parameters.items) |id| {
            if (ad.id_map[id] == ml.null_node) continue;
            const node = graph.node(id);
            const values = try a.alloc(f32, @intCast(node.output_shape.numElements().?));
            defer a.free(values);
            const fill: f32 = if (id == seeds[0].cotangent) 1 else if (id == seeds[1].cotangent) 16777216 else if (id == seeds[2].cotangent) -16777216 else if (std.mem.endsWith(u8, graph.parameterName(node), "text_mask")) 1 else 0;
            @memset(values, fill);
            const value = try cb.fromFloat32(values);
            errdefer cb.free(value);
            try bindings.append(a, .{ .node_id = ad.id_map[id], .value = value });
        }
        var result = try interpreter.execute(a, &ad.graph, &cb, .{ .runtime_inputs = bindings.items });
        defer result.deinit(&cb);
        const actual = try cb.toFloat32(result.outputs[0], a);
        defer a.free(actual);
        try std.testing.expectEqual(@as(usize, 24), actual.len);
        for (actual) |value| try std.testing.expectEqual(@as(f32, if (profile == .pytorch_v2) 1 else 0), value);
    }
}

test "boundary training graph validates shape masks index bounds cancellation and budget" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.Cancelled;
        }
    };
    try std.testing.expectError(error.Cancelled, graph_mod.GraphBuilder.init(a, &builder, config(), .{ .batch = 2, .words = 3, .queries = 2 }, .training, .{ .control = .{ .check_fn = Cancel.check } }));
    try std.testing.expectError(error.BoundaryTrainingGraphLimitExceeded, graph_mod.GraphBuilder.init(a, &builder, config(), .{ .batch = 2, .words = 3, .queries = 2 }, .training, .{ .max_words = 2 }));
    var g = try graph_mod.GraphBuilder.init(a, &builder, config(), .{ .batch = 2, .words = 3, .queries = 2 }, .training, .{ .max_attention_elements = 1 });
    defer g.deinit();
    try std.testing.expectError(error.BoundaryTrainingGraphLimitExceeded, build(&g));
    const binding = graph_mod.Binding{ .node = 0, .name = "mask", .shape = ml.Shape.init(.f32, &.{2}), .kind = .binary_mask, .stage = .proposals };
    try graph_mod.validateFloatBinding(binding, &.{ 0, 1 });
    try std.testing.expectError(error.InvalidBoundaryTrainingBinding, graph_mod.validateFloatBinding(binding, &.{ 0, 0.5 }));
    try std.testing.expectError(error.InvalidBoundaryTrainingBinding, graph_mod.validateFloatBinding(binding, &.{ 0, std.math.nan(f32) }));
    const index = graph_mod.Binding{ .node = 0, .name = "index", .shape = ml.Shape.init(.i32, &.{2}), .kind = .indices, .stage = .candidates, .index_bound = 3 };
    try graph_mod.validateIndexBinding(index, &.{ 0, 2 });
    try std.testing.expectError(error.InvalidBoundaryTrainingBinding, graph_mod.validateIndexBinding(index, &.{ 0, 3 }));
}

fn stageInput(g: *graph_mod.GraphBuilder, name: []const u8, dims: []const i64, kind: graph_mod.BindingKind, bound: usize) !ml.NodeId {
    var buffer: [160]u8 = undefined;
    return g.input(try std.fmt.bufPrint(&buffer, "__gliner25.{s}", .{name}), ml.Shape.init(if (kind == .indices) .i32 else .f32, dims), kind, .candidates, bound);
}

fn candidateLifecycle(a: Allocator) !void {
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var cfg = config();
    cfg.head.enable_span_content = true;
    var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 2, .classifications = 3 }, .training, .{});
    defer g.deinit();
    const built = try build(&g);
    _ = try candidate_graph.buildSharedPool(&g, built.proposals, try candidate_graph.poolInputs(&g, 3));
}

test "boundary training graph candidate scratch and descriptors survive allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, candidateLifecycle, .{});
}

test "boundary training graph builds explicit spans relations and every record mode with live inputs" {
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |extended| {
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var cfg = config();
        cfg.head.enable_span_content = true;
        cfg.head.enable_rotary_endpoints = extended;
        cfg.head.endpoint_difference_features = extended;
        cfg.head.query_conditioned_inside_weight = extended;
        cfg.head.directional_relation_states = extended;
        cfg.head.relation_biaffine_content = extended;
        cfg.head.record_instance_queries = 2;
        var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 2, .classifications = 3 }, .training, .{});
        defer g.deinit();
        const built = try build(&g);
        const pool = try candidate_graph.poolInputs(&g, 3);
        const scored = try candidate_graph.buildSharedPool(&g, built.proposals, pool);
        const explicit = try candidate_graph.buildExplicitSpans(&g, built.proposals, .{
            .capacity = 2,
            .starts = try stageInput(&g, "explicit.starts", &.{8}, .indices, 8),
            .ends = try stageInput(&g, "explicit.ends", &.{8}, .indices, 8),
            .marginal_starts = try stageInput(&g, "explicit.marginal_starts", &.{8}, .indices, 16),
            .marginal_ends = try stageInput(&g, "explicit.marginal_ends", &.{8}, .indices, 16),
            .valid = try stageInput(&g, "explicit.valid", &.{ 2, 2, 2 }, .binary_mask, 0),
            .lengths = try stageInput(&g, "explicit.lengths", &.{ 8, 1 }, .values, 0),
            .length_features = try stageInput(&g, "explicit.length_features", &.{ 8, 3 }, .values, 0),
            .inside_mean = pool.inside_mean,
        });
        try g.require(explicit.logits, ml.Shape.init(.f32, &.{ 2, 2, 2 }));
        const relation = try task_graph.buildRelations(&g, .{
            .pairs = 3,
            .relations = 1,
            .text = built.inputs[0],
            .relation_queries = try stageInput(&g, "relation.queries", &.{ 2, if (extended) 8 else 4 }, .values, 0),
            .query_indices = try stageInput(&g, "relation.query_indices", &.{3}, .indices, 2),
            .text_indices = .{
                try stageInput(&g, "relation.head_first", &.{3}, .indices, 6),
                try stageInput(&g, "relation.head_last", &.{3}, .indices, 6),
                try stageInput(&g, "relation.tail_first", &.{3}, .indices, 6),
                try stageInput(&g, "relation.tail_last", &.{3}, .indices, 6),
            },
            .head_prefix_start = try stageInput(&g, "relation.head_start", &.{3}, .indices, 8),
            .head_prefix_end = try stageInput(&g, "relation.head_end", &.{3}, .indices, 8),
            .tail_prefix_start = try stageInput(&g, "relation.tail_start", &.{3}, .indices, 8),
            .tail_prefix_end = try stageInput(&g, "relation.tail_end", &.{3}, .indices, 8),
            .head_length = try stageInput(&g, "relation.head_length", &.{ 3, 1 }, .values, 0),
            .tail_length = try stageInput(&g, "relation.tail_length", &.{ 3, 1 }, .values, 0),
            .geometry = try stageInput(&g, "relation.geometry", &.{ 3, 2 }, .values, 0),
            .valid = try stageInput(&g, "relation.valid", &.{3}, .binary_mask, 0),
        });
        try g.require(relation.logits, ml.Shape.init(.f32, &.{3}));
        try std.testing.expectEqual(extended, relation.head_content != null);
        const record_candidates = try g.gather(scored.candidate_states.?, try stageInput(&g, "record.candidate_indices", &.{3}, .indices, 6), 3, 4);
        const record_fields = try g.gather(built.inputs[1], try stageInput(&g, "record.field_indices", &.{2}, .indices, 4), 2, 4);
        const candidate_mask = try stageInput(&g, "record.candidate_mask", &.{3}, .binary_mask, 0);
        const membership = try stageInput(&g, "record.membership", &.{ 2, 3 }, .binary_mask, 0);
        const instance_mask = try stageInput(&g, "record.instance_mask", &.{3}, .binary_mask, 0);
        const natural_scores = try g.reshape(try g.gather(try g.reshape(scored.pair_logits, &.{ 12, 1 }), try stageInput(&g, "record.anchor_indices", &.{3}, .indices, 12), 3, 1), &.{3});
        for ([_]@import("../../pipelines/extraction_schema.zig").RecordMode{ .natural, .latent, .anchorless }) |mode| {
            const record = try task_graph.buildRecordGroupDense(&g, .{ .mode = mode, .candidates = 3, .fields = 2, .candidate_states = record_candidates, .field_queries = record_fields, .candidate_mask = candidate_mask, .field_membership = membership, .instance_mask = instance_mask, .natural_object_logits = if (mode == .natural) natural_scores else null });
            try g.require(record.assignment_logits, ml.Shape.init(.f32, &.{ 3, 2, 4 }));
            try g.require(record.object_logits, ml.Shape.init(.f32, &.{3}));
        }
        // Multiple record groups must share exact weight identities; neither
        // the source graph nor a subsequent PEFT rewrite may clone them.
        var projection_count: usize = 0;
        for (graph.parameters.items) |node| {
            if (std.mem.eql(u8, graph.parameterName(graph.node(node)), "record_decoder.inst_proj.weight")) projection_count += 1;
        }
        try std.testing.expectEqual(@as(usize, 1), projection_count);
        for (g.bindings.items) |binding| if (binding.kind == .indices) {
            try std.testing.expectEqual(ml.DType.i32, binding.shape.dtype);
            try std.testing.expectEqual(graph_mod.Stage.candidates, binding.stage);
            try std.testing.expect(binding.index_bound > 0);
        };
        try g.check();
    }
}

test "boundary training graph candidate construction fails closed at unsupported options and limits" {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var cfg = config();
    cfg.head.enable_span_content = true;
    var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 2, .classifications = 3 }, .evaluation, .{});
    defer g.deinit();
    const built = try build(&g);
    try std.testing.expectError(error.InvalidBoundaryTrainingGraphLayout, candidate_graph.poolInputs(&g, 0));
    const pool = try candidate_graph.poolInputs(&g, 3);
    g.config.head.candidate_attention_layers = 1;
    try std.testing.expectError(error.UnsupportedBoundaryTrainingGraphOption, candidate_graph.buildSharedPool(&g, built.proposals, pool));
    g.config.head.candidate_attention_layers = 0;
    var missing = pool;
    missing.inside_mean = null;
    try std.testing.expectError(error.MissingBoundaryTrainingInsideMean, candidate_graph.buildSharedPool(&g, built.proposals, missing));
    g.limits.max_tensor_elements = 16;
    try std.testing.expectError(error.BoundaryTrainingGraphLimitExceeded, candidate_graph.buildSharedPool(&g, built.proposals, pool));
    g.limits.max_tensor_elements = 64 * 1024 * 1024;
    g.limits.max_constant_bytes = graph.constant_pool.items.len;
    try std.testing.expectError(error.BoundaryTrainingGraphLimitExceeded, g.prefixSum(built.inputs[0], 2, 3, 4));
}

// Compare independently constructed per-group and batch graphs with the same
// weights and cotangents. Uneven group/field counts and an empty sample catch
// cross-sample routing, padding, and broadcast-adjoint mistakes.
fn recordBatchParity(batch: u32, candidates: u32) !void {
    const a = std.testing.allocator;
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var builder = ml.Builder.init(&graph);
    var cfg = config();
    cfg.head.record_instance_queries = 2;
    const instances = @max(candidates, cfg.head.record_instance_queries);
    var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = batch, .words = 3, .queries = 2 }, .training, .{});
    defer g.deinit();
    const pool = try builder.parameter("pool", ml.Shape.init(.f32, &.{ batch * candidates, 4 }));
    var groups: [3]task_graph.RecordBatchGroup = undefined;
    var individual: [3]task_graph.RecordOutput = undefined;
    for (&groups, &individual, [_]@import("../../pipelines/extraction_schema.zig").RecordMode{ .natural, .latent, .anchorless }, 0..) |*group, *out, mode, index| {
        const sample: u32 = if (batch > 1 and index == 2) 1 else 0;
        const fields: u32 = if (index == 1) 2 else 1;
        var name: [32]u8 = undefined;
        const query = try builder.parameter(try std.fmt.bufPrint(&name, "fields_{d}", .{index}), ml.Shape.init(.f32, &.{ fields, 4 }));
        const membership = try g.fill(&.{ fields, candidates }, 1);
        const natural = if (mode == .natural) try builder.parameter("natural", ml.Shape.init(.f32, &.{candidates})) else null;
        const active = if (mode == .anchorless) cfg.head.record_instance_queries else candidates;
        const mask = if (active == instances) try g.fill(&.{instances}, 1) else try builder.concat(try g.fill(&.{active}, 1), try g.fill(&.{instances - active}, 0), 0);
        group.* = .{ .sample = sample, .mode = mode, .fields = fields, .field_queries = query, .field_membership = membership, .instance_mask = mask, .natural_object_logits = natural };
        const indices = try a.alloc(i32, candidates);
        defer a.free(indices);
        for (indices, 0..) |*v, i| v.* = @intCast(sample * candidates + i);
        const ids = try builder.tensorConstBytes(std.mem.sliceAsBytes(indices), ml.Shape.init(.i32, &.{candidates}));
        out.* = try task_graph.buildRecordGroupDense(&g, .{ .mode = mode, .candidates = candidates, .fields = fields, .candidate_states = try g.gather(pool, ids, candidates, 4), .field_queries = query, .candidate_mask = try g.fill(&.{candidates}, 1), .field_membership = membership, .instance_mask = mask, .natural_object_logits = natural });
    }
    const input = task_graph.RecordBatchInput{ .candidates = candidates, .candidate_states = pool, .candidate_mask = try g.fill(&.{ batch, candidates }, 1), .groups = &groups };
    const batched = try task_graph.buildRecordBatchDense(&g, input);
    defer a.free(batched);
    const trainable_count = graph.parameters.items.len;
    var seeds: [2][9]ml.autodiff.Seed = undefined;
    for (individual, batched, 0..) |old, new, index| {
        const old_nodes = [_]ml.NodeId{ old.instance_states, old.object_logits, old.assignment_logits };
        const new_nodes = [_]ml.NodeId{ new.instance_states, new.object_logits, new.assignment_logits };
        for (old_nodes, new_nodes, 0..) |left, right, kind| {
            const shape = graph.node(left).output_shape;
            try g.require(right, shape);
            var name: [32]u8 = undefined;
            const cotangent = try builder.parameter(try std.fmt.bufPrint(&name, "seed_{d}_{d}", .{ index, kind }), shape);
            seeds[0][index * 3 + kind] = .{ .output = left, .cotangent = cotangent };
            seeds[1][index * 3 + kind] = .{ .output = right, .cotangent = cotangent };
        }
    }
    var reference: std.ArrayListUnmanaged([]f32) = .empty;
    defer {
        for (reference.items) |values| a.free(values);
        reference.deinit(a);
    }
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    var cb = compute.computeBackend();
    for (seeds, 0..) |arm_seeds, arm| {
        var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &arm_seeds, graph.parameters.items[0..trainable_count], .{ .require_all_gradients = true });
        defer gradients.deinit();
        gradients.graph.outputs.clearRetainingCapacity();
        for (arm_seeds) |seed| try gradients.graph.markOutput(gradients.id_map[seed.output]);
        for (gradients.param_grads) |node| try gradients.graph.markOutput(node);
        var inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
        defer {
            for (inputs.items) |binding| cb.free(binding.value);
            inputs.deinit(a);
        }
        for (graph.parameters.items, 0..) |node, parameter| {
            const shape = graph.node(node).output_shape;
            const values = try a.alloc(f32, @intCast(shape.numElements().?));
            defer a.free(values);
            for (values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast((i * 7 + parameter * 3) % 19)) - 9)) / 23;
            const tensor = try cb.fromFloat32(values);
            errdefer cb.free(tensor);
            try inputs.append(a, .{ .node_id = gradients.id_map[node], .value = tensor });
        }
        var result = try interpreter.execute(a, &gradients.graph, &cb, .{ .runtime_inputs = inputs.items });
        defer result.deinit(&cb);
        for (result.outputs, 0..) |tensor, index| {
            const values = try cb.toFloat32(tensor, a);
            if (arm == 0) {
                errdefer a.free(values);
                try reference.append(a, values);
            } else {
                defer a.free(values);
                try std.testing.expectEqual(reference.items[index].len, values.len);
                for (reference.items[index], values) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 5e-5 + 5e-5 * @abs(expected));
            }
        }
    }
    var invalid = groups;
    invalid[0].sample = batch;
    var rejected = input;
    rejected.groups = &invalid;
    try std.testing.expectError(error.InvalidBoundaryTrainingGraphLayout, task_graph.buildRecordBatchDense(&g, rejected));
    g.limits.max_tensor_elements = 1;
    try std.testing.expectError(error.BoundaryTrainingGraphLimitExceeded, task_graph.buildRecordBatchDense(&g, input));
}

test "boundary record batch preserves mixed-mode forward and VJPs with uneven padding" {
    try recordBatchParity(3, 3);
    try recordBatchParity(1, 1);
}
