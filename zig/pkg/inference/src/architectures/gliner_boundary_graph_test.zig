// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const graph_mod = @import("gliner_boundary_graph.zig");
const candidate_graph = @import("gliner_boundary_graph_candidates.zig");
const task_graph = @import("gliner_boundary_graph_tasks.zig");
const model = @import("../models/gliner_boundary.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const seeded = @import("../graph/seeded_training.zig");
const staged = @import("../graph/staged_training.zig");
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
        for ([_]@import("../pipelines/extraction_schema.zig").RecordMode{ .natural, .latent, .anchorless }) |mode| {
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
