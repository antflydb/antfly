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
fn number(value: std.json.Value) !f32 {
    return switch (value) {
        .float => @floatCast(value.float),
        .integer => @floatFromInt(value.integer),
        .bool => if (value.bool) 1 else 0,
        else => error.InvalidGraphOracle,
    };
}
fn values(a: Allocator, serialized: std.json.Value) ![]f32 {
    const array = try field(serialized, "values");
    if (array != .array) return error.InvalidGraphOracle;
    const out = try a.alloc(f32, array.array.items.len);
    errdefer a.free(out);
    for (array.array.items, out) |item, *destination| destination.* = try number(item);
    return out;
}
fn tensor(a: Allocator, cb: *const ops.ComputeBackend, expected: std.json.Value, shape: ml.Shape) !ops.CT {
    const data = try values(a, expected);
    defer a.free(data);
    const count = shape.numElements() orelse return error.InvalidGraphOracle;
    if (count < 0 or @as(u64, @intCast(count)) != data.len) return error.InvalidGraphOracle;
    var dims: [8]i32 = undefined;
    for (shape.dims[0..shape.rank_], dims[0..shape.rank_]) |dim, *out| out.* = std.math.cast(i32, dim) orelse return error.InvalidGraphOracle;
    return cb.fromFloat32Shape(data, dims[0..shape.rank_]);
}
fn bindingTensor(a: Allocator, cb: *const ops.ComputeBackend, expected: std.json.Value, binding: graph_mod.Binding) !ops.CT {
    if (binding.kind != .indices) {
        const data = try values(a, expected);
        defer a.free(data);
        try graph_mod.validateFloatBinding(binding, data);
        return tensor(a, cb, expected, binding.shape);
    }
    const serialized = try field(expected, "values");
    if (serialized != .array) return error.InvalidGraphOracle;
    const data = try a.alloc(i32, serialized.array.items.len);
    defer a.free(data);
    for (serialized.array.items, data) |item, *destination| {
        if (item != .integer) return error.InvalidGraphOracle;
        destination.* = std.math.cast(i32, item.integer) orelse return error.InvalidGraphOracle;
    }
    try graph_mod.validateIndexBinding(binding, data);
    var dims: [8]i32 = undefined;
    for (binding.shape.dims[0..binding.shape.rank_], dims[0..binding.shape.rank_]) |dim, *destination| destination.* = std.math.cast(i32, dim) orelse return error.InvalidGraphOracle;
    return (try cb.fromInt32Shape(data, dims[0..binding.shape.rank_])) orelse error.UnsupportedIntegerGraphBackend;
}
fn compare(a: Allocator, cb: *const ops.ComputeBackend, expected: std.json.Value, actual: ops.CT, tolerance: f32) !void {
    const wanted = try values(a, expected);
    defer a.free(wanted);
    const got = try cb.toFloat32(actual, a);
    defer a.free(got);
    try std.testing.expectEqual(wanted.len, got.len);
    for (wanted, got, 0..) |left, right, i| {
        errdefer std.debug.print("head graph value index={d} expected={d} actual={d}\n", .{ i, left, right });
        try std.testing.expectApproxEqAbs(left, right, tolerance * @max(@as(f32, 1), @abs(left)));
    }
}

fn checkGraphFixture(comptime with_candidates: bool, comptime use_staged: bool) !void {
    const a = std.testing.allocator;
    const bytes = try @import("gliner_boundary_parity_test.zig").fixtureBytes(a, if (with_candidates) "training_candidates.json" else "training_head.json");
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const fixture = parsed.value;
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", (try field(fixture, "source_commit")).string);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ((try field(fixture, "cases")).array.items) |case| {
        const case_id = (try field(case, "id")).string;
        errdefer std.debug.print("training head graph fixture: {s}, candidates={}\n", .{ case_id, with_candidates });
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var cfg = config();
        if (with_candidates) cfg.head.enable_span_content = true;
        var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 2, .classifications = 3 }, if ((try field(case, "training")).bool) .training else .evaluation, .{});
        defer g.deinit();
        const built = try build(&g);
        const outputs = if (with_candidates) blk: {
            const pool = try candidate_graph.poolInputs(&g, 3);
            const scored = try candidate_graph.buildSharedPool(&g, built.proposals, pool);
            break :blk built.outputs ++ [_]Output{
                .{ .name = "shared_logits", .node = scored.shared_logits },
                .{ .name = "proposal_logits", .node = scored.proposal_logits },
                .{ .name = "proposal_compat", .node = scored.proposal_compat },
                .{ .name = "candidate_features", .node = scored.candidate_features },
                .{ .name = "candidate_states", .node = scored.candidate_states.? },
            };
        } else built.outputs;
        var runtime = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer {
            for (runtime.items) |input| cb.free(input.value);
            runtime.deinit(a);
        }
        var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
        defer wrt.deinit(a);
        var expected_gradients = std.ArrayListUnmanaged(std.json.Value).empty;
        defer expected_gradients.deinit(a);
        const parameters = try field(fixture, "parameters");
        for (graph.parameters.items) |node| {
            const name = graph.parameterName(graph.node(node));
            if (std.mem.startsWith(u8, name, "__")) continue;
            const parameter = try field(parameters, name);
            const value = try tensor(a, &cb, parameter, graph.node(node).output_shape);
            runtime.append(a, .{ .node_id = node, .value = value }) catch |err| {
                cb.free(value);
                return err;
            };
            try wrt.append(a, node);
            try expected_gradients.append(a, try field(try field(case, "parameter_gradients"), name));
        }
        try std.testing.expectEqual(@as(usize, if (with_candidates) 72 else 46), wrt.items.len);
        try std.testing.expectEqual(parameters.object.count(), wrt.items.len);
        for (g.bindings.items) |binding| {
            const expected = if (std.mem.eql(u8, binding.name, "__gliner25.input.text")) try field(try field(fixture, "inputs"), "text_states") else if (std.mem.eql(u8, binding.name, "__gliner25.input.queries")) try field(try field(fixture, "inputs"), "query_states") else if (std.mem.eql(u8, binding.name, "__gliner25.input.classification")) try field(try field(fixture, "inputs"), "classification_states") else if (std.mem.eql(u8, binding.name, "__gliner25.input.text_mask")) try field(fixture, "text_mask") else if (std.mem.eql(u8, binding.name, "__gliner25.input.query_mask")) try field(fixture, "query_mask") else if (std.mem.eql(u8, binding.name, "__gliner25.pool.inside_mean")) try field(case, "inside_prefix_mean") else if (std.mem.startsWith(u8, binding.name, "__gliner25.pool.")) try field(try field(fixture, "geometry"), binding.name["__gliner25.pool.".len..]) else blk: {
                const route = try field(try field(fixture, "native_dropout_routes"), binding.name);
                break :blk try field(try field(fixture, "dropout_masks"), (try field(route, "source_mask")).string);
            };
            const value = try bindingTensor(a, &cb, expected, binding);
            errdefer cb.free(value);
            try runtime.append(a, .{ .node_id = binding.node, .value = value });
        }
        for (built.inputs, [_][]const u8{ "text_states", "query_states", "classification_states" }) |node, name| {
            try wrt.append(a, node);
            try expected_gradients.append(a, try field(try field(case, "input_gradients"), name));
        }
        var seeds: [if (with_candidates) 14 else 9]ml.autodiff.Seed = undefined;
        var cotangents = std.ArrayListUnmanaged(ops.CT).empty;
        defer {
            for (cotangents.items) |value| cb.free(value);
            cotangents.deinit(a);
        }
        for (outputs, &seeds) |output, *seed| {
            var name: [128]u8 = undefined;
            const shape = graph.node(output.node).output_shape;
            const cotangent = try builder.parameter(try std.fmt.bufPrint(&name, "__cotangent.{s}", .{output.name}), shape);
            seed.* = .{ .output = output.node, .cotangent = cotangent };
            const value = try tensor(a, &cb, try field(try field(case, "cotangents"), output.name), shape);
            errdefer cb.free(value);
            try cotangents.append(a, value);
        }
        var direct_session: ?seeded.Session = null;
        defer if (direct_session) |*session| session.deinit();
        var staged_session: ?staged.Session = null;
        defer if (staged_session) |*session| session.deinit();
        const identity = seeded.StepIdentity{ .binding = @splat(1), .optimizer_step = 0, .microbatch = 0 };
        var tape = if (use_staged) blk: {
            var deferred = std.ArrayListUnmanaged(ml.NodeId).empty;
            defer deferred.deinit(a);
            for (g.bindings.items) |binding| if (binding.stage == .candidates) try deferred.append(a, binding.node);
            var exposed: [9]ml.NodeId = undefined;
            for (built.outputs, &exposed) |output, *id| id.* = output.node;
            staged_session = try staged.Session.init(a, &graph, &seeds, wrt.items, .{ .deferred_parameters = deferred.items, .outputs = &exposed }, .{ .gradient = .{ .require_all_gradients = true } });
            var early = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
            defer early.deinit(a);
            for (runtime.items) |input| if (std.mem.indexOfScalar(ml.NodeId, deferred.items, input.node_id) == null) try early.append(a, input);
            var prefix = try staged_session.?.forwardPrefix(&cb, early.items, identity, null);
            defer prefix.deinit();
            for (built.outputs, 0..) |output, index| try compare(a, &cb, try field(try field(case, "outputs"), output.name), try prefix.logits(index), 3e-5);
            // The detached centering statistic is produced after the genuine
            // prefix forward, from those exact retained inside logits.
            const inside = try cb.toFloat32(try prefix.logits(3), a);
            defer a.free(inside);
            const text_mask = try values(a, try field(fixture, "text_mask"));
            defer a.free(text_mask);
            const query_mask = try values(a, try field(fixture, "query_mask"));
            defer a.free(query_mask);
            var means: [4]f32 = @splat(0);
            for (0..2) |b| for (0..2) |q| {
                if (query_mask[b * 2 + q] == 0) continue;
                var count: f32 = 0;
                for (0..3) |w| if (text_mask[b * 3 + w] != 0) {
                    means[b * 2 + q] += inside[(b * 2 + q) * 3 + w];
                    count += 1;
                };
                means[b * 2 + q] /= @max(count, 1);
            };
            var replaced = false;
            for (g.bindings.items) |binding| if (std.mem.eql(u8, binding.name, "__gliner25.pool.inside_mean")) {
                try graph_mod.validateFloatBinding(binding, &means);
                for (runtime.items) |*input| if (input.node_id == binding.node) {
                    const mean_tensor = try cb.fromFloat32Shape(&means, &.{ 2, 2, 1 });
                    cb.free(input.value);
                    input.value = mean_tensor;
                    replaced = true;
                };
            };
            try std.testing.expect(replaced);
            var late = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
            defer late.deinit(a);
            for (runtime.items) |input| if (std.mem.indexOfScalar(ml.NodeId, deferred.items, input.node_id) != null) try late.append(a, input);
            var fingerprint = std.crypto.hash.sha2.Sha256.init(.{});
            fingerprint.update("antfly.shared-pool-oracle.decisions.v1");
            fingerprint.update(bytes);
            fingerprint.update(case_id);
            fingerprint.update(std.mem.asBytes(&means));
            break :blk try prefix.forwardSuffix(identity, fingerprint.finalResult(), late.items, null);
        } else blk: {
            direct_session = try seeded.Session.init(a, &graph, &seeds, wrt.items, .{ .gradient = .{ .require_all_gradients = true } });
            break :blk try direct_session.?.forward(&cb, runtime.items, identity, null);
        };
        defer tape.deinit();
        var objective: f32 = 0;
        for (outputs, 0..) |output, index| {
            errdefer std.debug.print("head graph output: {s}\n", .{output.name});
            const value = try tape.logits(index);
            try compare(a, &cb, try field(try field(case, "outputs"), output.name), value, 3e-5);
            const data = try cb.toFloat32(value, a);
            defer a.free(data);
            const cotangent = try cb.toFloat32(cotangents.items[index], a);
            defer a.free(cotangent);
            var term: f32 = 0;
            for (data, cotangent) |x, dy| term += x * dy;
            objective += term;
        }
        try std.testing.expectApproxEqAbs(try number(try field(case, "objective")), objective, 0.02);
        const decisions: [32]u8 = @splat(2);
        try tape.sealDecisions(decisions);
        var result = try tape.backward(tape.identity, decisions, objective, cotangents.items, null);
        defer result.deinit(&cb);
        try std.testing.expectEqual(expected_gradients.items.len, result.gradients.outputs.len);
        try std.testing.expectEqualSlices(ml.NodeId, wrt.items, result.parameter_ids);
        for (expected_gradients.items, result.gradients.outputs, wrt.items) |expected, actual, node| {
            errdefer std.debug.print("head graph gradient: {s}\n", .{graph.parameterName(graph.node(node))});
            try compare(a, &cb, expected, actual, 5e-5);
        }
    }
}

test "boundary training graph stage one matches pinned forward and every input parameter VJP" {
    try checkGraphFixture(false, false);
}
test "boundary training graph shared candidates match pinned forward and every input parameter VJP" {
    try checkGraphFixture(true, false);
}
test "boundary training graph retained candidate stages match pinned values and every VJP" {
    try checkGraphFixture(true, true);
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

const TaskOracle = struct {
    const Leaf = struct { node: ml.NodeId, expected: std.json.Value, gradient: ?std.json.Value };
    allocator: Allocator,
    fixture: std.json.Value,
    case: std.json.Value,
    leaves: std.ArrayListUnmanaged(Leaf) = .empty,
    fn deinit(self: *TaskOracle) void {
        self.leaves.deinit(self.allocator);
    }
    fn leaf(self: *TaskOracle, g: *graph_mod.GraphBuilder, name: []const u8, dims: []const i64, kind: graph_mod.BindingKind, bound: usize, expected: std.json.Value, gradient: ?std.json.Value) !ml.NodeId {
        var buffer: [160]u8 = undefined;
        const node = try g.input(try std.fmt.bufPrint(&buffer, "__gliner25.oracle.{s}", .{name}), ml.Shape.init(if (kind == .indices) .i32 else .f32, dims), kind, .candidates, bound);
        try self.leaves.append(self.allocator, .{ .node = node, .expected = expected, .gradient = gradient });
        return node;
    }
    fn value(self: *TaskOracle, g: *graph_mod.GraphBuilder, name: []const u8, dims: []const i64) !ml.NodeId {
        return self.leaf(g, name, dims, .values, 0, try field(try field(self.fixture, "inputs"), name), try field(try field(self.case, "input_gradients"), name));
    }
    fn geometry(self: *TaskOracle, g: *graph_mod.GraphBuilder, name: []const u8, dims: []const i64, kind: graph_mod.BindingKind, bound: usize) !ml.NodeId {
        return self.leaf(g, name, dims, kind, bound, try field(try field(self.fixture, "geometry"), name), null);
    }
    fn execute(self: *TaskOracle, cb: *const ops.ComputeBackend, g: *graph_mod.GraphBuilder, outputs: []const Output) !void {
        const a = self.allocator;
        const graph = g.builder.graph;
        var runtime = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        defer {
            for (runtime.items) |input| cb.free(input.value);
            runtime.deinit(a);
        }
        var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
        defer wrt.deinit(a);
        var expected_gradients = std.ArrayListUnmanaged(std.json.Value).empty;
        defer expected_gradients.deinit(a);
        const parameters = try field(self.fixture, "parameters");
        for (graph.parameters.items) |node| {
            const name = graph.parameterName(graph.node(node));
            if (std.mem.startsWith(u8, name, "__")) continue;
            const parameter = try tensor(a, cb, try field(parameters, name), graph.node(node).output_shape);
            runtime.append(a, .{ .node_id = node, .value = parameter }) catch |err| {
                cb.free(parameter);
                return err;
            };
            try wrt.append(a, node);
            try expected_gradients.append(a, try field(try field(self.case, "parameter_gradients"), name));
        }
        try std.testing.expectEqual(parameters.object.count(), wrt.items.len);
        for (g.bindings.items) |binding| {
            var expected: ?std.json.Value = null;
            for (self.leaves.items) |entry| if (entry.node == binding.node) {
                expected = entry.expected;
                if (entry.gradient) |gradient| {
                    try wrt.append(a, entry.node);
                    try expected_gradients.append(a, gradient);
                }
            };
            if (expected == null) {
                const mask_name = if (self.fixture.object.get("native_dropout_routes")) |routes| (try field(try field(routes, binding.name), "source_mask")).string else binding.name;
                expected = try field(try field(self.fixture, "dropout_masks"), mask_name);
            }
            const uploaded = try bindingTensor(a, cb, expected.?, binding);
            errdefer cb.free(uploaded);
            try runtime.append(a, .{ .node_id = binding.node, .value = uploaded });
        }
        const seeds = try a.alloc(ml.autodiff.Seed, outputs.len);
        defer a.free(seeds);
        var cotangents = std.ArrayListUnmanaged(ops.CT).empty;
        defer {
            for (cotangents.items) |item| cb.free(item);
            cotangents.deinit(a);
        }
        for (outputs, seeds) |output, *seed| {
            var name: [192]u8 = undefined;
            const shape = graph.node(output.node).output_shape;
            seed.* = .{ .output = output.node, .cotangent = try g.builder.parameter(try std.fmt.bufPrint(&name, "__cotangent.{s}", .{output.name}), shape) };
            const uploaded = try tensor(a, cb, try field(try field(self.case, "cotangents"), output.name), shape);
            errdefer cb.free(uploaded);
            try cotangents.append(a, uploaded);
        }
        var session = try seeded.Session.init(a, graph, seeds, wrt.items, .{ .gradient = .{ .require_all_gradients = true } });
        defer session.deinit();
        const identity = seeded.StepIdentity{ .binding = @splat(17), .optimizer_step = 0, .microbatch = 0 };
        var tape = try session.forward(cb, runtime.items, identity, null);
        defer tape.deinit();
        var objective: f32 = 0;
        for (outputs, 0..) |output, index| {
            errdefer std.debug.print("task head output: {s}\n", .{output.name});
            const actual = try tape.logits(index);
            try compare(a, cb, try field(try field(self.case, "outputs"), output.name), actual, 3e-5);
            const data = try cb.toFloat32(actual, a);
            defer a.free(data);
            const cotangent = try cb.toFloat32(cotangents.items[index], a);
            defer a.free(cotangent);
            var term: f32 = 0;
            for (data, cotangent) |x, dy| term += x * dy;
            objective += term;
        }
        const decisions: [32]u8 = @splat(18);
        try tape.sealDecisions(decisions);
        var result = try tape.backward(identity, decisions, objective, cotangents.items, null);
        defer result.deinit(cb);
        try std.testing.expectEqualSlices(ml.NodeId, wrt.items, result.parameter_ids);
        try std.testing.expectEqual(expected_gradients.items.len, result.gradients.outputs.len);
        for (expected_gradients.items, result.gradients.outputs, wrt.items) |expected, actual, node| {
            errdefer std.debug.print("task head gradient: {s}\n", .{graph.parameterName(graph.node(node))});
            try compare(a, cb, expected, actual, 5e-5);
        }
    }
};

test "boundary training graph relations match pinned values and every input parameter VJP" {
    const a = std.testing.allocator;
    const bytes = try @import("gliner_boundary_parity_test.zig").fixtureBytes(a, "training_relations.json");
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const fixture = parsed.value;
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", (try field(fixture, "source_commit")).string);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ((try field(fixture, "cases")).array.items) |case| {
        const case_id = (try field(case, "id")).string;
        errdefer std.debug.print("relation training graph: {s}\n", .{case_id});
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var cfg = config();
        cfg.head.directional_relation_states = true;
        cfg.head.relation_biaffine_content = true;
        var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 0 }, if ((try field(case, "training")).bool) .training else .evaluation, .{});
        defer g.deinit();
        var oracle = TaskOracle{ .allocator = a, .fixture = fixture, .case = case };
        defer oracle.deinit();
        var text_indices: [4]ml.NodeId = undefined;
        for (&text_indices, (try field(try field(fixture, "geometry"), "text_indices")).array.items, 0..) |*node, expected, i| {
            var name: [64]u8 = undefined;
            node.* = try oracle.leaf(&g, try std.fmt.bufPrint(&name, "text_indices.{d}", .{i}), &.{6}, .indices, 6, expected, null);
        }
        const out = try task_graph.buildRelations(&g, .{
            .pairs = 6,
            .relations = 2,
            .text = try oracle.value(&g, "text_states", &.{ 6, 4 }),
            .relation_queries = try oracle.value(&g, "relation_query_states", &.{ 4, 8 }),
            .query_indices = try oracle.geometry(&g, "query_indices", &.{6}, .indices, 4),
            .text_indices = text_indices,
            .head_prefix_start = try oracle.geometry(&g, "head_prefix_start", &.{6}, .indices, 8),
            .head_prefix_end = try oracle.geometry(&g, "head_prefix_end", &.{6}, .indices, 8),
            .tail_prefix_start = try oracle.geometry(&g, "tail_prefix_start", &.{6}, .indices, 8),
            .tail_prefix_end = try oracle.geometry(&g, "tail_prefix_end", &.{6}, .indices, 8),
            .head_length = try oracle.geometry(&g, "head_length", &.{ 6, 1 }, .values, 0),
            .tail_length = try oracle.geometry(&g, "tail_length", &.{ 6, 1 }, .values, 0),
            .geometry = try oracle.geometry(&g, "geometry", &.{ 6, 2 }, .values, 0),
            .valid = try oracle.geometry(&g, "valid", &.{6}, .binary_mask, 0),
        });
        try oracle.execute(&cb, &g, &.{ .{ .name = "logits", .node = out.logits }, .{ .name = "features", .node = out.features }, .{ .name = "hidden", .node = out.hidden }, .{ .name = "mlp_logits", .node = out.mlp_logits }, .{ .name = "head_content", .node = out.head_content.? }, .{ .name = "tail_content", .node = out.tail_content.? } });
    }
}

test "boundary training graph explicit spans match pinned values and every input parameter VJP" {
    const a = std.testing.allocator;
    const bytes = try @import("gliner_boundary_parity_test.zig").fixtureBytes(a, "training_explicit.json");
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const fixture = parsed.value;
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", (try field(fixture, "source_commit")).string);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ((try field(fixture, "cases")).array.items) |case| {
        const case_id = (try field(case, "id")).string;
        errdefer std.debug.print("explicit training graph: {s}\n", .{case_id});
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var cfg = config();
        cfg.head.enable_span_content = true;
        cfg.head.enable_rotary_endpoints = true;
        cfg.head.query_conditioned_inside_weight = true;
        cfg.head.endpoint_difference_features = true;
        var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 2 }, if ((try field(case, "training")).bool) .training else .evaluation, .{});
        defer g.deinit();
        var oracle = TaskOracle{ .allocator = a, .fixture = fixture, .case = case };
        defer oracle.deinit();
        const boundary = try oracle.value(&g, "boundary_states", &.{ 8, 4 });
        const text = try oracle.value(&g, "text_states", &.{ 6, 4 });
        const queries = try oracle.value(&g, "query_states", &.{ 4, 4 });
        const text_mask = try oracle.leaf(&g, "text_mask", &.{ 2, 3 }, .binary_mask, 0, try field(fixture, "text_mask"), null);
        const query_mask = try oracle.leaf(&g, "query_mask", &.{ 2, 2 }, .binary_mask, 0, try field(fixture, "query_mask"), null);
        const proposal = graph_mod.Proposals{ .input = .{ .text = text, .queries = queries, .text_mask = text_mask, .query_mask = query_mask }, .boundary_states = boundary, .boundary_mask = try g.fill(&.{ 2, 4 }, 1), .start_logits = try oracle.value(&g, "start_logits", &.{ 2, 2, 4 }), .end_logits = try oracle.value(&g, "end_logits", &.{ 2, 2, 4 }), .inside_logits = try oracle.value(&g, "inside_logits", &.{ 2, 2, 3 }), .pool_start = boundary, .pool_end = boundary, .null_logits = null, .count_logits = null };
        const out = try candidate_graph.buildExplicitSpans(&g, proposal, .{
            .capacity = 3,
            .starts = try oracle.geometry(&g, "starts", &.{12}, .indices, 8),
            .ends = try oracle.geometry(&g, "ends", &.{12}, .indices, 8),
            .marginal_starts = try oracle.geometry(&g, "marginal_starts", &.{12}, .indices, 16),
            .marginal_ends = try oracle.geometry(&g, "marginal_ends", &.{12}, .indices, 16),
            .valid = try oracle.geometry(&g, "valid", &.{ 2, 2, 3 }, .binary_mask, 0),
            .lengths = try oracle.geometry(&g, "lengths", &.{ 12, 1 }, .values, 0),
            .length_features = try oracle.geometry(&g, "length_features", &.{ 12, 3 }, .values, 0),
            .inside_mean = try oracle.leaf(&g, "inside_mean", &.{ 2, 2, 1 }, .values, 0, try field(case, "inside_prefix_mean"), null),
        });
        try oracle.execute(&cb, &g, &.{ .{ .name = "logits", .node = out.logits }, .{ .name = "proposal_compat", .node = out.proposal_compat } });
    }
}

test "boundary training graph dense records match pinned values and shared parameter input VJPs" {
    const a = std.testing.allocator;
    const bytes = try @import("gliner_boundary_parity_test.zig").fixtureBytes(a, "training_records.json");
    defer a.free(bytes);
    var parsed = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer parsed.deinit();
    const fixture = parsed.value;
    try std.testing.expectEqualStrings("3c913c7369301133d3b7699252074c4303ada50e", (try field(fixture, "source_commit")).string);
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    for ((try field(fixture, "cases")).array.items) |case| {
        const case_id = (try field(case, "id")).string;
        errdefer std.debug.print("dense record training graph: {s}\n", .{case_id});
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var cfg = config();
        cfg.head.record_instance_queries = 2;
        var g = try graph_mod.GraphBuilder.init(a, &builder, cfg, .{ .batch = 2, .words = 3, .queries = 3 }, .training, .{});
        defer g.deinit();
        var oracle = TaskOracle{ .allocator = a, .fixture = fixture, .case = case };
        defer oracle.deinit();
        const pool_states = try oracle.value(&g, "pool_states", &.{ 6, 4 });
        const query_states = try oracle.value(&g, "query_states", &.{ 6, 4 });
        const pair_logits = try oracle.value(&g, "pair_logits", &.{ 18, 1 });
        var instances: ?ml.NodeId = null;
        var objects: ?ml.NodeId = null;
        var assignments: ?ml.NodeId = null;
        const groups = (try field(case, "native_groups")).array.items;
        try std.testing.expectEqual(@as(usize, 6), groups.len);
        for (groups, 0..) |raw, group_index| {
            const mode = std.meta.stringToEnum(@import("../pipelines/extraction_schema.zig").RecordMode, (try field(raw, "mode")).string) orelse return error.InvalidGraphOracle;
            var name: [128]u8 = undefined;
            const pool_indices = try oracle.leaf(&g, try std.fmt.bufPrint(&name, "record.{d}.pool", .{group_index}), &.{3}, .indices, 6, try field(raw, "pool_indices"), null);
            const field_indices = try oracle.leaf(&g, try std.fmt.bufPrint(&name, "record.{d}.queries", .{group_index}), &.{2}, .indices, 6, try field(raw, "field_query_indices"), null);
            const natural = if (mode == .natural) blk: {
                const indices = try oracle.leaf(&g, try std.fmt.bufPrint(&name, "record.{d}.natural", .{group_index}), &.{3}, .indices, 18, try field(raw, "natural_logit_indices"), null);
                break :blk try g.reshape(try g.gather(pair_logits, indices, 3, 1), &.{3});
            } else null;
            const out = try task_graph.buildRecordGroupDense(&g, .{
                .mode = mode,
                .candidates = 3,
                .fields = 2,
                .candidate_states = try g.gather(pool_states, pool_indices, 3, 4),
                .field_queries = try g.gather(query_states, field_indices, 2, 4),
                // Only anchorless attention consumes this mask. Natural and
                // latent groups consume membership and instance masks instead;
                // keep their unused shape witness out of runtime bindings.
                .candidate_mask = if (mode == .anchorless) try oracle.leaf(&g, try std.fmt.bufPrint(&name, "record.{d}.candidate_mask", .{group_index}), &.{3}, .binary_mask, 0, try field(raw, "candidate_mask"), null) else try g.fill(&.{3}, 1),
                .field_membership = try oracle.leaf(&g, try std.fmt.bufPrint(&name, "record.{d}.membership", .{group_index}), &.{ 2, 3 }, .binary_mask, 0, try field(raw, "field_membership"), null),
                .instance_mask = try oracle.leaf(&g, try std.fmt.bufPrint(&name, "record.{d}.instance_mask", .{group_index}), &.{3}, .binary_mask, 0, try field(raw, "instance_mask"), null),
                .natural_object_logits = natural,
            });
            instances = if (instances) |previous| try builder.concat(previous, out.instance_states, 0) else out.instance_states;
            const object_rows = try g.reshape(out.object_logits, &.{ 3, 1 });
            objects = if (objects) |previous| try builder.concat(previous, object_rows, 0) else object_rows;
            const assignment_rows = try g.reshape(out.assignment_logits, &.{ 3, 8 });
            assignments = if (assignments) |previous| try builder.concat(previous, assignment_rows, 0) else assignment_rows;
        }
        try oracle.execute(&cb, &g, &.{ .{ .name = "instance_states", .node = try g.reshape(instances.?, &.{ 2, 3, 3, 4 }) }, .{ .name = "object_logits", .node = try g.reshape(objects.?, &.{ 2, 3, 3 }) }, .{ .name = "assignment_logits", .node = try g.reshape(assignments.?, &.{ 2, 3, 3, 2, 4 }) } });
    }
}
