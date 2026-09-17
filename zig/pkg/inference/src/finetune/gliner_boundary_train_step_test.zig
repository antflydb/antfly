// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ml = @import("ml").graph;
const step = @import("gliner_boundary_train_step.zig");
const schema_mod = @import("../pipelines/extraction_schema.zig");
const processor = @import("../pipelines/gliner_boundary_processor.zig");
const targets = @import("gliner_boundary_targets.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const Allocator = std.mem.Allocator;
pub const TestTokenizer = struct {
    const markers = [_][]const u8{ "[P]", "[C]", "[E]", "[R]", "[L]", "[SEP_STRUCT]", "[SEP_TEXT]", "[DESCRIPTION]", "[EXAMPLE]", "[OUTPUT]" };
    state: u8 = 0,
    pub fn tokenizer(self: *TestTokenizer) @import("inference_tokenizer").Tokenizer {
        return .{ .ptr = self, .vtable = &.{ .encode = encode, .encodeInto = encodeInto, .encodeForModel = undefined, .encodeGeneration = undefined, .decode = undefined, .specialTokens = specialTokens, .allSpecialTokenIds = specialIds, .vocabSize = vocabSize, .deinit = undefined } };
    }
    fn encode(raw: *anyopaque, a: Allocator, text: []const u8) ![]i32 {
        var result = std.ArrayListUnmanaged(i32).empty;
        errdefer result.deinit(a);
        try encodeInto(raw, a, text, &result);
        return result.toOwnedSlice(a);
    }
    fn encodeInto(_: *anyopaque, a: Allocator, text: []const u8, output: *std.ArrayListUnmanaged(i32)) !void {
        var position: usize = 0;
        while (position < text.len) {
            var matched = false;
            for (markers, 0..) |marker, i| if (std.mem.startsWith(u8, text[position..], marker)) {
                try output.append(a, @intCast(300 + i));
                position += marker.len;
                matched = true;
                break;
            };
            if (!matched) {
                try output.append(a, @as(i32, text[position]) + 10);
                position += 1;
            }
        }
    }
    fn specialTokens(_: *anyopaque) @import("inference_tokenizer").SpecialTokens {
        return .{ .unk_id = 1 };
    }
    fn specialIds(_: *anyopaque, a: Allocator) ![]u32 {
        const output = try a.alloc(u32, markers.len);
        for (output, 0..) |*value, i| value.* = @intCast(300 + i);
        return output;
    }
    fn vocabSize(_: *anyopaque) usize {
        return 512;
    }
};
pub fn config() @import("../models/gliner_boundary.zig").Config {
    var value = @import("../architectures/gliner_boundary_engine.zig").TestBatch.config();
    value.max_len = 64;
    value.encoder = .{ .hidden_size = 4, .intermediate_size = 8, .num_hidden_layers = 1, .num_attention_heads = 2, .vocab_size = 512, .max_position_embeddings = 16, .position_buckets = 8, .layer_norm_eps = 1e-7, .hidden_dropout_prob = 0, .attention_probs_dropout_prob = 0, .pad_token_id = 0 };
    value.head.boundary_dim = 4;
    value.head.pair_dim = 4;
    value.head.content_dim = 2;
    value.head.boundary_attention_heads = 2;
    value.head.boundary_attention_layers = 1;
    value.head.boundary_attention_window = 1;
    value.head.candidate_attention_heads = 2;
    value.head.multihead_pair_compat_heads = 2;
    value.head.candidate_pool = .shared;
    value.head.candidate_attention_layers = 0;
    value.head.query_attention_layers = 0;
    value.head.dropout = 0;
    value.head.record_dim = 4;
    value.head.record_instance_queries = 2;
    value.head.pool_size = 32;
    value.head.pool_boundary_top_k = 32;
    value.head.min_pool_per_query = 0;
    value.head.max_gold_per_query = 4;
    value.head.negative_query_ratio = 0;
    value.head.enable_span_content = true;
    value.head.relation_pair_cap = 1024;
    value.head.relation_biaffine_content = true;
    return value;
}
const rich_schema =
    \\{"entities":["person","company"],"entity_attributes":{"tone":{"labels":["positive","negative"],"applies_to":["person"]}},"classifications":[{"name":"topics","labels":["meeting","billing"],"mode":"multi"}],"structures":{"deal":{"mode":"anchorless","fields":{"party":{"dtype":"str"},"state":{"dtype":"str","choices":["paid","due"]}}}},"relations":[{"type":"met"}]}
;
const entities = [_]targets.Entity{
    .{ .entity_type = 0, .source = .{ .start = 0, .end = 3 }, .attributes = &.{.{ .group = 0, .labels = &.{0} }} },
    .{ .entity_type = 1, .source = .{ .start = 4, .end = 8 } },
};
const records = [_]targets.Record{.{ .structure = 0, .id = "one-deal", .fields = &.{
    .{ .field = 0, .values = &.{.{ .document = &.{.{ .start = 0, .end = 3 }} }} },
    .{ .field = 1, .values = &.{.{ .choice = 0 }} },
} }};
fn annotation(fingerprint: [32]u8, supervised_classification: bool) targets.Annotations {
    return .{ .schema_fingerprint = fingerprint, .entities = &entities, .records = &records, .classifications = if (supervised_classification) &.{.{ .task = 0, .labels = &.{0} }} else &.{}, .relations = &.{.{ .relation_type = 0, .head = .{ .entity = 0 }, .tail = .{ .entity = 1 } }} };
}
const Weights = struct {
    allocator: Allocator,
    cb: *const ops.ComputeBackend,
    inputs: []interpreter.RuntimeInput,
    wrt: []ml.NodeId,
    fn deinit(self: *Weights) void {
        for (self.inputs) |input| self.cb.free(input.value);
        self.allocator.free(self.inputs);
        self.allocator.free(self.wrt);
    }
};
fn weights(a: Allocator, cb: *const ops.ComputeBackend, plan: *const step.Plan) !Weights {
    var list = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
    errdefer {
        for (list.items) |input| cb.free(input.value);
        list.deinit(a);
    }
    var wrt = std.ArrayListUnmanaged(ml.NodeId).empty;
    errdefer wrt.deinit(a);
    for (plan.graph.parameters.items) |id| {
        const node = plan.graph.node(id);
        const name = plan.graph.parameterName(node);
        if (std.mem.startsWith(u8, name, "__")) continue;
        const shape = node.output_shape;
        const values = try a.alloc(f32, @intCast(shape.numElements().?));
        defer a.free(values);
        for (values, 0..) |*value, i| {
            value.* = if (std.mem.endsWith(u8, name, ".bias")) 0 else if (std.mem.endsWith(u8, name, ".weight") and (std.mem.indexOf(u8, name, "norm") != null or std.mem.indexOf(u8, name, "LayerNorm") != null)) 1 else 0.07 * @sin(@as(f32, @floatFromInt(i + @as(usize, id) * 13 + 1)));
        }
        var dims: [8]i32 = undefined;
        for (shape.dims[0..shape.rank_], dims[0..shape.rank_]) |dim, *out| out.* = @intCast(dim);
        const tensor = try cb.fromFloat32Shape(values, dims[0..shape.rank_]);
        list.append(a, .{ .node_id = id, .value = tensor }) catch |err| {
            cb.free(tensor);
            return err;
        };
        try wrt.append(a, id);
    }
    const owned = try list.toOwnedSlice(a);
    errdefer {
        for (owned) |input| cb.free(input.value);
        a.free(owned);
    }
    return .{ .allocator = a, .cb = cb, .inputs = owned, .wrt = try wrt.toOwnedSlice(a) };
}

test "boundary training step runs mixed live encoder candidate record relation losses and preserves absence" {
    const a = std.testing.allocator;
    var schema = try schema_mod.compile(a, rich_schema, .{});
    defer schema.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada Acme", .schema = &schema }}, .{});
    defer prepared.deinit();
    var plan = try step.build(a, config(), &prepared, &.{&schema}, .{}, .training, .{});
    defer plan.deinit();
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var parameters = try weights(a, &cb, &plan);
    defer parameters.deinit();
    try plan.finalize(parameters.wrt, .{});
    var first = try plan.run(&cb, parameters.inputs, &prepared, &.{&schema}, &.{annotation(schema.fingerprint, true)}, .{ .identity = .{ .binding = @splat(1), .optimizer_step = 0, .microbatch = 0 }, .replay = .{ .seed = 4, .micro_batch = 0 }, .progress = .{ .optimizer_step = 0, .total_optimizer_steps = 100 }, .require_gold_relation_coverage = true }, null);
    defer first.deinit(&cb);
    try std.testing.expect(std.math.isFinite(first.terms.total) and first.terms.total > 0);
    try std.testing.expect(first.terms.classification > 0 and first.terms.record_object > 0 and first.terms.record_field > 0 and first.terms.relation > 0);
    try std.testing.expectEqual(@as(usize, 1), first.coverage.matched_records);
    try std.testing.expectEqual(@as(usize, 1), first.coverage.gold_relations);
    try std.testing.expectEqual(first.coverage.gold_relations, first.coverage.proposed_gold_relations);
    var encoder_live = false;
    for (first.backward.parameter_ids, first.backward.gradients.outputs) |id, gradient| {
        if (!std.mem.eql(u8, plan.graph.parameterName(plan.graph.node(id)), "embeddings.word_embeddings.weight")) continue;
        const values = try cb.toFloat32(gradient, a);
        defer a.free(values);
        for (values) |value| if (@abs(value) > 1e-10) {
            encoder_live = true;
            break;
        };
    }
    try std.testing.expect(encoder_live);
    var second = try plan.run(&cb, parameters.inputs, &prepared, &.{&schema}, &.{annotation(schema.fingerprint, false)}, .{ .identity = .{ .binding = @splat(2), .optimizer_step = 0, .microbatch = 1 }, .replay = .{ .seed = 4, .micro_batch = 1 }, .progress = .{ .optimizer_step = 0, .total_optimizer_steps = 100 }, .require_gold_relation_coverage = true }, null);
    defer second.deinit(&cb);
    try std.testing.expectEqual(@as(f32, 0), second.terms.classification);
    var absent_classifier: usize = 0;
    var touched_unused: usize = 0;
    for (second.presence) |presence| {
        const name = plan.graph.parameterName(plan.graph.node(presence.parameter));
        if (std.mem.startsWith(u8, name, "classifier.")) {
            try std.testing.expectEqual(step.GradientPresence.absent, presence.kind);
            absent_classifier += 1;
        }
        if (std.mem.startsWith(u8, name, "record_decoder.latent_seed_head.")) {
            try std.testing.expectEqual(step.GradientPresence.computed_zero, presence.kind);
            touched_unused += 1;
        }
    }
    try std.testing.expectEqual(@as(usize, 4), absent_classifier);
    try std.testing.expectEqual(@as(usize, 2), touched_unused);
    try std.testing.expect(!std.mem.eql(u8, &first.target_fingerprint, &second.target_fingerprint));
    try std.testing.expect(first.host_peak_bytes > 0 and first.host_peak_bytes <= plan.limits.max_step_host_bytes);
}

test "boundary training step rejects aggregate allocation limits and stale schema before forward" {
    const a = std.testing.allocator;
    var schema = try schema_mod.compile(a, "{\"entities\":[\"person\"]}", .{});
    defer schema.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada", .schema = &schema }}, .{});
    defer prepared.deinit();
    try std.testing.expectError(error.BoundaryTrainingStepLimitExceeded, step.build(a, config(), &prepared, &.{&schema}, .{}, .training, .{ .max_total_host_bytes = 1 }));
    try std.testing.expectError(error.InvalidBoundaryTrainingStepOptions, step.build(a, config(), &prepared, &.{&schema}, .{ .pool = 1 }, .training, .{}));
    var other = try schema_mod.compile(a, "{\"entities\":[\"company\"]}", .{});
    defer other.deinit();
    try std.testing.expectError(error.BoundaryTrainingSchemaMismatch, step.build(a, config(), &prepared, &.{&other}, .{}, .training, .{}));
}

test "boundary training step validates complete controlled dropout and deterministic replay" {
    const a = std.testing.allocator;
    var schema = try schema_mod.compile(a, rich_schema, .{});
    defer schema.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada Acme", .schema = &schema }}, .{});
    defer prepared.deinit();
    var settings = config();
    settings.encoder.hidden_dropout_prob = 0.125;
    settings.encoder.attention_probs_dropout_prob = 0.125;
    settings.head.dropout = 0.125;
    var plan = try step.build(a, settings, &prepared, &.{&schema}, .{}, .training, .{});
    defer plan.deinit();
    try plan.applyPeft(.{ .rank = 2, .alpha = 3, .dropout = 0.125 }, .{});
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    var parameters = try weights(a, &cb, &plan);
    defer parameters.deinit();
    try plan.finalize(parameters.wrt, .{});
    const descriptors = try plan.dropoutDescriptors(a);
    defer a.free(descriptors);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    const masks = try scratch.alloc(step.DropoutMask, descriptors.len);
    const buffers = try scratch.alloc([]f32, descriptors.len);
    var families: [3]bool = @splat(false);
    for (descriptors, masks, buffers, 0..) |descriptor, *mask, *buffer, site| {
        const count: usize = @intCast(descriptor.shape.numElements().?);
        buffer.* = try scratch.alloc(f32, count);
        for (buffer.*, 0..) |*value, i| value.* = if ((i + site) % 8 == 0) 0 else 1 / (1 - descriptor.probability);
        mask.* = .{ .name = descriptor.name, .values = buffer.* };
        if (std.mem.startsWith(u8, descriptor.name, "__gliner25.encoder.dropout.")) families[0] = true;
        if (std.mem.startsWith(u8, descriptor.name, "__gliner25.dropout.")) families[1] = true;
        if (std.mem.startsWith(u8, descriptor.name, "__boundary_peft_mask.")) families[2] = true;
    }
    try std.testing.expectEqualSlices(bool, &.{ true, true, true }, &families);
    const annotations = [_]targets.Annotations{annotation(schema.fingerprint, true)};
    var context = step.StepContext{ .identity = .{ .binding = @splat(41), .optimizer_step = 0, .microbatch = 0 }, .replay = .{ .seed = 19, .micro_batch = 0 }, .progress = .{ .optimizer_step = 0, .total_optimizer_steps = 10 }, .dropout_masks = masks, .require_gold_relation_coverage = true };
    context.dropout_masks = masks[0 .. masks.len - 1];
    try std.testing.expectError(error.MissingBoundaryTrainingDropoutMask, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    context.dropout_masks = masks;
    const name = masks[0].name;
    masks[0].name = "unknown-mask";
    try std.testing.expectError(error.UnexpectedBoundaryTrainingDropoutMask, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    masks[0].name = masks[1].name;
    try std.testing.expectError(error.DuplicateBoundaryTrainingDropoutMask, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    masks[0].name = name;
    const original = buffers[0][0];
    buffers[0][0] = std.math.nan(f32);
    try std.testing.expectError(error.InvalidBoundaryTrainingDropoutMask, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    buffers[0][0] = original;
    const complete = masks[0].values;
    masks[0].values = complete[0 .. complete.len - 1];
    try std.testing.expectError(error.InvalidBoundaryTrainingDropoutMask, plan.run(&cb, &.{}, &prepared, &.{&schema}, &annotations, context, null));
    masks[0].values = complete;
    var first = try plan.run(&cb, parameters.inputs, &prepared, &.{&schema}, &annotations, context, null);
    defer first.deinit(&cb);
    try std.testing.expect(first.dropout_fingerprint != null);
    // Set order is irrelevant; exact site identity and values govern replay.
    std.mem.reverse(step.DropoutMask, masks);
    var repeated = try plan.run(&cb, parameters.inputs, &prepared, &.{&schema}, &annotations, context, null);
    defer repeated.deinit(&cb);
    try std.testing.expectEqualDeep(first.terms, repeated.terms);
    try std.testing.expectEqual(first.dropout_fingerprint, repeated.dropout_fingerprint);
    try std.testing.expectEqual(first.decision_fingerprint, repeated.decision_fingerprint);
    try std.testing.expectEqualSlices(ml.NodeId, first.backward.parameter_ids, repeated.backward.parameter_ids);
    for (first.backward.gradients.outputs, repeated.backward.gradients.outputs) |one, two| {
        const x = try cb.toFloat32(one, a);
        defer a.free(x);
        const y = try cb.toFloat32(two, a);
        defer a.free(y);
        try std.testing.expectEqualSlices(f32, x, y);
    }
    for (masks) |mask| if (std.mem.eql(u8, mask.name, "__gliner25.dropout.marginals.start")) @memset(@constCast(mask.values), 0);
    var changed = try plan.run(&cb, parameters.inputs, &prepared, &.{&schema}, &annotations, context, null);
    defer changed.deinit(&cb);
    try std.testing.expect(!std.meta.eql(first.dropout_fingerprint, changed.dropout_fingerprint));
    try std.testing.expect(!std.mem.eql(u8, &first.decision_fingerprint, &changed.decision_fingerprint));
    try std.testing.expect(first.terms.start != changed.terms.start);
}

test "boundary training step construction failures require a fresh owned plan" {
    const a = std.testing.allocator;
    var schema = try schema_mod.compile(a, "{\"entities\":[\"person\"]}", .{});
    defer schema.deinit();
    var tokenizer = TestTokenizer{};
    var prepared = try processor.prepare(a, tokenizer.tokenizer(), &.{.{ .text = "Ada", .schema = &schema }}, .{});
    defer prepared.deinit();
    var plan = try step.build(a, config(), &prepared, &.{&schema}, .{}, .training, .{});
    defer plan.deinit();
    const parameter = blk: {
        for (plan.graph.parameters.items) |id| if (std.mem.eql(u8, plan.graph.parameterName(plan.graph.node(id)), "embeddings.word_embeddings.weight")) break :blk id;
        return error.MissingTrainingParameter;
    };
    try std.testing.expectError(error.TrainingTapeLimitExceeded, plan.finalize(&.{parameter}, .{ .max_tape_bytes = 0 }));
    try std.testing.expectError(error.BoundaryTrainingPlanInvalidated, plan.finalize(&.{parameter}, .{}));
    try std.testing.expectError(error.BoundaryTrainingPlanInvalidated, plan.dropoutDescriptors(a));
    var peft_plan = try step.build(a, config(), &prepared, &.{&schema}, .{}, .training, .{});
    defer peft_plan.deinit();
    try std.testing.expectError(error.InvalidBoundaryPeftConfig, peft_plan.applyPeft(.{ .rank = 0 }, .{}));
    try std.testing.expectError(error.BoundaryTrainingPlanInvalidated, peft_plan.applyPeft(.{ .rank = 2 }, .{}));
}
