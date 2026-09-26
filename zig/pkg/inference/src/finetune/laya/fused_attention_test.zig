// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end parity between the dense materialized-bias training graph
//! (`architecture.build`) and the flash-style fused segment attention graph
//! (`architecture.buildWithAttention(..., true)`, roadmap step 2c), through
//! the real `training.inputs` runtime-input construction and the CPU
//! backend -- not just the isolated kernel (see `lib/linalg/src/attention.zig`
//! and `ops/segment_training_attention.zig` for that).
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../../ops/ops.zig");
const native = @import("../../ops/native_compute.zig");
const interpreter = @import("../../graph/interpreter.zig");
const modern = @import("../../architectures/modern_bert.zig");
const train = @import("training.zig");
const tree = @import("../../pipelines/laya_tree.zig");
const Kind = @import("../../models/laya.zig").QuestionType;

fn smallConfig() modern.Config {
    return .{
        .laya = .{ .head_layers = 1, .max_len = 64 },
        .vocab_size = 16,
        .hidden_size = 64,
        .num_hidden_layers = 2,
        .num_attention_heads = 2,
        .intermediate_size = 32,
        .checkpoint_layout = .huggingface_fused_qkv_no_bias,
        .rope_interleaved = false,
        .global_attn_every_n_layers = 2, // layer 0 global, layer 1 local
        .local_attention_window = 4,
    };
}

fn bindRandomWeights(a: std.mem.Allocator, cb: *const ops.ComputeBackend, graph: *const ml.Graph, seed: u64) ![]interpreter.RuntimeInput {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var result: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    errdefer for (result.items) |input| cb.free(input.value);
    for (graph.parameters.items) |id| {
        const name = graph.parameterName(graph.node(id));
        if (std.mem.startsWith(u8, name, "__")) continue;
        const shape = graph.node(id).output_shape;
        const n: usize = @intCast(shape.numElements().?);
        const values = try a.alloc(f32, n);
        defer a.free(values);
        for (values) |*v| v.* = random.floatNorm(f32) * 0.2;
        var dims: [8]i32 = undefined;
        for (shape.dims[0..shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);
        const value = try cb.fromFloat32Shape(values, dims[0..shape.rank()]) orelse return error.UnsupportedLayaTrainingBackend;
        errdefer cb.free(value);
        try result.append(a, .{ .node_id = id, .value = value });
    }
    return result.toOwnedSlice(a);
}

fn runLogits(a: std.mem.Allocator, cb: *const ops.ComputeBackend, cfg: modern.Config, examples: []const train.Example, use_fused_attention: bool, weight_seed: u64) ![]f32 {
    const l = try train.bucketedLayout(examples, cfg);
    var program = try train.Program.initFrozenFused(a, cfg, l, 0, 0, use_fused_attention);
    defer program.deinit();
    const weights = try bindRandomWeights(a, cb, &program.graph, weight_seed);
    defer for (weights) |input| cb.free(input.value);
    var prng = std.Random.DefaultPrng.init(0);
    const runtime = try train.inputs(a, cb, &program.graph, program.built, cfg, examples, prng.random(), false, use_fused_attention);
    defer for (runtime) |input| cb.free(input.value);
    const combined = try std.mem.concat(a, interpreter.RuntimeInput, &.{ weights, runtime });
    defer a.free(combined);
    var result = try interpreter.execute(a, &program.graph, cb, .{ .runtime_inputs = combined, .strict_integer_constants = true });
    defer result.deinit(cb);
    return cb.toFloat32(result.outputs[0], a);
}

test "fused segment attention matches the dense materialized-bias graph (unpacked, global and local layers)" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();

    const cfg = smallConfig();
    const ids = [_]i64{ 1, 2, 3, 4, 5 };
    const examples = [_]train.Example{
        .{ .ids = &ids, .markers = &.{ 0, 1 }, .kind = .noul, .target = &.{ 1, 0 } },
    };
    const dense = try runLogits(a, &cb, cfg, &examples, false, 42);
    defer a.free(dense);
    const fused = try runLogits(a, &cb, cfg, &examples, true, 42);
    defer a.free(fused);
    try std.testing.expectEqual(dense.len, fused.len);
    var worst: f32 = 0;
    for (dense, fused) |d, f| worst = @max(worst, @abs(d - f));
    try std.testing.expect(worst < 2e-3);
}

test "fused segment attention matches the dense graph on a tree-packed row" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();

    const cfg = smallConfig();
    // Trunk [0,3) plus two question branches, positions restart per branch,
    // matching `laya_tree`'s layout (LAYA.md, "Layout").
    const row = tree.Row{
        .ids = &.{ 10, 11, 12, 13, 14, 15, 16 },
        .positions = &.{ 0, 1, 2, 3, 4, 3, 4 },
        .segments = &.{ 0, 0, 0, 1, 1, 2, 2 },
        .parents = &.{ -1, 0, 0 },
        .kinds = &.{ tree.trunk_kind, tree.trunk_kind, tree.trunk_kind, 0, 0, 0, 0 },
        .anchors = &.{ 3, 5 },
        .markers = &.{ 4, -1, 6, -1 },
        .question_index = &.{ 0, 1 },
        .width = 2,
    };
    try tree.validate(row, 64, 64, 8);
    const packed_row = train.Packed{ .row = row, .kinds = &.{ .noul, .noul }, .targets = &.{ &.{ 1, 0 }, &.{ 0, 1 } } };
    const examples = [_]train.Example{.{ .ids = row.ids, .packed_row = &packed_row }};
    const dense = try runLogits(a, &cb, cfg, &examples, false, 7);
    defer a.free(dense);
    const fused = try runLogits(a, &cb, cfg, &examples, true, 7);
    defer a.free(fused);
    try std.testing.expectEqual(dense.len, fused.len);
    var worst: f32 = 0;
    for (dense, fused) |d, f| worst = @max(worst, @abs(d - f));
    try std.testing.expect(worst < 2e-3);
}
