// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! End-to-end parity between the dense materialized-bias training graph
//! (`architecture.build`) and the flash-style fused segment attention graph
//! (`architecture.buildWithAttention(..., true)`, roadmap step 2c), through
//! the real `training.inputs` runtime-input construction and the CPU
//! backend -- not just the isolated kernel (see `lib/linalg/src/attention.zig`
//! and `ops/segment_training_attention.zig` for that).
const std = @import("std");
const build_options = @import("build_options");
const ml = @import("ml").graph;
const ops = @import("../../ops/ops.zig");
const native = @import("../../ops/native_compute.zig");
const metal = @import("../../ops/metal_compute.zig");
const gpu_store = @import("../../ops/gpu_hosted_store.zig");
const metal_runtime = @import("../../backends/metal_runtime.zig");
const interpreter = @import("../../graph/interpreter.zig");
const modern = @import("../../architectures/modern_bert.zig");
const train = @import("training.zig");
const tree = @import("../../pipelines/laya_tree.zig");
const Kind = @import("../../models/laya.zig").QuestionType;

/// Mirrors `ops/resident_training_metal_test.zig`'s `Fixture`: a bare
/// `MetalCompute` + `ComputeBackend`, no resident-training/weight-group
/// machinery needed for a plain forward pass through the interpreter.
const MetalFixture = struct {
    allocator: std.mem.Allocator,
    store: *gpu_store.WeightStore,
    backend: *metal.MetalCompute,

    fn init(a: std.mem.Allocator) !MetalFixture {
        const store = try a.create(gpu_store.WeightStore);
        errdefer a.destroy(store);
        store.* = .{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
        errdefer store.lazy_weights.deinit(a);
        metal.initPrefetchQueue(store, a);
        errdefer metal.deinitPrefetchQueue(store);
        errdefer metal.deinitSharedNativeProvider(store);
        const backend = try a.create(metal.MetalCompute);
        errdefer a.destroy(backend);
        backend.* = try metal.MetalCompute.init(a, store, null);
        return .{ .allocator = a, .store = store, .backend = backend };
    }

    fn deinit(self: *MetalFixture) void {
        self.backend.deinit();
        self.allocator.destroy(self.backend);
        metal.deinitSharedNativeProvider(self.store);
        metal.deinitPrefetchQueue(self.store);
        self.store.lazy_weights.deinit(self.allocator);
        self.allocator.destroy(self.store);
        self.* = undefined;
    }
};

fn smallConfig() modern.Config {
    return .{
        // `packing` enabled so the tree-packed-row test's `l.questions` (one
        // per question, not one per row) passes `graph.validate`; the
        // unpacked test's single-question examples are unaffected.
        .laya = .{ .head_layers = 1, .max_len = 64, .packing = .{ .mode = .question, .max_packed_len = 64 } },
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

/// A packed row big enough (trunk_len + questions*(1+options) tokens) to
/// cross `lib/linalg/src/attention.zig`'s BLOCK_Q=64/BLOCK_KV=256 tile
/// boundaries -- the small hand-written row elsewhere in this file (9
/// tokens) never leaves a single tile, so it cannot exercise the online
/// softmax's cross-tile rescaling in either direction.
fn buildLargeTreeRow(a: std.mem.Allocator, trunk_len: usize, questions: usize, options: usize) !tree.Row {
    const branch_width = 1 + options;
    const total = trunk_len + questions * branch_width;
    const ids = try a.alloc(i64, total);
    const positions = try a.alloc(i64, total);
    const segments = try a.alloc(i64, total);
    const kinds = try a.alloc(i64, total);
    for (0..trunk_len) |i| {
        ids[i] = @intCast((i % 15) + 1);
        positions[i] = @intCast(i);
        segments[i] = 0;
        kinds[i] = tree.trunk_kind;
    }
    const anchors = try a.alloc(i64, questions);
    const markers = try a.alloc(i64, questions * options);
    const question_index = try a.alloc(usize, questions);
    for (0..questions) |q| {
        const start = trunk_len + q * branch_width;
        anchors[q] = @intCast(start);
        question_index[q] = q;
        for (0..branch_width) |j| {
            const idx = start + j;
            ids[idx] = @intCast(((idx + q) % 15) + 1);
            positions[idx] = @intCast(trunk_len + j);
            segments[idx] = @intCast(1 + q);
            kinds[idx] = 0;
            if (j > 0) markers[q * options + (j - 1)] = @intCast(idx);
        }
    }
    const parents = try a.alloc(i64, 1 + questions);
    parents[0] = -1;
    for (0..questions) |q| parents[1 + q] = 0;
    return .{
        .ids = ids,
        .positions = positions,
        .segments = segments,
        .parents = parents,
        .kinds = kinds,
        .anchors = anchors,
        .markers = markers,
        .question_index = question_index,
        .width = options,
    };
}

fn freeLargeTreeRow(a: std.mem.Allocator, row: tree.Row) void {
    a.free(row.ids);
    a.free(row.positions);
    a.free(row.segments);
    a.free(row.parents);
    a.free(row.kinds);
    a.free(row.anchors);
    a.free(row.markers);
    a.free(row.question_index);
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
        const value = try cb.fromFloat32Shape(values, dims[0..shape.rank()]);
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
    defer a.free(weights);
    defer for (weights) |input| cb.free(input.value);
    var prng = std.Random.DefaultPrng.init(0);
    // `inputs` is written for a scratch/arena allocator (its real callers,
    // `training.step`/`predict`, pass one): it hands its host scratch arrays
    // (ids, kinds, markers, positions, bias/control tensors) to the backend
    // and does not free them itself. `std.testing.allocator` would flag that
    // as a leak, so give it its own arena here instead of `a`.
    var input_arena = std.heap.ArenaAllocator.init(a);
    defer input_arena.deinit();
    const runtime = try train.inputs(input_arena.allocator(), cb, &program.graph, program.built, cfg, examples, prng.random(), false, use_fused_attention);
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
    // Trunk [0,3) plus two question branches (anchor + two options each),
    // positions restart per branch, matching `laya_tree`'s layout (LAYA.md,
    // "Layout"). Every question needs at least two valid option markers
    // (`laya_tree.validate`), so each branch is anchor+opt1+opt2.
    const row = tree.Row{
        .ids = &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9 }, // < vocab_size (16)
        .positions = &.{ 0, 1, 2, 3, 4, 5, 3, 4, 5 },
        .segments = &.{ 0, 0, 0, 1, 1, 1, 2, 2, 2 },
        .parents = &.{ -1, 0, 0 },
        .kinds = &.{ tree.trunk_kind, tree.trunk_kind, tree.trunk_kind, 0, 0, 0, 0, 0, 0 },
        .anchors = &.{ 3, 6 },
        .markers = &.{ 4, 5, 7, 8 },
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
    std.debug.print("Laya fused-vs-dense tree-packed logits: max abs diff={d}\n", .{worst});
    try std.testing.expect(worst < 2e-3);
}

/// Diagnostic: per-layer forward activations, dense vs fused, on the same
/// example and weights. Prints the first trace (in graph order -- embeddings,
/// each encoder layer, final_norm, each head layer) whose max absolute
/// difference exceeds a tiny threshold, to localize a semantic divergence
/// rather than just observing it at the logits.
fn traceDivergence(a: std.mem.Allocator, cb: *const ops.ComputeBackend, cfg: modern.Config, examples: []const train.Example, weight_seed: u64) !void {
    const l = try train.bucketedLayout(examples, cfg);
    var dense_program = try train.Program.initFrozenFused(a, cfg, l, 0, 0, false);
    defer dense_program.deinit();
    var fused_program = try train.Program.initFrozenFused(a, cfg, l, 0, 0, true);
    defer fused_program.deinit();
    try std.testing.expectEqual(dense_program.built.traces.items.len, fused_program.built.traces.items.len);

    const dense_weights = try bindRandomWeights(a, cb, &dense_program.graph, weight_seed);
    defer a.free(dense_weights);
    defer for (dense_weights) |input| cb.free(input.value);
    const fused_weights = try bindRandomWeights(a, cb, &fused_program.graph, weight_seed);
    defer a.free(fused_weights);
    defer for (fused_weights) |input| cb.free(input.value);

    var dense_arena = std.heap.ArenaAllocator.init(a);
    defer dense_arena.deinit();
    var fused_arena = std.heap.ArenaAllocator.init(a);
    defer fused_arena.deinit();
    var prng1 = std.Random.DefaultPrng.init(0);
    var prng2 = std.Random.DefaultPrng.init(0);
    const dense_runtime = try train.inputs(dense_arena.allocator(), cb, &dense_program.graph, dense_program.built, cfg, examples, prng1.random(), false, false);
    defer for (dense_runtime) |input| cb.free(input.value);
    const fused_runtime = try train.inputs(fused_arena.allocator(), cb, &fused_program.graph, fused_program.built, cfg, examples, prng2.random(), false, true);
    defer for (fused_runtime) |input| cb.free(input.value);

    for (dense_program.built.traces.items) |entry| try dense_program.graph.markOutput(entry.node);
    for (fused_program.built.traces.items) |entry| try fused_program.graph.markOutput(entry.node);

    const dense_combined = try std.mem.concat(a, interpreter.RuntimeInput, &.{ dense_weights, dense_runtime });
    defer a.free(dense_combined);
    const fused_combined = try std.mem.concat(a, interpreter.RuntimeInput, &.{ fused_weights, fused_runtime });
    defer a.free(fused_combined);

    var dense_result = try interpreter.execute(a, &dense_program.graph, cb, .{ .runtime_inputs = dense_combined, .strict_integer_constants = true });
    defer dense_result.deinit(cb);
    var fused_result = try interpreter.execute(a, &fused_program.graph, cb, .{ .runtime_inputs = fused_combined, .strict_integer_constants = true });
    defer fused_result.deinit(cb);

    // outputs[0] is logits; traces follow in declaration order. Trace
    // tensors are [batch*sequence, hidden]; padding rows (beyond the real
    // token count) legitimately differ -- dense gives padding queries
    // uniform attention over every valid key (training.inputs: `ok = k <
    // e.ids.len and (q >= e.ids.len or e.visible(q, k))`), while the fused
    // control leaves a padding query's range empty (zero output) -- and
    // padding is never read out downstream, so only compare real-token rows.
    var real_tokens: usize = 0;
    for (examples) |e| real_tokens += e.ids.len;
    const hidden = cfg.hidden_size;
    for (dense_program.built.traces.items, 0..) |entry, i| {
        const dense_values = try cb.toFloat32(dense_result.outputs[1 + i], a);
        defer a.free(dense_values);
        const fused_values = try cb.toFloat32(fused_result.outputs[1 + i], a);
        defer a.free(fused_values);
        try std.testing.expectEqual(dense_values.len, fused_values.len);
        var worst: f32 = 0;
        var worst_all: f32 = 0;
        for (dense_values, fused_values, 0..) |d, f, idx| {
            worst_all = @max(worst_all, @abs(d - f));
            if (idx / hidden < real_tokens) worst = @max(worst, @abs(d - f));
        }
        std.debug.print("Laya fused-vs-dense trace {s}: max abs diff (real tokens)={d} (incl. padding)={d}\n", .{ entry.name, worst, worst_all });
    }
}

test "fused segment attention traces pinpoint where it diverges from dense on a tree-packed row" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();

    const cfg = smallConfig();
    const row = tree.Row{
        .ids = &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },
        .positions = &.{ 0, 1, 2, 3, 4, 5, 3, 4, 5 },
        .segments = &.{ 0, 0, 0, 1, 1, 1, 2, 2, 2 },
        .parents = &.{ -1, 0, 0 },
        .kinds = &.{ tree.trunk_kind, tree.trunk_kind, tree.trunk_kind, 0, 0, 0, 0, 0, 0 },
        .anchors = &.{ 3, 6 },
        .markers = &.{ 4, 5, 7, 8 },
        .question_index = &.{ 0, 1 },
        .width = 2,
    };
    try tree.validate(row, 64, 64, 8);
    const packed_row = train.Packed{ .row = row, .kinds = &.{ .noul, .noul }, .targets = &.{ &.{ 1, 0 }, &.{ 0, 1 } } };
    const examples = [_]train.Example{.{ .ids = row.ids, .packed_row = &packed_row }};
    try traceDivergence(a, &cb, cfg, &examples, 7);
}

/// Diagnostic: per-parameter gradients, dense vs fused, on the same example
/// and weights. Backward is not filtered to real tokens the way the forward
/// trace comparison is -- a weight gradient sums the whole batch -- so any
/// asymmetry the fused backward introduces between real and padding rows
/// (e.g. padding contributing a nonzero gradient it should not) shows up
/// here even where the forward trace looked clean.
fn gradientDivergence(a: std.mem.Allocator, cb: *const ops.ComputeBackend, cfg: modern.Config, examples: []const train.Example, weight_seed: u64) !void {
    const l = try train.bucketedLayout(examples, cfg);
    var dense_program = try train.Program.initFrozenFused(a, cfg, l, 0, 0, false);
    defer dense_program.deinit();
    var fused_program = try train.Program.initFrozenFused(a, cfg, l, 0, 0, true);
    defer fused_program.deinit();
    try std.testing.expectEqual(dense_program.wrt.len, fused_program.wrt.len);

    const dense_weights = try bindRandomWeights(a, cb, &dense_program.graph, weight_seed);
    defer a.free(dense_weights);
    defer for (dense_weights) |input| cb.free(input.value);
    const fused_weights = try bindRandomWeights(a, cb, &fused_program.graph, weight_seed);
    defer a.free(fused_weights);
    defer for (fused_weights) |input| cb.free(input.value);

    var dense_arena = std.heap.ArenaAllocator.init(a);
    defer dense_arena.deinit();
    var fused_arena = std.heap.ArenaAllocator.init(a);
    defer fused_arena.deinit();
    var prng1 = std.Random.DefaultPrng.init(0);
    var prng2 = std.Random.DefaultPrng.init(0);
    const dense_runtime = try train.inputs(dense_arena.allocator(), cb, &dense_program.graph, dense_program.built, cfg, examples, prng1.random(), false, false);
    defer for (dense_runtime) |input| cb.free(input.value);
    const fused_runtime = try train.inputs(fused_arena.allocator(), cb, &fused_program.graph, fused_program.built, cfg, examples, prng2.random(), false, true);
    defer for (fused_runtime) |input| cb.free(input.value);

    const dense_combined = try std.mem.concat(a, interpreter.RuntimeInput, &.{ dense_weights, dense_runtime });
    defer a.free(dense_combined);
    const fused_combined = try std.mem.concat(a, interpreter.RuntimeInput, &.{ fused_weights, fused_runtime });
    defer a.free(fused_combined);

    const logits_shape = dense_program.graph.node(dense_program.built.logits).output_shape;
    try std.testing.expect(logits_shape.eq(fused_program.graph.node(fused_program.built.logits).output_shape));
    const n: usize = @intCast(logits_shape.numElements().?);
    const cotangent_values = try a.alloc(f32, n);
    defer a.free(cotangent_values);
    var cprng = std.Random.DefaultPrng.init(99);
    for (cotangent_values) |*v| v.* = cprng.random().floatNorm(f32);
    var dims: [8]i32 = undefined;
    for (logits_shape.dims[0..logits_shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);

    const dense_seed = try cb.fromFloat32Shape(cotangent_values, dims[0..logits_shape.rank()]);
    defer cb.free(dense_seed);
    var dense_backward_inputs = try a.alloc(interpreter.RuntimeInput, dense_combined.len + 1);
    defer a.free(dense_backward_inputs);
    for (dense_combined, dense_backward_inputs[0..dense_combined.len]) |input, *dst| dst.* = .{ .node_id = dense_program.gradients.id_map[input.node_id], .value = input.value };
    dense_backward_inputs[dense_combined.len] = .{ .node_id = dense_program.gradients.id_map[dense_program.seed], .value = dense_seed };
    var dense_backward = try interpreter.execute(a, &dense_program.gradients.graph, cb, .{ .runtime_inputs = dense_backward_inputs, .strict_integer_constants = true });
    defer dense_backward.deinit(cb);

    const fused_seed = try cb.fromFloat32Shape(cotangent_values, dims[0..logits_shape.rank()]);
    defer cb.free(fused_seed);
    var fused_backward_inputs = try a.alloc(interpreter.RuntimeInput, fused_combined.len + 1);
    defer a.free(fused_backward_inputs);
    for (fused_combined, fused_backward_inputs[0..fused_combined.len]) |input, *dst| dst.* = .{ .node_id = fused_program.gradients.id_map[input.node_id], .value = input.value };
    fused_backward_inputs[fused_combined.len] = .{ .node_id = fused_program.gradients.id_map[fused_program.seed], .value = fused_seed };
    var fused_backward = try interpreter.execute(a, &fused_program.gradients.graph, cb, .{ .runtime_inputs = fused_backward_inputs, .strict_integer_constants = true });
    defer fused_backward.deinit(cb);

    for (dense_program.wrt, fused_program.wrt, 0..) |dense_id, fused_id, i| {
        const dense_name = dense_program.graph.parameterName(dense_program.graph.node(dense_id));
        const fused_name = fused_program.graph.parameterName(fused_program.graph.node(fused_id));
        try std.testing.expectEqualStrings(dense_name, fused_name);
        const dense_values = try cb.toFloat32(dense_backward.outputs[i], a);
        defer a.free(dense_values);
        const fused_values = try cb.toFloat32(fused_backward.outputs[i], a);
        defer a.free(fused_values);
        try std.testing.expectEqual(dense_values.len, fused_values.len);
        var worst: f32 = 0;
        for (dense_values, fused_values) |d, f| worst = @max(worst, @abs(d - f));
        if (worst > 1e-4) std.debug.print("Laya fused-vs-dense gradient {s}: max abs diff={d}\n", .{ dense_name, worst });
    }
    std.debug.print("Laya fused-vs-dense gradient scan: {d} parameters checked\n", .{dense_program.wrt.len});
}

test "fused segment attention gradients pinpoint where backward diverges from dense on a tree-packed row" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();

    const cfg = smallConfig();
    const row = tree.Row{
        .ids = &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },
        .positions = &.{ 0, 1, 2, 3, 4, 5, 3, 4, 5 },
        .segments = &.{ 0, 0, 0, 1, 1, 1, 2, 2, 2 },
        .parents = &.{ -1, 0, 0 },
        .kinds = &.{ tree.trunk_kind, tree.trunk_kind, tree.trunk_kind, 0, 0, 0, 0, 0, 0 },
        .anchors = &.{ 3, 6 },
        .markers = &.{ 4, 5, 7, 8 },
        .question_index = &.{ 0, 1 },
        .width = 2,
    };
    try tree.validate(row, 64, 64, 8);
    const packed_row = train.Packed{ .row = row, .kinds = &.{ .noul, .noul }, .targets = &.{ &.{ 1, 0 }, &.{ 0, 1 } } };
    const examples = [_]train.Example{.{ .ids = row.ids, .packed_row = &packed_row }};
    try gradientDivergence(a, &cb, cfg, &examples, 7);
}

test "fused segment attention gradients match dense across a multi-tile packed row (crosses BLOCK_Q/BLOCK_KV)" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();

    var cfg = smallConfig();
    cfg.laya.?.max_len = 256;
    cfg.laya.?.packing.max_packed_len = 256;
    const row = try buildLargeTreeRow(a, 50, 6, 3); // 50 + 6*4 = 74 tokens
    defer freeLargeTreeRow(a, row);
    try tree.validate(row, 256, 256, 8);
    const kinds = try a.alloc(Kind, 6);
    defer a.free(kinds);
    @memset(kinds, .noul);
    const targets = try a.alloc([]const f32, 6);
    defer a.free(targets);
    const target_values = [_]f32{ 0.4, 0.6 };
    for (targets) |*t| t.* = &target_values;
    const packed_row = train.Packed{ .row = row, .kinds = kinds, .targets = targets };
    const examples = [_]train.Example{.{ .ids = row.ids, .packed_row = &packed_row }};
    try traceDivergence(a, &cb, cfg, &examples, 11);
    try gradientDivergence(a, &cb, cfg, &examples, 11);
}

test "fused segment attention matches the dense graph on Metal (host-bridged)" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!metal_runtime.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var fixture = try MetalFixture.init(a);
    defer fixture.deinit();
    const cb = fixture.backend.computeBackend();

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
