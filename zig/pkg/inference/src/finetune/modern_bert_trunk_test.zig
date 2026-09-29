// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The ModernBERT training trunk's fused attention profile against the
//! materialized one, and a long-sequence training step on Metal.
const std = @import("std");
const build_options = @import("build_options");
const ml = @import("ml").graph;
const trunk = @import("modern_bert_trunk.zig");
const modern = @import("../architectures/modern_bert.zig");
const native = @import("../ops/native_compute.zig");
const ops = @import("../ops/ops.zig");
const interpreter = @import("../graph/interpreter.zig");
const Allocator = std.mem.Allocator;

const Run = struct {
    hidden: []f32,
    gradients: [][]f32,
    names: [][]const u8,

    fn deinit(self: *Run, a: Allocator) void {
        a.free(self.hidden);
        for (self.gradients) |g| a.free(g);
        a.free(self.gradients);
        for (self.names) |n| a.free(n);
        a.free(self.names);
    }
};

fn upload(a: Allocator, cb: *const ops.ComputeBackend, list: *std.ArrayListUnmanaged(interpreter.RuntimeInput), node: ml.NodeId, shape: ml.Shape, values: anytype) !void {
    var dims: [8]i32 = undefined;
    for (shape.dims[0..shape.rank_], dims[0..shape.rank_]) |dim, *out| out.* = @intCast(dim);
    const value = if (@TypeOf(values) == []const i32 or @TypeOf(values) == []i32)
        (try cb.fromInt32Shape(values, dims[0..shape.rank_])) orelse return error.UnsupportedTestBackend
    else if (cb.kind() == .native)
        try cb.fromFloat32Shape(values, dims[0..shape.rank_])
    else
        // Metal gather/scatter VJPs take device-resident training tensors.
        try cb.residentTrainingPrimitive(&.{ .upload_f32 = .{ .values = values, .shape = dims[0..shape.rank_] } }, .{});
    list.append(a, .{ .node_id = node, .value = value }) catch |err| {
        cb.free(value);
        return err;
    };
}

/// One forward and backward pass of the trunk with deterministic weights and
/// a cotangent on the rows `seed_rows` marks. Returns the hidden states and
/// every weight gradient, in parameter order.
fn runTrunk(a: Allocator, cb: *const ops.ComputeBackend, cfg: modern.Config, l: trunk.Layout, lengths: []const usize, profile: trunk.AttentionProfile) !Run {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const scratch = arena.allocator();
    var graph = ml.Graph.init(a);
    defer graph.deinit();
    var b = ml.Builder.init(&graph);
    const n = l.batch * l.sequence;
    const nh = cfg.num_attention_heads;
    const d = cfg.hidden_size / nh;
    const table = ml.Shape.init(.f32, &.{ n * nh, d / 2 });
    const bias_shape = ml.Shape.init(.f32, &.{ l.batch * nh, l.sequence, l.sequence });
    const fused = profile == .fused_v1;
    const in = trunk.Inputs{
        .ids = try b.parameter("__ids", ml.Shape.init(.i32, &.{n})),
        .encoder_bias = if (fused) ml.null_node else try b.parameter("__global", bias_shape),
        .local_bias = if (fused) ml.null_node else try b.parameter("__local", bias_shape),
        .rope = .{
            .{ try b.parameter("__gc", table), try b.parameter("__gs", table) },
            .{ try b.parameter("__lc", table), try b.parameter("__ls", table) },
        },
        .profile = profile,
        .control = if (fused) try b.parameter("__control", trunk.controlShape(l)) else ml.null_node,
    };
    var sites = trunk.Sites{ .prefix = "__t" };
    defer sites.deinit(a);
    const hidden = try trunk.encoder(&b, &sites, cfg, l, in, "");
    try graph.markOutput(hidden);
    const seed = try b.parameter("__seed", graph.node(hidden).output_shape);
    var wrt: std.ArrayListUnmanaged(ml.NodeId) = .empty;
    defer wrt.deinit(a);
    for (graph.parameters.items) |id| if (!std.mem.startsWith(u8, graph.parameterName(graph.node(id)), "__")) try wrt.append(a, id);
    var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = hidden, .cotangent = seed }}, wrt.items, .{ .require_all_gradients = true });
    defer gradients.deinit();
    gradients.graph.outputs.clearRetainingCapacity();
    for (gradients.param_grads) |id| try gradients.graph.markOutput(id);

    var inputs: std.ArrayListUnmanaged(interpreter.RuntimeInput) = .empty;
    defer {
        for (inputs.items) |input| cb.free(input.value);
        inputs.deinit(a);
    }
    const key_valid = try scratch.alloc(bool, n);
    const ids = try scratch.alloc(i32, n);
    const positions = try scratch.alloc(i64, n);
    for (0..l.batch) |row| for (0..l.sequence) |i| {
        const token = row * l.sequence + i;
        key_valid[token] = i < lengths[row];
        ids[token] = if (i < lengths[row]) @intCast((token * 7 + 3) % cfg.vocab_size) else 0;
        positions[token] = @intCast(i);
    };
    try upload(a, cb, &inputs, in.ids, graph.node(in.ids).output_shape, @as([]const i32, ids));
    if (fused) {
        try upload(a, cb, &inputs, in.control, trunk.controlShape(l), @as([]const i32, try trunk.paddingControl(scratch, l, key_valid)));
    } else {
        const biases = try trunk.paddingBiases(scratch, cfg, l, key_valid);
        try upload(a, cb, &inputs, in.encoder_bias, bias_shape, @as([]const f32, biases[0]));
        try upload(a, cb, &inputs, in.local_bias, bias_shape, @as([]const f32, biases[1]));
    }
    for (in.rope, [_]f32{ cfg.global_rope_theta, cfg.local_rope_theta }) |pair, theta| {
        const tables = try trunk.ropeTables(scratch, positions, nh, d, theta);
        for (pair, tables) |node, values| try upload(a, cb, &inputs, node, table, @as([]const f32, values));
    }
    for (wrt.items) |id| {
        const shape = graph.node(id).output_shape;
        const name = graph.parameterName(graph.node(id));
        const values = try scratch.alloc(f32, @intCast(shape.numElements().?));
        const is_norm = std.mem.indexOf(u8, name, "norm") != null;
        // Seeded by name: node ids differ between the two profiles' graphs.
        const salt: usize = @intCast(std.hash.Wyhash.hash(0, name) % 4096);
        for (values, 0..) |*v, i| v.* = if (is_norm) 1 else 0.08 * @sin(@as(f32, @floatFromInt(i * 7 + salt * 13 + 1)));
        try upload(a, cb, &inputs, id, shape, @as([]const f32, values));
    }
    const forward_count = inputs.items.len;
    const cotangent = try scratch.alloc(f32, @intCast(graph.node(hidden).output_shape.numElements().?));
    for (cotangent, 0..) |*v, i| v.* = if (key_valid[i / cfg.hidden_size]) @cos(@as(f32, @floatFromInt(i)) * 0.37) else 0;
    try upload(a, cb, &inputs, seed, graph.node(seed).output_shape, @as([]const f32, cotangent));

    var forward = try interpreter.execute(a, &graph, cb, .{ .runtime_inputs = inputs.items[0..forward_count], .strict_integer_constants = true });
    defer forward.deinit(cb);
    const mapped = try scratch.alloc(interpreter.RuntimeInput, inputs.items.len);
    for (inputs.items, mapped) |input, *dst| dst.* = .{ .node_id = gradients.id_map[input.node_id], .value = input.value };
    var backward = try interpreter.execute(a, &gradients.graph, cb, .{ .runtime_inputs = mapped, .strict_integer_constants = true });
    defer backward.deinit(cb);
    var run = Run{ .hidden = try cb.toFloat32(forward.outputs[0], a), .gradients = try a.alloc([]f32, wrt.items.len), .names = try a.alloc([]const u8, wrt.items.len) };
    for (wrt.items, backward.outputs, 0..) |id, output, i| {
        run.gradients[i] = try cb.toFloat32(output, a);
        run.names[i] = try a.dupe(u8, graph.parameterName(graph.node(id)));
    }
    return run;
}

fn expectSameGradients(expected: Run, actual: Run, absolute: f32, relative: f32) !f32 {
    var worst: f32 = 0;
    for (expected.gradients, actual.gradients, expected.names) |want, got, name| {
        var scale: f32 = 0;
        for (want) |v| scale = @max(scale, @abs(v));
        for (want, got) |w, g| {
            try std.testing.expect(std.math.isFinite(g));
            worst = @max(worst, @abs(w - g));
            if (@abs(w - g) > absolute + relative * scale) {
                std.debug.print("ModernBERT trunk gradient {s}: {d} vs {d} (scale {d})\n", .{ name, w, g, scale });
                return error.TestExpectedApproxEqAbs;
            }
        }
    }
    return worst;
}

test "modern bert trunk fused attention equals the materialized trunk on valid rows and every gradient" {
    const a = std.testing.allocator;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const cfg = modern.Config{ .vocab_size = 40, .hidden_size = 16, .num_hidden_layers = 3, .num_attention_heads = 2, .intermediate_size = 24, .global_attn_every_n_layers = 3, .local_attention_window = 4, .checkpoint_layout = .huggingface_fused_qkv_no_bias, .rope_interleaved = false };
    const l = trunk.Layout{ .batch = 2, .sequence = 12 };
    for ([_][2]usize{ .{ 12, 12 }, .{ 12, 7 } }) |lengths| {
        var materialized = try runTrunk(a, &cb, cfg, l, &lengths, .materialized_v1);
        defer materialized.deinit(a);
        var fused = try runTrunk(a, &cb, cfg, l, &lengths, .fused_v1);
        defer fused.deinit(a);
        // Padded queries far from every valid key differ by design (zero rows
        // versus uniform weights) but never reach a valid row or the loss.
        for (0..l.batch) |row| for (0..lengths[row]) |i| {
            const at = (row * l.sequence + i) * cfg.hidden_size;
            for (materialized.hidden[at..][0..cfg.hidden_size], fused.hidden[at..][0..cfg.hidden_size]) |want, got| {
                errdefer std.debug.print("lengths {d},{d}: row {d} token {d}\n", .{ lengths[0], lengths[1], row, i });
                try std.testing.expectApproxEqAbs(want, got, 2e-5);
            }
        };
        _ = try expectSameGradients(materialized, fused, 2e-5, 1e-3);
    }
}

test "modern bert trunk fused attention trains 4096 tokens on Metal where materialized scores exceed the cap" {
    if (comptime !build_options.enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const cfg = modern.Config{ .vocab_size = 64, .hidden_size = 64, .num_hidden_layers = 2, .num_attention_heads = 8, .intermediate_size = 64, .global_attn_every_n_layers = 2, .local_attention_window = 128, .checkpoint_layout = .huggingface_fused_qkv_no_bias, .rope_interleaved = false };
    const l = trunk.Layout{ .batch = 1, .sequence = 4096 };
    // 4096^2 * 8 heads = 128Mi score elements per layer: over the 64Mi cap.
    try std.testing.expect(trunk.attentionScoreElements(cfg, l) > 64 * 1024 * 1024);
    const lengths = [_]usize{4000};
    var device = try @import("../graph/resident_training_fixture.zig").Device.init(a);
    defer device.deinit();
    const metal_cb = device.backend.computeBackend();
    const began = @import("antfly_platform").time.monotonicNs();
    var on_metal = try runTrunk(a, &metal_cb, cfg, l, &lengths, .fused_v1);
    defer on_metal.deinit(a);
    const metal_ns = @import("antfly_platform").time.monotonicNs() - began;
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const native_cb = compute.computeBackend();
    var on_cpu = try runTrunk(a, &native_cb, cfg, l, &lengths, .fused_v1);
    defer on_cpu.deinit(a);
    var nonzero: usize = 0;
    for (on_metal.gradients) |g| for (g) |v| {
        if (v != 0) nonzero += 1;
    };
    try std.testing.expect(nonzero > 0);
    const worst = try expectSameGradients(on_cpu, on_metal, 1e-4, 2e-3);
    std.debug.print("ModernBERT trunk 4096 tokens fused on Metal: forward+backward {d:.1} ms, max gradient difference vs CPU {d}\n", .{ @as(f64, @floatFromInt(metal_ns)) / 1e6, worst });
}
