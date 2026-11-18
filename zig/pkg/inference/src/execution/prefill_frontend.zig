// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Functional prefix program: immutable model weights and request embeddings
//! produce final hidden rows and explicit KV operands. GPU graphs retain only
//! program-owned storage, never a request's paged KV addresses or sequence ID.
const std = @import("std");
const capture = @import("capture.zig");
const ops = @import("../ops/ops.zig");
const gpt = @import("../architectures/gpt.zig");
const Config = @import("../models/gpt.zig").Config;
const Scope = @import("forward_kv_reuse.zig").Scope;
const maximum_layers = 128;
const output_names = names: {
    @setEvalBranchQuota(100000);
    var result: [1 + 2 * maximum_layers][]const u8 = undefined;
    result[0] = "hidden";
    for (0..maximum_layers) |layer| {
        result[1 + 2 * layer] = std.fmt.comptimePrint("kv.{d}.key", .{layer});
        result[2 + 2 * layer] = std.fmt.comptimePrint("kv.{d}.value", .{layer});
    }
    break :names result;
};

pub const HiddenOutput = struct {
    last_row: bool = false,
    final_norm: bool = true,
};

pub const Spec = struct {
    config: Config,
    rows: usize,
    output: HiddenOutput = .{},

    pub fn donorLayers(self: *const Spec) usize {
        return self.config.num_hidden_layers - self.config.num_kv_shared_layers;
    }
    pub fn resolve(self: *Spec) capture.Forward {
        return .{ .context = self, .fingerprint = self.config.fingerprint() ^ 0x70726566696c6c01 ^ @as(u64, @intFromBool(self.output.last_row)) ^ (@as(u64, @intFromBool(self.output.final_norm)) << 1), .optimization = .authored_schedule, .device_inputs = .assigned, .output_abi = abi, .validate = validate, .run = run };
    }
};

fn abi(raw: *anyopaque, _: []const capture.Input) ![]const []const u8 {
    const spec: *Spec = @ptrCast(@alignCast(raw));
    if (spec.config.num_kv_shared_layers > spec.config.num_hidden_layers or spec.config.num_hidden_layers > maximum_layers) return error.InvalidInputShape;
    return output_names[0 .. 1 + 2 * spec.donorLayers()];
}
fn validate(raw: *anyopaque, inputs: []const capture.Input) !void {
    const spec: *Spec = @ptrCast(@alignCast(raw));
    _ = try abi(raw, inputs);
    if (spec.rows <= 1 or spec.rows > spec.config.max_position_embeddings or inputs.len != (if (spec.config.hasPle()) @as(usize, 2) else 1)) return error.InvalidInputShape;
    for (inputs, 0..) |input, index| {
        const shape = try input.shape();
        const width = if (index == 0) spec.config.hidden_size else spec.config.num_hidden_layers * spec.config.ple_hidden_size;
        if (!std.mem.eql(u8, input.tensor.name, if (index == 0) "hidden" else "ple") or shape.dtype != .f32 or shape.rank() != 2 or
            shape.dims[0] != @as(i64, @intCast(spec.rows)) or shape.dims[1] != @as(i64, @intCast(width))) return error.InvalidInputShape;
    }
}
fn run(raw: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const spec: *Spec = @ptrCast(@alignCast(raw));
    try validate(raw, inputs);
    const names = try abi(raw, inputs);
    const outputs = try a.alloc(capture.Output, names.len);
    var initialized: usize = 0;
    errdefer {
        for (outputs[0..initialized]) |output| cb.free(output.value);
        a.free(outputs);
    }
    var scope = try Scope.init(a, cb, spec.config.num_hidden_layers, null);
    defer scope.deinit();
    const hidden = try inputs[0].value(cb);
    var owns_hidden = true;
    errdefer if (owns_hidden) cb.free(hidden);
    const ple = if (inputs.len == 2) try inputs[1].value(cb) else null;
    defer if (ple) |value| cb.free(value);
    const context = gpt.DecodeContext{ .total_sequence_len = spec.rows, .query_sequence_len = spec.rows, .kv_sequence_len = spec.rows, .forward_kv_reuse = scope.context() };
    owns_hidden = false;
    const final = try gpt.forwardFinalHiddenWithSharedKvOutputAndFinalNorm(cb, a, spec.config, hidden, .{}, 1, spec.rows, &context, ple, null, null, if (spec.output.last_row) .last else .all, spec.output.final_norm);
    outputs[0] = .{ .name = names[0], .value = final.hidden };
    initialized = 1;
    for (0..spec.donorLayers()) |layer| {
        const pair = try scope.retainDonor(layer);
        outputs[initialized] = .{ .name = names[initialized], .value = pair.key };
        initialized += 1;
        outputs[initialized] = .{ .name = names[initialized], .value = pair.value };
        initialized += 1;
    }
    return outputs;
}

test "functional prefix exports KV and preserves ordinary decoder hidden values" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    var weights = native.WeightStore{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} };
    defer weights.deinitOwned();
    const Item = struct { name: []const u8, shape: []const i64 };
    const inventory = [_]Item{
        .{ .name = "lm_head.weight", .shape = &.{ 8, 4 } },
        .{ .name = "model.layers.0.input_layernorm.weight", .shape = &.{4} },
        .{ .name = "model.layers.0.post_attention_layernorm.weight", .shape = &.{4} },
        .{ .name = "model.norm.weight", .shape = &.{4} },
        .{ .name = "model.layers.0.self_attn.q_proj.weight", .shape = &.{ 4, 4 } },
        .{ .name = "model.layers.0.self_attn.k_proj.weight", .shape = &.{ 4, 4 } },
        .{ .name = "model.layers.0.self_attn.v_proj.weight", .shape = &.{ 4, 4 } },
        .{ .name = "model.layers.0.self_attn.o_proj.weight", .shape = &.{ 4, 4 } },
        .{ .name = "model.layers.0.mlp.gate_proj.weight", .shape = &.{ 8, 4 } },
        .{ .name = "model.layers.0.mlp.up_proj.weight", .shape = &.{ 8, 4 } },
        .{ .name = "model.layers.0.mlp.down_proj.weight", .shape = &.{ 4, 8 } },
    };
    for (inventory) |item| {
        var count: usize = 1;
        for (item.shape) |dim| count *= @intCast(dim);
        const values = try a.alloc(f32, count);
        defer a.free(values);
        for (values, 0..) |*value, index| value.* = if (std.mem.eql(u8, item.name, "model.norm.weight")) @as(f32, @floatFromInt(index + 1)) / 2 else if (item.shape.len == 1) 1 else @as(f32, @floatFromInt(@as(i32, @intCast(index % 7)) - 3)) / 32;
        const name = try a.dupe(u8, item.name);
        var tensor = try Tensor.initFloat32(a, name, item.shape, values);
        errdefer tensor.deinit();
        try weights.resident_weights.put(a, name, .{ .tensor = tensor });
    }
    var compute = native.NativeCompute.init(a, &weights, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const config = Config{ .family = .llama, .hidden_size = 4, .num_attention_heads = 1, .num_key_value_heads = 1, .attention_head_dim = 4, .num_hidden_layers = 1, .intermediate_size = 8, .vocab_size = 8, .max_position_embeddings = 16, .norm_type = .rms_norm, .activation = .silu, .position_encoding = .rope };
    var input = try Tensor.initFloat32(a, "hidden", &.{ 2, 4 }, &.{ 0.2, 0.5, -0.3, 0.7, -0.1, 0.9, 0.3, -0.7 });
    defer input.deinit();
    const declared = [_]capture.Input{.{ .tensor = input }};
    var spec = Spec{ .config = config, .rows = 2 };
    var last = spec;
    last.output.last_row = true;
    try std.testing.expect(spec.resolve().fingerprint != last.resolve().fingerprint);
    var raw_spec = spec;
    raw_spec.output.final_norm = false;
    try std.testing.expect(spec.resolve().fingerprint != raw_spec.resolve().fingerprint);
    const Resolve = struct {
        fn shape(raw: ?*anyopaque, allocator: std.mem.Allocator, name: []const u8) !?@import("ml").graph.Shape {
            const backend: *const ops.ComputeBackend = @ptrCast(@alignCast(raw.?));
            const weight = try backend.getWeight(name);
            defer backend.free(weight);
            const dims = try backend.tensorShape(weight, allocator);
            defer allocator.free(dims);
            return @import("ml").graph.Shape.init(.f32, dims);
        }
    };
    var traced = try capture.trace(a, spec.resolve(), &declared, .{ .context = @constCast(&cb), .resolve = Resolve.shape });
    defer traced.deinit();
    // A functional prefix must not form a host fallback island between native
    // CUDA operations. This validates the actual ordinary model trace too.
    for (traced.graph.nodes.items, 0..) |node, index| {
        const decision = @import("../graph/cuda_capabilities.zig").decide(.{ .graph = &traced.graph, .node_id = @intCast(index), .op = node.op });
        try std.testing.expect(decision.can_execute);
    }
    const outputs = try run(&spec, &cb, a, &declared);
    defer a.free(outputs);
    defer for (outputs) |output| cb.free(output.value);
    try std.testing.expectEqual(@as(usize, 3), outputs.len);
    const hidden = try cb.fromFloat32Shape(input.asFloat32(), &.{ 2, 4 });
    const context = gpt.DecodeContext{ .total_sequence_len = 2, .query_sequence_len = 2, .kv_sequence_len = 2 };
    const ordinary = try gpt.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(&cb, a, config, hidden, .{}, 1, 2, &context, null);
    defer cb.free(ordinary.hidden);
    const expected = try cb.toFloat32(ordinary.hidden, a);
    defer a.free(expected);
    const actual = try cb.toFloat32(outputs[0].value, a);
    defer a.free(actual);
    for (expected, actual) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00001);
    const raw_outputs = try run(&raw_spec, &cb, a, &declared);
    defer a.free(raw_outputs);
    defer for (raw_outputs) |output| cb.free(output.value);
    const independent_input = try cb.fromFloat32Shape(input.asFloat32(), &.{ 2, 4 });
    const independent = try gpt.forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(&cb, a, config, independent_input, .{}, 1, 2, &context, null);
    defer cb.free(independent.final_hidden);
    defer cb.free(independent.pre_norm_hidden);
    const pre_norm = try cb.toFloat32(independent.pre_norm_hidden, a);
    defer a.free(pre_norm);
    const raw_values = try cb.toFloat32(raw_outputs[0].value, a);
    defer a.free(raw_values);
    for (pre_norm, raw_values) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00001);
    // With no prepared norm slot, the tail must complete from its current raw
    // hidden value rather than requesting another prefix execution.
    const raw_last = try cb.sliceRows2D(a, raw_outputs[0].value, 1, 1, config.hidden_size);
    defer cb.free(raw_last);
    const token = (try @import("../backends/decoder_tail_runtime.zig").forwardGreedyTokenTensorFromFinalHidden(&cb, a, config, raw_last, .rms, 999)) orelse return error.UnexpectedTailDecline;
    defer if (token.token_tensor) |value| cb.free(value);
    const lm_head = try cb.getWeight("lm_head.weight");
    defer cb.free(lm_head);
    const all_logits = try cb.linearNoBias(outputs[0].value, lm_head, 2, config.hidden_size, config.vocab_size);
    defer cb.free(all_logits);
    const full = try cb.toFloat32(all_logits, a);
    defer a.free(full);
    var best: usize = 0;
    for (full[config.vocab_size..], 0..) |value, index| if (value > full[config.vocab_size + best]) {
        best = index;
    };
    try std.testing.expectEqual(best, token.token_id);
    var suppressed_config = config;
    suppressed_config.suppress_token_ids[0] = @intCast(best);
    suppressed_config.suppress_token_ids_len = 1;
    var alternate: ?usize = null;
    for (full[config.vocab_size..], 0..) |value, index| {
        if (index == best) continue;
        if (alternate == null or value > full[config.vocab_size + alternate.?]) alternate = index;
    }
    const suppressed = (try @import("../backends/decoder_tail_runtime.zig").forwardGreedyTokenTensorFromFinalHidden(&cb, a, suppressed_config, raw_last, .rms, 999)) orelse return error.UnexpectedTailDecline;
    defer if (suppressed.token_tensor) |value| cb.free(value);
    try std.testing.expectEqual(alternate.?, suppressed.token_id);
    const normalized = try gpt.greedyLastTokenTensorFromFinalHiddenRetain(&cb, a, suppressed_config, .{ .hidden = outputs[0].value, .total_rows = 2 });
    defer if (normalized.token_tensor) |value| cb.free(value);
    try std.testing.expectEqual(alternate.?, normalized.token_id);
    const generic = (try @import("../backends/decoder_tail_runtime.zig").forwardGreedyFromFinalHidden(&cb, a, suppressed_config, raw_last, .rms, 999)) orelse return error.UnexpectedTailDecline;
    try std.testing.expectEqual(@as(i64, @intCast(alternate.?)), generic);
    inline for (.{ false, true }) |positioned| {
        const current = try cb.fromFloat32Shape(input.asFloat32(), &.{ 2, 4 });
        const last_logits = if (positioned)
            try gpt.forwardLastLogitsFromPositionedEmbeddingsWithLayer0Overrides(&cb, a, config, current, .{}, 1, 2, &context, null)
        else
            try gpt.forwardLastLogitsFromEmbeddingsWithLayer0Overrides(&cb, a, config, current, .{}, 1, 2, &context, null);
        defer a.free(last_logits);
        try std.testing.expectEqual(@as(usize, config.vocab_size), last_logits.len);
        for (full[config.vocab_size..], last_logits) |want, got| try std.testing.expectApproxEqAbs(want, got, 0.00001);
    }
    for (outputs[1..]) |output| {
        const values = try cb.toFloat32(output.value, a);
        defer a.free(values);
        try std.testing.expectEqual(@as(usize, 8), values.len);
    }
}
