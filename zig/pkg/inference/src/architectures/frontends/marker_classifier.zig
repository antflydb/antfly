// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Contextual marker classification: encoder semantics and the published MLP
//! are reused by eager and compiled execution, with typed runtime row indices.
const std = @import("std");
const capture = @import("../../execution/capture.zig");
const ops = @import("../../ops/ops.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;
const Encoder = @import("../../models/gliner_encoder.zig").Config;
const Parsed = struct { ids: Tensor, mask: Tensor, positions: Tensor, valid: Tensor };

pub fn validate(config: Encoder, inputs: []const capture.Input) !Parsed {
    const geometry = config.geometry();
    if (geometry.hidden_size == 0 or geometry.num_attention_heads == 0 or geometry.hidden_size % geometry.num_attention_heads != 0) return error.InvalidModelConfig;
    var slots: [4]?Tensor = @splat(null);
    for (inputs) |input| {
        const index: usize = if (std.mem.eql(u8, input.tensor.name, "input_ids")) 0 else if (std.mem.eql(u8, input.tensor.name, "attention_mask")) 1 else if (std.mem.eql(u8, input.tensor.name, "decision_marker_positions")) 2 else if (std.mem.eql(u8, input.tensor.name, "decision_marker_mask")) 3 else return error.UnexpectedModelProgramInput;
        if (slots[index] != null) return error.DuplicateInputs;
        try capture.requireHostTensor(input.tensor, .i64, 2);
        slots[index] = input.tensor;
    }
    const parsed = Parsed{ .ids = slots[0] orelse return error.MissingInputs, .mask = slots[1] orelse return error.MissingInputs, .positions = slots[2] orelse return error.MissingInputs, .valid = slots[3] orelse return error.MissingInputs };
    if (!std.mem.eql(i64, parsed.ids.shape, parsed.mask.shape) or !std.mem.eql(i64, parsed.positions.shape, parsed.valid.shape) or parsed.ids.shape[0] != parsed.positions.shape[0] or parsed.ids.shape[1] > config.geometry().max_position_embeddings) return error.InvalidInputShape;
    for (parsed.ids.asInt64()) |id| if (id < 0 or id >= config.geometry().vocab_size) return error.InvalidTokenId;
    const seq: usize = @intCast(parsed.ids.shape[1]);
    for (0..@intCast(parsed.ids.shape[0])) |row| {
        var padding = false;
        var live: usize = 0;
        for (parsed.mask.asInt64()[row * seq ..][0..seq]) |bit| {
            if (bit != 0 and bit != 1) return error.InvalidAttentionMask;
            if (config == .modern_bert and bit == 1 and padding) return error.InvalidGlinerAttentionMask;
            padding = padding or bit == 0;
            live += @intFromBool(bit != 0);
        }
        if (config == .modern_bert and live == 0) return error.InvalidGlinerAttentionMask;
    }
    for (parsed.positions.asInt64(), parsed.valid.asInt64()) |position, bit| {
        if (bit != 0 and bit != 1) return error.InvalidGlinerDecisionMarkerMask;
        if (bit == 1 and (position < 0 or position >= seq)) return error.InvalidGlinerDecisionMarkerPosition;
    }
    return parsed;
}

pub fn run(config: Encoder, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const parsed = try validate(config, inputs);
    const batch: usize = @intCast(parsed.ids.shape[0]);
    const seq: usize = @intCast(parsed.ids.shape[1]);
    const labels: usize = @intCast(parsed.positions.shape[1]);
    const hidden = switch (config) {
        .deberta => |cfg| try @import("../deberta.zig").forwardCt(cb, allocator, cfg, parsed.ids.asInt64(), parsed.mask.asInt64(), batch, seq, false),
        .modern_bert => |cfg| try @import("../modern_bert.zig").forwardCT(cb, allocator, cfg, parsed.ids.asInt64(), parsed.mask.asInt64(), batch, seq),
    };
    defer cb.free(hidden);
    const result = try @import("../gliner_decision_head.zig").forwardCt(cb, allocator, hidden, parsed.positions.asInt64(), parsed.valid.asInt64(), batch, seq, labels, config.geometry().hidden_size);
    defer cb.free(result.logits);
    const shape = [_]i64{ @intCast(batch), @intCast(labels) };
    const logits = try cb.primReshape(result.logits, &shape);
    defer cb.free(logits);
    const active = try @import("../../execution/indexed_rows.zig").active(cb, allocator, parsed.valid.asInt64(), batch, labels);
    defer cb.free(active);
    const masked_value = try cb.fromFloat32Shape(&.{-1.0e4}, &.{ 1, 1 });
    defer cb.free(masked_value);
    const masked = try cb.primBroadcastInDim(masked_value, &shape, &.{ 0, 1 }, &.{ 1, 1 });
    defer cb.free(masked);
    const output = try cb.primWhereSelect(active, logits, masked);
    return capture.publishOutput(allocator, cb, "logits", output);
}

test "model program marker classifier executes both encoder and readout with dynamic indices and masks" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    const Source = struct {
        weights: @import("../../ops/native_compute.zig").WeightStore,
        config: Encoder = .{ .deberta = .{ .hidden_size = 2, .num_attention_heads = 1, .num_hidden_layers = 0, .intermediate_size = 4, .vocab_size = 4, .max_position_embeddings = 4, .position_buckets = 2 } },
        fn create(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @import("../../backends/compute_factory.zig").createNative(allocator, &self.weights, null, null, .{});
        }
        fn import(_: *anyopaque, cb: *const ops.ComputeBackend, value: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
            const native: *@import("../../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
            return native.importHostTensor(value);
        }
        fn forward(ptr: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return run(self.config, cb, allocator, inputs);
        }
        fn put(self: *@This(), name: []const u8, shape: []const i64, values: []const f32) !void {
            const allocator = self.weights.allocator;
            const key = try allocator.dupe(u8, name);
            errdefer allocator.free(key);
            var tensor = try Tensor.initFloat32(allocator, key, shape, values);
            errdefer tensor.deinit();
            try self.weights.resident_weights.put(allocator, key, .{ .tensor = tensor });
        }
    };
    var source = Source{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    try source.put("embeddings.word_embeddings.weight", &.{ 4, 2 }, &.{ 0, 1, 2, 0, 1, 2, -1, 0 });
    try source.put("encoder.rel_embeddings.weight", &.{ 4, 2 }, &.{ 0, 1, 2, 0, 1, 2, -1, 0 });
    for ([_][]const u8{ "embeddings.LayerNorm.weight", "encoder.LayerNorm.weight" }) |name| try source.put(name, &.{2}, &.{ 1, 1 });
    for ([_][]const u8{ "embeddings.LayerNorm.bias", "encoder.LayerNorm.bias" }) |name| try source.put(name, &.{2}, &.{ 0, 0 });
    try source.put("classifier.0.weight", &.{ 4, 2 }, &.{ 1, 0, 0, 1, 1, 0, 0, 1 });
    try source.put("classifier.0.bias", &.{4}, &.{ 0, 0, 0, 0 });
    try source.put("classifier.2.weight", &.{ 1, 4 }, &.{ 1, 2, 1, 2 });
    try source.put("classifier.2.bias", &.{1}, &.{1});
    var ids = try Tensor.initInt64(a, "input_ids", &.{ 2, 3 }, &.{ 1, 2, 3, 2, 1, 0 });
    defer ids.deinit();
    var visibility = try Tensor.initInt64(a, "attention_mask", &.{ 2, 3 }, &.{ 1, 1, 1, 1, 1, 1 });
    defer visibility.deinit();
    var positions = try Tensor.initInt64(a, "decision_marker_positions", &.{ 2, 2 }, &.{ 2, 0, 1, 0 });
    defer positions.deinit();
    var valid = try Tensor.initInt64(a, "decision_marker_mask", &.{ 2, 2 }, &.{ 1, 0, 1, 1 });
    defer valid.deinit();
    var reference = try Source.create(&source, a);
    defer reference.deinit();
    var cache = @import("../../execution/captured_forward.zig").Cache{};
    defer cache.deinit(a);
    for (0..2) |iteration| {
        if (iteration == 1) {
            @as([*]i64, @ptrCast(@alignCast(positions.data.ptr)))[0] = 1;
            @as([*]i64, @ptrCast(@alignCast(valid.data.ptr)))[1] = 1;
        }
        const inputs = [_]capture.Input{ .{ .tensor = ids }, .{ .tensor = visibility, .specialize = true }, .{ .tensor = positions }, .{ .tensor = valid } };
        const expected = try run(source.config, &reference, a, &inputs);
        defer a.free(expected);
        defer for (expected) |value| reference.free(value.value);
        const want = try reference.toFloat32(expected[0].value, a);
        defer a.free(want);
        const actual = try cache.run(a, .{ .context = &source, .create = Source.create, .import = Source.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } }, .{ .context = &source, .fingerprint = 0, .output_names = &.{"logits"}, .run = Source.forward }, &inputs, a, null);
        defer a.free(actual);
        defer for (actual) |*value| value.deinit();
        try std.testing.expectEqualSlices(i64, &.{ 2, 2 }, actual[0].shape);
        for (want, actual[0].asFloat32()) |left, right| try std.testing.expectApproxEqAbs(left, right, 1e-5);
        if (iteration == 0) try std.testing.expectEqual(@as(f32, -1.0e4), actual[0].asFloat32()[1]);
    }
    try std.testing.expectEqual(@as(usize, 1), cache.entries.entries.items.len);
}
