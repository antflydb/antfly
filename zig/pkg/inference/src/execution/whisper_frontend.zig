// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Configuration and typed stage adapters; all arithmetic stays in the ordinary
//! architecture forward and the shared semantic graph/backend implementations.
const std = @import("std");
const capture = @import("capture.zig");
const frontend = @import("model_frontend.zig");
const whisper = @import("../architectures/whisper.zig");
const ops = @import("../ops/ops.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;

pub fn resolve(config: *whisper.Config, stage: frontend.Stage) ?capture.Forward {
    return switch (stage) {
        .encoder => .{ .context = config, .fingerprint = 2, .output_names = &.{"last_hidden_state"}, .input_info = &.{.{ .name = "input_features", .dtype = .f32, .shape = &.{ -1, -1, -1 } }}, .validate = validateEncoder, .run = encoder },
        .decoder => .{ .context = config, .fingerprint = 3, .output_names = &.{"logits"}, .input_info = &.{ .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } }, .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &.{ -1, -1, -1 } }, .{ .name = "encoder_attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } } }, .validate = validateDecoder, .run = decoder },
        else => null,
    };
}

fn get(inputs: []const capture.Input, name: []const u8) !?Tensor {
    var found: ?Tensor = null;
    for (inputs) |input| if (std.mem.eql(u8, name, input.tensor.name)) {
        if (found != null) return error.DuplicateInputs;
        found = input.tensor;
    };
    return found;
}
fn check(config: whisper.Config) !void {
    if (config.d_model == 0 or config.num_mel_bins == 0 or config.vocab_size == 0 or config.encoder_attention_heads == 0 or config.decoder_attention_heads == 0) return error.InvalidModelConfig;
    if (config.d_model % config.encoder_attention_heads != 0 or config.d_model % config.decoder_attention_heads != 0) return error.InvalidModelConfig;
}
fn typed(value: Tensor, dtype: @import("../backends/tensor.zig").DType, rank: usize) !void {
    _ = try capture.inputShape(value);
    if (value.dtype != dtype or value.shape.len != rank) return error.InvalidInputShape;
    for (value.shape) |dim| if (dim <= 0 or dim > std.math.maxInt(i32)) return error.InvalidInputShape;
    if (dtype == .i64 and !value.isAlignedFor(i64)) return error.InvalidInputShape;
    if (dtype == .f32 and !value.isAlignedFor(f32)) return error.InvalidInputShape;
}
fn validateEncoder(ptr: *anyopaque, inputs: []const capture.Input) anyerror!void {
    const cfg: *const whisper.Config = @ptrCast(@alignCast(ptr));
    try check(cfg.*);
    if (inputs.len != 1) return error.UnexpectedModelProgramInput;
    const mel = (try get(inputs, "input_features")) orelse return error.MissingInputs;
    try typed(mel, .f32, 3);
    if (mel.shape[1] != cfg.num_mel_bins or @divTrunc(mel.shape[2], 2) + @mod(mel.shape[2], 2) > cfg.max_source_positions) return error.InvalidInputShape;
}
fn validateDecoder(ptr: *anyopaque, inputs: []const capture.Input) anyerror!void {
    const cfg: *const whisper.Config = @ptrCast(@alignCast(ptr));
    try check(cfg.*);
    for (inputs) |input| {
        if (!std.mem.eql(u8, input.tensor.name, "input_ids") and !std.mem.eql(u8, input.tensor.name, "decoder_input_ids") and !std.mem.eql(u8, input.tensor.name, "encoder_hidden_states") and !std.mem.eql(u8, input.tensor.name, "encoder_attention_mask")) return error.UnexpectedModelProgramInput;
    }
    const plain_ids = try get(inputs, "input_ids");
    const named_ids = try get(inputs, "decoder_input_ids");
    if (plain_ids != null and named_ids != null) return error.DuplicateInputs;
    const ids = named_ids orelse plain_ids orelse return error.MissingInputs;
    const hidden = (try get(inputs, "encoder_hidden_states")) orelse return error.MissingInputs;
    try typed(ids, .i64, 2);
    try typed(hidden, .f32, 3);
    if (ids.shape[0] != hidden.shape[0] or hidden.shape[2] != cfg.d_model or ids.shape[1] > cfg.max_target_positions or hidden.shape[1] > cfg.max_source_positions or ids.asInt64().len > 2048) return error.InvalidInputShape;
    for (ids.asInt64()) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidTokenId;
    if (try get(inputs, "encoder_attention_mask")) |mask| {
        try typed(mask, .i64, 2);
        if (!std.mem.eql(i64, mask.shape, hidden.shape[0..2])) return error.InvalidInputShape;
        for (mask.asInt64()) |value| if (value != 0 and value != 1) return error.InvalidAttentionMask;
    }
}
fn output(allocator: std.mem.Allocator, cb: *const ops.ComputeBackend, value: ops.CT, name: []const u8, shape: []const i64) ![]capture.Output {
    defer cb.free(value);
    const shaped = try cb.primReshape(value, shape);
    errdefer cb.free(shaped);
    const outputs = try allocator.alloc(capture.Output, 1);
    outputs[0] = .{ .name = name, .value = shaped };
    return outputs;
}
fn encoder(ptr: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
    try validateEncoder(ptr, inputs);
    const cfg: *const whisper.Config = @ptrCast(@alignCast(ptr));
    const mel = (try get(inputs, "input_features")).?;
    const batch: usize = @intCast(mel.shape[0]);
    const time: usize = @intCast(mel.shape[2]);
    const features = try cb.fromFloat32Shape(mel.asFloat32(), &.{ @intCast(batch), @intCast(cfg.num_mel_bins), @intCast(time) });
    defer cb.free(features);
    return output(allocator, cb, try whisper.encoderForwardTensor(cb, allocator, cfg.*, features, batch, time), "last_hidden_state", &.{ @intCast(batch), @intCast(time / 2 + time % 2), @intCast(cfg.d_model) });
}
fn decoder(ptr: *anyopaque, cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, inputs: []const capture.Input) anyerror![]capture.Output {
    try validateDecoder(ptr, inputs);
    const cfg: *const whisper.Config = @ptrCast(@alignCast(ptr));
    const ids = (try get(inputs, "decoder_input_ids")) orelse (try get(inputs, "input_ids")).?;
    const enc = (try get(inputs, "encoder_hidden_states")).?;
    const batch: usize = @intCast(ids.shape[0]);
    const seq: usize = @intCast(ids.shape[1]);
    const enc_seq: usize = @intCast(enc.shape[1]);
    const hidden = try cb.fromFloat32Shape(enc.asFloat32(), &.{ @intCast(batch * enc_seq), @intCast(cfg.d_model) });
    defer cb.free(hidden);
    const mask_tensor = try get(inputs, "encoder_attention_mask");
    const default_mask = if (mask_tensor == null) try allocator.alloc(i64, batch * enc_seq) else null;
    defer if (default_mask) |mask| allocator.free(mask);
    if (default_mask) |mask| @memset(mask, 1);
    const mask = if (mask_tensor) |tensor| tensor.asInt64() else default_mask.?;
    return output(allocator, cb, try whisper.decoderForwardTensor(cb, allocator, cfg.*, ids.asInt64(), hidden, mask, batch, seq, enc_seq), "logits", &.{ @intCast(batch), @intCast(seq), @intCast(cfg.vocab_size) });
}

test "model program Whisper encoder and decoder execute through PJRT across cache hits" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    const fixture = @import("testing/incremental_encoder_decoder.zig");
    const Source = struct {
        weights: @import("../ops/native_compute.zig").WeightStore,
        fn create(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @import("../backends/compute_factory.zig").createNative(allocator, &self.weights, null, null, .{});
        }
        fn import(_: *anyopaque, cb: *const ops.ComputeBackend, value: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
            const native: *@import("../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
            return native.importHostTensor(value);
        }
    };
    var source = Source{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    try fixture.populate(&source.weights);
    var cfg = fixture.config;
    var reference = try Source.create(&source, a);
    defer reference.deinit();
    var cache = @import("captured_forward.zig").Cache{};
    defer cache.deinit(a);
    const provider = @import("captured_forward.zig").Provider{ .context = &source, .create = Source.create, .import = Source.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } };
    var mel = try Tensor.initFloat32(a, "input_features", &.{ 1, 2, 4 }, &.{ 0.2, 0.5, -0.1, 0.4, 0.3, -0.2, 0.7, 0.1 });
    defer mel.deinit();
    var ids = try Tensor.initInt64(a, "decoder_input_ids", &.{ 1, 2 }, &.{ 0, 1 });
    defer ids.deinit();
    var hidden = try Tensor.initFloat32(a, "encoder_hidden_states", &.{ 1, 2, 4 }, &.{ 0.2, 0.5, -0.1, 0.4, 0.3, -0.2, 0.7, 0.1 });
    defer hidden.deinit();
    var mask = try Tensor.initInt64(a, "encoder_attention_mask", &.{ 1, 2 }, &.{ 1, 0 });
    defer mask.deinit();
    for ([_]frontend.Stage{ .encoder, .decoder }) |stage| {
        const forward = resolve(&cfg, stage).?;
        for (0..2) |iteration| {
            // Same compiled ABI, different activation and token values.
            @as([*]f32, @ptrCast(@alignCast(mel.data.ptr)))[0] = @as(f32, @floatFromInt(iteration)) / 3;
            @as([*]i64, @ptrCast(@alignCast(ids.data.ptr)))[1] = @intCast(iteration + 1);
            const declared = if (stage == .encoder) &[_]capture.Input{.{ .tensor = mel }} else &[_]capture.Input{ .{ .tensor = ids }, .{ .tensor = hidden }, .{ .tensor = mask, .specialize = true } };
            const expected = try forward.run(forward.context, &reference, a, declared);
            defer a.free(expected);
            defer for (expected) |value| reference.free(value.value);
            const want = try reference.toFloat32(expected[0].value, a);
            defer a.free(want);
            const actual = try cache.run(a, provider, forward, declared, a, null);
            defer a.free(actual);
            defer for (actual) |*value| value.deinit();
            try std.testing.expectEqualStrings(expected[0].name, actual[0].name);
            try std.testing.expectEqual(want.len, actual[0].asFloat32().len);
            for (want, actual[0].asFloat32()) |left, right| try std.testing.expectApproxEqAbs(left, right, 1e-5);
        }
    }
    @as([*]i64, @ptrCast(@alignCast(ids.data.ptr)))[0] = cfg.vocab_size;
    try std.testing.expectError(error.InvalidTokenId, cache.run(a, provider, resolve(&cfg, .decoder).?, &.{ .{ .tensor = ids }, .{ .tensor = hidden }, .{ .tensor = mask, .specialize = true } }, a, null));
}
