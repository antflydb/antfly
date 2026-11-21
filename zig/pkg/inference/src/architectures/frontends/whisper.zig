// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Configuration and typed stage adapters; all arithmetic stays in the ordinary
//! architecture forward and the shared semantic graph/backend implementations.
const std = @import("std");
const capture = @import("../../execution/capture.zig");
const frontend = @import("../../execution/model_frontend.zig");
const whisper = @import("../whisper.zig");
const ops = @import("../../ops/ops.zig");
const Tensor = @import("../../backends/tensor.zig").Tensor;

pub fn resolveCached(config: *whisper.Config) capture.Forward {
    var forward = resolve(config, .decoder).?;
    forward.fingerprint = 4;
    forward.run = cachedDecoder;
    return forward;
}
fn cachedDecoder(ptr: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, inputs: []const capture.Input) ![]capture.Output {
    const cfg: *whisper.Config = @ptrCast(@alignCast(ptr));
    const values = try decoder(ptr, cb, a, inputs);
    errdefer {
        cb.free(values[0].value);
        a.free(values);
    }
    const ids = (try get(inputs, "input_ids")).?;
    const rows: usize = @intCast(ids.shape[1]);
    const flat = try cb.primReshape(values[0].value, &.{ @intCast(rows), cfg.vocab_size });
    defer cb.free(flat);
    const last = try cb.sliceRows2D(a, flat, rows - 1, 1, cfg.vocab_size);
    defer cb.free(last);
    const result = try cb.primReshape(last, &.{ 1, 1, cfg.vocab_size });
    cb.free(values[0].value);
    values[0].value = result;
    return values;
}

pub fn resolve(config: *whisper.Config, stage: frontend.Stage) ?capture.Forward {
    return switch (stage) {
        .encoder => .{ .context = config, .fingerprint = 2, .output_names = &.{"last_hidden_state"}, .input_info = &.{.{ .name = "input_features", .dtype = .f32, .shape = &.{ -1, -1, -1 } }}, .validate = validateEncoder, .run = encoder },
        .decoder => .{ .context = config, .fingerprint = 3, .output_names = &.{"logits"}, .input_info = &.{ .{ .name = "input_ids", .dtype = .i64, .shape = &.{ -1, -1 } }, .{ .name = "encoder_hidden_states", .dtype = .f32, .shape = &.{ -1, -1, -1 } }, .{ .name = "encoder_attention_mask", .dtype = .i64, .shape = &.{ -1, -1 } } }, .validate = validateDecoder, .run = decoder },
        else => null,
    };
}

const get = capture.findUniqueTensor;
fn check(config: whisper.Config) !void {
    if (config.d_model == 0 or config.num_mel_bins == 0 or config.vocab_size == 0 or config.encoder_attention_heads == 0 or config.decoder_attention_heads == 0) return error.InvalidModelConfig;
    if (config.d_model % config.encoder_attention_heads != 0 or config.d_model % config.decoder_attention_heads != 0) return error.InvalidModelConfig;
}
const typed = capture.requireHostTensor;
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
        if (!std.mem.eql(u8, input.tensor.name, "input_ids") and !std.mem.eql(u8, input.tensor.name, "decoder_input_ids") and !std.mem.eql(u8, input.tensor.name, "encoder_hidden_states") and !std.mem.eql(u8, input.tensor.name, "encoder_attention_mask") and !std.mem.eql(u8, input.tensor.name, "position_ids")) return error.UnexpectedModelProgramInput;
    }
    const plain_ids = try get(inputs, "input_ids");
    const named_ids = try get(inputs, "decoder_input_ids");
    if (plain_ids != null and named_ids != null) return error.DuplicateInputs;
    const ids = named_ids orelse plain_ids orelse return error.MissingInputs;
    const hidden = (try get(inputs, "encoder_hidden_states")) orelse return error.MissingInputs;
    try typed(ids, .i64, 2);
    try typed(hidden, .f32, 3);
    if (ids.shape[0] != hidden.shape[0] or hidden.shape[2] != cfg.d_model or ids.shape[1] > cfg.max_target_positions or hidden.shape[1] > cfg.max_source_positions) return error.InvalidInputShape;
    if (try get(inputs, "position_ids")) |positions| {
        try typed(positions, .i64, 2);
        if (!std.mem.eql(i64, positions.shape, ids.shape)) return error.InvalidInputShape;
        for (positions.asInt64()) |position| if (position < 0 or position >= cfg.max_target_positions) return error.InvalidInputShape;
    }
    for (ids.asInt64()) |id| if (id < 0 or id >= cfg.vocab_size) return error.InvalidTokenId;
    if (try get(inputs, "encoder_attention_mask")) |mask| {
        try typed(mask, .i64, 2);
        if (!std.mem.eql(i64, mask.shape, hidden.shape[0..2])) return error.InvalidInputShape;
        for (mask.asInt64()) |value| if (value != 0 and value != 1) return error.InvalidAttentionMask;
    }
}
const output = capture.publishReshapedOutput;
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
    const logits = if (try get(inputs, "position_ids")) |positions| try whisper.decoderForwardTensorAtPositions(cb, allocator, cfg.*, ids.asInt64(), positions.asInt64(), hidden, mask, batch, seq, enc_seq) else try whisper.decoderForwardTensor(cb, allocator, cfg.*, ids.asInt64(), hidden, mask, batch, seq, enc_seq);
    return output(allocator, cb, logits, "logits", &.{ @intCast(batch), @intCast(seq), @intCast(cfg.vocab_size) });
}

test "model program Whisper encoder and decoder execute through PJRT across cache hits" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    const fixture = @import("../../execution/testing/incremental_encoder_decoder.zig");
    const Source = struct {
        weights: @import("../../ops/native_compute.zig").WeightStore,
        fn create(ptr: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return @import("../../backends/compute_factory.zig").createNative(allocator, &self.weights, null, null, .{});
        }
        fn import(_: *anyopaque, cb: *const ops.ComputeBackend, value: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
            const native: *@import("../../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
            return native.importHostTensor(value);
        }
    };
    var source = Source{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    try fixture.populate(&source.weights);
    var cfg = fixture.config;
    var reference = try Source.create(&source, a);
    defer reference.deinit();
    var cache = @import("../../execution/captured_forward.zig").Cache{};
    defer cache.deinit(a);
    const provider = @import("../../execution/captured_forward.zig").Provider{ .context = &source, .create = Source.create, .import = Source.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } };
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

test "model program Whisper resident self and immutable cross KV match full prefix" {
    if (comptime !@import("build_options").enable_pjrt) return error.SkipZigTest;
    const a = std.testing.allocator;
    var client = @import("pjrt").pjrt.Client.initFromEnv(a) catch |err| switch (err) {
        error.PjrtPluginNotFound => return error.SkipZigTest,
        else => return err,
    };
    defer client.deinit();
    const fixture = @import("../../execution/testing/incremental_encoder_decoder.zig");
    const Source = struct {
        weights: @import("../../ops/native_compute.zig").WeightStore,
        encoder_imports: usize = 0,
        fn create(raw: *anyopaque, allocator: std.mem.Allocator) anyerror!ops.ComputeBackend {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return @import("../../backends/compute_factory.zig").createNative(allocator, &self.weights, null, null, .{});
        }
        fn import(raw: *anyopaque, cb: *const ops.ComputeBackend, value: *const Tensor, _: std.mem.Allocator) anyerror!ops.CT {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (std.mem.eql(u8, value.name, "encoder_hidden_states")) self.encoder_imports += 1;
            const native: *@import("../../ops/native_compute.zig").NativeCompute = @ptrCast(@alignCast(cb.ptr));
            return native.importHostTensor(value);
        }
    };
    var source = Source{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer source.weights.deinitOwned();
    try fixture.populate(&source.weights);
    // Q/K/V must differ: identical fixture projections can hide a swapped
    // resident operand, while identity token IDs can hide position binding bugs.
    for ([_][]const u8{ "self_attn.k_proj", "self_attn.v_proj", "encoder_attn.k_proj", "encoder_attn.v_proj" }, 0..) |role, index| {
        var name: [128]u8 = undefined;
        const weight = source.weights.resident_weights.getPtr(try std.fmt.bufPrint(&name, "model.decoder.layers.0.{s}.weight", .{role})).?;
        for (@constCast(weight.tensor.asFloat32())) |*value| value.* *= @as(f32, @floatFromInt(index + 2)) / 3;
    }
    var cfg = fixture.config;
    var ordinary = try Source.create(&source, a);
    defer ordinary.deinit();
    var cache = @import("../../execution/captured_forward.zig").Cache{};
    defer cache.deinit(a);
    const provider = @import("../../execution/captured_forward.zig").Provider{ .context = &source, .create = Source.create, .import = Source.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } };
    var hidden = try Tensor.initFloat32(a, "encoder_hidden_states", &.{ 1, 2, 4 }, &.{ 0.2, 0.5, -0.1, 0.4, 0.3, -0.2, 0.7, 0.1 });
    defer hidden.deinit();
    const tokens = [_]i64{ 3, 1, 0, 2 };
    for ([_][2]i64{ .{ 1, 1 }, .{ 1, 0 }, .{ 0, 0 } }) |mask_values| {
        var mask = try Tensor.initInt64(a, "encoder_attention_mask", &.{ 1, 2 }, &mask_values);
        defer mask.deinit();
        var state = @import("../../graph/pjrt_decoder_state.zig").State{ .allocator = a, .capacity = cfg.max_target_positions };
        defer state.deinit();
        for ([_]usize{ 1, 3, 4 }) |length| {
            var forward = resolveCached(&cfg);
            forward.resident_capacity = state.capacity;
            forward.resident_initial = state.position == 0;
            const pending = tokens[state.position..length];
            var ids = try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(pending.len) }, pending);
            defer ids.deinit();
            var positions = [_]i64{ 0, 1, 2, 3 };
            var pos = try Tensor.initInt64(a, "position_ids", ids.shape, positions[state.position..length]);
            defer pos.deinit();
            const inputs = [_]capture.Input{ .{ .tensor = ids }, .{ .tensor = hidden }, .{ .tensor = mask }, .{ .tensor = pos } };
            const prior_imports = source.encoder_imports;
            const actual = try cache.runResident(a, provider, forward, &inputs, &state, a, null);
            try std.testing.expectEqual(prior_imports + @as(usize, if (length == 1) 1 else 0), source.encoder_imports);
            defer a.free(actual);
            defer for (actual) |*value| value.deinit();
            var full = try Tensor.initInt64(a, "input_ids", &.{ 1, @intCast(length) }, tokens[0..length]);
            defer full.deinit();
            const expected = try decoder(&cfg, &ordinary, a, &.{ .{ .tensor = full }, .{ .tensor = hidden }, .{ .tensor = mask } });
            defer a.free(expected);
            defer ordinary.free(expected[0].value);
            const values = try ordinary.toFloat32(expected[0].value, a);
            defer a.free(values);
            try std.testing.expectEqual(length, state.position);
            try std.testing.expectEqual(@as(usize, cfg.decoder_layers * 4), state.buffers.len);
            for (values[values.len - cfg.vocab_size ..], actual[0].asFloat32()) |want, got| try std.testing.expectApproxEqAbs(want, got, 1e-5);
        }
    }
    // A failed generation must preserve every prior buffer and the cursor.
    // Poison an unused token before immutable operand publication, then select
    // it only after a successful first generation.
    var poisoned = Source{ .weights = .{ .allocator = a, .resident_weights = .{}, .lazy_weights = .{} } };
    defer poisoned.weights.deinitOwned();
    try fixture.populate(&poisoned.weights);
    const table = poisoned.weights.resident_weights.getPtr("model.decoder.embed_tokens.weight").?;
    {
        // Keep the output head independent of the poisoned input embedding.
        const name = try a.dupe(u8, "proj_out.weight");
        errdefer a.free(name);
        var head = try Tensor.initFloat32(a, name, table.tensor.shape, table.tensor.asFloat32());
        errdefer head.deinit();
        try poisoned.weights.resident_weights.put(a, name, .{ .tensor = head });
    }
    const input_table = poisoned.weights.resident_weights.getPtr("model.decoder.embed_tokens.weight").?;
    @memset(@constCast(input_table.tensor.asFloat32())[3 * cfg.d_model .. 4 * cfg.d_model], std.math.nan(f32));
    var failed_cache = @import("../../execution/captured_forward.zig").Cache{};
    defer failed_cache.deinit(a);
    const failed_provider = @import("../../execution/captured_forward.zig").Provider{ .context = &poisoned, .create = Source.create, .import = Source.import, .compiled_context = .{ .cb = undefined, .requested_backend = .pjrt, .pjrt_client = @ptrCast(&client) } };
    var state = @import("../../graph/pjrt_decoder_state.zig").State{ .allocator = a, .capacity = cfg.max_target_positions };
    defer state.deinit();
    var mask = try Tensor.initInt64(a, "encoder_attention_mask", &.{ 1, 2 }, &.{ 1, 1 });
    defer mask.deinit();
    var first_ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 1 }, &.{0});
    defer first_ids.deinit();
    var first_pos = try Tensor.initInt64(a, "position_ids", first_ids.shape, &.{0});
    defer first_pos.deinit();
    var forward = resolveCached(&cfg);
    forward.resident_capacity = state.capacity;
    forward.resident_initial = true;
    const initial = try failed_cache.runResident(a, failed_provider, forward, &.{ .{ .tensor = first_ids }, .{ .tensor = hidden }, .{ .tensor = mask }, .{ .tensor = first_pos } }, &state, a, null);
    defer a.free(initial);
    defer for (initial) |*value| value.deinit();
    var handles: [fixture.config.decoder_layers * 4]usize = undefined;
    for (state.buffers, &handles) |buffer, *handle| handle.* = @intFromPtr(buffer.handle);
    var next_ids = try Tensor.initInt64(a, "input_ids", &.{ 1, 1 }, &.{3});
    defer next_ids.deinit();
    var next_pos = try Tensor.initInt64(a, "position_ids", next_ids.shape, &.{1});
    defer next_pos.deinit();
    forward.resident_initial = false;
    try std.testing.expectError(error.InvalidResidentDecoderOutput, failed_cache.runResident(a, failed_provider, forward, &.{ .{ .tensor = next_ids }, .{ .tensor = hidden }, .{ .tensor = mask }, .{ .tensor = next_pos } }, &state, a, null));
    try std.testing.expectEqual(@as(usize, 1), state.position);
    try std.testing.expectEqual(handles.len, state.buffers.len);
    for (state.buffers, handles) |buffer, handle| try std.testing.expectEqual(handle, @intFromPtr(buffer.handle));
}
