// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Full-prefix decoder stage with an optional prepared media prefix. Only the
//! last hidden row reaches the vocabulary projection. Prefix values and text
//! IDs remain runtime operands; rotary layout is explicit control metadata.
const std = @import("std");
const capture = @import("capture.zig");
const ops = @import("../ops/ops.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const gpt = @import("../architectures/gpt.zig");
const Config = @import("../models/gpt.zig").Config;

pub fn resolve(config: *Config) capture.Forward {
    return .{ .context = config, .fingerprint = 5, .output_names = &.{"logits"}, .validate = validate, .run = run };
}

const Inputs = struct {
    ids: Tensor,
    ple_ids: ?Tensor = null,
    prefix: ?Tensor = null,
    positions: ?Tensor = null,
    visibility: ?Tensor = null,
    fn prefixRows(self: Inputs) usize {
        return if (self.prefix) |prefix| @intCast(prefix.shape[prefix.shape.len - 2]) else 0;
    }
};

fn parse(config: Config, inputs: []const capture.Input) !Inputs {
    var ids: ?Tensor = null;
    var ple: ?Tensor = null;
    var prefix: ?Tensor = null;
    var positions: ?Tensor = null;
    var visibility: ?Tensor = null;
    for (inputs) |input| {
        _ = try capture.inputShape(input.tensor);
        const slot = if (std.mem.eql(u8, input.tensor.name, "input_ids")) &ids else if (std.mem.eql(u8, input.tensor.name, "ple_input_ids")) &ple else if (std.mem.eql(u8, input.tensor.name, "input_embeddings")) &prefix else if (std.mem.eql(u8, input.tensor.name, "position_ids")) &positions else if (std.mem.eql(u8, input.tensor.name, "attention_or_mask")) &visibility else return error.UnexpectedModelProgramInput;
        if (slot.* != null) return error.DuplicateInputs;
        slot.* = input.tensor;
    }
    const text = ids orelse return error.MissingInputs;
    if (text.dtype != .i64 or text.shape.len != 2 or text.shape[0] != 1 or text.shape[1] <= 0 or text.shape[1] > config.max_position_embeddings or !text.isAlignedFor(i64)) return error.InvalidInputShape;
    for (text.asInt64()) |id| if (id < 0 or id >= config.vocab_size) return error.InvalidTokenId;
    if (ple) |value| {
        if (!config.hasPle() or value.dtype != .i64 or !std.mem.eql(i64, value.shape, text.shape) or !value.isAlignedFor(i64)) return error.InvalidInputShape;
        for (value.asInt64()) |id| if (id < 0 or id >= config.vocab_size) return error.InvalidTokenId;
    }
    if (prefix) |value| {
        if (value.dtype != .f32 or (value.shape.len != 2 and value.shape.len != 3) or !value.isAlignedFor(f32)) return error.InvalidInputShape;
        if (value.shape.len == 3 and value.shape[0] != 1) return error.InvalidInputShape;
        if (value.shape[value.shape.len - 1] != config.hidden_size or value.shape[value.shape.len - 2] <= 0 or value.shape[value.shape.len - 2] > text.shape[1]) return error.InvalidInputShape;
        if (config.hasPle() and ple == null) return error.MissingInputs;
        if (config.hasQwen35VisionArchitectureContract() and positions == null) return error.MissingMultimodalPositions;
    }
    if (positions) |value| {
        if (value.dtype != .i64 or !value.isAlignedFor(i64) or value.shape.len != 2 or value.shape[0] != 3 or value.shape[1] != text.shape[1]) return error.InvalidInputShape;
        for (value.asInt64()) |position| if (position < 0 or position > std.math.maxInt(u32)) return error.InvalidInputShape;
    }
    if (visibility) |value| {
        if (value.dtype != .u8 or value.shape.len != 2 or value.shape[0] != text.shape[1] or value.shape[1] != text.shape[1]) return error.InvalidAttentionMask;
        for (value.data) |bit| if (bit > 1) return error.InvalidAttentionMask;
    }
    return .{ .ids = text, .ple_ids = ple, .prefix = prefix, .positions = positions, .visibility = visibility };
}

fn validate(raw: *anyopaque, inputs: []const capture.Input) !void {
    const config: *Config = @ptrCast(@alignCast(raw));
    _ = try parse(config.*, inputs);
}

fn run(raw: *anyopaque, cb: *const ops.ComputeBackend, a: std.mem.Allocator, declared: []const capture.Input) ![]capture.Output {
    const config: *Config = @ptrCast(@alignCast(raw));
    const input = try parse(config.*, declared);
    const rows: usize = @intCast(input.ids.shape[1]);
    const width: usize = config.hidden_size;
    const weight = try gpt.getEmbeddingWeight(cb, config.*);
    defer cb.free(weight);
    const embedded = try gpt.lookupScaledTokenEmbeddings(cb, a, config.*, weight, input.ids.asInt64(), rows, width);
    var hidden = embedded;
    var hidden_owned = true;
    errdefer if (hidden_owned) cb.free(hidden);
    if (input.prefix) |prefix| {
        const prefix_rows = input.prefixRows();
        const imported = try cb.fromFloat32Shape(prefix.asFloat32(), &.{ @intCast(prefix_rows), @intCast(width) });
        if (prefix_rows == rows) {
            cb.free(hidden);
            hidden = imported;
        } else {
            defer cb.free(imported);
            const tail = try cb.sliceRows2D(a, embedded, prefix_rows, rows - prefix_rows, width);
            defer cb.free(tail);
            const combined = try cb.primConcatPrim(imported, tail, 0, &.{ @intCast(prefix_rows), @intCast(width) }, &.{ @intCast(rows - prefix_rows), @intCast(width) });
            cb.free(hidden);
            hidden = combined;
        }
    }
    const ple = try gpt.computePleVectors(cb, a, config.*, if (input.ple_ids) |ids| ids.asInt64() else input.ids.asInt64(), hidden, rows);
    defer if (ple) |value| cb.free(value);
    const positions: ?[]u32 = if (input.positions) |tensor| values: {
        const owned = try a.alloc(u32, tensor.asInt64().len);
        for (owned, tensor.asInt64()) |*value, position| value.* = @intCast(position);
        break :values owned;
    } else null;
    defer if (positions) |values| a.free(values);
    const context = gpt.DecodeContext{ .total_sequence_len = rows, .query_sequence_len = rows, .kv_sequence_len = rows, .input_ids = input.ids.asInt64(), .mrope_positions = positions, .attn_or_mask = if (input.visibility) |value| value.data else null, .qwen3vl_text_only = positions == null };
    hidden_owned = false; // the ordinary decoder consumes its input rows
    const logits = try gpt.forwardLastLogitsLastRowTensorFromEmbeddingsWithLayer0Overrides(cb, a, config.*, hidden, .{}, 1, rows, &context, ple);
    errdefer cb.free(logits);
    const transform = gpt.finalLogitTransform(config.*);
    const outputs = try a.alloc(capture.Output, 1);
    outputs[0] = .{ .name = "logits", .value = logits, .float32_transform = .{ .scale = transform.scale, .softcap = transform.softcap } };
    return outputs;
}
