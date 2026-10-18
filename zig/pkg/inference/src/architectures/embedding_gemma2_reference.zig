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

//! EmbeddingGemma 2 encoder contracts and scalar FP32 reference primitives.
//!
//! Production CUDA executes the same contract resident on device.  These
//! routines intentionally favor clarity and deterministic differential tests.

const std = @import("std");
const model = @import("../models/embedding_gemma2.zig");
const ops = @import("../ops/ops.zig");
const ComputeBackend = ops.ComputeBackend;

pub const Config = model.Config;
pub const LayerType = model.LayerType;

pub const Input = union(enum) {
    token_ids: []const i64,
    /// Row-major `[batch, sequence, config.hidden_size]`; used after ordered
    /// image/audio soft tokens have been spliced into text embeddings.
    input_embeddings: []const f32,
};

pub const Request = struct {
    input: Input,
    attention_mask: []const i64,
    batch: usize,
    sequence: usize,

    pub fn validate(self: Request, config: Config) !void {
        if (self.batch == 0 or self.sequence == 0 or self.sequence > config.maxInputTokens())
            return error.InvalidShape;
        const total = std.math.mul(usize, self.batch, self.sequence) catch return error.InvalidShape;
        if (self.attention_mask.len != total) return error.InvalidShape;
        for (self.attention_mask) |valid| if (valid != 0 and valid != 1) return error.InvalidAttentionMask;
        switch (self.input) {
            .token_ids => |ids| {
                if (ids.len != total) return error.InvalidShape;
                for (ids) |id| if (id < 0 or id >= config.vocab_size) return error.InvalidTokenId;
            },
            .input_embeddings => |values| {
                const expected = std.math.mul(usize, total, config.hidden_size) catch return error.InvalidShape;
                if (values.len != expected) return error.InvalidShape;
                for (values) |value| if (!std.math.isFinite(value)) return error.InvalidInputEmbedding;
            },
        }
        try self.validateMask(config);
    }
    /// Shared layout contract for host inputs and trusted resident activations.
    pub fn validateMask(self: Request, config: Config) !void {
        if (self.batch == 0 or self.sequence == 0 or self.sequence > config.maxInputTokens()) return error.InvalidShape;
        const total = std.math.mul(usize, self.batch, self.sequence) catch return error.InvalidShape;
        if (self.attention_mask.len != total) return error.InvalidShape;
        for (self.attention_mask) |valid| if (valid != 0 and valid != 1) return error.InvalidAttentionMask;
        for (0..self.batch) |batch_index| {
            const mask = self.attention_mask[batch_index * self.sequence ..][0..self.sequence];
            var seen_padding = false;
            var valid_count: usize = 0;
            for (mask) |valid| {
                if (valid == 0) seen_padding = true else {
                    if (seen_padding) return error.NonContiguousAttentionMask;
                    valid_count += 1;
                }
            }
            if (valid_count == 0) return error.EmptyEmbeddingInput;
        }
    }
};

/// Canonical checkpoint names shared by the generic reference and resident
/// CUDA loader. The factory preserves the checkpoint's `language_model` prefix.
pub fn layerWeightName(buffer: []u8, layer: usize, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buffer, "language_model.layers.{d}.{s}", .{ layer, suffix });
}

pub const WeightNames = struct {
    pub const token_embedding = "language_model.embed_tokens.weight";
    pub const ple_projection = "language_model.ple.per_layer_model_projection.weight";
    pub const ple_norm = "language_model.ple.per_layer_projection_norm.weight";
    pub const final_norm = "language_model.norm.weight";
    pub const output_projection = "language_model.embedding_projection.weight";
};

fn loadWeightF32(cb: *const ComputeBackend, allocator: std.mem.Allocator, name: []const u8) ![]f32 {
    const tensor = try cb.getWeight(name);
    defer cb.free(tensor);
    return cb.toFloat32(tensor, allocator);
}

fn linearReference(output: []f32, input: []const f32, weight: []const f32, rows: usize, in_dim: usize, out_dim: usize) !void {
    if (input.len != rows * in_dim or output.len != rows * out_dim or weight.len != out_dim * in_dim)
        return error.InvalidShape;
    for (0..rows) |row| for (0..out_dim) |column| {
        var sum: f32 = 0;
        for (0..in_dim) |inner| sum += input[row * in_dim + inner] * weight[column * in_dim + inner];
        output[row * out_dim + column] = sum;
    };
}

fn geluTanh(value: f32) f32 {
    const root_two_over_pi: f32 = 0.7978845608028654;
    return 0.5 * value * (1.0 + std.math.tanh(root_two_over_pi * (value + 0.044715 * value * value * value)));
}

/// Exact, deliberately scalar FP32 oracle over the weights installed in a
/// ComputeBackend. It is suitable for qualification and intermediate-tensor
/// debugging; production requests use the resident backend implementation.
pub fn forwardReference(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    request: Request,
    requested_dim: usize,
) ![]f32 {
    if (cb.execution_control) |control| try control.check();
    try config.validate();
    try request.validate(config);
    const rows = request.batch * request.sequence;
    const hidden_dim: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const layer_count: usize = config.num_hidden_layers;
    if (requested_dim == 0 or requested_dim > config.embedding_dim) return error.InvalidOutputDimension;

    var hidden = try allocator.alloc(f32, rows * hidden_dim);
    defer allocator.free(hidden);
    switch (request.input) {
        .input_embeddings => |values| @memcpy(hidden, values),
        .token_ids => |ids| {
            const embedding = try loadWeightF32(cb, allocator, WeightNames.token_embedding);
            defer allocator.free(embedding);
            if (embedding.len != @as(usize, config.vocab_size) * hidden_dim) return error.InvalidWeightShape;
            const embed_scale = @sqrt(@as(f32, @floatFromInt(hidden_dim)));
            for (ids, 0..) |id, row| {
                for (0..hidden_dim) |d| {
                    hidden[row * hidden_dim + d] = embedding[@as(usize, @intCast(id)) * hidden_dim + d] * embed_scale;
                }
            }
        },
    }

    // Projection-only PLE is derived once from the immutable input embedding.
    const ple_w = try loadWeightF32(cb, allocator, WeightNames.ple_projection);
    defer allocator.free(ple_w);
    const ple_all = try allocator.alloc(f32, rows * layer_count * hidden_dim);
    defer allocator.free(ple_all);
    try linearReference(ple_all, hidden, ple_w, rows, hidden_dim, layer_count * hidden_dim);
    const ple_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(hidden_dim)));
    for (ple_all) |*value| value.* *= ple_scale;
    const ple_norm_w = try loadWeightF32(cb, allocator, WeightNames.ple_norm);
    defer allocator.free(ple_norm_w);
    const ple_normed = try allocator.alloc(f32, ple_all.len);
    defer allocator.free(ple_normed);
    try rmsNormDirect(ple_normed, ple_all, ple_norm_w, rows * layer_count, hidden_dim, config.rms_norm_eps);

    var name_buffer: [256]u8 = undefined;
    for (0..layer_count) |layer_index| {
        if (cb.execution_control) |control| try control.check();
        const layer = try config.layer(layer_index);
        const q_width: usize = layer.queryWidth();
        const kv_width: usize = layer.keyValueWidth();
        const q_heads: usize = layer.num_attention_heads;
        const kv_heads: usize = layer.num_key_value_heads;
        const head_dim: usize = layer.head_dim;

        const input_norm_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "input_layernorm.weight"));
        defer allocator.free(input_norm_w);
        const normed = try allocator.alloc(f32, hidden.len);
        defer allocator.free(normed);
        try rmsNormDirect(normed, hidden, input_norm_w, rows, hidden_dim, config.rms_norm_eps);

        const q_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "self_attn.q_proj.weight"));
        defer allocator.free(q_w);
        const k_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "self_attn.k_proj.weight"));
        defer allocator.free(k_w);
        const v_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "self_attn.v_proj.weight"));
        defer allocator.free(v_w);
        const q = try allocator.alloc(f32, rows * q_width);
        defer allocator.free(q);
        const k = try allocator.alloc(f32, rows * kv_width);
        defer allocator.free(k);
        const v = try allocator.alloc(f32, rows * kv_width);
        defer allocator.free(v);
        try linearReference(q, normed, q_w, rows, hidden_dim, q_width);
        try linearReference(k, normed, k_w, rows, hidden_dim, kv_width);
        try linearReference(v, normed, v_w, rows, hidden_dim, kv_width);
        const q_norm_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "self_attn.q_norm.weight"));
        defer allocator.free(q_norm_w);
        const k_norm_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "self_attn.k_norm.weight"));
        defer allocator.free(k_norm_w);
        const qn = try allocator.alloc(f32, q.len);
        defer allocator.free(qn);
        const kn = try allocator.alloc(f32, k.len);
        defer allocator.free(kn);
        const vn = try allocator.alloc(f32, v.len);
        defer allocator.free(vn);
        try rmsNormDirect(qn, q, q_norm_w, rows * q_heads, head_dim, config.rms_norm_eps);
        try rmsNormDirect(kn, k, k_norm_w, rows * kv_heads, head_dim, config.rms_norm_eps);
        try rmsNormDirect(vn, v, null, rows * kv_heads, head_dim, config.rms_norm_eps);
        try ropeHalfSplit(qn, rows, q_heads, head_dim, request.sequence, layer.rope_theta);
        try ropeHalfSplit(kn, rows, kv_heads, head_dim, request.sequence, layer.rope_theta);
        const attended = try allocator.alloc(f32, qn.len);
        defer allocator.free(attended);
        try attentionReference(allocator, attended, qn, kn, vn, request.attention_mask, request.batch, request.sequence, q_heads, kv_heads, head_dim, layer.kind, config.sliding_window);
        const o_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "self_attn.o_proj.weight"));
        defer allocator.free(o_w);
        var projected = try allocator.alloc(f32, hidden.len);
        defer allocator.free(projected);
        try linearReference(projected, attended, o_w, rows, q_width, hidden_dim);
        const post_attn_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "post_attention_layernorm.weight"));
        defer allocator.free(post_attn_w);
        try rmsNormDirect(normed, projected, post_attn_w, rows, hidden_dim, config.rms_norm_eps);
        for (hidden, normed) |*residual, addend| residual.* += addend;

        const pre_ffn_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "pre_feedforward_layernorm.weight"));
        defer allocator.free(pre_ffn_w);
        try rmsNormDirect(normed, hidden, pre_ffn_w, rows, hidden_dim, config.rms_norm_eps);
        const gate_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "mlp.gate_proj.weight"));
        defer allocator.free(gate_w);
        const up_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "mlp.up_proj.weight"));
        defer allocator.free(up_w);
        const gate = try allocator.alloc(f32, rows * intermediate);
        defer allocator.free(gate);
        const up = try allocator.alloc(f32, rows * intermediate);
        defer allocator.free(up);
        try linearReference(gate, normed, gate_w, rows, hidden_dim, intermediate);
        try linearReference(up, normed, up_w, rows, hidden_dim, intermediate);
        for (gate, up) |*g, u| g.* = geluTanh(g.*) * u;
        const down_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "mlp.down_proj.weight"));
        defer allocator.free(down_w);
        try linearReference(projected, gate, down_w, rows, intermediate, hidden_dim);
        const post_ffn_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "post_feedforward_layernorm.weight"));
        defer allocator.free(post_ffn_w);
        try rmsNormDirect(normed, projected, post_ffn_w, rows, hidden_dim, config.rms_norm_eps);
        for (hidden, normed) |*residual, addend| residual.* += addend;

        const ple_gate_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "ple_block.per_layer_input_gate.weight"));
        defer allocator.free(ple_gate_w);
        try linearReference(projected, hidden, ple_gate_w, rows, hidden_dim, hidden_dim);
        for (0..rows) |row| for (0..hidden_dim) |d| {
            const ple_index = (row * layer_count + layer_index) * hidden_dim + d;
            projected[row * hidden_dim + d] = geluTanh(projected[row * hidden_dim + d]) * ple_normed[ple_index];
        };
        const ple_out_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "ple_block.per_layer_projection.weight"));
        defer allocator.free(ple_out_w);
        try linearReference(normed, projected, ple_out_w, rows, hidden_dim, hidden_dim);
        const ple_post_w = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "ple_block.post_per_layer_input_norm.weight"));
        defer allocator.free(ple_post_w);
        try rmsNormDirect(projected, normed, ple_post_w, rows, hidden_dim, config.rms_norm_eps);
        for (hidden, projected) |*residual, addend| residual.* += addend;
        const layer_scalar = try loadWeightF32(cb, allocator, try layerWeightName(&name_buffer, layer_index, "layer_scalar"));
        defer allocator.free(layer_scalar);
        if (layer_scalar.len != 1) return error.InvalidWeightShape;
        for (hidden) |*value| value.* *= layer_scalar[0];
    }

    const final_norm_w = try loadWeightF32(cb, allocator, WeightNames.final_norm);
    defer allocator.free(final_norm_w);
    const final_hidden = try allocator.alloc(f32, hidden.len);
    defer allocator.free(final_hidden);
    try rmsNormDirect(final_hidden, hidden, final_norm_w, rows, hidden_dim, config.rms_norm_eps);
    const output_w = try loadWeightF32(cb, allocator, WeightNames.output_projection);
    defer allocator.free(output_w);
    const full_dim: usize = config.embedding_dim;
    const token_outputs = try allocator.alloc(f32, rows * full_dim);
    defer allocator.free(token_outputs);
    try linearReference(token_outputs, final_hidden, output_w, rows, hidden_dim, full_dim);
    const result = try allocator.alloc(f32, request.batch * requested_dim);
    errdefer allocator.free(result);
    try maskedMeanL2(result, token_outputs, request.attention_mask, request.batch, request.sequence, full_dim);
    if (cb.execution_control) |control| try control.check();
    return result;
}

/// EmbeddingGemma 2 local attention uses an inclusive radius: both endpoints
/// at `query +/- 512` are visible. A radius of zero denotes a global layer.
pub fn attentionVisible(kind: LayerType, radius: usize, query: usize, key: usize) bool {
    if (kind == .full_attention) return true;
    const distance = if (query >= key) query - key else key - query;
    return distance <= radius;
}

/// Direct RMSNorm: upstream multiplies by the stored weight itself, without
/// the Gemma decoder's historical `(1 + weight)` adjustment.
pub fn rmsNormDirect(output: []f32, input: []const f32, weight: ?[]const f32, rows: usize, dim: usize, eps: f32) !void {
    if (output.len != input.len or input.len != rows * dim) return error.InvalidShape;
    if (weight) |scale| if (scale.len != dim) return error.InvalidShape;
    for (0..rows) |row| {
        const base = row * dim;
        var squares: f32 = 0;
        for (input[base..][0..dim]) |value| squares += value * value;
        const inverse = 1.0 / @sqrt(squares / @as(f32, @floatFromInt(dim)) + eps);
        for (0..dim) |column| {
            const scale = if (weight) |values| values[column] else 1.0;
            output[base + column] = input[base + column] * inverse * scale;
        }
    }
}

/// Half-split RoPE used by upstream `rotate_half`, applied over the complete
/// head dimension. Computation is FP32 even when production activations are
/// BF16.
pub fn ropeHalfSplit(values: []f32, rows: usize, heads: usize, head_dim: usize, sequence: usize, theta: f32) !void {
    if (head_dim == 0 or head_dim % 2 != 0 or sequence == 0 or
        values.len != rows * heads * head_dim or rows % sequence != 0)
        return error.InvalidShape;
    const half = head_dim / 2;
    for (0..rows) |row| {
        const position: f32 = @floatFromInt(row % sequence);
        for (0..heads) |head| {
            const base = (row * heads + head) * head_dim;
            for (0..half) |i| {
                const exponent = @as(f32, @floatFromInt(2 * i)) / @as(f32, @floatFromInt(head_dim));
                const angle = position / std.math.pow(f32, theta, exponent);
                const cosine = @cos(angle);
                const sine = @sin(angle);
                const lhs = values[base + i];
                const rhs = values[base + half + i];
                values[base + i] = lhs * cosine - rhs * sine;
                values[base + half + i] = rhs * cosine + lhs * sine;
            }
        }
    }
}

/// Deterministic FP32 bidirectional GQA reference. Q/K/V are row-major
/// `[batch, sequence, heads, head_dim]`; output has Q's shape. Softmax and
/// accumulation remain FP32 and the score scale is exactly 1.0.
pub fn attentionReference(
    allocator: std.mem.Allocator,
    output: []f32,
    q: []const f32,
    k: []const f32,
    v: []const f32,
    mask: []const i64,
    batch: usize,
    sequence: usize,
    q_heads: usize,
    kv_heads: usize,
    head_dim: usize,
    kind: LayerType,
    local_radius: usize,
) !void {
    const rows = std.math.mul(usize, batch, sequence) catch return error.InvalidShape;
    if (q_heads == 0 or kv_heads == 0 or q_heads % kv_heads != 0 or head_dim == 0 or
        q.len != rows * q_heads * head_dim or output.len != q.len or
        k.len != rows * kv_heads * head_dim or v.len != k.len or mask.len != rows)
        return error.InvalidShape;
    var scores = try allocator.alloc(f32, sequence);
    defer allocator.free(scores);
    @memset(output, 0);
    const heads_per_kv = q_heads / kv_heads;
    for (0..batch) |b| for (0..sequence) |query| for (0..q_heads) |qh| {
        if (mask[b * sequence + query] == 0) continue;
        const kvh = qh / heads_per_kv;
        var maximum = -std.math.inf(f32);
        for (0..sequence) |key| {
            if (mask[b * sequence + key] == 0 or !attentionVisible(kind, local_radius, query, key)) {
                scores[key] = -std.math.inf(f32);
                continue;
            }
            const q_base = ((b * sequence + query) * q_heads + qh) * head_dim;
            const k_base = ((b * sequence + key) * kv_heads + kvh) * head_dim;
            var score: f32 = 0;
            for (0..head_dim) |d| score += q[q_base + d] * k[k_base + d];
            scores[key] = score;
            maximum = @max(maximum, score);
        }
        var denominator: f32 = 0;
        for (scores) |*score| if (std.math.isFinite(score.*)) {
            score.* = @exp(score.* - maximum);
            denominator += score.*;
        };
        if (denominator == 0) continue;
        const out_base = ((b * sequence + query) * q_heads + qh) * head_dim;
        for (0..sequence) |key| {
            if (!std.math.isFinite(scores[key])) continue;
            const probability = scores[key] / denominator;
            const v_base = ((b * sequence + key) * kv_heads + kvh) * head_dim;
            for (0..head_dim) |d| output[out_base + d] += probability * v[v_base + d];
        }
    };
}

/// SentenceTransformers-compatible masked mean followed by truncation and L2
/// normalization. Projection happens before this function.
pub fn maskedMeanL2(
    output: []f32,
    projected_tokens: []const f32,
    mask: []const i64,
    batch: usize,
    sequence: usize,
    full_dim: usize,
) !void {
    const requested_dim = output.len / batch;
    if (batch == 0 or requested_dim == 0 or requested_dim > full_dim or output.len != batch * requested_dim or
        projected_tokens.len != batch * sequence * full_dim or mask.len != batch * sequence)
        return error.InvalidShape;
    for (0..batch) |b| {
        const out = output[b * requested_dim ..][0..requested_dim];
        @memset(out, 0);
        var count: usize = 0;
        for (0..sequence) |token| if (mask[b * sequence + token] != 0) {
            count += 1;
            const source = projected_tokens[(b * sequence + token) * full_dim ..][0..requested_dim];
            for (out, source) |*value, addend| value.* += addend;
        };
        if (count == 0) return error.EmptyEmbeddingInput;
        const inverse_count = 1.0 / @as(f32, @floatFromInt(count));
        var squared_norm: f32 = 0;
        for (out) |*value| {
            value.* *= inverse_count;
            squared_norm += value.* * value.*;
        }
        if (squared_norm == 0 or !std.math.isFinite(squared_norm)) return error.InvalidEmbeddingOutput;
        const inverse_norm = 1.0 / @sqrt(squared_norm);
        for (out) |*value| value.* *= inverse_norm;
    }
}

test "local attention radius is inclusive and symmetric" {
    try std.testing.expect(attentionVisible(.sliding_attention, 512, 7, 519));
    try std.testing.expect(attentionVisible(.sliding_attention, 512, 519, 7));
    try std.testing.expect(!attentionVisible(.sliding_attention, 512, 7, 520));
    try std.testing.expect(attentionVisible(.full_attention, 0, 0, 10000));
}

test "FP32 GQA reference is bidirectional and honors padding" {
    const q = [_]f32{ 1, 1 };
    const k = [_]f32{ 0, 2 };
    const v = [_]f32{ 3, 9 };
    var output: [2]f32 = undefined;
    try attentionReference(std.testing.allocator, &output, &q, &k, &v, &.{ 1, 1 }, 1, 2, 1, 1, 1, .full_attention, 0);
    // Query 0 sees the future key, proving the reference is not causal.
    try std.testing.expect(output[0] > 4.0);
    try attentionReference(std.testing.allocator, &output, &q, &k, &v, &.{ 1, 0 }, 1, 2, 1, 1, 1, .full_attention, 0);
    try std.testing.expectApproxEqAbs(@as(f32, 3), output[0], 1e-6);
    try std.testing.expectEqual(@as(f32, 0), output[1]);
}

test "mean pooling truncates before normalization" {
    // Mean is [2, 2, 100], requested dimension two => normalized [sqrt(.5), sqrt(.5)].
    var output: [2]f32 = undefined;
    try maskedMeanL2(&output, &.{ 1, 1, 100, 3, 3, 100 }, &.{ 1, 1 }, 1, 2, 3);
    try std.testing.expectApproxEqAbs(@as(f32, 0.70710677), output[0], 1e-6);
    try std.testing.expectApproxEqAbs(output[0], output[1], 1e-6);
}

test "request accepts soft tokens and rejects holes in padding" {
    var config: Config = undefined;
    config.vocab_size = 8;
    config.hidden_size = 2;
    config.max_position_embeddings = 8192;
    var embeddings: [6]f32 = @splat(0);
    try (Request{ .input = .{ .input_embeddings = &embeddings }, .attention_mask = &.{ 1, 1, 0 }, .batch = 1, .sequence = 3 }).validate(config);
    try std.testing.expectError(error.NonContiguousAttentionMask, (Request{ .input = .{ .input_embeddings = &embeddings }, .attention_mask = &.{ 1, 0, 1 }, .batch = 1, .sequence = 3 }).validate(config));
    embeddings[3] = std.math.nan(f32);
    try std.testing.expectError(error.InvalidInputEmbedding, (Request{ .input = .{ .input_embeddings = &embeddings }, .attention_mask = &.{ 1, 1, 0 }, .batch = 1, .sequence = 3 }).validate(config));
}
