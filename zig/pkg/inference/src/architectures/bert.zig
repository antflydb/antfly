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

// BERT encoder architecture using abstract ComputeBackend ops.
//
// Single implementation works with any ComputeBackend implementation.
// The compute backend handles all hardware-specific execution.

const std = @import("std");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const bert_config = @import("../models/bert.zig");
const native_compute_mod = @import("../ops/native_compute.zig");
const InferenceExecutionControl = @import("../execution_control.zig").InferenceExecutionControl;

pub const Config = bert_config.Config;

const BertLinearSlotKind = @import("bert_blocks.zig").Linear;

const BertLayerNormSlotKind = enum(usize) {
    attention_output,
    output,
};

const bert_linear_specs = @import("bert_blocks.zig").linear_specs;

const bert_layer_norm_specs = [_]struct {
    kind: BertLayerNormSlotKind,
    weight: []const u8,
    bias: []const u8,
}{
    .{ .kind = .attention_output, .weight = "attention.output.LayerNorm.weight", .bias = "attention.output.LayerNorm.bias" },
    .{ .kind = .output, .weight = "output.LayerNorm.weight", .bias = "output.LayerNorm.bias" },
};

fn metalEncoderFrameEnabled() bool {
    if (@import("builtin").target.cpu.arch.isWasm()) return false;
    return !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_ENCODER_FRAME");
}

fn bertLinearSlot(layer: usize, kind: BertLinearSlotKind) usize {
    return layer * bert_linear_specs.len + @backingInt(kind);
}

fn bertLayerNormSlot(layer: usize, kind: BertLayerNormSlotKind) usize {
    return layer * bert_layer_norm_specs.len + @backingInt(kind);
}

fn bertEmbeddingLayerNormSlot(layer_count: usize) usize {
    return layer_count * bert_layer_norm_specs.len;
}

fn isBgeM3DenseConfig(config: Config) bool {
    return config.model_type == .roberta and
        config.vocab_size == 250002 and
        config.hidden_size == 1024 and
        config.num_hidden_layers == 24 and
        config.num_attention_heads == 16 and
        config.intermediate_size == 4096 and
        config.max_position_embeddings == 8194 and
        config.type_vocab_size == 1 and
        config.hidden_act == .gelu_exact and
        config.layer_norm_eps == 1e-5 and
        config.pad_token_id == 1 and
        config.position_id_mode == .roberta_padding;
}

fn metalBertWeightMirrorMaxBytes(config: Config) usize {
    // Exact BGE-M3 F32 projections occupy 1,152 MiB. Its session admission
    // policy accounts for that model-scoped residency, while generic BERT
    // models retain the lower historical ceiling.
    const default_mb: usize = if (isBgeM3DenseConfig(config)) 1280 else 768;
    const mb = platform.env.getenvUsize("TERMITE_METAL_BERT_WEIGHT_MIRROR_MAX_MB") orelse default_mb;
    return std.math.mul(usize, mb, 1024 * 1024) catch std.math.maxInt(usize);
}

fn bertDenseMirrorBytes(config: Config, bytes_per_element: usize) ?usize {
    const layers: usize = config.num_hidden_layers;
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const hidden_squared = std.math.mul(usize, hidden, hidden) catch return null;
    const hidden_intermediate = std.math.mul(usize, hidden, intermediate) catch return null;
    const hidden_projections = std.math.mul(usize, hidden_squared, 4) catch return null;
    const ffn_projections = std.math.mul(usize, hidden_intermediate, 2) catch return null;
    const elements_per_layer = std.math.add(usize, hidden_projections, ffn_projections) catch return null;
    const total_elements = std.math.mul(usize, elements_per_layer, layers) catch return null;
    return std.math.mul(usize, total_elements, bytes_per_element) catch null;
}

fn metalEncoderSlotsPrepared(cb: *const ComputeBackend, config: Config) bool {
    const layer_count: usize = @intCast(config.num_hidden_layers);
    const hidden: usize = @intCast(config.hidden_size);
    const intermediate: usize = @intCast(config.intermediate_size);

    for (0..layer_count) |layer| {
        for (bert_linear_specs) |spec| {
            if (!cb.decoderRuntimeLinearSlotPrepared(
                bertLinearSlot(layer, spec.kind),
                if (spec.input_intermediate) intermediate else hidden,
                if (spec.output_intermediate) intermediate else hidden,
            )) return false;
        }
        for (bert_layer_norm_specs) |spec| {
            if (!cb.decoderRuntimeLayerNormSlotPrepared(
                bertLayerNormSlot(layer, spec.kind),
                hidden,
            )) return false;
        }
    }
    return cb.decoderRuntimeLayerNormSlotPrepared(
        bertEmbeddingLayerNormSlot(layer_count),
        hidden,
    );
}

fn preplanMetalEncoder(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config) !bool {
    if (cb.kind() != .metal or !metalEncoderFrameEnabled() or cb.decoderRuntimeHasActiveFrame()) return false;

    const layer_count: usize = @intCast(config.num_hidden_layers);
    const hidden: usize = @intCast(config.hidden_size);
    const intermediate: usize = @intCast(config.intermediate_size);
    const heads: usize = @intCast(config.num_attention_heads);
    if (layer_count == 0 or hidden == 0 or intermediate == 0 or heads == 0 or hidden % heads != 0) return false;
    // Query backend-owned slot metadata before touching the weight store.
    // The check is allocation-free and remains valid across explicit slot
    // invalidation because it verifies every slot and dimension required by
    // this encoder.
    if (metalEncoderSlotsPrepared(cb, config)) return true;

    const exact_bge_m3 = isBgeM3DenseConfig(config);
    const weight_mirrors_requested = !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_WEIGHT_MIRRORS") and
        !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_Q8_STAGING");
    const prefer_q8_mirrors = weight_mirrors_requested and !exact_bge_m3 and
        platform.env.getenvBool("TERMITE_METAL_BERT_USE_Q8_MIRRORS");
    const prefer_bf16_requested = weight_mirrors_requested and !prefer_q8_mirrors and
        !exact_bge_m3 and
        platform.env.getenvBool("TERMITE_METAL_BERT_USE_BF16_MIRRORS");
    const force_bge_m3_f32 = exact_bge_m3 and
        platform.env.getenvBool("TERMITE_METAL_BGE_M3_USE_F32_WEIGHTS");
    const prefer_f32_requested = weight_mirrors_requested and !prefer_q8_mirrors and !prefer_bf16_requested and
        ((exact_bge_m3 and force_bge_m3_f32) or platform.env.getenvBool("TERMITE_METAL_BERT_USE_F32_MIRRORS"));
    const mirror_bytes = bertDenseMirrorBytes(config, if (prefer_f32_requested) @sizeOf(f32) else @sizeOf(u16)) orelse
        std.math.maxInt(usize);
    // ponytail: keep one explicit aggregate ceiling until the runtime exposes
    // the Metal device's working-set budget to architecture preplanning.
    const weight_mirrors_enabled = weight_mirrors_requested and mirror_bytes <= metalBertWeightMirrorMaxBytes(config);
    const prefer_bf16_mirrors = weight_mirrors_enabled and !prefer_q8_mirrors and
        prefer_bf16_requested;
    const prefer_f32_mps_mirrors = weight_mirrors_enabled and !prefer_q8_mirrors and !prefer_bf16_mirrors and
        prefer_f32_requested;
    const prefer_f16_mps_mirrors = weight_mirrors_enabled and !prefer_q8_mirrors and !prefer_bf16_mirrors and
        !prefer_f32_mps_mirrors;
    const retain_dense_fallback = weight_mirrors_enabled and (!exact_bge_m3 or !force_bge_m3_f32);

    for (0..layer_count) |layer| {
        for (bert_linear_specs) |spec| {
            var weight_name: [256]u8 = undefined;
            var bias_name: [256]u8 = undefined;
            const weight = try getLayerWeight(cb, allocator, layer, spec.weight, &weight_name);
            defer cb.free(weight);
            const bias = try getLayerWeight(cb, allocator, layer, spec.bias, &bias_name);
            defer cb.free(bias);
            if (!(try cb.decoderRuntimePrepareLinear(&.{
                .slot = bertLinearSlot(layer, spec.kind),
                .weight = weight,
                .bias = bias,
                .in_dim = if (spec.input_intermediate) intermediate else hidden,
                .out_dim = if (spec.output_intermediate) intermediate else hidden,
                // BGE-M3 arrives as exact F32 and is loaded densely by its
                // session policy. Its default F16 MPS mirror is selected
                // explicitly here and never enters the generic Q8 branch.
                .retain_dense_fallback = retain_dense_fallback,
                // Prefill-shaped encoder GEMMs reuse an F16 mirror so Metal can
                // use its mixed F32/F16 matrix path, matching CUDA's hybrid-
                // residency strategy. The packed quantized weight remains the
                // source of truth; direct, Q8, BF16, and F32 mirrors stay
                // available for A/B runs.
                .dense_fallback_max_bytes = if (retain_dense_fallback) 32 * 1024 * 1024 else null,
                .allow_direct_quant_fallback = retain_dense_fallback,
                .prefer_bf16_fallback = prefer_bf16_mirrors,
                .prefer_f16_mps_fallback = prefer_f16_mps_mirrors,
                .prefer_f32_mps_fallback = prefer_f32_mps_mirrors,
            }))) return false;
        }

        for (bert_layer_norm_specs) |spec| {
            var weight_name: [256]u8 = undefined;
            var bias_name: [256]u8 = undefined;
            const weight = try getLayerWeight(cb, allocator, layer, spec.weight, &weight_name);
            defer cb.free(weight);
            const bias = try getLayerWeight(cb, allocator, layer, spec.bias, &bias_name);
            defer cb.free(bias);
            if (!(try cb.decoderRuntimePrepareLayerNorm(&.{
                .slot = bertLayerNormSlot(layer, spec.kind),
                .weight = weight,
                .bias = bias,
                .hidden_size = hidden,
            }))) return false;
        }
    }

    const embedding_norm_weight = try cb.getWeight("embeddings.LayerNorm.weight");
    defer cb.free(embedding_norm_weight);
    const embedding_norm_bias = try cb.getWeight("embeddings.LayerNorm.bias");
    defer cb.free(embedding_norm_bias);
    return cb.decoderRuntimePrepareLayerNorm(&.{
        .slot = bertEmbeddingLayerNormSlot(layer_count),
        .weight = embedding_norm_weight,
        .bias = embedding_norm_bias,
        .hidden_size = hidden,
    });
}

/// Run the full BERT encoder forward pass.
/// Returns an owned f32 slice: [batch * seq_len * hidden_size].
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) ![]f32 {
    return forwardWithControl(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len, null);
}

pub fn forwardWithControl(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
    control: ?InferenceExecutionControl,
) ![]f32 {
    const hidden = try forwardCtWithControl(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len, control);
    defer cb.free(hidden);
    if (control) |active| try active.check();
    return cb.toFloat32(hidden, allocator);
}

fn validateBertForwardInputs(
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) !usize {
    const hidden: usize = config.hidden_size;
    const intermediate: usize = config.intermediate_size;
    const heads: usize = config.num_attention_heads;
    const max_positions: usize = config.maxSequenceLength();
    if (batch == 0 or seq_len == 0 or hidden == 0 or intermediate == 0 or heads == 0 or
        hidden % heads != 0 or max_positions == 0 or seq_len > max_positions)
    {
        return error.InvalidShape;
    }
    if (batch > std.math.maxInt(i32) or seq_len > std.math.maxInt(i32) or
        hidden > std.math.maxInt(i32) or intermediate > std.math.maxInt(i32))
    {
        return error.InvalidShape;
    }
    const total = std.math.mul(usize, batch, seq_len) catch return error.InvalidShape;
    if (total > std.math.maxInt(i32)) return error.InvalidShape;
    _ = std.math.mul(usize, total, hidden) catch return error.InvalidShape;
    _ = std.math.mul(usize, total, intermediate) catch return error.InvalidShape;
    if (input_ids.len != total or attention_mask.len != total) return error.InvalidShape;

    for (input_ids) |token_id| {
        if (token_id < 0 or token_id >= config.vocab_size) return error.InvalidTokenId;
    }
    for (attention_mask) |value| {
        if (value != 0 and value != 1) return error.InvalidAttentionMask;
    }
    if (token_type_ids) |values| {
        if (values.len != total or config.type_vocab_size == 0) return error.InvalidShape;
        for (values) |token_type_id| {
            if (token_type_id < 0 or token_type_id >= config.type_vocab_size) return error.InvalidTokenTypeId;
        }
    }
    return total;
}

/// Run the encoder while keeping intermediate tensors on the backend.
pub fn forwardCt(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
) !CT {
    return forwardCtWithControl(cb, allocator, config, input_ids, attention_mask, token_type_ids, batch, seq_len, null);
}

/// Controlled encoder entry point. GPU work is deliberately split into small
/// layer groups when a request can expire so cancellation latency is bounded
/// by a frame rather than the complete encoder.
pub fn forwardCtWithControl(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
    control: ?InferenceExecutionControl,
) !CT {
    if (control) |active| try active.update(.preparing_weights, 0, @intCast(config.num_hidden_layers));
    const H = config.hidden_size;
    const total = try validateBertForwardInputs(config, input_ids, attention_mask, token_type_ids, batch, seq_len);
    // Preparation is lazy on the first request; subsequent requests validate
    // backend-owned slot metadata without loading or wrapping model weights.
    const resident_slots = try preplanMetalEncoder(cb, allocator, config);
    if (control) |active| try active.update(.executing, 0, @intCast(config.num_hidden_layers));

    var encoder_frame_active = false;
    if (resident_slots and !cb.decoderRuntimeHasActiveFrame()) {
        encoder_frame_active = try cb.decoderRuntimeBeginFrame();
    }
    errdefer if (encoder_frame_active) cb.decoderRuntimeCancelFrame() catch {};

    // 1. Embeddings: word + position + token_type + LayerNorm
    var hidden = try embeddings(
        cb,
        allocator,
        config,
        input_ids,
        token_type_ids,
        total,
        seq_len,
        H,
        if (resident_slots) bertEmbeddingLayerNormSlot(@intCast(config.num_hidden_layers)) else null,
    );
    errdefer cb.free(hidden);

    // 2. Encoder layers
    for (0..config.num_hidden_layers) |layer| {
        if (control) |active| try active.update(.executing, @intCast(layer), @intCast(config.num_hidden_layers));
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer, resident_slots);
        cb.free(hidden);
        hidden = new_hidden;
        const bounded_frame = control != null and encoder_frame_active and
            ((layer + 1) % 2 == 0 or layer + 1 == config.num_hidden_layers);
        if (bounded_frame) {
            try cb.decoderRuntimeSubmitAndWaitFrame();
            encoder_frame_active = false;
            if (control) |active| try active.update(.executing, @intCast(layer + 1), @intCast(config.num_hidden_layers));
            if (layer + 1 < config.num_hidden_layers) {
                encoder_frame_active = try cb.decoderRuntimeBeginFrame();
            }
        }
    }

    if (encoder_frame_active) {
        try cb.decoderRuntimeSubmitAndWaitFrame();
        encoder_frame_active = false;
    }
    return hidden;
}

pub fn forwardUntilLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    attention_mask: []const i64,
    token_type_ids: ?[]const i64,
    batch: usize,
    seq_len: usize,
    stop_layer_exclusive: usize,
) ![]f32 {
    const H = config.hidden_size;
    const total = batch * seq_len;
    const clamped_stop = @min(stop_layer_exclusive, config.num_hidden_layers);

    var hidden = try embeddings(cb, allocator, config, input_ids, token_type_ids, total, seq_len, H, null);
    for (0..clamped_stop) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer, false);
        cb.free(hidden);
        hidden = new_hidden;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

pub fn forwardFromHidden(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: []const f32,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    start_layer: usize,
) ![]f32 {
    return forwardFromHiddenRange(cb, allocator, config, hidden_in, attention_mask, batch, seq_len, start_layer, config.num_hidden_layers);
}

pub fn forwardFromHiddenRange(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_in: []const f32,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    start_layer: usize,
    end_layer_exclusive: usize,
) ![]f32 {
    const H = config.hidden_size;
    const total = batch * seq_len;
    if (hidden_in.len != total * H) return error.ShapeMismatch;

    const shape = [_]i32{ @intCast(total), @intCast(H) };
    var hidden = try cb.fromFloat32Shape(hidden_in, &shape);
    const clamped_start = @min(start_layer, config.num_hidden_layers);
    const clamped_end = @max(clamped_start, @min(end_layer_exclusive, config.num_hidden_layers));
    for (clamped_start..clamped_end) |layer| {
        const new_hidden = try encoderLayer(cb, allocator, config, hidden, attention_mask, batch, seq_len, layer, false);
        cb.free(hidden);
        hidden = new_hidden;
    }

    const result = try cb.toFloat32(hidden, allocator);
    cb.free(hidden);
    return result;
}

fn embeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    token_type_ids: ?[]const i64,
    total: usize,
    seq_len: usize,
    H: u32,
    layer_norm_slot: ?usize,
) !CT {
    var context = EmbeddingContext{ .cb = cb, .allocator = allocator, .config = config, .input_ids = input_ids, .token_type_ids = token_type_ids, .total = total, .seq_len = seq_len, .hidden = H, .norm_slot = layer_norm_slot };
    return @import("bert_blocks.zig").embeddings(&context);
}

const EmbeddingContext = struct {
    pub const Value = CT;
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    token_type_ids: ?[]const i64,
    total: usize,
    seq_len: usize,
    hidden: u32,
    norm_slot: ?usize,
    pub fn lookup(self: *@This(), comptime role: @import("bert_blocks.zig").Embedding) !?CT {
        if (role == .token_type and self.config.model_type == .distilbert) return null;
        const weight = self.cb.getWeight(@import("bert_blocks.zig").embeddingName(role)) catch |err| {
            if (role == .token_type) return null;
            return err;
        };
        defer self.cb.free(weight);
        const ids = switch (role) {
            .word => self.input_ids,
            .position => try buildPositionIds(self.allocator, self.config, self.input_ids, self.total, self.seq_len),
            .token_type => if (self.token_type_ids) |ids| ids[0..self.total] else blk: {
                const zeros = try self.allocator.alloc(i64, self.total);
                @memset(zeros, 0);
                break :blk zeros;
            },
        };
        defer if (role == .position or (role == .token_type and self.token_type_ids == null)) self.allocator.free(ids);
        return try self.cb.embeddingLookup(weight, ids, self.total, self.hidden);
    }
    pub fn add(self: *@This(), lhs: CT, rhs: CT) !CT {
        return self.cb.add(lhs, rhs);
    }
    pub fn normalize(self: *@This(), input: CT) !CT {
        return embeddingLayerNormWithSlot(self.cb, input, self.hidden, self.config.layer_norm_eps, self.norm_slot);
    }
    pub fn free(self: *@This(), value: CT) void {
        self.cb.free(value);
    }
};

fn embeddingLayerNormWithSlot(
    cb: *const ComputeBackend,
    input: CT,
    hidden_size: usize,
    eps: f32,
    slot: ?usize,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLayerNorm(&.{
            .slot = prepared_slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        })) |output| return output;
    }

    const weight = try cb.getWeight("embeddings.LayerNorm.weight");
    defer cb.free(weight);
    const bias = try cb.getWeight("embeddings.LayerNorm.bias");
    defer cb.free(bias);
    return cb.layerNorm(input, weight, bias, hidden_size, eps);
}

fn buildPositionIds(
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    total: usize,
    seq_len: usize,
) ![]i64 {
    if (seq_len == 0 or seq_len > config.maxSequenceLength() or input_ids.len < total or total % seq_len != 0) {
        return error.InvalidShape;
    }
    const pos_ids = try allocator.alloc(i64, total);
    errdefer allocator.free(pos_ids);
    switch (config.position_id_mode) {
        .absolute => {
            for (0..total) |i| pos_ids[i] = @intCast(i % seq_len);
        },
        .roberta_padding => {
            if (config.pad_token_id < 0) return error.InvalidTokenId;
            for (0..total / seq_len) |batch_index| {
                var next = std.math.add(i64, config.pad_token_id, 1) catch return error.InvalidShape;
                for (0..seq_len) |sequence_index| {
                    const index = batch_index * seq_len + sequence_index;
                    if (input_ids[index] == config.pad_token_id) {
                        pos_ids[index] = @max(0, config.pad_token_id - @as(i64, config.position_embedding_offset));
                    } else {
                        pos_ids[index] = next - @as(i64, config.position_embedding_offset);
                        next = std.math.add(i64, next, 1) catch return error.InvalidShape;
                    }
                }
            }
        },
    }
    return pos_ids;
}

fn encoderLayer(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    attention_mask: []const i64,
    batch: usize,
    seq_len: usize,
    layer: usize,
    resident_slots: bool,
) !CT {
    var context = EncoderOutputContext{
        .cb = cb,
        .allocator = allocator,
        .config = config,
        .layer = layer,
        .total = batch * seq_len,
        .resident_slots = resident_slots,
        .batch = batch,
        .seq_len = seq_len,
        .attention_mask = attention_mask,
    };
    return @import("bert_blocks.zig").encoderLayer(&context, hidden);
}

const EncoderOutputContext = struct {
    pub const Value = CT;
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    layer: usize,
    total: usize,
    resident_slots: bool,
    batch: usize,
    seq_len: usize,
    attention_mask: []const i64,

    pub fn fusedQueryKeyValue(self: *@This(), hidden: CT) !?ops.LinearTripleResult {
        const cb = self.cb;
        const allocator = self.allocator;
        const config = self.config;
        const layer = self.layer;
        const resident_slots = self.resident_slots;
        const total = self.total;
        const hidden_dim: usize = config.hidden_size;
        const tp_world_size = tensorParallelWorldSize(cb);
        if (hidden_dim % tp_world_size != 0 or config.num_attention_heads % tp_world_size != 0 or config.intermediate_size % tp_world_size != 0) return error.InvalidTensorParallelShape;
        var name_buf: [256]u8 = undefined;
        return if (resident_slots and !platform.env.getenvBool("TERMITE_METAL_DISABLE_BERT_QKV")) blk: {
            if (try cb.decoderRuntimeApplyLinearQkv(&.{
                .q_slot = bertLinearSlot(layer, .q),
                .k_slot = bertLinearSlot(layer, .k),
                .v_slot = bertLinearSlot(layer, .v),
                .input = hidden,
                .in_dim = hidden_dim,
                .q_out_dim = hidden_dim,
                .kv_out_dim = hidden_dim,
            })) |projected| {
                break :blk .{
                    .first = projected.first,
                    .second = projected.second,
                    .third = projected.third,
                };
            }
            break :blk try layerLinearTriple(cb, allocator, layer, hidden, total, hidden_dim, &name_buf);
        } else try layerLinearTriple(cb, allocator, layer, hidden, total, hidden_dim, &name_buf);
    }

    pub fn attention(self: *@This(), q: CT, k: CT, v: CT) !CT {
        const cb = self.cb;
        const attention_mask = self.attention_mask;
        const total = self.total;
        const batch = self.batch;
        const seq_len = self.seq_len;
        const local_num_heads = self.config.num_attention_heads / tensorParallelWorldSize(cb);
        const head_dim = self.config.hidden_size / self.config.num_attention_heads;
        return if (allAttentionMaskOnes(attention_mask, total))
            (try cb.scaledDotProductAttentionFull(q, k, v, null, batch, seq_len, local_num_heads, head_dim)) orelse
                try cb.scaledDotProductAttention(q, k, v, attention_mask, null, batch, seq_len, local_num_heads, head_dim)
        else
            try cb.scaledDotProductAttention(q, k, v, attention_mask, null, batch, seq_len, local_num_heads, head_dim);
    }

    pub fn free(self: *@This(), value: CT) void {
        self.cb.free(value);
    }
    pub fn add(self: *@This(), a: CT, b: CT) !CT {
        return self.cb.add(a, b);
    }
    pub fn activate(self: *@This(), input: CT) !CT {
        return applyHiddenActivation(self.cb, input, self.config.hidden_act);
    }
    pub fn linear(self: *@This(), comptime role: @import("bert_blocks.zig").Linear, input: CT) !CT {
        const base = comptime @import("bert_blocks.zig").linearBase(role);
        const slot: BertLinearSlotKind = role;
        const H = self.config.hidden_size;
        const I = self.config.intermediate_size;
        var name_buf: [256]u8 = undefined;
        return layerLinearWithSlot(self.cb, self.allocator, self.layer, input, base ++ ".weight", base ++ ".bias", self.total, if (role == .output) I else H, if (role == .intermediate) I else H, if (self.resident_slots) bertLinearSlot(self.layer, slot) else null, &name_buf);
    }
    pub fn norm(self: *@This(), comptime role: @import("bert_blocks.zig").Norm, input: CT) !CT {
        const base = switch (role) {
            .attention_output => "attention.output.LayerNorm",
            .output => "output.LayerNorm",
        };
        const slot: BertLayerNormSlotKind = switch (role) {
            .attention_output => .attention_output,
            .output => .output,
        };
        var name_buf: [256]u8 = undefined;
        return layerNormForLayerWithSlot(self.cb, self.allocator, self.layer, input, base ++ ".weight", base ++ ".bias", self.config.hidden_size, self.config.layer_norm_eps, if (self.resident_slots) bertLayerNormSlot(self.layer, slot) else null, &name_buf);
    }
    pub fn fusedAttentionOutput(self: *@This(), hidden: CT, attention_value: CT) !?CT {
        if (!self.resident_slots) return null;
        return self.cb.decoderRuntimeApplyLinearLayerNorm(&.{
            .linear_slot = bertLinearSlot(self.layer, .attention_output),
            .layer_norm_slot = bertLayerNormSlot(self.layer, .attention_output),
            .input = attention_value,
            .residual = hidden,
            .in_dim = self.config.hidden_size,
            .hidden_size = self.config.hidden_size,
            .eps = self.config.layer_norm_eps,
        });
    }
    pub fn fusedFeedForward(self: *@This(), input: CT) !?CT {
        if (!self.resident_slots) return null;
        return self.cb.decoderRuntimeApplyFfnLayerNorm(&.{
            .first_linear_slot = bertLinearSlot(self.layer, .intermediate),
            .second_linear_slot = bertLinearSlot(self.layer, .output),
            .layer_norm_slot = bertLayerNormSlot(self.layer, .output),
            .input = input,
            .residual = input,
            .hidden_size = self.config.hidden_size,
            .intermediate_size = self.config.intermediate_size,
            .eps = self.config.layer_norm_eps,
            .activation = runtimeActivationKind(self.config.hidden_act),
        });
    }
    pub fn fusedFeedForwardResidual(self: *@This(), input: CT) !?CT {
        if (!self.resident_slots) return null;
        return self.cb.runDenseFfnResidual(&.{
            .first_linear_slot = bertLinearSlot(self.layer, .intermediate),
            .second_linear_slot = bertLinearSlot(self.layer, .output),
            .input = input,
            .residual = input,
            .hidden_size = self.config.hidden_size,
            .intermediate_size = self.config.intermediate_size,
            .activation = runtimeActivationKind(self.config.hidden_act),
        });
    }
};

fn runtimeActivationKind(activation: bert_config.HiddenActivation) ops.DecoderRuntimeActivationKind {
    return switch (activation) {
        .gelu_exact => .gelu_exact,
        .gelu_tanh => .gelu_new,
        .relu => .relu,
        .silu => .silu,
    };
}

fn applyHiddenActivation(cb: *const ComputeBackend, input: CT, activation: bert_config.HiddenActivation) !CT {
    return switch (activation) {
        .gelu_exact => (try cb.geluExact(input)) orelse return error.UnsupportedBertActivation,
        .gelu_tanh => cb.geluNew(input),
        .relu => cb.relu(input),
        .silu => cb.silu(input),
    };
}

/// Build a layer weight name like "encoder.layer.N.suffix" and look it up.
fn getLayerWeight(cb: *const ComputeBackend, _: std.mem.Allocator, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "encoder.layer.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return cb.getWeight(name);
}

fn layerLinearTriple(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    input: CT,
    rows: usize,
    hidden_dim: usize,
    name_buf: *[256]u8,
) !ops.LinearTripleResult {
    const q_w = try getLayerWeight(cb, allocator, layer, "attention.self.query.weight", name_buf);
    defer cb.free(q_w);
    const q_b = try getLayerWeight(cb, allocator, layer, "attention.self.query.bias", name_buf);
    defer cb.free(q_b);
    const k_w = try getLayerWeight(cb, allocator, layer, "attention.self.key.weight", name_buf);
    defer cb.free(k_w);
    const k_b = try getLayerWeight(cb, allocator, layer, "attention.self.key.bias", name_buf);
    defer cb.free(k_b);
    const v_w = try getLayerWeight(cb, allocator, layer, "attention.self.value.weight", name_buf);
    defer cb.free(v_w);
    const v_b = try getLayerWeight(cb, allocator, layer, "attention.self.value.bias", name_buf);
    defer cb.free(v_b);
    return cb.linearTriple(input, q_w, q_b, k_w, k_b, v_w, v_b, rows, hidden_dim, hidden_dim);
}

fn allAttentionMaskOnes(mask: []const i64, total: usize) bool {
    if (mask.len < total) return false;
    for (mask[0..total]) |value| if (value == 0) return false;
    return true;
}

fn tensorParallelWorldSize(cb: *const ComputeBackend) usize {
    _ = cb;
    return 1;
}

fn layerLinearWithSlot(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    input: CT,
    weight_suffix: []const u8,
    bias_suffix: []const u8,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    slot: ?usize,
    name_buf: *[256]u8,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLinear(&.{
            .slot = prepared_slot,
            .input = input,
            .in_dim = input_dim,
            .out_dim = output_dim,
        })) |output| return output;
    }
    const weight = try getLayerWeight(cb, allocator, layer, weight_suffix, name_buf);
    defer cb.free(weight);
    const bias = try getLayerWeight(cb, allocator, layer, bias_suffix, name_buf);
    defer cb.free(bias);
    return cb.linear(input, weight, bias, rows, input_dim, output_dim);
}

fn layerNormForLayerWithSlot(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    input: CT,
    weight_suffix: []const u8,
    bias_suffix: []const u8,
    hidden_size: usize,
    eps: f32,
    slot: ?usize,
    name_buf: *[256]u8,
) !CT {
    if (slot) |prepared_slot| {
        if (try cb.decoderRuntimeApplyLayerNorm(&.{
            .slot = prepared_slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        })) |output| return output;
    }
    const weight = try getLayerWeight(cb, allocator, layer, weight_suffix, name_buf);
    defer cb.free(weight);
    const bias = try getLayerWeight(cb, allocator, layer, bias_suffix, name_buf);
    defer cb.free(bias);
    return cb.layerNorm(input, weight, bias, hidden_size, eps);
}

test "BERT resident slot layout is stable and non-overlapping" {
    try std.testing.expectEqual(@as(usize, 143), bertLinearSlot(23, .output));
    try std.testing.expectEqual(@as(usize, 47), bertLayerNormSlot(23, .output));
    try std.testing.expectEqual(@as(usize, 48), bertEmbeddingLayerNormSlot(24));
}

const MetalEncoderPreparedProbe = struct {
    linear_calls: usize = 0,
    layer_norm_calls: usize = 0,
    missing_linear_slot: ?usize = null,
    missing_layer_norm_slot: ?usize = null,

    fn linearPrepared(ctx: *anyopaque, slot: usize, in_dim: usize, out_dim: usize) bool {
        const self: *MetalEncoderPreparedProbe = @ptrCast(@alignCast(ctx));
        self.linear_calls += 1;
        if (self.missing_linear_slot == slot) return false;
        const kind: BertLinearSlotKind = @fromBackingInt(@intCast(slot % bert_linear_specs.len));
        const spec = bert_linear_specs[@backingInt(kind)];
        const expected_in_dim: usize = if (spec.input_intermediate) 8 else 4;
        const expected_out_dim: usize = if (spec.output_intermediate) 8 else 4;
        return in_dim == expected_in_dim and out_dim == expected_out_dim;
    }

    fn layerNormPrepared(ctx: *anyopaque, slot: usize, hidden_size: usize) bool {
        const self: *MetalEncoderPreparedProbe = @ptrCast(@alignCast(ctx));
        self.layer_norm_calls += 1;
        return self.missing_layer_norm_slot != slot and hidden_size == 4;
    }

    const vtable = blk: {
        var vt = native_compute_mod.vtable_impl;
        vt.decoderRuntimeLinearSlotPrepared = linearPrepared;
        vt.decoderRuntimeLayerNormSlotPrepared = layerNormPrepared;
        break :blk vt;
    };
};

const EmbeddingLayerNormResidentProbe = struct {
    input_marker: u8 = 0,
    output_marker: u8 = 0,
    apply_calls: usize = 0,
    weight_calls: usize = 0,

    fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
        const self: *EmbeddingLayerNormResidentProbe = @ptrCast(@alignCast(ctx));
        _ = name;
        self.weight_calls += 1;
        return error.UnexpectedWeightLookup;
    }

    fn applyLayerNorm(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormRequest) anyerror!?CT {
        const self: *EmbeddingLayerNormResidentProbe = @ptrCast(@alignCast(ctx));
        self.apply_calls += 1;
        try std.testing.expectEqual(@as(usize, 7), request.slot);
        try std.testing.expectEqual(@as(usize, 4), request.hidden_size);
        try std.testing.expectApproxEqAbs(@as(f32, 1e-5), request.eps, 1e-9);
        try std.testing.expectEqual(@intFromPtr(&self.input_marker), @intFromPtr(request.input));
        return @ptrCast(&self.output_marker);
    }

    const vtable = blk: {
        var vt = native_compute_mod.vtable_impl;
        vt.getWeight = getWeight;
        vt.acquireWeight = getWeight;
        vt.decoderRuntimeApplyLayerNorm = applyLayerNorm;
        break :blk vt;
    };
};

test "BERT Metal resident slot probe is allocation-free and detects invalidation" {
    const config = Config{
        .hidden_size = 4,
        .intermediate_size = 8,
        .num_hidden_layers = 2,
        .num_attention_heads = 2,
    };
    var probe = MetalEncoderPreparedProbe{};
    const cb = ComputeBackend{ .ptr = &probe, .vtable = &MetalEncoderPreparedProbe.vtable };

    try std.testing.expect(metalEncoderSlotsPrepared(&cb, config));
    try std.testing.expectEqual(@as(usize, 12), probe.linear_calls);
    try std.testing.expectEqual(@as(usize, 5), probe.layer_norm_calls);

    probe.linear_calls = 0;
    probe.layer_norm_calls = 0;
    probe.missing_linear_slot = bertLinearSlot(1, .k);
    try std.testing.expect(!metalEncoderSlotsPrepared(&cb, config));
    try std.testing.expectEqual(bertLinearSlot(1, .k) + 1, probe.linear_calls);
    try std.testing.expectEqual(@as(usize, 2), probe.layer_norm_calls);
}

test "BERT resident embedding LayerNorm does not materialize fallback weights" {
    var probe = EmbeddingLayerNormResidentProbe{};
    const cb = ComputeBackend{ .ptr = &probe, .vtable = &EmbeddingLayerNormResidentProbe.vtable };
    const input: CT = @ptrCast(&probe.input_marker);

    const output = try embeddingLayerNormWithSlot(&cb, input, 4, 1e-5, 7);

    try std.testing.expectEqual(@intFromPtr(&probe.output_marker), @intFromPtr(output));
    try std.testing.expectEqual(@as(usize, 1), probe.apply_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.weight_calls);
}

test "RoBERTa position ids preserve padding indices" {
    const config = Config{ .position_id_mode = .roberta_padding, .pad_token_id = 1 };
    const input_ids = [_]i64{ 0, 42, 1, 9, 1, 1, 5, 6 };
    const positions = try buildPositionIds(std.testing.allocator, config, &input_ids, input_ids.len, 4);
    defer std.testing.allocator.free(positions);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3, 1, 4, 1, 1, 2, 3 }, positions);
}

test "RoBERTa position ids stay within the embedding table" {
    const config = Config{
        .position_id_mode = .roberta_padding,
        .pad_token_id = 1,
        .max_position_embeddings = 4,
    };
    try std.testing.expectEqual(@as(u32, 2), config.maxSequenceLength());

    const valid_ids = [_]i64{ 10, 11 };
    const positions = try buildPositionIds(std.testing.allocator, config, &valid_ids, valid_ids.len, valid_ids.len);
    defer std.testing.allocator.free(positions);
    try std.testing.expectEqualSlices(i64, &.{ 2, 3 }, positions);

    const too_many_ids = [_]i64{ 10, 11, 12 };
    try std.testing.expectError(
        error.InvalidShape,
        buildPositionIds(std.testing.allocator, config, &too_many_ids, too_many_ids.len, too_many_ids.len),
    );
}

test "BERT forward input validation rejects unsafe public inputs" {
    const config = Config{
        .vocab_size = 16,
        .hidden_size = 8,
        .intermediate_size = 16,
        .num_attention_heads = 2,
        .max_position_embeddings = 4,
    };
    const ids = [_]i64{ 1, 2, 3, 4 };
    const mask = [_]i64{ 1, 1, 1, 0 };
    try std.testing.expectEqual(@as(usize, 4), try validateBertForwardInputs(config, &ids, &mask, null, 1, 4));

    const invalid_mask = [_]i64{ 1, 1, -1, 0 };
    try std.testing.expectError(
        error.InvalidAttentionMask,
        validateBertForwardInputs(config, &ids, &invalid_mask, null, 1, 4),
    );
    try std.testing.expectError(error.InvalidShape, validateBertForwardInputs(config, &ids, &mask, null, 1, 5));

    const roberta_config = Config{
        .vocab_size = 16,
        .hidden_size = 8,
        .intermediate_size = 16,
        .num_attention_heads = 2,
        .max_position_embeddings = 4,
        .position_id_mode = .roberta_padding,
        .pad_token_id = 1,
    };
    try std.testing.expectError(error.InvalidShape, validateBertForwardInputs(roberta_config, &ids, &mask, null, 1, 4));
}

test "BGE-M3 exact geometry admits the F32 rollback ceiling" {
    const config = Config{
        .model_type = .roberta,
        .vocab_size = 250002,
        .hidden_size = 1024,
        .intermediate_size = 4096,
        .num_hidden_layers = 24,
        .num_attention_heads = 16,
        .max_position_embeddings = 8194,
        .type_vocab_size = 1,
        .hidden_act = .gelu_exact,
        .layer_norm_eps = 1e-5,
        .pad_token_id = 1,
        .position_id_mode = .roberta_padding,
    };
    try std.testing.expect(isBgeM3DenseConfig(config));
    try std.testing.expectEqual(@as(usize, 1152 * 1024 * 1024), bertDenseMirrorBytes(config, @sizeOf(f32)).?);
    try std.testing.expect(bertDenseMirrorBytes(config, @sizeOf(f32)).? <= metalBertWeightMirrorMaxBytes(config));

    var wrong_geometry = config;
    wrong_geometry.hidden_size = 768;
    try std.testing.expect(!isBgeM3DenseConfig(wrong_geometry));
}

test "RoBERTa cropped GGUF positions preserve padding and full context" {
    const allocator = std.testing.allocator;
    const config = Config{
        .position_id_mode = .roberta_padding,
        .pad_token_id = 1,
        .position_embedding_offset = 2,
        .max_position_embeddings = 4,
    };
    try std.testing.expectEqual(@as(u32, 4), config.maxSequenceLength());
    const ids = try buildPositionIds(allocator, config, &.{ 0, 4, 5, 2, 1, 0, 2, 1 }, 8, 4);
    defer allocator.free(ids);
    try std.testing.expectEqualSlices(i64, &.{ 0, 1, 2, 3, 0, 0, 1, 0 }, ids);
}
