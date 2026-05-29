// Copyright 2026 Antfly, Inc.
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

const std = @import("std");

const contracts = @import("../graph/backend_contracts.zig");
const gpt_arch = @import("gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");

const c_std = @cImport(@cInclude("stdlib.h"));

pub const max_runtime_layers = 256;

pub fn getenvBool(comptime name: [*:0]const u8) bool {
    const value = c_std.getenv(name) orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

pub fn shouldSkipSharedDecoderPrewarm(config: gpt_mod.Config) bool {
    return config.gemma4_mtp_assistant;
}

pub fn supportsRuntimeConfig(config: gpt_mod.Config) bool {
    return config.family == .gemma and !config.usesMoe();
}

pub fn supportsWholeFramePrefill(config: gpt_mod.Config, configured_layer_count: usize) bool {
    if (!supportsRuntimeConfig(config)) return false;
    if (config.num_hidden_layers == 0 or config.num_hidden_layers > max_runtime_layers) return false;
    if (preparedLayers(@min(configured_layer_count, config.num_hidden_layers)) != config.num_hidden_layers) return false;
    if (getenvBool("TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK")) return false;
    return true;
}

pub fn preparedLayers(configured_layers: usize) usize {
    const value = c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_GATED_LAYERS") orelse return configured_layers;
    const parsed = std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch return configured_layers;
    return @min(configured_layers, parsed);
}

pub fn decoderActivationKind(config: gpt_mod.Config) contracts.DecoderRuntimeActivationKind {
    return switch (config.activation) {
        .gelu => .gelu,
        .gelu_new => .gelu_new,
        .silu => .silu,
        .relu => .relu,
        .relu_squared => .relu_squared,
    };
}

pub fn normSlot(layer: usize, kind: anytype) usize {
    return switch (kind) {
        .attn_pre => layer * 4,
        .attn_post => layer * 4 + 1,
        .ffn_pre => layer * 4 + 2,
        .ffn_post => layer * 4 + 3,
        else => unreachable,
    };
}

pub fn linearSlot(layer: usize, kind: anytype) usize {
    return switch (kind) {
        .attn_q => layer * 7,
        .attn_k => layer * 7 + 1,
        .attn_v => layer * 7 + 2,
        .attn_out_proj => layer * 7 + 3,
        .mlp_gate => layer * 7 + 4,
        .mlp_up => layer * 7 + 5,
        .mlp_down => layer * 7 + 6,
        else => unreachable,
    };
}

pub fn pleGateSlot(configured_layer_count: usize, layer: usize) usize {
    return configured_layer_count * 8 + layer * 2;
}

pub fn pleProjSlot(configured_layer_count: usize, layer: usize) usize {
    return configured_layer_count * 8 + layer * 2 + 1;
}

pub fn finalLmHeadSlot(configured_layer_count: usize) usize {
    return configured_layer_count * 10;
}

pub fn pleModelProjSlot(configured_layer_count: usize) usize {
    return finalLmHeadSlot(configured_layer_count) + 1;
}

pub fn finalNormSlot(configured_layer_count: usize) usize {
    return configured_layer_count * 4;
}

pub fn pleProjNormSlot(configured_layer_count: usize) usize {
    return finalNormSlot(configured_layer_count) + 1 + configured_layer_count;
}

pub fn plePostNormSlot(configured_layer_count: usize, layer: usize) usize {
    return finalNormSlot(configured_layer_count) + 1 + layer;
}

pub fn qHeadNormSlot(configured_layer_count: usize, layer: usize) usize {
    return pleProjNormSlot(configured_layer_count) + 1 + configured_layer_count + layer * 2;
}

pub fn kHeadNormSlot(configured_layer_count: usize, layer: usize) usize {
    return qHeadNormSlot(configured_layer_count, layer) + 1;
}

pub fn layerOutputScaleValue(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_mod.Config,
    layer: usize,
) !?f32 {
    var output_scale_name_buf: [256]u8 = undefined;
    const output_scale_name = std.fmt.bufPrint(
        &output_scale_name_buf,
        "model.layers.{d}.per_layer_input.layer_output_scale.weight",
        .{layer},
    ) catch return error.NameTooLong;
    const scale = gpt_arch.getModelWeight(cb, config, output_scale_name) catch |err| switch (err) {
        error.MissingWeight => blk: {
            var fallback_buf: [256]u8 = undefined;
            const fallback_name = std.fmt.bufPrint(
                &fallback_buf,
                "model.layers.{d}.layer_scalar",
                .{layer},
            ) catch return error.NameTooLong;
            break :blk gpt_arch.getModelWeight(cb, config, fallback_name) catch |fallback_err| switch (fallback_err) {
                error.MissingWeight => return null,
                else => return fallback_err,
            };
        },
        else => return err,
    };
    defer cb.free(scale);

    const host = try cb.toFloat32(scale, allocator);
    defer allocator.free(host);
    if (host.len != 1) return error.InvalidTensorShape;
    return host[0];
}

pub fn layerSpec(
    config: gpt_mod.Config,
    configured_layer_count: usize,
    layer: usize,
    output_scale_value: ?f32,
) contracts.DecoderRuntimeLayerSpec {
    const shares_kv = config.layerSharesKv(layer);
    return .{
        .kv_heads = @intCast(config.effectiveKVHeadsForLayer(layer)),
        .head_dim = @intCast(config.effectiveHeadDimForLayer(layer)),
        .intermediate_size = @intCast(config.intermediateSize(layer)),
        .kv_layer_index = if (shares_kv) config.kvDonorLayerIndex(layer).? else layer,
        .shares_kv = shares_kv,
        .sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0,
        .rope_dim = @intCast(config.layerRopeFrequencyDim(layer)),
        .rope_active_dim = @intCast(config.layerRopeActiveDim(layer)),
        .rope_theta = config.layerRopeTheta(layer),
        .attn_pre_norm_slot = normSlot(layer, .attn_pre),
        .attn_post_norm_slot = normSlot(layer, .attn_post),
        .ffn_pre_norm_slot = normSlot(layer, .ffn_pre),
        .ffn_post_norm_slot = normSlot(layer, .ffn_post),
        .q_head_norm_slot = qHeadNormSlot(configured_layer_count, layer),
        .k_head_norm_slot = kHeadNormSlot(configured_layer_count, layer),
        .q_linear_slot = linearSlot(layer, .attn_q),
        .k_linear_slot = linearSlot(layer, .attn_k),
        .v_linear_slot = linearSlot(layer, .attn_v),
        .attention_linear_slot = linearSlot(layer, .attn_out_proj),
        .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
        .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
        .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
        .ple_gate_linear_slot = if (config.hasPle()) pleGateSlot(configured_layer_count, layer) else null,
        .ple_proj_linear_slot = if (config.hasPle()) pleProjSlot(configured_layer_count, layer) else null,
        .ple_post_norm_slot = if (config.hasPle()) plePostNormSlot(configured_layer_count, layer) else null,
        .output_scale_value = output_scale_value,
    };
}

pub fn fillLayerSpecs(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_mod.Config,
    configured_layer_count: usize,
    output: []contracts.DecoderRuntimeLayerSpec,
    include_output_scale: bool,
) ![]const contracts.DecoderRuntimeLayerSpec {
    const layer_count = config.num_hidden_layers;
    if (layer_count > output.len) return error.TooManyLayers;
    for (0..layer_count) |layer| {
        const scale_value = if (include_output_scale)
            try layerOutputScaleValue(cb, allocator, config, layer)
        else
            null;
        output[layer] = layerSpec(config, configured_layer_count, layer, scale_value);
    }
    return output[0..layer_count];
}

test "gemma4 runtime slot layout is stable" {
    try std.testing.expectEqual(@as(usize, 7), linearSlot(1, .attn_q));
    try std.testing.expectEqual(@as(usize, 10), linearSlot(1, .attn_out_proj));
    try std.testing.expectEqual(@as(usize, 4), normSlot(1, .attn_pre));
    try std.testing.expectEqual(@as(usize, 320), finalLmHeadSlot(32));
    try std.testing.expectEqual(@as(usize, 321), pleModelProjSlot(32));
    try std.testing.expectEqual(@as(usize, 32 * 4), finalNormSlot(32));
    try std.testing.expectEqual(@as(usize, 32 * 4 + 1 + 32), pleProjNormSlot(32));
}
