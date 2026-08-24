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

pub fn isQualifiedA4bArchitecture(config: gpt_mod.Config) bool {
    if (config.family != .gemma or !config.usesMoe()) return false;
    if (config.gemma4_mtp_assistant or config.isMultimodal() or config.num_shared_experts != 1) return false;
    const moe_layer_count = std.math.cast(u16, config.num_hidden_layers) orelse return false;
    const expert_count = std.math.cast(u16, config.num_local_experts) orelse return false;
    const top_k = std.math.cast(u8, config.num_experts_per_tok) orelse return false;
    return contracts.isQualifiedA4bGeometry(.{
        .moe_layer_count = moe_layer_count,
        .expert_count = expert_count,
        .top_k = top_k,
        .hidden_size = config.hidden_size,
        .expert_intermediate_size = config.expertIntermediateSize(),
        .encoded_expert_bytes = contracts.qualified_a4b_geometries[0].encoded_expert_bytes,
    });
}

pub fn supportsRuntimeConfig(config: gpt_mod.Config) bool {
    return config.family == .gemma and (!config.usesMoe() or isQualifiedA4bArchitecture(config));
}

/// The existing prepared whole-frame decoder owns dense FFNs. Qualified A4B
/// is accepted by the containing Metal executor but must use the MoE graph
/// path until its dedicated prepared runtime is installed.
pub fn supportsPreparedDenseRuntimeConfig(config: gpt_mod.Config) bool {
    return config.family == .gemma and !config.usesMoe();
}

/// The prepared A4B decoder intentionally sits behind the resident/high-memory
/// lane. This keeps the compact 2 GiB contract unchanged while allowing larger
/// Apple Silicon systems to retain all packed experts and backend descriptors.
pub fn supportsPreparedA4bRuntimeConfig(config: gpt_mod.Config) bool {
    if (!isQualifiedA4bArchitecture(config)) return false;
    if (getenvBool("TERMITE_METAL_DISABLE_A4B_PREPARED_DECODE")) return false;
    // Keep the backend-owned A4B frame independent from the qualified
    // high-memory bundle until its token-level parity gate passes.  This also
    // gives us a stable, same-binary rollback/control lane while iterating on
    // the frame implementation.
    return getenvBool("TERMITE_METAL_ENABLE_A4B_PREPARED_DECODE");
}

pub fn wholeFramePrefillExplicitlyDisabled() bool {
    return getenvBool("TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK");
}

pub fn supportsWholeFramePrefill(config: gpt_mod.Config, configured_layer_count: usize) bool {
    if (!supportsPreparedDenseRuntimeConfig(config)) return false;
    if (config.num_hidden_layers == 0 or config.num_hidden_layers > max_runtime_layers) return false;
    if (preparedLayers(@min(configured_layer_count, config.num_hidden_layers)) != config.num_hidden_layers) return false;
    if (wholeFramePrefillExplicitlyDisabled()) return false;
    return true;
}

test "Gemma4 runtime admits only the qualified A4B architecture" {
    var config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2816,
        .num_hidden_layers = 30,
        .num_local_experts = 128,
        .num_experts_per_tok = 8,
        .num_shared_experts = 1,
        .expert_intermediate_size = 704,
    };
    try std.testing.expect(isQualifiedA4bArchitecture(config));
    try std.testing.expect(supportsRuntimeConfig(config));
    try std.testing.expect(!supportsPreparedDenseRuntimeConfig(config));

    config.num_local_experts = 64;
    try std.testing.expect(!isQualifiedA4bArchitecture(config));
    try std.testing.expect(!supportsRuntimeConfig(config));
    config.num_local_experts = 65_536;
    try std.testing.expect(!isQualifiedA4bArchitecture(config));
}

pub fn preparedLayers(configured_layers: usize) usize {
    const value = c_std.getenv("TERMITE_METAL_WHOLE_TOKEN_GATED_LAYERS") orelse return configured_layers;
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

/// Dense Gemma uses seven fixed linear slots per layer. A4B reuses the three
/// FFN slots for its shared expert and places the small router matrix in the
/// otherwise unused gap immediately before the PLE slots.
pub fn moeRouterSlot(configured_layer_count: usize, layer: usize) usize {
    return configured_layer_count * 7 + layer;
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
    const a4b = isQualifiedA4bArchitecture(config);
    const shared_intermediate_size = if (a4b and config.shared_expert_intermediate_size > 0)
        config.shared_expert_intermediate_size
    else
        config.intermediateSize(layer);
    return .{
        .kv_heads = @intCast(config.effectiveKVHeadsForLayer(layer)),
        .head_dim = @intCast(config.effectiveHeadDimForLayer(layer)),
        .intermediate_size = @intCast(shared_intermediate_size),
        .kv_layer_index = if (shares_kv) config.kvDonorLayerIndex(layer).? else layer,
        .shares_kv = shares_kv,
        .sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0,
        .rope_dim = @intCast(config.layerRopeFrequencyDim(layer)),
        .rope_active_dim = @intCast(config.layerRopeActiveDim(layer)),
        .rope_theta = config.layerRopeEffectiveTheta(layer),
        .attn_pre_norm_slot = normSlot(layer, .attn_pre),
        .attn_post_norm_slot = normSlot(layer, .attn_post),
        .ffn_pre_norm_slot = normSlot(layer, .ffn_pre),
        .ffn_post_norm_slot = normSlot(layer, .ffn_post),
        .q_head_norm_slot = qHeadNormSlot(configured_layer_count, layer),
        .k_head_norm_slot = if (!shares_kv) kHeadNormSlot(configured_layer_count, layer) else null,
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
        .moe = if (a4b) .{
            .expert_intermediate_size = @intCast(config.expertIntermediateSize()),
            .num_experts = @intCast(config.num_local_experts),
            .top_k = @intCast(config.num_experts_per_tok),
            .router_linear_slot = moeRouterSlot(configured_layer_count, layer),
            .router_logit_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(config.hidden_size))),
        } else null,
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

pub fn layerSpecConfigFingerprint(config: gpt_mod.Config, configured_layer_count: usize, include_output_scale: bool) u64 {
    var hasher = std.hash.Wyhash.init(0);
    std.hash.autoHash(&hasher, config.family);
    std.hash.autoHash(&hasher, configured_layer_count);
    std.hash.autoHash(&hasher, include_output_scale);
    std.hash.autoHash(&hasher, config.hidden_size);
    std.hash.autoHash(&hasher, config.num_hidden_layers);
    std.hash.autoHash(&hasher, config.num_attention_heads);
    std.hash.autoHash(&hasher, config.num_key_value_heads);
    std.hash.autoHash(&hasher, config.attention_head_dim);
    std.hash.autoHash(&hasher, config.intermediate_size);
    std.hash.autoHash(&hasher, config.expert_intermediate_size);
    std.hash.autoHash(&hasher, config.shared_expert_intermediate_size);
    std.hash.autoHash(&hasher, config.num_local_experts);
    std.hash.autoHash(&hasher, config.num_experts_per_tok);
    std.hash.autoHash(&hasher, config.num_shared_experts);
    std.hash.autoHash(&hasher, config.sliding_window);
    std.hash.autoHash(&hasher, config.num_kv_shared_layers);
    std.hash.autoHash(&hasher, config.global_head_dim);
    std.hash.autoHash(&hasher, config.num_global_key_value_heads);
    std.hash.autoHash(&hasher, config.shared_layer_intermediate_size);
    std.hash.autoHash(&hasher, config.ple_hidden_size);
    std.hash.autoHash(&hasher, config.gemma4_mtp_assistant);
    std.hash.autoHash(&hasher, config.mtp_kv_sliding_donor_layer);
    std.hash.autoHash(&hasher, config.mtp_kv_full_donor_layer);
    std.hash.autoHash(&hasher, @as(u32, @bitCast(config.rope_theta)));
    std.hash.autoHash(&hasher, @as(u32, @bitCast(config.rope_local_theta)));
    std.hash.autoHash(&hasher, @as(u32, @bitCast(config.rope_partial_factor)));
    std.hash.autoHash(&hasher, config.rope_dim_override);
    std.hash.autoHash(&hasher, config.sliding_window_pattern);
    return hasher.final();
}

pub fn fillLayerSpecsCached(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_mod.Config,
    configured_layer_count: usize,
    fallback_output: []contracts.DecoderRuntimeLayerSpec,
    include_output_scale: bool,
    cache_opt: ?*gpt_arch.Gemma4LayerSpecCache,
) ![]const contracts.DecoderRuntimeLayerSpec {
    const layer_count = config.num_hidden_layers;
    const cache = cache_opt orelse return fillLayerSpecs(
        cb,
        allocator,
        config,
        configured_layer_count,
        fallback_output,
        include_output_scale,
    );
    const config_fingerprint = layerSpecConfigFingerprint(config, configured_layer_count, include_output_scale);
    if (cache.matches(configured_layer_count, layer_count, include_output_scale, config_fingerprint)) {
        return cache.layers[0..layer_count];
    }
    cache.valid = false;
    if (cache.layers.len < layer_count) {
        if (cache.layers.len > 0) allocator.free(cache.layers);
        cache.layers = try allocator.alloc(contracts.DecoderRuntimeLayerSpec, layer_count);
    }
    const layers = try fillLayerSpecs(
        cb,
        allocator,
        config,
        configured_layer_count,
        cache.layers,
        include_output_scale,
    );
    cache.configured_layer_count = configured_layer_count;
    cache.num_hidden_layers = layer_count;
    cache.include_output_scale = include_output_scale;
    cache.config_fingerprint = config_fingerprint;
    cache.valid = true;
    return layers;
}

test "gemma4 layer spec fingerprint changes with spec shaping fields" {
    var config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 2304,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .intermediate_size = 9216,
        .sliding_window = 1024,
        .ple_hidden_size = 256,
    };
    const base = layerSpecConfigFingerprint(config, 35, true);
    config.rope_partial_factor = 0.5;
    try std.testing.expect(base != layerSpecConfigFingerprint(config, 35, true));
    config.rope_partial_factor = 1.0;
    config.num_kv_shared_layers = 5;
    try std.testing.expect(base != layerSpecConfigFingerprint(config, 35, true));
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

test "gemma4 whole-frame prefill supports shared kv" {
    const config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .num_kv_shared_layers = 5,
        .intermediate_size = 8960,
        .ple_hidden_size = 256,
    };
    try std.testing.expect(supportsWholeFramePrefill(config, config.num_hidden_layers));
}

test "gemma4 shared kv layer spec omits k head norm slot" {
    const config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 4,
        .num_kv_shared_layers = 5,
        .intermediate_size = 8960,
        .ple_hidden_size = 256,
    };
    const donor = layerSpec(config, config.num_hidden_layers, 29, null);
    const shared = layerSpec(config, config.num_hidden_layers, 30, null);

    try std.testing.expect(!donor.shares_kv);
    try std.testing.expect(donor.k_head_norm_slot != null);
    try std.testing.expect(shared.shares_kv);
    try std.testing.expectEqual(@as(?usize, null), shared.k_head_norm_slot);
}

test "gemma4 layer spec resolves rope factors before the backend boundary" {
    const config = gpt_mod.Config{
        .family = .gemma,
        .hidden_size = 1536,
        .num_hidden_layers = 35,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .intermediate_size = 6144,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .rope_theta = 1_000_000.0,
        .rope_partial_factor = 1.0,
        .rope_dim_override = 128,
    };
    const full_attention = layerSpec(config, config.num_hidden_layers, 4, null);

    try std.testing.expectEqual(@as(usize, 512), full_attention.rope_dim);
    try std.testing.expectEqual(@as(usize, 128), full_attention.rope_active_dim);
    try std.testing.expectApproxEqRel(
        config.layerRopeEffectiveTheta(4),
        full_attention.rope_theta,
        1e-6,
    );
}
