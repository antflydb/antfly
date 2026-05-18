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
const build_options = @import("build_options");
const gpt_arch = @import("../architectures/gpt.zig");
const model_runtime = @import("../graph/model_runtime.zig");
const gpt_mod = @import("../models/gpt.zig");
const decoder_rms_runtime = @import("decoder_rms_runtime.zig");
const decoder_tail_runtime = @import("decoder_tail_runtime.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
const ops = @import("../ops/ops.zig");

const c_std = @cImport(@cInclude("stdlib.h"));

fn prefillTraceEnabled() bool {
    const value = c_std.getenv("TERMITE_METAL_PREFILL_TRACE") orelse return false;
    const text = std.mem.span(value);
    return std.mem.eql(u8, text, "1") or std.ascii.eqlIgnoreCase(text, "true");
}

pub const TimingStats = struct {
    prepare_calls: u64 = 0,
    prepare_greedy_nanos: u128 = 0,
    lookup_nanos: u128 = 0,
    norm_prep_nanos: u128 = 0,
    linear_prep_nanos: u128 = 0,
    final_lookup_nanos: u128 = 0,
    final_prep_nanos: u128 = 0,
    prefill_calls: u64 = 0,
    prefill_embed_nanos: u128 = 0,
    prefill_block_nanos: u128 = 0,
    prefill_tail_nanos: u128 = 0,
    greedy_calls: u64 = 0,
    greedy_embed_nanos: u128 = 0,
    greedy_block_nanos: u128 = 0,
    greedy_tail_nanos: u128 = 0,
    sampled_calls: u64 = 0,
    sampled_embed_nanos: u128 = 0,
    sampled_block_nanos: u128 = 0,
    sampled_tail_nanos: u128 = 0,
};

var timing_stats = TimingStats{};

pub fn resetTimingStats() void {
    timing_stats = .{};
}

pub fn getTimingStats() TimingStats {
    return timing_stats;
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

pub fn normSlot(layer: usize, kind: enum { attn, attn_sub, ffn, mlp_sub }) usize {
    return switch (kind) {
        .attn => layer * 4,
        .attn_sub => layer * 4 + 1,
        .ffn => layer * 4 + 2,
        .mlp_sub => layer * 4 + 3,
    };
}

pub fn linearSlot(layer: usize, kind: enum { attn_q, attn_k, attn_v, attn_out_proj, mlp_gate, mlp_up, mlp_down }) usize {
    return switch (kind) {
        .attn_q => layer * 7,
        .attn_k => layer * 7 + 1,
        .attn_v => layer * 7 + 2,
        .attn_out_proj => layer * 7 + 3,
        .mlp_gate => layer * 7 + 4,
        .mlp_up => layer * 7 + 5,
        .mlp_down => layer * 7 + 6,
    };
}

pub fn finalNormSlot(configured_layer_count: usize) usize {
    return configured_layer_count * 4;
}

pub fn finalLmHeadSlot(configured_layer_count: usize) usize {
    return configured_layer_count * 7;
}

const DirectHiddenResult = struct {
    hidden: ops.CT,
    total_rows: usize,
};

const ReservedHiddenCarrier = struct {
    front: ops.CT,
    back: ops.CT,
    active_front: bool = true,

    fn init(
        cb: *const ops.ComputeBackend,
        hidden_input: ops.CT,
        rows: usize,
        hidden_size: usize,
    ) !?ReservedHiddenCarrier {
        if (comptime !build_options.enable_metal) return null;
        const pair = (try metal_compute_mod.MetalCompute.reserveHiddenStatePair(cb, rows, hidden_size)) orelse return null;
        if (!(try metal_compute_mod.MetalCompute.copyTensorInto(cb, hidden_input, pair.front))) {
            cb.free(pair.front);
            cb.free(pair.back);
            return null;
        }
        return .{
            .front = pair.front,
            .back = pair.back,
        };
    }

    fn active(self: *const ReservedHiddenCarrier) ops.CT {
        return if (self.active_front) self.front else self.back;
    }

    fn inactive(self: *const ReservedHiddenCarrier) ops.CT {
        return if (self.active_front) self.back else self.front;
    }

    fn ownsSlot(self: *const ReservedHiddenCarrier, tensor: ops.CT) bool {
        return tensor == self.front or tensor == self.back;
    }

    fn replaceActive(self: *ReservedHiddenCarrier, cb: *const ops.ComputeBackend, next_hidden: ops.CT) !void {
        if (!(try metal_compute_mod.MetalCompute.copyTensorInto(cb, next_hidden, self.inactive()))) {
            return error.UnsupportedTensorType;
        }
        if (!self.ownsSlot(next_hidden)) cb.free(next_hidden);
        self.active_front = !self.active_front;
    }

    fn deinit(self: *ReservedHiddenCarrier, cb: *const ops.ComputeBackend, keep_active: bool) void {
        if (keep_active) {
            cb.free(self.inactive());
        } else {
            cb.free(self.front);
            cb.free(self.back);
        }
        self.* = undefined;
    }
};

test "ReservedHiddenCarrier does not free carrier slots during replacement" {
    const front: ops.CT = @ptrFromInt(0x1000);
    const back: ops.CT = @ptrFromInt(0x2000);
    const external: ops.CT = @ptrFromInt(0x3000);
    const carrier = ReservedHiddenCarrier{ .front = front, .back = back };

    try std.testing.expect(carrier.ownsSlot(front));
    try std.testing.expect(carrier.ownsSlot(back));
    try std.testing.expect(!carrier.ownsSlot(external));
}

pub fn preparedLayers(configured_layers: usize) usize {
    const value = c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_BITNET_LAYERS") orelse return configured_layers;
    const parsed = std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch return configured_layers;
    return @min(configured_layers, parsed);
}

pub fn overrideLevel() usize {
    const value = c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_BITNET_OVERRIDE_LEVEL") orelse return 4;
    return std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch 4;
}

pub fn supportsConfig(gpt_config: gpt_mod.Config) bool {
    return gpt_config.family == .bitnet and
        !gpt_config.usesMoe() and
        !gpt_config.hasPle() and
        !gpt_config.isMultimodal() and
        gpt_config.sliding_window == 0;
}

fn supportsDirectBitnetRuntime(gpt_config: gpt_mod.Config, configured_layer_count: usize, decode_context: *const gpt_arch.DecodeContext) bool {
    if (!supportsConfig(gpt_config)) return false;
    if (gpt_config.num_kv_shared_layers != 0) return false;
    if (preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers)) != gpt_config.num_hidden_layers) return false;
    return decode_context.attention_mode == .paged_decode or decode_context.attention_mode == .paged_prefill;
}

fn forwardFinalHiddenTensorDirect(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    hidden_input: ops.CT,
    _: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?DirectHiddenResult {
    if (!supportsDirectBitnetRuntime(gpt_config, configured_layer_count, decode_context)) return null;

    var reserved_hidden = try ReservedHiddenCarrier.init(
        cb,
        hidden_input,
        decode_context.query_sequence_len,
        gpt_config.hidden_size,
    );
    errdefer if (reserved_hidden) |*carrier| carrier.deinit(cb, false);

    var hidden = if (reserved_hidden) |*carrier| carrier.active() else hidden_input;
    var owns_hidden = false;
    errdefer if (owns_hidden) cb.free(hidden);

    const layer_count: usize = gpt_config.num_hidden_layers;
    for (0..layer_count) |layer| {
        const normed = (try cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = normSlot(layer, .attn),
            .input = hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
        })) orelse {
            if (owns_hidden) cb.free(hidden);
            return null;
        };
        errdefer cb.free(normed);

        var attention = gpt_arch.attentionContextFromDecode(decode_context);
        attention.layer_index = layer;
        attention.skip_kv_write = false;
        attention.sliding_window = 0;

        const block_out = (try cb.runGatedDecoderBlock(&.{
            .attention_input = normed,
            .residual = hidden,
            .attention = attention,
            .num_heads = gpt_config.num_attention_heads,
            .num_kv_heads = gpt_config.effectiveKVHeadsForLayer(layer),
            .head_dim = gpt_config.effectiveHeadDimForLayer(layer),
            .q_linear_slot = linearSlot(layer, .attn_q),
            .k_linear_slot = linearSlot(layer, .attn_k),
            .v_linear_slot = linearSlot(layer, .attn_v),
            .attention_linear_slot = linearSlot(layer, .attn_out_proj),
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .ffn_rms_norm_slot = normSlot(layer, .ffn),
            .ffn_post_gate_rms_norm_slot = normSlot(layer, .mlp_sub),
            .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
            .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
            .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
            .intermediate_size = gpt_config.intermediateSize(layer),
            .activation = .relu_squared,
            .graph_plan_tail_vocab_size = gpt_config.vocab_size,
        })) orelse {
            cb.free(normed);
            if (owns_hidden) cb.free(hidden);
            return null;
        };

        cb.free(normed);
        if (reserved_hidden) |*carrier| {
            try carrier.replaceActive(cb, block_out);
            hidden = carrier.active();
            owns_hidden = false;
        } else {
            if (owns_hidden) cb.free(hidden);
            hidden = block_out;
            owns_hidden = true;
        }
    }

    if (reserved_hidden) |*carrier| {
        carrier.deinit(cb, true);
        reserved_hidden = null;
    }
    return .{
        .hidden = hidden,
        .total_rows = decode_context.query_sequence_len,
    };
}

fn forwardFinalHiddenLastRowDirect(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    hidden_input: ops.CT,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?ops.CT {
    const hidden_result = (try forwardFinalHiddenTensorDirect(
        cb,
        gpt_config,
        configured_layer_count,
        hidden_input,
        seq_len,
        decode_context,
    )) orelse return null;
    errdefer cb.free(hidden_result.hidden);
    if (hidden_result.total_rows == 1) return hidden_result.hidden;
    const last_hidden = try cb.sliceRows2D(allocator, hidden_result.hidden, hidden_result.total_rows - 1, 1, gpt_config.hidden_size);
    cb.free(hidden_result.hidden);
    return last_hidden;
}

pub fn buildOverrides(gpt_config: gpt_mod.Config, configured_layer_count: usize) gpt_arch.Layer0DecoderOverrides {
    const prepared_layer_count = preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers));
    const configured_override_level = overrideLevel();
    var overrides = gpt_arch.Layer0DecoderOverrides{};
    for (0..prepared_layer_count) |layer| {
        if (configured_override_level >= 1) {
            overrides.attn_norm_slots[layer] = normSlot(layer, .attn);
        }
        if (configured_override_level >= 2) {
            overrides.attn_q_slots[layer] = linearSlot(layer, .attn_q);
            overrides.attn_k_slots[layer] = linearSlot(layer, .attn_k);
            overrides.attn_v_slots[layer] = linearSlot(layer, .attn_v);
            overrides.attn_sub_norm_slots[layer] = normSlot(layer, .attn_sub);
            overrides.attn_out_proj_linear_slots[layer] = linearSlot(layer, .attn_out_proj);
        }
        if (configured_override_level >= 3) {
            overrides.ffn_norm_slots[layer] = normSlot(layer, .ffn);
        }
        if (configured_override_level >= 4) {
            overrides.mlp_gate_slots[layer] = linearSlot(layer, .mlp_gate);
            overrides.mlp_up_slots[layer] = linearSlot(layer, .mlp_up);
            overrides.mlp_sub_norm_slots[layer] = normSlot(layer, .mlp_sub);
            overrides.mlp_down_slots[layer] = linearSlot(layer, .mlp_down);
        }
    }
    return overrides;
}

pub fn prepareDecodeRuntime(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    kv_tokens: usize,
    configured_layer_count: usize,
) !bool {
    if (!supportsConfig(gpt_config)) return false;
    timing_stats.prepare_calls += 1;
    var started_at = monotonicNowNs();
    if (!(try cb.decoderRuntimePrepareGreedy(&.{
        .hidden_size = gpt_config.hidden_size,
        .intermediate_size = gpt_config.intermediate_size,
        .num_layers = gpt_config.num_hidden_layers,
        .num_heads = gpt_config.num_attention_heads,
        .num_kv_heads = gpt_config.effectiveKVHeads(),
        .head_dim = gpt_config.headDim(),
        .vocab_size = gpt_config.vocab_size,
        .kv_tokens = kv_tokens,
    }))) return false;
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prepare_greedy_nanos += finished_at - started_at;

    const prepared_layer_count = preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers));
    for (0..prepared_layer_count) |layer| {
        var name_buf: [256]u8 = undefined;

        started_at = monotonicNowNs();
        const attn_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.input_layernorm.weight", .{layer});
        const attn_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, attn_norm_name);
        defer cb.free(attn_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .attn), attn_norm_w, gpt_config.hidden_size))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const attn_sub_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.attn_sub_norm.weight", .{layer});
        const attn_sub_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, attn_sub_norm_name);
        defer cb.free(attn_sub_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .attn_sub), attn_sub_norm_w, gpt_config.hidden_size))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const ffn_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer});
        const ffn_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, ffn_norm_name);
        defer cb.free(ffn_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .ffn), ffn_norm_w, gpt_config.hidden_size))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const mlp_sub_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.mlp.ffn_sub_norm.weight", .{layer});
        const mlp_sub_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, mlp_sub_norm_name);
        defer cb.free(mlp_sub_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .mlp_sub), mlp_sub_norm_w, gpt_config.intermediateSize(layer)))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const q_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.q_proj.weight", .{layer});
        const q_w = try gpt_arch.getModelWeight(cb, gpt_config, q_name);
        defer cb.free(q_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .attn_q), q_w, gpt_config.hidden_size, gpt_config.num_attention_heads * gpt_config.headDim()))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const k_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer});
        const k_w = try gpt_arch.getModelWeight(cb, gpt_config, k_name);
        defer cb.free(k_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .attn_k), k_w, gpt_config.hidden_size, gpt_config.effectiveKVHeads() * gpt_config.headDim()))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const v_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.v_proj.weight", .{layer});
        const v_w = try gpt_arch.getModelWeight(cb, gpt_config, v_name);
        defer cb.free(v_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .attn_v), v_w, gpt_config.hidden_size, gpt_config.effectiveKVHeads() * gpt_config.headDim()))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const attn_out_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer});
        const attn_out_w = try gpt_arch.getModelWeight(cb, gpt_config, attn_out_name);
        defer cb.free(attn_out_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .attn_out_proj), attn_out_w, gpt_config.hidden_size, gpt_config.hidden_size))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const gate_w = try gpt_arch.getFFNWeight(cb, gpt_config, layer, "gate", &name_buf);
        defer cb.free(gate_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .mlp_gate), gate_w, gpt_config.hidden_size, gpt_config.intermediateSize(layer)))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const up_w = try gpt_arch.getFFNWeight(cb, gpt_config, layer, "up", &name_buf);
        defer cb.free(up_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .mlp_up), up_w, gpt_config.hidden_size, gpt_config.intermediateSize(layer)))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const down_w = try gpt_arch.getFFNWeight(cb, gpt_config, layer, "down", &name_buf);
        defer cb.free(down_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, linearSlot(layer, .mlp_down), down_w, gpt_config.intermediateSize(layer), gpt_config.hidden_size))) return false;
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;
    }

    started_at = monotonicNowNs();
    const final_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, "model.norm.weight");
    defer cb.free(final_norm_w);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_lookup_nanos += finished_at - started_at;
    started_at = monotonicNowNs();
    if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, finalNormSlot(configured_layer_count), final_norm_w, gpt_config.hidden_size))) return false;
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_prep_nanos += finished_at - started_at;

    started_at = monotonicNowNs();
    const lm_head_w = try decoder_tail_runtime.getLmHeadWeight(cb, gpt_config);
    defer cb.free(lm_head_w);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_lookup_nanos += finished_at - started_at;
    started_at = monotonicNowNs();
    if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(
        cb,
        allocator,
        finalLmHeadSlot(configured_layer_count),
        lm_head_w,
        gpt_config.hidden_size,
        gpt_config.vocab_size,
    ))) return false;
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_prep_nanos += finished_at - started_at;

    return true;
}

pub fn forwardGreedyToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?i64 {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;
    timing_stats.greedy_calls += 1;

    var started_at = monotonicNowNs();
    const hidden = try decoder_rms_runtime.embedToken(cb, allocator, gpt_config, token_id);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_embed_nanos += finished_at - started_at;

    started_at = monotonicNowNs();
    const final_hidden = if (try forwardFinalHiddenLastRowDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
    )) |direct_hidden|
        direct_hidden
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        break :blk try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            null,
        );
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_block_nanos += finished_at - started_at;
    defer cb.free(final_hidden);

    started_at = monotonicNowNs();
    const token = if (try decoder_tail_runtime.forwardPreparedGreedyFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
    )) |prepared_token|
        prepared_token
    else
        try decoder_tail_runtime.forwardGreedyFromFinalHidden(
            cb,
            allocator,
            gpt_config,
            final_hidden,
            .rms,
            finalNormSlot(configured_layer_count),
        );
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_tail_nanos += finished_at - started_at;
    return token;
}

pub fn forwardLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    const logits = (try forwardLastLogitsTensor(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        token_id,
        seq_len,
        decode_context,
    )) orelse return null;
    const logits_host = try cb.toFloat32(logits, allocator);
    cb.free(logits);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    return logits_host;
}

pub fn forwardPrefillLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    if (!supportsConfig(gpt_config)) return null;
    if (input_ids.len == 0 or decode_context.query_sequence_len != input_ids.len) return null;
    if (decode_context.attention_mode != .paged_prefill) return null;
    timing_stats.prefill_calls += 1;
    if (prefillTraceEnabled()) std.debug.print("prefill-trace: bitnet forwardPrefillLastLogits start rows={d} seq={d}\n", .{ input_ids.len, seq_len });

    var started_at = monotonicNowNs();
    const hidden = try decoder_rms_runtime.embedTokens(cb, allocator, gpt_config, input_ids);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_embed_nanos += finished_at - started_at;

    started_at = monotonicNowNs();
    const final_hidden, const total_rows = if (try forwardFinalHiddenTensorDirect(
        cb,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
    )) |direct_hidden|
        .{ direct_hidden.hidden, direct_hidden.total_rows }
    else blk: {
        if (prefillTraceEnabled()) std.debug.print("prefill-trace: bitnet embedTokens ok\n", .{});
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        if (prefillTraceEnabled()) std.debug.print("prefill-trace: bitnet overrides built\n", .{});
        const fallback = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            null,
        );
        break :blk .{ fallback.hidden, fallback.total_rows };
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_block_nanos += finished_at - started_at;
    defer cb.free(final_hidden);
    const last_hidden = if (total_rows == 1)
        final_hidden
    else
        try cb.sliceRows2D(allocator, final_hidden, total_rows - 1, 1, gpt_config.hidden_size);
    defer if (last_hidden != final_hidden) cb.free(last_hidden);

    started_at = monotonicNowNs();
    const logits = if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        last_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
    )) |prepared_logits|
        prepared_logits
    else
        (try decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
            cb,
            gpt_config,
            last_hidden,
            .rms,
            finalNormSlot(configured_layer_count),
        )) orelse return null;
    defer cb.free(logits);
    const result = try cb.toFloat32(logits, allocator);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, result);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_tail_nanos += finished_at - started_at;
    return result;
}

pub fn forwardLastLogitsTensor(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?ops.CT {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode and decode_context.attention_mode != .paged_prefill) return null;

    const hidden = try decoder_rms_runtime.embedToken(cb, allocator, gpt_config, token_id);
    const final_hidden = if (try forwardFinalHiddenLastRowDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
    )) |direct_hidden|
        direct_hidden
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        break :blk try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            null,
        );
    };
    defer cb.free(final_hidden);

    if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
    )) |logits| {
        return logits;
    }
    return decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
    );
}

pub fn forwardSampledToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    sampling: model_runtime.SamplingConfig,
    token_history: []const i64,
) !?i64 {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;
    timing_stats.sampled_calls += 1;

    var started_at = monotonicNowNs();
    const hidden = try decoder_rms_runtime.embedToken(cb, allocator, gpt_config, token_id);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_embed_nanos += finished_at - started_at;

    started_at = monotonicNowNs();
    const final_hidden = if (try forwardFinalHiddenLastRowDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
    )) |direct_hidden|
        direct_hidden
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        break :blk try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            null,
        );
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_block_nanos += finished_at - started_at;
    defer cb.free(final_hidden);

    started_at = monotonicNowNs();
    const token = if (try decoder_tail_runtime.forwardPreparedSampledFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
        sampling,
        token_history,
    )) |prepared_token|
        prepared_token
    else
        try decoder_tail_runtime.forwardSampledFromFinalHidden(
            cb,
            allocator,
            gpt_config,
            final_hidden,
            .rms,
            finalNormSlot(configured_layer_count),
            sampling,
            token_history,
        );
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_tail_nanos += finished_at - started_at;
    return token;
}
