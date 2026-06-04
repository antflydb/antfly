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
const ops = @import("../ops/ops.zig");
const gpt_config = @import("../models/gpt.zig");
const deepseek_v4_host = @import("deepseek_v4_host.zig");

const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;

pub const Runtime = struct {
    ptr: *anyopaque,
    get_weight: *const fn (ptr: *anyopaque, name: []const u8) anyerror!CT,
    apply_activation: *const fn (ptr: *anyopaque, input: CT) anyerror!CT,
};

pub const SharedExpertRuntime = Runtime;

pub const AttentionPath = enum {
    sliding,
    compressed_sparse,
    heavily_compressed,
};

pub const SlidingWeights = struct {
    q_a: CT,
    q_a_norm: CT,
    q_b: CT,
    kv: CT,
    kv_norm: CT,
    o_a: CT,
    o_b: CT,
    sinks: CT,

    pub fn deinit(self: SlidingWeights, cb: *const ComputeBackend) void {
        cb.free(self.q_a);
        cb.free(self.q_a_norm);
        cb.free(self.q_b);
        cb.free(self.kv);
        cb.free(self.kv_norm);
        cb.free(self.o_a);
        cb.free(self.o_b);
        cb.free(self.sinks);
    }
};

pub const SlidingShape = struct {
    q_lora_rank: usize,
    o_lora_rank: usize,
    kv_cache_width: usize,
    kv_lora_rank: usize,
};

pub const SequenceContext = struct {
    query_sequence_len: ?usize = null,
    total_sequence_len: ?usize = null,
    mixed_batch: bool = false,

    fn queryLen(self: SequenceContext, seq_len: usize) usize {
        return self.query_sequence_len orelse seq_len;
    }

    fn positionOffset(self: SequenceContext, seq_len: usize, query_seq_len: usize) usize {
        if (self.total_sequence_len) |total| return total - query_seq_len;
        return seq_len - query_seq_len;
    }
};

pub const MoeContext = struct {
    input_ids: ?[]const i64 = null,
};

pub fn attentionPathForLayer(config: gpt_config.Config, layer: usize) !AttentionPath {
    return switch (config.deepseekV4AttentionKind(layer)) {
        .sliding_attention => .sliding,
        .compressed_sparse_attention => .compressed_sparse,
        .heavily_compressed_attention => .heavily_compressed,
        .unknown => error.UnsupportedDeepSeekV4AttentionSchedule,
    };
}

pub fn inferSchedulesFromGgufNames(config: *gpt_config.Config, all_names: []const []const u8) void {
    if (config.family != .deepseek_v4) return;

    const max_layers: usize = @min(@as(usize, @intCast(config.num_hidden_layers)), gpt_config.deepseek_v4_max_layers);
    if (max_layers == 0) return;

    var saw_layer = @as([gpt_config.deepseek_v4_max_layers]bool, @splat(false));
    var saw_compressor = @as([gpt_config.deepseek_v4_max_layers]bool, @splat(false));
    var saw_indexer = @as([gpt_config.deepseek_v4_max_layers]bool, @splat(false));
    var saw_mlp = @as([gpt_config.deepseek_v4_max_layers]bool, @splat(false));
    var saw_hash_gate = @as([gpt_config.deepseek_v4_max_layers]bool, @splat(false));
    var saw_router_bias = @as([gpt_config.deepseek_v4_max_layers]bool, @splat(false));
    var observed_layers: usize = 0;

    for (all_names) |name| {
        const parsed = parseGgufLayerTensorName(name) orelse continue;
        if (parsed.layer >= max_layers) continue;
        observed_layers = @max(observed_layers, parsed.layer + 1);
        saw_layer[parsed.layer] = true;

        if (isCompressorSuffix(parsed.suffix)) saw_compressor[parsed.layer] = true;
        if (std.mem.startsWith(u8, parsed.suffix, "indexer.")) saw_indexer[parsed.layer] = true;
        if (std.mem.startsWith(u8, parsed.suffix, "ffn_") or std.mem.eql(u8, parsed.suffix, "exp_probs_b")) saw_mlp[parsed.layer] = true;
        if (std.mem.eql(u8, parsed.suffix, "ffn_gate_tid2eid") or std.mem.eql(u8, parsed.suffix, "ffn_gate_inp.tid2eid")) saw_hash_gate[parsed.layer] = true;
        if (std.mem.eql(u8, parsed.suffix, "exp_probs_b") or std.mem.eql(u8, parsed.suffix, "ffn_gate_inp.bias") or std.mem.eql(u8, parsed.suffix, "ffn_gate_inp.e_score_correction_bias")) saw_router_bias[parsed.layer] = true;
    }

    if (observed_layers == 0) return;

    const attention_len = @max(@as(usize, @intCast(config.deepseek_v4_attention_schedule_len)), observed_layers);
    config.deepseek_v4_attention_schedule_len = @intCast(@min(attention_len, max_layers));
    for (0..@as(usize, @intCast(config.deepseek_v4_attention_schedule_len))) |layer| {
        if (config.deepseek_v4_attention_schedule[layer] != .unknown) continue;
        if (saw_indexer[layer]) {
            config.deepseek_v4_attention_schedule[layer] = .compressed_sparse_attention;
        } else if (saw_compressor[layer]) {
            config.deepseek_v4_attention_schedule[layer] = .heavily_compressed_attention;
        } else if (saw_layer[layer]) {
            config.deepseek_v4_attention_schedule[layer] = .sliding_attention;
        }
    }

    const mlp_len = @max(@as(usize, @intCast(config.deepseek_v4_mlp_schedule_len)), observed_layers);
    config.deepseek_v4_mlp_schedule_len = @intCast(@min(mlp_len, max_layers));
    for (0..@as(usize, @intCast(config.deepseek_v4_mlp_schedule_len))) |layer| {
        if (config.deepseek_v4_mlp_schedule[layer] != .unknown) continue;
        if (saw_hash_gate[layer]) {
            config.deepseek_v4_mlp_schedule[layer] = .hash_moe;
        } else if (saw_router_bias[layer] or saw_mlp[layer]) {
            config.deepseek_v4_mlp_schedule[layer] = .moe;
        }
    }

    recountSchedules(config);
}

const GgufLayerTensorName = struct {
    layer: usize,
    suffix: []const u8,
};

fn parseGgufLayerTensorName(name: []const u8) ?GgufLayerTensorName {
    const prefix = "blk.";
    if (!std.mem.startsWith(u8, name, prefix)) return null;
    const layer_end = std.mem.indexOfScalarPos(u8, name, prefix.len, '.') orelse return null;
    const layer = std.fmt.parseInt(usize, name[prefix.len..layer_end], 10) catch return null;
    if (layer_end + 1 >= name.len) return null;
    return .{
        .layer = layer,
        .suffix = name[layer_end + 1 ..],
    };
}

fn isCompressorSuffix(suffix: []const u8) bool {
    return std.mem.startsWith(u8, suffix, "attn_compress") or
        std.mem.startsWith(u8, suffix, "attn_compressor") or
        std.mem.startsWith(u8, suffix, "indexer.compress");
}

fn recountSchedules(config: *gpt_config.Config) void {
    config.deepseek_v4_sliding_attention_layers = 0;
    config.deepseek_v4_compressed_sparse_attention_layers = 0;
    config.deepseek_v4_heavily_compressed_attention_layers = 0;
    for (0..@as(usize, @intCast(config.deepseek_v4_attention_schedule_len))) |layer| {
        switch (config.deepseek_v4_attention_schedule[layer]) {
            .sliding_attention => config.deepseek_v4_sliding_attention_layers += 1,
            .compressed_sparse_attention => config.deepseek_v4_compressed_sparse_attention_layers += 1,
            .heavily_compressed_attention => config.deepseek_v4_heavily_compressed_attention_layers += 1,
            .unknown => {},
        }
    }

    config.deepseek_v4_hash_moe_layers = 0;
    config.deepseek_v4_moe_layers = 0;
    for (0..@as(usize, @intCast(config.deepseek_v4_mlp_schedule_len))) |layer| {
        switch (config.deepseek_v4_mlp_schedule[layer]) {
            .hash_moe => config.deepseek_v4_hash_moe_layers += 1,
            .moe => config.deepseek_v4_moe_layers += 1,
            .unknown => {},
        }
    }
}

pub fn normalizeGgufGlobalWeightKey(key: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, key, "hc_head.fn") or std.mem.eql(u8, key, "hc_head_fn.weight") or std.mem.eql(u8, key, "hc_head_fn")) return "model.hc_head.hc_fn";
    if (std.mem.eql(u8, key, "hc_head.base") or std.mem.eql(u8, key, "hc_head_base.weight") or std.mem.eql(u8, key, "hc_head_base")) return "model.hc_head.hc_base";
    if (std.mem.eql(u8, key, "hc_head.scale") or std.mem.eql(u8, key, "hc_head_scale.weight") or std.mem.eql(u8, key, "hc_head_scale")) return "model.hc_head.hc_scale";
    return null;
}

pub fn normalizeGgufWeightKey(layer: usize, suffix: []const u8, buf: *[256]u8) ?[]const u8 {
    if (std.mem.eql(u8, suffix, "attn_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.input_layernorm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_q_a.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.q_a_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_q_a_norm.weight") or std.mem.eql(u8, suffix, "attn_q_a_norm")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.q_a_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_q_b.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.q_b_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_kv_a_mqa.weight") or std.mem.eql(u8, suffix, "attn_kv_a.weight") or std.mem.eql(u8, suffix, "attn_kv.weight") or std.mem.eql(u8, suffix, "attn_kv_latent.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.kv_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_kv_a_norm.weight") or std.mem.eql(u8, suffix, "attn_kv_norm.weight") or std.mem.eql(u8, suffix, "attn_kv_norm")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.kv_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_o_a.weight") or std.mem.eql(u8, suffix, "attn_output_a.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_a_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_o_b.weight") or std.mem.eql(u8, suffix, "attn_output_b.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_b_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_sinks.weight") or std.mem.eql(u8, suffix, "attn_sinks")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.sinks", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_gate_inp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.gate.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_gate_inp.bias") or std.mem.eql(u8, suffix, "ffn_gate_inp.e_score_correction_bias") or std.mem.eql(u8, suffix, "exp_probs_b")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.gate.e_score_correction_bias", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_gate_inp.tid2eid") or std.mem.eql(u8, suffix, "ffn_gate_tid2eid")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.gate.tid2eid", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_gate_shexp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.shared_experts.gate_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_up_shexp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.shared_experts.up_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_down_shexp.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.shared_experts.down_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_hc_fn.weight") or std.mem.eql(u8, suffix, "attn_hc.fn") or std.mem.eql(u8, suffix, "hc_attn_fn")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.attn_hc.fn", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_hc_base.weight") or std.mem.eql(u8, suffix, "attn_hc.base") or std.mem.eql(u8, suffix, "hc_attn_base")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.attn_hc.base", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_hc_scale.weight") or std.mem.eql(u8, suffix, "attn_hc.scale") or std.mem.eql(u8, suffix, "hc_attn_scale")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.attn_hc.scale", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_hc_fn.weight") or std.mem.eql(u8, suffix, "ffn_hc.fn") or std.mem.eql(u8, suffix, "hc_ffn_fn")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.ffn_hc.fn", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_hc_base.weight") or std.mem.eql(u8, suffix, "ffn_hc.base") or std.mem.eql(u8, suffix, "hc_ffn_base")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.ffn_hc.base", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "ffn_hc_scale.weight") or std.mem.eql(u8, suffix, "ffn_hc.scale") or std.mem.eql(u8, suffix, "hc_ffn_scale")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.ffn_hc.scale", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_compressor_kv.weight") or std.mem.eql(u8, suffix, "attn_compress_kv.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.kv_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_compressor_gate.weight") or std.mem.eql(u8, suffix, "attn_compress_gate.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.gate_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_compressor_position_bias") or std.mem.eql(u8, suffix, "attn_compress_ape")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.position_bias", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "attn_compressor_kv_norm.weight") or std.mem.eql(u8, suffix, "attn_compress_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.kv_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "indexer.compress_kv.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.indexer.kv_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "indexer.compress_gate.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.indexer.gate_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "indexer.compress_ape")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.indexer.position_bias", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "indexer.compress_norm.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.indexer.kv_norm.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "indexer.attn_q_b.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.indexer.q_b_proj.weight", .{layer}) catch null;
    }
    if (std.mem.eql(u8, suffix, "indexer.proj.weight")) {
        return std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.compressor.indexer.weights_proj.weight", .{layer}) catch null;
    }
    return null;
}

pub fn moeProjectionName(proj: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, proj, "w1")) return "gate_proj";
    if (std.mem.eql(u8, proj, "w2")) return "down_proj";
    if (std.mem.eql(u8, proj, "w3")) return "up_proj";
    return null;
}

pub fn appendMissingRequiredWeights(
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
) !void {
    try appendMissingWeight(allocator, names, missing, "model.embed_tokens.weight");
    try appendMissingWeight(allocator, names, missing, "model.norm.weight");
    try appendMissingWeight(allocator, names, missing, "model.hc_head.hc_fn");
    try appendMissingWeight(allocator, names, missing, "model.hc_head.hc_base");
    try appendMissingWeight(allocator, names, missing, "model.hc_head.hc_scale");

    var buf: [256]u8 = undefined;
    for (0..config.num_hidden_layers) |layer| {
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.input_layernorm.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer});

        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.q_a_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.q_a_norm.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.q_b_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.kv_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.kv_norm.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.o_a_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.o_b_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.self_attn.sinks", .{layer});

        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn_hc.fn", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn_hc.base", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.attn_hc.scale", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.ffn_hc.fn", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.ffn_hc.base", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.ffn_hc.scale", .{layer});

        switch (config.deepseekV4AttentionKind(layer)) {
            .compressed_sparse_attention => {
                try appendMissingCompressorWeights(allocator, names, missing, &buf, layer);
                try appendMissingIndexerWeights(allocator, names, missing, &buf, layer);
            },
            .heavily_compressed_attention => try appendMissingCompressorWeights(allocator, names, missing, &buf, layer),
            .sliding_attention, .unknown => {},
        }

        switch (config.deepseekV4MlpKind(layer)) {
            .hash_moe => {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.gate.weight", .{layer});
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.gate.tid2eid", .{layer});
            },
            .moe, .unknown => {
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.gate.weight", .{layer});
                try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.gate.e_score_correction_bias", .{layer});
            },
        }
        try appendMissingExpertInputWeights(allocator, names, missing, &buf, layer);
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.experts.down_proj", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.shared_experts.gate_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.shared_experts.up_proj.weight", .{layer});
        try appendMissingFmt(allocator, names, missing, &buf, "model.layers.{d}.mlp.shared_experts.down_proj.weight", .{layer});
    }
}

fn appendMissingExpertInputWeights(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    buf: *[256]u8,
    layer: usize,
) !void {
    const fused = std.fmt.bufPrint(buf, "model.layers.{d}.mlp.experts.gate_up_proj", .{layer}) catch return error.NameTooLong;
    if (names.contains(fused)) return;

    var gate_buf: [256]u8 = undefined;
    var up_buf: [256]u8 = undefined;
    const gate = std.fmt.bufPrint(&gate_buf, "model.layers.{d}.mlp.experts.gate_proj", .{layer}) catch return error.NameTooLong;
    const up = std.fmt.bufPrint(&up_buf, "model.layers.{d}.mlp.experts.up_proj", .{layer}) catch return error.NameTooLong;
    if (names.contains(gate) and names.contains(up)) return;
    if (!names.contains(gate)) try appendMissingWeight(allocator, names, missing, gate);
    if (!names.contains(up)) try appendMissingWeight(allocator, names, missing, up);
}

fn appendMissingCompressorWeights(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    buf: *[256]u8,
    layer: usize,
) !void {
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.kv_proj.weight", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.gate_proj.weight", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.position_bias", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.kv_norm.weight", .{layer});
}

fn appendMissingIndexerWeights(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    buf: *[256]u8,
    layer: usize,
) !void {
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.indexer.kv_proj.weight", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.indexer.gate_proj.weight", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.indexer.position_bias", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.indexer.kv_norm.weight", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.indexer.q_b_proj.weight", .{layer});
    try appendMissingFmt(allocator, names, missing, buf, "model.layers.{d}.self_attn.compressor.indexer.weights_proj.weight", .{layer});
}

fn appendMissingFmt(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    buf: *[256]u8,
    comptime fmt: []const u8,
    args: anytype,
) !void {
    const name = std.fmt.bufPrint(buf, fmt, args) catch return error.NameTooLong;
    try appendMissingWeight(allocator, names, missing, name);
}

fn appendMissingWeight(
    allocator: std.mem.Allocator,
    names: *const std.StringHashMapUnmanaged(void),
    missing: *std.ArrayListUnmanaged([]const u8),
    name: []const u8,
) !void {
    if (names.contains(name)) return;
    try missing.append(allocator, try allocator.dupe(u8, name));
}

/// Host-side compressed attention/cache state for DeepSeek V4.
///
/// The storage is model-specific today because the tensor schedule, compressed
/// components, and cache invariants match the DeepSeek V4 attention layout. If
/// another architecture adopts the same compressed attention contract, this can
/// be promoted to a backend-neutral cache type without changing the GPT decoder
/// orchestration.
pub const CompressedCache = struct {
    allocator: std.mem.Allocator,
    layers: []Layer = &.{},

    pub const Component = struct {
        projected: []f32 = &.{},
        gate: []f32 = &.{},
        compressed: []f32 = &.{},
        positions: []u32 = &.{},
        row_dim: usize = 0,
        gate_width: usize = 0,

        pub fn deinit(self: *Component, allocator: std.mem.Allocator) void {
            allocator.free(self.projected);
            allocator.free(self.gate);
            allocator.free(self.compressed);
            allocator.free(self.positions);
            self.* = .{};
        }

        pub fn reset(self: *Component) void {
            @memset(self.projected, 0.0);
            @memset(self.gate, 0.0);
            @memset(self.compressed, 0.0);
            @memset(self.positions, 0);
        }

        pub fn ensureCapacity(
            self: *Component,
            allocator: std.mem.Allocator,
            token_capacity: usize,
            compressed_capacity: usize,
            row_dim: usize,
            gate_width: usize,
        ) !void {
            if (self.row_dim != 0 and self.row_dim != row_dim) return error.DeepSeekV4CompressedCacheShapeMismatch;
            if (self.gate_width != 0 and self.gate_width != gate_width) return error.DeepSeekV4CompressedCacheShapeMismatch;
            self.row_dim = row_dim;
            self.gate_width = gate_width;
            if (self.projected.len < token_capacity * row_dim) {
                const old_len = self.projected.len;
                self.projected = try allocator.realloc(self.projected, token_capacity * row_dim);
                @memset(self.projected[old_len..], 0.0);
            }
            if (self.gate.len < token_capacity * gate_width) {
                const old_len = self.gate.len;
                self.gate = try allocator.realloc(self.gate, token_capacity * gate_width);
                @memset(self.gate[old_len..], 0.0);
            }
            if (self.compressed.len < compressed_capacity * row_dim) {
                const old_len = self.compressed.len;
                self.compressed = try allocator.realloc(self.compressed, compressed_capacity * row_dim);
                @memset(self.compressed[old_len..], 0.0);
            }
            if (self.positions.len < compressed_capacity) {
                const old_len = self.positions.len;
                self.positions = try allocator.realloc(self.positions, compressed_capacity);
                @memset(self.positions[old_len..], 0);
            }
        }
    };

    pub const Layer = struct {
        local_kv: []f32 = &.{},
        local_dim: usize = 0,
        token_count: usize = 0,
        compressed_rows: usize = 0,
        index_rows: usize = 0,
        device_resident: bool = false,
        compressor: Component = .{},
        indexer: Component = .{},

        pub fn deinit(self: *Layer, allocator: std.mem.Allocator) void {
            allocator.free(self.local_kv);
            self.compressor.deinit(allocator);
            self.indexer.deinit(allocator);
            self.* = .{};
        }

        pub fn reset(self: *Layer) void {
            self.token_count = 0;
            self.compressed_rows = 0;
            self.index_rows = 0;
            self.device_resident = false;
            @memset(self.local_kv, 0.0);
            self.compressor.reset();
            self.indexer.reset();
        }

        pub fn ensureLocalCapacity(self: *Layer, allocator: std.mem.Allocator, token_capacity: usize, local_dim: usize) !void {
            if (self.local_dim != 0 and self.local_dim != local_dim) return error.DeepSeekV4CompressedCacheShapeMismatch;
            self.local_dim = local_dim;
            if (self.local_kv.len < token_capacity * local_dim) {
                const old_len = self.local_kv.len;
                self.local_kv = try allocator.realloc(self.local_kv, token_capacity * local_dim);
                @memset(self.local_kv[old_len..], 0.0);
            }
        }
    };

    pub fn init(allocator: std.mem.Allocator, layer_count: usize) !CompressedCache {
        const layers = try allocator.alloc(Layer, layer_count);
        @memset(layers, .{});
        return .{ .allocator = allocator, .layers = layers };
    }

    pub fn deinit(self: *CompressedCache) void {
        const allocator = self.allocator;
        for (self.layers) |*layer_state| layer_state.deinit(allocator);
        allocator.free(self.layers);
        self.* = .{ .allocator = allocator };
    }

    pub fn reset(self: *CompressedCache) void {
        for (self.layers) |*layer_state| layer_state.reset();
    }

    pub fn layer(self: *CompressedCache, layer_index: usize) !*Layer {
        if (layer_index >= self.layers.len) return error.InvalidLayerIndex;
        return &self.layers[layer_index];
    }

    pub fn rowsForTokens(token_count: usize, rate: usize) usize {
        if (token_count == 0 or rate == 0) return 0;
        return (token_count + rate - 1) / rate;
    }
};

fn layerWeight(runtime: Runtime, layer: usize, suffix: []const u8, buf: *[256]u8) !CT {
    const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    return runtime.get_weight(runtime.ptr, name);
}

pub fn slidingWeights(
    cb: *const ComputeBackend,
    runtime: Runtime,
    layer: usize,
    name_buf: *[256]u8,
) !SlidingWeights {
    const q_a = try layerWeight(runtime, layer, "q_a_proj.weight", name_buf);
    errdefer cb.free(q_a);
    const q_a_norm = try layerWeight(runtime, layer, "q_a_norm.weight", name_buf);
    errdefer cb.free(q_a_norm);
    const q_b = try layerWeight(runtime, layer, "q_b_proj.weight", name_buf);
    errdefer cb.free(q_b);
    const kv = try layerWeight(runtime, layer, "kv_proj.weight", name_buf);
    errdefer cb.free(kv);
    const kv_norm = try layerWeight(runtime, layer, "kv_norm.weight", name_buf);
    errdefer cb.free(kv_norm);
    const o_a = try layerWeight(runtime, layer, "o_a_proj.weight", name_buf);
    errdefer cb.free(o_a);
    const o_b = try layerWeight(runtime, layer, "o_b_proj.weight", name_buf);
    errdefer cb.free(o_b);
    const sinks = try layerWeight(runtime, layer, "sinks", name_buf);

    return .{
        .q_a = q_a,
        .q_a_norm = q_a_norm,
        .q_b = q_b,
        .kv = kv,
        .kv_norm = kv_norm,
        .o_a = o_a,
        .o_b = o_b,
        .sinks = sinks,
    };
}

pub fn validateSlidingWeightShapes(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    weights: SlidingWeights,
    num_kv_heads: u32,
    head_dim: u32,
) !SlidingShape {
    _ = num_kv_heads;
    if (config.deepseek_v4_q_lora_rank == 0 or config.deepseek_v4_qk_rope_head_dim == 0) {
        return error.MissingDeepSeekV4AttentionMetadata;
    }

    const hidden_size: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const q_lora_rank: usize = @intCast(config.deepseek_v4_q_lora_rank);
    const q_width: usize = num_heads * @as(usize, head_dim);
    const rope_width: usize = @intCast(config.deepseek_v4_qk_rope_head_dim);

    const q_a_shape = try tensorShape(cb, allocator, weights.q_a);
    defer allocator.free(q_a_shape);
    try expectRank(q_a_shape, 2);
    try expectDim(q_a_shape, 0, q_lora_rank);
    try expectDim(q_a_shape, 1, hidden_size);

    const q_a_norm_shape = try tensorShape(cb, allocator, weights.q_a_norm);
    defer allocator.free(q_a_norm_shape);
    try expectRank(q_a_norm_shape, 1);
    try expectDim(q_a_norm_shape, 0, q_lora_rank);

    const q_b_shape = try tensorShape(cb, allocator, weights.q_b);
    defer allocator.free(q_b_shape);
    try expectRank(q_b_shape, 2);
    try expectDim(q_b_shape, 0, q_width);
    try expectDim(q_b_shape, 1, q_lora_rank);

    const kv_shape = try tensorShape(cb, allocator, weights.kv);
    defer allocator.free(kv_shape);
    try expectRank(kv_shape, 2);
    try expectDim(kv_shape, 1, hidden_size);
    const kv_cache_width = try positiveDim(kv_shape, 0);
    if (kv_cache_width <= rope_width) return error.DeepSeekV4TensorShapeMismatch;
    const kv_lora_rank = kv_cache_width - rope_width;

    const kv_norm_shape = try tensorShape(cb, allocator, weights.kv_norm);
    defer allocator.free(kv_norm_shape);
    try expectRank(kv_norm_shape, 1);
    try expectDim(kv_norm_shape, 0, kv_cache_width);

    const o_a_shape = try tensorShape(cb, allocator, weights.o_a);
    defer allocator.free(o_a_shape);
    const o_groups: usize = @intCast(if (config.deepseek_v4_o_groups > 0) config.deepseek_v4_o_groups else 1);
    if (o_groups == 0 or num_heads % o_groups != 0) return error.DeepSeekV4TensorShapeMismatch;
    const o_group_width = (num_heads / o_groups) * @as(usize, head_dim);
    const o_lora_rank = try groupedOutputRank(o_a_shape, o_groups, o_group_width, q_width);
    if (config.deepseek_v4_o_lora_rank > 0 and o_lora_rank != @as(usize, @intCast(config.deepseek_v4_o_lora_rank))) return error.DeepSeekV4TensorShapeMismatch;

    const o_b_shape = try tensorShape(cb, allocator, weights.o_b);
    defer allocator.free(o_b_shape);
    try expectRank(o_b_shape, 2);
    try expectDim(o_b_shape, 0, hidden_size);
    try expectDim(o_b_shape, 1, o_groups * o_lora_rank);

    const sinks_shape = try tensorShape(cb, allocator, weights.sinks);
    defer allocator.free(sinks_shape);
    try expectRank(sinks_shape, 1);
    try expectDim(sinks_shape, 0, num_heads);

    return .{
        .q_lora_rank = q_lora_rank,
        .o_lora_rank = o_lora_rank,
        .kv_cache_width = kv_cache_width,
        .kv_lora_rank = kv_lora_rank,
    };
}

pub fn tensorShape(cb: *const ComputeBackend, allocator: std.mem.Allocator, tensor: CT) ![]i64 {
    return cb.tensorShape(tensor, allocator) catch |err| switch (err) {
        error.UnsupportedShape, error.UnsupportedTensorType => error.DeepSeekV4RequiresTensorShapeBackendFeature,
        else => err,
    };
}

pub fn expectRank(shape: []const i64, rank: usize) !void {
    if (shape.len != rank) return error.DeepSeekV4TensorShapeMismatch;
}

pub fn expectDim(shape: []const i64, axis: usize, expected: usize) !void {
    if (axis >= shape.len) return error.DeepSeekV4TensorShapeMismatch;
    if (shape[axis] < 0) return error.DeepSeekV4TensorShapeMismatch;
    if (@as(usize, @intCast(shape[axis])) != expected) return error.DeepSeekV4TensorShapeMismatch;
}

pub fn positiveDim(shape: []const i64, axis: usize) !usize {
    if (axis >= shape.len or shape[axis] <= 0) return error.DeepSeekV4TensorShapeMismatch;
    return @intCast(shape[axis]);
}

pub fn expectGateWidth(gate_width: usize, compress_rate: usize) !void {
    if (gate_width == 1 or gate_width == compress_rate) return;
    return error.DeepSeekV4TensorShapeMismatch;
}

pub fn groupedOutputRank(shape: []const i64, groups: usize, group_width: usize, q_width: usize) !usize {
    switch (shape.len) {
        2 => {
            const first = try positiveDim(shape, 0);
            const second = try positiveDim(shape, 1);
            if (second == group_width and first % groups == 0) return first / groups;
            if (groups == 1 and second == q_width) return first;
            return error.DeepSeekV4TensorShapeMismatch;
        },
        3 => {
            try expectDim(shape, 0, groups);
            try expectDim(shape, 2, group_width);
            return positiveDim(shape, 1);
        },
        else => return error.DeepSeekV4TensorShapeMismatch,
    }
}

pub fn unweightedRmsHeads(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: CT,
    total: usize,
    num_heads: usize,
    head_dim: u32,
    eps: f32,
) !CT {
    const head_width: usize = @intCast(head_dim);
    const data = try cb.toFloat32(tensor, allocator);
    defer allocator.free(data);
    if (data.len != total * num_heads * head_width) return error.DeepSeekV4TensorShapeMismatch;
    deepseek_v4_host.unweightedRmsRows(data, head_width, eps);
    const shape = [_]i32{ @intCast(total), @intCast(num_heads * head_width) };
    return cb.fromFloat32Shape(data, &shape);
}

pub fn applyTrailingRopeTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: CT,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: u32,
    rope_dim: usize,
    theta: f32,
    sequence_context: SequenceContext,
    inverse: bool,
) !CT {
    if (sequence_context.mixed_batch) return error.UnsupportedDeepSeekV4MixedBatchRope;
    const query_seq_len = sequence_context.queryLen(seq_len);
    const total = batch * query_seq_len;
    const width = num_heads * @as(usize, @intCast(head_dim));
    const data = try cb.toFloat32(tensor, allocator);
    defer allocator.free(data);
    if (data.len != total * width) return error.DeepSeekV4TensorShapeMismatch;

    const positions = try allocator.alloc(u32, total);
    defer allocator.free(positions);
    const offset = sequence_context.positionOffset(seq_len, query_seq_len);
    for (positions, 0..) |*position, row| {
        position.* = @intCast(offset + (row % query_seq_len));
    }
    if (inverse)
        deepseek_v4_host.applyInverseTrailingRopeRows(data, positions, num_heads, @intCast(head_dim), rope_dim, theta)
    else
        deepseek_v4_host.applyTrailingRopeRows(data, positions, num_heads, @intCast(head_dim), rope_dim, theta);
    const shape = [_]i32{ @intCast(total), @intCast(width) };
    return cb.fromFloat32Shape(data, &shape);
}

pub fn groupedOutputProject(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    o_a: CT,
    o_b: CT,
    total: usize,
    num_heads: usize,
    head_dim: u32,
    o_lora_rank: usize,
    hidden_size: usize,
    configured_groups: u32,
) !CT {
    const groups: usize = @intCast(if (configured_groups > 0) configured_groups else 1);
    if (groups == 0 or num_heads % groups != 0) return error.DeepSeekV4TensorShapeMismatch;
    const head_width: usize = @intCast(head_dim);
    const group_width = (num_heads / groups) * head_width;
    const q_width = num_heads * head_width;
    const inter_width = groups * o_lora_rank;

    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    if (input_data.len != total * q_width) return error.DeepSeekV4TensorShapeMismatch;
    const o_a_data = try cb.toFloat32(o_a, allocator);
    defer allocator.free(o_a_data);
    if (o_a_data.len != inter_width * group_width) return error.DeepSeekV4TensorShapeMismatch;

    const inter = try allocator.alloc(f32, total * inter_width);
    defer allocator.free(inter);
    deepseek_v4_host.groupedOutputProjectionRows(inter, input_data, o_a_data, .{
        .rows = total,
        .groups = groups,
        .group_in_dim = group_width,
        .out_dim = o_lora_rank,
    });
    const inter_shape = [_]i32{ @intCast(total), @intCast(inter_width) };
    const inter_ct = try cb.fromFloat32Shape(inter, &inter_shape);
    defer cb.free(inter_ct);
    return cb.linearNoBias(inter_ct, o_b, total, inter_width, hidden_size);
}

pub fn addSharedExpert(
    cb: *const ComputeBackend,
    config: gpt_config.Config,
    runtime: Runtime,
    input: CT,
    routed: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
) !CT {
    if (!config.hasSharedExpert()) return routed;
    const hidden_size: usize = @intCast(config.hidden_size);
    const inter_size: usize = @intCast(if (config.shared_expert_intermediate_size > 0) config.shared_expert_intermediate_size else config.expertIntermediateSize());

    const gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.mlp.shared_experts.gate_proj.weight", .{layer}) catch return error.NameTooLong;
    const gate_w = try runtime.get_weight(runtime.ptr, gate_name);
    defer cb.free(gate_w);
    const gate_proj = try cb.linearNoBias(input, gate_w, total, hidden_size, inter_size);
    defer cb.free(gate_proj);
    const gate_act = try runtime.apply_activation(runtime.ptr, gate_proj);
    defer cb.free(gate_act);

    const up_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.mlp.shared_experts.up_proj.weight", .{layer}) catch return error.NameTooLong;
    const up_w = try runtime.get_weight(runtime.ptr, up_name);
    defer cb.free(up_w);
    const up_proj = try cb.linearNoBias(input, up_w, total, hidden_size, inter_size);
    defer cb.free(up_proj);

    const gated = try cb.multiply(gate_act, up_proj);
    defer cb.free(gated);

    const down_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.mlp.shared_experts.down_proj.weight", .{layer}) catch return error.NameTooLong;
    const down_w = try runtime.get_weight(runtime.ptr, down_name);
    defer cb.free(down_w);
    const shared = try cb.linearNoBias(gated, down_w, total, inter_size, hidden_size);
    defer cb.free(shared);

    const result = try cb.add(routed, shared);
    cb.free(routed);
    return result;
}

pub fn feedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
    moe_context: MoeContext,
) !CT {
    return switch (config.deepseekV4MlpKind(layer)) {
        .hash_moe, .moe => moeFeedForward(cb, allocator, config, runtime, input, total, layer, name_buf, moe_context),
        .unknown => error.UnsupportedDeepSeekV4MlpSchedule,
    };
}

fn moeFeedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
    moe_context: MoeContext,
) !CT {
    const hidden_size: usize = @intCast(config.hidden_size);
    const inter_size: usize = @intCast(config.expertIntermediateSize());
    const num_experts: usize = @intCast(config.num_local_experts);
    const top_k: usize = @min(@as(usize, @intCast(config.num_experts_per_tok)), num_experts);
    if (num_experts == 0 or top_k == 0) return error.InvalidMoeConfig;

    const gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.mlp.gate.weight", .{layer}) catch return error.NameTooLong;
    const gate_w = try runtime.get_weight(runtime.ptr, gate_name);
    defer cb.free(gate_w);
    const router_logits_ct = try cb.linearNoBias(input, gate_w, total, hidden_size, num_experts);
    defer cb.free(router_logits_ct);

    const input_data = try cb.toFloat32(input, allocator);
    defer allocator.free(input_data);
    const router_logits = try cb.toFloat32(router_logits_ct, allocator);
    defer allocator.free(router_logits);

    const correction_bias = try optionalLayerWeightData(cb, allocator, runtime, layer, "mlp.gate.e_score_correction_bias", name_buf);
    defer if (correction_bias) |bias| allocator.free(bias);
    const tid2eid = try optionalLayerWeightData(cb, allocator, runtime, layer, "mlp.gate.tid2eid", name_buf);
    defer if (tid2eid) |ids| allocator.free(ids);

    const output = try allocator.alloc(f32, total * hidden_size);
    defer allocator.free(output);
    @memset(output, 0.0);
    const expert_gate_up = try allocator.alloc(f32, 2 * inter_size);
    defer allocator.free(expert_gate_up);
    const route_indices = try allocator.alloc(u32, top_k);
    defer allocator.free(route_indices);
    const route_weights = try allocator.alloc(f32, top_k);
    defer allocator.free(route_weights);
    const input_row_shape = [_]i32{ 1, @intCast(hidden_size) };

    for (0..total) |row| {
        const logits = router_logits[row * num_experts ..][0..num_experts];
        const route_count = try selectRoutesForRow(
            allocator,
            config,
            logits,
            correction_bias,
            tid2eid,
            moe_context,
            row,
            top_k,
            route_indices,
            route_weights,
        );
        {
            const input_row = input_data[row * hidden_size ..][0..hidden_size];
            const input_row_ct = try cb.fromFloat32Shape(input_row, &input_row_shape);
            defer cb.free(input_row_ct);
            const out_row = output[row * hidden_size ..][0..hidden_size];
            for (0..route_count) |route_i| {
                const expert_id: usize = @intCast(route_indices[route_i]);
                if (expert_id >= num_experts) return error.DeepSeekV4RouteOutOfRange;
                try applyLazyMoeExpert(
                    cb,
                    allocator,
                    config,
                    runtime,
                    input_row_ct,
                    out_row,
                    expert_gate_up,
                    layer,
                    expert_id,
                    route_weights[route_i],
                    hidden_size,
                    inter_size,
                );
            }
        }
    }

    const routed_shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    const routed = try cb.fromFloat32Shape(output, &routed_shape);
    errdefer cb.free(routed);
    return addSharedExpert(cb, config, runtime, input, routed, total, layer, name_buf);
}

fn applyLazyMoeExpert(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    input_row: CT,
    output_row: []f32,
    expert_gate_up: []f32,
    layer: usize,
    expert_id: usize,
    route_weight: f32,
    hidden_size: usize,
    inter_size: usize,
) !void {
    if (expert_gate_up.len != 2 * inter_size or output_row.len != hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    try projectLazyExpertGateUp(cb, allocator, runtime, input_row, expert_gate_up, layer, expert_id, hidden_size, inter_size);
    deepseek_v4_host.applyClampedSwiGLU(expert_gate_up[0 .. 2 * inter_size], config.deepseek_v4_swiglu_limit);

    const gated_shape = [_]i32{ 1, @intCast(inter_size) };
    const gated = try cb.fromFloat32Shape(expert_gate_up[0..inter_size], &gated_shape);
    defer cb.free(gated);

    var down_name_buf: [256]u8 = undefined;
    const down_name = expertWeightName(&down_name_buf, layer, expert_id, "down_proj") catch return error.NameTooLong;
    const down_w = try runtime.get_weight(runtime.ptr, down_name);
    defer cb.free(down_w);

    const down_ct = try cb.linearNoBias(gated, down_w, 1, inter_size, hidden_size);
    defer cb.free(down_ct);
    const down_data = try cb.toFloat32(down_ct, allocator);
    defer allocator.free(down_data);
    if (down_data.len != hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    for (0..hidden_size) |d| output_row[d] += route_weight * down_data[d];
}

fn projectLazyExpertGateUp(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    runtime: Runtime,
    input_row: CT,
    expert_gate_up: []f32,
    layer: usize,
    expert_id: usize,
    hidden_size: usize,
    inter_size: usize,
) !void {
    var fused_name_buf: [256]u8 = undefined;
    const fused_name = expertWeightName(&fused_name_buf, layer, expert_id, "gate_up_proj") catch return error.NameTooLong;
    if (try optionalRuntimeWeight(runtime, fused_name)) |gate_up_w| {
        defer cb.free(gate_up_w);
        const gate_up_ct = try cb.linearNoBias(input_row, gate_up_w, 1, hidden_size, 2 * inter_size);
        defer cb.free(gate_up_ct);
        const gate_up_data = try cb.toFloat32(gate_up_ct, allocator);
        defer allocator.free(gate_up_data);
        if (gate_up_data.len != 2 * inter_size) return error.DeepSeekV4TensorShapeMismatch;
        @memcpy(expert_gate_up[0 .. 2 * inter_size], gate_up_data);
        return;
    }

    var gate_name_buf: [256]u8 = undefined;
    var up_name_buf: [256]u8 = undefined;
    const gate_name = expertWeightName(&gate_name_buf, layer, expert_id, "gate_proj") catch return error.NameTooLong;
    const up_name = expertWeightName(&up_name_buf, layer, expert_id, "up_proj") catch return error.NameTooLong;
    const gate_w = try runtime.get_weight(runtime.ptr, gate_name);
    defer cb.free(gate_w);
    const up_w = try runtime.get_weight(runtime.ptr, up_name);
    defer cb.free(up_w);

    const gate_up = try cb.linearNoBiasPair(input_row, gate_w, up_w, 1, hidden_size, inter_size);
    defer cb.free(gate_up.first);
    defer cb.free(gate_up.second);
    const gate_data = try cb.toFloat32(gate_up.first, allocator);
    defer allocator.free(gate_data);
    const up_data = try cb.toFloat32(gate_up.second, allocator);
    defer allocator.free(up_data);
    if (gate_data.len != inter_size or up_data.len != inter_size) return error.DeepSeekV4TensorShapeMismatch;
    @memcpy(expert_gate_up[0..inter_size], gate_data);
    @memcpy(expert_gate_up[inter_size .. 2 * inter_size], up_data);
}

fn optionalRuntimeWeight(runtime: Runtime, name: []const u8) !?CT {
    return runtime.get_weight(runtime.ptr, name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound, error.TensorNotFound => return null,
        else => return err,
    };
}

fn expertWeightName(buf: *[256]u8, layer: usize, expert_id: usize, projection: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "model.layers.{d}.mlp.experts.{d}.{s}", .{ layer, expert_id, projection }) catch return error.NameTooLong;
}

fn selectRoutesForRow(
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    logits: []const f32,
    correction_bias: ?[]const f32,
    tid2eid: ?[]const f32,
    moe_context: MoeContext,
    row: usize,
    top_k: usize,
    route_indices: []u32,
    route_weights: []f32,
) !usize {
    if (tid2eid) |table| {
        const ids = moe_context.input_ids orelse return error.DeepSeekV4HashMoeRequiresInputIds;
        if (row >= ids.len) return error.DeepSeekV4HashMoeRequiresInputIds;
        const token_id = ids[row];
        if (token_id < 0) return error.DeepSeekV4RouteOutOfRange;
        const base: usize = @as(usize, @intCast(token_id)) * top_k;
        if (base + top_k > table.len) return error.DeepSeekV4RouteOutOfRange;
        var sum: f32 = 0.0;
        for (0..top_k) |i| {
            const expert_id_f = table[base + i];
            if (expert_id_f < 0.0) return error.DeepSeekV4RouteOutOfRange;
            const expert_id: usize = @intFromFloat(expert_id_f);
            if (expert_id >= logits.len) return error.DeepSeekV4RouteOutOfRange;
            route_indices[i] = @intCast(expert_id);
            route_weights[i] = deepseek_v4_host.scoreValue(config.deepseek_v4_scoring_func, logits[expert_id]);
            sum += route_weights[i];
        }
        if (config.deepseek_v4_norm_topk_prob and sum > 0.0) {
            for (route_weights[0..top_k]) |*weight| weight.* /= sum;
        }
        for (route_weights[0..top_k]) |*weight| weight.* *= config.deepseek_v4_routed_scaling_factor;
        return top_k;
    }

    const routes = try deepseek_v4_host.selectTopKRoutes(
        allocator,
        logits,
        correction_bias,
        top_k,
        config.deepseek_v4_scoring_func,
        config.deepseek_v4_norm_topk_prob,
        config.deepseek_v4_routed_scaling_factor,
    );
    defer routes.deinit(allocator);
    @memcpy(route_indices[0..routes.indices.len], routes.indices);
    @memcpy(route_weights[0..routes.weights.len], routes.weights);
    return routes.indices.len;
}

fn optionalLayerWeightData(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    runtime: Runtime,
    layer: usize,
    suffix: []const u8,
    name_buf: *[256]u8,
) !?[]f32 {
    const name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
    const weight = runtime.get_weight(runtime.ptr, name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => return null,
        else => return err,
    };
    defer cb.free(weight);
    return try cb.toFloat32(weight, allocator);
}

pub fn initHyperStreams(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    hidden: CT,
    total: usize,
) ![]f32 {
    const hc_mult: usize = @intCast(config.deepseek_v4_hc_mult);
    const hidden_size: usize = @intCast(config.hidden_size);
    if (hc_mult == 0) return error.MissingDeepSeekV4AttentionMetadata;
    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    if (hidden_data.len != total * hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    const streams = try allocator.alloc(f32, total * hc_mult * hidden_size);
    for (0..total) |row| {
        const src = hidden_data[row * hidden_size ..][0..hidden_size];
        for (0..hc_mult) |stream| {
            const dst = streams[(row * hc_mult + stream) * hidden_size ..][0..hidden_size];
            @memcpy(dst, src);
        }
    }
    return streams;
}

pub fn hyperPreReduceStreams(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    streams: []f32,
    total: usize,
    layer: usize,
    prefix: []const u8,
    name_buf: *[256]u8,
) !CT {
    const hidden_size: usize = @intCast(config.hidden_size);
    const out = try allocator.alloc(f32, total * hidden_size);
    defer allocator.free(out);
    try withLayerHyperWeights(cb, allocator, config, runtime, layer, prefix, name_buf, streams, null, out, .pre);
    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    return cb.fromFloat32Shape(out, &shape);
}

pub fn hyperPostUpdateStreams(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    streams: []f32,
    update: CT,
    total: usize,
    layer: usize,
    prefix: []const u8,
    name_buf: *[256]u8,
) !void {
    const hidden_size: usize = @intCast(config.hidden_size);
    const update_data = try cb.toFloat32(update, allocator);
    defer allocator.free(update_data);
    if (update_data.len != total * hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    try withLayerHyperWeights(cb, allocator, config, runtime, layer, prefix, name_buf, streams, update_data, null, .post);
}

const HyperPhase = enum { pre, post };

fn withLayerHyperWeights(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    layer: usize,
    prefix: []const u8,
    name_buf: *[256]u8,
    streams: []f32,
    update: ?[]const f32,
    pre_out: ?[]f32,
    phase: HyperPhase,
) !void {
    const fn_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}.fn", .{ layer, prefix }) catch return error.NameTooLong;
    const fn_w = try runtime.get_weight(runtime.ptr, fn_name);
    defer cb.free(fn_w);
    const base_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}.base", .{ layer, prefix }) catch return error.NameTooLong;
    const base_w = try runtime.get_weight(runtime.ptr, base_name);
    defer cb.free(base_w);
    const scale_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}.scale", .{ layer, prefix }) catch return error.NameTooLong;
    const scale_w = try runtime.get_weight(runtime.ptr, scale_name);
    defer cb.free(scale_w);

    const fn_data = try cb.toFloat32(fn_w, allocator);
    defer allocator.free(fn_data);
    const base_data = try cb.toFloat32(base_w, allocator);
    defer allocator.free(base_data);
    const scale_data = try cb.toFloat32(scale_w, allocator);
    defer allocator.free(scale_data);

    const hc_mult: usize = @intCast(config.deepseek_v4_hc_mult);
    const hidden_size: usize = @intCast(config.hidden_size);
    const flat_dim = hc_mult * hidden_size;
    const conn_mix = (2 + hc_mult) * hc_mult;
    if (streams.len % flat_dim != 0) return error.DeepSeekV4TensorShapeMismatch;
    const total = streams.len / flat_dim;
    if (fn_data.len == conn_mix * flat_dim) {
        if (base_data.len != conn_mix or scale_data.len < 3) return error.DeepSeekV4TensorShapeMismatch;
        return applyPersistentHyperRows(config, streams, update, pre_out, fn_data, base_data, scale_data, total, hc_mult, hidden_size, phase);
    }

    if (fn_data.len != hc_mult * hc_mult * hidden_size or base_data.len != hc_mult or scale_data.len == 0) return error.DeepSeekV4TensorShapeMismatch;
    const shape = deepseek_v4_host.HyperStreamsShape{ .rows = total, .hc_mult = hc_mult, .hidden_dim = hidden_size };
    const weights = try allocator.alloc(f32, total * hc_mult);
    defer allocator.free(weights);
    deepseek_v4_host.hyperHeadWeightsRows(weights, streams, fn_data, base_data, scale_data, shape, config.deepseek_v4_hc_eps, config.norm_eps);
    switch (phase) {
        .pre => deepseek_v4_host.weightedStreamReduceRows(pre_out orelse return error.DeepSeekV4TensorShapeMismatch, streams, weights, shape),
        .post => deepseek_v4_host.broadcastAddStreamsRows(streams, update orelse return error.DeepSeekV4TensorShapeMismatch, weights, shape),
    }
}

fn applyPersistentHyperRows(
    config: gpt_config.Config,
    streams: []f32,
    update: ?[]const f32,
    pre_out: ?[]f32,
    fn_data: []const f32,
    base_data: []const f32,
    scale_data: []const f32,
    total: usize,
    hc_mult: usize,
    hidden_size: usize,
    phase: HyperPhase,
) !void {
    const flat_dim = hc_mult * hidden_size;
    if (streams.len != total * flat_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (phase == .post and (update == null or update.?.len != total * hidden_size)) return error.DeepSeekV4TensorShapeMismatch;
    if (phase == .pre and (pre_out == null or pre_out.?.len != total * hidden_size)) return error.DeepSeekV4TensorShapeMismatch;

    if (phase == .post) {
        const shape = deepseek_v4_host.HyperStreamsShape{ .rows = total, .hc_mult = hc_mult, .hidden_dim = hidden_size };
        const scratch_next_row = try std.heap.page_allocator.alloc(f32, flat_dim);
        defer std.heap.page_allocator.free(scratch_next_row);
        const scratch_comb = try std.heap.page_allocator.alloc(f32, hc_mult * hc_mult);
        defer std.heap.page_allocator.free(scratch_comb);
        const scratch_pre_weights = try std.heap.page_allocator.alloc(f32, hc_mult);
        defer std.heap.page_allocator.free(scratch_pre_weights);
        const scratch_post_weights = try std.heap.page_allocator.alloc(f32, hc_mult);
        defer std.heap.page_allocator.free(scratch_post_weights);
        deepseek_v4_host.hyperConnectionUpdateRows(
            streams,
            update.?,
            scratch_next_row,
            scratch_comb,
            scratch_pre_weights,
            scratch_post_weights,
            fn_data,
            base_data,
            scale_data,
            shape,
            config.deepseek_v4_hc_eps,
            config.norm_eps,
            config.deepseek_v4_hc_sinkhorn_iters,
        );
        return;
    }

    const comb = try std.heap.page_allocator.alloc(f32, hc_mult * hc_mult);
    defer std.heap.page_allocator.free(comb);
    const pre_weights = try std.heap.page_allocator.alloc(f32, hc_mult);
    defer std.heap.page_allocator.free(pre_weights);
    const post_weights = try std.heap.page_allocator.alloc(f32, hc_mult);
    defer std.heap.page_allocator.free(post_weights);

    for (0..total) |row| {
        const stream_row = streams[row * flat_dim ..][0..flat_dim];
        var sum_sq: f32 = 0.0;
        for (stream_row) |value| sum_sq += value * value;
        const rsqrt = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(flat_dim)) + config.norm_eps);

        for (0..hc_mult) |dst_stream| {
            var pre_logit: f32 = 0.0;
            var post_logit: f32 = 0.0;
            const pre_weight_base = dst_stream * flat_dim;
            const post_weight_base = (hc_mult + dst_stream) * flat_dim;
            for (0..flat_dim) |col| {
                pre_logit += stream_row[col] * fn_data[pre_weight_base + col];
                post_logit += stream_row[col] * fn_data[post_weight_base + col];
            }
            pre_weights[dst_stream] = deepseek_v4_host.sigmoid(pre_logit * rsqrt * scale_data[0] + base_data[dst_stream]) + config.deepseek_v4_hc_eps;
            post_weights[dst_stream] = deepseek_v4_host.sigmoid(post_logit * rsqrt * scale_data[1] + base_data[hc_mult + dst_stream]) + config.deepseek_v4_hc_eps;

            for (0..hc_mult) |src_stream| {
                var comb_logit: f32 = 0.0;
                const comb_index = dst_stream * hc_mult + src_stream;
                const comb_weight_base = (2 * hc_mult + comb_index) * flat_dim;
                for (0..flat_dim) |col| {
                    comb_logit += stream_row[col] * fn_data[comb_weight_base + col];
                }
                comb[comb_index] = deepseek_v4_host.sigmoid(comb_logit * rsqrt * scale_data[2] + base_data[2 * hc_mult + comb_index]) + config.deepseek_v4_hc_eps;
            }
        }
        deepseek_v4_host.sinkhornInPlace(comb, hc_mult, config.deepseek_v4_hc_sinkhorn_iters, config.deepseek_v4_hc_eps);

        switch (phase) {
            .pre => {
                const out_row = pre_out.?[row * hidden_size ..][0..hidden_size];
                @memset(out_row, 0.0);
                for (0..hc_mult) |dst_stream| {
                    for (0..hc_mult) |src_stream| {
                        const weight = pre_weights[dst_stream] * comb[dst_stream * hc_mult + src_stream];
                        const src = stream_row[src_stream * hidden_size ..][0..hidden_size];
                        for (0..hidden_size) |d| out_row[d] += weight * src[d];
                    }
                }
            },
            .post => unreachable,
        }
    }
}

pub fn applyLayerHyperResidualFallback(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    residual: CT,
    update: CT,
    total: usize,
    layer: usize,
    prefix: []const u8,
    name_buf: *[256]u8,
) !CT {
    const hc_mult: usize = @intCast(config.deepseek_v4_hc_mult);
    const hidden_size: usize = @intCast(config.hidden_size);
    if (hc_mult == 0) return cb.add(update, residual);

    const fn_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}.fn", .{ layer, prefix }) catch return error.NameTooLong;
    const fn_w = try runtime.get_weight(runtime.ptr, fn_name);
    defer cb.free(fn_w);
    const base_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}.base", .{ layer, prefix }) catch return error.NameTooLong;
    const base_w = try runtime.get_weight(runtime.ptr, base_name);
    defer cb.free(base_w);
    const scale_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.{s}.scale", .{ layer, prefix }) catch return error.NameTooLong;
    const scale_w = try runtime.get_weight(runtime.ptr, scale_name);
    defer cb.free(scale_w);

    const residual_data = try cb.toFloat32(residual, allocator);
    defer allocator.free(residual_data);
    const update_data = try cb.toFloat32(update, allocator);
    defer allocator.free(update_data);
    if (residual_data.len != total * hidden_size or update_data.len != total * hidden_size) return error.DeepSeekV4TensorShapeMismatch;

    const fn_data = try cb.toFloat32(fn_w, allocator);
    defer allocator.free(fn_data);
    const base_data = try cb.toFloat32(base_w, allocator);
    defer allocator.free(base_data);
    const scale_data = try cb.toFloat32(scale_w, allocator);
    defer allocator.free(scale_data);
    if (scale_data.len == 0) return error.DeepSeekV4TensorShapeMismatch;

    const conn_mix = (2 + hc_mult) * hc_mult;
    const flat_dim = hc_mult * hidden_size;
    if (fn_data.len == conn_mix * flat_dim) {
        if (base_data.len != conn_mix or scale_data.len < 3) return error.DeepSeekV4TensorShapeMismatch;
        return applyLayerHyperConnectionRows(cb, allocator, config, residual_data, update_data, fn_data, base_data, scale_data, total, hc_mult, hidden_size);
    }

    if (fn_data.len != hc_mult * hc_mult * hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    if (base_data.len != hc_mult) return error.DeepSeekV4TensorShapeMismatch;

    const streams = try allocator.alloc(f32, total * hc_mult * hidden_size);
    defer allocator.free(streams);
    for (0..total) |row| {
        const row_src = residual_data[row * hidden_size ..][0..hidden_size];
        for (0..hc_mult) |stream| {
            const row_dst = streams[(row * hc_mult + stream) * hidden_size ..][0..hidden_size];
            @memcpy(row_dst, row_src);
        }
    }

    const stream_weights = try allocator.alloc(f32, total * hc_mult);
    defer allocator.free(stream_weights);
    const shape = deepseek_v4_host.HyperStreamsShape{ .rows = total, .hc_mult = hc_mult, .hidden_dim = hidden_size };
    deepseek_v4_host.hyperHeadWeightsRows(stream_weights, streams, fn_data, base_data, scale_data, shape, config.deepseek_v4_hc_eps, config.norm_eps);
    deepseek_v4_host.broadcastAddStreamsRows(streams, update_data, stream_weights, shape);

    const out = try allocator.alloc(f32, total * hidden_size);
    defer allocator.free(out);
    deepseek_v4_host.weightedStreamReduceRows(out, streams, stream_weights, shape);

    const out_shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    return cb.fromFloat32Shape(out, &out_shape);
}

fn applyLayerHyperConnectionRows(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    residual_data: []const f32,
    update_data: []const f32,
    fn_data: []const f32,
    base_data: []const f32,
    scale_data: []const f32,
    total: usize,
    hc_mult: usize,
    hidden_size: usize,
) !CT {
    const flat_dim = hc_mult * hidden_size;
    const mix_dim = (2 + hc_mult) * hc_mult;
    if (residual_data.len != total * hidden_size or update_data.len != total * hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    if (fn_data.len != mix_dim * flat_dim or base_data.len != mix_dim or scale_data.len < 3) return error.DeepSeekV4TensorShapeMismatch;

    const streams = try allocator.alloc(f32, total * flat_dim);
    defer allocator.free(streams);
    for (0..total) |row| {
        const row_src = residual_data[row * hidden_size ..][0..hidden_size];
        for (0..hc_mult) |stream| {
            const row_dst = streams[(row * hc_mult + stream) * hidden_size ..][0..hidden_size];
            @memcpy(row_dst, row_src);
        }
    }

    const out = try allocator.alloc(f32, total * hidden_size);
    defer allocator.free(out);
    const comb = try allocator.alloc(f32, hc_mult * hc_mult);
    defer allocator.free(comb);
    const pre_weights = try allocator.alloc(f32, hc_mult);
    defer allocator.free(pre_weights);
    const post_weights = try allocator.alloc(f32, hc_mult);
    defer allocator.free(post_weights);

    for (0..total) |row| {
        const stream_row = streams[row * flat_dim ..][0..flat_dim];
        var sum_sq: f32 = 0.0;
        for (stream_row) |value| sum_sq += value * value;
        const rsqrt = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(flat_dim)) + config.norm_eps);

        const residual_row = residual_data[row * hidden_size ..][0..hidden_size];
        const update_row = update_data[row * hidden_size ..][0..hidden_size];
        const out_row = out[row * hidden_size ..][0..hidden_size];
        @memset(out_row, 0.0);

        for (0..hc_mult) |dst_stream| {
            var pre_logit: f32 = 0.0;
            var post_logit: f32 = 0.0;
            const pre_weight_base = dst_stream * flat_dim;
            const post_weight_base = (hc_mult + dst_stream) * flat_dim;
            for (0..flat_dim) |col| {
                pre_logit += stream_row[col] * fn_data[pre_weight_base + col];
                post_logit += stream_row[col] * fn_data[post_weight_base + col];
            }
            pre_weights[dst_stream] = deepseek_v4_host.sigmoid(pre_logit * rsqrt * scale_data[0] + base_data[dst_stream]) + config.deepseek_v4_hc_eps;
            post_weights[dst_stream] = deepseek_v4_host.sigmoid(post_logit * rsqrt * scale_data[1] + base_data[hc_mult + dst_stream]) + config.deepseek_v4_hc_eps;

            for (0..hc_mult) |src_stream| {
                var comb_logit: f32 = 0.0;
                const comb_index = dst_stream * hc_mult + src_stream;
                const comb_weight_base = (2 * hc_mult + comb_index) * flat_dim;
                for (0..flat_dim) |col| {
                    comb_logit += stream_row[col] * fn_data[comb_weight_base + col];
                }
                comb[comb_index] = deepseek_v4_host.sigmoid(comb_logit * rsqrt * scale_data[2] + base_data[2 * hc_mult + comb_index]) + config.deepseek_v4_hc_eps;
            }
        }
        deepseek_v4_host.sinkhornInPlace(comb, hc_mult, config.deepseek_v4_hc_sinkhorn_iters, config.deepseek_v4_hc_eps);

        for (0..hc_mult) |dst_stream| {
            var comb_row_sum: f32 = 0.0;
            for (0..hc_mult) |src_stream| comb_row_sum += comb[dst_stream * hc_mult + src_stream];
            for (0..hidden_size) |d| {
                out_row[d] += pre_weights[dst_stream] * (comb_row_sum * residual_row[d] + post_weights[dst_stream] * update_row[d]);
            }
        }
    }

    const out_shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    return cb.fromFloat32Shape(out, &out_shape);
}

pub fn applyHyperHeadToStreams(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    streams: []const f32,
    total: usize,
) !CT {
    const hc_mult: usize = @intCast(config.deepseek_v4_hc_mult);
    const hidden_size: usize = @intCast(config.hidden_size);
    if (streams.len != total * hc_mult * hidden_size) return error.DeepSeekV4TensorShapeMismatch;

    const fn_w = try runtime.get_weight(runtime.ptr, "model.hc_head.hc_fn");
    defer cb.free(fn_w);
    const base_w = try runtime.get_weight(runtime.ptr, "model.hc_head.hc_base");
    defer cb.free(base_w);
    const scale_w = try runtime.get_weight(runtime.ptr, "model.hc_head.hc_scale");
    defer cb.free(scale_w);

    const fn_data = try cb.toFloat32(fn_w, allocator);
    defer allocator.free(fn_data);
    if (fn_data.len != hc_mult * hc_mult * hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    const base_data = try cb.toFloat32(base_w, allocator);
    defer allocator.free(base_data);
    if (base_data.len != hc_mult) return error.DeepSeekV4TensorShapeMismatch;
    const scale_data = try cb.toFloat32(scale_w, allocator);
    defer allocator.free(scale_data);
    if (scale_data.len == 0) return error.DeepSeekV4TensorShapeMismatch;

    const out = try allocator.alloc(f32, total * hidden_size);
    defer allocator.free(out);
    const scratch_weights = try allocator.alloc(f32, total * hc_mult);
    defer allocator.free(scratch_weights);
    deepseek_v4_host.hyperHeadCollapseRows(
        out,
        scratch_weights,
        streams,
        fn_data,
        base_data,
        scale_data,
        .{ .rows = total, .hc_mult = hc_mult, .hidden_dim = hidden_size },
        config.deepseek_v4_hc_eps,
        config.norm_eps,
    );

    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    return cb.fromFloat32Shape(out, &shape);
}

pub fn applyHyperHeadFallback(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: gpt_config.Config,
    runtime: Runtime,
    hidden: CT,
) !CT {
    const hc_mult: usize = @intCast(config.deepseek_v4_hc_mult);
    const hidden_size: usize = @intCast(config.hidden_size);
    if (hc_mult == 0) return hidden;

    const fn_w = try runtime.get_weight(runtime.ptr, "model.hc_head.hc_fn");
    defer cb.free(fn_w);
    const base_w = try runtime.get_weight(runtime.ptr, "model.hc_head.hc_base");
    defer cb.free(base_w);
    const scale_w = try runtime.get_weight(runtime.ptr, "model.hc_head.hc_scale");
    defer cb.free(scale_w);

    const hidden_data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(hidden_data);
    if (hidden_data.len % hidden_size != 0) return error.DeepSeekV4TensorShapeMismatch;
    const total = hidden_data.len / hidden_size;

    const fn_data = try cb.toFloat32(fn_w, allocator);
    defer allocator.free(fn_data);
    if (fn_data.len != hc_mult * hc_mult * hidden_size) return error.DeepSeekV4TensorShapeMismatch;
    const base_data = try cb.toFloat32(base_w, allocator);
    defer allocator.free(base_data);
    if (base_data.len != hc_mult) return error.DeepSeekV4TensorShapeMismatch;
    const scale_data = try cb.toFloat32(scale_w, allocator);
    defer allocator.free(scale_data);
    if (scale_data.len == 0) return error.DeepSeekV4TensorShapeMismatch;
    const out = try allocator.alloc(f32, hidden_data.len);
    defer allocator.free(out);
    const scratch_streams = try allocator.alloc(f32, total * hc_mult * hidden_size);
    defer allocator.free(scratch_streams);
    const scratch_weights = try allocator.alloc(f32, total * hc_mult);
    defer allocator.free(scratch_weights);
    for (0..total) |row| {
        const input_row = hidden_data[row * hidden_size ..][0..hidden_size];
        for (0..hc_mult) |stream| {
            const dst = scratch_streams[(row * hc_mult + stream) * hidden_size ..][0..hidden_size];
            @memcpy(dst, input_row);
        }
    }
    deepseek_v4_host.hyperHeadCollapseRows(
        out,
        scratch_weights,
        scratch_streams,
        fn_data,
        base_data,
        scale_data,
        .{ .rows = total, .hc_mult = hc_mult, .hidden_dim = hidden_size },
        config.deepseek_v4_hc_eps,
        config.norm_eps,
    );

    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    return cb.fromFloat32Shape(out, &shape);
}

test "deepseek v4 gguf tensor catalog infers attention and mlp schedules" {
    var cfg = gpt_config.Config{
        .family = .deepseek_v4,
        .num_hidden_layers = 5,
    };
    var names = [_][]const u8{
        "blk.0.attn_q_a.weight",
        "blk.0.ffn_gate_tid2eid",
        "blk.1.attn_compress_kv.weight",
        "blk.1.indexer.compress_kv.weight",
        "blk.1.ffn_gate_tid2eid",
        "blk.2.attn_compress_kv.weight",
        "blk.2.exp_probs_b",
        "blk.3.attn_compress_kv.weight",
        "blk.3.indexer.proj.weight",
        "blk.3.exp_probs_b",
        "blk.4.attn_q_a.weight",
        "blk.4.ffn_gate_inp.weight",
    };

    inferSchedulesFromGgufNames(&cfg, names[0..]);

    try std.testing.expectEqual(@as(u32, 5), cfg.deepseek_v4_attention_schedule_len);
    try std.testing.expectEqual(gpt_config.DeepseekV4AttentionKind.sliding_attention, cfg.deepseekV4AttentionKind(0));
    try std.testing.expectEqual(gpt_config.DeepseekV4AttentionKind.compressed_sparse_attention, cfg.deepseekV4AttentionKind(1));
    try std.testing.expectEqual(gpt_config.DeepseekV4AttentionKind.heavily_compressed_attention, cfg.deepseekV4AttentionKind(2));
    try std.testing.expectEqual(gpt_config.DeepseekV4AttentionKind.compressed_sparse_attention, cfg.deepseekV4AttentionKind(3));
    try std.testing.expectEqual(gpt_config.DeepseekV4AttentionKind.sliding_attention, cfg.deepseekV4AttentionKind(4));
    try std.testing.expectEqual(@as(u32, 2), cfg.deepseek_v4_sliding_attention_layers);
    try std.testing.expectEqual(@as(u32, 2), cfg.deepseek_v4_compressed_sparse_attention_layers);
    try std.testing.expectEqual(@as(u32, 1), cfg.deepseek_v4_heavily_compressed_attention_layers);

    try std.testing.expectEqual(@as(u32, 5), cfg.deepseek_v4_mlp_schedule_len);
    try std.testing.expectEqual(gpt_config.DeepseekV4MlpKind.hash_moe, cfg.deepseekV4MlpKind(0));
    try std.testing.expectEqual(gpt_config.DeepseekV4MlpKind.hash_moe, cfg.deepseekV4MlpKind(1));
    try std.testing.expectEqual(gpt_config.DeepseekV4MlpKind.moe, cfg.deepseekV4MlpKind(2));
    try std.testing.expectEqual(gpt_config.DeepseekV4MlpKind.moe, cfg.deepseekV4MlpKind(3));
    try std.testing.expectEqual(gpt_config.DeepseekV4MlpKind.moe, cfg.deepseekV4MlpKind(4));
    try std.testing.expectEqual(@as(u32, 2), cfg.deepseek_v4_hash_moe_layers);
    try std.testing.expectEqual(@as(u32, 3), cfg.deepseek_v4_moe_layers);
}

test "deepseek v4 required tensors accept fused or split expert inputs" {
    const allocator = std.testing.allocator;
    var names = std.StringHashMapUnmanaged(void){};
    defer {
        var it = names.keyIterator();
        while (it.next()) |key| allocator.free(key.*);
        names.deinit(allocator);
    }

    var missing = std.ArrayListUnmanaged([]const u8).empty;
    defer {
        for (missing.items) |name| allocator.free(name);
        missing.deinit(allocator);
    }

    try names.put(allocator, try allocator.dupe(u8, "model.layers.0.mlp.experts.gate_up_proj"), {});
    var buf: [256]u8 = undefined;
    try appendMissingExpertInputWeights(allocator, &names, &missing, &buf, 0);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);

    if (names.fetchRemove("model.layers.0.mlp.experts.gate_up_proj")) |removed| allocator.free(removed.key);
    try names.put(allocator, try allocator.dupe(u8, "model.layers.0.mlp.experts.gate_proj"), {});
    try names.put(allocator, try allocator.dupe(u8, "model.layers.0.mlp.experts.up_proj"), {});
    try appendMissingExpertInputWeights(allocator, &names, &missing, &buf, 0);
    try std.testing.expectEqual(@as(usize, 0), missing.items.len);
}
