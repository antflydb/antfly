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

//! Adapter for the released fixed-loop Inkling 2 Mini ModelOpt checkpoint.
const std = @import("std");
const files = @import("../util/c_file.zig");
const safetensors = @import("../models/safetensors.zig");
const DType = @import("../backends/tensor.zig").DType;
const Group = @import("../runtime/kv/hybrid_paged.zig").Group;

pub const architecture = "inkling2_loop";
pub const shard_name = "model-00001-of-00001.safetensors";
pub const draft_name = "mtp.safetensors";
pub const header_options: safetensors.HeaderOptions = .{ .allow_e4m3fn = true };
pub const layer_count = 24;
pub const visit_count = 48;
pub const full_slot_count = 8;
pub const sliding_slot_count = 40;
pub const nvfp4_layers = 20;

const LoopConfig = struct {
    core_start: u8,
    core_end: u8,
    num_passes: u8,
    gate_rank: u16,
    is_varloop: bool,
};

pub const TextConfig = struct {
    model_type: []const u8,
    model_max_length: u32,
    hidden_size: u16,
    num_hidden_layers: u8,
    vocab_size: u32,
    unpadded_vocab_size: u32,
    num_attention_heads: u8,
    num_key_value_heads: u8,
    head_dim: u16,
    d_rel: u16,
    rel_extent: u16,
    q_bias: bool,
    o_bias: bool,
    log_scaling_n_floor: u32,
    log_scaling_alpha: f32,
    rms_norm_eps: f32,
    use_value_norm_no_gamma: bool,
    qk_l2_clip_threshold: f32,
    rel_l2_clip_threshold: f32,
    local_layer_ids: [20]u8,
    sconv_kernel_size: u8,
    final_logit_softcapping: f32,
    swa_head_dim: u16,
    swa_num_attention_heads: u8,
    swa_num_key_value_heads: u8,
    sliding_window_size: u16,
    n_layers_per_attn_res_block: u8,
    attn_res_normalize_values: bool,
    n_routed_experts: u16,
    num_experts_per_tok: u8,
    n_shared_experts: u8,
    intermediate_size: u16,
    mlp_gate_softcap: f32,
    mlp_up_softcap: f32,
    route_scale: f32,
    use_gate_bias: bool,
    loop_config: LoopConfig,

    fn validate(self: TextConfig) !void {
        const loop = self.loop_config;
        if (!std.mem.eql(u8, self.model_type, "inkling2_loop_text") or
            self.num_hidden_layers != layer_count or self.hidden_size != 2304 or
            self.vocab_size != 201024 or self.unpadded_vocab_size != 200058 or
            self.model_max_length != 1048576 or self.num_attention_heads != 24 or
            self.num_key_value_heads != 8 or self.head_dim != 128 or self.d_rel != 16 or
            self.rel_extent != 1024 or self.sliding_window_size != 1024 or self.sconv_kernel_size != 4 or
            self.swa_head_dim != self.head_dim or self.swa_num_attention_heads != self.num_attention_heads or
            self.swa_num_key_value_heads != self.num_key_value_heads or self.n_layers_per_attn_res_block != 6 or
            self.n_routed_experts != 128 or self.num_experts_per_tok != 8 or self.n_shared_experts != 0 or
            self.intermediate_size != 1152 or self.q_bias or self.o_bias or !self.use_value_norm_no_gamma or
            !self.attn_res_normalize_values or !self.use_gate_bias or loop.is_varloop or loop.core_start != 6 or
            loop.core_end != 18 or loop.num_passes != 3 or loop.gate_rank != 64 or self.log_scaling_n_floor != 8192)
            return error.UnsupportedInkling2Checkpoint;
        for ([_]f32{ self.log_scaling_alpha, self.rms_norm_eps, self.qk_l2_clip_threshold, self.rel_l2_clip_threshold, self.final_logit_softcapping, self.mlp_gate_softcap, self.mlp_up_softcap, self.route_scale }) |value| {
            if (!std.math.isFinite(value) or value <= 0) return error.InvalidInkling2Config;
        }
    }
};

pub const Visit = struct {
    weight_layer: u8,
    pass: u8,
    group: Group,
    slot: u16,

    pub const PreludeAction = enum { none, save, restore };

    pub fn preludeAction(self: Visit, core_start: usize) PreludeAction {
        if (self.weight_layer != core_start) return .none;
        return if (self.pass == 0) .save else .restore;
    }
};

pub const DraftConfig = struct {
    hidden_size: u32,
    num_hidden_layers: u32,
    num_attention_heads: u32,
    num_key_value_heads: u32,
    head_dim: u32,
    intermediate_size: u32,
    max_position_embeddings: u32,
    rms_norm_eps: f32,
    rope_theta: f32,
    local_layer_ids: [2]u8,
    sliding_window_size: u32,
    block_size: u32,
    markov_rank: u32,
    markov_head_type: []const u8,
    target_layer_ids: [4]u8,
};

pub const Config = struct {
    model_type: []const u8,
    eos_token_id: u32,
    text_config: TextConfig,
    mtp_config: ?DraftConfig = null,
    quantization_config: struct {
        quant_method: []const u8,
        quant_algo: []const u8,
    },

    pub fn parse(allocator: std.mem.Allocator, bytes: []const u8) !std.json.Parsed(Config) {
        const parsed = try std.json.parseFromSlice(Config, allocator, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        errdefer parsed.deinit();
        const config = parsed.value;
        if (!std.mem.eql(u8, config.model_type, architecture) or
            !std.mem.eql(u8, config.quantization_config.quant_method, "modelopt") or
            !std.mem.eql(u8, config.quantization_config.quant_algo, "NVFP4") or
            config.eos_token_id >= config.text_config.unpadded_vocab_size)
            return error.UnsupportedInkling2Checkpoint;
        _ = try config.visits();
        return parsed;
    }

    pub fn visits(self: Config) ![visit_count]Visit {
        try self.text_config.validate();
        const loop = self.text_config.loop_config;
        var local: [layer_count]bool = @splat(false);
        for (self.text_config.local_layer_ids) |layer| {
            if (layer >= layer_count or local[layer]) return error.InvalidInkling2Config;
            local[layer] = true;
        }
        var visits_out: [visit_count]Visit = undefined;
        var n: usize = 0;
        var full: u16 = 0;
        var sliding: u16 = 0;
        for (0..layer_count) |layer| {
            if (layer == loop.core_start) {
                for (0..loop.num_passes) |pass| {
                    for (loop.core_start..loop.core_end) |core_layer| {
                        visits_out[n] = makeVisit(&local, core_layer, pass, &full, &sliding);
                        n += 1;
                    }
                }
            }
            if (layer >= loop.core_start and layer < loop.core_end) continue;
            visits_out[n] = makeVisit(&local, layer, 0, &full, &sliding);
            n += 1;
        }
        if (n != visits_out.len or full != full_slot_count or sliding != sliding_slot_count)
            return error.InvalidInkling2Config;
        return visits_out;
    }
};

fn makeVisit(local: *const [layer_count]bool, layer: usize, pass: usize, full: *u16, sliding: *u16) Visit {
    const group: Group = if (local[layer]) .sliding else .full;
    const slot = if (group == .sliding) sliding else full;
    const result: Visit = .{ .weight_layer = @intCast(layer), .pass = @intCast(pass), .group = group, .slot = slot.* };
    slot.* += 1;
    return result;
}

pub const Projection = enum { w13, w2 };
pub const ExpertFormat = @import("antfly_tile").native_projection.BlockScaledFormat;
pub const ExpertWeights = struct {
    format: ExpertFormat,
    weights: []const u8,
    block_scales: []const u8,
    outer_amax: ?[]const u8 = null,
    outer_denominator: ?[]const u8 = null,
};

pub const RouterWeights = struct {
    weight: []const u8,
    bias: []const u8,
};

fn layerName(buf: []u8, layer: usize, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "model.llm.layers.{d}.{s}", .{ layer, suffix });
}

fn expertName(buf: []u8, layer: usize, projection: Projection, suffix: []const u8) ![]const u8 {
    return std.fmt.bufPrint(buf, "model.llm.layers.{d}.mlp.experts.{s}_weight{s}", .{ layer, @tagName(projection), suffix });
}

fn require(header: *const safetensors.Header, name: []const u8, dtype: DType, encoding: safetensors.Encoding, shape: []const i64) !void {
    const meta = header.tensors.get(name) orelse return error.MissingInkling2Weight;
    if (meta.dtype != dtype or meta.encoding != encoding or !std.mem.eql(i64, meta.shape, shape))
        return error.InvalidInkling2Weight;
    var bytes: usize = dtype.byteSize();
    for (shape) |dim| bytes = try std.math.mul(usize, bytes, @intCast(dim));
    if (meta.data_end < meta.data_start or meta.data_end - meta.data_start != bytes)
        return error.InvalidInkling2Weight;
}

pub fn validateDecoder(header: *const safetensors.Header, config: TextConfig) !void {
    const hidden: i64 = config.hidden_size;
    const q_width = @as(i64, config.num_attention_heads) * config.head_dim;
    const kv_width = @as(i64, config.num_key_value_heads) * config.head_dim;
    const rel_width = @as(i64, config.num_attention_heads) * config.d_rel;
    const taps: i64 = config.sconv_kernel_size;
    const expert_count: i64 = config.n_routed_experts;
    const rank: i64 = config.loop_config.gate_rank;
    try require(header, "model.llm.embed.weight", .bf16, .dense, &.{ config.vocab_size, hidden });
    try require(header, "model.llm.unembed.weight", .bf16, .dense, &.{ config.vocab_size, hidden });
    for ([_][]const u8{ "model.llm.embed_norm.weight", "model.llm.norm.weight" }) |name|
        try require(header, name, .bf16, .dense, &.{hidden});
    try require(header, "model.llm.attn_res.output_query", .f32, .dense, &.{hidden});
    const core_layers: i64 = config.loop_config.core_end - config.loop_config.core_start;
    for ([_][]const u8{ "model.llm.middle_cycle_cross_pass_kv_queries", "model.llm.middle_cycle_cross_pass_queries" }) |name|
        try require(header, name, .f32, .dense, &.{ 2 * core_layers, hidden });
    for (0..layer_count) |layer| {
        var name_buf: [128]u8 = undefined;
        const local = std.mem.indexOfScalar(u8, &config.local_layer_ids, @intCast(layer)) != null;
        const AttnTensor = struct { name: []const u8, shape: []const i64 };
        for ([_]AttnTensor{
            .{ .name = "attn.wq_du.weight", .shape = &.{ q_width, hidden } },
            .{ .name = "attn.wk_dv.weight", .shape = &.{ kv_width, hidden } },
            .{ .name = "attn.wv_dv.weight", .shape = &.{ kv_width, hidden } },
            .{ .name = "attn.wr_du.weight", .shape = &.{ rel_width, hidden } },
            .{ .name = "attn.gate_attn.weight", .shape = &.{ q_width, hidden } },
            .{ .name = "attn.wo_ud.weight", .shape = &.{ hidden, q_width } },
            .{ .name = "attn.attn_sink_proj.weight", .shape = &.{ config.num_attention_heads, hidden } },
            .{ .name = "attn.q_l2_clip.scale", .shape = &.{ config.num_attention_heads, config.head_dim } },
            .{ .name = "attn.rel_l2_clip.scale", .shape = &.{ config.num_attention_heads, config.d_rel } },
            .{ .name = "attn.rel_logits_proj.proj", .shape = &.{ config.d_rel, config.rel_extent } },
            .{ .name = "attn_sconv.weight", .shape = &.{ hidden, 1, taps } },
            .{ .name = "mlp_sconv.weight", .shape = &.{ hidden, 1, taps } },
            .{ .name = "mlp_norm.weight", .shape = &.{hidden} },
            .{ .name = "mlp.gate.weight", .shape = &.{ expert_count, hidden } },
        }) |tensor| try require(header, try layerName(&name_buf, layer, tensor.name), .bf16, .dense, tensor.shape);
        try require(header, try layerName(&name_buf, layer, "mlp.gate.bias"), .f32, .dense, &.{expert_count});
        if (local) {
            for ([_][]const u8{ "attn.k_sconv.weight", "attn.v_sconv.weight" }) |name|
                try require(header, try layerName(&name_buf, layer, name), .bf16, .dense, &.{ kv_width, 1, taps });
            try require(header, try layerName(&name_buf, layer, "attn_norm.weight"), .bf16, .dense, &.{hidden});
        } else {
            for ([_][]const u8{ "attn.k_sconv.static_w", "attn.v_sconv.static_w" }) |name|
                try require(header, try layerName(&name_buf, layer, name), .bf16, .dense, &.{ taps, kv_width });
            for ([_][]const u8{ "attn.k_sconv.weight_proj", "attn.v_sconv.weight_proj" }) |name|
                try require(header, try layerName(&name_buf, layer, name), .bf16, .dense, &.{ taps * config.num_key_value_heads, hidden });
            for ([_][]const u8{ "q_input_norm.weight", "k_input_norm.weight", "v_input_norm.weight" }) |name|
                try require(header, try layerName(&name_buf, layer, name), .bf16, .dense, &.{hidden});
        }
        for ([_][]const u8{ "attn_queries", "mlp_queries" }) |kind| {
            const attention_query = std.mem.eql(u8, kind, "attn_queries");
            if (attention_query and layer + 1 == layer_count) continue;
            const name = try std.fmt.bufPrint(&name_buf, "model.llm.attn_res.{s}.{d}", .{ kind, layer });
            // Attention reads the residual query published by the previous
            // layer. The final output has its own query.
            const next_full = layer + 1 < layer_count and
                std.mem.indexOfScalar(u8, &config.local_layer_ids, @intCast(layer + 1)) == null;
            const shape: []const i64 = if (next_full and attention_query) &.{ 3, hidden } else &.{hidden};
            try require(header, name, .f32, .dense, shape);
        }
        if (layer >= config.loop_config.core_start and layer < config.loop_config.core_end) {
            for ([_][]const u8{ "attn", "k", "v", "mlp" }) |input| {
                const name = try std.fmt.bufPrint(&name_buf, "model.llm.layers.{d}.controllers.{s}.down_weight", .{ layer, input });
                try require(header, name, .bf16, .dense, &.{ rank, 2 * hidden });
                for ([_][]const u8{ "forget_weight", "gain_weight" }) |projection| {
                    const up_name = try std.fmt.bufPrint(&name_buf, "model.llm.layers.{d}.controllers.{s}.{s}", .{ layer, input, projection });
                    try require(header, up_name, .bf16, .dense, &.{ hidden, rank });
                }
            }
        }
        try validateExpertProjection(header, config, layer, .w13);
        try validateExpertProjection(header, config, layer, .w2);
    }
}

fn validateExpertProjection(header: *const safetensors.Header, config: TextConfig, layer: usize, projection: Projection) !void {
    const format: ExpertFormat = if (layer < nvfp4_layers) .nvfp4 else .mxfp8;
    const k = if (projection == .w13) config.hidden_size else config.intermediate_size;
    const row = format.row(k) catch return error.InvalidInkling2Config;
    const nvfp4 = format == .nvfp4;
    const n: i64 = if (projection == .w13) @as(i64, config.intermediate_size) * 2 else config.hidden_size;
    const experts: i64 = config.n_routed_experts;
    var buf: [128]u8 = undefined;
    try require(header, try expertName(&buf, layer, projection, ""), .u8, if (nvfp4) .dense else .e4m3fn, &.{ experts, n, @intCast(row.values_bytes) });
    try require(header, try expertName(&buf, layer, projection, "_scale"), .u8, if (nvfp4) .e4m3fn else .dense, &.{ experts, n, @intCast(row.scales_bytes) });
    if (nvfp4) {
        try require(header, try expertName(&buf, layer, projection, "_amax"), .f32, .dense, &.{experts});
        try require(header, try expertName(&buf, layer, projection, "_outer_scale_denominator"), .f32, .dense, &.{1});
    }
}

pub const Checkpoint = struct {
    config: std.json.Parsed(Config),
    reader: safetensors.MMapReader,
    schedule: [visit_count]Visit,

    pub fn open(allocator: std.mem.Allocator, dir: []const u8) !Checkpoint {
        const config_path = try std.fs.path.join(allocator, &.{ dir, "config.json" });
        defer allocator.free(config_path);
        const shard_path = try std.fs.path.join(allocator, &.{ dir, shard_name });
        defer allocator.free(shard_path);
        return openFiles(allocator, config_path, shard_path);
    }

    pub fn openFiles(allocator: std.mem.Allocator, config_path: []const u8, shard_path: []const u8) !Checkpoint {
        const bytes = try files.readFileMax(allocator, config_path, 64 * 1024);
        defer allocator.free(bytes);
        var config = try Config.parse(allocator, bytes);
        errdefer config.deinit();
        var reader = try safetensors.MMapReader.openFileAbsoluteWithOptions(allocator, shard_path, std.math.maxInt(usize), safetensors.max_header_size, header_options);
        errdefer reader.deinit();
        try safetensors.validateReader(allocator, &reader);
        try validateDecoder(&reader.header, config.value.text_config);
        return .{ .config = config, .reader = reader, .schedule = try config.value.visits() };
    }

    pub fn deinit(self: *Checkpoint) void {
        self.reader.deinit();
        self.config.deinit();
    }

    pub fn experts(self: *const Checkpoint, layer: usize, projection: Projection) !ExpertWeights {
        if (layer >= layer_count) return error.InvalidInkling2Layer;
        var buf: [128]u8 = undefined;
        var result: ExpertWeights = .{
            .format = if (layer < nvfp4_layers) .nvfp4 else .mxfp8,
            .weights = try self.reader.rawTensorBytes(try expertName(&buf, layer, projection, "")),
            .block_scales = try self.reader.rawTensorBytes(try expertName(&buf, layer, projection, "_scale")),
        };
        if (result.format == .nvfp4) {
            result.outer_amax = try self.reader.rawTensorBytes(try expertName(&buf, layer, projection, "_amax"));
            result.outer_denominator = try self.reader.rawTensorBytes(try expertName(&buf, layer, projection, "_outer_scale_denominator"));
        }
        return result;
    }

    pub fn router(self: *const Checkpoint, layer: usize) !RouterWeights {
        if (layer >= layer_count) return error.InvalidInkling2Layer;
        var buf: [128]u8 = undefined;
        return .{
            .weight = try self.reader.rawTensorBytes(try layerName(&buf, layer, "mlp.gate.weight")),
            .bias = try self.reader.rawTensorBytes(try layerName(&buf, layer, "mlp.gate.bias")),
        };
    }

    pub fn layerTensor(self: *const Checkpoint, layer: usize, suffix: []const u8) ![]const u8 {
        if (layer >= layer_count) return error.InvalidInkling2Layer;
        var buf: [128]u8 = undefined;
        return self.reader.rawTensorBytes(try layerName(&buf, layer, suffix));
    }
};
