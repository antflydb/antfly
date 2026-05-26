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

// GPT/decoder-only architecture using abstract ComputeBackend ops.
//
// Supports GPT-2, LLaMA, Mistral, Phi, Qwen2, Gemma, Falcon, OPT, BLOOM
// and other causal language models via the unified gpt.Config.
//
// Key differences from BERT:
// - Causal (unidirectional) self-attention only, no cross-attention
// - Pre-norm (LLaMA, Mistral) or post-norm (GPT-2) architectures
// - RoPE (LLaMA, Mistral), absolute (GPT-2), or ALiBi (BLOOM) position encoding
// - GQA (Mistral, LLaMA-2+) where num_kv_heads < num_heads
// - Gated FFN with SiLU (LLaMA, Mistral) vs plain FFN with GELU (GPT-2)

const std = @import("std");
const is_freestanding = @import("builtin").os.tag == .freestanding;
const build_options = @import("build_options");
const platform = @import("antfly_platform");
const ops = @import("../ops/ops.zig");
const backend_contracts = @import("../graph/backend_contracts.zig");
const CT = ops.CT;
const ComputeBackend = ops.ComputeBackend;
const gpt_config = @import("../models/gpt.zig");
const runtime = @import("../runtime/root.zig");
const activations = @import("../backends/activations.zig");
const native_blas = @import("../backends/native.zig");
const native_compute_mod = @import("../ops/native_compute.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
const deepseek_v4 = @import("deepseek_v4.zig");
const deepseek_v4_host = @import("deepseek_v4_host.zig");
const tensor_mod = @import("../backends/tensor.zig");
const weight_source_mod = @import("../models/weight_source.zig");

const default_moe_decode_eval_stride: usize = 12;

fn debugPrint(comptime fmt: []const u8, args: anytype) void {
    if (comptime is_freestanding) return;
    std.debug.print(fmt, args);
}

pub const Qwen35LinearLayerState = struct {
    conv: []f32 = &.{},
    recurrent: []f32 = &.{},
    initialized: bool = false,

    fn deinit(self: *Qwen35LinearLayerState, allocator: std.mem.Allocator) void {
        if (self.conv.len > 0) allocator.free(self.conv);
        if (self.recurrent.len > 0) allocator.free(self.recurrent);
        self.* = .{};
    }

    fn reset(self: *Qwen35LinearLayerState) void {
        if (self.conv.len > 0) @memset(self.conv, 0);
        if (self.recurrent.len > 0) @memset(self.recurrent, 0);
        self.initialized = false;
    }
};

pub const Qwen35LinearCache = struct {
    allocator: std.mem.Allocator,
    layers: []Qwen35LinearLayerState,

    pub fn init(allocator: std.mem.Allocator, config: Config) !Qwen35LinearCache {
        const layer_count: usize = @intCast(config.num_hidden_layers);
        const layers = try allocator.alloc(Qwen35LinearLayerState, layer_count);
        errdefer allocator.free(layers);
        for (layers) |*state| state.* = .{};

        const key_heads: usize = @intCast(config.qwen35_linear_num_key_heads);
        const value_heads: usize = @intCast(config.qwen35_linear_num_value_heads);
        const key_dim_per_head: usize = @intCast(config.qwen35_linear_key_head_dim);
        const value_dim_per_head: usize = @intCast(config.qwen35_linear_value_head_dim);
        const conv_kernel: usize = @intCast(config.qwen35_linear_conv_kernel_dim);
        const key_dim = key_heads * key_dim_per_head;
        const value_dim = value_heads * value_dim_per_head;
        const conv_dim = key_dim * 2 + value_dim;
        const recurrent_len = value_heads * key_dim_per_head * value_dim_per_head;

        for (layers, 0..) |*state, layer| {
            if (!config.layerUsesQwen35LinearAttention(layer)) continue;
            state.conv = try allocator.alloc(f32, conv_dim * conv_kernel);
            errdefer state.deinit(allocator);
            @memset(state.conv, 0);
            state.recurrent = try allocator.alloc(f32, recurrent_len);
            errdefer state.deinit(allocator);
            @memset(state.recurrent, 0);
        }

        return .{ .allocator = allocator, .layers = layers };
    }

    pub fn deinit(self: *Qwen35LinearCache) void {
        for (self.layers) |*state| state.deinit(self.allocator);
        self.allocator.free(self.layers);
        self.* = undefined;
    }

    pub fn reset(self: *Qwen35LinearCache) void {
        for (self.layers) |*state| state.reset();
    }

    fn layerState(self: *Qwen35LinearCache, layer: usize) ?*Qwen35LinearLayerState {
        if (layer >= self.layers.len) return null;
        if (self.layers[layer].conv.len == 0 or self.layers[layer].recurrent.len == 0) return null;
        return &self.layers[layer];
    }
};

fn monotonicNowNs() u64 {
    if (comptime @import("builtin").os.tag == .freestanding) return 0;
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

pub const Config = gpt_config.Config;
pub const ModelFamily = gpt_config.ModelFamily;
pub const NormType = gpt_config.NormType;
pub const PositionEncoding = gpt_config.PositionEncoding;
pub const ActivationType = gpt_config.ActivationType;
pub const DeepSeekV4CompressedCache = deepseek_v4.CompressedCache;

const LogitsTensorResult = struct {
    logits: CT,
    total_rows: usize,
};

const HiddenTensorResult = struct {
    hidden: CT,
    total_rows: usize,
};

const FinalAndPreNormHiddenTensorResult = struct {
    final_hidden: CT,
    pre_norm_hidden: CT,
    total_rows: usize,
};

const ReservedHiddenCarrier = struct {
    front: CT,
    back: CT,
    active_front: bool = true,

    fn init(
        cb: *const ComputeBackend,
        hidden_input: CT,
        rows: usize,
        hidden_size: usize,
    ) !?ReservedHiddenCarrier {
        if (comptime !build_options.enable_metal) return null;
        if (disableReservedHiddenCarrierDebug() or forceLayerCloneDebug()) return null;
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

    fn active(self: *const ReservedHiddenCarrier) CT {
        return if (self.active_front) self.front else self.back;
    }

    fn inactive(self: *const ReservedHiddenCarrier) CT {
        return if (self.active_front) self.back else self.front;
    }

    fn ownsSlot(self: *const ReservedHiddenCarrier, tensor: CT) bool {
        return tensor == self.front or tensor == self.back;
    }

    fn replaceActive(self: *ReservedHiddenCarrier, cb: *const ComputeBackend, next_hidden: CT) !void {
        if (!(try metal_compute_mod.MetalCompute.copyTensorInto(cb, next_hidden, self.inactive()))) {
            return error.UnsupportedTensorType;
        }
        if (!self.ownsSlot(next_hidden)) cb.free(next_hidden);
        self.active_front = !self.active_front;
    }

    fn deinit(self: *ReservedHiddenCarrier, cb: *const ComputeBackend, keep_active: bool) void {
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
    const front: CT = @ptrFromInt(0x1000);
    const back: CT = @ptrFromInt(0x2000);
    const external: CT = @ptrFromInt(0x3000);
    const carrier = ReservedHiddenCarrier{ .front = front, .back = back };

    try std.testing.expect(carrier.ownsSlot(front));
    try std.testing.expect(carrier.ownsSlot(back));
    try std.testing.expect(!carrier.ownsSlot(external));
}

const decoder_override_layer_capacity: usize = 256;

fn decoderOverrideLayerSlot(slots: [decoder_override_layer_capacity]?usize, layer: usize) ?usize {
    if (layer >= decoder_override_layer_capacity) return null;
    return slots[layer];
}

pub const Layer0DecoderOverrides = struct {
    attn_norm: ?CT = null,
    fused_qkv: ?CT = null,
    q: ?CT = null,
    k: ?CT = null,
    v: ?CT = null,
    attn_norm_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    attn_q_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    attn_k_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    attn_v_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    fused_qkv_linear_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    attn_out_proj_linear_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    attn_sub_norm_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    ffn_norm_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    mlp_fc1_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    mlp_fc2_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    mlp_gate_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    mlp_up_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    mlp_sub_norm_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
    mlp_down_slots: [decoder_override_layer_capacity]?usize = [_]?usize{null} ** decoder_override_layer_capacity,
};

pub const GreedyDeviceTokenResult = struct {
    token_id: usize,
    token_tensor: ?CT = null,
};

fn deepSeekV4DecodeContextWithInputIds(
    config: Config,
    input_ids: []const i64,
    total: usize,
    seq_len: usize,
    query_seq_len: usize,
    decode_context: ?*const DecodeContext,
    storage: *DecodeContext,
) ?*const DecodeContext {
    if (config.family != .deepseek_v4) return decode_context;
    storage.* = if (decode_context) |dc| dc.* else .{
        .attention_mode = .full_recompute,
        .total_sequence_len = seq_len,
        .query_sequence_len = query_seq_len,
        .kv_sequence_len = seq_len,
    };
    storage.input_ids = input_ids[0..@min(total, input_ids.len)];
    return storage;
}

pub const DecodeContext = struct {
    pub const AttentionMode = enum {
        full_recompute,
        paged_prefill,
        paged_decode,
    };

    pub const KvCacheView = struct {
        sequence_id: runtime.kv.manager.SequenceId,
        pool_id: runtime.kv.block.KvPoolId,
        logical_block_count: usize,
        tail_tokens: u16,
        position_offset: usize = 0,
        logical_blocks: ?[]const runtime.kv.block.KvBlockId = null,
        kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
    };

    pub const KvBatchView = struct {
        kv_cache: KvCacheView,
        kv_manager: *runtime.kv.manager.KvManager,
        kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
        // Per-item overrides for mixed prefill+decode batches.
        per_item_query_len: ?usize = null,
        per_item_total_len: ?usize = null,
        per_item_kv_len: ?usize = null,
        per_item_kv_position_offset: ?usize = null,
        per_item_mode: ?@import("../ops/ops.zig").AttentionMode = null,
    };

    attention_mode: AttentionMode = .full_recompute,
    total_sequence_len: usize,
    query_sequence_len: usize,
    kv_sequence_len: usize,
    kv_position_offset: usize = 0,
    decoder_runtime_resident_kv_sequence_len: ?usize = null,
    decoder_runtime_resident_kv_position_offset: ?usize = null,
    sliding_window: usize = 0,
    attn_or_mask: ?[]const u8 = null,
    kv_cache: ?KvCacheView = null,
    kv_manager: ?*runtime.kv.manager.KvManager = null,
    kv_storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
    kv_batch: ?[]const KvBatchView = null,
    deepseek_v4_compressed_cache: ?*DeepSeekV4CompressedCache = null,
    moe_runtime: ?*runtime.moe.runtime.MoeRuntime = null,
    qwen35_linear_cache: ?*Qwen35LinearCache = null,
    input_ids: ?[]const i64 = null,

    pub fn usesPagedKv(self: DecodeContext) bool {
        return self.kv_cache != null or self.kv_batch != null;
    }

    /// Returns true if this is a mixed prefill+decode batch with per-item overrides.
    pub fn isMixedBatch(self: DecodeContext) bool {
        const batch = self.kv_batch orelse return false;
        if (batch.len == 0) return false;
        return batch[0].per_item_query_len != null;
    }
};

/// GPT decoder forward pass. Returns logits: [batch * seq_len * vocab_size] as f32.
pub fn forward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
) ![]f32 {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const embedded = try cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
    try maybeDebugTensorLastRow(cb, allocator, "embed", embedded, hidden_size);
    try maybeDebugTensor(cb, allocator, "embed_full", embedded);
    const hidden = try maybeScaleTokenEmbeddings(cb, allocator, config, embedded, total, hidden_size);
    if (hidden != embedded) try maybeDebugTensorLastRow(cb, allocator, "embed_scaled", hidden, hidden_size);
    if (hidden != embedded) try maybeDebugTensor(cb, allocator, "embed_scaled_full", hidden);

    const ple_vectors = try computePleVectors(cb, allocator, config, input_ids, hidden, total);
    defer if (ple_vectors) |pv| cb.free(pv);

    var deepseek_v4_decode_context_storage: DecodeContext = undefined;
    const effective_decode_context = deepSeekV4DecodeContextWithInputIds(
        config,
        input_ids,
        total,
        seq_len,
        query_seq_len,
        decode_context,
        &deepseek_v4_decode_context_storage,
    );
    return forwardFromEmbeddings(cb, allocator, config, hidden, batch, seq_len, effective_decode_context, ple_vectors);
}

pub fn forwardGreedyLastToken(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
) !usize {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const embedded = try cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
    const hidden = try maybeScaleTokenEmbeddings(cb, allocator, config, embedded, total, hidden_size);

    const ple_vectors = try computePleVectors(cb, allocator, config, input_ids, hidden, total);
    defer if (ple_vectors) |pv| cb.free(pv);

    var deepseek_v4_decode_context_storage: DecodeContext = undefined;
    const effective_decode_context = deepSeekV4DecodeContextWithInputIds(
        config,
        input_ids,
        total,
        seq_len,
        query_seq_len,
        decode_context,
        &deepseek_v4_decode_context_storage,
    );
    return forwardGreedyLastTokenFromEmbeddings(cb, allocator, config, hidden, batch, seq_len, effective_decode_context, ple_vectors);
}

pub fn forwardGreedyLastTokenFromTokenTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_token: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
) !?GreedyDeviceTokenResult {
    if (config.hasPle()) return null;
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;
    if (batch != 1 or query_seq_len != 1 or total != 1) return null;

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const embedded = (try cb.embeddingLookupTensor(embed_w, input_token, total, hidden_size)) orelse return null;
    const hidden = try maybeScaleTokenEmbeddings(cb, allocator, config, embedded, total, hidden_size);

    return try forwardGreedyLastTokenTensorFromEmbeddings(cb, allocator, config, hidden, batch, seq_len, decode_context, null);
}

pub fn forwardFromEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) ![]f32 {
    const logits_result = try forwardLogitsTensorFromEmbeddings(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    const logits = logits_result.logits;
    defer cb.free(logits);
    const result = try cb.toFloat32(logits, allocator);
    applyFinalLogitSoftcap(config, result);
    maybeDebugTopLogits(result, config.vocab_size);
    return result;
}

pub fn forwardGreedyLastTokenFromEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !usize {
    const result = try forwardGreedyLastTokenTensorFromEmbeddings(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    defer if (result.token_tensor) |token_tensor| cb.free(token_tensor);
    return result.token_id;
}

pub fn forwardGreedyLastTokenFromPositionedEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !usize {
    const result = try forwardGreedyLastTokenTensorFromPositionedEmbeddings(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    defer if (result.token_tensor) |token_tensor| cb.free(token_tensor);
    return result.token_id;
}

pub fn forwardGreedyLastTokenFromPositionedEmbeddingsWithLayer0AttnNormAndFusedQkv(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    layer0_attn_norm: CT,
    layer0_fused_qkv: CT,
    layer0_attn_out_proj_linear_slot: usize,
    layer0_ffn_norm_slot: usize,
    layer0_mlp_fc1_slot: usize,
    layer0_mlp_fc2_slot: usize,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !usize {
    const result = try forwardGreedyLastTokenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        .{
            .attn_norm = layer0_attn_norm,
            .fused_qkv = layer0_fused_qkv,
            .attn_out_proj_linear_slots = .{ layer0_attn_out_proj_linear_slot, null },
            .ffn_norm_slots = .{ layer0_ffn_norm_slot, null },
            .mlp_fc1_slots = .{ layer0_mlp_fc1_slot, null },
            .mlp_fc2_slots = .{ layer0_mlp_fc2_slot, null },
        },
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    defer if (result.token_tensor) |token_tensor| cb.free(token_tensor);
    return result.token_id;
}

pub fn forwardGreedyLastTokenFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !usize {
    const result = try forwardGreedyLastTokenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    defer if (result.token_tensor) |token_tensor| cb.free(token_tensor);
    return result.token_id;
}

pub fn forwardGreedyLastTokenFromPositionedEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !usize {
    const result = try forwardGreedyLastTokenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    defer if (result.token_tensor) |token_tensor| cb.free(token_tensor);
    return result.token_id;
}

pub fn forwardLastLogitsFromPositionedEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) ![]f32 {
    const hidden_result = try forwardFinalHiddenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    const hidden = hidden_result.hidden;
    defer cb.free(hidden);

    const lm_w = if (config.weight_tying)
        try getEmbeddingWeight(cb, config)
    else
        cb.getWeight("lm_head.weight") catch try getEmbeddingWeight(cb, config);
    defer cb.free(lm_w);

    const logits = try cb.linearNoBias(hidden, lm_w, hidden_result.total_rows, config.hidden_size, config.vocab_size);
    defer cb.free(logits);
    try maybeDebugTensor(cb, allocator, "lm_head", logits);

    const result = try cb.toFloat32(logits, allocator);
    defer allocator.free(result);
    applyFinalLogitSoftcap(config, result);
    maybeDebugTopLogits(result, config.vocab_size);

    const vocab_size = config.vocab_size;
    const last_pos_offset = (hidden_result.total_rows - 1) * vocab_size;
    return allocator.dupe(f32, result[last_pos_offset..][0..vocab_size]);
}

pub fn forwardLastLogitsFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) ![]f32 {
    const hidden_result = try forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    const hidden = hidden_result.hidden;
    defer cb.free(hidden);

    const lm_w = if (config.weight_tying)
        try getEmbeddingWeight(cb, config)
    else
        cb.getWeight("lm_head.weight") catch try getEmbeddingWeight(cb, config);
    defer cb.free(lm_w);

    const logits = try cb.linearNoBias(hidden, lm_w, hidden_result.total_rows, config.hidden_size, config.vocab_size);
    defer cb.free(logits);
    try maybeDebugTensor(cb, allocator, "lm_head", logits);

    const result = try cb.toFloat32(logits, allocator);
    applyFinalLogitSoftcap(config, result);
    maybeDebugTopLogits(result, config.vocab_size);

    const vocab_size = config.vocab_size;
    const last_pos_offset = (hidden_result.total_rows - 1) * vocab_size;
    return allocator.dupe(f32, result[last_pos_offset..][0..vocab_size]);
}

pub fn forwardLastLogitsLastRowFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) ![]f32 {
    const hidden_result = try forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    const hidden = hidden_result.hidden;
    defer cb.free(hidden);

    const last_hidden = if (hidden_result.total_rows == 1)
        hidden
    else
        try cb.sliceRows2D(allocator, hidden, hidden_result.total_rows - 1, 1, config.hidden_size);
    defer if (last_hidden != hidden) cb.free(last_hidden);

    const lm_w = if (config.weight_tying)
        try getEmbeddingWeight(cb, config)
    else
        cb.getWeight("lm_head.weight") catch try getEmbeddingWeight(cb, config);
    defer cb.free(lm_w);

    const logits = try cb.linearNoBias(last_hidden, lm_w, 1, config.hidden_size, config.vocab_size);
    defer cb.free(logits);
    try maybeDebugTensor(cb, allocator, "lm_head", logits);

    const result = try cb.toFloat32(logits, allocator);
    applyFinalLogitSoftcap(config, result);
    maybeDebugTopLogits(result, config.vocab_size);
    return result;
}

pub fn forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !CT {
    const hidden_result = try forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    if (hidden_result.total_rows != 1) {
        cb.free(hidden_result.hidden);
        return error.InvalidTensorShape;
    }
    return hidden_result.hidden;
}

pub fn forwardFinalHiddenLastRowFromPositionedEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !CT {
    const hidden_result = try forwardFinalHiddenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    if (hidden_result.total_rows != 1) {
        cb.free(hidden_result.hidden);
        return error.InvalidTensorShape;
    }
    return hidden_result.hidden;
}

pub fn applyFinalLogitSoftcapInPlace(config: Config, logits: []f32) void {
    applyFinalLogitSoftcap(config, logits);
}

fn forwardGreedyLastTokenTensorFromEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !GreedyDeviceTokenResult {
    const hidden_result = try forwardFinalHiddenTensorFromEmbeddings(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    return forwardGreedyLastTokenTensorFromFinalHidden(cb, allocator, config, hidden_result);
}

fn forwardGreedyLastTokenTensorFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !GreedyDeviceTokenResult {
    const hidden_result = try forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    return forwardGreedyLastTokenTensorFromFinalHidden(cb, allocator, config, hidden_result);
}

fn forwardGreedyLastTokenTensorFromPositionedEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !GreedyDeviceTokenResult {
    const hidden_result = try forwardFinalHiddenTensorFromPositionedEmbeddings(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    return forwardGreedyLastTokenTensorFromFinalHidden(cb, allocator, config, hidden_result);
}

fn forwardGreedyLastTokenTensorFromPositionedEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !GreedyDeviceTokenResult {
    const hidden_result = try forwardFinalHiddenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
    return forwardGreedyLastTokenTensorFromFinalHidden(cb, allocator, config, hidden_result);
}

fn forwardGreedyLastTokenTensorFromFinalHidden(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_result: HiddenTensorResult,
) !GreedyDeviceTokenResult {
    const hidden = hidden_result.hidden;
    defer cb.free(hidden);

    const lm_w = if (config.weight_tying)
        try getEmbeddingWeight(cb, config)
    else
        cb.getWeight("lm_head.weight") catch try getEmbeddingWeight(cb, config);
    defer cb.free(lm_w);

    if (try cb.linearNoBiasArgmaxLastRowTensor(hidden, lm_w, hidden_result.total_rows, config.hidden_size, config.vocab_size)) |token_tensor| {
        errdefer cb.free(token_tensor);
        const ids = try cb.toFloat32(token_tensor, allocator);
        defer allocator.free(ids);
        if (ids.len != 1 or ids[0] < 0) return error.InvalidTensorShape;
        return .{
            .token_id = @intFromFloat(ids[0]),
            .token_tensor = token_tensor,
        };
    }

    if (try cb.linearNoBiasArgmaxLastRow(hidden, lm_w, hidden_result.total_rows, config.hidden_size, config.vocab_size)) |token_id| {
        return .{ .token_id = @intCast(token_id) };
    }

    const logits = try cb.linearNoBias(hidden, lm_w, hidden_result.total_rows, config.hidden_size, config.vocab_size);
    defer cb.free(logits);
    try maybeDebugTensor(cb, allocator, "lm_head", logits);

    // Final logit softcapping is tanh-based and monotonic, so argmax is preserved.
    if (try cb.argmaxLastRow(logits, hidden_result.total_rows, config.vocab_size)) |token_id| {
        return .{ .token_id = @intCast(token_id) };
    }

    const result = try cb.toFloat32(logits, allocator);
    defer allocator.free(result);
    applyFinalLogitSoftcap(config, result);
    return .{
        .token_id = activations.argmax(result[(hidden_result.total_rows - 1) * config.vocab_size ..][0..config.vocab_size]),
    };
}

fn forwardLogitsTensorFromEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !LogitsTensorResult {
    const hidden_result = try forwardFinalHiddenTensorFromEmbeddings(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    const hidden = hidden_result.hidden;
    errdefer cb.free(hidden);

    // 5. LM head: project to vocab
    const lm_w = if (config.weight_tying)
        try getEmbeddingWeight(cb, config)
    else
        cb.getWeight("lm_head.weight") catch try getEmbeddingWeight(cb, config);
    defer cb.free(lm_w);
    const logits = try cb.linearNoBias(hidden, lm_w, hidden_result.total_rows, config.hidden_size, config.vocab_size);
    cb.free(hidden);
    try maybeDebugTensor(cb, allocator, "lm_head", logits);
    return .{
        .logits = logits,
        .total_rows = hidden_result.total_rows,
    };
}

fn forwardFinalHiddenTensorFromEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !HiddenTensorResult {
    return forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        .{},
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
}

pub fn forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !HiddenTensorResult {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;
    const position_offset = positionOffset(seq_len, query_seq_len, decode_context);
    var hidden = hidden_input;
    var owns_hidden = true;
    errdefer if (owns_hidden) cb.free(hidden);

    // 2. Position embeddings (absolute only; RoPE is applied inside attention)
    if (config.position_encoding == .absolute) {
        var pos_ids_buf: [2048]i64 = undefined;
        if (total > 2048) return error.SequenceTooLong;
        const pos_ids = pos_ids_buf[0..total];
        for (0..total) |i| pos_ids[i] = @intCast(position_offset + (i % query_seq_len));

        const pos_w = try cb.getWeight("wpe.weight");
        defer cb.free(pos_w);
        const pos_emb = try cb.embeddingLookup(pos_w, pos_ids, total, hidden_size);
        defer cb.free(pos_emb);

        const with_pos = try cb.add(hidden, pos_emb);
        if (with_pos != hidden) cb.free(hidden);
        hidden = with_pos;
    }

    owns_hidden = false;
    return forwardFinalHiddenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
}

pub fn forwardFinalAndPreNormHiddenTensorFromEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !FinalAndPreNormHiddenTensorResult {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;
    const position_offset = positionOffset(seq_len, query_seq_len, decode_context);
    var hidden = hidden_input;
    var owns_hidden = true;
    errdefer if (owns_hidden) cb.free(hidden);

    // 2. Position embeddings (absolute only; RoPE is applied inside attention)
    if (config.position_encoding == .absolute) {
        var pos_ids_buf: [2048]i64 = undefined;
        if (total > 2048) return error.SequenceTooLong;
        const pos_ids = pos_ids_buf[0..total];
        for (0..total) |i| pos_ids[i] = @intCast(position_offset + (i % query_seq_len));

        const pos_w = try cb.getWeight("wpe.weight");
        defer cb.free(pos_w);
        const pos_emb = try cb.embeddingLookup(pos_w, pos_ids, total, hidden_size);
        defer cb.free(pos_emb);

        const with_pos = try cb.add(hidden, pos_emb);
        if (with_pos != hidden) cb.free(hidden);
        hidden = with_pos;
    }

    var pre_norm_hidden: CT = undefined;
    owns_hidden = false;
    const final_result = try forwardFinalHiddenTensorFromPositionedEmbeddingsWithOptionalLayer0Overrides(
        cb,
        allocator,
        config,
        hidden,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
        &pre_norm_hidden,
    );
    errdefer cb.free(final_result.hidden);
    errdefer cb.free(pre_norm_hidden);

    return .{
        .final_hidden = final_result.hidden,
        .pre_norm_hidden = pre_norm_hidden,
        .total_rows = final_result.total_rows,
    };
}

fn forwardFinalHiddenTensorFromPositionedEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !HiddenTensorResult {
    return forwardFinalHiddenTensorFromPositionedEmbeddingsWithLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        .{},
        batch,
        seq_len,
        decode_context,
        ple_vectors,
    );
}

fn forwardFinalHiddenTensorFromPositionedEmbeddingsWithLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !HiddenTensorResult {
    return forwardFinalHiddenTensorFromPositionedEmbeddingsWithOptionalLayer0Overrides(
        cb,
        allocator,
        config,
        hidden_input,
        overrides,
        batch,
        seq_len,
        decode_context,
        ple_vectors,
        null,
    );
}

fn forwardFinalHiddenTensorFromPositionedEmbeddingsWithOptionalLayer0Overrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    overrides: Layer0DecoderOverrides,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
    pre_norm_out: ?*CT,
) !HiddenTensorResult {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;
    var hidden = hidden_input;
    var owns_hidden = true;

    if (config.family == .deepseek_v4 and config.deepseek_v4_hc_mult > 0) {
        return forwardDeepSeekV4FinalHiddenWithStreams(
            cb,
            allocator,
            config,
            hidden_input,
            batch,
            seq_len,
            decode_context,
            pre_norm_out,
        );
    }

    var layer0_attn_norm_pending = overrides.attn_norm;
    var layer0_fused_qkv_pending = overrides.fused_qkv;
    var layer0_q_pending = overrides.q;
    var layer0_k_pending = overrides.k;
    var layer0_v_pending = overrides.v;
    errdefer if (owns_hidden) cb.free(hidden);
    errdefer if (layer0_attn_norm_pending) |override| cb.free(override);
    errdefer if (layer0_fused_qkv_pending) |override| cb.free(override);
    errdefer if (layer0_q_pending) |override| cb.free(override);
    errdefer if (layer0_k_pending) |override| cb.free(override);
    errdefer if (layer0_v_pending) |override| cb.free(override);

    var reserved_hidden = try ReservedHiddenCarrier.init(cb, hidden, total, hidden_size);
    errdefer if (reserved_hidden) |*carrier| carrier.deinit(cb, false);
    if (reserved_hidden) |*carrier| {
        cb.free(hidden);
        hidden = carrier.active();
        owns_hidden = false;
    }

    // 3. Decoder blocks
    const eval_stride = decoderLayerEvalStride(config, decode_context);
    for (0..config.num_hidden_layers) |layer| {
        if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill) {
            debugPrint("prefill-trace: gpt decoderBlock layer={d} start\n", .{layer});
        }
        const num_kv_heads = config.effectiveKVHeadsForLayer(layer);
        const head_dim = config.effectiveHeadDimForLayer(layer);
        const new_hidden = try decoderBlock(
            cb,
            allocator,
            config,
            hidden,
            batch,
            seq_len,
            num_kv_heads,
            head_dim,
            layer,
            decode_context,
            ple_vectors,
            overrides,
            layer0_attn_norm_pending,
            layer0_fused_qkv_pending,
            layer0_q_pending,
            layer0_k_pending,
            layer0_v_pending,
        );
        if (layer == 0) layer0_attn_norm_pending = null;
        if (layer == 0) layer0_fused_qkv_pending = null;
        if (layer == 0) layer0_q_pending = null;
        if (layer == 0) layer0_k_pending = null;
        if (layer == 0) layer0_v_pending = null;
        if (reserved_hidden) |*carrier| {
            try carrier.replaceActive(cb, new_hidden);
            hidden = carrier.active();
            owns_hidden = false;
        } else {
            if (new_hidden != hidden) cb.free(hidden);
            hidden = new_hidden;
            owns_hidden = true;
        }
        if (cb.kind() == .metal and forceLayerCloneDebug()) {
            const cloned_hidden = try cloneTensorMaterialized(cb, allocator, hidden);
            if (owns_hidden) cb.free(hidden);
            hidden = cloned_hidden;
            owns_hidden = true;
        }
        // Materialize layer groups on lazy backends when the policy asks for a
        // bounded graph. Dense decode can leave this disabled and let the final
        // token read force evaluation of the full stack.
        if (shouldEvalDecoderLayer(eval_stride, layer, config.num_hidden_layers)) {
            const eval_started_at = monotonicNowNs();
            try cb.evalTensor(hidden);
            debug_timing_stats.eval_nanos += @intCast(monotonicNowNs() - eval_started_at);
            debug_timing_stats.eval_count += 1;
        }
        try maybeDebugLayerTensorLastRow(cb, allocator, layer, "out", hidden, hidden_size);
        try maybeDebugLayerTensor(cb, allocator, layer, "out", hidden);
        if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill) {
            debugPrint("prefill-trace: gpt decoderBlock layer={d} done\n", .{layer});
        }
    }

    var captured_pre_norm = false;
    errdefer if (captured_pre_norm) {
        if (pre_norm_out) |out| cb.free(out.*);
    };
    if (pre_norm_out) |out| {
        out.* = try cloneTensorMaterialized(cb, allocator, hidden);
        captured_pre_norm = true;
    }

    // 4. Final layer norm
    if (!is_freestanding and prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill) {
        debugPrint("prefill-trace: gpt final norm start\n", .{});
    }
    const final_hidden = try applyFinalNorm(cb, allocator, config, hidden);
    if (reserved_hidden) |*carrier| {
        carrier.deinit(cb, false);
        reserved_hidden = null;
        owns_hidden = true;
    } else if (final_hidden != hidden) {
        cb.free(hidden);
    }
    hidden = final_hidden;
    if (!is_freestanding and prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill) {
        debugPrint("prefill-trace: gpt final norm done\n", .{});
    }
    try maybeDebugTensorLastRow(cb, allocator, "final_norm", hidden, hidden_size);
    try maybeDebugTensor(cb, allocator, "final_norm_full", hidden);

    return .{
        .hidden = hidden,
        .total_rows = total,
    };
}

/// Run encoder-style forward pass (return hidden states, not logits).
/// Used for embedding extraction from decoder-only models.
pub fn hiddenForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
) ![]f32 {
    const hidden = try hiddenForwardResident(cb, allocator, config, input_ids, batch, seq_len, decode_context);
    defer cb.free(hidden);
    return cb.toFloat32(hidden, allocator);
}

pub fn hiddenForwardResident(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
) !CT {
    return hiddenForwardResidentWithOverrides(cb, allocator, config, input_ids, batch, seq_len, decode_context, .{});
}

pub fn hiddenForwardResidentWithOverrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    raw_overrides: Layer0DecoderOverrides,
) !CT {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;

    const embed_w = try getEmbeddingWeight(cb, config);
    defer cb.free(embed_w);
    const embedded = try cb.embeddingLookup(embed_w, input_ids, total, hidden_size);
    try maybeDebugTensorLastRow(cb, allocator, "embed", embedded, hidden_size);
    const hidden = try maybeScaleTokenEmbeddings(cb, allocator, config, embedded, total, hidden_size);
    if (hidden != embedded) try maybeDebugTensorLastRow(cb, allocator, "embed_scaled", hidden, hidden_size);

    const ple_vectors = try computePleVectors(cb, allocator, config, input_ids, hidden, total);
    defer if (ple_vectors) |pv| cb.free(pv);

    return hiddenForwardFromEmbeddingsResidentWithOverrides(cb, allocator, config, hidden, batch, seq_len, decode_context, ple_vectors, raw_overrides);
}

pub fn hiddenForwardFromEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) ![]f32 {
    const hidden = try hiddenForwardFromEmbeddingsResident(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors);
    defer cb.free(hidden);
    return cb.toFloat32(hidden, allocator);
}

pub fn hiddenForwardFromEmbeddingsResident(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !CT {
    return hiddenForwardFromEmbeddingsResidentWithOverrides(cb, allocator, config, hidden_input, batch, seq_len, decode_context, ple_vectors, .{});
}

pub fn hiddenForwardFromEmbeddingsResidentWithOverrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
    raw_overrides: Layer0DecoderOverrides,
) !CT {
    const hidden_size = config.hidden_size;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;
    var hidden = hidden_input;
    errdefer cb.free(hidden);

    // 2. Position embeddings (absolute only)
    if (config.position_encoding == .absolute) {
        const position_offset = positionOffset(seq_len, query_seq_len, decode_context);
        var pos_ids_buf: [2048]i64 = undefined;
        if (total > 2048) return error.SequenceTooLong;
        const pos_ids = pos_ids_buf[0..total];
        for (0..total) |i| pos_ids[i] = @intCast(position_offset + (i % query_seq_len));

        const pos_w = try cb.getWeight("wpe.weight");
        defer cb.free(pos_w);
        const pos_emb = try cb.embeddingLookup(pos_w, pos_ids, total, hidden_size);
        defer cb.free(pos_emb);

        const with_pos = try cb.add(hidden, pos_emb);
        if (with_pos != hidden) cb.free(hidden);
        hidden = with_pos;
    }

    // 3. Decoder blocks
    const eval_stride = decoderLayerEvalStride(config, decode_context);
    for (0..config.num_hidden_layers) |layer| {
        const num_kv_heads = config.effectiveKVHeadsForLayer(layer);
        const head_dim = config.effectiveHeadDimForLayer(layer);
        const new_hidden = try decoderBlock(
            cb,
            allocator,
            config,
            hidden,
            batch,
            seq_len,
            num_kv_heads,
            head_dim,
            layer,
            decode_context,
            ple_vectors,
            raw_overrides,
            null,
            null,
            null,
            null,
            null,
        );
        if (new_hidden != hidden) cb.free(hidden);
        hidden = new_hidden;
        if (cb.kind() == .metal and forceLayerCloneDebug()) {
            const cloned_hidden = try cloneTensorMaterialized(cb, allocator, hidden);
            cb.free(hidden);
            hidden = cloned_hidden;
        }
        if (shouldEvalDecoderLayer(eval_stride, layer, config.num_hidden_layers)) {
            try cb.evalTensor(hidden);
        }
    }

    // 4. Final layer norm
    const final_hidden = try applyFinalNorm(cb, allocator, config, hidden);
    if (final_hidden != hidden) cb.free(hidden);
    hidden = final_hidden;

    return hidden;
}

// --- Decoder block ---

fn deepSeekV4DecoderBlockAfterAttnNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    normed: CT,
    hidden: CT,
    batch: usize,
    seq_len: usize,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    decode_context: ?*const DecodeContext,
    name_buf: *[256]u8,
) !CT {
    const path = try deepseek_v4.attentionPathForLayer(config, layer);
    return deepSeekV4AttentionDecoderBlock(
        cb,
        allocator,
        config,
        normed,
        hidden,
        batch,
        seq_len,
        num_kv_heads,
        head_dim,
        layer,
        path,
        decode_context,
        name_buf,
    );
}

fn deepSeekV4AttentionDecoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    normed: CT,
    hidden: CT,
    batch: usize,
    seq_len: usize,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    path: deepseek_v4.AttentionPath,
    decode_context: ?*const DecodeContext,
    name_buf: *[256]u8,
) !CT {
    var runtime_adapter = DeepSeekV4RuntimeAdapter.init(cb, config);
    const deepseek_runtime = runtime_adapter.runtime();

    const projected = try deepSeekV4AttentionUpdate(cb, allocator, config, deepseek_runtime, normed, batch, seq_len, num_kv_heads, head_dim, layer, path, decode_context, name_buf);
    defer cb.free(projected);

    const total = batch * actualQuerySeqLen(seq_len, decode_context);
    const attn_res = try deepseek_v4.applyLayerHyperResidualFallback(cb, allocator, config, deepseek_runtime, hidden, projected, total, layer, "attn_hc", name_buf);
    errdefer cb.free(attn_res);

    const ffn_normed = try applyFFNNorm(cb, allocator, config, attn_res, layer, name_buf);
    defer cb.free(ffn_normed);
    const ffn_out = try deepseek_v4.feedForward(cb, allocator, config, deepseek_runtime, ffn_normed, total, layer, name_buf, deepSeekV4MoeContext(decode_context));
    defer cb.free(ffn_out);

    const result = try deepseek_v4.applyLayerHyperResidualFallback(cb, allocator, config, deepseek_runtime, attn_res, ffn_out, total, layer, "ffn_hc", name_buf);
    cb.free(attn_res);
    return result;
}

fn deepSeekV4AttentionUpdate(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    deepseek_runtime: deepseek_v4.Runtime,
    normed: CT,
    batch: usize,
    seq_len: usize,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    path: deepseek_v4.AttentionPath,
    decode_context: ?*const DecodeContext,
    name_buf: *[256]u8,
) !CT {
    const weights = try deepseek_v4.slidingWeights(cb, deepseek_runtime, layer, name_buf);
    defer weights.deinit(cb);

    const shape = try deepseek_v4.validateSlidingWeightShapes(
        cb,
        allocator,
        config,
        weights,
        num_kv_heads,
        head_dim,
    );

    const total = batch * actualQuerySeqLen(seq_len, decode_context);
    const hidden_size: usize = @intCast(config.hidden_size);
    const num_heads: usize = @intCast(config.num_attention_heads);
    const q_dim = num_heads * @as(usize, head_dim);

    const q_a = try cb.linearNoBias(normed, weights.q_a, total, hidden_size, shape.q_lora_rank);
    defer cb.free(q_a);
    const q_a_normed = try cb.rmsNorm(q_a, weights.q_a_norm, shape.q_lora_rank, config.norm_eps);
    defer cb.free(q_a_normed);
    const q_projected = try cb.linearNoBias(q_a_normed, weights.q_b, total, shape.q_lora_rank, q_dim);
    defer cb.free(q_projected);
    const q_normed = try deepseek_v4.unweightedRmsHeads(cb, allocator, q_projected, total, num_heads, head_dim, config.norm_eps);
    defer cb.free(q_normed);

    const kv_projected = try cb.linearNoBias(normed, weights.kv, total, hidden_size, shape.kv_cache_width);
    defer cb.free(kv_projected);
    const kv_normed = try cb.rmsNorm(kv_projected, weights.kv_norm, shape.kv_cache_width, config.norm_eps);
    defer cb.free(kv_normed);

    const rope_dim: usize = @intCast(config.deepseek_v4_qk_rope_head_dim);
    const rope_theta = config.layerRopeTheta(layer);
    const sequence_context = deepSeekV4SequenceContext(decode_context);
    const q_rope = try deepseek_v4.applyTrailingRopeTensor(cb, allocator, q_normed, batch, seq_len, num_heads, head_dim, rope_dim, rope_theta, sequence_context, false);
    defer cb.free(q_rope);
    const k_rope = try deepseek_v4.applyTrailingRopeTensor(cb, allocator, kv_normed, batch, seq_len, @intCast(num_kv_heads), head_dim, rope_dim, rope_theta, sequence_context, false);
    defer cb.free(k_rope);

    var attention_config = config;
    attention_config.position_encoding = .absolute;
    attention_config.sliding_window = if (path == .sliding) config.sliding_window else 0;
    const attn_started_at = monotonicNowNs();
    const attn_out_rotated = if (path == .sliding)
        try applyAttentionWithSink(cb, attention_config, q_rope, k_rope, k_rope, weights.sinks, batch, seq_len, config.num_attention_heads, num_kv_heads, head_dim, layer, layer, false, decode_context)
    else
        try deepSeekV4CompressedAttentionReference(cb, allocator, config, normed, q_a_normed, q_rope, k_rope, weights.sinks, batch, seq_len, total, num_heads, @intCast(num_kv_heads), @intCast(head_dim), layer, path, decode_context, name_buf);
    defer cb.free(attn_out_rotated);
    debug_timing_stats.attention_core_nanos += @intCast(monotonicNowNs() - attn_started_at);

    const attn_out = try deepseek_v4.applyTrailingRopeTensor(cb, allocator, attn_out_rotated, batch, seq_len, num_heads, head_dim, rope_dim, rope_theta, sequence_context, true);
    defer cb.free(attn_out);
    return deepseek_v4.groupedOutputProject(cb, allocator, attn_out, weights.o_a, weights.o_b, total, num_heads, head_dim, shape.o_lora_rank, hidden_size, config.deepseek_v4_o_groups);
}

const DeepSeekV4CompressedPool = struct {
    data: []f32,
    positions: []u32,

    fn deinit(self: DeepSeekV4CompressedPool, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
        allocator.free(self.positions);
    }
};

fn deepSeekV4CompressedAttentionReference(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    normed: CT,
    q_lora: CT,
    q_rope: CT,
    local_k_rope: CT,
    sinks: CT,
    batch: usize,
    seq_len: usize,
    total: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    layer: usize,
    path: deepseek_v4.AttentionPath,
    decode_context: ?*const DecodeContext,
    name_buf: *[256]u8,
) !CT {
    if (decode_context) |dc| {
        if (dc.deepseek_v4_compressed_cache) |cache| {
            if (dc.isMixedBatch()) return error.UnsupportedDeepSeekV4MixedBatchCompressedAttention;
            if (dc.query_sequence_len == 0 or dc.query_sequence_len > dc.total_sequence_len) return error.DeepSeekV4TensorShapeMismatch;
            const layer_state = try cache.layer(layer);
            if (try deepSeekV4CompressedAttentionDeviceFastPath(
                cb,
                allocator,
                config,
                @intFromPtr(cache),
                normed,
                q_lora,
                q_rope,
                local_k_rope,
                sinks,
                batch,
                total,
                num_heads,
                num_kv_heads,
                head_dim,
                layer,
                path,
                dc,
                name_buf,
            )) |fast| {
                layer_state.device_resident = true;
                layer_state.token_count = dc.total_sequence_len;
                return fast;
            }
            if (layer_state.device_resident and dc.total_sequence_len > dc.query_sequence_len) {
                return error.DeepSeekV4CompressedDeviceCacheUnavailable;
            }
            if (!layer_state.device_resident and layer_state.token_count != 0 and layer_state.token_count != dc.total_sequence_len - dc.query_sequence_len) {
                return error.DeepSeekV4CompressedCacheStateMismatch;
            }
            return deepSeekV4CompressedAttentionCached(
                cb,
                allocator,
                config,
                cache,
                normed,
                q_lora,
                q_rope,
                local_k_rope,
                sinks,
                batch,
                seq_len,
                total,
                num_heads,
                num_kv_heads,
                head_dim,
                layer,
                path,
                dc,
                name_buf,
            );
        }
        if (dc.usesPagedKv()) return error.DeepSeekV4CompressedPagedAttentionRequiresV4CacheState;
        if (dc.isMixedBatch()) return error.UnsupportedDeepSeekV4MixedBatchCompressedAttention;
    }
    if (batch == 0 or total != batch * seq_len) return error.DeepSeekV4TensorShapeMismatch;
    if (num_kv_heads != 1) return error.DeepSeekV4TensorShapeMismatch;

    const compress_rate: usize = @intCast(config.deepseekV4CompressRateForLayer(layer));
    if (compress_rate <= 1) return error.DeepSeekV4TensorShapeMismatch;

    var compressor_pool = try deepSeekV4BuildCompressedPool(cb, allocator, config, normed, batch, seq_len, layer, "compressor", compress_rate, head_dim, config.layerRopeTheta(layer), name_buf);
    defer compressor_pool.deinit(allocator);

    var index_pool: ?DeepSeekV4CompressedPool = null;
    var index_query: ?[]f32 = null;
    var index_head_weights: ?[]f32 = null;
    defer if (index_pool) |pool| pool.deinit(allocator);
    defer if (index_query) |buf| allocator.free(buf);
    defer if (index_head_weights) |buf| allocator.free(buf);
    if (path == .compressed_sparse and config.deepseek_v4_index_n_heads > 0 and config.deepseek_v4_index_head_dim > 0) {
        index_pool = try deepSeekV4BuildCompressedPool(cb, allocator, config, normed, batch, seq_len, layer, "compressor.indexer", compress_rate, @intCast(config.deepseek_v4_index_head_dim), config.layerRopeTheta(layer), name_buf);
        index_query = try deepSeekV4BuildIndexerQuery(cb, allocator, config, q_lora, total, layer, name_buf);
        index_head_weights = try deepSeekV4BuildIndexerHeadWeights(cb, allocator, config, normed, total, layer, name_buf);
    }

    const q_data = try cb.toFloat32(q_rope, allocator);
    defer allocator.free(q_data);
    const local_k_data = try cb.toFloat32(local_k_rope, allocator);
    defer allocator.free(local_k_data);
    const sink_data = try cb.toFloat32(sinks, allocator);
    defer allocator.free(sink_data);
    if (q_data.len != total * num_heads * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (local_k_data.len != total * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (sink_data.len != num_heads) return error.DeepSeekV4TensorShapeMismatch;

    const out = try allocator.alloc(f32, q_data.len);
    defer allocator.free(out);
    const top_k: usize = if (path == .compressed_sparse and config.deepseek_v4_index_topk > 0)
        @intCast(config.deepseek_v4_index_topk)
    else
        std.math.maxInt(usize);

    try deepSeekV4CompressedAttentionRows(
        allocator,
        out,
        q_data,
        local_k_data,
        compressor_pool.data,
        compressor_pool.positions,
        if (index_pool) |pool| pool.data else null,
        if (index_pool) |pool| pool.positions else null,
        index_query,
        index_head_weights,
        sink_data,
        batch,
        seq_len,
        num_heads,
        head_dim,
        if (config.sliding_window > 0) @as(usize, @intCast(config.sliding_window)) else seq_len,
        top_k,
        if (config.deepseek_v4_index_n_heads > 0) @as(usize, @intCast(config.deepseek_v4_index_n_heads)) else 0,
        if (config.deepseek_v4_index_head_dim > 0) @as(usize, @intCast(config.deepseek_v4_index_head_dim)) else 0,
    );
    const shape = [_]i32{ @intCast(total), @intCast(num_heads * head_dim) };
    return cb.fromFloat32Shape(out, &shape);
}

fn deepSeekV4CompressedAttentionCached(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    cache: *DeepSeekV4CompressedCache,
    normed: CT,
    q_lora: CT,
    q_rope: CT,
    local_k_rope: CT,
    sinks: CT,
    batch: usize,
    seq_len: usize,
    total: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    layer: usize,
    path: deepseek_v4.AttentionPath,
    decode_context: *const DecodeContext,
    name_buf: *[256]u8,
) !CT {
    _ = seq_len;
    if (batch != 1) return error.UnsupportedDeepSeekV4MixedBatchCompressedAttention;
    if (num_kv_heads != 1) return error.DeepSeekV4TensorShapeMismatch;
    const query_rows = decode_context.query_sequence_len;
    if (total != query_rows) return error.DeepSeekV4TensorShapeMismatch;
    const total_tokens = decode_context.total_sequence_len;
    if (query_rows == 0 or query_rows > total_tokens) return error.DeepSeekV4TensorShapeMismatch;
    const query_abs_start = total_tokens - query_rows;

    const compress_rate: usize = @intCast(config.deepseekV4CompressRateForLayer(layer));
    if (compress_rate <= 1) return error.DeepSeekV4TensorShapeMismatch;

    const layer_state = try cache.layer(layer);
    if (layer_state.device_resident and query_abs_start != 0) return error.DeepSeekV4CompressedDeviceCacheUnavailable;
    if (query_abs_start == 0 and layer_state.device_resident) layer_state.reset();
    layer_state.device_resident = false;
    try layer_state.ensureLocalCapacity(allocator, total_tokens, head_dim);

    const local_k_data = try cb.toFloat32(local_k_rope, allocator);
    defer allocator.free(local_k_data);
    if (local_k_data.len != query_rows * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    for (0..query_rows) |row| {
        const dst = (query_abs_start + row) * head_dim;
        @memcpy(layer_state.local_kv[dst..][0..head_dim], local_k_data[row * head_dim ..][0..head_dim]);
    }
    layer_state.token_count = @max(layer_state.token_count, total_tokens);

    const compressed_rows = try deepSeekV4UpdateCompressedComponent(
        cb,
        allocator,
        config,
        normed,
        query_rows,
        layer,
        "compressor",
        compress_rate,
        head_dim,
        config.layerRopeTheta(layer),
        query_abs_start,
        total_tokens,
        &layer_state.compressor,
        name_buf,
    );
    layer_state.compressed_rows = compressed_rows;

    var index_rows: usize = 0;
    var index_query: ?[]f32 = null;
    var index_head_weights: ?[]f32 = null;
    defer if (index_query) |buf| allocator.free(buf);
    defer if (index_head_weights) |buf| allocator.free(buf);
    if (path == .compressed_sparse and config.deepseek_v4_index_n_heads > 0 and config.deepseek_v4_index_head_dim > 0) {
        index_rows = try deepSeekV4UpdateCompressedComponent(
            cb,
            allocator,
            config,
            normed,
            query_rows,
            layer,
            "compressor.indexer",
            compress_rate,
            @intCast(config.deepseek_v4_index_head_dim),
            config.layerRopeTheta(layer),
            query_abs_start,
            total_tokens,
            &layer_state.indexer,
            name_buf,
        );
        layer_state.index_rows = index_rows;
        index_query = try deepSeekV4BuildIndexerQuery(cb, allocator, config, q_lora, query_rows, layer, name_buf);
        index_head_weights = try deepSeekV4BuildIndexerHeadWeights(cb, allocator, config, normed, query_rows, layer, name_buf);
    }

    const q_data = try cb.toFloat32(q_rope, allocator);
    defer allocator.free(q_data);
    const sink_data = try cb.toFloat32(sinks, allocator);
    defer allocator.free(sink_data);
    if (q_data.len != query_rows * num_heads * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (sink_data.len != num_heads) return error.DeepSeekV4TensorShapeMismatch;

    const out = try allocator.alloc(f32, q_data.len);
    defer allocator.free(out);
    const top_k: usize = if (path == .compressed_sparse and config.deepseek_v4_index_topk > 0)
        @intCast(config.deepseek_v4_index_topk)
    else
        std.math.maxInt(usize);

    try deepSeekV4CompressedAttentionCachedRows(
        allocator,
        out,
        q_data,
        layer_state.local_kv[0 .. layer_state.token_count * head_dim],
        layer_state.compressor.compressed[0 .. compressed_rows * head_dim],
        layer_state.compressor.positions[0..compressed_rows],
        if (index_rows > 0) layer_state.indexer.compressed[0 .. index_rows * @as(usize, @intCast(config.deepseek_v4_index_head_dim))] else null,
        if (index_rows > 0) layer_state.indexer.positions[0..index_rows] else null,
        index_query,
        index_head_weights,
        sink_data,
        query_abs_start,
        query_rows,
        layer_state.token_count,
        num_heads,
        head_dim,
        if (config.sliding_window > 0) @as(usize, @intCast(config.sliding_window)) else layer_state.token_count,
        top_k,
        if (config.deepseek_v4_index_n_heads > 0) @as(usize, @intCast(config.deepseek_v4_index_n_heads)) else 0,
        if (config.deepseek_v4_index_head_dim > 0) @as(usize, @intCast(config.deepseek_v4_index_head_dim)) else 0,
    );
    const shape = [_]i32{ @intCast(query_rows), @intCast(num_heads * head_dim) };
    return cb.fromFloat32Shape(out, &shape);
}

fn deepSeekV4CompressedAttentionDeviceFastPath(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    cache_key: usize,
    normed: CT,
    q_lora: CT,
    q_rope: CT,
    local_k_rope: CT,
    sinks: CT,
    batch: usize,
    total: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    layer: usize,
    path: deepseek_v4.AttentionPath,
    decode_context: *const DecodeContext,
    name_buf: *[256]u8,
) !?CT {
    if (cb.kind() != .metal) return null;
    if (batch != 1 or num_kv_heads != 1) return null;
    const query_rows = decode_context.query_sequence_len;
    if (total != query_rows) return error.DeepSeekV4TensorShapeMismatch;
    const total_tokens = decode_context.total_sequence_len;
    if (query_rows == 0 or query_rows > total_tokens) return error.DeepSeekV4TensorShapeMismatch;
    const query_abs_start = total_tokens - query_rows;
    const compress_rate: usize = @intCast(config.deepseekV4CompressRateForLayer(layer));
    if (compress_rate <= 1) return error.DeepSeekV4TensorShapeMismatch;
    const hidden_size: usize = @intCast(config.hidden_size);

    const comp_kv_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.kv_proj.weight", .{layer}) catch return error.NameTooLong;
    const comp_kv_w = try getModelWeight(cb, config, comp_kv_name);
    defer cb.free(comp_kv_w);
    const comp_gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.gate_proj.weight", .{layer}) catch return error.NameTooLong;
    const comp_gate_w = try getModelWeight(cb, config, comp_gate_name);
    defer cb.free(comp_gate_w);
    const comp_bias_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.position_bias", .{layer}) catch return error.NameTooLong;
    const comp_bias = try getModelWeight(cb, config, comp_bias_name);
    defer cb.free(comp_bias);
    const comp_norm_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.kv_norm.weight", .{layer}) catch return error.NameTooLong;
    const comp_norm = try getModelWeight(cb, config, comp_norm_name);
    defer cb.free(comp_norm);

    const comp_gate_shape = try deepseek_v4.tensorShape(cb, allocator, comp_gate_w);
    defer allocator.free(comp_gate_shape);
    try deepseek_v4.expectRank(comp_gate_shape, 2);
    const comp_gate_width = try deepseek_v4.positiveDim(comp_gate_shape, 0);
    try deepseek_v4.expectGateWidth(comp_gate_width, compress_rate);
    const comp_projected = try cb.linearNoBias(normed, comp_kv_w, query_rows, hidden_size, head_dim);
    defer cb.free(comp_projected);
    const comp_gate_projected = try cb.linearNoBias(normed, comp_gate_w, query_rows, hidden_size, comp_gate_width);
    defer cb.free(comp_gate_projected);

    var index_component: ?ops.DeepSeekV4CompressedComponentRequest = null;
    var index_projected: ?CT = null;
    var index_gate_projected: ?CT = null;
    var index_bias: ?CT = null;
    var index_norm: ?CT = null;
    var index_query: ?CT = null;
    var index_head_weights: ?CT = null;
    defer if (index_projected) |tensor| cb.free(tensor);
    defer if (index_gate_projected) |tensor| cb.free(tensor);
    defer if (index_bias) |tensor| cb.free(tensor);
    defer if (index_norm) |tensor| cb.free(tensor);
    defer if (index_query) |tensor| cb.free(tensor);
    defer if (index_head_weights) |tensor| cb.free(tensor);

    if (path == .compressed_sparse and config.deepseek_v4_index_n_heads > 0 and config.deepseek_v4_index_head_dim > 0) {
        const index_dim: usize = @intCast(config.deepseek_v4_index_head_dim);
        const idx_kv_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.kv_proj.weight", .{layer}) catch return error.NameTooLong;
        const idx_kv_w = try getModelWeight(cb, config, idx_kv_name);
        defer cb.free(idx_kv_w);
        const idx_gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.gate_proj.weight", .{layer}) catch return error.NameTooLong;
        const idx_gate_w = try getModelWeight(cb, config, idx_gate_name);
        defer cb.free(idx_gate_w);
        const idx_bias_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.position_bias", .{layer}) catch return error.NameTooLong;
        index_bias = try getModelWeight(cb, config, idx_bias_name);
        const idx_norm_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.kv_norm.weight", .{layer}) catch return error.NameTooLong;
        index_norm = try getModelWeight(cb, config, idx_norm_name);
        const idx_gate_shape = try deepseek_v4.tensorShape(cb, allocator, idx_gate_w);
        defer allocator.free(idx_gate_shape);
        try deepseek_v4.expectRank(idx_gate_shape, 2);
        const idx_gate_width = try deepseek_v4.positiveDim(idx_gate_shape, 0);
        try deepseek_v4.expectGateWidth(idx_gate_width, compress_rate);
        index_projected = try cb.linearNoBias(normed, idx_kv_w, query_rows, hidden_size, index_dim);
        index_gate_projected = try cb.linearNoBias(normed, idx_gate_w, query_rows, hidden_size, idx_gate_width);

        const idx_q_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.q_b_proj.weight", .{layer}) catch return error.NameTooLong;
        const idx_q_w = try getModelWeight(cb, config, idx_q_name);
        defer cb.free(idx_q_w);
        const idx_weight_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.weights_proj.weight", .{layer}) catch return error.NameTooLong;
        const idx_weight_w = try getModelWeight(cb, config, idx_weight_name);
        defer cb.free(idx_weight_w);
        index_query = try cb.linearNoBias(q_lora, idx_q_w, query_rows, @intCast(config.deepseek_v4_q_lora_rank), @as(usize, @intCast(config.deepseek_v4_index_n_heads)) * index_dim);
        index_head_weights = try cb.linearNoBias(normed, idx_weight_w, query_rows, hidden_size, @intCast(config.deepseek_v4_index_n_heads));

        index_component = .{
            .projected = index_projected.?,
            .gate = index_gate_projected.?,
            .bias = index_bias.?,
            .norm = index_norm.?,
            .row_dim = index_dim,
            .gate_width = idx_gate_width,
        };
    }

    return cb.runDeepSeekV4CompressedAttention(&.{
        .cache_key = cache_key,
        .layer_index = layer,
        .path = switch (path) {
            .heavily_compressed => .heavily_compressed,
            .compressed_sparse => .compressed_sparse,
            .sliding => return null,
        },
        .q = q_rope,
        .local_kv = local_k_rope,
        .sinks = sinks,
        .compressor = .{
            .projected = comp_projected,
            .gate = comp_gate_projected,
            .bias = comp_bias,
            .norm = comp_norm,
            .row_dim = head_dim,
            .gate_width = comp_gate_width,
        },
        .indexer = index_component,
        .index_query = index_query,
        .index_head_weights = index_head_weights,
        .query_abs_start = query_abs_start,
        .query_rows = query_rows,
        .total_tokens = total_tokens,
        .num_heads = num_heads,
        .head_dim = head_dim,
        .sliding_window = if (config.sliding_window > 0) @intCast(config.sliding_window) else total_tokens,
        .compress_rate = compress_rate,
        .top_k = if (path == .compressed_sparse and config.deepseek_v4_index_topk > 0) @intCast(config.deepseek_v4_index_topk) else 0,
        .index_heads = if (config.deepseek_v4_index_n_heads > 0) @intCast(config.deepseek_v4_index_n_heads) else 0,
        .index_head_dim = if (config.deepseek_v4_index_head_dim > 0) @intCast(config.deepseek_v4_index_head_dim) else 0,
        .rope_dim = @intCast(config.deepseek_v4_qk_rope_head_dim),
        .rope_theta = config.layerRopeTheta(layer),
        .rope_freq_scale = 1.0,
        .rope_consecutive_pairs = true,
        .eps = config.norm_eps,
    });
}

fn deepSeekV4UpdateCompressedComponent(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    normed: CT,
    query_rows: usize,
    layer: usize,
    prefix: []const u8,
    compress_rate: usize,
    row_dim: usize,
    rope_theta: f32,
    query_abs_start: usize,
    total_tokens: usize,
    component: *DeepSeekV4CompressedCache.Component,
    name_buf: *[256]u8,
) !usize {
    const hidden_size: usize = @intCast(config.hidden_size);
    const kv_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.kv_proj.weight", .{ layer, prefix }) catch return error.NameTooLong;
    const kv_w = try getModelWeight(cb, config, kv_name);
    defer cb.free(kv_w);
    const gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.gate_proj.weight", .{ layer, prefix }) catch return error.NameTooLong;
    const gate_w = try getModelWeight(cb, config, gate_name);
    defer cb.free(gate_w);
    const bias_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.position_bias", .{ layer, prefix }) catch return error.NameTooLong;
    const bias_w = try getModelWeight(cb, config, bias_name);
    defer cb.free(bias_w);
    const norm_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.kv_norm.weight", .{ layer, prefix }) catch return error.NameTooLong;
    const norm_w = try getModelWeight(cb, config, norm_name);
    defer cb.free(norm_w);

    const gate_shape = try deepseek_v4.tensorShape(cb, allocator, gate_w);
    defer allocator.free(gate_shape);
    try deepseek_v4.expectRank(gate_shape, 2);
    const gate_width = try deepseek_v4.positiveDim(gate_shape, 0);
    try deepseek_v4.expectGateWidth(gate_width, compress_rate);

    const row_count = DeepSeekV4CompressedCache.rowsForTokens(total_tokens, compress_rate);
    try component.ensureCapacity(allocator, total_tokens, row_count, row_dim, gate_width);

    const kv_projected = try cb.linearNoBias(normed, kv_w, query_rows, hidden_size, row_dim);
    defer cb.free(kv_projected);
    const gate_projected = try cb.linearNoBias(normed, gate_w, query_rows, hidden_size, gate_width);
    defer cb.free(gate_projected);

    const kv_data = try cb.toFloat32(kv_projected, allocator);
    defer allocator.free(kv_data);
    const gate_data = try cb.toFloat32(gate_projected, allocator);
    defer allocator.free(gate_data);
    const bias_data = try cb.toFloat32(bias_w, allocator);
    defer allocator.free(bias_data);
    const norm_data = try cb.toFloat32(norm_w, allocator);
    defer allocator.free(norm_data);
    if (kv_data.len != query_rows * row_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (gate_data.len != query_rows * gate_width) return error.DeepSeekV4TensorShapeMismatch;
    if (norm_data.len != row_dim) return error.DeepSeekV4TensorShapeMismatch;

    for (0..query_rows) |row| {
        const abs_pos = query_abs_start + row;
        @memcpy(component.projected[abs_pos * row_dim ..][0..row_dim], kv_data[row * row_dim ..][0..row_dim]);
        @memcpy(component.gate[abs_pos * gate_width ..][0..gate_width], gate_data[row * gate_width ..][0..gate_width]);
    }

    const first_block = query_abs_start / compress_rate;
    const last_block = (total_tokens - 1) / compress_rate;
    for (first_block..last_block + 1) |block| {
        const start = block * compress_rate;
        const end = @min(total_tokens, start + compress_rate);
        const out_row = block * row_dim;
        @memset(component.compressed[out_row..][0..row_dim], 0.0);
        var denom: f32 = 0.0;
        for (start..end) |pos| {
            const gate_slot = if (gate_width == compress_rate) pos - start else 0;
            const gate = deepseek_v4_host.sigmoid(component.gate[pos * gate_width + gate_slot]);
            denom += gate;
            for (0..row_dim) |d| {
                const bias = if (bias_data.len == compress_rate * row_dim)
                    bias_data[(pos - start) * row_dim + d]
                else if (bias_data.len == row_dim)
                    bias_data[d]
                else
                    0.0;
                component.compressed[out_row + d] += gate * (component.projected[pos * row_dim + d] + bias);
            }
        }
        if (denom > 0.0) {
            for (0..row_dim) |d| component.compressed[out_row + d] /= denom;
        }
        deepSeekV4RmsNormRow(component.compressed[out_row..][0..row_dim], norm_data, config.norm_eps);
        component.positions[block] = @intCast(@min(total_tokens - 1, end - 1));
        const positions = [_]u32{component.positions[block]};
        deepseek_v4_host.applyTrailingRopeRows(component.compressed[out_row..][0..row_dim], &positions, 1, row_dim, @intCast(config.deepseek_v4_qk_rope_head_dim), rope_theta);
    }
    return row_count;
}

fn deepSeekV4BuildCompressedPool(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    normed: CT,
    batch: usize,
    seq_len: usize,
    layer: usize,
    prefix: []const u8,
    compress_rate: usize,
    head_dim: usize,
    rope_theta: f32,
    name_buf: *[256]u8,
) !DeepSeekV4CompressedPool {
    const hidden_size: usize = @intCast(config.hidden_size);
    const kv_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.kv_proj.weight", .{ layer, prefix }) catch return error.NameTooLong;
    const kv_w = try getModelWeight(cb, config, kv_name);
    defer cb.free(kv_w);
    const gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.gate_proj.weight", .{ layer, prefix }) catch return error.NameTooLong;
    const gate_w = try getModelWeight(cb, config, gate_name);
    defer cb.free(gate_w);
    const bias_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.position_bias", .{ layer, prefix }) catch return error.NameTooLong;
    const bias_w = try getModelWeight(cb, config, bias_name);
    defer cb.free(bias_w);
    const norm_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.{s}.kv_norm.weight", .{ layer, prefix }) catch return error.NameTooLong;
    const norm_w = try getModelWeight(cb, config, norm_name);
    defer cb.free(norm_w);

    const gate_shape = try deepseek_v4.tensorShape(cb, allocator, gate_w);
    defer allocator.free(gate_shape);
    try deepseek_v4.expectRank(gate_shape, 2);
    const gate_width = try deepseek_v4.positiveDim(gate_shape, 0);
    try deepseek_v4.expectGateWidth(gate_width, compress_rate);

    const total = batch * seq_len;
    const kv_projected = try cb.linearNoBias(normed, kv_w, total, hidden_size, head_dim);
    defer cb.free(kv_projected);
    const gate_projected = try cb.linearNoBias(normed, gate_w, total, hidden_size, gate_width);
    defer cb.free(gate_projected);

    const kv_data = try cb.toFloat32(kv_projected, allocator);
    defer allocator.free(kv_data);
    const gate_data = try cb.toFloat32(gate_projected, allocator);
    defer allocator.free(gate_data);
    const bias_data = try cb.toFloat32(bias_w, allocator);
    defer allocator.free(bias_data);
    const norm_data = try cb.toFloat32(norm_w, allocator);
    defer allocator.free(norm_data);
    if (kv_data.len != total * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (gate_data.len != total * gate_width) return error.DeepSeekV4TensorShapeMismatch;
    if (norm_data.len != head_dim) return error.DeepSeekV4TensorShapeMismatch;

    const rows_per_batch = (seq_len + compress_rate - 1) / compress_rate;
    const pool_rows = batch * rows_per_batch;
    const pooled = try allocator.alloc(f32, pool_rows * head_dim);
    errdefer allocator.free(pooled);
    const positions = try allocator.alloc(u32, pool_rows);
    errdefer allocator.free(positions);

    for (0..batch) |b| {
        for (0..rows_per_batch) |pool_idx| {
            const start = pool_idx * compress_rate;
            const end = @min(seq_len, start + compress_rate);
            const out_row = (b * rows_per_batch + pool_idx) * head_dim;
            @memset(pooled[out_row..][0..head_dim], 0.0);
            var denom: f32 = 0.0;
            for (start..end) |pos| {
                const token = b * seq_len + pos;
                const gate_slot = if (gate_width == compress_rate) pos - start else 0;
                const gate = deepseek_v4_host.sigmoid(gate_data[token * gate_width + gate_slot]);
                denom += gate;
                for (0..head_dim) |d| {
                    const bias = if (bias_data.len == compress_rate * head_dim)
                        bias_data[(pos - start) * head_dim + d]
                    else if (bias_data.len == head_dim)
                        bias_data[d]
                    else
                        0.0;
                    pooled[out_row + d] += gate * (kv_data[token * head_dim + d] + bias);
                }
            }
            if (denom > 0.0) {
                for (0..head_dim) |d| pooled[out_row + d] /= denom;
            }
            deepSeekV4RmsNormRow(pooled[out_row..][0..head_dim], norm_data, config.norm_eps);
            positions[b * rows_per_batch + pool_idx] = @intCast(@min(seq_len - 1, end - 1));
        }
    }
    deepseek_v4_host.applyTrailingRopeRows(pooled, positions, 1, head_dim, @intCast(config.deepseek_v4_qk_rope_head_dim), rope_theta);
    return .{ .data = pooled, .positions = positions };
}

fn deepSeekV4BuildIndexerQuery(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    q_lora: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
) ![]f32 {
    const n_heads: usize = @intCast(config.deepseek_v4_index_n_heads);
    const head_dim: usize = @intCast(config.deepseek_v4_index_head_dim);
    const q_lora_rank: usize = @intCast(config.deepseek_v4_q_lora_rank);
    const q_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.q_b_proj.weight", .{layer}) catch return error.NameTooLong;
    const q_w = try getModelWeight(cb, config, q_name);
    defer cb.free(q_w);
    const projected = try cb.linearNoBias(q_lora, q_w, total, q_lora_rank, n_heads * head_dim);
    defer cb.free(projected);
    return cb.toFloat32(projected, allocator);
}

fn deepSeekV4BuildIndexerHeadWeights(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    normed: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
) ![]f32 {
    const hidden_size: usize = @intCast(config.hidden_size);
    const n_heads: usize = @intCast(config.deepseek_v4_index_n_heads);
    const w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.self_attn.compressor.indexer.weights_proj.weight", .{layer}) catch return error.NameTooLong;
    const w = try getModelWeight(cb, config, w_name);
    defer cb.free(w);
    const projected = try cb.linearNoBias(normed, w, total, hidden_size, n_heads);
    defer cb.free(projected);
    return cb.toFloat32(projected, allocator);
}

fn deepSeekV4RmsNormRow(row: []f32, weight: []const f32, eps: f32) void {
    var sum_sq: f32 = 0.0;
    for (row) |value| sum_sq += value * value;
    const inv = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(row.len)) + eps);
    for (row, 0..) |*value, i| value.* *= inv * weight[i];
}

fn deepSeekV4CompressedAttentionRows(
    allocator: std.mem.Allocator,
    output: []f32,
    query: []const f32,
    local_kv: []const f32,
    compressed_kv: []const f32,
    compressed_positions: []const u32,
    index_kv: ?[]const f32,
    index_positions: ?[]const u32,
    index_query: ?[]const f32,
    index_head_weights: ?[]const f32,
    sinks: []const f32,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    head_dim: usize,
    sliding_window: usize,
    top_k: usize,
    index_heads: usize,
    index_head_dim: usize,
) !void {
    const q_width = num_heads * head_dim;
    const comp_rows_per_batch = compressed_positions.len / batch;
    if (compressed_positions.len != batch * comp_rows_per_batch) return error.DeepSeekV4TensorShapeMismatch;
    if (compressed_kv.len != compressed_positions.len * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (local_kv.len != batch * seq_len * head_dim or query.len != batch * seq_len * q_width or output.len != query.len) return error.DeepSeekV4TensorShapeMismatch;
    if (sinks.len != num_heads) return error.DeepSeekV4TensorShapeMismatch;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const selected = try allocator.alloc(usize, comp_rows_per_batch);
    defer allocator.free(selected);
    const selected_scores = try allocator.alloc(f32, comp_rows_per_batch);
    defer allocator.free(selected_scores);

    for (0..batch) |b| {
        for (0..seq_len) |row| {
            const global_row = b * seq_len + row;
            const comp_start = b * comp_rows_per_batch;
            const comp_end = comp_start + comp_rows_per_batch;
            const selected_count = deepSeekV4SelectCompressedRows(
                selected,
                selected_scores,
                compressed_positions[comp_start..comp_end],
                index_kv,
                index_positions,
                index_query,
                index_head_weights,
                b,
                row,
                global_row,
                comp_start,
                comp_rows_per_batch,
                top_k,
                index_heads,
                index_head_dim,
            );
            for (0..num_heads) |head| {
                const local_start = if (sliding_window > 0 and row + 1 > sliding_window) row + 1 - sliding_window else 0;
                var max_logit = sinks[head];
                for (local_start..row + 1) |key_row| {
                    const logit = deepSeekV4CompressedAttentionDot(query, local_kv, global_row, b * seq_len + key_row, head, 0, q_width, head_dim, head_dim) * scale;
                    max_logit = @max(max_logit, logit);
                }
                for (selected[0..selected_count]) |comp_idx| {
                    const logit = deepSeekV4CompressedAttentionDot(query, compressed_kv, global_row, comp_idx, head, 0, q_width, head_dim, head_dim) * scale;
                    max_logit = @max(max_logit, logit);
                }

                var denom: f32 = @exp(sinks[head] - max_logit);
                for (local_start..row + 1) |key_row| {
                    const logit = deepSeekV4CompressedAttentionDot(query, local_kv, global_row, b * seq_len + key_row, head, 0, q_width, head_dim, head_dim) * scale;
                    denom += @exp(logit - max_logit);
                }
                for (selected[0..selected_count]) |comp_idx| {
                    const logit = deepSeekV4CompressedAttentionDot(query, compressed_kv, global_row, comp_idx, head, 0, q_width, head_dim, head_dim) * scale;
                    denom += @exp(logit - max_logit);
                }

                for (0..head_dim) |d| {
                    var sum: f32 = 0.0;
                    for (local_start..row + 1) |key_row| {
                        const abs_key_row = b * seq_len + key_row;
                        const logit = deepSeekV4CompressedAttentionDot(query, local_kv, global_row, abs_key_row, head, 0, q_width, head_dim, head_dim) * scale;
                        sum += (@exp(logit - max_logit) / denom) * local_kv[abs_key_row * head_dim + d];
                    }
                    for (selected[0..selected_count]) |comp_idx| {
                        const logit = deepSeekV4CompressedAttentionDot(query, compressed_kv, global_row, comp_idx, head, 0, q_width, head_dim, head_dim) * scale;
                        sum += (@exp(logit - max_logit) / denom) * compressed_kv[comp_idx * head_dim + d];
                    }
                    output[global_row * q_width + head * head_dim + d] = sum;
                }
            }
        }
    }
}

fn deepSeekV4CompressedAttentionCachedRows(
    allocator: std.mem.Allocator,
    output: []f32,
    query: []const f32,
    local_kv: []const f32,
    compressed_kv: []const f32,
    compressed_positions: []const u32,
    index_kv: ?[]const f32,
    index_positions: ?[]const u32,
    index_query: ?[]const f32,
    index_head_weights: ?[]const f32,
    sinks: []const f32,
    query_abs_start: usize,
    query_rows: usize,
    token_count: usize,
    num_heads: usize,
    head_dim: usize,
    sliding_window: usize,
    top_k: usize,
    index_heads: usize,
    index_head_dim: usize,
) !void {
    const q_width = num_heads * head_dim;
    if (query.len != query_rows * q_width or output.len != query.len) return error.DeepSeekV4TensorShapeMismatch;
    if (local_kv.len != token_count * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (compressed_kv.len != compressed_positions.len * head_dim) return error.DeepSeekV4TensorShapeMismatch;
    if (sinks.len != num_heads) return error.DeepSeekV4TensorShapeMismatch;
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const selected = try allocator.alloc(usize, compressed_positions.len);
    defer allocator.free(selected);
    const selected_scores = try allocator.alloc(f32, compressed_positions.len);
    defer allocator.free(selected_scores);

    for (0..query_rows) |query_row| {
        const query_abs = query_abs_start + query_row;
        const selected_count = deepSeekV4SelectCompressedRows(
            selected,
            selected_scores,
            compressed_positions,
            index_kv,
            index_positions,
            index_query,
            index_head_weights,
            0,
            query_abs,
            query_row,
            0,
            compressed_positions.len,
            top_k,
            index_heads,
            index_head_dim,
        );
        for (0..num_heads) |head| {
            const local_start = if (sliding_window > 0 and query_abs + 1 > sliding_window) query_abs + 1 - sliding_window else 0;
            const local_end = @min(query_abs + 1, token_count);
            var max_logit = sinks[head];
            for (local_start..local_end) |key_row| {
                const logit = deepSeekV4CompressedAttentionDot(query, local_kv, query_row, key_row, head, 0, q_width, head_dim, head_dim) * scale;
                max_logit = @max(max_logit, logit);
            }
            for (selected[0..selected_count]) |comp_idx| {
                const logit = deepSeekV4CompressedAttentionDot(query, compressed_kv, query_row, comp_idx, head, 0, q_width, head_dim, head_dim) * scale;
                max_logit = @max(max_logit, logit);
            }

            var denom: f32 = @exp(sinks[head] - max_logit);
            for (local_start..local_end) |key_row| {
                const logit = deepSeekV4CompressedAttentionDot(query, local_kv, query_row, key_row, head, 0, q_width, head_dim, head_dim) * scale;
                denom += @exp(logit - max_logit);
            }
            for (selected[0..selected_count]) |comp_idx| {
                const logit = deepSeekV4CompressedAttentionDot(query, compressed_kv, query_row, comp_idx, head, 0, q_width, head_dim, head_dim) * scale;
                denom += @exp(logit - max_logit);
            }

            for (0..head_dim) |d| {
                var sum: f32 = 0.0;
                for (local_start..local_end) |key_row| {
                    const logit = deepSeekV4CompressedAttentionDot(query, local_kv, query_row, key_row, head, 0, q_width, head_dim, head_dim) * scale;
                    sum += (@exp(logit - max_logit) / denom) * local_kv[key_row * head_dim + d];
                }
                for (selected[0..selected_count]) |comp_idx| {
                    const logit = deepSeekV4CompressedAttentionDot(query, compressed_kv, query_row, comp_idx, head, 0, q_width, head_dim, head_dim) * scale;
                    sum += (@exp(logit - max_logit) / denom) * compressed_kv[comp_idx * head_dim + d];
                }
                output[query_row * q_width + head * head_dim + d] = sum;
            }
        }
    }
}

fn deepSeekV4SelectCompressedRows(
    selected: []usize,
    selected_scores: []f32,
    compressed_positions: []const u32,
    index_kv: ?[]const f32,
    index_positions: ?[]const u32,
    index_query: ?[]const f32,
    index_head_weights: ?[]const f32,
    batch_index: usize,
    query_pos: usize,
    global_query_row: usize,
    comp_start: usize,
    comp_rows_per_batch: usize,
    top_k: usize,
    index_heads: usize,
    index_head_dim: usize,
) usize {
    var count: usize = 0;
    for (compressed_positions, 0..) |position, local_idx| {
        if (position > query_pos) continue;
        const abs_idx = comp_start + local_idx;
        const score: f32 = if (index_kv != null and index_positions != null and index_query != null and index_head_weights != null and index_heads > 0 and index_head_dim > 0)
            deepSeekV4IndexerScore(index_query.?, index_kv.?, index_head_weights.?, global_query_row, abs_idx, index_heads, index_head_dim)
        else
            -@as(f32, @floatFromInt(query_pos - position));
        if (count < selected.len) {
            selected[count] = abs_idx;
            selected_scores[count] = score;
            count += 1;
        }
    }
    const limit = @min(count, top_k);
    var i: usize = 0;
    while (i < limit) : (i += 1) {
        var best = i;
        var j = i + 1;
        while (j < count) : (j += 1) {
            if (selected_scores[j] > selected_scores[best]) best = j;
        }
        std.mem.swap(usize, &selected[i], &selected[best]);
        std.mem.swap(f32, &selected_scores[i], &selected_scores[best]);
    }
    _ = batch_index;
    _ = comp_rows_per_batch;
    return limit;
}

fn deepSeekV4IndexerScore(
    index_query: []const f32,
    index_kv: []const f32,
    index_head_weights: []const f32,
    query_row: usize,
    comp_row: usize,
    index_heads: usize,
    index_head_dim: usize,
) f32 {
    var score: f32 = 0.0;
    const query_width = index_heads * index_head_dim;
    for (0..index_heads) |head| {
        var dot: f32 = 0.0;
        for (0..index_head_dim) |d| {
            dot += index_query[query_row * query_width + head * index_head_dim + d] * index_kv[comp_row * index_head_dim + d];
        }
        score += deepseek_v4_host.sigmoid(index_head_weights[query_row * index_heads + head]) * dot;
    }
    return score / @sqrt(@as(f32, @floatFromInt(index_head_dim)));
}

fn deepSeekV4CompressedAttentionDot(
    query: []const f32,
    key: []const f32,
    query_row: usize,
    key_row: usize,
    query_head: usize,
    kv_head: usize,
    query_width: usize,
    key_width: usize,
    head_dim: usize,
) f32 {
    var dot: f32 = 0.0;
    const q_base = query_row * query_width + query_head * head_dim;
    const k_base = key_row * key_width + kv_head * head_dim;
    for (0..head_dim) |d| dot += query[q_base + d] * key[k_base + d];
    return dot;
}

fn forwardDeepSeekV4FinalHiddenWithStreams(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden_input: CT,
    batch: usize,
    seq_len: usize,
    decode_context: ?*const DecodeContext,
    pre_norm_out: ?*CT,
) !HiddenTensorResult {
    const hidden_size: usize = @intCast(config.hidden_size);
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;
    var name_buf: [256]u8 = undefined;
    var owns_hidden_input = true;
    errdefer if (owns_hidden_input) cb.free(hidden_input);

    var runtime_adapter = DeepSeekV4RuntimeAdapter.init(cb, config);
    const deepseek_runtime = runtime_adapter.runtime();
    const streams = try deepseek_v4.initHyperStreams(cb, allocator, config, hidden_input, total);
    defer allocator.free(streams);
    cb.free(hidden_input);
    owns_hidden_input = false;

    const eval_stride = decoderLayerEvalStride(config, decode_context);
    for (0..config.num_hidden_layers) |layer| {
        if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill) {
            debugPrint("prefill-trace: deepseek_v4 stream layer={d} start\n", .{layer});
        }
        const num_kv_heads = config.effectiveKVHeadsForLayer(layer);
        const head_dim = config.effectiveHeadDimForLayer(layer);
        const path = try deepseek_v4.attentionPathForLayer(config, layer);

        const attn_input = try deepseek_v4.hyperPreReduceStreams(cb, allocator, config, deepseek_runtime, streams, total, layer, "attn_hc", &name_buf);
        defer cb.free(attn_input);
        const attn_normed = try applyAttnNorm(cb, allocator, config, attn_input, layer, &name_buf);
        defer cb.free(attn_normed);
        const attn_update = try deepSeekV4AttentionUpdate(cb, allocator, config, deepseek_runtime, attn_normed, batch, seq_len, num_kv_heads, head_dim, layer, path, decode_context, &name_buf);
        defer cb.free(attn_update);
        try deepseek_v4.hyperPostUpdateStreams(cb, allocator, config, deepseek_runtime, streams, attn_update, total, layer, "attn_hc", &name_buf);

        const ffn_input = try deepseek_v4.hyperPreReduceStreams(cb, allocator, config, deepseek_runtime, streams, total, layer, "ffn_hc", &name_buf);
        defer cb.free(ffn_input);
        const ffn_normed = try applyFFNNorm(cb, allocator, config, ffn_input, layer, &name_buf);
        defer cb.free(ffn_normed);
        const ffn_update = try deepseek_v4.feedForward(cb, allocator, config, deepseek_runtime, ffn_normed, total, layer, &name_buf, deepSeekV4MoeContext(decode_context));
        defer cb.free(ffn_update);
        try deepseek_v4.hyperPostUpdateStreams(cb, allocator, config, deepseek_runtime, streams, ffn_update, total, layer, "ffn_hc", &name_buf);

        if (shouldEvalDecoderLayer(eval_stride, layer, config.num_hidden_layers)) {
            const layer_hidden = try deepseek_v4.applyHyperHeadToStreams(cb, allocator, config, deepseek_runtime, streams, total);
            defer cb.free(layer_hidden);
            const eval_started_at = monotonicNowNs();
            try cb.evalTensor(layer_hidden);
            debug_timing_stats.eval_nanos += @intCast(monotonicNowNs() - eval_started_at);
            debug_timing_stats.eval_count += 1;
        }
    }

    const collapsed = try deepseek_v4.applyHyperHeadToStreams(cb, allocator, config, deepseek_runtime, streams, total);
    defer cb.free(collapsed);
    if (pre_norm_out) |out| {
        out.* = try cloneTensorMaterialized(cb, allocator, collapsed);
    }

    const base_w = try getModelWeight(cb, config, "model.norm.weight");
    defer cb.free(base_w);
    const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, hidden_size);
    defer if (w != base_w) cb.free(w);
    const final_hidden = try cb.rmsNorm(collapsed, w, hidden_size, config.norm_eps);
    return .{ .hidden = final_hidden, .total_rows = total };
}

const DeepSeekV4RuntimeAdapter = struct {
    cb: *const ComputeBackend,
    config: Config,

    fn init(cb: *const ComputeBackend, config: Config) @This() {
        return .{ .cb = cb, .config = config };
    }

    fn runtime(self: *@This()) deepseek_v4.Runtime {
        return .{
            .ptr = self,
            .get_weight = getWeight,
            .apply_activation = applyActivationForConfig,
        };
    }

    fn getWeight(ptr: *anyopaque, name: []const u8) anyerror!CT {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return getModelWeight(self.cb, self.config, name);
    }

    fn applyActivationForConfig(ptr: *anyopaque, input: CT) anyerror!CT {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        return applyActivation(self.cb, self.config, input);
    }
};

fn deepSeekV4SequenceContext(decode_context: ?*const DecodeContext) deepseek_v4.SequenceContext {
    if (decode_context) |dc| {
        return .{
            .query_sequence_len = dc.query_sequence_len,
            .total_sequence_len = dc.total_sequence_len,
            .mixed_batch = dc.isMixedBatch(),
        };
    }
    return .{};
}

fn deepSeekV4MoeContext(decode_context: ?*const DecodeContext) deepseek_v4.MoeContext {
    if (decode_context) |dc| return .{ .input_ids = dc.input_ids };
    return .{};
}

test "deepseek v4 dispatch rejects unsupported non-sliding attention kinds" {
    var csa = Config{ .family = .deepseek_v4 };
    csa.deepseek_v4_attention_schedule_len = 1;
    csa.deepseek_v4_attention_schedule[0] = .compressed_sparse_attention;
    try deepSeekV4AttentionKindUnsupported(csa, 0);

    var hca = Config{ .family = .deepseek_v4 };
    hca.deepseek_v4_attention_schedule_len = 1;
    hca.deepseek_v4_attention_schedule[0] = .heavily_compressed_attention;
    try deepSeekV4AttentionKindUnsupported(hca, 0);
}

test "deepseek v4 cached compressed attention rows match full prefix rows" {
    const allocator = std.testing.allocator;
    const seq_len: usize = 3;
    const num_heads: usize = 2;
    const head_dim: usize = 2;
    const q_width = num_heads * head_dim;
    const query = [_]f32{
        0.2, 0.1,  0.4,  -0.2,
        0.3, 0.5,  -0.1, 0.2,
        0.6, -0.3, 0.2,  0.7,
    };
    const local = [_]f32{
        0.1, 0.0,
        0.0, 0.2,
        0.3, -0.1,
    };
    const compressed = [_]f32{
        0.05, 0.15,
        0.25, -0.05,
    };
    const positions = [_]u32{ 1, 2 };
    const sinks = [_]f32{ -1.0, -0.5 };

    var full: [seq_len * q_width]f32 = undefined;
    try deepSeekV4CompressedAttentionRows(
        allocator,
        &full,
        &query,
        &local,
        &compressed,
        &positions,
        null,
        null,
        null,
        null,
        &sinks,
        1,
        seq_len,
        num_heads,
        head_dim,
        seq_len,
        std.math.maxInt(usize),
        0,
        0,
    );

    var cached_full: [seq_len * q_width]f32 = undefined;
    try deepSeekV4CompressedAttentionCachedRows(
        allocator,
        &cached_full,
        &query,
        &local,
        &compressed,
        &positions,
        null,
        null,
        null,
        null,
        &sinks,
        0,
        seq_len,
        seq_len,
        num_heads,
        head_dim,
        seq_len,
        std.math.maxInt(usize),
        0,
        0,
    );
    for (full, cached_full) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
    }

    var cached_decode: [q_width]f32 = undefined;
    try deepSeekV4CompressedAttentionCachedRows(
        allocator,
        &cached_decode,
        query[2 * q_width ..][0..q_width],
        &local,
        &compressed,
        &positions,
        null,
        null,
        null,
        null,
        &sinks,
        2,
        1,
        seq_len,
        num_heads,
        head_dim,
        seq_len,
        std.math.maxInt(usize),
        0,
        0,
    );
    for (full[2 * q_width ..][0..q_width], cached_decode) |expected, actual| {
        try std.testing.expectApproxEqAbs(expected, actual, 1e-6);
    }
}

const DeepSeekV4DeviceFastPathTestBackend = struct {
    const Record = struct {
        accept: bool = true,
        calls: usize = 0,
        cache_key: usize = 0,
        layer_index: usize = 0,
        path: ?ops.DeepSeekV4CompressedAttentionPath = null,
        query_abs_start: usize = 0,
        query_rows: usize = 0,
        total_tokens: usize = 0,
        num_heads: usize = 0,
        head_dim: usize = 0,
        sliding_window: usize = 0,
        compress_rate: usize = 0,
        top_k: usize = 0,
        index_heads: usize = 0,
        index_head_dim: usize = 0,
        rope_dim: usize = 0,
        rope_theta: f32 = 0.0,
        rope_consecutive_pairs: bool = false,
        eps: f32 = 0.0,
        compressor_gate_width: usize = 0,
        has_indexer: bool = false,
    };

    var active: ?*Record = null;

    const vtable = blk: {
        var vt = native_compute_mod.vtable_impl;
        vt.backendKind = backendKind;
        vt.runDeepSeekV4CompressedAttention = runDeepSeekV4CompressedAttention;
        break :blk vt;
    };

    fn backendKind(_: *anyopaque) ops.BackendKind {
        return .metal;
    }

    fn runDeepSeekV4CompressedAttention(ctx: *anyopaque, request: *const ops.DeepSeekV4CompressedAttentionRequest) anyerror!?CT {
        const record = active orelse return error.MissingDeepSeekV4DeviceFastPathRecorder;
        record.calls += 1;
        record.cache_key = request.cache_key;
        record.layer_index = request.layer_index;
        record.path = request.path;
        record.query_abs_start = request.query_abs_start;
        record.query_rows = request.query_rows;
        record.total_tokens = request.total_tokens;
        record.num_heads = request.num_heads;
        record.head_dim = request.head_dim;
        record.sliding_window = request.sliding_window;
        record.compress_rate = request.compress_rate;
        record.top_k = request.top_k;
        record.index_heads = request.index_heads;
        record.index_head_dim = request.index_head_dim;
        record.rope_dim = request.rope_dim;
        record.rope_theta = request.rope_theta;
        record.rope_consecutive_pairs = request.rope_consecutive_pairs;
        record.eps = request.eps;
        record.compressor_gate_width = request.compressor.gate_width;
        record.has_indexer = request.indexer != null;
        if (!record.accept) return null;

        const out_len = request.query_rows * request.num_heads * request.head_dim;
        if (out_len > 16) return error.TestOutputTooLarge;
        var out = [_]f32{0} ** 16;
        for (out[0..out_len], 0..) |*value, idx| value.* = 100.0 + @as(f32, @floatFromInt(idx));
        const shape = [_]i32{ @intCast(request.query_rows), @intCast(request.num_heads * request.head_dim) };
        return native_compute_mod.vtable_impl.fromFloat32Shape(ctx, out[0..out_len], &shape);
    }
};

fn deinitDeepSeekV4TestWeightStore(allocator: std.mem.Allocator, store: *native_compute_mod.WeightStore) void {
    native_compute_mod.deinitPrefetchQueue(store);
    var resident_it = store.resident_weights.iterator();
    while (resident_it.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        entry.value_ptr.deinit();
    }
    store.resident_weights.deinit(allocator);
    store.lazy_weights.deinit(allocator);
}

fn putDeepSeekV4TestWeight(
    allocator: std.mem.Allocator,
    store: *native_compute_mod.WeightStore,
    name: []const u8,
    shape: []const i64,
    data: []const f32,
) !void {
    const owned_name = try allocator.dupe(u8, name);
    errdefer allocator.free(owned_name);
    var tensor = try tensor_mod.Tensor.initFloat32(allocator, owned_name, shape, data);
    errdefer tensor.deinit();
    try store.resident_weights.put(allocator, owned_name, weight_source_mod.LoadedWeight{ .tensor = tensor });
}

test "deepseek v4 compressed attention dispatches resident backend request and fails closed after device ownership" {
    const allocator = std.testing.allocator;
    var store = native_compute_mod.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    defer deinitDeepSeekV4TestWeightStore(allocator, &store);
    var compute = native_compute_mod.NativeCompute.init(allocator, &store, null);
    defer compute.weight_reservations.deinit(allocator);
    var cb = ComputeBackend{ .ptr = &compute, .vtable = &DeepSeekV4DeviceFastPathTestBackend.vtable };

    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.kv_proj.weight", &.{ 2, 2 }, &.{
        1.0, 0.0,
        0.0, 1.0,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.gate_proj.weight", &.{ 1, 2 }, &.{
        0.5, -0.25,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.position_bias", &.{2}, &.{
        0.0, 0.0,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.kv_norm.weight", &.{2}, &.{
        1.0, 1.0,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.indexer.kv_proj.weight", &.{ 2, 2 }, &.{
        1.0, 0.0,
        0.0, 1.0,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.indexer.gate_proj.weight", &.{ 1, 2 }, &.{
        0.25, 0.75,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.indexer.position_bias", &.{2}, &.{
        0.0, 0.0,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.indexer.kv_norm.weight", &.{2}, &.{
        1.0, 1.0,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.indexer.q_b_proj.weight", &.{ 4, 2 }, &.{
        1.0, 0.0,
        0.0, 1.0,
        0.5, 0.0,
        0.0, 0.5,
    });
    try putDeepSeekV4TestWeight(allocator, &store, "model.layers.0.self_attn.compressor.indexer.weights_proj.weight", &.{ 2, 2 }, &.{
        1.0, 0.0,
        0.0, 1.0,
    });

    var config = Config{
        .family = .deepseek_v4,
        .hidden_size = 2,
        .num_hidden_layers = 1,
        .num_attention_heads = 1,
        .num_key_value_heads = 1,
        .attention_head_dim = 2,
        .sliding_window = 4,
        .deepseek_v4_qk_rope_head_dim = 2,
        .deepseek_v4_q_lora_rank = 2,
        .deepseek_v4_compress_rate_hca = 2,
        .deepseek_v4_compress_rate_csa = 2,
        .deepseek_v4_compress_rope_theta = 160_000.0,
        .deepseek_v4_heavily_compressed_attention_layers = 1,
        .norm_eps = 1e-6,
    };
    config.deepseek_v4_attention_schedule_len = 1;
    config.deepseek_v4_attention_schedule[0] = .heavily_compressed_attention;

    var cache = try DeepSeekV4CompressedCache.init(allocator, 1);
    defer cache.deinit();
    var record = DeepSeekV4DeviceFastPathTestBackend.Record{};
    DeepSeekV4DeviceFastPathTestBackend.active = &record;
    defer DeepSeekV4DeviceFastPathTestBackend.active = null;

    const normed = try cb.fromFloat32Shape(&.{
        0.2, 0.4,
        0.6, 0.8,
    }, &.{ 2, 2 });
    defer cb.free(normed);
    const q_rope = try cb.fromFloat32Shape(&.{
        0.1, 0.2,
        0.3, 0.4,
    }, &.{ 2, 2 });
    defer cb.free(q_rope);
    const local_k = try cb.fromFloat32Shape(&.{
        0.5, 0.6,
        0.7, 0.8,
    }, &.{ 2, 2 });
    defer cb.free(local_k);
    const sinks = try cb.fromFloat32Shape(&.{-1.0}, &.{1});
    defer cb.free(sinks);

    var ctx = DecodeContext{
        .attention_mode = .paged_prefill,
        .total_sequence_len = 2,
        .query_sequence_len = 2,
        .kv_sequence_len = 2,
        .deepseek_v4_compressed_cache = &cache,
    };
    var name_buf: [256]u8 = undefined;
    const out = try deepSeekV4CompressedAttentionReference(
        &cb,
        allocator,
        config,
        normed,
        q_rope,
        q_rope,
        local_k,
        sinks,
        1,
        2,
        2,
        1,
        1,
        2,
        0,
        .heavily_compressed,
        &ctx,
        &name_buf,
    );
    defer cb.free(out);

    try std.testing.expectEqual(@as(usize, 1), record.calls);
    try std.testing.expectEqual(@as(usize, @intFromPtr(&cache)), record.cache_key);
    try std.testing.expectEqual(@as(usize, 0), record.layer_index);
    try std.testing.expectEqual(ops.DeepSeekV4CompressedAttentionPath.heavily_compressed, record.path.?);
    try std.testing.expectEqual(@as(usize, 0), record.query_abs_start);
    try std.testing.expectEqual(@as(usize, 2), record.query_rows);
    try std.testing.expectEqual(@as(usize, 2), record.total_tokens);
    try std.testing.expectEqual(@as(usize, 1), record.num_heads);
    try std.testing.expectEqual(@as(usize, 2), record.head_dim);
    try std.testing.expectEqual(@as(usize, 4), record.sliding_window);
    try std.testing.expectEqual(@as(usize, 2), record.compress_rate);
    try std.testing.expectEqual(@as(usize, 0), record.top_k);
    try std.testing.expectEqual(@as(usize, 0), record.index_heads);
    try std.testing.expectEqual(@as(usize, 0), record.index_head_dim);
    try std.testing.expectEqual(@as(usize, 2), record.rope_dim);
    try std.testing.expectApproxEqAbs(@as(f32, 160_000.0), record.rope_theta, 1e-3);
    try std.testing.expect(record.rope_consecutive_pairs);
    try std.testing.expectApproxEqAbs(@as(f32, 1e-6), record.eps, 1e-9);
    try std.testing.expectEqual(@as(usize, 1), record.compressor_gate_width);
    try std.testing.expect(!record.has_indexer);
    try std.testing.expect(cache.layers[0].device_resident);
    try std.testing.expectEqual(@as(usize, 2), cache.layers[0].token_count);

    var sparse_config = config;
    sparse_config.deepseek_v4_heavily_compressed_attention_layers = 0;
    sparse_config.deepseek_v4_compressed_sparse_attention_layers = 1;
    sparse_config.deepseek_v4_index_n_heads = 2;
    sparse_config.deepseek_v4_index_head_dim = 2;
    sparse_config.deepseek_v4_index_topk = 512;
    sparse_config.deepseek_v4_attention_schedule[0] = .compressed_sparse_attention;
    var sparse_cache = try DeepSeekV4CompressedCache.init(allocator, 1);
    defer sparse_cache.deinit();
    record = .{};
    var sparse_ctx = DecodeContext{
        .attention_mode = .paged_prefill,
        .total_sequence_len = 2,
        .query_sequence_len = 2,
        .kv_sequence_len = 2,
        .deepseek_v4_compressed_cache = &sparse_cache,
    };
    const sparse_out = try deepSeekV4CompressedAttentionReference(
        &cb,
        allocator,
        sparse_config,
        normed,
        q_rope,
        q_rope,
        local_k,
        sinks,
        1,
        2,
        2,
        1,
        1,
        2,
        0,
        .compressed_sparse,
        &sparse_ctx,
        &name_buf,
    );
    defer cb.free(sparse_out);
    try std.testing.expectEqual(@as(usize, 1), record.calls);
    try std.testing.expectEqual(ops.DeepSeekV4CompressedAttentionPath.compressed_sparse, record.path.?);
    try std.testing.expectEqual(@as(usize, 512), record.top_k);
    try std.testing.expectEqual(@as(usize, 2), record.index_heads);
    try std.testing.expectEqual(@as(usize, 2), record.index_head_dim);
    try std.testing.expect(record.has_indexer);
    try std.testing.expect(sparse_cache.layers[0].device_resident);

    const decode_normed = try cb.fromFloat32Shape(&.{ 1.0, 1.1 }, &.{ 1, 2 });
    defer cb.free(decode_normed);
    const decode_q = try cb.fromFloat32Shape(&.{ 0.9, 0.8 }, &.{ 1, 2 });
    defer cb.free(decode_q);
    const decode_local = try cb.fromFloat32Shape(&.{ 0.7, 0.6 }, &.{ 1, 2 });
    defer cb.free(decode_local);
    ctx = .{
        .attention_mode = .paged_decode,
        .total_sequence_len = 3,
        .query_sequence_len = 1,
        .kv_sequence_len = 3,
        .deepseek_v4_compressed_cache = &cache,
    };
    record.accept = false;
    try std.testing.expectError(error.DeepSeekV4CompressedDeviceCacheUnavailable, deepSeekV4CompressedAttentionReference(
        &cb,
        allocator,
        config,
        decode_normed,
        decode_q,
        decode_q,
        decode_local,
        sinks,
        1,
        3,
        1,
        1,
        1,
        2,
        0,
        .heavily_compressed,
        &ctx,
        &name_buf,
    ));
    try std.testing.expectEqual(@as(usize, 2), record.calls);
}

fn deepSeekV4AttentionKindUnsupported(config: Config, layer: usize) !void {
    switch (config.deepseekV4AttentionKind(layer)) {
        .sliding_attention, .compressed_sparse_attention, .heavily_compressed_attention => {},
        .unknown => return error.UnsupportedDeepSeekV4AttentionSchedule,
    }
}

fn decoderBlock(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    batch: usize,
    seq_len: usize,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
    raw_overrides: Layer0DecoderOverrides,
    layer0_attn_norm_override: ?CT,
    layer0_fused_qkv_override: ?CT,
    layer0_q_override: ?CT,
    layer0_k_override: ?CT,
    layer0_v_override: ?CT,
) !CT {
    const attn_started_at = monotonicNowNs();
    const hidden_size = config.hidden_size;
    const num_heads = config.num_attention_heads;
    const query_seq_len = actualQuerySeqLen(seq_len, decode_context);
    const total = batch * query_seq_len;

    var name_buf: [256]u8 = undefined;
    var attn_norm_slot = decoderOverrideLayerSlot(raw_overrides.attn_norm_slots, layer);
    var attn_q_slot = decoderOverrideLayerSlot(raw_overrides.attn_q_slots, layer);
    var attn_k_slot = decoderOverrideLayerSlot(raw_overrides.attn_k_slots, layer);
    var attn_v_slot = decoderOverrideLayerSlot(raw_overrides.attn_v_slots, layer);
    var fused_qkv_linear_slot = decoderOverrideLayerSlot(raw_overrides.fused_qkv_linear_slots, layer);
    var attn_out_proj_linear_slot = decoderOverrideLayerSlot(raw_overrides.attn_out_proj_linear_slots, layer);
    var attn_sub_norm_slot = decoderOverrideLayerSlot(raw_overrides.attn_sub_norm_slots, layer);
    var ffn_norm_slot = decoderOverrideLayerSlot(raw_overrides.ffn_norm_slots, layer);
    var mlp_fc1_slot = decoderOverrideLayerSlot(raw_overrides.mlp_fc1_slots, layer);
    var mlp_fc2_slot = decoderOverrideLayerSlot(raw_overrides.mlp_fc2_slots, layer);
    var mlp_gate_slot = decoderOverrideLayerSlot(raw_overrides.mlp_gate_slots, layer);
    var mlp_up_slot = decoderOverrideLayerSlot(raw_overrides.mlp_up_slots, layer);
    var mlp_sub_norm_slot = decoderOverrideLayerSlot(raw_overrides.mlp_sub_norm_slots, layer);
    var mlp_down_slot = decoderOverrideLayerSlot(raw_overrides.mlp_down_slots, layer);
    if (disablePreparedDecoderSlotsDebug()) {
        attn_norm_slot = null;
        attn_q_slot = null;
        attn_k_slot = null;
        attn_v_slot = null;
        fused_qkv_linear_slot = null;
        attn_out_proj_linear_slot = null;
        attn_sub_norm_slot = null;
        ffn_norm_slot = null;
        mlp_fc1_slot = null;
        mlp_fc2_slot = null;
        mlp_gate_slot = null;
        mlp_up_slot = null;
        mlp_sub_norm_slot = null;
        mlp_down_slot = null;
    }
    // --- Self-attention sublayer ---
    // Pre-norm
    const attn_norm_started_at = monotonicNowNs();
    const normed = if (layer == 0 and layer0_attn_norm_override != null)
        layer0_attn_norm_override.?
    else if (attn_norm_slot != null) blk: {
        const raw_norm = switch (config.norm_type) {
            .layer_norm => try cb.decoderRuntimeApplyLayerNorm(&.{
                .slot = attn_norm_slot.?,
                .input = hidden,
                .hidden_size = hidden_size,
                .eps = config.norm_eps,
            }),
            .rms_norm => try cb.decoderRuntimeApplyRmsNorm(&.{
                .slot = attn_norm_slot.?,
                .input = hidden,
                .hidden_size = hidden_size,
                .eps = config.norm_eps,
            }),
        };
        break :blk raw_norm orelse try applyAttnNorm(cb, allocator, config, hidden, layer, &name_buf);
    } else try applyAttnNorm(cb, allocator, config, hidden, layer, &name_buf);
    defer cb.free(normed);
    debug_timing_stats.attention_norm_nanos += @intCast(monotonicNowNs() - attn_norm_started_at);
    try maybeDebugLayerTensorLastRow(cb, allocator, layer, "attn_norm", normed, hidden_size);
    try maybeDebugLayerTensor(cb, allocator, layer, "attn_norm", normed);
    try maybeDumpGatedLayerStageStats(cb, allocator, layer, "attn-norm", normed, hidden_size);
    if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill and layer == 0) {
        debugPrint("prefill-trace: gpt layer0 attn norm done\n", .{});
    }

    if (config.family == .deepseek_v4) {
        return try deepSeekV4DecoderBlockAfterAttnNorm(
            cb,
            allocator,
            config,
            normed,
            hidden,
            batch,
            seq_len,
            num_kv_heads,
            head_dim,
            layer,
            decode_context,
            &name_buf,
        );
    }

    // Q, K, V projections
    const attn_qkv_started_at = monotonicNowNs();
    if (config.layerUsesQwen35LinearAttention(layer)) {
        const attn_out = try qwen35LinearAttention(cb, allocator, config, normed, batch, query_seq_len, layer, decode_context, &name_buf);
        defer cb.free(attn_out);
        const attn_res = try cb.add(attn_out, hidden);
        errdefer cb.free(attn_res);

        const ffn_normed = try applyFFNNorm(cb, allocator, config, attn_res, layer, &name_buf);
        defer cb.free(ffn_normed);
        const ffn_started_at = monotonicNowNs();
        const ffn_out = try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
        defer cb.free(ffn_out);
        debug_timing_stats.ffn_nanos += @intCast(monotonicNowNs() - ffn_started_at);

        const result = try cb.add(ffn_out, attn_res);
        cb.free(attn_res);
        try dumpLayerLastRowStats(cb, allocator, layer, result, hidden_size);
        return result;
    }

    const shares_kv = config.layerSharesKv(layer) and !disableSharedKvDebug();
    const kv_layer_index = if (shares_kv) config.kvDonorLayerIndex(layer).? else layer;

    // Shared KV layers read K/V from their donor layer's cache — create placeholder zeros.
    const kv_dim: usize = @as(usize, num_kv_heads) * head_dim;
    const q_dim: usize = @as(usize, num_heads) * head_dim;
    if (!disableDecoderRuntimeActivationDebug() and !shares_kv and config.family != .gpt2 and
        config.family != .qwen3 and config.family != .qwen3_5 and
        attn_q_slot != null and attn_k_slot != null and attn_v_slot != null and
        attn_out_proj_linear_slot != null and mlp_gate_slot != null and
        mlp_up_slot != null and mlp_down_slot != null)
    {
        if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill and layer == 0) {
            debugPrint("prefill-trace: gpt layer0 gated block attempt input path\n", .{});
        }
        const block_started_at = monotonicNowNs();
        debug_timing_stats.gated_block_attempts += 1;
        debug_timing_stats.gated_block_input_attempts += 1;
        if (try cb.runGatedDecoderBlock(&.{
            .attention_input = normed,
            .residual = hidden,
            .attention = blk: {
                var attention = attentionContext(seq_len, decode_context);
                attention.layer_index = kv_layer_index;
                attention.skip_kv_write = shares_kv;
                attention.sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0;
                if (disableSlidingAttentionDebug()) attention.sliding_window = 0;
                break :blk attention;
            },
            .num_heads = num_heads,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .q_linear_slot = attn_q_slot.?,
            .k_linear_slot = attn_k_slot.?,
            .v_linear_slot = attn_v_slot.?,
            .attention_linear_slot = attn_out_proj_linear_slot.?,
            .hidden_size = hidden_size,
            .eps = config.norm_eps,
            .ffn_layer_norm_slot = if (config.norm_type == .layer_norm) ffn_norm_slot.? else null,
            .ffn_rms_norm_slot = if (config.norm_type == .rms_norm) ffn_norm_slot.? else null,
            .ffn_post_gate_rms_norm_slot = if (config.family == .bitnet) mlp_sub_norm_slot else null,
            .gate_ffn_linear_slot = mlp_gate_slot.?,
            .up_ffn_linear_slot = mlp_up_slot.?,
            .down_ffn_linear_slot = mlp_down_slot.?,
            .intermediate_size = config.intermediateSize(layer),
            .activation = decoderRuntimeActivationKind(config.activation),
            .graph_plan_tail_vocab_size = config.vocab_size,
        })) |block_out| {
            debug_timing_stats.gated_block_successes += 1;
            debug_timing_stats.gated_block_input_successes += 1;
            const block_elapsed = monotonicNowNs() - block_started_at;
            debug_timing_stats.attention_nanos += @intCast(block_elapsed);
            debug_timing_stats.ffn_nanos += @intCast(block_elapsed);
            if (decode_context) |ctx| {
                switch (ctx.attention_mode) {
                    .paged_prefill => debug_timing_stats.gated_block_input_prefill_nanos += @intCast(block_elapsed),
                    .paged_decode => debug_timing_stats.gated_block_input_decode_nanos += @intCast(block_elapsed),
                    else => {},
                }
            }
            if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill and layer == 0) {
                debugPrint("prefill-trace: gpt layer0 gated block input success\n", .{});
            }
            return block_out;
        }
        if (prefillTraceEnabled() and decode_context != null and decode_context.?.attention_mode == .paged_prefill and layer == 0) {
            debugPrint("prefill-trace: gpt layer0 gated block input null\n", .{});
        }
    }
    if (allowDenseBlockFastPath(decode_context) and
        !disableDecoderRuntimeActivationDebug() and !shares_kv and config.family == .gpt2 and
        fused_qkv_linear_slot != null and attn_out_proj_linear_slot != null and
        mlp_fc1_slot != null and mlp_fc2_slot != null)
    {
        const block_started_at = monotonicNowNs();
        debug_timing_stats.dense_block_attempts += 1;
        if (try cb.runDenseDecoderBlock(&.{
            .attention_input = normed,
            .residual = hidden,
            .attention = blk: {
                var attention = attentionContext(seq_len, decode_context);
                attention.layer_index = kv_layer_index;
                attention.skip_kv_write = shares_kv;
                attention.sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0;
                if (disableSlidingAttentionDebug()) attention.sliding_window = 0;
                break :blk attention;
            },
            .num_heads = num_heads,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .fused_qkv_linear_slot = fused_qkv_linear_slot.?,
            .attention_linear_slot = attn_out_proj_linear_slot.?,
            .hidden_size = hidden_size,
            .eps = config.norm_eps,
            .ffn_layer_norm_slot = if (config.norm_type == .layer_norm) ffn_norm_slot.? else null,
            .ffn_rms_norm_slot = if (config.norm_type == .rms_norm) ffn_norm_slot.? else null,
            .first_ffn_linear_slot = mlp_fc1_slot.?,
            .second_ffn_linear_slot = mlp_fc2_slot.?,
            .intermediate_size = config.intermediateSize(layer),
            .activation = decoderRuntimeActivationKind(config.activation),
        })) |block_out| {
            debug_timing_stats.dense_block_successes += 1;
            const block_elapsed = monotonicNowNs() - block_started_at;
            debug_timing_stats.attention_nanos += @intCast(block_elapsed);
            debug_timing_stats.ffn_nanos += @intCast(block_elapsed);
            return block_out;
        }
    }
    const fused_qkv_override_active = layer == 0 and layer0_fused_qkv_override != null and !shares_kv and config.family == .gpt2;
    const fused_qkv_linear_slot_active = fused_qkv_linear_slot != null and !shares_kv and config.family == .gpt2;
    const split_qkv_override_active = layer == 0 and layer0_q_override != null and layer0_k_override != null and layer0_v_override != null and !shares_kv;
    const AttentionProjectionSet = struct {
        q: CT,
        q_gate: ?CT = null,
        k: CT,
        v_omitted: bool = false,
        v: CT,
    };
    const projected: AttentionProjectionSet = if (fused_qkv_override_active) blk: {
        const fused_qkv = layer0_fused_qkv_override.?;
        defer cb.free(fused_qkv);
        const q = try cb.sliceLastDim(fused_qkv, 0, q_dim);
        errdefer cb.free(q);
        const k = try cb.sliceLastDim(fused_qkv, q_dim, q_dim + kv_dim);
        errdefer cb.free(k);
        const v = try cb.sliceLastDim(fused_qkv, q_dim + kv_dim, q_dim + kv_dim * 2);
        break :blk .{ .q = q, .k = k, .v = v };
    } else if (fused_qkv_linear_slot_active) blk: {
        const fused_qkv = (try cb.decoderRuntimeApplyLinear(&.{
            .slot = fused_qkv_linear_slot.?,
            .input = normed,
            .in_dim = hidden_size,
            .out_dim = q_dim + kv_dim * 2,
        })) orelse break :blk blk_fallback: {
            const q = try attnProject(cb, allocator, config, normed, total, hidden_size, num_heads * head_dim, layer, "q", &name_buf);
            errdefer cb.free(q);
            const k = try attnProject(cb, allocator, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "k", &name_buf);
            errdefer cb.free(k);
            const projected_v = try attnProject(cb, allocator, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "v", &name_buf);
            break :blk_fallback .{ .q = q, .k = k, .v = projected_v };
        };
        defer cb.free(fused_qkv);
        const q = try cb.sliceLastDim(fused_qkv, 0, q_dim);
        errdefer cb.free(q);
        const k = try cb.sliceLastDim(fused_qkv, q_dim, q_dim + kv_dim);
        errdefer cb.free(k);
        const v = try cb.sliceLastDim(fused_qkv, q_dim + kv_dim, q_dim + kv_dim * 2);
        break :blk .{ .q = q, .k = k, .v = v };
    } else if (split_qkv_override_active) blk: {
        break :blk .{ .q = layer0_q_override.?, .k = layer0_k_override.?, .v = layer0_v_override.? };
    } else blk: {
        const q_projection_dim: usize = if (config.family == .qwen3_5 and config.qwen35_attn_output_gate) q_dim * 2 else q_dim;
        const q_projected = if (attn_q_slot != null)
            (try cb.decoderRuntimeApplyLinear(&.{
                .slot = attn_q_slot.?,
                .input = normed,
                .in_dim = hidden_size,
                .out_dim = q_projection_dim,
            })) orelse try attnProject(cb, allocator, config, normed, total, hidden_size, @intCast(q_projection_dim), layer, "q", &name_buf)
        else
            try attnProject(cb, allocator, config, normed, total, hidden_size, @intCast(q_projection_dim), layer, "q", &name_buf);
        const q, const q_gate: ?CT = if (q_projection_dim == q_dim) .{ q_projected, null } else split_q: {
            defer cb.free(q_projected);
            const q_main = try cb.sliceLastDim(q_projected, 0, q_dim);
            errdefer cb.free(q_main);
            const gate = try cb.sliceLastDim(q_projected, q_dim, q_dim * 2);
            break :split_q .{ q_main, gate };
        };
        const k: CT, const v_omitted_local: bool, const v_local: CT = if (shares_kv)
            .{ try createZeroCT(cb, allocator, total * kv_dim), false, try createZeroCT(cb, allocator, total * kv_dim) }
        else if (attn_k_slot != null and attn_v_slot != null and config.family == .qwen3) blk_kv: {
            // The dense K/V pair slot path is not Qwen3-correct for Jina v5;
            // individual prepared slots match the dynamic Metal projections.
            const k_local = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = attn_k_slot.?,
                .input = normed,
                .in_dim = hidden_size,
                .out_dim = num_kv_heads * head_dim,
            })) orelse try attnProject(cb, allocator, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "k", &name_buf);
            errdefer cb.free(k_local);
            const v_local = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = attn_v_slot.?,
                .input = normed,
                .in_dim = hidden_size,
                .out_dim = num_kv_heads * head_dim,
            })) orelse try attnProject(cb, allocator, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "v", &name_buf);
            break :blk_kv .{ k_local, false, v_local };
        } else if (attn_k_slot != null and attn_v_slot != null and config.family != .gemma) blk_kv: {
            const kv = (try cb.decoderRuntimeApplyLinearPair(&.{
                .slot_a = attn_k_slot.?,
                .slot_b = attn_v_slot.?,
                .input = normed,
                .in_dim = hidden_size,
                .out_dim = num_kv_heads * head_dim,
            })) orelse try attnProjectPair(cb, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "k", "v", &name_buf);
            break :blk_kv .{ kv.first, false, kv.second };
        } else if (config.family == .bitnet) blk_kv: {
            const kv = try attnProjectPair(cb, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "k", "v", &name_buf);
            break :blk_kv .{ kv.first, false, kv.second };
        } else blk_kv: {
            const k_local = try attnProject(cb, allocator, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "k", &name_buf);
            errdefer cb.free(k_local);
            const projected_v = attnProject(cb, allocator, config, normed, total, hidden_size, num_kv_heads * head_dim, layer, "v", &name_buf);
            break :blk_kv if (projected_v) |v|
                .{ k_local, false, v }
            else |err| switch (err) {
                error.MissingWeight, error.WeightNotFound => if (config.layerOmitsVProj(layer))
                    .{ k_local, true, k_local } // Gemma 4 checkpoints may omit v_proj on some full-attention layers.
                else
                    return err,
                else => return err,
            };
        };
        break :blk .{ .q = q, .q_gate = q_gate, .k = k, .v_omitted = v_omitted_local, .v = v_local };
    };
    const Q = projected.q;
    const K = projected.k;
    const V = projected.v;
    const q_gate = projected.q_gate;
    const v_omitted = projected.v_omitted;
    defer cb.free(Q);
    defer if (q_gate) |gate| cb.free(gate);
    defer cb.free(K);

    // Gemma 4: apply bare RMS normalization to V (no weight, matching HF v_norm with_scale=False).
    // Use all-ones weight since bare RMS norm is rmsNorm(x, ones, dim, eps).
    const V_normed = blk: {
        if (config.global_head_dim == 0 or shares_kv) break :blk V; // Not Gemma 4 or shared KV
        const v_dim: usize = head_dim;
        // Create ones weight for bare RMS norm.
        const ones = try allocator.alloc(f32, v_dim);
        defer allocator.free(ones);
        @memset(ones, 1.0);
        const ones_shape = [_]i32{@intCast(v_dim)};
        const ones_ct = try cb.fromFloat32Shape(ones, &ones_shape);
        defer cb.free(ones_ct);

        if (try cb.reshape2d(V, total * num_kv_heads, v_dim)) |reshaped| {
            // GPU path: reshape so last dim matches weight, then rmsNorm on GPU.
            defer cb.free(reshaped);
            const normed_flat = try cb.rmsNorm(reshaped, ones_ct, v_dim, config.norm_eps);
            defer cb.free(normed_flat);
            const result = (try cb.reshape2d(normed_flat, total, kv_dim)) orelse return error.ReshapeFailed;
            if (!v_omitted) cb.free(V); // V = K when omitted, don't double-free
            break :blk result;
        }
        // CPU fallback (native — rmsNorm handles chunked dim natively).
        const result = try cb.rmsNorm(V, ones_ct, v_dim, config.norm_eps);
        if (!v_omitted) cb.free(V); // V = K when omitted, don't double-free
        break :blk result;
    };
    defer cb.free(V_normed); // Either V_normed is new (V already freed above), or V_normed == V

    try maybeDebugLayerTensorLastRow(cb, allocator, layer, "q", Q, num_heads * head_dim);
    try maybeDebugLayerTensorLastRow(cb, allocator, layer, "k", K, num_kv_heads * head_dim);
    try maybeDebugLayerTensorLastRow(cb, allocator, layer, "v", V_normed, num_kv_heads * head_dim);
    try maybeDebugLayerTensor(cb, allocator, layer, "q", Q);
    try maybeDebugLayerTensor(cb, allocator, layer, "k", K);
    try maybeDebugLayerTensor(cb, allocator, layer, "v", V_normed);

    const Q_attn = if (try maybeApplyQKHeadNorm(cb, allocator, config, Q, total, num_heads * head_dim, layer, "q", head_dim, &name_buf)) |normed_q|
        normed_q
    else
        Q;
    defer if (Q_attn != Q) cb.free(Q_attn);

    // Shared KV layers skip K head norm (K is read from the donor layer's cache).
    const K_attn = if (!shares_kv)
        if (try maybeApplyQKHeadNorm(cb, allocator, config, K, total, num_kv_heads * head_dim, layer, "k", head_dim, &name_buf)) |normed_k|
            normed_k
        else
            K
    else
        K;
    defer if (K_attn != K) cb.free(K_attn);

    // Gemma 4 attention scale correction: HF Gemma4 uses scaling=1.0 (no 1/sqrt(head_dim)),
    // but the backend always computes attention as QK^T / sqrt(head_dim).
    // Pre-scale Q by sqrt(head_dim) to cancel the backend's division.
    const Q_for_attn = blk: {
        if (config.global_head_dim == 0) break :blk Q_attn; // Not Gemma 4
        const scale = @sqrt(@as(f32, @floatFromInt(head_dim)));
        if (cb.vtable.reshape2d != null) {
            // GPU path: scalar broadcast multiply avoids GPU→CPU sync.
            const scale_shape = [_]i32{1};
            const scale_ct = try cb.fromFloat32Shape(&[_]f32{scale}, &scale_shape);
            defer cb.free(scale_ct);
            break :blk try cb.multiply(Q_attn, scale_ct);
        }
        // CPU fallback (native — no GPU sync penalty).
        const q_data = try cb.toFloat32(Q_attn, allocator);
        defer allocator.free(q_data);
        for (q_data) |*v| v.* *= scale;
        const shape = [_]i32{ @intCast(total), @intCast(@as(usize, num_heads) * head_dim) };
        break :blk try cb.fromFloat32Shape(q_data, &shape);
    };
    defer if (Q_for_attn != Q_attn) cb.free(Q_for_attn);
    try maybeDumpGatedLayerStageStats(cb, allocator, layer, "q", Q_for_attn, num_heads * head_dim);
    try maybeDumpGatedLayerStageStats(cb, allocator, layer, "k", K_attn, num_kv_heads * head_dim);
    try maybeDumpGatedLayerStageStats(cb, allocator, layer, "v", V_normed, num_kv_heads * head_dim);
    debug_timing_stats.attention_qkv_nanos += @intCast(monotonicNowNs() - attn_qkv_started_at);

    if (!disableDecoderRuntimeActivationDebug() and attn_out_proj_linear_slot != null and ffn_norm_slot != null) {
        switch (config.family) {
            .gpt2 => {
                if (allowDenseBlockFastPath(decode_context) and mlp_fc1_slot != null and mlp_fc2_slot != null) {
                    const block_started_at = monotonicNowNs();
                    debug_timing_stats.dense_block_attempts += 1;
                    if (try cb.runDenseDecoderBlock(&.{
                        .q = Q_for_attn,
                        .k = K_attn,
                        .v = V_normed,
                        .residual = hidden,
                        .attention = blk: {
                            var attention = attentionContext(seq_len, decode_context);
                            attention.layer_index = kv_layer_index;
                            attention.skip_kv_write = shares_kv;
                            attention.sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0;
                            if (disableSlidingAttentionDebug()) attention.sliding_window = 0;
                            break :blk attention;
                        },
                        .num_heads = num_heads,
                        .num_kv_heads = num_kv_heads,
                        .head_dim = head_dim,
                        .attention_linear_slot = attn_out_proj_linear_slot.?,
                        .hidden_size = hidden_size,
                        .eps = config.norm_eps,
                        .ffn_layer_norm_slot = if (config.norm_type == .layer_norm) ffn_norm_slot.? else null,
                        .ffn_rms_norm_slot = if (config.norm_type == .rms_norm) ffn_norm_slot.? else null,
                        .first_ffn_linear_slot = mlp_fc1_slot.?,
                        .second_ffn_linear_slot = mlp_fc2_slot.?,
                        .intermediate_size = config.intermediateSize(layer),
                        .activation = decoderRuntimeActivationKind(config.activation),
                    })) |block_out| {
                        debug_timing_stats.dense_block_successes += 1;
                        const block_elapsed = monotonicNowNs() - block_started_at;
                        debug_timing_stats.attention_nanos += @intCast(block_elapsed);
                        debug_timing_stats.ffn_nanos += @intCast(block_elapsed);
                        return block_out;
                    }
                }
            },
            .llama, .mistral, .qwen2, .qwen3, .bitnet => {
                if (mlp_gate_slot != null and mlp_up_slot != null and mlp_down_slot != null) {
                    const block_started_at = monotonicNowNs();
                    debug_timing_stats.gated_block_attempts += 1;
                    debug_timing_stats.gated_block_qkv_attempts += 1;
                    if (try cb.runGatedDecoderBlock(&.{
                        .q = Q_for_attn,
                        .k = K_attn,
                        .v = V_normed,
                        .residual = hidden,
                        .attention = blk: {
                            var attention = attentionContext(seq_len, decode_context);
                            attention.layer_index = kv_layer_index;
                            attention.skip_kv_write = shares_kv;
                            attention.sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0;
                            if (disableSlidingAttentionDebug()) attention.sliding_window = 0;
                            break :blk attention;
                        },
                        .num_heads = num_heads,
                        .num_kv_heads = num_kv_heads,
                        .head_dim = head_dim,
                        .attention_linear_slot = attn_out_proj_linear_slot.?,
                        .hidden_size = hidden_size,
                        .eps = config.norm_eps,
                        .ffn_layer_norm_slot = if (config.norm_type == .layer_norm) ffn_norm_slot.? else null,
                        .ffn_rms_norm_slot = if (config.norm_type == .rms_norm) ffn_norm_slot.? else null,
                        .ffn_post_gate_rms_norm_slot = if (config.family == .bitnet) mlp_sub_norm_slot else null,
                        .ffn_post_down_rms_norm_slot = if (config.family == .gemma) mlp_sub_norm_slot else null,
                        .gate_ffn_linear_slot = mlp_gate_slot.?,
                        .up_ffn_linear_slot = mlp_up_slot.?,
                        .down_ffn_linear_slot = mlp_down_slot.?,
                        .intermediate_size = config.intermediateSize(layer),
                        .activation = decoderRuntimeActivationKind(config.activation),
                        .graph_plan_tail_vocab_size = config.vocab_size,
                    })) |block_out| {
                        debug_timing_stats.gated_block_successes += 1;
                        debug_timing_stats.gated_block_qkv_successes += 1;
                        const block_elapsed = monotonicNowNs() - block_started_at;
                        debug_timing_stats.attention_nanos += @intCast(block_elapsed);
                        debug_timing_stats.ffn_nanos += @intCast(block_elapsed);
                        return block_out;
                    }
                }
            },
            else => {},
        }
    }

    const attn_res = blk: {
        if (attn_out_proj_linear_slot != null and config.family != .qwen3_5) {
            const fused_attn_started_at = monotonicNowNs();
            if (try applyAttentionResidual(
                cb,
                config,
                Q_for_attn,
                K_attn,
                V_normed,
                hidden,
                batch,
                seq_len,
                num_heads,
                num_kv_heads,
                head_dim,
                hidden_size,
                layer,
                kv_layer_index,
                shares_kv,
                attn_out_proj_linear_slot.?,
                if (config.family == .bitnet) attn_sub_norm_slot else null,
                if (config.family == .gemma) ffn_norm_slot else null,
                decode_context,
            )) |fused_attn_res| {
                debug_timing_stats.attention_nanos += @intCast(monotonicNowNs() - attn_started_at);
                debug_timing_stats.attention_core_nanos += @intCast(monotonicNowNs() - fused_attn_started_at);
                break :blk fused_attn_res;
            }
        }

        // Attention (with optional RoPE)
        const attn_core_started_at = monotonicNowNs();
        const attn_out = try applyAttention(cb, config, Q_for_attn, K_attn, V_normed, batch, seq_len, num_heads, num_kv_heads, head_dim, layer, kv_layer_index, shares_kv, decode_context);
        defer cb.free(attn_out);
        debug_timing_stats.attention_core_nanos += @intCast(monotonicNowNs() - attn_core_started_at);
        const gated_attn_out = if (q_gate) |gate| blk_gate: {
            const sigmoid_gate = try cb.sigmoid(gate);
            defer cb.free(sigmoid_gate);
            break :blk_gate try cb.multiply(attn_out, sigmoid_gate);
        } else attn_out;
        defer if (gated_attn_out != attn_out) cb.free(gated_attn_out);
        try maybeDebugLayerTensorLastRow(cb, allocator, layer, "attn_out", gated_attn_out, num_heads * head_dim);
        try maybeDebugLayerTensor(cb, allocator, layer, "attn_out", gated_attn_out);
        try maybeDumpGatedLayerStageStats(cb, allocator, layer, "attn-out", gated_attn_out, num_heads * head_dim);

        const attn_proj_input = if (config.family == .bitnet)
            if (attn_sub_norm_slot != null)
                (try cb.decoderRuntimeApplyRmsNorm(&.{
                    .slot = attn_sub_norm_slot.?,
                    .input = gated_attn_out,
                    .hidden_size = hidden_size,
                    .eps = config.norm_eps,
                })) orelse try applyBitNetAttentionSubNorm(cb, allocator, config, gated_attn_out, layer, &name_buf)
            else
                try applyBitNetAttentionSubNorm(cb, allocator, config, gated_attn_out, layer, &name_buf)
        else
            gated_attn_out;
        defer if (attn_proj_input != gated_attn_out) cb.free(attn_proj_input);

        // Output projection
        const attn_out_proj_started_at = monotonicNowNs();
        const proj = if (attn_out_proj_linear_slot != null)
            (try cb.decoderRuntimeApplyLinear(&.{
                .slot = attn_out_proj_linear_slot.?,
                .input = attn_proj_input,
                .in_dim = @as(usize, num_heads) * head_dim,
                .out_dim = hidden_size,
            })) orelse try attnOutputProject(cb, allocator, config, attn_proj_input, total, num_heads * head_dim, hidden_size, layer, &name_buf)
        else
            try attnOutputProject(cb, allocator, config, attn_proj_input, total, num_heads * head_dim, hidden_size, layer, &name_buf);
        defer cb.free(proj);
        debug_timing_stats.attention_out_proj_nanos += @intCast(monotonicNowNs() - attn_out_proj_started_at);
        try maybeDebugLayerTensorLastRow(cb, allocator, layer, "attn_proj", proj, hidden_size);
        try maybeDebugLayerTensor(cb, allocator, layer, "attn_proj", proj);
        try maybeDumpGatedLayerStageStats(cb, allocator, layer, "attn-proj", proj, hidden_size);
        debug_timing_stats.attention_nanos += @intCast(monotonicNowNs() - attn_started_at);

        if (config.family == .gemma) {
            const attn_post = try applyGemmaPostAttentionNorm(cb, allocator, config, proj, layer, &name_buf);
            defer if (attn_post != proj) cb.free(attn_post);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "attn-post", attn_post, hidden_size);

            const sa_out = try cb.add(attn_post, hidden);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "attn-residual", sa_out, hidden_size);

            if (config.usesMoe() and config.hasSharedExpert()) {
                // Gemma 4 three-sublayer block: attention → shared expert FFN → MoE routed experts.
                // Each sublayer has its own pre/post norms and residual connection.

                // Gemma 4 parallel FFN: shared expert and MoE both branch from attn_out,
                // their outputs are summed, then an overall post-norm + residual is applied.
                //
                // Shared expert path: RMSNorm → dense FFN → RMSNorm
                var norm_started_at = monotonicNowNs();
                const shared_normed = try applyGemmaFfnPreNorm(cb, allocator, config, sa_out, layer, &name_buf);
                defer cb.free(shared_normed);
                debug_timing_stats.norm_nanos += @intCast(monotonicNowNs() - norm_started_at);
                const shared_ffn_started_at = monotonicNowNs();
                const shared_out = try denseFeedForward(cb, allocator, config, shared_normed, total, layer, &name_buf);
                defer cb.free(shared_out);
                const shared_ffn_elapsed = monotonicNowNs() - shared_ffn_started_at;
                debug_timing_stats.ffn_nanos += @intCast(shared_ffn_elapsed);
                debug_timing_stats.shared_expert_ffn_nanos += @intCast(shared_ffn_elapsed);
                norm_started_at = monotonicNowNs();
                const shared_post = try applyGemmaSharedFfnPostNorm(cb, allocator, config, shared_out, layer, &name_buf);
                defer if (shared_post != shared_out) cb.free(shared_post);

                // MoE routed expert path: RMSNorm → MoE → RMSNorm (branches from sa_out too)
                const moe_normed = try applyGemmaMoeFfnPreNorm(cb, allocator, config, sa_out, layer, &name_buf);
                defer cb.free(moe_normed);
                debug_timing_stats.norm_nanos += @intCast(monotonicNowNs() - norm_started_at);
                const moe_started_at = monotonicNowNs();
                const moe_out = try moeFeedForwardRoutedOnly(cb, allocator, config, moe_normed, total, layer, &name_buf, decode_context);
                defer cb.free(moe_out);
                debug_timing_stats.ffn_nanos += @intCast(monotonicNowNs() - moe_started_at);
                norm_started_at = monotonicNowNs();
                const moe_post = try applyGemmaMoeFfnPostNorm(cb, allocator, config, moe_out, layer, &name_buf);
                defer if (moe_post != moe_out) cb.free(moe_post);

                // Combine: shared + MoE, then overall FFN post-norm, then residual
                const combined = try cb.add(shared_post, moe_post);
                defer cb.free(combined);
                const combined_normed = try applyGemmaFfnPostNorm(cb, allocator, config, combined, layer, &name_buf);
                defer if (combined_normed != combined) cb.free(combined_normed);
                debug_timing_stats.norm_nanos += @intCast(monotonicNowNs() - norm_started_at);
                var layer_result = try cb.add(combined_normed, sa_out);
                cb.free(sa_out);

                if (config.hasPle()) {
                    if (ple_vectors) |ple| {
                        const ple_result = try applyPle(cb, allocator, config, layer_result, ple, total, layer, &name_buf);
                        cb.free(layer_result);
                        layer_result = ple_result;
                    }
                }
                const scaled = try applyLayerOutputScale(cb, allocator, config, layer_result, total, hidden_size, layer);
                try dumpLayerLastRowStats(cb, allocator, layer, scaled, hidden_size);
                return scaled;
            }

            const ffn_normed = try applyGemmaFfnPreNorm(cb, allocator, config, sa_out, layer, &name_buf);
            defer cb.free(ffn_normed);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "ffn-norm", ffn_normed, hidden_size);

            const ffn_started_at = monotonicNowNs();
            const ffn_out_raw = try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
            defer cb.free(ffn_out_raw);
            try maybeDebugLayerTensorLastRow(cb, allocator, layer, "ffn_raw", ffn_out_raw, hidden_size);
            try maybeDebugLayerTensor(cb, allocator, layer, "ffn_raw", ffn_out_raw);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "ffn-raw", ffn_out_raw, hidden_size);
            debug_timing_stats.ffn_nanos += @intCast(monotonicNowNs() - ffn_started_at);

            const ffn_out = try applyGemmaFfnPostNorm(cb, allocator, config, ffn_out_raw, layer, &name_buf);
            defer if (ffn_out != ffn_out_raw) cb.free(ffn_out);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "ffn-post", ffn_out, hidden_size);

            const ffn_residual = try cb.add(ffn_out, sa_out);
            cb.free(sa_out);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "ffn-residual", ffn_residual, hidden_size);

            var layer_result = ffn_residual;
            if (config.hasPle()) {
                if (ple_vectors) |ple| {
                    const ple_result = try applyPle(cb, allocator, config, ffn_residual, ple, total, layer, &name_buf);
                    cb.free(ffn_residual);
                    layer_result = ple_result;
                }
            }
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "ple", layer_result, hidden_size);
            const scaled = try applyLayerOutputScale(cb, allocator, config, layer_result, total, hidden_size, layer);
            try maybeDumpGatedLayerStageStats(cb, allocator, layer, "output-scale", scaled, hidden_size);
            try dumpLayerLastRowStats(cb, allocator, layer, scaled, hidden_size);
            return scaled;
        }

        // Residual
        break :blk try cb.add(proj, hidden);
    };

    // --- FFN sublayer ---
    const ffn_normed = if (ffn_norm_slot != null) blk: {
        const raw_norm = switch (config.norm_type) {
            .layer_norm => try cb.decoderRuntimeApplyLayerNorm(&.{
                .slot = ffn_norm_slot.?,
                .input = attn_res,
                .hidden_size = hidden_size,
                .eps = config.norm_eps,
            }),
            .rms_norm => try cb.decoderRuntimeApplyRmsNorm(&.{
                .slot = ffn_norm_slot.?,
                .input = attn_res,
                .hidden_size = hidden_size,
                .eps = config.norm_eps,
            }),
        };
        break :blk raw_norm orelse try applyFFNNorm(cb, allocator, config, attn_res, layer, &name_buf);
    } else try applyFFNNorm(cb, allocator, config, attn_res, layer, &name_buf);
    defer cb.free(ffn_normed);
    try maybeDebugLayerTensorLastRow(cb, allocator, layer, "ffn_norm", ffn_normed, hidden_size);
    const ffn_started_at = monotonicNowNs();
    var ffn_out_includes_residual = false;
    const ffn_out = if (mlp_fc1_slot != null and mlp_fc2_slot != null) blk: {
        const inter_size = config.intermediateSize(layer);
        const activation_kind = decoderRuntimeActivationKind(config.activation);
        if (debugDenseFfnCompareEnabled() and config.family == .gpt2 and decode_context != null and decode_context.?.attention_mode == .paged_decode) {
            if (try cb.runDenseFfnResidual(&.{
                .first_linear_slot = mlp_fc1_slot.?,
                .second_linear_slot = mlp_fc2_slot.?,
                .input = ffn_normed,
                .residual = attn_res,
                .hidden_size = hidden_size,
                .intermediate_size = inter_size,
                .activation = activation_kind,
            })) |fused| {
                defer cb.free(fused);
                const fallback_ffn = try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
                defer cb.free(fallback_ffn);
                const fallback_residual = try cb.add(fallback_ffn, attn_res);
                defer cb.free(fallback_residual);
                try maybeDebugTensorLastRowDiff(cb, allocator, "dense_ffn_compare", fused, fallback_residual, hidden_size);
            }
        }
        if (!disableDecoderRuntimeActivationDebug() and allowDenseFfnResidualFastPath(config, decode_context)) {
            if (try cb.runDenseFfnResidual(&.{
                .first_linear_slot = mlp_fc1_slot.?,
                .second_linear_slot = mlp_fc2_slot.?,
                .input = ffn_normed,
                .residual = attn_res,
                .hidden_size = hidden_size,
                .intermediate_size = inter_size,
                .activation = activation_kind,
            })) |fused| {
                ffn_out_includes_residual = true;
                break :blk fused;
            }
        }

        const fc1_out = (try cb.decoderRuntimeApplyLinear(&.{
            .slot = mlp_fc1_slot.?,
            .input = ffn_normed,
            .in_dim = hidden_size,
            .out_dim = inter_size,
        })) orelse break :blk try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
        defer cb.free(fc1_out);

        const activated = try applyActivation(cb, config, fc1_out);
        defer cb.free(activated);

        break :blk (try cb.decoderRuntimeApplyLinear(&.{
            .slot = mlp_fc2_slot.?,
            .input = activated,
            .in_dim = inter_size,
            .out_dim = hidden_size,
        })) orelse try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
    } else if (mlp_gate_slot != null and mlp_up_slot != null and mlp_down_slot != null and switch (config.family) {
        .llama, .mistral, .qwen2, .gemma => true,
        else => false,
    }) blk: {
        const inter_size = config.intermediateSize(layer);
        if (!disableDecoderRuntimeActivationDebug()) {
            if (try cb.runGatedFfnResidual(&.{
                .gate_linear_slot = mlp_gate_slot.?,
                .up_linear_slot = mlp_up_slot.?,
                .down_linear_slot = mlp_down_slot.?,
                .input = ffn_normed,
                .residual = attn_res,
                .post_down_rms_norm_slot = if (config.family == .gemma) mlp_sub_norm_slot else null,
                .hidden_size = hidden_size,
                .intermediate_size = inter_size,
                .eps = config.norm_eps,
                .activation = decoderRuntimeActivationKind(config.activation),
            })) |fused| {
                ffn_out_includes_residual = true;
                break :blk fused;
            }
        }
        break :blk try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
    } else if (config.family == .bitnet and mlp_gate_slot != null and mlp_up_slot != null and mlp_sub_norm_slot != null and mlp_down_slot != null) blk: {
        const inter_size = config.intermediateSize(layer);
        const gate_up = (try cb.decoderRuntimeApplyLinearPair(&.{
            .slot_a = mlp_gate_slot.?,
            .slot_b = mlp_up_slot.?,
            .input = ffn_normed,
            .in_dim = hidden_size,
            .out_dim = inter_size,
        })) orelse break :blk try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
        const gate_proj = gate_up.first;
        defer cb.free(gate_proj);
        const up_proj = gate_up.second;
        defer cb.free(up_proj);

        const activated = try applyActivation(cb, config, gate_proj);
        defer cb.free(activated);

        const gated = try cb.multiply(activated, up_proj);
        defer cb.free(gated);

        const normed_gated = (try cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = mlp_sub_norm_slot.?,
            .input = gated,
            .hidden_size = inter_size,
            .eps = config.norm_eps,
        })) orelse break :blk try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
        defer cb.free(normed_gated);

        break :blk (try cb.decoderRuntimeApplyLinear(&.{
            .slot = mlp_down_slot.?,
            .input = normed_gated,
            .in_dim = inter_size,
            .out_dim = hidden_size,
        })) orelse try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
    } else try feedForward(cb, allocator, config, ffn_normed, total, layer, &name_buf, decode_context);
    var free_ffn_out = true;
    defer if (free_ffn_out) cb.free(ffn_out);
    try maybeDebugLayerTensorLastRow(cb, allocator, layer, "ffn_out", ffn_out, hidden_size);
    try maybeDebugLayerTensor(cb, allocator, layer, "ffn_out", ffn_out);
    debug_timing_stats.ffn_nanos += @intCast(monotonicNowNs() - ffn_started_at);

    // Residual
    const result = if (ffn_out_includes_residual) blk: {
        free_ffn_out = false;
        break :blk ffn_out;
    } else try cb.add(ffn_out, attn_res);
    cb.free(attn_res);

    try dumpLayerLastRowStats(cb, allocator, layer, result, hidden_size);
    return result;
}

pub fn debugDecoderBlockNoOverrides(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    batch: usize,
    seq_len: usize,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    decode_context: ?*const DecodeContext,
    ple_vectors: ?CT,
) !CT {
    return decoderBlock(
        cb,
        allocator,
        config,
        hidden,
        batch,
        seq_len,
        num_kv_heads,
        head_dim,
        layer,
        decode_context,
        ple_vectors,
        .{},
        null,
        null,
        null,
        null,
        null,
    );
}

pub fn debugGemmaFfnPreNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return applyGemmaFfnPreNorm(cb, allocator, config, hidden, layer, buf);
}

pub fn debugFeedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    layer: usize,
    buf: *[256]u8,
    decode_context: ?*const DecodeContext,
) !CT {
    return feedForward(cb, allocator, config, input, total, layer, buf, decode_context);
}

pub fn debugAttentionProject(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    in_dim: u32,
    out_dim: u32,
    layer: usize,
    proj: []const u8,
    buf: *[256]u8,
) !CT {
    return attnProject(cb, allocator, config, input, total, in_dim, out_dim, layer, proj, buf);
}

pub fn debugAttentionOutputProject(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    in_dim: u32,
    out_dim: u32,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return attnOutputProject(cb, allocator, config, input, total, in_dim, out_dim, layer, buf);
}

pub fn debugGemmaPostAttentionNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return applyGemmaPostAttentionNorm(cb, allocator, config, hidden, layer, buf);
}

pub fn debugGemmaFfnPostNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return applyGemmaFfnPostNorm(cb, allocator, config, hidden, layer, buf);
}

pub fn maybeApplyQKHeadNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    tensor: CT,
    total_rows: usize,
    total_dim: u32,
    layer: usize,
    proj: []const u8,
    head_dim: u32,
    buf: *[256]u8,
) !?CT {
    switch (config.family) {
        .gemma, .qwen3, .qwen3_5 => {},
        else => return null,
    }

    const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_norm.weight", .{ layer, proj }) catch return error.NameTooLong;
    const base_weight = getModelWeight(cb, config, name) catch |err| switch (err) {
        error.MissingWeight, error.WeightNotFound => return null,
        else => return err,
    };
    defer cb.free(base_weight);
    const adjusted_weight = try maybeAdjustNormWeight(cb, allocator, config, base_weight, head_dim);
    defer if (adjusted_weight != base_weight) cb.free(adjusted_weight);

    const num_heads_for_norm = total_dim / head_dim;
    if (try cb.reshape2d(tensor, total_rows * num_heads_for_norm, head_dim)) |reshaped| {
        // GPU path: reshape so last dim matches weight [head_dim], rmsNorm on GPU.
        defer cb.free(reshaped);
        const normed_flat = try cb.rmsNorm(reshaped, adjusted_weight, head_dim, config.norm_eps);
        defer cb.free(normed_flat);
        return (try cb.reshape2d(normed_flat, total_rows, total_dim)) orelse return error.ReshapeFailed;
    }

    // CPU fallback (native — rmsNorm handles chunked dim natively).
    const tensor_data = try cb.toFloat32(tensor, allocator);
    defer allocator.free(tensor_data);
    const weight_data = try cb.toFloat32(adjusted_weight, allocator);
    defer allocator.free(weight_data);

    const output = try allocator.dupe(f32, tensor_data);
    defer allocator.free(output);
    activations.rmsNorm(output, weight_data, head_dim, config.norm_eps);

    const shape = [_]i32{ @intCast(total_rows), @intCast(total_dim) };
    return try cb.fromFloat32Shape(output, &shape);
}

pub fn maybeScaleTokenEmbeddings(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    embeddings: CT,
    total_rows: usize,
    dim: usize,
) !CT {
    const scale = config.tokenEmbeddingScale();
    if (std.math.approxEqAbs(f32, scale, 1.0, 1e-6)) return embeddings;

    if (cb.kind() == .graph or cb.kind() == .metal) {
        // Graph tracing and Metal decode should avoid materializing device
        // embeddings just to apply a scalar.
        const scale_data: [1]f32 = .{scale};
        const scale_tensor = try cb.fromFloat32(&scale_data);
        defer cb.free(scale_tensor);
        const result = try cb.multiply(embeddings, scale_tensor);
        cb.free(embeddings);
        return result;
    }

    const data = try cb.toFloat32(embeddings, allocator);
    defer allocator.free(data);
    const scaled = try allocator.alloc(f32, data.len);
    errdefer allocator.free(scaled);
    for (data, 0..) |value, i| scaled[i] = value * scale;

    const shape = [_]i32{ @intCast(total_rows), @intCast(dim) };
    const result = try cb.fromFloat32Shape(scaled, &shape);
    allocator.free(scaled);
    cb.free(embeddings);
    return result;
}

pub fn maybeAdjustNormWeight(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    weight: CT,
    dim: usize,
) !CT {
    if (std.math.approxEqAbs(f32, config.norm_weight_offset, 0.0, 1e-6)) return weight;

    if (cb.kind() == .graph) {
        // Graph tracing: use GPU add to avoid toFloat32 which registers spurious outputs.
        const offset_data: [1]f32 = .{config.norm_weight_offset};
        const offset_tensor = try cb.fromFloat32(&offset_data);
        defer cb.free(offset_tensor);
        return cb.add(weight, offset_tensor);
    }

    const weight_data = try cb.toFloat32(weight, allocator);
    defer allocator.free(weight_data);
    const adjusted = try allocator.alloc(f32, weight_data.len);
    errdefer allocator.free(adjusted);
    for (weight_data, 0..) |value, i| adjusted[i] = value + config.norm_weight_offset;

    const shape = [_]i32{@intCast(dim)};
    const result = try cb.fromFloat32Shape(adjusted, &shape);
    allocator.free(adjusted);
    return result;
}

fn applyFinalLogitSoftcap(config: Config, logits: []f32) void {
    if (config.final_logit_softcapping <= 0.0) return;
    const softcap = config.final_logit_softcapping;
    for (logits) |*value| {
        value.* = std.math.tanh(value.* / softcap) * softcap;
    }
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    if (comptime is_freestanding) return false;
    return platform.env.getenvBool(name);
}

fn getenvUsize(comptime name: [*:0]const u8) ?usize {
    if (comptime is_freestanding) return null;
    return platform.env.getenvUsize(name);
}

fn isDecodeStep(decode_context: ?*const DecodeContext) bool {
    const dc = decode_context orelse return false;
    return dc.attention_mode == .paged_decode or
        (dc.query_sequence_len == 1 and dc.kv_sequence_len > dc.query_sequence_len);
}

fn decoderLayerEvalStride(config: Config, decode_context: ?*const DecodeContext) usize {
    if (isDecodeStep(decode_context)) {
        if (config.usesMoe()) {
            const stride = getenvUsize("TERMITE_MOE_DECODE_EVAL_STRIDE") orelse default_moe_decode_eval_stride;
            return @max(@as(usize, 1), stride);
        }
        return getenvUsize("TERMITE_DENSE_DECODE_EVAL_STRIDE") orelse 0;
    }
    if (config.usesMoe() or decode_context != null) return 2;
    return 1;
}

fn shouldEvalDecoderLayer(eval_stride: usize, layer: usize, num_hidden_layers: usize) bool {
    if (eval_stride == 0) return false;
    return layer % eval_stride == eval_stride - 1 or layer == num_hidden_layers - 1;
}

fn disableSlidingAttentionDebug() bool {
    return getenvBool("TERMITE_DISABLE_SLIDING_ATTENTION");
}

fn disableSharedKvDebug() bool {
    return getenvBool("TERMITE_DISABLE_SHARED_KV");
}

fn disablePleDebug() bool {
    return getenvBool("TERMITE_DISABLE_PLE");
}

fn disableDecoderRuntimeActivationDebug() bool {
    return getenvBool("TERMITE_METAL_DECODER_RUNTIME_DISABLE_ACTIVATION") or
        getenvBool("TERMITE_METAL_WHOLE_TOKEN_DISABLE_ACTIVATION");
}

fn disableDenseBlockFastPathDebug() bool {
    return getenvBool("TERMITE_METAL_DISABLE_DENSE_BLOCK");
}

fn disableReservedHiddenCarrierDebug() bool {
    return getenvBool("TERMITE_METAL_DISABLE_RESERVED_HIDDEN_CARRIER");
}

fn forceLayerCloneDebug() bool {
    return getenvBool("TERMITE_METAL_FORCE_LAYER_CLONE");
}

fn disablePreparedDecoderSlotsDebug() bool {
    return getenvBool("TERMITE_METAL_DISABLE_PREPARED_DECODER_SLOTS");
}

fn allowDenseBlockFastPath(decode_context: ?*const DecodeContext) bool {
    if (disableDenseBlockFastPathDebug()) return false;
    const ctx = decode_context orelse return true;
    // GPT-2 paged prefill now matches native on the dense whole-block path, but
    // paged decode still has a late-step correctness regression there. Keep the
    // fast path for prefill and route decode through the smaller, correct paths
    // until the direct dense decode block is fixed.
    return ctx.attention_mode != .paged_decode;
}

fn allowDenseFfnResidualFastPath(config: Config, decode_context: ?*const DecodeContext) bool {
    const ctx = decode_context orelse return true;
    // The smaller dense FFN residual fast path is still not decode-correct for
    // GPT-2 under paged decode, even when routed through the fused runtime
    // kernel. Keep it available elsewhere and continue using the unfused path
    // here until the fused FFN output matches native.
    return !(config.family == .gpt2 and ctx.attention_mode == .paged_decode);
}

fn prefillTraceEnabled() bool {
    return getenvBool("TERMITE_METAL_PREFILL_TRACE");
}

fn debugLastRowEnabled() bool {
    return getenvBool("TERMITE_DEBUG_GPT_LAST_ROW");
}

fn debugTopLogitsEnabled() bool {
    return getenvBool("TERMITE_DEBUG_TOP_LOGITS");
}

fn debugTopLogitRowsEnabled() bool {
    return getenvBool("TERMITE_DEBUG_TOP_LOGIT_ROWS");
}

fn debugTensorStatsEnabled() bool {
    return getenvBool("TERMITE_DEBUG_GPT_STATS");
}

fn debugDenseFfnCompareEnabled() bool {
    return getenvBool("TERMITE_DEBUG_DENSE_FFN_COMPARE");
}

fn debugTensorSampleIndex() ?usize {
    return getenvUsize("TERMITE_DEBUG_GPT_SAMPLE_INDEX");
}

fn dumpGatedLayerTarget() ?usize {
    return getenvUsize("TERMITE_METAL_DUMP_GATED_LAYER");
}

fn shouldDumpGatedLayer(layer: usize) bool {
    return if (dumpGatedLayerTarget()) |target| target == layer else false;
}

pub fn maybeDumpGatedLayerStageStats(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    stage: []const u8,
    tensor: CT,
    row_dim: usize,
) !void {
    if (!shouldDumpGatedLayer(layer)) return;
    const values = try cb.toFloat32(tensor, allocator);
    defer allocator.free(values);
    if (values.len < row_dim or row_dim == 0) return;
    const row_count = values.len / row_dim;
    if (row_count == 0) return;
    const row = values[(row_count - 1) * row_dim ..][0..row_dim];
    var min_value: f32 = row[0];
    var max_value: f32 = row[0];
    var sum: f64 = 0;
    var sum_abs: f64 = 0;
    for (row) |value| {
        min_value = @min(min_value, value);
        max_value = @max(max_value, value);
        sum += value;
        sum_abs += @abs(value);
    }
    const hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(row));
    debugPrint(
        "gated-family-dump layer-{d}-{s}: row_dim={d} hash=0x{x} min={d:.6} max={d:.6} mean={d:.6} mean_abs={d:.6}\n",
        .{
            layer,
            stage,
            row.len,
            hash,
            min_value,
            max_value,
            sum / @as(f64, @floatFromInt(row.len)),
            sum_abs / @as(f64, @floatFromInt(row.len)),
        },
    );
}

fn dumpLayerLastRowStats(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    tensor: CT,
    row_dim: usize,
) !void {
    try maybeDumpGatedLayerStageStats(cb, allocator, layer, "hidden", tensor, row_dim);
}

fn maybeDebugTopLogits(logits: []const f32, vocab_size: usize) void {
    if (!debugTopLogitsEnabled()) return;
    if (vocab_size == 0 or logits.len < vocab_size) return;
    const row_count = logits.len / vocab_size;
    if (row_count == 0) return;
    if (debugTopLogitRowsEnabled()) {
        debugTopLogitsRow("top_logits_row0", logits[0..vocab_size]);
        if (row_count > 1) {
            debugTopLogitsRow("top_logits_row_last", logits[(row_count - 1) * vocab_size ..][0..vocab_size]);
        }
    }
    const row = logits[(row_count - 1) * vocab_size ..][0..vocab_size];
    debugTopLogitsRow("top_logits", row);
}

fn debugTopLogitsRow(label: []const u8, row: []const f32) void {
    if (is_freestanding) return;
    var top_ids = [_]usize{0} ** 8;
    var top_vals = [_]f32{-std.math.inf(f32)} ** 8;
    for (row, 0..) |logit, idx| {
        var insert_at: ?usize = null;
        for (top_vals, 0..) |current, slot| {
            if (logit > current) {
                insert_at = slot;
                break;
            }
        }
        if (insert_at) |slot| {
            var i: usize = top_vals.len - 1;
            while (i > slot) : (i -= 1) {
                top_vals[i] = top_vals[i - 1];
                top_ids[i] = top_ids[i - 1];
            }
            top_vals[slot] = logit;
            top_ids[slot] = idx;
        }
    }
    debugPrint("{s}:", .{label});
    for (top_ids, top_vals) |id, value| {
        debugPrint(" {d}:{d:.6}", .{ id, value });
    }
    debugPrint("\n", .{});
}

fn maybeDebugTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    label: []const u8,
    tensor: CT,
) !void {
    if (is_freestanding) return;
    if (!debugTensorStatsEnabled()) return;
    const values = try cb.toFloat32(tensor, allocator);
    defer allocator.free(values);
    if (values.len == 0) return;

    var sum: f64 = 0.0;
    var l2: f64 = 0.0;
    var max_abs: f32 = 0.0;
    var max_abs_idx: usize = 0;
    var nan_count: usize = 0;
    var inf_count: usize = 0;
    var first_nan_idx: ?usize = null;
    var first_inf_idx: ?usize = null;
    for (values, 0..) |value, idx| {
        if (std.math.isNan(value)) {
            nan_count += 1;
            if (first_nan_idx == null) first_nan_idx = idx;
            continue;
        }
        if (!std.math.isFinite(value)) {
            inf_count += 1;
            if (first_inf_idx == null) first_inf_idx = idx;
            continue;
        }
        sum += value;
        l2 += @as(f64, value) * @as(f64, value);
        const abs_value = @abs(value);
        if (abs_value > max_abs) {
            max_abs = abs_value;
            max_abs_idx = idx;
        }
    }
    const hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(values));
    debugPrint(
        "{s} stats: len={d} hash=0x{x} sum={d:.6} l2={d:.6} max_abs={d:.6}@{d} nan={d}@{any} inf={d}@{any}\n",
        .{ label, values.len, hash, sum, l2, max_abs, max_abs_idx, nan_count, first_nan_idx, inf_count, first_inf_idx },
    );
    if (debugTensorSampleIndex()) |sample_idx| {
        if (sample_idx < values.len) {
            debugPrint("{s} sample[{d}]={d}\n", .{ label, sample_idx, values[sample_idx] });
        }
    }
}

fn maybeDebugTensorLastRow(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    label: []const u8,
    tensor: CT,
    row_dim: usize,
) !void {
    if (is_freestanding) return;
    if (!debugLastRowEnabled()) return;
    const values = try cb.toFloat32(tensor, allocator);
    defer allocator.free(values);
    if (values.len < row_dim or row_dim == 0) return;
    const row_count = values.len / row_dim;
    if (row_count == 0) return;
    const row = values[(row_count - 1) * row_dim ..][0..row_dim];
    const limit = @min(row_dim, 8);
    debugPrint("{s} last_row:", .{label});
    for (row[0..limit]) |value| {
        debugPrint(" {d:.6}", .{value});
    }
    debugPrint("\n", .{});
}

fn maybeDebugTensorLastRowDiff(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    label: []const u8,
    lhs: CT,
    rhs: CT,
    row_dim: usize,
) !void {
    if (is_freestanding) return;
    if (!debugDenseFfnCompareEnabled()) return;
    const lhs_values = try cb.toFloat32(lhs, allocator);
    defer allocator.free(lhs_values);
    const rhs_values = try cb.toFloat32(rhs, allocator);
    defer allocator.free(rhs_values);
    if (lhs_values.len != rhs_values.len or lhs_values.len < row_dim or row_dim == 0) return;
    const row_count = lhs_values.len / row_dim;
    if (row_count == 0) return;
    const lhs_row = lhs_values[(row_count - 1) * row_dim ..][0..row_dim];
    const rhs_row = rhs_values[(row_count - 1) * row_dim ..][0..row_dim];
    var max_abs: f32 = 0.0;
    var max_idx: usize = 0;
    var sum_abs: f64 = 0.0;
    for (lhs_row, rhs_row, 0..) |l, r, idx| {
        const abs_value = @abs(l - r);
        sum_abs += abs_value;
        if (abs_value > max_abs) {
            max_abs = abs_value;
            max_idx = idx;
        }
    }
    debugPrint(
        "{s} diff: row_dim={d} max_abs={d:.6}@{d} mean_abs={d:.6}\n",
        .{ label, row_dim, max_abs, max_idx, sum_abs / @as(f64, @floatFromInt(row_dim)) },
    );
    const limit = @min(row_dim, 8);
    debugPrint("{s} lhs:", .{label});
    for (lhs_row[0..limit]) |value| debugPrint(" {d:.6}", .{value});
    debugPrint("\n", .{});
    debugPrint("{s} rhs:", .{label});
    for (rhs_row[0..limit]) |value| debugPrint(" {d:.6}", .{value});
    debugPrint("\n", .{});
}

fn cloneTensorMaterialized(cb: *const ComputeBackend, allocator: std.mem.Allocator, tensor: CT) !CT {
    const values = try cb.toFloat32(tensor, allocator);
    defer allocator.free(values);
    const shape_i64 = try cb.tensorShape(tensor, allocator);
    defer allocator.free(shape_i64);
    const shape_i32 = try allocator.alloc(i32, shape_i64.len);
    defer allocator.free(shape_i32);
    for (shape_i64, 0..) |dim, idx| shape_i32[idx] = @intCast(dim);
    return cb.fromFloat32Shape(values, shape_i32);
}

pub fn qwen35LinearAttention(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    batch: usize,
    seq_len: usize,
    layer: usize,
    decode_context: ?*const DecodeContext,
    name_buf: *[256]u8,
) !CT {
    if (batch != 1) return error.UnsupportedQwen35LinearAttentionBatch;
    if (config.qwen35_linear_conv_kernel_dim == 0 or
        config.qwen35_linear_key_head_dim == 0 or
        config.qwen35_linear_value_head_dim == 0 or
        config.qwen35_linear_num_key_heads == 0 or
        config.qwen35_linear_num_value_heads == 0)
    {
        return error.InvalidQwen35LinearAttentionConfig;
    }

    const hidden_size: usize = config.hidden_size;
    const key_heads: usize = @intCast(config.qwen35_linear_num_key_heads);
    const value_heads: usize = @intCast(config.qwen35_linear_num_value_heads);
    const key_head_dim: usize = @intCast(config.qwen35_linear_key_head_dim);
    const value_head_dim: usize = @intCast(config.qwen35_linear_value_head_dim);
    const key_dim = key_heads * key_head_dim;
    const value_dim = value_heads * value_head_dim;
    const conv_dim = key_dim * 2 + value_dim;
    const conv_kernel: usize = @intCast(config.qwen35_linear_conv_kernel_dim);
    const total = batch * seq_len;
    if (total == 0) return error.InvalidSequenceLength;
    if (value_heads % key_heads != 0) return error.InvalidQwen35LinearAttentionConfig;

    const mixed_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.in_proj_qkv.weight", .{layer}) catch return error.NameTooLong;
    const mixed_w = try getModelWeight(cb, config, mixed_w_name);
    defer cb.free(mixed_w);
    const mixed = try cb.linearNoBias(input, mixed_w, total, hidden_size, conv_dim);
    defer cb.free(mixed);

    const z_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.in_proj_z.weight", .{layer}) catch return error.NameTooLong;
    const z_w = try getModelWeight(cb, config, z_w_name);
    defer cb.free(z_w);
    const z = try cb.linearNoBias(input, z_w, total, hidden_size, value_dim);
    defer cb.free(z);

    const b_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.in_proj_b.weight", .{layer}) catch return error.NameTooLong;
    const b_w = try getModelWeight(cb, config, b_w_name);
    defer cb.free(b_w);
    const beta_proj = try cb.linearNoBias(input, b_w, total, hidden_size, value_heads);
    defer cb.free(beta_proj);

    const a_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.in_proj_a.weight", .{layer}) catch return error.NameTooLong;
    const a_w = try getModelWeight(cb, config, a_w_name);
    defer cb.free(a_w);
    const a_proj = try cb.linearNoBias(input, a_w, total, hidden_size, value_heads);
    defer cb.free(a_proj);

    const conv_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.conv1d.weight", .{layer}) catch return error.NameTooLong;
    const conv_w = try getModelWeight(cb, config, conv_w_name);
    defer cb.free(conv_w);
    const a_log_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.A_log", .{layer}) catch return error.NameTooLong;
    const a_log_w = try getModelWeight(cb, config, a_log_name);
    defer cb.free(a_log_w);
    const dt_bias_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.dt_bias", .{layer}) catch return error.NameTooLong;
    const dt_bias_w = try getModelWeight(cb, config, dt_bias_name);
    defer cb.free(dt_bias_w);
    const norm_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.norm.weight", .{layer}) catch return error.NameTooLong;
    const norm_w = try getModelWeight(cb, config, norm_w_name);
    defer cb.free(norm_w);

    const mixed_host = try cb.toFloat32(mixed, allocator);
    defer allocator.free(mixed_host);
    const z_host = try cb.toFloat32(z, allocator);
    defer allocator.free(z_host);
    const beta_host = try cb.toFloat32(beta_proj, allocator);
    defer allocator.free(beta_host);
    const a_host = try cb.toFloat32(a_proj, allocator);
    defer allocator.free(a_host);
    const conv_weight = try cb.toFloat32(conv_w, allocator);
    defer allocator.free(conv_weight);
    const a_log = try cb.toFloat32(a_log_w, allocator);
    defer allocator.free(a_log);
    const dt_bias = try cb.toFloat32(dt_bias_w, allocator);
    defer allocator.free(dt_bias);
    const adjusted_norm_w = try maybeAdjustNormWeight(cb, allocator, config, norm_w, value_head_dim);
    defer if (adjusted_norm_w != norm_w) cb.free(adjusted_norm_w);
    const norm_weight = try cb.toFloat32(adjusted_norm_w, allocator);
    defer allocator.free(norm_weight);

    const maybe_state = if (decode_context) |dc|
        if (dc.qwen35_linear_cache) |cache| cache.layerState(layer) else null
    else
        null;

    const conv_out = try allocator.alloc(f32, total * conv_dim);
    defer allocator.free(conv_out);
    try qwen35CausalDepthwiseConv1d(
        mixed_host,
        conv_weight,
        if (maybe_state) |state| state.conv else null,
        if (maybe_state) |state| state.initialized else false,
        conv_out,
        seq_len,
        conv_dim,
        conv_kernel,
    );

    const recurrent_state = if (maybe_state) |state| state.recurrent else null;
    const recurrent_initialized = if (maybe_state) |state| state.initialized else false;
    const core = try allocator.alloc(f32, total * value_dim);
    defer allocator.free(core);
    try qwen35RecurrentGatedDeltaRuleHost(
        conv_out,
        beta_host,
        a_host,
        a_log,
        dt_bias,
        recurrent_state,
        recurrent_initialized,
        core,
        seq_len,
        key_heads,
        value_heads,
        key_head_dim,
        value_head_dim,
    );

    if (maybe_state) |state| state.initialized = true;

    qwen35RmsNormGatedHost(core, z_host, norm_weight, config.norm_eps, total, value_heads, value_head_dim);
    const core_shape = [_]i32{ @intCast(total), @intCast(value_dim) };
    const core_ct = try cb.fromFloat32Shape(core, &core_shape);
    defer cb.free(core_ct);

    const out_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.linear_attn.out_proj.weight", .{layer}) catch return error.NameTooLong;
    const out_w = try getModelWeight(cb, config, out_w_name);
    defer cb.free(out_w);
    return cb.linearNoBias(core_ct, out_w, total, value_dim, hidden_size);
}

fn qwen35CausalDepthwiseConv1d(
    input: []const f32,
    weight: []const f32,
    state: ?[]f32,
    state_initialized: bool,
    output: []f32,
    seq_len: usize,
    channels: usize,
    kernel: usize,
) !void {
    if (input.len != seq_len * channels or output.len != input.len) return error.InvalidTensorShape;
    if (weight.len < channels * kernel) return error.InvalidTensorShape;
    if (state) |s| if (s.len != channels * kernel) return error.InvalidTensorShape;

    for (0..seq_len) |t| {
        for (0..channels) |c| {
            var sum: f32 = 0;
            for (0..kernel) |kk| {
                const x = if (state != null and state_initialized) blk: {
                    const combined_index = t + 1 + kk;
                    if (combined_index < kernel) {
                        break :blk state.?[c * kernel + combined_index];
                    }
                    const input_t = combined_index - kernel;
                    if (input_t >= seq_len) break :blk @as(f32, 0);
                    break :blk input[input_t * channels + c];
                } else blk: {
                    const padded_index: isize = @as(isize, @intCast(t)) + @as(isize, @intCast(kk)) + 1 - @as(isize, @intCast(kernel));
                    if (padded_index < 0 or padded_index >= @as(isize, @intCast(seq_len))) break :blk @as(f32, 0);
                    break :blk input[@as(usize, @intCast(padded_index)) * channels + c];
                };
                sum += x * weight[c * kernel + kk];
            }
            output[t * channels + c] = sum / (1.0 + @exp(-sum));
        }
    }

    if (state) |s| {
        for (0..channels) |c| {
            for (0..kernel) |slot| {
                const combined_len = if (state_initialized) kernel + seq_len else seq_len;
                const start = if (combined_len > kernel) combined_len - kernel else 0;
                const src = start + slot;
                s[c * kernel + slot] = if (state_initialized and src < kernel)
                    s[c * kernel + src]
                else blk: {
                    const input_index = if (state_initialized) src - kernel else src;
                    if (input_index >= seq_len) break :blk @as(f32, 0);
                    break :blk input[input_index * channels + c];
                };
            }
        }
    }
}

fn qwen35RecurrentGatedDeltaRuleHost(
    conv_out: []const f32,
    beta_projection: []const f32,
    a_projection: []const f32,
    a_log: []const f32,
    dt_bias: []const f32,
    state: ?[]f32,
    state_initialized: bool,
    output: []f32,
    seq_len: usize,
    key_heads: usize,
    value_heads: usize,
    key_head_dim: usize,
    value_head_dim: usize,
) !void {
    const key_dim = key_heads * key_head_dim;
    const value_dim = value_heads * value_head_dim;
    const conv_dim = key_dim * 2 + value_dim;
    if (conv_out.len != seq_len * conv_dim) return error.InvalidTensorShape;
    if (beta_projection.len != seq_len * value_heads or a_projection.len != beta_projection.len) return error.InvalidTensorShape;
    if (a_log.len < value_heads or dt_bias.len < value_heads) return error.InvalidTensorShape;
    if (output.len != seq_len * value_dim) return error.InvalidTensorShape;
    if (state) |s| {
        if (s.len != value_heads * key_head_dim * value_head_dim) return error.InvalidTensorShape;
        if (!state_initialized) @memset(s, 0);
    }

    var stack_state: [4096]f32 = undefined;
    var heap_state: ?[]f32 = null;
    defer if (heap_state) |buf| std.heap.page_allocator.free(buf);
    const recurrent = if (state) |s|
        s
    else blk: {
        const len = value_heads * key_head_dim * value_head_dim;
        if (len <= stack_state.len) {
            @memset(stack_state[0..len], 0);
            break :blk stack_state[0..len];
        }
        heap_state = try std.heap.page_allocator.alloc(f32, len);
        @memset(heap_state.?, 0);
        break :blk heap_state.?;
    };

    const repeat = value_heads / key_heads;
    const q_scale = 1.0 / @sqrt(@as(f32, @floatFromInt(key_head_dim)));
    var q_buf_stack: [512]f32 = undefined;
    var k_buf_stack: [512]f32 = undefined;
    if (key_head_dim > q_buf_stack.len) return error.UnsupportedQwen35HeadDim;
    const q_buf = q_buf_stack[0..key_head_dim];
    const k_buf = k_buf_stack[0..key_head_dim];

    for (0..seq_len) |t| {
        const row = conv_out[t * conv_dim ..][0..conv_dim];
        const q_all = row[0..key_dim];
        const k_all = row[key_dim..][0..key_dim];
        const v_all = row[key_dim * 2 ..][0..value_dim];
        for (0..value_heads) |vh| {
            const kh = vh / repeat;
            const q_src = q_all[kh * key_head_dim ..][0..key_head_dim];
            const k_src = k_all[kh * key_head_dim ..][0..key_head_dim];
            l2NormInto(q_src, q_buf);
            l2NormInto(k_src, k_buf);
            for (q_buf) |*value| value.* *= q_scale;

            const beta = 1.0 / (1.0 + @exp(-beta_projection[t * value_heads + vh]));
            const g = -@exp(a_log[vh]) * softplus(a_projection[t * value_heads + vh] + dt_bias[vh]);
            const g_exp = @exp(g);
            const state_head = recurrent[vh * key_head_dim * value_head_dim ..][0 .. key_head_dim * value_head_dim];
            for (state_head) |*value| value.* *= g_exp;

            const v_src = v_all[vh * value_head_dim ..][0..value_head_dim];
            const out_dst = output[t * value_dim + vh * value_head_dim ..][0..value_head_dim];
            var v_idx: usize = 0;
            while (v_idx < value_head_dim) : (v_idx += 1) {
                var kv_mem: f32 = 0;
                for (0..key_head_dim) |k_idx| {
                    kv_mem += state_head[k_idx * value_head_dim + v_idx] * k_buf[k_idx];
                }
                const delta = (v_src[v_idx] - kv_mem) * beta;
                for (0..key_head_dim) |k_idx| {
                    state_head[k_idx * value_head_dim + v_idx] += k_buf[k_idx] * delta;
                }
                var out: f32 = 0;
                for (0..key_head_dim) |k_idx| {
                    out += state_head[k_idx * value_head_dim + v_idx] * q_buf[k_idx];
                }
                out_dst[v_idx] = out;
            }
        }
    }
}

fn qwen35RmsNormGatedHost(
    data: []f32,
    gate: []const f32,
    weight: []const f32,
    eps: f32,
    rows: usize,
    heads: usize,
    head_dim: usize,
) void {
    const value_dim = heads * head_dim;
    for (0..rows) |row| {
        for (0..heads) |head| {
            const start = row * value_dim + head * head_dim;
            const chunk = data[start..][0..head_dim];
            var sum_sq: f32 = 0;
            for (chunk) |value| sum_sq += value * value;
            const inv_rms = 1.0 / @sqrt(sum_sq / @as(f32, @floatFromInt(head_dim)) + eps);
            for (chunk, 0..) |*value, idx| {
                const z = gate[start + idx];
                value.* = value.* * inv_rms * weight[idx] * (z / (1.0 + @exp(-z)));
            }
        }
    }
}

fn l2NormInto(src: []const f32, dst: []f32) void {
    var sum_sq: f32 = 0;
    for (src) |value| sum_sq += value * value;
    const inv_norm = 1.0 / @sqrt(sum_sq + 1e-6);
    for (src, dst) |s, *d| d.* = s * inv_norm;
}

fn softplus(x: f32) f32 {
    if (x > 20.0) return x;
    if (x < -20.0) return @exp(x);
    return @log(1.0 + @exp(x));
}

test "qwen3.5 causal depthwise conv updates rolling state" {
    const input = [_]f32{ 1, 2, 3 };
    const weight = [_]f32{ 1, 10 };
    var state = [_]f32{ 0, 0 };
    var output = [_]f32{0} ** 3;
    try qwen35CausalDepthwiseConv1d(&input, &weight, &state, false, &output, 3, 1, 2);

    const expected_raw = [_]f32{ 10, 21, 32 };
    for (output, expected_raw) |got, raw| {
        const want = raw / (1.0 + @exp(-raw));
        try std.testing.expectApproxEqAbs(want, got, 1e-5);
    }
    try std.testing.expectApproxEqAbs(@as(f32, 2), state[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 3), state[1], 1e-6);

    const next = [_]f32{4};
    var next_out = [_]f32{0};
    try qwen35CausalDepthwiseConv1d(&next, &weight, &state, true, &next_out, 1, 1, 2);
    const want = @as(f32, 43) / (1.0 + @exp(-@as(f32, 43)));
    try std.testing.expectApproxEqAbs(want, next_out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 3), state[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4), state[1], 1e-6);
}

test "qwen3.5 recurrent gated delta rule updates state" {
    // conv layout per token: [q, k, v] for one key head and one value head.
    const conv_out = [_]f32{
        2, 3, 2,
        4, 5, 3,
    };
    const beta = [_]f32{ 100, 100 };
    const a_proj = [_]f32{ -100, -100 };
    const a_log = [_]f32{0};
    const dt_bias = [_]f32{0};
    var state = [_]f32{0};
    var output = [_]f32{0} ** 2;

    try qwen35RecurrentGatedDeltaRuleHost(
        &conv_out,
        &beta,
        &a_proj,
        &a_log,
        &dt_bias,
        &state,
        false,
        &output,
        2,
        1,
        1,
        1,
        1,
    );

    try std.testing.expectApproxEqAbs(@as(f32, 2), output[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 3), output[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 3), state[0], 1e-4);
}

fn applyActivationHostInPlace(activation: ActivationType, data: []f32) void {
    switch (activation) {
        .gelu => activations.gelu(data),
        .gelu_new => {
            const k: f32 = 0.044715;
            const s: f32 = 0.7978845608028654;
            for (data) |*x| {
                const v = x.*;
                const inner = s * (v + k * v * v * v);
                x.* = 0.5 * v * (1.0 + std.math.tanh(inner));
            }
        },
        .silu => activations.silu(data),
        .relu => activations.relu(data),
        .relu_squared => {
            activations.relu(data);
            for (data) |*x| x.* *= x.*;
        },
    }
}

fn maybeDebugLayerTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    suffix: []const u8,
    tensor: CT,
) !void {
    var label_buf: [64]u8 = undefined;
    const label = std.fmt.bufPrint(&label_buf, "layer{d}_{s}", .{ layer, suffix }) catch return;
    try maybeDebugTensor(cb, allocator, label, tensor);
}

fn maybeDebugLayerTensorLastRow(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    layer: usize,
    suffix: []const u8,
    tensor: CT,
    row_dim: usize,
) !void {
    var label_buf: [64]u8 = undefined;
    const label = std.fmt.bufPrint(&label_buf, "layer{d}_{s}", .{ layer, suffix }) catch return;
    try maybeDebugTensorLastRow(cb, allocator, label, tensor, row_dim);
}

// --- Attention with optional RoPE ---

pub fn applyAttention(
    cb: *const ComputeBackend,
    config: Config,
    Q: CT,
    K: CT,
    V: CT,
    batch: usize,
    seq_len: usize,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    kv_layer_index: usize,
    skip_kv_write: bool,
    decode_context: ?*const DecodeContext,
) !CT {
    return applyAttentionWithSink(cb, config, Q, K, V, null, batch, seq_len, num_heads, num_kv_heads, head_dim, layer, kv_layer_index, skip_kv_write, decode_context);
}

fn applyAttentionWithSink(
    cb: *const ComputeBackend,
    config: Config,
    Q: CT,
    K: CT,
    V: CT,
    attention_sink: ?CT,
    batch: usize,
    seq_len: usize,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    layer: usize,
    kv_layer_index: usize,
    skip_kv_write: bool,
    decode_context: ?*const DecodeContext,
) !CT {
    var attention = attentionContext(seq_len, decode_context);
    attention.layer_index = kv_layer_index;
    attention.skip_kv_write = skip_kv_write;
    attention.sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0;
    if (disableSlidingAttentionDebug()) {
        attention.sliding_window = 0;
    }
    const sink_metadata: backend_contracts.AttentionSinkMetadata = if (attention_sink) |sink| .{ .per_head_tensor = sink } else .{};
    if (sink_metadata.hasMetadata()) {
        attention.attention_sink = sink_metadata;
    }
    const rope_dim: usize = config.layerRopeActiveDim(layer);
    const rope_consecutive_pairs = config.rope_layout == .consecutive_pairs;
    // When rope_dim_override is active (from rope_freqs.weight), the frequency formula
    // in the backend uses rope_dim (active_dim). We adjust theta so that:
    //   1/theta'^(2j/active_dim) == 1/theta^(2j/freq_dim)
    // which gives theta' = theta^(active_dim/freq_dim).
    const rope_theta = blk: {
        const base_theta = config.layerRopeTheta(layer);
        const freq_dim: f32 = @floatFromInt(config.layerRopeFrequencyDim(layer));
        const active_dim: f32 = @floatFromInt(rope_dim);
        if (active_dim < freq_dim) {
            break :blk std.math.pow(f32, base_theta, active_dim / freq_dim);
        }
        break :blk base_theta;
    };

    // Mixed batch: per-item RoPE with different position offsets.
    const is_mixed = if (decode_context) |dc| dc.isMixedBatch() else false;

    if (config.position_encoding == .rope) {
        if (is_mixed) {
            // Apply RoPE per-item with per-item position offsets, then reassemble.
            const dc = decode_context.?;
            const kv_batch_views = dc.kv_batch.?;
            const max_q_len = attention.query_sequence_len;
            const rope_started_at = monotonicNowNs();
            const Q_rope = try applyPerItemRope(cb, Q, kv_batch_views, batch, max_q_len, num_heads * head_dim, head_dim, rope_dim, rope_theta, config.rope_freq_scale, rope_consecutive_pairs);
            defer cb.free(Q_rope);
            const K_rope = try applyPerItemRope(cb, K, kv_batch_views, batch, max_q_len, num_kv_heads * head_dim, head_dim, rope_dim, rope_theta, config.rope_freq_scale, rope_consecutive_pairs);
            defer cb.free(K_rope);
            debug_timing_stats.attention_rope_nanos += @intCast(monotonicNowNs() - rope_started_at);
            try maybeDebugLayerTensorLastRow(cb, std.heap.page_allocator, layer, "q_rope", Q_rope, num_heads * head_dim);
            try maybeDebugLayerTensorLastRow(cb, std.heap.page_allocator, layer, "k_rope", K_rope, num_kv_heads * head_dim);
            try maybeDebugLayerTensor(cb, std.heap.page_allocator, layer, "q_rope", Q_rope);
            try maybeDebugLayerTensor(cb, std.heap.page_allocator, layer, "k_rope", K_rope);
            const gqa_started_at = monotonicNowNs();
            const result = try cb.gqaPagedAttention(Q_rope, K_rope, V, null, attention, batch, num_heads, num_kv_heads, head_dim);
            debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
            return result;
        }

        const position_offset = positionOffset(seq_len, attention.query_sequence_len, decode_context);
        const rope_started_at = monotonicNowNs();
        const Q_rope = try cb.rope(Q, attention.query_sequence_len, head_dim, rope_dim, rope_theta, config.rope_freq_scale, position_offset, rope_consecutive_pairs);
        defer cb.free(Q_rope);
        const K_rope = try cb.rope(K, attention.query_sequence_len, head_dim, rope_dim, rope_theta, config.rope_freq_scale, position_offset, rope_consecutive_pairs);
        defer cb.free(K_rope);
        debug_timing_stats.attention_rope_nanos += @intCast(monotonicNowNs() - rope_started_at);
        try maybeDumpGatedLayerStageStats(cb, std.heap.page_allocator, layer, "q-rope", Q_rope, num_heads * head_dim);
        try maybeDumpGatedLayerStageStats(cb, std.heap.page_allocator, layer, "k-rope", K_rope, num_kv_heads * head_dim);
        try maybeDebugLayerTensorLastRow(cb, std.heap.page_allocator, layer, "q_rope", Q_rope, num_heads * head_dim);
        try maybeDebugLayerTensorLastRow(cb, std.heap.page_allocator, layer, "k_rope", K_rope, num_kv_heads * head_dim);
        try maybeDebugLayerTensor(cb, std.heap.page_allocator, layer, "q_rope", Q_rope);
        try maybeDebugLayerTensor(cb, std.heap.page_allocator, layer, "k_rope", K_rope);
        if (!sink_metadata.hasMetadata() and attention.mode == .dense_causal and attention.sliding_window == 0 and attention.attn_or_mask == null) {
            const gqa_started_at = monotonicNowNs();
            const result = try cb.gqaCausalAttention(Q_rope, K_rope, V, null, batch, attention.query_sequence_len, num_heads, num_kv_heads, head_dim);
            debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
            return result;
        }
        if (try cb.runAttention(&.{
            .q = Q_rope,
            .k = K_rope,
            .v = V,
            .attention = attention,
            .attention_sink = sink_metadata,
            .num_heads = num_heads,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
        })) |result| {
            return result;
        }
        if (sink_metadata.hasMetadata()) {
            if (sink_metadata.per_head_tensor) |sink| {
                if (attention.mode == .dense_causal and attention.kv_batch == null and attention.kv_cache == null and attention.kv_manager == null and attention.kv_storage == null and attention.query_sequence_len == attention.kv_sequence_len and attention.kv_position_offset == 0) {
                    return deepSeekV4SinkAwareAttentionFallback(cb, Q_rope, K_rope, V, sink, batch, attention.query_sequence_len, attention.kv_sequence_len, num_heads, num_kv_heads, head_dim, attention.sliding_window);
                }
            }
            if ((cb.kind() == .native or cb.kind() == .metal or cb.kind() == .metal) and (attention.kv_batch != null or attention.kv_cache != null or attention.kv_manager != null or attention.kv_storage != null)) {
                const gqa_started_at = monotonicNowNs();
                const result = try cb.gqaPagedAttention(Q_rope, K_rope, V, null, attention, batch, num_heads, num_kv_heads, head_dim);
                debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
                return result;
            }
            return error.AttentionSinkBackendFeatureRequired;
        }
        const gqa_started_at = monotonicNowNs();
        const result = try cb.gqaPagedAttention(Q_rope, K_rope, V, null, attention, batch, num_heads, num_kv_heads, head_dim);
        debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
        return result;
    }
    if (!sink_metadata.hasMetadata() and attention.mode == .dense_causal and attention.sliding_window == 0 and attention.attn_or_mask == null) {
        const gqa_started_at = monotonicNowNs();
        const result = try cb.gqaCausalAttention(Q, K, V, null, batch, attention.query_sequence_len, num_heads, num_kv_heads, head_dim);
        debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
        return result;
    }
    if (try cb.runAttention(&.{
        .q = Q,
        .k = K,
        .v = V,
        .attention = attention,
        .attention_sink = sink_metadata,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
    })) |result| {
        return result;
    }
    if (sink_metadata.hasMetadata()) {
        if (sink_metadata.per_head_tensor) |sink| {
            if (attention.mode == .dense_causal and attention.kv_batch == null and attention.kv_cache == null and attention.kv_manager == null and attention.kv_storage == null and attention.query_sequence_len == attention.kv_sequence_len and attention.kv_position_offset == 0) {
                return deepSeekV4SinkAwareAttentionFallback(cb, Q, K, V, sink, batch, attention.query_sequence_len, attention.kv_sequence_len, num_heads, num_kv_heads, head_dim, attention.sliding_window);
            }
        }
        if ((cb.kind() == .native or cb.kind() == .metal or cb.kind() == .metal) and (attention.kv_batch != null or attention.kv_cache != null or attention.kv_manager != null or attention.kv_storage != null)) {
            const gqa_started_at = monotonicNowNs();
            const result = try cb.gqaPagedAttention(Q, K, V, null, attention, batch, num_heads, num_kv_heads, head_dim);
            debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
            return result;
        }
        return error.AttentionSinkBackendFeatureRequired;
    }
    const gqa_started_at = monotonicNowNs();
    const result = try cb.gqaPagedAttention(Q, K, V, null, attention, batch, num_heads, num_kv_heads, head_dim);
    debug_timing_stats.attention_gqa_nanos += @intCast(monotonicNowNs() - gqa_started_at);
    return result;
}

fn deepSeekV4SinkAwareAttentionFallback(
    cb: *const ComputeBackend,
    Q: CT,
    K: CT,
    V: CT,
    sink: CT,
    batch: usize,
    query_seq_len: usize,
    kv_seq_len: usize,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    sliding_window: usize,
) !CT {
    const allocator = std.heap.page_allocator;
    const q_heads: usize = @intCast(num_heads);
    const kv_heads: usize = @intCast(num_kv_heads);
    const dim: usize = @intCast(head_dim);
    if (q_heads == 0 or kv_heads == 0 or dim == 0 or q_heads % kv_heads != 0) return error.DeepSeekV4TensorShapeMismatch;
    const q_width = q_heads * dim;
    const kv_width = kv_heads * dim;
    const q_data = try cb.toFloat32(Q, allocator);
    defer allocator.free(q_data);
    const k_data = try cb.toFloat32(K, allocator);
    defer allocator.free(k_data);
    const v_data = try cb.toFloat32(V, allocator);
    defer allocator.free(v_data);
    const sink_data = try cb.toFloat32(sink, allocator);
    defer allocator.free(sink_data);
    if (q_data.len != batch * query_seq_len * q_width) return error.DeepSeekV4TensorShapeMismatch;
    if (k_data.len != batch * kv_seq_len * kv_width or v_data.len != batch * kv_seq_len * kv_width) return error.DeepSeekV4TensorShapeMismatch;
    if (sink_data.len != q_heads) return error.DeepSeekV4TensorShapeMismatch;

    const out = try allocator.alloc(f32, q_data.len);
    defer allocator.free(out);
    const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(dim)));
    for (0..batch) |b| {
        const q_batch = q_data[b * query_seq_len * q_width ..][0 .. query_seq_len * q_width];
        const k_batch = k_data[b * kv_seq_len * kv_width ..][0 .. kv_seq_len * kv_width];
        const v_batch = v_data[b * kv_seq_len * kv_width ..][0 .. kv_seq_len * kv_width];
        const out_batch = out[b * query_seq_len * q_width ..][0 .. query_seq_len * q_width];
        deepSeekV4SinkAwareAttentionRows(out_batch, q_batch, k_batch, v_batch, sink_data, query_seq_len, kv_seq_len, q_heads, kv_heads, dim, sliding_window, scale);
    }
    const shape = [_]i32{ @intCast(batch * query_seq_len), @intCast(q_width) };
    return cb.fromFloat32Shape(out, &shape);
}

fn deepSeekV4SinkAwareAttentionRows(
    output: []f32,
    query: []const f32,
    key: []const f32,
    value: []const f32,
    sinks: []const f32,
    query_rows: usize,
    key_rows: usize,
    num_query_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    sliding_window: usize,
    scale: f32,
) void {
    const q_width = num_query_heads * head_dim;
    const kv_width = num_kv_heads * head_dim;
    @memset(output, 0.0);
    for (0..query_rows) |query_row| {
        const query_abs = key_rows - query_rows + query_row;
        const key_end = @min(query_abs + 1, key_rows);
        const key_start = if (sliding_window > 0 and key_end > sliding_window) key_end - sliding_window else 0;
        for (0..num_query_heads) |query_head| {
            const kv_head = query_head / (num_query_heads / num_kv_heads);
            var max_logit = sinks[query_head];
            for (key_start..key_end) |key_row| {
                const logit = deepSeekV4AttentionRowDot(query, key, query_row, key_row, query_head, kv_head, q_width, kv_width, head_dim) * scale;
                max_logit = @max(max_logit, logit);
            }
            var denom: f32 = @exp(sinks[query_head] - max_logit);
            for (key_start..key_end) |key_row| {
                const logit = deepSeekV4AttentionRowDot(query, key, query_row, key_row, query_head, kv_head, q_width, kv_width, head_dim) * scale;
                denom += @exp(logit - max_logit);
            }
            for (0..head_dim) |col| {
                var sum: f32 = 0.0;
                for (key_start..key_end) |key_row| {
                    const logit = deepSeekV4AttentionRowDot(query, key, query_row, key_row, query_head, kv_head, q_width, kv_width, head_dim) * scale;
                    const weight = @exp(logit - max_logit) / denom;
                    sum += weight * value[key_row * kv_width + kv_head * head_dim + col];
                }
                output[query_row * q_width + query_head * head_dim + col] = sum;
            }
        }
    }
}

fn deepSeekV4AttentionRowDot(
    query: []const f32,
    key: []const f32,
    query_row: usize,
    key_row: usize,
    query_head: usize,
    kv_head: usize,
    q_width: usize,
    kv_width: usize,
    head_dim: usize,
) f32 {
    var dot: f32 = 0.0;
    const q_base = query_row * q_width + query_head * head_dim;
    const k_base = key_row * kv_width + kv_head * head_dim;
    for (0..head_dim) |col| dot += query[q_base + col] * key[k_base + col];
    return dot;
}

pub fn applyAttentionResidual(
    cb: *const ComputeBackend,
    config: Config,
    Q: CT,
    K: CT,
    V: CT,
    residual: CT,
    batch: usize,
    seq_len: usize,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    hidden_size: usize,
    layer: usize,
    kv_layer_index: usize,
    skip_kv_write: bool,
    out_proj_linear_slot: usize,
    pre_linear_rms_norm_slot: ?usize,
    post_linear_rms_norm_slot: ?usize,
    decode_context: ?*const DecodeContext,
) !?CT {
    var attention = attentionContext(seq_len, decode_context);
    attention.layer_index = kv_layer_index;
    attention.skip_kv_write = skip_kv_write;
    attention.sliding_window = if (config.layerUsesSlidingAttention(layer)) config.sliding_window else 0;
    if (disableSlidingAttentionDebug()) {
        attention.sliding_window = 0;
    }
    if (batch != 1 or attention.mode != .paged_decode or attention.query_sequence_len != 1 or attention.attn_or_mask != null) return null;
    if (decode_context) |dc| {
        if (dc.isMixedBatch()) return null;
    }

    if (config.position_encoding == .rope) {
        const rope_dim: usize = config.layerRopeActiveDim(layer);
        const rope_consecutive_pairs = config.rope_layout == .consecutive_pairs;
        const rope_theta = blk: {
            const base_theta = config.layerRopeTheta(layer);
            const freq_dim: f32 = @floatFromInt(config.layerRopeFrequencyDim(layer));
            const active_dim: f32 = @floatFromInt(rope_dim);
            if (active_dim < freq_dim) {
                break :blk std.math.pow(f32, base_theta, active_dim / freq_dim);
            }
            break :blk base_theta;
        };
        const position_offset = positionOffset(seq_len, attention.query_sequence_len, decode_context);
        const rope_started_at = monotonicNowNs();
        const Q_rope = try cb.rope(Q, attention.query_sequence_len, head_dim, rope_dim, rope_theta, config.rope_freq_scale, position_offset, rope_consecutive_pairs);
        defer cb.free(Q_rope);
        const K_rope = try cb.rope(K, attention.query_sequence_len, head_dim, rope_dim, rope_theta, config.rope_freq_scale, position_offset, rope_consecutive_pairs);
        defer cb.free(K_rope);
        debug_timing_stats.attention_rope_nanos += @intCast(monotonicNowNs() - rope_started_at);
        try maybeDumpGatedLayerStageStats(cb, std.heap.page_allocator, layer, "q-rope", Q_rope, num_heads * head_dim);
        try maybeDumpGatedLayerStageStats(cb, std.heap.page_allocator, layer, "k-rope", K_rope, num_kv_heads * head_dim);
        return cb.runAttentionResidual(&.{
            .q = Q_rope,
            .k = K_rope,
            .v = V,
            .residual = residual,
            .attention = attention,
            .num_heads = num_heads,
            .num_kv_heads = num_kv_heads,
            .head_dim = head_dim,
            .linear_slot = out_proj_linear_slot,
            .pre_linear_rms_norm_slot = pre_linear_rms_norm_slot,
            .post_linear_rms_norm_slot = post_linear_rms_norm_slot,
            .hidden_size = hidden_size,
            .eps = config.norm_eps,
        });
    }

    return cb.runAttentionResidual(&.{
        .q = Q,
        .k = K,
        .v = V,
        .residual = residual,
        .attention = attention,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .linear_slot = out_proj_linear_slot,
        .pre_linear_rms_norm_slot = pre_linear_rms_norm_slot,
        .post_linear_rms_norm_slot = post_linear_rms_norm_slot,
        .hidden_size = hidden_size,
        .eps = config.norm_eps,
    });
}

/// Apply RoPE per-item with per-item position offsets for mixed prefill+decode batches.
/// Input tensor is [batch * max_q_len, num_heads_dim]. Each item is padded to max_q_len.
/// Returns a new tensor with RoPE applied per-item using each item's position offset.
///
fn applyPerItemRope(
    cb: *const ComputeBackend,
    input: CT,
    kv_batch_views: []const DecodeContext.KvBatchView,
    batch: usize,
    max_q_len: usize,
    num_heads_dim: usize,
    head_dim: u32,
    rope_dim: usize,
    rope_theta: f32,
    rope_freq_scale: f32,
    rope_consecutive_pairs: bool,
) !CT {
    // Single-item fast path: delegate to the backend's rope() directly.
    if (batch == 1) {
        const item_q_len = kv_batch_views[0].per_item_query_len orelse max_q_len;
        const item_total_len = kv_batch_views[0].per_item_total_len orelse max_q_len;
        const item_offset = item_total_len - item_q_len;
        return cb.rope(input, max_q_len, head_dim, rope_dim, rope_theta, rope_freq_scale, item_offset, rope_consecutive_pairs);
    }

    const allocator = std.heap.page_allocator;
    const query_lengths = try allocator.alloc(usize, batch);
    defer allocator.free(query_lengths);
    const position_offsets = try allocator.alloc(usize, batch);
    defer allocator.free(position_offsets);
    for (0..batch) |b| {
        const item_q_len = kv_batch_views[b].per_item_query_len orelse max_q_len;
        const item_total_len = kv_batch_views[b].per_item_total_len orelse max_q_len;
        query_lengths[b] = item_q_len;
        position_offsets[b] = item_total_len - item_q_len;
    }
    _ = num_heads_dim;
    return cb.ropePerItem(input, batch, max_q_len, head_dim, rope_dim, rope_theta, rope_freq_scale, query_lengths, position_offsets, rope_consecutive_pairs);
}

/// Create a zero-filled compute tensor of the given flat length.
fn createZeroCT(cb: *const ComputeBackend, allocator: std.mem.Allocator, len: usize) !CT {
    if (try cb.zeroTensor(1, len)) |ct| return ct;
    // Fallback for backends without native zeroTensor (e.g. native).
    const zeros = try allocator.alloc(f32, len);
    defer allocator.free(zeros);
    @memset(zeros, 0);
    return cb.fromFloat32(zeros);
}

/// Build an AttentionContext from a DecodeContext. Public for the graph
/// interpreter's ExecuteOptions.
pub fn attentionContextFromDecode(dc: *const DecodeContext) ops.AttentionContext {
    return attentionContext(dc.total_sequence_len, dc);
}

fn attentionContext(seq_len: usize, decode_context: ?*const DecodeContext) ops.AttentionContext {
    const dc = decode_context orelse return .{
        .mode = .dense_causal,
        .total_sequence_len = seq_len,
        .query_sequence_len = seq_len,
        .kv_sequence_len = seq_len,
    };

    return .{
        .mode = switch (dc.attention_mode) {
            .full_recompute => .dense_causal,
            .paged_prefill => .paged_prefill,
            .paged_decode => .paged_decode,
        },
        .total_sequence_len = dc.total_sequence_len,
        .query_sequence_len = dc.query_sequence_len,
        .kv_sequence_len = dc.kv_sequence_len,
        .kv_position_offset = dc.kv_position_offset,
        .decoder_runtime_resident_kv_sequence_len = dc.decoder_runtime_resident_kv_sequence_len,
        .decoder_runtime_resident_kv_position_offset = dc.decoder_runtime_resident_kv_position_offset,
        .sliding_window = dc.sliding_window,
        .attn_or_mask = dc.attn_or_mask,
        .kv_manager = dc.kv_manager,
        .kv_storage = dc.kv_storage,
        .kv_batch = if (dc.kv_batch) |batch|
            @ptrCast(batch)
        else
            null,
        .kv_cache = if (dc.kv_cache) |kv|
            .{
                .sequence_id = kv.sequence_id,
                .pool_id = kv.pool_id,
                .logical_block_count = kv.logical_block_count,
                .tail_tokens = kv.tail_tokens,
                .position_offset = kv.position_offset,
                .logical_blocks = kv.logical_blocks,
                .kv_storage = kv.kv_storage,
            }
        else
            null,
    };
}

fn actualQuerySeqLen(seq_len: usize, decode_context: ?*const DecodeContext) usize {
    const dc = decode_context orelse return seq_len;
    return dc.query_sequence_len;
}

fn positionOffset(seq_len: usize, query_seq_len: usize, decode_context: ?*const DecodeContext) usize {
    const dc = decode_context orelse return 0;
    _ = seq_len;
    return dc.total_sequence_len - query_seq_len;
}

// --- Feed-forward network ---

/// Dense gated FFN using the standard mlp.gate/up/down_proj weights.
/// Used for the shared expert sublayer in Gemma 4 MoE blocks.
fn denseFeedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
) !CT {
    const hidden_size = config.hidden_size;
    const inter_size = config.intermediateSize(layer);
    _ = allocator;

    const gate_w = try getFFNWeight(cb, config, layer, "gate", name_buf);
    defer cb.free(gate_w);
    const gate_proj = try cb.linearNoBias(input, gate_w, total, hidden_size, inter_size);
    defer cb.free(gate_proj);
    const gate_act = try applyActivation(cb, config, gate_proj);
    defer cb.free(gate_act);

    const up_w = try getFFNWeight(cb, config, layer, "up", name_buf);
    defer cb.free(up_w);
    const up_proj = try cb.linearNoBias(input, up_w, total, hidden_size, inter_size);
    defer cb.free(up_proj);

    const gated = try cb.multiply(gate_act, up_proj);
    defer cb.free(gated);

    const down_w = try getFFNWeight(cb, config, layer, "down", name_buf);
    defer cb.free(down_w);
    return cb.linearNoBias(gated, down_w, total, inter_size, hidden_size);
}

fn feedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
    decode_context: ?*const DecodeContext,
) !CT {
    const hidden_size = config.hidden_size;
    const inter_size = config.intermediateSize(layer);

    if (config.usesMoe()) {
        return moeFeedForward(cb, allocator, config, input, total, layer, name_buf, decode_context);
    }

    switch (config.family) {
        .bitnet => {
            const gate_w = try getFFNWeight(cb, config, layer, "gate", name_buf);
            defer cb.free(gate_w);
            const up_w = try getFFNWeight(cb, config, layer, "up", name_buf);
            defer cb.free(up_w);
            debug_timing_stats.ffn_project_pair_calls += 1;
            const gate_up = try cb.linearNoBiasPair(input, gate_w, up_w, total, hidden_size, inter_size);
            const gate_proj = gate_up.first;
            defer cb.free(gate_proj);
            const gate_act = try applyActivation(cb, config, gate_proj);
            defer cb.free(gate_act);
            const up_proj = gate_up.second;
            defer cb.free(up_proj);

            const gated = try cb.multiply(gate_act, up_proj);
            defer cb.free(gated);

            const ffn_sub_norm_w_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.mlp.ffn_sub_norm.weight", .{layer}) catch return error.NameTooLong;
            const ffn_sub_norm_base_w = try getModelWeight(cb, config, ffn_sub_norm_w_name);
            defer cb.free(ffn_sub_norm_base_w);
            const ffn_sub_norm_w = try maybeAdjustNormWeight(cb, allocator, config, ffn_sub_norm_base_w, inter_size);
            defer if (ffn_sub_norm_w != ffn_sub_norm_base_w) cb.free(ffn_sub_norm_w);
            const normed_gated = try cb.rmsNorm(gated, ffn_sub_norm_w, inter_size, config.norm_eps);
            defer cb.free(normed_gated);

            const down_w = try getFFNWeight(cb, config, layer, "down", name_buf);
            defer cb.free(down_w);
            return cb.linearNoBias(normed_gated, down_w, total, inter_size, hidden_size);
        },
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma => {
            // Gated FFN: gate = silu(x @ gate_w), up = x @ up_w, out = (gate * up) @ down_w
            const gate_w = try getFFNWeight(cb, config, layer, "gate", name_buf);
            defer cb.free(gate_w);
            const up_w = try getFFNWeight(cb, config, layer, "up", name_buf);
            defer cb.free(up_w);
            debug_timing_stats.ffn_project_pair_calls += 1;
            const gate_up = try cb.linearNoBiasPair(input, gate_w, up_w, total, hidden_size, inter_size);
            const gate_proj = gate_up.first;
            defer cb.free(gate_proj);
            const gate_act = try applyActivation(cb, config, gate_proj);
            defer cb.free(gate_act);

            const up_proj = gate_up.second;
            defer cb.free(up_proj);

            const gated = try cb.multiply(gate_act, up_proj);
            defer cb.free(gated);

            const down_w = try getFFNWeight(cb, config, layer, "down", name_buf);
            defer cb.free(down_w);
            const output = try cb.linearNoBias(gated, down_w, total, inter_size, hidden_size);
            return output;
        },
        else => {
            // Standard FFN: fc1 → activation → fc2
            const fc1_w = try getFFNWeight(cb, config, layer, "fc1", name_buf);
            defer cb.free(fc1_w);

            const fc1_out = if (config.family == .gpt2 or config.family == .gpt_neo or config.family == .opt) blk: {
                const fc1_b = try getFFNBias(cb, config, layer, "fc1", name_buf);
                defer cb.free(fc1_b);
                break :blk try cb.linear(input, fc1_w, fc1_b, total, hidden_size, inter_size);
            } else try cb.linearNoBias(input, fc1_w, total, hidden_size, inter_size);
            defer cb.free(fc1_out);

            const activated = try applyActivation(cb, config, fc1_out);
            defer cb.free(activated);

            const fc2_w = try getFFNWeight(cb, config, layer, "fc2", name_buf);
            defer cb.free(fc2_w);

            return if (config.family == .gpt2 or config.family == .gpt_neo or config.family == .opt) blk: {
                const fc2_b = try getFFNBias(cb, config, layer, "fc2", name_buf);
                defer cb.free(fc2_b);
                break :blk try cb.linear(activated, fc2_w, fc2_b, total, inter_size, hidden_size);
            } else try cb.linearNoBias(activated, fc2_w, total, inter_size, hidden_size);
        },
    }
}

const MoeSelection = struct {
    count: usize,
    indices: [8]u32,
    weights: [8]f32,
};

const ExpertBatch = struct {
    rows: []u32,
    route_weights: []f32,
};

const GroupedExpertBatch = struct {
    rows: []u32,
    expert_ids: []u32,
    route_weights: []f32,
    expert_tile_ids: []u32,
    tile_row_starts: []u32,
    tile_row_counts: []u32,
};

const GroupedExpertTiles = struct {
    expert_tile_ids: []u32,
    tile_row_starts: []u32,
    tile_row_counts: []u32,

    fn deinit(self: GroupedExpertTiles, allocator: std.mem.Allocator) void {
        allocator.free(self.expert_tile_ids);
        allocator.free(self.tile_row_starts);
        allocator.free(self.tile_row_counts);
    }
};

fn buildGroupedExpertTiles(
    allocator: std.mem.Allocator,
    expert_ids: []const u32,
    row_tile_size: usize,
) !GroupedExpertTiles {
    if (expert_ids.len == 0) {
        return .{
            .expert_tile_ids = &.{},
            .tile_row_starts = &.{},
            .tile_row_counts = &.{},
        };
    }

    var tile_count: usize = 0;
    var segment_start: usize = 0;
    while (segment_start < expert_ids.len) {
        const expert_id = expert_ids[segment_start];
        var segment_end = segment_start + 1;
        while (segment_end < expert_ids.len and expert_ids[segment_end] == expert_id) : (segment_end += 1) {}
        const segment_len = segment_end - segment_start;
        tile_count += (segment_len + row_tile_size - 1) / row_tile_size;
        segment_start = segment_end;
    }

    const expert_tile_ids = try allocator.alloc(u32, tile_count);
    errdefer allocator.free(expert_tile_ids);
    const tile_row_starts = try allocator.alloc(u32, tile_count);
    errdefer allocator.free(tile_row_starts);
    const tile_row_counts = try allocator.alloc(u32, tile_count);
    errdefer allocator.free(tile_row_counts);

    var tile_index: usize = 0;
    segment_start = 0;
    while (segment_start < expert_ids.len) {
        const expert_id = expert_ids[segment_start];
        var segment_end = segment_start + 1;
        while (segment_end < expert_ids.len and expert_ids[segment_end] == expert_id) : (segment_end += 1) {}
        var row_cursor = segment_start;
        while (row_cursor < segment_end) : (row_cursor += row_tile_size) {
            const remaining = segment_end - row_cursor;
            expert_tile_ids[tile_index] = expert_id;
            tile_row_starts[tile_index] = @intCast(row_cursor);
            tile_row_counts[tile_index] = @intCast(@min(remaining, row_tile_size));
            tile_index += 1;
        }
        segment_start = segment_end;
    }

    return .{
        .expert_tile_ids = expert_tile_ids,
        .tile_row_starts = tile_row_starts,
        .tile_row_counts = tile_row_counts,
    };
}

pub const DebugTimingStats = struct {
    attention_nanos: u128 = 0,
    attention_norm_nanos: u128 = 0,
    attention_qkv_nanos: u128 = 0,
    attention_project_pair_calls: u64 = 0,
    attention_core_nanos: u128 = 0,
    attention_rope_nanos: u128 = 0,
    attention_gqa_nanos: u128 = 0,
    attention_out_proj_nanos: u128 = 0,
    ffn_nanos: u128 = 0,
    moe_router_weight_fetch_nanos: u128 = 0,
    moe_router_proj_nanos: u128 = 0,
    moe_router_download_nanos: u128 = 0,
    moe_route_select_nanos: u128 = 0,
    moe_expert_scale_download_nanos: u128 = 0,
    moe_expert_weight_fetch_nanos: u128 = 0,
    moe_input_download_nanos: u128 = 0,
    moe_prepare_layer_nanos: u128 = 0,
    moe_append_route_nanos: u128 = 0,
    moe_finalize_layer_nanos: u128 = 0,
    moe_prefetch_hint_nanos: u128 = 0,
    moe_grouped_attempts: u64 = 0,
    moe_grouped_successes: u64 = 0,
    moe_grouped_nanos: u128 = 0,
    moe_fallback_nanos: u128 = 0,
    moe_grouped_input_copy_nanos: u128 = 0,
    moe_grouped_input_upload_nanos: u128 = 0,
    moe_grouped_ops_nanos: u128 = 0,
    moe_grouped_sync_w1_nanos: u128 = 0,
    moe_grouped_sync_w3_nanos: u128 = 0,
    moe_grouped_sync_gate_nanos: u128 = 0,
    moe_grouped_sync_w2_nanos: u128 = 0,
    moe_grouped_sync_ops_nanos: u128 = 0,
    moe_grouped_output_download_nanos: u128 = 0,
    moe_grouped_scatter_nanos: u128 = 0,
    moe_grouped_sync_scatter_nanos: u128 = 0,
    moe_grouped_cleanup_nanos: u128 = 0,
    eval_nanos: u128 = 0,
    eval_count: u64 = 0,
    shared_expert_ffn_nanos: u128 = 0,
    norm_nanos: u128 = 0,
    ffn_project_pair_calls: u64 = 0,
    dense_block_attempts: u64 = 0,
    dense_block_successes: u64 = 0,
    gated_block_attempts: u64 = 0,
    gated_block_successes: u64 = 0,
    gated_block_input_attempts: u64 = 0,
    gated_block_input_successes: u64 = 0,
    gated_block_input_prefill_nanos: u128 = 0,
    gated_block_input_decode_nanos: u128 = 0,
    gated_block_qkv_attempts: u64 = 0,
    gated_block_qkv_successes: u64 = 0,
};

var debug_timing_stats = DebugTimingStats{};

pub fn resetDebugTimingStats() void {
    debug_timing_stats = .{};
}

pub fn getDebugTimingStats() DebugTimingStats {
    return debug_timing_stats;
}

fn enableMoeSyncProfileDebug() bool {
    return getenvBool("TERMITE_MOE_SYNC_PROFILE");
}

fn moeFeedForward(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
    decode_context: ?*const DecodeContext,
) !CT {
    return moeFeedForwardInner(cb, allocator, config, input, total, layer, name_buf, decode_context, false);
}

/// MoE feed-forward that skips the shared expert addition (used when the caller
/// already handles the shared expert in a separate sublayer, e.g. Gemma 4).
fn moeFeedForwardRoutedOnly(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
    decode_context: ?*const DecodeContext,
) !CT {
    return moeFeedForwardInner(cb, allocator, config, input, total, layer, name_buf, decode_context, true);
}

fn moeFeedForwardInner(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
    decode_context: ?*const DecodeContext,
    skip_shared_expert: bool,
) !CT {
    const hidden_size = config.hidden_size;
    const inter_size = config.expertIntermediateSize();
    const num_experts: usize = config.num_local_experts;
    const top_k: usize = @min(@as(usize, @intCast(config.num_experts_per_tok)), num_experts);
    if (num_experts == 0 or top_k == 0) return error.InvalidMoeConfig;
    const router_weight_fetch_started_at = monotonicNowNs();
    const router_w = try getMoeRouterWeight(cb, config, layer, name_buf);
    debug_timing_stats.moe_router_weight_fetch_nanos += @intCast(monotonicNowNs() - router_weight_fetch_started_at);
    defer cb.free(router_w);

    const router_input = try scaleMoeRouterInput(cb, config, input, hidden_size, layer, name_buf);
    defer if (router_input != input) cb.free(router_input);

    const router_proj_started_at = monotonicNowNs();
    const router_logits_ct = try cb.linearNoBias(router_input, router_w, total, hidden_size, num_experts);
    defer cb.free(router_logits_ct);
    debug_timing_stats.moe_router_proj_nanos += @intCast(monotonicNowNs() - router_proj_started_at);

    // The fused Metal MoE kernel is SiLU-only. Models with other expert
    // activations must use the generic path for correctness.
    if (cb.kind() != .graph and config.activation == .silu) {
        const w1 = getMoeExpertWeight(cb, config, layer, 0, "w1", name_buf) catch null;
        const w3 = getMoeExpertWeight(cb, config, layer, 0, "w3", name_buf) catch null;
        const w2 = getMoeExpertWeight(cb, config, layer, 0, "w2", name_buf) catch null;
        defer if (w1) |w| cb.free(w);
        defer if (w3) |w| cb.free(w);
        defer if (w2) |w| cb.free(w);
        if (w1 != null and w3 != null and w2 != null) {
            const expert_scale_ct: ?CT = blk: {
                const sn = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.expert_output_scale", .{layer}) catch break :blk null;
                break :blk getModelWeight(cb, config, sn) catch null;
            };
            defer if (expert_scale_ct) |s| cb.free(s);
            if (try cb.runMoeBlock(&.{
                .input = input,
                .router_logits = router_logits_ct,
                .w1 = w1.?,
                .w3 = w3.?,
                .w2 = w2.?,
                .expert_scale = expert_scale_ct,
                .total = total,
                .hidden_size = hidden_size,
                .inter_size = inter_size,
                .num_experts = num_experts,
                .top_k = top_k,
            })) |routed_output| {
                if (skip_shared_expert) return routed_output;
                return maybeAddSharedExpert(cb, allocator, config, input, routed_output, total, layer, name_buf);
            }
        }
    }

    const route_select_started_at = monotonicNowNs();
    const backend_routes = try cb.moeSelectRoutes(router_logits_ct, total, num_experts, top_k, allocator);
    debug_timing_stats.moe_route_select_nanos += @intCast(monotonicNowNs() - route_select_started_at);
    defer if (backend_routes) |routes| {
        allocator.free(routes.expert_ids);
        allocator.free(routes.route_weights);
    };
    const router_logits = if (backend_routes == null) blk: {
        const router_download_started_at = monotonicNowNs();
        const downloaded = try cb.toFloat32(router_logits_ct, allocator);
        debug_timing_stats.moe_router_download_nanos += @intCast(monotonicNowNs() - router_download_started_at);
        break :blk downloaded;
    } else null;
    defer if (router_logits) |downloaded| allocator.free(downloaded);
    var input_data: ?[]f32 = null;
    defer if (input_data) |downloaded| allocator.free(downloaded);

    // Gemma 4: per-expert output scale (ffn_down_exps.scale).
    // Folded into route weights: effective_weight = route_weight * expert_scale[expert_id].
    // In graph mode, pass the scale tensor to the backend so the interpreter
    // can apply it during fused_moe_scatter_add execution.
    if (cb.kind() == .graph) {
        const scale_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.expert_output_scale", .{layer}) catch null;
        if (scale_name) |sn| {
            if (getModelWeight(cb, config, sn)) |scale_w| {
                cb.setMoeExpertScale(scale_w);
                cb.free(scale_w);
            } else |_| {}
        }
    }
    const expert_output_scale: ?[]const f32 = if (cb.kind() == .graph) null else blk: {
        const scale_download_started_at = monotonicNowNs();
        const scale_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.expert_output_scale", .{layer}) catch break :blk null;
        const scale_w = getModelWeight(cb, config, scale_name) catch break :blk null;
        defer cb.free(scale_w);
        const result = try cb.toFloat32(scale_w, allocator);
        debug_timing_stats.moe_expert_scale_download_nanos += @intCast(monotonicNowNs() - scale_download_started_at);
        break :blk result;
    };
    defer if (expert_output_scale) |s| allocator.free(s);

    const output = try allocator.alloc(f32, total * hidden_size);
    errdefer allocator.free(output);
    @memset(output, 0.0);

    if (decode_context) |dc| {
        if (dc.moe_runtime) |moe_runtime| {
            const prepare_started_at = monotonicNowNs();
            try moe_runtime.prepareLayer(layer, num_experts);
            debug_timing_stats.moe_prepare_layer_nanos += @intCast(monotonicNowNs() - prepare_started_at);
            for (0..total) |row| {
                const selection = if (backend_routes) |routes|
                    selectionFromFlatRoutes(routes, row)
                else blk: {
                    const downloaded = router_logits.?;
                    const routing_logits = downloaded[row * num_experts ..][0..num_experts];
                    break :blk try selectTopExperts(allocator, routing_logits, top_k);
                };
                for (0..selection.count) |i| {
                    const append_started_at = monotonicNowNs();
                    var weight = selection.weights[i];
                    if (expert_output_scale) |eos| {
                        const eid = selection.indices[i];
                        if (eid < eos.len) weight *= eos[eid];
                    }
                    try moe_runtime.appendRoute(layer, selection.indices[i], @intCast(row), weight);
                    debug_timing_stats.moe_append_route_nanos += @intCast(monotonicNowNs() - append_started_at);
                }
            }
            const finalize_started_at = monotonicNowNs();
            try moe_runtime.finalizeLayer(layer, top_k);
            debug_timing_stats.moe_finalize_layer_nanos += @intCast(monotonicNowNs() - finalize_started_at);
            const prefetch_started_at = monotonicNowNs();
            prefetchMoeExperts(cb, layer, moe_runtime.predictedExperts(layer), moe_runtime.predictedExpertScores(layer), name_buf);
            debug_timing_stats.moe_prefetch_hint_nanos += @intCast(monotonicNowNs() - prefetch_started_at);
            if (try runMoeWithRuntimeBatches(cb, allocator, config, moe_runtime, input, output, layer, hidden_size, inter_size, name_buf)) |grouped_output| {
                allocator.free(output);
                if (skip_shared_expert) return grouped_output;
                return maybeAddSharedExpert(cb, allocator, config, input, grouped_output, total, layer, name_buf);
            } else {
                if (input_data == null) input_data = try downloadMoeInput(cb, allocator, input);
                for (moe_runtime.activeExperts(layer)) |expert_index| {
                    const batch = moe_runtime.batchView(layer, expert_index);
                    try runExpertBatch(cb, allocator, config, input_data.?, output, layer, expert_index, batch, hidden_size, inter_size, name_buf);
                }
            }
        } else {
            if (try runMoeWithLocalBatches(cb, allocator, config, router_logits, backend_routes, input, output, layer, num_experts, top_k, hidden_size, inter_size, name_buf, expert_output_scale)) |grouped_output| {
                allocator.free(output);
                if (skip_shared_expert) return grouped_output;
                return maybeAddSharedExpert(cb, allocator, config, input, grouped_output, total, layer, name_buf);
            }
        }
    } else {
        if (try runMoeWithLocalBatches(cb, allocator, config, router_logits, backend_routes, input, output, layer, num_experts, top_k, hidden_size, inter_size, name_buf, expert_output_scale)) |grouped_output| {
            allocator.free(output);
            if (skip_shared_expert) return grouped_output;
            return maybeAddSharedExpert(cb, allocator, config, input, grouped_output, total, layer, name_buf);
        }
    }

    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    const routed_output = try cb.fromFloat32Shape(output, &shape);
    allocator.free(output);
    if (skip_shared_expert) return routed_output;
    return maybeAddSharedExpert(cb, allocator, config, input, routed_output, total, layer, name_buf);
}

/// Add shared expert output to the routed expert output if the model has a shared expert.
/// The shared expert runs on every token and its output is simply added to the MoE output.
fn maybeAddSharedExpert(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    routed_output: CT,
    total: usize,
    layer: usize,
    name_buf: *[256]u8,
) !CT {
    if (!config.hasSharedExpert()) return routed_output;
    const hidden_size = config.hidden_size;
    const shared_inter = if (config.shared_expert_intermediate_size > 0)
        config.shared_expert_intermediate_size
    else
        config.intermediate_size;

    // Shared expert: gated FFN with gate, up, down projections.
    const gate_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.shared_expert.gate_proj.weight", .{layer}) catch return routed_output;
    const gate_w = getModelWeight(cb, config, gate_name) catch return routed_output;
    defer cb.free(gate_w);
    const gate_proj = try cb.linearNoBias(input, gate_w, total, hidden_size, shared_inter);
    defer cb.free(gate_proj);

    const up_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.shared_expert.up_proj.weight", .{layer}) catch return routed_output;
    const up_w = getModelWeight(cb, config, up_name) catch return routed_output;
    defer cb.free(up_w);
    const up_proj = try cb.linearNoBias(input, up_w, total, hidden_size, shared_inter);
    defer cb.free(up_proj);

    const gate_act = try applyActivation(cb, config, gate_proj);
    defer cb.free(gate_act);
    const gated = try cb.multiply(gate_act, up_proj);
    defer cb.free(gated);

    const down_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.shared_expert.down_proj.weight", .{layer}) catch return routed_output;
    const down_w = getModelWeight(cb, config, down_name) catch return routed_output;
    defer cb.free(down_w);
    const shared_out = try cb.linearNoBias(gated, down_w, total, shared_inter, hidden_size);
    defer cb.free(shared_out);

    _ = allocator;
    const result = try cb.add(routed_output, shared_out);
    cb.free(routed_output);
    return result;
}

fn downloadMoeInput(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
) ![]f32 {
    const input_download_started_at = monotonicNowNs();
    const input_data = try cb.toFloat32(input, allocator);
    debug_timing_stats.moe_input_download_nanos += @intCast(monotonicNowNs() - input_download_started_at);
    return input_data;
}

fn runMoeWithLocalBatches(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    router_logits: ?[]const f32,
    backend_routes: ?ops.MoeRouteSelection,
    input: CT,
    output: []f32,
    layer: usize,
    num_experts: usize,
    top_k: usize,
    hidden_size: usize,
    inter_size: usize,
    name_buf: *[256]u8,
    expert_output_scale: ?[]const f32,
) !?CT {
    const total = output.len / hidden_size;
    const selections = try allocator.alloc(MoeSelection, total);
    defer allocator.free(selections);

    var expert_counts = try allocator.alloc(usize, num_experts);
    defer allocator.free(expert_counts);
    @memset(expert_counts, 0);

    for (0..total) |row| {
        const selection = if (backend_routes) |routes|
            selectionFromFlatRoutes(routes, row)
        else blk: {
            const downloaded = router_logits.?;
            const routing_logits_row = downloaded[row * num_experts ..][0..num_experts];
            break :blk try selectTopExperts(allocator, routing_logits_row, top_k);
        };
        selections[row] = selection;
        for (selection.indices[0..selection.count]) |expert_index| {
            expert_counts[expert_index] += 1;
        }
    }

    var expert_batches = try allocator.alloc(ExpertBatch, num_experts);
    defer {
        for (expert_batches) |batch| {
            allocator.free(batch.rows);
            allocator.free(batch.route_weights);
        }
        allocator.free(expert_batches);
    }
    for (expert_counts, 0..) |count, expert_index| {
        expert_batches[expert_index] = .{
            .rows = try allocator.alloc(u32, count),
            .route_weights = try allocator.alloc(f32, count),
        };
    }

    var cursors = try allocator.alloc(usize, num_experts);
    defer allocator.free(cursors);
    @memset(cursors, 0);

    for (selections, 0..) |selection, row| {
        for (0..selection.count) |i| {
            const expert_index: usize = selection.indices[i];
            const cursor = cursors[expert_index];
            expert_batches[expert_index].rows[cursor] = @intCast(row);
            var weight = selection.weights[i];
            if (expert_output_scale) |eos| {
                if (expert_index < eos.len) weight *= eos[expert_index];
            }
            expert_batches[expert_index].route_weights[cursor] = weight;
            cursors[expert_index] = cursor + 1;
        }
    }

    var first_active_expert: ?usize = null;
    var grouped_count: usize = 0;
    for (expert_batches, 0..) |batch, expert_index| {
        if (batch.rows.len == 0) continue;
        if (first_active_expert == null) first_active_expert = expert_index;
        grouped_count += batch.rows.len;
    }
    if (first_active_expert) |representative_expert| {
        const grouped_rows = try allocator.alloc(u32, grouped_count);
        defer allocator.free(grouped_rows);
        const grouped_expert_ids = try allocator.alloc(u32, grouped_count);
        defer allocator.free(grouped_expert_ids);
        const grouped_route_weights = try allocator.alloc(f32, grouped_count);
        defer allocator.free(grouped_route_weights);

        var cursor: usize = 0;
        for (expert_batches, 0..) |batch, expert_index| {
            if (batch.rows.len == 0) continue;
            for (batch.rows, batch.route_weights, 0..) |row_index, route_weight, batch_row| {
                grouped_rows[cursor + batch_row] = row_index;
                grouped_expert_ids[cursor + batch_row] = @intCast(expert_index);
                grouped_route_weights[cursor + batch_row] = route_weight;
            }
            cursor += batch.rows.len;
        }
        const grouped_tiles = try buildGroupedExpertTiles(allocator, grouped_expert_ids, 4);
        defer grouped_tiles.deinit(allocator);

        if (try runGroupedExpertBatchTensor(
            cb,
            allocator,
            config,
            input,
            output.len / hidden_size,
            layer,
            representative_expert,
            .{
                .rows = grouped_rows,
                .expert_ids = grouped_expert_ids,
                .route_weights = grouped_route_weights,
                .expert_tile_ids = grouped_tiles.expert_tile_ids,
                .tile_row_starts = grouped_tiles.tile_row_starts,
                .tile_row_counts = grouped_tiles.tile_row_counts,
            },
            hidden_size,
            inter_size,
            name_buf,
        )) |result| {
            return result;
        }
        const input_data = try downloadMoeInput(cb, allocator, input);
        defer allocator.free(input_data);
        if (try runGroupedExpertBatch(
            cb,
            allocator,
            config,
            input_data,
            output,
            output.len / hidden_size,
            layer,
            representative_expert,
            .{
                .rows = grouped_rows,
                .expert_ids = grouped_expert_ids,
                .route_weights = grouped_route_weights,
                .expert_tile_ids = grouped_tiles.expert_tile_ids,
                .tile_row_starts = grouped_tiles.tile_row_starts,
                .tile_row_counts = grouped_tiles.tile_row_counts,
            },
            hidden_size,
            inter_size,
            name_buf,
        )) {
            return null;
        }
    }

    const input_data = try downloadMoeInput(cb, allocator, input);
    defer allocator.free(input_data);
    for (expert_batches, 0..) |batch, expert_index| {
        if (batch.rows.len == 0) continue;
        try runExpertBatch(cb, allocator, config, input_data, output, layer, expert_index, batch, hidden_size, inter_size, name_buf);
    }
    return null;
}

fn runMoeWithRuntimeBatches(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    moe_runtime: *runtime.moe.runtime.MoeRuntime,
    input: CT,
    output: []f32,
    layer: usize,
    hidden_size: usize,
    inter_size: usize,
    name_buf: *[256]u8,
) !?CT {
    debug_timing_stats.moe_grouped_attempts += 1;
    const active_experts = moe_runtime.activeExperts(layer);
    if (active_experts.len == 0) return null;
    var grouped_count: usize = 0;
    for (active_experts) |expert_index| {
        const batch = moe_runtime.batchView(layer, expert_index);
        grouped_count += batch.rows.len;
    }
    if (grouped_count == 0) return null;

    const grouped = try allocator.alloc(u32, grouped_count);
    defer allocator.free(grouped);
    const grouped_expert_ids = try allocator.alloc(u32, grouped_count);
    defer allocator.free(grouped_expert_ids);
    const grouped_route_weights = try allocator.alloc(f32, grouped_count);
    defer allocator.free(grouped_route_weights);

    var cursor: usize = 0;
    for (active_experts) |expert_index| {
        const batch = moe_runtime.batchView(layer, expert_index);
        for (batch.rows, batch.route_weights, 0..) |row_index, route_weight, batch_row| {
            grouped[cursor + batch_row] = row_index;
            grouped_expert_ids[cursor + batch_row] = expert_index;
            grouped_route_weights[cursor + batch_row] = route_weight;
        }
        cursor += batch.rows.len;
    }
    const grouped_tiles = try buildGroupedExpertTiles(allocator, grouped_expert_ids, 4);
    defer grouped_tiles.deinit(allocator);

    if (try runGroupedExpertBatchTensor(
        cb,
        allocator,
        config,
        input,
        output.len / hidden_size,
        layer,
        @intCast(active_experts[0]),
        .{
            .rows = grouped,
            .expert_ids = grouped_expert_ids,
            .route_weights = grouped_route_weights,
            .expert_tile_ids = grouped_tiles.expert_tile_ids,
            .tile_row_starts = grouped_tiles.tile_row_starts,
            .tile_row_counts = grouped_tiles.tile_row_counts,
        },
        hidden_size,
        inter_size,
        name_buf,
    )) |result| {
        debug_timing_stats.moe_grouped_successes += 1;
        return result;
    }
    const input_data = try downloadMoeInput(cb, allocator, input);
    defer allocator.free(input_data);
    const ok = try runGroupedExpertBatch(
        cb,
        allocator,
        config,
        input_data,
        output,
        output.len / hidden_size,
        layer,
        @intCast(active_experts[0]),
        .{
            .rows = grouped,
            .expert_ids = grouped_expert_ids,
            .route_weights = grouped_route_weights,
            .expert_tile_ids = grouped_tiles.expert_tile_ids,
            .tile_row_starts = grouped_tiles.tile_row_starts,
            .tile_row_counts = grouped_tiles.tile_row_counts,
        },
        hidden_size,
        inter_size,
        name_buf,
    );
    if (ok) {
        debug_timing_stats.moe_grouped_successes += 1;
    }
    return null;
}

fn selectTopExperts(allocator: std.mem.Allocator, router_logits: []const f32, top_k: usize) !MoeSelection {
    const num_experts = router_logits.len;
    const probs_buf = try allocator.alloc(f32, num_experts);
    defer allocator.free(probs_buf);
    @memcpy(probs_buf, router_logits);
    activations.softmax(probs_buf, num_experts);

    var selection = MoeSelection{
        .count = top_k,
        .indices = [_]u32{0} ** 8,
        .weights = [_]f32{0.0} ** 8,
    };
    const used = try allocator.alloc(bool, num_experts);
    defer allocator.free(used);
    @memset(used, false);
    var weight_sum: f32 = 0.0;

    for (0..top_k) |slot| {
        var best_index: usize = 0;
        var best_value: f32 = -std.math.inf(f32);
        for (probs_buf, 0..) |value, idx| {
            if (used[idx]) continue;
            if (value > best_value) {
                best_value = value;
                best_index = idx;
            }
        }
        used[best_index] = true;
        selection.indices[slot] = @intCast(best_index);
        selection.weights[slot] = best_value;
        weight_sum += best_value;
    }

    if (weight_sum > 0.0) {
        for (selection.weights[0..selection.count]) |*weight| {
            weight.* /= weight_sum;
        }
    }
    return selection;
}

fn selectionFromFlatRoutes(routes: ops.MoeRouteSelection, row: usize) MoeSelection {
    var selection = MoeSelection{
        .count = routes.top_k,
        .indices = [_]u32{0} ** 8,
        .weights = [_]f32{0.0} ** 8,
    };
    const base = row * routes.top_k;
    for (0..routes.top_k) |i| {
        selection.indices[i] = routes.expert_ids[base + i];
        selection.weights[i] = routes.route_weights[base + i];
    }
    return selection;
}

fn runExpertBatch(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_data: []const f32,
    output: []f32,
    layer: usize,
    expert_index: usize,
    batch: anytype,
    hidden_size: usize,
    inter_size: usize,
    name_buf: *[256]u8,
) !void {
    const started_at = monotonicNowNs();
    const batch_size = batch.rows.len;
    const expert_input = try allocator.alloc(f32, batch_size * hidden_size);
    defer allocator.free(expert_input);
    for (batch.rows, 0..) |row_index, batch_row| {
        const src = input_data[@as(usize, row_index) * hidden_size ..][0..hidden_size];
        const dst = expert_input[batch_row * hidden_size ..][0..hidden_size];
        @memcpy(dst, src);
    }

    const input_shape = [_]i32{ @intCast(batch_size), @intCast(hidden_size) };
    const expert_input_ct = try cb.fromFloat32Shape(expert_input, &input_shape);
    defer cb.free(expert_input_ct);

    const expert_weight_fetch_started_at = monotonicNowNs();
    const w1 = try getMoeExpertWeight(cb, config, layer, expert_index, "w1", name_buf);
    defer cb.free(w1);
    const w2 = try getMoeExpertWeight(cb, config, layer, expert_index, "w2", name_buf);
    defer cb.free(w2);
    const w3 = try getMoeExpertWeight(cb, config, layer, expert_index, "w3", name_buf);
    defer cb.free(w3);
    debug_timing_stats.moe_expert_weight_fetch_nanos += @intCast(monotonicNowNs() - expert_weight_fetch_started_at);

    const gate_proj = try cb.linearNoBias(expert_input_ct, w1, batch_size, hidden_size, inter_size);
    defer cb.free(gate_proj);
    const gate_act = try applyActivation(cb, config, gate_proj);
    defer cb.free(gate_act);
    const up_proj = try cb.linearNoBias(expert_input_ct, w3, batch_size, hidden_size, inter_size);
    defer cb.free(up_proj);
    const gated = try cb.multiply(gate_act, up_proj);
    defer cb.free(gated);
    const expert_out_ct = try cb.linearNoBias(gated, w2, batch_size, inter_size, hidden_size);
    defer cb.free(expert_out_ct);

    const expert_out = try cb.toFloat32(expert_out_ct, allocator);
    defer allocator.free(expert_out);

    if (expert_out.len != batch_size * hidden_size) return error.InvalidMoeBatchOutput;

    for (batch.rows, batch.route_weights, 0..) |row_index, route_weight, batch_row| {
        const src = expert_out[batch_row * hidden_size ..][0..hidden_size];
        const dst = output[@as(usize, row_index) * hidden_size ..][0..hidden_size];
        for (dst, src) |*out_value, in_value| {
            out_value.* += route_weight * in_value;
        }
    }
    debug_timing_stats.moe_fallback_nanos += @intCast(monotonicNowNs() - started_at);
}

fn runGroupedExpertBatch(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_data: []const f32,
    output: []f32,
    total_rows: usize,
    layer: usize,
    representative_expert: usize,
    grouped: GroupedExpertBatch,
    hidden_size: usize,
    inter_size: usize,
    name_buf: *[256]u8,
) !bool {
    const started_at = monotonicNowNs();
    if (cb.vtable.moeLinearNoBias == null) return false;
    if (grouped.rows.len == 0) return true;

    const batch_size = grouped.rows.len;
    const input_copy_started_at = monotonicNowNs();
    const expert_input = try allocator.alloc(f32, batch_size * hidden_size);
    defer allocator.free(expert_input);
    for (grouped.rows, 0..) |row_index, batch_row| {
        const src = input_data[@as(usize, row_index) * hidden_size ..][0..hidden_size];
        const dst = expert_input[batch_row * hidden_size ..][0..hidden_size];
        @memcpy(dst, src);
    }
    debug_timing_stats.moe_grouped_input_copy_nanos += @intCast(monotonicNowNs() - input_copy_started_at);

    const input_shape = [_]i32{ @intCast(batch_size), @intCast(hidden_size) };
    const input_upload_started_at = monotonicNowNs();
    const expert_input_ct = try cb.fromFloat32Shape(expert_input, &input_shape);
    debug_timing_stats.moe_grouped_input_upload_nanos += @intCast(monotonicNowNs() - input_upload_started_at);
    defer cb.free(expert_input_ct);

    const ops_started_at = monotonicNowNs();
    const expert_weight_fetch_started_at = monotonicNowNs();
    const w1 = try getMoeExpertWeight(cb, config, layer, representative_expert, "w1", name_buf);
    defer cb.free(w1);
    const w3 = try getMoeExpertWeight(cb, config, layer, representative_expert, "w3", name_buf);
    defer cb.free(w3);
    debug_timing_stats.moe_expert_weight_fetch_nanos += @intCast(monotonicNowNs() - expert_weight_fetch_started_at);
    const gate_proj = (try cb.moeLinearNoBias(expert_input_ct, grouped.expert_ids, grouped.expert_tile_ids, grouped.tile_row_starts, grouped.tile_row_counts, w1, batch_size, hidden_size, inter_size)) orelse return false;
    const up_proj = (try cb.moeLinearNoBias(expert_input_ct, grouped.expert_ids, grouped.expert_tile_ids, grouped.tile_row_starts, grouped.tile_row_counts, w3, batch_size, hidden_size, inter_size)) orelse return false;
    defer cb.free(gate_proj);
    defer cb.free(up_proj);
    const gate_act = try applyActivation(cb, config, gate_proj);
    defer cb.free(gate_act);

    const gated = try cb.multiply(gate_act, up_proj);
    defer cb.free(gated);

    const down_weight_fetch_started_at = monotonicNowNs();
    const w2 = try getMoeExpertWeight(cb, config, layer, representative_expert, "w2", name_buf);
    defer cb.free(w2);
    debug_timing_stats.moe_expert_weight_fetch_nanos += @intCast(monotonicNowNs() - down_weight_fetch_started_at);
    const expert_out_ct = (try cb.moeLinearNoBias(gated, grouped.expert_ids, grouped.expert_tile_ids, grouped.tile_row_starts, grouped.tile_row_counts, w2, batch_size, inter_size, hidden_size)) orelse return false;
    defer cb.free(expert_out_ct);
    debug_timing_stats.moe_grouped_ops_nanos += @intCast(monotonicNowNs() - ops_started_at);

    if (cb.vtable.moeScatterAdd != null) {
        const scatter_started_at = monotonicNowNs();
        const output_ct = if (try cb.zeroTensor(total_rows, hidden_size)) |zero_ct|
            zero_ct
        else blk: {
            const zero_output = try allocator.alloc(f32, total_rows * hidden_size);
            defer allocator.free(zero_output);
            @memset(zero_output, 0.0);
            const output_shape = [_]i32{ @intCast(total_rows), @intCast(hidden_size) };
            break :blk try cb.fromFloat32Shape(zero_output, &output_shape);
        };
        defer cb.free(output_ct);

        if (try cb.moeScatterAdd(output_ct, grouped.rows, grouped.route_weights, expert_out_ct, batch_size, hidden_size)) |scattered_ct| {
            defer cb.free(scattered_ct);
            debug_timing_stats.moe_grouped_scatter_nanos += @intCast(monotonicNowNs() - scatter_started_at);

            const output_download_started_at = monotonicNowNs();
            const scattered_out = try cb.toFloat32(scattered_ct, allocator);
            debug_timing_stats.moe_grouped_output_download_nanos += @intCast(monotonicNowNs() - output_download_started_at);
            defer allocator.free(scattered_out);
            if (scattered_out.len != output.len) return error.InvalidMoeBatchOutput;
            @memcpy(output, scattered_out);
            debug_timing_stats.moe_grouped_nanos += @intCast(monotonicNowNs() - started_at);
            return true;
        }
    }

    const output_download_started_at = monotonicNowNs();
    const expert_out = try cb.toFloat32(expert_out_ct, allocator);
    debug_timing_stats.moe_grouped_output_download_nanos += @intCast(monotonicNowNs() - output_download_started_at);
    defer allocator.free(expert_out);
    if (expert_out.len != batch_size * hidden_size) return error.InvalidMoeBatchOutput;

    const scatter_started_at = monotonicNowNs();
    for (grouped.rows, grouped.route_weights, 0..) |row_index, route_weight, batch_row| {
        const src = expert_out[batch_row * hidden_size ..][0..hidden_size];
        const dst = output[@as(usize, row_index) * hidden_size ..][0..hidden_size];
        for (dst, src) |*out_value, in_value| {
            out_value.* += route_weight * in_value;
        }
    }
    debug_timing_stats.moe_grouped_scatter_nanos += @intCast(monotonicNowNs() - scatter_started_at);
    debug_timing_stats.moe_grouped_nanos += @intCast(monotonicNowNs() - started_at);
    return true;
}

fn runGroupedExpertBatchTensor(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total_rows: usize,
    layer: usize,
    representative_expert: usize,
    grouped: GroupedExpertBatch,
    hidden_size: usize,
    inter_size: usize,
    name_buf: *[256]u8,
) !?CT {
    const started_at = monotonicNowNs();
    if (cb.vtable.moeLinearNoBias == null or cb.vtable.moeScatterAdd == null) return null;
    if (grouped.rows.len == 0) return null;

    const batch_size = grouped.rows.len;
    const input_upload_started_at = monotonicNowNs();
    const expert_input_ct = (try cb.takeRows(input, grouped.rows, batch_size, hidden_size)) orelse return null;
    debug_timing_stats.moe_grouped_input_upload_nanos += @intCast(monotonicNowNs() - input_upload_started_at);
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(expert_input_ct);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }

    const ops_started_at = monotonicNowNs();
    const expert_weight_fetch_started_at = monotonicNowNs();
    const w1 = try getMoeExpertWeight(cb, config, layer, representative_expert, "w1", name_buf);
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(w1);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }
    const w3 = try getMoeExpertWeight(cb, config, layer, representative_expert, "w3", name_buf);
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(w3);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }
    debug_timing_stats.moe_expert_weight_fetch_nanos += @intCast(monotonicNowNs() - expert_weight_fetch_started_at);
    const gate_proj = (try cb.moeLinearNoBias(expert_input_ct, grouped.expert_ids, grouped.expert_tile_ids, grouped.tile_row_starts, grouped.tile_row_counts, w1, batch_size, hidden_size, inter_size)) orelse return null;
    errdefer cb.free(gate_proj);
    if (enableMoeSyncProfileDebug()) {
        const sync_started_at = monotonicNowNs();
        try cb.evalTensor(gate_proj);
        debug_timing_stats.moe_grouped_sync_w1_nanos += @intCast(monotonicNowNs() - sync_started_at);
    }
    const up_proj = (try cb.moeLinearNoBias(expert_input_ct, grouped.expert_ids, grouped.expert_tile_ids, grouped.tile_row_starts, grouped.tile_row_counts, w3, batch_size, hidden_size, inter_size)) orelse return null;
    if (enableMoeSyncProfileDebug()) {
        const sync_started_at = monotonicNowNs();
        try cb.evalTensor(up_proj);
        debug_timing_stats.moe_grouped_sync_w3_nanos += @intCast(monotonicNowNs() - sync_started_at);
    }
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(gate_proj);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(up_proj);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }

    const gate_act = try applyActivation(cb, config, gate_proj);
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(gate_act);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }

    const gated = try cb.multiply(gate_act, up_proj);
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(gated);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }
    if (enableMoeSyncProfileDebug()) {
        const sync_started_at = monotonicNowNs();
        try cb.evalTensor(gated);
        debug_timing_stats.moe_grouped_sync_gate_nanos += @intCast(monotonicNowNs() - sync_started_at);
    }

    const down_weight_fetch_started_at = monotonicNowNs();
    const w2 = try getMoeExpertWeight(cb, config, layer, representative_expert, "w2", name_buf);
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(w2);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }
    debug_timing_stats.moe_expert_weight_fetch_nanos += @intCast(monotonicNowNs() - down_weight_fetch_started_at);
    const expert_out_ct = (try cb.moeLinearNoBias(gated, grouped.expert_ids, grouped.expert_tile_ids, grouped.tile_row_starts, grouped.tile_row_counts, w2, batch_size, inter_size, hidden_size)) orelse return null;
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(expert_out_ct);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }
    if (enableMoeSyncProfileDebug()) {
        const sync_started_at = monotonicNowNs();
        try cb.evalTensor(expert_out_ct);
        debug_timing_stats.moe_grouped_sync_w2_nanos += @intCast(monotonicNowNs() - sync_started_at);
        debug_timing_stats.moe_grouped_sync_ops_nanos += @intCast(monotonicNowNs() - sync_started_at);
    }
    debug_timing_stats.moe_grouped_ops_nanos += @intCast(monotonicNowNs() - ops_started_at);

    const scatter_started_at = monotonicNowNs();
    const output_ct = if (try cb.zeroTensor(total_rows, hidden_size)) |zero_ct|
        zero_ct
    else blk: {
        const zero_output = try allocator.alloc(f32, total_rows * hidden_size);
        defer allocator.free(zero_output);
        @memset(zero_output, 0.0);
        const output_shape = [_]i32{ @intCast(total_rows), @intCast(hidden_size) };
        break :blk try cb.fromFloat32Shape(zero_output, &output_shape);
    };
    defer {
        const cleanup_started_at = monotonicNowNs();
        cb.free(output_ct);
        debug_timing_stats.moe_grouped_cleanup_nanos += @intCast(monotonicNowNs() - cleanup_started_at);
    }

    const scattered_ct = (try cb.moeScatterAdd(output_ct, grouped.rows, grouped.route_weights, expert_out_ct, batch_size, hidden_size)) orelse return null;
    debug_timing_stats.moe_grouped_scatter_nanos += @intCast(monotonicNowNs() - scatter_started_at);
    if (enableMoeSyncProfileDebug()) {
        const sync_started_at = monotonicNowNs();
        try cb.evalTensor(scattered_ct);
        debug_timing_stats.moe_grouped_sync_scatter_nanos += @intCast(monotonicNowNs() - sync_started_at);
    }
    debug_timing_stats.moe_grouped_nanos += @intCast(monotonicNowNs() - started_at);
    return scattered_ct;
}

// --- Normalization helpers ---

fn applyAttnNorm(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, hidden: CT, layer: usize, buf: *[256]u8) !CT {
    return applyNormAt(cb, allocator, config, hidden, layer, "attn", buf);
}

fn applyFFNNorm(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, hidden: CT, layer: usize, buf: *[256]u8) !CT {
    return applyNormAt(cb, allocator, config, hidden, layer, "ffn", buf);
}

fn applyNormAt(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, hidden: CT, layer: usize, which: []const u8, buf: *[256]u8) !CT {
    const dim = config.hidden_size;
    const is_attn = std.mem.eql(u8, which, "attn");

    switch (config.family) {
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .bitnet => {
            const suffix = if (is_attn) "input_layernorm.weight" else "post_attention_layernorm.weight";
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
            const base_w = try getModelWeight(cb, config, name);
            defer cb.free(base_w);
            const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, dim);
            defer if (w != base_w) cb.free(w);
            return cb.rmsNorm(hidden, w, dim, config.norm_eps);
        },
        .gpt2 => {
            const suffix = if (is_attn) "ln_1" else "ln_2";
            const w_name = std.fmt.bufPrint(buf, "h.{d}.{s}.weight", .{ layer, suffix }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "h.{d}.{s}.bias", .{ layer, suffix }) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        .gpt_neo => {
            const suffix = if (is_attn) "ln_1" else "ln_2";
            const w_name = std.fmt.bufPrint(buf, "h.{d}.{s}.weight", .{ layer, suffix }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "h.{d}.{s}.bias", .{ layer, suffix }) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        .phi => {
            const suffix = if (is_attn) "input_layernorm.weight" else "post_attention_layernorm.weight";
            const w_name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, @as([]const u8, if (is_attn) "input_layernorm.bias" else "post_attention_layernorm.bias") }) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        .gptj, .gpt_neox => {
            const suffix = if (is_attn) "input_layernorm.weight" else "post_attention_layernorm.weight";
            const w_name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, suffix }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, @as([]const u8, if (is_attn) "input_layernorm.bias" else "post_attention_layernorm.bias") }) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        else => {
            // Generic: try RMS norm first, fall back to layer norm
            if (config.norm_type == .rms_norm) {
                const name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, @as([]const u8, if (is_attn) "input_layernorm.weight" else "post_attention_layernorm.weight") }) catch return error.NameTooLong;
                const base_w = try getModelWeight(cb, config, name);
                defer cb.free(base_w);
                const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, dim);
                defer if (w != base_w) cb.free(w);
                return cb.rmsNorm(hidden, w, dim, config.norm_eps);
            } else {
                const w_name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, @as([]const u8, if (is_attn) "input_layernorm.weight" else "post_attention_layernorm.weight") }) catch return error.NameTooLong;
                const w = try getModelWeight(cb, config, w_name);
                defer cb.free(w);
                const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.{s}", .{ layer, @as([]const u8, if (is_attn) "input_layernorm.bias" else "post_attention_layernorm.bias") }) catch return error.NameTooLong;
                const b = try getModelWeight(cb, config, b_name);
                defer cb.free(b);
                return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
            }
        },
    }
}

fn applyFinalNorm(cb: *const ComputeBackend, allocator: std.mem.Allocator, config: Config, hidden: CT) !CT {
    const dim = config.hidden_size;
    switch (config.family) {
        .deepseek_v4 => {
            var runtime_adapter = DeepSeekV4RuntimeAdapter.init(cb, config);
            const headed = try deepseek_v4.applyHyperHeadFallback(cb, allocator, config, runtime_adapter.runtime(), hidden);
            defer if (headed != hidden) cb.free(headed);
            const base_w = try getModelWeight(cb, config, "model.norm.weight");
            defer cb.free(base_w);
            const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, dim);
            defer if (w != base_w) cb.free(w);
            return cb.rmsNorm(headed, w, dim, config.norm_eps);
        },
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .bitnet => {
            const base_w = try getModelWeight(cb, config, "model.norm.weight");
            defer cb.free(base_w);
            const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, dim);
            defer if (w != base_w) cb.free(w);
            const result = try cb.rmsNorm(hidden, w, dim, config.norm_eps);
            return result;
        },
        .gpt2 => {
            const w = try cb.getWeight("ln_f.weight");
            defer cb.free(w);
            const b = try cb.getWeight("ln_f.bias");
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        .gpt_neo => {
            const w = try cb.getWeight("ln_f.weight");
            defer cb.free(w);
            const b = try cb.getWeight("ln_f.bias");
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        .phi => {
            const w = try getModelWeight(cb, config, "model.norm.weight");
            defer cb.free(w);
            const b = try getModelWeight(cb, config, "model.norm.bias");
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        .gptj, .gpt_neox => {
            const w = try getModelWeight(cb, config, "model.norm.weight");
            defer cb.free(w);
            const b = try getModelWeight(cb, config, "model.norm.bias");
            defer cb.free(b);
            return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
        },
        else => {
            // Try common patterns
            if (getModelWeight(cb, config, "model.norm.weight")) |base_w| {
                defer cb.free(base_w);
                const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, dim);
                defer if (w != base_w) cb.free(w);
                return cb.rmsNorm(hidden, w, dim, config.norm_eps);
            } else |_| {}
            if (cb.getWeight("ln_f.weight")) |w| {
                defer cb.free(w);
                const b = try cb.getWeight("ln_f.bias");
                defer cb.free(b);
                return cb.layerNorm(hidden, w, b, dim, config.norm_eps);
            } else |_| {}
            return error.MissingFinalNormWeight;
        },
    }
}

fn applyBitNetAttentionSubNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.attn_sub_norm.weight", .{layer}) catch return error.NameTooLong;
    const base_w = try getModelWeight(cb, config, name);
    defer cb.free(base_w);
    const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, config.hidden_size);
    defer if (w != base_w) cb.free(w);
    return cb.rmsNorm(hidden, w, config.hidden_size, config.norm_eps);
}

fn applyGemmaPostAttentionNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return (try applyOptionalAdjustedRmsNormByNames(cb, allocator, config, hidden, config.hidden_size, &.{
        std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer}) catch return error.NameTooLong,
    })) orelse hidden;
}

fn applyGemmaFfnPreNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    var primary_buf: [256]u8 = undefined;
    const primary = std.fmt.bufPrint(&primary_buf, "model.layers.{d}.pre_feedforward_layernorm.weight", .{layer}) catch return error.NameTooLong;
    const fallback = std.fmt.bufPrint(buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer}) catch return error.NameTooLong;
    return (try applyOptionalAdjustedRmsNormByNames(cb, allocator, config, hidden, config.hidden_size, &.{ primary, fallback })) orelse error.MissingLayerNormWeight;
}

fn applyGemmaFfnPostNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return (try applyOptionalAdjustedRmsNormByNames(cb, allocator, config, hidden, config.hidden_size, &.{
        std.fmt.bufPrint(buf, "model.layers.{d}.post_feedforward_layernorm.weight", .{layer}) catch return error.NameTooLong,
    })) orelse hidden;
}

/// Gemma 4: post-norm for the shared expert FFN sublayer (post_ffw_norm_1).
fn applyGemmaSharedFfnPostNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    var primary_buf: [256]u8 = undefined;
    const primary = std.fmt.bufPrint(&primary_buf, "model.layers.{d}.post_feedforward_layernorm_1.weight", .{layer}) catch return error.NameTooLong;
    const fallback = std.fmt.bufPrint(buf, "model.layers.{d}.post_feedforward_layernorm.weight", .{layer}) catch return error.NameTooLong;
    return (try applyOptionalAdjustedRmsNormByNames(cb, allocator, config, hidden, config.hidden_size, &.{ primary, fallback })) orelse hidden;
}

/// Gemma 4: pre-norm for the MoE routed expert sublayer (pre_ffw_norm_2).
fn applyGemmaMoeFfnPreNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return (try applyOptionalAdjustedRmsNormByNames(cb, allocator, config, hidden, config.hidden_size, &.{
        std.fmt.bufPrint(buf, "model.layers.{d}.pre_feedforward_layernorm_2.weight", .{layer}) catch return error.NameTooLong,
    })) orelse error.MissingLayerNormWeight;
}

/// Gemma 4: post-norm for the MoE routed expert sublayer (post_ffw_norm_2).
fn applyGemmaMoeFfnPostNorm(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    layer: usize,
    buf: *[256]u8,
) !CT {
    return (try applyOptionalAdjustedRmsNormByNames(cb, allocator, config, hidden, config.hidden_size, &.{
        std.fmt.bufPrint(buf, "model.layers.{d}.post_feedforward_layernorm_2.weight", .{layer}) catch return error.NameTooLong,
    })) orelse hidden;
}

fn applyOptionalAdjustedRmsNormByNames(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    dim: usize,
    names: []const []const u8,
) !?CT {
    for (names) |name| {
        const base_w = getModelWeight(cb, config, name) catch |err| switch (err) {
            error.MissingWeight, error.WeightNotFound => continue,
            else => return err,
        };
        defer cb.free(base_w);
        const w = try maybeAdjustNormWeight(cb, allocator, config, base_w, dim);
        defer if (w != base_w) cb.free(w);
        return try cb.rmsNorm(hidden, w, dim, config.norm_eps);
    }
    return null;
}

// --- Attention projection helpers ---

fn attnProject(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    in_dim: u32,
    out_dim: u32,
    layer: usize,
    proj: []const u8, // "q", "k", "v"
    buf: *[256]u8,
) !CT {
    switch (config.family) {
        .llama, .mistral, .gemma, .bitnet => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            const projected = try cb.linearNoBias(input, w, total, in_dim, out_dim);
            return projected;
        },
        .qwen2 => {
            // Qwen2 q/k/v_proj carry biases (unlike LLaMA/Mistral/Gemma).
            const w_name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.bias", .{ layer, proj }) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.linear(input, w, b, total, in_dim, out_dim);
        },
        .qwen3, .qwen3_5 => {
            // Qwen3 dropped the q/k/v_proj biases that Qwen2 had. QK-norm is
            // applied separately in maybeApplyQKHeadNorm before RoPE.
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            return cb.linearNoBias(input, w, total, in_dim, out_dim);
        },
        .gpt2 => {
            // Try individual projections first (common in modern GPT-2 checkpoints)
            const w_name = std.fmt.bufPrint(buf, "h.{d}.attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            if (cb.getWeight(w_name)) |w| {
                defer cb.free(w);
                const b_name = std.fmt.bufPrint(buf, "h.{d}.attn.{s}_proj.bias", .{ layer, proj }) catch return error.NameTooLong;
                const b = try getModelWeight(cb, config, b_name);
                defer cb.free(b);
                return cb.linear(input, w, b, total, in_dim, out_dim);
            } else |_| {
                // Fused c_attn: single [3*hidden, hidden] weight + [3*hidden] bias
                return fusedCAttnProject(cb, allocator, input, total, in_dim, out_dim, layer, proj, buf);
            }
        },
        .gpt_neo => {
            const w_name = std.fmt.bufPrint(buf, "h.{d}.attn.attention.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);

            const b_name = std.fmt.bufPrint(buf, "h.{d}.attn.attention.{s}_proj.bias", .{ layer, proj }) catch return error.NameTooLong;
            if (cb.getWeight(b_name)) |b| {
                defer cb.free(b);
                return cb.linear(input, w, b, total, in_dim, out_dim);
            } else |_| {
                return cb.linearNoBias(input, w, total, in_dim, out_dim);
            }
        },
        .phi => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.bias", .{ layer, proj }) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.linear(input, w, b, total, in_dim, out_dim);
        },
        .gptj, .gpt_neox => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.bias", .{ layer, proj }) catch return error.NameTooLong;
            if (cb.getWeight(b_name)) |b| {
                defer cb.free(b);
                return cb.linear(input, w, b, total, in_dim, out_dim);
            } else |_| {
                return cb.linearNoBias(input, w, total, in_dim, out_dim);
            }
        },
        else => {
            // Generic: try no-bias first (most common for modern models)
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            return cb.linearNoBias(input, w, total, in_dim, out_dim);
        },
    }
}

fn attnProjectPair(
    cb: *const ComputeBackend,
    config: Config,
    input: CT,
    total: usize,
    in_dim: u32,
    out_dim: u32,
    layer: usize,
    proj_a: []const u8,
    proj_b: []const u8,
    buf: *[256]u8,
) !ops.LinearNoBiasPairResult {
    switch (config.family) {
        .llama, .mistral, .gemma, .bitnet => {
            const name_a = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj_a }) catch return error.NameTooLong;
            const w_a = try getModelWeight(cb, config, name_a);
            defer cb.free(w_a);
            var second_buf: [256]u8 = undefined;
            const name_b = std.fmt.bufPrint(&second_buf, "model.layers.{d}.self_attn.{s}_proj.weight", .{ layer, proj_b }) catch return error.NameTooLong;
            const w_b = try getModelWeight(cb, config, name_b);
            defer cb.free(w_b);
            debug_timing_stats.attention_project_pair_calls += 1;
            return cb.linearNoBiasPair(input, w_a, w_b, total, in_dim, out_dim);
        },
        else => return error.UnsupportedTensorType,
    }
}

/// Split a fused c_attn weight [3*hidden, hidden] into Q/K/V chunks and project.
fn fusedCAttnProject(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    input: CT,
    total: usize,
    in_dim: u32,
    out_dim: u32,
    layer: usize,
    proj: []const u8,
    buf: *[256]u8,
) !CT {
    const chunk_idx: usize = if (std.mem.eql(u8, proj, "q")) 0 else if (std.mem.eql(u8, proj, "k")) 1 else 2;

    // Load fused weight [3*out_dim, in_dim] and bias [3*out_dim]
    const fused_w_name = std.fmt.bufPrint(buf, "h.{d}.attn.c_attn.weight", .{layer}) catch return error.NameTooLong;
    const fused_w = try cb.getWeight(fused_w_name);
    defer cb.free(fused_w);
    const fused_b_name = std.fmt.bufPrint(buf, "h.{d}.attn.c_attn.bias", .{layer}) catch return error.NameTooLong;
    const fused_b = try cb.getWeight(fused_b_name);
    defer cb.free(fused_b);
    if (layer == 0) {
        try maybeDebugTensor(cb, allocator, "layer0_c_attn_weight", fused_w);
        try maybeDebugTensor(cb, allocator, "layer0_c_attn_bias", fused_b);
    }

    if (cb.kind() == .graph) {
        const fused_out_dim: usize = @as(usize, out_dim) * 3;
        const projected = try cb.linear(input, fused_w, fused_b, total, in_dim, fused_out_dim);
        defer cb.free(projected);
        const start = chunk_idx * @as(usize, out_dim);
        return cb.sliceLastDim(projected, start, start + @as(usize, out_dim));
    }

    // Convert to f32 for slicing
    const w_data = try cb.toFloat32(fused_w, allocator);
    defer allocator.free(w_data);
    const b_data = try cb.toFloat32(fused_b, allocator);
    defer allocator.free(b_data);

    // Slice weight: row [chunk_idx*out_dim .. (chunk_idx+1)*out_dim] of [3*out_dim, in_dim]
    const w_slice = try allocator.alloc(f32, out_dim * in_dim);
    defer allocator.free(w_slice);
    const row_start = chunk_idx * out_dim;
    for (0..out_dim) |r| {
        @memcpy(w_slice[r * in_dim ..][0..in_dim], w_data[(row_start + r) * in_dim ..][0..in_dim]);
    }

    // Slice bias: [chunk_idx*out_dim .. (chunk_idx+1)*out_dim]
    const b_slice = try allocator.alloc(f32, out_dim);
    defer allocator.free(b_slice);
    @memcpy(b_slice, b_data[row_start..][0..out_dim]);

    // Wrap as CT and do linear
    const w_shape = [_]i32{ @intCast(out_dim), @intCast(in_dim) };
    const w_ct = try cb.fromFloat32Shape(w_slice, &w_shape);
    defer cb.free(w_ct);
    const b_shape = [_]i32{@intCast(out_dim)};
    const b_ct = try cb.fromFloat32Shape(b_slice, &b_shape);
    defer cb.free(b_ct);
    if (layer == 0 and chunk_idx == 0) {
        try maybeDebugTensor(cb, allocator, "layer0_c_attn_q_weight", w_ct);
        try maybeDebugTensor(cb, allocator, "layer0_c_attn_q_bias", b_ct);
    }

    return cb.linear(input, w_ct, b_ct, total, in_dim, out_dim);
}

fn attnOutputProject(
    cb: *const ComputeBackend,
    _: std.mem.Allocator,
    config: Config,
    input: CT,
    total: usize,
    in_dim: u32,
    out_dim: u32,
    layer: usize,
    buf: *[256]u8,
) !CT {
    switch (config.family) {
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .bitnet => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer}) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            const projected = try cb.linearNoBias(input, w, total, in_dim, out_dim);
            return projected;
        },
        .gpt2 => {
            const w_name = std.fmt.bufPrint(buf, "h.{d}.attn.c_proj.weight", .{layer}) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "h.{d}.attn.c_proj.bias", .{layer}) catch return error.NameTooLong;
            const b = try getModelWeight(cb, config, b_name);
            defer cb.free(b);
            return cb.linear(input, w, b, total, in_dim, out_dim);
        },
        .gpt_neo => {
            const w_name = std.fmt.bufPrint(buf, "h.{d}.attn.attention.out_proj.weight", .{layer}) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, w_name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "h.{d}.attn.attention.out_proj.bias", .{layer}) catch return error.NameTooLong;
            if (cb.getWeight(b_name)) |b| {
                defer cb.free(b);
                return cb.linear(input, w, b, total, in_dim, out_dim);
            } else |_| {
                return cb.linearNoBias(input, w, total, in_dim, out_dim);
            }
        },
        .gptj, .gpt_neox => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer}) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            const b_name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_proj.bias", .{layer}) catch return error.NameTooLong;
            if (cb.getWeight(b_name)) |b| {
                defer cb.free(b);
                return cb.linear(input, w, b, total, in_dim, out_dim);
            } else |_| {
                return cb.linearNoBias(input, w, total, in_dim, out_dim);
            }
        },
        else => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer}) catch return error.NameTooLong;
            const w = try getModelWeight(cb, config, name);
            defer cb.free(w);
            return cb.linearNoBias(input, w, total, in_dim, out_dim);
        },
    }
}

// --- FFN weight helpers ---

fn maybePrefixedModelName(config: Config, name: []const u8, buf: *[256]u8) ![]const u8 {
    if (config.weight_prefix.len == 0) return name;
    if (!std.mem.startsWith(u8, name, "model.")) return name;
    return std.fmt.bufPrint(buf, "{s}.{s}", .{ config.weight_prefix, name["model.".len..] });
}

pub fn getModelWeight(cb: *const ComputeBackend, config: Config, name: []const u8) !CT {
    if (config.weight_prefix.len != 0 and std.mem.startsWith(u8, name, "model.")) {
        var buf: [256]u8 = undefined;
        const prefixed = try maybePrefixedModelName(config, name, &buf);
        return cb.getWeight(prefixed) catch |err| switch (err) {
            error.MissingWeight => getModelWeightUnprefixedFallback(cb, name),
            else => err,
        };
    }
    return getModelWeightUnprefixedFallback(cb, name);
}

fn getModelWeightUnprefixedFallback(cb: *const ComputeBackend, name: []const u8) !CT {
    return cb.getWeight(name) catch |err| switch (err) {
        error.MissingWeight => if (modelPrefixStrippedName(name)) |stripped| cb.getWeight(stripped) else err,
        else => err,
    };
}

fn modelPrefixStrippedName(name: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, name, "model.")) return null;
    return name["model.".len..];
}

test "model weight fallback strips model prefix for bare Jina/Qwen backbones" {
    try std.testing.expectEqualStrings("embed_tokens.weight", modelPrefixStrippedName("model.embed_tokens.weight") orelse return error.MissingFallback);
    try std.testing.expectEqualStrings("layers.0.self_attn.q_proj.weight", modelPrefixStrippedName("model.layers.0.self_attn.q_proj.weight") orelse return error.MissingFallback);
    try std.testing.expect(modelPrefixStrippedName("embed_tokens.weight") == null);
}

pub fn getFFNWeight(cb: *const ComputeBackend, config: Config, layer: usize, proj: []const u8, buf: *[256]u8) !CT {
    switch (config.family) {
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .bitnet => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.mlp.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
        .gpt2 => {
            // GPT-2: h.N.mlp.c_fc.weight / h.N.mlp.c_proj.weight
            const suffix = if (std.mem.eql(u8, proj, "fc1")) "c_fc" else "c_proj";
            const name = std.fmt.bufPrint(buf, "h.{d}.mlp.{s}.weight", .{ layer, suffix }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
        .gpt_neo => {
            const suffix = if (std.mem.eql(u8, proj, "fc1")) "c_fc" else "c_proj";
            const name = std.fmt.bufPrint(buf, "h.{d}.mlp.{s}.weight", .{ layer, suffix }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
        .gptj, .gpt_neox => {
            const suffix = if (std.mem.eql(u8, proj, "fc1")) "fc1_proj" else "fc2_proj";
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.mlp.{s}.weight", .{ layer, suffix }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
        else => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.mlp.{s}_proj.weight", .{ layer, proj }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
    }
}

fn getMoeRouterWeight(cb: *const ComputeBackend, config: Config, layer: usize, buf: *[256]u8) !CT {
    const primary = std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.gate.weight", .{layer}) catch return error.NameTooLong;
    return getModelWeight(cb, config, primary);
}

fn getMoeExpertWeight(cb: *const ComputeBackend, config: Config, layer: usize, expert_index: usize, proj: []const u8, buf: *[256]u8) !CT {
    const packed_name = std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.packed.{s}.weight", .{ layer, proj }) catch return error.NameTooLong;
    return getModelWeight(cb, config, packed_name) catch |err| switch (err) {
        error.MissingWeight => {
            const primary = std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.experts.{d}.{s}.weight", .{ layer, expert_index, proj }) catch return error.NameTooLong;
            return getModelWeight(cb, config, primary);
        },
        else => err,
    };
}

fn prefetchMoeExperts(cb: *const ComputeBackend, layer: usize, expert_indices: []const u32, expert_scores: []const u32, buf: *[256]u8) void {
    if (cb.vtable.moeLinearNoBias != null and cb.vtable.moeScatterAdd != null) return;
    for (expert_indices, 0..) |expert_index, rank| {
        const score = if (rank < expert_scores.len) expert_scores[rank] else 0;
        const expert_hint = prefetchHintForExpertPrediction(expert_indices.len, rank, score);
        prefetchMoeExpertWeight(cb, layer, expert_index, "w1", expert_hint + 3, buf);
        prefetchMoeExpertWeight(cb, layer, expert_index, "w2", expert_hint + 1, buf);
        prefetchMoeExpertWeight(cb, layer, expert_index, "w3", expert_hint + 2, buf);
    }
}

fn prefetchMoeExpertWeight(cb: *const ComputeBackend, layer: usize, expert_index: u32, proj: []const u8, hint: u32, buf: *[256]u8) void {
    const name = std.fmt.bufPrint(buf, "model.layers.{d}.block_sparse_moe.experts.{d}.{s}.weight", .{ layer, expert_index, proj }) catch return;
    cb.prefetchWeightHint(name, hint);
}

fn prefetchHintForExpertPrediction(total: usize, rank: usize, score: u32) u32 {
    const clamped_rank = @min(rank, total);
    const remaining = total -| clamped_rank;
    const rank_hint: usize = @max(@as(usize, 1), remaining * 16);
    const score_hint: usize = @min(@as(usize, score), 255);
    return @intCast(rank_hint + score_hint);
}

fn getFFNBias(cb: *const ComputeBackend, config: Config, layer: usize, proj: []const u8, buf: *[256]u8) !CT {
    switch (config.family) {
        .gpt2, .gpt_neo => {
            const suffix = if (std.mem.eql(u8, proj, "fc1")) "c_fc" else "c_proj";
            const name = std.fmt.bufPrint(buf, "h.{d}.mlp.{s}.bias", .{ layer, suffix }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
        .gptj, .gpt_neox => {
            const suffix = if (std.mem.eql(u8, proj, "fc1")) "fc1_proj" else "fc2_proj";
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.mlp.{s}.bias", .{ layer, suffix }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
        else => {
            const name = std.fmt.bufPrint(buf, "model.layers.{d}.mlp.{s}_proj.bias", .{ layer, proj }) catch return error.NameTooLong;
            return getModelWeight(cb, config, name);
        },
    }
}

/// Apply per-layer output scale (HF: self.layer_scalar / skip_scale).
/// Multiplies hidden by a scalar loaded from layer_output_scale.weight.
/// Returns the input unchanged if the weight is ~1.0.
pub fn applyLayerOutputScale(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    total: usize,
    hidden_size: usize,
    layer: usize,
) !CT {
    var name_buf: [256]u8 = undefined;
    const scale_name = std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.layer_output_scale.weight", .{layer}) catch return hidden;
    const scale_w = getModelWeight(cb, config, scale_name) catch |err| switch (err) {
        error.MissingWeight => blk: {
            var fallback_buf: [256]u8 = undefined;
            const fallback = std.fmt.bufPrint(&fallback_buf, "model.layers.{d}.layer_scalar", .{layer}) catch return error.NameTooLong;
            break :blk try getModelWeight(cb, config, fallback);
        },
        else => return err,
    };

    if (cb.kind() == .graph) {
        // Graph tracing: always emit the multiply. Avoid toFloat32 which
        // creates a spurious graph output and, via the fused_to_float32
        // pass-through, aliases the weight handle — causing use-after-free
        // when ExecutionResult.deinit frees the output.
        const result = try cb.multiply(hidden, scale_w);
        cb.free(scale_w);
        cb.free(hidden);
        return result;
    }

    if (cb.vtable.reshape2d != null) {
        // GPU path: keep the scalar multiply on device. Downloading the scalar
        // every layer to check for ~1.0 introduces synchronization in the hot
        // path and is more expensive than the multiply we would skip.
        const result = try cb.multiply(hidden, scale_w);
        cb.free(scale_w);
        cb.free(hidden);
        return result;
    }

    // CPU fallback (native — no GPU sync penalty).
    defer cb.free(scale_w);
    const scale_data = try cb.toFloat32(scale_w, allocator);
    defer allocator.free(scale_data);
    const scale_val = scale_data[0];

    if (std.math.approxEqAbs(f32, scale_val, 1.0, 1e-6)) return hidden;

    const data = try cb.toFloat32(hidden, allocator);
    defer allocator.free(data);
    for (data) |*v| v.* *= scale_val;

    const shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    const result = try cb.fromFloat32Shape(data, &shape);
    cb.free(hidden);
    return result;
}

// --- Per-Layer Embeddings (PLE) ---

/// Pre-compute combined PLE vectors for all layers at once.
/// Returns a CPU f32 array of [total * ple_hidden_size * num_hidden_layers], or null if PLE is disabled.
///
/// HF reference (Gemma4TextModel):
///   token_path  = embed_tokens_per_layer(input_ids) * sqrt(ple_dim)   # ScaledWordEmbedding
///   context_path = per_layer_model_projection(hidden) * (1/sqrt(hidden_size))
///   context_path = RMSNorm(context_path, per_layer_projection_norm)   # norm on ple_dim chunks
///   combined    = (context_path + token_path) * (1/sqrt(2))
pub fn computePleVectors(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    input_ids: []const i64,
    hidden: CT,
    total: usize,
) !?CT {
    if (!config.hasPle() or disablePleDebug()) return null;
    const ple_dim: usize = config.ple_hidden_size;
    const num_layers: usize = config.num_hidden_layers;
    const ple_total_dim: usize = ple_dim * num_layers;

    // Token-identity path: look up concatenated per-layer token embeddings,
    // then scale by sqrt(ple_dim) (Gemma4TextScaledWordEmbedding).
    const token_w = getModelWeight(cb, config, "model.per_layer_input.per_layer_token_embd.weight") catch |err| switch (err) {
        error.MissingWeight => try getModelWeight(cb, config, "model.embed_tokens_per_layer.weight"),
        else => return err,
    };
    defer cb.free(token_w);
    const token_embd_raw = try cb.embeddingLookup(token_w, input_ids, total, ple_total_dim);
    defer cb.free(token_embd_raw);

    // Context-aware path: project hidden → all-layer PLE space,
    // then scale by 1/sqrt(hidden_size).
    const proj_w = getModelWeight(cb, config, "model.per_layer_input.per_layer_model_proj.weight") catch |err| switch (err) {
        error.MissingWeight => try getModelWeight(cb, config, "model.per_layer_model_projection.weight"),
        else => return err,
    };
    defer cb.free(proj_w);
    const model_proj_raw = try cb.linearNoBias(hidden, proj_w, total, config.hidden_size, ple_total_dim);

    // RMSNorm the projection on ple_dim-sized chunks (before combining with token path).
    // Metal fused rms_norm requires weight size == last dim. Reshape
    // [total, ple_total_dim] → [total*num_layers, ple_dim] so weight [ple_dim] matches,
    // then reshape back after normalization.
    const proj_norm_base_w = getModelWeight(cb, config, "model.per_layer_input.per_layer_proj_norm.weight") catch |err| switch (err) {
        error.MissingWeight => try getModelWeight(cb, config, "model.per_layer_projection_norm.weight"),
        else => return err,
    };
    defer cb.free(proj_norm_base_w);
    const proj_norm_w = try maybeAdjustNormWeight(cb, allocator, config, proj_norm_base_w, ple_dim);
    defer if (proj_norm_w != proj_norm_base_w) cb.free(proj_norm_w);

    // GPU-native path: reshape → rmsNorm → reshape back → scale/combine on GPU.
    if (try cb.reshape2d(model_proj_raw, total * num_layers, ple_dim)) |reshaped| {
        defer cb.free(model_proj_raw);
        defer cb.free(reshaped);

        const normed_flat = try cb.rmsNorm(reshaped, proj_norm_w, ple_dim, config.norm_eps);
        defer cb.free(normed_flat);

        const normed_proj = (try cb.reshape2d(normed_flat, total, ple_total_dim)) orelse
            return error.ReshapeFailed;
        defer cb.free(normed_proj);

        // HF: (RMSNorm(proj) + embed * sqrt(ple_dim)) * (1/sqrt(2))
        // RMSNorm is scale-invariant so 1/sqrt(hidden) pre-norm factor washes out.
        const embed_scale_val = @sqrt(@as(f32, @floatFromInt(ple_dim)));
        const scale_shape = [_]i32{1};
        const embed_scale_ct = try cb.fromFloat32Shape(&[_]f32{embed_scale_val}, &scale_shape);
        defer cb.free(embed_scale_ct);

        const token_scaled = try cb.multiply(token_embd_raw, embed_scale_ct);
        defer cb.free(token_scaled);

        const combined = try cb.add(token_scaled, normed_proj);
        defer cb.free(combined);

        const combine_scale_val: f32 = 1.0 / @sqrt(2.0);
        const combine_ct = try cb.fromFloat32Shape(&[_]f32{combine_scale_val}, &scale_shape);
        defer cb.free(combine_ct);

        const result = try cb.multiply(combined, combine_ct);
        return result;
    }

    // CPU fallback (native path): rmsNorm handles chunked dim natively.
    defer cb.free(model_proj_raw);
    const normed_proj = try cb.rmsNorm(model_proj_raw, proj_norm_w, ple_dim, config.norm_eps);
    defer cb.free(normed_proj);

    const token_data = try cb.toFloat32(token_embd_raw, allocator);
    defer allocator.free(token_data);
    const proj_data = try cb.toFloat32(normed_proj, allocator);
    defer allocator.free(proj_data);

    const embed_scale = @sqrt(@as(f32, @floatFromInt(ple_dim)));
    const combine_scale: f32 = 1.0 / @sqrt(2.0);

    for (proj_data, 0..) |*v, i| {
        v.* = (token_data[i] * embed_scale + v.*) * combine_scale;
    }
    const result_shape = [_]i32{ @intCast(total), @intCast(ple_total_dim) };
    const result = try cb.fromFloat32Shape(proj_data, &result_shape);
    return result;
}

/// Apply PLE conditioning to a layer's hidden state.
/// ple_vectors is a CT of shape [total, ple_total_dim] from computePleVectors.
///
/// HF reference (Gemma4TextDecoderLayer):
///   gate    = act_fn(per_layer_input_gate(hidden))     # hidden_size → ple_dim, using config.hidden_activation
///   gated   = gate * per_layer_input                   # element-wise with PLE conditioning vector
///   proj    = per_layer_projection(gated)              # ple_dim → hidden_size
///   normed  = post_per_layer_input_norm(proj)          # RMSNorm on hidden_size
///   result  = hidden + normed                          # residual
pub fn applyPle(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    config: Config,
    hidden: CT,
    ple_vectors: CT,
    total: usize,
    layer: usize,
    buf: *[256]u8,
) !CT {
    const ple_dim: usize = config.ple_hidden_size;
    const hidden_size: usize = config.hidden_size;
    const ple_offset = layer * ple_dim;

    // 1. Slice this layer's PLE vectors on GPU: [total, ple_total_dim] → [total, ple_dim].
    const ple_ct = try cb.sliceLastDim(ple_vectors, ple_offset, ple_offset + ple_dim);
    defer cb.free(ple_ct);

    // 2. Gate: project hidden → ple_dim and apply the model's configured activation.
    const gate_name = std.fmt.bufPrint(buf, "model.layers.{d}.per_layer_input.inp_gate.weight", .{layer}) catch return error.NameTooLong;
    const gate_w = getModelWeight(cb, config, gate_name) catch |err| switch (err) {
        error.MissingWeight => blk: {
            var fallback_buf: [256]u8 = undefined;
            const fallback = std.fmt.bufPrint(&fallback_buf, "model.layers.{d}.per_layer_input_gate.weight", .{layer}) catch return error.NameTooLong;
            break :blk try getModelWeight(cb, config, fallback);
        },
        else => return err,
    };
    defer cb.free(gate_w);
    const gate_proj = try cb.linearNoBias(hidden, gate_w, total, hidden_size, ple_dim);
    defer cb.free(gate_proj);
    const gate = try applyActivation(cb, config, gate_proj);
    defer cb.free(gate);

    // 3. Element-wise multiply gate with PLE conditioning vector.
    const gated = try cb.multiply(gate, ple_ct);
    defer cb.free(gated);

    // 4. Project back to hidden_size.
    var proj_buf: [256]u8 = undefined;
    const proj_name = std.fmt.bufPrint(&proj_buf, "model.layers.{d}.per_layer_input.proj.weight", .{layer}) catch return error.NameTooLong;
    const proj_w = getModelWeight(cb, config, proj_name) catch |err| switch (err) {
        error.MissingWeight => blk: {
            var fallback_buf: [256]u8 = undefined;
            const fallback = std.fmt.bufPrint(&fallback_buf, "model.layers.{d}.per_layer_projection.weight", .{layer}) catch return error.NameTooLong;
            break :blk try getModelWeight(cb, config, fallback);
        },
        else => return err,
    };
    defer cb.free(proj_w);
    const projected = try cb.linearNoBias(gated, proj_w, total, ple_dim, hidden_size);
    defer cb.free(projected);

    // 5. Post-PLE RMSNorm on hidden_size.
    var norm_name_buf: [256]u8 = undefined;
    const norm_name = std.fmt.bufPrint(&norm_name_buf, "model.layers.{d}.per_layer_input.post_norm.weight", .{layer}) catch return error.NameTooLong;
    const post_norm_base_w = getModelWeight(cb, config, norm_name) catch |err| switch (err) {
        error.MissingWeight => blk: {
            var fallback_buf: [256]u8 = undefined;
            const fallback = std.fmt.bufPrint(&fallback_buf, "model.layers.{d}.post_per_layer_input_norm.weight", .{layer}) catch return error.NameTooLong;
            break :blk try getModelWeight(cb, config, fallback);
        },
        else => return err,
    };
    defer cb.free(post_norm_base_w);
    const post_norm_w = try maybeAdjustNormWeight(cb, allocator, config, post_norm_base_w, hidden_size);
    defer if (post_norm_w != post_norm_base_w) cb.free(post_norm_w);
    const normed = try cb.rmsNorm(projected, post_norm_w, hidden_size, config.norm_eps);
    defer cb.free(normed);

    // 6. Residual add.
    const result = try cb.add(hidden, normed);
    return result;
}

// --- Activation helper ---

// `.gelu_new` is the tanh-approx GELU variant used by GGUF's
// `gelu_pytorch_tanh` metadata.
fn applyGeluNew(cb: *const ComputeBackend, input: CT) !CT {
    return cb.geluNew(input);
}

fn scaleMoeRouterInput(
    cb: *const ComputeBackend,
    config: Config,
    input: CT,
    hidden_size: usize,
    layer: usize,
    name_buf: *[256]u8,
) !CT {
    const scaled = input;

    _ = hidden_size;

    const scale_name = std.fmt.bufPrint(name_buf, "model.layers.{d}.block_sparse_moe.gate.input_scale", .{layer}) catch return scaled;
    const scale_w = getModelWeight(cb, config, scale_name) catch return scaled;
    defer cb.free(scale_w);
    return cb.multiply(scaled, scale_w);
}

pub fn applyActivation(cb: *const ComputeBackend, config: Config, input: CT) !CT {
    return switch (config.activation) {
        .gelu => cb.gelu(input),
        .gelu_new => applyGeluNew(cb, input),
        .silu => cb.silu(input),
        .relu => cb.relu(input),
        .relu_squared => applyReluSquared(cb, input),
    };
}

pub fn decoderRuntimeActivationKind(activation: ActivationType) ops.DecoderRuntimeActivationKind {
    return switch (activation) {
        .gelu => .gelu,
        .gelu_new => .gelu_new,
        .silu => .silu,
        .relu => .relu,
        .relu_squared => .relu_squared,
    };
}

fn applyReluSquared(cb: *const ComputeBackend, input: CT) !CT {
    const relu = try cb.relu(input);
    defer cb.free(relu);
    return cb.multiply(relu, relu);
}

// --- Embedding weight helper ---

test "selectTopExperts picks correct top-k from 8 experts" {
    const allocator = std.testing.allocator;
    const logits = [_]f32{ 1.0, 3.0, 0.5, 2.0, 4.0, 0.1, 0.2, 1.5 };
    const sel = try selectTopExperts(allocator, &logits, 2);
    try std.testing.expectEqual(@as(usize, 2), sel.count);
    // top-1 should be index 4 (value 4.0), top-2 should be index 1 (value 3.0)
    try std.testing.expectEqual(@as(u32, 4), sel.indices[0]);
    try std.testing.expectEqual(@as(u32, 1), sel.indices[1]);
    // weights should be normalized to sum to 1.0
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sel.weights[0] + sel.weights[1], 1e-5);
}

test "selectTopExperts handles 128 experts with top_k=8" {
    const allocator = std.testing.allocator;
    var logits: [128]f32 = undefined;
    for (&logits, 0..) |*v, i| {
        v.* = @floatFromInt(i);
    }
    // Highest values: 127, 126, 125, ..., 120
    const sel = try selectTopExperts(allocator, &logits, 8);
    try std.testing.expectEqual(@as(usize, 8), sel.count);
    try std.testing.expectEqual(@as(u32, 127), sel.indices[0]);
    try std.testing.expectEqual(@as(u32, 126), sel.indices[1]);
    try std.testing.expectEqual(@as(u32, 120), sel.indices[7]);
    // weights should sum to 1.0
    var sum: f32 = 0.0;
    for (sel.weights[0..8]) |w| sum += w;
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), sum, 1e-5);
}

pub fn getEmbeddingWeight(cb: *const ComputeBackend, config: Config) !CT {
    return switch (config.family) {
        .gpt2 => cb.getWeight("wte.weight"),
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .gemma, .bitnet, .phi => getModelWeight(cb, config, "model.embed_tokens.weight"),
        else => getModelWeight(cb, config, "model.embed_tokens.weight") catch try cb.getWeight("wte.weight"),
    };
}
