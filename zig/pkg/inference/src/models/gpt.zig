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

// GPT/decoder-only model configuration.
//
// Supports GPT-2, GPT-Neo, GPT-NeoX, GPT-J, LLaMA, Mistral, Phi, Qwen2,
// Gemma, BitNet, Falcon, OPT, BLOOM, and other causal language models.
//
// These models vary in attention mechanism (MHA, GQA, MQA),
// position encoding (absolute, RoPE, ALiBi), normalization (LayerNorm, RMSNorm),
// and activation function (GELU, SiLU, ReLU²). The config captures these
// differences.

const std = @import("std");
const gguf_metadata = @import("../gguf/metadata.zig");

pub const ModelFamily = enum {
    gpt2,
    gpt_neo,
    gpt_neox,
    gptj,
    llama,
    mistral,
    phi,
    qwen2,
    qwen3,
    qwen3_5,
    deepseek_v4,
    gemma,
    bitnet,
    falcon,
    opt,
    bloom,
    other,
};

pub const NormType = enum {
    layer_norm,
    rms_norm,
};

pub const PositionEncoding = enum {
    absolute, // GPT-2, OPT
    rope, // LLaMA, Mistral, Qwen2
    alibi, // BLOOM, Falcon (some)
};

pub const RopeLayout = enum {
    half_split,
    consecutive_pairs,
};

pub const ActivationType = enum {
    gelu,
    gelu_new, // GPT-2 variant
    silu, // LLaMA, Mistral
    relu,
    relu_squared, // BitNet
};

pub const DeepseekV4ScoringFunc = enum {
    unknown,
    sqrtsoftplus,
    softmax,
    sigmoid,
};

pub const DeepseekV4AttentionKind = enum {
    unknown,
    sliding_attention,
    compressed_sparse_attention,
    heavily_compressed_attention,
};

pub const DeepseekV4MlpKind = enum {
    unknown,
    hash_moe,
    moe,
};

pub const deepseek_v4_max_layers = 256;

pub const Config = struct {
    family: ModelFamily = .gpt2,
    hidden_size: u32 = 768,
    num_hidden_layers: u32 = 12,
    num_attention_heads: u32 = 12,
    num_key_value_heads: u32 = 0, // 0 = same as num_attention_heads (MHA); < num_attention_heads = GQA
    attention_head_dim: u32 = 0, // 0 = derive from hidden_size / num_attention_heads
    intermediate_size: u32 = 3072,
    vocab_size: u32 = 50257,
    max_position_embeddings: u32 = 1024,
    sliding_window: u32 = 0,
    num_local_experts: u32 = 0,
    num_experts_per_tok: u32 = 0,
    num_shared_experts: u32 = 0,
    shared_expert_intermediate_size: u32 = 0,
    expert_intermediate_size: u32 = 0, // 0 = same as intermediate_size; per-expert FFN hidden dim (GGUF: expert_feed_forward_length)

    // DeepSeek V4: value-only architecture metadata from HF configs.
    deepseek_v4_q_lora_rank: u32 = 0,
    deepseek_v4_kv_lora_rank: u32 = 0,
    deepseek_v4_qk_rope_head_dim: u32 = 0,
    deepseek_v4_routed_scaling_factor: f32 = 0.0,
    deepseek_v4_norm_topk_prob: bool = false,
    deepseek_v4_scoring_func: DeepseekV4ScoringFunc = .unknown,
    deepseek_v4_attention_bias: bool = false,
    deepseek_v4_compress_rate_csa: u32 = 0,
    deepseek_v4_compress_rate_hca: u32 = 0,
    deepseek_v4_compress_rope_theta: f32 = 0.0,
    deepseek_v4_hc_mult: u32 = 0,
    deepseek_v4_hc_sinkhorn_iters: u32 = 0,
    deepseek_v4_hc_eps: f32 = 0.0,
    deepseek_v4_swiglu_limit: f32 = 0.0,
    deepseek_v4_o_groups: u32 = 0,
    deepseek_v4_o_lora_rank: u32 = 0,
    deepseek_v4_index_n_heads: u32 = 0,
    deepseek_v4_index_head_dim: u32 = 0,
    deepseek_v4_index_topk: u32 = 0,
    deepseek_v4_num_nextn_predict_layers: u32 = 0,
    deepseek_v4_hash_moe_layers: u32 = 0,
    deepseek_v4_moe_layers: u32 = 0,
    deepseek_v4_sliding_attention_layers: u32 = 0,
    deepseek_v4_compressed_sparse_attention_layers: u32 = 0,
    deepseek_v4_heavily_compressed_attention_layers: u32 = 0,
    deepseek_v4_attention_schedule_len: u32 = 0,
    deepseek_v4_attention_schedule: [deepseek_v4_max_layers]DeepseekV4AttentionKind = [_]DeepseekV4AttentionKind{.unknown} ** deepseek_v4_max_layers,
    deepseek_v4_mlp_schedule_len: u32 = 0,
    deepseek_v4_mlp_schedule: [deepseek_v4_max_layers]DeepseekV4MlpKind = [_]DeepseekV4MlpKind{.unknown} ** deepseek_v4_max_layers,
    deepseek_v4_original_seq_len: u32 = 0,
    deepseek_v4_rope_factor: f32 = 0.0,
    deepseek_v4_beta_fast: f32 = 0.0,
    deepseek_v4_beta_slow: f32 = 0.0,

    // Gemma 4: shared KV cache and per-layer GQA.
    num_kv_shared_layers: u32 = 0,
    global_head_dim: u32 = 0,
    num_global_key_value_heads: u32 = 0,
    shared_layer_intermediate_size: u32 = 0, // 0 = same as intermediate_size

    // Gemma 4: Per-Layer Embeddings (PLE).
    ple_hidden_size: u32 = 0, // 0 = disabled; hidden_size_per_layer_input in HF config

    // Gemma 4 MTP assistants. These checkpoints are not standalone decoder
    // models: attention projects Q only and reads K/V from the target model
    // cache, while pre/post projections bridge target and assistant widths.
    gemma4_mtp_assistant: bool = false,
    mtp_backbone_hidden_size: u32 = 0,
    mtp_num_centroids: u32 = 0,
    mtp_centroid_intermediate_top_k: u32 = 0,
    mtp_use_ordered_embeddings: bool = false,
    mtp_kv_sliding_donor_layer: u32 = std.math.maxInt(u32),
    mtp_kv_full_donor_layer: u32 = std.math.maxInt(u32),

    // Architecture choices
    norm_type: NormType = .layer_norm,
    position_encoding: PositionEncoding = .absolute,
    activation: ActivationType = .gelu,
    norm_eps: f32 = 1e-5,

    // Token IDs
    bos_token_id: i32 = -1,
    eos_token_id: i32 = -1,
    pad_token_id: i32 = -1,
    image_token_index: i32 = -1,
    boi_token_index: i32 = -1,
    eoi_token_index: i32 = -1,
    mm_tokens_per_image: u32 = 0,

    // Optional multimodal vision metadata.
    vision_hidden_size: u32 = 0,
    vision_num_hidden_layers: u32 = 0,
    vision_num_attention_heads: u32 = 0,
    vision_intermediate_size: u32 = 0,
    vision_image_size: u32 = 0,
    vision_patch_size: u32 = 0,
    vision_embed_dim: u32 = 0,
    vision_mlp_ratio: u32 = 0,
    vision_spatial_merge_size: u32 = 1,
    vision_temporal_patch_size: u32 = 1,
    vision_use_quick_gelu: bool = false,

    // Qwen3.5 hybrid attention metadata. Qwen3.5 alternates linear-attention
    // layers with full-attention layers; linear layers require recurrent state
    // rather than the standard KV cache.
    qwen35_has_linear_attention: bool = false,
    qwen35_full_attention_interval: u32 = 0,
    qwen35_linear_conv_kernel_dim: u32 = 0,
    qwen35_linear_key_head_dim: u32 = 0,
    qwen35_linear_value_head_dim: u32 = 0,
    qwen35_linear_num_key_heads: u32 = 0,
    qwen35_linear_num_value_heads: u32 = 0,
    qwen35_attn_output_gate: bool = false,
    qwen35_mrope_interleaved: bool = false,
    qwen35_mrope_section: [3]u32 = .{ 0, 0, 0 },

    // RoPE parameters
    rope_theta: f32 = 10000.0,
    rope_local_theta: f32 = 10000.0,
    rope_freq_scale: f32 = 1.0,
    rope_layout: RopeLayout = .half_split,
    sliding_window_pattern: u32 = 6,
    rope_partial_factor: f32 = 1.0, // Gemma 4 full attention: 0.25 (only rotate 25% of head_dim)
    rope_dim_override: u32 = 0, // When >0, overrides rope_dim for all layers (from rope_freqs.weight)

    // Optional model-specific decode semantics.
    norm_weight_offset: f32 = 0.0,
    final_logit_softcapping: f32 = 0.0,

    // Weight tying: when true, lm_head reuses embedding weights (no separate lm_head.weight tensor).
    weight_tying: bool = false,

    // Weight naming
    weight_prefix: []const u8 = "",

    pub fn effectiveKVHeads(self: Config) u32 {
        return if (self.num_key_value_heads > 0) self.num_key_value_heads else self.num_attention_heads;
    }

    pub fn headDim(self: Config) u32 {
        return if (self.attention_head_dim > 0) self.attention_head_dim else self.hidden_size / self.num_attention_heads;
    }

    pub fn intermediateSize(self: Config, layer_index: usize) u32 {
        if (self.gemma4_mtp_assistant) return self.intermediate_size;
        if (self.shared_layer_intermediate_size > 0 and self.layerUsesSharedTail(layer_index)) {
            return self.shared_layer_intermediate_size;
        }
        return self.intermediate_size;
    }

    pub fn expertIntermediateSize(self: Config) u32 {
        return if (self.expert_intermediate_size > 0) self.expert_intermediate_size else self.intermediate_size;
    }

    pub fn usesMoe(self: Config) bool {
        return self.num_local_experts > 0 and self.num_experts_per_tok > 0;
    }

    pub fn hasSharedExpert(self: Config) bool {
        return self.num_shared_experts > 0;
    }

    pub fn isMultimodal(self: Config) bool {
        return self.image_token_index >= 0 and (self.mm_tokens_per_image > 0 or self.vision_patch_size > 0 or self.vision_embed_dim > 0);
    }

    pub fn supportsNativeQwen2VlVision(self: Config) bool {
        return (self.family == .qwen2 or self.family == .qwen3_5) and
            self.isMultimodal() and
            self.vision_patch_size > 0 and
            self.vision_embed_dim > 0 and
            self.vision_num_hidden_layers > 0 and
            self.vision_num_attention_heads > 0 and
            self.vision_spatial_merge_size > 0 and
            self.hidden_size > 0;
    }

    pub fn tokenEmbeddingScale(self: Config) f32 {
        return switch (self.family) {
            .gemma => @sqrt(@as(f32, @floatFromInt(self.hidden_size))),
            else => 1.0,
        };
    }

    pub fn usesGemmaSlidingAttention(self: Config) bool {
        return self.family == .gemma and self.sliding_window > 0;
    }

    pub fn isQwen35(self: Config) bool {
        return self.family == .qwen3_5;
    }

    pub fn layerUsesQwen35LinearAttention(self: Config, layer_index: usize) bool {
        if (!self.isQwen35() or !self.qwen35_has_linear_attention) return false;
        const interval = if (self.qwen35_full_attention_interval > 0) self.qwen35_full_attention_interval else 4;
        return ((layer_index + 1) % interval) != 0;
    }

    pub fn hasPle(self: Config) bool {
        return self.ple_hidden_size > 0;
    }

    pub fn layerUsesSlidingAttention(self: Config, layer_index: usize) bool {
        if (self.family == .deepseek_v4) {
            return self.deepseekV4AttentionKind(layer_index) == .sliding_attention;
        }
        if (!self.usesGemmaSlidingAttention()) return false;
        return ((layer_index + 1) % self.sliding_window_pattern) != 0;
    }

    pub fn layerRopeTheta(self: Config, layer_index: usize) f32 {
        if (self.family == .deepseek_v4 and
            self.deepseekV4AttentionKind(layer_index) != .sliding_attention and
            self.deepseek_v4_compress_rope_theta > 0.0)
        {
            return self.deepseek_v4_compress_rope_theta;
        }
        if (!self.usesGemmaSlidingAttention()) return self.rope_theta;
        return if (self.layerUsesSlidingAttention(layer_index)) self.rope_local_theta else self.rope_theta;
    }

    /// Number of dimensions to apply RoPE to in each head.
    /// Full attention layers may use partial_rotary_factor < 1.0, rotating only a fraction of dims.
    /// Returns head_dim for full rotation, or a smaller even number for partial.
    pub fn layerRopeDim(self: Config, layer_index: usize) u32 {
        const hd = self.effectiveHeadDimForLayer(layer_index);
        if (self.family == .deepseek_v4 and self.deepseek_v4_qk_rope_head_dim > 0) {
            return @min(hd, self.deepseek_v4_qk_rope_head_dim);
        }
        if (self.rope_partial_factor >= 1.0) return hd;
        if (self.usesGemmaSlidingAttention() and self.layerUsesSlidingAttention(layer_index)) return hd;
        if (self.isQwen35() and self.layerUsesQwen35LinearAttention(layer_index)) return hd;
        if (!self.usesGemmaSlidingAttention() and !self.isQwen35()) return hd;
        const raw: u32 = @intFromFloat(@as(f32, @floatFromInt(hd)) * self.rope_partial_factor);
        return raw & ~@as(u32, 1);
    }

    /// Number of dimensions to actually rotate.
    ///
    /// Gemma 4 GGUF exports `rope_freqs.weight` for the full-attention path,
    /// where only a prefix of the head participates in rotary embedding. Sliding
    /// attention keeps its full local rotary width, so the override only applies
    /// on non-sliding layers.
    ///
    /// When rope_dim_override > 0, only the first N dims are rotated, but the
    /// frequency formula still uses the full layerRopeDim() for correct spacing.
    pub fn layerRopeActiveDim(self: Config, layer_index: usize) u32 {
        const rope_dim = self.layerRopeDim(layer_index);
        if (self.rope_dim_override > 0 and self.usesGemmaSlidingAttention() and !self.layerUsesSlidingAttention(layer_index)) {
            return @min(rope_dim, self.rope_dim_override);
        }
        return rope_dim;
    }

    /// Dimension used in the RoPE frequency denominator.
    ///
    /// Gemma 4 proportional RoPE rotates only a prefix of full-attention heads,
    /// but the frequencies remain proportional to the full head dimension.
    pub fn layerRopeFrequencyDim(self: Config, layer_index: usize) u32 {
        if (self.usesGemmaSlidingAttention() and
            !self.layerUsesSlidingAttention(layer_index) and
            self.rope_partial_factor < 1.0)
        {
            return self.effectiveHeadDimForLayer(layer_index);
        }
        return self.layerRopeDim(layer_index);
    }

    pub fn layerUsesSharedTail(self: Config, layer_index: usize) bool {
        if (self.gemma4_mtp_assistant) return layer_index < self.num_hidden_layers;
        if (self.num_kv_shared_layers == 0) return false;
        return layer_index >= self.num_hidden_layers - self.num_kv_shared_layers;
    }

    pub fn layerSharesKv(self: Config, layer_index: usize) bool {
        return self.layerUsesSharedTail(layer_index);
    }

    pub fn kvDonorLayerIndex(self: Config, layer_index: usize) ?usize {
        if (self.gemma4_mtp_assistant) {
            const unset = std.math.maxInt(u32);
            if (self.mtp_kv_sliding_donor_layer == unset or self.mtp_kv_full_donor_layer == unset) return layer_index;
            return if (self.layerUsesSlidingAttention(layer_index))
                self.mtp_kv_sliding_donor_layer
            else
                self.mtp_kv_full_donor_layer;
        }
        if (!self.layerSharesKv(layer_index)) return null;
        const num_non_shared = self.num_hidden_layers - self.num_kv_shared_layers;
        // HF mapping: find the LAST non-shared layer of the same attention type.
        // This means all shared sliding layers share KV from one donor,
        // and all shared full-attention layers share KV from another donor.
        const is_sliding = self.layerUsesSlidingAttention(layer_index);
        var donor: usize = 0;
        for (0..num_non_shared) |i| {
            if (self.layerUsesSlidingAttention(i) == is_sliding) {
                donor = i;
            }
        }
        return donor;
    }

    /// Gemma 4 full-attention layers may omit v_proj (V = K).
    pub fn layerOmitsVProj(self: Config, layer_index: usize) bool {
        if (self.global_head_dim == 0) return false;
        if (!self.usesGemmaSlidingAttention()) return false;
        return !self.layerUsesSlidingAttention(layer_index);
    }

    pub fn effectiveKVHeadsForLayer(self: Config, layer_index: usize) u32 {
        if (self.num_global_key_value_heads > 0 and self.usesGemmaSlidingAttention()) {
            if (!self.layerUsesSlidingAttention(layer_index)) return self.num_global_key_value_heads;
        }
        return self.effectiveKVHeads();
    }

    pub fn effectiveHeadDimForLayer(self: Config, layer_index: usize) u32 {
        if (self.global_head_dim > 0 and self.usesGemmaSlidingAttention()) {
            if (!self.layerUsesSlidingAttention(layer_index)) return self.global_head_dim;
        }
        return self.headDim();
    }

    /// Maximum num_kv_heads across all layer types (for KV pool allocation).
    pub fn maxKvHeads(self: Config) u32 {
        if (self.family == .deepseek_v4) return 1;
        if (self.num_global_key_value_heads > 0) return @max(self.effectiveKVHeads(), self.num_global_key_value_heads);
        return self.effectiveKVHeads();
    }

    /// Maximum head_dim across all layer types (for KV pool allocation).
    pub fn maxHeadDim(self: Config) u32 {
        if (self.family == .deepseek_v4) return @intCast(self.deepseekV4MaxKvCacheWidth());
        if (self.global_head_dim > 0) return @max(self.headDim(), self.global_head_dim);
        return self.headDim();
    }

    pub fn maxKvWidthPerToken(self: Config) usize {
        if (self.family == .deepseek_v4) return self.deepseekV4MaxKvCacheWidth();
        const sliding_width = @as(usize, self.effectiveKVHeads()) * self.headDim();
        if (self.num_global_key_value_heads == 0 and self.global_head_dim == 0) return sliding_width;
        const global_kv_heads: usize = if (self.num_global_key_value_heads > 0) self.num_global_key_value_heads else self.effectiveKVHeads();
        const global_dim: usize = if (self.global_head_dim > 0) self.global_head_dim else self.headDim();
        const global_width = global_kv_heads * global_dim;
        return @max(sliding_width, global_width);
    }

    pub fn deepseekV4CompressRateForLayer(self: Config, layer_index: usize) u32 {
        return switch (self.deepseekV4AttentionKind(layer_index)) {
            .sliding_attention, .unknown => 0,
            .compressed_sparse_attention => self.deepseek_v4_compress_rate_csa,
            .heavily_compressed_attention => self.deepseek_v4_compress_rate_hca,
        };
    }

    pub fn deepseekV4KvLoraRank(self: Config) usize {
        if (self.deepseek_v4_kv_lora_rank > 0) return self.deepseek_v4_kv_lora_rank;
        const hd = self.headDim();
        const rope = self.deepseek_v4_qk_rope_head_dim;
        if (hd > rope) return hd - rope;
        return 0;
    }

    pub fn deepseekV4KvCacheWidthForLayer(self: Config, layer_index: usize) usize {
        _ = layer_index;
        if (self.family != .deepseek_v4) return @as(usize, self.effectiveKVHeads()) * self.headDim();
        const rope = self.deepseek_v4_qk_rope_head_dim;
        const kv_lora = self.deepseekV4KvLoraRank();
        const width = kv_lora + rope;
        if (width > 0) return width;
        return @as(usize, self.effectiveKVHeads()) * self.headDim();
    }

    pub fn deepseekV4MaxKvCacheWidth(self: Config) usize {
        if (self.family != .deepseek_v4) return self.maxKvWidthPerToken();
        var max_width: usize = 0;
        const layer_count: usize = @intCast(self.num_hidden_layers);
        for (0..layer_count) |layer| {
            max_width = @max(max_width, self.deepseekV4KvCacheWidthForLayer(layer));
        }
        if (max_width > 0) return max_width;
        return @as(usize, self.effectiveKVHeads()) * self.headDim();
    }

    pub fn deepseekV4AttentionKind(self: Config, layer_index: usize) DeepseekV4AttentionKind {
        if (layer_index < self.deepseek_v4_attention_schedule_len and layer_index < deepseek_v4_max_layers) {
            const kind = self.deepseek_v4_attention_schedule[layer_index];
            if (kind != .unknown) return kind;
        }
        return .unknown;
    }

    pub fn deepseekV4MlpKind(self: Config, layer_index: usize) DeepseekV4MlpKind {
        if (layer_index < self.deepseek_v4_mlp_schedule_len and layer_index < deepseek_v4_max_layers) {
            const kind = self.deepseek_v4_mlp_schedule[layer_index];
            if (kind != .unknown) return kind;
        }
        if (layer_index < self.deepseek_v4_hash_moe_layers) return .hash_moe;
        return .moe;
    }
};

pub fn parseConfig(allocator: std.mem.Allocator, json_bytes: []const u8) !Config {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const obj = parsed.value.object;
    const has_vlm_wrapper = if (obj.get("vlm_config")) |value| value == .object else false;
    const model_obj = blk: {
        if (obj.get("vlm_config")) |value| {
            if (value == .object) break :blk value.object;
        }
        break :blk obj;
    };
    const text_obj = blk: {
        const nested_candidates = [_][]const u8{
            "text_config",
            "phi_config",
            "llm_config",
            "language_config",
            "decoder_config",
        };
        inline for (nested_candidates) |key| {
            if (model_obj.get(key)) |value| {
                if (value == .object) break :blk value.object;
            }
        }
        break :blk model_obj;
    };
    var config = Config{};
    if (has_vlm_wrapper) config.weight_prefix = "vlm.model.language_model";
    if (!has_vlm_wrapper) {
        if (obj.get("model_type")) |v| {
            if (v == .string and obj.get("text_config") != null) {
                if (std.mem.eql(u8, v.string, "gemma4") or std.mem.eql(u8, v.string, "qwen3_5")) {
                    config.weight_prefix = "model.language_model";
                }
            }
        }
    }

    // Detect family from model_type
    if (model_obj.get("model_type")) |v| {
        if (v == .string) {
            config.gemma4_mtp_assistant = std.mem.eql(u8, v.string, "gemma4_assistant");
            config.family = detectFamily(v.string);
            applyFamilyDefaults(&config);
            if (isGemma4ModelType(v.string)) config.norm_weight_offset = 0.0;
        }
    } else if (obj.get("model_type")) |v| {
        if (v == .string) {
            config.gemma4_mtp_assistant = std.mem.eql(u8, v.string, "gemma4_assistant");
            config.family = detectFamily(v.string);
            applyFamilyDefaults(&config);
            if (isGemma4ModelType(v.string)) config.norm_weight_offset = 0.0;
        }
    }

    if (config.gemma4_mtp_assistant) {
        if (model_obj.get("backbone_hidden_size")) |v| if (jsonU32(v)) |val| {
            config.mtp_backbone_hidden_size = val;
        };
        if (model_obj.get("num_centroids")) |v| if (jsonU32(v)) |val| {
            config.mtp_num_centroids = val;
        };
        if (model_obj.get("centroid_intermediate_top_k")) |v| if (jsonU32(v)) |val| {
            config.mtp_centroid_intermediate_top_k = val;
        };
        if (model_obj.get("use_ordered_embeddings")) |v| if (jsonBool(v)) |val| {
            config.mtp_use_ordered_embeddings = val;
        };
    }

    if (text_obj.get("hidden_size")) |v| if (jsonU32(v)) |val| {
        config.hidden_size = val;
    };
    if (text_obj.get("n_embd")) |v| if (jsonU32(v)) |val| {
        config.hidden_size = val;
    };
    if (text_obj.get("num_hidden_layers")) |v| if (jsonU32(v)) |val| {
        config.num_hidden_layers = val;
    };
    if (text_obj.get("n_layer")) |v| if (jsonU32(v)) |val| {
        config.num_hidden_layers = val;
    };
    if (text_obj.get("num_attention_heads")) |v| if (jsonU32(v)) |val| {
        config.num_attention_heads = val;
    };
    if (text_obj.get("head_dim")) |v| if (jsonU32(v)) |val| {
        config.attention_head_dim = val;
    };
    if (text_obj.get("linear_conv_kernel_dim")) |v| if (jsonU32(v)) |val| {
        config.qwen35_linear_conv_kernel_dim = val;
    };
    if (text_obj.get("linear_key_head_dim")) |v| if (jsonU32(v)) |val| {
        config.qwen35_linear_key_head_dim = val;
    };
    if (text_obj.get("linear_value_head_dim")) |v| if (jsonU32(v)) |val| {
        config.qwen35_linear_value_head_dim = val;
    };
    if (text_obj.get("linear_num_key_heads")) |v| if (jsonU32(v)) |val| {
        config.qwen35_linear_num_key_heads = val;
    };
    if (text_obj.get("linear_num_value_heads")) |v| if (jsonU32(v)) |val| {
        config.qwen35_linear_num_value_heads = val;
    };
    if (text_obj.get("attn_output_gate")) |v| if (jsonBool(v)) |val| {
        config.qwen35_attn_output_gate = val;
    };
    if (text_obj.get("n_head")) |v| if (jsonU32(v)) |val| {
        config.num_attention_heads = val;
    };
    if (text_obj.get("num_key_value_heads")) |v| if (jsonU32(v)) |val| {
        config.num_key_value_heads = val;
    };
    if (text_obj.get("intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.intermediate_size = val;
    };
    if (text_obj.get("n_inner")) |v| if (jsonU32(v)) |val| {
        config.intermediate_size = val;
    };
    if (text_obj.get("moe_intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.expert_intermediate_size = val;
        if (config.shared_expert_intermediate_size == 0) config.shared_expert_intermediate_size = val;
    };
    if (text_obj.get("hidden_activation") orelse text_obj.get("hidden_act")) |v| {
        if (v == .string) {
            if (std.mem.eql(u8, v.string, "gelu_pytorch_tanh")) {
                config.activation = .gelu_new;
            } else if (std.mem.eql(u8, v.string, "gelu")) {
                config.activation = .gelu;
            } else if (std.mem.eql(u8, v.string, "silu")) {
                config.activation = .silu;
            } else if (std.mem.eql(u8, v.string, "relu")) {
                config.activation = .relu;
            } else if (std.mem.eql(u8, v.string, "relu2") or std.mem.eql(u8, v.string, "relu_squared")) {
                config.activation = .relu_squared;
            }
        }
    }
    if (text_obj.get("vocab_size")) |v| if (jsonU32(v)) |val| {
        config.vocab_size = val;
    } else if (model_obj.get("vocab_size")) |model_vocab| if (jsonU32(model_vocab)) |val| {
        config.vocab_size = val;
    };
    if (text_obj.get("max_position_embeddings")) |v| if (jsonU32(v)) |val| {
        config.max_position_embeddings = val;
    };
    if (text_obj.get("n_positions")) |v| if (jsonU32(v)) |val| {
        config.max_position_embeddings = val;
    };
    if (text_obj.get("sliding_window")) |v| if (jsonU32(v)) |val| {
        config.sliding_window = val;
    };
    if (obj.get("num_local_experts")) |v| if (jsonU32(v)) |val| {
        config.num_local_experts = val;
    };
    if (text_obj.get("num_local_experts")) |v| if (jsonU32(v)) |val| {
        config.num_local_experts = val;
    };
    if (obj.get("n_routed_experts")) |v| if (jsonU32(v)) |val| {
        config.num_local_experts = val;
    };
    if (text_obj.get("n_routed_experts")) |v| if (jsonU32(v)) |val| {
        config.num_local_experts = val;
    };
    if (obj.get("num_experts_per_tok")) |v| if (jsonU32(v)) |val| {
        config.num_experts_per_tok = val;
    };
    if (text_obj.get("num_experts_per_tok")) |v| if (jsonU32(v)) |val| {
        config.num_experts_per_tok = val;
    };
    if (obj.get("num_shared_experts")) |v| if (jsonU32(v)) |val| {
        config.num_shared_experts = val;
    };
    if (text_obj.get("num_shared_experts")) |v| if (jsonU32(v)) |val| {
        config.num_shared_experts = val;
    };
    if (obj.get("n_shared_experts")) |v| if (jsonU32(v)) |val| {
        config.num_shared_experts = val;
    };
    if (text_obj.get("n_shared_experts")) |v| if (jsonU32(v)) |val| {
        config.num_shared_experts = val;
    };
    if (obj.get("shared_expert_intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.shared_expert_intermediate_size = val;
    };
    if (text_obj.get("shared_expert_intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.shared_expert_intermediate_size = val;
    };
    if (obj.get("expert_intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.expert_intermediate_size = val;
    };
    if (text_obj.get("expert_intermediate_size")) |v| if (jsonU32(v)) |val| {
        config.expert_intermediate_size = val;
    };

    if (config.family == .deepseek_v4) {
        if (text_obj.get("q_lora_rank")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_q_lora_rank = val;
        };
        if (text_obj.get("kv_lora_rank")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_kv_lora_rank = val;
        };
        if (text_obj.get("qk_rope_head_dim")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_qk_rope_head_dim = val;
        };
        if (text_obj.get("rope_head_dim")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_qk_rope_head_dim = val;
        };
        if (text_obj.get("scoring_func")) |v| if (v == .string) {
            config.deepseek_v4_scoring_func = parseDeepseekV4ScoringFunc(v.string);
        };
        if (text_obj.get("score_func")) |v| if (v == .string) {
            config.deepseek_v4_scoring_func = parseDeepseekV4ScoringFunc(v.string);
        };
        if (text_obj.get("norm_topk_prob")) |v| if (jsonBool(v)) |val| {
            config.deepseek_v4_norm_topk_prob = val;
        };
        if (text_obj.get("routed_scaling_factor")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_routed_scaling_factor = val;
        };
        if (text_obj.get("route_scale")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_routed_scaling_factor = val;
        };
        if (text_obj.get("attention_bias")) |v| if (jsonBool(v)) |val| {
            config.deepseek_v4_attention_bias = val;
        };
        if (text_obj.get("compress_rope_theta")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_compress_rope_theta = val;
        };
        if (text_obj.get("compress_rate_csa")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_compress_rate_csa = val;
        };
        if (text_obj.get("compress_rate_hca")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_compress_rate_hca = val;
        };
        if (text_obj.get("compress_rates")) |v| {
            if (v == .object) {
                if (v.object.get("compressed_sparse_attention")) |rate| if (jsonU32(rate)) |val| {
                    config.deepseek_v4_compress_rate_csa = val;
                };
                if (v.object.get("heavily_compressed_attention")) |rate| if (jsonU32(rate)) |val| {
                    config.deepseek_v4_compress_rate_hca = val;
                };
                if (v.object.get("csa")) |rate| if (jsonU32(rate)) |val| {
                    config.deepseek_v4_compress_rate_csa = val;
                };
                if (v.object.get("hca")) |rate| if (jsonU32(rate)) |val| {
                    config.deepseek_v4_compress_rate_hca = val;
                };
            }
        }
        if (text_obj.get("hc_mult")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_hc_mult = val;
        };
        if (text_obj.get("hc_sinkhorn_iters")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_hc_sinkhorn_iters = val;
        };
        if (text_obj.get("hc_eps")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_hc_eps = val;
        };
        if (text_obj.get("swiglu_limit")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_swiglu_limit = val;
        };
        if (text_obj.get("o_groups")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_o_groups = val;
        };
        if (text_obj.get("o_lora_rank")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_o_lora_rank = val;
        };
        if (text_obj.get("index_n_heads")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_index_n_heads = val;
        };
        if (text_obj.get("index_head_dim")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_index_head_dim = val;
        };
        if (text_obj.get("index_topk")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_index_topk = val;
        };
        if (text_obj.get("num_nextn_predict_layers")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_num_nextn_predict_layers = val;
        };
        if (text_obj.get("num_hash_layers")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_hash_moe_layers = val;
        };
        if (text_obj.get("layer_types")) |v| {
            parseDeepseekV4LayerTypes(&config, v);
        }
        if (text_obj.get("compress_ratios")) |v| {
            parseDeepseekV4CompressRatios(&config, v);
        }
        if (text_obj.get("mlp_layer_types")) |v| {
            parseDeepseekV4MlpLayerTypes(&config, v);
        }
    }

    // Gemma 4: shared KV cache and per-layer GQA.
    if (text_obj.get("num_kv_shared_layers")) |v| if (jsonU32(v)) |val| {
        config.num_kv_shared_layers = val;
    };
    if (text_obj.get("global_head_dim")) |v| if (jsonU32(v)) |val| {
        config.global_head_dim = val;
    };
    if (text_obj.get("num_global_key_value_heads")) |v| if (jsonU32(v)) |val| {
        config.num_global_key_value_heads = val;
    };
    if (config.family == .gemma and config.num_kv_shared_layers > 0 and config.shared_layer_intermediate_size == 0) {
        config.shared_layer_intermediate_size = config.intermediate_size * 2;
    }

    // Gemma 4: Per-Layer Embeddings (PLE).
    if (text_obj.get("hidden_size_per_layer_input")) |v| if (jsonU32(v)) |val| {
        config.ple_hidden_size = val;
    };

    if (text_obj.get("bos_token_id")) |v| if (jsonI32(v)) |val| {
        config.bos_token_id = val;
    } else if (obj.get("bos_token_id")) |root_v| if (jsonI32(root_v)) |val| {
        config.bos_token_id = val;
    };
    if (text_obj.get("eos_token_id")) |v| if (jsonI32(v)) |val| {
        config.eos_token_id = val;
    };
    if (text_obj.get("pad_token_id")) |v| if (jsonI32(v)) |val| {
        config.pad_token_id = val;
    };
    if (text_obj.get("tie_word_embeddings")) |v| if (jsonBool(v)) |val| {
        config.weight_tying = val;
    };
    if (model_obj.get("image_token_index")) |v| if (jsonI32(v)) |val| {
        config.image_token_index = val;
    };
    if (model_obj.get("image_token_id")) |v| if (jsonI32(v)) |val| {
        config.image_token_index = val;
    };
    if (model_obj.get("boi_token_index")) |v| if (jsonI32(v)) |val| {
        config.boi_token_index = val;
    };
    if (model_obj.get("vision_start_token_id")) |v| if (jsonI32(v)) |val| {
        config.boi_token_index = val;
    };
    if (model_obj.get("eoi_token_index")) |v| if (jsonI32(v)) |val| {
        config.eoi_token_index = val;
    };
    if (model_obj.get("vision_end_token_id")) |v| if (jsonI32(v)) |val| {
        config.eoi_token_index = val;
    };
    if (model_obj.get("mm_tokens_per_image")) |v| if (jsonU32(v)) |val| {
        config.mm_tokens_per_image = val;
    };
    if (model_obj.get("image_seq_length")) |v| if (jsonU32(v)) |val| {
        config.mm_tokens_per_image = val;
    };

    if (text_obj.get("rms_norm_eps")) |v| if (jsonF32(v)) |val| {
        config.norm_eps = val;
    };
    if (text_obj.get("layer_norm_epsilon")) |v| if (jsonF32(v)) |val| {
        config.norm_eps = val;
    };
    if (text_obj.get("layer_norm_eps")) |v| if (jsonF32(v)) |val| {
        config.norm_eps = val;
    };

    var has_explicit_rope_theta = false;
    if (text_obj.get("rope_theta")) |v| if (jsonF32(v)) |val| {
        config.rope_theta = val;
        has_explicit_rope_theta = true;
    };
    if (text_obj.get("partial_rotary_factor")) |v| if (jsonF32(v)) |val| {
        config.rope_partial_factor = val;
    };
    if (text_obj.get("rope_parameters")) |v| {
        if (v == .object) {
            if (v.object.get("rope_theta")) |theta_value| if (jsonF32(theta_value)) |theta| {
                config.rope_theta = theta;
                has_explicit_rope_theta = true;
            };
            if (v.object.get("partial_rotary_factor")) |prf| if (jsonF32(prf)) |val| {
                config.rope_partial_factor = val;
            };
            if (v.object.get("mrope_interleaved")) |mrope| if (jsonBool(mrope)) |val| {
                config.qwen35_mrope_interleaved = val;
            };
            if (v.object.get("mrope_section")) |section| if (parseU32Triple(section)) |val| {
                config.qwen35_mrope_section = val;
            };
            if (v.object.get("full_attention")) |full_value| {
                if (full_value == .object) {
                    if (full_value.object.get("rope_theta")) |theta_value| if (jsonF32(theta_value)) |theta| {
                        config.rope_theta = theta;
                        has_explicit_rope_theta = true;
                    };
                    if (full_value.object.get("partial_rotary_factor")) |prf| if (jsonF32(prf)) |val| {
                        config.rope_partial_factor = val;
                    };
                }
            }
            if (v.object.get("sliding_attention")) |sliding_value| {
                if (sliding_value == .object) {
                    if (sliding_value.object.get("rope_theta")) |theta_value| if (jsonF32(theta_value)) |theta| {
                        config.rope_local_theta = theta;
                    };
                }
            }
        }
    }
    if (text_obj.get("rope_scaling")) |v| {
        if (v == .object) {
            const rope_obj = v.object;
            if (rope_obj.get("rope_type")) |rope_type| {
                if (rope_type == .string and std.mem.eql(u8, rope_type.string, "linear")) {
                    if (rope_obj.get("factor")) |factor_value| if (jsonF32(factor_value)) |factor| {
                        if (factor > 0.0 and config.family != .gemma) config.rope_freq_scale = 1.0 / factor;
                    };
                }
            }
            if (config.family == .deepseek_v4) {
                if (rope_obj.get("factor")) |factor_value| if (jsonF32(factor_value)) |factor| {
                    config.deepseek_v4_rope_factor = factor;
                };
                if (rope_obj.get("original_max_position_embeddings")) |original| if (jsonU32(original)) |val| {
                    config.deepseek_v4_original_seq_len = val;
                };
                if (rope_obj.get("beta_fast")) |beta| if (jsonF32(beta)) |val| {
                    config.deepseek_v4_beta_fast = val;
                };
                if (rope_obj.get("beta_slow")) |beta| if (jsonF32(beta)) |val| {
                    config.deepseek_v4_beta_slow = val;
                };
            }
        }
    }
    if (config.family == .deepseek_v4) {
        if (text_obj.get("original_seq_len")) |v| if (jsonU32(v)) |val| {
            config.deepseek_v4_original_seq_len = val;
        };
        if (text_obj.get("rope_factor")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_rope_factor = val;
        };
        if (text_obj.get("beta_fast")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_beta_fast = val;
        };
        if (text_obj.get("beta_slow")) |v| if (jsonF32(v)) |val| {
            config.deepseek_v4_beta_slow = val;
        };
    }
    if (text_obj.get("sliding_window_pattern")) |v| if (jsonU32(v)) |val| {
        config.sliding_window_pattern = val;
    };
    if (text_obj.get("_sliding_window_pattern")) |v| if (jsonU32(v)) |val| {
        config.sliding_window_pattern = val;
    };
    // Gemma 4: derive sliding_window_pattern from layer_types array if present.
    if (text_obj.get("layer_types")) |v| {
        if (v == .array) {
            var has_linear = false;
            for (v.array.items, 0..) |item, i| {
                if (item == .string and std.mem.eql(u8, item.string, "linear_attention")) {
                    has_linear = true;
                }
                if (item == .string and std.mem.eql(u8, item.string, "full_attention")) {
                    config.sliding_window_pattern = @intCast(i + 1);
                    if (config.family == .qwen3_5 and config.qwen35_full_attention_interval == 0) {
                        config.qwen35_full_attention_interval = @intCast(i + 1);
                    }
                    break;
                }
            }
            if (config.family == .qwen3_5) config.qwen35_has_linear_attention = has_linear;
        }
    }
    if (text_obj.get("full_attention_interval")) |v| if (jsonU32(v)) |val| {
        if (config.family == .qwen3_5) {
            config.qwen35_full_attention_interval = val;
            config.qwen35_has_linear_attention = val > 1;
        }
    };
    if (text_obj.get("final_logit_softcapping")) |v| if (jsonF32(v)) |val| {
        config.final_logit_softcapping = val;
    };
    if (obj.get("final_logit_softcapping")) |v| if (jsonF32(v)) |val| {
        config.final_logit_softcapping = val;
    };

    if (model_obj.get("vision_config")) |v| {
        if (v == .object) {
            const vision_obj = v.object;
            if (vision_obj.get("hidden_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_hidden_size = val;
            };
            if (vision_obj.get("embed_dim")) |vv| if (jsonU32(vv)) |val| {
                config.vision_embed_dim = val;
            };
            if (vision_obj.get("hidden_size")) |vv| if (jsonU32(vv)) |val| {
                if (config.vision_embed_dim == 0) config.vision_embed_dim = val;
            };
            if (vision_obj.get("num_hidden_layers")) |vv| if (jsonU32(vv)) |val| {
                config.vision_num_hidden_layers = val;
            };
            if (vision_obj.get("depth")) |vv| if (jsonU32(vv)) |val| {
                config.vision_num_hidden_layers = val;
            };
            if (vision_obj.get("num_attention_heads")) |vv| if (jsonU32(vv)) |val| {
                config.vision_num_attention_heads = val;
            };
            if (vision_obj.get("num_heads")) |vv| if (jsonU32(vv)) |val| {
                config.vision_num_attention_heads = val;
            };
            if (vision_obj.get("intermediate_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_intermediate_size = val;
            };
            if (vision_obj.get("mlp_ratio")) |vv| if (jsonU32(vv)) |val| {
                config.vision_mlp_ratio = val;
            };
            if (vision_obj.get("image_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_image_size = val;
            };
            if (vision_obj.get("patch_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_patch_size = val;
            };
            if (vision_obj.get("spatial_patch_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_patch_size = val;
            };
            if (vision_obj.get("spatial_merge_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_spatial_merge_size = val;
            };
            if (vision_obj.get("temporal_patch_size")) |vv| if (jsonU32(vv)) |val| {
                config.vision_temporal_patch_size = val;
            };
            if (vision_obj.get("hidden_act")) |vv| if (vv == .string) {
                config.vision_use_quick_gelu = std.mem.eql(u8, vv.string, "quick_gelu");
            };
        }
    }

    if (config.family == .gemma and config.sliding_window > 0 and !has_explicit_rope_theta) {
        config.rope_theta = 1_000_000.0;
    }

    return config;
}

pub fn parseGgufMetadata(view: gguf_metadata.View) ?Config {
    const arch = view.getString("general.architecture") orelse return null;
    if (!isGenerativeModel(arch)) return null;

    var config = Config{
        .family = detectFamily(arch),
    };
    applyFamilyDefaults(&config);
    if (config.family == .gemma) {
        // llama.cpp's Gemma/Gemma3 GGUF conversion already bakes the RMSNorm
        // +1 shift into *.norm.weight tensors, so native runtime should not
        // apply the HF safetensors-time offset again.
        config.norm_weight_offset = 0.0;
    }

    var key_buf: [96]u8 = undefined;

    if (metaU32(view, &key_buf, arch, "embedding_length")) |value| config.hidden_size = value;
    if (metaU32(view, &key_buf, arch, "vocab_size")) |value| config.vocab_size = value;
    if (metaU32(view, &key_buf, arch, "block_count")) |value| config.num_hidden_layers = value;
    if (metaU32(view, &key_buf, arch, "attention.head_count")) |value| config.num_attention_heads = value;
    if (metaU32(view, &key_buf, arch, "attention.head_count_kv")) |value| {
        config.num_key_value_heads = value;
    } else if (metaI32FromArrayAt(view, &key_buf, arch, "attention.head_count_kv", 0)) |first| {
        // Per-layer KV head counts (Gemma 4): sliding layers have more KV heads,
        // full-attention layers have fewer. First element = sliding layer value.
        config.num_key_value_heads = first;
    }
    if (metaU32(view, &key_buf, arch, "attention.key_length")) |value| config.attention_head_dim = value;
    if (config.attention_head_dim == 0) {
        if (metaU32(view, &key_buf, arch, "attention.value_length")) |value| config.attention_head_dim = value;
    }
    // Gemma 4: key_length is the global (full_attention) head_dim, key_length_swa is the sliding one.
    if (metaU32(view, &key_buf, arch, "attention.key_length_swa")) |swa_value| {
        config.global_head_dim = config.attention_head_dim; // key_length = global
        config.attention_head_dim = swa_value; // key_length_swa = sliding (base)
    }
    if (metaU32(view, &key_buf, arch, "feed_forward_length")) |value| {
        config.intermediate_size = value;
    } else if (metaFirstI32FromArray(view, &key_buf, arch, "feed_forward_length")) |value| {
        // feed_forward_length is a per-layer array; use first element as base.
        config.intermediate_size = value;
    }
    // Check for a second distinct FFN size in the per-layer array (shared KV layers may differ).
    const ffn_last_idx: usize = if (config.num_hidden_layers > 0) config.num_hidden_layers - 1 else 0;
    const ffn_last = metaI32FromArrayAt(view, &key_buf, arch, "feed_forward_length", ffn_last_idx);
    if (ffn_last) |last| {
        if (last != config.intermediate_size) config.shared_layer_intermediate_size = last;
    }
    if (metaU32(view, &key_buf, arch, "context_length")) |value| config.max_position_embeddings = value;
    if (metaU32(view, &key_buf, arch, "attention.sliding_window")) |value| config.sliding_window = value;
    if (metaU32(view, &key_buf, arch, "sliding_window")) |value| config.sliding_window = value;
    if (metaU32(view, &key_buf, arch, "expert_count")) |value| config.num_local_experts = value;
    if (metaU32(view, &key_buf, arch, "expert_used_count")) |value| config.num_experts_per_tok = value;
    if (metaU32(view, &key_buf, arch, "expert_shared_count")) |value| config.num_shared_experts = value;
    if (metaU32(view, &key_buf, arch, "expert_feed_forward_length")) |value| config.expert_intermediate_size = value;
    // Gemma 4 MoE: the GGUF may omit expert_shared_count but still have a shared (dense) FFN
    // sublayer alongside the routed experts. Detect by checking for expert_count > 0 + Gemma family.
    if (config.family == .gemma and config.num_local_experts > 0 and config.num_shared_experts == 0) {
        config.num_shared_experts = 1;
    }
    if (metaF32(view, &key_buf, arch, "rope.freq_base")) |value| config.rope_theta = value;
    if (metaF32(view, &key_buf, arch, "rope.scaling.factor")) |value| {
        if (value > 0.0 and config.family != .gemma) config.rope_freq_scale = 1.0 / value;
    }
    if (metaF32(view, &key_buf, arch, "attention.layer_norm_rms_epsilon")) |value| config.norm_eps = value;
    if (metaF32(view, &key_buf, arch, "layer_norm_rms_epsilon")) |value| config.norm_eps = value;
    if (metaF32(view, &key_buf, arch, "final_logit_softcapping")) |value| config.final_logit_softcapping = value;

    if (metaF32(view, &key_buf, arch, "rope.freq_base_swa")) |value| config.rope_local_theta = value;

    // Gemma 4: partial rotary for full attention layers.
    // rope.dimension_count gives the number of dims to rotate for full-attn layers.
    // Derive partial_rotary_factor from it: factor = rope_dim / head_dim.
    if (metaU32(view, &key_buf, arch, "rope.dimension_count")) |rope_dim| {
        if (config.global_head_dim > 0 and rope_dim < config.global_head_dim) {
            config.rope_partial_factor = @as(f32, @floatFromInt(rope_dim)) / @as(f32, @floatFromInt(config.global_head_dim));
        } else if (config.attention_head_dim > 0 and rope_dim < config.attention_head_dim) {
            config.rope_partial_factor = @as(f32, @floatFromInt(rope_dim)) / @as(f32, @floatFromInt(config.attention_head_dim));
        }
    }

    // Gemma 4: shared KV cache and per-layer GQA.
    if (metaU32(view, &key_buf, arch, "attention.kv_shared_layer_count")) |value| config.num_kv_shared_layers = value;
    if (metaU32(view, &key_buf, arch, "attention.shared_kv_layers")) |value| config.num_kv_shared_layers = value;
    if (metaU32(view, &key_buf, arch, "attention.global_head_dim")) |value| config.global_head_dim = value;
    if (metaU32(view, &key_buf, arch, "attention.global_head_count_kv")) |value| config.num_global_key_value_heads = value;

    // Gemma 4: Per-Layer Embeddings (PLE).
    if (metaU32(view, &key_buf, arch, "embedding_length_per_layer_input")) |value| config.ple_hidden_size = value;

    // Gemma 4: sliding_window_pattern may be a per-layer bool array.
    if (metaSlidingPatternFromBoolArray(view, &key_buf, arch, "attention.sliding_window_pattern")) |value| config.sliding_window_pattern = value;

    // Gemma 4: derive global KV head count from per-layer head_count_kv array if not set explicitly.
    // Full-attention layers (at sliding_window_pattern-1) may have different KV heads than sliding layers.
    if (config.num_global_key_value_heads == 0 and config.sliding_window_pattern > 0) {
        const full_attn_idx = config.sliding_window_pattern - 1;
        if (metaI32FromArrayAt(view, &key_buf, arch, "attention.head_count_kv", full_attn_idx)) |global_kv| {
            if (global_kv != config.num_key_value_heads) {
                config.num_global_key_value_heads = global_kv;
            }
        }
    }

    return config;
}

fn metaU32(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8) ?u32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    const value = view.getU64(key) orelse return null;
    return @intCast(value);
}

fn metaF32(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8) ?f32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    return view.getF32(key);
}

/// Read element at index from an i32/u32 metadata array.
fn metaI32FromArrayAt(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8, index: usize) ?u32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    const entry = view.find(key) orelse return null;
    switch (entry.value) {
        .array => |arr| {
            if (index >= arr.values.len) return null;
            switch (arr.values[index]) {
                .i32 => |v| return if (v > 0) @intCast(v) else null,
                .u32 => |v| return v,
                else => return null,
            }
        },
        else => return null,
    }
}

/// Read the first element of an i32 metadata array as u32 (for per-layer arrays like feed_forward_length).
fn metaFirstI32FromArray(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8) ?u32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    const entry = view.find(key) orelse return null;
    switch (entry.value) {
        .array => |arr| {
            if (arr.values.len == 0) return null;
            switch (arr.values[0]) {
                .i32 => |v| return if (v > 0) @intCast(v) else null,
                .u32 => |v| return v,
                else => return null,
            }
        },
        else => return null,
    }
}

/// Derive sliding_window_pattern from a per-layer bool array.
/// In GGUF, true = sliding attention, false = full attention.
/// Returns the position+1 of the first full_attention (false) layer.
fn metaSlidingPatternFromBoolArray(view: gguf_metadata.View, buf: *[96]u8, arch: []const u8, suffix: []const u8) ?u32 {
    const key = std.fmt.bufPrint(buf, "{s}.{s}", .{ arch, suffix }) catch return null;
    const entry = view.find(key) orelse return null;
    switch (entry.value) {
        .array => |arr| {
            for (arr.values, 0..) |val, i| {
                switch (val) {
                    .bool_ => |is_sliding| {
                        if (!is_sliding) return @intCast(i + 1);
                    },
                    else => return null,
                }
            }
            return null; // No full_attention layer found
        },
        else => return null,
    }
}

fn detectFamily(model_type: []const u8) ModelFamily {
    const families = .{
        .{ "gpt2", ModelFamily.gpt2 },
        .{ "gpt_neo", ModelFamily.gpt_neo },
        .{ "gpt_neox", ModelFamily.gpt_neox },
        .{ "gptj", ModelFamily.gptj },
        .{ "llama", ModelFamily.llama },
        .{ "mistral", ModelFamily.mistral },
        .{ "mixtral", ModelFamily.mistral },
        .{ "phi", ModelFamily.phi },
        .{ "phi3", ModelFamily.phi },
        .{ "qwen2", ModelFamily.qwen2 },
        .{ "qwen2_vl", ModelFamily.qwen2 },
        .{ "colqwen2", ModelFamily.qwen2 },
        .{ "qwen3", ModelFamily.qwen3 },
        .{ "jina_embeddings_v5", ModelFamily.qwen3 },
        .{ "qwen3_5", ModelFamily.qwen3_5 },
        .{ "qwen3_5_text", ModelFamily.qwen3_5 },
        .{ "deepseek_v4", ModelFamily.deepseek_v4 },
        .{ "deepseek_v4_text", ModelFamily.deepseek_v4 },
        .{ "deepseek_v4_flash", ModelFamily.deepseek_v4 },
        .{ "deepseek_v4_flash_base", ModelFamily.deepseek_v4 },
        .{ "deepseek_v4_pro", ModelFamily.deepseek_v4 },
        .{ "deepseek_v4_pro_base", ModelFamily.deepseek_v4 },
        .{ "deepseek-v4", ModelFamily.deepseek_v4 },
        .{ "deepseek-v4-flash", ModelFamily.deepseek_v4 },
        .{ "deepseek-v4-flash-base", ModelFamily.deepseek_v4 },
        .{ "deepseek-v4-pro", ModelFamily.deepseek_v4 },
        .{ "deepseek-v4-pro-base", ModelFamily.deepseek_v4 },
        .{ "deepseekv4", ModelFamily.deepseek_v4 },
        .{ "gemma", ModelFamily.gemma },
        .{ "gemma2", ModelFamily.gemma },
        .{ "gemma3", ModelFamily.gemma },
        .{ "gemma3_text", ModelFamily.gemma },
        .{ "gemma4", ModelFamily.gemma },
        .{ "gemma4_text", ModelFamily.gemma },
        .{ "gemma4_assistant", ModelFamily.gemma },
        .{ "bitnet", ModelFamily.bitnet },
        .{ "bitnet-b1.58", ModelFamily.bitnet },
        .{ "falcon", ModelFamily.falcon },
        .{ "opt", ModelFamily.opt },
        .{ "bloom", ModelFamily.bloom },
    };
    inline for (families) |pair| {
        if (std.mem.eql(u8, model_type, pair[0])) return pair[1];
    }
    return .other;
}

fn isGemma4ModelType(model_type: []const u8) bool {
    return std.mem.eql(u8, model_type, "gemma4") or
        std.mem.eql(u8, model_type, "gemma4_text") or
        std.mem.eql(u8, model_type, "gemma4_assistant");
}

fn applyFamilyDefaults(config: *Config) void {
    switch (config.family) {
        .llama, .mistral, .qwen2, .qwen3, .qwen3_5, .deepseek_v4 => {
            config.norm_type = .rms_norm;
            config.position_encoding = .rope;
            config.activation = .silu;
            config.norm_eps = switch (config.family) {
                .qwen3, .qwen3_5, .deepseek_v4 => 1e-6,
                else => 1e-5,
            };
            config.rope_layout = switch (config.family) {
                .llama, .mistral => .consecutive_pairs,
                .qwen2, .qwen3, .qwen3_5 => .half_split,
                .deepseek_v4 => .consecutive_pairs,
                else => config.rope_layout,
            };
            if (config.family == .qwen3_5) {
                config.norm_weight_offset = 1.0;
                config.rope_partial_factor = 0.25;
                config.qwen35_full_attention_interval = 4;
                config.qwen35_has_linear_attention = true;
                config.qwen35_attn_output_gate = true;
            }
            if (config.family == .deepseek_v4) {
                config.deepseek_v4_scoring_func = .sqrtsoftplus;
                config.deepseek_v4_norm_topk_prob = true;
                config.deepseek_v4_routed_scaling_factor = 1.5;
                config.deepseek_v4_compress_rate_csa = 4;
                config.deepseek_v4_compress_rate_hca = 128;
                config.deepseek_v4_compress_rope_theta = 160_000.0;
                config.deepseek_v4_hc_mult = 4;
                config.deepseek_v4_hc_sinkhorn_iters = 20;
                config.deepseek_v4_hc_eps = 1e-6;
                config.deepseek_v4_swiglu_limit = 10.0;
                config.deepseek_v4_o_groups = 8;
                config.deepseek_v4_index_head_dim = 128;
            }
        },
        .gemma => {
            config.norm_type = .rms_norm;
            config.position_encoding = .rope;
            config.activation = .gelu_new;
            config.norm_eps = 1e-6;
            config.norm_weight_offset = 1.0;
            config.rope_layout = .half_split;
        },
        .bitnet => {
            config.norm_type = .rms_norm;
            config.position_encoding = .rope;
            config.activation = .relu_squared;
            config.norm_eps = 1e-5;
            config.rope_layout = .half_split;
            config.weight_tying = true;
        },
        .phi => {
            config.norm_type = .layer_norm;
            config.position_encoding = .rope;
            config.activation = .gelu_new;
            config.rope_layout = .half_split;
        },
        .bloom => {
            config.position_encoding = .alibi;
        },
        .falcon => {
            config.norm_type = .layer_norm;
        },
        .gpt2, .gpt_neo, .gpt_neox, .gptj => {
            config.activation = .gelu_new;
        },
        .opt => {
            config.activation = .relu;
        },
        .other => {},
    }
}

/// Detect if a model_type string is a decoder-only generative model.
pub fn isGenerativeModel(model_type: []const u8) bool {
    if (std.mem.eql(u8, model_type, "jina_embeddings_v5")) return false;
    return detectFamily(model_type) != .other;
}

fn parseDeepseekV4ScoringFunc(value: []const u8) DeepseekV4ScoringFunc {
    if (std.mem.eql(u8, value, "sqrtsoftplus")) return .sqrtsoftplus;
    if (std.mem.eql(u8, value, "softmax")) return .softmax;
    if (std.mem.eql(u8, value, "sigmoid")) return .sigmoid;
    return .unknown;
}

fn parseDeepseekV4LayerTypes(config: *Config, value: std.json.Value) void {
    if (value != .array) return;
    config.deepseek_v4_sliding_attention_layers = 0;
    config.deepseek_v4_compressed_sparse_attention_layers = 0;
    config.deepseek_v4_heavily_compressed_attention_layers = 0;
    config.deepseek_v4_attention_schedule_len = @intCast(@min(value.array.items.len, deepseek_v4_max_layers));
    for (value.array.items, 0..) |item, idx| {
        if (item != .string) continue;
        if (std.mem.eql(u8, item.string, "sliding_attention")) {
            config.deepseek_v4_sliding_attention_layers += 1;
            if (idx < deepseek_v4_max_layers) config.deepseek_v4_attention_schedule[idx] = .sliding_attention;
        } else if (std.mem.eql(u8, item.string, "compressed_sparse_attention")) {
            config.deepseek_v4_compressed_sparse_attention_layers += 1;
            if (idx < deepseek_v4_max_layers) config.deepseek_v4_attention_schedule[idx] = .compressed_sparse_attention;
        } else if (std.mem.eql(u8, item.string, "heavily_compressed_attention")) {
            config.deepseek_v4_heavily_compressed_attention_layers += 1;
            if (idx < deepseek_v4_max_layers) config.deepseek_v4_attention_schedule[idx] = .heavily_compressed_attention;
        }
    }
}

fn parseDeepseekV4CompressRatios(config: *Config, value: std.json.Value) void {
    if (value != .array) return;
    config.deepseek_v4_sliding_attention_layers = 0;
    config.deepseek_v4_compressed_sparse_attention_layers = 0;
    config.deepseek_v4_heavily_compressed_attention_layers = 0;
    const schedule_len = @min(@min(value.array.items.len, deepseek_v4_max_layers), @as(usize, @intCast(config.num_hidden_layers)));
    config.deepseek_v4_attention_schedule_len = @intCast(schedule_len);
    for (value.array.items, 0..) |item, idx| {
        if (idx >= schedule_len) break;
        const ratio = jsonU32(item) orelse continue;
        if (ratio == 0) {
            config.deepseek_v4_sliding_attention_layers += 1;
            config.deepseek_v4_attention_schedule[idx] = .sliding_attention;
        } else if (config.deepseek_v4_compress_rate_csa > 0 and ratio == config.deepseek_v4_compress_rate_csa) {
            config.deepseek_v4_compressed_sparse_attention_layers += 1;
            config.deepseek_v4_attention_schedule[idx] = .compressed_sparse_attention;
        } else if (config.deepseek_v4_compress_rate_hca > 0 and ratio == config.deepseek_v4_compress_rate_hca) {
            config.deepseek_v4_heavily_compressed_attention_layers += 1;
            config.deepseek_v4_attention_schedule[idx] = .heavily_compressed_attention;
        }
    }
}

fn parseDeepseekV4MlpLayerTypes(config: *Config, value: std.json.Value) void {
    if (value != .array) return;
    config.deepseek_v4_hash_moe_layers = 0;
    config.deepseek_v4_moe_layers = 0;
    config.deepseek_v4_mlp_schedule_len = @intCast(@min(value.array.items.len, deepseek_v4_max_layers));
    for (value.array.items, 0..) |item, idx| {
        if (item != .string) continue;
        if (std.mem.eql(u8, item.string, "hash_moe")) {
            config.deepseek_v4_hash_moe_layers += 1;
            if (idx < deepseek_v4_max_layers) config.deepseek_v4_mlp_schedule[idx] = .hash_moe;
        } else if (std.mem.eql(u8, item.string, "moe")) {
            config.deepseek_v4_moe_layers += 1;
            if (idx < deepseek_v4_max_layers) config.deepseek_v4_mlp_schedule[idx] = .moe;
        }
    }
}

fn jsonU32(val: std.json.Value) ?u32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn parseU32Triple(v: std.json.Value) ?[3]u32 {
    if (v != .array or v.array.items.len != 3) return null;
    var result: [3]u32 = undefined;
    for (v.array.items, 0..) |item, idx| {
        result[idx] = jsonU32(item) orelse return null;
    }
    return result;
}

fn jsonI32(val: std.json.Value) ?i32 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn jsonBool(val: std.json.Value) ?bool {
    return switch (val) {
        .bool => |value| value,
        else => null,
    };
}

fn jsonF32(val: std.json.Value) ?f32 {
    return switch (val) {
        .float => |f| @floatCast(f),
        .integer => |i| @floatFromInt(i),
        else => null,
    };
}

// -- Tests --

test "parse gpt2 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "gpt2", "n_embd": 768, "n_layer": 12, "n_head": 12, "vocab_size": 50257}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gpt2, config.family);
    try std.testing.expectEqual(@as(u32, 768), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 12), config.num_hidden_layers);
    try std.testing.expectEqual(ActivationType.gelu_new, config.activation);
}

test "parse llama config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "llama", "hidden_size": 4096, "num_hidden_layers": 32, "num_attention_heads": 32, "num_key_value_heads": 8, "intermediate_size": 11008, "vocab_size": 32000, "rms_norm_eps": 1e-05}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.llama, config.family);
    try std.testing.expectEqual(@as(u32, 4096), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 8), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 128), config.headDim());
    try std.testing.expectEqual(NormType.rms_norm, config.norm_type);
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
    try std.testing.expectEqual(ActivationType.silu, config.activation);
}

test "parse bitnet config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "bitnet",
        \\  "hidden_act": "relu2",
        \\  "hidden_size": 2560,
        \\  "intermediate_size": 6912,
        \\  "max_position_embeddings": 4096,
        \\  "num_attention_heads": 20,
        \\  "num_hidden_layers": 30,
        \\  "num_key_value_heads": 5,
        \\  "rms_norm_eps": 0.00001,
        \\  "rope_theta": 500000.0,
        \\  "tie_word_embeddings": true,
        \\  "vocab_size": 128256
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.bitnet, config.family);
    try std.testing.expectEqual(@as(u32, 2560), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 30), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 20), config.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 5), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 128), config.headDim());
    try std.testing.expectEqual(@as(u32, 6912), config.intermediate_size);
    try std.testing.expectEqual(@as(u32, 4096), config.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 128256), config.vocab_size);
    try std.testing.expectEqual(NormType.rms_norm, config.norm_type);
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
    try std.testing.expectEqual(ActivationType.relu_squared, config.activation);
    try std.testing.expectApproxEqAbs(@as(f32, 500000.0), config.rope_theta, 1e-3);
    try std.testing.expect(config.weight_tying);
}

test "parse mistral config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "mistral", "hidden_size": 4096, "num_key_value_heads": 8}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.mistral, config.family);
    try std.testing.expectEqual(@as(u32, 8), config.effectiveKVHeads());
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
}

test "parse jina embeddings v5 config as qwen3 backbone" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "jina_embeddings_v5",
        \\  "hidden_size": 1024,
        \\  "num_hidden_layers": 28,
        \\  "num_attention_heads": 16,
        \\  "num_key_value_heads": 8,
        \\  "head_dim": 128,
        \\  "intermediate_size": 3072,
        \\  "max_position_embeddings": 32768,
        \\  "rms_norm_eps": 1e-06,
        \\  "rope_theta": 3500000,
        \\  "tie_word_embeddings": true,
        \\  "vocab_size": 151936
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.qwen3, config.family);
    try std.testing.expectEqual(@as(u32, 1024), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 28), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 8), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 128), config.headDim());
    try std.testing.expectEqual(NormType.rms_norm, config.norm_type);
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
    try std.testing.expectApproxEqAbs(@as(f32, 3_500_000.0), config.rope_theta, 1.0);
    try std.testing.expect(config.weight_tying);
}

test "parse gemma3 multimodal config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma3",
        \\  "image_token_index": 262144,
        \\  "boi_token_index": 255999,
        \\  "eoi_token_index": 256000,
        \\  "mm_tokens_per_image": 256,
        \\  "text_config": {
        \\    "hidden_size": 2560,
        \\    "num_hidden_layers": 34,
        \\    "num_attention_heads": 8,
        \\    "intermediate_size": 10240
        \\  },
        \\  "vision_config": {
        \\    "hidden_size": 1152,
        \\    "num_hidden_layers": 27,
        \\    "num_attention_heads": 16,
        \\    "intermediate_size": 4304,
        \\    "image_size": 896,
        \\    "patch_size": 14
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectEqual(@as(i32, 262144), config.image_token_index);
    try std.testing.expectEqual(@as(i32, 255999), config.boi_token_index);
    try std.testing.expectEqual(@as(i32, 256000), config.eoi_token_index);
    try std.testing.expectEqual(@as(u32, 256), config.mm_tokens_per_image);
    try std.testing.expectEqual(@as(u32, 1152), config.vision_hidden_size);
    try std.testing.expectEqual(@as(u32, 27), config.vision_num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 16), config.vision_num_attention_heads);
    try std.testing.expectEqual(@as(u32, 4304), config.vision_intermediate_size);
    try std.testing.expectEqual(@as(u32, 896), config.vision_image_size);
    try std.testing.expectEqual(@as(u32, 14), config.vision_patch_size);
    try std.testing.expect(config.isMultimodal());
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.norm_weight_offset, 1e-6);
}

test "parse gemma3 sliding rope defaults" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma3",
        \\  "text_config": {
        \\    "hidden_size": 2560,
        \\    "num_hidden_layers": 34,
        \\    "num_attention_heads": 8,
        \\    "intermediate_size": 10240,
        \\    "sliding_window": 1024
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectApproxEqAbs(@as(f32, 1_000_000.0), config.rope_theta, 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 10_000.0), config.rope_local_theta, 1e-3);
    try std.testing.expect(config.layerUsesSlidingAttention(0));
    try std.testing.expect(config.layerUsesSlidingAttention(4));
    try std.testing.expect(!config.layerUsesSlidingAttention(5));
    try std.testing.expectApproxEqAbs(@as(f32, 10_000.0), config.layerRopeTheta(0), 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 1_000_000.0), config.layerRopeTheta(5), 1e-3);
}

test "parse functiongemma gemma3_text config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma3_text",
        \\  "hidden_size": 640,
        \\  "num_hidden_layers": 18,
        \\  "num_attention_heads": 4,
        \\  "num_key_value_heads": 1,
        \\  "intermediate_size": 2048,
        \\  "vocab_size": 262144,
        \\  "sliding_window": 512,
        \\  "rope_theta": 1000000.0,
        \\  "rms_norm_eps": 0.000001
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectEqual(@as(u32, 18), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 4), config.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 1), config.num_key_value_heads);
}

test "parse gemma3 ignores generic linear rope freq scaling factor" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma3",
        \\  "text_config": {
        \\    "hidden_size": 2560,
        \\    "num_hidden_layers": 34,
        \\    "num_attention_heads": 8,
        \\    "intermediate_size": 10240,
        \\    "sliding_window": 1024,
        \\    "rope_scaling": {
        \\      "rope_type": "linear",
        \\      "factor": 8.0
        \\    }
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.rope_freq_scale, 1e-6);
}

test "parse mixtral config" {
    const allocator = std.testing.allocator;
    const json =
        \\{"model_type": "mixtral", "hidden_size": 4096, "num_hidden_layers": 32, "num_attention_heads": 32, "num_key_value_heads": 8, "intermediate_size": 14336, "num_local_experts": 8, "num_experts_per_tok": 2, "sliding_window": 4096}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.mistral, config.family);
    try std.testing.expectEqual(@as(u32, 8), config.num_local_experts);
    try std.testing.expectEqual(@as(u32, 2), config.num_experts_per_tok);
    try std.testing.expectEqual(@as(u32, 4096), config.sliding_window);
    try std.testing.expect(config.usesMoe());
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
}

test "parse deepseek v4 config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "deepseek_v4",
        \\  "vocab_size": 129280,
        \\  "hidden_size": 4096,
        \\  "moe_intermediate_size": 2048,
        \\  "num_hidden_layers": 6,
        \\  "num_attention_heads": 64,
        \\  "num_key_value_heads": 1,
        \\  "head_dim": 512,
        \\  "q_lora_rank": 1024,
        \\  "kv_lora_rank": 448,
        \\  "qk_rope_head_dim": 64,
        \\  "num_experts_per_tok": 6,
        \\  "n_routed_experts": 256,
        \\  "n_shared_experts": 1,
        \\  "scoring_func": "sqrtsoftplus",
        \\  "norm_topk_prob": true,
        \\  "attention_bias": false,
        \\  "routed_scaling_factor": 1.5,
        \\  "max_position_embeddings": 1048576,
        \\  "rope_theta": 10000,
        \\  "partial_rotary_factor": 0.25,
        \\  "rope_scaling": {
        \\    "type": "yarn",
        \\    "factor": 16,
        \\    "original_max_position_embeddings": 65536,
        \\    "beta_fast": 32,
        \\    "beta_slow": 1
        \\  },
        \\  "compress_rates": {
        \\    "compressed_sparse_attention": 4,
        \\    "heavily_compressed_attention": 128
        \\  },
        \\  "compress_rope_theta": 160000,
        \\  "hc_mult": 4,
        \\  "hc_sinkhorn_iters": 20,
        \\  "hc_eps": 0.000001,
        \\  "mlp_layer_types": ["hash_moe", "hash_moe", "hash_moe", "moe", "moe", "moe"],
        \\  "layer_types": [
        \\    "sliding_attention",
        \\    "sliding_attention",
        \\    "compressed_sparse_attention",
        \\    "heavily_compressed_attention",
        \\    "compressed_sparse_attention",
        \\    "heavily_compressed_attention"
        \\  ],
        \\  "compress_ratios": [0, 0, 4, 128, 4, 128],
        \\  "swiglu_limit": 10.0,
        \\  "sliding_window": 128,
        \\  "o_groups": 8,
        \\  "o_lora_rank": 1024,
        \\  "index_n_heads": 64,
        \\  "index_head_dim": 128,
        \\  "index_topk": 512,
        \\  "num_nextn_predict_layers": 1,
        \\  "rms_norm_eps": 0.000001,
        \\  "hidden_act": "silu",
        \\  "tie_word_embeddings": false
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.deepseek_v4, config.family);
    try std.testing.expectEqual(@as(u32, 4096), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 6), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 64), config.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 1), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 512), config.headDim());
    try std.testing.expectEqual(@as(u32, 2048), config.expert_intermediate_size);
    try std.testing.expectEqual(@as(u32, 2048), config.shared_expert_intermediate_size);
    try std.testing.expectEqual(@as(u32, 256), config.num_local_experts);
    try std.testing.expectEqual(@as(u32, 6), config.num_experts_per_tok);
    try std.testing.expectEqual(@as(u32, 1), config.num_shared_experts);
    try std.testing.expect(config.usesMoe());
    try std.testing.expect(config.hasSharedExpert());
    try std.testing.expectEqual(NormType.rms_norm, config.norm_type);
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
    try std.testing.expectEqual(RopeLayout.consecutive_pairs, config.rope_layout);
    try std.testing.expectEqual(ActivationType.silu, config.activation);
    try std.testing.expectApproxEqAbs(@as(f32, 1e-6), config.norm_eps, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), config.rope_partial_factor, 1e-6);
    try std.testing.expectEqual(@as(u32, 64), config.layerRopeDim(0));
    try std.testing.expectEqual(@as(u32, 64), config.deepseek_v4_qk_rope_head_dim);
    try std.testing.expectEqual(@as(u32, 1024), config.deepseek_v4_q_lora_rank);
    try std.testing.expectEqual(@as(u32, 448), config.deepseek_v4_kv_lora_rank);
    try std.testing.expectEqual(DeepseekV4ScoringFunc.sqrtsoftplus, config.deepseek_v4_scoring_func);
    try std.testing.expect(config.deepseek_v4_norm_topk_prob);
    try std.testing.expect(!config.deepseek_v4_attention_bias);
    try std.testing.expectApproxEqAbs(@as(f32, 1.5), config.deepseek_v4_routed_scaling_factor, 1e-6);
    try std.testing.expectEqual(@as(u32, 4), config.deepseek_v4_compress_rate_csa);
    try std.testing.expectEqual(@as(u32, 128), config.deepseek_v4_compress_rate_hca);
    try std.testing.expectApproxEqAbs(@as(f32, 160000.0), config.deepseek_v4_compress_rope_theta, 1e-3);
    try std.testing.expectEqual(@as(u32, 4), config.deepseek_v4_hc_mult);
    try std.testing.expectEqual(@as(u32, 20), config.deepseek_v4_hc_sinkhorn_iters);
    try std.testing.expectApproxEqAbs(@as(f32, 1e-6), config.deepseek_v4_hc_eps, 1e-9);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), config.deepseek_v4_swiglu_limit, 1e-6);
    try std.testing.expectEqual(@as(u32, 8), config.deepseek_v4_o_groups);
    try std.testing.expectEqual(@as(u32, 1024), config.deepseek_v4_o_lora_rank);
    try std.testing.expectEqual(@as(u32, 64), config.deepseek_v4_index_n_heads);
    try std.testing.expectEqual(@as(u32, 128), config.deepseek_v4_index_head_dim);
    try std.testing.expectEqual(@as(u32, 512), config.deepseek_v4_index_topk);
    try std.testing.expectEqual(@as(u32, 1), config.deepseek_v4_num_nextn_predict_layers);
    try std.testing.expectEqual(@as(u32, 3), config.deepseek_v4_hash_moe_layers);
    try std.testing.expectEqual(@as(u32, 3), config.deepseek_v4_moe_layers);
    try std.testing.expectEqual(@as(u32, 2), config.deepseek_v4_sliding_attention_layers);
    try std.testing.expectEqual(@as(u32, 2), config.deepseek_v4_compressed_sparse_attention_layers);
    try std.testing.expectEqual(@as(u32, 2), config.deepseek_v4_heavily_compressed_attention_layers);
    try std.testing.expectEqual(@as(u32, 6), config.deepseek_v4_attention_schedule_len);
    try std.testing.expectEqual(DeepseekV4AttentionKind.sliding_attention, config.deepseekV4AttentionKind(0));
    try std.testing.expectEqual(DeepseekV4AttentionKind.sliding_attention, config.deepseekV4AttentionKind(1));
    try std.testing.expectEqual(DeepseekV4AttentionKind.compressed_sparse_attention, config.deepseekV4AttentionKind(2));
    try std.testing.expectEqual(DeepseekV4AttentionKind.heavily_compressed_attention, config.deepseekV4AttentionKind(3));
    try std.testing.expectEqual(DeepseekV4AttentionKind.compressed_sparse_attention, config.deepseekV4AttentionKind(4));
    try std.testing.expectEqual(DeepseekV4AttentionKind.heavily_compressed_attention, config.deepseekV4AttentionKind(5));
    try std.testing.expect(config.layerUsesSlidingAttention(0));
    try std.testing.expect(!config.layerUsesSlidingAttention(2));
    try std.testing.expectApproxEqAbs(@as(f32, 10000.0), config.layerRopeTheta(0), 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 160000.0), config.layerRopeTheta(2), 1e-3);
    try std.testing.expectEqual(@as(usize, 512), config.deepseekV4KvCacheWidthForLayer(0));
    try std.testing.expectEqual(@as(usize, 512), config.deepseekV4KvCacheWidthForLayer(2));
    try std.testing.expectEqual(@as(usize, 512), config.deepseekV4MaxKvCacheWidth());
    try std.testing.expectEqual(@as(u32, 1), config.maxKvHeads());
    try std.testing.expectEqual(@as(u32, 512), config.maxHeadDim());
    try std.testing.expectEqual(@as(u32, 6), config.deepseek_v4_mlp_schedule_len);
    try std.testing.expectEqual(DeepseekV4MlpKind.hash_moe, config.deepseekV4MlpKind(0));
    try std.testing.expectEqual(DeepseekV4MlpKind.hash_moe, config.deepseekV4MlpKind(2));
    try std.testing.expectEqual(DeepseekV4MlpKind.moe, config.deepseekV4MlpKind(3));
    try std.testing.expectEqual(DeepseekV4MlpKind.moe, config.deepseekV4MlpKind(5));
    try std.testing.expectEqual(@as(u32, 65536), config.deepseek_v4_original_seq_len);
    try std.testing.expectApproxEqAbs(@as(f32, 16.0), config.deepseek_v4_rope_factor, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32.0), config.deepseek_v4_beta_fast, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.deepseek_v4_beta_slow, 1e-6);
}

test "parse deepseek v4 flash public config shape" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "architectures": ["DeepseekV4ForCausalLM"],
        \\  "attention_bias": false,
        \\  "bos_token_id": 0,
        \\  "eos_token_id": 1,
        \\  "expert_dtype": "fp4",
        \\  "hc_eps": 1e-06,
        \\  "hc_mult": 4,
        \\  "hc_sinkhorn_iters": 20,
        \\  "head_dim": 512,
        \\  "hidden_act": "silu",
        \\  "hidden_size": 4096,
        \\  "index_head_dim": 128,
        \\  "index_n_heads": 64,
        \\  "index_topk": 512,
        \\  "max_position_embeddings": 1048576,
        \\  "model_type": "deepseek_v4",
        \\  "moe_intermediate_size": 2048,
        \\  "n_routed_experts": 256,
        \\  "n_shared_experts": 1,
        \\  "norm_topk_prob": true,
        \\  "num_attention_heads": 64,
        \\  "num_experts_per_tok": 6,
        \\  "num_hidden_layers": 43,
        \\  "num_hash_layers": 3,
        \\  "num_key_value_heads": 1,
        \\  "num_nextn_predict_layers": 1,
        \\  "o_groups": 8,
        \\  "o_lora_rank": 1024,
        \\  "q_lora_rank": 1024,
        \\  "qk_rope_head_dim": 64,
        \\  "quantization_config": {
        \\    "activation_scheme": "dynamic",
        \\    "fmt": "e4m3",
        \\    "quant_method": "fp8",
        \\    "scale_fmt": "ue8m0",
        \\    "weight_block_size": [128, 128]
        \\  },
        \\  "rms_norm_eps": 1e-06,
        \\  "rope_scaling": {
        \\    "beta_fast": 32,
        \\    "beta_slow": 1,
        \\    "factor": 16,
        \\    "original_max_position_embeddings": 65536,
        \\    "type": "yarn"
        \\  },
        \\  "rope_theta": 10000,
        \\  "routed_scaling_factor": 1.5,
        \\  "scoring_func": "sqrtsoftplus",
        \\  "sliding_window": 128,
        \\  "swiglu_limit": 10.0,
        \\  "tie_word_embeddings": false,
        \\  "topk_method": "noaux_tc",
        \\  "vocab_size": 129280,
        \\  "compress_rope_theta": 160000,
        \\  "compress_ratios": [0, 0, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 128, 4, 0]
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.deepseek_v4, config.family);
    try std.testing.expectEqual(@as(u32, 43), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 4096), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 64), config.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 1), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 512), config.headDim());
    try std.testing.expectEqual(@as(u32, 129280), config.vocab_size);
    try std.testing.expectEqual(@as(u32, 1024), config.deepseek_v4_q_lora_rank);
    try std.testing.expectEqual(@as(u32, 64), config.deepseek_v4_qk_rope_head_dim);
    try std.testing.expectEqual(@as(u32, 4), config.deepseek_v4_compress_rate_csa);
    try std.testing.expectEqual(@as(u32, 128), config.deepseek_v4_compress_rate_hca);
    try std.testing.expectEqual(@as(u32, 43), config.deepseek_v4_attention_schedule_len);
    try std.testing.expectEqual(@as(u32, 2), config.deepseek_v4_sliding_attention_layers);
    try std.testing.expectEqual(@as(u32, 21), config.deepseek_v4_compressed_sparse_attention_layers);
    try std.testing.expectEqual(@as(u32, 20), config.deepseek_v4_heavily_compressed_attention_layers);
    try std.testing.expectEqual(DeepseekV4AttentionKind.sliding_attention, config.deepseekV4AttentionKind(0));
    try std.testing.expectEqual(DeepseekV4AttentionKind.sliding_attention, config.deepseekV4AttentionKind(1));
    try std.testing.expectEqual(DeepseekV4AttentionKind.compressed_sparse_attention, config.deepseekV4AttentionKind(2));
    try std.testing.expectEqual(DeepseekV4AttentionKind.heavily_compressed_attention, config.deepseekV4AttentionKind(3));
    try std.testing.expectEqual(DeepseekV4AttentionKind.heavily_compressed_attention, config.deepseekV4AttentionKind(41));
    try std.testing.expectEqual(DeepseekV4AttentionKind.compressed_sparse_attention, config.deepseekV4AttentionKind(42));
    try std.testing.expectEqual(@as(u32, 3), config.deepseek_v4_hash_moe_layers);
    try std.testing.expectEqual(DeepseekV4MlpKind.hash_moe, config.deepseekV4MlpKind(0));
    try std.testing.expectEqual(DeepseekV4MlpKind.hash_moe, config.deepseekV4MlpKind(2));
    try std.testing.expectEqual(DeepseekV4MlpKind.moe, config.deepseekV4MlpKind(3));
    try std.testing.expectEqual(DeepseekV4MlpKind.moe, config.deepseekV4MlpKind(42));
    try std.testing.expectEqual(@as(u32, 256), config.num_local_experts);
    try std.testing.expectEqual(@as(u32, 6), config.num_experts_per_tok);
    try std.testing.expectEqual(@as(u32, 1), config.num_shared_experts);
    try std.testing.expectEqual(@as(u32, 64), config.deepseek_v4_index_n_heads);
    try std.testing.expectEqual(@as(u32, 128), config.deepseek_v4_index_head_dim);
    try std.testing.expectEqual(@as(u32, 512), config.deepseek_v4_index_topk);
    try std.testing.expectApproxEqAbs(@as(f32, 160000.0), config.deepseek_v4_compress_rope_theta, 1e-3);
    try std.testing.expectApproxEqAbs(@as(f32, 16.0), config.deepseek_v4_rope_factor, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 32.0), config.deepseek_v4_beta_fast, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.deepseek_v4_beta_slow, 1e-6);
}

test "isGenerativeModel" {
    try std.testing.expect(isGenerativeModel("gpt2"));
    try std.testing.expect(isGenerativeModel("llama"));
    try std.testing.expect(isGenerativeModel("mistral"));
    try std.testing.expect(isGenerativeModel("bitnet"));
    try std.testing.expect(isGenerativeModel("bitnet-b1.58"));
    try std.testing.expect(isGenerativeModel("deepseek_v4"));
    try std.testing.expect(isGenerativeModel("deepseek-v4-flash"));
    try std.testing.expect(!isGenerativeModel("jina_embeddings_v5"));
    try std.testing.expect(!isGenerativeModel("bert"));
    try std.testing.expect(!isGenerativeModel("t5"));
}

test "parse gguf metadata for llama config" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 8);

    try appendMetadataString(allocator, &data, "general.architecture", "llama");
    try appendMetadataU32(allocator, &data, "llama.embedding_length", 4096);
    try appendMetadataU32(allocator, &data, "llama.block_count", 32);
    try appendMetadataU32(allocator, &data, "llama.attention.head_count", 32);
    try appendMetadataU32(allocator, &data, "llama.attention.head_count_kv", 8);
    try appendMetadataU32(allocator, &data, "llama.feed_forward_length", 14336);
    try appendMetadataU32(allocator, &data, "llama.context_length", 8192);
    try appendMetadataF32(allocator, &data, "llama.rope.freq_base", 500000.0);

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.llama, config.family);
    try std.testing.expectEqual(@as(u32, 4096), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 32), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 8), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 14336), config.intermediate_size);
    try std.testing.expectEqual(@as(u32, 8192), config.max_position_embeddings);
    try std.testing.expectApproxEqAbs(@as(f32, 500000.0), config.rope_theta, 1e-3);
}

test "parse gguf metadata for bitnet config" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 11);

    try appendMetadataString(allocator, &data, "general.architecture", "bitnet-b1.58");
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.vocab_size", 128256);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.context_length", 4096);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.embedding_length", 2560);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.block_count", 30);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.feed_forward_length", 6912);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.rope.dimension_count", 128);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.attention.head_count", 20);
    try appendMetadataU32(allocator, &data, "bitnet-b1.58.attention.head_count_kv", 5);
    try appendMetadataF32(allocator, &data, "bitnet-b1.58.attention.layer_norm_rms_epsilon", 0.00001);
    try appendMetadataF32(allocator, &data, "bitnet-b1.58.rope.freq_base", 500000.0);

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.bitnet, config.family);
    try std.testing.expectEqual(@as(u32, 128256), config.vocab_size);
    try std.testing.expectEqual(@as(u32, 4096), config.max_position_embeddings);
    try std.testing.expectEqual(@as(u32, 2560), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 30), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 6912), config.intermediate_size);
    try std.testing.expectEqual(@as(u32, 20), config.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 5), config.effectiveKVHeads());
    try std.testing.expectEqual(@as(u32, 128), config.headDim());
    try std.testing.expectEqual(NormType.rms_norm, config.norm_type);
    try std.testing.expectEqual(PositionEncoding.rope, config.position_encoding);
    try std.testing.expectEqual(ActivationType.relu_squared, config.activation);
    try std.testing.expectApproxEqAbs(@as(f32, 0.00001), config.norm_eps, 1e-8);
    try std.testing.expectApproxEqAbs(@as(f32, 500000.0), config.rope_theta, 1e-3);
    try std.testing.expect(config.weight_tying);
}

test "parse gguf metadata for mixtral config" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 11);

    try appendMetadataString(allocator, &data, "general.architecture", "llama");
    try appendMetadataU32(allocator, &data, "llama.embedding_length", 4096);
    try appendMetadataU32(allocator, &data, "llama.block_count", 32);
    try appendMetadataU32(allocator, &data, "llama.attention.head_count", 32);
    try appendMetadataU32(allocator, &data, "llama.attention.head_count_kv", 8);
    try appendMetadataU32(allocator, &data, "llama.feed_forward_length", 14336);
    try appendMetadataU32(allocator, &data, "llama.context_length", 32768);
    try appendMetadataU32(allocator, &data, "llama.attention.sliding_window", 4096);
    try appendMetadataU32(allocator, &data, "llama.expert_count", 8);
    try appendMetadataU32(allocator, &data, "llama.expert_used_count", 2);
    try appendMetadataF32(allocator, &data, "llama.rope.freq_base", 1000000.0);

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.llama, config.family);
    try std.testing.expectEqual(@as(u32, 8), config.num_local_experts);
    try std.testing.expectEqual(@as(u32, 2), config.num_experts_per_tok);
    try std.testing.expectEqual(@as(u32, 4096), config.sliding_window);
    try std.testing.expect(config.usesMoe());
}

test "parse gguf metadata for gemma ignores generic rope scaling factor" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 10);

    try appendMetadataString(allocator, &data, "general.architecture", "gemma");
    try appendMetadataU32(allocator, &data, "gemma.embedding_length", 2560);
    try appendMetadataU32(allocator, &data, "gemma.block_count", 34);
    try appendMetadataU32(allocator, &data, "gemma.attention.head_count", 8);
    try appendMetadataU32(allocator, &data, "gemma.attention.head_count_kv", 4);
    try appendMetadataU32(allocator, &data, "gemma.feed_forward_length", 10240);
    try appendMetadataU32(allocator, &data, "gemma.context_length", 32768);
    try appendMetadataU32(allocator, &data, "gemma.attention.sliding_window", 1024);
    try appendMetadataF32(allocator, &data, "gemma.rope.freq_base", 1000000.0);
    try appendMetadataF32(allocator, &data, "gemma.rope.scaling.factor", 8.0);
    try appendMetadataF32(allocator, &data, "gemma.attention.layer_norm_rms_epsilon", 0.000001);

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.rope_freq_scale, 1e-6);
}

test "parse gemma4 config with shared kv and per-layer gqa" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma4",
        \\  "text_config": {
        \\    "hidden_size": 3072,
        \\    "num_hidden_layers": 36,
        \\    "num_attention_heads": 16,
        \\    "num_key_value_heads": 4,
        \\    "head_dim": 128,
        \\    "intermediate_size": 12288,
        \\    "sliding_window": 512,
        \\    "sliding_window_pattern": 6,
        \\    "num_kv_shared_layers": 12,
        \\    "global_head_dim": 256,
        \\    "num_global_key_value_heads": 8
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectEqual(@as(u32, 3072), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 36), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 12), config.num_kv_shared_layers);
    try std.testing.expectEqual(@as(u32, 256), config.global_head_dim);
    try std.testing.expectEqual(@as(u32, 8), config.num_global_key_value_heads);

    // Shared KV detection: last 12 of 36 layers share KV.
    try std.testing.expect(!config.layerSharesKv(0));
    try std.testing.expect(!config.layerSharesKv(23));
    try std.testing.expect(config.layerSharesKv(24));
    try std.testing.expect(config.layerSharesKv(35));

    // Donor layer indices: shared layers map to the last non-shared layer of the same type.
    try std.testing.expectEqual(@as(?usize, null), config.kvDonorLayerIndex(0));
    try std.testing.expectEqual(@as(?usize, null), config.kvDonorLayerIndex(23));
    try std.testing.expectEqual(@as(?usize, 22), config.kvDonorLayerIndex(24)); // sliding -> last sliding in [0,23] = 22
    try std.testing.expectEqual(@as(?usize, 23), config.kvDonorLayerIndex(35)); // full -> last full in [0,23] = 23

    // Per-layer GQA: sliding layers (e.g. layer 0) use default KV heads/dim.
    // Global layers (e.g. layer 5, since (5+1)%6==0) use global values.
    try std.testing.expectEqual(@as(u32, 4), config.effectiveKVHeadsForLayer(0));
    try std.testing.expectEqual(@as(u32, 128), config.effectiveHeadDimForLayer(0));
    try std.testing.expectEqual(@as(u32, 8), config.effectiveKVHeadsForLayer(5));
    try std.testing.expectEqual(@as(u32, 256), config.effectiveHeadDimForLayer(5));

    // Max helpers for pool sizing.
    try std.testing.expectEqual(@as(u32, 8), config.maxKvHeads());
    try std.testing.expectEqual(@as(u32, 256), config.maxHeadDim());
    try std.testing.expectEqual(@as(usize, 2048), config.maxKvWidthPerToken());
}

test "parse gemma4 text-only config defaults" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma4_text",
        \\  "text_config": {
        \\    "hidden_size": 4096,
        \\    "num_hidden_layers": 48,
        \\    "num_attention_heads": 16,
        \\    "num_key_value_heads": 8,
        \\    "head_dim": 256,
        \\    "intermediate_size": 16384
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gemma, config.family);

    // No shared KV or per-layer GQA when fields are absent (default 0).
    try std.testing.expectEqual(@as(u32, 0), config.num_kv_shared_layers);
    try std.testing.expect(!config.layerSharesKv(0));
    try std.testing.expect(!config.layerSharesKv(47));
    try std.testing.expectEqual(@as(u32, 8), config.effectiveKVHeadsForLayer(0));
    try std.testing.expectEqual(@as(u32, 256), config.effectiveHeadDimForLayer(0));
    try std.testing.expectEqual(@as(u32, 8), config.maxKvHeads());
    try std.testing.expectEqual(@as(u32, 256), config.maxHeadDim());
}

test "parse gemma4 moe fields from text_config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma4",
        \\  "text_config": {
        \\    "hidden_size": 2048,
        \\    "num_hidden_layers": 26,
        \\    "num_attention_heads": 8,
        \\    "intermediate_size": 8192,
        \\    "num_local_experts": 128,
        \\    "num_experts_per_tok": 8
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectEqual(@as(u32, 128), config.num_local_experts);
    try std.testing.expectEqual(@as(u32, 8), config.num_experts_per_tok);
    try std.testing.expect(config.usesMoe());
}

test "parse gemma4 shared expert config from text_config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma4",
        \\  "text_config": {
        \\    "hidden_size": 2048,
        \\    "num_hidden_layers": 26,
        \\    "num_attention_heads": 8,
        \\    "intermediate_size": 8192,
        \\    "num_local_experts": 128,
        \\    "num_experts_per_tok": 8,
        \\    "num_shared_experts": 1,
        \\    "shared_expert_intermediate_size": 16384
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(u32, 1), config.num_shared_experts);
    try std.testing.expectEqual(@as(u32, 16384), config.shared_expert_intermediate_size);
    try std.testing.expect(config.hasSharedExpert());
    try std.testing.expect(config.usesMoe());
}

test "hasSharedExpert false when num_shared_experts is zero" {
    const config: Config = .{};
    try std.testing.expectEqual(@as(u32, 0), config.num_shared_experts);
    try std.testing.expect(!config.hasSharedExpert());
}

test "parse gguf metadata for gemma4 moe with shared experts" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 11);

    try appendMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendMetadataU32(allocator, &data, "gemma4.embedding_length", 2048);
    try appendMetadataU32(allocator, &data, "gemma4.block_count", 26);
    try appendMetadataU32(allocator, &data, "gemma4.attention.head_count", 8);
    try appendMetadataU32(allocator, &data, "gemma4.attention.head_count_kv", 4);
    try appendMetadataI32Array(allocator, &data, "gemma4.feed_forward_length", &.{8192});
    try appendMetadataU32(allocator, &data, "gemma4.context_length", 131072);
    try appendMetadataU32(allocator, &data, "gemma4.expert_count", 128);
    try appendMetadataU32(allocator, &data, "gemma4.expert_used_count", 8);
    try appendMetadataU32(allocator, &data, "gemma4.expert_shared_count", 1);
    try appendMetadataU32(allocator, &data, "gemma4.expert_feed_forward_length", 704);

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectEqual(@as(u32, 128), config.num_local_experts);
    try std.testing.expectEqual(@as(u32, 8), config.num_experts_per_tok);
    try std.testing.expectEqual(@as(u32, 1), config.num_shared_experts);
    try std.testing.expectEqual(@as(u32, 704), config.expert_intermediate_size);
    try std.testing.expectEqual(@as(u32, 704), config.expertIntermediateSize());
    try std.testing.expect(config.hasSharedExpert());
    try std.testing.expect(config.usesMoe());
}

test "parse gguf gemma4 per-layer head_count_kv array" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    try appendLe(u64, allocator, &data, 8);

    try appendMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendMetadataU32(allocator, &data, "gemma4.embedding_length", 2816);
    try appendMetadataU32(allocator, &data, "gemma4.block_count", 6);
    try appendMetadataU32(allocator, &data, "gemma4.attention.head_count", 16);
    // Per-layer KV heads: sliding=8, full-attention(idx 5)=2
    try appendMetadataU32Array(allocator, &data, "gemma4.attention.head_count_kv", &.{ 8, 8, 8, 8, 8, 2 });
    try appendMetadataU32(allocator, &data, "gemma4.attention.sliding_window", 1024);
    try appendMetadataBoolArray(allocator, &data, "gemma4.attention.sliding_window_pattern", &.{ true, true, true, true, true, false });
    try appendMetadataU32(allocator, &data, "gemma4.attention.key_length_swa", 256);

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    // Sliding layers get 8 KV heads
    try std.testing.expectEqual(@as(u32, 8), config.num_key_value_heads);
    // Full-attention layers get 2 KV heads
    try std.testing.expectEqual(@as(u32, 2), config.num_global_key_value_heads);
    // sliding_window_pattern derived correctly
    try std.testing.expectEqual(@as(u32, 6), config.sliding_window_pattern);
}

test "parse gemma4 e2b config with layer_types and shared kv" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "gemma4",
        \\  "text_config": {
        \\    "hidden_size": 1536,
        \\    "num_hidden_layers": 35,
        \\    "num_attention_heads": 8,
        \\    "num_key_value_heads": 1,
        \\    "head_dim": 256,
        \\    "global_head_dim": 512,
        \\    "intermediate_size": 6144,
        \\    "sliding_window": 512,
        \\    "num_kv_shared_layers": 20,
        \\    "layer_types": [
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention",
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention",
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention",
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention",
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention",
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention",
        \\      "sliding_attention", "sliding_attention", "sliding_attention", "sliding_attention",
        \\      "full_attention"
        \\    ]
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.gemma, config.family);

    // sliding_window_pattern derived from layer_types (first full_attention at index 4 → pattern=5).
    try std.testing.expectEqual(@as(u32, 5), config.sliding_window_pattern);

    // Shared KV: 35 layers, 20 shared → 15 non-shared (0-14).
    // Donor = last non-shared layer of the same attention type.
    try std.testing.expect(!config.layerSharesKv(14));
    try std.testing.expect(config.layerSharesKv(15));
    try std.testing.expectEqual(@as(?usize, 13), config.kvDonorLayerIndex(15)); // sliding -> last sliding in [0,14] = 13
    try std.testing.expectEqual(@as(?usize, 14), config.kvDonorLayerIndex(29)); // full -> last full in [0,14] = 14
    try std.testing.expectEqual(@as(?usize, 13), config.kvDonorLayerIndex(30)); // sliding -> 13
    try std.testing.expectEqual(@as(?usize, 14), config.kvDonorLayerIndex(34)); // full -> 14

    // Per-layer head_dim: sliding=256, global=512. KV heads=1 for all layers.
    try std.testing.expectEqual(@as(u32, 256), config.effectiveHeadDimForLayer(0)); // sliding
    try std.testing.expectEqual(@as(u32, 512), config.effectiveHeadDimForLayer(4)); // global (pattern=5)
    try std.testing.expectEqual(@as(u32, 1), config.effectiveKVHeadsForLayer(0));
    try std.testing.expectEqual(@as(u32, 1), config.effectiveKVHeadsForLayer(4));

    // Pool sizing: max across sliding (1*256=256) and global (1*512=512).
    try std.testing.expectEqual(@as(u32, 1), config.maxKvHeads());
    try std.testing.expectEqual(@as(u32, 512), config.maxHeadDim());
    try std.testing.expectEqual(@as(usize, 512), config.maxKvWidthPerToken());
}

test "parse gguf metadata for gemma4 shared kv config" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 0);
    // Use actual Gemma 4 GGUF key names as exported by Unsloth/llama.cpp.
    try appendLe(u64, allocator, &data, 15);

    try appendMetadataString(allocator, &data, "general.architecture", "gemma4");
    try appendMetadataU32(allocator, &data, "gemma4.embedding_length", 1536);
    try appendMetadataU32(allocator, &data, "gemma4.block_count", 35);
    try appendMetadataU32(allocator, &data, "gemma4.attention.head_count", 8);
    try appendMetadataU32(allocator, &data, "gemma4.attention.head_count_kv", 1);
    try appendMetadataI32Array(allocator, &data, "gemma4.feed_forward_length", &.{ 6144, 6144, 6144 });
    try appendMetadataU32(allocator, &data, "gemma4.context_length", 131072);
    try appendMetadataU32(allocator, &data, "gemma4.attention.key_length", 512);
    try appendMetadataU32(allocator, &data, "gemma4.attention.key_length_swa", 256);
    try appendMetadataU32(allocator, &data, "gemma4.attention.shared_kv_layers", 20);
    try appendMetadataU32(allocator, &data, "gemma4.attention.sliding_window", 512);
    try appendMetadataF32(allocator, &data, "gemma4.attention.layer_norm_rms_epsilon", 0.000001);
    try appendMetadataF32(allocator, &data, "gemma4.rope.freq_base", 1000000.0);
    try appendMetadataF32(allocator, &data, "gemma4.rope.freq_base_swa", 10000.0);
    // sliding_window_pattern: bool array [true,true,true,true,false,...] → pattern=5
    try appendMetadataBoolArray(allocator, &data, "gemma4.attention.sliding_window_pattern", &.{ true, true, true, true, false });

    var parsed = try @import("../gguf/format.zig").parse(allocator, data.items);
    defer parsed.deinit(allocator);

    const view = gguf_metadata.View.init(&parsed);
    const config = parseGgufMetadata(view).?;
    try std.testing.expectEqual(ModelFamily.gemma, config.family);
    try std.testing.expectEqual(@as(u32, 20), config.num_kv_shared_layers);
    try std.testing.expectEqual(@as(u32, 512), config.global_head_dim);
    try std.testing.expectEqual(@as(u32, 256), config.attention_head_dim);
    try std.testing.expectEqual(@as(u32, 6144), config.intermediate_size);
    try std.testing.expectEqual(@as(u32, 5), config.sliding_window_pattern);
    try std.testing.expectEqual(@as(f32, 10000.0), config.rope_local_theta);
    try std.testing.expectEqual(@as(f32, 1000000.0), config.rope_theta);
    // Per-layer head_dim: sliding=256 (from key_length_swa), global=512 (from key_length).
    try std.testing.expectEqual(@as(u32, 256), config.effectiveHeadDimForLayer(0)); // sliding
    try std.testing.expectEqual(@as(u32, 512), config.effectiveHeadDimForLayer(4)); // global
}

test "gemma family defaults to gelu_new activation" {
    var config = Config{
        .family = .gemma,
    };
    applyFamilyDefaults(&config);
    try std.testing.expectEqual(ActivationType.gelu_new, config.activation);
}

test "gemma4 rope_dim_override only limits active rotary lanes" {
    const config = Config{
        .family = .gemma,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .rope_partial_factor = 0.25,
        .rope_dim_override = 64,
    };

    try std.testing.expectEqual(@as(u32, 256), config.layerRopeDim(0));
    try std.testing.expectEqual(@as(u32, 256), config.layerRopeActiveDim(0));

    try std.testing.expectEqual(@as(u32, 128), config.layerRopeDim(4));
    try std.testing.expectEqual(@as(u32, 64), config.layerRopeActiveDim(4));
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendMetadataString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: []const u8) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 8);
    try appendString(allocator, data, value);
}

fn appendMetadataU32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: u32) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 4);
    try appendLe(u32, allocator, data, value);
}

fn appendMetadataF32(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, value: f32) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 6);
    try appendLe(u32, allocator, data, @bitCast(value));
}

fn appendMetadataBoolArray(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const bool) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 9); // array type
    try appendLe(u32, allocator, data, 7); // element type = bool
    try appendLe(u64, allocator, data, values.len);
    for (values) |v| try data.append(allocator, if (v) @as(u8, 1) else 0);
}

fn appendMetadataI32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const i32) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 9); // array type
    try appendLe(u32, allocator, data, 5); // element type = i32
    try appendLe(u64, allocator, data, values.len);
    for (values) |v| try appendLe(i32, allocator, data, v);
}

fn appendMetadataU32Array(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), key: []const u8, values: []const u32) !void {
    try appendString(allocator, data, key);
    try appendLe(u32, allocator, data, 9); // array type
    try appendLe(u32, allocator, data, 4); // element type = u32
    try appendLe(u64, allocator, data, values.len);
    for (values) |v| try appendLe(u32, allocator, data, v);
}

test "parse qwen2vl token aliases" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "qwen2_vl",
        \\  "image_token_id": 151655,
        \\  "vision_start_token_id": 151652,
        \\  "vision_end_token_id": 151653,
        \\  "vision_config": {
        \\    "embed_dim": 1280,
        \\    "depth": 32,
        \\    "num_heads": 16,
        \\    "patch_size": 14,
        \\    "mlp_ratio": 4,
        \\    "spatial_merge_size": 2,
        \\    "temporal_patch_size": 2,
        \\    "hidden_act": "quick_gelu"
        \\  },
        \\  "text_config": {
        \\    "hidden_size": 1536,
        \\    "num_hidden_layers": 28,
        \\    "num_attention_heads": 12,
        \\    "intermediate_size": 8960
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(@as(i32, 151655), config.image_token_index);
    try std.testing.expectEqual(@as(i32, 151652), config.boi_token_index);
    try std.testing.expectEqual(@as(i32, 151653), config.eoi_token_index);
    try std.testing.expectEqual(@as(u32, 1280), config.vision_embed_dim);
    try std.testing.expectEqual(@as(u32, 32), config.vision_num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 16), config.vision_num_attention_heads);
    try std.testing.expectEqual(@as(u32, 14), config.vision_patch_size);
    try std.testing.expectEqual(@as(u32, 4), config.vision_mlp_ratio);
    try std.testing.expectEqual(@as(u32, 2), config.vision_spatial_merge_size);
    try std.testing.expectEqual(@as(u32, 2), config.vision_temporal_patch_size);
    try std.testing.expect(config.vision_use_quick_gelu);
    try std.testing.expect(config.supportsNativeQwen2VlVision());
}

test "parse qwen3.5 chandra-style hybrid multimodal config" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "architectures": ["Qwen3_5ForConditionalGeneration"],
        \\  "image_token_id": 248056,
        \\  "model_type": "qwen3_5",
        \\  "text_config": {
        \\    "attn_output_gate": true,
        \\    "dtype": "bfloat16",
        \\    "eos_token_id": 248044,
        \\    "full_attention_interval": 4,
        \\    "head_dim": 256,
        \\    "hidden_act": "silu",
        \\    "hidden_size": 2560,
        \\    "intermediate_size": 9216,
        \\    "layer_types": [
        \\      "linear_attention",
        \\      "linear_attention",
        \\      "linear_attention",
        \\      "full_attention"
        \\    ],
        \\    "linear_conv_kernel_dim": 4,
        \\    "linear_key_head_dim": 128,
        \\    "linear_num_key_heads": 16,
        \\    "linear_num_value_heads": 32,
        \\    "linear_value_head_dim": 128,
        \\    "max_position_embeddings": 262144,
        \\    "model_type": "qwen3_5_text",
        \\    "num_attention_heads": 16,
        \\    "num_hidden_layers": 32,
        \\    "num_key_value_heads": 4,
        \\    "partial_rotary_factor": 0.25,
        \\    "rms_norm_eps": 1e-06,
        \\    "rope_parameters": {
        \\      "mrope_interleaved": true,
        \\      "mrope_section": [11, 11, 10],
        \\      "partial_rotary_factor": 0.25,
        \\      "rope_theta": 10000000,
        \\      "rope_type": "default"
        \\    },
        \\    "tie_word_embeddings": true,
        \\    "vocab_size": 248320
        \\  },
        \\  "tie_word_embeddings": true,
        \\  "video_token_id": 248057,
        \\  "vision_config": {
        \\    "depth": 24,
        \\    "hidden_act": "gelu_pytorch_tanh",
        \\    "hidden_size": 1024,
        \\    "in_channels": 3,
        \\    "intermediate_size": 4096,
        \\    "model_type": "qwen3_5",
        \\    "num_heads": 16,
        \\    "num_position_embeddings": 2304,
        \\    "out_hidden_size": 2560,
        \\    "patch_size": 16,
        \\    "spatial_merge_size": 2,
        \\    "temporal_patch_size": 2
        \\  },
        \\  "vision_end_token_id": 248054,
        \\  "vision_start_token_id": 248053
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.qwen3_5, config.family);
    try std.testing.expectEqualStrings("model.language_model", config.weight_prefix);
    try std.testing.expectEqual(@as(u32, 2560), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 32), config.num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 16), config.num_attention_heads);
    try std.testing.expectEqual(@as(u32, 4), config.num_key_value_heads);
    try std.testing.expectEqual(@as(u32, 256), config.attention_head_dim);
    try std.testing.expectEqual(@as(i32, 248056), config.image_token_index);
    try std.testing.expectEqual(@as(i32, 248053), config.boi_token_index);
    try std.testing.expectEqual(@as(i32, 248054), config.eoi_token_index);
    try std.testing.expect(config.qwen35_has_linear_attention);
    try std.testing.expect(config.layerUsesQwen35LinearAttention(0));
    try std.testing.expect(!config.layerUsesQwen35LinearAttention(3));
    try std.testing.expectEqual(@as(u32, 4), config.qwen35_full_attention_interval);
    try std.testing.expectEqual(@as(u32, 4), config.qwen35_linear_conv_kernel_dim);
    try std.testing.expectEqual(@as(u32, 128), config.qwen35_linear_key_head_dim);
    try std.testing.expectEqual(@as(u32, 128), config.qwen35_linear_value_head_dim);
    try std.testing.expectEqual(@as(u32, 16), config.qwen35_linear_num_key_heads);
    try std.testing.expectEqual(@as(u32, 32), config.qwen35_linear_num_value_heads);
    try std.testing.expect(config.qwen35_attn_output_gate);
    try std.testing.expect(config.qwen35_mrope_interleaved);
    try std.testing.expectEqualSlices(u32, &.{ 11, 11, 10 }, &config.qwen35_mrope_section);
    try std.testing.expectApproxEqAbs(@as(f32, 10000000.0), config.rope_theta, 1.0);
    try std.testing.expectApproxEqAbs(@as(f32, 0.25), config.rope_partial_factor, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), config.norm_weight_offset, 1e-6);
    try std.testing.expectEqual(@as(u32, 1024), config.vision_hidden_size);
    try std.testing.expectEqual(@as(u32, 1024), config.vision_embed_dim);
    try std.testing.expectEqual(@as(u32, 24), config.vision_num_hidden_layers);
    try std.testing.expectEqual(@as(u32, 16), config.vision_num_attention_heads);
    try std.testing.expectEqual(@as(u32, 4096), config.vision_intermediate_size);
    try std.testing.expectEqual(@as(u32, 16), config.vision_patch_size);
    try std.testing.expect(config.supportsNativeQwen2VlVision());
}

test "parse colqwen2 vlm_config wrapper" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "colqwen2",
        \\  "vlm_config": {
        \\    "model_type": "qwen2_vl",
        \\    "bos_token_id": 151643,
        \\    "eos_token_id": 151645,
        \\    "image_token_id": 151655,
        \\    "vision_start_token_id": 151652,
        \\    "vision_end_token_id": 151653,
        \\    "hidden_size": 1536,
        \\    "num_hidden_layers": 28,
        \\    "num_attention_heads": 12,
        \\    "intermediate_size": 8960,
        \\    "vision_config": {
        \\      "hidden_size": 1280,
        \\      "depth": 32,
        \\      "num_heads": 16,
        \\      "spatial_patch_size": 14,
        \\      "spatial_merge_size": 2,
        \\      "temporal_patch_size": 2,
        \\      "hidden_act": "quick_gelu"
        \\    }
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.qwen2, config.family);
    try std.testing.expectEqual(@as(i32, 151655), config.image_token_index);
    try std.testing.expectEqual(@as(i32, 151652), config.boi_token_index);
    try std.testing.expectEqual(@as(i32, 151653), config.eoi_token_index);
    try std.testing.expectEqual(@as(u32, 1536), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 1280), config.vision_embed_dim);
    try std.testing.expectEqual(@as(u32, 14), config.vision_patch_size);
    try std.testing.expect(config.supportsNativeQwen2VlVision());
}

test "parse colqwen2 merged config lacks native vision detail" {
    const allocator = std.testing.allocator;
    const json =
        \\{
        \\  "model_type": "qwen2_vl",
        \\  "hidden_size": 1536,
        \\  "image_token_id": 151655,
        \\  "vision_start_token_id": 151652,
        \\  "vision_end_token_id": 151653,
        \\  "vision_config": {
        \\    "hidden_size": 1536,
        \\    "spatial_patch_size": 14
        \\  }
        \\}
    ;
    const config = try parseConfig(allocator, json);
    try std.testing.expectEqual(ModelFamily.qwen2, config.family);
    try std.testing.expectEqual(@as(u32, 1536), config.hidden_size);
    try std.testing.expectEqual(@as(u32, 1536), config.vision_embed_dim);
    try std.testing.expectEqual(@as(u32, 14), config.vision_patch_size);
    try std.testing.expect(!config.supportsNativeQwen2VlVision());
}
