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

const std = @import("std");
const artifact = @import("artifact.zig");
const driver = @import("driver.zig");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
pub const RmsReductionSchedule = @import("../kernel_geometry.zig").RmsReductionSchedule;

pub const PixelScaleStorage = @import("../kernel_geometry.zig").PixelScaleStorage;

pub const Attention = struct {
    batch: u32,
    seq: u32,
    q_heads: u32,
    kv_heads: u32,
    head_dim: u32,
    /// Zero selects global attention. A nonzero value is an inclusive,
    /// symmetric radius, so radius=512 permits abs(query-key) <= 512.
    local_radius: u32,

    pub fn validate(self: Attention) !void {
        if (self.batch == 0 or self.seq == 0 or self.seq > 8192) return error.InvalidEmbeddingGemma2Shape;
        if (self.q_heads != 4 or (self.kv_heads != 1 and self.kv_heads != 2)) return error.InvalidEmbeddingGemma2Shape;
        if ((self.kv_heads == 1 and self.head_dim != 512) or (self.kv_heads == 2 and self.head_dim != 256)) return error.InvalidEmbeddingGemma2Shape;
        if (self.q_heads % self.kv_heads != 0) return error.InvalidEmbeddingGemma2Shape;
    }
};

/// A numerical operation descriptor, separate from the geometry reused by
/// RoPE and F32 training contractions. No provider, model name or flag is kept.
pub const AttentionSchedule = enum(u32) {
    flash_bf16 = 0,
    efficient_bf16 = 1,

    pub fn forResolvedMask(shape: Attention, all_tokens_valid: bool) AttentionSchedule {
        const full_window = shape.local_radius == 0 or shape.seq < shape.local_radius;
        return if (shape.head_dim <= 256 and all_tokens_valid and full_window) .flash_bf16 else .efficient_bf16;
    }
};
pub const AttentionProgram = struct {
    shape: Attention,
    /// Original source extent; shape.seq remains the physical bank stride.
    source_sequence: u32,
    /// Borrowed only for this dispatch; retained storage belongs to the Arena.
    logical_metadata: Buffer = .{},
    probability_schedule: AttentionSchedule,
    split_count: u32 = 1,

    pub fn validate(self: AttentionProgram) !void {
        try self.shape.validate();
        if ((self.logical_metadata.ptr == 0 and self.logical_metadata.len != 0) or (self.logical_metadata.ptr != 0 and (self.logical_metadata.len != 8 or self.logical_metadata.ptr % 4 != 0))) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (self.shape.local_radius > 8192) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (self.source_sequence == 0 or self.source_sequence > self.shape.seq) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (self.split_count == 0 or self.split_count > 128 or self.split_count > (self.source_sequence + 63) / 64 or
            (self.probability_schedule == .efficient_bf16 and self.split_count != 1)) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (self.shape.batch > 65535 / self.split_count) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (self.probability_schedule == .flash_bf16 and self.shape.head_dim > 256)
            return error.InvalidEmbeddingGemma2AttentionSchedule;
    }
    pub fn forResolvedMask(shape: Attention, source_sequence: u32, all_tokens_valid: bool, multiprocessors: i32) !AttentionProgram {
        try shape.validate();
        if (source_sequence == 0 or source_sequence > shape.seq) return error.InvalidEmbeddingGemma2AttentionSchedule;
        if (multiprocessors <= 0) return error.InvalidCudaState;
        var source = shape;
        source.seq = source_sequence;
        const schedule = AttentionSchedule.forResolvedMask(source, all_tokens_valid);
        var result = AttentionProgram{ .shape = shape, .source_sequence = source_sequence, .probability_schedule = schedule };
        if (schedule == .flash_bf16) result.split_count = try splitCount(source, multiprocessors);
        try result.validate();
        return result;
    }

    /// Exact backend-free Flash split policy: no allocation, callbacks or hash.
    /// The caller supplies an actual cold device property, never a GPU name.
    fn splitCount(shape: Attention, multiprocessors: i32) !u32 {
        if (multiprocessors <= 0) return error.InvalidCudaState;
        const sms = try std.math.mul(u32, @intCast(multiprocessors), 2);
        const blocks = (shape.seq + 63) / 64;
        const work = try std.math.mul(u32, try std.math.mul(u32, shape.batch, shape.q_heads), blocks);
        if (@as(f32, @floatFromInt(work)) >= 0.8 * @as(f32, @floatFromInt(sms))) return 1;
        const limit: u32 = @min(@as(u32, 128), sms, blocks);
        var efficiency: [128]f32 = @splat(0);
        var maximum: f32 = 0;
        for (1..limit + 1) |value| {
            const n: u32 = @intCast(value);
            if (n != 1 and (blocks + n - 1) / n == (blocks + n - 2) / (n - 1)) continue;
            const waves = @as(f32, @floatFromInt(try std.math.mul(u32, work, n))) / @as(f32, @floatFromInt(sms));
            efficiency[n - 1] = waves / @ceil(waves);
            maximum = @max(maximum, efficiency[n - 1]);
        }
        for (1..limit + 1) |value| {
            const n: u32 = @intCast(value);
            if (n != 1 and (blocks + n - 1) / n == (blocks + n - 2) / (n - 1)) continue;
            if (@as(f64, efficiency[n - 1]) >= 0.85 * @as(f64, maximum)) return n;
        }
        return 1;
    }

    /// Exact live shared span: efficient QK/PV no longer stage K/V; dead
    /// query/score bytes hold previous F32 output until the next tile barrier.
    pub fn sharedMemoryBytes(self: AttentionProgram) !u32 {
        try self.validate();
        const dim = self.shape.head_dim;
        return attentionSharedMemoryBytes(self.probability_schedule, dim);
    }

    pub const Workspace = struct { output_bytes: usize = 0, lse_offset: usize = 0, total_bytes: usize = 0 };
    pub fn workspace(self: AttentionProgram) !Workspace {
        try self.validate();
        if (self.split_count == 1) return .{};
        const rows = try std.math.mul(usize, try std.math.mul(usize, self.shape.batch, self.shape.q_heads), self.shape.seq);
        const count = try std.math.mul(usize, rows, self.split_count);
        const bytes = try std.math.mul(usize, try std.math.mul(usize, count, self.shape.head_dim), @sizeOf(f32));
        // Every validated BF16 head has256/512 elements, so F32 rows
        // already align the LSE region to the256-byte device ABI.
        const lse_offset = bytes;
        return .{ .output_bytes = bytes, .lse_offset = lse_offset, .total_bytes = try std.math.add(usize, lse_offset, try std.math.mul(usize, count, @sizeOf(f32))) };
    }
};

/// Contiguous BSHD MHA with independently live query rows and prefix keys.
/// Inputs/output are BF16; the completed F32 QK dot is multiplied once by
/// roundF32(score_scale * log2(e)). RF rescales the F32 carry before PV MMA and
/// divides by the F32 denominator only after the last 64-key block.
/// This is an operation contract, not a model or backend-selection policy.
pub const Bf16BshdPrefixRfAttention = struct {
    pub const query_tile: u32 = 64;
    pub const key_tile: u32 = 64;
    pub const head_dim: u32 = 64;
    pub const threads: u32 = 128;

    batch: u32,
    physical_sequence: u32,
    query_sequence: u32,
    key_sequence: u32,
    heads: u32,
    score_scale: f32,

    pub fn validate(self: @This()) !void {
        if (self.batch == 0 or self.batch > 65535 or self.physical_sequence == 0 or self.physical_sequence > 8192 or
            self.query_sequence == 0 or self.query_sequence > self.physical_sequence or
            self.key_sequence == 0 or self.key_sequence > self.physical_sequence or self.heads == 0 or self.heads > 64 or
            !std.math.isFinite(self.score_scale) or self.score_scale <= 0 or
            !std.math.isFinite(self.score_scale * @as(f32, 1.4426950408889634074))) return error.InvalidShape;
    }
    pub fn bytes(self: @This()) !usize {
        try self.validate();
        return std.math.mul(usize, try std.math.mul(usize, try std.math.mul(usize, self.batch, self.physical_sequence), self.heads), head_dim * 2);
    }
    pub fn sharedMemoryBytes(self: @This()) !u32 {
        try self.validate();
        return query_tile * head_dim * 2 + query_tile * key_tile * 6 + 6 * query_tile * 4;
    }
    pub fn validateBuffers(self: @This(), output: Buffer, q: Buffer, k: Buffer, v: Buffer) !void {
        const count = try self.bytes();
        try @import("buffer.zig").validateRegions(4, .{ q, k, v, output }, @splat(count), @splat(4), 3);
    }
};

fn attentionSharedMemoryBytes(schedule: AttentionSchedule, dim: u32) !u32 {
    const keys: u32 = if (schedule == .flash_bf16) 64 else 128;
    const query_bytes = try std.math.mul(u32, 16 * 2, dim);
    const score_bytes = 16 * keys * 4;
    const old_output_bytes = try std.math.mul(u32, 16 * 4, dim);
    // QS and dead score bytes overlap the previous F32 output. Small heads
    // need no extra padding; subtraction must not underflow for HD64.
    const prefix = if (schedule == .efficient_bf16)
        @max(query_bytes, old_output_bytes -| score_bytes)
    else
        try std.math.mul(u32, 32 * 2, dim);
    return std.math.add(u32, prefix, 16 * keys * 6 + 6 * 16 * 4);
}

pub const Stats = struct {
    launches: u64 = 0,
    audio_frontend_fused_launches: u64 = 0,
    audio_fft_launches: u64 = 0,
    audio_conv_tiled_launches: u64 = 0,
    audio_conv_bf16_launches: u64 = 0,
    audio_batch_layout_launches: u64 = 0,
    cublaslt_matmuls: u64 = 0,
    cublas_f32_matmuls: u64 = 0,
    cublas_bf16_matmuls: u64 = 0,
    attention_launches: u64 = 0,
    bf16_bshd_prefix_rf_launches: u64 = 0,
    attention_scalar_fallbacks: u64 = 0,
    scalar_matmul_fallbacks: u64 = 0,
    output_bytes: u64 = 0,
    packed_projection_launches: u64 = 0,
    fused_vision_launches: u64 = 0,
    extended_flash_launches: u64 = 0,
    fused_backbone_launches: u64 = 0,
    packed_head_launches: u64 = 0,
    packed_gelu_launches: u64 = 0,
    tuned_gemm_calls: u64 = 0,
    gemm_tuning_declines: u64 = 0,
    vision_residual_launches: u64 = 0,
    spatial_pool_launches: u64 = 0,
    short_cudnn_attention_launches: u64 = 0,
};

pub const Contrastive = struct {
    queries: u32,
    candidates: u32,
    width: u32,
    temperature: f32,
    objective_weight: f32,

    pub fn validate(self: Contrastive) !void {
        if (self.queries == 0 or self.queries > 64 or self.candidates == 0 or self.candidates > 384 or
            self.width == 0 or self.width > 768 or !std.math.isFinite(self.temperature) or self.temperature <= 0 or
            !std.math.isFinite(@as(f32, 1) / self.temperature) or
            !std.math.isFinite(self.objective_weight) or self.objective_weight <= 0)
            return error.InvalidEmbeddingGemma2TrainingShape;
    }
};

pub const Module = struct {
    module: driver.CUmodule,
    lookup: driver.CUfunction,
    audio_attention: driver.CUfunction,
    independent_local_attention: driver.CUfunction,
    local_relative_source_pack: driver.CUfunction,
    local_relative_source_softmax: driver.CUfunction,
    local_relative_source_publish: driver.CUfunction,
    audio_clamp: driver.CUfunction,
    audio_glu_rows: driver.CUfunction,
    audio_frontend: driver.CUfunction,
    audio_logmel: driver.CUfunction,
    audio_fft512: driver.CUfunction,
    audio_conv_tiled: driver.CUfunction,
    audio_conv_bf16: driver.CUfunction,
    audio_depthwise_conv: driver.CUfunction,
    pillow_horizontal: driver.CUfunction,
    pillow_vertical_patches: driver.CUfunction,
    rms_norm: driver.CUfunction,
    training_rms_norm: driver.CUfunction,
    source_rms_norm: driver.CUfunction,
    source_rms_norm_values_f32: driver.CUfunction,
    rms_norm_input_vjp: driver.CUfunction,
    training_rms_norm_input_vjp: driver.CUfunction,
    rope: driver.CUfunction,
    rope_vjp: driver.CUfunction,
    training_rope: driver.CUfunction,
    training_rope_vjp: driver.CUfunction,
    split_columns: driver.CUfunction,
    packed_heads: driver.CUfunction,
    packed_gelu: driver.CUfunction,
    spatial_pool: driver.CUfunction,
    spatial_pool_gemm_phases: driver.CUfunction,
    vision_rope_pack: driver.CUfunction,
    source_vision_rope_pack: driver.CUfunction,
    native_vision: driver.CUfunction,
    source_native_vision: driver.CUfunction,
    audio_segment_conv: driver.CUfunction,
    norm_residual: driver.CUfunction,
    attention: driver.CUfunction,
    local_flash_attention: driver.CUfunction,
    efficient_attention_hd256: driver.CUfunction,
    bf16_bshd_prefix_rf: driver.CUfunction = null,
    attention_split_merge: driver.CUfunction,
    attention_softmax: driver.CUfunction,
    source_attention_softmax: driver.CUfunction,
    matmul: driver.CUfunction,
    gelu_product: driver.CUfunction,
    gelu_mul_vjp: driver.CUfunction,
    bf16_to_f32: driver.CUfunction,
    f32_to_bf16: driver.CUfunction,
    training_pack_bhsd_bf16: driver.CUfunction,
    training_unpack_bhsd_bf16: driver.CUfunction,
    pack_bshd_bf16_f32: driver.CUfunction,
    pack_bshd_f32: driver.CUfunction,
    unpack_bhsd_f32: driver.CUfunction,
    unpack_bhsd_f32_bf16: driver.CUfunction,
    scale: driver.CUfunction,
    scale_device: driver.CUfunction,
    scale_device_f32_bf16: driver.CUfunction,
    accumulate_f32: driver.CUfunction,
    accumulate_bf16_boundary_f32: driver.CUfunction,
    residual: driver.CUfunction,
    residual_vjp: driver.CUfunction,
    contrastive_prob: driver.CUfunction,
    contrastive_query_vjp: driver.CUfunction,
    contrastive_candidate_vjp: driver.CUfunction,
    training_contrastive_ce: driver.CUfunction,
    training_softmax: driver.CUfunction,
    training_softmax_vjp: driver.CUfunction,
    mean_l2: driver.CUfunction,
    training_mean_l2: driver.CUfunction,
    mean_l2_vjp: driver.CUfunction,
    prefix_normalization: @import("prefix_normalization.zig").Program,
    stats_: Stats = .{},

    pub fn init(ctx: *Context) !Module {
        // BF16 is a hard model contract. Refuse pre-Ampere devices rather
        // than silently executing an FP16 or host fallback.
        if (ctx.info.compute_major < 8) return error.CudaBf16Unsupported;
        try ctx.makeCurrent();
        var m: driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&m, artifact.image.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(m);
        return .{
            .module = m,
            .lookup = try function(ctx, m, "termite_embedding_gemma2_lookup_bf16"),
            .audio_attention = try function(ctx, m, "termite_gemma4_audio_local_attention_f32"),
            .independent_local_attention = try function(ctx, m, "antfly_local_relative_attention_independent_f32"),
            .local_relative_source_pack = try function(ctx, m, "antfly_local_relative_source_pack_f32"),
            .local_relative_source_softmax = try function(ctx, m, "antfly_local_relative_source_softmax_f32"),
            .local_relative_source_publish = try function(ctx, m, "antfly_local_relative_source_publish_bf16_values_f32"),
            .audio_clamp = try function(ctx, m, "termite_gemma4_audio_clamp_f32"),
            .audio_glu_rows = try function(ctx, m, "termite_gemma4_audio_glu_rows_f32"),
            .audio_frontend = try function(ctx, m, "antfly_audio_frontend_f32"),
            .audio_logmel = try function(ctx, m, "antfly_audio_logmel_f32"),
            .audio_fft512 = try function(ctx, m, "antfly_audio_fft512_f32"),
            .audio_conv_tiled = try function(ctx, m, "antfly_audio_conv_tiled_f32"),
            .audio_conv_bf16 = try function(ctx, m, "antfly_audio_conv_bf16_wmma"),
            .audio_depthwise_conv = try function(ctx, m, "termite_causal_depthwise_f32"),
            .pillow_horizontal = try function(ctx, m, "termite_embedding_gemma2_pillow_horizontal_rgb8"),
            .pillow_vertical_patches = try function(ctx, m, "termite_embedding_gemma2_pillow_vertical_patches_f32"),
            .rms_norm = try function(ctx, m, "termite_embedding_gemma2_rms_norm_bf16"),
            .training_rms_norm = try function(ctx, m, "termite_embedding_gemma2_training_rms_norm_bf16"),
            .source_rms_norm = try function(ctx, m, "antfly_rms_contiguous_source_bf16"),
            .source_rms_norm_values_f32 = try function(ctx, m, "antfly_rms_contiguous_source_bf16_values_f32"),
            .rms_norm_input_vjp = try function(ctx, m, "termite_embedding_gemma2_rms_norm_input_vjp_f32"),
            .training_rms_norm_input_vjp = try function(ctx, m, "termite_embedding_gemma2_training_rms_norm_input_vjp_f32"),
            .rope = try function(ctx, m, "termite_embedding_gemma2_rope_bf16"),
            .rope_vjp = try function(ctx, m, "termite_embedding_gemma2_rope_vjp_f32"),
            .training_rope = try function(ctx, m, "termite_embedding_gemma2_training_rope_bf16"),
            .training_rope_vjp = try function(ctx, m, "termite_embedding_gemma2_training_rope_vjp_f32"),
            .packed_heads = try function(ctx, m, "termite_embedding_gemma2_packed_heads_bf16"),
            .packed_gelu = try function(ctx, m, "termite_embedding_gemma2_packed_gelu_bf16"),
            .spatial_pool = try function(ctx, m, "termite_embedding_gemma2_spatial_pool_f32"),
            .spatial_pool_gemm_phases = try function(ctx, m, "antfly_spatial_pool_gemm_f32_bf16_phases"),
            .split_columns = try function(ctx, m, "termite_embedding_gemma2_split_columns_bf16"),
            .vision_rope_pack = try function(ctx, m, "termite_embedding_gemma2_vision_rope_pack_bf16"),
            .source_vision_rope_pack = try function(ctx, m, "termite_rotary_pair_i32_bf16_source"),
            .audio_segment_conv = try function(ctx, m, "termite_embedding_gemma2_audio_segment_conv_f32"),
            .native_vision = try function(ctx, m, "termite_embedding_gemma2_native_vision_bf16"),
            .source_native_vision = try function(ctx, m, "termite_attention_bf16_bshd_native_masked_m32n16"),
            .norm_residual = try function(ctx, m, "termite_embedding_gemma2_norm_residual_bf16"),
            .attention = try function(ctx, m, "termite_embedding_gemma2_attention_bf16"),
            .local_flash_attention = try function(ctx, m, "termite_embedding_gemma2_attention_local_flash_bf16"),
            .efficient_attention_hd256 = try function(ctx, m, "antfly_attention_efficient_bf16_hd256"),
            .attention_split_merge = try function(ctx, m, "antfly_attention_split_merge_bf16"),
            .attention_softmax = try function(ctx, m, "termite_embedding_gemma2_attention_softmax_bf16"),
            .source_attention_softmax = try function(ctx, m, "termite_softmax_scores_bf16_prefix"),
            .matmul = try function(ctx, m, "termite_embedding_gemma2_matmul_bf16"),
            .gelu_product = try function(ctx, m, "termite_embedding_gemma2_gelu_mul_bf16"),
            .gelu_mul_vjp = try function(ctx, m, "termite_embedding_gemma2_gelu_mul_vjp_f32"),
            .bf16_to_f32 = try function(ctx, m, "termite_embedding_gemma2_bf16_to_f32"),
            .f32_to_bf16 = try function(ctx, m, "termite_embedding_gemma2_f32_to_bf16"),
            .training_pack_bhsd_bf16 = try function(ctx, m, "termite_embedding_gemma2_training_pack_bhsd_bf16"),
            .training_unpack_bhsd_bf16 = try function(ctx, m, "termite_embedding_gemma2_training_unpack_bhsd_bf16"),
            .pack_bshd_bf16_f32 = try function(ctx, m, "termite_embedding_gemma2_pack_bshd_bf16_f32"),
            .pack_bshd_f32 = try function(ctx, m, "termite_embedding_gemma2_pack_bshd_f32"),
            .unpack_bhsd_f32 = try function(ctx, m, "termite_embedding_gemma2_unpack_bhsd_f32"),
            .unpack_bhsd_f32_bf16 = try function(ctx, m, "termite_embedding_gemma2_unpack_bhsd_f32_bf16"),
            .scale = try function(ctx, m, "termite_embedding_gemma2_scale_bf16"),
            .scale_device = try function(ctx, m, "termite_embedding_gemma2_scale_device_bf16"),
            .scale_device_f32_bf16 = try function(ctx, m, "termite_embedding_gemma2_scale_device_f32_bf16"),
            .accumulate_f32 = try function(ctx, m, "termite_embedding_gemma2_accumulate_f32"),
            .accumulate_bf16_boundary_f32 = try function(ctx, m, "termite_embedding_gemma2_accumulate_bf16_boundary_f32"),
            .residual = try function(ctx, m, "termite_embedding_gemma2_residual_bf16"),
            .residual_vjp = try function(ctx, m, "termite_embedding_gemma2_residual_vjp_f32"),
            .contrastive_prob = try function(ctx, m, "termite_embedding_gemma2_contrastive_prob_f32"),
            .contrastive_query_vjp = try function(ctx, m, "termite_embedding_gemma2_contrastive_query_vjp_f32"),
            .contrastive_candidate_vjp = try function(ctx, m, "termite_embedding_gemma2_contrastive_candidate_vjp_f32"),
            .training_contrastive_ce = try function(ctx, m, "termite_embedding_gemma2_training_contrastive_ce_f32"),
            .training_softmax = try function(ctx, m, "termite_embedding_gemma2_training_softmax_f32"),
            .training_softmax_vjp = try function(ctx, m, "termite_embedding_gemma2_training_softmax_vjp_f32"),
            .mean_l2 = try function(ctx, m, "termite_embedding_gemma2_mean_l2_f32"),
            .training_mean_l2 = try function(ctx, m, "termite_embedding_gemma2_training_mean_l2_f32"),
            .mean_l2_vjp = try function(ctx, m, "termite_embedding_gemma2_mean_l2_vjp_f32"),
            .prefix_normalization = try @import("prefix_normalization.zig").Program.init(ctx, m),
        };
    }

    pub fn deinit(self: *Module, ctx: *Context) void {
        ctx.makeCurrent() catch {};
        if (self.module != null) _ = ctx.driver.fns.cuModuleUnload(self.module);
        self.* = undefined;
    }

    pub fn stats(self: *const Module) Stats {
        return self.stats_;
    }

    pub fn noteCublasLtMatmul(self: *Module) void {
        self.stats_.cublaslt_matmuls += 1;
    }
    pub fn noteCublasBf16Matmul(self: *Module) void {
        self.stats_.cublas_bf16_matmuls += 1;
    }
    pub fn noteCublasF32Matmul(self: *Module) void {
        self.stats_.cublas_f32_matmuls += 1;
    }
    pub fn notePackedProjection(self: *Module) void {
        self.stats_.packed_projection_launches += 1;
    }
    pub fn noteFusedVision(self: *Module) void {
        self.stats_.fused_vision_launches += 1;
    }
    pub fn noteExtendedFlash(self: *Module) void {
        self.stats_.extended_flash_launches += 1;
    }
    pub fn noteAttentionFallback(self: *Module) void {
        self.stats_.attention_scalar_fallbacks += 1;
    }
    pub fn noteAttentionLaunch(self: *Module) void {
        self.stats_.attention_launches += 1;
    }

    // Coefficient indices are produced by the shared CPU Pillow plan, never
    // caller-provided device data. Validate all backing spans before launch.
    pub fn launchPillowHorizontal(self: *Module, ctx: *Context, out: Buffer, input: Buffer, starts: Buffer, offsets: Buffer, weights: Buffer, source_width: u32, source_height: u32, target_width: u32, source_stride: u64, precision_bits: u32) !void {
        if (source_width == 0 or source_height == 0 or target_width == 0 or precision_bits == 0 or precision_bits > 31) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, try std.math.mul(usize, source_height, target_width), 3);
        if (count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        const row_bytes = try std.math.mul(usize, source_width, 3);
        if (source_stride < row_bytes or weights.len % @sizeOf(i32) != 0) return error.InvalidEmbeddingGemma2Shape;
        try require(input, try std.math.add(usize, try std.math.mul(usize, source_height - 1, @intCast(source_stride)), row_bytes));
        try require(weights, @sizeOf(i32));
        try require(out, count);
        try require(starts, try std.math.mul(usize, target_width, 8));
        try require(offsets, try std.math.mul(usize, @as(usize, target_width) + 1, 8));
        var op = out.ptr;
        var ip = input.ptr;
        var sp = starts.ptr;
        var fp = offsets.ptr;
        var wp = weights.ptr;
        var h = source_height;
        var w = target_width;
        var stride = source_stride;
        var bits = precision_bits;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&sp), @ptrCast(&fp), @ptrCast(&wp), @ptrCast(&h), @ptrCast(&w), @ptrCast(&stride), @ptrCast(&bits) };
        try self.launch1d(ctx, self.pillow_horizontal, &args, @intCast(count));
    }

    pub fn launchPillowVerticalPatches(self: *Module, ctx: *Context, out: Buffer, horizontal: Buffer, starts: Buffer, offsets: Buffer, weights: Buffer, source_height: u32, target_width: u32, target_height: u32, precision_bits: u32) !void {
        return self.launchPillowVerticalPatchesStored(ctx, out, horizontal, starts, offsets, weights, source_height, target_width, target_height, precision_bits, .f32);
    }

    pub fn launchPillowVerticalPatchesStored(self: *Module, ctx: *Context, out: Buffer, horizontal: Buffer, starts: Buffer, offsets: Buffer, weights: Buffer, source_height: u32, target_width: u32, target_height: u32, precision_bits: u32, storage: PixelScaleStorage) !void {
        if (source_height == 0 or target_width == 0 or target_height == 0 or target_width % 16 != 0 or target_height % 16 != 0 or precision_bits == 0 or precision_bits > 31 or weights.len % @sizeOf(i32) != 0) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, try std.math.mul(usize, target_width, target_height), 3);
        if (count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        try require(out, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(horizontal, try std.math.mul(usize, try std.math.mul(usize, source_height, target_width), 3));
        try require(weights, @sizeOf(i32));
        try require(starts, try std.math.mul(usize, target_height, 8));
        try require(offsets, try std.math.mul(usize, @as(usize, target_height) + 1, 8));
        var op = out.ptr;
        var hp = horizontal.ptr;
        var sp = starts.ptr;
        var fp = offsets.ptr;
        var wp = weights.ptr;
        var w = target_width;
        var h = target_height;
        var patch: u32 = 16;
        var bits = precision_bits;
        var source_storage = @backingInt(storage);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&hp), @ptrCast(&sp), @ptrCast(&fp), @ptrCast(&wp), @ptrCast(&w), @ptrCast(&h), @ptrCast(&patch), @ptrCast(&bits), @ptrCast(&source_storage) };
        try self.launch1d(ctx, self.pillow_vertical_patches, &args, @intCast(count));
    }

    pub fn launchAudioAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, rel: Buffer, scales: Buffer, valid: Buffer, p: @import("../ops.zig").Gemma4AudioLocalAttentionParams) !void {
        if (p.rows == 0 or p.rows > std.math.maxInt(i32) or p.hidden == 0 or p.heads == 0 or p.heads > 65535 or p.head_dim == 0 or
            p.chunk == 0 or p.chunk > std.math.maxInt(i32) or p.context == 0 or p.context_left < 2 or p.context_left > p.context or p.context > 24 or
            !std.math.isFinite(p.query_scale) or !std.math.isFinite(p.k_scale) or !std.math.isFinite(p.logit_cap) or p.logit_cap <= 0 or !std.math.isFinite(p.invalid_value))
            return error.InvalidEmbeddingGemma2Shape;
        if (p.arithmetic == .combined_f32 and p.publication != .f32) return error.InvalidEmbeddingGemma2Shape;
        const derived_hidden = std.math.mul(usize, p.heads, p.head_dim) catch return error.InvalidEmbeddingGemma2Shape;
        if (derived_hidden != p.hidden) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, p.rows, p.hidden);
        const matrix_bytes = try std.math.mul(usize, count, @sizeOf(f32));
        const relative_count = try std.math.mul(usize, p.context_left, p.hidden);
        const relative_bytes = try std.math.mul(usize, relative_count, @sizeOf(f32));
        const scale_bytes = try std.math.mul(usize, p.head_dim, @sizeOf(f32));
        const valid_bytes = try std.math.mul(usize, p.rows, @sizeOf(f32));
        try require(out, matrix_bytes);
        try require(q, matrix_bytes);
        try require(k, matrix_bytes);
        try require(v, matrix_bytes);
        try require(rel, relative_bytes);
        try require(scales, scale_bytes);
        try require(valid, valid_bytes);
        var op = out.ptr;
        var qp = q.ptr;
        var kp = k.ptr;
        var vp = v.ptr;
        var rp = rel.ptr;
        var sp = scales.ptr;
        var mp = valid.ptr;
        if (p.rows > std.math.maxInt(u32) or p.hidden > std.math.maxInt(u32) or p.head_dim > std.math.maxInt(u32) or p.chunk > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        var rows: u32 = @intCast(p.rows);
        var hidden: u32 = @intCast(p.hidden);
        var heads: u32 = @intCast(p.heads);
        var hd: u32 = @intCast(p.head_dim);
        var chunk: u32 = @intCast(p.chunk);
        var left: u32 = @intCast(p.context_left);
        var context: u32 = @intCast(p.context);
        var flags: u32 = @as(u32, @intFromBool(p.exclude_farthest_key)) | (@as(u32, @intFromBool(p.segmented)) << 1) | (@as(u32, @intFromBool(p.publication == .bf16_values_f32)) << 2);
        var ks = p.k_scale;
        var cap = p.logit_cap;
        var invalid = p.invalid_value;
        var query_scale = p.query_scale;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&rp), @ptrCast(&sp), @ptrCast(&mp), @ptrCast(&rows), @ptrCast(&hidden), @ptrCast(&heads), @ptrCast(&hd), @ptrCast(&chunk), @ptrCast(&left), @ptrCast(&context), @ptrCast(&ks), @ptrCast(&cap), @ptrCast(&invalid), @ptrCast(&flags), @ptrCast(&query_scale) };
        switch (p.arithmetic) {
            .combined_f32 => try launch(ctx, self.audio_attention, args[0..18], .{ rows, heads, 1 }, .{ 32, 1, 1 }),
            .independent_f32 => try launch(ctx, self.independent_local_attention, &args, .{ rows, heads, 1 }, .{ 32, 1, 1 }),
        }
        self.stats_.launches += 1;
        self.stats_.attention_launches += 1;
    }

    pub fn launchLocalRelativeSourcePack(self: *Module, ctx: *Context, plan: @import("local_relative_source.zig").Plan, query: Buffer, packed_query: Buffer, key: Buffer, value: Buffer, q: Buffer, k: Buffer, v: Buffer, scales: Buffer, query_scale: f32, key_scale: f32) !void {
        try require(query, plan.query.bytes);
        try require(packed_query, plan.packed_query.bytes);
        try require(key, plan.key.bytes);
        try require(value, plan.value.bytes);
        const input_bytes = plan.rows * plan.heads * plan.dim * 4;
        try require(q, input_bytes);
        try require(k, input_bytes);
        try require(v, input_bytes);
        try require(scales, plan.dim * 4);
        var qp = query.ptr;
        var pp = packed_query.ptr;
        var kp = key.ptr;
        var vp = value.ptr;
        var iq = q.ptr;
        var ik = k.ptr;
        var iv = v.ptr;
        var sp = scales.ptr;
        var rows: u32 = @intCast(plan.rows);
        var heads: u32 = @intCast(plan.heads);
        var dim: u32 = @intCast(plan.dim);
        var chunk: u32 = @intCast(plan.chunk);
        var context: u32 = @intCast(plan.context);
        var relative: u32 = @intCast(plan.relative);
        var blocks: u32 = @intCast(plan.blocks);
        var qs = query_scale;
        var ks = key_scale;
        var args = [_]?*anyopaque{ @ptrCast(&qp), @ptrCast(&pp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&iq), @ptrCast(&ik), @ptrCast(&iv), @ptrCast(&sp), @ptrCast(&rows), @ptrCast(&heads), @ptrCast(&dim), @ptrCast(&chunk), @ptrCast(&context), @ptrCast(&relative), @ptrCast(&blocks), @ptrCast(&qs), @ptrCast(&ks) };
        try self.launch1d(ctx, self.local_relative_source_pack, &args, @max(plan.query.bytes, plan.key.bytes) / 4);
    }
    pub fn launchLocalRelativeSourceSoftmax(self: *Module, ctx: *Context, plan: @import("local_relative_source.zig").Plan, scores: Buffer, relative_scores: Buffer, valid: Buffer, cap: f32, invalid_value: f32, exclude_farthest: bool) !void {
        try require(scores, plan.scores.bytes);
        try require(relative_scores, plan.relative_scores.bytes);
        try require(valid, plan.rows * 4);
        var op = scores.ptr;
        var rp = relative_scores.ptr;
        var vp = valid.ptr;
        var rows: u32 = @intCast(plan.rows);
        var heads: u32 = @intCast(plan.heads);
        var chunk: u32 = @intCast(plan.chunk);
        var context: u32 = @intCast(plan.context);
        var relative: u32 = @intCast(plan.relative);
        var blocks: u32 = @intCast(plan.blocks);
        var c = cap;
        var invalid = invalid_value;
        var exclude: u32 = @intFromBool(exclude_farthest);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&rp), @ptrCast(&vp), @ptrCast(&rows), @ptrCast(&heads), @ptrCast(&chunk), @ptrCast(&context), @ptrCast(&relative), @ptrCast(&blocks), @ptrCast(&c), @ptrCast(&invalid), @ptrCast(&exclude) };
        try launch(ctx, self.local_relative_source_softmax, &args, .{ @intCast(plan.heads * plan.padded), 1, 1 }, .{ 32, 1, 1 });
        self.stats_.launches += 1;
    }
    pub fn launchLocalRelativeSourcePublish(self: *Module, ctx: *Context, plan: @import("local_relative_source.zig").Plan, output: Buffer, input: Buffer) !void {
        try require(output, plan.rows * plan.heads * plan.dim * 4);
        try require(input, plan.result.bytes);
        var op = output.ptr;
        var ip = input.ptr;
        var rows: u32 = @intCast(plan.rows);
        var heads: u32 = @intCast(plan.heads);
        var dim: u32 = @intCast(plan.dim);
        var chunk: u32 = @intCast(plan.chunk);
        var blocks: u32 = @intCast(plan.blocks);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&rows), @ptrCast(&heads), @ptrCast(&dim), @ptrCast(&chunk), @ptrCast(&blocks) };
        try self.launch1d(ctx, self.local_relative_source_publish, &args, plan.rows * plan.heads * plan.dim);
        self.stats_.attention_launches += 1;
    }

    pub fn launchAudioClamp(self: *Module, ctx: *Context, out: Buffer, input: Buffer, count: u32, min_value: ?f32, max_value: ?f32) !void {
        if (count == 0 or (min_value == null and max_value == null)) return error.InvalidEmbeddingGemma2Shape;
        if (min_value) |value| if (!std.math.isFinite(value)) return error.InvalidEmbeddingGemma2Shape;
        if (max_value) |value| if (!std.math.isFinite(value)) return error.InvalidEmbeddingGemma2Shape;
        if (min_value != null and max_value != null and min_value.? > max_value.?) return error.InvalidEmbeddingGemma2Shape;
        const bytes = try std.math.mul(usize, count, @sizeOf(f32));
        try require(out, bytes);
        try require(input, bytes);
        var op = out.ptr;
        var ip = input.ptr;
        var n = count;
        var lower = min_value orelse 0;
        var upper = max_value orelse 0;
        var has_lower: u32 = @intFromBool(min_value != null);
        var has_upper: u32 = @intFromBool(max_value != null);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&n), @ptrCast(&lower), @ptrCast(&upper), @ptrCast(&has_lower), @ptrCast(&has_upper) };
        try self.launch1d(ctx, self.audio_clamp, &args, count);
    }

    pub fn launchAudioLogMel(self: *Module, ctx: *Context, out: Buffer, pcm: Buffer, plan: Buffer, mask: Buffer, frames: usize, hop: usize, floor: f32) !void {
        const needed = try std.math.add(usize, try std.math.mul(usize, frames - 1, hop), 320);
        if (needed > std.math.maxInt(u32) or frames > std.math.maxInt(u32) / 512) return error.InvalidEmbeddingGemma2Shape;
        try require(pcm, needed * 4);
        try require(out, frames * 128 * 4);
        try require(plan, 2048 * 4);
        try require(mask, frames * 4);
        var ip = pcm.ptr;
        var pp = plan.ptr;
        var mp = mask.ptr;
        var op = out.ptr;
        var hp: u32 = @intCast(hop);
        var fp = floor;
        var args = [_]?*anyopaque{ @ptrCast(&ip), @ptrCast(&pp), @ptrCast(&mp), @ptrCast(&op), @ptrCast(&hp), @ptrCast(&fp) };
        try ctx.makeCurrent();
        try ctx.launchKernel(self.audio_logmel, @intCast(frames), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null);
        self.stats_.launches += 1;
        self.stats_.audio_frontend_fused_launches += 1;
    }
    pub fn launchAudioFft512(self: *Module, ctx: *Context, out: Buffer, input: Buffer, tw: Buffer, frames: usize) !void {
        if (frames == 0 or frames > std.math.maxInt(u32) / 320) return error.InvalidEmbeddingGemma2Shape;
        try require(out, frames * 257 * 4);
        try require(input, frames * 320 * 4);
        try require(tw, 512 * 4);
        var ip = input.ptr;
        var tp = tw.ptr;
        var op = out.ptr;
        var args = [_]?*anyopaque{ @ptrCast(&ip), @ptrCast(&tp), @ptrCast(&op) };
        try ctx.makeCurrent();
        try ctx.launchKernel(self.audio_fft512, @intCast(frames), 1, 1, 256, 1, 1, 0, ctx.stream, &args, null);
        self.stats_.audio_fft_launches += 1;
        self.stats_.launches += 1;
    }
    pub fn launchAudioConv(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: Buffer, bias: Buffer, batch: usize, ic: usize, oc: usize, h: usize, w: usize, bf16: bool) !void {
        const oh = h / 2 + h % 2;
        const ow = w / 2 + w % 2;
        if (batch == 0 or batch > 65535 or ic == 0 or oc == 0 or h == 0 or w == 0 or oh * ow > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        if (ic > std.math.maxInt(u32) / 9 or oc > 65535 * 16 or h > std.math.maxInt(i32) - 2 or w > std.math.maxInt(i32) - 2) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, try std.math.mul(usize, batch, oc), try std.math.mul(usize, oh, ow));
        if (count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        try require(out, count * 4);
        try require(input, batch * ic * h * w * 4);
        try require(weight, oc * ic * 9 * 4);
        try require(bias, oc * 4);
        var op = out.ptr;
        var ip = input.ptr;
        var wp = weight.ptr;
        var bp = bias.ptr;
        var ci: u32 = @intCast(ic);
        var co: u32 = @intCast(oc);
        var hh: u32 = @intCast(h);
        var ww: u32 = @intCast(w);
        var ho: u32 = @intCast(oh);
        var wo: u32 = @intCast(ow);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&bp), @ptrCast(&ci), @ptrCast(&co), @ptrCast(&hh), @ptrCast(&ww), @ptrCast(&ho), @ptrCast(&wo) };
        try ctx.makeCurrent();
        try ctx.launchKernel(if (bf16) self.audio_conv_bf16 else self.audio_conv_tiled, @intCast((oh * ow + 15) / 16), @intCast((oc + 15) / 16), @intCast(batch), if (bf16) 32 else 16, if (bf16) 1 else 16, 1, 0, ctx.stream, &args, null);
        self.stats_.launches += 1;
        if (bf16) self.stats_.audio_conv_bf16_launches += 1 else self.stats_.audio_conv_tiled_launches += 1;
    }
    pub fn launchAudioFrontend(self: *Module, ctx: *Context, out: Buffer, input: Buffer, auxiliary: Buffer, params: @import("../ops.zig").AudioFrontendParams) !void {
        const n = try params.counts();
        try require(out, n.output * 4);
        try require(input, n.input * 4);
        try require(auxiliary, n.auxiliary * 4);
        var op = out.ptr;
        var ip = input.ptr;
        var ap = auxiliary.ptr;
        var mode = params.mode;
        var rows = params.rows;
        var width = params.width;
        var hop = params.hop;
        var floor = params.floor;
        var batch = params.batch;
        var args = [_]?*anyopaque{ @ptrCast(&ip), @ptrCast(&ap), @ptrCast(&op), @ptrCast(&mode), @ptrCast(&rows), @ptrCast(&width), @ptrCast(&hop), @ptrCast(&floor), @ptrCast(&batch) };
        try self.launch1d(ctx, self.audio_frontend, &args, if (params.mode == 3) @as(usize, params.rows) * params.batch else n.output);
        if (params.batch > 1 and (params.mode == 3 or params.mode == 4)) self.stats_.audio_batch_layout_launches += 1;
    }
    pub fn launchAudioGluRows(self: *Module, ctx: *Context, out: Buffer, input: Buffer, rows: u32, dim: u32) !void {
        if (rows == 0 or dim == 0) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, rows, dim);
        if (count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        try require(out, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(input, try std.math.mul(usize, count, 2 * @sizeOf(f32)));
        var op = out.ptr;
        var ip = input.ptr;
        var r = rows;
        var d = dim;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&r), @ptrCast(&d) };
        try self.launch1d(ctx, self.audio_glu_rows, &args, count);
    }

    pub fn launchAudioDepthwiseConv(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: Buffer, rows: u32, dim: u32, kernel_size: u32) !void {
        if (rows == 0 or dim == 0 or kernel_size == 0) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, rows, dim);
        if (count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        try require(out, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(input, try std.math.mul(usize, count, @sizeOf(f32)));
        const weight_count = try std.math.mul(usize, kernel_size, dim);
        try require(weight, try std.math.mul(usize, weight_count, @sizeOf(f32)));
        var op = out.ptr;
        var ip = input.ptr;
        var wp = weight.ptr;
        var r = rows;
        var d = dim;
        var k = kernel_size;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&d), @ptrCast(&k) };
        try self.launch1d(ctx, self.audio_depthwise_conv, &args, count);
    }

    pub fn launchLookup(self: *Module, ctx: *Context, out: Buffer, table: Buffer, ids: Buffer, rows: u32, vocab: u32, hidden: u32, scale: f32) !void {
        const count = try std.math.mul(usize, rows, hidden);
        try require(out, count * 2);
        try require(table, @as(usize, vocab) * hidden * 2);
        try require(ids, @as(usize, rows) * 8);
        var op = out.ptr;
        var tp = table.ptr;
        var ip = ids.ptr;
        var r = rows;
        var v = vocab;
        var h = hidden;
        var s = scale;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&tp), @ptrCast(&ip), @ptrCast(&r), @ptrCast(&v), @ptrCast(&h), @ptrCast(&s) };
        try self.launch1d(ctx, self.lookup, &args, count);
    }

    pub fn launchAudioSegmentConv(self: *Module, ctx: *Context, output: Buffer, input: Buffer, weight: Buffer, starts: Buffer, rows: u32, dim: u32, kernel_size: u32) !void {
        if (rows == 0 or dim == 0 or kernel_size == 0) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, rows, dim);
        if (count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2Shape;
        try require(output, count * 4);
        try require(input, count * 4);
        try require(starts, @as(usize, rows) * 4);
        try require(weight, try std.math.mul(usize, try std.math.mul(usize, dim, kernel_size), 4));
        var op = output.ptr;
        var ip = input.ptr;
        var wp = weight.ptr;
        var sp = starts.ptr;
        var r = rows;
        var d = dim;
        var k = kernel_size;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&sp), @ptrCast(&r), @ptrCast(&d), @ptrCast(&k) };
        try self.launch1d(ctx, self.audio_segment_conv, &args, count);
    }

    pub fn noteTunedGemm(self: *Module) void {
        self.stats_.tuned_gemm_calls += 1;
    }
    pub fn noteTuningDecline(self: *Module) void {
        self.stats_.gemm_tuning_declines += 1;
    }
    pub fn noteVisionResidual(self: *Module) void {
        self.stats_.vision_residual_launches += 1;
    }

    pub fn launchPackedHeads(self: *Module, ctx: *Context, q: Buffer, k: Buffer, v: Buffer, input: Buffer, qw: Buffer, kw: Buffer, mask: Buffer, positions: Buffer, rows: u32, sequence: u32, q_heads: u32, kv_heads: u32, dim: u32, eps: f32, theta: f32, vision: bool, q_scale: f32) !void {
        if (rows == 0 or sequence == 0 or rows % sequence != 0 or !std.math.isFinite(eps) or eps <= 0 or !std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(q_scale)) return error.InvalidEmbeddingGemma2Shape;
        if (vision) {
            if (q_heads != 12 or kv_heads != 12 or dim != 64) return error.InvalidEmbeddingGemma2Shape;
        } else try (Attention{ .batch = rows / sequence, .seq = sequence, .q_heads = q_heads, .kv_heads = kv_heads, .head_dim = dim, .local_radius = 0 }).validate();
        const q_count = try std.math.mul(usize, try std.math.mul(usize, rows, q_heads), dim);
        const kv_count = try std.math.mul(usize, try std.math.mul(usize, rows, kv_heads), dim);
        try require(q, q_count * 2);
        try require(k, kv_count * 2);
        try require(v, kv_count * 2);
        try require(input, try std.math.mul(usize, try std.math.add(usize, q_count, try std.math.mul(usize, kv_count, 2)), 2));
        try require(qw, @as(usize, dim) * 2);
        try require(kw, @as(usize, dim) * 2);
        if (vision) try require(positions, try std.math.mul(usize, try std.math.mul(usize, rows, q_heads), 8)) else if (mask.ptr != 0) try require(mask, @as(usize, rows) * 8);
        var qp = q.ptr;
        var kp = k.ptr;
        var vp = v.ptr;
        var ip = input.ptr;
        var qwp = qw.ptr;
        var kwp = kw.ptr;
        var mp = mask.ptr;
        var pp = positions.ptr;
        var r = rows;
        var seq = sequence;
        var qh = q_heads;
        var kh = kv_heads;
        var d = dim;
        var e = eps;
        var t = theta;
        var mode: u32 = @intFromBool(vision);
        var scale = q_scale;
        var args = [_]?*anyopaque{ @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&ip), @ptrCast(&qwp), @ptrCast(&kwp), @ptrCast(&mp), @ptrCast(&pp), @ptrCast(&r), @ptrCast(&seq), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&e), @ptrCast(&t), @ptrCast(&mode), @ptrCast(&scale) };
        try launch(ctx, self.packed_heads, &args, .{ rows, q_heads + 2 * kv_heads, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.packed_head_launches += 1;
    }
    pub fn noteShortCudnnAttention(self: *Module) void {
        self.stats_.short_cudnn_attention_launches += 1;
    }
    pub fn launchSpatialPool(self: *Module, ctx: *Context, output: Buffer, input: Buffer, bias: Buffer, scale: Buffer, rows: u32, gx: u32, gy: u32, merge: u32, dim: u32, factor: f32) !void {
        if (gx == 0 or gy == 0 or merge == 0 or dim == 0 or gx % merge != 0 or gy % merge != 0 or !std.math.isFinite(factor)) return error.InvalidEmbeddingGemma2Shape;
        const frame_rows = try std.math.mul(usize, gx / merge, gy / merge);
        if (rows == 0 or rows % frame_rows != 0) return error.InvalidEmbeddingGemma2Shape;
        const params = @import("../ops.zig").SpatialPoolParams{ .batch = @intCast(rows / frame_rows), .grid_x = gx, .grid_y = gy, .merge = merge, .dim = dim };
        try params.validate();
        try require(input, @as(usize, params.batch) * gx * gy * dim * 4);
        try require(output, @as(usize, rows) * dim * 4);
        if (bias.ptr != 0) {
            try require(bias, @as(usize, dim) * 4);
            try require(scale, @as(usize, dim) * 4);
        }
        var op = output.ptr;
        var ip = input.ptr;
        var bp = bias.ptr;
        var sp = scale.ptr;
        var r = rows;
        var x = gx;
        var y = gy;
        var m = merge;
        var d = dim;
        var f = factor;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&bp), @ptrCast(&sp), @ptrCast(&r), @ptrCast(&x), @ptrCast(&y), @ptrCast(&m), @ptrCast(&d), @ptrCast(&f) };
        try self.launch1d(ctx, self.spatial_pool, &args, @as(usize, rows) * dim);
        self.stats_.spatial_pool_launches += 1;
    }

    pub fn launchSpatialPoolGemmPhase(self: *Module, ctx: *Context, output: Buffer, input: Buffer, bias: Buffer, scale: Buffer, plan: @import("spatial_pool_f32.zig").Plan, phase: @import("spatial_pool_f32.zig").Phase) !void {
        if (!std.meta.eql(plan, try @import("spatial_pool_f32.zig").Plan.resolve(plan.params))) return error.InvalidShape;
        const count = plan.phaseElements(phase);
        try require(output, count * 4);
        if (phase == .input) try require(input, plan.input.bytes);
        if (phase == .publish) try require(input, plan.result.bytes);
        if (bias.ptr != 0) {
            try require(bias, @as(usize, plan.params.dim) * 4);
            try require(scale, @as(usize, plan.params.dim) * 4);
        }
        try @import("buffer.zig").validateRegions(4, .{ input, bias, scale, output }, .{
            switch (phase) {
                .weights => 0,
                .input => plan.input.bytes,
                .publish => count * 4,
            },
            if (bias.ptr != 0) @as(usize, plan.params.dim) * 4 else 0,
            if (bias.ptr != 0) @as(usize, plan.params.dim) * 4 else 0,
            count * 4,
        }, .{ 4, 4, 4, 4 }, 3);
        var op = output.ptr;
        var ip = input.ptr;
        var bp = bias.ptr;
        var sp = scale.ptr;
        var physical: u32 = @intCast(plan.physical);
        var pooled: u32 = @intCast(plan.pooled);
        var gx = plan.params.grid_x;
        var gy = plan.params.grid_y;
        var merge = plan.params.merge;
        var dim = plan.params.dim;
        var mode: u32 = @backingInt(phase);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&bp), @ptrCast(&sp), @ptrCast(&physical), @ptrCast(&pooled), @ptrCast(&gx), @ptrCast(&gy), @ptrCast(&merge), @ptrCast(&dim), @ptrCast(&mode) };
        try self.launch1d(ctx, self.spatial_pool_gemm_phases, &args, count);
        self.stats_.spatial_pool_launches += 1;
    }

    pub fn launchPackedGelu(self: *Module, ctx: *Context, output: Buffer, input: Buffer, rows: u32, width: u32) !void {
        if (rows == 0 or width == 0) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, rows, width);
        try require(output, try std.math.mul(usize, count, 2));
        try require(input, try std.math.mul(usize, count, 4));
        var op = output.ptr;
        var ip = input.ptr;
        var r = rows;
        var w = width;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&r), @ptrCast(&w) };
        try self.launch1d(ctx, self.packed_gelu, &args, count);
        self.stats_.packed_gelu_launches += 1;
    }

    pub fn launchNormResidual(self: *Module, ctx: *Context, hidden: Buffer, next: Buffer, branch: Buffer, post_weight: Buffer, next_weight: ?Buffer, scalar: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        if (rows == 0 or width == 0 or !std.math.isFinite(eps) or eps <= 0) return error.InvalidEmbeddingGemma2Shape;
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), 2);
        try require(hidden, bytes);
        try require(branch, bytes);
        try require(post_weight, @as(usize, width) * 2);
        if (next_weight) |weight| {
            try require(weight, @as(usize, width) * 2);
            try require(next, bytes);
            if (next.ptr == hidden.ptr) return error.InvalidEmbeddingGemma2Shape;
        }
        if (scalar) |weight| try require(weight, 2);
        var hp = hidden.ptr;
        var np = next.ptr;
        var bp = branch.ptr;
        var pp = post_weight.ptr;
        var wp = if (next_weight) |weight| weight.ptr else 0;
        var sp = if (scalar) |weight| weight.ptr else 0;
        var r = rows;
        var w = width;
        var e = eps;
        var args = [_]?*anyopaque{ @ptrCast(&hp), @ptrCast(&np), @ptrCast(&bp), @ptrCast(&pp), @ptrCast(&wp), @ptrCast(&sp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&e) };
        try launch(ctx, self.norm_residual, &args, .{ rows, 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.fused_backbone_launches += 1;
    }

    pub fn launchNativeVision(self: *Module, ctx: *Context, output: Buffer, q: Buffer, k: Buffer, v: Buffer, batch: usize, sequence: usize) !void {
        if (batch == 0 or batch > 8 or sequence == 0 or sequence > 3136 or !@import("artifact.zig").hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.InvalidEmbeddingGemma2Shape;
        const bytes = batch * sequence * 768 * 2;
        try require(output, bytes);
        try require(q, bytes);
        try require(k, bytes);
        try require(v, bytes);
        var op = output.ptr;
        var qp = q.ptr;
        var kp = k.ptr;
        var vp = v.ptr;
        var b: u32 = @intCast(batch);
        var seq: u32 = @intCast(sequence);
        var heads: u32 = 12;
        var dim: u32 = 64;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&b), @ptrCast(&seq), @ptrCast(&heads), @ptrCast(&dim) };
        try launch(ctx, self.native_vision, &args, .{ @intCast(batch * 12 * ((sequence + 31) / 32)), 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.attention_launches += 1;
    }

    pub fn launchSourceNativeVision(self: *Module, ctx: *Context, output: Buffer, q: Buffer, k: Buffer, v: Buffer, batch: usize, physical_sequence: usize, valid_sequence: usize) !void {
        if (batch == 0 or batch > 8 or physical_sequence == 0 or physical_sequence > 3136 or valid_sequence == 0 or valid_sequence > physical_sequence) return error.InvalidEmbeddingGemma2Shape;
        const bytes = batch * physical_sequence * 768 * @sizeOf(u16);
        try @import("buffer.zig").validateRegions(4, .{ q, k, v, output }, @splat(bytes), @splat(2), 3);
        if (!@import("artifact.zig").hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.CudaKernelUnavailable;
        var arguments: @import("launch_abi.zig").ArgumentsFor("ATTENTION_BF16_BSHD_NATIVE_MASKED_M32N16") = .{
            .dst = output.ptr,
            .q = q.ptr,
            .k = k.ptr,
            .v = v.ptr,
            .batch = @intCast(batch),
            .seq_len = @intCast(physical_sequence),
            .valid_sequence = @intCast(valid_sequence),
            .num_heads = 12,
            .head_dim = 64,
        };
        var params = @import("launch_abi.zig").fieldsFor("ATTENTION_BF16_BSHD_NATIVE_MASKED_M32N16", &arguments);
        try launch(ctx, self.source_native_vision, &params, .{ @intCast(batch * 12 * ((physical_sequence + 31) / 32)), 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.attention_launches += 1;
    }

    /// Resolve the new optional instance only on an explicitly selected path,
    /// before any graph capture. Old artifact bundles keep ordinary routes.
    pub fn prepareBf16BshdPrefixRfAttention(self: *Module, ctx: *Context) !void {
        if (self.bf16_bshd_prefix_rf != null) return;
        if (ctx.debug_graph_capture_active or try ctx.streamCaptureActive()) return error.CudaGraphCaptureUnsafeMatmulPlan;
        if (!artifact.hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.CudaKernelUnavailable;
        self.bf16_bshd_prefix_rf = try function(ctx, self.module, "antfly_attention_bf16_bshd_prefix_rf_q64_k64_hd64");
    }

    pub fn launchBf16BshdPrefixRfAttention(self: *Module, ctx: *Context, output: Buffer, q: Buffer, k: Buffer, v: Buffer, program: Bf16BshdPrefixRfAttention) !void {
        // Reject geometry, extents and aliasing before SDK/provider access.
        try program.validateBuffers(output, q, k, v);
        const shared = try program.sharedMemoryBytes();
        try self.prepareBf16BshdPrefixRfAttention(ctx);
        var arguments: @import("launch_abi.zig").ArgumentsFor("ATTENTION_BF16_BSHD_PREFIX_RF_Q64_K64_HD64") = .{
            .dst = output.ptr,
            .q = q.ptr,
            .k = k.ptr,
            .v = v.ptr,
            .batch = program.batch,
            .physical_sequence = program.physical_sequence,
            .query_sequence = program.query_sequence,
            .key_sequence = program.key_sequence,
            .heads = program.heads,
            .score_scale = program.score_scale,
        };
        var params = @import("launch_abi.zig").fieldsFor("ATTENTION_BF16_BSHD_PREFIX_RF_Q64_K64_HD64", &arguments);
        try launchShared(ctx, self.bf16_bshd_prefix_rf, &params, .{ program.heads, (program.physical_sequence + Bf16BshdPrefixRfAttention.query_tile - 1) / Bf16BshdPrefixRfAttention.query_tile, program.batch }, .{ Bf16BshdPrefixRfAttention.threads, 1, 1 }, shared);
        self.stats_.launches += 1;
        self.stats_.attention_launches += 1;
        self.stats_.bf16_bshd_prefix_rf_launches += 1;
    }

    pub fn launchSourceVisionRopePack(self: *Module, ctx: *Context, qo: Buffer, ko: Buffer, q: Buffer, k: Buffer, positions: Buffer, count: u32, theta: f32, scale: f32) !void {
        if (count == 0 or count % 32 != 0 or theta != 100 or !std.math.isFinite(scale)) return error.InvalidEmbeddingGemma2Shape;
        const bytes = try std.math.mul(usize, count, @sizeOf(u16));
        try @import("buffer.zig").validateRegions(5, .{ q, k, positions, qo, ko }, .{ bytes, bytes, try std.math.mul(usize, count / 32, @sizeOf(i32)), bytes, bytes }, .{ 2, 2, 4, 2, 2 }, 3);
        var arguments: @import("launch_abi.zig").ArgumentsFor("ROTARY_PAIR_I32_BF16_SOURCE") = .{ .qo = qo.ptr, .ko = ko.ptr, .q = q.ptr, .k = k.ptr, .positions = positions.ptr, .count = count, .theta = theta, .scale = scale };
        var params = @import("launch_abi.zig").fieldsFor("ROTARY_PAIR_I32_BF16_SOURCE", &arguments);
        try self.launch1d(ctx, self.source_vision_rope_pack, &params, count);
    }

    pub fn launchRmsNorm(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        return self.launchRmsNormScheduled(ctx, out, input, weight, rows, rows, width, eps, .warp_block_256);
    }

    /// Historical training adapter. Its qualified widths and launch geometry
    /// stay unchanged; serving can select the same numerical schedule explicitly.
    pub fn launchTrainingRmsNorm(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        return self.launchRmsNormScheduled(ctx, out, input, weight, rows, rows, width, eps, .contiguous_four_lane);
    }

    /// Reuse retained native programs without changing their arithmetic or ABI.
    /// Physical rows bound input/output storage; reference rows retain the source
    /// tensor's reduction grouping before any physical sequence padding.
    /// Equal input/output addresses are valid: each row is fully reduced before
    /// the barrier preceding its disjoint per-column reads and stores.
    pub fn launchRmsNormScheduled(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, physical_rows: u32, reference_rows: u32, width: u32, eps: f32, schedule: RmsReductionSchedule) !void {
        return self.launchRmsNormStorage(ctx, out, input, weight, physical_rows, reference_rows, width, eps, schedule, .bf16);
    }

    /// F32 physical storage whose values cross the explicit BF16 source nodes.
    /// Existing F32 RMS operations retain their original publication policy.
    pub fn launchSourceRmsNormBf16ValuesF32(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        return self.launchRmsNormStorage(ctx, out, input, weight, rows, rows, width, eps, .source_contiguous, .bf16_values_f32);
    }

    const RmsStorage = enum { bf16, bf16_values_f32 };
    fn launchRmsNormStorage(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, physical_rows: u32, reference_rows: u32, width: u32, eps: f32, schedule: RmsReductionSchedule, comptime storage: RmsStorage) !void {
        if (!std.math.isFinite(eps) or eps <= 0) return error.InvalidEmbeddingGemma2Shape;
        const geometry = @import("../kernel_geometry.zig").RmsReductionLaunch.init(schedule, physical_rows, reference_rows, width) catch return error.InvalidEmbeddingGemma2Shape;
        const value_bytes: usize = if (storage == .bf16) @sizeOf(u16) else @sizeOf(f32);
        const bytes = try std.math.mul(usize, try std.math.mul(usize, physical_rows, width), value_bytes);
        try require(out, bytes);
        try require(input, bytes);
        if (weight) |wbuf| try require(wbuf, try std.math.mul(usize, width, value_bytes));
        var op = out.ptr;
        var ip = input.ptr;
        var wp = if (weight) |wbuf| wbuf.ptr else 0;
        var r = physical_rows;
        var w = width;
        var e = eps;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&e) };
        const program = if (storage == .bf16_values_f32) self.source_rms_norm_values_f32 else switch (schedule) {
            .warp_block_256 => self.rms_norm,
            // The historical export name is retained as a compatibility ABI.
            .contiguous_four_lane => self.training_rms_norm,
            .source_contiguous => self.source_rms_norm,
        };
        try launch(ctx, program, &args, geometry.grid, geometry.block);
        self.stats_.launches += 1;
    }

    pub fn launchRmsNormInputVjp(self: *Module, ctx: *Context, dx: Buffer, dy: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        if (rows == 0 or width == 0 or !std.math.isFinite(eps) or eps <= 0) return error.InvalidEmbeddingGemma2Shape;
        const count = std.math.mul(usize, rows, width) catch return error.InvalidEmbeddingGemma2Shape;
        try require(dx, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(dy, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(input, try std.math.mul(usize, count, @sizeOf(u16)));
        if (weight) |wbuf| try require(wbuf, try std.math.mul(usize, width, @sizeOf(u16)));
        var dxp = dx.ptr;
        var dyp = dy.ptr;
        var xp = input.ptr;
        var wp = if (weight) |wbuf| wbuf.ptr else 0;
        var r = rows;
        var w = width;
        var e = eps;
        var args = [_]?*anyopaque{ @ptrCast(&dxp), @ptrCast(&dyp), @ptrCast(&xp), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&e) };
        try launch(ctx, self.rms_norm_input_vjp, &args, .{ rows, 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchTrainingRmsNormInputVjp(self: *Module, ctx: *Context, dx: Buffer, dy: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        if (rows == 0 or (width != 256 and width != 512) or !std.math.isFinite(eps) or eps <= 0) return error.InvalidEmbeddingGemma2Shape;
        const count = try std.math.mul(usize, rows, width);
        try require(dx, count * 4);
        try require(dy, count * 4);
        try require(input, count * 2);
        if (weight) |wbuf| try require(wbuf, @as(usize, width) * 2);
        const height: u32 = @max(1, std.math.floorPowerOfTwo(u32, @min(rows, 16)));
        const block_width: u32 = @min(width / 4, 512 / height);
        var dxp = dx.ptr;
        var dyp = dy.ptr;
        var xp = input.ptr;
        var wp = if (weight) |wbuf| wbuf.ptr else 0;
        var r = rows;
        var w = width;
        var e = eps;
        var args = [_]?*anyopaque{ @ptrCast(&dxp), @ptrCast(&dyp), @ptrCast(&xp), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&e) };
        try launch(ctx, self.training_rms_norm_input_vjp, &args, .{ std.math.divCeil(u32, rows, height) catch return error.InvalidEmbeddingGemma2Shape, 1, 1 }, .{ block_width, height, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchRope(self: *Module, ctx: *Context, q: Buffer, k: Buffer, mask: Buffer, shape: Attention, theta: f32, rope_scale: f32) !void {
        try shape.validate();
        if (!std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(rope_scale) or rope_scale <= 0) return error.InvalidEmbeddingGemma2Shape;
        try require(q, @as(usize, shape.batch) * shape.seq * shape.q_heads * shape.head_dim * 2);
        try require(k, @as(usize, shape.batch) * shape.seq * shape.kv_heads * shape.head_dim * 2);
        if (mask.ptr != 0) try require(mask, @as(usize, shape.batch) * shape.seq * 8);
        var qp = q.ptr;
        var kp = k.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var qh = shape.q_heads;
        var kh = shape.kv_heads;
        var d = shape.head_dim;
        var t = theta;
        var rs = rope_scale;
        var args = [_]?*anyopaque{ @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&t), @ptrCast(&rs) };
        const token_rows = try std.math.mul(u32, shape.batch, shape.seq);
        try launch(ctx, self.rope, &args, .{ token_rows, (shape.head_dim / 2 + 127) / 128, shape.q_heads }, .{ 128, 1, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchRopeVjp(self: *Module, ctx: *Context, dq: Buffer, dk: Buffer, dq_rotated: Buffer, dk_rotated: Buffer, mask: Buffer, shape: Attention, theta: f32, rope_scale: f32) !void {
        try shape.validate();
        if (!std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(rope_scale) or rope_scale <= 0) return error.InvalidEmbeddingGemma2Shape;
        const q_count = std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), try std.math.mul(usize, shape.q_heads, shape.head_dim)) catch return error.InvalidEmbeddingGemma2Shape;
        const kv_count = std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), try std.math.mul(usize, shape.kv_heads, shape.head_dim)) catch return error.InvalidEmbeddingGemma2Shape;
        for ([_]Buffer{ dq, dq_rotated }) |buffer| try require(buffer, try std.math.mul(usize, q_count, @sizeOf(f32)));
        for ([_]Buffer{ dk, dk_rotated }) |buffer| try require(buffer, try std.math.mul(usize, kv_count, @sizeOf(f32)));
        if (mask.ptr != 0) try require(mask, try std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), @sizeOf(i64)));
        var dqp = dq.ptr;
        var dkp = dk.ptr;
        var dqrp = dq_rotated.ptr;
        var dkrp = dk_rotated.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var qh = shape.q_heads;
        var kh = shape.kv_heads;
        var d = shape.head_dim;
        var t = theta;
        var rs = rope_scale;
        var args = [_]?*anyopaque{ @ptrCast(&dqp), @ptrCast(&dkp), @ptrCast(&dqrp), @ptrCast(&dkrp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&t), @ptrCast(&rs) };
        const token_rows = try std.math.mul(u32, shape.batch, shape.seq);
        try launch(ctx, self.rope_vjp, &args, .{ token_rows, (shape.head_dim / 2 + 127) / 128, shape.q_heads }, .{ 128, 1, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchTrainingRope(self: *Module, ctx: *Context, q: Buffer, k: Buffer, mask: Buffer, shape: Attention, theta: f32, rope_scale: f32) !void {
        try shape.validate();
        if (!std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(rope_scale) or rope_scale <= 0) return error.InvalidEmbeddingGemma2Shape;
        try require(q, @as(usize, shape.batch) * shape.seq * shape.q_heads * shape.head_dim * 2);
        try require(k, @as(usize, shape.batch) * shape.seq * shape.kv_heads * shape.head_dim * 2);
        if (mask.ptr != 0) try require(mask, @as(usize, shape.batch) * shape.seq * 8);
        var qp = q.ptr;
        var kp = k.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var qh = shape.q_heads;
        var kh = shape.kv_heads;
        var d = shape.head_dim;
        var t = theta;
        var rs = rope_scale;
        var args = [_]?*anyopaque{ @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&t), @ptrCast(&rs) };
        const token_rows = try std.math.mul(u32, shape.batch, shape.seq);
        try launch(ctx, self.training_rope, &args, .{ token_rows, (shape.head_dim / 2 + 127) / 128, shape.q_heads }, .{ 128, 1, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchTrainingRopeVjp(self: *Module, ctx: *Context, dq: Buffer, dk: Buffer, dq_rotated: Buffer, dk_rotated: Buffer, mask: Buffer, shape: Attention, theta: f32, rope_scale: f32) !void {
        try shape.validate();
        if (!std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(rope_scale) or rope_scale <= 0) return error.InvalidEmbeddingGemma2Shape;
        const q_count = std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), try std.math.mul(usize, shape.q_heads, shape.head_dim)) catch return error.InvalidEmbeddingGemma2Shape;
        const kv_count = std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), try std.math.mul(usize, shape.kv_heads, shape.head_dim)) catch return error.InvalidEmbeddingGemma2Shape;
        for ([_]Buffer{ dq, dq_rotated }) |buffer| try require(buffer, try std.math.mul(usize, q_count, @sizeOf(f32)));
        for ([_]Buffer{ dk, dk_rotated }) |buffer| try require(buffer, try std.math.mul(usize, kv_count, @sizeOf(f32)));
        if (mask.ptr != 0) try require(mask, try std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), @sizeOf(i64)));
        var dqp = dq.ptr;
        var dkp = dk.ptr;
        var dqrp = dq_rotated.ptr;
        var dkrp = dk_rotated.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var qh = shape.q_heads;
        var kh = shape.kv_heads;
        var d = shape.head_dim;
        var t = theta;
        var rs = rope_scale;
        var args = [_]?*anyopaque{ @ptrCast(&dqp), @ptrCast(&dkp), @ptrCast(&dqrp), @ptrCast(&dkrp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&t), @ptrCast(&rs) };
        const token_rows = try std.math.mul(u32, shape.batch, shape.seq);
        try launch(ctx, self.training_rope_vjp, &args, .{ token_rows, (shape.head_dim / 2 + 127) / 128, shape.q_heads }, .{ 128, 1, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchSplitColumns(self: *Module, ctx: *Context, outputs: [3]Buffer, input: Buffer, rows: u32, widths: [3]u32) !void {
        const width = try std.math.add(u32, widths[0], try std.math.add(u32, widths[1], widths[2]));
        const count = try std.math.mul(u32, rows, width);
        if (rows == 0 or widths[0] == 0 or widths[1] == 0) return error.InvalidEmbeddingGemma2Shape;
        try require(input, @as(usize, count) * 2);
        for (outputs, widths) |output, w| try require(output, @as(usize, rows) * w * 2);
        var pointers = [_]usize{ outputs[0].ptr, outputs[1].ptr, outputs[2].ptr, input.ptr };
        var sizes = [_]u32{ rows, widths[0], widths[1], widths[2] };
        var args: [8]?*anyopaque = undefined;
        for (&pointers, 0..) |*ptr, i| args[i] = ptr;
        for (&sizes, 4..) |*size, i| args[i] = size;
        try self.launch1d(ctx, self.split_columns, &args, count);
    }

    pub fn launchVisionRopePack(self: *Module, ctx: *Context, qo: Buffer, ko: Buffer, q: Buffer, k: Buffer, positions: Buffer, count: u32, theta: f32, scale: f32) !void {
        if (count == 0 or count % 64 != 0 or !std.math.isFinite(theta) or theta <= 0 or !std.math.isFinite(scale)) return error.InvalidEmbeddingGemma2Shape;
        for ([_]Buffer{ qo, ko, q, k }) |buffer| try require(buffer, @as(usize, count) * 2);
        try require(positions, @as(usize, count / 32) * 4);
        var pointers = [_]usize{ qo.ptr, ko.ptr, q.ptr, k.ptr, positions.ptr };
        var n = count;
        var base = theta;
        var multiplier = scale;
        var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &pointers[4], &n, &base, &multiplier };
        try self.launch1d(ctx, self.vision_rope_pack, &args, count);
    }

    pub fn launchAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, mask: Buffer, program: AttentionProgram, workspace: Buffer) !void {
        try program.validate();
        if (artifact.hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor))
            return self.launchLocalFlashAttention(ctx, out, q, k, v, mask, program, workspace);
        return self.launchLegacyAttention(ctx, out, q, k, v, mask, program.shape);
    }

    // Retain the original non-WMMA receiver and its numerical contract for
    // legacy deployment profiles; do not use it as the new path's own oracle.
    pub fn launchLegacyAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, mask: Buffer, shape: Attention) !void {
        try shape.validate();
        const qb = @as(usize, shape.batch) * shape.seq * shape.q_heads * shape.head_dim * 2;
        const kvb = @as(usize, shape.batch) * shape.seq * shape.kv_heads * shape.head_dim * 2;
        try require(out, qb);
        try require(q, qb);
        try require(k, kvb);
        try require(v, kvb);
        if (mask.ptr != 0) try require(mask, @as(usize, shape.batch) * shape.seq * 8);
        var op = out.ptr;
        var qp = q.ptr;
        var kp = k.ptr;
        var vp = v.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var qh = shape.q_heads;
        var kh = shape.kv_heads;
        var d = shape.head_dim;
        var radius = shape.local_radius;
        var query_offset: u32 = 0;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&radius), @ptrCast(&query_offset) };
        try launch(ctx, self.attention, &args, .{ shape.seq, shape.q_heads, shape.batch }, .{ 32, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.attention_launches += 1;
    }

    /// Kernel ABI: existing parameters0..15 are unchanged; the optional stable
    /// logical metadata device pointer is appended at16. Null is legacy math.
    pub const local_attention_parameter_count = 17;
    pub const local_attention_metadata_parameter = 16;
    pub fn launchLocalFlashAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, mask: Buffer, program: AttentionProgram, workspace: Buffer) !void {
        try program.validate();
        const shape = program.shape;
        if (!localFlashAttentionGeometrySupported(shape)) return error.InvalidEmbeddingGemma2Shape;
        const qb = try std.math.mul(usize, @as(usize, shape.batch) * shape.seq * shape.q_heads, shape.head_dim * 2);
        const kvb = try std.math.mul(usize, @as(usize, shape.batch) * shape.seq * shape.kv_heads, shape.head_dim * 2);
        try require(out, qb);
        try require(q, qb);
        try require(k, kvb);
        try require(v, kvb);
        if (mask.ptr != 0) try require(mask, @as(usize, shape.batch) * shape.seq * 8);
        const required_workspace = try program.workspace();
        try require(workspace, required_workspace.total_bytes);
        const output_bytes = required_workspace.output_bytes;
        const lse_bytes = required_workspace.total_bytes - required_workspace.lse_offset;
        const partial_output = Buffer{ .ptr = if (output_bytes == 0) 0 else workspace.ptr, .len = output_bytes };
        const partial_lse = Buffer{ .ptr = if (lse_bytes == 0) 0 else try std.math.add(u64, workspace.ptr, required_workspace.lse_offset), .len = lse_bytes };
        try @import("buffer.zig").validateRegions(8, .{ q, k, v, mask, out, partial_output, partial_lse, program.logical_metadata }, .{ qb, kvb, kvb, if (mask.ptr != 0) @as(usize, shape.batch) * shape.seq * 8 else 0, qb, output_bytes, lse_bytes, if (program.logical_metadata.ptr != 0) 8 else 0 }, .{ 2, 2, 2, 8, 2, 256, 4, 4 }, 4);

        var op = out.ptr;
        var qp = q.ptr;
        var kp = k.ptr;
        var vp = v.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var seq = shape.seq;
        var source_seq = program.source_sequence;
        var qh = shape.q_heads;
        var kh = shape.kv_heads;
        var d = shape.head_dim;
        var radius = shape.local_radius;
        var schedule = @backingInt(program.probability_schedule);
        var partial = partial_output.ptr;
        var lse = partial_lse.ptr;
        var splits = program.split_count;
        var logical_metadata = program.logical_metadata.ptr;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&seq), @ptrCast(&source_seq), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&radius), @ptrCast(&schedule), @ptrCast(&partial), @ptrCast(&lse), @ptrCast(&splits), @ptrCast(&logical_metadata) };
        comptime std.debug.assert(local_attention_metadata_parameter + 1 == local_attention_parameter_count);
        std.debug.assert(args.len == local_attention_parameter_count);
        const shared = try program.sharedMemoryBytes();
        const entry = if (program.probability_schedule == .efficient_bf16 and shape.head_dim == 256) self.efficient_attention_hd256 else self.local_flash_attention;
        try launchShared(ctx, entry, &args, .{ shape.q_heads, (shape.seq + 15) / 16, try std.math.mul(u32, shape.batch, splits) }, .{ 128, 1, 1 }, shared);
        self.stats_.launches += 1;
        if (splits > 1) {
            var merge_args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&partial), @ptrCast(&lse), @ptrCast(&b), @ptrCast(&seq), @ptrCast(&qh), @ptrCast(&d), @ptrCast(&splits) };
            try launch(ctx, self.attention_split_merge, &merge_args, .{ try std.math.mul(u32, try std.math.mul(u32, shape.batch, shape.q_heads), shape.seq), 1, 1 }, .{ 32, 1, 1 });
            self.stats_.launches += 1;
        }
        self.stats_.attention_launches += 1;
    }

    fn localFlashAttentionGeometrySupported(shape: Attention) bool {
        return shape.q_heads == 4 and ((shape.kv_heads == 2 and shape.head_dim == 256) or (shape.kv_heads == 1 and shape.head_dim == 512));
    }

    pub fn launchAttentionSoftmax(self: *Module, ctx: *Context, probs: Buffer, scores: Buffer, mask: Buffer, sequence: u32, query_base: u32, rows: u32, batch_index: u32, heads: u32, tile_capacity: u32) !void {
        if (sequence == 0 or sequence > 8192 or tile_capacity == 0 or tile_capacity > 2048 or rows == 0 or rows > tile_capacity or query_base > sequence or rows > sequence - query_base or heads == 0 or heads > 64) return error.InvalidEmbeddingGemma2Shape;
        const elements = try std.math.mul(usize, try std.math.mul(usize, heads, tile_capacity), sequence);
        try require(scores, try std.math.mul(usize, elements, 4));
        try require(probs, try std.math.mul(usize, elements, 2));
        if (mask.ptr != 0) try require(mask, (@as(usize, batch_index) + 1) * sequence * 8);
        // Scores/mask are read-only until normalization finishes. Writable
        // BF16 probabilities cannot alias either input, including partial views.
        try @import("buffer.zig").validateRegions(3, .{ scores, mask, probs }, .{
            try std.math.mul(usize, elements, 4),
            if (mask.ptr == 0) 0 else try std.math.mul(usize, try std.math.mul(usize, @as(usize, batch_index) + 1, sequence), 8),
            try std.math.mul(usize, elements, 2),
        }, .{ 4, 8, 2 }, 2);
        var pp = probs.ptr;
        var sp = scores.ptr;
        var mp = mask.ptr;
        var s = sequence;
        var qb = query_base;
        var r = rows;
        var bi = batch_index;
        var tc = tile_capacity;
        var args = [_]?*anyopaque{ @ptrCast(&pp), @ptrCast(&sp), @ptrCast(&mp), @ptrCast(&s), @ptrCast(&qb), @ptrCast(&r), @ptrCast(&bi), @ptrCast(&tc) };
        try launchShared(ctx, self.attention_softmax, &args, .{ rows * heads, 1, 1 }, .{ 256, 1, 1 }, 256 * 4);
        self.stats_.launches += 1;
    }

    /// Source attention masks only keys. Padded queries remain live, and
    /// probabilities acquire BF16 storage only after F32 normalization.
    pub fn launchSourceAttentionSoftmax(self: *Module, ctx: *Context, probs: Buffer, scores: Buffer, physical_sequence: u32, valid_keys: u32, query_start: u32, query_count: u32, heads: u32, tile_capacity: u32) !void {
        if (physical_sequence == 0 or physical_sequence > 8192 or valid_keys == 0 or valid_keys > physical_sequence or tile_capacity == 0 or tile_capacity > 2048 or query_count == 0 or query_count > tile_capacity or query_start > physical_sequence or query_count > physical_sequence - query_start or heads == 0 or heads > 64) return error.InvalidEmbeddingGemma2Shape;
        const elements = try std.math.mul(usize, try std.math.mul(usize, heads, tile_capacity), physical_sequence);
        try @import("buffer.zig").validateRegions(2, .{ scores, probs }, .{
            try std.math.mul(usize, elements, @sizeOf(f32)),
            try std.math.mul(usize, elements, @sizeOf(u16)),
        }, .{ 4, 2 }, 1);
        var arguments: @import("launch_abi.zig").ArgumentsFor("SOFTMAX_SCORES_BF16_PREFIX") = .{
            .probs = probs.ptr,
            .scores = scores.ptr,
            .sequence = physical_sequence,
            .valid_keys = valid_keys,
            .rows = query_count,
            .tile_capacity = tile_capacity,
        };
        var params = @import("launch_abi.zig").fieldsFor("SOFTMAX_SCORES_BF16_PREFIX", &arguments);
        try launchShared(ctx, self.source_attention_softmax, &params, .{ query_count * heads, 1, 1 }, .{ 256, 1, 1 }, 256 * @sizeOf(f32));
        self.stats_.launches += 1;
    }

    pub fn launchMatmul(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: Buffer, rows: u32, in_dim: u32, out_dim: u32) !void {
        try require(out, @as(usize, rows) * out_dim * 2);
        try require(input, @as(usize, rows) * in_dim * 2);
        try require(weight, @as(usize, out_dim) * in_dim * 2);
        var op = out.ptr;
        var ip = input.ptr;
        var wp = weight.ptr;
        var r = rows;
        var k = in_dim;
        var n = out_dim;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&k), @ptrCast(&n) };
        try launch(ctx, self.matmul, &args, .{ rows, (out_dim + 127) / 128, 1 }, .{ 128, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.scalar_matmul_fallbacks += 1;
    }

    pub inline fn launchGeluPle(self: *Module, ctx: *Context, out: Buffer, gate: Buffer, ple: Buffer, count: u32) !void {
        return self.launchGeluMul(ctx, out, gate, ple, count);
    }

    pub fn launchGeluMul(self: *Module, ctx: *Context, out: Buffer, gate: Buffer, up: Buffer, count: u32) !void {
        const bytes = @as(usize, count) * 2;
        try require(out, bytes);
        try require(gate, bytes);
        try require(up, bytes);
        var op = out.ptr;
        var gp = gate.ptr;
        var uptr = up.ptr;
        var n = count;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&gp), @ptrCast(&uptr), @ptrCast(&n) };
        try self.launch1d(ctx, self.gelu_product, &args, count);
    }

    pub fn launchGeluMulVjp(self: *Module, ctx: *Context, dgate: Buffer, dup: Buffer, dy: Buffer, gate: Buffer, up: Buffer, count: u32) !void {
        if (count == 0) return error.InvalidEmbeddingGemma2Shape;
        const f32_bytes = try std.math.mul(usize, count, @sizeOf(f32));
        const bf16_bytes = try std.math.mul(usize, count, @sizeOf(u16));
        for ([_]Buffer{ dgate, dup, dy }) |buffer| try require(buffer, f32_bytes);
        for ([_]Buffer{ gate, up }) |buffer| try require(buffer, bf16_bytes);
        var dgp = dgate.ptr;
        var dup_ptr = dup.ptr;
        var dyp = dy.ptr;
        var gp = gate.ptr;
        var uptr = up.ptr;
        var n = count;
        var args = [_]?*anyopaque{ @ptrCast(&dgp), @ptrCast(&dup_ptr), @ptrCast(&dyp), @ptrCast(&gp), @ptrCast(&uptr), @ptrCast(&n) };
        try self.launch1d(ctx, self.gelu_mul_vjp, &args, count);
    }

    pub fn launchBf16ToF32(self: *Module, ctx: *Context, out: Buffer, input: Buffer, count: u32) !void {
        try require(out, @as(usize, count) * @sizeOf(f32));
        try require(input, @as(usize, count) * @sizeOf(u16));
        var op = out.ptr;
        var ip = input.ptr;
        var n = count;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&n) };
        try self.launch1d(ctx, self.bf16_to_f32, &args, count);
    }
    pub fn launchF32ToBf16(self: *Module, ctx: *Context, out: Buffer, input: Buffer, count: u32) !void {
        try require(out, @as(usize, count) * @sizeOf(u16));
        try require(input, @as(usize, count) * @sizeOf(f32));
        var op = out.ptr;
        var ip = input.ptr;
        var n = count;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&n) };
        try self.launch1d(ctx, self.f32_to_bf16, &args, count);
    }
    pub fn launchScale(self: *Module, ctx: *Context, out: Buffer, input: Buffer, count: u32, scale_value: f32) !void {
        const bytes = @as(usize, count) * 2;
        try require(out, bytes);
        try require(input, bytes);
        var op = out.ptr;
        var ip = input.ptr;
        var n = count;
        var s = scale_value;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&n), @ptrCast(&s) };
        try self.launch1d(ctx, self.scale, &args, count);
    }
    pub fn launchScaleDevice(self: *Module, ctx: *Context, out: Buffer, input: Buffer, scale_value: Buffer, count: u32) !void {
        const bytes = @as(usize, count) * 2;
        try require(out, bytes);
        try require(input, bytes);
        try require(scale_value, 2);
        var op = out.ptr;
        var ip = input.ptr;
        var sp = scale_value.ptr;
        var n = count;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&sp), @ptrCast(&n) };
        try self.launch1d(ctx, self.scale_device, &args, count);
    }
    pub fn launchResidual(self: *Module, ctx: *Context, out: Buffer, residual: Buffer, branch: Buffer, ple: ?Buffer, count: u32, branch_scale: f32, ple_scale: f32) !void {
        const bytes = @as(usize, count) * 2;
        try require(out, bytes);
        try require(residual, bytes);
        try require(branch, bytes);
        if (ple) |p| try require(p, bytes);
        var op = out.ptr;
        var rp = residual.ptr;
        var bp = branch.ptr;
        var pp = if (ple) |p| p.ptr else 0;
        var n = count;
        var bs = branch_scale;
        var ps = ple_scale;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&rp), @ptrCast(&bp), @ptrCast(&pp), @ptrCast(&n), @ptrCast(&bs), @ptrCast(&ps) };
        try self.launch1d(ctx, self.residual, &args, count);
    }
    pub fn launchResidualVjp(self: *Module, ctx: *Context, dresidual: Buffer, dbranch: Buffer, dout: Buffer, ple: ?Buffer, count: u32, branch_scale: f32, ple_scale: f32) !void {
        if (count == 0 or !std.math.isFinite(branch_scale) or !std.math.isFinite(ple_scale)) return error.InvalidEmbeddingGemma2Shape;
        const f32_bytes = try std.math.mul(usize, count, @sizeOf(f32));
        for ([_]Buffer{ dresidual, dbranch, dout }) |buffer| try require(buffer, f32_bytes);
        if (ple) |p| try require(p, try std.math.mul(usize, count, @sizeOf(u16)));
        var drp = dresidual.ptr;
        var dbp = dbranch.ptr;
        var dop = dout.ptr;
        var pp = if (ple) |p| p.ptr else 0;
        var n = count;
        var bs = branch_scale;
        var ps = ple_scale;
        var args = [_]?*anyopaque{ @ptrCast(&drp), @ptrCast(&dbp), @ptrCast(&dop), @ptrCast(&pp), @ptrCast(&n), @ptrCast(&bs), @ptrCast(&ps) };
        try self.launch1d(ctx, self.residual_vjp, &args, count);
    }
    pub fn launchContrastive(self: *Module, ctx: *Context, probabilities: Buffer, losses: Buffer, dqueries: Buffer, dcandidates: Buffer, queries: Buffer, candidates: Buffer, valid: Buffer, targets: Buffer, shape: Contrastive, loss_scale: f32) !void {
        try shape.validate();
        if (!std.math.isFinite(loss_scale) or loss_scale <= 0) return error.InvalidEmbeddingGemma2TrainingShape;
        const probability_count = try std.math.mul(usize, shape.queries, shape.candidates);
        const query_count = try std.math.mul(usize, shape.queries, shape.width);
        const candidate_count = try std.math.mul(usize, shape.candidates, shape.width);
        try require(probabilities, try std.math.mul(usize, probability_count, @sizeOf(f32)));
        try require(losses, try std.math.mul(usize, shape.queries, @sizeOf(f32)));
        try require(dqueries, try std.math.mul(usize, query_count, @sizeOf(f32)));
        try require(dcandidates, try std.math.mul(usize, candidate_count, @sizeOf(f32)));
        try require(queries, try std.math.mul(usize, query_count, @sizeOf(f32)));
        try require(candidates, try std.math.mul(usize, candidate_count, @sizeOf(f32)));
        if (valid.ptr != 0) try require(valid, probability_count);
        try require(targets, try std.math.mul(usize, shape.queries, @sizeOf(u32)));
        var pp = probabilities.ptr;
        var lp = losses.ptr;
        var dqp = dqueries.ptr;
        var dcp = dcandidates.ptr;
        var qp = queries.ptr;
        var cp = candidates.ptr;
        var vp = valid.ptr;
        var tp = targets.ptr;
        var q = shape.queries;
        var c = shape.candidates;
        var w = shape.width;
        var inverse_temperature: f32 = 1.0 / shape.temperature;
        var objective_weight = shape.objective_weight;
        var probability_args = [_]?*anyopaque{ @ptrCast(&pp), @ptrCast(&lp), @ptrCast(&qp), @ptrCast(&cp), @ptrCast(&vp), @ptrCast(&tp), @ptrCast(&q), @ptrCast(&c), @ptrCast(&w), @ptrCast(&inverse_temperature), @ptrCast(&objective_weight) };
        try launchShared(ctx, self.contrastive_prob, &probability_args, .{ q, 1, 1 }, .{ 256, 1, 1 }, 8 * @sizeOf(f32));
        const gradient_scale = loss_scale * objective_weight * inverse_temperature / @as(f32, @floatFromInt(shape.queries));
        var gs = gradient_scale;
        var query_args = [_]?*anyopaque{ @ptrCast(&dqp), @ptrCast(&pp), @ptrCast(&cp), @ptrCast(&tp), @ptrCast(&q), @ptrCast(&c), @ptrCast(&w), @ptrCast(&gs) };
        try launch(ctx, self.contrastive_query_vjp, &query_args, .{ (w + 255) / 256, q, 1 }, .{ 256, 1, 1 });
        var candidate_args = [_]?*anyopaque{ @ptrCast(&dcp), @ptrCast(&pp), @ptrCast(&qp), @ptrCast(&tp), @ptrCast(&q), @ptrCast(&c), @ptrCast(&w), @ptrCast(&gs) };
        try launch(ctx, self.contrastive_candidate_vjp, &candidate_args, .{ (w + 255) / 256, c, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 3;
    }
    pub fn launchContrastiveCoefficients(self: *Module, ctx: *Context, coefficients: Buffer, losses: Buffer, logits: Buffer, valid: Buffer, targets: Buffer, shape: Contrastive, loss_scale: f32) !void {
        try shape.validate();
        if (!std.math.isFinite(loss_scale) or loss_scale <= 0) return error.InvalidEmbeddingGemma2TrainingShape;
        const count = try std.math.mul(usize, shape.queries, shape.candidates);
        const bytes = try std.math.mul(usize, count, @sizeOf(f32));
        try require(coefficients, bytes);
        try require(logits, bytes);
        try require(losses, try std.math.mul(usize, shape.queries, @sizeOf(f32)));
        if (valid.ptr != 0) try require(valid, count);
        try require(targets, try std.math.mul(usize, shape.queries, @sizeOf(u32)));
        var cp = coefficients.ptr;
        var lp = losses.ptr;
        var ip = logits.ptr;
        var vp = valid.ptr;
        var tp = targets.ptr;
        var q = shape.queries;
        var c = shape.candidates;
        var inverse_temperature: f32 = 1.0 / shape.temperature;
        var objective_weight = shape.objective_weight;
        var scale = loss_scale;
        var args = [_]?*anyopaque{ @ptrCast(&cp), @ptrCast(&lp), @ptrCast(&ip), @ptrCast(&vp), @ptrCast(&tp), @ptrCast(&q), @ptrCast(&c), @ptrCast(&inverse_temperature), @ptrCast(&objective_weight), @ptrCast(&scale) };
        var lanes: u32 = 1;
        while (lanes < c and lanes < 32) lanes *= 2;
        try launchShared(ctx, self.training_contrastive_ce, &args, .{ q, 1, 1 }, .{ lanes, 1, 1 }, 0);
        self.stats_.launches += 1;
    }

    pub fn launchTrainingSoftmax(self: *Module, ctx: *Context, probabilities: Buffer, scores: Buffer, mask: Buffer, shape: Attention) !void {
        try shape.validate();
        if (shape.seq > 512) return error.InvalidEmbeddingGemma2TrainingShape;
        const elements = std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.q_heads), try std.math.mul(usize, shape.seq, shape.seq)) catch return error.InvalidEmbeddingGemma2TrainingShape;
        const bytes = try std.math.mul(usize, elements, @sizeOf(f32));
        try require(probabilities, bytes);
        try require(scores, bytes);
        if (mask.ptr != 0) try require(mask, try std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.seq), @sizeOf(i64)));
        var pp = probabilities.ptr;
        var sp = scores.ptr;
        var mp = mask.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var h = shape.q_heads;
        var radius = shape.local_radius;
        var args = [_]?*anyopaque{ @ptrCast(&pp), @ptrCast(&sp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&h), @ptrCast(&radius) };
        try launchShared(ctx, self.training_softmax, &args, .{ s, h, b }, .{ 32, 1, 1 }, 0);
        self.stats_.launches += 1;
    }
    pub fn launchTrainingSoftmaxVjp(self: *Module, ctx: *Context, dscores: Buffer, dprobabilities: Buffer, probabilities: Buffer, shape: Attention) !void {
        try shape.validate();
        if (shape.seq > 512) return error.InvalidEmbeddingGemma2TrainingShape;
        const elements = std.math.mul(usize, try std.math.mul(usize, shape.batch, shape.q_heads), try std.math.mul(usize, shape.seq, shape.seq)) catch return error.InvalidEmbeddingGemma2TrainingShape;
        const bytes = try std.math.mul(usize, elements, @sizeOf(f32));
        for ([_]Buffer{ dscores, dprobabilities, probabilities }) |buffer| try require(buffer, bytes);
        var dsp = dscores.ptr;
        var dpp = dprobabilities.ptr;
        var pp = probabilities.ptr;
        var b = shape.batch;
        var s = shape.seq;
        var h = shape.q_heads;
        var args = [_]?*anyopaque{ @ptrCast(&dsp), @ptrCast(&dpp), @ptrCast(&pp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&h) };
        try launchShared(ctx, self.training_softmax_vjp, &args, .{ s, h, b }, .{ 32, 1, 1 }, 0);
        self.stats_.launches += 1;
    }
    pub fn launchMeanL2(self: *Module, ctx: *Context, out: Buffer, projected: Buffer, mask: Buffer, batch: u32, seq: u32, width: u32) !void {
        return self.launchMeanL2Impl(ctx, self.mean_l2, out, projected, mask, batch, seq, width);
    }

    pub fn launchTrainingMeanL2(self: *Module, ctx: *Context, out: Buffer, projected: Buffer, mask: Buffer, batch: u32, seq: u32, width: u32) !void {
        if (batch == 0 or batch > 2 or seq == 0 or seq > 512 or width != 768) return error.InvalidEmbeddingGemma2TrainingShape;
        return self.launchMeanL2Impl(ctx, self.training_mean_l2, out, projected, mask, batch, seq, width);
    }

    fn launchMeanL2Impl(self: *Module, ctx: *Context, kernel: driver.CUfunction, out: Buffer, projected: Buffer, mask: Buffer, batch: u32, seq: u32, width: u32) !void {
        try require(out, @as(usize, batch) * width * 4);
        try require(projected, @as(usize, batch) * seq * width * 2);
        if (mask.ptr != 0) try require(mask, @as(usize, batch) * seq * 8);
        var op = out.ptr;
        var pp = projected.ptr;
        var mp = mask.ptr;
        var b = batch;
        var s = seq;
        var w = width;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&pp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&w) };
        try launch(ctx, kernel, &args, .{ batch, 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
        self.stats_.output_bytes += @as(u64, batch) * width * 4;
    }
    pub fn launchMeanL2Vjp(self: *Module, ctx: *Context, dprojected: Buffer, dembedding: Buffer, projected: Buffer, mask: Buffer, batch: u32, seq: u32, width: u32) !void {
        if (batch == 0 or batch > 2 or seq == 0 or seq > 512 or width != 768) return error.InvalidEmbeddingGemma2TrainingShape;
        const projected_elements = std.math.mul(usize, try std.math.mul(usize, batch, seq), width) catch return error.InvalidEmbeddingGemma2TrainingShape;
        try require(dprojected, try std.math.mul(usize, projected_elements, @sizeOf(f32)));
        try require(projected, try std.math.mul(usize, projected_elements, @sizeOf(u16)));
        try require(dembedding, try std.math.mul(usize, try std.math.mul(usize, batch, width), @sizeOf(f32)));
        if (mask.ptr != 0) try require(mask, try std.math.mul(usize, try std.math.mul(usize, batch, seq), @sizeOf(i64)));
        var dp = dprojected.ptr;
        var de = dembedding.ptr;
        var p = projected.ptr;
        var m = mask.ptr;
        var b = batch;
        var s = seq;
        var w = width;
        var args = [_]?*anyopaque{ @ptrCast(&dp), @ptrCast(&de), @ptrCast(&p), @ptrCast(&m), @ptrCast(&b), @ptrCast(&s), @ptrCast(&w) };
        try launchShared(ctx, self.mean_l2_vjp, &args, .{ batch, 1, 1 }, .{ 256, 1, 1 }, 16 * @sizeOf(f32));
        self.stats_.launches += 1;
    }
    pub fn launchScaleDeviceF32Bf16(self: *Module, ctx: *Context, out: Buffer, input: Buffer, scale: Buffer, count: usize) !void {
        if (count == 0 or count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2TrainingShape;
        try require(out, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(input, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(scale, @sizeOf(u16));
        var op = out.ptr;
        var ip = input.ptr;
        var sp = scale.ptr;
        var n: u32 = @intCast(count);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&sp), @ptrCast(&n) };
        try self.launch1d(ctx, self.scale_device_f32_bf16, &args, count);
    }
    pub fn launchAccumulateF32(self: *Module, ctx: *Context, destination: Buffer, source: Buffer, count: usize) !void {
        if (count == 0 or count > std.math.maxInt(u32)) return error.InvalidEmbeddingGemma2TrainingShape;
        const bytes = try std.math.mul(usize, count, @sizeOf(f32));
        try require(destination, bytes);
        try require(source, bytes);
        var dp = destination.ptr;
        var sp = source.ptr;
        var n: u32 = @intCast(count);
        var args = [_]?*anyopaque{ @ptrCast(&dp), @ptrCast(&sp), @ptrCast(&n) };
        try self.launch1d(ctx, self.accumulate_f32, &args, count);
    }

    pub fn launchAccumulateBf16BoundaryF32(self: *Module, ctx: *Context, destination: Buffer, source: Buffer, count: usize) !void {
        const bytes = std.math.mul(usize, count, @sizeOf(f32)) catch return error.InvalidEmbeddingGemma2TrainingShape;
        try require(destination, bytes);
        try require(source, bytes);
        const count32 = std.math.cast(u32, count) orelse return error.InvalidEmbeddingGemma2TrainingShape;
        var dp = destination.ptr;
        var sp = source.ptr;
        var n = count32;
        var args = [_]?*anyopaque{ @ptrCast(&dp), @ptrCast(&sp), @ptrCast(&n) };
        try self.launch1d(ctx, self.accumulate_bf16_boundary_f32, &args, count32);
    }
    pub fn launchPackBshdBf16F32(self: *Module, ctx: *Context, out: Buffer, input: Buffer, batch: u32, sequence: u32, heads: u32, dim: u32) !void {
        const count = try attentionTensorCount(batch, sequence, heads, dim);
        try require(out, try std.math.mul(usize, count, @sizeOf(f32)));
        try require(input, try std.math.mul(usize, count, @sizeOf(u16)));
        var op = out.ptr;
        var ip = input.ptr;
        var b = batch;
        var s = sequence;
        var h = heads;
        var d = dim;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&b), @ptrCast(&s), @ptrCast(&h), @ptrCast(&d) };
        try self.launch1d(ctx, self.pack_bshd_bf16_f32, &args, count);
    }
    pub fn launchTrainingPackBhsdBf16(self: *Module, ctx: *Context, out: Buffer, input: Buffer, batch: u32, sequence: u32, source_heads: u32, target_heads: u32, dim: u32, input_f32: bool) !void {
        if (source_heads == 0 or target_heads % source_heads != 0) return error.InvalidEmbeddingGemma2TrainingShape;
        const source_count = try attentionTensorCount(batch, sequence, source_heads, dim);
        const count = try attentionTensorCount(batch, sequence, target_heads, dim);
        try require(out, try std.math.mul(usize, count, @sizeOf(u16)));
        try require(input, try std.math.mul(usize, source_count, if (input_f32) @sizeOf(f32) else @sizeOf(u16)));
        var op = out.ptr;
        var ip = input.ptr;
        var b = batch;
        var s = sequence;
        var sh = source_heads;
        var th = target_heads;
        var d = dim;
        var f: u32 = @intFromBool(input_f32);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&b), @ptrCast(&s), @ptrCast(&sh), @ptrCast(&th), @ptrCast(&d), @ptrCast(&f) };
        try self.launch1d(ctx, self.training_pack_bhsd_bf16, &args, count);
    }

    pub fn launchTrainingUnpackBhsdBf16(self: *Module, ctx: *Context, out: Buffer, input: Buffer, batch: u32, sequence: u32, source_heads: u32, target_heads: u32, dim: u32, output_f32: bool) !void {
        if (target_heads == 0 or source_heads % target_heads != 0) return error.InvalidEmbeddingGemma2TrainingShape;
        const source_count = try attentionTensorCount(batch, sequence, source_heads, dim);
        const count = try attentionTensorCount(batch, sequence, target_heads, dim);
        try require(input, try std.math.mul(usize, source_count, @sizeOf(u16)));
        try require(out, try std.math.mul(usize, count, if (output_f32) @sizeOf(f32) else @sizeOf(u16)));
        var op = out.ptr;
        var ip = input.ptr;
        var b = batch;
        var s = sequence;
        var sh = source_heads;
        var th = target_heads;
        var d = dim;
        var f: u32 = @intFromBool(output_f32);
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&b), @ptrCast(&s), @ptrCast(&sh), @ptrCast(&th), @ptrCast(&d), @ptrCast(&f) };
        try self.launch1d(ctx, self.training_unpack_bhsd_bf16, &args, count);
    }

    pub fn launchPackBshdF32(self: *Module, ctx: *Context, out: Buffer, input: Buffer, batch: u32, sequence: u32, heads: u32, dim: u32) !void {
        const count = try attentionTensorCount(batch, sequence, heads, dim);
        const bytes = try std.math.mul(usize, count, @sizeOf(f32));
        try require(out, bytes);
        try require(input, bytes);
        var op = out.ptr;
        var ip = input.ptr;
        var b = batch;
        var s = sequence;
        var h = heads;
        var d = dim;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&b), @ptrCast(&s), @ptrCast(&h), @ptrCast(&d) };
        try self.launch1d(ctx, self.pack_bshd_f32, &args, count);
    }
    pub fn launchUnpackBhsdF32(self: *Module, ctx: *Context, out: Buffer, input: Buffer, batch: u32, sequence: u32, heads: u32, dim: u32, bf16: bool) !void {
        const count = try attentionTensorCount(batch, sequence, heads, dim);
        try require(out, try std.math.mul(usize, count, if (bf16) @sizeOf(u16) else @sizeOf(f32)));
        try require(input, try std.math.mul(usize, count, @sizeOf(f32)));
        var op = out.ptr;
        var ip = input.ptr;
        var b = batch;
        var s = sequence;
        var h = heads;
        var d = dim;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&b), @ptrCast(&s), @ptrCast(&h), @ptrCast(&d) };
        try self.launch1d(ctx, if (bf16) self.unpack_bhsd_f32_bf16 else self.unpack_bhsd_f32, &args, count);
    }
    pub fn launchMrlNormalize(self: *Module, ctx: *Context, out: Buffer, input: Buffer, rows: u32, width: u32) !void {
        if (rows == 0 or rows > 384 or width == 0 or width > 768) return error.InvalidEmbeddingGemma2TrainingShape;
        try self.launchPrefixNormalize(ctx, out, input, .{ .rows = rows, .width = width, .input_stride = 768 });
    }
    pub fn launchMrlNormalizeVjp(self: *Module, ctx: *Context, dinput: Buffer, dout: Buffer, input: Buffer, rows: u32, width: u32, objective_weight: f32) !void {
        if (rows == 0 or rows > 384 or width == 0 or width > 768 or !std.math.isFinite(objective_weight)) return error.InvalidEmbeddingGemma2TrainingShape;
        try self.launchPrefixNormalizeVjp(ctx, dinput, dout, input, .{ .rows = rows, .width = width, .input_stride = 768 }, objective_weight);
    }
    pub fn launchPrefixNormalize(self: *Module, ctx: *Context, out: Buffer, input: Buffer, shape: @import("prefix_normalization.zig").Geometry) !void {
        try self.prefix_normalization.normalize(ctx, out, input, shape);
        self.stats_.launches += 1;
    }
    pub fn launchPrefixNormalizeVjp(self: *Module, ctx: *Context, dinput: Buffer, dout: Buffer, input: Buffer, shape: @import("prefix_normalization.zig").Geometry, objective_weight: f32) !void {
        try self.prefix_normalization.accumulateVjp(ctx, dinput, dout, input, shape, objective_weight);
        self.stats_.launches += 1;
    }
    fn launch1d(self: *Module, ctx: *Context, f: driver.CUfunction, args: []?*anyopaque, n: usize) !void {
        if (n == 0) return;
        try launch(ctx, f, args, .{ @intCast((n + 255) / 256), 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
    }
};

fn attentionTensorCount(batch: u32, sequence: u32, heads: u32, dim: u32) !usize {
    if (batch == 0 or batch > 2 or sequence == 0 or sequence > 512 or heads == 0 or heads > 4 or (dim != 256 and dim != 512))
        return error.InvalidEmbeddingGemma2TrainingShape;
    const rows = std.math.mul(usize, batch, sequence) catch return error.InvalidEmbeddingGemma2TrainingShape;
    return std.math.mul(usize, try std.math.mul(usize, rows, heads), dim) catch error.InvalidEmbeddingGemma2TrainingShape;
}

fn function(ctx: *Context, m: driver.CUmodule, name: [:0]const u8) !driver.CUfunction {
    var f: driver.CUfunction = null;
    try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&f, m, name.ptr));
    return f;
}
fn require(b: Buffer, n: usize) !void {
    if ((n != 0 and b.ptr == 0) or b.len < n) return error.InvalidCudaState;
}
fn launch(ctx: *Context, f: driver.CUfunction, args: []?*anyopaque, grid: [3]u32, block: [3]u32) !void {
    try ctx.makeCurrent();
    try ctx.launchKernel(f, grid[0], grid[1], grid[2], block[0], block[1], block[2], 0, ctx.stream, args.ptr, null);
    ctx.noteKernelLaunch();
}
fn launchShared(ctx: *Context, f: driver.CUfunction, args: []?*anyopaque, grid: [3]u32, block: [3]u32, shared: u32) !void {
    try ctx.makeCurrent();
    try ctx.launchKernel(f, grid[0], grid[1], grid[2], block[0], block[1], block[2], shared, ctx.stream, args.ptr, null);
    ctx.noteKernelLaunch();
}

test "EmbeddingGemma 2 attention shape contract" {
    try (Attention{ .batch = 1, .seq = 8192, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 512 }).validate();
    try (Attention{ .batch = 2, .seq = 17, .q_heads = 4, .kv_heads = 1, .head_dim = 512, .local_radius = 0 }).validate();
    try std.testing.expectError(error.InvalidEmbeddingGemma2Shape, (Attention{ .batch = 1, .seq = 8193, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 512 }).validate());
}

test "EmbeddingGemma 2 local flash accepts only supported local and global geometries" {
    const local = Attention{ .batch = 32, .seq = 512, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 512 };
    const global = Attention{ .batch = 32, .seq = 512, .q_heads = 4, .kv_heads = 1, .head_dim = 512, .local_radius = 0 };
    try std.testing.expect(Module.localFlashAttentionGeometrySupported(local));
    try std.testing.expect(Module.localFlashAttentionGeometrySupported(global));
    // The shared BF16 program now supports both window policies for each
    // validated head geometry; schedule selection remains independently typed.
    const clipped_global = Attention{ .batch = 32, .seq = 512, .q_heads = 4, .kv_heads = 1, .head_dim = 512, .local_radius = 512 };
    try clipped_global.validate();
    try std.testing.expect(Module.localFlashAttentionGeometrySupported(clipped_global));
    try std.testing.expectEqual(AttentionSchedule.efficient_bf16, (try AttentionProgram.forResolvedMask(clipped_global, 512, true, 48)).probability_schedule);
    const full_local = Attention{ .batch = 32, .seq = 512, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 0 };
    try full_local.validate();
    try std.testing.expect(Module.localFlashAttentionGeometrySupported(full_local));
    try std.testing.expectEqual(AttentionSchedule.flash_bf16, (try AttentionProgram.forResolvedMask(full_local, 512, true, 48)).probability_schedule);
    try std.testing.expect(!Module.localFlashAttentionGeometrySupported(.{ .batch = 32, .seq = 512, .q_heads = 2, .kv_heads = 1, .head_dim = 512, .local_radius = 0 }));
}

test "EmbeddingGemma 2 resident contrastive objective bounds logical batch" {
    try (Contrastive{ .queries = 64, .candidates = 384, .width = 768, .temperature = 0.05, .objective_weight = 0.25 }).validate();
    try std.testing.expectError(error.InvalidEmbeddingGemma2TrainingShape, (Contrastive{ .queries = 65, .candidates = 384, .width = 768, .temperature = 0.05, .objective_weight = 0.25 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2TrainingShape, (Contrastive{ .queries = 1, .candidates = 1, .width = 768, .temperature = 0, .objective_weight = 1 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2TrainingShape, (Contrastive{ .queries = 1, .candidates = 1, .width = 769, .temperature = 0.05, .objective_weight = 1 }).validate());
}

test "embeddinggemma2 CUDA packed column split preserves BF16 bits and row tails" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    var host: [17 * 11]u16 = undefined;
    for (&host, 0..) |*value, i| value.* = @intCast(i * 17);
    var input = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(host)));
    defer input.free(&ctx);
    try input.copyFromHost(&ctx, std.mem.sliceAsBytes(&host));
    var outputs: [3]Buffer = @splat(.{});
    defer for (&outputs) |*output| output.free(&ctx);
    const widths = [3]u32{ 3, 5, 3 };
    for (&outputs, widths) |*output, width| output.* = try Buffer.alloc(&ctx, 17 * width * 2);
    try module.launchSplitColumns(&ctx, outputs, input, 17, widths);
    var offset: usize = 0;
    for (outputs, widths) |output, width| {
        var values: [17 * 5]u16 = undefined;
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(values[0 .. 17 * width]));
        try ctx.synchronize();
        for (0..17) |row| try std.testing.expectEqualSlices(u16, host[row * 11 + offset ..][0..width], values[row * width ..][0..width]);
        offset += width;
    }
}

test "embeddinggemma2 CUDA extended flash matches scalar attention with mask holes and tile tails" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (!@import("artifact.zig").hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    for ([_]u32{ 64, 65, 129, 512, 513, 1024, 2048, 8192 }) |sequence| {
        for ([_]u32{ 1, 2, 4 }) |batch| {
            for (0..3) |mask_kind| {
                for ([_]bool{ false, true }) |local| {
                    const shape = Attention{ .batch = batch, .seq = sequence, .q_heads = 4, .kv_heads = if (local) 2 else 1, .head_dim = if (local) 256 else 512, .local_radius = if (local) 512 else 0 };
                    const q_count = @as(usize, shape.batch) * sequence * shape.q_heads * shape.head_dim;
                    const kv_count = @as(usize, shape.batch) * sequence * shape.kv_heads * shape.head_dim;
                    const host = try allocator.alloc(u16, q_count);
                    defer allocator.free(host);
                    for (host, 0..) |*value, i| value.* = @truncate(@as(u32, @bitCast(@as(f32, @floatFromInt(i % 31)) / 64 - 0.25)) >> 16);
                    const masks = try allocator.alloc(i64, shape.batch * sequence);
                    defer allocator.free(masks);
                    for (masks, 0..) |*value, i| value.* = @intFromBool(switch (mask_kind) {
                        0 => true,
                        1 => i % sequence < sequence - @as(u32, @intCast(3 + i / sequence)),
                        else => i % 17 != 0 and i % sequence < sequence - 3,
                    });
                    var q = try Buffer.alloc(&ctx, q_count * 2);
                    defer q.free(&ctx);
                    var k = try Buffer.alloc(&ctx, kv_count * 2);
                    defer k.free(&ctx);
                    var v = try Buffer.alloc(&ctx, kv_count * 2);
                    defer v.free(&ctx);
                    var mask = try Buffer.alloc(&ctx, masks.len * 8);
                    defer mask.free(&ctx);
                    var expected = try Buffer.alloc(&ctx, q_count * 2);
                    defer expected.free(&ctx);
                    var actual = try Buffer.alloc(&ctx, q_count * 2);
                    defer actual.free(&ctx);
                    try q.copyFromHost(&ctx, std.mem.sliceAsBytes(host));
                    try k.copyFromHost(&ctx, std.mem.sliceAsBytes(host[0..kv_count]));
                    try v.copyFromHost(&ctx, std.mem.sliceAsBytes(host[0..kv_count]));
                    try mask.copyFromHost(&ctx, std.mem.sliceAsBytes(masks));
                    try ctx.synchronize();
                    const clock = std.testing.io;
                    const reference_start = std.Io.Clock.awake.now(clock);
                    try module.launchLegacyAttention(&ctx, expected, q, k, v, mask, shape);
                    try ctx.synchronize();
                    const reference_ns = std.Io.Clock.awake.now(clock).nanoseconds - reference_start.nanoseconds;
                    const candidate_start = std.Io.Clock.awake.now(clock);
                    try module.launchLocalFlashAttention(&ctx, actual, q, k, v, mask, .{ .shape = shape, .source_sequence = shape.seq, .probability_schedule = AttentionSchedule.forResolvedMask(shape, mask_kind == 0) }, .{});
                    try ctx.synchronize();
                    const candidate_ns = std.Io.Clock.awake.now(clock).nanoseconds - candidate_start.nanoseconds;
                    std.debug.print("embeddinggemma2 attention_qualification batch={} sequence={} local={} mask={} exact_ns={} flash_ns={}\n", .{ batch, sequence, local, mask_kind, reference_ns, candidate_ns });
                    const reference = try allocator.alloc(u16, q_count);
                    defer allocator.free(reference);
                    const result = try allocator.alloc(u16, q_count);
                    defer allocator.free(result);
                    try expected.copyToHost(&ctx, std.mem.sliceAsBytes(reference));
                    try actual.copyToHost(&ctx, std.mem.sliceAsBytes(result));
                    try ctx.synchronize();
                    for (reference, result) |want, got| {
                        const x: f32 = @bitCast(@as(u32, want) << 16);
                        const y: f32 = @bitCast(@as(u32, got) << 16);
                        try std.testing.expectApproxEqAbs(x, y, 0.005);
                    }
                }
            }
        }
    }
}

test "CUDA BF16 GELU product preserves PLE and FFN adapter outputs and tails" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const Round = struct {
        fn bf16(value: f32) u16 {
            const bits: u32 = @bitCast(value);
            return @intCast((bits +% (0x7fff + ((bits >> 16) & 1))) >> 16);
        }
        fn f32Value(bits: u16) f32 {
            return @bitCast(@as(u32, bits) << 16);
        }
    };
    var gates: [1033]u16 = undefined;
    var right: [1033]u16 = undefined;
    const guards: [1033]u16 = @splat(0x4bad);
    for (&gates, &right, 0..) |*gate, *value, i| {
        gate.* = Round.bf16(@as(f32, @floatFromInt(@as(i32, @intCast(i % 37)) - 18)) / 8);
        value.* = Round.bf16(@as(f32, @floatFromInt(@as(i32, @intCast(i % 29)) - 14)) / 16);
    }
    var gate_buffer = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(gates)));
    defer gate_buffer.free(&ctx);
    var right_buffer = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(right)));
    defer right_buffer.free(&ctx);
    var ple = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(guards)));
    defer ple.free(&ctx);
    var ffn = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(guards)));
    defer ffn.free(&ctx);
    try gate_buffer.copyFromHost(&ctx, std.mem.sliceAsBytes(&gates));
    try right_buffer.copyFromHost(&ctx, std.mem.sliceAsBytes(&right));
    for ([_]u32{ 0, 1, 7, 255, 256, 257, 1025 }) |count| {
        try ple.copyFromHost(&ctx, std.mem.sliceAsBytes(&guards));
        try ffn.copyFromHost(&ctx, std.mem.sliceAsBytes(&guards));
        const before = module.stats_.launches;
        try module.launchGeluPle(&ctx, ple, gate_buffer, right_buffer, count);
        try module.launchGeluMul(&ctx, ffn, gate_buffer, right_buffer, count);
        try std.testing.expectEqual(before + (if (count == 0) @as(u64, 0) else 2), module.stats_.launches);
        var first: [1033]u16 = undefined;
        var second: [1033]u16 = undefined;
        try ple.copyToHost(&ctx, std.mem.sliceAsBytes(&first));
        try ffn.copyToHost(&ctx, std.mem.sliceAsBytes(&second));
        try ctx.synchronize();
        try std.testing.expectEqualSlices(u16, &first, &second);
        try std.testing.expectEqualSlices(u16, guards[count..], first[count..]);
        for (first[0..count], 0..) |actual, i| {
            const x: f64 = Round.f32Value(gates[i]);
            const gelu = 0.5 * x * (1.0 + std.math.tanh(0.7978845608028654 * (x + 0.044715 * x * x * x)));
            const rounded = Round.f32Value(Round.bf16(@floatCast(gelu)));
            const expected = Round.f32Value(Round.bf16(rounded * Round.f32Value(right[i])));
            try std.testing.expectApproxEqAbs(expected, Round.f32Value(actual), 0.008 * (1 + @abs(expected)));
        }
    }
}

test "embeddinggemma2 CUDA attention softmax refuses read operand overlap before SDK" {
    var ctx: Context = undefined;
    var module: Module = undefined;
    // No SDK state is touched: each invalid output aliases a declared read.
    const scores: Buffer = .{ .ptr = 4096, .len = 4 * 1024 * 8 };
    const mask: Buffer = .{ .ptr = 65536, .len = 8 * 8 };
    try std.testing.expectError(error.InvalidShape, module.launchAttentionSoftmax(&ctx, .{ .ptr = scores.ptr, .len = 2 * 1024 * 8 }, scores, .{}, 8, 0, 8, 0, 1, 1024));
    try std.testing.expectError(error.InvalidShape, module.launchAttentionSoftmax(&ctx, .{ .ptr = scores.ptr + scores.len - 2, .len = 2 * 1024 * 8 }, scores, .{}, 8, 0, 8, 0, 1, 1024));
    try std.testing.expectError(error.InvalidShape, module.launchAttentionSoftmax(&ctx, .{ .ptr = mask.ptr + 2, .len = 2 * 1024 * 8 }, scores, mask, 8, 0, 8, 0, 1, 1024));
}

test "embeddinggemma2 CUDA attention softmax shared reduction reuse preserves uniform tail rows" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const a = std.testing.allocator;
    const sequence = 846;
    const rows = sequence;
    const heads = 12;
    const tile = 1024;
    const elements = heads * tile * sequence;
    const zero = try a.alloc(f32, elements);
    defer a.free(zero);
    @memset(zero, 0);
    var scores = try Buffer.alloc(&ctx, elements * 4);
    defer scores.free(&ctx);
    var probs = try Buffer.alloc(&ctx, elements * 2);
    defer probs.free(&ctx);
    const actual = try a.alloc(u16, elements);
    defer a.free(actual);
    // Exact CPU oracle: exp(0)==1, integer row sum846, one BF16 rounding.
    // Original arithmetic rounds exponentials before division; ones are exact.
    const inverse: f32 = 1.0 / @as(f32, sequence);
    const bits: u32 = @bitCast(inverse);
    const expected: u16 = @intCast((bits +% (0x7fff + ((bits >> 16) & 1))) >> 16);
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try scores.copyFromHost(&ctx, std.mem.sliceAsBytes(zero));
    for (0..4) |iteration| for (0..4) |batch_index| {
        // Same generic fullB4 family: four serial single-frame softmax launches,
        // full846 tail rows and all12heads. No production flags are altered.
        try module.launchAttentionSoftmax(&ctx, probs, scores, .{}, sequence, 0, rows, 0, heads, tile);
        try probs.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (0..heads) |head| for (0..rows) |row| for (0..sequence) |key| {
            const value = actual[(head * tile + row) * sequence + key];
            if (value != expected) {
                std.debug.print("softmax shared reuse mismatch iteration={} batch={} head={} row={} key={}\n", .{ iteration, batch_index, head, row, key });
                try std.testing.expectEqual(expected, value);
            }
        };
    };
}

test "CUDA source score softmax validates key prefix and disjoint output before SDK" {
    var ctx: Context = undefined;
    var module: Module = undefined;
    const scores: Buffer = .{ .ptr = 4096, .len = 8 * 8 * 4 };
    const probs: Buffer = .{ .ptr = 8192, .len = 8 * 8 * 2 };
    try std.testing.expectError(error.InvalidEmbeddingGemma2Shape, module.launchSourceAttentionSoftmax(&ctx, probs, scores, 8, 9, 0, 4, 1, 8));
    try std.testing.expectError(error.InvalidEmbeddingGemma2Shape, module.launchSourceAttentionSoftmax(&ctx, probs, scores, 8, 4, 6, 4, 1, 8));
    try std.testing.expectError(error.InvalidShape, module.launchSourceAttentionSoftmax(&ctx, .{ .ptr = scores.ptr + 2, .len = probs.len }, scores, 8, 4, 0, 4, 1, 8));
}

test "CUDA source score softmax masks only keys and publishes normalized BF16 once" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const sequence = 3;
    const heads = 2;
    const tile = 3;
    const elements = heads * tile * sequence;
    var input: [elements]f32 = undefined;
    for (0..heads * tile) |row| @memcpy(input[row * sequence ..][0..sequence], &[_]f32{ 0, -1.2039728, 1000 });
    var scores = try Buffer.alloc(&ctx, elements * 4);
    defer scores.free(&ctx);
    var probs = try Buffer.alloc(&ctx, (elements + 2) * 2);
    defer probs.free(&ctx);
    var mask = try Buffer.alloc(&ctx, sequence * 8);
    defer mask.free(&ctx);
    var actual: [elements + 2]u16 = @splat(0x7fc5);
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try scores.copyFromHost(&ctx, std.mem.sliceAsBytes(&input));
    try probs.copyFromHost(&ctx, std.mem.sliceAsBytes(&actual));
    try module.launchSourceAttentionSoftmax(&ctx, probs, scores, sequence, 2, 0, sequence, heads, tile);
    try probs.copyToHost(&ctx, std.mem.sliceAsBytes(&actual));
    try ctx.synchronize();
    // exp(log(.3)) / (1 + exp(log(.3))) rounds to0x3e6c. Rounding
    // the unnormalized beta first would instead publish0x3e6d.
    for (0..heads * tile) |row| try std.testing.expectEqualSlices(u16, &.{ 0x3f45, 0x3e6c, 0 }, actual[row * sequence ..][0..sequence]);
    try std.testing.expectEqualSlices(u16, &.{ 0x7fc5, 0x7fc5 }, actual[elements..]);
    const mask_values = [_]i64{ 1, 1, 0 };
    try mask.copyFromHost(&ctx, std.mem.sliceAsBytes(&mask_values));
    try module.launchAttentionSoftmax(&ctx, probs, scores, mask, sequence, 0, sequence, 0, heads, tile);
    try probs.copyToHost(&ctx, std.mem.sliceAsBytes(&actual));
    try ctx.synchronize();
    // The old adapter retains its beta publication and query masking.
    for (0..heads) |head| for (0..tile) |row| {
        const expected: [3]u16 = if (row < 2) .{ 0x3f45, 0x3e6d, 0 } else .{ 0, 0, 0 };
        try std.testing.expectEqualSlices(u16, &expected, actual[(head * tile + row) * sequence ..][0..sequence]);
    };
}

test "embeddinggemma2 CUDA contrastive shared reduction reuse preserves masked uniform objective and gradients" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const a = std.testing.allocator;
    const queries = 64;
    const capacity = 384;
    const zeros: [capacity]f32 = @splat(0);
    var targets: [queries]u32 = undefined;
    const valid = try a.alloc(u8, queries * capacity);
    defer a.free(valid);
    const actual = try a.alloc(f32, queries * capacity);
    defer a.free(actual);
    const baseline = try a.alloc(f32, queries * capacity);
    defer a.free(baseline);
    var buffers: [8]Buffer = @splat(.{});
    defer for (&buffers) |*buffer| buffer.free(&ctx);
    const lengths = [_]usize{ queries * 4, capacity * 4, queries * 4, queries * capacity, queries * capacity * 4, queries * 4, queries * 4, capacity * 4 };
    for (&buffers, lengths) |*buffer, bytes| buffer.* = try Buffer.alloc(&ctx, bytes);
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try buffers[0].copyFromHost(&ctx, std.mem.sliceAsBytes(zeros[0..queries]));
    try buffers[1].copyFromHost(&ctx, std.mem.sliceAsBytes(&zeros));
    for ([_]bool{ false, true }) |masked| {
        const candidates: usize = if (masked) capacity else 257;
        var visible: [queries]usize = @splat(0);
        for (0..queries) |row| {
            // Targets cover other warps and candidate tails; lane0 cannot
            // assume the owning thread has finished probability normalization.
            targets[row] = @intCast((37 * row + 65) % candidates);
            for (0..candidates) |c| {
                const allowed = !masked or c % 7 != 6 or c == targets[row];
                if (allowed) visible[row] += 1;
                valid[row * candidates + c] = @intFromBool(allowed);
            }
        }
        try buffers[2].copyFromHost(&ctx, std.mem.sliceAsBytes(&targets));
        if (masked) try buffers[3].copyFromHost(&ctx, valid[0 .. queries * candidates]);
        var loss_baseline: [queries]f32 = undefined;
        for (0..32) |iteration| {
            try module.launchContrastive(&ctx, buffers[4], buffers[5], buffers[6], buffers[7], buffers[0], buffers[1], if (masked) buffers[3] else .{}, buffers[2], .{ .queries = queries, .candidates = @intCast(candidates), .width = 1, .temperature = 1, .objective_weight = 1 }, 1);
            try buffers[4].copyToHost(&ctx, std.mem.sliceAsBytes(actual[0 .. queries * candidates]));
            var losses: [queries]f32 = undefined;
            var dqueries: [queries]f32 = undefined;
            var dcandidates: [capacity]f32 = undefined;
            // Drain earlier D2H submissions before these loop-local stack
            // arrays can unwind on a later SDK error; success keeps one wait.
            errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            try buffers[5].copyToHost(&ctx, std.mem.sliceAsBytes(&losses));
            try buffers[6].copyToHost(&ctx, std.mem.sliceAsBytes(&dqueries));
            try buffers[7].copyToHost(&ctx, std.mem.sliceAsBytes(dcandidates[0..candidates]));
            try ctx.synchronize();
            // Dot products are exactly0, exp(0)==1, integer sums exact: every
            // visible F32 probability has an exact CPU division oracle.
            for (0..queries) |row| {
                const expected: f32 = 1.0 / @as(f32, @floatFromInt(visible[row]));
                for (0..candidates) |c| try std.testing.expectEqual(if (valid[row * candidates + c] != 0) expected else @as(f32, 0), actual[row * candidates + c]);
                // Match the actual loss boundary: F32 normalized probability,
                // then logf/negation at objective_weight1, not log(N) fusion.
                const expected_loss: f32 = -@log(expected);
                // Existing F32 log-softmax/scalar-reference bound from
                // backends/activations.zig; same-device repeats stay byte exact.
                try std.testing.expect(@abs(expected_loss - losses[row]) < 1e-5);
            }
            for (dqueries) |value| try std.testing.expectEqual(@as(f32, 0), value);
            for (dcandidates[0..candidates]) |value| try std.testing.expectEqual(@as(f32, 0), value);
            for (losses) |value| try std.testing.expect(std.math.isFinite(value) and value > 0);
            if (iteration == 0) {
                @memcpy(baseline[0 .. queries * candidates], actual[0 .. queries * candidates]);
                loss_baseline = losses;
            } else {
                try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(baseline[0 .. queries * candidates]), std.mem.sliceAsBytes(actual[0 .. queries * candidates]));
                try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(&loss_baseline), std.mem.sliceAsBytes(&losses));
            }
        }
    }
}

test "embeddinggemma2 CUDA nonzero contrastive objective and both VJPs match independent CPU reference across masked tails" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const a = std.testing.allocator;
    const queries = 9;
    const capacity = 384;
    const width = 17;
    const temperature: f32 = 0.5;
    const objective_weight: f32 = 0.75;
    const loss_scale: f32 = 1.25;
    var random = std.Random.DefaultPrng.init(0x133d_846_17);
    var query_values: [queries * width]f32 = undefined;
    var candidate_values: [capacity * width]f32 = undefined;
    // Dyadic inputs make all17-term dots and inverse-temperature2 exact,
    // independently of CPU/GPU FMA contraction. No host math from GPU output.
    for (&query_values) |*value| value.* = @as(f32, @floatFromInt(random.random().intRangeLessThan(i32, -8, 9))) / 32;
    for (&candidate_values) |*value| value.* = @as(f32, @floatFromInt(random.random().intRangeLessThan(i32, -8, 9))) / 32;
    var targets: [queries]u32 = undefined;
    var valid: [queries * capacity]u8 = undefined;
    const offsets = [_]usize{ 0, queries * capacity, queries * capacity + queries, queries * capacity + queries + queries * width };
    const host_elements = offsets[3] + capacity * width;
    const actual = try a.alloc(f32, host_elements);
    defer a.free(actual);
    const baseline = try a.alloc(f32, host_elements);
    defer a.free(baseline);
    const reference = try a.alloc(f64, host_elements);
    defer a.free(reference);
    var buffers: [8]Buffer = @splat(.{});
    defer for (&buffers) |*buffer| buffer.free(&ctx);
    const allocation_bytes = [_]usize{ queries * width * 4, capacity * width * 4, queries * 4, queries * capacity, queries * capacity * 4, queries * 4, queries * width * 4, capacity * width * 4 };
    for (&buffers, allocation_bytes) |*buffer, bytes| buffer.* = try Buffer.alloc(&ctx, bytes);
    // All DMA destinations and input sources have function scope; this drain
    // runs before any heap free or function-scope input lifetime can end.
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try buffers[0].copyFromHost(&ctx, std.mem.sliceAsBytes(&query_values));
    try buffers[1].copyFromHost(&ctx, std.mem.sliceAsBytes(&candidate_values));
    const cases = [_]struct { candidates: usize, masked: bool }{
        .{ .candidates = 257, .masked = false },
        .{ .candidates = 257, .masked = true },
        .{ .candidates = 384, .masked = true },
    };
    for (cases) |case| {
        const candidates = case.candidates;
        const lengths = [_]usize{ queries * candidates, queries, queries * width, candidates * width };
        @memset(reference, 0);
        for (0..queries) |row| {
            targets[row] = @intCast(if (row + 1 == queries) candidates - 1 else (37 * row + 65) % candidates);
            for (0..candidates) |column| valid[row * candidates + column] = @intFromBool(!case.masked or (row + column) % 11 != 0 or column == targets[row]);
            // Independent stable F64 softmax; preserve the actual stored F32
            // probability boundary before loss and derivative calculations.
            var scores: [capacity]f64 = undefined;
            var maximum: f64 = -std.math.inf(f64);
            for (0..candidates) |column| {
                var dot: f64 = 0;
                for (0..width) |d| dot += @as(f64, query_values[row * width + d]) * @as(f64, candidate_values[column * width + d]);
                const score = if (valid[row * candidates + column] != 0) dot / @as(f64, temperature) else -std.math.inf(f64);
                scores[column] = score;
                maximum = @max(maximum, score);
            }
            var total: f64 = 0;
            for (0..candidates) |column| {
                scores[column] = if (valid[row * candidates + column] != 0) @exp(scores[column] - maximum) else 0;
                total += scores[column];
            }
            for (0..candidates) |column| {
                const probability: f32 = @floatCast(scores[column] / total);
                reference[row * candidates + column] = probability;
            }
            reference[offsets[1] + row] = -@log(reference[row * candidates + targets[row]]) * @as(f64, objective_weight);
        }
        // Per-row losses above are weighted but NOT averaged/loss-scaled.
        // Actual VJPs differentiate the MEAN weighted objective, then apply
        // loss_scale and inverse temperature exactly once at the final boundary.
        const gradient_scale: f64 = @as(f64, loss_scale) * @as(f64, objective_weight) / @as(f64, temperature) / queries;
        for (0..queries) |row| for (0..width) |d| {
            var sum: f64 = 0;
            for (0..candidates) |column| {
                const coefficient = reference[row * candidates + column] - @as(f64, @floatFromInt(@intFromBool(column == targets[row])));
                sum += coefficient * @as(f64, candidate_values[column * width + d]);
            }
            reference[offsets[2] + row * width + d] = sum * gradient_scale;
        };
        for (0..candidates) |column| for (0..width) |d| {
            var sum: f64 = 0;
            for (0..queries) |row| {
                const coefficient = reference[row * candidates + column] - @as(f64, @floatFromInt(@intFromBool(column == targets[row])));
                sum += coefficient * @as(f64, query_values[row * width + d]);
            }
            reference[offsets[3] + column * width + d] = sum * gradient_scale;
        };
        inline for (.{ 2, 3 }) |role| {
            var nonzero = false;
            for (reference[offsets[role]..][0..lengths[role]]) |value| if (@abs(value) > 0.0001) {
                nonzero = true;
            };
            try std.testing.expect(nonzero); // Neither VJP oracle is trivial0.
        }
        try buffers[2].copyFromHost(&ctx, std.mem.sliceAsBytes(&targets));
        if (case.masked) try buffers[3].copyFromHost(&ctx, valid[0 .. queries * candidates]);
        for (0..16) |iteration| {
            try module.launchContrastive(&ctx, buffers[4], buffers[5], buffers[6], buffers[7], buffers[0], buffers[1], if (case.masked) buffers[3] else .{}, buffers[2], .{ .queries = queries, .candidates = @intCast(candidates), .width = width, .temperature = temperature, .objective_weight = objective_weight }, loss_scale);
            for (buffers[4..8], offsets, lengths) |buffer, offset, length| try buffer.copyToHost(&ctx, std.mem.sliceAsBytes(actual[offset..][0..length]));
            try ctx.synchronize();
            for (offsets, lengths, 0..) |offset, length, role| {
                for (reference[offset..][0..length], actual[offset..][0..length], 0..) |expected, got, index| {
                    if (!std.math.isFinite(got) or @abs(@as(f64, got) - expected) >= 1e-5) {
                        std.debug.print("contrastive nonzero CPU mismatch C={} masked={} iteration={} role={} index={}\n", .{ candidates, case.masked, iteration, role, index });
                        try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), got, 1e-5);
                    }
                }
                if (iteration == 0) {
                    @memcpy(baseline[offset..][0..length], actual[offset..][0..length]);
                } else try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(baseline[offset..][0..length]), std.mem.sliceAsBytes(actual[offset..][0..length]));
            }
        }
    }
}

test "CUDA BF16 attention stores unnormalized probabilities before value multiplication" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    const a = std.testing.allocator;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const Round = struct {
        fn bf16(input: f64) u16 {
            const f: f32 = @floatCast(input);
            const bits: u32 = @bitCast(f);
            return @truncate((bits +% (0x7fff + ((bits >> 16) & 1))) >> 16);
        }
        fn value(bits: u16) f64 {
            const f: f32 = @bitCast(@as(u32, bits) << 16);
            return f;
        }
    };
    for ([_]u32{ 15, 65, 577 }) |sequence| for ([_]u32{ 0, 7, 512 }) |radius| {
        if (sequence == 577 and radius != 512) continue;
        const local = radius != 0;
        const shape = Attention{ .batch = 1, .seq = sequence, .q_heads = 4, .kv_heads = if (local) 2 else 1, .head_dim = if (local) 256 else 512, .local_radius = radius };
        const q_count = @as(usize, sequence) * shape.q_heads * shape.head_dim;
        const kv_count = @as(usize, sequence) * shape.kv_heads * shape.head_dim;
        const host_q = try a.alloc(u16, q_count);
        defer a.free(host_q);
        const host_k = try a.alloc(u16, kv_count);
        defer a.free(host_k);
        const host_v = try a.alloc(u16, kv_count);
        defer a.free(host_v);
        const masks = try a.alloc(i64, sequence);
        defer a.free(masks);
        @memset(host_q, 0);
        @memset(host_k, 0);
        for (0..sequence) |row| {
            masks[row] = @intFromBool(row % 7 != 3);
            for (0..shape.q_heads) |head| host_q[(row * shape.q_heads + head) * shape.head_dim] = Round.bf16(1);
            for (0..shape.kv_heads) |head| {
                host_k[(row * shape.kv_heads + head) * shape.head_dim] = Round.bf16(@as(f64, @floatFromInt(@as(i32, @intCast(row % 11)) - 5)) / 4);
                for (0..shape.head_dim) |d| host_v[(row * shape.kv_heads + head) * shape.head_dim + d] = Round.bf16(@as(f64, @floatFromInt(@as(i32, @intCast((3 * row + 7 * d) % 17)) - 8)) / 8);
            }
        }
        var q = try Buffer.alloc(&ctx, q_count * 2);
        defer q.free(&ctx);
        var k = try Buffer.alloc(&ctx, kv_count * 2);
        defer k.free(&ctx);
        var v = try Buffer.alloc(&ctx, kv_count * 2);
        defer v.free(&ctx);
        var mask = try Buffer.alloc(&ctx, masks.len * @sizeOf(i64));
        defer mask.free(&ctx);
        var output = try Buffer.alloc(&ctx, q_count * 2);
        defer output.free(&ctx);
        try q.copyFromHost(&ctx, std.mem.sliceAsBytes(host_q));
        try k.copyFromHost(&ctx, std.mem.sliceAsBytes(host_k));
        try v.copyFromHost(&ctx, std.mem.sliceAsBytes(host_v));
        try mask.copyFromHost(&ctx, std.mem.sliceAsBytes(masks));
        const actual = try a.alloc(u16, q_count);
        defer a.free(actual);
        const selected_queries = [_]u32{ 0, 3, 288, 575, 576 };
        const untouched = Round.bf16(17);
        if (sequence == 577) {
            @memset(actual, untouched);
            try output.copyFromHost(&ctx, std.mem.sliceAsBytes(actual));
            // Exercise the SAME production scalar/tail entry and ABI without
            // executing an unnecessary full O(sequence^2) diagnostic sweep.
            for (selected_queries) |query| {
                var pointers = [_]u64{ output.ptr, q.ptr, k.ptr, v.ptr, mask.ptr };
                var b: u32 = 1;
                var seq = sequence;
                var qh = shape.q_heads;
                var kh = shape.kv_heads;
                var dim = shape.head_dim;
                var window = radius;
                var offset = query;
                var args = [_]?*anyopaque{ &pointers[0], &pointers[1], &pointers[2], &pointers[3], &pointers[4], &b, &seq, &qh, &kh, &dim, &window, &offset };
                try launch(&ctx, module.attention, &args, .{ 1, shape.q_heads, 1 }, .{ 32, 1, 1 });
            }
        } else try module.launchLegacyAttention(&ctx, output, q, k, v, mask, shape);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        var distinct_from_f32 = false;
        // Independent F64 scalar oracle: exact constructed QK logits, explicit
        // BF16 value-store node, F32-represented V, and final BF16 output.
        // This has no dependency on the CUDA traversal/reduction implementation.
        if (sequence == 577) for (0..sequence) |query| {
            if (std.mem.indexOfScalar(u32, &selected_queries, @intCast(query)) == null)
                for (actual[query * shape.q_heads * shape.head_dim ..][0 .. shape.q_heads * shape.head_dim]) |value| try std.testing.expectEqual(untouched, value);
        };
        for (0..sequence) |query| for (0..shape.head_dim) |d| {
            if (sequence == 577 and std.mem.indexOfScalar(u32, &selected_queries, @intCast(query)) == null) continue;
            const begin = if (radius != 0) query -| radius else 0;
            const limit = if (radius != 0) @min(@as(usize, sequence), query + radius + 1) else sequence;
            var numerator: f64 = 0;
            var unrounded_numerator: f64 = 0;
            var denominator: f64 = 0;
            var maximum: f64 = -std.math.inf(f64);
            var start: usize = begin / 16 * 16;
            while (start < limit) : (start += 16) {
                const first = @max(begin, start);
                const end = @min(limit, start + 16);
                var next = maximum;
                for (first..end) |row| if (masks[row] != 0) {
                    next = @max(next, @as(f64, @floatFromInt(@as(i32, @intCast(row % 11)) - 5)) / 4);
                };
                const scale = if (std.math.isFinite(maximum)) @exp(maximum - next) else 0;
                numerator *= scale;
                unrounded_numerator *= scale;
                denominator *= scale;
                maximum = next;
                for (first..end) |row| if (masks[row] != 0) {
                    const score = @as(f64, @floatFromInt(@as(i32, @intCast(row % 11)) - 5)) / 4;
                    const probability = @exp(score - maximum);
                    const value = @as(f64, @floatFromInt(@as(i32, @intCast((3 * row + 7 * d) % 17)) - 8)) / 8;
                    numerator += Round.value(Round.bf16(probability)) * value;
                    unrounded_numerator += probability * value;
                    denominator += probability;
                };
            }
            const expected = Round.bf16(numerator / denominator);
            distinct_from_f32 = distinct_from_f32 or expected != Round.bf16(unrounded_numerator / denominator);
            for (0..shape.q_heads) |head| {
                const want = if (masks[query] != 0) expected else @as(u16, 0);
                try std.testing.expectEqual(want, actual[(query * shape.q_heads + head) * shape.head_dim + d]);
            }
        };
        try std.testing.expect(distinct_from_f32);
    };
}

test "BF16 attention schedules derive bounded split workspace from explicit cold device facts" {
    const local = Attention{ .batch = 1, .seq = 128, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 512 };
    // Seventeen is a simulated policy input. Production receives the property
    // solely from the cold driver query; this test never attests a GPU.
    const program = try AttentionProgram.forResolvedMask(local, local.seq, true, 17);
    try std.testing.expectEqual(AttentionSchedule.flash_bf16, program.probability_schedule);
    try std.testing.expectEqual(@as(u32, 2), program.split_count);
    const workspace = try program.workspace();
    try std.testing.expectEqual(@as(usize, 1048576), workspace.output_bytes);
    try std.testing.expectEqual(workspace.output_bytes, workspace.lse_offset);
    try std.testing.expectEqual(@as(usize, 1052672), workspace.total_bytes);
    for ([_]i32{ 0, -1 }) |invalid| {
        try std.testing.expectError(error.InvalidCudaState, AttentionProgram.forResolvedMask(local, local.seq, true, invalid));
        var global = local;
        global.head_dim = 512;
        global.kv_heads = 1;
        global.local_radius = 0;
        try std.testing.expectError(error.InvalidCudaState, AttentionProgram.forResolvedMask(global, global.seq, true, invalid));
    }
    const masked = try AttentionProgram.forResolvedMask(local, local.seq, false, 17);
    try std.testing.expectEqual(@as(u32, 22_912), try program.sharedMemoryBytes());
    try std.testing.expectEqual(@as(u32, 20_864), try masked.sharedMemoryBytes());
    const global_storage = AttentionProgram{ .shape = .{ .batch = 1, .seq = 303, .q_heads = 4, .kv_heads = 1, .head_dim = 512, .local_radius = 0 }, .source_sequence = 303, .probability_schedule = .efficient_bf16 };
    try std.testing.expectEqual(@as(u32, 37_248), try global_storage.sharedMemoryBytes());
    try std.testing.expectEqual(AttentionSchedule.efficient_bf16, masked.probability_schedule);
    try std.testing.expectEqual(@as(usize, 0), (try masked.workspace()).total_bytes);
    var exact_window = local;
    exact_window.seq = exact_window.local_radius;
    try std.testing.expectEqual(AttentionSchedule.efficient_bf16, (try AttentionProgram.forResolvedMask(exact_window, exact_window.seq, true, 17)).probability_schedule);
    var padded = local;
    padded.seq = 4096;
    const padded_program = try AttentionProgram.forResolvedMask(padded, local.seq, true, 17);
    try std.testing.expectEqual(program.probability_schedule, padded_program.probability_schedule);
    try std.testing.expectEqual(program.split_count, padded_program.split_count);
    try std.testing.expectEqual(local.seq, padded_program.source_sequence);
    try std.testing.expectEqual(padded.seq, padded_program.shape.seq);
    try std.testing.expectEqual(@as(usize, 33685504), (try padded_program.workspace()).total_bytes);
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, AttentionProgram.forResolvedMask(padded, 0, true, 17));
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, AttentionProgram.forResolvedMask(padded, padded.seq + 1, true, 17));
    var malformed = program;
    malformed.split_count = 0;
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, malformed.workspace());
    malformed.split_count = 3; // More splits than actual 64-key blocks.
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, malformed.workspace());
    malformed = program;
    malformed.shape.local_radius = 8193;
    try std.testing.expectError(error.InvalidEmbeddingGemma2AttentionSchedule, malformed.workspace());
}

test "CUDA split attention publishes rounded LSE before uniform score shift" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (!artifact.hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const a = std.testing.allocator;
    // Three explicit numerical partitions are valid independently of the cold
    // device heuristic. Uniform logits make each partition's logsumexp shift
    // by exactly c, with a single F32 rounding AFTER its published base LSE.
    const shape = Attention{ .batch = 1, .seq = 177, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 512 };
    const program = AttentionProgram{ .shape = shape, .source_sequence = shape.seq, .probability_schedule = .flash_bf16, .split_count = 3 };
    const layout = try program.workspace();
    const q_count = @as(usize, shape.seq) * shape.q_heads * shape.head_dim;
    const kv_count = @as(usize, shape.seq) * shape.kv_heads * shape.head_dim;
    const host_q = try a.alloc(u16, q_count);
    defer a.free(host_q);
    const host_k = try a.alloc(u16, kv_count);
    defer a.free(host_k);
    const host_v = try a.alloc(u16, kv_count);
    defer a.free(host_v);
    const masks = try a.alloc(i64, shape.seq);
    defer a.free(masks);
    const baseline = try a.alloc(f32, (layout.total_bytes - layout.lse_offset) / @sizeOf(f32));
    defer a.free(baseline);
    const actual = try a.alloc(f32, baseline.len);
    defer a.free(actual);
    @memset(host_q, 0);
    @memset(host_k, 0);
    @memset(host_v, 0);
    @memset(masks, 1);
    for (0..@as(usize, shape.seq) * shape.kv_heads) |row| host_k[row * shape.head_dim] = 0x3f80;
    var q = try Buffer.alloc(&ctx, q_count * 2);
    defer q.free(&ctx);
    var k = try Buffer.alloc(&ctx, kv_count * 2);
    defer k.free(&ctx);
    var v = try Buffer.alloc(&ctx, kv_count * 2);
    defer v.free(&ctx);
    var mask = try Buffer.alloc(&ctx, masks.len * @sizeOf(i64));
    defer mask.free(&ctx);
    var output = try Buffer.alloc(&ctx, q_count * 2);
    defer output.free(&ctx);
    var workspace = try Buffer.alloc(&ctx, layout.total_bytes);
    defer workspace.free(&ctx);
    // Async host transfers must settle before any host allocation unwinds.
    defer ctx.synchronize() catch {};
    try q.copyFromHost(&ctx, std.mem.sliceAsBytes(host_q));
    try k.copyFromHost(&ctx, std.mem.sliceAsBytes(host_k));
    try v.copyFromHost(&ctx, std.mem.sliceAsBytes(host_v));
    try mask.copyFromHost(&ctx, std.mem.sliceAsBytes(masks));
    const lse = Buffer{ .ptr = try std.math.add(u64, workspace.ptr, layout.lse_offset), .len = baseline.len * @sizeOf(f32) };
    try module.launchLocalFlashAttention(&ctx, output, q, k, v, mask, program, workspace);
    try lse.copyToHost(&ctx, std.mem.sliceAsBytes(baseline));
    try ctx.synchronize();
    for ([_]f32{ 0.125, 0.25, 0.5, 1, 2, 4, 8, 16 }) |shift| {
        const bits: u32 = @bitCast(shift);
        for (0..@as(usize, shape.seq) * shape.q_heads) |row| host_q[row * shape.head_dim] = @truncate(bits >> 16);
        try q.copyFromHost(&ctx, std.mem.sliceAsBytes(host_q));
        try module.launchLocalFlashAttention(&ctx, output, q, k, v, mask, program, workspace);
        try lse.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (baseline, actual) |zero, value| {
            try std.testing.expect(std.math.isFinite(zero));
            const expected: f32 = @floatCast(@as(f64, zero) + @as(f64, shift));
            try std.testing.expectEqual(@as(u32, @bitCast(expected)), @as(u32, @bitCast(value)));
        }
    }
}

test "CUDA RGB patch source stores match independent finite byte fixtures and preserve input" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    // Independently enumerated IEEE source BF16 nodes / original F32 affine.
    // Each RGB8 value is visited at its true row-major patch position.
    const source_bits = [_]u16{
        0xbf80, 0xbf7e, 0xbf7c, 0xbf7a, 0xbf78, 0xbf76, 0xbf74, 0xbf72,
        0xbf70, 0xbf6e, 0xbf6c, 0xbf6a, 0xbf68, 0xbf66, 0xbf64, 0xbf62,
        0xbf60, 0xbf5e, 0xbf5c, 0xbf5a, 0xbf58, 0xbf56, 0xbf54, 0xbf52,
        0xbf50, 0xbf4e, 0xbf4c, 0xbf4a, 0xbf48, 0xbf46, 0xbf44, 0xbf42,
        0xbf40, 0xbf3e, 0xbf3c, 0xbf3a, 0xbf38, 0xbf36, 0xbf34, 0xbf32,
        0xbf30, 0xbf2e, 0xbf2c, 0xbf2a, 0xbf28, 0xbf26, 0xbf24, 0xbf22,
        0xbf20, 0xbf1e, 0xbf1c, 0xbf1a, 0xbf18, 0xbf16, 0xbf14, 0xbf12,
        0xbf10, 0xbf0e, 0xbf0c, 0xbf0a, 0xbf08, 0xbf06, 0xbf04, 0xbf02,
        0xbefe, 0xbefa, 0xbef6, 0xbef2, 0xbeee, 0xbeea, 0xbee6, 0xbee2,
        0xbede, 0xbeda, 0xbed6, 0xbed2, 0xbece, 0xbeca, 0xbec6, 0xbec2,
        0xbebe, 0xbeba, 0xbeb6, 0xbeb2, 0xbeae, 0xbeaa, 0xbea6, 0xbea2,
        0xbe9e, 0xbe9a, 0xbe96, 0xbe92, 0xbe8e, 0xbe8a, 0xbe86, 0xbe82,
        0xbe7c, 0xbe74, 0xbe6c, 0xbe64, 0xbe5c, 0xbe54, 0xbe4c, 0xbe44,
        0xbe3c, 0xbe34, 0xbe2c, 0xbe24, 0xbe1c, 0xbe14, 0xbe0c, 0xbe04,
        0xbdf8, 0xbde8, 0xbdd8, 0xbdc8, 0xbdb8, 0xbda8, 0xbd98, 0xbd88,
        0xbd70, 0xbd50, 0xbd30, 0xbd10, 0xbce0, 0xbca0, 0xbc40, 0xbb80,
        0x3c00, 0x3c80, 0x3cc0, 0x3d00, 0x3d20, 0x3d40, 0x3d60, 0x3d80,
        0x3d90, 0x3da0, 0x3db0, 0x3dc0, 0x3dd0, 0x3de0, 0x3df0, 0x3e00,
        0x3e08, 0x3e10, 0x3e18, 0x3e20, 0x3e28, 0x3e30, 0x3e38, 0x3e40,
        0x3e48, 0x3e50, 0x3e58, 0x3e60, 0x3e68, 0x3e70, 0x3e78, 0x3e80,
        0x3e84, 0x3e88, 0x3e8c, 0x3e90, 0x3e94, 0x3e98, 0x3e9c, 0x3ea0,
        0x3ea4, 0x3ea8, 0x3eac, 0x3eb0, 0x3eb4, 0x3eb8, 0x3ebc, 0x3ec0,
        0x3ec4, 0x3ec8, 0x3ecc, 0x3ed0, 0x3ed4, 0x3ed8, 0x3edc, 0x3ee0,
        0x3ee4, 0x3ee8, 0x3eec, 0x3ef0, 0x3ef4, 0x3ef8, 0x3efc, 0x3f00,
        0x3f02, 0x3f04, 0x3f06, 0x3f08, 0x3f0a, 0x3f0c, 0x3f0e, 0x3f10,
        0x3f12, 0x3f14, 0x3f16, 0x3f18, 0x3f1a, 0x3f1c, 0x3f1e, 0x3f20,
        0x3f22, 0x3f24, 0x3f26, 0x3f28, 0x3f2a, 0x3f2c, 0x3f2e, 0x3f30,
        0x3f32, 0x3f34, 0x3f36, 0x3f38, 0x3f3a, 0x3f3c, 0x3f3e, 0x3f40,
        0x3f42, 0x3f44, 0x3f46, 0x3f48, 0x3f4a, 0x3f4c, 0x3f4e, 0x3f50,
        0x3f52, 0x3f54, 0x3f56, 0x3f58, 0x3f5a, 0x3f5c, 0x3f5e, 0x3f60,
        0x3f62, 0x3f64, 0x3f66, 0x3f68, 0x3f6a, 0x3f6c, 0x3f6e, 0x3f70,
        0x3f72, 0x3f74, 0x3f76, 0x3f78, 0x3f7a, 0x3f7c, 0x3f7e, 0x3f80,
    };
    const legacy_bits = [_]u32{
        0xbf800000, 0xbf7dfdfe, 0xbf7bfbfc, 0xbf79f9fa, 0xbf77f7f8, 0xbf75f5f6, 0xbf73f3f4, 0xbf71f1f2,
        0xbf6feff0, 0xbf6dedee, 0xbf6bebec, 0xbf69e9ea, 0xbf67e7e8, 0xbf65e5e6, 0xbf63e3e4, 0xbf61e1e2,
        0xbf5fdfe0, 0xbf5dddde, 0xbf5bdbdc, 0xbf59d9da, 0xbf57d7d8, 0xbf55d5d6, 0xbf53d3d4, 0xbf51d1d2,
        0xbf4fcfd0, 0xbf4dcdce, 0xbf4bcbcc, 0xbf49c9ca, 0xbf47c7c8, 0xbf45c5c6, 0xbf43c3c4, 0xbf41c1c2,
        0xbf3fbfc0, 0xbf3dbdbe, 0xbf3bbbbc, 0xbf39b9ba, 0xbf37b7b8, 0xbf35b5b6, 0xbf33b3b4, 0xbf31b1b2,
        0xbf2fafb0, 0xbf2dadae, 0xbf2babac, 0xbf29a9aa, 0xbf27a7a8, 0xbf25a5a6, 0xbf23a3a4, 0xbf21a1a2,
        0xbf1f9f9f, 0xbf1d9d9d, 0xbf1b9b9b, 0xbf199999, 0xbf179797, 0xbf159595, 0xbf139393, 0xbf119191,
        0xbf0f8f8f, 0xbf0d8d8d, 0xbf0b8b8b, 0xbf098989, 0xbf078787, 0xbf058585, 0xbf038383, 0xbf018181,
        0xbefefefe, 0xbefafafa, 0xbef6f6f6, 0xbef2f2f2, 0xbeeeeeee, 0xbeeaeaea, 0xbee6e6e6, 0xbee2e2e2,
        0xbededede, 0xbedadada, 0xbed6d6d6, 0xbed2d2d2, 0xbececece, 0xbecacaca, 0xbec6c6c6, 0xbec2c2c2,
        0xbebebebe, 0xbebababa, 0xbeb6b6b6, 0xbeb2b2b2, 0xbeaeaeae, 0xbeaaaaaa, 0xbea6a6a6, 0xbea2a2a2,
        0xbe9e9e9e, 0xbe9a9a9a, 0xbe969696, 0xbe929292, 0xbe8e8e8e, 0xbe8a8a8a, 0xbe868686, 0xbe828282,
        0xbe7cfcf8, 0xbe74f4f0, 0xbe6cece8, 0xbe64e4e0, 0xbe5cdcd8, 0xbe54d4d0, 0xbe4cccc8, 0xbe44c4c0,
        0xbe3cbcb8, 0xbe34b4b0, 0xbe2caca8, 0xbe24a4a0, 0xbe1c9c98, 0xbe149490, 0xbe0c8c88, 0xbe048480,
        0xbdf8f8f0, 0xbde8e8e0, 0xbdd8d8d0, 0xbdc8c8c0, 0xbdb8b8b0, 0xbda8a8a0, 0xbd989890, 0xbd888880,
        0xbd70f0e0, 0xbd50d0c0, 0xbd30b0a0, 0xbd109080, 0xbce0e0c0, 0xbca0a080, 0xbc40c080, 0xbb808000,
        0x3b808100, 0x3c40c100, 0x3ca0a0c0, 0x3ce0e100, 0x3d1090a0, 0x3d30b0c0, 0x3d50d0e0, 0x3d70f100,
        0x3d888890, 0x3d9898a0, 0x3da8a8b0, 0x3db8b8c0, 0x3dc8c8d0, 0x3dd8d8e0, 0x3de8e8f0, 0x3df8f900,
        0x3e048488, 0x3e0c8c90, 0x3e149498, 0x3e1c9ca0, 0x3e24a4a8, 0x3e2cacb0, 0x3e34b4b8, 0x3e3cbcc0,
        0x3e44c4c8, 0x3e4cccd0, 0x3e54d4d8, 0x3e5cdce0, 0x3e64e4e8, 0x3e6cecf0, 0x3e74f4f8, 0x3e7cfd00,
        0x3e828284, 0x3e868688, 0x3e8a8a8c, 0x3e8e8e90, 0x3e929294, 0x3e969698, 0x3e9a9a9c, 0x3e9e9ea0,
        0x3ea2a2a4, 0x3ea6a6a8, 0x3eaaaaac, 0x3eaeaeb0, 0x3eb2b2b4, 0x3eb6b6b8, 0x3ebababc, 0x3ebebec0,
        0x3ec2c2c4, 0x3ec6c6c8, 0x3ecacacc, 0x3ececed0, 0x3ed2d2d4, 0x3ed6d6d8, 0x3edadadc, 0x3ededee0,
        0x3ee2e2e4, 0x3ee6e6e8, 0x3eeaeaec, 0x3eeeeef0, 0x3ef2f2f4, 0x3ef6f6f8, 0x3efafafc, 0x3efeff00,
        0x3f018184, 0x3f038386, 0x3f058588, 0x3f07878a, 0x3f09898c, 0x3f0b8b8e, 0x3f0d8d90, 0x3f0f8f92,
        0x3f119194, 0x3f139396, 0x3f159598, 0x3f17979a, 0x3f19999c, 0x3f1b9b9e, 0x3f1d9da0, 0x3f1f9fa2,
        0x3f21a1a4, 0x3f23a3a6, 0x3f25a5a8, 0x3f27a7aa, 0x3f29a9ac, 0x3f2babae, 0x3f2dadb0, 0x3f2fafb2,
        0x3f31b1b4, 0x3f33b3b6, 0x3f35b5b8, 0x3f37b7ba, 0x3f39b9bc, 0x3f3bbbbe, 0x3f3dbdc0, 0x3f3fbfc2,
        0x3f41c1c4, 0x3f43c3c6, 0x3f45c5c8, 0x3f47c7ca, 0x3f49c9cc, 0x3f4bcbce, 0x3f4dcdd0, 0x3f4fcfd2,
        0x3f51d1d4, 0x3f53d3d6, 0x3f55d5d8, 0x3f57d7da, 0x3f59d9dc, 0x3f5bdbde, 0x3f5ddde0, 0x3f5fdfe2,
        0x3f61e1e4, 0x3f63e3e6, 0x3f65e5e8, 0x3f67e7ea, 0x3f69e9ec, 0x3f6bebee, 0x3f6dedf0, 0x3f6feff2,
        0x3f71f1f4, 0x3f73f3f6, 0x3f75f5f8, 0x3f77f7fa, 0x3f79f9fc, 0x3f7bfbfe, 0x3f7dfe00, 0x3f800000,
    };
    var pixels: [16 * 16 * 3]u8 = undefined;
    for (&pixels, 0..) |*v, i| v.* = @intCast(i / 3);
    var starts: [16]u64 = undefined;
    var offsets: [17]u64 = undefined;
    for (&starts, 0..) |*v, i| v.* = i;
    for (&offsets, 0..) |*v, i| v.* = i;
    const weights: [16]i32 = @splat(2);
    var horizontal = try Buffer.alloc(&ctx, pixels.len);
    defer horizontal.free(&ctx);
    try horizontal.copyFromHost(&ctx, &pixels);
    var y_starts = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(starts)));
    defer y_starts.free(&ctx);
    try y_starts.copyFromHost(&ctx, std.mem.sliceAsBytes(&starts));
    var y_offsets = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(offsets)));
    defer y_offsets.free(&ctx);
    try y_offsets.copyFromHost(&ctx, std.mem.sliceAsBytes(&offsets));
    var y_weights = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(weights)));
    defer y_weights.free(&ctx);
    try y_weights.copyFromHost(&ctx, std.mem.sliceAsBytes(&weights));
    var out = try Buffer.alloc(&ctx, pixels.len * @sizeOf(f32));
    defer out.free(&ctx);
    for ([_]PixelScaleStorage{ .f32, .bf16 }) |storage| {
        try module.launchPillowVerticalPatchesStored(&ctx, out, horizontal, y_starts, y_offsets, y_weights, 16, 16, 16, 1, storage);
        var values: [16 * 16 * 3]f32 = undefined;
        try out.copyToHost(&ctx, std.mem.sliceAsBytes(&values));
        try ctx.synchronize();
        for (values, 0..) |value, i| {
            const expected: u32 = if (storage == .bf16) @as(u32, source_bits[i / 3]) << 16 else legacy_bits[i / 3];
            try std.testing.expectEqual(expected, @as(u32, @bitCast(value)));
        }
    }
    var unchanged: [16 * 16 * 3]u8 = undefined;
    try horizontal.copyToHost(&ctx, &unchanged);
    try ctx.synchronize();
    try std.testing.expectEqualSlices(u8, &pixels, &unchanged);
}

test "CUDA source RMS publishes physical F32 BF16 values with separate and aliased output" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (ctx.info.compute_major < 8) return error.SkipZigTest;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    inline for (.{ 64, 768 }) |width| {
        const values: [2 * width]f32 = @splat(1);
        var input = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(values)));
        defer input.free(&ctx);
        var separate = try Buffer.alloc(&ctx, @sizeOf(@TypeOf(values)));
        defer separate.free(&ctx);
        defer ctx.synchronize() catch {};
        for ([_]bool{ false, true }) |alias| {
            try input.copyFromHost(&ctx, std.mem.sliceAsBytes(&values));
            const output = if (alias) input else separate;
            try module.launchSourceRmsNormBf16ValuesF32(&ctx, output, input, null, 2, width, 0.125);
            var actual: [2 * width]f32 = undefined;
            try output.copyToHost(&ctx, std.mem.sliceAsBytes(&actual));
            try ctx.synchronize();
            // Exact source mean is1; reciprocal sqrt(9/8) rounds to BF16
            // 241/256. Compare physical F32 bits, without recasting actual.
            for (actual) |value| try std.testing.expectEqual(@as(u32, 0x3f710000), @as(u32, @bitCast(value)));
        }
    }
}

test "CUDA source BF16 arena rotary and prefix attention retain signed padding" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (!@import("artifact.zig").hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    {
        var input: [32]u16 = @splat(0);
        @memset(input[0..16], 0x3f80);
        var source = try Buffer.alloc(&ctx, 32 * 2);
        defer source.free(&ctx);
        var qo = try Buffer.alloc(&ctx, 32 * 2);
        defer qo.free(&ctx);
        var ko = try Buffer.alloc(&ctx, 32 * 2);
        defer ko.free(&ctx);
        var positions = try Buffer.alloc(&ctx, 4);
        defer positions.free(&ctx);
        errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        const negative = [_]i32{-1};
        try source.copyFromHost(&ctx, std.mem.sliceAsBytes(&input));
        try positions.copyFromHost(&ctx, std.mem.sliceAsBytes(&negative));
        try module.launchSourceVisionRopePack(&ctx, qo, ko, source, source, positions, 32, 100, 8);
        var actual_q: [32]u16 = undefined;
        var actual_k: [32]u16 = undefined;
        try qo.copyToHost(&ctx, std.mem.sliceAsBytes(&actual_q));
        try ko.copyToHost(&ctx, std.mem.sliceAsBytes(&actual_k));
        try ctx.synchronize();
        try std.testing.expectEqual(@as(u16, 0x408a), actual_q[0]);
        try std.testing.expectEqual(@as(u16, 0xc0d7), actual_q[16]);
        try std.testing.expectEqual(@as(u16, 0x3f0a), actual_k[0]);
        try std.testing.expectEqual(@as(u16, 0xbf57), actual_k[16]);
    }
    const batch = 2;
    const physical = 33;
    const valid = 17;
    const heads = 12;
    const dim = 64;
    const count = batch * physical * heads * dim;
    var zeros: [count]u16 = @splat(0);
    var values: [count]u16 = undefined;
    for (0..batch) |b| for (0..physical) |row| for (0..heads) |head| {
        const value: f32 = if (row < valid) @floatFromInt(2 + b * heads + head) else 1000;
        @memset(values[(b * physical + row) * heads * dim + head * dim ..][0..dim], @truncate(@as(u32, @bitCast(value)) >> 16));
    };
    var qk = try Buffer.alloc(&ctx, count * 2);
    defer qk.free(&ctx);
    var v = try Buffer.alloc(&ctx, count * 2);
    defer v.free(&ctx);
    var output = try Buffer.alloc(&ctx, count * 2);
    defer output.free(&ctx);
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try qk.copyFromHost(&ctx, std.mem.sliceAsBytes(&zeros));
    try v.copyFromHost(&ctx, std.mem.sliceAsBytes(&values));
    try module.launchSourceNativeVision(&ctx, output, qk, qk, v, batch, physical, valid);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(&zeros));
    try ctx.synchronize();
    for (0..batch) |b| for (0..physical) |row| for (0..heads) |head| {
        const value: f32 = @floatFromInt(2 + b * heads + head);
        const expected: u16 = @truncate(@as(u32, @bitCast(value)) >> 16);
        for (zeros[(b * physical + row) * heads * dim + head * dim ..][0..dim]) |actual| try std.testing.expectEqual(expected, actual);
    };
}

test "CUDA source BF16 arena methods refuse malformed geometry and aliases before SDK" {
    var ctx: Context = undefined;
    var module: Module = undefined;
    const input: Buffer = .{ .ptr = 4096, .len = 32 * 2 };
    const other: Buffer = .{ .ptr = 8192, .len = 32 * 2 };
    const positions: Buffer = .{ .ptr = 16384, .len = 4 };
    try std.testing.expectError(error.InvalidEmbeddingGemma2Shape, module.launchSourceVisionRopePack(&ctx, .{}, .{}, input, other, positions, 33, 100, 8));
    try std.testing.expectError(error.InvalidShape, module.launchSourceVisionRopePack(&ctx, input, other, input, other, positions, 32, 100, 8));
    try std.testing.expectError(error.InvalidEmbeddingGemma2Shape, module.launchSourceNativeVision(&ctx, .{}, .{}, .{}, .{}, 1, 33, 34));
    const tensor: Buffer = .{ .ptr = 4096, .len = 33 * 768 * 2 };
    const k: Buffer = .{ .ptr = 131072, .len = tensor.len };
    const v: Buffer = .{ .ptr = 262144, .len = tensor.len };
    try std.testing.expectError(error.InvalidShape, module.launchSourceNativeVision(&ctx, tensor, tensor, k, v, 1, 33, 17));
}

test "spatial GEMM source phases reject short or aliased caller storage before launch" {
    var ctx: Context = undefined;
    var module: Module = undefined;
    const plan = try @import("spatial_pool_f32.zig").Plan.resolve(.{ .batch = 1, .grid_x = 6, .grid_y = 3, .merge = 3, .dim = 4, .physical_rows = 27, .publication = .bf16_mean_scaled });
    const buffer = Buffer{ .ptr = 0x1000, .len = plan.input.bytes };
    try std.testing.expectError(error.InvalidCudaState, module.launchSpatialPoolGemmPhase(&ctx, .{ .ptr = 0x2000, .len = plan.input.bytes - 1 }, buffer, .{}, .{}, plan, .input));
    try std.testing.expectError(error.InvalidShape, module.launchSpatialPoolGemmPhase(&ctx, buffer, buffer, .{}, .{}, plan, .input));
    try std.testing.expectError(error.InvalidShape, module.launchSpatialPoolGemmPhase(&ctx, .{ .ptr = 0x1004, .len = plan.input.bytes }, buffer, .{}, .{}, plan, .input));
}

test "RF Q64 K64 BF16 BSHD prefix descriptor preserves padded queries and independent key extent" {
    const program = Bf16BshdPrefixRfAttention{ .batch = 2, .physical_sequence = 2520, .query_sequence = 2520, .key_sequence = 2340, .heads = 12, .score_scale = 0.125 };
    try program.validate();
    try std.testing.expectEqual(@as(usize, 2 * 2520 * 12 * 64 * 2), try program.bytes());
    try std.testing.expectEqual(@as(u32, 34304), try program.sharedMemoryBytes());
    // Old text schedules retain their exact scratch layout and geometry gate.
    try std.testing.expectEqual(@as(u32, 20864), try attentionSharedMemoryBytes(.efficient_bf16, 256));
    try std.testing.expectEqual(@as(u32, 37248), try attentionSharedMemoryBytes(.efficient_bf16, 512));
    try std.testing.expectEqual(@as(u32, 22912), try attentionSharedMemoryBytes(.flash_bf16, 256));
    try std.testing.expectError(error.InvalidEmbeddingGemma2Shape, (Attention{ .batch = 1, .seq = 2520, .q_heads = 12, .kv_heads = 12, .head_dim = 64, .local_radius = 0 }).validate());
    var invalid = program;
    invalid.query_sequence = 2521;
    try std.testing.expectError(error.InvalidShape, invalid.validate());
    invalid = program;
    invalid.key_sequence = 2521;
    try std.testing.expectError(error.InvalidShape, invalid.validate());
    inline for (.{ @as(f32, 0), -1.0, std.math.nan(f32), std.math.inf(f32), std.math.floatMax(f32) }) |scale| {
        invalid = program;
        invalid.score_scale = scale;
        try std.testing.expectError(error.InvalidShape, invalid.validate());
    }
}

test "RF Q64 K64 BF16 BSHD prefix rejects malformed storage before provider access" {
    var module: Module = undefined;
    var ctx: Context = undefined;
    const program = Bf16BshdPrefixRfAttention{ .batch = 1, .physical_sequence = 4, .query_sequence = 4, .key_sequence = 2, .heads = 1, .score_scale = 0.125 };
    const bytes = try program.bytes();
    const input = Buffer{ .ptr = 0x1000, .len = bytes };
    const output = Buffer{ .ptr = 0x2000, .len = bytes };
    try program.validateBuffers(output, input, input, input);
    try std.testing.expectError(error.InvalidShape, module.launchBf16BshdPrefixRfAttention(&ctx, input, input, input, input, program));
    try std.testing.expectError(error.InvalidShape, module.launchBf16BshdPrefixRfAttention(&ctx, .{ .ptr = output.ptr, .len = bytes - 1 }, input, input, input, program));
    try std.testing.expectError(error.InvalidShape, module.launchBf16BshdPrefixRfAttention(&ctx, output, .{ .ptr = input.ptr + 2, .len = bytes }, input, input, program));
    try std.testing.expectError(error.Overflow, program.validateBuffers(.{ .ptr = std.math.maxInt(u64) - 3, .len = bytes }, input, input, input));
}

test "RF Q64 K64 BF16 BSHD prefix probability contract retains unnormalized BF16 and F32 denominator" {
    const Reference = struct {
        fn bf16(value: f32) f32 {
            const bits: u32 = @bitCast(value);
            const rounded = bits + 0x7fff + ((bits >> 16) & 1);
            return @bitCast(rounded & 0xffff0000);
        }
    };
    // Independent two-key boundary fixture: logits[-1,0], V[1,-8].
    // Normalizing before BF16 publication is observably another program.
    const p: f32 = @exp(@as(f32, -1));
    const denominator: f32 = p + 1;
    const efficient = Reference.bf16((Reference.bf16(p) - 8) / denominator);
    const normalized = Reference.bf16(Reference.bf16(p / denominator) - 8 * Reference.bf16(1 / denominator));
    try std.testing.expectEqual(@as(f32, -5.59375), efficient);
    try std.testing.expectEqual(@as(f32, -5.5625), normalized);
    // The source adapter's BF16 Q prescale remains published independently;
    // score scaling happens on the completed dot, not on acquired operands.
    try std.testing.expectEqual(@as(f32, 1), @as(f32, 8) * @as(f32, 0.125));
}

test "RF BF16 BSHD prefix cold symbol refuses capture before poisoned driver access" {
    var module: Module = undefined;
    module.bf16_bshd_prefix_rf = null;
    var ctx: Context = undefined;
    ctx.debug_graph_capture_active = true;
    try std.testing.expectError(error.CudaGraphCaptureUnsafeMatmulPlan, module.prepareBf16BshdPrefixRfAttention(&ctx));
}

test "CUDA BF16 BSHD prefix RF Q64 K64 HD64 executes independent extents tails and BF16 probability publication" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (!artifact.hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const a = std.testing.allocator;
    const Fixture = struct {
        fn bits(value: f32) u16 {
            // Every constructed value is exactly representable in BF16.
            return @truncate(@as(u32, @bitCast(value)) >> 16);
        }
        fn constant(batch: usize, head: usize, dim: usize) u16 {
            const numerator: i32 = @as(i32, @intCast((batch * 17 + head * 7 + dim) % 31)) - 15;
            return bits(@as(f32, @floatFromInt(numerator)) / 8);
        }
    };
    const programs = [_]Bf16BshdPrefixRfAttention{
        .{ .batch = 1, .heads = 1, .physical_sequence = 1, .query_sequence = 1, .key_sequence = 1, .score_scale = 0.125 },
        .{ .batch = 2, .heads = 12, .physical_sequence = 17, .query_sequence = 17, .key_sequence = 9, .score_scale = 0.125 },
        .{ .batch = 1, .heads = 3, .physical_sequence = 129, .query_sequence = 129, .key_sequence = 128, .score_scale = 0.125 },
        .{ .batch = 2, .heads = 2, .physical_sequence = 257, .query_sequence = 253, .key_sequence = 129, .score_scale = 0.125 },
        .{ .batch = 1, .heads = 12, .physical_sequence = 31, .query_sequence = 16, .key_sequence = 31, .score_scale = 0.125 },
        // Exact dyadic QK dot: 8*(-1)*0.125=-1, 8*0*0.125=0.
        .{ .batch = 1, .heads = 1, .physical_sequence = 3, .query_sequence = 3, .key_sequence = 2, .score_scale = 0.125 },
    };
    const before = module.stats();
    for (programs, 0..) |program, fixture| {
        const rounding = fixture == programs.len - 1;
        const bytes = try program.bytes();
        const host_q = try a.alloc(u16, bytes / 2);
        defer a.free(host_q);
        const host_k = try a.alloc(u16, bytes / 2);
        defer a.free(host_k);
        const host_v = try a.alloc(u16, bytes / 2);
        defer a.free(host_v);
        const actual = try a.alloc(u16, bytes / 2);
        defer a.free(actual);
        @memset(host_q, 0);
        @memset(host_k, 0);
        @memset(host_v, Fixture.bits(1000)); // Invalid keys must never contribute.
        @memset(actual, 0x7fc1); // Poison proves zero query tails are written.
        for (0..program.batch) |batch| for (0..program.physical_sequence) |row| for (0..program.heads) |head| {
            const base = ((batch * program.physical_sequence + row) * program.heads + head) * 64;
            if (rounding) {
                host_q[base] = Fixture.bits(8);
                if (row == 0) host_k[base] = Fixture.bits(-1);
            }
            if (row < program.key_sequence) for (0..64) |dim| {
                host_v[base + dim] = if (rounding) Fixture.bits(if (row == 0) 1 else -8) else Fixture.constant(batch, head, dim);
            };
        };
        var q = try Buffer.alloc(&ctx, bytes);
        defer q.free(&ctx);
        var k = try Buffer.alloc(&ctx, bytes);
        defer k.free(&ctx);
        var v = try Buffer.alloc(&ctx, bytes);
        defer v.free(&ctx);
        var output = try Buffer.alloc(&ctx, bytes);
        defer output.free(&ctx);
        errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        try q.copyFromHost(&ctx, std.mem.sliceAsBytes(host_q));
        try k.copyFromHost(&ctx, std.mem.sliceAsBytes(host_k));
        try v.copyFromHost(&ctx, std.mem.sliceAsBytes(host_v));
        try output.copyFromHost(&ctx, std.mem.sliceAsBytes(actual));
        try module.launchBf16BshdPrefixRfAttention(&ctx, output, q, k, v, program);
        try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
        try ctx.synchronize();
        for (0..program.batch) |batch| for (0..program.physical_sequence) |row| for (0..program.heads) |head| for (0..64) |dim| {
            const index = ((batch * program.physical_sequence + row) * program.heads + head) * 64 + dim;
            // Independent exact expectation. Normalized BF16 P would instead
            // publish -5.5625 in the two-key fixture, so cannot pass this test.
            const expected: u16 = if (row >= program.query_sequence) 0 else if (rounding) Fixture.bits(-5.59375) else Fixture.constant(batch, head, dim);
            try std.testing.expectEqual(expected, actual[index]);
        };
        try std.testing.expectEqual(before.bf16_bshd_prefix_rf_launches + fixture + 1, module.stats().bf16_bshd_prefix_rf_launches);
    }
    try std.testing.expectEqual(before.launches + programs.len, module.stats().launches);
    try std.testing.expectEqual(before.attention_launches + programs.len, module.stats().attention_launches);
}

test "CUDA BF16 BSHD prefix RF Q64 K64 preserves nonuniform multiblock carry rounding" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    var ctx = Context.initDefault() catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer ctx.deinit();
    if (!artifact.hasBf16WmmaCodeFor(ctx.info.compute_major, ctx.info.compute_minor)) return error.SkipZigTest;
    var module = try Module.init(&ctx);
    defer module.deinit(&ctx);
    const Fixture = struct {
        fn bits(input: f32) u16 {
            return @truncate(@as(u32, @bitCast(input)) >> 16);
        }
        fn score(key: usize) f32 {
            if (key == 64 or key == 72 or key == 80) return 3;
            if (key == 128) return 4;
            return @as(f32, @floatFromInt(key % 7)) / 4 + @as(f32, @floatFromInt(key / 64)) / 2;
        }
        fn value(key: usize, dim: usize) f32 {
            if (key % 8 != 0) return 0;
            return switch (dim) {
                0 => switch (key) {
                    0 => 0.125,
                    64 => 16777216,
                    72 => -16777216,
                    80 => 1,
                    128 => 0.25,
                    else => 0,
                },
                1 => switch (key) {
                    0 => -8,
                    8 => 3,
                    16 => 0.5,
                    64 => 128,
                    72 => -64,
                    88 => 1,
                    128 => -0.125,
                    else => 0,
                },
                2 => (if (key / 8 % 2 == 0) @as(f32, 1) else -1) * (1 + @as(f32, @floatFromInt(key % 40)) / 8),
                3 => if (key == 0) 1 else 0,
                else => 0,
            };
        }
        // Independent scalar F32 rounding reference, with only one nonzero
        // V operand per K8 quantum. No unknown tensor-core sum ordering is
        // needed: each PV quantum is one exactly representable BF16 product
        // added to the F32 carry. Nonuniform scores span three 64-key blocks.
        // The q=4,dim=0 word is 0x3cdd rather than post-PV-merge 0x3ce2;
        // q=16,dim=3 is 0x3934 rather than old KT128 publication 0x3933.
        const expected = [3][4]u16{
            .{ 0x3d88, 0x4021, 0x3e54, 0x3b01 },
            .{ 0x3cdd, 0x3f9b, 0x3d8e, 0x3b8c },
            .{ 0x3e4a, 0x408c, 0x3f81, 0x3934 },
        };
    };
    const program = Bf16BshdPrefixRfAttention{ .batch = 1, .physical_sequence = 193, .query_sequence = 67, .key_sequence = 129, .heads = 1, .score_scale = 0.125 };
    const a = std.testing.allocator;
    const count = try program.bytes();
    const host_q = try a.alloc(u16, count / 2);
    defer a.free(host_q);
    const host_k = try a.alloc(u16, count / 2);
    defer a.free(host_k);
    const host_v = try a.alloc(u16, count / 2);
    defer a.free(host_v);
    const actual = try a.alloc(u16, count / 2);
    defer a.free(actual);
    @memset(host_q, 0);
    @memset(host_k, 0);
    @memset(host_v, 0);
    @memset(actual, 0x7fc1);
    const queries = [3]f32{ 8, 4, 16 };
    for (0..program.physical_sequence) |row| {
        host_q[row * 64] = Fixture.bits(queries[row % 3]);
        host_k[row * 64] = Fixture.bits(if (row < program.key_sequence) Fixture.score(row) else 64);
        for (0..64) |dim| host_v[row * 64 + dim] = Fixture.bits(if (row < program.key_sequence) Fixture.value(row, dim) else 1000);
    }
    var q = try Buffer.alloc(&ctx, count);
    defer q.free(&ctx);
    var k = try Buffer.alloc(&ctx, count);
    defer k.free(&ctx);
    var v = try Buffer.alloc(&ctx, count);
    defer v.free(&ctx);
    var output = try Buffer.alloc(&ctx, count);
    defer output.free(&ctx);
    errdefer ctx.synchronize() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    try q.copyFromHost(&ctx, std.mem.sliceAsBytes(host_q));
    try k.copyFromHost(&ctx, std.mem.sliceAsBytes(host_k));
    try v.copyFromHost(&ctx, std.mem.sliceAsBytes(host_v));
    try output.copyFromHost(&ctx, std.mem.sliceAsBytes(actual));
    const before = module.stats();
    try module.launchBf16BshdPrefixRfAttention(&ctx, output, q, k, v, program);
    try output.copyToHost(&ctx, std.mem.sliceAsBytes(actual));
    try ctx.synchronize();
    for (0..program.physical_sequence) |row| for (0..64) |dim| {
        const expected: u16 = if (row >= program.query_sequence or dim >= 4) 0 else Fixture.expected[row % 3][dim];
        try std.testing.expectEqual(expected, actual[row * 64 + dim]);
    };
    try std.testing.expectEqual(before.bf16_bshd_prefix_rf_launches + 1, module.stats().bf16_bshd_prefix_rf_launches);
}
