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

pub const Stats = struct {
    launches: u64 = 0,
    cublaslt_matmuls: u64 = 0,
    cublas_f32_matmuls: u64 = 0,
    attention_launches: u64 = 0,
    attention_scalar_fallbacks: u64 = 0,
    scalar_matmul_fallbacks: u64 = 0,
    output_bytes: u64 = 0,
    packed_projection_launches: u64 = 0,
    fused_vision_launches: u64 = 0,
    extended_flash_launches: u64 = 0,
    fused_backbone_launches: u64 = 0,
    packed_ple_groups: u64 = 0,
    packed_head_launches: u64 = 0,
    packed_gelu_launches: u64 = 0,
    tuned_gemm_calls: u64 = 0,
    gemm_tuning_declines: u64 = 0,
    vision_residual_launches: u64 = 0,
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
    audio_clamp: driver.CUfunction,
    audio_glu_rows: driver.CUfunction,
    audio_depthwise_conv: driver.CUfunction,
    pillow_horizontal: driver.CUfunction,
    pillow_vertical_patches: driver.CUfunction,
    rms_norm: driver.CUfunction,
    training_rms_norm: driver.CUfunction,
    rms_norm_input_vjp: driver.CUfunction,
    training_rms_norm_input_vjp: driver.CUfunction,
    rope: driver.CUfunction,
    rope_vjp: driver.CUfunction,
    training_rope: driver.CUfunction,
    training_rope_vjp: driver.CUfunction,
    split_columns: driver.CUfunction,
    packed_heads: driver.CUfunction,
    packed_gelu: driver.CUfunction,
    vision_rope_pack: driver.CUfunction,
    native_vision: driver.CUfunction,
    audio_segment_conv: driver.CUfunction,
    norm_residual: driver.CUfunction,
    scale_rms: driver.CUfunction,
    attention: driver.CUfunction,
    local_flash_attention: driver.CUfunction,
    attention_softmax: driver.CUfunction,
    matmul: driver.CUfunction,
    gelu_mul: driver.CUfunction,
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
    gelu_ple: driver.CUfunction,
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
    mrl_normalize: driver.CUfunction,
    mrl_normalize_vjp: driver.CUfunction,
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
            .audio_clamp = try function(ctx, m, "termite_gemma4_audio_clamp_f32"),
            .audio_glu_rows = try function(ctx, m, "termite_gemma4_audio_glu_rows_f32"),
            .audio_depthwise_conv = try function(ctx, m, "termite_gemma4_audio_depthwise_causal_conv1d_f32"),
            .pillow_horizontal = try function(ctx, m, "termite_embedding_gemma2_pillow_horizontal_rgb8"),
            .pillow_vertical_patches = try function(ctx, m, "termite_embedding_gemma2_pillow_vertical_patches_f32"),
            .rms_norm = try function(ctx, m, "termite_embedding_gemma2_rms_norm_bf16"),
            .training_rms_norm = try function(ctx, m, "termite_embedding_gemma2_training_rms_norm_bf16"),
            .rms_norm_input_vjp = try function(ctx, m, "termite_embedding_gemma2_rms_norm_input_vjp_f32"),
            .training_rms_norm_input_vjp = try function(ctx, m, "termite_embedding_gemma2_training_rms_norm_input_vjp_f32"),
            .rope = try function(ctx, m, "termite_embedding_gemma2_rope_bf16"),
            .rope_vjp = try function(ctx, m, "termite_embedding_gemma2_rope_vjp_f32"),
            .training_rope = try function(ctx, m, "termite_embedding_gemma2_training_rope_bf16"),
            .training_rope_vjp = try function(ctx, m, "termite_embedding_gemma2_training_rope_vjp_f32"),
            .packed_heads = try function(ctx, m, "termite_embedding_gemma2_packed_heads_bf16"),
            .packed_gelu = try function(ctx, m, "termite_embedding_gemma2_packed_gelu_bf16"),
            .split_columns = try function(ctx, m, "termite_embedding_gemma2_split_columns_bf16"),
            .vision_rope_pack = try function(ctx, m, "termite_embedding_gemma2_vision_rope_pack_bf16"),
            .audio_segment_conv = try function(ctx, m, "termite_embedding_gemma2_audio_segment_conv_f32"),
            .native_vision = try function(ctx, m, "termite_embedding_gemma2_native_vision_bf16"),
            .norm_residual = try function(ctx, m, "termite_embedding_gemma2_norm_residual_bf16"),
            .scale_rms = try function(ctx, m, "termite_embedding_gemma2_scale_rms_bf16"),
            .attention = try function(ctx, m, "termite_embedding_gemma2_attention_bf16"),
            .local_flash_attention = try function(ctx, m, "termite_embedding_gemma2_attention_local_flash_bf16"),
            .attention_softmax = try function(ctx, m, "termite_embedding_gemma2_attention_softmax_bf16"),
            .matmul = try function(ctx, m, "termite_embedding_gemma2_matmul_bf16"),
            .gelu_mul = try function(ctx, m, "termite_embedding_gemma2_gelu_mul_bf16"),
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
            .gelu_ple = try function(ctx, m, "termite_embedding_gemma2_gelu_ple_bf16"),
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
            .mrl_normalize = try function(ctx, m, "termite_embedding_gemma2_mrl_normalize_f32"),
            .mrl_normalize_vjp = try function(ctx, m, "termite_embedding_gemma2_mrl_normalize_vjp_f32"),
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
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&hp), @ptrCast(&sp), @ptrCast(&fp), @ptrCast(&wp), @ptrCast(&w), @ptrCast(&h), @ptrCast(&patch), @ptrCast(&bits) };
        try self.launch1d(ctx, self.pillow_vertical_patches, &args, @intCast(count));
    }

    pub fn launchAudioAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, rel: Buffer, scales: Buffer, valid: Buffer, p: @import("../ops.zig").Gemma4AudioLocalAttentionParams) !void {
        if (p.rows == 0 or p.rows > std.math.maxInt(i32) or p.hidden == 0 or p.heads == 0 or p.heads > 65535 or p.head_dim == 0 or
            p.chunk == 0 or p.chunk > std.math.maxInt(i32) or p.context == 0 or p.context_left < 2 or p.context_left > p.context or p.context > 24 or
            !std.math.isFinite(p.k_scale) or !std.math.isFinite(p.logit_cap) or p.logit_cap <= 0 or !std.math.isFinite(p.invalid_value))
            return error.InvalidEmbeddingGemma2Shape;
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
        var flags: u32 = @as(u32, @intFromBool(p.exclude_farthest_key)) | (@as(u32, @intFromBool(p.segmented)) << 1);
        var ks = p.k_scale;
        var cap = p.logit_cap;
        var invalid = p.invalid_value;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&rp), @ptrCast(&sp), @ptrCast(&mp), @ptrCast(&rows), @ptrCast(&hidden), @ptrCast(&heads), @ptrCast(&hd), @ptrCast(&chunk), @ptrCast(&left), @ptrCast(&context), @ptrCast(&ks), @ptrCast(&cap), @ptrCast(&invalid), @ptrCast(&flags) };
        try launch(ctx, self.audio_attention, &args, .{ rows, heads, 1 }, .{ 32, 1, 1 });
        self.stats_.launches += 1;
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

    pub fn notePackedPle(self: *Module) void {
        self.stats_.packed_ple_groups += 1;
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
    pub fn launchScaleRms(self: *Module, ctx: *Context, output: Buffer, input: Buffer, weight: Buffer, rows: u32, width: u32, scale: f32, eps: f32) !void {
        return self.launchScaleRmsStrided(ctx, output, input, weight, rows, width, width, scale, eps);
    }
    /// Read a packed layer directly, retaining GEMM and scaled BF16 boundaries.
    pub fn launchScaleRmsStrided(self: *Module, ctx: *Context, output: Buffer, input: Buffer, weight: Buffer, rows: u32, width: u32, input_stride: u32, scale: f32, eps: f32) !void {
        if (rows == 0 or width == 0 or input_stride < width or (input_stride != width and output.ptr == input.ptr) or !std.math.isFinite(eps) or eps <= 0 or !std.math.isFinite(scale)) return error.InvalidEmbeddingGemma2Shape;
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), 2);
        try require(output, bytes);
        try require(input, try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, rows - 1, input_stride), width), 2));
        try require(weight, @as(usize, width) * 2);
        var op = output.ptr;
        var ip = input.ptr;
        var wp = weight.ptr;
        var r = rows;
        var w = width;
        var v = scale;
        var e = eps;
        var stride = input_stride;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&v), @ptrCast(&e), @ptrCast(&stride) };
        try launch(ctx, self.scale_rms, &args, .{ rows, 1, 1 }, .{ 256, 1, 1 });
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

    pub fn launchRmsNorm(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        if (rows == 0 or width == 0 or !std.math.isFinite(eps) or eps <= 0) return error.InvalidEmbeddingGemma2Shape;
        const bytes = @as(usize, rows) * width * 2;
        try require(out, bytes);
        try require(input, bytes);
        if (weight) |wbuf| try require(wbuf, @as(usize, width) * 2);
        var op = out.ptr;
        var ip = input.ptr;
        var wp = if (weight) |wbuf| wbuf.ptr else 0;
        var r = rows;
        var w = width;
        var e = eps;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&e) };
        try launch(ctx, self.rms_norm, &args, .{ rows, 1, 1 }, .{ 256, 1, 1 });
        self.stats_.launches += 1;
    }

    pub fn launchTrainingRmsNorm(self: *Module, ctx: *Context, out: Buffer, input: Buffer, weight: ?Buffer, rows: u32, width: u32, eps: f32) !void {
        if (rows == 0 or (width != 256 and width != 512) or !std.math.isFinite(eps) or eps <= 0) return error.InvalidEmbeddingGemma2Shape;
        const bytes = try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(u16));
        try require(out, bytes);
        try require(input, bytes);
        if (weight) |wbuf| try require(wbuf, @as(usize, width) * 2);
        const row_power = std.math.floorPowerOfTwo(u32, @min(rows, 16));
        const block_height: u32 = @max(1, row_power);
        const vectors = width / 4;
        const block_width: u32 = @min(vectors, 512 / block_height);
        var op = out.ptr;
        var ip = input.ptr;
        var wp = if (weight) |wbuf| wbuf.ptr else 0;
        var r = rows;
        var w = width;
        var e = eps;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&wp), @ptrCast(&r), @ptrCast(&w), @ptrCast(&e) };
        try launch(ctx, self.training_rms_norm, &args, .{ std.math.divCeil(u32, rows, block_height) catch return error.InvalidEmbeddingGemma2Shape, 1, 1 }, .{ block_width, block_height, 1 });
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

    pub fn launchAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, mask: Buffer, shape: Attention) !void {
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

    pub fn launchLocalFlashAttention(self: *Module, ctx: *Context, out: Buffer, q: Buffer, k: Buffer, v: Buffer, mask: Buffer, shape: Attention) !void {
        try shape.validate();
        if (!localFlashAttentionGeometrySupported(shape)) return error.InvalidEmbeddingGemma2Shape;
        const qb = try std.math.mul(usize, @as(usize, shape.batch) * shape.seq * shape.q_heads, shape.head_dim * 2);
        const kvb = try std.math.mul(usize, @as(usize, shape.batch) * shape.seq * shape.kv_heads, shape.head_dim * 2);
        try require(out, qb);
        try require(q, qb);
        try require(k, kvb);
        try require(v, kvb);
        if (mask.ptr != 0) try require(mask, @as(usize, shape.batch) * shape.seq * 8);
        const full_tiles = shape.seq / 64;
        if (full_tiles != 0) {
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
            var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&radius) };
            const shared = @as(u32, 16) * shape.head_dim * 2 + 4 * 64 * 16 * 4 + 64 * 16 * 2 + 3 * 64 * 4;
            try launchShared(ctx, self.local_flash_attention, &args, .{ shape.q_heads, full_tiles, shape.batch }, .{ 512, 1, 1 }, shared);
            self.stats_.launches += 1;
        }
        const tail = shape.seq % 64;
        if (tail != 0) {
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
            var query_offset = shape.seq - tail;
            var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&qp), @ptrCast(&kp), @ptrCast(&vp), @ptrCast(&mp), @ptrCast(&b), @ptrCast(&s), @ptrCast(&qh), @ptrCast(&kh), @ptrCast(&d), @ptrCast(&radius), @ptrCast(&query_offset) };
            try launch(ctx, self.attention, &args, .{ tail, shape.q_heads, shape.batch }, .{ 32, 1, 1 });
            self.stats_.launches += 1;
        }
        self.stats_.attention_launches += 1;
    }

    fn localFlashAttentionGeometrySupported(shape: Attention) bool {
        const local = shape.local_radius == 512 and shape.kv_heads == 2 and shape.head_dim == 256;
        const global = shape.local_radius == 0 and shape.kv_heads == 1 and shape.head_dim == 512;
        return shape.q_heads == 4 and (local or global);
    }

    pub fn launchAttentionSoftmax(self: *Module, ctx: *Context, probs: Buffer, scores: Buffer, mask: Buffer, sequence: u32, query_base: u32, rows: u32, batch_index: u32, heads: u32, tile_capacity: u32) !void {
        if (sequence == 0 or sequence > 8192 or tile_capacity == 0 or tile_capacity > 2048 or rows == 0 or rows > tile_capacity or query_base > sequence or rows > sequence - query_base or heads == 0 or heads > 64) return error.InvalidEmbeddingGemma2Shape;
        const elements = try std.math.mul(usize, try std.math.mul(usize, heads, tile_capacity), sequence);
        try require(scores, try std.math.mul(usize, elements, 4));
        try require(probs, try std.math.mul(usize, elements, 2));
        if (mask.ptr != 0) try require(mask, (@as(usize, batch_index) + 1) * sequence * 8);
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

    pub fn launchGeluPle(self: *Module, ctx: *Context, out: Buffer, gate: Buffer, ple: Buffer, count: u32) !void {
        const bytes = @as(usize, count) * 2;
        try require(out, bytes);
        try require(gate, bytes);
        try require(ple, bytes);
        var op = out.ptr;
        var gp = gate.ptr;
        var pp = ple.ptr;
        var n = count;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&gp), @ptrCast(&pp), @ptrCast(&n) };
        try self.launch1d(ctx, self.gelu_ple, &args, count);
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
        try self.launch1d(ctx, self.gelu_mul, &args, count);
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
        try require(input, try std.math.mul(usize, try std.math.mul(usize, rows, 768), @sizeOf(f32)));
        try require(out, try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(f32)));
        var op = out.ptr;
        var ip = input.ptr;
        var r = rows;
        var w = width;
        var args = [_]?*anyopaque{ @ptrCast(&op), @ptrCast(&ip), @ptrCast(&r), @ptrCast(&w) };
        try launchShared(ctx, self.mrl_normalize, &args, .{ rows, 1, 1 }, .{ 256, 1, 1 }, 8 * @sizeOf(f32));
        self.stats_.launches += 1;
    }
    pub fn launchMrlNormalizeVjp(self: *Module, ctx: *Context, dinput: Buffer, dout: Buffer, input: Buffer, rows: u32, width: u32, objective_weight: f32) !void {
        if (rows == 0 or rows > 384 or width == 0 or width > 768 or !std.math.isFinite(objective_weight)) return error.InvalidEmbeddingGemma2TrainingShape;
        try require(dinput, try std.math.mul(usize, try std.math.mul(usize, rows, 768), @sizeOf(f32)));
        try require(input, try std.math.mul(usize, try std.math.mul(usize, rows, 768), @sizeOf(f32)));
        try require(dout, try std.math.mul(usize, try std.math.mul(usize, rows, width), @sizeOf(f32)));
        var dip = dinput.ptr;
        var dop = dout.ptr;
        var ip = input.ptr;
        var r = rows;
        var w = width;
        var weight = objective_weight;
        var args = [_]?*anyopaque{ @ptrCast(&dip), @ptrCast(&dop), @ptrCast(&ip), @ptrCast(&r), @ptrCast(&w), @ptrCast(&weight) };
        try launchShared(ctx, self.mrl_normalize_vjp, &args, .{ rows, 1, 1 }, .{ 256, 1, 1 }, 16 * @sizeOf(f32));
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
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(f, grid[0], grid[1], grid[2], block[0], block[1], block[2], 0, ctx.stream, args.ptr, null));
    ctx.noteKernelLaunch();
}
fn launchShared(ctx: *Context, f: driver.CUfunction, args: []?*anyopaque, grid: [3]u32, block: [3]u32, shared: u32) !void {
    try ctx.makeCurrent();
    try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(f, grid[0], grid[1], grid[2], block[0], block[1], block[2], shared, ctx.stream, args.ptr, null));
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
    try std.testing.expect(!Module.localFlashAttentionGeometrySupported(.{ .batch = 32, .seq = 512, .q_heads = 4, .kv_heads = 1, .head_dim = 512, .local_radius = 512 }));
    try std.testing.expect(!Module.localFlashAttentionGeometrySupported(.{ .batch = 32, .seq = 512, .q_heads = 4, .kv_heads = 2, .head_dim = 256, .local_radius = 0 }));
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
    var ctx = try Context.initDefault();
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
    var ctx = try Context.initDefault();
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
                    try module.launchAttention(&ctx, expected, q, k, v, mask, shape);
                    try ctx.synchronize();
                    const reference_ns = std.Io.Clock.awake.now(clock).nanoseconds - reference_start.nanoseconds;
                    const candidate_start = std.Io.Clock.awake.now(clock);
                    try module.launchLocalFlashAttention(&ctx, actual, q, k, v, mask, shape);
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
