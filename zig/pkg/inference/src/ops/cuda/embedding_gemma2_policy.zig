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

//! Immutable per-session qualification policy. New arithmetic routes are opt-in
//! until matched vector/ranking and completed-device benchmarks qualify them.
const env = @import("antfly_platform").env;
/// Complete eager audio requires classic BF16, the source addmm Lt bias
/// epilogue, and a retained source convolution provider. Text is independent.
pub const AudioArithmetic = enum {
    full_f32,
    source_bf16,
    pub fn needsConvolution(audio_capable: bool, policy: @import("libraries.zig").Policy, classic: bool, bias_epilogue: bool) bool {
        return audio_capable and policy != .off and classic and bias_epilogue;
    }
    pub fn select(policy: @import("libraries.zig").Policy, classic: bool, bias_epilogue: bool, convolution: bool) !@This() {
        if (policy == .off) return .full_f32;
        if (classic and bias_epilogue and convolution) return .source_bf16;
        if (policy == .required) return if (!classic or !bias_epilogue) error.CudaLibrariesUnavailable else error.CudnnUnavailable;
        return .full_f32;
    }
};

/// Cold source phase selection uses the retained vendor program and cached
/// device capability only. It is independent of classic/text arithmetic.
pub fn selectSourceVisionGeometry(retained_lt: bool, compute_major: i32) @import("../ops.zig").SourceVisionGeometry {
    return if (retained_lt and compute_major >= 8) .source_padded else .legacy_dense;
}

pub const PrefixAttentionSchedule = enum(u32) { tiled_bf16, online_wmma_bf16, register_bf16_q64_k64 };

pub const SourceConvolution = enum { legacy, backend_graph };

/// Preference limit over an already funded source BF16 bias workspace.
/// This changes vendor plan selection without acquiring additional storage.
pub const SourceBiasWorkspace = enum {
    funded_capacity,
    source_1mib,

    pub const source_bytes: usize = 1024 * 1024;
    pub fn bytes(self: @This(), funded: usize) usize {
        return switch (self) {
            .funded_capacity => funded,
            .source_1mib => @min(funded, source_bytes),
        };
    }
};

pub const Policy = struct {
    /// Actual cold-selected full padded source phase. A missing retained Lt
    /// keeps the complete historical dense-row phase, independently of text
    /// and audio arithmetic fallback. Never infer this from model/backend name.
    source_vision_geometry: @import("../ops.zig").SourceVisionGeometry = .source_padded,
    audio_arithmetic: AudioArithmetic = .source_bf16,
    /// Default available-vendor serving contract. A cold optional fallback
    /// publishes its different arithmetic through this same value-only policy.
    matmul_arithmetic: @import("libraries.zig").MatmulArithmetic = .bf16_reduced,
    source_bias_workspace: SourceBiasWorkspace = .funded_capacity,
    source_convolution: SourceConvolution = .legacy,
    extended_flash: bool = false,
    fused_vision: bool = false,
    packed_projections: bool = false,
    graph_replay: bool = false,
    image_batch: bool = false,
    native_vision: bool = false,
    /// Register-output Q64/K64 BF16 prefix-key schedule; qualification opt-in only.
    prefix_rf_q64_k64: bool = false,
    media_batch: bool = false,
    fused_backbone: bool = false,
    tuned_gemm: bool = false,
    spatial_pool: bool = false,
    audio_conv_gemm: bool = false,
    audio_conv_bf16: bool = false,
    audio_fft: bool = false,
    audio_frontend_fused: bool = false,
    audio_graph: bool = false,
    pub fn prefixAttentionSchedule(self: Policy, native_available: bool) !PrefixAttentionSchedule {
        if (self.prefix_rf_q64_k64) {
            if (!native_available) return error.CudaKernelUnavailable;
            return .register_bf16_q64_k64;
        }
        return if (self.native_vision and native_available) .online_wmma_bf16 else .tiled_bf16;
    }
    pub fn arithmeticBits(self: Policy) u32 {
        return @as(u32, @intFromBool(self.extended_flash)) | (@as(u32, @intFromBool(self.fused_vision)) << 1) | (@as(u32, @intFromBool(self.packed_projections)) << 2) | (@as(u32, @intFromBool(self.graph_replay)) << 3) | (@as(u32, @intFromBool(self.image_batch)) << 4) | (@as(u32, @intFromBool(self.native_vision)) << 5) | (@as(u32, @intFromBool(self.media_batch)) << 6) | (@as(u32, @intFromBool(self.fused_backbone)) << 7) | (@as(u32, @intFromBool(self.tuned_gemm)) << 8) | (@as(u32, @intFromBool(self.spatial_pool)) << 9) | (@as(u32, @intFromBool(self.audio_conv_gemm)) << 10) | (@as(u32, @intFromBool(self.audio_conv_bf16)) << 11) | (@as(u32, @intFromBool(self.audio_fft)) << 12) | (@as(u32, @intFromBool(self.audio_frontend_fused)) << 13) | (@as(u32, @intFromBool(self.audio_graph)) << 14) | (@as(u32, @intFromBool(self.matmul_arithmetic == .full_f32)) << 15) | (@as(u32, @intFromBool(self.audio_arithmetic == .full_f32)) << 16) | (@as(u32, @intFromBool(self.source_vision_geometry == .legacy_dense)) << 17) | (@as(u32, @intFromBool(self.prefix_rf_q64_k64)) << 18) | (@as(u32, @intFromBool(self.source_bias_workspace == .source_1mib)) << 19) | (@as(u32, @intFromBool(self.source_convolution == .backend_graph)) << 20);
    }
};
pub fn configured() Policy {
    return .{
        .source_convolution = if (env.getenvBool("ANTFLY_INFERENCE_CUDA_BF16_CONVOLUTION_GRAPH")) .backend_graph else .legacy,
        .source_bias_workspace = if (env.getenvBool("ANTFLY_INFERENCE_CUDA_SOURCE_BIAS_WORKSPACE_1MIB")) .source_1mib else .funded_capacity,
        .audio_arithmetic = if (@import("libraries.zig").Policy.current() == .off) .full_f32 else .source_bf16,
        .matmul_arithmetic = if (@import("libraries.zig").Policy.current() == .off) .full_f32 else .bf16_reduced,
        .audio_conv_gemm = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_CONV_GEMM"),
        .audio_conv_bf16 = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_CONV_BF16"),
        .audio_graph = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_GRAPH"),
        .audio_frontend_fused = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_FRONTEND_FUSED"),
        .audio_fft = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_FFT"),
        .spatial_pool = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_SPATIAL_POOL", false),
        .tuned_gemm = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_TUNED_GEMM", false),
        .fused_backbone = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE", false),
        .prefix_rf_q64_k64 = env.getenvBool("ANTFLY_INFERENCE_CUDA_BF16_PREFIX_ATTENTION"),
        .native_vision = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_NATIVE_VISION", false),
        .media_batch = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_MEDIA_BATCH", false),
        .extended_flash = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_EXTENDED_FLASH", false),
        .fused_vision = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_VISION", false),
        .packed_projections = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_PACKED_PROJECTIONS", false),
        .image_batch = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_IMAGE_BATCH", false),
        .graph_replay = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GRAPH_REPLAY", false),
    };
}

test "embeddinggemma2 CUDA arithmetic profiles are disjoint including graph padding and image cohorts" {
    const std = @import("std");
    const profiles = [_]Policy{ .{}, .{ .matmul_arithmetic = .full_f32 }, .{ .extended_flash = true }, .{ .fused_vision = true }, .{ .packed_projections = true }, .{ .graph_replay = true }, .{ .image_batch = true }, .{ .native_vision = true }, .{ .prefix_rf_q64_k64 = true }, .{ .source_bias_workspace = .source_1mib }, .{ .source_convolution = .backend_graph }, .{ .media_batch = true }, .{ .fused_backbone = true }, .{ .tuned_gemm = true }, .{ .spatial_pool = true }, .{ .audio_conv_gemm = true }, .{ .audio_conv_bf16 = true }, .{ .audio_fft = true }, .{ .audio_frontend_fused = true }, .{ .audio_graph = true }, .{ .tuned_gemm = true, .packed_projections = true }, .{ .tuned_gemm = true, .fused_backbone = true, .graph_replay = true } };
    for (profiles, 0..) |left, i| for (profiles[i + 1 ..]) |right| try std.testing.expect(left.arithmeticBits() != right.arithmeticBits());
}

/// Bound redundant GEMM rows independently of graph reuse. Small queries retain
/// their exact geometry; tiled tails may pad only within a 12.5% work budget.
pub fn graphSequence(sequence: usize, maximum: usize) usize {
    if (sequence < 128) return sequence;
    const std = @import("std");
    const rounded = std.math.add(usize, sequence, 63) catch return sequence;
    const candidate = rounded / 64 * 64;
    return if (candidate <= maximum and candidate - sequence <= sequence / 8) candidate else sequence;
}

pub const AttentionRoutes = struct { cudnn: bool = false, tiled: bool = false };
/// Only official GQA/MQA layouts enter the extended profile. Arbitrary masks
/// use native tiles; cuDNN descriptors accept unmasked or right-padded rows.
pub fn attentionRoutes(shape: anytype, has_wmma: bool, mask_kind: u8) AttentionRoutes {
    const local = shape.local_radius == 512 and shape.kv_heads == 2 and shape.head_dim == 256;
    const global = shape.local_radius == 0 and shape.kv_heads == 1 and shape.head_dim == 512;
    if (shape.batch == 0 or shape.seq == 0 or shape.seq > 8192 or shape.q_heads != 4 or (!local and !global)) return .{};
    return .{ .cudnn = shape.seq >= 16 and mask_kind < 2, .tiled = shape.seq >= 64 and has_wmma };
}

test "embeddinggemma2 graph buckets bound redundant work for short and tail queries" {
    const std = @import("std");
    for (1..8193) |sequence| {
        const bucket = graphSequence(sequence, 8192);
        try std.testing.expect(bucket >= sequence and bucket <= 8192);
        try std.testing.expect(bucket - sequence <= sequence / 8);
        if (sequence < 128) try std.testing.expectEqual(sequence, bucket);
    }
    try std.testing.expectEqual(@as(usize, 8191), graphSequence(8191, 8191));
    try std.testing.expectEqual(std.math.maxInt(usize), graphSequence(std.math.maxInt(usize), std.math.maxInt(usize)));
}

test "embeddinggemma2 extended attention routes qualify mask geometry and artifact boundaries" {
    const std = @import("std");
    const Shape = struct { batch: u32 = 2, seq: u32, q_heads: u32 = 4, kv_heads: u32 = 1, head_dim: u32 = 512, local_radius: u32 = 0 };
    for ([_]u32{ 1, 15, 16, 63, 64, 65, 512, 8192 }) |seq| {
        for (0..3) |mask| {
            const routes = attentionRoutes(Shape{ .seq = seq }, true, @intCast(mask));
            try std.testing.expectEqual(seq >= 16 and mask < 2, routes.cudnn);
            try std.testing.expectEqual(seq >= 64, routes.tiled);
            try std.testing.expect(!attentionRoutes(Shape{ .seq = seq }, false, @intCast(mask)).tiled);
        }
    }
    try std.testing.expect(!attentionRoutes(Shape{ .seq = 8193 }, true, 0).cudnn);
    try std.testing.expect(!attentionRoutes(Shape{ .seq = 512, .q_heads = 8 }, true, 0).tiled);
    try std.testing.expect(attentionRoutes(Shape{ .seq = 65, .kv_heads = 2, .head_dim = 256, .local_radius = 512 }, true, 2).tiled);
}

test "cold audio completeness is independent of bias-free text arithmetic" {
    const std = @import("std");
    for ([_]bool{ false, true }) |classic| for ([_]bool{ false, true }) |bias| for ([_]bool{ false, true }) |convolution| {
        try std.testing.expectEqual(AudioArithmetic.full_f32, try AudioArithmetic.select(.off, classic, bias, convolution));
        try std.testing.expectEqual(if (classic and bias and convolution) AudioArithmetic.source_bf16 else AudioArithmetic.full_f32, try AudioArithmetic.select(.auto, classic, bias, convolution));
        if (classic and bias and convolution) {
            try std.testing.expectEqual(AudioArithmetic.source_bf16, try AudioArithmetic.select(.required, classic, bias, convolution));
        } else try std.testing.expectError(if (!classic or !bias) error.CudaLibrariesUnavailable else error.CudnnUnavailable, AudioArithmetic.select(.required, classic, bias, convolution));
        try std.testing.expect(!AudioArithmetic.needsConvolution(false, .auto, classic, bias));
        try std.testing.expect(!AudioArithmetic.needsConvolution(true, .off, classic, bias));
        try std.testing.expectEqual(classic and bias, AudioArithmetic.needsConvolution(true, .auto, classic, bias));
    };
    const complete = Policy{};
    const audio_fallback = Policy{ .audio_arithmetic = .full_f32 };
    try std.testing.expectEqual(complete.matmul_arithmetic, audio_fallback.matmul_arithmetic);
    try std.testing.expectEqual(@as(u32, 1 << 16), complete.arithmeticBits() ^ audio_fallback.arithmeticBits());
}

test "source vision geometry records retained Lt independently of text and audio arithmetic" {
    const std = @import("std");
    try std.testing.expectEqual(@import("../ops.zig").SourceVisionGeometry.legacy_dense, selectSourceVisionGeometry(false, 12));
    try std.testing.expectEqual(@import("../ops.zig").SourceVisionGeometry.legacy_dense, selectSourceVisionGeometry(true, 7));
    try std.testing.expectEqual(@import("../ops.zig").SourceVisionGeometry.source_padded, selectSourceVisionGeometry(true, 8));
    try std.testing.expectEqual(@import("../ops.zig").SourceVisionGeometry.source_padded, selectSourceVisionGeometry(true, 12));
    const padded = Policy{ .matmul_arithmetic = .full_f32, .audio_arithmetic = .full_f32 };
    const legacy = Policy{ .matmul_arithmetic = .full_f32, .audio_arithmetic = .full_f32, .source_vision_geometry = .legacy_dense };
    try std.testing.expectEqual(padded.matmul_arithmetic, legacy.matmul_arithmetic);
    try std.testing.expectEqual(padded.audio_arithmetic, legacy.audio_arithmetic);
    try std.testing.expectEqual(@as(u32, 1 << 17), padded.arithmeticBits() ^ legacy.arithmeticBits());
}

test "register BF16 Q64 K64 prefix attention is explicit and retains default routes" {
    const std = @import("std");
    try std.testing.expectEqual(PrefixAttentionSchedule.tiled_bf16, try (Policy{}).prefixAttentionSchedule(true));
    try std.testing.expectEqual(PrefixAttentionSchedule.online_wmma_bf16, try (Policy{ .native_vision = true }).prefixAttentionSchedule(true));
    try std.testing.expectEqual(PrefixAttentionSchedule.tiled_bf16, try (Policy{ .native_vision = true }).prefixAttentionSchedule(false));
    const efficient = Policy{ .prefix_rf_q64_k64 = true };
    try std.testing.expectEqual(PrefixAttentionSchedule.register_bf16_q64_k64, try efficient.prefixAttentionSchedule(true));
    try std.testing.expectError(error.CudaKernelUnavailable, efficient.prefixAttentionSchedule(false));
    try std.testing.expectEqual(@as(u32, 1) << 18, efficient.arithmeticBits() ^ (Policy{}).arithmeticBits());
}

test "source bias workspace preference never grows existing funding" {
    const std = @import("std");
    for ([_]usize{ 0, 256, SourceBiasWorkspace.source_bytes, 32 * 1024 * 1024, std.math.maxInt(usize) }) |funded| {
        try std.testing.expectEqual(funded, SourceBiasWorkspace.funded_capacity.bytes(funded));
        try std.testing.expectEqual(@min(funded, SourceBiasWorkspace.source_bytes), SourceBiasWorkspace.source_1mib.bytes(funded));
    }
    const candidate = Policy{ .source_bias_workspace = .source_1mib };
    try std.testing.expectEqual(@as(u32, 1) << 19, candidate.arithmeticBits() ^ (Policy{}).arithmeticBits());
}

test "backend convolution graph policy preserves default and separates cached arithmetic" {
    const std = @import("std");
    try std.testing.expectEqual(SourceConvolution.legacy, (Policy{}).source_convolution);
    const graph = Policy{ .source_convolution = .backend_graph };
    try std.testing.expectEqual(@as(u32, 1) << 20, graph.arithmeticBits() ^ (Policy{}).arithmeticBits());
    try std.testing.expectEqual((Policy{}).audio_arithmetic, graph.audio_arithmetic);
    try std.testing.expectEqual((Policy{}).matmul_arithmetic, graph.matmul_arithmetic);
}
