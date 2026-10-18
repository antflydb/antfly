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
pub const Policy = struct {
    extended_flash: bool = false,
    fused_vision: bool = false,
    packed_projections: bool = false,
    graph_replay: bool = false,
    image_batch: bool = false,
    native_vision: bool = false,
    media_batch: bool = false,
    fused_backbone: bool = false,
    tuned_gemm: bool = false,
    spatial_pool: bool = false,
    audio_conv_gemm: bool = false,
    audio_conv_bf16: bool = false,
    audio_fft: bool = false,
    audio_frontend_fused: bool = false,
    audio_graph: bool = false,
    pub fn arithmeticBits(self: Policy) u16 {
        return @as(u16, @intFromBool(self.extended_flash)) | (@as(u16, @intFromBool(self.fused_vision)) << 1) | (@as(u16, @intFromBool(self.packed_projections)) << 2) | (@as(u16, @intFromBool(self.graph_replay)) << 3) | (@as(u16, @intFromBool(self.image_batch)) << 4) | (@as(u16, @intFromBool(self.native_vision)) << 5) | (@as(u16, @intFromBool(self.media_batch)) << 6) | (@as(u16, @intFromBool(self.fused_backbone)) << 7) | (@as(u16, @intFromBool(self.tuned_gemm)) << 8) | (@as(u16, @intFromBool(self.spatial_pool)) << 9) | (@as(u16, @intFromBool(self.audio_conv_gemm)) << 10) | (@as(u16, @intFromBool(self.audio_conv_bf16)) << 11) | (@as(u16, @intFromBool(self.audio_fft)) << 12) | (@as(u16, @intFromBool(self.audio_frontend_fused)) << 13) | (@as(u16, @intFromBool(self.audio_graph)) << 14);
    }
};
pub fn configured() Policy {
    return .{
        .audio_conv_gemm = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_CONV_GEMM"),
        .audio_conv_bf16 = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_CONV_BF16"),
        .audio_graph = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_GRAPH"),
        .audio_frontend_fused = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_FRONTEND_FUSED"),
        .audio_fft = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_FFT"),
        .spatial_pool = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_SPATIAL_POOL", false),
        .tuned_gemm = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_TUNED_GEMM", false),
        .fused_backbone = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE", false),
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
    const profiles = [_]Policy{ .{}, .{ .extended_flash = true }, .{ .fused_vision = true }, .{ .packed_projections = true }, .{ .graph_replay = true }, .{ .image_batch = true }, .{ .native_vision = true }, .{ .media_batch = true }, .{ .fused_backbone = true }, .{ .tuned_gemm = true }, .{ .spatial_pool = true }, .{ .audio_conv_gemm = true }, .{ .audio_conv_bf16 = true }, .{ .audio_fft = true }, .{ .audio_frontend_fused = true }, .{ .audio_graph = true }, .{ .tuned_gemm = true, .packed_projections = true }, .{ .tuned_gemm = true, .fused_backbone = true, .graph_replay = true } };
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
