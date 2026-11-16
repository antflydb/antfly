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

//! Immutable dispatch recipe. Qualified L4 singleton encoders select the measured
//! profile automatically; explicit overrides retain their existing behavior.
const std = @import("std");
const env = @import("antfly_platform").env;
pub const automatic_recipe = "automatic-l4-b1-encoder8192-tiled-tails-v2";
pub const Stage = enum { encoder, vision };
pub const Device = struct {
    name: []const u8,
    major: i32,
    minor: i32,
    has_wmma: bool,
    has_blas: bool,

    pub fn qualified(self: Device) bool {
        return std.mem.eql(u8, self.name, "NVIDIA L4") and self.major == 8 and self.minor == 9 and self.has_wmma and self.has_blas;
    }
};
pub const Policy = struct {
    /// Potential defaults, distinct from explicit arithmetic flags. Hash this
    /// recipe before device discovery so planning and loaded identities agree.
    automatic_bits: u16 = 0,
    extended_flash: bool = false,
    global_gemm: bool = false,
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

    pub fn resolve(self: Policy, device: Device, stage: Stage, batch: usize, sequence: usize) Policy {
        var result = self;
        result.automatic_bits = 0;
        if (stage != .encoder or !device.qualified() or batch != 1 or sequence == 0 or sequence > 8192) return result;
        inline for (promoted_fields) |name| {
            if (self.automatic_bits & fieldBit(name) != 0) @field(result, name) = true;
        }
        // Graph buckets that refuse a costly tail also suppress the existing
        // 64-row padding. Those exact tails lose more attention throughput than
        // replay saves. Keep normal tiled padding unless replay was forced.
        if (self.automatic_bits & fieldBit("graph_replay") != 0 and
            sequence >= 128 and sequence % 64 != 0 and graphSequence(sequence, 8192) == sequence)
            result.graph_replay = false;
        return result;
    }

    pub fn globalPlansEnabled(self: Policy, device: Device) bool {
        return self.global_gemm or (device.qualified() and self.automatic_bits & fieldBit("global_gemm") != 0);
    }
    pub fn arithmeticBits(self: Policy) u16 {
        return @as(u16, @intFromBool(self.extended_flash)) | (@as(u16, @intFromBool(self.fused_vision)) << 1) | (@as(u16, @intFromBool(self.packed_projections)) << 2) | (@as(u16, @intFromBool(self.graph_replay)) << 3) | (@as(u16, @intFromBool(self.image_batch)) << 4) | (@as(u16, @intFromBool(self.native_vision)) << 5) | (@as(u16, @intFromBool(self.media_batch)) << 6) | (@as(u16, @intFromBool(self.fused_backbone)) << 7) | (@as(u16, @intFromBool(self.tuned_gemm)) << 8) | (@as(u16, @intFromBool(self.spatial_pool)) << 9) | (@as(u16, @intFromBool(self.audio_conv_gemm)) << 10) | (@as(u16, @intFromBool(self.audio_conv_bf16)) << 11) | (@as(u16, @intFromBool(self.audio_fft)) << 12) | (@as(u16, @intFromBool(self.audio_frontend_fused)) << 13) | (@as(u16, @intFromBool(self.audio_graph)) << 14) | (@as(u16, @intFromBool(self.global_gemm)) << 15);
    }
};
const promoted_fields = .{ "extended_flash", "global_gemm", "packed_projections", "graph_replay", "fused_backbone" };
fn fieldBit(comptime name: []const u8) u16 {
    var one = Policy{};
    @field(one, name) = true;
    return one.arithmeticBits();
}
pub const Overrides = struct {
    extended_flash: ?bool = null,
    global_gemm: ?bool = null,
    packed_projections: ?bool = null,
    graph_replay: ?bool = null,
    fused_backbone: ?bool = null,
};
pub fn automatic(enabled: bool, overrides: Overrides) Policy {
    var result = Policy{};
    inline for (promoted_fields) |name| {
        if (@field(overrides, name)) |value| {
            @field(result, name) = value;
        } else if (enabled) result.automatic_bits |= fieldBit(name);
    }
    return result;
}
fn optionalFlag(comptime name: [:0]const u8) ?bool {
    if (env.getenv(name) == null) return null;
    return env.getenvBoolDefault(name, false);
}
pub fn configured() Policy {
    var result = automatic(env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUTOMATIC", true), .{
        .global_gemm = optionalFlag("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GLOBAL_GEMM"),
        .extended_flash = optionalFlag("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_EXTENDED_FLASH"),
        .packed_projections = optionalFlag("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_PACKED_PROJECTIONS"),
        .graph_replay = optionalFlag("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GRAPH_REPLAY"),
        .fused_backbone = optionalFlag("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE"),
    });
    const experimental = Policy{
        .audio_conv_gemm = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_CONV_GEMM"),
        .audio_conv_bf16 = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_CONV_BF16"),
        .audio_graph = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_GRAPH"),
        .audio_frontend_fused = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_FRONTEND_FUSED"),
        .audio_fft = env.getenvBool("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_AUDIO_FFT"),
        .spatial_pool = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_SPATIAL_POOL", false),
        .tuned_gemm = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_TUNED_GEMM", false),
        .native_vision = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_NATIVE_VISION", false),
        .media_batch = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_MEDIA_BATCH", false),
        .fused_vision = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_VISION", false),
        .image_batch = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_IMAGE_BATCH", false),
    };
    inline for (@typeInfo(Policy).@"struct".field_names) |name| {
        if (comptime !std.mem.eql(u8, name, "automatic_bits")) {
            @field(result, name) = @field(result, name) or @field(experimental, name);
        }
    }
    return result;
}

const qualified_l4 = Device{ .name = "NVIDIA L4", .major = 8, .minor = 9, .has_wmma = true, .has_blas = true };

test "embeddinggemma2 CUDA arithmetic automatic defaults match qualified explicit stages" {
    const defaults = automatic(true, .{});
    const measured = automatic(false, .{ .extended_flash = true, .global_gemm = true, .packed_projections = true, .graph_replay = true, .fused_backbone = true });
    for ([_]usize{ 1, 29, 128, 304, 512, 754, 1157, 2048, 8192 }) |n| {
        const resolved = defaults.resolve(qualified_l4, .encoder, 1, n);
        try std.testing.expectEqual(measured, resolved);
        try std.testing.expect(!resolved.tuned_gemm and !resolved.native_vision and !resolved.audio_conv_bf16 and !resolved.media_batch);
    }
    try std.testing.expectEqual(Policy{}, defaults.resolve(qualified_l4, .vision, 1, 2520));
    try std.testing.expect(defaults.globalPlansEnabled(qualified_l4));
    try std.testing.expect(defaults.automatic_bits != 0);
    try std.testing.expectEqual(@as(u16, 0), defaults.arithmeticBits());
}

test "embeddinggemma2 CUDA arithmetic automatic graph tails preserve tiled padding and explicit overrides" {
    const defaults = automatic(true, .{});
    for ([_]usize{ 129, 264, 270, 513 }) |sequence| {
        const resolved = defaults.resolve(qualified_l4, .encoder, 1, sequence);
        const expected_graph = graphSequence(sequence, 8192) != sequence;
        try std.testing.expectEqual(expected_graph, resolved.graph_replay);
        try std.testing.expect(resolved.extended_flash and resolved.packed_projections and resolved.fused_backbone and resolved.global_gemm);
        const forced = automatic(true, .{ .graph_replay = true });
        try std.testing.expect(forced.resolve(qualified_l4, .encoder, 1, sequence).graph_replay);
    }
}

test "embeddinggemma2 CUDA arithmetic automatic defaults reject unsupported geometry and devices" {
    const defaults = automatic(true, .{});
    for ([_]usize{ 0, 2, 4, 8 }) |batch| try std.testing.expectEqual(Policy{}, defaults.resolve(qualified_l4, .encoder, batch, 512));
    for ([_]usize{ 0, 8193, 40000 }) |sequence| try std.testing.expectEqual(Policy{}, defaults.resolve(qualified_l4, .encoder, 1, sequence));
    try std.testing.expectEqual(Policy{}, defaults.resolve(qualified_l4, .vision, 1, 3137));
    try std.testing.expectEqual(Policy{}, defaults.resolve(qualified_l4, .vision, 2, 2520));
    for ([_]Device{
        .{ .name = "NVIDIA A100", .major = 8, .minor = 0, .has_wmma = true, .has_blas = true },
        .{ .name = "NVIDIA GeForce RTX 4090", .major = 8, .minor = 9, .has_wmma = true, .has_blas = true },
        .{ .name = "NVIDIA L4", .major = 9, .minor = 0, .has_wmma = true, .has_blas = true },
        .{ .name = "NVIDIA L4", .major = 8, .minor = 9, .has_wmma = false, .has_blas = true },
        .{ .name = "NVIDIA L4", .major = 8, .minor = 9, .has_wmma = true, .has_blas = false },
    }) |device| {
        try std.testing.expectEqual(Policy{}, defaults.resolve(device, .encoder, 1, 2048));
        try std.testing.expect(!defaults.globalPlansEnabled(device));
    }
}

test "embeddinggemma2 CUDA arithmetic automatic defaults honor explicit enables and disables" {
    try std.testing.expectEqual(Policy{}, automatic(false, .{}));
    const no_global = automatic(true, .{ .global_gemm = false });
    const resolved = no_global.resolve(qualified_l4, .encoder, 1, 8192);
    try std.testing.expect(!resolved.global_gemm and resolved.extended_flash and resolved.packed_projections and resolved.graph_replay and resolved.fused_backbone);
    try std.testing.expect(!no_global.globalPlansEnabled(qualified_l4));
    const manual = automatic(false, .{ .packed_projections = true, .global_gemm = false });
    try std.testing.expectEqual(Policy{ .packed_projections = true }, manual.resolve(qualified_l4, .encoder, 4, 16384));
    const disabled = automatic(true, .{ .extended_flash = false, .global_gemm = false, .packed_projections = false, .graph_replay = false, .fused_backbone = false });
    try std.testing.expectEqual(Policy{}, disabled);
}

test "embeddinggemma2 CUDA arithmetic profiles are disjoint including graph padding and image cohorts" {
    const profiles = [_]Policy{ .{}, .{ .global_gemm = true }, .{ .extended_flash = true }, .{ .fused_vision = true }, .{ .packed_projections = true }, .{ .graph_replay = true }, .{ .image_batch = true }, .{ .native_vision = true }, .{ .media_batch = true }, .{ .fused_backbone = true }, .{ .tuned_gemm = true }, .{ .spatial_pool = true }, .{ .audio_conv_gemm = true }, .{ .audio_conv_bf16 = true }, .{ .audio_fft = true }, .{ .audio_frontend_fused = true }, .{ .audio_graph = true }, .{ .tuned_gemm = true, .packed_projections = true }, .{ .tuned_gemm = true, .fused_backbone = true, .graph_replay = true } };
    for (profiles, 0..) |left, i| for (profiles[i + 1 ..]) |right| try std.testing.expect(left.arithmeticBits() != right.arithmeticBits());
}

/// Bound redundant GEMM rows independently of graph reuse. Small queries retain
/// their exact geometry; tiled tails may pad only within a 12.5% work budget.
pub fn graphSequence(sequence: usize, maximum: usize) usize {
    if (sequence < 128) return sequence;
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
