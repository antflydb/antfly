// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native CUDA Gemma4 policy shared by model loading and request execution.
//! Decode continues to use the quantized primary weights and the same paged KV.
const std = @import("std");
const platform = @import("antfly_platform");
const Config = @import("../models/gpt.zig").Config;

pub const Projection = enum { packed_q4, bf16_donors };

pub fn selected() ?Projection {
    const value = platform.env.getenv("ANTFLY_INFERENCE_CUDA_GEMMA4_NATIVE_PLAN") orelse return null;
    return std.meta.stringToEnum(Projection, value);
}

pub const Plan = struct {
    projection: Projection,
    geometry: Geometry,
    wide_f32_vendor: bool = true,
    flash_prefill: bool = true,
    capture_prefill: bool = false,
    trace_prefill: bool = false,
    warp_decode: bool = false,

    pub fn resolve(config: Config) ?Plan {
        return .{
            .projection = selected() orelse return null,
            .geometry = Geometry.resolve(config) orelse return null,
            .wide_f32_vendor = !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_WIDE_F32_VENDOR"),
            .flash_prefill = !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_PAGED_F16_FLASH"),
            .capture_prefill = platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_CAPTURE_PREFILL"),
            .trace_prefill = platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_TRACE_PREFILL_PROGRAM"),
            .warp_decode = platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_F16_WARP_DECODE", true),
        };
    }
};

pub const Geometry = struct {
    donor_layers: usize,

    pub fn resolve(config: Config) ?Geometry {
        // Scope the plan to the E4B geometry exercised by the native prefill
        // path. Other Gemma configurations continue through their own routes.
        if (config.family != .gemma or config.hidden_size != 2560 or config.num_hidden_layers != 42 or
            config.num_kv_shared_layers != 18 or config.num_attention_heads != 8 or
            config.num_key_value_heads != 2 or config.attention_head_dim != 256 or
            config.global_head_dim != 512 or config.position_encoding != .rope or
            config.gemma4_mtp_assistant or config.usesMoe()) return null;
        const boundary = config.num_hidden_layers - config.num_kv_shared_layers;
        for (boundary..config.num_hidden_layers) |layer| {
            const donor = config.kvDonorGeometry(layer) catch return null;
            if (donor.donor_layer >= boundary or config.layerSharesKv(donor.donor_layer)) return null;
        }
        return .{ .donor_layers = boundary };
    }

    /// Only the full-row donor span uses mirrored projection matrices in
    /// this policy. The shared suffix and vocabulary stay quantized.
    pub fn mirrorsWeight(self: Geometry, name: []const u8) bool {
        const gguf = std.mem.startsWith(u8, name, "blk.");
        const prefix: []const u8 = if (gguf) "blk." else "model.layers.";
        if (!std.mem.startsWith(u8, name, prefix)) return false;
        const stem = name[prefix.len..];
        const separator = std.mem.indexOfScalar(u8, stem, '.') orelse return false;
        const layer = std.fmt.parseInt(usize, stem[0..separator], 10) catch return false;
        if (layer >= self.donor_layers) return false;
        const suffix = stem[separator + 1 ..];
        const names: []const []const u8 = if (gguf)
            &.{ "attn_q.weight", "attn_k.weight", "attn_v.weight", "attn_output.weight", "ffn_gate.weight", "ffn_up.weight", "ffn_down.weight" }
        else
            &.{ "self_attn.q_proj.weight", "self_attn.k_proj.weight", "self_attn.v_proj.weight", "self_attn.o_proj.weight", "mlp.gate_proj.weight", "mlp.up_proj.weight", "mlp.down_proj.weight" };
        for (names) |candidate| if (std.mem.eql(u8, suffix, candidate)) return true;
        return false;
    }
};

test "native Gemma4 donor mirror inventory excludes the shared suffix and non-projections" {
    const geometry: Geometry = .{ .donor_layers = 24 };
    try std.testing.expect(geometry.mirrorsWeight("blk.23.ffn_gate.weight"));
    try std.testing.expect(geometry.mirrorsWeight("model.layers.0.self_attn.q_proj.weight"));
    try std.testing.expect(!geometry.mirrorsWeight("blk.24.ffn_gate.weight"));
    try std.testing.expect(!geometry.mirrorsWeight("model.layers.24.self_attn.q_proj.weight"));
    try std.testing.expect(!geometry.mirrorsWeight("blk.0.attn_norm.weight"));
    try std.testing.expect(!geometry.mirrorsWeight("output.weight"));
}
