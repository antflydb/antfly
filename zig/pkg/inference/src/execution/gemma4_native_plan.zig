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
    vision_f16: bool = true,

    /// Publish a request allowance for the qualified full-window decoder in
    /// addition to the existing auxiliary/media envelope. These are automatic
    /// category floors, not reservations: callers still admit actual amounts,
    /// apply operator overrides last, and retain physical/process admission.
    pub fn generationBudgetFloor(self: Plan, config: Config, backend_residency_bytes: usize, auxiliary: @import("../runtime/tier/memory.zig").Limits) !@import("../runtime/tier/memory.zig").Limits {
        const memory = @import("../runtime/tier/memory.zig");
        const context_tokens = @min(@as(usize, config.max_position_embeddings), @import("../graph/quant_kernel_cuda_renderer.zig").FlashPrefillQueryLengthPolicy.paged_f16_q2_to_8192_128k_v2.maxContextTokens());
        if (context_tokens < 2) return error.InvalidModelConfig;
        const component = memory.GptGenerationBudgetComponent{
            .backend = .cuda,
            .kv_dtype = .f16,
            .config = config,
            .prompt_input_copies = 2,
            .host_prompt_input_copies = 1,
        };
        return component.fullWindowBudgetFloor(context_tokens, self.geometry.prefillChunkRows(context_tokens, 0), backend_residency_bytes, auxiliary);
    }

    pub fn resolve(config: Config) ?Plan {
        return .{
            .projection = selected() orelse return null,
            .geometry = Geometry.resolve(config) orelse return null,
            .wide_f32_vendor = !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_WIDE_F32_VENDOR"),
            .flash_prefill = !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_PAGED_F16_FLASH"),
            .capture_prefill = platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_CAPTURE_PREFILL"),
            .trace_prefill = platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_TRACE_PREFILL_PROGRAM"),
            .warp_decode = platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_F16_WARP_DECODE", true),
            .vision_f16 = platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_GEMMA4_VISION_F16", true),
        };
    }
};

pub const Geometry = struct {
    donor_layers: usize,

    /// Bound standalone execution by the installed physical query schedule.
    /// Explicit chunks and scheduler admission remain authoritative.
    pub fn prefillQueryLimit(_: Geometry) usize {
        return @import("../graph/quant_kernel_cuda_renderer.zig").FlashPrefillQueryLengthPolicy.paged_f16_q2_to_8192_128k_v2.maxQueryRows();
    }

    /// Short prefixes retain their allocation-free functional program. Long
    /// prefixes use the measured bounded working set rather than filling the
    /// largest legal query tile. Explicit/admitted chunks remain authoritative.
    pub fn prefillChunkRows(self: Geometry, prompt_rows: usize, requested_rows: usize) usize {
        const physical = self.prefillQueryLimit();
        return @min(physical, if (requested_rows != 0) requested_rows else if (prompt_rows > physical) @as(usize, 2048) else physical);
    }

    pub fn resolve(config: Config) ?Geometry {
        // Scope the plan to the E4B geometry exercised by the native prefill
        // path. Other Gemma configurations continue through their own routes.
        const boundary = config.sharedKvPrefillDonorLayers() orelse return null;
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

test "Gemma4 CUDA bounded prefill scratch retains explicit chunks" {
    const geometry: Geometry = .{ .donor_layers = 24 };
    try std.testing.expectEqual(@as(usize, 8192), geometry.prefillChunkRows(8192, 0));
    try std.testing.expectEqual(@as(usize, 2048), geometry.prefillChunkRows(8193, 0));
    try std.testing.expectEqual(@as(usize, 2048), geometry.prefillChunkRows(131040, 0));
    try std.testing.expectEqual(@as(usize, 1024), geometry.prefillChunkRows(131040, 1024));
    try std.testing.expectEqual(@as(usize, 8192), geometry.prefillChunkRows(131040, 8192));
    try std.testing.expectEqual(@as(usize, 8192), geometry.prefillChunkRows(131040, 16384));
}

test "Gemma4 CUDA full window budget floor preserves explicit limits" {
    const memory = @import("../runtime/tier/memory.zig");
    const config = Config{ .family = .gemma, .hidden_size = 2560, .num_hidden_layers = 42, .num_attention_heads = 8, .num_key_value_heads = 2, .attention_head_dim = 256, .global_head_dim = 512, .intermediate_size = 10240, .num_kv_shared_layers = 18, .sliding_window = 512, .sliding_window_pattern = 6, .ple_hidden_size = 256, .vocab_size = 262144, .max_position_embeddings = 131072 };
    const plan = Plan{ .projection = .bf16_donors, .geometry = .{ .donor_layers = 24 } };
    const auxiliary = memory.Limits{ .host_limit_bytes = 3 << 30, .backend_limit_bytes = 9 << 30, .combined_limit_bytes = 12 << 30, .kv_limit_bytes = 3 << 30, .scratch_limit_bytes = 2 << 30 };
    const resident_bytes: usize = 13 << 30;
    const floor = try plan.generationBudgetFloor(config, resident_bytes, auxiliary);
    const component = memory.GptGenerationBudgetComponent{ .backend = .cuda, .kv_dtype = .f16, .config = config, .prompt_input_copies = 2, .host_prompt_input_copies = 1 };
    const required = try component.estimate(131040, 32, 2048);
    try std.testing.expect(floor.kv_limit_bytes >= required.kv_bytes);
    try std.testing.expect(floor.backend_limit_bytes >= required.kv_bytes + required.scratch_bytes);
    var admitted = memory.RunBudget.init(floor);
    // Persistent weights share the model domain with each request. The
    // automatic floor must admit the planned chunk beside real residency.
    try admitted.tryReserve(.{ .kind = .weight, .tier = .backend, .bytes = resident_bytes });
    try admitted.reserveEstimate(required);
    admitted.releaseEstimate(required);
    try std.testing.expectEqual(resident_bytes, admitted.backendTotalBytes());
    admitted.release(.{ .kind = .weight, .tier = .backend, .bytes = resident_bytes });
    try std.testing.expectEqual(@as(usize, 0), admitted.backendTotalBytes());
    try std.testing.expectEqual(@as(usize, 0), admitted.hostTotalBytes());
    // The same request must still fail an operator's explicit KV cap. A
    // model floor never edits a shared/physical domain or waives that cap.
    const restricted = memory.applyLimitOverrides(floor, .{ .kv_limit_bytes = auxiliary.kv_limit_bytes });
    var denied = memory.RunBudget.init(restricted);
    try std.testing.expectError(error.MemoryBudgetExceeded, denied.reserveEstimate(required));
    try std.testing.expectEqual(@as(usize, 0), denied.backendTotalBytes());
    inline for (@typeInfo(memory.Limits).@"struct".field_names) |name| {
        var override: memory.Limits = .{};
        @field(override, name) = 1;
        try std.testing.expectEqual(@as(usize, 1), @field(memory.applyLimitOverrides(floor, override), name));
    }
}
