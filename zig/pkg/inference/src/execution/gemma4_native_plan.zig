// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Native CUDA Gemma4 policy shared by model loading and request execution.
//! Decode continues to use the quantized primary weights and the same paged KV.
const std = @import("std");
const platform = @import("antfly_platform");
const Config = @import("../models/gpt.zig").Config;

pub const Projection = enum { packed_q4, bf16_donors };

/// Explicit values, including off/invalid values, override automatic policy.
/// Probe only the same default device used by CudaContext; no context or model
/// allocation is created. Unknown devices retain the compatibility route.
pub fn explicitSelection() ?Projection {
    const value = platform.env.getenv("ANTFLY_INFERENCE_CUDA_GEMMA4_NATIVE_PLAN") orelse return null;
    return std.meta.stringToEnum(Projection, value);
}
var default_device: std.atomic.Value(u8) = .init(0);
fn qualifiedDefaultDevice() bool {
    if (comptime !@import("build_options").enable_cuda) return false;
    const cached = default_device.load(.acquire);
    if (cached != 0) return cached == 2;
    var driver = @import("../ops/cuda/driver.zig").CudaDriver.open() catch return false;
    defer driver.deinit();
    driver.check(driver.fns.cuInit(0)) catch return false;
    var device: @import("../ops/cuda/driver.zig").CUdevice = 0;
    driver.check(driver.fns.cuDeviceGet(&device, 0)) catch return false;
    var major: c_int = 0;
    var minor: c_int = 0;
    driver.check(driver.fns.cuDeviceComputeCapability(&major, &minor, device)) catch return false;
    const qualified = automaticProjection(major, minor) != null and @import("../ops/cuda/artifact.zig").hasBf16WmmaCodeFor(major, minor);
    default_device.store(if (qualified) 2 else 1, .release);
    return qualified;
}
fn automaticProjection(major: i32, minor: i32) ?Projection {
    return if (major == 12 and minor == 1) .bf16_donors else null;
}
pub fn selected() ?Projection {
    if (platform.env.getenv("ANTFLY_INFERENCE_CUDA_GEMMA4_NATIVE_PLAN") != null) return explicitSelection();
    return if (qualifiedDefaultDevice()) .bf16_donors else null;
}

/// Automatic rollout is narrower than explicit experimental policy. Check
/// parsed geometry and the actual donor storage inventory, never a model name.
pub fn automaticGeometry(config: Config) bool {
    return Geometry.resolve(config) != null and config.intermediate_size == 10240 and
        config.sliding_window == 512 and config.sliding_window_pattern == 6 and
        config.ple_hidden_size == 256 and config.vocab_size == 262144 and config.max_position_embeddings == 131072;
}
pub fn automaticArtifact(config: Config, file: ?*const @import("../gguf/format.zig").File) bool {
    if (!automaticGeometry(config)) return false;
    const geometry = Geometry.resolve(config).?;
    var seen: [24]u8 = @splat(0);
    if (geometry.donor_layers != seen.len) return false;
    for ((file orelse return false).tensors) |tensor| {
        if (!geometry.mirrorsWeight(tensor.name)) continue;
        if ((tensor.tensor_type != .known or tensor.tensor_type.known != .Q4_0) or tensor.dimensions.len != 2 or !std.mem.startsWith(u8, tensor.name, "blk.")) return false;
        const stem = tensor.name[4..];
        const dot = std.mem.indexOfScalar(u8, stem, '.') orelse return false;
        const layer = std.fmt.parseInt(usize, stem[0..dot], 10) catch return false;
        const suffix = stem[dot + 1 ..];
        const names = [_][]const u8{ "attn_q.weight", "attn_k.weight", "attn_v.weight", "attn_output.weight", "ffn_gate.weight", "ffn_up.weight", "ffn_down.weight" };
        const head = config.effectiveHeadDimForLayer(layer);
        const q = @as(u64, config.num_attention_heads) * head;
        const kv = @as(u64, config.num_key_value_heads) * head;
        const h: u64 = config.hidden_size;
        const f: u64 = config.intermediate_size;
        const shapes = [_][2]u64{ .{ h, q }, .{ h, kv }, .{ h, kv }, .{ q, h }, .{ h, f }, .{ h, f }, .{ f, h } };
        for (names, shapes, 0..) |name, shape, index| {
            if (!std.mem.eql(u8, suffix, name)) continue;
            const bit: u8 = @as(u8, 1) << @intCast(index);
            if (seen[layer] & bit != 0 or !std.mem.eql(u64, tensor.dimensions, &shape)) return false;
            seen[layer] |= bit;
            break;
        }
    }
    for (seen) |mask| if (mask != 0x7f) return false;
    return true;
}

pub const Plan = struct {
    projection: Projection,
    /// Installed only after exact artifact qualification and load admission.
    automatic: bool = false,
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
        const projection = selected() orelse return null;
        if (platform.env.getenv("ANTFLY_INFERENCE_CUDA_GEMMA4_NATIVE_PLAN") == null and !automaticGeometry(config)) return null;
        return .{
            .projection = projection,
            .automatic = platform.env.getenv("ANTFLY_INFERENCE_CUDA_GEMMA4_NATIVE_PLAN") == null,
            .geometry = Geometry.resolve(config) orelse return null,
            .wide_f32_vendor = !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_WIDE_F32_VENDOR"),
            .flash_prefill = !platform.env.getenvBool("ANTFLY_INFERENCE_CUDA_DISABLE_PAGED_F16_FLASH"),
            .capture_prefill = platform.env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_CAPTURE_PREFILL", platform.env.getenv("ANTFLY_INFERENCE_CUDA_GEMMA4_NATIVE_PLAN") == null and qualifiedDefaultDevice()),
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

    /// Additional resident storage for the selected donor representations.
    /// Keep the original packed-weight/staging floor independently funded.
    pub fn mirroredBf16Bytes(self: Geometry, file: *const @import("../gguf/format.zig").File) !usize {
        var bytes: usize = 0;
        for (file.tensors) |tensor| {
            if (!self.mirrorsWeight(tensor.name)) continue;
            if (tensor.dimensions.len != 2) return error.InvalidNativeMirrorInventory;
            var elements: usize = 1;
            for (tensor.dimensions) |dimension| elements = try std.math.mul(usize, elements, std.math.cast(usize, dimension) orelse return error.ResourceLimitExceeded);
            bytes = try std.math.add(usize, bytes, try std.math.mul(usize, elements, @sizeOf(u16)));
        }
        return bytes;
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
    const weights = try admitted.tryReserveWeight(.backend, resident_bytes);
    try admitted.reserveEstimate(required);
    admitted.releaseEstimate(required);
    try std.testing.expectEqual(resident_bytes, admitted.backendTotalBytes());
    admitted.release(weights);
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

test "native plan automatic projection is restricted to the qualified CUDA target" {
    try std.testing.expectEqual(@as(?Projection, .bf16_donors), automaticProjection(12, 1));
    for ([_][2]i32{ .{ 8, 0 }, .{ 8, 9 }, .{ 9, 0 }, .{ 12, 0 }, .{ 0, 0 } }) |device|
        try std.testing.expectEqual(@as(?Projection, null), automaticProjection(device[0], device[1]));
}

test "native plan automatic artifact requires complete unique Q4 donors and exact geometry" {
    const a = std.testing.allocator;
    const gguf = @import("../gguf/format.zig");
    const cfg = Config{ .family = .gemma, .position_encoding = .rope, .hidden_size = 2560, .num_hidden_layers = 42, .num_attention_heads = 8, .num_key_value_heads = 2, .attention_head_dim = 256, .global_head_dim = 512, .intermediate_size = 10240, .num_kv_shared_layers = 18, .sliding_window = 512, .sliding_window_pattern = 6, .ple_hidden_size = 256, .vocab_size = 262144, .max_position_embeddings = 131072 };
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const allocator = arena.allocator();
    const tensors = try allocator.alloc(gguf.TensorInfo, 24 * 7);
    const names = [_][]const u8{ "attn_q.weight", "attn_k.weight", "attn_v.weight", "attn_output.weight", "ffn_gate.weight", "ffn_up.weight", "ffn_down.weight" };
    for (0..24) |layer| {
        const head: u64 = cfg.effectiveHeadDimForLayer(layer);
        const shapes = [_][2]u64{ .{ 2560, 8 * head }, .{ 2560, 2 * head }, .{ 2560, 2 * head }, .{ 8 * head, 2560 }, .{ 2560, 10240 }, .{ 2560, 10240 }, .{ 10240, 2560 } };
        for (names, shapes, 0..) |name, shape, index| tensors[layer * 7 + index] = .{ .name = try std.fmt.allocPrint(allocator, "blk.{d}.{s}", .{ layer, name }), .dimensions = try allocator.dupe(u64, &shape), .tensor_type = .{ .known = .Q4_0 }, .offset = 0, .data_offset = 0 };
    }
    var file = gguf.File{ .header = .{ .version = 3, .tensor_count = tensors.len, .metadata_count = 0 }, .metadata = &.{}, .tensors = tensors, .alignment = 32, .data_region_offset = 0 };
    try std.testing.expect(automaticArtifact(cfg, &file));
    if (comptime @sizeOf(usize) >= 8) try std.testing.expectEqual(@as(usize, 4508876800), try Geometry.resolve(cfg).?.mirroredBf16Bytes(&file));
    try std.testing.expect(!automaticArtifact(cfg, null));
    var other = cfg;
    other.sliding_window = 1024;
    try std.testing.expect(!automaticArtifact(other, &file));
    tensors[0].tensor_type = .{ .known = .Q8_0 };
    try std.testing.expect(!automaticArtifact(cfg, &file));
    tensors[0].tensor_type = .{ .known = .Q4_0 };
    tensors[0].dimensions[0] -= 1;
    try std.testing.expect(!automaticArtifact(cfg, &file));
    tensors[0].dimensions[0] += 1;
    file.tensors = tensors[1..];
    try std.testing.expect(!automaticArtifact(cfg, &file));
    file.tensors = tensors;
    const first = tensors[0];
    tensors[0] = tensors[1];
    try std.testing.expect(!automaticArtifact(cfg, &file));
    tensors[0] = first;
    try std.testing.expect(automaticArtifact(cfg, &file));
}
