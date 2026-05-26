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

// MLX compute backend: implements ComputeBackend using Apple Metal via mlx-c.

const std = @import("std");
const ops = @import("ops.zig");
const AttentionContext = ops.AttentionContext;
const BackendKind = ops.BackendKind;
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;
const mlx = @import("../backends/mlx.zig");
const c = mlx.c;
const metal_runtime = @import("../backends/metal_runtime.zig");
const mlx_quant = @import("../backends/mlx_quant.zig");
const native = @import("../backends/native.zig");
const activations_mod = @import("../backends/activations.zig");
const tensor_mod = @import("../backends/tensor.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const QuantizedStorage = weight_source_mod.QuantizedStorage;
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const native_compute_mod = @import("native_compute.zig");
const gpu_hosted_store = @import("gpu_hosted_store.zig");
const linalg = @import("termite_linalg");
const runtime = @import("../runtime/root.zig");
const moe_residency = runtime.moe.residency;
const tier_planner = runtime.tier.planner;
const tier_cache_mod = runtime.tier.cache;
const prefetch_mod = runtime.tier.prefetch;
const tier_shared_mod = runtime.tier.shared;
const ExpertCoord = moe_residency.ExpertCoord;
const ResidencyTier = tier_planner.ResidencyTier;
const PlacementPlan = tier_planner.PlacementPlan;
const PrefetchQueue = gpu_hosted_store.PrefetchQueue;
const QuantExecutionPlan = mlx_quant.ExecutionPlan;
const run_memory = runtime.tier.memory;
var tensor_parallel_ctx_cached: ?mlx.DistributedContext = null;

fn cachedTensorParallelContext() !mlx.DistributedContext {
    if (tensor_parallel_ctx_cached == null) {
        tensor_parallel_ctx_cached = try mlx.initDistributed(false, null);
    }
    return tensor_parallel_ctx_cached.?;
}
const max_packed_expert_prefetch_backend_entries: usize = 8;

fn mib(value: usize) usize {
    return value * 1024 * 1024;
}

fn gib(value: usize) usize {
    return value * 1024 * 1024 * 1024;
}

fn residentWeightReservationBytes(budget: *const run_memory.RunBudget, estimate_bytes: usize) usize {
    if (estimate_bytes == 0) return 0;
    const host_limit = budget.limits.host_limit_bytes;
    if (host_limit == 0) return estimate_bytes;

    // Eager MLX estimates approximate the total preloaded footprint. Reserving
    // that whole amount at request admission rejects models before the runtime
    // can benefit from cache limits, eviction, and staged execution. Reserve a
    // bounded hot set instead.
    const soft_cap = @min(@max(host_limit / 3, mib(256)), gib(2));
    return @min(estimate_bytes, @min(soft_cap, host_limit));
}

fn forceWeightHandleLinearDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_FORCE_WEIGHT_HANDLE_LINEAR") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuTiedLogitsDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_TIED_LOGITS") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn enableMetalLmHeadArgmaxDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_LM_HEAD_ARGMAX") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn enableMetalCompressedAttentionBlockDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_COMPRESSED_ATTENTION_BLOCK") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn enableMetalCompressedAttentionSpanDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_COMPRESSED_ATTENTION_SPAN") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn disableFusedSdpaDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_DISABLE_FUSED_SDPA") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn enableMetalDecoderRuntimeDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_DECODER_RUNTIME") orelse
        c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn disableMetalDecoderRuntimeAttentionSpanDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_METAL_DECODER_RUNTIME_DISABLE_ATTENTION_SPAN") orelse
        c_std.getenv("TERMITE_MLX_RAW_METAL_WHOLE_TOKEN_DISABLE_ATTENTION_SPAN") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn useMetalDecoderRuntimeFastPath(self: *const MlxCompute) bool {
    return self.hosted_backend == .metal or enableMetalDecoderRuntimeDebug();
}

test "resident weight reservation keeps small estimates" {
    var budget = run_memory.RunBudget.init(.{
        .host_limit_bytes = gib(3),
        .backend_limit_bytes = gib(9),
        .combined_limit_bytes = gib(12),
        .kv_limit_bytes = mib(768),
        .scratch_limit_bytes = mib(512),
    });
    try std.testing.expectEqual(@as(usize, mib(192)), residentWeightReservationBytes(&budget, mib(192)));
}

test "resident weight reservation caps eager estimate to hot set" {
    var budget = run_memory.RunBudget.init(.{
        .host_limit_bytes = gib(3),
        .backend_limit_bytes = gib(9),
        .combined_limit_bytes = gib(12),
        .kv_limit_bytes = mib(768),
        .scratch_limit_bytes = mib(512),
    });
    try std.testing.expectEqual(@as(usize, gib(1)), residentWeightReservationBytes(&budget, gib(12)));
}

fn forceCpuRmsNormDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_RMSNORM") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuGqaDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_GQA") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuAllLinearDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_ALL_LINEAR") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuEmbeddingDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_EMBEDDING") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuConv1dDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_CONV1D") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuGeluDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_GELU") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuSiluDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_SILU") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuRopeDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_ROPE") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuAddDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_ADD") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceCpuMultiplyDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_CPU_MULTIPLY") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn keepHostAfterBackendPromotionDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_KEEP_HOST_AFTER_PROMOTION") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn forceRowWiseToFloat32Debug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_TOFLOAT_ROW_WISE") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

const ReservationState = struct {
    count: usize,
    reservation: run_memory.Reservation,
};
const max_packed_expert_backend_entries: usize = 12;

const CachedPackedExpertView = struct {
    key: []const u8,
    owned_key: bool,
    bytes: []const u8,
};

const PackedExpertViewEntry = gpu_hosted_store.PackedExpertViewEntry;

pub const QuantExecutionTimingStats = ops.QuantExecutionTimingStats;

var quant_execution_timing_stats = QuantExecutionTimingStats{};

pub fn resetQuantExecutionTimingStats() void {
    quant_execution_timing_stats = .{};
}

fn forceFlatTiedLogitsDebug() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_MLX_FLAT_TIED_LOGITS") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn debugTiedLogitsShapeEnabled() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_DEBUG_TIED_LOGITS_SHAPE") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn debugTiedLogitsInputRowsEnabled() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_DEBUG_TIED_LOGITS_INPUT_ROWS") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn debugTiedLogitsOutputEnabled() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    const value = c_std.getenv("TERMITE_DEBUG_TIED_LOGITS_OUTPUT") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn debugTopValues(label: []const u8, row: []const f32) void {
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
    std.debug.print("{s}:", .{label});
    for (top_ids, top_vals) |id, value| {
        std.debug.print(" {d}:{d:.6}", .{ id, value });
    }
    std.debug.print("\n", .{});
}

pub fn getQuantExecutionTimingStats() QuantExecutionTimingStats {
    return quant_execution_timing_stats;
}

pub fn snapshotDirectFamilyTiming() ops.DirectFamilyTimingSnapshot {
    return .{
        .project_nanos = quant_execution_timing_stats.gated_input_projection_nanos,
        .span_prep_nanos = mlx_quant.getTimingStats().compressed_block_span_prep_nanos,
        .quant_attn_nanos = mlx_quant.getTimingStats().compressed_block_quantized_attention_nanos,
        .block_apply_nanos = mlx_quant.getTimingStats().compressed_block_apply_nanos,
    };
}

pub fn debugTimingSnapshot(cb: *const ComputeBackend) ops.BackendDebugTimingSnapshot {
    if (cb.kind() != .mlx) return .{};
    const self: *const MlxCompute = @ptrCast(@alignCast(cb.ptr));
    return .{
        .native_quant_null = mlx_quant.isNullProvider(self.data.native_quant),
        .provider = mlx_quant.getTimingStats(),
        .quant = quant_execution_timing_stats,
    };
}

pub fn resetDebugTimingStats(cb: *const ComputeBackend) void {
    if (cb.kind() != .mlx) return;
    mlx_quant.resetTimingStats();
    resetQuantExecutionTimingStats();
}

pub fn dequantizeTensorToFloat32(cb: *const ComputeBackend, tensor: CT, allocator: std.mem.Allocator) ![]f32 {
    if (cb.kind() != .mlx) return error.UnsupportedTensorType;
    const self: *MlxCompute = @ptrCast(@alignCast(cb.ptr));
    const arr = toArr(tensor);
    if (arr.quantized_storage) |storage| {
        if (storage.packed_expert != null or storage.shape.len != 2) return error.UnsupportedTensorType;
        const rows: usize = @intCast(storage.shape[0]);
        const cols: usize = @intCast(storage.shape[1]);
        const output = try allocator.alloc(f32, rows * cols);
        errdefer allocator.free(output);
        try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, output);
        return output;
    }
    return toFloat32Op(self, tensor, allocator);
}

pub fn getQuantizedStorage(cb: *const ComputeBackend, tensor: CT) ?*const QuantizedStorage {
    if (cb.kind() != .mlx) return null;
    return toArr(tensor).quantized_storage;
}

fn decoderRuntimeReady(cb: *const ComputeBackend) bool {
    if (cb.kind() != .mlx) return false;
    const self: *const MlxCompute = @ptrCast(@alignCast(cb.ptr));
    return mlx_quant.decoderRuntimeReady(self.data.native_quant);
}

fn nativeMetalProvider(cb: *const ComputeBackend) ?*mlx_quant.MetalProvider {
    if (cb.kind() != .mlx) return null;
    const self: *const MlxCompute = @ptrCast(@alignCast(cb.ptr));
    return mlx_quant.metalProvider(self.data.native_quant);
}

fn decoderRuntimeFamilyPrepared(cb: *const ComputeBackend) bool {
    const provider = nativeMetalProvider(cb) orelse return false;
    return metal_runtime.decoderRuntimeFamilyPrepared(provider);
}

fn decoderRuntimePreparedKvTokens(cb: *const ComputeBackend) usize {
    const provider = nativeMetalProvider(cb) orelse return 0;
    return metal_runtime.decoderRuntimePreparedKvTokens(provider);
}

fn decoderRuntimePreparedSlotsMatchFamily(cb: *const ComputeBackend, gpt_config: anytype) bool {
    const provider = nativeMetalProvider(cb) orelse return false;
    return metal_runtime.decoderRuntimePreparedSlotsMatchFamily(provider, gpt_config);
}

fn noteDecoderRuntimeFamilyPrepared(cb: *const ComputeBackend, kv_tokens: usize) void {
    const provider = nativeMetalProvider(cb) orelse return;
    metal_runtime.noteDecoderRuntimeFamilyPrepared(provider, kv_tokens);
}

fn noteDecoderRuntimeGreedyPrepared(cb: *const ComputeBackend, kv_tokens: usize) void {
    const provider = nativeMetalProvider(cb) orelse return;
    metal_runtime.noteDecoderRuntimeGreedyPrepared(provider, kv_tokens);
}

fn decoderRuntimeAbsoluteEmbeddingsPrepared(cb: *const ComputeBackend) bool {
    const native_provider = nativeMetalProvider(cb) orelse return false;
    return metal_runtime.decoderRuntimeAbsoluteEmbeddingsPrepared(native_provider);
}

fn decoderRuntimeReserveKvTokens(gpt_config: anytype, current_kv_tokens: usize) usize {
    if (gpt_config.sliding_window > 0) return gpt_config.sliding_window;
    if (gpt_config.max_position_embeddings > 0) return gpt_config.max_position_embeddings;
    return if (current_kv_tokens > 0) current_kv_tokens else 1;
}

fn prepareOrReuseDecoderRuntimeFamily(
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: anytype,
    current_kv_tokens: usize,
    configured_layer_count: usize,
) !ops.DecoderRuntimePrepareReuseResult {
    const reserve_kv_tokens = decoderRuntimeReserveKvTokens(gpt_config, current_kv_tokens);
    if (decoderRuntimeFamilyPrepared(cb) and decoderRuntimePreparedSlotsMatchFamily(cb, gpt_config)) {
        if (reserve_kv_tokens <= decoderRuntimePreparedKvTokens(cb)) {
            return .{
                .prepared = true,
                .reserve_kv_tokens = reserve_kv_tokens,
                .fast_hit = true,
            };
        }
        const prepared = try cb.decoderRuntimePrepareGreedy(&.{
            .hidden_size = gpt_config.hidden_size,
            .intermediate_size = gpt_config.intermediate_size,
            .num_layers = gpt_config.num_hidden_layers,
            .num_heads = gpt_config.num_attention_heads,
            .num_kv_heads = gpt_config.effectiveKVHeads(),
            .head_dim = gpt_config.headDim(),
            .vocab_size = gpt_config.vocab_size,
            .kv_tokens = reserve_kv_tokens,
        });
        if (prepared) noteDecoderRuntimeGreedyPrepared(cb, reserve_kv_tokens);
        return .{
            .prepared = prepared,
            .reserve_kv_tokens = reserve_kv_tokens,
            .used_greedy = prepared,
        };
    }

    const prepared = try metal_runtime.prepareDecodeRuntimeFamily(
        cb,
        allocator,
        gpt_config,
        reserve_kv_tokens,
        configured_layer_count,
    );
    if (prepared) noteDecoderRuntimeFamilyPrepared(cb, reserve_kv_tokens);
    return .{
        .prepared = prepared,
        .reserve_kv_tokens = reserve_kv_tokens,
    };
}

const WeightExecClass = enum {
    other,
    attn_proj,
    router,
    lm_head,
};

fn classifyWeightExec(name: []const u8) WeightExecClass {
    if (std.mem.eql(u8, name, "lm_head.weight")) return .lm_head;
    if (std.mem.endsWith(u8, name, ".block_sparse_moe.gate.weight")) return .router;
    if (std.mem.endsWith(u8, name, ".self_attn.q_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.k_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.v_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.o_proj.weight"))
    {
        return .attn_proj;
    }
    return .other;
}

fn shouldDegradeQuantBudgetPressure(is_packed_expert: bool, expert_coord: ?ExpertCoord) bool {
    return !is_packed_expert and expert_coord == null;
}

fn shouldUseCpuChunkedDenseFallback(name: []const u8) bool {
    return std.mem.eql(u8, name, "lm_head.weight") or
        std.mem.endsWith(u8, name, ".self_attn.q_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.k_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.v_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.o_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.gate_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.up_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.down_proj.weight") or
        std.mem.endsWith(u8, name, ".block_sparse_moe.gate.weight") or
        std.mem.endsWith(u8, name, ".w1.weight") or
        std.mem.endsWith(u8, name, ".w2.weight") or
        std.mem.endsWith(u8, name, ".w3.weight") or
        std.mem.endsWith(u8, name, ".c_attn.weight") or
        std.mem.endsWith(u8, name, ".c_proj.weight") or
        std.mem.endsWith(u8, name, ".c_fc.weight") or
        std.mem.endsWith(u8, name, ".fc1.weight") or
        std.mem.endsWith(u8, name, ".fc2.weight");
}

fn canFallbackDenseBudgetPressure(name: []const u8, entry: *const LazyWeightEntry) bool {
    return entry.expert_coord == null and shouldUseCpuChunkedDenseFallback(name);
}

fn canFallbackQuantizedBudgetPressure(weight_arr: *const Arr, storage: *const QuantizedStorage) bool {
    return shouldDegradeQuantBudgetPressure(
        storage.packed_expert != null,
        if (weight_arr.lazy_entry) |entry| entry.expert_coord else null,
    );
}

fn noteSharedCacheDenial(
    run_budget: ?*run_memory.RunBudget,
    tier_cache: *const tier_cache_mod.SharedCache,
    tier: ResidencyTier,
    bytes: usize,
) void {
    if (run_budget) |budget| {
        budget.noteSharedCacheDenial(
            tier,
            bytes,
            switch (tier) {
                .disk => 0,
                .host => tier_cache.host_bytes,
                .backend => tier_cache.backend_bytes,
            },
            switch (tier) {
                .disk => 0,
                .host => tier_cache.budget.host_limit_bytes,
                .backend => tier_cache.budget.backend_limit_bytes,
            },
        );
    }
}

fn logSharedCacheDenial(stage: []const u8, tier_cache: *const tier_cache_mod.SharedCache, name: []const u8) void {
    var buf: [256]u8 = undefined;
    const detail = tier_cache.lastDenialString(&buf) catch "shared tier cache memory budget exceeded";
    std.log.warn("MLX shared cache denial stage={s} weight={s}: {s}", .{
        stage,
        if (name.len > 0) name else "<unnamed>",
        detail,
    });
}

fn logQuantBudgetFallback(self: *MlxCompute, stage: []const u8, weight_arr: *const Arr) void {
    if (self.run_budget) |budget| {
        var buf: [256]u8 = undefined;
        const detail = budget.lastDenialString(&buf) catch "memory budget exceeded";
        std.log.warn("MLX quant budget fallback stage={s} weight={s}: {s}", .{
            stage,
            if (weight_arr.name.len > 0) weight_arr.name else "<unnamed>",
            detail,
        });
        return;
    }
    std.log.warn("MLX quant budget fallback stage={s} weight={s}", .{
        stage,
        if (weight_arr.name.len > 0) weight_arr.name else "<unnamed>",
    });
}

fn logDenseBudgetFallback(self: *MlxCompute, stage: []const u8, name: []const u8) void {
    if (self.run_budget) |budget| {
        var buf: [256]u8 = undefined;
        const detail = budget.lastDenialString(&buf) catch "memory budget exceeded";
        std.log.warn("MLX dense budget fallback stage={s} weight={s}: {s}", .{
            stage,
            if (name.len > 0) name else "<unnamed>",
            detail,
        });
        return;
    }
    std.log.warn("MLX dense budget fallback stage={s} weight={s}", .{
        stage,
        if (name.len > 0) name else "<unnamed>",
    });
}

fn recordQuantWrapperStats(weight_class: WeightExecClass, is_packed: bool) void {
    quant_execution_timing_stats.wrapper_calls += 1;
    if (is_packed) quant_execution_timing_stats.wrapper_packed_calls += 1;
    switch (weight_class) {
        .attn_proj => quant_execution_timing_stats.attn_quant_wrapper_calls += 1,
        .router => quant_execution_timing_stats.router_quant_wrapper_calls += 1,
        .lm_head => quant_execution_timing_stats.lm_head_quant_wrapper_calls += 1,
        .other => {},
    }
}

fn recordQuantBackendDenseStats(weight_class: WeightExecClass) void {
    quant_execution_timing_stats.backend_dense_calls += 1;
    switch (weight_class) {
        .attn_proj => quant_execution_timing_stats.attn_quant_backend_dense_calls += 1,
        .router => quant_execution_timing_stats.router_quant_backend_dense_calls += 1,
        .lm_head => quant_execution_timing_stats.lm_head_quant_backend_dense_calls += 1,
        .other => {},
    }
}

fn recordQuantDeviceNativeStats(weight_class: WeightExecClass, is_packed: bool) void {
    quant_execution_timing_stats.device_native_calls += 1;
    if (is_packed) quant_execution_timing_stats.device_native_packed_calls += 1;
    switch (weight_class) {
        .attn_proj => quant_execution_timing_stats.attn_quant_device_native_calls += 1,
        .router => quant_execution_timing_stats.router_quant_device_native_calls += 1,
        .lm_head => quant_execution_timing_stats.lm_head_quant_device_native_calls += 1,
        .other => {},
    }
}

fn fallbackQuantizedWrapperOnBudgetPressure(
    self: *MlxCompute,
    stage: []const u8,
    input: CT,
    weight_arr: *Arr,
    storage: *QuantizedStorage,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    weight_class: WeightExecClass,
    is_packed: bool,
) !?CT {
    logQuantBudgetFallback(self, stage, weight_arr);
    const result = try linearNoBiasQuantizedWrapper(self, input, weight_arr, storage, rows, in_dim, out_dim);
    recordQuantWrapperStats(weight_class, is_packed);
    return result;
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn quantizedStorageBudgetBytes(storage: *const QuantizedStorage) usize {
    const group_cache_bytes = if (storage.prepared_group_cache) |cache| cache.ownedBytes() else 0;
    if (storage.packed_expert) |packed_view| {
        if (!storage.raw_owned) return group_cache_bytes;
        const count: usize = @max(@as(usize, 1), @as(usize, @intCast(packed_view.expert_count)));
        return @max(@as(usize, 1), storage.raw_bytes.len / count) + group_cache_bytes;
    }
    return storage.raw_bytes.len + group_cache_bytes;
}

const LinearDims = struct {
    out_dim: usize,
    in_dim: usize,
};

fn quantizedStorageLinearDims(storage: *const QuantizedStorage) !LinearDims {
    if (storage.packed_expert) |packed_view| {
        if (storage.shape.len != 3) return error.InvalidPackedExpertTensor;
        const expert_axis: usize = @intCast(packed_view.expert_axis);
        if (expert_axis >= storage.shape.len - 1) return error.InvalidPackedExpertTensor;

        var dims: [2]usize = undefined;
        var dst: usize = 0;
        for (storage.shape, 0..) |dim, axis| {
            if (axis == expert_axis) continue;
            if (dim <= 0) return error.InvalidPackedExpertTensor;
            dims[dst] = @intCast(dim);
            dst += 1;
        }
        if (dst != 2) return error.InvalidPackedExpertTensor;
        return .{ .out_dim = dims[0], .in_dim = dims[1] };
    }

    if (storage.shape.len != 2) return error.InvalidQuantizedLinearShape;
    if (storage.shape[0] <= 0 or storage.shape[1] <= 0) return error.InvalidQuantizedLinearShape;
    return .{
        .out_dim = @intCast(storage.shape[0]),
        .in_dim = @intCast(storage.shape[1]),
    };
}

const StagedPackedMoeStorage = struct {
    storage: QuantizedStorage,
    expert_ids: []u32,
    expert_tile_ids: ?[]u32 = null,
    allocator: std.mem.Allocator,

    fn deinit(self: *StagedPackedMoeStorage) void {
        self.storage.deinit();
        self.allocator.free(self.expert_ids);
        if (self.expert_tile_ids) |ids| self.allocator.free(ids);
    }
};

fn noteSelectedPackedExpert(
    allocator: std.mem.Allocator,
    expert_id: u32,
    expert_map: []i32,
    selected: *std.ArrayListUnmanaged(u32),
) !void {
    const expert_idx: usize = @intCast(expert_id);
    if (expert_idx >= expert_map.len) return error.InvalidPackedExpertTensor;
    if (expert_map[expert_idx] >= 0) return;
    expert_map[expert_idx] = @intCast(selected.items.len);
    try selected.append(allocator, expert_id);
}

fn remapPackedExpertIds(
    allocator: std.mem.Allocator,
    expert_ids: []const u32,
    expert_map: []const i32,
) ![]u32 {
    const remapped = try allocator.alloc(u32, expert_ids.len);
    errdefer allocator.free(remapped);
    for (expert_ids, 0..) |expert_id, idx| {
        const expert_idx: usize = @intCast(expert_id);
        if (expert_idx >= expert_map.len) return error.InvalidPackedExpertTensor;
        const mapped = expert_map[expert_idx];
        if (mapped < 0) return error.InvalidPackedExpertTensor;
        remapped[idx] = @intCast(mapped);
    }
    return remapped;
}

fn stageSelectedPackedMoeStorage(
    allocator: std.mem.Allocator,
    storage: *QuantizedStorage,
    expert_ids: []const u32,
    expert_tile_ids: ?[]const u32,
) !?StagedPackedMoeStorage {
    const packed_view = storage.packed_expert orelse return null;
    if (packed_view.expert_axis != 0) return null;
    const expert_count: usize = @intCast(packed_view.expert_count);
    if (expert_count <= 1 or expert_ids.len == 0) return null;
    if (storage.raw_bytes.len % expert_count != 0) return error.InvalidPackedExpertTensor;

    const expert_stride = storage.raw_bytes.len / expert_count;
    if (expert_stride == 0) return error.InvalidPackedExpertTensor;

    const expert_map = try allocator.alloc(i32, expert_count);
    defer allocator.free(expert_map);
    @memset(expert_map, -1);

    var selected: std.ArrayListUnmanaged(u32) = .empty;
    defer selected.deinit(allocator);

    for (expert_ids) |expert_id| {
        try noteSelectedPackedExpert(allocator, expert_id, expert_map, &selected);
    }
    if (expert_tile_ids) |tile_ids| {
        for (tile_ids) |expert_id| {
            try noteSelectedPackedExpert(allocator, expert_id, expert_map, &selected);
        }
    }

    if (selected.items.len == 0 or selected.items.len == expert_count) return null;

    const stage_started_at = monotonicNowNs();
    const staged_raw_len = try std.math.mul(usize, expert_stride, selected.items.len);
    const staged_raw = try allocator.alloc(u8, staged_raw_len);
    errdefer allocator.free(staged_raw);
    for (selected.items, 0..) |expert_id, compact_idx| {
        const src_start = @as(usize, @intCast(expert_id)) * expert_stride;
        const dst_start = compact_idx * expert_stride;
        @memcpy(staged_raw[dst_start .. dst_start + expert_stride], storage.raw_bytes[src_start .. src_start + expert_stride]);
    }

    const staged_shape = try allocator.dupe(i64, storage.shape);
    errdefer allocator.free(staged_shape);
    staged_shape[@intCast(packed_view.expert_axis)] = @intCast(selected.items.len);

    const remapped_expert_ids = try remapPackedExpertIds(allocator, expert_ids, expert_map);
    errdefer allocator.free(remapped_expert_ids);

    const remapped_tile_ids = if (expert_tile_ids) |tile_ids| blk: {
        const remapped = try remapPackedExpertIds(allocator, tile_ids, expert_map);
        break :blk remapped;
    } else null;
    errdefer if (remapped_tile_ids) |ids| allocator.free(ids);

    quant_execution_timing_stats.moe_grouped_stage_calls += 1;
    quant_execution_timing_stats.moe_grouped_stage_bytes += @intCast(staged_raw_len);
    quant_execution_timing_stats.moe_grouped_stage_experts += @intCast(selected.items.len);
    quant_execution_timing_stats.moe_grouped_stage_nanos += @intCast(monotonicNowNs() - stage_started_at);

    return StagedPackedMoeStorage{
        .storage = .{
            .tensor_type = storage.tensor_type,
            .raw_bytes = staged_raw,
            .shape = staged_shape,
            .source_name = null,
            .packed_expert = .{
                .expert_index = 0,
                .expert_count = @intCast(selected.items.len),
                .expert_axis = packed_view.expert_axis,
                .row_offset = packed_view.row_offset,
            },
            .raw_owned = true,
            .allocator = allocator,
        },
        .expert_ids = remapped_expert_ids,
        .expert_tile_ids = remapped_tile_ids,
        .allocator = allocator,
    };
}

fn findPackedExpertAxisLocal(shape: []const i64, expert_count: u32) ?usize {
    var found: ?usize = null;
    for (shape, 0..) |dim, axis| {
        if (dim != @as(i64, @intCast(expert_count))) continue;
        if (found != null) return null;
        found = axis;
    }
    return found;
}

fn parseExpertIndexFromLazyName(name: []const u8) ?u32 {
    const marker = ".block_sparse_moe.experts.";
    const marker_index = std.mem.indexOf(u8, name, marker) orelse return null;
    const rest = name[marker_index + marker.len ..];
    const dot_index = std.mem.indexOfScalar(u8, rest, '.') orelse return null;
    if (dot_index == 0) return null;
    return std.fmt.parseInt(u32, rest[0..dot_index], 10) catch null;
}

fn recoveredPackedExpertStorage(
    storage: *QuantizedStorage,
    lazy_entry: ?*LazyWeightEntry,
    default_expert_count: usize,
) ?QuantizedStorage {
    const entry = lazy_entry orelse return null;
    const expert_index = entry.tensor_ref.packed_expert_index orelse parseExpertIndexFromLazyName(entry.tensor_ref.name) orelse return null;
    const source_name = entry.tensor_ref.source_name orelse "";
    const expert_count: u32 = if (entry.tensor_ref.packed_expert_count != 0)
        entry.tensor_ref.packed_expert_count
    else if (source_name.len > 0 and
        (std.mem.endsWith(u8, source_name, "_exps.weight") or
            std.mem.endsWith(u8, source_name, "_exps.bias")))
        @intCast(default_expert_count)
    else
        return null;
    const expert_axis = findPackedExpertAxisLocal(storage.shape, expert_count) orelse return null;

    var recovered = storage.*;
    recovered.packed_expert = .{
        .expert_index = expert_index,
        .expert_count = expert_count,
        .expert_axis = @intCast(expert_axis),
    };
    return recovered;
}

fn reloadedPackedExpertStorageFromTensorStore(
    self: *MlxCompute,
    lazy_entry: ?*LazyWeightEntry,
) !?QuantizedStorage {
    const entry = lazy_entry orelse return null;
    const source_name = entry.tensor_ref.source_name orelse return null;
    if (!std.mem.endsWith(u8, source_name, "_exps.weight") and !std.mem.endsWith(u8, source_name, "_exps.bias")) return null;
    const expert_index = parseExpertIndexFromLazyName(entry.tensor_ref.name) orelse return null;
    if (self.data.moe_num_experts == 0) return null;
    const tensor_store = self.data.tensor_store orelse return null;

    const synthetic_ref: tensor_store_mod.LazyTensorRef = .{
        .name = entry.tensor_ref.name,
        .source_name = source_name,
        .byte_len = entry.tensor_ref.byte_len,
        .quantized = true,
        .packed_expert_index = expert_index,
        .packed_expert_count = @intCast(self.data.moe_num_experts),
    };
    return try tensor_store.loadQuantizedStorageRef(&synthetic_ref);
}

fn repairMalformedPackedExpertEntryLocked(self: *MlxCompute, entry: *LazyWeightEntry) !void {
    if (entry.quantized_storage == null) return;
    if (entry.quantized_storage.?.packed_expert != null) return;

    const expert_index = parseExpertIndexFromLazyName(entry.tensor_ref.name) orelse return;
    const source_name = entry.tensor_ref.source_name orelse return;
    if (!std.mem.endsWith(u8, source_name, "_exps.weight") and !std.mem.endsWith(u8, source_name, "_exps.bias")) return;
    if (self.data.moe_num_experts == 0) return;

    // Try to set packed_expert in-place by inferring the expert axis from shape.
    // This preserves prepared-layout cache ownership and avoids storage lifecycle issues.
    const storage = &entry.quantized_storage.?;
    if (findPackedExpertAxisLocal(storage.shape, @intCast(self.data.moe_num_experts))) |expert_axis| {
        storage.packed_expert = .{
            .expert_index = expert_index,
            .expert_count = @intCast(self.data.moe_num_experts),
            .expert_axis = @intCast(expert_axis),
        };
        entry.tensor_ref.packed_expert_index = expert_index;
        entry.tensor_ref.packed_expert_count = @intCast(self.data.moe_num_experts);
        return;
    }

    // Axis inference failed (ambiguous shape) — fall back to full reload.
    if (try reloadedPackedExpertStorageFromTensorStore(self, entry)) |new_storage| {
        storage.deinit();
        entry.quantized_storage = new_storage;
        entry.tensor_ref.packed_expert_index = expert_index;
        entry.tensor_ref.packed_expert_count = @intCast(self.data.moe_num_experts);
    }
}

const GatherCacheKey = struct {
    manager_ptr: usize,
    sequence_id: u32,
    layer_index: usize,
};

const GatherCacheEntry = struct {
    const EncodedKeyState = struct {
        arr: ?c.mlx_array,
        tokens: usize,
        position_offset: usize,
        row_bytes: usize,
    };

    k: c.mlx_array,
    v: c.mlx_array,
    token_count: usize,
    position_offset: usize,
    encoded_key: ?c.mlx_array = null,
    encoded_key_tokens: usize = 0,
    encoded_key_position_offset: usize = 0,
    encoded_key_row_bytes: usize = 0,

    fn deinit(self: *GatherCacheEntry) void {
        _ = c.mlx_array_free(self.k);
        _ = c.mlx_array_free(self.v);
        if (self.encoded_key) |arr| _ = c.mlx_array_free(arr);
    }

    fn detachEncodedKey(self: *GatherCacheEntry) EncodedKeyState {
        const state: EncodedKeyState = .{
            .arr = self.encoded_key,
            .tokens = self.encoded_key_tokens,
            .position_offset = self.encoded_key_position_offset,
            .row_bytes = self.encoded_key_row_bytes,
        };
        self.encoded_key = null;
        self.encoded_key_tokens = 0;
        self.encoded_key_position_offset = 0;
        self.encoded_key_row_bytes = 0;
        return state;
    }
};

const KvCacheKey = struct {
    manager_ptr: usize,
    pool_id: u32,
    block_id: u32,
    layer_index: usize,
};

const KvCacheEntry = struct {
    k: c.mlx_array,
    v: c.mlx_array,
    token_count: usize,
    encoded_key: ?c.mlx_array = null,
    encoded_key_tokens: usize = 0,
    encoded_key_row_bytes: usize = 0,

    fn deinit(self: *KvCacheEntry) void {
        _ = c.mlx_array_free(self.k);
        _ = c.mlx_array_free(self.v);
        if (self.encoded_key) |arr| _ = c.mlx_array_free(arr);
    }
};

const PagedKvBlockBootstrap = struct {
    k_blocks: []c.mlx_array,
    v_blocks: []c.mlx_array,
    token_counts: []usize,

    fn deinit(self: *PagedKvBlockBootstrap, allocator: std.mem.Allocator) void {
        allocator.free(self.k_blocks);
        allocator.free(self.v_blocks);
        allocator.free(self.token_counts);
        self.* = undefined;
    }
};

const PagedKvArrays = struct {
    k: c.mlx_array,
    v: c.mlx_array,
    owned: bool,
};

const AttentionKvSource = struct {
    ptr_id: usize,
    pool: *runtime.kv.pool.KvPool,
    block_ids: []const runtime.kv.block.KvBlockId,
    manager: ?*runtime.kv.manager.KvManager = null,
    storage: ?*runtime.kv.storage_runtime.KvStorageRuntime = null,
};

fn attentionKvSource(attention: AttentionContext) !AttentionKvSource {
    const kv = attention.kv_cache orelse return error.InvalidPagedKvState;
    if (attention.kv_storage) |storage| {
        const block_ids = if (kv.logical_blocks) |blocks|
            blocks
        else blk: {
            const table = storage.blockTable(kv.sequence_id) orelse return error.InvalidSequenceId;
            break :blk table.blocks.items;
        };
        const kv_store = storage.getPoolMut(kv.pool_id) orelse return error.InvalidPoolId;
        const pool = kv_store.hostPool() orelse return error.InvalidPoolId;
        return .{
            .ptr_id = @intFromPtr(storage),
            .pool = pool,
            .block_ids = block_ids,
            .storage = storage,
        };
    }
    const manager = attention.kv_manager orelse return error.InvalidPagedKvState;
    const block_ids = if (kv.logical_blocks) |blocks|
        blocks
    else blk: {
        const table = manager.blockTable(kv.sequence_id) orelse return error.InvalidSequenceId;
        break :blk table.blocks.items;
    };
    const kv_store = manager.getPoolMut(kv.pool_id) orelse return error.InvalidPoolId;
    const pool = kv_store.hostPool() orelse return error.InvalidPoolId;
    return .{
        .ptr_id = @intFromPtr(manager),
        .pool = pool,
        .block_ids = block_ids,
        .manager = manager,
    };
}

fn attentionWriteLayerKvSuffix(source: AttentionKvSource, kv: ops.KvCacheView, attention: AttentionContext, k_rows: []const f32, v_rows: []const f32) !void {
    if (source.storage) |storage| {
        try storage.writeLayerKvSuffix(
            kv.sequence_id,
            attention.layer_index,
            attention.kv_sequence_len,
            attention.query_sequence_len,
            k_rows,
            v_rows,
        );
        return;
    }
    const manager = source.manager orelse return error.InvalidPagedKvState;
    try manager.writeLayerKvSuffix(
        kv.sequence_id,
        attention.layer_index,
        attention.kv_sequence_len,
        attention.query_sequence_len,
        k_rows,
        v_rows,
    );
}

pub const QuantExecutionMode = gpu_hosted_store.QuantExecutionMode;

const QuantLinearExecutor = enum {
    backend_dense,
    ephemeral_dense,
    wrapper_direct_quant,
    device_native,
};

/// Internal wrapper: an owned or borrowed mlx_array handle.
const Arr = struct {
    arr: c.mlx_array,
    owned: bool,
    host_f32: ?[]f32 = null,
    name: []const u8 = "",
    lazy_entry: ?*LazyWeightEntry = null,
    pinned_lazy: bool = false,
    quantized_storage: ?*QuantizedStorage = null,
    owned_quantized_storage: ?*QuantizedStorage = null,
    reservation: ?run_memory.Reservation = null,
};

const TensorParallelShard = struct {
    data: []f32,
    rows: usize,
    cols: usize,
    arr: ?c.mlx_array = null,
    arr_transposed: ?c.mlx_array = null,

    fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        if (self.arr_transposed) |arr| _ = c.mlx_array_free(arr);
        if (self.arr) |arr| _ = c.mlx_array_free(arr);
        allocator.free(self.data);
    }
};

fn toArr(ct: CT) *Arr {
    return @ptrCast(@alignCast(ct));
}

fn getArr(ct: CT) c.mlx_array {
    return toArr(ct).arr;
}

pub const MlxCompute = struct {
    pub const TrainingAdamWOptions = struct {
        lr: f32,
        beta1: f32,
        beta2: f32,
        eps: f32,
        weight_decay: f32,
        bias_correction1: f32,
        bias_correction2: f32,
        grad_scale: f32,
    };

    pub const TrainingAdamWResult = struct {
        weight: CT,
        grad_accum: CT,
        m: CT,
        v: CT,
    };

    pub const HostedBackend = enum {
        mlx,
        metal,
    };

    allocator: std.mem.Allocator,
    data: *WeightStore,
    hosted_backend: HostedBackend = .mlx,
    tp_ctx: ?mlx.DistributedContext = null,
    run_budget: ?*run_memory.RunBudget = null,
    resident_weight_reservation: ?run_memory.Reservation = null,
    weight_reservations: std.StringHashMapUnmanaged(ReservationState) = .empty,
    tp_linear_shards: std.StringHashMapUnmanaged(TensorParallelShard) = .empty,
    kv_cache: std.AutoHashMapUnmanaged(KvCacheKey, KvCacheEntry) = .empty,
    gather_cache: std.AutoHashMapUnmanaged(GatherCacheKey, GatherCacheEntry) = .empty,
    mixed_precision: bool = false,
    /// Optional Io for the CPU-fallback linalg path; see NativeCompute.io.
    io: ?std.Io = null,

    pub fn init(
        allocator: std.mem.Allocator,
        data: *WeightStore,
        run_budget: ?*run_memory.RunBudget,
    ) !MlxCompute {
        return initHosted(.mlx, allocator, data, run_budget);
    }

    pub fn initMlxHosted(
        allocator: std.mem.Allocator,
        data: *WeightStore,
        run_budget: ?*run_memory.RunBudget,
    ) !MlxCompute {
        return initHosted(.mlx, allocator, data, run_budget);
    }

    pub fn initMlxHostedWithIo(
        allocator: std.mem.Allocator,
        data: *WeightStore,
        run_budget: ?*run_memory.RunBudget,
        io: std.Io,
    ) !MlxCompute {
        var self = try initHosted(.mlx, allocator, data, run_budget);
        self.io = io;
        return self;
    }

    inline fn dispatchSgemmTransB(
        self: *const MlxCompute,
        m: usize,
        n: usize,
        k: usize,
        alpha: f32,
        a: []const f32,
        b: []const f32,
        beta: f32,
        c_out: []f32,
    ) error{Canceled}!void {
        if (self.io) |io| {
            return native.sgemmTransB(io, m, n, k, alpha, a, b, beta, c_out);
        }
        native.sgemmTransBSync(m, n, k, alpha, a, b, beta, c_out);
    }

    pub fn initMetalHosted(
        allocator: std.mem.Allocator,
        data: *WeightStore,
        run_budget: ?*run_memory.RunBudget,
    ) !MlxCompute {
        return initHosted(.metal, allocator, data, run_budget);
    }

    fn initHosted(
        hosted_backend: HostedBackend,
        allocator: std.mem.Allocator,
        data: *WeightStore,
        run_budget: ?*run_memory.RunBudget,
    ) !MlxCompute {
        // Set MLX memory/cache limits to prevent unbounded buffer cache growth.
        // Without this, MLX caches all freed Metal buffers and memory grows without bound.
        // mlx-lm does this via mx.set_cache_limit() and periodic mx.clear_cache().
        initMlxMemoryLimits(run_budget, data.quant_execution_mode);
        initPrefetchQueue(data, allocator);

        var compute: MlxCompute = .{
            .allocator = allocator,
            .data = data,
            .hosted_backend = hosted_backend,
            .run_budget = run_budget,
        };
        const dist_cfg = runtime.distributed.configFromEnv();
        if (dist_cfg.isTensorParallel()) {
            compute.tp_ctx = try cachedTensorParallelContext();
        }
        if (run_budget) |budget| {
            if (data.resident_weight_estimate_bytes > 0) {
                const reservation_bytes = residentWeightReservationBytes(budget, data.resident_weight_estimate_bytes);
                if (reservation_bytes > 0) {
                    if (reservation_bytes < data.resident_weight_estimate_bytes) {
                        std.log.info("capping eager MLX resident weight reservation to {d} MB from estimated {d} MB", .{
                            reservation_bytes / mib(1),
                            data.resident_weight_estimate_bytes / mib(1),
                        });
                    }
                    compute.resident_weight_reservation = try budget.tryReserveWeight(.host, reservation_bytes);
                }
            }
        }
        return compute;
    }

    /// Lightweight constructor for use in training (gradient computation).
    /// Does NOT set up lazy weight loading or memory budgeting — all tensor data
    /// comes via runtime_inputs, not the weight store.
    /// The caller owns `weight_store_out` and must keep it alive for the lifetime
    /// of the returned MlxCompute.  Pass the address of a local WeightStore:
    ///
    ///     var ws: mlx_compute.WeightStore = undefined;
    ///     var compute = try mlx_compute.MlxCompute.initMinimal(allocator, &ws);
    ///     var cb = compute.computeBackend();
    ///
    pub fn initMinimal(allocator: std.mem.Allocator, weight_store_out: *WeightStore) !MlxCompute {
        weight_store_out.* = WeightStore{
            .allocator = allocator,
            .resident_weights = c.mlx_map_string_to_array_new(),
            .stream = mlx.openDefaultStream().stream,
            .prefix = "",
            .lazy_weights = .{},
        };
        return MlxCompute.init(allocator, weight_store_out, null);
    }

    /// Convenience constructor that enables mixed precision (bf16 weights/activations on GPU).
    pub fn initMixedPrecision(
        allocator: std.mem.Allocator,
        data: *WeightStore,
        run_budget: ?*run_memory.RunBudget,
    ) !MlxCompute {
        var self = try MlxCompute.init(allocator, data, run_budget);
        self.mixed_precision = true;
        return self;
    }

    pub fn hostedBackend(self: *const MlxCompute) HostedBackend {
        return self.hosted_backend;
    }

    pub fn metalHosted(self: *const MlxCompute) bool {
        return self.hosted_backend == .metal;
    }

    /// When mixed_precision is enabled, cast a float32 MLX array to bfloat16 in-place
    /// (freeing the original) and return the bf16 array. If the input is already bf16
    /// or mixed_precision is false, returns the input unchanged.
    fn castToBf16IfMixed(self: *const MlxCompute, arr: c.mlx_array) !c.mlx_array {
        if (!self.mixed_precision) return arr;
        if (c.mlx_array_dtype(arr) == c.MLX_BFLOAT16) return arr;
        if (c.mlx_array_dtype(arr) != c.MLX_FLOAT32) return arr;
        var bf16_arr = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(bf16_arr);
        errdefer _ = c.mlx_array_free(arr);
        try mlx.check(c.mlx_astype(&bf16_arr, arr, c.MLX_BFLOAT16, self.data.stream));
        _ = c.mlx_array_free(arr);
        return bf16_arr;
    }

    fn initMlxMemoryLimits(run_budget: ?*run_memory.RunBudget, quant_mode: QuantExecutionMode) void {
        var old_cache_limit: usize = 0;
        var old_mem_limit: usize = 0;
        if (run_budget) |budget| {
            const backend_bytes = budget.limits.backend_limit_bytes;
            if (backend_bytes > 0) {
                // Set total Metal memory limit to prevent the OS OOM killer.
                // Without this, MLX defaults to 1.5× system RAM and will allocate
                // until jetsam kills the process on unified memory systems.
                _ = c.mlx_set_memory_limit(&old_mem_limit, backend_bytes);

                // In device_native mode, quantized weights are uploaded transiently per
                // kernel call and freed immediately — the buffer cache doesn't need
                // to hold the full backend budget.
                const cache_bytes = if (quant_mode == .device_native)
                    @min(backend_bytes, 2 * 1024 * 1024 * 1024)
                else
                    backend_bytes;
                _ = c.mlx_set_cache_limit(&old_cache_limit, cache_bytes);
                std.log.info("MLX memory limit set to {d} MB, cache limit {d} MB (was mem={d} MB cache={d} MB, quant_mode={s})", .{
                    backend_bytes / (1024 * 1024),
                    cache_bytes / (1024 * 1024),
                    old_mem_limit / (1024 * 1024),
                    old_cache_limit / (1024 * 1024),
                    @tagName(quant_mode),
                });
                return;
            }
        }
        // No budget specified — set reasonable defaults.
        _ = c.mlx_set_memory_limit(&old_mem_limit, 16 * 1024 * 1024 * 1024);
        _ = c.mlx_set_cache_limit(&old_cache_limit, 4 * 1024 * 1024 * 1024);
    }

    pub fn computeBackend(self: *MlxCompute) ComputeBackend {
        return .{ .ptr = self, .vtable = &vtable_impl };
    }

    pub fn tensorParallelEnabled(self: *const MlxCompute) bool {
        return self.tp_ctx != null;
    }

    pub fn tensorParallelWorldSize(self: *const MlxCompute) usize {
        return if (self.tp_ctx) |ctx| ctx.world_size else 1;
    }

    pub fn tensorParallelRank(self: *const MlxCompute) usize {
        return if (self.tp_ctx) |ctx| ctx.rank else 0;
    }

    pub fn fromComputeBackend(cb: *const ComputeBackend) ?*MlxCompute {
        if (cb.kind() != .mlx) return null;
        return @ptrCast(@alignCast(cb.ptr));
    }

    pub fn gatherPagedKvLayerFromCache(
        self: *MlxCompute,
        allocator: std.mem.Allocator,
        kv: ops.KvCacheView,
        token_count: usize,
        layer_index: usize,
    ) !ops.PagedKvLayerCacheRows {
        const attention: AttentionContext = .{
            .kv_cache = kv,
            .kv_storage = kv.kv_storage,
            .layer_index = layer_index,
            .total_sequence_len = token_count,
            .kv_sequence_len = token_count,
            .query_sequence_len = token_count,
            .kv_position_offset = kv.position_offset,
            .mode = .paged_prefill,
        };
        const gathered = try gatherPagedKvArraysFromBlockCache(self, attention);
        defer if (gathered.owned) {
            _ = c.mlx_array_free(gathered.k);
            _ = c.mlx_array_free(gathered.v);
        };
        const k = try mlx.readFloat32(gathered.k, allocator);
        errdefer allocator.free(k);
        const v = try mlx.readFloat32(gathered.v, allocator);
        return .{ .k = k, .v = v };
    }

    pub fn seedPagedKvLayerCache(
        self: *MlxCompute,
        kv: ops.KvCacheView,
        token_count: usize,
        layer_index: usize,
        k_rows_host: []const f32,
        v_rows_host: []const f32,
    ) !void {
        const pool = (kv.kv_storage orelse return error.InvalidPagedKvState).getPool(kv.pool_id) orelse return error.InvalidPoolId;
        if (!pool.config.hasSymmetricValueWidth()) return error.UnsupportedAsymmetricKvWidths;
        const token_width = pool.valuesPerToken();
        if (token_count == 0) return;
        if (k_rows_host.len != token_count * token_width or v_rows_host.len != token_count * token_width) return error.InvalidPagedKvShape;
        const shape = [_]i32{ @intCast(token_count), @intCast(token_width) };
        const k_arr = mlx.arrayFromFloat32(k_rows_host, &shape);
        defer _ = c.mlx_array_free(k_arr);
        const v_arr = mlx.arrayFromFloat32(v_rows_host, &shape);
        defer _ = c.mlx_array_free(v_arr);
        try updatePagedKvBlocks(self, k_arr, v_arr, .{
            .kv_cache = kv,
            .kv_storage = kv.kv_storage,
            .layer_index = layer_index,
            .total_sequence_len = token_count,
            .kv_sequence_len = token_count,
            .query_sequence_len = token_count,
            .kv_position_offset = kv.position_offset,
            .mode = .paged_prefill,
        });
    }

    pub fn linearTensorParallelReplicatedToSharded(
        self: *MlxCompute,
        input: CT,
        weight: CT,
        bias: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !CT {
        const tp = self.tp_ctx orelse return error.MlxTensorParallelDisabled;
        const weight_arr = toArr(weight);
        const bias_arr = toArr(bias);
        if (weight_arr.name.len != 0 and bias_arr.name.len != 0) {
            const weight_shard = try self.tensorParallelWeightRowShard(weight_arr, out_dim, in_dim, tp.rank, tp.world_size);
            const bias_shard = try self.tensorParallelBiasShard(bias_arr, out_dim, tp.rank, tp.world_size);
            var result = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(result);
            try mlx.check(c.mlx_matmul(&result, getArr(input), weight_shard.arr_transposed.?, self.data.stream));

            var biased = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(biased);
            try mlx.check(c.mlx_add(&biased, result, bias_shard.arr.?, self.data.stream));
            _ = c.mlx_array_free(result);
            return self.makeArr(biased, true);
        }

        const input_host = try mlx.readFloat32(getArr(input), self.allocator);
        defer self.allocator.free(input_host);
        const weight_host = try mlx.readFloat32(weight_arr.arr, self.allocator);
        defer self.allocator.free(weight_host);
        const bias_host = try mlx.readFloat32(bias_arr.arr, self.allocator);
        defer self.allocator.free(bias_host);
        const weight_shard = try mlx.shardMatrixRowsFloat32(self.allocator, weight_host, out_dim, in_dim, tp.rank, tp.world_size);
        defer self.allocator.free(weight_shard.data);
        const bias_shard = try mlx.shardVectorFloat32(self.allocator, bias_host, tp.rank, tp.world_size);
        defer self.allocator.free(bias_shard.data);
        const out_host = try std.heap.c_allocator.alloc(f32, rows * weight_shard.range.len);
        errdefer std.heap.c_allocator.free(out_host);
        try mlx.linearReplicatedInputToShardedOutputOnStream(out_host, self.data.stream, input_host, rows, in_dim, weight_shard.data, weight_shard.range.len, bias_shard.data);
        const shape = [_]i32{ @intCast(rows), @intCast(weight_shard.range.len) };
        const arr = mlx.arrayFromOwnedFloat32(out_host, &shape);
        return self.makeArr(arr, true);
    }

    pub fn linearTensorParallelShardedToReplicated(
        self: *MlxCompute,
        input: CT,
        weight: CT,
        bias: CT,
        rows: usize,
        in_dim: usize,
        out_dim: usize,
    ) !CT {
        const tp = self.tp_ctx orelse return error.MlxTensorParallelDisabled;
        const weight_arr = toArr(weight);
        const bias_arr = toArr(bias);
        if (weight_arr.name.len != 0 and bias_arr.name.len != 0) {
            const weight_shard = try self.tensorParallelWeightColumnShard(weight_arr, out_dim, in_dim, tp.rank, tp.world_size);
            const bias_full = try self.tensorParallelFullBias(bias_arr, out_dim);
            var partial = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(partial);
            try mlx.check(c.mlx_matmul(&partial, getArr(input), weight_shard.arr_transposed.?, self.data.stream));

            var summed = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(summed);
            try mlx.check(c.mlx_distributed_all_sum(&summed, partial, tp.group, self.data.stream));
            _ = c.mlx_array_free(partial);

            var biased = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(biased);
            try mlx.check(c.mlx_add(&biased, summed, bias_full.arr.?, self.data.stream));
            _ = c.mlx_array_free(summed);
            return self.makeArr(biased, true);
        }

        const input_host = try mlx.readFloat32(getArr(input), self.allocator);
        defer self.allocator.free(input_host);
        const weight_host = try mlx.readFloat32(weight_arr.arr, self.allocator);
        defer self.allocator.free(weight_host);
        const bias_host = try mlx.readFloat32(bias_arr.arr, self.allocator);
        defer self.allocator.free(bias_host);
        const weight_shard = try mlx.shardMatrixColumnsFloat32(self.allocator, weight_host, out_dim, in_dim, tp.rank, tp.world_size);
        defer self.allocator.free(weight_shard.data);
        const out_host = try std.heap.c_allocator.alloc(f32, rows * out_dim);
        errdefer std.heap.c_allocator.free(out_host);
        try mlx.linearShardedInputToReplicatedOutputOnStream(out_host, self.data.stream, input_host, rows, weight_shard.range.len, weight_shard.data, out_dim, bias_host, tp.group);
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        const arr = mlx.arrayFromOwnedFloat32(out_host, &shape);
        return self.makeArr(arr, true);
    }

    pub fn deinit(self: *MlxCompute) void {
        if (self.run_budget) |run_budget| {
            if (self.resident_weight_reservation) |reservation| {
                run_budget.release(reservation);
            }
        }
        if (self.run_budget) |run_budget| {
            var reservation_it = self.weight_reservations.iterator();
            while (reservation_it.next()) |entry| {
                run_budget.release(entry.value_ptr.reservation);
                self.allocator.free(entry.key_ptr.*);
            }
        } else {
            var reservation_it = self.weight_reservations.iterator();
            while (reservation_it.next()) |entry| self.allocator.free(entry.key_ptr.*);
        }
        self.weight_reservations.deinit(self.allocator);
        var tp_shard_it = self.tp_linear_shards.iterator();
        while (tp_shard_it.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.tp_linear_shards.deinit(self.allocator);
        var it = self.kv_cache.iterator();
        while (it.next()) |entry| entry.value_ptr.deinit();
        self.kv_cache.deinit(self.allocator);
        var gather_it = self.gather_cache.iterator();
        while (gather_it.next()) |entry| entry.value_ptr.deinit();
        self.gather_cache.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    fn makeArr(self: *MlxCompute, arr: c.mlx_array, owned: bool) !CT {
        return self.makeArrWithEntry(arr, owned, "", null, false, null);
    }

    fn makeHostF32(self: *MlxCompute, data: []f32, name: []const u8) !CT {
        const a = try self.allocator.create(Arr);
        a.* = .{
            .arr = c.mlx_array_new(),
            .owned = false,
            .host_f32 = data,
            .name = name,
            .lazy_entry = null,
            .pinned_lazy = false,
            .quantized_storage = null,
            .owned_quantized_storage = null,
            .reservation = null,
        };
        return a;
    }

    fn makeArrWithEntry(
        self: *MlxCompute,
        arr: c.mlx_array,
        owned: bool,
        name: []const u8,
        lazy_entry: ?*LazyWeightEntry,
        pinned_lazy: bool,
        quantized_storage: ?*QuantizedStorage,
    ) !CT {
        const a = try self.allocator.create(Arr);
        a.* = .{
            .arr = arr,
            .owned = owned,
            .host_f32 = null,
            .name = name,
            .lazy_entry = lazy_entry,
            .pinned_lazy = pinned_lazy,
            .quantized_storage = quantized_storage,
            .owned_quantized_storage = null,
            .reservation = null,
        };
        return a;
    }

    fn makeArrWithOwnedQuantizedStorage(
        self: *MlxCompute,
        arr: c.mlx_array,
        owned: bool,
        name: []const u8,
        storage: QuantizedStorage,
    ) !CT {
        const owned_storage = try self.allocator.create(QuantizedStorage);
        errdefer self.allocator.destroy(owned_storage);
        owned_storage.* = storage;
        const a = try self.allocator.create(Arr);
        a.* = .{
            .arr = arr,
            .owned = owned,
            .host_f32 = null,
            .name = name,
            .lazy_entry = null,
            .pinned_lazy = false,
            .quantized_storage = owned_storage,
            .owned_quantized_storage = owned_storage,
            .reservation = null,
        };
        return a;
    }

    pub fn fromFloat32Shape(self: *MlxCompute, data: []const f32, shape: []const i32) !CT {
        const arr = mlx.arrayFromFloat32(data, shape);
        const final_arr = try self.castToBf16IfMixed(arr);
        return self.makeArr(final_arr, true);
    }

    pub fn trainingUploadF32(self: *MlxCompute, data: []const f32, shape: []const i32) !CT {
        return self.fromFloat32Shape(data, shape);
    }

    pub fn trainingZeroF32(self: *MlxCompute, elem_count: usize, shape: []const i32) !CT {
        _ = elem_count;
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_zeros(&result, shape.ptr, shape.len, c.MLX_FLOAT32, self.data.stream));
        return self.makeArr(result, true);
    }

    pub fn trainingAccumulateF32Replace(
        self: *MlxCompute,
        accum: CT,
        grad: CT,
        scale: f32,
        first: bool,
    ) !CT {
        const s = self.data.stream;
        const scale_arr = c.mlx_array_new_float(scale);
        defer _ = c.mlx_array_free(scale_arr);

        var scaled = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(scaled);
        try mlx.check(c.mlx_multiply(&scaled, getArr(grad), scale_arr, s));
        if (first) return self.makeArr(scaled, true);

        var result = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(result);
        try mlx.check(c.mlx_add(&result, getArr(accum), scaled, s));
        _ = c.mlx_array_free(scaled);
        return self.makeArr(result, true);
    }

    pub fn trainingSumSquaresF32(self: *MlxCompute, input: CT) !f32 {
        const s = self.data.stream;
        var squared = c.mlx_array_new();
        defer _ = c.mlx_array_free(squared);
        try mlx.check(c.mlx_multiply(&squared, getArr(input), getArr(input), s));

        var sum = c.mlx_array_new();
        defer _ = c.mlx_array_free(sum);
        try mlx.check(c.mlx_sum(&sum, squared, false, s));

        const values = try mlx.readFloat32(sum, self.allocator);
        defer self.allocator.free(values);
        return if (values.len > 0) values[0] else 0.0;
    }

    pub fn trainingAdamWF32Replace(
        self: *MlxCompute,
        weight: CT,
        grad_accum: CT,
        m: CT,
        v: CT,
        opts: TrainingAdamWOptions,
    ) !TrainingAdamWResult {
        const s = self.data.stream;
        const beta1 = c.mlx_array_new_float(opts.beta1);
        defer _ = c.mlx_array_free(beta1);
        const beta2 = c.mlx_array_new_float(opts.beta2);
        defer _ = c.mlx_array_free(beta2);
        const one_minus_beta1 = c.mlx_array_new_float(1.0 - opts.beta1);
        defer _ = c.mlx_array_free(one_minus_beta1);
        const one_minus_beta2 = c.mlx_array_new_float(1.0 - opts.beta2);
        defer _ = c.mlx_array_free(one_minus_beta2);
        const grad_scale = c.mlx_array_new_float(opts.grad_scale);
        defer _ = c.mlx_array_free(grad_scale);
        const bias_correction1 = c.mlx_array_new_float(opts.bias_correction1);
        defer _ = c.mlx_array_free(bias_correction1);
        const bias_correction2 = c.mlx_array_new_float(opts.bias_correction2);
        defer _ = c.mlx_array_free(bias_correction2);
        const eps = c.mlx_array_new_float(opts.eps);
        defer _ = c.mlx_array_free(eps);
        const lr = c.mlx_array_new_float(opts.lr);
        defer _ = c.mlx_array_free(lr);
        const weight_decay = c.mlx_array_new_float(opts.weight_decay);
        defer _ = c.mlx_array_free(weight_decay);

        var g = c.mlx_array_new();
        defer _ = c.mlx_array_free(g);
        try mlx.check(c.mlx_multiply(&g, getArr(grad_accum), grad_scale, s));

        var beta1_m = c.mlx_array_new();
        defer _ = c.mlx_array_free(beta1_m);
        try mlx.check(c.mlx_multiply(&beta1_m, getArr(m), beta1, s));
        var one_minus_beta1_g = c.mlx_array_new();
        defer _ = c.mlx_array_free(one_minus_beta1_g);
        try mlx.check(c.mlx_multiply(&one_minus_beta1_g, g, one_minus_beta1, s));
        var new_m = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(new_m);
        try mlx.check(c.mlx_add(&new_m, beta1_m, one_minus_beta1_g, s));

        var g2 = c.mlx_array_new();
        defer _ = c.mlx_array_free(g2);
        try mlx.check(c.mlx_multiply(&g2, g, g, s));
        var beta2_v = c.mlx_array_new();
        defer _ = c.mlx_array_free(beta2_v);
        try mlx.check(c.mlx_multiply(&beta2_v, getArr(v), beta2, s));
        var one_minus_beta2_g2 = c.mlx_array_new();
        defer _ = c.mlx_array_free(one_minus_beta2_g2);
        try mlx.check(c.mlx_multiply(&one_minus_beta2_g2, g2, one_minus_beta2, s));
        var new_v = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(new_v);
        try mlx.check(c.mlx_add(&new_v, beta2_v, one_minus_beta2_g2, s));

        var m_hat = c.mlx_array_new();
        defer _ = c.mlx_array_free(m_hat);
        try mlx.check(c.mlx_divide(&m_hat, new_m, bias_correction1, s));
        var v_hat = c.mlx_array_new();
        defer _ = c.mlx_array_free(v_hat);
        try mlx.check(c.mlx_divide(&v_hat, new_v, bias_correction2, s));
        var sqrt_v = c.mlx_array_new();
        defer _ = c.mlx_array_free(sqrt_v);
        try mlx.check(c.mlx_sqrt(&sqrt_v, v_hat, s));
        var denom = c.mlx_array_new();
        defer _ = c.mlx_array_free(denom);
        try mlx.check(c.mlx_add(&denom, sqrt_v, eps, s));
        var adam_update = c.mlx_array_new();
        defer _ = c.mlx_array_free(adam_update);
        try mlx.check(c.mlx_divide(&adam_update, m_hat, denom, s));

        var decayed = c.mlx_array_new();
        defer _ = c.mlx_array_free(decayed);
        try mlx.check(c.mlx_multiply(&decayed, getArr(weight), weight_decay, s));
        var update = c.mlx_array_new();
        defer _ = c.mlx_array_free(update);
        try mlx.check(c.mlx_add(&update, adam_update, decayed, s));
        var scaled_update = c.mlx_array_new();
        defer _ = c.mlx_array_free(scaled_update);
        try mlx.check(c.mlx_multiply(&scaled_update, update, lr, s));
        var new_weight = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(new_weight);
        try mlx.check(c.mlx_subtract(&new_weight, getArr(weight), scaled_update, s));

        var zero_grad = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(zero_grad);
        try mlx.check(c.mlx_zeros_like(&zero_grad, getArr(grad_accum), s));

        return .{
            .weight = try self.makeArr(new_weight, true),
            .grad_accum = try self.makeArr(zero_grad, true),
            .m = try self.makeArr(new_m, true),
            .v = try self.makeArr(new_v, true),
        };
    }

    fn tensorParallelWeightRowShard(
        self: *MlxCompute,
        weight_arr: *Arr,
        out_dim: usize,
        in_dim: usize,
        rank: usize,
        world_size: usize,
    ) !TensorParallelShard {
        return self.tensorParallelMatrixShard(weight_arr, .row, out_dim, in_dim, rank, world_size);
    }

    fn tensorParallelWeightColumnShard(
        self: *MlxCompute,
        weight_arr: *Arr,
        out_dim: usize,
        in_dim: usize,
        rank: usize,
        world_size: usize,
    ) !TensorParallelShard {
        return self.tensorParallelMatrixShard(weight_arr, .column, out_dim, in_dim, rank, world_size);
    }

    const TensorParallelShardKind = enum { row, column, bias_full, bias_shard };

    fn tensorParallelCacheKey(
        self: *MlxCompute,
        name: []const u8,
        kind: TensorParallelShardKind,
    ) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}|{s}", .{
            name,
            switch (kind) {
                .row => "tp_row",
                .column => "tp_col",
                .bias_full => "tp_bias_full",
                .bias_shard => "tp_bias_shard",
            },
        });
    }

    fn tensorParallelMatrixShard(
        self: *MlxCompute,
        weight_arr: *Arr,
        kind: TensorParallelShardKind,
        out_dim: usize,
        in_dim: usize,
        rank: usize,
        world_size: usize,
    ) !TensorParallelShard {
        if (weight_arr.name.len != 0) {
            const lookup_key = try self.tensorParallelCacheKey(weight_arr.name, kind);
            defer self.allocator.free(lookup_key);
            if (self.tp_linear_shards.get(lookup_key)) |cached| return cached;

            const weight_host = try mlx.readFloat32(weight_arr.arr, self.allocator);
            defer self.allocator.free(weight_host);
            const sharded = switch (kind) {
                .row => try mlx.shardMatrixRowsFloat32(self.allocator, weight_host, out_dim, in_dim, rank, world_size),
                .column => try mlx.shardMatrixColumnsFloat32(self.allocator, weight_host, out_dim, in_dim, rank, world_size),
                else => unreachable,
            };
            const shard_rows = switch (kind) {
                .row => sharded.range.len,
                .column => out_dim,
                else => unreachable,
            };
            const shard_cols = switch (kind) {
                .row => in_dim,
                .column => sharded.range.len,
                else => unreachable,
            };
            const shard_shape = [_]i32{ @intCast(shard_rows), @intCast(shard_cols) };
            const shard_arr = mlx.arrayFromBorrowedFloat32(sharded.data, &shard_shape);
            var shard_arr_t = c.mlx_array_new();
            errdefer _ = c.mlx_array_free(shard_arr);
            errdefer _ = c.mlx_array_free(shard_arr_t);
            try mlx.check(c.mlx_transpose(&shard_arr_t, shard_arr, self.data.stream));
            const owned_key = try self.tensorParallelCacheKey(weight_arr.name, kind);
            try self.tp_linear_shards.putNoClobber(self.allocator, owned_key, .{
                .data = sharded.data,
                .rows = shard_rows,
                .cols = shard_cols,
                .arr = shard_arr,
                .arr_transposed = shard_arr_t,
            });
            return self.tp_linear_shards.get(owned_key).?;
        }

        return error.MissingWeightName;
    }

    fn tensorParallelBiasShard(
        self: *MlxCompute,
        bias_arr: *Arr,
        out_dim: usize,
        rank: usize,
        world_size: usize,
    ) !TensorParallelShard {
        if (bias_arr.name.len != 0) {
            const lookup_key = try self.tensorParallelCacheKey(bias_arr.name, .bias_shard);
            defer self.allocator.free(lookup_key);
            if (self.tp_linear_shards.get(lookup_key)) |cached| return cached;

            const bias_host = try mlx.readFloat32(bias_arr.arr, self.allocator);
            defer self.allocator.free(bias_host);
            if (bias_host.len != out_dim) return error.ShapeMismatch;
            const sharded = try mlx.shardVectorFloat32(self.allocator, bias_host, rank, world_size);
            const bias_shape = [_]i32{@intCast(sharded.range.len)};
            const bias_arr_cached = mlx.arrayFromBorrowedFloat32(sharded.data, &bias_shape);
            const owned_key = try self.tensorParallelCacheKey(bias_arr.name, .bias_shard);
            try self.tp_linear_shards.putNoClobber(self.allocator, owned_key, .{
                .data = sharded.data,
                .rows = sharded.range.len,
                .cols = 1,
                .arr = bias_arr_cached,
            });
            return self.tp_linear_shards.get(owned_key).?;
        }

        return error.MissingWeightName;
    }

    fn tensorParallelFullBias(
        self: *MlxCompute,
        bias_arr: *Arr,
        out_dim: usize,
    ) !TensorParallelShard {
        if (bias_arr.name.len != 0) {
            const lookup_key = try self.tensorParallelCacheKey(bias_arr.name, .bias_full);
            defer self.allocator.free(lookup_key);
            if (self.tp_linear_shards.get(lookup_key)) |cached| return cached;

            const bias_host = try mlx.readFloat32(bias_arr.arr, self.allocator);
            defer self.allocator.free(bias_host);
            if (bias_host.len != out_dim) return error.ShapeMismatch;
            const bias_copy = try self.allocator.dupe(f32, bias_host);
            const bias_shape = [_]i32{@intCast(out_dim)};
            const bias_arr_cached = mlx.arrayFromBorrowedFloat32(bias_copy, &bias_shape);
            const owned_key = try self.tensorParallelCacheKey(bias_arr.name, .bias_full);
            try self.tp_linear_shards.putNoClobber(self.allocator, owned_key, .{
                .data = bias_copy,
                .rows = out_dim,
                .cols = 1,
                .arr = bias_arr_cached,
            });
            return self.tp_linear_shards.get(owned_key).?;
        }

        return error.MissingWeightName;
    }
};

pub const LazyWeightEntry = gpu_hosted_store.LazyWeightEntry;

pub const WeightStore = gpu_hosted_store.WeightStore;

fn preferF32DenseTensors(data: *const WeightStore) bool {
    return data.prefer_f32_dense_tensors;
}

fn shouldForceF32DenseTensorName(data: *const WeightStore, name: []const u8) bool {
    if (!preferF32DenseTensors(data)) return false;
    // Exclude large non-linear weights from f32 promotion: vision/projector
    // towers and embedding tables used for lookup (not matmul).
    return !(std.mem.startsWith(u8, name, "vision_tower.") or
        std.mem.startsWith(u8, name, "multi_modal_projector.") or
        std.mem.endsWith(u8, name, "token_embd.weight"));
}

fn estimateResidentTensorBytes(tensor: *const tensor_mod.Tensor, prefer_f32_dense_tensors: bool) usize {
    if (prefer_f32_dense_tensors and (tensor.dtype == .f16 or tensor.dtype == .bf16)) {
        var elements: usize = 1;
        for (tensor.shape) |dim| {
            elements = std.math.mul(usize, elements, @intCast(dim)) catch return tensor.data.len;
        }
        return std.math.mul(usize, elements, @sizeOf(f32)) catch return tensor.data.len;
    }
    return tensor.data.len;
}

fn estimateResidentTensorBytesForPromotion(
    tensor: *const tensor_mod.Tensor,
    force_f32: bool,
    downcast_quant_to_f16: bool,
) usize {
    if (downcast_quant_to_f16 and !force_f32 and tensor.dtype == .f32) {
        return tensor.data.len / 2;
    }
    return estimateResidentTensorBytes(tensor, force_f32);
}

pub fn initPrefetchQueue(data: *WeightStore, allocator: std.mem.Allocator) void {
    gpu_hosted_store.installPrefetchQueue(data, allocator, &prefetchProcess, &prefetchPriority);
}

pub fn startPrefetchWorker(data: *WeightStore) !void {
    try gpu_hosted_store.startPrefetchWorker(data);
}

pub fn stopPrefetchWorker(data: *WeightStore) void {
    gpu_hosted_store.stopPrefetchWorker(data);
}

pub fn deinitPrefetchQueue(data: *WeightStore) void {
    gpu_hosted_store.deinitPrefetchQueue(data);
}

pub fn deinitPackedExpertViews(data: *WeightStore, allocator: std.mem.Allocator) void {
    gpu_hosted_store.deinitPackedExpertViews(data, allocator);
}

/// CPU transpose: re-order a [rows, cols] f32 buffer into [cols, rows].
fn transpose2Df32(out: []f32, in: []const f32, rows: usize, cols: usize) void {
    for (0..rows) |r| {
        for (0..cols) |col| {
            out[col * rows + r] = in[r * cols + col];
        }
    }
}

/// Run a 2-D matmul on the MLX GPU stream and download the result into `out`.
///
///   lhs: [lhs_rows, lhs_cols]
///   rhs: [rhs_rows, rhs_cols]   (lhs_cols must equal rhs_rows)
///   out: [lhs_rows, rhs_cols]   — overwritten on return
fn mlxMatmul2DInto(
    stream: c.mlx_stream,
    out: []f32,
    lhs: []const f32,
    lhs_rows: usize,
    lhs_cols: usize,
    rhs: []const f32,
    rhs_rows: usize,
    rhs_cols: usize,
) !void {
    std.debug.assert(lhs_cols == rhs_rows);
    std.debug.assert(out.len == lhs_rows * rhs_cols);

    const lhs_shape = [_]i32{ @intCast(lhs_rows), @intCast(lhs_cols) };
    const rhs_shape = [_]i32{ @intCast(rhs_rows), @intCast(rhs_cols) };

    const lhs_arr = mlx.arrayFromFloat32(lhs, &lhs_shape);
    defer _ = c.mlx_array_free(lhs_arr);
    const rhs_arr = mlx.arrayFromFloat32(rhs, &rhs_shape);
    defer _ = c.mlx_array_free(rhs_arr);

    var result = c.mlx_array_new();
    defer _ = c.mlx_array_free(result);
    try mlx.check(c.mlx_matmul(&result, lhs_arr, rhs_arr, stream));

    try mlx.readFloat32Into(result, out);
}

/// GPU-accelerated LoRA gradient accumulation.
///
/// Termite-zig shapes:
///   lora_a:       [rank, in_features]
///   lora_b:       [out_features, rank]
///   inputs:       [rows, in_features]
///   output_grads: [rows, out_features]
///
/// Math:
///   h       = inputs @ A^T           ([rows, rank])
///   grad_B  = dOut^T @ h * scale     ([out_features, rank])
///   dh      = dOut @ B * scale       ([rows, rank])
///   grad_A  = dh^T @ inputs          ([rank, in_features])
fn accumulateLoRAGradsMlxOp(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    grad_a: []f32,
    grad_b: []f32,
    inputs: []const f32,
    output_grads: []const f32,
    lora_a: []const f32,
    lora_b: []const f32,
    rows: usize,
    in_features: usize,
    out_features: usize,
    rank: usize,
    scale: f32,
) anyerror!void {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    if (rank == 0 or scale == 0.0) return;

    // Step 1: h = inputs @ A^T  ([rows, rank])
    // A is [rank, in_features], so A^T is [in_features, rank].
    const a_t = try allocator.alloc(f32, in_features * rank);
    defer allocator.free(a_t);
    transpose2Df32(a_t, lora_a, rank, in_features);

    const h = try allocator.alloc(f32, rows * rank);
    defer allocator.free(h);
    try mlxMatmul2DInto(s, h, inputs, rows, in_features, a_t, in_features, rank);

    // Step 2: grad_B = dOut^T @ h * scale  ([out_features, rank])
    // dOut is [rows, out_features]; dOut^T is [out_features, rows].
    const dout_t = try allocator.alloc(f32, out_features * rows);
    defer allocator.free(dout_t);
    transpose2Df32(dout_t, output_grads, rows, out_features);

    try mlxMatmul2DInto(s, grad_b, dout_t, out_features, rows, h, rows, rank);
    for (grad_b) |*v| v.* *= scale;

    // Step 3: dh = dOut @ B * scale  ([rows, rank])
    // B is [out_features, rank], so dOut @ B = [rows, out_features] @ [out_features, rank].
    const dh = try allocator.alloc(f32, rows * rank);
    defer allocator.free(dh);
    try mlxMatmul2DInto(s, dh, output_grads, rows, out_features, lora_b, out_features, rank);
    for (dh) |*v| v.* *= scale;

    // Step 4: grad_A = dh^T @ inputs  ([rank, in_features])
    // dh is [rows, rank]; dh^T is [rank, rows].
    const dh_t = try allocator.alloc(f32, rank * rows);
    defer allocator.free(dh_t);
    transpose2Df32(dh_t, dh, rows, rank);

    try mlxMatmul2DInto(s, grad_a, dh_t, rank, rows, inputs, rows, in_features);
}

fn getIo(ctx: *anyopaque) ?std.Io {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.io;
}

const vtable_impl = ComputeBackend.VTable{
    .backendKind = &backendKind,
    .deinitBackend = &deinitBackend,
    .freeTensor = &freeTensor,
    .getIo = &getIo,
    .getWeight = &getWeight,
    .prefetchWeightHint = &prefetchWeightHint,
    .drainPrefetchBudget = &drainPrefetchBudget,
    .embeddingLookup = &embeddingLookup,
    .embeddingLookupTensor = &embeddingLookupTensorOp,
    .takeRows = &takeRowsOp,
    .linear = &linearOp,
    .linearNoBias = &linearNoBiasOp,
    .linearNoBiasPair = &linearNoBiasPairOp,
    .zeroTensor = &zeroTensorOp,
    .linearLoRA = &linearLoRAMlxOp,
    .splitLastDim3 = &splitLastDim3Op,
    .reshape2D = &reshape2DOp,
    .concatRows2D = &concatRows2DOp,
    .sliceRows2D = &sliceRows2DOp,
    .mulMatId = &moeLinearNoBiasOp,
    .moeLinearNoBias = &moeLinearNoBiasOp,
    .moeLinearNoBiasPair = &moeLinearNoBiasPairOp,
    .moeScatterAdd = &moeScatterAddOp,
    .moeSelectRoutes = &moeSelectRoutesOp,
    .moeForwardFused = &moeForwardFusedOp,
    .runMoeBlock = &runMoeBlockOp,
    .layerNorm = &layerNormOp,
    .rmsNorm = &rmsNormOp,
    .gelu = &geluOp,
    .relu = &reluOp,
    .silu = &siluOp,
    .quickGelu = &quickGeluOp,
    .sigmoid = &sigmoidOp,
    .tanh_act = &tanhActOp,
    .concat = &concatOp,
    .add = &addOp,
    .scaledDotProductAttention = &sdpaOp,
    .causalSelfAttention = &causalSelfAttentionOp,
    .crossAttention = &crossAttentionOp,
    .relativePositionBias = &relativePositionBiasOp,
    .disentangledRelativeAttention = &disentangledRelativeAttentionOp,
    .windowedSelfAttention = &windowedSelfAttentionOp,
    .channelSelfAttention = &channelSelfAttentionOp,
    .tokenGridConv2d = &tokenGridConv2dOp,
    .multiply = &multiplyOp,
    .conv1d = &conv1dOp,
    .conv2d = &conv2dOp,
    .rope = &ropeOp,
    .ropePerItem = &ropePerItemOp,
    .gqaCausalAttention = &gqaCausalAttentionOp,
    .gqaPagedAttention = &gqaPagedAttentionOp,
    .fromFloat32 = &fromFloat32Op,
    .fromFloat32Shape = &fromFloat32ShapeOp,
    .fromInt32Shape = &fromInt32ShapeOp,
    .toFloat32 = &toFloat32Op,
    .tensorShape = &tensorShapeOp,
    .evalTensor = &evalTensorOp,
    .toFloat32Batch = &toFloat32BatchOp,
    .argmaxLastRow = &argmaxLastRowOp,
    .sampleLastRow = &sampleLastRowOp,
    .linearNoBiasArgmaxLastRow = &linearNoBiasArgmaxLastRowOp,
    .linearNoBiasArgmaxLastRowTensor = &linearNoBiasArgmaxLastRowTensorOp,
    .decoderRuntimePrepareGreedy = &decoderRuntimePrepareGreedyOp,
    .decoderRuntimeResetState = &decoderRuntimeResetStateOp,
    .gatherPagedKvLayerCache = &gatherPagedKvLayerCacheOp,
    .seedPagedKvLayerCache = &seedPagedKvLayerCacheOp,
    .directFamilyTimingSnapshot = &directFamilyTimingSnapshotOp,
    .debugTimingSnapshot = &debugTimingSnapshotOp,
    .resetDebugTimingStats = &resetDebugTimingStatsOp,
    .decoderRuntimePrepareOrReuseFamily = &decoderRuntimePrepareOrReuseFamilyOp,
    .decoderRuntimeReady = &decoderRuntimeReadyOp,
    .decoderRuntimeAbsoluteEmbeddingsPrepared = &decoderRuntimeAbsoluteEmbeddingsPreparedOp,
    .decoderRuntimePrepareAbsoluteEmbeddings = &decoderRuntimePrepareAbsoluteEmbeddingsOp,
    .decoderRuntimeEmbedAbsolutePosition = &decoderRuntimeEmbedAbsolutePositionOp,
    .decoderRuntimePrepareLayerNorm = &decoderRuntimePrepareLayerNormOp,
    .decoderRuntimeApplyLayerNorm = &decoderRuntimeApplyLayerNormOp,
    .decoderRuntimePrepareRmsNorm = &decoderRuntimePrepareRmsNormOp,
    .decoderRuntimeApplyRmsNorm = &decoderRuntimeApplyRmsNormOp,
    .decoderRuntimeApplyLayerNormLinear = &decoderRuntimeApplyLayerNormLinearOp,
    .decoderRuntimeApplyLayerNormLinearArgmax = &decoderRuntimeApplyLayerNormLinearArgmaxOp,
    .decoderRuntimeApplyLayerNormLinearSample = &decoderRuntimeApplyLayerNormLinearSampleOp,
    .decoderRuntimeApplyRmsNormLinear = &decoderRuntimeApplyRmsNormLinearOp,
    .decoderRuntimeApplyRmsNormLinearArgmax = &decoderRuntimeApplyRmsNormLinearArgmaxOp,
    .decoderRuntimeApplyRmsNormLinearSample = &decoderRuntimeApplyRmsNormLinearSampleOp,
    .decoderRuntimePrepareLinear = &decoderRuntimePrepareLinearOp,
    .decoderRuntimeApplyLinear = &decoderRuntimeApplyLinearOp,
    .decoderRuntimeApplyLinearArgmax = &decoderRuntimeApplyLinearArgmaxOp,
    .decoderRuntimeApplyLinearPair = &decoderRuntimeApplyLinearPairOp,
    .decoderRuntimeApplyLinearQkv = &decoderRuntimeApplyLinearQkvOp,
    .decoderRuntimeApplyActivation = &decoderRuntimeApplyActivationOp,
    .decoderRuntimeApplyAdd = &decoderRuntimeApplyAddOp,
    .runDenseFfnResidual = &runDenseFfnResidualOp,
    .runGatedFfnResidual = &runGatedFfnResidualOp,
    .runAttention = &runAttentionOp,
    .runAttentionResidual = &runAttentionResidualOp,
    .runDenseDecoderBlock = &runDenseDecoderBlockOp,
    .runGatedDecoderBlock = &runGatedDecoderBlockOp,
    .reshape2d = &reshape2dOp,
    .sliceLastDim = &sliceLastDimOp,
    // ── Primitive ops for training ──
    .subtract = &primSubtractOp,
    .divide = &primDivideOp,
    .negate = &primNegateOp,
    .sqrtOp = &primSqrtOp,
    .rsqrtOp = &primRsqrtOp,
    .expOp = &primExpOp,
    .logOp = &primLogOp,
    .sinOp = &primSinOp,
    .cosOp = &primCosOp,
    .tanhOp = &primTanhOp,
    .erfOp = &primErfOp,
    .absOp = &primAbsOp,
    .lessThan = &primLessThanOp,
    .whereSelect = &primWhereSelectOp,
    .reduceSumOp = &primReduceSumOp,
    .reduceMaxOp = &primReduceMaxOp,
    .reduceMeanOp = &primReduceMeanOp,
    .reshapeOp = &primReshapeOp,
    .transposeOp = &primTransposeOp,
    .broadcastInDimOp = &primBroadcastInDimOp,
    .dotGeneralOp = &primDotGeneralOp,
    .scatterAddOp = &primScatterAddOp,
    .gatherOp = &primGatherOp,
    .sliceOp = &primSliceOp,
    .concatPrimOp = &primConcatPrimOp,
    .softmaxOp = &primSoftmaxOp,
    .logSoftmaxOp = &primLogSoftmaxOp,
    .accumulateLoRAGrads = &accumulateLoRAGradsMlxOp,
};

fn backendKind(_: *anyopaque) BackendKind {
    return .mlx;
}

fn deinitBackend(ctx: *anyopaque) void {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    self.deinit();
}

fn freeTensor(ctx: *anyopaque, tensor: CT) void {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const a = toArr(tensor);
    if (a.lazy_entry) |entry| {
        if (entry.guard) |guard| {
            while (!guard.tryLock()) {
                std.Thread.yield() catch {};
            }
            defer guard.unlock();
            if (a.pinned_lazy and entry.pin_count > 0) entry.pin_count -= 1;
            maybeDemoteColdHostExpertLocked(self.data, entry);
            maybeDemoteColdBackendLazyWeightLocked(self.data, entry);
        } else if (a.pinned_lazy and entry.pin_count > 0) {
            entry.pin_count -= 1;
            maybeDemoteColdHostExpertLocked(self.data, entry);
            maybeDemoteColdBackendLazyWeightLocked(self.data, entry);
        }
        if (a.name.len > 0) releaseWeightReservation(self, a.name);
    }
    if (a.owned) _ = c.mlx_array_free(a.arr);
    if (a.host_f32) |data| self.allocator.free(data);
    if (a.owned_quantized_storage) |storage| {
        storage.deinit();
        self.allocator.destroy(storage);
    }
    self.allocator.destroy(a);
}

fn maybeDemoteColdHostExpertLocked(data: *WeightStore, entry: *LazyWeightEntry) void {
    if (entry.pin_count != 0) return;
    const coord = entry.expert_coord orelse return;
    if (entry.loaded != null) return;
    const tier_cache = data.tier_cache orelse return;
    if (tier_cache.budget.host_limit_bytes == 0) return;
    if (tier_cache.host_bytes * 100 < tier_cache.budget.host_limit_bytes * 95) return;
    if (!expertCanEvictLocked(data, coord, .host)) return;
    unloadExpertTierLocked(data, coord, .host);
}

fn maybeDemoteColdBackendLazyWeightLocked(data: *WeightStore, entry: *LazyWeightEntry) void {
    if (entry.pin_count != 0) return;
    if (entry.expert_coord != null) return;
    if (!entryResidentAtTier(entry.*, .backend)) return;
    const tier_cache = data.tier_cache orelse return;
    if (tier_cache.budget.backend_limit_bytes == 0) return;
    if (tier_cache.backend_bytes * 100 < tier_cache.budget.backend_limit_bytes * 90) return;
    unloadLazyEntryTierLocked(data, entry, .backend);
}

fn countPackedExpertPrefetchBackendEntriesLocked(data: *const WeightStore) usize {
    var count: usize = 0;
    var it = data.lazy_weights.iterator();
    while (it.next()) |kv| {
        const entry = kv.value_ptr;
        if (entry.expert_coord != null and entry.loaded_quantized != null) count += 1;
    }
    return count;
}

fn findPackedExpertPrefetchEvictionVictimLocked(data: *WeightStore, protected: *LazyWeightEntry) ?*LazyWeightEntry {
    var best: ?*LazyWeightEntry = null;
    var it = data.lazy_weights.iterator();
    while (it.next()) |kv| {
        const candidate = kv.value_ptr;
        if (candidate == protected) continue;
        if (candidate.expert_coord == null) continue;
        if (candidate.loaded_quantized == null) continue;
        if (candidate.pin_count != 0) continue;
        if (best == null or
            candidate.last_access_epoch < best.?.last_access_epoch or
            (candidate.last_access_epoch == best.?.last_access_epoch and candidate.backend_loaded_bytes > best.?.backend_loaded_bytes))
        {
            best = candidate;
        }
    }
    return best;
}

fn maybeStagePackedExpertPrefetchBackendLocked(_: *WeightStore, _: *LazyWeightEntry) !void {
    // No-op: packed expert weights are accessed via arrayFromBorrowedBytes in the
    // grouped MoE kernel, not via loaded_quantized. Staging a Metal copy of the full
    // packed tensor per expert entry wastes memory — each expert's raw_bytes points to
    // the entire packed tensor (all 128 experts), so 8 active experts × 3 projections
    // × 30 layers = 720 copies of ~200MB tensors = ~144GB attempted.
}

fn acquireWeightReservation(self: *MlxCompute, name: []const u8, tier: ResidencyTier, bytes: usize) !void {
    if (self.run_budget == null or name.len == 0) return;
    if (self.weight_reservations.getPtr(name)) |state| {
        state.count += 1;
        return;
    }
    const reservation = try self.run_budget.?.tryReserveWeight(tier, bytes);
    try self.weight_reservations.put(self.allocator, try self.allocator.dupe(u8, name), .{
        .count = 1,
        .reservation = reservation,
    });
}

fn releaseWeightReservation(self: *MlxCompute, name: []const u8) void {
    if (self.run_budget == null or name.len == 0) return;
    if (self.weight_reservations.fetchRemove(name)) |removed| {
        if (removed.value.count > 1) {
            self.weight_reservations.put(self.allocator, removed.key, .{
                .count = removed.value.count - 1,
                .reservation = removed.value.reservation,
            }) catch {
                self.allocator.free(removed.key);
                self.run_budget.?.release(removed.value.reservation);
                return;
            };
            return;
        }
        self.allocator.free(removed.key);
        self.run_budget.?.release(removed.value.reservation);
    }
}

fn reserveLazyWeightForUse(self: *MlxCompute, name: []const u8, entry: *LazyWeightEntry) !void {
    if (entry.loaded == null and entry.loaded_quantized == null) {
        if (entry.quantized_storage) |*storage| {
            if (storage.packed_expert != null) return;
        } else if (entry.host_loaded) |*loaded| {
            if (loaded.quantized_storage) |*storage| {
                if (storage.packed_expert != null) return;
            }
        }
    }
    if (entryResidentAtTier(entry.*, .backend)) {
        const bytes = if (entry.backend_loaded_bytes != 0) entry.backend_loaded_bytes else entry.loaded_bytes;
        try acquireWeightReservation(self, name, .backend, bytes);
        return;
    }
    if (entryResidentAtTier(entry.*, .host)) {
        try acquireWeightReservation(self, name, .host, entry.loaded_bytes);
    }
}

fn loadEphemeralQuantizedLazyWeight(
    self: *MlxCompute,
    entry: *LazyWeightEntry,
    name: []const u8,
) !?CT {
    if (entry.expert_coord != null) return null;
    const tensor_store = self.data.tensor_store orelse return null;
    const storage = (try tensor_store.loadQuantizedStorageRef(&entry.tensor_ref)) orelse return null;
    return self.makeArrWithOwnedQuantizedStorage(c.mlx_array_new(), false, name, storage);
}

fn shouldUseQuantizedPlaceholder(
    name: []const u8,
    storage: *QuantizedStorage,
) bool {
    if (storage.packed_expert != null) return true;
    if (storage.shape.len != 2) return false;
    // Only hand a quantized-placeholder entry to the backend if MLX has a
    // native Metal kernel for this quant type. Otherwise the weight ends up
    // with `loaded_quantized` set but `loaded` null, and the dense fallback
    // path in linearNoBiasDenseMlx errors at ensureLazyWeightTranspose.
    if (!mlxHasNativeQuantKernel(storage.tensor_type)) return false;

    return std.mem.eql(u8, name, "lm_head.weight") or
        std.mem.endsWith(u8, name, ".self_attn.q_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.k_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.v_proj.weight") or
        std.mem.endsWith(u8, name, ".self_attn.o_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.gate_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.up_proj.weight") or
        std.mem.endsWith(u8, name, ".mlp.down_proj.weight") or
        std.mem.endsWith(u8, name, ".block_sparse_moe.gate.weight") or
        std.mem.endsWith(u8, name, ".w1.weight") or
        std.mem.endsWith(u8, name, ".w2.weight") or
        std.mem.endsWith(u8, name, ".w3.weight") or
        std.mem.endsWith(u8, name, ".c_attn.weight") or
        std.mem.endsWith(u8, name, ".c_proj.weight") or
        std.mem.endsWith(u8, name, ".c_fc.weight") or
        std.mem.endsWith(u8, name, ".fc1.weight") or
        std.mem.endsWith(u8, name, ".fc2.weight");
}

fn shouldUseQuantizedPlaceholderWithMode(
    data: *const WeightStore,
    name: []const u8,
    storage: *QuantizedStorage,
) bool {
    if (data.quant_execution_mode == .prefer_backend_dense) return false;
    return shouldUseQuantizedPlaceholder(name, storage);
}

/// Q1_0 → Q4_0 re-packer (retained as a fallback for builds without the
/// native Metal Q1_0 kernel).
///
/// Q1_0 is a 1-bit format: each weight is `±d` where `d` is a per-block fp16
/// scale. If a Metal Q1_0 kernel is available (the usual case), we run on
/// the compressed 1.16 GB representation directly. If not, Q4_0 can hold
/// ±d losslessly — Q4_0 decodes each 4-bit code `q` as `d × (q - 8)`, so
/// `q = 9` encodes `+d` and `q = 7` encodes `-d`. The repack is exact.
///
/// Returns null when the native Q1_0 kernel is present (opt-out gate
/// `TERMITE_FORCE_Q1_0_REPACK=1` keeps this path alive for debugging).
fn repackQuantStorageForMlx(
    allocator: std.mem.Allocator,
    storage: *const weight_source_mod.QuantizedStorage,
) !?weight_source_mod.QuantizedStorage {
    const known = switch (storage.tensor_type) {
        .known => |k| k,
        else => return null,
    };
    if (known != .Q1_0) return null;
    if (storage.packed_expert != null) return null; // Packed MoE experts untouched for now.
    if (mlxHasNativeQuantKernel(storage.tensor_type) and !forceQ1_0RepackDebug()) return null;

    const q1_block_bytes: usize = 18;
    const q1_values_per_block: usize = 128;
    const q4_block_bytes: usize = 18;
    const q4_values_per_block: usize = 32;
    const q4_per_q1: usize = q1_values_per_block / q4_values_per_block; // 4

    if (storage.raw_bytes.len % q1_block_bytes != 0) return null;
    const num_q1_blocks = storage.raw_bytes.len / q1_block_bytes;
    const num_q4_blocks = num_q1_blocks * q4_per_q1;

    const out = try allocator.alloc(u8, num_q4_blocks * q4_block_bytes);
    errdefer allocator.free(out);

    for (0..num_q1_blocks) |bi| {
        const q1 = storage.raw_bytes[bi * q1_block_bytes ..][0..q1_block_bytes];
        const scale_bytes = q1[0..2];
        const sign_bits = q1[2..18];

        for (0..q4_per_q1) |sub| {
            const q4 = out[(bi * q4_per_q1 + sub) * q4_block_bytes ..][0..q4_block_bytes];
            q4[0] = scale_bytes[0];
            q4[1] = scale_bytes[1];
            // Q4_0 layout in the dequant kernel:
            //   for i in 0..16: value[i]      = (qs[i]      & 0x0F) - 8
            //   for i in 0..16: value[16 + i] = (qs[i] >> 4)         - 8
            // So to encode ±d we need q = 9 for +d, q = 7 for -d.
            for (0..16) |i| {
                const lo_idx = sub * q4_values_per_block + i;
                const hi_idx = sub * q4_values_per_block + i + 16;
                const lo_bit: u8 = (sign_bits[lo_idx / 8] >> @as(u3, @intCast(lo_idx % 8))) & 1;
                const hi_bit: u8 = (sign_bits[hi_idx / 8] >> @as(u3, @intCast(hi_idx % 8))) & 1;
                const lo_code: u8 = if (lo_bit != 0) 9 else 7;
                const hi_code: u8 = if (hi_bit != 0) 9 else 7;
                q4[2 + i] = lo_code | (hi_code << 4);
            }
        }
    }

    const shape_copy = try allocator.dupe(i64, storage.shape);
    errdefer allocator.free(shape_copy);
    const source_name_copy = if (storage.source_name) |n| try allocator.dupe(u8, n) else null;
    errdefer if (source_name_copy) |n| allocator.free(n);

    return weight_source_mod.QuantizedStorage{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = out,
        .shape = shape_copy,
        .source_name = source_name_copy,
        .packed_expert = null,
        .raw_owned = true,
        .allocator = allocator,
    };
}

/// Mirror of `mlx_quant.MetalProvider.planLinearNoBias`'s supported-type set.
/// If this returns false, the MLX backend has no device-native quant kernel
/// for this tensor type and weights of this type must be dequantized to
/// dense f32 on load instead of being handed across as a quant placeholder.
fn mlxHasNativeQuantKernel(tensor_type: gguf_tensor_types.TensorType) bool {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .Q4_0, .Q4_1, .Q5_0, .Q5_1, .Q8_0, .Q8_1, .IQ4_NL => true,
            .Q1_0 => true,
            .I2_S => true,
            .Q2_K, .Q3_K, .Q4_K, .Q5_K, .Q6_K, .Q8_K, .IQ4_XS => true,
            else => false,
        },
        .bitnet_tl2 => false,
        .unknown => false,
    };
}

fn forceQ1_0RepackDebug() bool {
    const C = @cImport(@cInclude("stdlib.h"));
    const value = C.getenv("TERMITE_FORCE_Q1_0_REPACK") orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes");
}

fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    // Build the null-terminated full weight name using a stack buffer to avoid
    // malloc/free per call (~600 allocations per token eliminated).
    var name_buf: [1024]u8 = undefined;
    const name_z: [:0]const u8 = blk: {
        const written = if (self.data.prefix.len > 0)
            std.fmt.bufPrint(&name_buf, "{s}.{s}", .{ self.data.prefix, name }) catch
                return error.WeightNameTooLong
        else src: {
            if (name.len >= name_buf.len) return error.WeightNameTooLong;
            @memcpy(name_buf[0..name.len], name);
            break :src name_buf[0..name.len];
        };
        name_buf[written.len] = 0;
        break :blk name_buf[0..written.len :0];
    };

    if (mlx.getWeight(self.data.resident_weights, name_z)) |arr| {
        const casted = self.mixed_precision and c.mlx_array_dtype(arr) == c.MLX_FLOAT32;
        const final_arr = try self.castToBf16IfMixed(arr);
        return self.makeArrWithEntry(final_arr, casted, name, null, false, null);
    }

    // Fallback: try without prefix (needed for GLiNER's span_rep/count_embed weights
    // which exist in SafeTensors without the encoder. prefix)
    if (self.data.prefix.len > 0) {
        var raw_z_buf: [1024]u8 = undefined;
        if (name.len >= raw_z_buf.len) return error.WeightNameTooLong;
        @memcpy(raw_z_buf[0..name.len], name);
        raw_z_buf[name.len] = 0;
        const raw_z: [:0]const u8 = raw_z_buf[0..name.len :0];
        if (mlx.getWeight(self.data.resident_weights, raw_z)) |arr| {
            const casted = self.mixed_precision and c.mlx_array_dtype(arr) == c.MLX_FLOAT32;
            const final_arr = try self.castToBf16IfMixed(arr);
            return self.makeArrWithEntry(final_arr, casted, name, null, false, null);
        }
    }
    if (try tryGetVisualHfWeight(self, name)) |arr| {
        const casted = self.mixed_precision and c.mlx_array_dtype(arr) == c.MLX_FLOAT32;
        const final_arr = try self.castToBf16IfMixed(arr);
        return self.makeArrWithEntry(final_arr, casted, name, null, false, null);
    }

    self.data.prefetch.lock();
    defer self.data.prefetch.unlock();
    if (self.data.lazy_weights.getPtr(name)) |entry| {
        touchLazyWeightLocked(self.data, entry);
        ensureHostLazyWeightLoadedLocked(self.data, self.run_budget, entry) catch |err| switch (err) {
            error.MemoryBudgetExceeded => {
                if (self.data.tier_cache) |*tier_cache| {
                    logSharedCacheDenial("get_weight_host_load", tier_cache, name);
                }
                if (entry.expert_coord == null) {
                    if (try loadEphemeralQuantizedLazyWeight(self, entry, name)) |ephemeral_quantized| {
                        logQuantBudgetFallback(self, "host_cache_ephemeral_quantized", toArr(ephemeral_quantized));
                        return ephemeral_quantized;
                    }
                    if (canFallbackDenseBudgetPressure(name, entry)) {
                        logDenseBudgetFallback(self, "host_cache_placeholder_dense", name);
                        return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, false, null);
                    }
                    return denseLazyWeightEphemeral(self, entry);
                }
                return err;
            },
            else => return err,
        };
        try repairMalformedPackedExpertEntryLocked(self, entry);
        const quantized_storage = if (entry.quantized_storage) |*storage|
            storage
        else if (entry.host_loaded) |*loaded|
            if (loaded.quantized_storage) |*storage| storage else null
        else
            null;
        if (quantized_storage != null and
            quantized_storage.?.packed_expert != null and
            entry.loaded_quantized != null)
        {
            entry.pin_count += 1;
            try reserveLazyWeightForUse(self, name, entry);
            return self.makeArrWithEntry(entry.loaded_quantized.?, false, name, entry, true, quantized_storage);
        }
        if (quantized_storage != null and shouldUseQuantizedPlaceholderWithMode(self.data, name, quantized_storage.?)) {
            try reserveLazyWeightForUse(self, name, entry);
            if (quantized_storage.?.packed_expert != null) {
                // Packed expert weights: skip Metal buffer copy. The grouped MoE
                // kernel borrows directly from raw_bytes via arrayFromBorrowedBytes,
                // and per-expert fallback reads from quantized_storage. Copying the
                // full packed tensor (~285MB per projection) per expert entry causes
                // massive duplication (8 copies × 30 layers = ~70GB attempted).
                entry.pin_count += 1;
                return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, false, quantized_storage);
            }
            // Cache the UINT8 MLX array on first access so resolveWeightArray() can
            // reuse it across tokens instead of recreating from raw_bytes each time.
            if (entry.loaded_quantized == null) {
                const storage = quantized_storage.?;
                const backend_bytes = storage.raw_bytes.len;
                if (self.data.tier_cache) |*tier_cache| {
                    evictColdNonExpertWeightsToFitLocked(self.data, .backend, backend_bytes, entry);
                    if (!tier_cache.canFitAdditional(.backend, backend_bytes)) {
                        tier_cache.noteDenied(.backend, backend_bytes);
                        noteSharedCacheDenial(self.run_budget, tier_cache, .backend, backend_bytes);
                        logSharedCacheDenial("get_weight_backend_quantized_cache", tier_cache, name);
                        entry.pin_count += 1;
                        return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, false, quantized_storage);
                    }
                }
                const quant_shape = [_]i32{@intCast(storage.raw_bytes.len)};
                entry.loaded_quantized = mlx.arrayFromBytes(storage.raw_bytes, &quant_shape, c.MLX_UINT8);
                entry.active_tier = .backend;
                entry.backend_loaded_bytes = backend_bytes;
                if (self.data.tier_cache) |*tier_cache| tier_cache.noteResident(.backend, backend_bytes);
            }
            entry.pin_count += 1;
            return self.makeArrWithEntry(entry.loaded_quantized.?, false, name, entry, true, quantized_storage);
        }
        if (quantized_storage != null and
            quantized_storage.?.packed_expert == null and
            entry.loaded == null and
            shouldUseQuantizedPlaceholder(name, quantized_storage.?))
        {
            try reserveLazyWeightForUse(self, name, entry);
            return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, false, quantized_storage);
        }
        if (quantized_storage == null or quantized_storage.?.packed_expert == null) {
            if (quantized_storage != null and !shouldUseQuantizedPlaceholder(name, quantized_storage.?)) {
                // Quantized weight not suitable for GPU placeholder (e.g. large embedding
                // tables used for lookup, not matmul). Skip backend promotion entirely —
                // callers like embeddingLookup handle these via per-row CPU dequantization
                // from quantized_storage.raw_bytes, avoiding full tensor materialization.
                entry.pin_count += 1;
                try reserveLazyWeightForUse(self, name, entry);
                return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, false, quantized_storage);
            }
            prefetchLazyWeightLocked(self, entry, .backend) catch |err| switch (err) {
                error.MemoryBudgetExceeded => {
                    if (entry.expert_coord == null) {
                        if (quantized_storage == null) return denseLazyWeightEphemeral(self, entry);
                        // Quantized weight that passed the placeholder check but still
                        // can't fit on GPU. Return for CPU-side handling.
                        entry.pin_count += 1;
                        try reserveLazyWeightForUse(self, name, entry);
                        return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, false, quantized_storage);
                    }
                    return err;
                },
                else => return err,
            };
        } else if (self.data.quant_execution_mode == .device_native and
            shouldUseQuantizedPlaceholder(name, quantized_storage.?))
        {
            prefetchLazyWeightLocked(self, entry, .backend) catch |err| switch (err) {
                error.MemoryBudgetExceeded => {},
                else => return err,
            };
        }
        entry.pin_count += 1;
        if (entry.loaded == null and quantized_storage != null and quantized_storage.?.packed_expert != null) {
            if (entry.loaded_quantized) |arr| {
                try reserveLazyWeightForUse(self, name, entry);
                return self.makeArrWithEntry(arr, false, name, entry, true, quantized_storage);
            }
            try reserveLazyWeightForUse(self, name, entry);
            return self.makeArrWithEntry(c.mlx_array_new(), false, name, entry, true, quantized_storage);
        }
        if (entry.loaded == null and
            quantized_storage != null and
            entry.loaded_quantized != null and
            shouldUseQuantizedPlaceholder(name, quantized_storage.?))
        {
            try reserveLazyWeightForUse(self, name, entry);
            return self.makeArrWithEntry(entry.loaded_quantized.?, false, name, entry, false, quantized_storage);
        }
        if (entry.loaded == null and quantized_storage != null and entry.loaded_quantized != null) {
            return denseLazyWeightEphemeral(self, entry);
        }
        try reserveLazyWeightForUse(self, name, entry);
        const cloned = try cloneArray(self, entry.loaded.?);
        const final_cloned = try self.castToBf16IfMixed(cloned);
        return self.makeArrWithEntry(
            final_cloned,
            true,
            name,
            entry,
            true,
            quantized_storage,
        );
    }

    // Some architecture paths probe optional/shared weights and catch this;
    // keep normal logs quiet while still surfacing required misses via the error.
    return error.MissingWeight;
}

fn tryGetVisualHfWeight(self: *MlxCompute, name: []const u8) !?c.mlx_array {
    if (!std.mem.startsWith(u8, name, "visual.")) return null;
    const hf_name = try std.fmt.allocPrint(self.allocator, "vlm.model.{s}", .{name});
    defer self.allocator.free(hf_name);
    const hf_name_z = try self.allocator.dupeZ(u8, hf_name);
    defer self.allocator.free(hf_name_z);
    return mlx.getWeight(self.data.resident_weights, hf_name_z);
}

fn tryGetVisualHfWeightNoErr(self: *MlxCompute, name: []const u8) ?c.mlx_array {
    if (!std.mem.startsWith(u8, name, "visual.")) return null;
    const hf_name = std.fmt.allocPrint(self.allocator, "vlm.model.{s}", .{name}) catch return null;
    defer self.allocator.free(hf_name);
    const hf_name_z = self.allocator.dupeZ(u8, hf_name) catch return null;
    defer self.allocator.free(hf_name_z);
    return mlx.getWeight(self.data.resident_weights, hf_name_z);
}

fn prefetchWeightHint(ctx: *anyopaque, name: []const u8, hint: u32) void {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (self.data.prefix.len > 0) {
        const full_name = std.fmt.allocPrint(self.allocator, "{s}.{s}", .{ self.data.prefix, name }) catch return;
        defer self.allocator.free(full_name);
        const name_z = self.allocator.dupeZ(u8, full_name) catch return;
        defer self.allocator.free(name_z);
        if (mlx.getWeight(self.data.resident_weights, name_z)) |_| return;
    }
    const raw_z = self.allocator.dupeZ(u8, name) catch return;
    defer self.allocator.free(raw_z);
    if (mlx.getWeight(self.data.resident_weights, raw_z)) |_| return;
    if (tryGetVisualHfWeightNoErr(self, name)) |_| return;
    self.data.prefetch.lock();
    defer self.data.prefetch.unlock();
    if (self.data.lazy_weights.getPtr(name)) |entry| {
        if (entry.pending_prefetch) {
            entry.prefetch_score +|= hint;
            return;
        }
        if (entry.loaded_quantized != null or entry.loaded != null) return;
        if (self.data.shared_prefetch) |shared_prefetch| {
            entry.prefetch_score = shared_prefetch.noteRequest(name, hint) catch entry.prefetch_score;
        } else {
            entry.prefetch_score +|= hint;
        }
        enqueuePrefetchLocked(self.data, entry) catch {};
        self.data.prefetch.signal();
    }
}

fn drainPrefetchBudget(ctx: *anyopaque, max_items: usize) void {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    self.data.prefetch.drainBudget(max_items);
}

fn enqueuePrefetchLocked(data: *WeightStore, entry: *LazyWeightEntry) !void {
    if (entry.pending_prefetch) return;
    entry.pending_prefetch = true;
    try data.prefetch.appendLocked(entry);
}

fn prefetchProcess(ctx: *anyopaque, entry: *LazyWeightEntry) void {
    const data: *WeightStore = @ptrCast(@alignCast(ctx));
    entry.pending_prefetch = false;
    ensureHostLazyWeightLoadedLocked(data, null, entry) catch {};
    maybeStagePackedExpertPrefetchBackendLocked(data, entry) catch {};
    if (data.shared_prefetch) |shared_prefetch| {
        entry.prefetch_score = shared_prefetch.noteComplete(entry.tensor_ref.name) catch 0;
    } else {
        entry.prefetch_score = 0;
    }
}

fn prefetchPriority(entry: *LazyWeightEntry) u64 {
    return entry.prefetch_score;
}

fn touchLazyWeightLocked(data: *WeightStore, entry: *LazyWeightEntry) void {
    entry.last_access_epoch = data.access_epoch;
    data.access_epoch +|= 1;
}

fn touchPackedExpertViewEntryLocked(data: *WeightStore, entry: *PackedExpertViewEntry) void {
    entry.last_access_epoch = data.access_epoch;
    data.access_epoch +|= 1;
}

fn packedExpertViewCacheKey(allocator: std.mem.Allocator, storage: *const QuantizedStorage) !?[]u8 {
    const packed_view = storage.packed_expert orelse return null;
    const source_name = storage.source_name orelse return null;
    return try std.fmt.allocPrint(allocator, "{s}#{d}", .{ source_name, packed_view.expert_index });
}

fn findPackedExpertViewEvictionVictimLocked(data: *WeightStore) ?[]const u8 {
    var best_key: ?[]const u8 = null;
    var best_epoch: u64 = 0;

    var it = data.packed_expert_views.iterator();
    while (it.next()) |entry| {
        const candidate = entry.value_ptr;
        if (candidate.owned_copy == null or candidate.pin_count != 0) continue;
        if (best_key == null or candidate.last_access_epoch < best_epoch) {
            best_key = entry.key_ptr.*;
            best_epoch = candidate.last_access_epoch;
        }
    }
    return best_key;
}

fn evictPackedExpertViewsToFitLocked(data: *WeightStore, required_bytes: usize) void {
    while (true) {
        const tier_cache = data.tier_cache orelse break;
        if (tier_cache.canFitAdditional(.host, required_bytes)) break;
        const victim_key = findPackedExpertViewEvictionVictimLocked(data) orelse break;
        if (data.packed_expert_views.fetchRemove(victim_key)) |removed| {
            if (removed.value.owned_copy) |bytes| {
                if (data.tier_cache) |*cache| cache.noteRelease(.host, bytes.len);
                data.packed_expert_view_bytes -= bytes.len;
            }
            var entry = removed.value;
            entry.deinit();
            data.allocator.free(removed.key);
        } else break;
    }
}

fn acquirePreparedPackedExpertViewLocked(
    data: *WeightStore,
    storage: *QuantizedStorage,
    in_dim: usize,
    out_dim: usize,
) !?CachedPackedExpertView {
    const owned_key = (try packedExpertViewCacheKey(data.allocator, storage)) orelse return null;
    errdefer data.allocator.free(owned_key);

    if (data.packed_expert_views.getPtr(owned_key)) |entry| {
        mlx_quant.notePreparedViewCacheHit();
        entry.pin_count += 1;
        touchPackedExpertViewEntryLocked(data, entry);
        return .{ .key = owned_key, .owned_key = true, .bytes = entry.bytes };
    }

    mlx_quant.notePreparedViewCacheMiss();
    const prepared = try mlx_quant.preparePackedExpertViewForLinear(storage, in_dim, out_dim, storage.tensor_type);
    var release_prepared = prepared.owned;
    errdefer if (release_prepared) std.heap.c_allocator.free(@constCast(prepared.bytes));
    if (prepared.owned) {
        mlx_quant.notePreparedViewOwnedMaterialization();
        evictPackedExpertViewsToFitLocked(data, prepared.bytes.len);
        if (data.tier_cache) |*tier_cache| {
            if (!tier_cache.canFitAdditional(.host, prepared.bytes.len)) {
                tier_cache.noteDenied(.host, prepared.bytes.len);
                std.heap.c_allocator.free(@constCast(prepared.bytes));
                return null;
            }
        }
    }

    try data.packed_expert_views.put(data.allocator, owned_key, .{
        .bytes = prepared.bytes,
        .owned_copy = if (prepared.owned) prepared.bytes else null,
    });
    release_prepared = false;
    const entry = data.packed_expert_views.getPtr(owned_key).?;
    entry.pin_count = 1;
    touchPackedExpertViewEntryLocked(data, entry);
    if (prepared.owned) {
        if (data.tier_cache) |*tier_cache| tier_cache.noteResident(.host, prepared.bytes.len);
        data.packed_expert_view_bytes += prepared.bytes.len;
    }
    return .{ .key = owned_key, .owned_key = false, .bytes = entry.bytes };
}

fn releasePreparedPackedExpertViewLocked(data: *WeightStore, key: []const u8) void {
    if (data.packed_expert_views.getPtr(key)) |entry| {
        if (entry.pin_count > 0) entry.pin_count -= 1;
    }
}

fn ensureHostLazyWeightLoadedLocked(data: *WeightStore, run_budget: ?*run_memory.RunBudget, entry: *LazyWeightEntry) !void {
    if (entry.expert_coord) |coord| {
        if (data.residency) |*residency| {
            try residency.noteTouch(coord, data.moe_num_experts);
        }
    }
    if (entry.quantized_storage == null and entry.host_loaded == null) {
        const mmap_direct_packed_quant = data.allow_direct_quant and
            entry.tensor_ref.quantized and
            entry.tensor_ref.packed_expert_index != null;
        const expected_bytes = if (entry.loaded_bytes != 0)
            entry.loaded_bytes
        else if (mmap_direct_packed_quant)
            0
        else
            entry.tensor_ref.byte_len;
        if (entry.expert_coord) |coord| {
            evictColdExpertsLocked(data, coord, .host);
        } else {
            evictColdNonExpertWeightsToFitLocked(data, .host, expected_bytes, entry);
        }
        if (data.tier_cache) |*tier_cache| {
            if (!tier_cache.canFitAdditional(.host, expected_bytes)) {
                if (entry.expert_coord) |coord| {
                    evictColdExpertsToFitLocked(data, coord, .host, expected_bytes);
                } else {
                    evictColdNonExpertWeightsToFitLocked(data, .host, expected_bytes, entry);
                }
                if (!tier_cache.canFitAdditional(.host, expected_bytes)) {
                    tier_cache.noteDenied(.host, expected_bytes);
                    noteSharedCacheDenial(run_budget, tier_cache, .host, expected_bytes);
                    return error.MemoryBudgetExceeded;
                }
            }
        }
        const tensor_store = data.tensor_store orelse {
            std.log.err("MLX missing tensor store for lazy weight {s}", .{entry.tensor_ref.name});
            return error.MissingWeight;
        };
        if (data.allow_direct_quant) {
            if (try tensor_store.loadQuantizedStorageRef(&entry.tensor_ref)) |loaded_storage| {
                var storage = loaded_storage;
                var effective_storage = storage;
                if (try repackQuantStorageForMlx(data.allocator, &storage)) |repacked| {
                    storage.deinit();
                    effective_storage = repacked;
                }
                entry.loaded_bytes = quantizedStorageBudgetBytes(&effective_storage);
                entry.quantized_storage = effective_storage;
            } else {
                entry.host_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
                if (entry.host_loaded.?.quantized_storage) |*storage| {
                    storage.deinit();
                    entry.host_loaded.?.quantized_storage = null;
                    entry.host_loaded.?.quantized = false;
                }
                entry.loaded_bytes = entry.host_loaded.?.tensor.data.len;
            }
        } else {
            entry.host_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
            if (entry.host_loaded.?.quantized_storage) |*storage| {
                storage.deinit();
                entry.host_loaded.?.quantized_storage = null;
                entry.host_loaded.?.quantized = false;
            }
            entry.loaded_bytes = entry.host_loaded.?.tensor.data.len;
        }
        entry.active_tier = if (entry.loaded_bytes == 0 and entry.host_loaded == null) .disk else .host;
        if (entry.loaded_bytes != 0) {
            if (data.tier_cache) |*tier_cache| {
                tier_cache.noteResident(.host, entry.loaded_bytes);
            }
        }
        if (entry.expert_coord) |coord| {
            evictColdExpertsLocked(data, coord, .host);
        }
    }
}

fn prefetchLazyWeightLocked(self: *MlxCompute, entry: *LazyWeightEntry, target_tier: ResidencyTier) !void {
    ensureHostLazyWeightLoadedLocked(self.data, self.run_budget, entry) catch |err| switch (err) {
        error.MemoryBudgetExceeded => {
            if (self.data.tier_cache) |*tier_cache| {
                logSharedCacheDenial("host_prefetch", tier_cache, entry.tensor_ref.name);
            }
            return err;
        },
        else => return err,
    };
    if (target_tier != .backend or entry.loaded != null or entry.loaded_quantized != null) return;
    if (entry.host_loaded == null) {
        const tensor_store = self.data.tensor_store orelse {
            std.log.err("MLX missing tensor store while promoting {s} to backend", .{entry.tensor_ref.name});
            return error.MissingWeight;
        };
        entry.host_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
    }
    if (entry.quantized_storage) |*storage| {
        if (self.data.quant_execution_mode == .device_native and
            shouldUseQuantizedPlaceholder(entry.tensor_ref.name, storage))
        {
            if (storage.packed_expert != null) {
                quant_execution_timing_stats.packed_backend_prefetch_attempts += 1;
            }
            const dims = try quantizedStorageLinearDims(storage);
            const prepared = try mlx_quant.prepareWeightBytesForLinear(storage, dims.in_dim, dims.out_dim, storage.tensor_type);
            const backend_bytes = prepared.bytes.len;
            if (entry.expert_coord) |coord| {
                evictColdExpertsLocked(self.data, coord, .backend);
            } else {
                evictColdNonExpertWeightsToFitLocked(self.data, .backend, backend_bytes, entry);
            }
            if (self.data.tier_cache) |*tier_cache| {
                if (!tier_cache.canFitAdditional(.backend, backend_bytes)) {
                    if (entry.expert_coord) |coord| {
                        evictColdExpertsToFitLocked(self.data, coord, .backend, backend_bytes);
                    } else {
                        evictColdNonExpertWeightsToFitLocked(self.data, .backend, backend_bytes, entry);
                    }
                    if (!tier_cache.canFitAdditional(.backend, backend_bytes)) {
                        tier_cache.noteDenied(.backend, backend_bytes);
                        if (storage.packed_expert != null) {
                            quant_execution_timing_stats.packed_backend_prefetch_denials += 1;
                        }
                        noteSharedCacheDenial(self.run_budget, tier_cache, .backend, backend_bytes);
                        logSharedCacheDenial("backend_prefetch_quantized", tier_cache, entry.tensor_ref.name);
                        return error.MemoryBudgetExceeded;
                    }
                }
            }
            const quant_shape = [_]i32{@intCast(prepared.bytes.len)};
            entry.loaded_quantized = mlx.arrayFromBytes(prepared.bytes, &quant_shape, c.MLX_UINT8);
            entry.active_tier = .backend;
            entry.backend_loaded_bytes = backend_bytes;
            if (storage.packed_expert != null) {
                quant_execution_timing_stats.packed_backend_prefetch_successes += 1;
            }
            if (self.data.tier_cache) |*tier_cache| {
                tier_cache.noteResident(.backend, backend_bytes);
            }
            if (entry.expert_coord) |coord| {
                if (self.data.residency) |*residency| {
                    try residency.noteLoad(coord, self.data.moe_num_experts, entry.projection_mask, backend_bytes);
                }
                evictColdExpertsLocked(self.data, coord, .backend);
            }
            return;
        }
    }

    const host_loaded = entry.host_loaded orelse {
        std.log.err("MLX missing host-loaded tensor for backend promotion: {s}", .{entry.tensor_ref.name});
        return error.MissingWeight;
    };
    const force_f32 = shouldForceF32DenseTensorName(self.data, entry.tensor_ref.name);
    const will_downcast_f16 = !force_f32 and entry.tensor_ref.quantized and host_loaded.tensor.dtype == .f32;
    const backend_bytes = estimateResidentTensorBytesForPromotion(&host_loaded.tensor, force_f32, will_downcast_f16);
    if (entry.expert_coord) |coord| {
        evictColdExpertsLocked(self.data, coord, .backend);
    } else {
        evictColdNonExpertWeightsToFitLocked(self.data, .backend, backend_bytes, entry);
    }
    if (self.data.tier_cache) |*tier_cache| {
        if (!tier_cache.canFitAdditional(.backend, backend_bytes)) {
            if (entry.expert_coord) |coord| {
                evictColdExpertsToFitLocked(self.data, coord, .backend, backend_bytes);
            } else {
                evictColdNonExpertWeightsToFitLocked(self.data, .backend, backend_bytes, entry);
            }
            if (!tier_cache.canFitAdditional(.backend, backend_bytes)) {
                tier_cache.noteDenied(.backend, backend_bytes);
                noteSharedCacheDenial(self.run_budget, tier_cache, .backend, backend_bytes);
                logSharedCacheDenial("backend_prefetch_dense", tier_cache, entry.tensor_ref.name);
                return error.MemoryBudgetExceeded;
            }
        }
    }
    // For quantized weights that MLX has no native Metal kernel for (e.g.
    // Q1_0), the host-loaded tensor has already been dequantized to f32.
    // Pushing 4 bytes/param to the backend for a dense matmul doubles the
    // resident footprint of the model. Downcast to f16 — Metal has tuned
    // f16 matmul and the accuracy loss on a ±1-per-weight source is a
    // non-issue. `force_f32` (Gemma-only today) overrides this.
    var loaded_arr = try mlx.arrayFromTensor(self.allocator, &host_loaded.tensor, force_f32);
    if (will_downcast_f16) {
        var f16_arr = c.mlx_array_new();
        if (mlx.check(c.mlx_astype(&f16_arr, loaded_arr, c.MLX_FLOAT16, self.data.stream))) {
            _ = c.mlx_array_free(loaded_arr);
            loaded_arr = f16_arr;
        } else |_| {
            _ = c.mlx_array_free(f16_arr);
        }
    }
    entry.loaded = loaded_arr;
    entry.active_tier = .backend;
    entry.backend_loaded_bytes = backend_bytes;
    if (self.data.tier_cache) |*tier_cache| {
        tier_cache.noteResident(.backend, backend_bytes);
    }
    maybeDropHostResidencyAfterBackendPromotion(self.data, entry);
    if (entry.expert_coord) |coord| {
        if (self.data.residency) |*residency| {
            try residency.noteLoad(coord, self.data.moe_num_experts, entry.projection_mask, backend_bytes);
        }
        evictColdExpertsLocked(self.data, coord, .backend);
    }
}

fn maybeDropHostResidencyAfterBackendPromotion(data: *WeightStore, entry: *LazyWeightEntry) void {
    if (keepHostAfterBackendPromotionDebug()) return;
    if (entry.expert_coord != null) return;
    if (entry.loaded == null and entry.loaded_quantized == null) return;
    if (entry.quantized_storage != null) return;
    if (entry.host_loaded) |*host_loaded| {
        host_loaded.deinit();
        entry.host_loaded = null;
        if (data.tier_cache) |*tier_cache| {
            tier_cache.noteRelease(.host, entry.loaded_bytes);
        }
        entry.active_tier = .backend;
    }
}

fn denseLazyWeightEphemeral(self: *MlxCompute, entry: *LazyWeightEntry) !CT {
    if (entry.quantized_storage != null) {
        return self.makeArrWithEntry(c.mlx_array_new(), false, entry.tensor_ref.name, null, false, &entry.quantized_storage.?);
    }

    if (entry.host_loaded) |*host_loaded| {
        if (host_loaded.quantized_storage) |*storage| {
            return self.makeArrWithEntry(c.mlx_array_new(), false, entry.tensor_ref.name, null, false, storage);
        }
        const arr = try mlx.arrayFromTensor(self.allocator, &host_loaded.tensor, shouldForceF32DenseTensorName(self.data, entry.tensor_ref.name));
        const arr_cast = try self.castToBf16IfMixed(arr);
        const result = try self.makeArr(arr_cast, true);
        if (entry.expert_coord == null and entry.loaded == null) {
            host_loaded.deinit();
            entry.host_loaded = null;
            if (self.data.tier_cache) |*tier_cache| {
                tier_cache.noteRelease(.host, entry.loaded_bytes);
            }
            entry.active_tier = .disk;
        }
        return result;
    }

    const tensor_store = self.data.tensor_store orelse return error.MissingWeight;
    if (try loadEphemeralQuantizedLazyWeight(self, entry, entry.tensor_ref.name)) |ephemeral_quantized| {
        return ephemeral_quantized;
    }
    var loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
    defer loaded.deinit();
    if (loaded.quantized_storage) |*storage| {
        const owned_storage = storage.*;
        loaded.quantized_storage = null;
        loaded.quantized = false;
        return self.makeArrWithOwnedQuantizedStorage(c.mlx_array_new(), false, entry.tensor_ref.name, owned_storage);
    }
    const arr = try mlx.arrayFromTensor(self.allocator, &loaded.tensor, shouldForceF32DenseTensorName(self.data, entry.tensor_ref.name));
    const arr_cast = try self.castToBf16IfMixed(arr);
    return self.makeArr(arr_cast, true);
}

fn evictColdExpertsLocked(data: *WeightStore, protected: ExpertCoord, tier: ResidencyTier) void {
    while (shouldEvictTierLocked(data, protected.layer_index, tier)) {
        const victim = findEvictionVictimLocked(data, protected, tier) orelse break;
        unloadExpertTierLocked(data, victim, tier);
    }
}

fn evictColdExpertsToFitLocked(data: *WeightStore, protected: ExpertCoord, tier: ResidencyTier, required_bytes: usize) void {
    while (true) {
        const tier_cache = data.tier_cache orelse break;
        if (tier_cache.canFitAdditional(tier, required_bytes)) break;
        const victim = findEvictionVictimLocked(data, protected, tier) orelse break;
        unloadExpertTierLocked(data, victim, tier);
    }
}

fn evictColdNonExpertWeightsToFitLocked(
    data: *WeightStore,
    tier: ResidencyTier,
    required_bytes: usize,
    protected: ?*LazyWeightEntry,
) void {
    while (true) {
        const tier_cache = data.tier_cache orelse break;
        if (tier_cache.canFitAdditional(tier, required_bytes)) break;
        const victim = findNonExpertEvictionVictimLocked(data, tier, protected) orelse break;
        unloadLazyEntryTierLocked(data, victim, tier);
    }
}

fn shouldEvictTierLocked(data: *const WeightStore, layer_index: usize, tier: ResidencyTier) bool {
    if (data.tier_cache) |tier_cache| {
        if (tier_cache.isOverBudget(tier)) return true;
    }
    if (tier == .backend) {
        if (data.residency) |residency| {
            return residency.isOverCapacity(layer_index);
        }
    }
    return false;
}

fn findEvictionVictimLocked(data: *WeightStore, protected: ExpertCoord, tier: ResidencyTier) ?ExpertCoord {
    const residency = data.residency;
    var best: ?ExpertCoord = null;

    var it = data.lazy_weights.iterator();
    while (it.next()) |entry| {
        const coord = entry.value_ptr.expert_coord orelse continue;
        if (tier == .backend and coord.layer_index != protected.layer_index) continue;
        if (coord.layer_index == protected.layer_index and coord.expert_index == protected.expert_index) continue;
        if (!entryResidentAtTier(entry.value_ptr.*, tier)) continue;
        if (!expertCanEvictLocked(data, coord, tier)) continue;
        if (best == null or (residency != null and residency.?.isMoreEvictable(coord, best.?))) {
            best = coord;
        }
    }
    return best;
}

fn findNonExpertEvictionVictimLocked(
    data: *WeightStore,
    tier: ResidencyTier,
    protected: ?*LazyWeightEntry,
) ?*LazyWeightEntry {
    var best: ?*LazyWeightEntry = null;

    var it = data.lazy_weights.iterator();
    while (it.next()) |entry| {
        const candidate = entry.value_ptr;
        if (candidate.expert_coord != null) continue;
        if (protected != null and candidate == protected.?) continue;
        if (!entryResidentAtTier(candidate.*, tier)) continue;
        if (candidate.pin_count != 0) continue;

        if (best == null or
            candidate.last_access_epoch < best.?.last_access_epoch or
            (candidate.last_access_epoch == best.?.last_access_epoch and candidate.backend_loaded_bytes > best.?.backend_loaded_bytes))
        {
            best = candidate;
        }
    }
    return best;
}

fn entryResidentAtTier(entry: LazyWeightEntry, tier: ResidencyTier) bool {
    return switch (tier) {
        .disk => false,
        .host => entry.host_loaded != null or entry.quantized_storage != null,
        .backend => entry.loaded != null or entry.loaded_quantized != null,
    };
}

fn expertCanEvictLocked(data: *const WeightStore, coord: ExpertCoord, tier: ResidencyTier) bool {
    var has_loaded_projection = false;
    var it = data.lazy_weights.iterator();
    while (it.next()) |entry| {
        const entry_coord = entry.value_ptr.expert_coord orelse continue;
        if (entry_coord.layer_index != coord.layer_index or entry_coord.expert_index != coord.expert_index) continue;
        if (!entryResidentAtTier(entry.value_ptr.*, tier)) continue;
        has_loaded_projection = true;
        if (entry.value_ptr.pin_count != 0) return false;
    }
    return has_loaded_projection;
}

fn unloadExpertTierLocked(data: *WeightStore, coord: ExpertCoord, tier: ResidencyTier) void {
    var it = data.lazy_weights.iterator();
    while (it.next()) |entry| {
        const entry_coord = entry.value_ptr.expert_coord orelse continue;
        if (entry_coord.layer_index != coord.layer_index or entry_coord.expert_index != coord.expert_index) continue;
        unloadLazyEntryTierLocked(data, entry.value_ptr, tier);
    }
}

fn unloadLazyEntryTierLocked(data: *WeightStore, entry: *LazyWeightEntry, tier: ResidencyTier) void {
    switch (tier) {
        .backend => if (entry.loaded) |arr| {
            if (entry.loaded_transposed) |transposed| {
                _ = c.mlx_array_free(transposed);
                entry.loaded_transposed = null;
            }
            _ = c.mlx_array_free(arr);
            entry.loaded = null;
            if (data.tier_cache) |*tier_cache| {
                tier_cache.noteRelease(.backend, entry.backend_loaded_bytes);
            }
            entry.active_tier = if (entry.host_loaded != null or entry.quantized_storage != null) .host else .disk;
            if (entry.expert_coord) |coord| {
                if (data.residency) |*residency| {
                    residency.noteUnload(coord, entry.projection_mask, entry.backend_loaded_bytes);
                }
            }
            entry.backend_loaded_bytes = 0;
        } else if (entry.loaded_quantized) |arr| {
            _ = c.mlx_array_free(arr);
            entry.loaded_quantized = null;
            if (data.tier_cache) |*tier_cache| {
                tier_cache.noteRelease(.backend, entry.backend_loaded_bytes);
            }
            entry.active_tier = if (entry.host_loaded != null or entry.quantized_storage != null) .host else .disk;
            if (entry.expert_coord) |coord| {
                if (data.residency) |*residency| {
                    residency.noteUnload(coord, entry.projection_mask, entry.backend_loaded_bytes);
                }
            }
            entry.backend_loaded_bytes = 0;
        },
        .host => if (entry.host_loaded) |*host_loaded| {
            host_loaded.deinit();
            entry.host_loaded = null;
            if (entry.quantized_storage) |*storage| {
                storage.deinit();
                entry.quantized_storage = null;
            }
            if (data.tier_cache) |*tier_cache| {
                tier_cache.noteRelease(.host, entry.loaded_bytes);
            }
            entry.active_tier = if (entry.loaded != null or entry.loaded_quantized != null) .backend else .disk;
        } else if (entry.quantized_storage) |*storage| {
            storage.deinit();
            entry.quantized_storage = null;
            if (data.tier_cache) |*tier_cache| {
                tier_cache.noteRelease(.host, entry.loaded_bytes);
            }
            entry.active_tier = if (entry.loaded != null or entry.loaded_quantized != null) .backend else .disk;
        },
        .disk => {},
    }
}

fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const weight_arr = toArr(weight);
    // Use CPU gather for quantized embeddings: MLX's Metal take kernel
    // cannot operate on packed quantized types (e.g. Q4_K scales are
    // stored as uint8 but the kernel template instantiates float, causing
    // bitwise-op compile errors in utils.h).
    if (forceCpuEmbeddingDebug() or weight_arr.quantized_storage != null) {
        // Fast path: gather directly from quantized storage without full dequantization.
        // This avoids materializing the entire weight (e.g. 262144×8960 Q4_K → 8.75GB f32)
        // and instead dequantizes only the requested rows.
        if (weight_arr.quantized_storage) |storage| {
            const maybe_rows: ?usize = native_compute_mod.quantizedEmbeddingRows(storage, dim) catch null;
            if (maybe_rows) |rows| {
                const output = try self.allocator.alloc(f32, total * dim);
                defer self.allocator.free(output);
                for (ids, 0..) |id, i| {
                    if (id < 0) return error.InvalidTensorShape;
                    const row: usize = @intCast(id);
                    if (row >= rows) return error.InvalidTensorShape;
                    try quant_codec.dequantizeRow(
                        storage.tensor_type,
                        storage.raw_bytes,
                        dim,
                        row,
                        output[i * dim ..][0..dim],
                    );
                }
                const shape = [_]i32{ @intCast(total), @intCast(dim) };
                return self.fromFloat32Shape(output, &shape);
            }
        }
        var maybe_loaded: ?weight_source_mod.LoadedWeight = null;
        defer if (maybe_loaded) |*loaded| loaded.deinit();
        const tensor: *const tensor_mod.Tensor = if (weight_arr.lazy_entry) |entry|
            if (entry.host_loaded) |*loaded|
                &loaded.tensor
            else blk: {
                const tensor_store = self.data.tensor_store orelse return error.MissingWeight;
                maybe_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
                break :blk &maybe_loaded.?.tensor;
            }
        else {
            const owned = try mlx.readFloat32(getArr(weight), self.allocator);
            defer self.allocator.free(owned);
            const output = try self.allocator.alloc(f32, total * dim);
            defer self.allocator.free(output);
            for (ids, 0..) |id, i| {
                const row: usize = @intCast(id);
                const src = owned[row * dim ..][0..dim];
                const dst = output[i * dim ..][0..dim];
                @memcpy(dst, src);
            }
            const shape = [_]i32{ @intCast(total), @intCast(dim) };
            return self.fromFloat32Shape(output, &shape);
        };

        const output = try self.allocator.alloc(f32, total * dim);
        defer self.allocator.free(output);
        try gatherSourceTensorRowsToF32(tensor, ids, dim, output);
        const shape = [_]i32{ @intCast(total), @intCast(dim) };
        return self.fromFloat32Shape(output, &shape);
    }

    // Convert i64 ids to i32 for MLX
    const ids_i32 = try self.allocator.alloc(i32, total);
    defer self.allocator.free(ids_i32);
    for (ids, 0..) |id, i| ids_i32[i] = @intCast(id);

    const shape = [_]i32{@intCast(total)};
    const mlx_ids = mlx.arrayFromInt32(ids_i32, &shape);
    defer _ = c.mlx_array_free(mlx_ids);

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_take_axis(&result, getArr(weight), mlx_ids, 0, s));

    return self.makeArr(result, true);
}

fn embeddingLookupTensorOp(ctx: *anyopaque, weight: CT, ids: CT, total: usize, dim: usize) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const weight_arr = toArr(weight);
    if (forceCpuEmbeddingDebug() or weight_arr.quantized_storage != null) return null;

    const ids_arr = getArr(ids);
    const ids_ndim = c.mlx_array_ndim(ids_arr);
    if (ids_ndim != 1) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(ids_arr, 0))) != total) return null;

    var casted_ids: ?c.mlx_array = null;
    defer {
        if (casted_ids) |arr| _ = c.mlx_array_free(arr);
    }
    const lookup_ids = switch (c.mlx_array_dtype(ids_arr)) {
        c.MLX_INT32, c.MLX_INT64 => ids_arr,
        c.MLX_FLOAT32 => blk: {
            var casted = c.mlx_array_new();
            try mlx.check(c.mlx_astype(&casted, ids_arr, c.MLX_INT32, s));
            casted_ids = casted;
            break :blk casted;
        },
        else => return null,
    };

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_take_axis(&result, getArr(weight), lookup_ids, 0, s));
    errdefer _ = c.mlx_array_free(result);
    if (c.mlx_array_ndim(result) != 2) return error.UnexpectedOutputShape;
    if (@as(usize, @intCast(c.mlx_array_dim(result, 0))) != total) return error.UnexpectedOutputShape;
    if (@as(usize, @intCast(c.mlx_array_dim(result, 1))) != dim) return error.UnexpectedOutputShape;
    return self.makeArr(result, true);
}

fn takeRowsOp(ctx: *anyopaque, request: *const ops.TakeRowsRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    if (request.row_ids.len != request.rows) return error.ShapeMismatch;

    const ids_i32 = try self.allocator.alloc(i32, request.rows);
    defer self.allocator.free(ids_i32);
    for (request.row_ids, 0..) |id, i| ids_i32[i] = @intCast(id);

    const shape = [_]i32{@intCast(request.rows)};
    const mlx_ids = mlx.arrayFromInt32(ids_i32, &shape);
    defer _ = c.mlx_array_free(mlx_ids);

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_take_axis(&result, getArr(request.input), mlx_ids, 0, s));
    return self.makeArr(result, true);
}

fn zeroTensorOp(ctx: *anyopaque, rows: usize, dim: usize) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const shape = [_]i32{ @intCast(rows), @intCast(dim) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_zeros(&result, &shape, shape.len, c.MLX_FLOAT32, self.data.stream));
    return self.makeArr(result, true);
}

fn gatherSourceTensorRowsToF32(
    tensor: *const tensor_mod.Tensor,
    ids: []const i64,
    row_width: usize,
    out: []f32,
) !void {
    if (tensor.shape.len != 2) return error.UnsupportedTensorType;
    if (out.len < ids.len * row_width) return error.ShapeMismatch;
    const row_count: usize = @intCast(tensor.shape[0]);
    if (@as(usize, @intCast(tensor.shape[1])) != row_width) return error.ShapeMismatch;
    switch (tensor.dtype) {
        .f32 => {
            if (tensor.asFloat32IfAligned()) |data| {
                for (ids, 0..) |id, i| {
                    const row: usize = @intCast(id);
                    if (row >= row_count) return error.InvalidTokenId;
                    const src = data[row * row_width ..][0..row_width];
                    const dst = out[i * row_width ..][0..row_width];
                    @memcpy(dst, src);
                }
            } else {
                const src_bytes: [*]const u8 = tensor.data.ptr;
                for (ids, 0..) |id, i| {
                    const row: usize = @intCast(id);
                    if (row >= row_count) return error.InvalidTokenId;
                    for (0..row_width) |col| {
                        const offset = (row * row_width + col) * 4;
                        const bits: u32 = @bitCast([4]u8{
                            src_bytes[offset],
                            src_bytes[offset + 1],
                            src_bytes[offset + 2],
                            src_bytes[offset + 3],
                        });
                        out[i * row_width + col] = @bitCast(bits);
                    }
                }
            }
        },
        .f16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (ids, 0..) |id, i| {
                const row: usize = @intCast(id);
                if (row >= row_count) return error.InvalidTokenId;
                for (0..row_width) |col| {
                    const offset = (row * row_width + col) * 2;
                    const half: f16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                    out[i * row_width + col] = @floatCast(half);
                }
            }
        },
        .bf16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (ids, 0..) |id, i| {
                const row: usize = @intCast(id);
                if (row >= row_count) return error.InvalidTokenId;
                for (0..row_width) |col| {
                    const offset = (row * row_width + col) * 2;
                    const bits: u16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                    const f32_bits: u32 = @as(u32, bits) << 16;
                    out[i * row_width + col] = @bitCast(f32_bits);
                }
            }
        },
        else => return error.UnsupportedTensorType,
    }
}

fn linearOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const weight_arr = toArr(weight);

    if (weight_arr.quantized_storage) |storage| {
        if (try executeQuantizedLinear(self, input, weight, weight_arr, storage, rows, in_dim, out_dim, s)) |linear_out| {
            defer freeTensor(self, linear_out);
            var result = c.mlx_array_new();
            try mlx.check(c.mlx_add(&result, getArr(linear_out), getArr(bias), s));
            return self.makeArr(result, true);
        }
    }

    const weight_ndim = c.mlx_array_ndim(getArr(weight));
    if (weight_ndim != 2) return error.InvalidTensorShape;
    const weight_dim0: usize = @intCast(c.mlx_array_dim(getArr(weight), 0));
    const weight_dim1: usize = @intCast(c.mlx_array_dim(getArr(weight), 1));

    var rhs_owned = false;
    const rhs = if (weight_dim0 == out_dim and weight_dim1 == in_dim) blk: {
        var w_t = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(w_t);
        try mlx.check(c.mlx_transpose(&w_t, getArr(weight), s));
        rhs_owned = true;
        break :blk w_t;
    } else if (weight_dim0 == in_dim and weight_dim1 == out_dim)
        getArr(weight)
    else
        return error.InvalidTensorShape;
    defer {
        if (rhs_owned) _ = c.mlx_array_free(rhs);
    }

    // result = bias + input @ rhs, where rhs is either W or W^T depending on
    // the checkpoint's dense weight layout.
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_addmm(&result, getArr(bias), getArr(input), rhs, 1.0, 1.0, s));

    return self.makeArr(result, true);
}

fn linearLoRAMlxOp(
    ctx: *anyopaque,
    input: CT,
    base_weight: CT,
    bias: CT,
    lora_a: CT,
    lora_b: CT,
    alpha: f32,
    rank: usize,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    _ = .{ rows, in_dim, out_dim };
    const s = self.data.stream;

    const input_arr = getArr(input);
    const base_w_arr = getArr(base_weight);
    const lora_a_arr = getArr(lora_a);
    const lora_b_arr = getArr(lora_b);
    const bias_arr = getArr(bias);

    const scale = alpha / @as(f32, @floatFromInt(rank));

    // base_out = input @ base_w^T + bias  [rows, out_dim]
    var base_w_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(base_w_t);
    try mlx.check(c.mlx_transpose(&base_w_t, base_w_arr, s));
    var base_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(base_out);
    try mlx.check(c.mlx_addmm(&base_out, bias_arr, input_arr, base_w_t, 1.0, 1.0, s));

    // h = input @ lora_a^T  [rows, rank]
    var lora_a_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(lora_a_t);
    try mlx.check(c.mlx_transpose(&lora_a_t, lora_a_arr, s));
    var h = c.mlx_array_new();
    defer _ = c.mlx_array_free(h);
    try mlx.check(c.mlx_matmul(&h, input_arr, lora_a_t, s));

    // lora_out_unscaled = h @ lora_b^T  [rows, out_dim]
    var lora_b_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(lora_b_t);
    try mlx.check(c.mlx_transpose(&lora_b_t, lora_b_arr, s));
    var lora_out_unscaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(lora_out_unscaled);
    try mlx.check(c.mlx_matmul(&lora_out_unscaled, h, lora_b_t, s));

    // lora_out = lora_out_unscaled * scale
    const scale_arr = c.mlx_array_new_float(scale);
    defer _ = c.mlx_array_free(scale_arr);
    var lora_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(lora_out);
    try mlx.check(c.mlx_multiply(&lora_out, lora_out_unscaled, scale_arr, s));

    // out = base_out + lora_out
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_add(&result, base_out, lora_out, s));

    return self.makeArr(result, true);
}

fn layerNormOp(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    _ = dim;

    var normed = c.mlx_array_new();
    try mlx.check(c.mlx_fast_layer_norm(&normed, getArr(input), getArr(gamma), getArr(beta), eps, s));

    // Ensure f32 output
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_astype(&result, normed, c.MLX_FLOAT32, s));
    _ = c.mlx_array_free(normed);

    return self.makeArr(result, true);
}

fn geluOp(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const x = getArr(input);

    if (forceCpuGeluDebug()) {
        const input_host = try mlx.readFloat32(x, self.allocator);
        defer self.allocator.free(input_host);
        activations_mod.gelu(input_host);
        var shape_buf: [2]i32 = undefined;
        const ndim = c.mlx_array_ndim(x);
        if (ndim != 2) return error.InvalidTensorShape;
        shape_buf[0] = @intCast(c.mlx_array_dim(x, 0));
        shape_buf[1] = @intCast(c.mlx_array_dim(x, 1));
        return self.fromFloat32Shape(input_host, &shape_buf);
    }

    // GELU(x) = x * 0.5 * (1 + erf(x / sqrt(2)))
    const sqrt2_inv = c.mlx_array_new_float(1.0 / @sqrt(2.0));
    defer _ = c.mlx_array_free(sqrt2_inv);

    var scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scaled);
    try mlx.check(c.mlx_multiply(&scaled, x, sqrt2_inv, s));

    var erf_val = c.mlx_array_new();
    defer _ = c.mlx_array_free(erf_val);
    try mlx.check(c.mlx_erf(&erf_val, scaled, s));

    const one = c.mlx_array_new_float(1.0);
    defer _ = c.mlx_array_free(one);
    var erf_plus1 = c.mlx_array_new();
    defer _ = c.mlx_array_free(erf_plus1);
    try mlx.check(c.mlx_add(&erf_plus1, erf_val, one, s));

    const half = c.mlx_array_new_float(0.5);
    defer _ = c.mlx_array_free(half);
    var cdf = c.mlx_array_new();
    defer _ = c.mlx_array_free(cdf);
    try mlx.check(c.mlx_multiply(&cdf, erf_plus1, half, s));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_multiply(&result, x, cdf, s));

    return self.makeArr(result, true);
}

fn addOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (forceCpuAddDebug()) {
        const a_host = try mlx.readFloat32(getArr(a), self.allocator);
        defer self.allocator.free(a_host);
        const b_host = try mlx.readFloat32(getArr(b), self.allocator);
        defer self.allocator.free(b_host);
        if (a_host.len != b_host.len) return error.ShapeMismatch;
        const out = try self.allocator.alloc(f32, a_host.len);
        defer self.allocator.free(out);
        for (out, a_host, b_host) |*dst, av, bv| dst.* = av + bv;
        var shape_buf: [2]i32 = undefined;
        const arr = getArr(a);
        if (c.mlx_array_ndim(arr) != 2) return error.InvalidTensorShape;
        shape_buf[0] = @intCast(c.mlx_array_dim(arr, 0));
        shape_buf[1] = @intCast(c.mlx_array_dim(arr, 1));
        return self.fromFloat32Shape(out, &shape_buf);
    }
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_add(&result, getArr(a), getArr(b), self.data.stream));
    return self.makeArr(result, true);
}

fn sdpaOp(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: []const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const H: usize = num_heads * head_dim;

    // Reshape Q,K,V from [batch*seq, H] to [batch, seq, num_heads, head_dim]
    // then transpose to [batch, num_heads, seq, head_dim]
    const qkv_shape = [_]c_int{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) };
    const perm = [_]c_int{ 0, 2, 1, 3 };

    var q_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_r);
    try mlx.check(c.mlx_reshape(&q_r, getArr(q_ct), &qkv_shape, 4, s));
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_r, &perm, 4, s));

    var k_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_r);
    try mlx.check(c.mlx_reshape(&k_r, getArr(k_ct), &qkv_shape, 4, s));
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_r, &perm, 4, s));

    var v_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_r);
    try mlx.check(c.mlx_reshape(&v_r, getArr(v_ct), &qkv_shape, 4, s));
    var v_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_t);
    try mlx.check(c.mlx_transpose_axes(&v_t, v_r, &perm, 4, s));

    // scores = Q @ K^T / sqrt(head_dim)
    const scale_val: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));

    var k_tp = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_tp);
    const tp_axes = [_]c_int{ 0, 1, 3, 2 };
    try mlx.check(c.mlx_transpose_axes(&k_tp, k_t, &tp_axes, 4, s));

    var scores = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores);
    try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));

    const scale_arr = c.mlx_array_new_float(scale_val);
    defer _ = c.mlx_array_free(scale_arr);
    var scores_scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores_scaled);
    try mlx.check(c.mlx_multiply(&scores_scaled, scores, scale_arr, s));

    // Add position bias if provided: bias shape [num_heads, seq_len, seq_len] → broadcast to [batch, num_heads, seq_len, seq_len]
    if (attn_bias_ct) |bias_ct| {
        const bias_arr = getArr(bias_ct);
        var bias_4d = c.mlx_array_new();
        defer _ = c.mlx_array_free(bias_4d);
        const bias_ndim = c.mlx_array_ndim(bias_arr);
        if (bias_ndim == 4) {
            try mlx.check(c.mlx_array_set(&bias_4d, bias_arr));
        } else {
            // Reshape from [num_heads, seq, seq] to [1, num_heads, seq, seq] for broadcast.
            const bias_shape = [_]c_int{ 1, @intCast(num_heads), @intCast(seq_len), @intCast(seq_len) };
            try mlx.check(c.mlx_reshape(&bias_4d, bias_arr, &bias_shape, 4, s));
        }

        var biased = c.mlx_array_new();
        try mlx.check(c.mlx_add(&biased, scores_scaled, bias_4d, s));

        _ = c.mlx_array_free(scores_scaled);
        scores_scaled = biased;
    }

    // Build additive mask: (1 - mask) * -1e9, reshape to [batch, 1, 1, seq_len]
    const mask_i32 = try self.allocator.alloc(i32, mask.len);
    defer self.allocator.free(mask_i32);
    for (mask, 0..) |v, i| mask_i32[i] = @intCast(v);
    const mask_shape_2d = [_]i32{ @intCast(batch), @intCast(seq_len) };
    const mlx_mask = mlx.arrayFromInt32(mask_i32, &mask_shape_2d);
    defer _ = c.mlx_array_free(mlx_mask);

    var mask_float = c.mlx_array_new();
    defer _ = c.mlx_array_free(mask_float);
    try mlx.check(c.mlx_astype(&mask_float, mlx_mask, c.MLX_FLOAT32, s));

    const one = c.mlx_array_new_float(1.0);
    defer _ = c.mlx_array_free(one);
    var inv_mask = c.mlx_array_new();
    defer _ = c.mlx_array_free(inv_mask);
    try mlx.check(c.mlx_subtract(&inv_mask, one, mask_float, s));

    const large_neg = c.mlx_array_new_float(-1e9);
    defer _ = c.mlx_array_free(large_neg);
    var additive_mask = c.mlx_array_new();
    defer _ = c.mlx_array_free(additive_mask);
    try mlx.check(c.mlx_multiply(&additive_mask, inv_mask, large_neg, s));

    const mask_4d_shape = [_]c_int{ @intCast(batch), 1, 1, @intCast(seq_len) };
    var mask_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(mask_4d);
    try mlx.check(c.mlx_reshape(&mask_4d, additive_mask, &mask_4d_shape, 4, s));

    var masked = c.mlx_array_new();
    defer _ = c.mlx_array_free(masked);
    try mlx.check(c.mlx_add(&masked, scores_scaled, mask_4d, s));

    // softmax
    var attn_weights = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_weights);
    try mlx.check(c.mlx_softmax_axis(&attn_weights, masked, -1, true, s));

    // attn_weights @ V
    var attn_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_out);
    try mlx.check(c.mlx_matmul(&attn_out, attn_weights, v_t, s));

    // Transpose back and reshape to [batch*seq, H]
    const perm_back = [_]c_int{ 0, 2, 1, 3 };
    var attn_back = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_back);
    try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &perm_back, 4, s));

    const flat_shape = [_]c_int{ @intCast(batch * seq_len), @intCast(H) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));

    return self.makeArr(result, true);
}

fn windowedSelfAttentionOp(
    ctx: *anyopaque,
    input: CT,
    norm_weight: CT,
    norm_bias: CT,
    qkv_weight: CT,
    qkv_bias: CT,
    proj_weight: CT,
    proj_bias: CT,
    batch: usize,
    height: usize,
    width: usize,
    dim: usize,
    num_heads: usize,
    window_size: usize,
) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const pad_h = (window_size - (height % window_size)) % window_size;
    const pad_w = (window_size - (width % window_size)) % window_size;
    const padded_h = height + pad_h;
    const padded_w = width + pad_w;
    const windows_h = padded_h / window_size;
    const windows_w = padded_w / window_size;
    const window_count = batch * windows_h * windows_w;
    const window_area = window_size * window_size;
    const rows = window_count * window_area;
    const head_dim = dim / num_heads;

    const normed_ct = try layerNormOp(ctx, input, norm_weight, norm_bias, dim, 1e-5);
    defer freeTensor(ctx, normed_ct);

    const input_shape = [_]c_int{ @intCast(batch), @intCast(height), @intCast(width), @intCast(dim) };
    var tokens_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(tokens_4d);
    try mlx.check(c.mlx_reshape(&tokens_4d, getArr(normed_ct), &input_shape, 4, s));

    var padded = tokens_4d;
    var owns_padded = false;
    if (pad_h != 0 or pad_w != 0) {
        const axes = [_]c_int{ 1, 2 };
        const low_pad = [_]c_int{ 0, 0 };
        const high_pad = [_]c_int{ @intCast(pad_h), @intCast(pad_w) };
        const pad_value = c.mlx_array_new_float(0.0);
        defer _ = c.mlx_array_free(pad_value);
        var padded_arr = c.mlx_array_new();
        try mlx.check(c.mlx_pad(&padded_arr, tokens_4d, &axes, axes.len, &low_pad, low_pad.len, &high_pad, high_pad.len, pad_value, "constant", s));
        padded = padded_arr;
        owns_padded = true;
    }
    defer {
        if (owns_padded) _ = c.mlx_array_free(padded);
    }

    const windows_shape = [_]c_int{
        @intCast(batch),
        @intCast(windows_h),
        @intCast(window_size),
        @intCast(windows_w),
        @intCast(window_size),
        @intCast(dim),
    };
    var windows_6d = c.mlx_array_new();
    defer _ = c.mlx_array_free(windows_6d);
    try mlx.check(c.mlx_reshape(&windows_6d, padded, &windows_shape, 6, s));

    const partition_perm = [_]c_int{ 0, 1, 3, 2, 4, 5 };
    var windowed = c.mlx_array_new();
    defer _ = c.mlx_array_free(windowed);
    try mlx.check(c.mlx_transpose_axes(&windowed, windows_6d, &partition_perm, 6, s));

    const flat_shape = [_]c_int{ @intCast(rows), @intCast(dim) };
    var flat_windows = c.mlx_array_new();
    defer _ = c.mlx_array_free(flat_windows);
    try mlx.check(c.mlx_reshape(&flat_windows, windowed, &flat_shape, 2, s));

    const flat_windows_ct = try self.makeArr(flat_windows, false);
    defer freeTensor(ctx, flat_windows_ct);
    const qkv_ct = try linearOp(ctx, flat_windows_ct, qkv_weight, qkv_bias, rows, dim, dim * 3);
    defer freeTensor(ctx, qkv_ct);

    const qkv_shape = [_]c_int{
        @intCast(window_count),
        @intCast(window_area),
        3,
        @intCast(num_heads),
        @intCast(head_dim),
    };
    var qkv_5d = c.mlx_array_new();
    defer _ = c.mlx_array_free(qkv_5d);
    try mlx.check(c.mlx_reshape(&qkv_5d, getArr(qkv_ct), &qkv_shape, 5, s));

    const qkv_strides = [_]c_int{ 1, 1, 1, 1, 1 };
    const q_shape = [_]c_int{ @intCast(window_count), @intCast(window_area), @intCast(num_heads), @intCast(head_dim) };

    const q_starts = [_]c_int{ 0, 0, 0, 0, 0 };
    const q_stops = [_]c_int{ @intCast(window_count), @intCast(window_area), 1, @intCast(num_heads), @intCast(head_dim) };
    var q_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_slice);
    try mlx.check(c.mlx_slice(&q_slice, qkv_5d, &q_starts, 5, &q_stops, 5, &qkv_strides, 5, s));
    var q_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_4d);
    try mlx.check(c.mlx_reshape(&q_4d, q_slice, &q_shape, 4, s));

    const k_starts = [_]c_int{ 0, 0, 1, 0, 0 };
    const k_stops = [_]c_int{ @intCast(window_count), @intCast(window_area), 2, @intCast(num_heads), @intCast(head_dim) };
    var k_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_slice);
    try mlx.check(c.mlx_slice(&k_slice, qkv_5d, &k_starts, 5, &k_stops, 5, &qkv_strides, 5, s));
    var k_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_4d);
    try mlx.check(c.mlx_reshape(&k_4d, k_slice, &q_shape, 4, s));

    const v_starts = [_]c_int{ 0, 0, 2, 0, 0 };
    const v_stops = [_]c_int{ @intCast(window_count), @intCast(window_area), 3, @intCast(num_heads), @intCast(head_dim) };
    var v_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_slice);
    try mlx.check(c.mlx_slice(&v_slice, qkv_5d, &v_starts, 5, &v_stops, 5, &qkv_strides, 5, s));
    var v_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_4d);
    try mlx.check(c.mlx_reshape(&v_4d, v_slice, &q_shape, 4, s));

    const qkv_perm = [_]c_int{ 0, 2, 1, 3 };
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_4d, &qkv_perm, 4, s));
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_4d, &qkv_perm, 4, s));
    var v_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_t);
    try mlx.check(c.mlx_transpose_axes(&v_t, v_4d, &qkv_perm, 4, s));

    const tp_axes = [_]c_int{ 0, 1, 3, 2 };
    var k_tp = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_tp);
    try mlx.check(c.mlx_transpose_axes(&k_tp, k_t, &tp_axes, 4, s));

    var scores = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores);
    try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));

    const scale = c.mlx_array_new_float(1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
    defer _ = c.mlx_array_free(scale);
    var scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scaled);
    try mlx.check(c.mlx_multiply(&scaled, scores, scale, s));

    var attn_weights = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_weights);
    try mlx.check(c.mlx_softmax_axis(&attn_weights, scaled, -1, true, s));

    var attn_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_out);
    try mlx.check(c.mlx_matmul(&attn_out, attn_weights, v_t, s));

    const attn_perm_back = [_]c_int{ 0, 2, 1, 3 };
    var attn_back = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_back);
    try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &attn_perm_back, 4, s));

    var flat_attn = c.mlx_array_new();
    defer _ = c.mlx_array_free(flat_attn);
    try mlx.check(c.mlx_reshape(&flat_attn, attn_back, &flat_shape, 2, s));

    const flat_attn_ct = try self.makeArr(flat_attn, false);
    defer freeTensor(ctx, flat_attn_ct);
    const proj_ct = try linearOp(ctx, flat_attn_ct, proj_weight, proj_bias, rows, dim, dim);
    defer freeTensor(ctx, proj_ct);

    const proj_shape = [_]c_int{
        @intCast(batch),
        @intCast(windows_h),
        @intCast(windows_w),
        @intCast(window_size),
        @intCast(window_size),
        @intCast(dim),
    };
    var proj_6d = c.mlx_array_new();
    defer _ = c.mlx_array_free(proj_6d);
    try mlx.check(c.mlx_reshape(&proj_6d, getArr(proj_ct), &proj_shape, 6, s));

    var merged_perm = c.mlx_array_new();
    defer _ = c.mlx_array_free(merged_perm);
    try mlx.check(c.mlx_transpose_axes(&merged_perm, proj_6d, &partition_perm, 6, s));

    const padded_shape = [_]c_int{ @intCast(batch), @intCast(padded_h), @intCast(padded_w), @intCast(dim) };
    var merged = c.mlx_array_new();
    defer _ = c.mlx_array_free(merged);
    try mlx.check(c.mlx_reshape(&merged, merged_perm, &padded_shape, 4, s));

    var cropped = merged;
    var owns_cropped = false;
    if (pad_h != 0 or pad_w != 0) {
        const crop_starts = [_]c_int{ 0, 0, 0, 0 };
        const crop_stops = [_]c_int{ @intCast(batch), @intCast(height), @intCast(width), @intCast(dim) };
        const crop_strides = [_]c_int{ 1, 1, 1, 1 };
        var cropped_arr = c.mlx_array_new();
        try mlx.check(c.mlx_slice(&cropped_arr, merged, &crop_starts, 4, &crop_stops, 4, &crop_strides, 4, s));
        cropped = cropped_arr;
        owns_cropped = true;
    }
    defer {
        if (owns_cropped) _ = c.mlx_array_free(cropped);
    }

    var contiguous = c.mlx_array_new();
    defer _ = c.mlx_array_free(contiguous);
    try mlx.check(c.mlx_contiguous(&contiguous, cropped, false, s));

    const output_shape = [_]c_int{ @intCast(batch * height * width), @intCast(dim) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, contiguous, &output_shape, 2, s));
    return self.makeArr(result, true);
}

fn channelSelfAttentionOp(
    ctx: *anyopaque,
    input: CT,
    norm_weight: CT,
    norm_bias: CT,
    qkv_weight: CT,
    qkv_bias: CT,
    proj_weight: CT,
    proj_bias: CT,
    batch: usize,
    seq_len: usize,
    dim: usize,
    groups: usize,
) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const channels_per_group = dim / groups;

    const normed_ct = try layerNormOp(ctx, input, norm_weight, norm_bias, dim, 1e-5);
    defer freeTensor(ctx, normed_ct);
    const qkv_ct = try linearOp(ctx, normed_ct, qkv_weight, qkv_bias, batch * seq_len, dim, dim * 3);
    defer freeTensor(ctx, qkv_ct);

    const qkv_shape = [_]c_int{
        @intCast(batch),
        @intCast(seq_len),
        3,
        @intCast(groups),
        @intCast(channels_per_group),
    };
    var qkv_5d = c.mlx_array_new();
    defer _ = c.mlx_array_free(qkv_5d);
    try mlx.check(c.mlx_reshape(&qkv_5d, getArr(qkv_ct), &qkv_shape, 5, s));

    const qkv_strides = [_]c_int{ 1, 1, 1, 1, 1 };
    const qkgc_shape = [_]c_int{
        @intCast(batch),
        @intCast(seq_len),
        @intCast(groups),
        @intCast(channels_per_group),
    };

    const q_starts = [_]c_int{ 0, 0, 0, 0, 0 };
    const q_stops = [_]c_int{ @intCast(batch), @intCast(seq_len), 1, @intCast(groups), @intCast(channels_per_group) };
    var q_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_slice);
    try mlx.check(c.mlx_slice(&q_slice, qkv_5d, &q_starts, 5, &q_stops, 5, &qkv_strides, 5, s));
    var q = c.mlx_array_new();
    defer _ = c.mlx_array_free(q);
    try mlx.check(c.mlx_reshape(&q, q_slice, &qkgc_shape, 4, s));

    const k_starts = [_]c_int{ 0, 0, 1, 0, 0 };
    const k_stops = [_]c_int{ @intCast(batch), @intCast(seq_len), 2, @intCast(groups), @intCast(channels_per_group) };
    var k_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_slice);
    try mlx.check(c.mlx_slice(&k_slice, qkv_5d, &k_starts, 5, &k_stops, 5, &qkv_strides, 5, s));
    var k = c.mlx_array_new();
    defer _ = c.mlx_array_free(k);
    try mlx.check(c.mlx_reshape(&k, k_slice, &qkgc_shape, 4, s));

    const v_starts = [_]c_int{ 0, 0, 2, 0, 0 };
    const v_stops = [_]c_int{ @intCast(batch), @intCast(seq_len), 3, @intCast(groups), @intCast(channels_per_group) };
    var v_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_slice);
    try mlx.check(c.mlx_slice(&v_slice, qkv_5d, &v_starts, 5, &v_stops, 5, &qkv_strides, 5, s));
    var v = c.mlx_array_new();
    defer _ = c.mlx_array_free(v);
    try mlx.check(c.mlx_reshape(&v, v_slice, &qkgc_shape, 4, s));

    const q_perm = [_]c_int{ 0, 2, 3, 1 };
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q, &q_perm, 4, s));

    const k_perm = [_]c_int{ 0, 2, 1, 3 };
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k, &k_perm, 4, s));

    var scores = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores);
    try mlx.check(c.mlx_matmul(&scores, q_t, k_t, s));

    const scale = c.mlx_array_new_float(1.0 / @sqrt(@as(f32, @floatFromInt(seq_len))));
    defer _ = c.mlx_array_free(scale);
    var scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scaled);
    try mlx.check(c.mlx_multiply(&scaled, scores, scale, s));

    var weights = c.mlx_array_new();
    defer _ = c.mlx_array_free(weights);
    try mlx.check(c.mlx_softmax_axis(&weights, scaled, -1, true, s));

    const v_perm = [_]c_int{ 0, 2, 3, 1 };
    var v_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_t);
    try mlx.check(c.mlx_transpose_axes(&v_t, v, &v_perm, 4, s));

    var attended = c.mlx_array_new();
    defer _ = c.mlx_array_free(attended);
    try mlx.check(c.mlx_matmul(&attended, weights, v_t, s));

    const out_perm = [_]c_int{ 0, 3, 1, 2 };
    var out_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(out_t);
    try mlx.check(c.mlx_transpose_axes(&out_t, attended, &out_perm, 4, s));

    const flat_shape = [_]c_int{ @intCast(batch * seq_len), @intCast(dim) };
    var flat = c.mlx_array_new();
    defer _ = c.mlx_array_free(flat);
    try mlx.check(c.mlx_reshape(&flat, out_t, &flat_shape, 2, s));

    const flat_ct = try self.makeArr(flat, false);
    defer freeTensor(ctx, flat_ct);
    return linearOp(ctx, flat_ct, proj_weight, proj_bias, batch * seq_len, dim, dim);
}

fn tokenGridConv2dOp(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    bias: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    const hwc_shape = [_]c_int{ @intCast(batch), @intCast(height), @intCast(width), @intCast(in_channels) };
    var input_hwc = c.mlx_array_new();
    defer _ = c.mlx_array_free(input_hwc);
    try mlx.check(c.mlx_reshape(&input_hwc, getArr(input), &hwc_shape, 4, s));

    const to_nchw = [_]c_int{ 0, 3, 1, 2 };
    var input_nchw = c.mlx_array_new();
    defer _ = c.mlx_array_free(input_nchw);
    try mlx.check(c.mlx_transpose_axes(&input_nchw, input_hwc, &to_nchw, 4, s));

    const input_ct = try self.makeArr(input_nchw, false);
    defer freeTensor(ctx, input_ct);
    const conv_ct = try conv2dOp(ctx, input_ct, weight, bias, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups);
    defer freeTensor(ctx, conv_ct);

    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const to_hwc = [_]c_int{ 0, 2, 3, 1 };
    var out_hwc = c.mlx_array_new();
    defer _ = c.mlx_array_free(out_hwc);
    try mlx.check(c.mlx_transpose_axes(&out_hwc, getArr(conv_ct), &to_hwc, 4, s));

    var contiguous = c.mlx_array_new();
    defer _ = c.mlx_array_free(contiguous);
    try mlx.check(c.mlx_contiguous(&contiguous, out_hwc, false, s));

    const flat_shape = [_]c_int{ @intCast(batch * out_h * out_w), @intCast(out_channels) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, contiguous, &flat_shape, 2, s));
    return self.makeArr(result, true);
}

fn linearNoBiasOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const weight_arr = toArr(weight);

    if (weight_arr.quantized_storage) |storage| {
        if (try executeQuantizedLinear(self, input, weight, weight_arr, storage, rows, in_dim, out_dim, s)) |result| {
            return result;
        }
    }

    if (weight_arr.lazy_entry) |entry| {
        if (entry.loaded == null and canFallbackDenseBudgetPressure(weight_arr.name, entry)) {
            logDenseBudgetFallback(self, "linear_op_cpu_chunked", weight_arr.name);
            return linearNoBiasCpuSourceTensorChunked(self, getArr(input), entry, weight_arr.name, out_dim, in_dim);
        }
    }

    return linearNoBiasDenseMlx(self, input, weight, s);
}

fn selectQuantLinearExecutor(data: *WeightStore) QuantLinearExecutor {
    return switch (data.quant_execution_mode) {
        .prefer_backend_dense => .backend_dense,
        .wrapper_direct_quant => .wrapper_direct_quant,
        .device_native => .wrapper_direct_quant,
    };
}

fn selectQuantLinearExecutorForLinear(
    data: *WeightStore,
    weight: *Arr,
    storage: *QuantizedStorage,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) QuantLinearExecutor {
    if (storage.packed_expert != null) {
        if (data.quant_execution_mode == .device_native) {
            const packed_plan_request: mlx_quant.LinearNoBiasPlanRequest = .{
                .prepared_weight = .{
                    .weight = weight.arr,
                    .quantized_storage = storage,
                    .staged_backend_dense = false,
                    .has_lazy_owner = weight.lazy_entry != null,
                    .packed_expert = true,
                },
                .rows = rows,
                .in_dim = in_dim,
                .out_dim = out_dim,
            };
            return switch (data.native_quant.planLinearNoBias(&packed_plan_request)) {
                .device_native => .device_native,
                else => .wrapper_direct_quant,
            };
        }
        return .wrapper_direct_quant;
    }

    if (data.quant_execution_mode != .device_native) return selectQuantLinearExecutor(data);

    const plan_request: mlx_quant.LinearNoBiasPlanRequest = .{
        .prepared_weight = .{
            .weight = weight.arr,
            .quantized_storage = storage,
            .staged_backend_dense = weight.lazy_entry != null,
            .has_lazy_owner = weight.lazy_entry != null,
            .packed_expert = storage.packed_expert != null,
        },
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
    };

    return switch (data.native_quant.planLinearNoBias(&plan_request)) {
        .device_native => .device_native,
        .backend_dense => .backend_dense,
        .wrapper_direct_quant => .wrapper_direct_quant,
        .unsupported => if (weight.lazy_entry != null) .backend_dense else .wrapper_direct_quant,
    };
}

fn executeQuantizedLinear(
    self: *MlxCompute,
    input: CT,
    weight: CT,
    weight_arr: *Arr,
    storage: *QuantizedStorage,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    s: c.mlx_stream,
) !?CT {
    const executor = selectQuantLinearExecutorForLinear(self.data, weight_arr, storage, rows, in_dim, out_dim);
    const is_packed = storage.packed_expert != null;
    const weight_class = classifyWeightExec(weight_arr.name);
    return switch (executor) {
        .backend_dense => blk: {
            if (weight_arr.lazy_entry) |entry| {
                prefetchLazyWeightLocked(self, entry, .backend) catch |err| switch (err) {
                    error.MemoryBudgetExceeded => {
                        if (canFallbackQuantizedBudgetPressure(weight_arr, storage)) {
                            break :blk try fallbackQuantizedWrapperOnBudgetPressure(
                                self,
                                "backend_dense_prefetch",
                                input,
                                weight_arr,
                                storage,
                                rows,
                                in_dim,
                                out_dim,
                                weight_class,
                                is_packed,
                            );
                        }
                        return err;
                    },
                    else => return err,
                };
            }
            const result = try linearNoBiasDenseMlx(self, input, weight, s);
            recordQuantBackendDenseStats(weight_class);
            break :blk result;
        },
        .ephemeral_dense => try linearNoBiasDenseEphemeral(self, input, weight_arr, rows, in_dim, out_dim, s),
        .wrapper_direct_quant => blk: {
            const result = linearNoBiasQuantizedWrapper(self, input, weight_arr, storage, rows, in_dim, out_dim) catch |err| {
                const src_name = if (storage.source_name) |sn| sn else @as([]const u8, "<none>");
                const row_off: u32 = if (storage.packed_expert) |pe| pe.row_offset else 0;
                std.log.err(
                    "MLX quant wrapper failed for {s}: rows={d} in_dim={d} out_dim={d} shape={any} tensor_type={any} source={s} row_offset={d} err={}",
                    .{ weight_arr.name, rows, in_dim, out_dim, storage.shape, storage.tensor_type, src_name, row_off, err },
                );
                return err;
            };
            recordQuantWrapperStats(weight_class, is_packed);
            break :blk result;
        },
        .device_native => blk: {
            var degrade_to_wrapper = false;
            if (linearNoBiasQuantizedDeviceNative(self, input, weight_arr, storage, rows, in_dim, out_dim, s) catch |err| native_fallback: {
                if (err == error.MemoryBudgetExceeded and canFallbackQuantizedBudgetPressure(weight_arr, storage)) {
                    degrade_to_wrapper = true;
                    break :native_fallback null;
                }
                std.log.err(
                    "MLX native quant failed for {s}: rows={d} in_dim={d} out_dim={d} shape={any} tensor_type={any} err={}",
                    .{ weight_arr.name, rows, in_dim, out_dim, storage.shape, storage.tensor_type, err },
                );
                return err;
            }) |result| {
                recordQuantDeviceNativeStats(weight_class, is_packed);
                break :blk result;
            }
            if (degrade_to_wrapper) {
                break :blk try fallbackQuantizedWrapperOnBudgetPressure(
                    self,
                    "device_native",
                    input,
                    weight_arr,
                    storage,
                    rows,
                    in_dim,
                    out_dim,
                    weight_class,
                    is_packed,
                );
            }
            if (weight_arr.lazy_entry != null) {
                prefetchLazyWeightLocked(self, weight_arr.lazy_entry.?, .backend) catch |err| switch (err) {
                    error.MemoryBudgetExceeded => {
                        if (canFallbackQuantizedBudgetPressure(weight_arr, storage)) {
                            break :blk try fallbackQuantizedWrapperOnBudgetPressure(
                                self,
                                "device_native_backend_dense_prefetch",
                                input,
                                weight_arr,
                                storage,
                                rows,
                                in_dim,
                                out_dim,
                                weight_class,
                                is_packed,
                            );
                        }
                        return err;
                    },
                    else => return err,
                };
                const result = try linearNoBiasDenseMlx(self, input, weight, s);
                recordQuantBackendDenseStats(weight_class);
                break :blk result;
            }
            const result = try linearNoBiasQuantizedWrapper(self, input, weight_arr, storage, rows, in_dim, out_dim);
            recordQuantWrapperStats(weight_class, is_packed);
            break :blk result;
        },
    };
}

fn linearNoBiasPairOp(
    ctx: *anyopaque,
    input: CT,
    weight_a: CT,
    weight_b: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) anyerror!ops.LinearNoBiasPairResult {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr_a = toArr(weight_a);
    const arr_b = toArr(weight_b);
    const storage_a = arr_a.quantized_storage orelse {
        const first = try linearNoBiasOp(ctx, input, weight_a, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, first);
        const second = try linearNoBiasOp(ctx, input, weight_b, rows, in_dim, out_dim);
        return .{ .first = first, .second = second };
    };
    const storage_b = arr_b.quantized_storage orelse {
        const first = try linearNoBiasOp(ctx, input, weight_a, rows, in_dim, out_dim);
        errdefer freeTensor(ctx, first);
        const second = try linearNoBiasOp(ctx, input, weight_b, rows, in_dim, out_dim);
        return .{ .first = first, .second = second };
    };

    if (self.data.quant_execution_mode == .device_native) {
        const plan_request: mlx_quant.LinearNoBiasPairPlanRequest = .{
            .prepared_weight_a = .{
                .weight = arr_a.arr,
                .quantized_storage = storage_a,
                .staged_backend_dense = arr_a.lazy_entry != null,
                .has_lazy_owner = arr_a.lazy_entry != null,
                .packed_expert = storage_a.packed_expert != null,
            },
            .prepared_weight_b = .{
                .weight = arr_b.arr,
                .quantized_storage = storage_b,
                .staged_backend_dense = arr_b.lazy_entry != null,
                .has_lazy_owner = arr_b.lazy_entry != null,
                .packed_expert = storage_b.packed_expert != null,
            },
            .rows = rows,
            .in_dim = in_dim,
            .out_dim = out_dim,
        };
        if (self.data.native_quant.planLinearNoBiasPair(&plan_request) == .device_native) {
            const request: mlx_quant.LinearNoBiasPairRequest = .{
                .input = getArr(input),
                .weight_a = arr_a.arr,
                .weight_b = arr_b.arr,
                .quantized_storage_a = storage_a,
                .quantized_storage_b = storage_b,
                .rows = rows,
                .in_dim = in_dim,
                .out_dim = out_dim,
                .stream = self.data.stream,
            };
            if (self.data.native_quant.linearNoBiasPair(&request) catch |err| blk: {
                if (err == error.MemoryBudgetExceeded) {
                    if (canFallbackQuantizedBudgetPressure(arr_a, storage_a) and
                        canFallbackQuantizedBudgetPressure(arr_b, storage_b))
                    {
                        logQuantBudgetFallback(self, "device_native_pair_first", arr_a);
                        logQuantBudgetFallback(self, "device_native_pair_second", arr_b);
                        break :blk null;
                    }
                }
                return err;
            }) |result| {
                quant_execution_timing_stats.device_native_pair_calls += 1;
                if (storage_a.packed_expert != null and storage_b.packed_expert != null) {
                    quant_execution_timing_stats.device_native_pair_packed_calls += 1;
                }
                return .{
                    .first = try self.makeArr(result.first, true),
                    .second = try self.makeArr(result.second, true),
                };
            }
        }
    }

    const first = try linearNoBiasOp(ctx, input, weight_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linearNoBiasOp(ctx, input, weight_b, rows, in_dim, out_dim);
    return .{ .first = first, .second = second };
}

fn moeLinearNoBiasOp(
    ctx: *anyopaque,
    request: *const ops.MoeLinearNoBiasRequest,
) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (self.data.quant_execution_mode != .device_native) {
        quant_execution_timing_stats.moe_grouped_fail_not_device_native += 1;
        return null;
    }

    const weight_arr = toArr(request.weight);
    const base_storage = weight_arr.quantized_storage orelse {
        quant_execution_timing_stats.moe_grouped_fail_missing_quant_storage += 1;
        return null;
    };
    var reloaded_storage = if (base_storage.packed_expert == null)
        try reloadedPackedExpertStorageFromTensorStore(self, weight_arr.lazy_entry)
    else
        null;
    defer if (reloaded_storage) |*storage| storage.deinit();
    var recovered_storage = if (base_storage.packed_expert == null)
        recoveredPackedExpertStorage(base_storage, weight_arr.lazy_entry, self.data.moe_num_experts)
    else
        null;
    const storage = if (reloaded_storage) |*reloaded| blk: {
        quant_execution_timing_stats.moe_grouped_recovered_packed_metadata += 1;
        break :blk reloaded;
    } else if (recovered_storage) |*recovered| blk: {
        quant_execution_timing_stats.moe_grouped_recovered_packed_metadata += 1;
        break :blk recovered;
    } else base_storage;
    if (storage.packed_expert == null) {
        quant_execution_timing_stats.moe_grouped_fail_not_packed += 1;
        if (quant_execution_timing_stats.moe_grouped_fail_not_packed <= 16) {
            const lazy_name = if (weight_arr.lazy_entry) |entry| entry.tensor_ref.name else "<resident>";
            const source_name = if (weight_arr.lazy_entry) |entry|
                (entry.tensor_ref.source_name orelse entry.tensor_ref.name)
            else
                "<resident>";
            std.log.err("MLX grouped MoE miss not_packed: weight={s} lazy={s} source={s}", .{ weight_arr.name, lazy_name, source_name });
        }
        return null;
    }

    // The grouped kernels index packed expert storage by expert id, so avoid
    // copying selected expert slabs into a temporary host buffer per token.
    const expert_axis = storage.packed_expert.?.expert_axis;
    const can_index_full_storage = expert_axis == 0 or expert_axis == 2;
    var staged_storage = if (can_index_full_storage)
        null
    else
        try stageSelectedPackedMoeStorage(self.allocator, storage, request.expert_ids, request.expert_tile_ids);
    defer if (staged_storage) |*staged| staged.deinit();

    const active_storage = if (staged_storage) |*staged| &staged.storage else storage;
    const active_expert_ids = if (staged_storage) |*staged| staged.expert_ids else request.expert_ids;
    const active_expert_tile_ids = if (staged_storage) |*staged| staged.expert_tile_ids else request.expert_tile_ids;

    const mlx_request: mlx_quant.MoeLinearNoBiasRequest = .{
        .input = getArr(request.input),
        .weight = weight_arr.arr,
        .quantized_storage = active_storage,
        .expert_ids = active_expert_ids,
        .expert_tile_ids = active_expert_tile_ids,
        .tile_row_starts = request.tile_row_starts,
        .tile_row_counts = request.tile_row_counts,
        .rows = request.rows,
        .in_dim = request.in_dim,
        .out_dim = request.out_dim,
        .stream = self.data.stream,
    };

    if (try self.data.native_quant.mulMatId(&mlx_request)) |result| {
        quant_execution_timing_stats.device_native_moe_grouped_calls += 1;
        return self.makeArr(result, true);
    }
    quant_execution_timing_stats.moe_grouped_fail_provider_null += 1;
    return null;
}

fn moeLinearNoBiasPairOp(
    ctx: *anyopaque,
    input: CT,
    expert_ids: []const u32,
    weight_a: CT,
    weight_b: CT,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) anyerror!?ops.MoeLinearNoBiasPairResult {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (self.data.quant_execution_mode != .device_native) return null;

    const arr_a = toArr(weight_a);
    const arr_b = toArr(weight_b);
    const storage_a = arr_a.quantized_storage orelse return null;
    const storage_b = arr_b.quantized_storage orelse return null;
    if (storage_a.packed_expert == null or storage_b.packed_expert == null) return null;

    const request: mlx_quant.MoeLinearNoBiasPairRequest = .{
        .input = getArr(input),
        .weight_a = arr_a.arr,
        .weight_b = arr_b.arr,
        .quantized_storage_a = storage_a,
        .quantized_storage_b = storage_b,
        .expert_ids = expert_ids,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .stream = self.data.stream,
    };
    if (try self.data.native_quant.moeLinearNoBiasPair(&request)) |result| {
        return .{
            .first = try self.makeArr(result.first, true),
            .second = try self.makeArr(result.second, true),
        };
    }
    return null;
}

fn moeScatterAddOp(
    ctx: *anyopaque,
    request: *const ops.MoeScatterAddRequest,
) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    const row_count = request.row_ids.len;
    if (request.row_weights.len != row_count or request.rows != row_count) return error.ShapeMismatch;

    const indices_i32 = try self.allocator.alloc(i32, row_count * request.dim);
    defer self.allocator.free(indices_i32);
    for (request.row_ids, 0..) |row_id, row| {
        const base = row * request.dim;
        for (0..request.dim) |col| indices_i32[base + col] = @intCast(row_id);
    }
    const index_shape = [_]i32{ @intCast(row_count), @intCast(request.dim) };
    const indices_arr = mlx.arrayFromInt32(indices_i32, &index_shape);
    errdefer _ = c.mlx_array_free(indices_arr);

    const weight_data = try self.allocator.dupe(f32, request.row_weights);
    defer self.allocator.free(weight_data);
    const weight_shape = [_]i32{ @intCast(row_count), 1 };
    const weights_arr = mlx.arrayFromFloat32(weight_data, &weight_shape);
    errdefer _ = c.mlx_array_free(weights_arr);

    var weighted = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(weighted);
    try mlx.check(c.mlx_multiply(&weighted, getArr(request.updates), weights_arr, s));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_scatter_add_axis(&result, getArr(request.base), indices_arr, weighted, 0, s));

    _ = c.mlx_array_free(indices_arr);
    _ = c.mlx_array_free(weights_arr);
    _ = c.mlx_array_free(weighted);
    return self.makeArr(result, true);
}

fn moeSelectRoutesOp(
    ctx: *anyopaque,
    logits: CT,
    rows: usize,
    num_experts: usize,
    top_k: usize,
    allocator: std.mem.Allocator,
) anyerror!?ops.MoeRouteSelection {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    if (rows == 0 or top_k == 0 or num_experts == 0) return .{
        .expert_ids = try allocator.alloc(u32, 0),
        .route_weights = try allocator.alloc(f32, 0),
        .rows = rows,
        .top_k = top_k,
    };
    const logits_arr = getArr(logits);
    if (c.mlx_array_ndim(logits_arr) != 2) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(logits_arr, 0))) != rows) {
        std.log.warn("mlx moeSelectRoutes fallback: logits rows {d} != expected {d}", .{
            c.mlx_array_dim(logits_arr, 0),
            rows,
        });
        return null;
    }
    if (@as(usize, @intCast(c.mlx_array_dim(logits_arr, 1))) != num_experts) {
        std.log.warn("mlx moeSelectRoutes fallback: logits experts {d} != expected {d}", .{
            c.mlx_array_dim(logits_arr, 1),
            num_experts,
        });
        return null;
    }

    const start_col = num_experts - top_k;
    const top_idx = try mlx.argpartitionAxis(logits_arr, @intCast(start_col), 1);
    defer _ = c.mlx_array_free(top_idx);

    const starts = [_]c_int{ 0, @intCast(start_col) };
    const stops = [_]c_int{ @intCast(rows), @intCast(num_experts) };
    const strides = [_]c_int{ 1, 1 };
    var top_idx_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(top_idx_slice);
    try mlx.check(c.mlx_slice(&top_idx_slice, top_idx, &starts, 2, &stops, 2, &strides, 2, s));
    var top_idx_contig = c.mlx_array_new();
    defer _ = c.mlx_array_free(top_idx_contig);
    try mlx.check(c.mlx_contiguous(&top_idx_contig, top_idx_slice, false, s));

    const top_vals = try mlx.takeAlongAxis(logits_arr, top_idx_contig, 1);
    defer _ = c.mlx_array_free(top_vals);

    // Softmax on GPU before downloading — avoids CPU softmax and reduces sync work.
    var top_vals_softmax = c.mlx_array_new();
    defer _ = c.mlx_array_free(top_vals_softmax);
    try mlx.check(c.mlx_softmax_axis(&top_vals_softmax, top_vals, -1, true, s));

    var top_vals_contig = c.mlx_array_new();
    defer _ = c.mlx_array_free(top_vals_contig);
    try mlx.check(c.mlx_contiguous(&top_vals_contig, top_vals_softmax, false, s));

    const ids_i32 = try mlx.readInt32(top_idx_contig, allocator);
    defer allocator.free(ids_i32);
    const vals = try mlx.readFloat32(top_vals_contig, allocator);
    defer allocator.free(vals);
    if (ids_i32.len != rows * top_k or vals.len != rows * top_k) {
        std.log.warn(
            "mlx moeSelectRoutes fallback: ids_len={d} vals_len={d} expected={d} top_idx=[{d},{d}] top_vals=[{d},{d}]",
            .{
                ids_i32.len,
                vals.len,
                rows * top_k,
                c.mlx_array_dim(top_idx_slice, 0),
                c.mlx_array_dim(top_idx_slice, 1),
                c.mlx_array_dim(top_vals_softmax, 0),
                c.mlx_array_dim(top_vals_softmax, 1),
            },
        );
        return null;
    }

    const expert_ids = try allocator.alloc(u32, ids_i32.len);
    errdefer allocator.free(expert_ids);
    const route_weights = try allocator.alloc(f32, vals.len);
    errdefer allocator.free(route_weights);

    // Values are already softmax-normalized on GPU. Just copy and sort descending.
    for (0..rows) |row| {
        const base = row * top_k;
        for (0..top_k) |i| {
            expert_ids[base + i] = @intCast(ids_i32[base + i]);
            route_weights[base + i] = vals[base + i];
        }
        // Sort descending by weight within each row's top_k.
        for (0..top_k) |i| {
            const lhs = base + i;
            var best = lhs;
            for ((i + 1)..top_k) |j| {
                const rhs = base + j;
                if (route_weights[rhs] > route_weights[best]) best = rhs;
            }
            if (best != lhs) {
                std.mem.swap(u32, &expert_ids[lhs], &expert_ids[best]);
                std.mem.swap(f32, &route_weights[lhs], &route_weights[best]);
            }
        }
    }

    return .{
        .expert_ids = expert_ids,
        .route_weights = route_weights,
        .rows = rows,
        .top_k = top_k,
    };
}

/// Fused MoE forward: route selection + expert compute + scatter-add on GPU.
/// Eliminates ~30 GPU↔CPU sync round-trips per token from route selection downloads.
fn moeForwardFusedOp(ctx: *anyopaque, request: *const ops.MoeForwardFusedRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    if (self.data.quant_execution_mode != .device_native) return null;

    const total = request.total;
    const top_k = request.top_k;
    const num_experts = request.num_experts;
    const hidden_size = request.hidden_size;
    const inter_size = request.inter_size;
    const batch_size = total * top_k;

    // Extract quantized storage from weight CTs
    const w1_arr_s = toArr(request.w1);
    const w3_arr_s = toArr(request.w3);
    const w2_arr_s = toArr(request.w2);

    const w1_storage = w1_arr_s.quantized_storage orelse return null;
    const w3_storage = w3_arr_s.quantized_storage orelse return null;
    const w2_storage = w2_arr_s.quantized_storage orelse return null;

    if (w1_storage.packed_expert == null or w3_storage.packed_expert == null or w2_storage.packed_expert == null) return null;

    // --- 1. Route selection on GPU (all lazy, no CPU sync) ---

    const logits_arr = getArr(request.router_logits);
    const start_col: c_int = @intCast(num_experts - top_k);

    const top_idx = try mlx.argpartitionAxis(logits_arr, start_col, 1);
    defer _ = c.mlx_array_free(top_idx);

    // Slice to keep only top_k columns: [total, top_k]
    const slice_starts = [_]c_int{ 0, start_col };
    const slice_stops = [_]c_int{ @intCast(total), @intCast(num_experts) };
    const slice_strides = [_]c_int{ 1, 1 };
    var top_idx_slice = c.mlx_array_new();
    defer _ = c.mlx_array_free(top_idx_slice);
    try mlx.check(c.mlx_slice(&top_idx_slice, top_idx, &slice_starts, 2, &slice_stops, 2, &slice_strides, 2, s));

    var expert_ids_2d = c.mlx_array_new();
    defer _ = c.mlx_array_free(expert_ids_2d);
    try mlx.check(c.mlx_contiguous(&expert_ids_2d, top_idx_slice, false, s));

    // Flatten expert_ids to [batch_size] for Metal kernels
    var expert_ids_flat = c.mlx_array_new();
    defer _ = c.mlx_array_free(expert_ids_flat);
    const flat_ids_shape = [_]i32{@intCast(batch_size)};
    try mlx.check(c.mlx_reshape(&expert_ids_flat, expert_ids_2d, &flat_ids_shape, 1, s));

    // Gather top values, softmax for route weights: [total, top_k]
    const top_vals = try mlx.takeAlongAxis(logits_arr, expert_ids_2d, 1);
    defer _ = c.mlx_array_free(top_vals);

    // Build route weights with optional expert output scale
    const route_weights_2d: c.mlx_array = blk: {
        var softmaxed = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(softmaxed);
        try mlx.check(c.mlx_softmax_axis(&softmaxed, top_vals, -1, true, s));

        if (request.expert_scale) |scale_ct| {
            const scale_arr = getArr(scale_ct);
            // Flatten scale tensor to [num_experts] — it may be [1, num_experts] or [num_experts, 1]
            var scale_flat = c.mlx_array_new();
            defer _ = c.mlx_array_free(scale_flat);
            const scale_flat_shape = [_]i32{@intCast(num_experts)};
            try mlx.check(c.mlx_reshape(&scale_flat, scale_arr, &scale_flat_shape, 1, s));
            // Gather per-expert scales for selected experts
            var scales_gathered = c.mlx_array_new();
            defer _ = c.mlx_array_free(scales_gathered);
            try mlx.check(c.mlx_take_axis(&scales_gathered, scale_flat, expert_ids_flat, 0, s));
            var scales_2d = c.mlx_array_new();
            defer _ = c.mlx_array_free(scales_2d);
            const scale_2d_shape = [_]i32{ @intCast(total), @intCast(top_k) };
            try mlx.check(c.mlx_reshape(&scales_2d, scales_gathered, &scale_2d_shape, 2, s));

            var scaled = c.mlx_array_new();
            try mlx.check(c.mlx_multiply(&scaled, softmaxed, scales_2d, s));
            _ = c.mlx_array_free(softmaxed);
            break :blk scaled;
        }
        break :blk softmaxed;
    };
    defer _ = c.mlx_array_free(route_weights_2d);

    // --- 2. Expand input from [total, hidden] to [batch_size, hidden] ---
    // Each token row is duplicated top_k times for its assigned experts.
    const input_arr = getArr(request.input);
    var input_expanded: c.mlx_array = undefined;
    defer _ = c.mlx_array_free(input_expanded);
    {
        var input_3d = c.mlx_array_new();
        defer _ = c.mlx_array_free(input_3d);
        const in_3d_shape = [_]i32{ @intCast(total), 1, @intCast(hidden_size) };
        try mlx.check(c.mlx_reshape(&input_3d, input_arr, &in_3d_shape, 3, s));
        var input_broadcast = c.mlx_array_new();
        defer _ = c.mlx_array_free(input_broadcast);
        const expand_shape = [_]i32{ @intCast(total), @intCast(top_k), @intCast(hidden_size) };
        try mlx.check(c.mlx_broadcast_to(&input_broadcast, input_3d, &expand_shape, 3, s));
        input_expanded = c.mlx_array_new();
        const flat_in_shape = [_]i32{ @intCast(batch_size), @intCast(hidden_size) };
        try mlx.check(c.mlx_reshape(&input_expanded, input_broadcast, &flat_in_shape, 2, s));
    }

    // --- 3. MoE gate (w1) + up (w3) + silu*up + down (w2) on GPU ---

    // Keep fused MoE on full packed storage and pass device-side expert ids.
    // Staging selected experts here copies large slabs from host storage every token.
    var staged_w1: ?StagedPackedMoeStorage = null;
    defer if (staged_w1) |*staged| staged.deinit();
    var staged_w3: ?StagedPackedMoeStorage = null;
    defer if (staged_w3) |*staged| staged.deinit();

    const w1_active_storage = if (staged_w1) |*staged| &staged.storage else w1_storage;
    const w1_active_expert_ids = if (staged_w1) |*staged| staged.expert_ids else &.{};
    const w1_active_expert_ids_arr = if (staged_w1 != null) null else expert_ids_flat;
    const w3_active_storage = if (staged_w3) |*staged| &staged.storage else w3_storage;
    const w3_active_expert_ids = if (staged_w3) |*staged| staged.expert_ids else &.{};
    const w3_active_expert_ids_arr = if (staged_w3 != null) null else expert_ids_flat;

    const gate_result = try self.data.native_quant.mulMatId(&.{
        .input = input_expanded,
        .weight = w1_arr_s.arr,
        .quantized_storage = w1_active_storage,
        .expert_ids = w1_active_expert_ids,
        .expert_ids_arr = w1_active_expert_ids_arr,
        .rows = batch_size,
        .in_dim = hidden_size,
        .out_dim = inter_size,
        .stream = s,
    }) orelse return null;
    defer _ = c.mlx_array_free(gate_result);

    const up_result = try self.data.native_quant.mulMatId(&.{
        .input = input_expanded,
        .weight = w3_arr_s.arr,
        .quantized_storage = w3_active_storage,
        .expert_ids = w3_active_expert_ids,
        .expert_ids_arr = w3_active_expert_ids_arr,
        .rows = batch_size,
        .in_dim = hidden_size,
        .out_dim = inter_size,
        .stream = s,
    }) orelse return null;
    defer _ = c.mlx_array_free(up_result);

    // silu(gate) * up
    var gate_act = c.mlx_array_new();
    defer _ = c.mlx_array_free(gate_act);
    try mlx.check(c.mlx_sigmoid(&gate_act, gate_result, s));
    var silu_gate = c.mlx_array_new();
    defer _ = c.mlx_array_free(silu_gate);
    try mlx.check(c.mlx_multiply(&silu_gate, gate_result, gate_act, s));
    var gated = c.mlx_array_new();
    defer _ = c.mlx_array_free(gated);
    try mlx.check(c.mlx_multiply(&gated, silu_gate, up_result, s));

    // Down projection
    var staged_w2: ?StagedPackedMoeStorage = null;
    defer if (staged_w2) |*staged| staged.deinit();
    const w2_active_storage = if (staged_w2) |*staged| &staged.storage else w2_storage;
    const w2_active_expert_ids = if (staged_w2) |*staged| staged.expert_ids else &.{};
    const w2_active_expert_ids_arr = if (staged_w2 != null) null else expert_ids_flat;

    const down_result = try self.data.native_quant.mulMatId(&.{
        .input = gated,
        .weight = w2_arr_s.arr,
        .quantized_storage = w2_active_storage,
        .expert_ids = w2_active_expert_ids,
        .expert_ids_arr = w2_active_expert_ids_arr,
        .rows = batch_size,
        .in_dim = inter_size,
        .out_dim = hidden_size,
        .stream = s,
    }) orelse return null;
    defer _ = c.mlx_array_free(down_result);

    // --- 4. Weighted sum (scatter-add equivalent) ---
    // Reshape expert output [batch_size, hidden] → [total, top_k, hidden]
    // Multiply by route_weights [total, top_k, 1] and sum along top_k axis
    var expert_out_3d = c.mlx_array_new();
    defer _ = c.mlx_array_free(expert_out_3d);
    const out_3d_shape = [_]i32{ @intCast(total), @intCast(top_k), @intCast(hidden_size) };
    try mlx.check(c.mlx_reshape(&expert_out_3d, down_result, &out_3d_shape, 3, s));

    var weights_3d = c.mlx_array_new();
    defer _ = c.mlx_array_free(weights_3d);
    const w_3d_shape = [_]i32{ @intCast(total), @intCast(top_k), 1 };
    try mlx.check(c.mlx_reshape(&weights_3d, route_weights_2d, &w_3d_shape, 3, s));

    var weighted_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(weighted_out);
    try mlx.check(c.mlx_multiply(&weighted_out, expert_out_3d, weights_3d, s));

    // Sum along top_k axis (axis 1)
    var summed = c.mlx_array_new();
    defer _ = c.mlx_array_free(summed);
    try mlx.check(c.mlx_sum_axis(&summed, weighted_out, 1, false, s));

    // Result is [total, hidden_size]
    var result = c.mlx_array_new();
    const result_shape = [_]i32{ @intCast(total), @intCast(hidden_size) };
    try mlx.check(c.mlx_reshape(&result, summed, &result_shape, 2, s));

    return self.makeArr(result, true);
}

fn runMoeBlockOp(ctx: *anyopaque, request: *const ops.RunMoeBlockRequest) anyerror!?CT {
    return moeForwardFusedOp(ctx, request);
}

fn linearNoBiasDenseEphemeral(
    self: *MlxCompute,
    input: CT,
    weight_arr: *Arr,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    s: c.mlx_stream,
) !CT {
    _ = rows;
    const entry = weight_arr.lazy_entry orelse return error.MissingWeight;
    const tensor_store = self.data.tensor_store orelse return error.MissingWeight;

    var host_reservation: ?run_memory.Reservation = null;
    var backend_reservation: ?run_memory.Reservation = null;
    const temp_bytes = out_dim * in_dim * @sizeOf(f32);
    if (self.run_budget) |run_budget| {
        host_reservation = run_budget.tryReserveWeight(.host, temp_bytes) catch |err| {
            if (err == error.MemoryBudgetExceeded and canFallbackDenseBudgetPressure(weight_arr.name, entry)) {
                logDenseBudgetFallback(self, "dense_ephemeral_host_reservation", weight_arr.name);
                return linearNoBiasCpuSourceTensorChunked(self, getArr(input), entry, weight_arr.name, out_dim, in_dim);
            }
            return err;
        };
        errdefer if (host_reservation) |reservation| run_budget.release(reservation);
        backend_reservation = run_budget.tryReserveWeight(.backend, temp_bytes) catch |err| {
            if (err == error.MemoryBudgetExceeded and canFallbackDenseBudgetPressure(weight_arr.name, entry)) {
                logDenseBudgetFallback(self, "dense_ephemeral_backend_reservation", weight_arr.name);
                return linearNoBiasCpuSourceTensorChunked(self, getArr(input), entry, weight_arr.name, out_dim, in_dim);
            }
            return err;
        };
        errdefer if (backend_reservation) |reservation| run_budget.release(reservation);
    }

    var host_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
    defer host_loaded.deinit();
    if (host_loaded.quantized_storage) |*storage| {
        storage.deinit();
        host_loaded.quantized_storage = null;
        host_loaded.quantized = false;
    }

    const dense_arr = try mlx.arrayFromTensor(self.allocator, &host_loaded.tensor, shouldForceF32DenseTensorName(self.data, entry.tensor_ref.name));
    defer _ = c.mlx_array_free(dense_arr);
    const dense_ct = try self.makeArr(dense_arr, false);
    defer freeTensor(self, dense_ct);

    const result = try linearNoBiasDenseMlx(self, input, dense_ct, s);
    if (self.run_budget) |run_budget| {
        if (backend_reservation) |reservation| run_budget.release(reservation);
        if (host_reservation) |reservation| run_budget.release(reservation);
    }
    return result;
}

fn linearNoBiasQuantizedWrapper(
    self: *MlxCompute,
    input: CT,
    weight_arr: *Arr,
    storage: *QuantizedStorage,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
) !?CT {
    const input_data = try mlx.readFloat32(getArr(input), self.allocator);
    defer self.allocator.free(input_data);
    const output_data = try self.allocator.alloc(f32, rows * out_dim);
    defer self.allocator.free(output_data);
    @memset(output_data, 0.0);

    if (try native_compute_mod.linearNoBiasQuantized(null, storage, input_data, output_data, rows, in_dim, out_dim)) {
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        const result = mlx.arrayFromFloat32(output_data, &shape);
        return self.makeArrWithEntry(result, true, weight_arr.name, null, false, null);
    }
    return null;
}

fn linearNoBiasQuantizedDeviceNative(
    self: *MlxCompute,
    input: CT,
    weight_arr: *Arr,
    storage: *QuantizedStorage,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    s: c.mlx_stream,
) !?CT {
    var cached_view: ?CachedPackedExpertView = null;
    const packed_backend_weight = storage.packed_expert != null and weight_arr.arr.ctx != null;
    if (storage.packed_expert != null and !packed_backend_weight) {
        self.data.prefetch.lock();
        cached_view = try acquirePreparedPackedExpertViewLocked(self.data, storage, in_dim, out_dim);
        self.data.prefetch.unlock();
    }
    defer if (cached_view) |view| {
        self.data.prefetch.lock();
        releasePreparedPackedExpertViewLocked(self.data, view.key);
        self.data.prefetch.unlock();
        if (view.owned_key) self.data.allocator.free(view.key);
    };

    const request: mlx_quant.LinearNoBiasRequest = .{
        .input = getArr(input),
        .weight = weight_arr.arr,
        .quantized_storage = storage,
        .prepared_weight_bytes = if (cached_view) |view| view.bytes else null,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .stream = s,
    };
    if (try self.data.native_quant.linearNoBias(&request)) |result| {
        if (storage.packed_expert != null) {
            if (packed_backend_weight and cached_view == null) {
                quant_execution_timing_stats.device_native_packed_backend_weight_calls += 1;
            } else if (cached_view != null) {
                quant_execution_timing_stats.device_native_packed_prepared_view_calls += 1;
            }
        }
        return self.makeArr(result, true);
    }
    return null;
}

fn linearNoBiasDenseMlx(self: *MlxCompute, input: CT, weight: CT, s: c.mlx_stream) !CT {
    const weight_arr = toArr(weight);
    if (forceCpuAllLinearDebug()) {
        const input_arr = getArr(input);
        const ndim = c.mlx_array_ndim(input_arr);
        if (ndim < 2) return error.InvalidTensorShape;
        const in_dim: usize = @intCast(c.mlx_array_dim(input_arr, @intCast(ndim - 1)));
        var rows: usize = 1;
        var axis: usize = 0;
        while (axis < ndim - 1) : (axis += 1) {
            rows *= @intCast(c.mlx_array_dim(input_arr, @intCast(axis)));
        }
        const out_dim: usize = @intCast(c.mlx_array_dim(getArr(weight), 0));
        if (weight_arr.lazy_entry) |entry| {
            return linearNoBiasCpuSourceTensorChunked(self, input_arr, entry, weight_arr.name, out_dim, in_dim);
        }

        const input_host = try mlx.readFloat32(input_arr, self.allocator);
        defer self.allocator.free(input_host);
        const weight_host = try mlx.readFloat32(getArr(weight), self.allocator);
        defer self.allocator.free(weight_host);
        const output = try self.allocator.alloc(f32, rows * out_dim);
        defer self.allocator.free(output);
        @memset(output, 0.0);
        try self.dispatchSgemmTransB(rows, out_dim, in_dim, 1.0, input_host, weight_host, 0.0, output);
        const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
        return self.fromFloat32Shape(output, &shape);
    }
    if (forceCpuTiedLogitsDebug() and
        (std.mem.eql(u8, weight_arr.name, "lm_head.weight") or
            std.mem.eql(u8, weight_arr.name, "model.embed_tokens.weight") or
            std.mem.eql(u8, weight_arr.name, "wte.weight")))
    {
        const out_dim: usize = @intCast(c.mlx_array_dim(getArr(weight), 0));
        const in_dim: usize = @intCast(c.mlx_array_dim(getArr(weight), 1));
        if (out_dim >= 100_000) {
            if (weight_arr.lazy_entry) |entry| {
                return linearNoBiasCpuSourceTensorChunked(self, getArr(input), entry, weight_arr.name, out_dim, in_dim);
            }
        }
    }
    if (weight_arr.lazy_entry) |entry| {
        if (classifyWeightExec(weight_arr.name) == .attn_proj) {
            quant_execution_timing_stats.attn_dense_lazy_calls += 1;
        }
        if (forceWeightHandleLinearDebug()) {
            var w_t_dbg = c.mlx_array_new();
            defer _ = c.mlx_array_free(w_t_dbg);
            try mlx.check(c.mlx_transpose(&w_t_dbg, getArr(weight), s));
            var result_dbg = c.mlx_array_new();
            try mlx.check(c.mlx_matmul(&result_dbg, getArr(input), w_t_dbg, s));
            return self.makeArr(result_dbg, true);
        }
        const transposed = try ensureLazyWeightTranspose(self, entry, s);
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_matmul(&result, getArr(input), transposed, s));
        return self.makeArr(result, true);
    }

    const w_t = if (weight_arr.name.len > 0)
        try ensureResidentWeightTranspose(self, weight_arr.name, getArr(weight), s)
    else blk: {
        var transposed = c.mlx_array_new();
        errdefer _ = c.mlx_array_free(transposed);
        try mlx.check(c.mlx_transpose(&transposed, getArr(weight), s));
        break :blk transposed;
    };
    defer {
        if (weight_arr.name.len == 0) _ = c.mlx_array_free(w_t);
    }

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_matmul(&result, getArr(input), w_t, s));

    return self.makeArr(result, true);
}

fn dotLocal(a: [*]const f32, b: [*]const f32, len: usize) f32 {
    var sum: f32 = 0.0;
    for (0..len) |i| sum += a[i] * b[i];
    return sum;
}

fn axpyLocal(alpha: f32, x: [*]const f32, y: [*]f32, len: usize) void {
    for (0..len) |i| y[i] += alpha * x[i];
}

fn gqaAttentionCpuFallback(
    self: *MlxCompute,
    q_arr: c.mlx_array,
    k_arr: c.mlx_array,
    v_arr: c.mlx_array,
    attn_bias_ct: ?CT,
    attn_or_mask: ?[]const u8,
    sliding_window: usize,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !CT {
    const Q = try mlx.readFloat32(q_arr, self.allocator);
    defer self.allocator.free(Q);
    const K = try mlx.readFloat32(k_arr, self.allocator);
    defer self.allocator.free(K);
    const V = try mlx.readFloat32(v_arr, self.allocator);
    defer self.allocator.free(V);
    const bias: ?[]f32 = if (attn_bias_ct) |b| try self.computeBackend().toFloat32(b, self.allocator) else null;
    defer if (bias) |owned| self.allocator.free(owned);
    const output = try linalg.flashCausalAttentionHost(
        self.allocator,
        Q,
        K,
        V,
        bias,
        attn_or_mask,
        sliding_window,
        batch,
        q_seq_len,
        kv_seq_len,
        query_position_offset,
        kv_position_offset,
        num_heads,
        num_kv_heads,
        head_dim,
    );
    defer self.allocator.free(output);

    const H_q = std.math.mul(usize, num_heads, head_dim) catch return error.InvalidAttentionShape;
    const shape = [_]i32{ @intCast(batch * q_seq_len), @intCast(H_q) };
    return self.fromFloat32Shape(output, &shape);
}

fn convertTensorRowsToF32Local(
    tensor: *const tensor_mod.Tensor,
    row_start: usize,
    row_count: usize,
    row_width: usize,
    out: []f32,
) !void {
    if (tensor.shape.len != 2) return error.UnsupportedTensorType;
    if (tensor.dtype != .f32 and tensor.dtype != .f16 and tensor.dtype != .bf16) return error.UnsupportedTensorType;
    if (out.len < row_count * row_width) return error.ShapeMismatch;

    switch (tensor.dtype) {
        .f32 => {
            if (tensor.asFloat32IfAligned()) |data| {
                const start = row_start * row_width;
                @memcpy(out[0 .. row_count * row_width], data[start .. start + row_count * row_width]);
            } else {
                const src_bytes: [*]const u8 = tensor.data.ptr;
                for (0..row_count) |row| {
                    const src_row = row_start + row;
                    for (0..row_width) |col| {
                        const offset = (src_row * row_width + col) * 4;
                        const bits: u32 = @bitCast([4]u8{
                            src_bytes[offset],
                            src_bytes[offset + 1],
                            src_bytes[offset + 2],
                            src_bytes[offset + 3],
                        });
                        out[row * row_width + col] = @bitCast(bits);
                    }
                }
            }
        },
        .f16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..row_count) |row| {
                const src_row = row_start + row;
                for (0..row_width) |col| {
                    const offset = (src_row * row_width + col) * 2;
                    const half: f16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                    out[row * row_width + col] = @floatCast(half);
                }
            }
        },
        .bf16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..row_count) |row| {
                const src_row = row_start + row;
                for (0..row_width) |col| {
                    const offset = (src_row * row_width + col) * 2;
                    const bits: u16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                    const f32_bits: u32 = @as(u32, bits) << 16;
                    out[row * row_width + col] = @bitCast(f32_bits);
                }
            }
        },
        else => return error.UnsupportedTensorType,
    }
}

fn linearNoBiasCpuSourceTensorChunked(
    self: *MlxCompute,
    input_arr: c.mlx_array,
    entry: *LazyWeightEntry,
    name: []const u8,
    out_dim: usize,
    in_dim: usize,
) !CT {
    const rows: usize = @intCast(c.mlx_array_dim(input_arr, 0));
    const input = try self.allocator.alloc(f32, rows * in_dim);
    defer self.allocator.free(input);
    for (0..rows) |row| {
        const row_arr = try sliceRows(self, input_arr, row, 1);
        defer _ = c.mlx_array_free(row_arr);
        const row_values = try mlx.readFloat32(row_arr, self.allocator);
        defer self.allocator.free(row_values);
        @memcpy(input[row * in_dim ..][0..in_dim], row_values[0..in_dim]);
    }
    const output = try self.allocator.alloc(f32, rows * out_dim);
    errdefer self.allocator.free(output);
    @memset(output, 0.0);

    var maybe_loaded: ?weight_source_mod.LoadedWeight = null;
    defer if (maybe_loaded) |*loaded| loaded.deinit();
    const tensor: *const tensor_mod.Tensor = if (entry.host_loaded) |*loaded|
        &loaded.tensor
    else blk: {
        const tensor_store = self.data.tensor_store orelse return error.MissingWeight;
        maybe_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
        break :blk &maybe_loaded.?.tensor;
    };

    const target_weight_bytes: usize = 8 * 1024 * 1024;
    const block_rows = @max(@as(usize, 1), @min(out_dim, target_weight_bytes / (@max(@as(usize, 1), in_dim) * @sizeOf(f32))));
    const weight_block = try self.allocator.alloc(f32, block_rows * in_dim);
    defer self.allocator.free(weight_block);
    const output_block = try self.allocator.alloc(f32, rows * block_rows);
    defer self.allocator.free(output_block);

    var row_start: usize = 0;
    while (row_start < out_dim) : (row_start += block_rows) {
        const row_count = @min(block_rows, out_dim - row_start);
        try convertTensorRowsToF32Local(tensor, row_start, row_count, in_dim, weight_block[0 .. row_count * in_dim]);
        @memset(output_block[0 .. rows * row_count], 0.0);
        try self.dispatchSgemmTransB(
            rows,
            row_count,
            in_dim,
            1.0,
            input,
            weight_block[0 .. row_count * in_dim],
            0.0,
            output_block[0 .. rows * row_count],
        );
        for (0..rows) |r| {
            const src = output_block[r * row_count ..][0..row_count];
            const dst = output[r * out_dim + row_start ..][0..row_count];
            @memcpy(dst, src);
        }
    }
    if (debugTiedLogitsShapeEnabled() and
        (std.mem.eql(u8, name, "model.embed_tokens.weight") or std.mem.eql(u8, name, "lm_head.weight")))
    {
        std.log.info("MLX tied logits shape rows={d} in_dim={d} out_dim={d}", .{ rows, in_dim, out_dim });
    }
    if (debugTiedLogitsInputRowsEnabled() and
        rows > 0 and
        (std.mem.eql(u8, name, "model.embed_tokens.weight") or std.mem.eql(u8, name, "lm_head.weight")))
    {
        const limit = @min(in_dim, 8);
        std.debug.print("mlx_tied_input_row0:", .{});
        for (input[0..limit]) |value| std.debug.print(" {d:.6}", .{value});
        std.debug.print("\n", .{});
        const last = (rows - 1) * in_dim;
        std.debug.print("mlx_tied_input_row_last:", .{});
        for (input[last .. last + limit]) |value| std.debug.print(" {d:.6}", .{value});
        std.debug.print("\n", .{});
    }
    if (debugTiedLogitsOutputEnabled() and
        rows > 0 and
        (std.mem.eql(u8, name, "model.embed_tokens.weight") or std.mem.eql(u8, name, "lm_head.weight")))
    {
        debugTopValues("mlx_tied_output_row0", output[0..out_dim]);
        const last = (rows - 1) * out_dim;
        debugTopValues("mlx_tied_output_row_last", output[last .. last + out_dim]);
    }
    if (std.mem.eql(u8, name, "model.embed_tokens.weight") or std.mem.eql(u8, name, "lm_head.weight")) {
        return self.makeHostF32(output, name);
    }
    if (forceFlatTiedLogitsDebug() and
        (std.mem.eql(u8, name, "model.embed_tokens.weight") or std.mem.eql(u8, name, "lm_head.weight")))
    {
        const flat_shape = [_]i32{@intCast(rows * out_dim)};
        return self.fromFloat32Shape(output, &flat_shape);
    }
    const shape = [_]i32{ @intCast(rows), @intCast(out_dim) };
    return self.fromFloat32Shape(output, &shape);
}

fn ensureLazyWeightTranspose(self: *MlxCompute, entry: *LazyWeightEntry, s: c.mlx_stream) !c.mlx_array {
    _ = self;
    if (entry.loaded_transposed) |arr| return arr;
    const loaded = entry.loaded orelse {
        std.log.err("MLX missing loaded backend tensor for transpose: {s}", .{entry.tensor_ref.name});
        return error.MissingWeight;
    };
    var transposed = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(transposed);
    try mlx.check(c.mlx_transpose(&transposed, loaded, s));
    entry.loaded_transposed = transposed;
    return transposed;
}

fn ensureResidentWeightTranspose(
    self: *MlxCompute,
    name: []const u8,
    weight: c.mlx_array,
    s: c.mlx_stream,
) !c.mlx_array {
    if (self.data.resident_transposed_weights.get(name)) |arr| return arr;

    const owned_name = try self.allocator.dupe(u8, name);
    errdefer self.allocator.free(owned_name);

    var transposed = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(transposed);
    try mlx.check(c.mlx_transpose(&transposed, weight, s));

    try self.data.resident_transposed_weights.put(self.allocator, owned_name, transposed);
    return transposed;
}

fn rmsNormOp(ctx: *anyopaque, input: CT, weight: CT, _: usize, eps: f32) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    if (forceCpuRmsNormDebug()) {
        const input_arr = getArr(input);
        if (c.mlx_array_ndim(input_arr) != 2) return error.InvalidTensorShape;
        const rows: usize = @intCast(c.mlx_array_dim(input_arr, 0));
        const dim: usize = @intCast(c.mlx_array_dim(input_arr, 1));
        const input_host = try mlx.readFloat32(input_arr, self.allocator);
        defer self.allocator.free(input_host);
        const weight_host = try mlx.readFloat32(getArr(weight), self.allocator);
        defer self.allocator.free(weight_host);
        activations_mod.rmsNorm(input_host, weight_host, dim, eps);
        const shape = [_]i32{ @intCast(rows), @intCast(dim) };
        return self.fromFloat32Shape(input_host, &shape);
    }

    // MLX has mlx_fast_rms_norm
    var normed = c.mlx_array_new();
    try mlx.check(c.mlx_fast_rms_norm(&normed, getArr(input), getArr(weight), eps, s));

    // Ensure f32 output
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_astype(&result, normed, c.MLX_FLOAT32, s));
    _ = c.mlx_array_free(normed);

    return self.makeArr(result, true);
}

fn reluOp(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const zero = c.mlx_array_new_float(0.0);
    defer _ = c.mlx_array_free(zero);

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_maximum(&result, getArr(input), zero, self.data.stream));
    return self.makeArr(result, true);
}

fn siluOp(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (forceCpuSiluDebug()) {
        const values = try mlx.readFloat32(getArr(input), self.allocator);
        errdefer self.allocator.free(values);
        activations_mod.silu(values);
        var shape_buf: [2]i32 = undefined;
        const arr = getArr(input);
        if (c.mlx_array_ndim(arr) != 2) return error.InvalidTensorShape;
        shape_buf[0] = @intCast(c.mlx_array_dim(arr, 0));
        shape_buf[1] = @intCast(c.mlx_array_dim(arr, 1));
        return self.fromFloat32Shape(values, &shape_buf);
    }
    const s = self.data.stream;
    const x = getArr(input);

    // SiLU(x) = x * sigmoid(x)
    var sig = c.mlx_array_new();
    defer _ = c.mlx_array_free(sig);
    try mlx.check(c.mlx_sigmoid(&sig, x, s));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_multiply(&result, x, sig, s));
    return self.makeArr(result, true);
}

fn quickGeluOp(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const x = getArr(input);

    // quickGELU(x) = x * sigmoid(1.702 * x)
    const coeff = c.mlx_array_new_float(1.702);
    defer _ = c.mlx_array_free(coeff);

    var scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scaled);
    try mlx.check(c.mlx_multiply(&scaled, x, coeff, s));

    var sig = c.mlx_array_new();
    defer _ = c.mlx_array_free(sig);
    try mlx.check(c.mlx_sigmoid(&sig, scaled, s));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_multiply(&result, x, sig, s));
    return self.makeArr(result, true);
}

fn sigmoidOp(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_sigmoid(&result, getArr(input), self.data.stream));
    return self.makeArr(result, true);
}

fn tanhActOp(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_tanh(&result, getArr(input), self.data.stream));
    return self.makeArr(result, true);
}

fn concatOp(ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    _ = total;
    _ = dim_a;
    _ = dim_b;

    const arrays = c.mlx_vector_array_new();
    defer _ = c.mlx_vector_array_free(arrays);
    _ = c.mlx_vector_array_append_value(arrays, getArr(a));
    _ = c.mlx_vector_array_append_value(arrays, getArr(b));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_concatenate_axis(&result, arrays, -1, s));
    return self.makeArr(result, true);
}

fn causalSelfAttentionOp(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    // Reshape Q,K,V from [batch*seq, H] to [batch, num_heads, seq, head_dim]
    const qkv_shape = [_]c_int{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) };
    const perm = [_]c_int{ 0, 2, 1, 3 };

    var q_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_r);
    try mlx.check(c.mlx_reshape(&q_r, getArr(q_ct), &qkv_shape, 4, s));
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_r, &perm, 4, s));

    var k_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_r);
    try mlx.check(c.mlx_reshape(&k_r, getArr(k_ct), &qkv_shape, 4, s));
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_r, &perm, 4, s));

    var v_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_r);
    try mlx.check(c.mlx_reshape(&v_r, getArr(v_ct), &qkv_shape, 4, s));
    var v_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_t);
    try mlx.check(c.mlx_transpose_axes(&v_t, v_r, &perm, 4, s));

    const scale_val: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));

    // Use fused SDPA with causal mask mode when no attention bias is present.
    // When bias is provided, fall back to manual attention since the bias
    // must be combined with the causal mask.
    if (attn_bias_ct == null and !disableFusedSdpaDebug()) {
        var attn_out = c.mlx_array_new();
        defer _ = c.mlx_array_free(attn_out);
        try mlx.check(c.mlx_fast_scaled_dot_product_attention(
            &attn_out,
            q_t,
            k_t,
            v_t,
            scale_val,
            "causal",
            .{ .ctx = null }, // no explicit mask array
            .{ .ctx = null }, // no sinks
            s,
        ));

        // Transpose back and reshape to [batch*seq, H]
        const H = num_heads * head_dim;
        const perm_back = [_]c_int{ 0, 2, 1, 3 };
        var attn_back = c.mlx_array_new();
        defer _ = c.mlx_array_free(attn_back);
        try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &perm_back, 4, s));

        const flat_shape = [_]c_int{ @intCast(batch * seq_len), @intCast(H) };
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));

        return self.makeArr(result, true);
    }

    // Fallback: manual attention with attention bias.
    // scores = Q @ K^T / sqrt(head_dim)
    const tp_axes = [_]c_int{ 0, 1, 3, 2 };

    var k_tp = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_tp);
    try mlx.check(c.mlx_transpose_axes(&k_tp, k_t, &tp_axes, 4, s));

    var scores = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores);
    try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));

    const scale_arr = c.mlx_array_new_float(scale_val);
    defer _ = c.mlx_array_free(scale_arr);
    var scores_scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores_scaled);
    try mlx.check(c.mlx_multiply(&scores_scaled, scores, scale_arr, s));

    // Add position bias: [num_heads, seq, seq] -> [1, num_heads, seq, seq]
    const scores_after_bias = try applyAttnBias(attn_bias_ct, scores_scaled, num_heads, seq_len, seq_len, s);
    defer _ = c.mlx_array_free(scores_after_bias);

    // Apply causal mask: build lower-triangular mask
    var tri = c.mlx_array_new();
    defer _ = c.mlx_array_free(tri);
    try mlx.check(c.mlx_tri(&tri, @intCast(seq_len), @intCast(seq_len), 0, c.MLX_FLOAT32, s));

    // inv_mask = (1 - tri) * -1e9
    const one = c.mlx_array_new_float(1.0);
    defer _ = c.mlx_array_free(one);
    var inv_mask = c.mlx_array_new();
    defer _ = c.mlx_array_free(inv_mask);
    try mlx.check(c.mlx_subtract(&inv_mask, one, tri, s));

    const large_neg = c.mlx_array_new_float(-1e9);
    defer _ = c.mlx_array_free(large_neg);
    var causal_mask = c.mlx_array_new();
    defer _ = c.mlx_array_free(causal_mask);
    try mlx.check(c.mlx_multiply(&causal_mask, inv_mask, large_neg, s));

    var masked = c.mlx_array_new();
    defer _ = c.mlx_array_free(masked);
    try mlx.check(c.mlx_add(&masked, scores_after_bias, causal_mask, s));

    // softmax
    var attn_weights = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_weights);
    try mlx.check(c.mlx_softmax_axis(&attn_weights, masked, -1, true, s));

    // attn_weights @ V
    var attn_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_out);
    try mlx.check(c.mlx_matmul(&attn_out, attn_weights, v_t, s));

    // Transpose back and reshape to [batch*seq, H]
    const H = num_heads * head_dim;
    const perm_back = [_]c_int{ 0, 2, 1, 3 };
    var attn_back = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_back);
    try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &perm_back, 4, s));

    const flat_shape = [_]c_int{ @intCast(batch * seq_len), @intCast(H) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));

    return self.makeArr(result, true);
}

fn crossAttentionOp(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, enc_mask: []const i64, batch: usize, dec_seq: usize, enc_seq: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    // Reshape Q: [batch*dec_seq, H] → [batch, num_heads, dec_seq, head_dim]
    const q_shape = [_]c_int{ @intCast(batch), @intCast(dec_seq), @intCast(num_heads), @intCast(head_dim) };
    const perm = [_]c_int{ 0, 2, 1, 3 };

    var q_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_r);
    try mlx.check(c.mlx_reshape(&q_r, getArr(q_ct), &q_shape, 4, s));
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_r, &perm, 4, s));

    // Reshape K,V: [batch*enc_seq, H] → [batch, num_heads, enc_seq, head_dim]
    const kv_shape = [_]c_int{ @intCast(batch), @intCast(enc_seq), @intCast(num_heads), @intCast(head_dim) };

    var k_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_r);
    try mlx.check(c.mlx_reshape(&k_r, getArr(k_ct), &kv_shape, 4, s));
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_r, &perm, 4, s));

    var v_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_r);
    try mlx.check(c.mlx_reshape(&v_r, getArr(v_ct), &kv_shape, 4, s));
    var v_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_t);
    try mlx.check(c.mlx_transpose_axes(&v_t, v_r, &perm, 4, s));

    // scores = Q @ K^T / sqrt(head_dim)
    const scale_val: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));
    const tp_axes = [_]c_int{ 0, 1, 3, 2 };

    var k_tp = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_tp);
    try mlx.check(c.mlx_transpose_axes(&k_tp, k_t, &tp_axes, 4, s));

    var scores = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores);
    try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));

    const scale_arr = c.mlx_array_new_float(scale_val);
    defer _ = c.mlx_array_free(scale_arr);
    var scores_scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores_scaled);
    try mlx.check(c.mlx_multiply(&scores_scaled, scores, scale_arr, s));

    // Apply encoder mask: [batch, enc_seq] → [batch, 1, 1, enc_seq]
    const mask_i32 = try self.allocator.alloc(i32, enc_mask.len);
    defer self.allocator.free(mask_i32);
    for (enc_mask, 0..) |v, i| mask_i32[i] = @intCast(v);
    const mask_shape = [_]i32{ @intCast(batch), @intCast(enc_seq) };
    const mlx_mask = mlx.arrayFromInt32(mask_i32, &mask_shape);
    defer _ = c.mlx_array_free(mlx_mask);

    var mask_float = c.mlx_array_new();
    defer _ = c.mlx_array_free(mask_float);
    try mlx.check(c.mlx_astype(&mask_float, mlx_mask, c.MLX_FLOAT32, s));

    const one_val = c.mlx_array_new_float(1.0);
    defer _ = c.mlx_array_free(one_val);
    var inv_mask = c.mlx_array_new();
    defer _ = c.mlx_array_free(inv_mask);
    try mlx.check(c.mlx_subtract(&inv_mask, one_val, mask_float, s));

    const large_neg = c.mlx_array_new_float(-1e9);
    defer _ = c.mlx_array_free(large_neg);
    var additive_mask = c.mlx_array_new();
    defer _ = c.mlx_array_free(additive_mask);
    try mlx.check(c.mlx_multiply(&additive_mask, inv_mask, large_neg, s));

    const mask_4d_shape = [_]c_int{ @intCast(batch), 1, 1, @intCast(enc_seq) };
    var mask_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(mask_4d);
    try mlx.check(c.mlx_reshape(&mask_4d, additive_mask, &mask_4d_shape, 4, s));

    var masked = c.mlx_array_new();
    defer _ = c.mlx_array_free(masked);
    try mlx.check(c.mlx_add(&masked, scores_scaled, mask_4d, s));

    // softmax
    var attn_weights = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_weights);
    try mlx.check(c.mlx_softmax_axis(&attn_weights, masked, -1, true, s));

    // attn_weights @ V
    var attn_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_out);
    try mlx.check(c.mlx_matmul(&attn_out, attn_weights, v_t, s));

    // Transpose back and reshape to [batch*dec_seq, H]
    const H = num_heads * head_dim;
    const perm_back = [_]c_int{ 0, 2, 1, 3 };
    var attn_back = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_back);
    try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &perm_back, 4, s));

    const flat_shape = [_]c_int{ @intCast(batch * dec_seq), @intCast(H) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));

    return self.makeArr(result, true);
}

const GQAResult = struct {
    k: c.mlx_array,
    v: c.mlx_array,
    owned: bool,
};

/// Expand K,V heads for GQA. If num_kv_heads == num_heads, returns originals (not owned).
fn expandKVHeads(
    k_t: c.mlx_array,
    v_t: c.mlx_array,
    batch: usize,
    seq_len: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
    s: c.mlx_stream,
) !GQAResult {
    if (num_kv_heads == num_heads) {
        return .{ .k = k_t, .v = v_t, .owned = false };
    }

    const repeats = num_heads / num_kv_heads;
    const tile_shape = [_]c_int{ @intCast(batch), @intCast(num_kv_heads), @intCast(repeats), @intCast(seq_len), @intCast(head_dim) };
    const expand_shape = [_]c_int{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(head_dim) };
    const kv_expand_shape = [_]c_int{ @intCast(batch), @intCast(num_kv_heads), 1, @intCast(seq_len), @intCast(head_dim) };

    var k_5d = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_5d);
    try mlx.check(c.mlx_reshape(&k_5d, k_t, &kv_expand_shape, 5, s));

    var k_broadcast = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_broadcast);
    try mlx.check(c.mlx_broadcast_to(&k_broadcast, k_5d, &tile_shape, 5, s));

    var k_out = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&k_out, k_broadcast, &expand_shape, 4, s));

    var v_5d = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_5d);
    try mlx.check(c.mlx_reshape(&v_5d, v_t, &kv_expand_shape, 5, s));

    var v_broadcast = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_broadcast);
    try mlx.check(c.mlx_broadcast_to(&v_broadcast, v_5d, &tile_shape, 5, s));

    var v_out = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&v_out, v_broadcast, &expand_shape, 4, s));

    return .{ .k = k_out, .v = v_out, .owned = true };
}

/// Apply optional attention bias. Returns a new owned array (always, for uniform cleanup).
fn applyAttnBias(attn_bias_ct: ?CT, scores: c.mlx_array, num_heads: usize, q_len: usize, k_len: usize, s: c.mlx_stream) !c.mlx_array {
    if (attn_bias_ct) |bias_ct| {
        const bias_3d = getArr(bias_ct);
        const bias_shape = [_]c_int{ 1, @intCast(num_heads), @intCast(q_len), @intCast(k_len) };
        var bias_4d = c.mlx_array_new();
        defer _ = c.mlx_array_free(bias_4d);
        try mlx.check(c.mlx_reshape(&bias_4d, bias_3d, &bias_shape, 4, s));

        var result = c.mlx_array_new();
        try mlx.check(c.mlx_add(&result, scores, bias_4d, s));
        return result;
    }
    // No bias — copy the reference (caller will free, but scores is already deferred elsewhere).
    // Return a new reference by adding zero.
    const zero = c.mlx_array_new_float(0.0);
    defer _ = c.mlx_array_free(zero);
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_add(&result, scores, zero, s));
    return result;
}

fn relativePositionBiasOp(ctx: *anyopaque, weight: CT, q_len: usize, k_len: usize, num_heads: usize, num_buckets: usize, max_distance: usize, bidirectional: bool) anyerror!CT {
    // Compute bucket indices on CPU, then gather from MLX weight table.
    // This is simpler than building the bucket logic in MLX ops.
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    const indices = try self.allocator.alloc(i32, q_len * k_len);
    defer self.allocator.free(indices);

    for (0..q_len) |qi| {
        for (0..k_len) |ki| {
            const bucket = linalg.t5RelativePositionBucket(
                @as(i64, @intCast(ki)) - @as(i64, @intCast(qi)),
                num_buckets,
                max_distance,
                bidirectional,
            );
            indices[qi * k_len + ki] = @intCast(bucket);
        }
    }

    // Create MLX array of indices: [q_len * k_len]
    const idx_shape = [_]i32{@intCast(q_len * k_len)};
    const mlx_indices = mlx.arrayFromInt32(indices, &idx_shape);
    defer _ = c.mlx_array_free(mlx_indices);

    // weight is [num_heads, num_buckets]. For each head, gather bias[bucket].
    // Reshape weight to [num_heads * num_buckets], take indices, reshape to [num_heads, q*k]
    // Actually: transpose weight to [num_buckets, num_heads], take along axis 0, then transpose back.
    // Simpler: iterate heads or use take on flattened.

    // Approach: for each head h, take weight[h, indices] → [q_len*k_len]
    // Then stack to get [num_heads, q_len, k_len]
    const w = getArr(weight);

    // Flatten: gather along last axis for each head
    // weight shape: [num_heads, num_buckets]
    // We want output[h, qi, ki] = weight[h, bucket(qi,ki)]
    // = take weight transposed to [num_buckets, num_heads] along axis 0 with indices,
    //   giving [q*k, num_heads], then transpose + reshape.

    var w_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(w_t);
    try mlx.check(c.mlx_transpose(&w_t, w, s));
    // w_t: [num_buckets, num_heads]

    var gathered = c.mlx_array_new();
    defer _ = c.mlx_array_free(gathered);
    try mlx.check(c.mlx_take_axis(&gathered, w_t, mlx_indices, 0, s));
    // gathered: [q_len*k_len, num_heads]

    var gathered_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(gathered_t);
    try mlx.check(c.mlx_transpose(&gathered_t, gathered, s));
    // gathered_t: [num_heads, q_len*k_len]

    const out_shape = [_]c_int{ @intCast(num_heads), @intCast(q_len), @intCast(k_len) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, gathered_t, &out_shape, 3, s));

    return self.makeArr(result, true);
}

fn disentangledRelativeAttentionOp(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, q_r_ct: CT, k_r_ct: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const num_rel = 2 * seq_len - 1;
    const q_scale: f32 = 1.0 / @sqrt(3.0);
    const bias_scale: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)) * 3.0);
    const qkv_shape = [_]c_int{ @intCast(batch), @intCast(seq_len), @intCast(num_heads), @intCast(head_dim) };
    const qkv_perm = [_]c_int{ 0, 2, 1, 3 };
    const rel_shape = [_]c_int{ @intCast(num_rel), @intCast(num_heads), @intCast(head_dim) };
    const rel_q_perm = [_]c_int{ 1, 0, 2 };
    const rel_k_perm = [_]c_int{ 1, 2, 0 };
    const shared_q_rel_shape = [_]c_int{ 1, @intCast(num_heads), @intCast(num_rel), @intCast(head_dim) };
    const shared_k_rel_shape = [_]c_int{ 1, @intCast(num_heads), @intCast(head_dim), @intCast(num_rel) };
    const tp_axes = [_]c_int{ 0, 1, 3, 2 };

    var q_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_r);
    try mlx.check(c.mlx_reshape(&q_r, getArr(q_ct), &qkv_shape, 4, s));
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_r, &qkv_perm, 4, s));

    var k_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_r);
    try mlx.check(c.mlx_reshape(&k_r, getArr(k_ct), &qkv_shape, 4, s));
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_r, &qkv_perm, 4, s));

    var k_tp = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_tp);
    try mlx.check(c.mlx_transpose_axes(&k_tp, k_t, &tp_axes, 4, s));

    var q_rel_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_rel_r);
    try mlx.check(c.mlx_reshape(&q_rel_r, getArr(q_r_ct), &rel_shape, 3, s));
    var q_rel_h = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_rel_h);
    try mlx.check(c.mlx_transpose_axes(&q_rel_h, q_rel_r, &rel_q_perm, 3, s));
    var q_rel = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_rel);
    try mlx.check(c.mlx_reshape(&q_rel, q_rel_h, &shared_q_rel_shape, 4, s));

    var k_rel_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_rel_r);
    try mlx.check(c.mlx_reshape(&k_rel_r, getArr(k_r_ct), &rel_shape, 3, s));
    var k_rel_h = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_rel_h);
    try mlx.check(c.mlx_transpose_axes(&k_rel_h, k_rel_r, &rel_k_perm, 3, s));
    var k_rel = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_rel);
    try mlx.check(c.mlx_reshape(&k_rel, k_rel_h, &shared_k_rel_shape, 4, s));

    var raw_c2p = c.mlx_array_new();
    defer _ = c.mlx_array_free(raw_c2p);
    try mlx.check(c.mlx_matmul(&raw_c2p, q_t, k_rel, s));

    var raw_p2c = c.mlx_array_new();
    defer _ = c.mlx_array_free(raw_p2c);
    try mlx.check(c.mlx_matmul(&raw_p2c, q_rel, k_tp, s));

    const rel_index_data = try self.allocator.alloc(i32, seq_len * seq_len);
    defer self.allocator.free(rel_index_data);
    for (0..seq_len) |qi| {
        for (0..seq_len) |ki| {
            const rel_idx = @as(i64, @intCast(qi)) - @as(i64, @intCast(ki)) + @as(i64, @intCast(seq_len - 1));
            rel_index_data[qi * seq_len + ki] = @intCast(rel_idx);
        }
    }
    const rel_index_shape = [_]i32{ 1, 1, @intCast(seq_len), @intCast(seq_len) };
    const rel_index_base = mlx.arrayFromInt32(rel_index_data, &rel_index_shape);
    defer _ = c.mlx_array_free(rel_index_base);
    const rel_index_broadcast_shape = [_]c_int{ @intCast(batch), @intCast(num_heads), @intCast(seq_len), @intCast(seq_len) };
    var rel_index = c.mlx_array_new();
    defer _ = c.mlx_array_free(rel_index);
    try mlx.check(c.mlx_broadcast_to(&rel_index, rel_index_base, &rel_index_broadcast_shape, 4, s));

    const c2p = try mlx.takeAlongAxis(raw_c2p, rel_index, 3);
    defer _ = c.mlx_array_free(c2p);

    const p2c = try mlx.takeAlongAxis(raw_p2c, rel_index, 2);
    defer _ = c.mlx_array_free(p2c);

    var bias_sum = c.mlx_array_new();
    defer _ = c.mlx_array_free(bias_sum);
    try mlx.check(c.mlx_add(&bias_sum, c2p, p2c, s));

    const bias_scale_arr = c.mlx_array_new_float(bias_scale);
    defer _ = c.mlx_array_free(bias_scale_arr);
    var bias_arr = c.mlx_array_new();
    try mlx.check(c.mlx_multiply(&bias_arr, bias_sum, bias_scale_arr, s));
    const bias_ct = try self.makeArr(bias_arr, true);
    defer freeTensor(ctx, bias_ct);

    const q_scale_arr = c.mlx_array_new_float(q_scale);
    defer _ = c.mlx_array_free(q_scale_arr);
    var scaled_q_arr = c.mlx_array_new();
    try mlx.check(c.mlx_multiply(&scaled_q_arr, getArr(q_ct), q_scale_arr, s));
    const scaled_q_ct = try self.makeArr(scaled_q_arr, true);
    defer freeTensor(ctx, scaled_q_ct);

    return sdpaOp(ctx, scaled_q_ct, k_ct, v_ct, mask, bias_ct, batch, seq_len, num_heads, head_dim);
}

fn multiplyOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (forceCpuMultiplyDebug()) {
        const a_host = try mlx.readFloat32(getArr(a), self.allocator);
        defer self.allocator.free(a_host);
        const b_host = try mlx.readFloat32(getArr(b), self.allocator);
        defer self.allocator.free(b_host);
        if (a_host.len != b_host.len) return error.ShapeMismatch;
        const out = try self.allocator.alloc(f32, a_host.len);
        defer self.allocator.free(out);
        for (out, a_host, b_host) |*dst, av, bv| dst.* = av * bv;
        var shape_buf: [2]i32 = undefined;
        const arr = getArr(a);
        if (c.mlx_array_ndim(arr) != 2) return error.InvalidTensorShape;
        shape_buf[0] = @intCast(c.mlx_array_dim(arr, 0));
        shape_buf[1] = @intCast(c.mlx_array_dim(arr, 1));
        return self.fromFloat32Shape(out, &shape_buf);
    }
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_multiply(&result, getArr(a), getArr(b), self.data.stream));
    return self.makeArr(result, true);
}

fn conv1dOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, time_steps: usize, kernel_size: usize, stride_val: usize, padding_val: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    if (forceCpuConv1dDebug()) {
        const in_data = try mlx.readFloat32(getArr(input), self.allocator);
        defer self.allocator.free(in_data);
        const w_data = try mlx.readFloat32(getArr(weight), self.allocator);
        defer self.allocator.free(w_data);
        const b_data = try mlx.readFloat32(getArr(bias), self.allocator);
        defer self.allocator.free(b_data);

        const out_time = (time_steps + 2 * padding_val - kernel_size) / stride_val + 1;
        const output = try self.allocator.alloc(f32, batch * out_channels * out_time);
        defer self.allocator.free(output);

        for (0..batch) |b| {
            for (0..out_channels) |oc| {
                const bias_val = b_data[oc];
                const out_base = (b * out_channels + oc) * out_time;
                for (0..out_time) |t| {
                    output[out_base + t] = bias_val;
                }
            }
        }

        for (0..batch) |b| {
            for (0..out_channels) |oc| {
                for (0..out_time) |t| {
                    var sum: f32 = 0.0;
                    for (0..in_channels) |ic| {
                        for (0..kernel_size) |k| {
                            const in_t_signed: i64 = @as(i64, @intCast(t * stride_val + k)) - @as(i64, @intCast(padding_val));
                            if (in_t_signed >= 0 and in_t_signed < @as(i64, @intCast(time_steps))) {
                                const in_t: usize = @intCast(in_t_signed);
                                const in_idx = (b * in_channels + ic) * time_steps + in_t;
                                const w_idx = (oc * in_channels + ic) * kernel_size + k;
                                sum += in_data[in_idx] * w_data[w_idx];
                            }
                        }
                    }
                    output[(b * out_channels + oc) * out_time + t] += sum;
                }
            }
        }

        const shape = [_]i32{ @intCast(batch), @intCast(out_channels), @intCast(out_time) };
        return self.fromFloat32Shape(output, &shape);
    }
    // Input is [batch, in_channels, time] (channels-first, PyTorch convention)
    // MLX conv1d expects [batch, time, in_channels] (channels-last)
    // Transpose input: [batch, in_ch, time] → [batch, time, in_ch]
    const perm_in = [_]c_int{ 0, 2, 1 };
    var input_cl = c.mlx_array_new();
    defer _ = c.mlx_array_free(input_cl);
    try mlx.check(c.mlx_transpose_axes(&input_cl, getArr(input), &perm_in, 3, s));

    // MLX weight for conv1d: [out_channels, kernel, in_channels]
    // PyTorch weight: [out_channels, in_channels, kernel]
    // Transpose: [out_ch, in_ch, kernel] → [out_ch, kernel, in_ch]
    const perm_w = [_]c_int{ 0, 2, 1 };
    var weight_cl = c.mlx_array_new();
    defer _ = c.mlx_array_free(weight_cl);
    try mlx.check(c.mlx_transpose_axes(&weight_cl, getArr(weight), &perm_w, 3, s));

    // Run conv1d
    var conv_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(conv_out);
    try mlx.check(c.mlx_conv1d(&conv_out, input_cl, weight_cl, @intCast(stride_val), @intCast(padding_val), 1, 1, s));

    // Add bias: conv_out is [batch, out_time, out_channels], bias is [out_channels]
    var biased = c.mlx_array_new();
    try mlx.check(c.mlx_add(&biased, conv_out, getArr(bias), s));

    // Transpose back to channels-first: [batch, out_time, out_ch] → [batch, out_ch, out_time].
    // Materialize a contiguous buffer so later toFloat32 reads see logical order.
    const perm_out = [_]c_int{ 0, 2, 1 };
    var transposed = c.mlx_array_new();
    defer _ = c.mlx_array_free(transposed);
    try mlx.check(c.mlx_transpose_axes(&transposed, biased, &perm_out, 3, s));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_contiguous(&result, transposed, false, s));
    _ = c.mlx_array_free(biased);
    return self.makeArr(result, true);
}

fn conv2dOp(ctx: *anyopaque, input: CT, weight: CT, bias: CT, batch: usize, in_channels: usize, out_channels: usize, height: usize, width: usize, kernel_h: usize, kernel_w: usize, stride_h: usize, stride_w: usize, padding_h: usize, padding_w: usize, groups: usize) anyerror!CT {
    _ = batch;
    _ = in_channels;
    _ = out_channels;
    _ = height;
    _ = width;
    _ = kernel_h;
    _ = kernel_w;
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    const perm_in = [_]c_int{ 0, 2, 3, 1 };
    var input_cl = c.mlx_array_new();
    defer _ = c.mlx_array_free(input_cl);
    try mlx.check(c.mlx_transpose_axes(&input_cl, getArr(input), &perm_in, 4, s));

    const perm_w = [_]c_int{ 0, 2, 3, 1 };
    var weight_cl = c.mlx_array_new();
    defer _ = c.mlx_array_free(weight_cl);
    try mlx.check(c.mlx_transpose_axes(&weight_cl, getArr(weight), &perm_w, 4, s));

    var conv_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(conv_out);
    try mlx.check(c.mlx_conv2d(
        &conv_out,
        input_cl,
        weight_cl,
        @intCast(stride_h),
        @intCast(stride_w),
        @intCast(padding_h),
        @intCast(padding_w),
        1,
        1,
        @intCast(groups),
        s,
    ));

    var biased = c.mlx_array_new();
    try mlx.check(c.mlx_add(&biased, conv_out, getArr(bias), s));

    const perm_out = [_]c_int{ 0, 3, 1, 2 };
    var transposed = c.mlx_array_new();
    defer _ = c.mlx_array_free(transposed);
    try mlx.check(c.mlx_transpose_axes(&transposed, biased, &perm_out, 4, s));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_contiguous(&result, transposed, false, s));
    _ = c.mlx_array_free(biased);
    return self.makeArr(result, true);
}

fn ropeOp(ctx: *anyopaque, input: CT, seq_len: usize, head_dim: usize, rope_dim: usize, theta: f32, freq_scale: f32, position_offset: usize, consecutive_pairs: bool) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const total_tokens: usize = @intCast(c.mlx_array_dim(getArr(input), 0));
    const positions = try self.allocator.alloc(f32, total_tokens);
    defer self.allocator.free(positions);
    for (0..total_tokens) |i| {
        positions[i] = @floatFromInt(position_offset + (i % seq_len));
    }
    return ropeWithPositions(self, input, head_dim, rope_dim, theta, freq_scale, positions, consecutive_pairs);
}

fn ropePerItemOp(
    ctx: *anyopaque,
    input: CT,
    batch: usize,
    max_seq_len: usize,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    query_lengths: []const usize,
    position_offsets: []const usize,
    consecutive_pairs: bool,
) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (query_lengths.len != batch or position_offsets.len != batch) return error.InvalidRoPEInput;

    const total_tokens = batch * max_seq_len;
    const positions = try self.allocator.alloc(f32, total_tokens);
    defer self.allocator.free(positions);
    for (0..batch) |b| {
        if (query_lengths[b] > max_seq_len) return error.InvalidRoPEInput;
        for (0..max_seq_len) |pos| {
            positions[b * max_seq_len + pos] = @floatFromInt(if (pos < query_lengths[b]) position_offsets[b] + pos else 0);
        }
    }
    return ropeWithPositions(self, input, head_dim, rope_dim, theta, freq_scale, positions, consecutive_pairs);
}

fn ropeWithPositions(
    self: *MlxCompute,
    input: CT,
    head_dim: usize,
    rope_dim: usize,
    theta: f32,
    freq_scale: f32,
    positions: []const f32,
    consecutive_pairs: bool,
) !CT {
    if (forceCpuRopeDebug()) {
        const input_data = try mlx.readFloat32(getArr(input), self.allocator);
        defer self.allocator.free(input_data);
        const output = try self.allocator.dupe(f32, input_data);
        defer self.allocator.free(output);

        const total_rows = positions.len;
        if (total_rows == 0 or output.len % total_rows != 0) return error.InvalidRoPEInput;
        const total_dim_local = output.len / total_rows;
        if (total_dim_local % head_dim != 0) return error.InvalidRoPEInput;
        const num_heads = total_dim_local / head_dim;
        const total_chunks = total_rows * num_heads;
        const pos_expanded = try self.allocator.alloc(usize, total_chunks);
        defer self.allocator.free(pos_expanded);
        for (0..total_rows) |row| {
            const pos: usize = @intFromFloat(positions[row]);
            const base = row * num_heads;
            for (0..num_heads) |h| pos_expanded[base + h] = pos;
        }
        linalg.ropeCore(output, pos_expanded, head_dim, rope_dim, theta, freq_scale, consecutive_pairs);
        const shape = [_]i32{ @intCast(total_rows), @intCast(total_dim_local) };
        return self.fromFloat32Shape(output, &shape);
    }
    const s = self.data.stream;
    const x = getArr(input);

    const ndim = c.mlx_array_ndim(x);
    if (ndim != 2) return error.InvalidRoPEInput;

    const total_tokens: usize = @intCast(c.mlx_array_dim(x, 0));
    const total_dim: usize = @intCast(c.mlx_array_dim(x, 1));
    if (positions.len != total_tokens) return error.InvalidRoPEInput;
    if (total_dim % head_dim != 0) return error.InvalidRoPEInput;
    const num_heads = total_dim / head_dim;
    const rope_half = rope_dim / 2;
    const head_half = head_dim / 2;
    const partial = rope_dim < head_dim;

    const pos_shape = [_]i32{@intCast(total_tokens)};
    const mlx_pos = mlx.arrayFromFloat32(positions, &pos_shape);
    defer _ = c.mlx_array_free(mlx_pos);

    // Compute inverse frequencies using rope_dim (not head_dim).
    const inv_freq = try self.allocator.alloc(f32, rope_half);
    defer self.allocator.free(inv_freq);
    const rope_dim_f: f32 = @floatFromInt(rope_dim);
    for (0..rope_half) |j| {
        inv_freq[j] = 1.0 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / rope_dim_f);
    }

    const freq_shape = [_]i32{@intCast(rope_half)};
    const mlx_freq = mlx.arrayFromFloat32(inv_freq, &freq_shape);
    defer _ = c.mlx_array_free(mlx_freq);

    const pos_shape_2d = [_]c_int{ @intCast(total_tokens), 1 };
    var pos_col = c.mlx_array_new();
    defer _ = c.mlx_array_free(pos_col);
    try mlx.check(c.mlx_reshape(&pos_col, mlx_pos, &pos_shape_2d, 2, s));

    const freq_scale_arr = c.mlx_array_new_float(freq_scale);
    defer _ = c.mlx_array_free(freq_scale_arr);
    var scaled_pos_col = c.mlx_array_new();
    defer _ = c.mlx_array_free(scaled_pos_col);
    try mlx.check(c.mlx_multiply(&scaled_pos_col, pos_col, freq_scale_arr, s));

    const freq_shape_2d = [_]c_int{ 1, @intCast(rope_half) };
    var freq_row = c.mlx_array_new();
    defer _ = c.mlx_array_free(freq_row);
    try mlx.check(c.mlx_reshape(&freq_row, mlx_freq, &freq_shape_2d, 2, s));

    var angles = c.mlx_array_new();
    defer _ = c.mlx_array_free(angles);
    try mlx.check(c.mlx_multiply(&angles, scaled_pos_col, freq_row, s));

    var cos_vals = c.mlx_array_new();
    defer _ = c.mlx_array_free(cos_vals);
    try mlx.check(c.mlx_cos(&cos_vals, angles, s));

    var sin_vals = c.mlx_array_new();
    defer _ = c.mlx_array_free(sin_vals);
    try mlx.check(c.mlx_sin(&sin_vals, angles, s));

    if (consecutive_pairs) {
        // Reshape to [tokens, heads, head_dim] to handle partial rotation.
        const shape_3d = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(head_dim) };
        var reshaped_3d = c.mlx_array_new();
        defer _ = c.mlx_array_free(reshaped_3d);
        try mlx.check(c.mlx_reshape(&reshaped_3d, x, &shape_3d, 3, s));

        // Slice out the rotatable portion: first rope_dim elements of each head.
        var rope_part = reshaped_3d;
        var rope_part_owned = false;
        if (partial) {
            const rp_starts = [_]c_int{ 0, 0, 0 };
            const rp_stops = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(rope_dim) };
            const rp_strides = [_]c_int{ 1, 1, 1 };
            rope_part = c.mlx_array_new();
            rope_part_owned = true;
            try mlx.check(c.mlx_slice(&rope_part, reshaped_3d, &rp_starts, 3, &rp_stops, 3, &rp_strides, 3, s));
        }
        defer if (rope_part_owned) {
            _ = c.mlx_array_free(rope_part);
        };

        // Reshape rope portion to [tokens, heads, rope_half, 2] for pair rotation.
        const shape_4d = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(rope_half), 2 };
        var reshaped_4d = c.mlx_array_new();
        defer _ = c.mlx_array_free(reshaped_4d);
        try mlx.check(c.mlx_reshape(&reshaped_4d, rope_part, &shape_4d, 4, s));

        const cs_shape_4d = [_]c_int{ @intCast(total_tokens), 1, @intCast(rope_half), 1 };
        var cos_4d = c.mlx_array_new();
        defer _ = c.mlx_array_free(cos_4d);
        try mlx.check(c.mlx_reshape(&cos_4d, cos_vals, &cs_shape_4d, 4, s));
        var sin_4d = c.mlx_array_new();
        defer _ = c.mlx_array_free(sin_4d);
        try mlx.check(c.mlx_reshape(&sin_4d, sin_vals, &cs_shape_4d, 4, s));

        const starts_0 = [_]c_int{ 0, 0, 0, 0 };
        const stops_0 = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(rope_half), 1 };
        const strides_4d = [_]c_int{ 1, 1, 1, 1 };
        var x0 = c.mlx_array_new();
        defer _ = c.mlx_array_free(x0);
        try mlx.check(c.mlx_slice(&x0, reshaped_4d, &starts_0, 4, &stops_0, 4, &strides_4d, 4, s));

        const starts_1 = [_]c_int{ 0, 0, 0, 1 };
        const stops_1 = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(rope_half), 2 };
        var x1 = c.mlx_array_new();
        defer _ = c.mlx_array_free(x1);
        try mlx.check(c.mlx_slice(&x1, reshaped_4d, &starts_1, 4, &stops_1, 4, &strides_4d, 4, s));

        var x0_cos = c.mlx_array_new();
        defer _ = c.mlx_array_free(x0_cos);
        try mlx.check(c.mlx_multiply(&x0_cos, x0, cos_4d, s));
        var x1_sin = c.mlx_array_new();
        defer _ = c.mlx_array_free(x1_sin);
        try mlx.check(c.mlx_multiply(&x1_sin, x1, sin_4d, s));
        var out0 = c.mlx_array_new();
        defer _ = c.mlx_array_free(out0);
        try mlx.check(c.mlx_subtract(&out0, x0_cos, x1_sin, s));

        var x0_sin = c.mlx_array_new();
        defer _ = c.mlx_array_free(x0_sin);
        try mlx.check(c.mlx_multiply(&x0_sin, x0, sin_4d, s));
        var x1_cos = c.mlx_array_new();
        defer _ = c.mlx_array_free(x1_cos);
        try mlx.check(c.mlx_multiply(&x1_cos, x1, cos_4d, s));
        var out1 = c.mlx_array_new();
        defer _ = c.mlx_array_free(out1);
        try mlx.check(c.mlx_add(&out1, x0_sin, x1_cos, s));

        // Stack rotated pairs back to [tokens, heads, rope_half, 2].
        const pair_concat_inputs = [_]c.mlx_array{ out0, out1 };
        const pair_concat_vec = c.mlx_vector_array_new_data(&pair_concat_inputs, 2);
        defer _ = c.mlx_vector_array_free(pair_concat_vec);
        var rotated_4d = c.mlx_array_new();
        defer _ = c.mlx_array_free(rotated_4d);
        try mlx.check(c.mlx_concatenate_axis(&rotated_4d, pair_concat_vec, 3, s));

        // Flatten pairs back to [tokens, heads, rope_dim].
        const rotated_3d_shape = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(rope_dim) };
        var rotated_3d = c.mlx_array_new();
        defer _ = c.mlx_array_free(rotated_3d);
        try mlx.check(c.mlx_reshape(&rotated_3d, rotated_4d, &rotated_3d_shape, 3, s));

        // Concatenate with pass-through dims if partial.
        var final_3d = rotated_3d;
        var final_3d_owned = false;
        if (partial) {
            const pp_starts = [_]c_int{ 0, 0, @intCast(rope_dim) };
            const pp_stops = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(head_dim) };
            const pp_strides = [_]c_int{ 1, 1, 1 };
            var pass_part = c.mlx_array_new();
            defer _ = c.mlx_array_free(pass_part);
            try mlx.check(c.mlx_slice(&pass_part, reshaped_3d, &pp_starts, 3, &pp_stops, 3, &pp_strides, 3, s));

            const cat_inputs = [_]c.mlx_array{ rotated_3d, pass_part };
            const cat_vec = c.mlx_vector_array_new_data(&cat_inputs, 2);
            defer _ = c.mlx_vector_array_free(cat_vec);
            final_3d = c.mlx_array_new();
            final_3d_owned = true;
            try mlx.check(c.mlx_concatenate_axis(&final_3d, cat_vec, 2, s));
        }
        defer if (final_3d_owned) {
            _ = c.mlx_array_free(final_3d);
        };

        const flat_shape = [_]c_int{ @intCast(total_tokens), @intCast(total_dim) };
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_reshape(&result, final_3d, &flat_shape, 2, s));
        return self.makeArr(result, true);
    }

    // Half-split layout: first half and second half of each head are paired.
    const shape_3d = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(head_dim) };
    var reshaped = c.mlx_array_new();
    defer _ = c.mlx_array_free(reshaped);
    try mlx.check(c.mlx_reshape(&reshaped, x, &shape_3d, 3, s));

    const cs_shape = [_]c_int{ @intCast(total_tokens), 1, @intCast(rope_half) };
    var cos_3d = c.mlx_array_new();
    defer _ = c.mlx_array_free(cos_3d);
    try mlx.check(c.mlx_reshape(&cos_3d, cos_vals, &cs_shape, 3, s));
    var sin_3d = c.mlx_array_new();
    defer _ = c.mlx_array_free(sin_3d);
    try mlx.check(c.mlx_reshape(&sin_3d, sin_vals, &cs_shape, 3, s));

    // Slice the rope-active portions from each half of the head.
    const starts_0 = [_]c_int{ 0, 0, 0 };
    const stops_0 = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(rope_half) };
    const strides = [_]c_int{ 1, 1, 1 };
    var x0 = c.mlx_array_new();
    defer _ = c.mlx_array_free(x0);
    try mlx.check(c.mlx_slice(&x0, reshaped, &starts_0, 3, &stops_0, 3, &strides, 3, s));

    const starts_1 = [_]c_int{ 0, 0, @intCast(head_half) };
    const stops_1 = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(head_half + rope_half) };
    var x1 = c.mlx_array_new();
    defer _ = c.mlx_array_free(x1);
    try mlx.check(c.mlx_slice(&x1, reshaped, &starts_1, 3, &stops_1, 3, &strides, 3, s));

    var x0_cos = c.mlx_array_new();
    defer _ = c.mlx_array_free(x0_cos);
    try mlx.check(c.mlx_multiply(&x0_cos, x0, cos_3d, s));
    var x1_sin = c.mlx_array_new();
    defer _ = c.mlx_array_free(x1_sin);
    try mlx.check(c.mlx_multiply(&x1_sin, x1, sin_3d, s));
    var out0 = c.mlx_array_new();
    defer _ = c.mlx_array_free(out0);
    try mlx.check(c.mlx_subtract(&out0, x0_cos, x1_sin, s));

    var x0_sin = c.mlx_array_new();
    defer _ = c.mlx_array_free(x0_sin);
    try mlx.check(c.mlx_multiply(&x0_sin, x0, sin_3d, s));
    var x1_cos = c.mlx_array_new();
    defer _ = c.mlx_array_free(x1_cos);
    try mlx.check(c.mlx_multiply(&x1_cos, x1, cos_3d, s));
    var out1 = c.mlx_array_new();
    defer _ = c.mlx_array_free(out1);
    try mlx.check(c.mlx_add(&out1, x0_sin, x1_cos, s));

    if (!partial) {
        // Full rotation: just concat the two halves.
        const concat_inputs = [_]c.mlx_array{ out0, out1 };
        const concat_vec = c.mlx_vector_array_new_data(&concat_inputs, 2);
        defer _ = c.mlx_vector_array_free(concat_vec);

        var concatenated = c.mlx_array_new();
        defer _ = c.mlx_array_free(concatenated);
        try mlx.check(c.mlx_concatenate_axis(&concatenated, concat_vec, 2, s));

        const flat_shape = [_]c_int{ @intCast(total_tokens), @intCast(total_dim) };
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_reshape(&result, concatenated, &flat_shape, 2, s));
        return self.makeArr(result, true);
    }

    // Partial rotation: slice out pass-through portions and reassemble.
    // First half pass-through: [rope_half..head_half]
    const p0_starts = [_]c_int{ 0, 0, @intCast(rope_half) };
    const p0_stops = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(head_half) };
    var pass0 = c.mlx_array_new();
    defer _ = c.mlx_array_free(pass0);
    try mlx.check(c.mlx_slice(&pass0, reshaped, &p0_starts, 3, &p0_stops, 3, &strides, 3, s));

    // Second half pass-through: [head_half+rope_half..head_dim]
    const p1_starts = [_]c_int{ 0, 0, @intCast(head_half + rope_half) };
    const p1_stops = [_]c_int{ @intCast(total_tokens), @intCast(num_heads), @intCast(head_dim) };
    var pass1 = c.mlx_array_new();
    defer _ = c.mlx_array_free(pass1);
    try mlx.check(c.mlx_slice(&pass1, reshaped, &p1_starts, 3, &p1_stops, 3, &strides, 3, s));

    // Reassemble: [out0 | pass0 | out1 | pass1]
    const concat_inputs = [_]c.mlx_array{ out0, pass0, out1, pass1 };
    const concat_vec = c.mlx_vector_array_new_data(&concat_inputs, 4);
    defer _ = c.mlx_vector_array_free(concat_vec);

    var concatenated = c.mlx_array_new();
    defer _ = c.mlx_array_free(concatenated);
    try mlx.check(c.mlx_concatenate_axis(&concatenated, concat_vec, 2, s));

    const flat_shape = [_]c_int{ @intCast(total_tokens), @intCast(total_dim) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, concatenated, &flat_shape, 2, s));
    return self.makeArr(result, true);
}

fn gqaCausalAttentionOp(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return gqaAttentionArrays(self, getArr(q_ct), getArr(k_ct), getArr(v_ct), attn_bias_ct, null, 0, batch, seq_len, seq_len, 0, 0, num_heads, num_kv_heads, head_dim);
}

fn cloneArray(self: *MlxCompute, arr: c.mlx_array) !c.mlx_array {
    var cloned = c.mlx_array_new();
    try mlx.check(c.mlx_astype(&cloned, arr, c.MLX_FLOAT32, self.data.stream));
    return cloned;
}

fn sliceRows(self: *MlxCompute, arr: c.mlx_array, start_row: usize, row_count: usize) !c.mlx_array {
    if (c.mlx_array_ndim(arr) != 2) return error.InvalidPagedKvShape;

    const total_rows: usize = @intCast(c.mlx_array_dim(arr, 0));
    const row_width: usize = @intCast(c.mlx_array_dim(arr, 1));
    if (start_row + row_count > total_rows) return error.InvalidPagedKvSlice;

    const starts = [_]c_int{ @intCast(start_row), 0 };
    const stops = [_]c_int{ @intCast(start_row + row_count), @intCast(row_width) };
    const strides = [_]c_int{ 1, 1 };
    var sliced = c.mlx_array_new();
    try mlx.check(c.mlx_slice(&sliced, arr, &starts, 2, &stops, 2, &strides, 2, self.data.stream));
    return sliced;
}

fn sliceColumns(self: *MlxCompute, arr: c.mlx_array, start_col: usize, col_count: usize) !c.mlx_array {
    if (c.mlx_array_ndim(arr) != 2) return error.InvalidPagedKvShape;

    const total_rows: usize = @intCast(c.mlx_array_dim(arr, 0));
    const total_cols: usize = @intCast(c.mlx_array_dim(arr, 1));
    if (start_col + col_count > total_cols) return error.InvalidPagedKvSlice;

    const starts = [_]c_int{ 0, @intCast(start_col) };
    const stops = [_]c_int{ @intCast(total_rows), @intCast(start_col + col_count) };
    const strides = [_]c_int{ 1, 1 };
    var sliced = c.mlx_array_new();
    try mlx.check(c.mlx_slice(&sliced, arr, &starts, 2, &stops, 2, &strides, 2, self.data.stream));
    return sliced;
}

fn concatenateRows(self: *MlxCompute, lhs: c.mlx_array, rhs: c.mlx_array) !c.mlx_array {
    const inputs = [_]c.mlx_array{ lhs, rhs };
    const input_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
    defer _ = c.mlx_vector_array_free(input_vec);

    var concatenated = c.mlx_array_new();
    try mlx.check(c.mlx_concatenate_axis(&concatenated, input_vec, 0, self.data.stream));
    return concatenated;
}

fn compressedKeyRowBytesForPool(pool: *const runtime.kv.pool.KvPool, format: mlx_quant.CompressedKeyFormat) usize {
    return switch (format) {
        .polar4 => runtime.kv.turboquant.polar4KeyBytes(pool.config.num_kv_heads, pool.config.head_dim),
        .turbo3 => runtime.kv.turboquant.turbo3KeyBytes(pool.config.num_kv_heads, pool.config.head_dim) + runtime.kv.turboquant.turbo3ResidualBytes(pool.config.num_kv_heads, pool.config.head_dim),
    };
}

fn compressedKeyFormatForPool(pool: *const runtime.kv.pool.KvPool) ?mlx_quant.CompressedKeyFormat {
    return switch (pool.config.dtype) {
        .polar4 => .polar4,
        .turbo3 => .turbo3,
        else => null,
    };
}

fn encodeCompressedKeyBytesFromRows(
    allocator: std.mem.Allocator,
    pool: *const runtime.kv.pool.KvPool,
    format: mlx_quant.CompressedKeyFormat,
    rows: []const f32,
    token_count: usize,
) ![]u8 {
    if (!pool.config.hasSymmetricValueWidth()) return error.UnsupportedAsymmetricKvWidths;
    const value_width = pool.valuesPerToken();
    if (rows.len != token_count * value_width) return error.InvalidPagedKvShape;
    const key_row_bytes = compressedKeyRowBytesForPool(pool, format);
    const encoded = try allocator.alloc(u8, token_count * key_row_bytes);
    errdefer allocator.free(encoded);

    const base_key_bytes = switch (format) {
        .polar4 => runtime.kv.turboquant.polar4KeyBytes(pool.config.num_kv_heads, pool.config.head_dim),
        .turbo3 => runtime.kv.turboquant.turbo3KeyBytes(pool.config.num_kv_heads, pool.config.head_dim),
    };
    const residual_key_bytes = switch (format) {
        .polar4 => @as(usize, 0),
        .turbo3 => runtime.kv.turboquant.turbo3ResidualBytes(pool.config.num_kv_heads, pool.config.head_dim),
    };

    for (0..token_count) |token_idx| {
        const row = rows[token_idx * value_width ..][0..value_width];
        const row_dst = encoded[token_idx * key_row_bytes ..][0..key_row_bytes];
        switch (format) {
            .polar4 => try runtime.kv.turboquant.encodePolar4Key(row, row_dst[0..base_key_bytes], pool.config.num_kv_heads, pool.config.head_dim),
            .turbo3 => {
                try runtime.kv.turboquant.encodeTurbo3Key(row, row_dst[0..base_key_bytes], pool.config.num_kv_heads, pool.config.head_dim);
                try runtime.kv.turboquant.encodeTurbo3ResidualSketch(
                    row,
                    row_dst[0..base_key_bytes],
                    row_dst[base_key_bytes .. base_key_bytes + residual_key_bytes],
                    pool.config.num_kv_heads,
                    pool.config.head_dim,
                );
            },
        }
    }
    return encoded;
}

fn encodedKeyArrayFromCachedRows(
    self: *MlxCompute,
    pool: *const runtime.kv.pool.KvPool,
    format: mlx_quant.CompressedKeyFormat,
    k_arr: c.mlx_array,
    token_count: usize,
) !c.mlx_array {
    const rows = try mlx.readFloat32(k_arr, self.allocator);
    defer self.allocator.free(rows);
    const encoded = try encodeCompressedKeyBytesFromRows(self.allocator, pool, format, rows, token_count);
    defer self.allocator.free(encoded);
    const encoded_shape = [_]i32{@intCast(encoded.len)};
    return mlx.arrayFromBytes(encoded, &encoded_shape, c.MLX_UINT8);
}

fn metadataOnlyKvColdMiss(pool: *const runtime.kv.pool.KvPool, what: []const u8) anyerror {
    if (!pool.config.store_cpu_bytes) {
        std.log.err("MLX metadata-only KV cold miss: {s} requires cached MLX block state for backend={s} dtype={s}", .{
            what,
            @tagName(pool.config.backend),
            @tagName(pool.config.dtype),
        });
        return error.KvColdMissRequiresCachedBlock;
    }
    return error.InvalidPagedKvState;
}

fn ensureEncodedKeyArray(
    self: *MlxCompute,
    pool: *const runtime.kv.pool.KvPool,
    key: KvCacheKey,
    entry: *KvCacheEntry,
    block_tokens: usize,
    key_row_bytes: usize,
) !c.mlx_array {
    if (entry.encoded_key) |arr| {
        if (entry.encoded_key_row_bytes == key_row_bytes and entry.encoded_key_tokens >= block_tokens) {
            return arr;
        }
        _ = c.mlx_array_free(arr);
        entry.encoded_key = null;
        entry.encoded_key_tokens = 0;
        entry.encoded_key_row_bytes = 0;
    }

    if (!pool.config.store_cpu_bytes) {
        const format = compressedKeyFormatForPool(pool) orelse return metadataOnlyKvColdMiss(pool, "encoded key rebuild");
        const encoded_arr = try encodedKeyArrayFromCachedRows(self, pool, format, entry.k, block_tokens);
        entry.encoded_key = encoded_arr;
        entry.encoded_key_tokens = block_tokens;
        entry.encoded_key_row_bytes = key_row_bytes;
        return encoded_arr;
    }

    const encoded_bytes = try self.allocator.alloc(u8, block_tokens * key_row_bytes);
    defer self.allocator.free(encoded_bytes);
    for (0..block_tokens) |token_offset| {
        const encoded = try pool.readEncodedToken(key.block_id, key.layer_index, token_offset);
        @memcpy(encoded_bytes[token_offset * key_row_bytes ..][0..key_row_bytes], encoded.k_bytes[0..key_row_bytes]);
    }
    const encoded_shape = [_]i32{@intCast(encoded_bytes.len)};
    const encoded_arr = mlx.arrayFromBytes(encoded_bytes, &encoded_shape, c.MLX_UINT8);
    entry.encoded_key = encoded_arr;
    entry.encoded_key_tokens = block_tokens;
    entry.encoded_key_row_bytes = key_row_bytes;
    return encoded_arr;
}

fn encodedKeyArrayForRange(
    self: *MlxCompute,
    pool: *const runtime.kv.pool.KvPool,
    attention: AttentionContext,
    start_token: usize,
    token_count: usize,
    key_row_bytes: usize,
) !c.mlx_array {
    const kv = attention.kv_cache orelse return error.InvalidPagedKvState;
    const block_ids = if (kv.logical_blocks) |blocks|
        blocks
    else if (attention.kv_storage) |storage|
        (storage.blockTable(kv.sequence_id) orelse return error.InvalidSequenceId).blocks.items
    else blk: {
        const manager = attention.kv_manager orelse return error.InvalidPagedKvState;
        const table = manager.blockTable(kv.sequence_id) orelse return error.InvalidSequenceId;
        break :blk table.blocks.items;
    };
    if (start_token + token_count > attention.kv_sequence_len) return error.InvalidPagedKvState;
    if (!pool.config.store_cpu_bytes) {
        const format = compressedKeyFormatForPool(pool) orelse return metadataOnlyKvColdMiss(pool, "encoded key range rebuild");
        const source = try attentionKvSource(attention);
        const key_row_bytes_expected = compressedKeyRowBytesForPool(pool, format);
        if (key_row_bytes != key_row_bytes_expected) return error.InvalidKvRowWidth;
        const encoded_bytes = try self.allocator.alloc(u8, token_count * key_row_bytes);
        defer self.allocator.free(encoded_bytes);
        var copied_tokens: usize = 0;
        var remaining_tokens = token_count;
        while (remaining_tokens > 0) {
            const token_idx = start_token + copied_tokens;
            const logical_block_idx = token_idx / pool.config.page_size_tokens;
            if (logical_block_idx >= block_ids.len) return error.InvalidPagedKvState;
            const block_id = block_ids[logical_block_idx];
            const token_offset = token_idx % pool.config.page_size_tokens;
            const block_tokens = @min(remaining_tokens, pool.config.page_size_tokens - token_offset);
            const key = KvCacheKey{
                .manager_ptr = source.ptr_id,
                .pool_id = kv.pool_id,
                .block_id = block_id,
                .layer_index = attention.layer_index,
            };
            const entry = self.kv_cache.getPtr(key) orelse return metadataOnlyKvColdMiss(pool, "encoded key range rebuild");
            if (entry.token_count < token_offset + block_tokens) return error.InvalidPagedKvState;
            const block_slice = try sliceRows(self, entry.k, token_offset, block_tokens);
            defer _ = c.mlx_array_free(block_slice);
            const block_encoded = try encodedKeyArrayFromCachedRows(self, pool, format, block_slice, block_tokens);
            defer _ = c.mlx_array_free(block_encoded);
            try mlx.evalArray(block_encoded);
            const block_bytes = c.mlx_array_data_uint8(block_encoded) orelse return error.MlxDataNull;
            @memcpy(
                encoded_bytes[copied_tokens * key_row_bytes ..][0 .. block_tokens * key_row_bytes],
                block_bytes[0 .. block_tokens * key_row_bytes],
            );
            copied_tokens += block_tokens;
            remaining_tokens -= block_tokens;
        }
        const encoded_shape = [_]i32{@intCast(encoded_bytes.len)};
        return mlx.arrayFromBytes(encoded_bytes, &encoded_shape, c.MLX_UINT8);
    }

    const encoded_bytes = try self.allocator.alloc(u8, token_count * key_row_bytes);
    defer self.allocator.free(encoded_bytes);
    for (0..token_count) |i| {
        const token_idx = start_token + i;
        const logical_block_idx = token_idx / pool.config.page_size_tokens;
        if (logical_block_idx >= block_ids.len) return error.InvalidPagedKvState;
        const block_id = block_ids[logical_block_idx];
        const token_offset = token_idx % pool.config.page_size_tokens;
        const encoded = try pool.readEncodedToken(block_id, attention.layer_index, token_offset);
        @memcpy(encoded_bytes[i * key_row_bytes ..][0..key_row_bytes], encoded.k_bytes[0..key_row_bytes]);
    }

    const encoded_shape = [_]i32{@intCast(encoded_bytes.len)};
    return mlx.arrayFromBytes(encoded_bytes, &encoded_shape, c.MLX_UINT8);
}

fn rebuildGatheredEncodedKeyArray(
    self: *MlxCompute,
    entry: *GatherCacheEntry,
    pool: *const runtime.kv.pool.KvPool,
    attention: AttentionContext,
    key_row_bytes: usize,
) !c.mlx_array {
    if (entry.encoded_key) |arr| _ = c.mlx_array_free(arr);
    const encoded = try encodedKeyArrayForRange(self, pool, attention, 0, attention.kv_sequence_len, key_row_bytes);
    entry.encoded_key = encoded;
    entry.encoded_key_tokens = attention.kv_sequence_len;
    entry.encoded_key_position_offset = attention.kv_position_offset;
    entry.encoded_key_row_bytes = key_row_bytes;
    return encoded;
}

fn updateGatheredEncodedKeyCache(
    self: *MlxCompute,
    entry: *GatherCacheEntry,
    pool: *const runtime.kv.pool.KvPool,
    attention: AttentionContext,
    format: mlx_quant.CompressedKeyFormat,
) !c.mlx_array {
    const key_row_bytes = compressedKeyRowBytesForPool(pool, format);
    if (entry.encoded_key) |arr| {
        if (entry.encoded_key_row_bytes == key_row_bytes and
            entry.encoded_key_tokens == attention.kv_sequence_len and
            entry.encoded_key_position_offset == attention.kv_position_offset)
        {
            return arr;
        }

        if (entry.encoded_key_row_bytes == key_row_bytes and attention.query_sequence_len <= attention.kv_sequence_len) {
            const dropped = if (attention.kv_position_offset >= entry.encoded_key_position_offset)
                attention.kv_position_offset - entry.encoded_key_position_offset
            else
                std.math.maxInt(usize);
            const suffix_tokens = attention.query_sequence_len;
            const expected_prefix_tokens = attention.kv_sequence_len - suffix_tokens;
            if (dropped != std.math.maxInt(usize) and
                entry.encoded_key_tokens >= dropped and
                entry.encoded_key_tokens - dropped == expected_prefix_tokens)
            {
                const retained_bytes = expected_prefix_tokens * key_row_bytes;
                var retained: ?c.mlx_array = null;
                defer {
                    if (retained) |retained_arr| _ = c.mlx_array_free(retained_arr);
                }
                if (retained_bytes > 0) {
                    const starts = [_]c_int{@intCast(dropped * key_row_bytes)};
                    const stops = [_]c_int{@intCast((dropped + expected_prefix_tokens) * key_row_bytes)};
                    const strides = [_]c_int{1};
                    var sliced = c.mlx_array_new();
                    try mlx.check(c.mlx_slice(&sliced, arr, &starts, 1, &stops, 1, &strides, 1, self.data.stream));
                    retained = sliced;
                }

                const suffix = try encodedKeyArrayForRange(self, pool, attention, expected_prefix_tokens, suffix_tokens, key_row_bytes);
                var suffix_owned = true;
                defer {
                    if (suffix_owned) _ = c.mlx_array_free(suffix);
                }

                const new_encoded = if (retained) |retained_arr| blk: {
                    const inputs = [_]c.mlx_array{ retained_arr, suffix };
                    const input_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
                    defer _ = c.mlx_vector_array_free(input_vec);
                    var concatenated = c.mlx_array_new();
                    try mlx.check(c.mlx_concatenate_axis(&concatenated, input_vec, 0, self.data.stream));
                    break :blk concatenated;
                } else blk: {
                    suffix_owned = false;
                    break :blk suffix;
                };

                _ = c.mlx_array_free(arr);
                entry.encoded_key = new_encoded;
                entry.encoded_key_tokens = attention.kv_sequence_len;
                entry.encoded_key_position_offset = attention.kv_position_offset;
                entry.encoded_key_row_bytes = key_row_bytes;
                return new_encoded;
            }
        }
    }

    return rebuildGatheredEncodedKeyArray(self, entry, pool, attention, key_row_bytes);
}

fn cacheBlockRows(
    self: *MlxCompute,
    pool: *runtime.kv.pool.KvPool,
    key: KvCacheKey,
    k_rows: c.mlx_array,
    v_rows: c.mlx_array,
    block_offset: usize,
    row_count: usize,
) !void {
    const gop = try self.kv_cache.getOrPut(self.allocator, key);
    if (!gop.found_existing) {
        if (block_offset != 0) {
            const prefix_k = try loadBlockPrefixRows(self, pool, key, block_offset, true);
            defer {
                if (prefix_k.owned) _ = c.mlx_array_free(prefix_k.arr);
            }
            const prefix_v = try loadBlockPrefixRows(self, pool, key, block_offset, false);
            defer {
                if (prefix_v.owned) _ = c.mlx_array_free(prefix_v.arr);
            }

            gop.value_ptr.* = .{
                .k = try concatenateRows(self, prefix_k.arr, k_rows),
                .v = try concatenateRows(self, prefix_v.arr, v_rows),
                .token_count = block_offset + row_count,
            };
            return;
        }
        gop.value_ptr.* = .{
            .k = try cloneArray(self, k_rows),
            .v = try cloneArray(self, v_rows),
            .token_count = row_count,
        };
        return;
    }

    const entry = gop.value_ptr;
    if (block_offset == 0) {
        entry.deinit();
        entry.* = .{
            .k = try cloneArray(self, k_rows),
            .v = try cloneArray(self, v_rows),
            .token_count = row_count,
        };
        return;
    }

    if (entry.token_count > block_offset) {
        const overlap = entry.token_count - block_offset;
        if (overlap >= row_count) return;

        const append_row_count = row_count - overlap;
        const append_k_rows = try sliceRows(self, k_rows, overlap, append_row_count);
        defer _ = c.mlx_array_free(append_k_rows);
        const append_v_rows = try sliceRows(self, v_rows, overlap, append_row_count);
        defer _ = c.mlx_array_free(append_v_rows);

        const new_k = try concatenateRows(self, entry.k, append_k_rows);
        errdefer _ = c.mlx_array_free(new_k);
        const new_v = try concatenateRows(self, entry.v, append_v_rows);
        errdefer _ = c.mlx_array_free(new_v);

        entry.deinit();
        entry.* = .{
            .k = new_k,
            .v = new_v,
            .token_count = block_offset + row_count,
        };
        return;
    }

    if (entry.token_count != block_offset) return error.InvalidPagedKvState;

    const new_k = try concatenateRows(self, entry.k, k_rows);
    errdefer _ = c.mlx_array_free(new_k);
    const new_v = try concatenateRows(self, entry.v, v_rows);
    errdefer _ = c.mlx_array_free(new_v);

    entry.deinit();
    entry.* = .{
        .k = new_k,
        .v = new_v,
        .token_count = block_offset + row_count,
    };
}

fn loadBlockPrefixRows(
    self: *MlxCompute,
    pool: *runtime.kv.pool.KvPool,
    key: KvCacheKey,
    row_count: usize,
    comptime load_k: bool,
) !struct { arr: c.mlx_array, owned: bool } {
    if (row_count == 0) return .{ .arr = c.mlx_array_new(), .owned = false };
    if (!pool.config.store_cpu_bytes) return metadataOnlyKvColdMiss(pool, "paged KV block prefix rebuild");
    if (!pool.config.hasSymmetricValueWidth()) return error.UnsupportedAsymmetricKvWidths;

    const token_width = pool.valuesPerToken();
    const rows = try self.allocator.alloc(f32, row_count * token_width);
    defer self.allocator.free(rows);

    for (0..row_count) |row_idx| {
        const token = try pool.readToken(key.block_id, key.layer_index, row_idx);
        const dst = rows[row_idx * token_width ..][0..token_width];
        if (load_k) {
            @memcpy(dst, token.k);
        } else {
            @memcpy(dst, token.v);
        }
    }

    const shape = [_]i32{ @intCast(row_count), @intCast(token_width) };
    return .{ .arr = mlx.arrayFromFloat32(rows, &shape), .owned = true };
}

fn ensureCachedBlockEntryFromManager(
    self: *MlxCompute,
    pool: *runtime.kv.pool.KvPool,
    key: KvCacheKey,
) !*KvCacheEntry {
    const started_at = monotonicNowNs();
    defer quant_execution_timing_stats.paged_cache_lookup_nanos += @intCast(monotonicNowNs() - started_at);
    if (self.kv_cache.getPtr(key)) |entry| return entry;
    if (!pool.config.store_cpu_bytes) return metadataOnlyKvColdMiss(pool, "paged KV block cache rebuild");

    const block_storage = pool.storage(key.block_id) orelse return error.InvalidBlockId;
    const token_count = block_storage.meta.tokens_written;
    if (token_count == 0) {
        std.log.err(
            "MLX missing paged KV block cache source: pool={d} block={d} layer={d} tokens_written=0 page_size={d}",
            .{ key.pool_id, key.block_id, key.layer_index, pool.config.page_size_tokens },
        );
        return error.MissingPagedKvBlock;
    }

    const prefix_k = try loadBlockPrefixRows(self, pool, key, token_count, true);
    defer {
        if (prefix_k.owned) _ = c.mlx_array_free(prefix_k.arr);
    }
    const prefix_v = try loadBlockPrefixRows(self, pool, key, token_count, false);
    defer {
        if (prefix_v.owned) _ = c.mlx_array_free(prefix_v.arr);
    }

    const gop = try self.kv_cache.getOrPut(self.allocator, key);
    if (!gop.found_existing) {
        gop.value_ptr.* = .{
            .k = try cloneArray(self, prefix_k.arr),
            .v = try cloneArray(self, prefix_v.arr),
            .token_count = token_count,
        };
    }
    return gop.value_ptr;
}

fn transposedPagedBlockKey(
    self: *MlxCompute,
    entry: *const KvCacheEntry,
    block_tokens: usize,
    expected_width: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !c.mlx_array {
    const s = self.data.stream;
    const entry_rows: usize = @intCast(c.mlx_array_dim(entry.k, 0));
    const entry_width: usize = @intCast(c.mlx_array_dim(entry.k, 1));
    const needs_row_slice = entry_rows != block_tokens;
    const needs_col_slice = entry_width != expected_width;
    const entry_k = blk: {
        const arr = entry.k;
        if (needs_row_slice or needs_col_slice) {
            const starts = [_]c_int{ 0, 0 };
            const stops = [_]c_int{ @intCast(block_tokens), @intCast(expected_width) };
            const strides = [_]c_int{ 1, 1 };
            var sliced = c.mlx_array_new();
            try mlx.check(c.mlx_slice(&sliced, arr, &starts, 2, &stops, 2, &strides, 2, s));
            break :blk sliced;
        }
        break :blk arr;
    };
    defer {
        if (needs_row_slice or needs_col_slice) _ = c.mlx_array_free(entry_k);
    }

    const kv_shape = [_]c_int{ 1, @intCast(block_tokens), @intCast(num_kv_heads), @intCast(head_dim) };
    var k_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_r);
    try mlx.check(c.mlx_reshape(&k_r, entry_k, &kv_shape, 4, s));

    const perm = [_]c_int{ 0, 2, 1, 3 };
    var k_t = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_r, &perm, 4, s));
    return k_t;
}

fn updatePagedKvBlocks(self: *MlxCompute, k_arr: c.mlx_array, v_arr: c.mlx_array, attention: AttentionContext) !void {
    if (attention.skip_kv_write) return;
    const started_at = monotonicNowNs();
    defer quant_execution_timing_stats.paged_update_kv_nanos += @intCast(monotonicNowNs() - started_at);
    const kv = attention.kv_cache orelse return;
    const source = try attentionKvSource(attention);
    const block_ids = source.block_ids;
    const pool = source.pool;
    if (c.mlx_array_ndim(k_arr) != 2 or c.mlx_array_ndim(v_arr) != 2) return error.InvalidPagedKvShape;
    if (!pool.config.hasSymmetricValueWidth()) return error.UnsupportedAsymmetricKvWidths;

    const token_width = pool.valuesPerToken();
    const actual_width = @as(usize, @intCast(c.mlx_array_dim(k_arr, 1)));
    if (actual_width > token_width) return error.InvalidPagedKvShape;
    if (@as(usize, @intCast(c.mlx_array_dim(v_arr, 1))) != actual_width) return error.InvalidPagedKvShape;

    // Per-layer GQA: pad K/V to pool width if narrower.
    var k_padded: c.mlx_array = c.mlx_array_new();
    var v_padded: c.mlx_array = c.mlx_array_new();
    const needs_pad = actual_width < token_width;
    if (needs_pad) {
        const s = self.data.stream;
        const num_tokens = @as(usize, @intCast(c.mlx_array_dim(k_arr, 0)));
        const pad_width = token_width - actual_width;
        const pad_shape = [2]i32{ @intCast(num_tokens), @intCast(pad_width) };
        var pad_zeros: c.mlx_array = c.mlx_array_new();
        try mlx.check(c.mlx_zeros(&pad_zeros, &pad_shape, 2, c.MLX_FLOAT32, s));
        defer _ = c.mlx_array_free(pad_zeros);

        const k_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(k_vec);
        _ = c.mlx_vector_array_append_value(k_vec, k_arr);
        _ = c.mlx_vector_array_append_value(k_vec, pad_zeros);
        try mlx.check(c.mlx_concatenate_axis(&k_padded, k_vec, 1, s));

        const v_vec = c.mlx_vector_array_new();
        defer _ = c.mlx_vector_array_free(v_vec);
        _ = c.mlx_vector_array_append_value(v_vec, v_arr);
        _ = c.mlx_vector_array_append_value(v_vec, pad_zeros);
        try mlx.check(c.mlx_concatenate_axis(&v_padded, v_vec, 1, s));
    }
    defer if (needs_pad) {
        _ = c.mlx_array_free(k_padded);
        _ = c.mlx_array_free(v_padded);
    };
    const k_use = if (needs_pad) k_padded else k_arr;
    const v_use = if (needs_pad) v_padded else v_arr;

    if (self.data.mirror_kv_to_manager) {
        const k_rows_host = try mlx.readFloat32(k_use, self.allocator);
        defer self.allocator.free(k_rows_host);
        const v_rows_host = try mlx.readFloat32(v_use, self.allocator);
        defer self.allocator.free(v_rows_host);
        try attentionWriteLayerKvSuffix(source, kv, attention, k_rows_host, v_rows_host);
    }

    var suffix_row_start: usize = 0;
    var remaining_rows = attention.query_sequence_len;
    // Block-table indexing is relative to the currently retained KV window,
    // not the absolute decoded position. Once sliding-window trimming starts,
    // total_sequence_len can exceed kv_sequence_len by the dropped prefix.
    const suffix_token_start = attention.kv_sequence_len - attention.query_sequence_len;

    while (remaining_rows > 0) {
        const token_idx = suffix_token_start + suffix_row_start;
        const logical_block_idx = token_idx / pool.config.page_size_tokens;
        if (logical_block_idx >= block_ids.len) return error.InvalidPagedKvState;

        const block_offset = token_idx % pool.config.page_size_tokens;
        const rows_in_block = @min(remaining_rows, pool.config.page_size_tokens - block_offset);
        const block_id = block_ids[logical_block_idx];
        const key = KvCacheKey{
            .manager_ptr = source.ptr_id,
            .pool_id = kv.pool_id,
            .block_id = block_id,
            .layer_index = attention.layer_index,
        };

        const k_rows = try sliceRows(self, k_use, suffix_row_start, rows_in_block);
        defer _ = c.mlx_array_free(k_rows);
        const v_rows = try sliceRows(self, v_use, suffix_row_start, rows_in_block);
        defer _ = c.mlx_array_free(v_rows);

        try cacheBlockRows(self, pool, key, k_rows, v_rows, block_offset, rows_in_block);
        suffix_row_start += rows_in_block;
        remaining_rows -= rows_in_block;
    }
}

fn gatherPagedKvArrays(self: *MlxCompute, attention: AttentionContext) !PagedKvArrays {
    const kv = attention.kv_cache orelse return error.InvalidPagedKvState;
    const source = try attentionKvSource(attention);
    const block_ids = source.block_ids;
    if (block_ids.len == 0) return error.InvalidPagedKvState;
    const pool = source.pool;

    var needed_blocks: usize = 0;
    var remaining_tokens = attention.kv_sequence_len;
    for (block_ids) |block_id| {
        if (remaining_tokens == 0) break;
        const storage = pool.storage(block_id) orelse return error.InvalidBlockId;
        const block_tokens = storage.meta.tokens_written;
        if (block_tokens == 0) {
            std.log.err(
                "MLX gather saw empty needed block: seq={d} layer={d} pool={d} block={d} remaining={d} kv_seq={d} logical_blocks={d} tail={d}",
                .{ kv.sequence_id, attention.layer_index, kv.pool_id, block_id, remaining_tokens, attention.kv_sequence_len, block_ids.len, kv.tail_tokens },
            );
            return error.MissingPagedKvBlock;
        }
        needed_blocks += 1;
        remaining_tokens -= @min(remaining_tokens, block_tokens);
    }
    if (remaining_tokens != 0) return error.InvalidPagedKvState;

    if (needed_blocks == 1) {
        const key = KvCacheKey{
            .manager_ptr = source.ptr_id,
            .pool_id = kv.pool_id,
            .block_id = block_ids[0],
            .layer_index = attention.layer_index,
        };
        const entry = try ensureCachedBlockEntryFromManager(self, pool, key);
        return .{ .k = entry.k, .v = entry.v, .owned = false };
    }

    const k_inputs = try self.allocator.alloc(c.mlx_array, needed_blocks);
    defer self.allocator.free(k_inputs);
    const v_inputs = try self.allocator.alloc(c.mlx_array, needed_blocks);
    defer self.allocator.free(v_inputs);

    var i: usize = 0;
    remaining_tokens = attention.kv_sequence_len;
    for (block_ids) |block_id| {
        if (remaining_tokens == 0) break;
        const key = KvCacheKey{
            .manager_ptr = source.ptr_id,
            .pool_id = kv.pool_id,
            .block_id = block_id,
            .layer_index = attention.layer_index,
        };
        const entry = try ensureCachedBlockEntryFromManager(self, pool, key);
        k_inputs[i] = entry.k;
        v_inputs[i] = entry.v;
        remaining_tokens -= @min(remaining_tokens, entry.token_count);
        i += 1;
    }

    const k_vec = c.mlx_vector_array_new_data(k_inputs.ptr, @intCast(k_inputs.len));
    defer _ = c.mlx_vector_array_free(k_vec);
    const v_vec = c.mlx_vector_array_new_data(v_inputs.ptr, @intCast(v_inputs.len));
    defer _ = c.mlx_vector_array_free(v_vec);

    var gathered_k = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(gathered_k);
    try mlx.check(c.mlx_concatenate_axis(&gathered_k, k_vec, 0, self.data.stream));

    var gathered_v = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(gathered_v);
    try mlx.check(c.mlx_concatenate_axis(&gathered_v, v_vec, 0, self.data.stream));

    return .{ .k = gathered_k, .v = gathered_v, .owned = true };
}

fn gatherPagedKvArraysFromBlockCache(self: *MlxCompute, attention: AttentionContext) !PagedKvArrays {
    const kv = attention.kv_cache orelse return error.InvalidPagedKvState;
    const source = try attentionKvSource(attention);
    const block_ids = source.block_ids;
    if (block_ids.len == 0) return error.InvalidPagedKvState;

    const k_inputs = try self.allocator.alloc(c.mlx_array, block_ids.len);
    defer self.allocator.free(k_inputs);
    const v_inputs = try self.allocator.alloc(c.mlx_array, block_ids.len);
    defer self.allocator.free(v_inputs);

    var needed_blocks: usize = 0;
    var remaining_tokens = attention.kv_sequence_len;
    for (block_ids) |block_id| {
        if (remaining_tokens == 0) break;
        const key = KvCacheKey{
            .manager_ptr = source.ptr_id,
            .pool_id = kv.pool_id,
            .block_id = block_id,
            .layer_index = attention.layer_index,
        };
        const entry = self.kv_cache.getPtr(key) orelse return error.MissingPagedKvBlock;
        const block_tokens = @min(entry.token_count, remaining_tokens);
        if (block_tokens == 0) return error.MissingPagedKvBlock;
        k_inputs[needed_blocks] = entry.k;
        v_inputs[needed_blocks] = entry.v;
        needed_blocks += 1;
        remaining_tokens -= block_tokens;
    }
    if (remaining_tokens != 0 or needed_blocks == 0) return error.InvalidPagedKvState;

    if (needed_blocks == 1) {
        return .{ .k = k_inputs[0], .v = v_inputs[0], .owned = false };
    }

    const k_vec = c.mlx_vector_array_new_data(k_inputs.ptr, @intCast(needed_blocks));
    defer _ = c.mlx_vector_array_free(k_vec);
    const v_vec = c.mlx_vector_array_new_data(v_inputs.ptr, @intCast(needed_blocks));
    defer _ = c.mlx_vector_array_free(v_vec);

    var gathered_k = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(gathered_k);
    try mlx.check(c.mlx_concatenate_axis(&gathered_k, k_vec, 0, self.data.stream));

    var gathered_v = c.mlx_array_new();
    errdefer _ = c.mlx_array_free(gathered_v);
    try mlx.check(c.mlx_concatenate_axis(&gathered_v, v_vec, 0, self.data.stream));

    return .{ .k = gathered_k, .v = gathered_v, .owned = true };
}

fn gatherPagedKvBlockBootstrap(self: *MlxCompute, attention: AttentionContext) !PagedKvBlockBootstrap {
    const kv = attention.kv_cache orelse return error.InvalidPagedKvState;
    const source = try attentionKvSource(attention);
    const block_ids = source.block_ids;
    if (block_ids.len == 0) return error.InvalidPagedKvState;
    const pool = source.pool;

    const k_blocks = try self.allocator.alloc(c.mlx_array, block_ids.len);
    errdefer self.allocator.free(k_blocks);
    const v_blocks = try self.allocator.alloc(c.mlx_array, block_ids.len);
    errdefer self.allocator.free(v_blocks);
    const token_counts = try self.allocator.alloc(usize, block_ids.len);
    errdefer self.allocator.free(token_counts);

    var needed_blocks: usize = 0;
    var remaining_tokens = attention.kv_sequence_len;
    for (block_ids) |block_id| {
        if (remaining_tokens == 0) break;
        const key = KvCacheKey{
            .manager_ptr = source.ptr_id,
            .pool_id = kv.pool_id,
            .block_id = block_id,
            .layer_index = attention.layer_index,
        };
        const entry = try ensureCachedBlockEntryFromManager(self, pool, key);
        const block_tokens = @min(entry.token_count, remaining_tokens);
        if (block_tokens == 0) return error.MissingPagedKvBlock;
        k_blocks[needed_blocks] = entry.k;
        v_blocks[needed_blocks] = entry.v;
        token_counts[needed_blocks] = block_tokens;
        needed_blocks += 1;
        remaining_tokens -= block_tokens;
    }
    if (remaining_tokens != 0 or needed_blocks == 0) return error.InvalidPagedKvState;

    return .{
        .k_blocks = k_blocks[0..needed_blocks],
        .v_blocks = v_blocks[0..needed_blocks],
        .token_counts = token_counts[0..needed_blocks],
    };
}

fn metadataOnlyBlockBootstrapRequired(attention: AttentionContext) bool {
    const kv = attention.kv_cache orelse return false;
    const storage = kv.kv_storage orelse return false;
    const pool = storage.getPool(kv.pool_id) orelse return false;
    return !pool.config.store_cpu_bytes and attention.query_sequence_len < attention.kv_sequence_len;
}

fn rebuildGatheredKvEntry(self: *MlxCompute, entry: *GatherCacheEntry, attention: AttentionContext) !void {
    const gathered = try gatherPagedKvArrays(self, attention);

    entry.deinit();
    if (gathered.owned) {
        entry.* = .{
            .k = gathered.k,
            .v = gathered.v,
            .token_count = attention.kv_sequence_len,
            .position_offset = attention.kv_position_offset,
        };
    } else {
        entry.* = .{
            .k = try cloneArray(self, gathered.k),
            .v = try cloneArray(self, gathered.v),
            .token_count = attention.kv_sequence_len,
            .position_offset = attention.kv_position_offset,
        };
    }
}

fn updateGatheredKvCache(self: *MlxCompute, suffix_k: c.mlx_array, suffix_v: c.mlx_array, attention: AttentionContext) !struct { k: c.mlx_array, v: c.mlx_array } {
    const kv = attention.kv_cache orelse return .{ .k = suffix_k, .v = suffix_v };
    const ptr_id: usize = if (attention.kv_storage) |storage|
        @intFromPtr(storage)
    else if (attention.kv_manager) |manager|
        @intFromPtr(manager)
    else
        return .{ .k = suffix_k, .v = suffix_v };
    const key = GatherCacheKey{
        .manager_ptr = ptr_id,
        .sequence_id = kv.sequence_id,
        .layer_index = attention.layer_index,
    };

    const gop = try self.gather_cache.getOrPut(self.allocator, key);
    if (!gop.found_existing) {
        if (attention.query_sequence_len == attention.kv_sequence_len or attention.mode == .paged_prefill) {
            gop.value_ptr.* = .{
                .k = try cloneArray(self, suffix_k),
                .v = try cloneArray(self, suffix_v),
                .token_count = attention.kv_sequence_len,
                .position_offset = attention.kv_position_offset,
            };
        } else {
            const gathered = gatherPagedKvArraysFromBlockCache(self, attention) catch try gatherPagedKvArrays(self, attention);
            if (gathered.owned) {
                gop.value_ptr.* = .{
                    .k = gathered.k,
                    .v = gathered.v,
                    .token_count = attention.kv_sequence_len,
                    .position_offset = attention.kv_position_offset,
                };
            } else {
                gop.value_ptr.* = .{
                    .k = try cloneArray(self, gathered.k),
                    .v = try cloneArray(self, gathered.v),
                    .token_count = attention.kv_sequence_len,
                    .position_offset = attention.kv_position_offset,
                };
            }
        }
        return .{ .k = gop.value_ptr.k, .v = gop.value_ptr.v };
    }

    const entry = gop.value_ptr;
    const suffix_token_count = attention.query_sequence_len;
    const expected_prefix_tokens = attention.kv_sequence_len - suffix_token_count;
    if (attention.mode == .paged_prefill or suffix_token_count == attention.kv_sequence_len) {
        entry.deinit();
        entry.* = .{
            .k = try cloneArray(self, suffix_k),
            .v = try cloneArray(self, suffix_v),
            .token_count = attention.kv_sequence_len,
            .position_offset = attention.kv_position_offset,
        };
        return .{ .k = entry.k, .v = entry.v };
    }

    const dropped_tokens = attention.kv_position_offset - entry.position_offset;
    if (entry.token_count >= dropped_tokens and entry.token_count - dropped_tokens == expected_prefix_tokens) {
        const retained = try sliceRows(self, entry.k, dropped_tokens, expected_prefix_tokens);
        defer _ = c.mlx_array_free(retained);
        const retained_v = try sliceRows(self, entry.v, dropped_tokens, expected_prefix_tokens);
        defer _ = c.mlx_array_free(retained_v);

        const new_k = if (expected_prefix_tokens > 0) try concatenateRows(self, retained, suffix_k) else try cloneArray(self, suffix_k);
        errdefer _ = c.mlx_array_free(new_k);
        const new_v = if (expected_prefix_tokens > 0) try concatenateRows(self, retained_v, suffix_v) else try cloneArray(self, suffix_v);
        errdefer _ = c.mlx_array_free(new_v);

        const encoded = entry.detachEncodedKey();
        _ = c.mlx_array_free(entry.k);
        _ = c.mlx_array_free(entry.v);
        entry.* = .{
            .k = new_k,
            .v = new_v,
            .token_count = attention.kv_sequence_len,
            .position_offset = attention.kv_position_offset,
            .encoded_key = encoded.arr,
            .encoded_key_tokens = encoded.tokens,
            .encoded_key_position_offset = encoded.position_offset,
            .encoded_key_row_bytes = encoded.row_bytes,
        };
        return .{ .k = entry.k, .v = entry.v };
    }

    try rebuildGatheredKvEntry(self, entry, attention);
    return .{ .k = entry.k, .v = entry.v };
}

fn cachedKvArrays(self: *MlxCompute, k_arr: c.mlx_array, v_arr: c.mlx_array, attention: AttentionContext) !PagedKvArrays {
    if (attention.kv_manager == null and attention.kv_storage == null) {
        return .{ .k = k_arr, .v = v_arr, .owned = false };
    }
    _ = attention.kv_cache orelse return .{ .k = k_arr, .v = v_arr, .owned = false };
    try updatePagedKvBlocks(self, k_arr, v_arr, attention);
    const gathered = try updateGatheredKvCache(self, k_arr, v_arr, attention);
    return .{ .k = gathered.k, .v = gathered.v, .owned = false };
}

fn gqaPagedAttentionOp(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, attention: AttentionContext, batch: usize, num_heads: usize, num_kv_heads: usize, head_dim: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const has_attention_sink = attention.attention_sink.hasMetadata();
    if (batch > 1) {
        if (attention.attn_or_mask != null) return error.AttentionOrMaskBatchUnsupported;
        const kv_batch = attention.kv_batch orelse {
            std.log.err("MLX paged attention fallback: batch={d} but kv_batch missing mode={s}", .{ batch, @tagName(attention.mode) });
            return gqaAttentionArrays(self, getArr(q_ct), getArr(k_ct), getArr(v_ct), attn_bias_ct, attention.attn_or_mask, attention.sliding_window, batch, attention.query_sequence_len, attention.kv_sequence_len, attention.total_sequence_len - attention.query_sequence_len, attention.kv_position_offset, num_heads, num_kv_heads, head_dim);
        };
        if (kv_batch.len != batch) return error.InvalidPagedKvBatch;

        // Check if this is a mixed batch (per-item overrides present).
        const is_mixed = kv_batch[0].per_item_query_len != null;
        const max_q_rows = attention.query_sequence_len;
        const q_span = max_q_rows;
        var parts = try self.allocator.alloc(c.mlx_array, batch);
        defer self.allocator.free(parts);

        for (0..batch) |b| {
            const view = kv_batch[b];
            const item_q_len = if (is_mixed) (view.per_item_query_len orelse max_q_rows) else max_q_rows;

            const q_slice = try sliceRows(self, getArr(q_ct), b * q_span, item_q_len);
            const k_slice = try sliceRows(self, getArr(k_ct), b * q_span, item_q_len);
            const v_slice = try sliceRows(self, getArr(v_ct), b * q_span, item_q_len);
            errdefer {
                _ = c.mlx_array_free(q_slice);
                _ = c.mlx_array_free(k_slice);
                _ = c.mlx_array_free(v_slice);
            }

            const item_attention: AttentionContext = if (is_mixed) .{
                .mode = view.per_item_mode orelse attention.mode,
                .total_sequence_len = view.per_item_total_len orelse attention.total_sequence_len,
                .query_sequence_len = item_q_len,
                .kv_sequence_len = view.per_item_kv_len orelse attention.kv_sequence_len,
                .kv_position_offset = view.per_item_kv_position_offset orelse attention.kv_position_offset,
                .sliding_window = attention.sliding_window,
                .kv_cache = view.kv_cache,
                .kv_manager = view.kv_manager,
                .kv_storage = view.kv_storage,
                .kv_batch = null,
                .layer_index = attention.layer_index,
                .skip_kv_write = attention.skip_kv_write,
                .attention_sink = attention.attention_sink,
            } else blk: {
                var a = attention;
                a.kv_batch = null;
                a.kv_cache = view.kv_cache;
                a.kv_manager = view.kv_manager;
                a.kv_storage = view.kv_storage;
                break :blk a;
            };

            try updatePagedKvBlocks(self, k_slice, v_slice, item_attention);
            parts[b] = getArr(try gqaPagedAttentionDirect(self, q_slice, item_attention, num_heads, num_kv_heads, head_dim));
            _ = c.mlx_array_free(q_slice);
            _ = c.mlx_array_free(k_slice);
            _ = c.mlx_array_free(v_slice);
        }
        defer {
            for (parts) |arr| _ = c.mlx_array_free(arr);
        }

        const vec = c.mlx_vector_array_new_data(parts.ptr, parts.len);
        defer _ = c.mlx_vector_array_free(vec);
        var concatenated = c.mlx_array_new();
        try mlx.check(c.mlx_concatenate_axis(&concatenated, vec, 0, self.data.stream));
        return self.makeArr(concatenated, true);
    }

    if (attention.kv_manager == null and attention.kv_storage == null) {
        std.log.err("MLX paged attention fallback: kv backing missing mode={s} total={d} query={d} kv={d}", .{ @tagName(attention.mode), attention.total_sequence_len, attention.query_sequence_len, attention.kv_sequence_len });
        if (has_attention_sink) return error.UnsupportedAttentionSink;
        return gqaAttentionArrays(self, getArr(q_ct), getArr(k_ct), getArr(v_ct), attn_bias_ct, attention.attn_or_mask, attention.sliding_window, batch, attention.query_sequence_len, attention.kv_sequence_len, attention.total_sequence_len - attention.query_sequence_len, attention.kv_position_offset, num_heads, num_kv_heads, head_dim);
    }
    _ = attention.kv_cache orelse {
        std.log.err("MLX paged attention fallback: kv_cache missing mode={s} total={d} query={d} kv={d}", .{ @tagName(attention.mode), attention.total_sequence_len, attention.query_sequence_len, attention.kv_sequence_len });
        if (has_attention_sink) return error.UnsupportedAttentionSink;
        return gqaAttentionArrays(self, getArr(q_ct), getArr(k_ct), getArr(v_ct), attn_bias_ct, attention.attn_or_mask, attention.sliding_window, batch, attention.query_sequence_len, attention.kv_sequence_len, attention.total_sequence_len - attention.query_sequence_len, attention.kv_position_offset, num_heads, num_kv_heads, head_dim);
    };
    if (attn_bias_ct != null) {
        std.log.err("MLX paged attention fallback: attention bias present mode={s} total={d} query={d} kv={d}", .{ @tagName(attention.mode), attention.total_sequence_len, attention.query_sequence_len, attention.kv_sequence_len });
        if (has_attention_sink) return error.UnsupportedAttentionSink;
        const cached = try cachedKvArrays(self, getArr(k_ct), getArr(v_ct), attention);
        defer {
            if (cached.owned) {
                _ = c.mlx_array_free(cached.k);
                _ = c.mlx_array_free(cached.v);
            }
        }
        _ = attention.mode;
        return gqaAttentionArrays(self, getArr(q_ct), cached.k, cached.v, attn_bias_ct, attention.attn_or_mask, attention.sliding_window, batch, attention.query_sequence_len, attention.kv_sequence_len, attention.total_sequence_len - attention.query_sequence_len, attention.kv_position_offset, num_heads, num_kv_heads, head_dim);
    }

    try updatePagedKvBlocks(self, getArr(k_ct), getArr(v_ct), attention);
    if (try gqaPagedCompressedAttentionSpan(self, getArr(q_ct), getArr(k_ct), getArr(v_ct), attention, num_heads, num_kv_heads, head_dim)) |result| {
        return result;
    }
    _ = attention.mode;
    return gqaPagedAttentionDirect(self, getArr(q_ct), attention, num_heads, num_kv_heads, head_dim);
}

fn gqaPagedCompressedAttentionSpan(
    self: *MlxCompute,
    q_arr: c.mlx_array,
    suffix_k: c.mlx_array,
    suffix_v: c.mlx_array,
    attention: AttentionContext,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !?CT {
    if (attention.attention_sink.hasMetadata()) return null;
    if (!enableMetalCompressedAttentionSpanDebug() and !useMetalDecoderRuntimeFastPath(self)) return null;
    if (enableMetalDecoderRuntimeDebug() and disableMetalDecoderRuntimeAttentionSpanDebug()) return null;
    if (attention.query_sequence_len != 1 or attention.attn_or_mask != null) return null;
    const kv = attention.kv_cache orelse return null;
    const source = try attentionKvSource(attention);
    const block_ids = source.block_ids;
    if (block_ids.len == 0) return null;
    const pool = source.pool;
    const format: mlx_quant.CompressedKeyFormat = switch (pool.config.dtype) {
        .polar4 => .polar4,
        .turbo3 => .turbo3,
        else => return null,
    };
    if (c.mlx_array_ndim(q_arr) != 2) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(q_arr, 0))) != 1) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(q_arr, 1))) != num_heads * head_dim) return null;

    const started_at = monotonicNowNs();
    defer quant_execution_timing_stats.paged_decode_total_nanos += @intCast(monotonicNowNs() - started_at);
    quant_execution_timing_stats.paged_decode_calls += 1;
    quant_execution_timing_stats.paged_decode_blocks += block_ids.len;

    const setup_started_at = monotonicNowNs();
    const gathered = try updateGatheredKvCache(self, suffix_k, suffix_v, attention);
    _ = gathered.k;
    const gather_key = GatherCacheKey{
        .manager_ptr = source.ptr_id,
        .sequence_id = kv.sequence_id,
        .layer_index = attention.layer_index,
    };
    const entry = self.gather_cache.getPtr(gather_key) orelse return error.InvalidPagedKvState;
    const key_row_bytes = compressedKeyRowBytesForPool(pool, format);
    const encoded_key = try updateGatheredEncodedKeyCache(self, entry, pool, attention, format);
    var suffix_encoded_key: ?c.mlx_array = null;
    defer {
        if (suffix_encoded_key) |arr| _ = c.mlx_array_free(arr);
    }
    if (attention.query_sequence_len > 0 and attention.query_sequence_len <= attention.kv_sequence_len) {
        const suffix_encoded_start = (attention.kv_sequence_len - attention.query_sequence_len) * key_row_bytes;
        const suffix_encoded_stop = suffix_encoded_start + attention.query_sequence_len * key_row_bytes;
        const starts = [_]c_int{@intCast(suffix_encoded_start)};
        const stops = [_]c_int{@intCast(suffix_encoded_stop)};
        const strides = [_]c_int{1};
        var suffix_slice = c.mlx_array_new();
        try mlx.check(c.mlx_slice(&suffix_slice, encoded_key, &starts, 1, &stops, 1, &strides, 1, self.data.stream));
        suffix_encoded_key = suffix_slice;
    }
    quant_execution_timing_stats.paged_block_setup_nanos += @intCast(monotonicNowNs() - setup_started_at);

    const apply_started_at = monotonicNowNs();
    const maybe_output = self.data.native_quant.compressedAttentionSpan(&.{
        .q = q_arr,
        .encoded_key = encoded_key,
        .v = gathered.v,
        .suffix_encoded_key = suffix_encoded_key,
        .suffix_v = suffix_v,
        .suffix_tokens = attention.query_sequence_len,
        .kv_tokens = attention.kv_sequence_len,
        .num_heads = num_heads,
        .num_kv_heads = num_kv_heads,
        .head_dim = head_dim,
        .key_row_bytes = key_row_bytes,
        .query_position = attention.total_sequence_len - 1,
        .kv_position_offset = attention.kv_position_offset,
        .sliding_window = attention.sliding_window,
        .format = format,
        .stream = self.data.stream,
    }) catch |err| blk: {
        std.log.debug("MLX compressed attention span unavailable for {s}: {}", .{ @tagName(format), err });
        break :blk null;
    };
    const output = maybe_output orelse return null;
    quant_execution_timing_stats.paged_block_apply_nanos += @intCast(monotonicNowNs() - apply_started_at);
    return self.makeArr(output, true);
}

fn gqaPagedAttentionDirect(
    self: *MlxCompute,
    q_arr: c.mlx_array,
    attention: AttentionContext,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !CT {
    const started_at = monotonicNowNs();
    defer quant_execution_timing_stats.paged_decode_total_nanos += @intCast(monotonicNowNs() - started_at);
    quant_execution_timing_stats.paged_decode_calls += 1;
    const kv = attention.kv_cache orelse return error.InvalidPagedKvState;
    const source = try attentionKvSource(attention);
    const block_ids = source.block_ids;
    if (block_ids.len == 0) return error.InvalidPagedKvState;
    const q_seq_len = attention.query_sequence_len;
    const kv_seq_len = attention.kv_sequence_len;
    const query_position_offset = attention.total_sequence_len - q_seq_len;
    const H_q = num_heads * head_dim;
    const s = self.data.stream;
    var sink_scores_owned: ?[]f32 = null;
    defer if (sink_scores_owned) |scores| self.allocator.free(scores);
    const sink_scores: ?[]const f32 = if (attention.attention_sink.per_head_tensor) |sink_tensor| blk: {
        const scores = try mlx.readFloat32(getArr(sink_tensor), self.allocator);
        errdefer self.allocator.free(scores);
        if (scores.len < num_heads) return error.InvalidAttentionSinkShape;
        sink_scores_owned = scores;
        break :blk scores[0..num_heads];
    } else blk: {
        if (attention.attention_sink.slot != null) return error.UnsupportedAttentionSink;
        break :blk null;
    };

    const q_shape = [_]c_int{ 1, @intCast(q_seq_len), @intCast(num_heads), @intCast(head_dim) };
    const perm = [_]c_int{ 0, 2, 1, 3 };
    var q_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_r);
    try mlx.check(c.mlx_reshape(&q_r, q_arr, &q_shape, 4, s));
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_r, &perm, 4, s));

    const max_shape = [_]i32{ 1, @intCast(num_heads), @intCast(q_seq_len), 1 };
    const acc_shape = [_]i32{ 1, @intCast(num_heads), @intCast(q_seq_len), @intCast(head_dim) };
    const max_init = try self.allocator.alloc(f32, num_heads * q_seq_len);
    defer self.allocator.free(max_init);
    if (sink_scores) |scores| {
        for (0..num_heads) |h| {
            for (0..q_seq_len) |qi| {
                max_init[h * q_seq_len + qi] = scores[h];
            }
        }
    } else {
        @memset(max_init, -std.math.inf(f32));
    }
    var running_max = mlx.arrayFromFloat32(max_init, &max_shape);
    errdefer _ = c.mlx_array_free(running_max);

    const sum_init = try self.allocator.alloc(f32, num_heads * q_seq_len);
    defer self.allocator.free(sum_init);
    @memset(sum_init, if (sink_scores != null) 1.0 else 0.0);
    var running_sum = mlx.arrayFromFloat32(sum_init, &max_shape);
    errdefer _ = c.mlx_array_free(running_sum);

    const acc_init = try self.allocator.alloc(f32, num_heads * q_seq_len * head_dim);
    defer self.allocator.free(acc_init);
    @memset(acc_init, 0.0);
    var running_acc = mlx.arrayFromFloat32(acc_init, &acc_shape);
    errdefer _ = c.mlx_array_free(running_acc);

    const scale_arr = c.mlx_array_new_float(1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))));
    defer _ = c.mlx_array_free(scale_arr);
    const tp_axes = [_]c_int{ 0, 1, 3, 2 };

    var seen_tokens: usize = 0;
    for (block_ids) |block_id| {
        if (seen_tokens == kv_seq_len) break;
        quant_execution_timing_stats.paged_decode_blocks += 1;
        const key = KvCacheKey{
            .manager_ptr = source.ptr_id,
            .pool_id = kv.pool_id,
            .block_id = block_id,
            .layer_index = attention.layer_index,
        };
        const pool = source.pool;
        const compressed_key_format: ?mlx_quant.CompressedKeyFormat = switch (pool.config.dtype) {
            .polar4 => .polar4,
            .turbo3 => .turbo3,
            else => null,
        };
        const entry = try ensureCachedBlockEntryFromManager(self, pool, key);
        const block_tokens = @min(entry.token_count, kv_seq_len - seen_tokens);
        if (block_tokens == 0) continue;
        const entry_rows: usize = @intCast(c.mlx_array_dim(entry.k, 0));
        const entry_width: usize = @intCast(c.mlx_array_dim(entry.k, 1));
        const expected_width = num_kv_heads * head_dim;
        // Pool stores blocks at max width; per-layer GQA layers may be narrower.
        if (entry_rows < block_tokens or entry_width < expected_width) {
            std.log.err(
                "MLX paged KV shape mismatch layer={d} block={d}: entry_rows={d} block_tokens={d} entry_width={d} expected_width={d} kv_heads={d} head_dim={d} total_seq={d} query_seq={d} kv_seq={d}",
                .{
                    attention.layer_index,
                    block_id,
                    entry_rows,
                    block_tokens,
                    entry_width,
                    expected_width,
                    num_kv_heads,
                    head_dim,
                    attention.total_sequence_len,
                    attention.query_sequence_len,
                    attention.kv_sequence_len,
                },
            );
            return error.PagedKvShapeMismatch;
        }

        const block_position_offset = attention.kv_position_offset + seen_tokens;
        seen_tokens += block_tokens;

        const block_setup_started_at = monotonicNowNs();
        const needs_row_slice = entry_rows != block_tokens;
        const needs_col_slice = entry_width != expected_width;
        const kv_shape = [_]c_int{ 1, @intCast(block_tokens), @intCast(num_kv_heads), @intCast(head_dim) };
        const entry_v = blk: {
            const arr = entry.v;
            if (needs_row_slice or needs_col_slice) {
                const starts = [_]c_int{ 0, 0 };
                const stops = [_]c_int{ @intCast(block_tokens), @intCast(expected_width) };
                const strides = [_]c_int{ 1, 1 };
                var sliced = c.mlx_array_new();
                try mlx.check(c.mlx_slice(&sliced, arr, &starts, 2, &stops, 2, &strides, 2, s));
                break :blk sliced;
            }
            break :blk arr;
        };
        defer {
            if (needs_row_slice or needs_col_slice) _ = c.mlx_array_free(entry_v);
        }

        if (enableMetalCompressedAttentionBlockDebug()) {
            if (compressed_key_format) |format| direct_block: {
                if (q_seq_len != 1 or attention.attn_or_mask != null) break :direct_block;
                const key_row_bytes = compressedKeyRowBytesForPool(pool, format);
                const encoded_arr = try ensureEncodedKeyArray(self, pool, key, entry, block_tokens, key_row_bytes);

                const direct_block_apply_started_at = monotonicNowNs();
                const maybe_direct = self.data.native_quant.compressedAttentionBlock(&.{
                    .q = q_arr,
                    .encoded_key = encoded_arr,
                    .v = entry_v,
                    .running_max = running_max,
                    .running_sum = running_sum,
                    .running_acc = running_acc,
                    .block_tokens = block_tokens,
                    .num_heads = num_heads,
                    .num_kv_heads = num_kv_heads,
                    .head_dim = head_dim,
                    .key_row_bytes = key_row_bytes,
                    .query_position = query_position_offset,
                    .block_position_offset = block_position_offset,
                    .sliding_window = attention.sliding_window,
                    .format = format,
                    .stream = s,
                }) catch |err| blk: {
                    std.log.debug("MLX compressed attention block unavailable for {s}: {}", .{ @tagName(format), err });
                    break :blk null;
                };
                if (maybe_direct) |direct| {
                    quant_execution_timing_stats.paged_block_setup_nanos += @intCast(monotonicNowNs() - block_setup_started_at);
                    _ = c.mlx_array_free(running_max);
                    _ = c.mlx_array_free(running_sum);
                    _ = c.mlx_array_free(running_acc);
                    running_max = direct.running_max;
                    running_sum = direct.running_sum;
                    running_acc = direct.running_acc;
                    quant_execution_timing_stats.paged_block_apply_nanos += @intCast(monotonicNowNs() - direct_block_apply_started_at);
                    continue;
                }
            }
        }

        var v_r = c.mlx_array_new();
        defer _ = c.mlx_array_free(v_r);
        try mlx.check(c.mlx_reshape(&v_r, entry_v, &kv_shape, 4, s));
        var v_t = c.mlx_array_new();
        defer _ = c.mlx_array_free(v_t);
        try mlx.check(c.mlx_transpose_axes(&v_t, v_r, &perm, 4, s));

        var k_t: c.mlx_array = undefined;
        var k_t_owned = false;
        if (compressed_key_format == null) {
            k_t = try transposedPagedBlockKey(self, entry, block_tokens, expected_width, num_kv_heads, head_dim);
            k_t_owned = true;
        }
        defer {
            if (k_t_owned) _ = c.mlx_array_free(k_t);
        }

        const gqa_block = if (compressed_key_format == null)
            try expandKVHeads(k_t, v_t, 1, block_tokens, num_heads, num_kv_heads, head_dim, s)
        else
            try expandKVHeads(v_t, v_t, 1, block_tokens, num_heads, num_kv_heads, head_dim, s);
        defer {
            if (gqa_block.owned) {
                _ = c.mlx_array_free(gqa_block.k);
                _ = c.mlx_array_free(gqa_block.v);
            }
        }
        quant_execution_timing_stats.paged_block_setup_nanos += @intCast(monotonicNowNs() - block_setup_started_at);

        const block_apply_started_at = monotonicNowNs();
        var scores: c.mlx_array = undefined;
        var scores_owned = false;
        defer {
            if (scores_owned) _ = c.mlx_array_free(scores);
        }
        if (compressed_key_format) |format| {
            const key_row_bytes = compressedKeyRowBytesForPool(pool, format);
            const encoded_arr = try ensureEncodedKeyArray(self, pool, key, entry, block_tokens, key_row_bytes);
            const maybe_compressed_scores = self.data.native_quant.compressedKeyScores(&.{
                .q = q_arr,
                .encoded_key = encoded_arr,
                .q_len = q_seq_len,
                .block_tokens = block_tokens,
                .num_heads = num_heads,
                .num_kv_heads = num_kv_heads,
                .head_dim = head_dim,
                .key_row_bytes = key_row_bytes,
                .format = format,
                .stream = s,
            }) catch |err| blk: {
                std.log.debug("MLX compressed key scores unavailable for {s}: {}", .{ @tagName(format), err });
                break :blk null;
            };
            if (maybe_compressed_scores) |compressed_scores| {
                scores = compressed_scores;
                scores_owned = true;
            } else {
                if (!k_t_owned) {
                    k_t = try transposedPagedBlockKey(self, entry, block_tokens, expected_width, num_kv_heads, head_dim);
                    k_t_owned = true;
                }
                const fallback_gqa_key = try expandKVHeads(k_t, k_t, 1, block_tokens, num_heads, num_kv_heads, head_dim, s);
                defer {
                    if (fallback_gqa_key.owned) {
                        _ = c.mlx_array_free(fallback_gqa_key.k);
                        _ = c.mlx_array_free(fallback_gqa_key.v);
                    }
                }
                scores = c.mlx_array_new();
                scores_owned = true;
                var k_tp = c.mlx_array_new();
                defer _ = c.mlx_array_free(k_tp);
                try mlx.check(c.mlx_transpose_axes(&k_tp, fallback_gqa_key.k, &tp_axes, 4, s));
                try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));
            }
        } else {
            scores = c.mlx_array_new();
            scores_owned = true;
            var k_tp = c.mlx_array_new();
            defer _ = c.mlx_array_free(k_tp);
            try mlx.check(c.mlx_transpose_axes(&k_tp, gqa_block.k, &tp_axes, 4, s));
            try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));
        }

        var scores_scaled = c.mlx_array_new();
        defer _ = c.mlx_array_free(scores_scaled);
        try mlx.check(c.mlx_multiply(&scores_scaled, scores, scale_arr, s));

        const mask_started_at = monotonicNowNs();
        const mask_data = try self.allocator.alloc(f32, q_seq_len * block_tokens);
        defer self.allocator.free(mask_data);
        for (0..q_seq_len) |qi| {
            const query_pos = query_position_offset + qi;
            for (0..block_tokens) |ki| {
                const key_pos = block_position_offset + ki;
                mask_data[qi * block_tokens + ki] = if (allowsPastAttention(attention.sliding_window, query_pos, key_pos) and (key_pos <= query_pos or allowsFutureAttention(attention.attn_or_mask, attention.total_sequence_len, query_pos, key_pos))) 0.0 else -1e9;
            }
        }
        const mask_shape = [_]i32{ 1, 1, @intCast(q_seq_len), @intCast(block_tokens) };
        const causal_mask = mlx.arrayFromFloat32(mask_data, &mask_shape);
        defer _ = c.mlx_array_free(causal_mask);
        quant_execution_timing_stats.paged_mask_nanos += @intCast(monotonicNowNs() - mask_started_at);

        var masked_scores = c.mlx_array_new();
        defer _ = c.mlx_array_free(masked_scores);
        try mlx.check(c.mlx_add(&masked_scores, scores_scaled, causal_mask, s));

        var block_max = c.mlx_array_new();
        defer _ = c.mlx_array_free(block_max);
        try mlx.check(c.mlx_max_axis(&block_max, masked_scores, -1, true, s));

        var new_max = c.mlx_array_new();
        defer _ = c.mlx_array_free(new_max);
        try mlx.check(c.mlx_maximum(&new_max, running_max, block_max, s));

        var prev_delta = c.mlx_array_new();
        defer _ = c.mlx_array_free(prev_delta);
        try mlx.check(c.mlx_subtract(&prev_delta, running_max, new_max, s));
        var prev_scale = c.mlx_array_new();
        defer _ = c.mlx_array_free(prev_scale);
        try mlx.check(c.mlx_exp(&prev_scale, prev_delta, s));

        var shifted = c.mlx_array_new();
        defer _ = c.mlx_array_free(shifted);
        try mlx.check(c.mlx_subtract(&shifted, masked_scores, new_max, s));
        var block_exp = c.mlx_array_new();
        defer _ = c.mlx_array_free(block_exp);
        try mlx.check(c.mlx_exp(&block_exp, shifted, s));

        var block_sum = c.mlx_array_new();
        defer _ = c.mlx_array_free(block_sum);
        try mlx.check(c.mlx_sum_axis(&block_sum, block_exp, -1, true, s));

        var scaled_sum = c.mlx_array_new();
        defer _ = c.mlx_array_free(scaled_sum);
        try mlx.check(c.mlx_multiply(&scaled_sum, running_sum, prev_scale, s));
        var new_sum = c.mlx_array_new();
        defer _ = c.mlx_array_free(new_sum);
        try mlx.check(c.mlx_add(&new_sum, scaled_sum, block_sum, s));

        var scaled_acc = c.mlx_array_new();
        defer _ = c.mlx_array_free(scaled_acc);
        try mlx.check(c.mlx_multiply(&scaled_acc, running_acc, prev_scale, s));
        var weighted_v = c.mlx_array_new();
        defer _ = c.mlx_array_free(weighted_v);
        try mlx.check(c.mlx_matmul(&weighted_v, block_exp, gqa_block.v, s));
        var new_acc = c.mlx_array_new();
        defer _ = c.mlx_array_free(new_acc);
        try mlx.check(c.mlx_add(&new_acc, scaled_acc, weighted_v, s));

        _ = c.mlx_array_free(running_max);
        _ = c.mlx_array_free(running_sum);
        _ = c.mlx_array_free(running_acc);
        const owned_new_max = new_max;
        const owned_new_sum = new_sum;
        const owned_new_acc = new_acc;
        new_max = c.mlx_array_new();
        new_sum = c.mlx_array_new();
        new_acc = c.mlx_array_new();
        running_max = owned_new_max;
        running_sum = owned_new_sum;
        running_acc = owned_new_acc;
        quant_execution_timing_stats.paged_block_apply_nanos += @intCast(monotonicNowNs() - block_apply_started_at);
    }

    if (seen_tokens != kv_seq_len) return error.InvalidPagedKvState;

    var result_4d = c.mlx_array_new();
    defer _ = c.mlx_array_free(result_4d);
    try mlx.check(c.mlx_divide(&result_4d, running_acc, running_sum, s));

    const perm_back = [_]c_int{ 0, 2, 1, 3 };
    var attn_back = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_back);
    try mlx.check(c.mlx_transpose_axes(&attn_back, result_4d, &perm_back, 4, s));

    const flat_shape = [_]c_int{ @intCast(q_seq_len), @intCast(H_q) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));
    _ = c.mlx_array_free(running_max);
    _ = c.mlx_array_free(running_sum);
    _ = c.mlx_array_free(running_acc);
    return self.makeArr(result, true);
}

fn gqaAttentionArrays(
    self: *MlxCompute,
    q_arr: c.mlx_array,
    k_arr: c.mlx_array,
    v_arr: c.mlx_array,
    attn_bias_ct: ?CT,
    attn_or_mask: ?[]const u8,
    sliding_window: usize,
    batch: usize,
    q_seq_len: usize,
    kv_seq_len: usize,
    query_position_offset: usize,
    kv_position_offset: usize,
    num_heads: usize,
    num_kv_heads: usize,
    head_dim: usize,
) !CT {
    if (forceCpuGqaDebug()) {
        return gqaAttentionCpuFallback(
            self,
            q_arr,
            k_arr,
            v_arr,
            attn_bias_ct,
            attn_or_mask,
            sliding_window,
            batch,
            q_seq_len,
            kv_seq_len,
            query_position_offset,
            kv_position_offset,
            num_heads,
            num_kv_heads,
            head_dim,
        );
    }
    const s = self.data.stream;
    const H_q = num_heads * head_dim;
    const mask_seq_len = @max(query_position_offset + q_seq_len, kv_position_offset + kv_seq_len);
    if (c.mlx_array_ndim(q_arr) == 2 and c.mlx_array_ndim(k_arr) == 2 and c.mlx_array_ndim(v_arr) == 2) {
        const q_rows: usize = @intCast(c.mlx_array_dim(q_arr, 0));
        const q_width: usize = @intCast(c.mlx_array_dim(q_arr, 1));
        const k_rows: usize = @intCast(c.mlx_array_dim(k_arr, 0));
        const k_width: usize = @intCast(c.mlx_array_dim(k_arr, 1));
        const v_rows: usize = @intCast(c.mlx_array_dim(v_arr, 0));
        const v_width: usize = @intCast(c.mlx_array_dim(v_arr, 1));
        const expected_q_width = num_heads * head_dim;
        const expected_kv_width = num_kv_heads * head_dim;
        if (q_rows != batch * q_seq_len or q_width != expected_q_width or k_rows != batch * kv_seq_len or k_width != expected_kv_width or v_rows != batch * kv_seq_len or v_width != expected_kv_width) {
            std.log.err(
                "MLX GQA input shape mismatch: q={d}x{d} k={d}x{d} v={d}x{d} expected_q={d}x{d} expected_kv={d}x{d} batch={d} q_seq={d} kv_seq={d} heads={d} kv_heads={d} head_dim={d}",
                .{
                    q_rows,
                    q_width,
                    k_rows,
                    k_width,
                    v_rows,
                    v_width,
                    batch * q_seq_len,
                    expected_q_width,
                    batch * kv_seq_len,
                    expected_kv_width,
                    batch,
                    q_seq_len,
                    kv_seq_len,
                    num_heads,
                    num_kv_heads,
                    head_dim,
                },
            );
            return error.GqaInputShapeMismatch;
        }
    }

    const q_shape = [_]c_int{ @intCast(batch), @intCast(q_seq_len), @intCast(num_heads), @intCast(head_dim) };
    const perm = [_]c_int{ 0, 2, 1, 3 };

    var q_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_r);
    try mlx.check(c.mlx_reshape(&q_r, q_arr, &q_shape, 4, s));
    var q_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(q_t);
    try mlx.check(c.mlx_transpose_axes(&q_t, q_r, &perm, 4, s));

    const kv_shape = [_]c_int{ @intCast(batch), @intCast(kv_seq_len), @intCast(num_kv_heads), @intCast(head_dim) };
    var k_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_r);
    try mlx.check(c.mlx_reshape(&k_r, k_arr, &kv_shape, 4, s));
    var k_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_t);
    try mlx.check(c.mlx_transpose_axes(&k_t, k_r, &perm, 4, s));

    var v_r = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_r);
    try mlx.check(c.mlx_reshape(&v_r, v_arr, &kv_shape, 4, s));
    var v_t = c.mlx_array_new();
    defer _ = c.mlx_array_free(v_t);
    try mlx.check(c.mlx_transpose_axes(&v_t, v_r, &perm, 4, s));

    const scale_val: f32 = 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim)));

    // Use fused SDPA when:
    // 1. No attention bias (bias requires combining with causal mask manually)
    // 2. No position offsets (offsets shift the causal window, not supported by "causal" mode)
    // 3. Q and KV sequence lengths are equal (standard self-attention alignment)
    // The fused kernel handles GQA natively via head-dimension broadcasting.
    const can_use_fused = !disableFusedSdpaDebug() and
        attn_bias_ct == null and
        attn_or_mask == null and
        sliding_window == 0 and
        query_position_offset == 0 and kv_position_offset == 0 and
        q_seq_len == kv_seq_len;

    if (can_use_fused) {
        var attn_out = c.mlx_array_new();
        defer _ = c.mlx_array_free(attn_out);
        try mlx.check(c.mlx_fast_scaled_dot_product_attention(
            &attn_out,
            q_t,
            k_t,
            v_t,
            scale_val,
            "causal",
            .{ .ctx = null }, // no explicit mask array
            .{ .ctx = null }, // no sinks
            s,
        ));

        const perm_back = [_]c_int{ 0, 2, 1, 3 };
        var attn_back = c.mlx_array_new();
        defer _ = c.mlx_array_free(attn_back);
        try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &perm_back, 4, s));

        const flat_shape = [_]c_int{ @intCast(batch * q_seq_len), @intCast(H_q) };
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));
        return self.makeArr(result, true);
    }

    // Fallback: manual attention for cases with bias, position offsets,
    // or mismatched Q/KV sequence lengths (e.g., paged KV cache decode).
    const gqa_result = try expandKVHeads(k_t, v_t, batch, kv_seq_len, num_heads, num_kv_heads, head_dim, s);
    defer {
        if (gqa_result.owned) {
            _ = c.mlx_array_free(gqa_result.k);
            _ = c.mlx_array_free(gqa_result.v);
        }
    }

    const tp_axes = [_]c_int{ 0, 1, 3, 2 };

    var k_tp = c.mlx_array_new();
    defer _ = c.mlx_array_free(k_tp);
    try mlx.check(c.mlx_transpose_axes(&k_tp, gqa_result.k, &tp_axes, 4, s));

    var scores = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores);
    try mlx.check(c.mlx_matmul(&scores, q_t, k_tp, s));

    const scale_arr = c.mlx_array_new_float(scale_val);
    defer _ = c.mlx_array_free(scale_arr);
    var scores_scaled = c.mlx_array_new();
    defer _ = c.mlx_array_free(scores_scaled);
    try mlx.check(c.mlx_multiply(&scores_scaled, scores, scale_arr, s));

    const scores_after_bias = try applyAttnBias(attn_bias_ct, scores_scaled, num_heads, q_seq_len, kv_seq_len, s);
    defer _ = c.mlx_array_free(scores_after_bias);

    const causal_mask_data = try self.allocator.alloc(f32, q_seq_len * kv_seq_len);
    defer self.allocator.free(causal_mask_data);
    for (0..q_seq_len) |qi| {
        const query_pos = query_position_offset + qi;
        for (0..kv_seq_len) |ki| {
            const key_pos = kv_position_offset + ki;
            causal_mask_data[qi * kv_seq_len + ki] = if (allowsPastAttention(sliding_window, query_pos, key_pos) and (key_pos <= query_pos or allowsFutureAttention(attn_or_mask, mask_seq_len, query_pos, key_pos))) 0.0 else -1e9;
        }
    }
    const mask_shape = [_]i32{ 1, 1, @intCast(q_seq_len), @intCast(kv_seq_len) };
    const causal_mask = mlx.arrayFromFloat32(causal_mask_data, &mask_shape);
    defer _ = c.mlx_array_free(causal_mask);

    var masked = c.mlx_array_new();
    defer _ = c.mlx_array_free(masked);
    try mlx.check(c.mlx_add(&masked, scores_after_bias, causal_mask, s));

    var attn_weights = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_weights);
    try mlx.check(c.mlx_softmax_axis(&attn_weights, masked, -1, true, s));

    var attn_out = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_out);
    try mlx.check(c.mlx_matmul(&attn_out, attn_weights, gqa_result.v, s));

    const perm_back = [_]c_int{ 0, 2, 1, 3 };
    var attn_back = c.mlx_array_new();
    defer _ = c.mlx_array_free(attn_back);
    try mlx.check(c.mlx_transpose_axes(&attn_back, attn_out, &perm_back, 4, s));

    const flat_shape = [_]c_int{ @intCast(batch * q_seq_len), @intCast(H_q) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, attn_back, &flat_shape, 2, s));
    return self.makeArr(result, true);
}

fn ropeConsecutivePairsRows(
    output: []f32,
    positions: []const f32,
    total_dim_local: usize,
    head_dim: usize,
    theta: f32,
    freq_scale: f32,
) void {
    const num_heads = total_dim_local / head_dim;
    const half = head_dim / 2;
    for (0..positions.len) |tok| {
        const pos = positions[tok];
        const row_base = tok * total_dim_local;
        for (0..num_heads) |head| {
            const base = row_base + head * head_dim;
            for (0..half) |j| {
                const freq = 1.0 / std.math.pow(f32, theta, @as(f32, @floatFromInt(2 * j)) / @as(f32, @floatFromInt(head_dim)));
                const angle = pos * freq_scale * freq;
                const cos_val = @cos(angle);
                const sin_val = @sin(angle);
                const idx0 = 2 * j;
                const idx1 = 2 * j + 1;
                const x0 = output[base + idx0];
                const x1 = output[base + idx1];
                output[base + idx0] = x0 * cos_val - x1 * sin_val;
                output[base + idx1] = x0 * sin_val + x1 * cos_val;
            }
        }
    }
}

test "ropeConsecutivePairsRows rotates every head in each row" {
    var data = [_]f32{
        1, 2,  3,  4,  5,  6,  7,  8,
        9, 10, 11, 12, 13, 14, 15, 16,
    };
    const positions = [_]f32{ 1, 2 };
    ropeConsecutivePairsRows(&data, &positions, 8, 4, 10000.0, 1.0);

    var expected = [_]f32{
        1, 2,  3,  4,  5,  6,  7,  8,
        9, 10, 11, 12, 13, 14, 15, 16,
    };
    const expanded_positions = [_]usize{ 1, 1, 2, 2 };
    @import("native_compute.zig").ropeCore(&expected, &expanded_positions, 4, 4, 10000.0, 1.0, true);

    for (expected, data) |exp, got| {
        try std.testing.expectApproxEqAbs(exp, got, 1e-5);
    }
}

fn allowsFutureAttention(attn_or_mask: ?[]const u8, total_sequence_len: usize, query_pos: usize, key_pos: usize) bool {
    const mask = attn_or_mask orelse return false;
    if (query_pos >= total_sequence_len or key_pos >= total_sequence_len) return false;
    return mask[query_pos * total_sequence_len + key_pos] != 0;
}

fn allowsPastAttention(sliding_window: usize, query_pos: usize, key_pos: usize) bool {
    if (key_pos > query_pos) return false;
    if (sliding_window == 0) return true;
    return query_pos - key_pos < sliding_window;
}

fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const shape = [_]i32{@intCast(data.len)};
    const arr = mlx.arrayFromFloat32(data, &shape);
    const final_arr = try self.castToBf16IfMixed(arr);
    return self.makeArr(final_arr, true);
}

fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.fromFloat32Shape(data, shape);
}

fn fromInt32ShapeOp(ctx: *anyopaque, data: []const i32, shape: []const i32) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr = mlx.arrayFromInt32(data, shape);
    return self.makeArr(arr, true);
}

fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (toArr(tensor).host_f32) |data| {
        return allocator.dupe(f32, data);
    }
    const arr = getArr(tensor);
    if (forceRowWiseToFloat32Debug() and c.mlx_array_ndim(arr) == 2) {
        const rows: usize = @intCast(c.mlx_array_dim(arr, 0));
        const cols: usize = @intCast(c.mlx_array_dim(arr, 1));
        const out = try allocator.alloc(f32, rows * cols);
        errdefer allocator.free(out);
        for (0..rows) |row| {
            const row_arr = try sliceRows(self, arr, row, 1);
            defer _ = c.mlx_array_free(row_arr);
            const row_values = try mlx.readFloat32(row_arr, allocator);
            defer allocator.free(row_values);
            @memcpy(out[row * cols ..][0..cols], row_values[0..cols]);
        }
        return out;
    }
    var casted = c.mlx_array_new();
    defer _ = c.mlx_array_free(casted);
    try mlx.check(c.mlx_astype(&casted, arr, c.MLX_FLOAT32, self.data.stream));
    return mlx.readFloat32(casted, allocator);
}

fn splitLastDim3Op(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!ops.SplitLastDim3Result {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr = getArr(input);
    if (c.mlx_array_ndim(arr) != 2) return error.UnexpectedOutputShape;
    const total_rows: usize = @intCast(c.mlx_array_dim(arr, 0));
    const total_cols: usize = @intCast(c.mlx_array_dim(arr, 1));
    if (total_rows != rows or total_cols != dim * 3) return error.UnexpectedOutputShape;

    const first = try sliceColumns(self, arr, 0, dim);
    errdefer _ = c.mlx_array_free(first);
    const second = try sliceColumns(self, arr, dim, dim);
    errdefer _ = c.mlx_array_free(second);
    const third = try sliceColumns(self, arr, dim * 2, dim);
    errdefer _ = c.mlx_array_free(third);

    return .{
        .first = try self.makeArr(first, true),
        .second = try self.makeArr(second, true),
        .third = try self.makeArr(third, true),
    };
}

fn reshape2DOp(ctx: *anyopaque, input: CT, old_rows: usize, old_cols: usize, new_rows: usize, new_cols: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr = getArr(input);
    if (c.mlx_array_ndim(arr) != 2) return error.UnexpectedOutputShape;
    const actual_rows: usize = @intCast(c.mlx_array_dim(arr, 0));
    const actual_cols: usize = @intCast(c.mlx_array_dim(arr, 1));
    if (actual_rows != old_rows or actual_cols != old_cols) return error.UnexpectedOutputShape;
    if (old_rows * old_cols != new_rows * new_cols) return error.UnexpectedOutputShape;

    const shape = [_]c_int{ @intCast(new_rows), @intCast(new_cols) };
    var reshaped = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&reshaped, arr, &shape, 2, self.data.stream));
    return self.makeArr(reshaped, true);
}

fn concatRows2DOp(ctx: *anyopaque, a: CT, b: CT, rows_a: usize, rows_b: usize, cols: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const lhs = getArr(a);
    const rhs = getArr(b);
    if (c.mlx_array_ndim(lhs) != 2 or c.mlx_array_ndim(rhs) != 2) return error.UnexpectedOutputShape;
    if (@as(usize, @intCast(c.mlx_array_dim(lhs, 0))) != rows_a or @as(usize, @intCast(c.mlx_array_dim(lhs, 1))) != cols) return error.UnexpectedOutputShape;
    if (@as(usize, @intCast(c.mlx_array_dim(rhs, 0))) != rows_b or @as(usize, @intCast(c.mlx_array_dim(rhs, 1))) != cols) return error.UnexpectedOutputShape;
    const inputs = [_]c.mlx_array{ lhs, rhs };
    const input_vec = c.mlx_vector_array_new_data(&inputs, inputs.len);
    defer _ = c.mlx_vector_array_free(input_vec);
    var concatenated = c.mlx_array_new();
    try mlx.check(c.mlx_concatenate_axis(&concatenated, input_vec, 0, self.data.stream));
    return self.makeArr(concatenated, true);
}

fn sliceRows2DOp(ctx: *anyopaque, input: CT, start_row: usize, row_count: usize, cols: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr = getArr(input);
    if (c.mlx_array_ndim(arr) != 2) return error.UnexpectedOutputShape;
    const actual_rows: usize = @intCast(c.mlx_array_dim(arr, 0));
    const actual_cols: usize = @intCast(c.mlx_array_dim(arr, 1));
    if (actual_cols != cols) return error.UnexpectedOutputShape;
    if (start_row + row_count > actual_rows) return error.UnexpectedOutputShape;
    const sliced = try sliceRows(self, arr, start_row, row_count);
    return self.makeArr(sliced, true);
}

fn tensorShapeOp(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 {
    const arr = toArr(tensor);
    if (arr.quantized_storage) |storage| return allocator.dupe(i64, storage.shape);
    if (arr.lazy_entry) |entry| {
        if (entry.quantized_storage) |*storage| return allocator.dupe(i64, storage.shape);
        if (entry.host_loaded) |*loaded| return allocator.dupe(i64, loaded.tensor.shape);
        if (entry.loaded) |loaded| return mlxArrayShape(loaded, allocator);
    }

    return mlxArrayShape(arr.arr, allocator);
}

fn mlxArrayShape(arr: c.mlx_array, allocator: std.mem.Allocator) anyerror![]i64 {
    const ndim = c.mlx_array_ndim(arr);
    const shape = try allocator.alloc(i64, @intCast(ndim));
    errdefer allocator.free(shape);
    for (shape, 0..) |*dim, axis| {
        dim.* = @intCast(c.mlx_array_dim(arr, @intCast(axis)));
    }
    return shape;
}

fn evalTensorOp(_: *anyopaque, tensor: CT) anyerror!void {
    try mlx.evalArray(getArr(tensor));
}

/// Batch-download multiple tensors to f32 using a single MLX eval call.
/// Builds one mlx_vector_array containing all (casted) arrays, calls mlx_eval
/// once, then reads each array out individually. This issues a single Metal
/// command buffer submission instead of one per tensor.
fn toFloat32BatchOp(ctx: *anyopaque, cts: []const CT, allocator: std.mem.Allocator) anyerror![][]f32 {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));

    // Allocate an array of casted handles parallel to cts.
    // casted[i] is non-null only when we had to insert a dtype conversion.
    // Initialize to null so the defer cleanup is safe even on partial init.
    const casted = try allocator.alloc(?c.mlx_array, cts.len);
    @memset(casted, null);
    defer {
        for (casted) |mc| {
            if (mc) |arr| _ = c.mlx_array_free(arr);
        }
        allocator.free(casted);
    }
    // read_arrs[i] is the array we will actually read from (either original or casted).
    const read_arrs = try allocator.alloc(c.mlx_array, cts.len);
    defer allocator.free(read_arrs);

    for (cts, 0..) |ct, i| {
        // host_f32 tensors don't need an MLX eval — handle them in the read loop.
        read_arrs[i] = getArr(ct);
        if (c.mlx_array_dtype(read_arrs[i]) != c.MLX_FLOAT32) {
            var tmp = c.mlx_array_new();
            try mlx.check(c.mlx_astype(&tmp, read_arrs[i], c.MLX_FLOAT32, self.data.stream));
            casted[i] = tmp;
            read_arrs[i] = tmp;
        }
    }

    // Single MLX eval: materialize all arrays in one GPU round-trip.
    const eval_vec = c.mlx_vector_array_new();
    defer _ = c.mlx_vector_array_free(eval_vec);
    for (cts, 0..) |ct, i| {
        // Skip host_f32 tensors — they are already on the CPU.
        if (toArr(ct).host_f32 != null) continue;
        _ = c.mlx_vector_array_append_value(eval_vec, read_arrs[i]);
    }
    if (c.mlx_eval(eval_vec) != 0) return error.MlxEvalFailed;

    // Allocate the outer results slice.
    const results = try allocator.alloc([]f32, cts.len);
    var done: usize = 0;
    errdefer {
        for (results[0..done]) |r| allocator.free(r);
        allocator.free(results);
    }

    // Download each array (no per-array eval needed — already materialized).
    for (cts, 0..) |ct, i| {
        if (toArr(ct).host_f32) |data| {
            results[i] = try allocator.dupe(f32, data);
        } else {
            const arr = read_arrs[i];
            const size = c.mlx_array_size(arr);
            const ptr = c.mlx_array_data_float32(arr);
            if (ptr == null) return error.MlxDataNull;
            results[i] = try allocator.alloc(f32, size);
            @memcpy(results[i], ptr[0..size]);
        }
        done += 1;
    }

    return results;
}

fn reshape2dOp(ctx: *anyopaque, input: CT, rows: usize, cols: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const shape = [_]i32{ @intCast(rows), @intCast(cols) };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, getArr(input), &shape, 2, s));
    return self.makeArr(result, true);
}

fn sliceLastDimOp(ctx: *anyopaque, input: CT, start: usize, stop: usize) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const arr = getArr(input);
    const starts = [_]i32{ 0, @intCast(start) };
    const stops = [_]i32{ c.mlx_array_dim(arr, 0), @intCast(stop) };
    const strides = [_]i32{ 1, 1 };
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_slice(&result, arr, &starts, 2, &stops, 2, &strides, 2, s));
    return self.makeArr(result, true);
}

fn argmaxLastRowOp(ctx: *anyopaque, tensor: CT, rows: usize, dim: usize) anyerror!?u32 {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr = getArr(tensor);
    if (rows == 0 or dim == 0) return error.InvalidTensorShape;
    if (c.mlx_array_ndim(arr) != 2) return error.InvalidTensorShape;
    if (@as(usize, @intCast(c.mlx_array_dim(arr, 0))) != rows) return error.InvalidTensorShape;
    if (@as(usize, @intCast(c.mlx_array_dim(arr, 1))) != dim) return error.InvalidTensorShape;

    const row_argmax = try mlx.argmaxAxis(arr, 1, false);
    defer _ = c.mlx_array_free(row_argmax);
    const ids = try mlx.readInt32(row_argmax, self.allocator);
    defer self.allocator.free(ids);
    if (ids.len != rows) return error.InvalidTensorShape;
    return @intCast(ids[rows - 1]);
}

fn sampleLastRowOp(ctx: *anyopaque, request: *const ops.SampleLastRowRequest) anyerror!?u32 {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const arr = getArr(request.tensor);
    if (request.rows == 0 or request.dim == 0) return error.InvalidTensorShape;
    if (c.mlx_array_ndim(arr) != 2) return error.InvalidTensorShape;
    if (@as(usize, @intCast(c.mlx_array_dim(arr, 0))) != request.rows) return error.InvalidTensorShape;
    if (@as(usize, @intCast(c.mlx_array_dim(arr, 1))) != request.dim) return error.InvalidTensorShape;

    const last_row = if (request.rows == 1) arr else try sliceRows(self, arr, request.rows - 1, 1);
    defer {
        if (request.rows != 1) _ = c.mlx_array_free(last_row);
    }

    return if (try self.data.native_quant.sampleLogitsDevice(&.{
        .input = last_row,
        .out_dim = request.dim,
        .temperature = request.temperature,
        .top_k = request.top_k,
        .top_p = request.top_p,
        .min_p = request.min_p,
        .repetition_penalty = request.repetition_penalty,
        .frequency_penalty = request.frequency_penalty,
        .presence_penalty = request.presence_penalty,
        .token_history = request.token_history,
    })) |token_id| @intCast(token_id) else null;
}

fn linearNoBiasArgmaxLastRowTensorOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (!enableMetalLmHeadArgmaxDebug()) return null;
    if (rows == 0 or in_dim == 0 or out_dim == 0) return error.InvalidTensorShape;

    const input_arr = getArr(input);
    const weight_arr = toArr(weight);
    const weight_mlx = getArr(weight);
    if (weight_arr.quantized_storage != null) return null;
    if (weight_mlx.ctx == null) return null;
    if (c.mlx_array_dtype(input_arr) != c.MLX_FLOAT32) return null;
    if (c.mlx_array_dtype(weight_mlx) != c.MLX_FLOAT32) return null;
    if (c.mlx_array_ndim(input_arr) != 2 or c.mlx_array_ndim(weight_mlx) != 2) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(input_arr, 0))) != rows) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(input_arr, 1))) != in_dim) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(weight_mlx, 0))) != out_dim) return null;
    if (@as(usize, @intCast(c.mlx_array_dim(weight_mlx, 1))) != in_dim) return null;

    const token_arr = (try self.data.native_quant.lmHeadArgmax(&.{
        .hidden = input_arr,
        .weight = weight_mlx,
        .rows = rows,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .stream = self.data.stream,
    })) orelse return null;

    return self.makeArr(token_arr, true);
}

fn linearNoBiasArgmaxLastRowOp(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?u32 {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const token_tensor = (try linearNoBiasArgmaxLastRowTensorOp(ctx, input, weight, rows, in_dim, out_dim)) orelse return null;
    defer freeTensor(ctx, token_tensor);

    const ids = try mlx.readFloat32(getArr(token_tensor), self.allocator);
    defer self.allocator.free(ids);
    if (ids.len != 1 or ids[0] < 0) return error.InvalidTensorShape;
    return @intFromFloat(ids[0]);
}

fn decoderRuntimePrepareGreedyOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeGreedyRequest) anyerror!bool {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.data.native_quant.decoderRuntimePrepareGreedy(&.{
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .num_layers = request.num_layers,
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
        .vocab_size = request.vocab_size,
        .kv_tokens = request.kv_tokens,
    });
}

fn decoderRuntimeResetStateOp(ctx: *anyopaque) anyerror!void {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.data.native_quant.decoderRuntimeResetState();
}

fn gatherPagedKvLayerCacheOp(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    kv: ops.KvCacheView,
    token_count: usize,
    layer_index: usize,
) anyerror!?ops.PagedKvLayerCacheRows {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try self.gatherPagedKvLayerFromCache(allocator, kv, token_count, layer_index);
}

fn seedPagedKvLayerCacheOp(
    ctx: *anyopaque,
    kv: ops.KvCacheView,
    token_count: usize,
    layer_index: usize,
    k_rows_host: []const f32,
    v_rows_host: []const f32,
) anyerror!bool {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    try self.seedPagedKvLayerCache(kv, token_count, layer_index, k_rows_host, v_rows_host);
    return true;
}

fn directFamilyTimingSnapshotOp(ctx: *anyopaque) ops.DirectFamilyTimingSnapshot {
    _ = ctx;
    return snapshotDirectFamilyTiming();
}

fn debugTimingSnapshotOp(ctx: *anyopaque) ops.BackendDebugTimingSnapshot {
    const self: *const MlxCompute = @ptrCast(@alignCast(ctx));
    return .{
        .native_quant_null = mlx_quant.isNullProvider(self.data.native_quant),
        .provider = mlx_quant.getTimingStats(),
        .quant = quant_execution_timing_stats,
    };
}

fn resetDebugTimingStatsOp(ctx: *anyopaque) void {
    _ = ctx;
    mlx_quant.resetTimingStats();
    resetQuantExecutionTimingStats();
}

fn decoderRuntimeReadyOp(ctx: *anyopaque) bool {
    const self: *const MlxCompute = @ptrCast(@alignCast(ctx));
    return mlx_quant.decoderRuntimeReady(self.data.native_quant);
}

fn decoderRuntimeAbsoluteEmbeddingsPreparedOp(ctx: *anyopaque) bool {
    const self: *const MlxCompute = @ptrCast(@alignCast(ctx));
    const native_provider = mlx_quant.metalProvider(self.data.native_quant) orelse return false;
    return metal_runtime.decoderRuntimeAbsoluteEmbeddingsPrepared(native_provider);
}

fn decoderRuntimePrepareOrReuseFamilyOp(
    ctx: *anyopaque,
    allocator: std.mem.Allocator,
    gpt_config: @import("../models/gpt.zig").Config,
    current_kv_tokens: usize,
    configured_layer_count: usize,
) anyerror!ops.DecoderRuntimePrepareReuseResult {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try prepareOrReuseDecoderRuntimeFamily(
        &self.computeBackend(),
        allocator,
        gpt_config,
        current_kv_tokens,
        configured_layer_count,
    );
}

fn decoderRuntimePrepareAbsoluteEmbeddingsOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareAbsoluteEmbeddingsRequest) anyerror!bool {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.data.native_quant.decoderRuntimePrepareAbsoluteEmbeddings(&.{
        .token_embedding = getArr(request.token_embedding),
        .position_embedding = getArr(request.position_embedding),
        .vocab_size = request.vocab_size,
        .max_position_embeddings = request.max_position_embeddings,
        .hidden_size = request.hidden_size,
    });
}

fn decoderRuntimeEmbedAbsolutePositionOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeEmbedAbsolutePositionRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeEmbedAbsolutePosition(&.{
        .token_id = request.token_id,
        .position_id = request.position_id,
        .hidden_size = request.hidden_size,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimePrepareLayerNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareLayerNormRequest) anyerror!bool {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.data.native_quant.decoderRuntimePrepareLayerNorm(&.{
        .weight = getArr(request.weight),
        .bias = getArr(request.bias),
        .slot = request.slot,
        .hidden_size = request.hidden_size,
    });
}

fn decoderRuntimeApplyLayerNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyLayerNorm(&.{
        .input = getArr(request.input),
        .slot = request.slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimePrepareRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareRmsNormRequest) anyerror!bool {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.data.native_quant.decoderRuntimePrepareRmsNorm(&.{
        .weight = getArr(request.weight),
        .slot = request.slot,
        .hidden_size = request.hidden_size,
    });
}

fn decoderRuntimeApplyRmsNormOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyRmsNorm(&.{
        .input = getArr(request.input),
        .slot = request.slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimeApplyLayerNormLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormLinearArgmaxRequest) anyerror!?usize {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try self.data.native_quant.decoderRuntimeApplyLayerNormLinearArgmax(&.{
        .input = getArr(request.input),
        .norm_slot = request.norm_slot,
        .linear_slot = request.linear_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
        .out_dim = request.out_dim,
    });
}

fn decoderRuntimeApplyLayerNormLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormLinearRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyLayerNormLinear(&.{
        .input = getArr(request.input),
        .norm_slot = request.norm_slot,
        .linear_slot = request.linear_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
        .out_dim = request.out_dim,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimeApplyLayerNormLinearSampleOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLayerNormLinearSampleRequest) anyerror!?usize {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try self.data.native_quant.decoderRuntimeApplyLayerNormLinearSample(&.{
        .input = getArr(request.input),
        .norm_slot = request.norm_slot,
        .linear_slot = request.linear_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
        .out_dim = request.out_dim,
        .temperature = request.temperature,
        .top_k = request.top_k,
        .top_p = request.top_p,
        .min_p = request.min_p,
        .repetition_penalty = request.repetition_penalty,
        .frequency_penalty = request.frequency_penalty,
        .presence_penalty = request.presence_penalty,
        .token_history = request.token_history,
    });
}

fn decoderRuntimeApplyRmsNormLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormLinearArgmaxRequest) anyerror!?usize {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try self.data.native_quant.decoderRuntimeApplyRmsNormLinearArgmax(&.{
        .input = getArr(request.input),
        .norm_slot = request.norm_slot,
        .linear_slot = request.linear_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
        .out_dim = request.out_dim,
    });
}

fn decoderRuntimeApplyRmsNormLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormLinearRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyRmsNormLinear(&.{
        .input = getArr(request.input),
        .norm_slot = request.norm_slot,
        .linear_slot = request.linear_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
        .out_dim = request.out_dim,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimeApplyRmsNormLinearSampleOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyRmsNormLinearSampleRequest) anyerror!?usize {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try self.data.native_quant.decoderRuntimeApplyRmsNormLinearSample(&.{
        .input = getArr(request.input),
        .norm_slot = request.norm_slot,
        .linear_slot = request.linear_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
        .out_dim = request.out_dim,
        .temperature = request.temperature,
        .top_k = request.top_k,
        .top_p = request.top_p,
        .min_p = request.min_p,
        .repetition_penalty = request.repetition_penalty,
        .frequency_penalty = request.frequency_penalty,
        .presence_penalty = request.presence_penalty,
        .token_history = request.token_history,
    });
}

fn decoderRuntimePrepareLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimePrepareLinearRequest) anyerror!bool {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return self.data.native_quant.decoderRuntimePrepareLinear(&.{
        .weight = getArr(request.weight),
        .bias = getArr(request.bias),
        .quantized_storage = toArr(request.weight).quantized_storage,
        .slot = request.slot,
        .in_dim = request.in_dim,
        .out_dim = request.out_dim,
        .retain_dense_fallback = request.retain_dense_fallback,
    });
}

fn decoderRuntimeApplyLinearOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyLinear(&.{
        .input = getArr(request.input),
        .slot = request.slot,
        .in_dim = request.in_dim,
        .out_dim = request.out_dim,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimeApplyLinearArgmaxOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearArgmaxRequest) anyerror!?usize {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    return try self.data.native_quant.decoderRuntimeApplyLinearArgmax(&.{
        .input = getArr(request.input),
        .slot = request.slot,
        .in_dim = request.in_dim,
        .out_dim = request.out_dim,
    });
}

fn decoderRuntimeApplyLinearPairOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearPairRequest) anyerror!?ops.LinearNoBiasPairResult {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const result = (try self.data.native_quant.decoderRuntimeApplyLinearPair(&.{
        .input = getArr(request.input),
        .slot_a = request.slot_a,
        .slot_b = request.slot_b,
        .in_dim = request.in_dim,
        .out_dim = request.out_dim,
    })) orelse return null;
    return .{
        .first = try self.makeArr(result.first, true),
        .second = try self.makeArr(result.second, true),
    };
}

fn decoderRuntimeApplyLinearQkvOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyLinearQkvRequest) anyerror!?ops.LinearNoBiasTripleResult {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const result = (try self.data.native_quant.decoderRuntimeApplyLinearQkv(&.{
        .input = getArr(request.input),
        .q_slot = request.q_slot,
        .k_slot = request.k_slot,
        .v_slot = request.v_slot,
        .in_dim = request.in_dim,
        .q_out_dim = request.q_out_dim,
        .kv_out_dim = request.kv_out_dim,
    })) orelse return null;
    return .{
        .first = try self.makeArr(result.first, true),
        .second = try self.makeArr(result.second, true),
        .third = try self.makeArr(result.third, true),
    };
}

fn decoderRuntimeApplyAddOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyAddRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyAdd(&.{
        .lhs = getArr(request.lhs),
        .rhs = getArr(request.rhs),
        .dim = request.dim,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn decoderRuntimeApplyActivationOp(ctx: *anyopaque, request: *const ops.DecoderRuntimeApplyActivationRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.decoderRuntimeApplyActivation(&.{
        .input = getArr(request.input),
        .kind = request.kind,
        .dim = request.dim,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn runDenseFfnResidualOp(ctx: *anyopaque, request: *const ops.RunDenseFfnResidualRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.runDenseFfnResidual(&.{
        .input = getArr(request.input),
        .residual = getArr(request.residual),
        .first_linear_slot = request.first_linear_slot,
        .second_linear_slot = request.second_linear_slot,
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .activation = request.activation,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn runGatedFfnResidualOp(ctx: *anyopaque, request: *const ops.RunGatedFfnResidualRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const hidden = (try self.data.native_quant.runGatedFfnResidual(&.{
        .input = getArr(request.input),
        .residual = getArr(request.residual),
        .gate_linear_slot = request.gate_linear_slot,
        .up_linear_slot = request.up_linear_slot,
        .down_linear_slot = request.down_linear_slot,
        .post_gate_rms_norm_slot = request.post_gate_rms_norm_slot,
        .post_down_rms_norm_slot = request.post_down_rms_norm_slot,
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .eps = request.eps,
        .activation = request.activation,
    })) orelse return null;
    return self.makeArr(hidden, true);
}

fn runAttentionOp(ctx: *anyopaque, request: *const ops.RunAttentionRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    if (!useMetalDecoderRuntimeFastPath(self)) return null;
    var attention = request.attention;
    if (request.attention_sink.hasMetadata()) attention.attention_sink = request.attention_sink;
    if (attention.attention_sink.hasMetadata()) return null;
    if (attention.mode != .paged_decode) return null;
    if (attention.query_sequence_len != 1 or attention.attn_or_mask != null) return null;
    if (attention.kv_manager == null and attention.kv_storage == null) return null;
    _ = attention.kv_cache orelse return null;
    if (attention.decoder_runtime_resident_kv_sequence_len != null and
        attention.decoder_runtime_resident_kv_position_offset != null)
    {
        const prior_kv_tokens = attention.decoder_runtime_resident_kv_sequence_len.?;
        const prior_kv_position_offset = attention.decoder_runtime_resident_kv_position_offset.?;
        const prior_end = prior_kv_position_offset + prior_kv_tokens;
        const current_end = attention.kv_position_offset + attention.kv_sequence_len;
        if (attention.kv_position_offset < prior_kv_position_offset or current_end < prior_end) {
            try self.data.native_quant.decoderRuntimeResetState();
        }
    }
    try updatePagedKvBlocks(self, getArr(request.k), getArr(request.v), attention);
    if (try gqaPagedCompressedAttentionSpan(
        self,
        getArr(request.q),
        getArr(request.k),
        getArr(request.v),
        attention,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
    )) |result| {
        return result;
    }
    return try gqaPagedAttentionDirect(
        self,
        getArr(request.q),
        attention,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
    );
}

fn runAttentionResidualOp(ctx: *anyopaque, request: *const ops.RunAttentionResidualRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var attention = request.attention;
    if (request.attention_sink.hasMetadata()) attention.attention_sink = request.attention_sink;
    const attention_input_size = request.num_heads * request.head_dim;
    const attention_core_started_at = monotonicNowNs();
    const attn_out = (try runAttentionOp(ctx, &.{
        .q = request.q,
        .k = request.k,
        .v = request.v,
        .attention = attention,
        .attention_sink = request.attention_sink,
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
    })) orelse try gqaPagedAttentionOp(
        ctx,
        request.q,
        request.k,
        request.v,
        null,
        attention,
        1,
        request.num_heads,
        request.num_kv_heads,
        request.head_dim,
    );
    const attention_core_finished_at = monotonicNowNs();
    if (attention_core_finished_at > attention_core_started_at) {
        quant_execution_timing_stats.attention_residual_core_nanos += @intCast(attention_core_finished_at - attention_core_started_at);
    }
    var current = attn_out;
    const free_current = true;
    defer if (free_current) freeTensor(ctx, current);

    if (request.pre_linear_rms_norm_slot != null or request.post_linear_rms_norm_slot != null) {
        const post_linear_fast_started_at = monotonicNowNs();
        if (try self.data.native_quant.runAttentionResidualPostLinear(&.{
            .attention_input = getArr(current),
            .residual = getArr(request.residual),
            .attention_input_size = attention_input_size,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
            .attention_linear_slot = request.linear_slot,
            .attention_pre_linear_rms_norm_slot = request.pre_linear_rms_norm_slot,
            .attention_post_linear_rms_norm_slot = request.post_linear_rms_norm_slot,
        })) |hidden| {
            const post_linear_fast_finished_at = monotonicNowNs();
            if (post_linear_fast_finished_at > post_linear_fast_started_at) {
                quant_execution_timing_stats.attention_residual_post_linear_fast_nanos += @intCast(post_linear_fast_finished_at - post_linear_fast_started_at);
            }
            return self.makeArr(hidden, true);
        }
        const post_linear_fast_finished_at = monotonicNowNs();
        if (post_linear_fast_finished_at > post_linear_fast_started_at) {
            quant_execution_timing_stats.attention_residual_post_linear_fast_nanos += @intCast(post_linear_fast_finished_at - post_linear_fast_started_at);
        }
    }

    const post_linear_fallback_started_at = monotonicNowNs();
    if (request.pre_linear_rms_norm_slot) |slot| {
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot,
            .input = current,
            .hidden_size = attention_input_size,
            .eps = request.eps,
        })) orelse return null;
        freeTensor(ctx, current);
        current = normed;
    }

    const projected = (try decoderRuntimeApplyLinearOp(ctx, &.{
        .slot = request.linear_slot,
        .input = current,
        .in_dim = attention_input_size,
        .out_dim = request.hidden_size,
    })) orelse return null;
    freeTensor(ctx, current);
    current = projected;

    if (request.post_linear_rms_norm_slot) |slot| {
        const normed = (try decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot,
            .input = current,
            .hidden_size = request.hidden_size,
            .eps = request.eps,
        })) orelse return null;
        freeTensor(ctx, current);
        current = normed;
    }

    const result = try addOp(ctx, current, request.residual);
    const post_linear_fallback_finished_at = monotonicNowNs();
    if (post_linear_fallback_finished_at > post_linear_fallback_started_at) {
        quant_execution_timing_stats.attention_residual_post_linear_fallback_nanos += @intCast(post_linear_fallback_finished_at - post_linear_fallback_started_at);
    }
    return result;
}

fn decoderRuntimeApplyBlockFfnNorm(
    ctx: *anyopaque,
    input: CT,
    layer_norm_slot: ?usize,
    rms_norm_slot: ?usize,
    hidden_size: usize,
    eps: f32,
) anyerror!?CT {
    if (layer_norm_slot) |slot| {
        return decoderRuntimeApplyLayerNormOp(ctx, &.{
            .slot = slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        });
    }
    if (rms_norm_slot) |slot| {
        return decoderRuntimeApplyRmsNormOp(ctx, &.{
            .slot = slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        });
    }
    return input;
}

fn runDenseDecoderBlockOp(ctx: *anyopaque, request: *const ops.RunDenseDecoderBlockRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const attention = request.attention;
    var q_tensor = request.q;
    var k_tensor = request.k;
    var v_tensor = request.v;
    var owns_q = false;
    var owns_k = false;
    var owns_v = false;
    defer if (owns_q and q_tensor != null) freeTensor(ctx, q_tensor.?);
    defer if (owns_k and k_tensor != null) freeTensor(ctx, k_tensor.?);
    defer if (owns_v and v_tensor != null) freeTensor(ctx, v_tensor.?);

    const can_project_from_attention_input = request.attention_input != null and request.fused_qkv_linear_slot != null;
    if (can_project_from_attention_input) {
        const q_dim = request.num_heads * request.head_dim;
        const kv_dim = request.num_kv_heads * request.head_dim;
        const fused_qkv = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.fused_qkv_linear_slot.?,
            .input = request.attention_input.?,
            .in_dim = request.hidden_size,
            .out_dim = q_dim + kv_dim * 2,
        })) orelse return null;
        errdefer freeTensor(ctx, fused_qkv);
        const q = try sliceLastDimOp(ctx, fused_qkv, 0, q_dim);
        errdefer freeTensor(ctx, q);
        const k = try sliceLastDimOp(ctx, fused_qkv, q_dim, q_dim + kv_dim);
        errdefer freeTensor(ctx, k);
        const v = try sliceLastDimOp(ctx, fused_qkv, q_dim + kv_dim, q_dim + kv_dim * 2);
        freeTensor(ctx, fused_qkv);
        q_tensor = q;
        k_tensor = k;
        v_tensor = v;
        owns_q = true;
        owns_k = true;
        owns_v = true;
    }
    const q_ct = q_tensor orelse return null;
    const k_ct = k_tensor orelse return null;
    const v_ct = v_tensor orelse return null;

    if (useMetalDecoderRuntimeFastPath(self) and
        (attention.mode == .paged_decode or attention.mode == .paged_prefill) and
        attention.attn_or_mask == null and
        (attention.kv_manager != null or attention.kv_storage != null) and
        attention.kv_cache != null)
    {
        quant_execution_timing_stats.dense_block_fast_attempts += 1;
        const source = try attentionKvSource(attention);
        const block_ids = source.block_ids;
        if (block_ids.len > 0) {
            const pool = source.pool;
            const format: ?mlx_quant.CompressedKeyFormat = switch (pool.config.dtype) {
                .polar4 => .polar4,
                .turbo3 => .turbo3,
                else => null,
            };
            if (format) |compressed_format| {
                if (attention.decoder_runtime_resident_kv_sequence_len != null and
                    attention.decoder_runtime_resident_kv_position_offset != null)
                {
                    const prior_kv_tokens = attention.decoder_runtime_resident_kv_sequence_len.?;
                    const prior_kv_position_offset = attention.decoder_runtime_resident_kv_position_offset.?;
                    const prior_end = prior_kv_position_offset + prior_kv_tokens;
                    const current_end = attention.kv_position_offset + attention.kv_sequence_len;
                    if (attention.kv_position_offset < prior_kv_position_offset or current_end < prior_end) {
                        try self.data.native_quant.decoderRuntimeResetState();
                    }
                }
                try updatePagedKvBlocks(self, getArr(k_ct), getArr(v_ct), attention);
                const key_row_bytes = compressedKeyRowBytesForPool(pool, compressed_format);
                const base_request: mlx_quant.RunCompressedAttentionDenseDecoderBlockRequest = .{
                    .q = getArr(q_ct),
                    .k_suffix = getArr(k_ct),
                    .v_suffix = getArr(v_ct),
                    .bootstrap_k_blocks = &.{},
                    .bootstrap_v_blocks = &.{},
                    .bootstrap_block_token_counts = &.{},
                    .source_ptr_id = source.ptr_id,
                    .sequence_id = attention.kv_cache.?.sequence_id,
                    .layer_index = attention.layer_index,
                    .query_sequence_len = attention.query_sequence_len,
                    .kv_tokens = attention.kv_sequence_len,
                    .num_heads = request.num_heads,
                    .num_kv_heads = request.num_kv_heads,
                    .head_dim = request.head_dim,
                    .key_row_bytes = key_row_bytes,
                    .query_position = attention.total_sequence_len - 1,
                    .kv_position_offset = attention.kv_position_offset,
                    .sliding_window = attention.sliding_window,
                    .format = compressed_format,
                    .attention_linear_slot = request.attention_linear_slot,
                    .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
                    .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
                    .residual = getArr(request.residual),
                    .hidden_size = request.hidden_size,
                    .eps = request.eps,
                    .ffn_layer_norm_slot = request.ffn_layer_norm_slot,
                    .ffn_rms_norm_slot = request.ffn_rms_norm_slot,
                    .first_ffn_linear_slot = request.first_ffn_linear_slot,
                    .second_ffn_linear_slot = request.second_ffn_linear_slot,
                    .intermediate_size = request.intermediate_size,
                    .activation = request.activation,
                };
                const hidden_fast = (try self.data.native_quant.runCompressedAttentionDenseDecoderBlock(&base_request)) orelse null;
                if (hidden_fast) |arr| return self.makeArr(arr, true);
                var had_bootstrap_cache = false;
                if (attention.query_sequence_len < attention.kv_sequence_len) {
                    const bootstrap = gatherPagedKvBlockBootstrap(self, attention) catch null;
                    if (bootstrap) |owned_bootstrap| {
                        had_bootstrap_cache = true;
                        var boot = owned_bootstrap;
                        defer boot.deinit(self.allocator);
                        const hidden_bootstrap = (try self.data.native_quant.runCompressedAttentionDenseDecoderBlock(&.{
                            .q = base_request.q,
                            .k_suffix = base_request.k_suffix,
                            .v_suffix = base_request.v_suffix,
                            .bootstrap_k_blocks = boot.k_blocks,
                            .bootstrap_v_blocks = boot.v_blocks,
                            .bootstrap_block_token_counts = boot.token_counts,
                            .source_ptr_id = base_request.source_ptr_id,
                            .sequence_id = base_request.sequence_id,
                            .layer_index = base_request.layer_index,
                            .query_sequence_len = base_request.query_sequence_len,
                            .kv_tokens = base_request.kv_tokens,
                            .num_heads = base_request.num_heads,
                            .num_kv_heads = base_request.num_kv_heads,
                            .head_dim = base_request.head_dim,
                            .key_row_bytes = base_request.key_row_bytes,
                            .query_position = base_request.query_position,
                            .kv_position_offset = base_request.kv_position_offset,
                            .sliding_window = base_request.sliding_window,
                            .format = base_request.format,
                            .attention_linear_slot = base_request.attention_linear_slot,
                            .attention_pre_linear_rms_norm_slot = base_request.attention_pre_linear_rms_norm_slot,
                            .attention_post_linear_rms_norm_slot = base_request.attention_post_linear_rms_norm_slot,
                            .residual = base_request.residual,
                            .hidden_size = base_request.hidden_size,
                            .eps = base_request.eps,
                            .ffn_layer_norm_slot = base_request.ffn_layer_norm_slot,
                            .ffn_rms_norm_slot = base_request.ffn_rms_norm_slot,
                            .first_ffn_linear_slot = base_request.first_ffn_linear_slot,
                            .second_ffn_linear_slot = base_request.second_ffn_linear_slot,
                            .intermediate_size = base_request.intermediate_size,
                            .activation = base_request.activation,
                        })) orelse null;
                        if (hidden_bootstrap) |arr| return self.makeArr(arr, true);
                    }
                    if (!had_bootstrap_cache and metadataOnlyBlockBootstrapRequired(attention)) {
                        return error.KvColdMissRequiresCachedBlock;
                    }
                }
            } else {
                quant_execution_timing_stats.dense_block_fast_unsupported_format += 1;
            }
        } else {
            quant_execution_timing_stats.dense_block_fast_no_blocks += 1;
        }
    }

    const attn_res = (try runAttentionResidualOp(ctx, &.{
        .q = q_ct,
        .k = k_ct,
        .v = v_ct,
        .residual = request.residual,
        .attention = request.attention,
        .attention_sink = request.attention.attention_sink,
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
        .linear_slot = request.attention_linear_slot,
        .pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
        .post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    })) orelse return null;
    defer freeTensor(ctx, attn_res);

    const ffn_normed = (try decoderRuntimeApplyBlockFfnNorm(
        ctx,
        attn_res,
        request.ffn_layer_norm_slot,
        request.ffn_rms_norm_slot,
        request.hidden_size,
        request.eps,
    )) orelse return null;
    defer if (ffn_normed != attn_res) freeTensor(ctx, ffn_normed);

    return runDenseFfnResidualOp(ctx, &.{
        .first_linear_slot = request.first_ffn_linear_slot,
        .second_linear_slot = request.second_ffn_linear_slot,
        .input = ffn_normed,
        .residual = attn_res,
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .activation = request.activation,
    });
}

fn runGatedDecoderBlockOp(ctx: *anyopaque, request: *const ops.RunGatedDecoderBlockRequest) anyerror!?CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const attention = request.attention;
    var q_tensor = request.q;
    var k_tensor = request.k;
    var v_tensor = request.v;
    var owns_q = false;
    var owns_k = false;
    var owns_v = false;
    defer if (owns_q and q_tensor != null) freeTensor(ctx, q_tensor.?);
    defer if (owns_k and k_tensor != null) freeTensor(ctx, k_tensor.?);
    defer if (owns_v and v_tensor != null) freeTensor(ctx, v_tensor.?);

    const can_project_from_attention_input = request.attention_input != null and
        request.q_linear_slot != null and
        request.k_linear_slot != null and
        request.v_linear_slot != null;

    if (can_project_from_attention_input) {
        const project_started_at = monotonicNowNs();
        const kv_projected = (try decoderRuntimeApplyLinearPairOp(ctx, &.{
            .slot_a = request.k_linear_slot.?,
            .slot_b = request.v_linear_slot.?,
            .input = request.attention_input.?,
            .in_dim = request.hidden_size,
            .out_dim = request.num_kv_heads * request.head_dim,
        })) orelse {
            quant_execution_timing_stats.gated_input_project_failures += 1;
            return null;
        };

        quant_execution_timing_stats.gated_input_projection_nanos += @intCast(monotonicNowNs() - project_started_at);
        k_tensor = kv_projected.first;
        v_tensor = kv_projected.second;
        owns_k = true;
        owns_v = true;
    }
    const k = k_tensor orelse return null;
    const v = v_tensor orelse return null;

    if (useMetalDecoderRuntimeFastPath(self) and
        (attention.mode == .paged_decode or attention.mode == .paged_prefill) and
        attention.query_sequence_len == 1 and
        attention.attn_or_mask == null and
        (attention.kv_manager != null or attention.kv_storage != null) and
        attention.kv_cache != null)
    {
        quant_execution_timing_stats.gated_block_fast_attempts += 1;
        const source = try attentionKvSource(attention);
        const block_ids = source.block_ids;
        if (block_ids.len > 0) {
            const pool = source.pool;
            const format: ?mlx_quant.CompressedKeyFormat = switch (pool.config.dtype) {
                .polar4 => .polar4,
                .turbo3 => .turbo3,
                else => null,
            };
            if (format) |compressed_format| {
                if (attention.decoder_runtime_resident_kv_sequence_len != null and
                    attention.decoder_runtime_resident_kv_position_offset != null)
                {
                    const prior_kv_tokens = attention.decoder_runtime_resident_kv_sequence_len.?;
                    const prior_kv_position_offset = attention.decoder_runtime_resident_kv_position_offset.?;
                    const prior_end = prior_kv_position_offset + prior_kv_tokens;
                    const current_end = attention.kv_position_offset + attention.kv_sequence_len;
                    if (attention.kv_position_offset < prior_kv_position_offset or current_end < prior_end) {
                        try self.data.native_quant.decoderRuntimeResetState();
                    }
                }
                try updatePagedKvBlocks(self, getArr(k), getArr(v), attention);
                const key_row_bytes = compressedKeyRowBytesForPool(pool, compressed_format);
                const base_request: mlx_quant.RunCompressedAttentionGatedDecoderBlockRequest = .{
                    .q = if (q_tensor) |q_arr| getArr(q_arr) else null,
                    .k_suffix = getArr(k),
                    .v_suffix = getArr(v),
                    .attention_input = if (q_tensor == null) getArr(request.attention_input.?) else null,
                    .q_linear_slot = if (q_tensor == null) request.q_linear_slot else null,
                    .k_linear_slot = null,
                    .v_linear_slot = null,
                    .bootstrap_k_blocks = &.{},
                    .bootstrap_v_blocks = &.{},
                    .bootstrap_block_token_counts = &.{},
                    .source_ptr_id = source.ptr_id,
                    .sequence_id = attention.kv_cache.?.sequence_id,
                    .layer_index = attention.layer_index,
                    .query_sequence_len = attention.query_sequence_len,
                    .kv_tokens = attention.kv_sequence_len,
                    .num_heads = request.num_heads,
                    .num_kv_heads = request.num_kv_heads,
                    .head_dim = request.head_dim,
                    .key_row_bytes = key_row_bytes,
                    .query_position = attention.total_sequence_len - 1,
                    .kv_position_offset = attention.kv_position_offset,
                    .sliding_window = attention.sliding_window,
                    .format = compressed_format,
                    .attention_linear_slot = request.attention_linear_slot,
                    .attention_pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
                    .attention_post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
                    .residual = getArr(request.residual),
                    .hidden_size = request.hidden_size,
                    .eps = request.eps,
                    .ffn_layer_norm_slot = request.ffn_layer_norm_slot,
                    .ffn_rms_norm_slot = request.ffn_rms_norm_slot,
                    .ffn_post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
                    .ffn_post_down_rms_norm_slot = request.ffn_post_down_rms_norm_slot,
                    .gate_ffn_linear_slot = request.gate_ffn_linear_slot,
                    .up_ffn_linear_slot = request.up_ffn_linear_slot,
                    .down_ffn_linear_slot = request.down_ffn_linear_slot,
                    .intermediate_size = request.intermediate_size,
                    .activation = request.activation,
                };
                const should_try_cold_fast = !(attention.query_sequence_len < attention.kv_sequence_len and
                    !self.data.native_quant.hasGatheredSpanCache(
                        source.ptr_id,
                        attention.kv_cache.?.sequence_id,
                        attention.layer_index,
                    ));
                const hidden_fast = if (should_try_cold_fast)
                    (try self.data.native_quant.runCompressedAttentionGatedDecoderBlock(&base_request)) orelse null
                else
                    null;
                if (hidden_fast) |arr| return self.makeArr(arr, true);
                var had_bootstrap_cache = false;
                if (attention.query_sequence_len < attention.kv_sequence_len) {
                    const full_span = gatherPagedKvArraysFromBlockCache(self, attention) catch try gatherPagedKvArrays(self, attention);
                    defer if (full_span.owned) {
                        _ = c.mlx_array_free(full_span.k);
                        _ = c.mlx_array_free(full_span.v);
                    };
                    const hidden_full = (try self.data.native_quant.runCompressedAttentionGatedDecoderBlock(&.{
                        .q = base_request.q,
                        .k_suffix = base_request.k_suffix,
                        .v_suffix = base_request.v_suffix,
                        .bootstrap_k_blocks = &.{},
                        .bootstrap_v_blocks = &.{},
                        .bootstrap_block_token_counts = &.{},
                        .full_k = full_span.k,
                        .full_v = full_span.v,
                        .source_ptr_id = base_request.source_ptr_id,
                        .sequence_id = base_request.sequence_id,
                        .layer_index = base_request.layer_index,
                        .query_sequence_len = base_request.query_sequence_len,
                        .kv_tokens = base_request.kv_tokens,
                        .num_heads = base_request.num_heads,
                        .num_kv_heads = base_request.num_kv_heads,
                        .head_dim = base_request.head_dim,
                        .key_row_bytes = base_request.key_row_bytes,
                        .query_position = base_request.query_position,
                        .kv_position_offset = base_request.kv_position_offset,
                        .sliding_window = base_request.sliding_window,
                        .format = base_request.format,
                        .attention_linear_slot = base_request.attention_linear_slot,
                        .attention_pre_linear_rms_norm_slot = base_request.attention_pre_linear_rms_norm_slot,
                        .attention_post_linear_rms_norm_slot = base_request.attention_post_linear_rms_norm_slot,
                        .residual = base_request.residual,
                        .hidden_size = base_request.hidden_size,
                        .eps = base_request.eps,
                        .ffn_layer_norm_slot = base_request.ffn_layer_norm_slot,
                        .ffn_rms_norm_slot = base_request.ffn_rms_norm_slot,
                        .ffn_post_gate_rms_norm_slot = base_request.ffn_post_gate_rms_norm_slot,
                        .ffn_post_down_rms_norm_slot = base_request.ffn_post_down_rms_norm_slot,
                        .gate_ffn_linear_slot = base_request.gate_ffn_linear_slot,
                        .up_ffn_linear_slot = base_request.up_ffn_linear_slot,
                        .down_ffn_linear_slot = base_request.down_ffn_linear_slot,
                        .intermediate_size = base_request.intermediate_size,
                        .activation = base_request.activation,
                    })) orelse null;
                    if (hidden_full) |arr| return self.makeArr(arr, true);
                    const bootstrap = gatherPagedKvBlockBootstrap(self, attention) catch null;
                    if (bootstrap) |owned_bootstrap| {
                        had_bootstrap_cache = true;
                        var boot = owned_bootstrap;
                        defer boot.deinit(self.allocator);
                        const hidden_bootstrap = (try self.data.native_quant.runCompressedAttentionGatedDecoderBlock(&.{
                            .q = base_request.q,
                            .k_suffix = base_request.k_suffix,
                            .v_suffix = base_request.v_suffix,
                            .bootstrap_k_blocks = boot.k_blocks,
                            .bootstrap_v_blocks = boot.v_blocks,
                            .bootstrap_block_token_counts = boot.token_counts,
                            .source_ptr_id = base_request.source_ptr_id,
                            .sequence_id = base_request.sequence_id,
                            .layer_index = base_request.layer_index,
                            .query_sequence_len = base_request.query_sequence_len,
                            .kv_tokens = base_request.kv_tokens,
                            .num_heads = base_request.num_heads,
                            .num_kv_heads = base_request.num_kv_heads,
                            .head_dim = base_request.head_dim,
                            .key_row_bytes = base_request.key_row_bytes,
                            .query_position = base_request.query_position,
                            .kv_position_offset = base_request.kv_position_offset,
                            .sliding_window = base_request.sliding_window,
                            .format = base_request.format,
                            .attention_linear_slot = base_request.attention_linear_slot,
                            .attention_pre_linear_rms_norm_slot = base_request.attention_pre_linear_rms_norm_slot,
                            .attention_post_linear_rms_norm_slot = base_request.attention_post_linear_rms_norm_slot,
                            .residual = base_request.residual,
                            .hidden_size = base_request.hidden_size,
                            .eps = base_request.eps,
                            .ffn_layer_norm_slot = base_request.ffn_layer_norm_slot,
                            .ffn_rms_norm_slot = base_request.ffn_rms_norm_slot,
                            .ffn_post_gate_rms_norm_slot = base_request.ffn_post_gate_rms_norm_slot,
                            .ffn_post_down_rms_norm_slot = base_request.ffn_post_down_rms_norm_slot,
                            .gate_ffn_linear_slot = base_request.gate_ffn_linear_slot,
                            .up_ffn_linear_slot = base_request.up_ffn_linear_slot,
                            .down_ffn_linear_slot = base_request.down_ffn_linear_slot,
                            .intermediate_size = base_request.intermediate_size,
                            .activation = base_request.activation,
                        })) orelse null;
                        if (hidden_bootstrap) |arr| return self.makeArr(arr, true);
                    }
                    if (!had_bootstrap_cache and metadataOnlyBlockBootstrapRequired(attention)) {
                        return error.KvColdMissRequiresCachedBlock;
                    }
                }
            } else {
                quant_execution_timing_stats.gated_block_fast_unsupported_format += 1;
            }
        } else {
            quant_execution_timing_stats.gated_block_fast_no_blocks += 1;
        }
    }

    if (q_tensor == null and can_project_from_attention_input) {
        const q_project_started_at = monotonicNowNs();
        const q_projected = (try decoderRuntimeApplyLinearOp(ctx, &.{
            .slot = request.q_linear_slot.?,
            .input = request.attention_input.?,
            .in_dim = request.hidden_size,
            .out_dim = request.num_heads * request.head_dim,
        })) orelse {
            quant_execution_timing_stats.gated_input_project_failures += 1;
            return null;
        };
        quant_execution_timing_stats.gated_input_projection_nanos += @intCast(monotonicNowNs() - q_project_started_at);
        q_tensor = q_projected;
        owns_q = true;
    }

    const q = q_tensor orelse return null;

    const attention_started_at = monotonicNowNs();
    const attn_res = (try runAttentionResidualOp(ctx, &.{
        .q = q,
        .k = k,
        .v = v,
        .residual = request.residual,
        .attention = request.attention,
        .attention_sink = request.attention.attention_sink,
        .num_heads = request.num_heads,
        .num_kv_heads = request.num_kv_heads,
        .head_dim = request.head_dim,
        .linear_slot = request.attention_linear_slot,
        .pre_linear_rms_norm_slot = request.attention_pre_linear_rms_norm_slot,
        .post_linear_rms_norm_slot = request.attention_post_linear_rms_norm_slot,
        .hidden_size = request.hidden_size,
        .eps = request.eps,
    })) orelse {
        if (request.attention_input != null) {
            quant_execution_timing_stats.gated_input_attention_failures += 1;
        } else {
            quant_execution_timing_stats.gated_qkv_attention_failures += 1;
        }
        return null;
    };
    if (request.attention_input != null) {
        quant_execution_timing_stats.gated_input_attention_nanos += @intCast(monotonicNowNs() - attention_started_at);
    } else {
        quant_execution_timing_stats.gated_qkv_attention_nanos += @intCast(monotonicNowNs() - attention_started_at);
    }
    defer freeTensor(ctx, attn_res);

    const ffn_norm_started_at = monotonicNowNs();
    const ffn_normed = (try decoderRuntimeApplyBlockFfnNorm(
        ctx,
        attn_res,
        request.ffn_layer_norm_slot,
        request.ffn_rms_norm_slot,
        request.hidden_size,
        request.eps,
    )) orelse {
        if (request.attention_input != null) {
            quant_execution_timing_stats.gated_input_ffn_norm_failures += 1;
        } else {
            quant_execution_timing_stats.gated_qkv_ffn_norm_failures += 1;
        }
        return null;
    };
    if (request.attention_input != null) {
        quant_execution_timing_stats.gated_input_ffn_norm_nanos += @intCast(monotonicNowNs() - ffn_norm_started_at);
    } else {
        quant_execution_timing_stats.gated_qkv_ffn_norm_nanos += @intCast(monotonicNowNs() - ffn_norm_started_at);
    }
    defer if (ffn_normed != attn_res) freeTensor(ctx, ffn_normed);

    const ffn_started_at = monotonicNowNs();
    const ffn_out = (try runGatedFfnResidualOp(ctx, &.{
        .gate_linear_slot = request.gate_ffn_linear_slot,
        .up_linear_slot = request.up_ffn_linear_slot,
        .down_linear_slot = request.down_ffn_linear_slot,
        .input = ffn_normed,
        .residual = attn_res,
        .post_gate_rms_norm_slot = request.ffn_post_gate_rms_norm_slot,
        .post_down_rms_norm_slot = request.ffn_post_down_rms_norm_slot,
        .hidden_size = request.hidden_size,
        .intermediate_size = request.intermediate_size,
        .activation = request.activation,
    })) orelse {
        if (request.attention_input != null) {
            quant_execution_timing_stats.gated_input_ffn_failures += 1;
        } else {
            quant_execution_timing_stats.gated_qkv_ffn_failures += 1;
        }
        return null;
    };
    if (request.attention_input != null) {
        quant_execution_timing_stats.gated_input_ffn_nanos += @intCast(monotonicNowNs() - ffn_started_at);
    } else {
        quant_execution_timing_stats.gated_qkv_ffn_nanos += @intCast(monotonicNowNs() - ffn_started_at);
    }
    return ffn_out;
}

// ── Primitive VTable ops for training / lowered graph execution ──────

fn primSubtractOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_subtract(&result, getArr(a), getArr(b), self.data.stream));
    return self.makeArr(result, true);
}

fn primDivideOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_divide(&result, getArr(a), getArr(b), self.data.stream));
    return self.makeArr(result, true);
}

fn primNegateOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_negative(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primSqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_sqrt(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primRsqrtOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_rsqrt(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primExpOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_exp(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primLogOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_log(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primSinOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_sin(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primCosOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_cos(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primTanhOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_tanh(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primErfOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_erf(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primAbsOp(ctx: *anyopaque, a: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_abs(&result, getArr(a), self.data.stream));
    return self.makeArr(result, true);
}

fn primLessThanOp(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    // mlx_less returns bool array; cast to f32 (1.0/0.0) for VTable contract.
    var cmp = c.mlx_array_new();
    defer _ = c.mlx_array_free(cmp);
    try mlx.check(c.mlx_less(&cmp, getArr(a), getArr(b), s));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_astype(&result, cmp, c.MLX_FLOAT32, s));
    return self.makeArr(result, true);
}

fn primWhereSelectOp(ctx: *anyopaque, cond: CT, on_true: CT, on_false: CT) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_where(&result, getArr(cond), getArr(on_true), getArr(on_false), self.data.stream));
    return self.makeArr(result, true);
}

fn mlxAutoReshapeReduce(arr: c.mlx_array, axes: []const u8, input_shape: []const i64, s: c.mlx_stream) !?c.mlx_array {
    const ndim: usize = @intCast(c.mlx_array_ndim(arr));
    for (axes) |ax| {
        if (ax >= ndim) {
            const mlx_size: usize = @intCast(c.mlx_array_size(arr));
            var declared_size: usize = 1;
            for (input_shape) |d| declared_size *= @intCast(d);
            if (mlx_size == declared_size and input_shape.len > 0) {
                var shape_i32: [8]c_int = undefined;
                for (input_shape, 0..) |d, i| shape_i32[i] = @intCast(d);
                var reshaped = c.mlx_array_new();
                try mlx.check(c.mlx_reshape(&reshaped, arr, &shape_i32, input_shape.len, s));
                return reshaped;
            }
            return error.ReduceAxisOutOfBounds;
        }
    }
    return null;
}

fn primReduceSumOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const reshaped = try mlxAutoReshapeReduce(getArr(input), axes, input_shape, s);
    const actual = reshaped orelse getArr(input);
    defer if (reshaped != null) {
        _ = c.mlx_array_free(reshaped.?);
    };
    var axes_i32: [8]c_int = undefined;
    for (axes, 0..) |ax, i| axes_i32[i] = @intCast(ax);
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_sum_axes(&result, actual, &axes_i32, axes.len, false, s));
    return self.makeArr(result, true);
}

fn primReduceMaxOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const reshaped = try mlxAutoReshapeReduce(getArr(input), axes, input_shape, s);
    const actual = reshaped orelse getArr(input);
    defer if (reshaped != null) {
        _ = c.mlx_array_free(reshaped.?);
    };
    var axes_i32: [8]c_int = undefined;
    for (axes, 0..) |ax, i| axes_i32[i] = @intCast(ax);
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_max_axes(&result, actual, &axes_i32, axes.len, false, s));
    return self.makeArr(result, true);
}

fn primReduceMeanOp(ctx: *anyopaque, input: CT, axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const reshaped = try mlxAutoReshapeReduce(getArr(input), axes, input_shape, s);
    const actual = reshaped orelse getArr(input);
    defer if (reshaped != null) {
        _ = c.mlx_array_free(reshaped.?);
    };
    var axes_i32: [8]c_int = undefined;
    for (axes, 0..) |ax, i| axes_i32[i] = @intCast(ax);
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_mean_axes(&result, actual, &axes_i32, axes.len, false, s));
    return self.makeArr(result, true);
}

fn primReshapeOp(ctx: *anyopaque, input: CT, new_shape: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var shape_i32: [8]c_int = undefined;
    for (new_shape, 0..) |d, i| shape_i32[i] = @intCast(d);
    var contiguous = c.mlx_array_new();
    defer _ = c.mlx_array_free(contiguous);
    try mlx.check(c.mlx_contiguous(&contiguous, getArr(input), false, self.data.stream));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_reshape(&result, contiguous, &shape_i32, new_shape.len, self.data.stream));
    return self.makeArr(result, true);
}

fn primTransposeOp(ctx: *anyopaque, input: CT, perm: []const u8, _: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    var perm_i32: [8]c_int = undefined;
    for (perm, 0..) |p, i| perm_i32[i] = @intCast(p);
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_transpose_axes(&result, getArr(input), &perm_i32, perm.len, self.data.stream));
    return self.makeArr(result, true);
}

fn primBroadcastInDimOp(ctx: *anyopaque, input: CT, target_shape: []const i64, broadcast_axes: []const u8, input_shape: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const arr = getArr(input);
    const target_rank = target_shape.len;

    // If the MLX array already has the target shape (or is broadcastable
    // to it via numpy rules), skip the manual reshape + broadcast and
    // let MLX handle it directly. This handles cases where the actual
    // tensor shape diverges from the graph's declared input_shape.
    const actual_ndim: usize = @intCast(c.mlx_array_ndim(arr));
    const actual_size: usize = @intCast(c.mlx_array_size(arr));

    // Compute target size.
    var target_size: usize = 1;
    for (target_shape) |d| target_size *= @intCast(d);

    // Fast path: actual tensor already matches target.
    if (actual_size == target_size) {
        var shape_i32: [8]c_int = undefined;
        for (target_shape, 0..) |d, i| shape_i32[i] = @intCast(d);
        var result = c.mlx_array_new();
        try mlx.check(c.mlx_reshape(&result, arr, &shape_i32, target_rank, s));
        return self.makeArr(result, true);
    }

    // Build intermediate shape using declared input_shape.
    const input_rank = input_shape.len;
    var inter_shape: [8]c_int = undefined;
    for (0..target_rank) |d| inter_shape[d] = 1;
    for (broadcast_axes, 0..) |ax, i| {
        if (i < input_rank) {
            inter_shape[ax] = @intCast(input_shape[i]);
        }
    }

    // Check if intermediate reshape is feasible with the actual tensor.
    var inter_size: usize = 1;
    for (0..target_rank) |d| inter_size *= @intCast(inter_shape[d]);

    if (actual_size != inter_size) {
        // Size mismatch — use actual MLX dims at broadcast_axes positions.
        for (0..target_rank) |d| inter_shape[d] = 1;
        if (actual_ndim == 1) {
            // 1D tensor: put its entire size at the first broadcast axis.
            if (broadcast_axes.len > 0) {
                inter_shape[broadcast_axes[0]] = @intCast(actual_size);
            }
        } else {
            // Multi-dim: use actual MLX dims.
            for (0..@min(actual_ndim, broadcast_axes.len)) |i| {
                inter_shape[broadcast_axes[i]] = c.mlx_array_dim(arr, @intCast(i));
            }
        }
    }

    var reshaped = c.mlx_array_new();
    defer _ = c.mlx_array_free(reshaped);
    try mlx.check(c.mlx_reshape(&reshaped, arr, &inter_shape, target_rank, s));

    var target_i32: [8]c_int = undefined;
    for (target_shape, 0..) |d, i| target_i32[i] = @intCast(d);
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_broadcast_to(&result, reshaped, &target_i32, target_rank, s));
    return self.makeArr(result, true);
}

fn primDotGeneralOp(ctx: *anyopaque, lhs: CT, rhs: CT, lhs_shape: []const i64, rhs_shape: []const i64, lhs_contracting: []const u8, rhs_contracting: []const u8, lhs_batch: []const u8, rhs_batch: []const u8) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    // Reshape inputs to declared shapes if needed. The lowered graph's
    // shapes may differ from the actual MLX array shapes when upstream
    // operations produce differently-shaped tensors via broadcasting.
    var lhs_reshaped: ?c.mlx_array = null;
    defer if (lhs_reshaped) |r| {
        _ = c.mlx_array_free(r);
    };
    var rhs_reshaped: ?c.mlx_array = null;
    defer if (rhs_reshaped) |r| {
        _ = c.mlx_array_free(r);
    };
    const lhs_base = blk: {
        const arr = getArr(lhs);
        const ndim: usize = @intCast(c.mlx_array_ndim(arr));
        const nsize: usize = @intCast(c.mlx_array_size(arr));
        var declared_size: usize = 1;
        for (lhs_shape) |d| declared_size *= @intCast(d);
        if (nsize == declared_size) {
            var matches = ndim == lhs_shape.len;
            if (matches) {
                for (0..ndim) |d| {
                    if (c.mlx_array_dim(arr, @intCast(d)) != @as(c_int, @intCast(lhs_shape[d]))) {
                        matches = false;
                        break;
                    }
                }
            }
            if (!matches) {
                var sh: [8]c_int = undefined;
                for (lhs_shape, 0..) |d, i| sh[i] = @intCast(d);
                var r = c.mlx_array_new();
                if (mlx.check(c.mlx_reshape(&r, arr, &sh, lhs_shape.len, s))) {
                    lhs_reshaped = r;
                    break :blk r;
                } else |_| {
                    _ = c.mlx_array_free(r);
                }
            }
        }
        break :blk arr;
    };
    const rhs_base = blk: {
        const arr = getArr(rhs);
        const ndim: usize = @intCast(c.mlx_array_ndim(arr));
        const nsize: usize = @intCast(c.mlx_array_size(arr));
        var declared_size: usize = 1;
        for (rhs_shape) |d| declared_size *= @intCast(d);
        if (nsize == declared_size) {
            var matches = ndim == rhs_shape.len;
            if (matches) {
                for (0..ndim) |d| {
                    if (c.mlx_array_dim(arr, @intCast(d)) != @as(c_int, @intCast(rhs_shape[d]))) {
                        matches = false;
                        break;
                    }
                }
            }
            if (!matches) {
                var sh: [8]c_int = undefined;
                for (rhs_shape, 0..) |d, i| sh[i] = @intCast(d);
                var r = c.mlx_array_new();
                if (mlx.check(c.mlx_reshape(&r, arr, &sh, rhs_shape.len, s))) {
                    rhs_reshaped = r;
                    break :blk r;
                } else |_| {
                    _ = c.mlx_array_free(r);
                }
            }
        }
        break :blk arr;
    };

    // Common case: 2D matmul, no batch dims, one contracting dim each.
    if (lhs_batch.len == 0 and lhs_contracting.len == 1 and rhs_contracting.len == 1 and
        lhs_shape.len == 2 and rhs_shape.len == 2)
    {
        const lc = lhs_contracting[0];
        const rc = rhs_contracting[0];

        var lhs_arr = lhs_base;
        var rhs_arr = rhs_base;

        // mlx_matmul computes A @ B. We need to transpose inputs to get the
        // right contracting dimension alignment.
        var lhs_t: ?c.mlx_array = null;
        defer if (lhs_t) |t| {
            _ = c.mlx_array_free(t);
        };
        var rhs_t: ?c.mlx_array = null;
        defer if (rhs_t) |t| {
            _ = c.mlx_array_free(t);
        };

        // mlx_matmul contracts last dim of A with second-to-last dim of B.
        // For 2D: A[M,K] @ B[K,N] → C[M,N].
        // If lhs contracts on dim 0, transpose lhs.
        if (lc == 0) {
            var tmp = c.mlx_array_new();
            try mlx.check(c.mlx_transpose(&tmp, lhs_arr, s));
            lhs_t = tmp;
            lhs_arr = tmp;
        }
        // If rhs contracts on dim 1, transpose rhs.
        if (rc == 1) {
            var tmp = c.mlx_array_new();
            try mlx.check(c.mlx_transpose(&tmp, rhs_arr, s));
            rhs_t = tmp;
            rhs_arr = tmp;
        }

        var result = c.mlx_array_new();
        try mlx.check(c.mlx_matmul(&result, lhs_arr, rhs_arr, s));
        return self.makeArr(result, true);
    }

    // Batched case: batch dims present, 1 contracting dim each.
    if (lhs_batch.len >= 1 and lhs_contracting.len == 1 and rhs_contracting.len == 1) {
        // Transpose inputs to standard layout [batch..., M, K] @ [batch..., K, N].
        // The native backend does this on CPU; we do it with MLX transpose + matmul.
        const lhs_rank = lhs_shape.len;
        const rhs_rank = rhs_shape.len;

        // Build permutation for lhs: [batch_dims..., free_dim, contracting_dim]
        var lhs_perm: [8]c_int = undefined;
        var lhs_perm_len: usize = 0;
        for (lhs_batch) |bd| {
            lhs_perm[lhs_perm_len] = @intCast(bd);
            lhs_perm_len += 1;
        }
        // Find free dim (not batch, not contracting).
        for (0..lhs_rank) |d| {
            var is_special = false;
            for (lhs_batch) |bd| {
                if (bd == d) {
                    is_special = true;
                    break;
                }
            }
            if (d == lhs_contracting[0]) is_special = true;
            if (!is_special) {
                lhs_perm[lhs_perm_len] = @intCast(d);
                lhs_perm_len += 1;
            }
        }
        lhs_perm[lhs_perm_len] = @intCast(lhs_contracting[0]);
        lhs_perm_len += 1;

        // Build permutation for rhs: [batch_dims..., contracting_dim, free_dim]
        var rhs_perm: [8]c_int = undefined;
        var rhs_perm_len: usize = 0;
        for (rhs_batch) |bd| {
            rhs_perm[rhs_perm_len] = @intCast(bd);
            rhs_perm_len += 1;
        }
        rhs_perm[rhs_perm_len] = @intCast(rhs_contracting[0]);
        rhs_perm_len += 1;
        for (0..rhs_rank) |d| {
            var is_special = false;
            for (rhs_batch) |bd| {
                if (bd == d) {
                    is_special = true;
                    break;
                }
            }
            if (d == rhs_contracting[0]) is_special = true;
            if (!is_special) {
                rhs_perm[rhs_perm_len] = @intCast(d);
                rhs_perm_len += 1;
            }
        }

        var lhs_tr = c.mlx_array_new();
        defer _ = c.mlx_array_free(lhs_tr);
        try mlx.check(c.mlx_transpose_axes(&lhs_tr, lhs_base, &lhs_perm, lhs_perm_len, s));

        var rhs_tr = c.mlx_array_new();
        defer _ = c.mlx_array_free(rhs_tr);
        try mlx.check(c.mlx_transpose_axes(&rhs_tr, rhs_base, &rhs_perm, rhs_perm_len, s));

        var result = c.mlx_array_new();
        try mlx.check(c.mlx_matmul(&result, lhs_tr, rhs_tr, s));
        return self.makeArr(result, true);
    }

    return error.UnsupportedPrimitiveOp;
}

fn primScatterAddOp(ctx: *anyopaque, input: CT, indices: CT, input_shape: []const i64, indices_shape: []const i64, axis: u8) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));

    // Scatter-add: output[indices[i]] += input[i] along axis.
    // For the common case (axis=0, input [N,D], indices [N]):
    // Create zero output of shape [out_rows, D] and accumulate on CPU.

    if (input_shape.len == 2 and axis == 0) {
        const n = @as(usize, @intCast(input_shape[0]));
        const d = @as(usize, @intCast(input_shape[1]));

        // Determine output rows from indices_shape.
        const out_rows: usize = if (indices_shape.len > 0)
            @as(usize, @intCast(indices_shape[0]))
        else blk: {
            // Fallback: read indices to find max.
            const idx_data = try mlx.readFloat32(getArr(indices), self.allocator);
            defer self.allocator.free(idx_data);
            var max_idx: usize = 0;
            for (idx_data) |v| {
                const idx = @as(usize, @intFromFloat(v));
                if (idx > max_idx) max_idx = idx;
            }
            break :blk max_idx + 1;
        };

        // Fall back to CPU for scatter-add (MLX scatter API is complex).
        const in_data = try mlx.readFloat32(getArr(input), self.allocator);
        defer self.allocator.free(in_data);
        const idx_data = try mlx.readFloat32(getArr(indices), self.allocator);
        defer self.allocator.free(idx_data);

        const output = try self.allocator.alloc(f32, out_rows * d);
        defer self.allocator.free(output);
        @memset(output, 0.0);
        for (0..n) |i| {
            const row = @as(usize, @intFromFloat(idx_data[i]));
            if (row >= out_rows) return error.IndexOutOfBounds;
            for (0..d) |j| {
                output[row * d + j] += in_data[i * d + j];
            }
        }
        const shape = [_]i32{ @intCast(out_rows), @intCast(d) };
        return self.fromFloat32Shape(output, &shape);
    }

    return error.UnsupportedPrimitiveOp;
}

fn primGatherOp(ctx: *anyopaque, input: CT, indices: CT, axis: u8, input_shape: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    // For axis=0, 2D case: input [M, D], indices [N] -> output [N, D].
    // Use mlx_take which gathers rows along an axis.
    if (input_shape.len == 2 and axis == 0) {
        // Convert float indices to int32 for mlx_take.
        const idx_f32 = try mlx.readFloat32(getArr(indices), self.allocator);
        defer self.allocator.free(idx_f32);
        const n = idx_f32.len;
        const idx_i32 = try std.heap.c_allocator.alloc(i32, n);
        for (idx_f32, 0..) |v, i| idx_i32[i] = @intFromFloat(v);
        const idx_shape = [_]i32{@intCast(n)};
        const idx_arr = mlx.arrayFromOwnedInt32(idx_i32, &idx_shape);
        defer _ = c.mlx_array_free(idx_arr);

        var result = c.mlx_array_new();
        try mlx.check(c.mlx_take_axis(&result, getArr(input), idx_arr, 0, s));
        return self.makeArr(result, true);
    }

    return error.UnsupportedPrimitiveOp;
}

fn primSliceOp(ctx: *anyopaque, input: CT, starts: []const i64, limits: []const i64, strides_param: []const i64, _: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    const rank = starts.len;

    var starts_i32: [8]c_int = undefined;
    var stops_i32: [8]c_int = undefined;
    var strides_i32: [8]c_int = undefined;
    for (0..rank) |d| {
        starts_i32[d] = @intCast(starts[d]);
        stops_i32[d] = @intCast(limits[d]);
        strides_i32[d] = @intCast(strides_param[d]);
    }

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_slice(&result, getArr(input), &starts_i32, rank, &stops_i32, rank, &strides_i32, rank, s));
    return self.makeArr(result, true);
}

fn primConcatPrimOp(ctx: *anyopaque, a: CT, b: CT, axis: u8, _: []const i64, _: []const i64) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;

    const arrays = c.mlx_vector_array_new();
    defer _ = c.mlx_vector_array_free(arrays);
    _ = c.mlx_vector_array_append_value(arrays, getArr(a));
    _ = c.mlx_vector_array_append_value(arrays, getArr(b));

    var result = c.mlx_array_new();
    try mlx.check(c.mlx_concatenate_axis(&result, arrays, @intCast(axis), s));
    return self.makeArr(result, true);
}

fn primSoftmaxOp(ctx: *anyopaque, input: CT, last_dim_size: u32) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    _ = last_dim_size;
    var result = c.mlx_array_new();
    // Softmax along last axis.
    try mlx.check(c.mlx_softmax_axis(&result, getArr(input), -1, true, self.data.stream));
    return self.makeArr(result, true);
}

fn primLogSoftmaxOp(ctx: *anyopaque, input: CT, last_dim_size: u32) anyerror!CT {
    const self: *MlxCompute = @ptrCast(@alignCast(ctx));
    const s = self.data.stream;
    _ = last_dim_size;
    // log_softmax(x) = x - log(sum(exp(x))) = x - max - log(sum(exp(x - max)))
    // Compose via softmax + log.
    var sm = c.mlx_array_new();
    defer _ = c.mlx_array_free(sm);
    try mlx.check(c.mlx_softmax_axis(&sm, getArr(input), -1, true, s));
    var result = c.mlx_array_new();
    try mlx.check(c.mlx_log(&result, sm, s));
    return self.makeArr(result, true);
}

const MockNativeQuantProvider = struct {
    fn planLinearNoBias(_: *anyopaque, _: *const mlx_quant.LinearNoBiasPlanRequest) QuantExecutionPlan {
        return .device_native;
    }

    fn linearNoBias(_: *anyopaque, _: *const mlx_quant.LinearNoBiasRequest) !?c.mlx_array {
        return null;
    }

    fn planLinearNoBiasPair(_: *anyopaque, _: *const mlx_quant.LinearNoBiasPairPlanRequest) QuantExecutionPlan {
        return .unsupported;
    }

    fn linearNoBiasPair(_: *anyopaque, _: *const mlx_quant.LinearNoBiasPairRequest) !?mlx_quant.LinearNoBiasPairResult {
        return null;
    }

    fn mulMatId(_: *anyopaque, _: *const mlx_quant.MulMatIdRequest) !?c.mlx_array {
        return null;
    }

    fn moeLinearNoBias(ctx: *anyopaque, request: *const mlx_quant.MoeLinearNoBiasRequest) !?c.mlx_array {
        return MockNativeQuantProvider.mulMatId(ctx, request);
    }

    fn moeLinearNoBiasPair(_: *anyopaque, _: *const mlx_quant.MoeLinearNoBiasPairRequest) !?mlx_quant.MoeLinearNoBiasPairResult {
        return null;
    }

    fn compressedKeyScores(_: *anyopaque, _: *const mlx_quant.CompressedKeyScoresRequest) !?c.mlx_array {
        return null;
    }

    fn compressedAttentionBlock(_: *anyopaque, _: *const mlx_quant.CompressedAttentionBlockRequest) !?mlx_quant.CompressedAttentionBlockResult {
        return null;
    }

    fn compressedAttentionSpan(_: *anyopaque, _: *const mlx_quant.CompressedAttentionSpanRequest) !?c.mlx_array {
        return null;
    }

    fn lmHeadArgmax(_: *anyopaque, _: *const mlx_quant.LmHeadArgmaxRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimePrepareGreedy(_: *anyopaque, _: *const ops.DecoderRuntimeGreedyRequest) !bool {
        return false;
    }

    fn decoderRuntimeResetState(_: *anyopaque) !void {}

    fn decoderRuntimePrepareAbsoluteEmbeddings(_: *anyopaque, _: *const mlx_quant.DecoderRuntimePrepareAbsoluteEmbeddingsRequest) !bool {
        return false;
    }

    fn decoderRuntimeEmbedAbsolutePosition(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeEmbedAbsolutePositionRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimePrepareLayerNorm(_: *anyopaque, _: *const mlx_quant.DecoderRuntimePrepareLayerNormRequest) !bool {
        return false;
    }

    fn decoderRuntimeApplyLayerNorm(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLayerNormRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimePrepareRmsNorm(_: *anyopaque, _: *const mlx_quant.DecoderRuntimePrepareRmsNormRequest) !bool {
        return false;
    }

    fn decoderRuntimeApplyRmsNorm(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyRmsNormRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyLayerNormLinear(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLayerNormLinearRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyLayerNormLinearSample(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLayerNormLinearSampleRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyLayerNormLinearArgmax(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLayerNormLinearArgmaxRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyRmsNormLinear(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyRmsNormLinearRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyRmsNormLinearSample(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyRmsNormLinearSampleRequest) !?usize {
        return null;
    }

    fn sampleLogitsDevice(_: *anyopaque, _: *const mlx_quant.SampleLogitsDeviceRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyRmsNormLinearArgmax(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyRmsNormLinearArgmaxRequest) !?usize {
        return null;
    }

    fn decoderRuntimePrepareLinear(_: *anyopaque, _: *const mlx_quant.DecoderRuntimePrepareLinearRequest) !bool {
        return false;
    }

    fn decoderRuntimeApplyLinear(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLinearRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyLinearArgmax(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLinearArgmaxRequest) !?usize {
        return null;
    }

    fn decoderRuntimeApplyLinearPair(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLinearPairRequest) !?mlx_quant.LinearNoBiasPairResult {
        return null;
    }

    fn decoderRuntimeApplyLinearQkv(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyLinearQkvRequest) !?mlx_quant.LinearNoBiasTripleResult {
        return null;
    }

    fn decoderRuntimeApplyActivation(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyActivationRequest) !?c.mlx_array {
        return null;
    }

    fn decoderRuntimeApplyAdd(_: *anyopaque, _: *const mlx_quant.DecoderRuntimeApplyAddRequest) !?c.mlx_array {
        return null;
    }

    fn runDenseFfnResidual(_: *anyopaque, _: *const mlx_quant.RunDenseFfnResidualRequest) !?c.mlx_array {
        return null;
    }

    fn runGatedFfnResidual(_: *anyopaque, _: *const mlx_quant.RunGatedFfnResidualRequest) !?c.mlx_array {
        return null;
    }

    fn runAttentionResidualPostLinear(_: *anyopaque, _: *const mlx_quant.RunAttentionResidualPostLinearRequest) !?c.mlx_array {
        return null;
    }

    fn runCompressedAttentionDenseDecoderBlock(_: *anyopaque, _: *const mlx_quant.RunCompressedAttentionDenseDecoderBlockRequest) !?c.mlx_array {
        return null;
    }

    fn runCompressedAttentionGatedDecoderBlock(_: *anyopaque, _: *const mlx_quant.RunCompressedAttentionGatedDecoderBlockRequest) !?c.mlx_array {
        return null;
    }

    fn hasGatheredSpanCache(_: *anyopaque, _: usize, _: runtime.kv.manager.SequenceId, _: usize) bool {
        return false;
    }
};

const mock_native_quant_vtable = mlx_quant.Provider.VTable{
    .planLinearNoBias = &MockNativeQuantProvider.planLinearNoBias,
    .linearNoBias = &MockNativeQuantProvider.linearNoBias,
    .planLinearNoBiasPair = &MockNativeQuantProvider.planLinearNoBiasPair,
    .linearNoBiasPair = &MockNativeQuantProvider.linearNoBiasPair,
    .mulMatId = &MockNativeQuantProvider.mulMatId,
    .moeLinearNoBias = &MockNativeQuantProvider.moeLinearNoBias,
    .moeLinearNoBiasPair = &MockNativeQuantProvider.moeLinearNoBiasPair,
    .compressedKeyScores = &MockNativeQuantProvider.compressedKeyScores,
    .compressedAttentionBlock = &MockNativeQuantProvider.compressedAttentionBlock,
    .compressedAttentionSpan = &MockNativeQuantProvider.compressedAttentionSpan,
    .lmHeadArgmax = &MockNativeQuantProvider.lmHeadArgmax,
    .decoderRuntimePrepareGreedy = &MockNativeQuantProvider.decoderRuntimePrepareGreedy,
    .decoderRuntimeResetState = &MockNativeQuantProvider.decoderRuntimeResetState,
    .decoderRuntimePrepareAbsoluteEmbeddings = &MockNativeQuantProvider.decoderRuntimePrepareAbsoluteEmbeddings,
    .decoderRuntimeEmbedAbsolutePosition = &MockNativeQuantProvider.decoderRuntimeEmbedAbsolutePosition,
    .decoderRuntimePrepareLayerNorm = &MockNativeQuantProvider.decoderRuntimePrepareLayerNorm,
    .decoderRuntimeApplyLayerNorm = &MockNativeQuantProvider.decoderRuntimeApplyLayerNorm,
    .decoderRuntimePrepareRmsNorm = &MockNativeQuantProvider.decoderRuntimePrepareRmsNorm,
    .decoderRuntimeApplyRmsNorm = &MockNativeQuantProvider.decoderRuntimeApplyRmsNorm,
    .decoderRuntimeApplyLayerNormLinear = &MockNativeQuantProvider.decoderRuntimeApplyLayerNormLinear,
    .decoderRuntimeApplyLayerNormLinearSample = &MockNativeQuantProvider.decoderRuntimeApplyLayerNormLinearSample,
    .decoderRuntimeApplyLayerNormLinearArgmax = &MockNativeQuantProvider.decoderRuntimeApplyLayerNormLinearArgmax,
    .decoderRuntimeApplyRmsNormLinear = &MockNativeQuantProvider.decoderRuntimeApplyRmsNormLinear,
    .decoderRuntimeApplyRmsNormLinearSample = &MockNativeQuantProvider.decoderRuntimeApplyRmsNormLinearSample,
    .decoderRuntimeApplyRmsNormLinearArgmax = &MockNativeQuantProvider.decoderRuntimeApplyRmsNormLinearArgmax,
    .sampleLogitsDevice = &MockNativeQuantProvider.sampleLogitsDevice,
    .decoderRuntimePrepareLinear = &MockNativeQuantProvider.decoderRuntimePrepareLinear,
    .decoderRuntimeApplyLinear = &MockNativeQuantProvider.decoderRuntimeApplyLinear,
    .decoderRuntimeApplyLinearArgmax = &MockNativeQuantProvider.decoderRuntimeApplyLinearArgmax,
    .decoderRuntimeApplyLinearPair = &MockNativeQuantProvider.decoderRuntimeApplyLinearPair,
    .decoderRuntimeApplyLinearQkv = &MockNativeQuantProvider.decoderRuntimeApplyLinearQkv,
    .decoderRuntimeApplyActivation = &MockNativeQuantProvider.decoderRuntimeApplyActivation,
    .decoderRuntimeApplyAdd = &MockNativeQuantProvider.decoderRuntimeApplyAdd,
    .runDenseFfnResidual = &MockNativeQuantProvider.runDenseFfnResidual,
    .runGatedFfnResidual = &MockNativeQuantProvider.runGatedFfnResidual,
    .runAttentionResidualPostLinear = &MockNativeQuantProvider.runAttentionResidualPostLinear,
    .runCompressedAttentionDenseDecoderBlock = &MockNativeQuantProvider.runCompressedAttentionDenseDecoderBlock,
    .runCompressedAttentionGatedDecoderBlock = &MockNativeQuantProvider.runCompressedAttentionGatedDecoderBlock,
    .hasGatheredSpanCache = &MockNativeQuantProvider.hasGatheredSpanCache,
};

var mock_native_quant_state: u8 = 0;

fn mockNativeQuantProvider() mlx_quant.Provider {
    return .{
        .ptr = &mock_native_quant_state,
        .vtable = &mock_native_quant_vtable,
    };
}

fn metalShaderValidationEnabledForTest() bool {
    const c_std = @cImport(@cInclude("stdlib.h"));
    return c_std.getenv("MTL_SHADER_VALIDATION") != null;
}

test "MLX compute initializes prefetch queue for direct empty weight stores" {
    if (metalShaderValidationEnabledForTest()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var ws: WeightStore = .{
        .allocator = allocator,
        .resident_weights = c.mlx_map_string_to_array_new(),
        .stream = mlx.gpuStream(),
        .prefix = "",
        .lazy_weights = .empty,
    };
    defer deinitPrefetchQueue(&ws);
    defer _ = c.mlx_stream_free(ws.stream);
    defer _ = c.mlx_map_string_to_array_free(ws.resident_weights);

    const compute = try allocator.create(MlxCompute);
    compute.* = try MlxCompute.init(allocator, &ws, null);
    defer compute.deinit();

    var cb = compute.computeBackend();
    try std.testing.expectError(error.MissingWeight, cb.getWeight("missing"));
}

test "metal-hosted MLX compute records metal hosting identity" {
    if (metalShaderValidationEnabledForTest()) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var ws: WeightStore = .{
        .allocator = allocator,
        .resident_weights = c.mlx_map_string_to_array_new(),
        .stream = mlx.gpuStream(),
        .prefix = "",
        .lazy_weights = .empty,
    };
    defer deinitPrefetchQueue(&ws);
    defer _ = c.mlx_stream_free(ws.stream);
    defer _ = c.mlx_map_string_to_array_free(ws.resident_weights);

    const compute = try allocator.create(MlxCompute);
    compute.* = try MlxCompute.initMetalHosted(allocator, &ws, null);
    defer compute.deinit();

    try std.testing.expectEqual(MlxCompute.HostedBackend.metal, compute.hostedBackend());
    try std.testing.expect(compute.metalHosted());
}

test "lazy quantized MLX weights use device-native execution when supported" {
    var data: WeightStore = .{
        .allocator = std.testing.allocator,
        .resident_weights = undefined,
        .stream = undefined,
        .prefix = "",
        .lazy_weights = .empty,
        .quant_execution_mode = .device_native,
        .native_quant = mockNativeQuantProvider(),
    };
    var lazy: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "weight",
            .byte_len = 4096,
            .quantized = true,
        },
    };
    var storage: QuantizedStorage = .{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = &.{},
        .shape = &.{ 64, 64 },
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    var weight = Arr{
        .arr = c.mlx_array_new(),
        .owned = false,
        .name = "weight",
        .lazy_entry = &lazy,
        .quantized_storage = &storage,
    };

    const executor = selectQuantLinearExecutorForLinear(&data, &weight, &storage, 1, 64, 64);
    try std.testing.expectEqual(QuantLinearExecutor.device_native, executor);
}

test "lazy quantized MLX weights fall back to backend dense when native quant is unavailable" {
    var data: WeightStore = .{
        .allocator = std.testing.allocator,
        .resident_weights = undefined,
        .stream = undefined,
        .prefix = "",
        .lazy_weights = .empty,
        .quant_execution_mode = .device_native,
        .native_quant = mlx_quant.nullProvider(),
    };
    var lazy: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "weight",
            .byte_len = 4096,
            .quantized = true,
        },
    };
    var storage: QuantizedStorage = .{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = &.{},
        .shape = &.{ 64, 64 },
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    var weight = Arr{
        .arr = c.mlx_array_new(),
        .owned = false,
        .name = "weight",
        .lazy_entry = &lazy,
        .quantized_storage = &storage,
    };

    const executor = selectQuantLinearExecutorForLinear(&data, &weight, &storage, 1, 64, 64);
    try std.testing.expectEqual(QuantLinearExecutor.backend_dense, executor);
}

test "quant budget pressure degrades only for non-packed non-expert weights" {
    try std.testing.expect(shouldDegradeQuantBudgetPressure(false, null));
    try std.testing.expect(!shouldDegradeQuantBudgetPressure(true, null));
    try std.testing.expect(!shouldDegradeQuantBudgetPressure(false, .{
        .layer_index = 1,
        .expert_index = 2,
    }));
}

test "canFallbackQuantizedBudgetPressure respects packed and expert metadata" {
    var storage: QuantizedStorage = .{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = &.{},
        .shape = &.{ 64, 64 },
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    var lazy_non_expert: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "weight",
            .byte_len = 4096,
            .quantized = true,
        },
    };
    var lazy_expert: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "blk.1.ffn_gate_exps.2.weight",
            .byte_len = 4096,
            .quantized = true,
        },
        .expert_coord = .{
            .layer_index = 1,
            .expert_index = 2,
        },
    };
    var non_expert_arr = Arr{
        .arr = c.mlx_array_new(),
        .owned = false,
        .name = "weight",
        .lazy_entry = &lazy_non_expert,
        .quantized_storage = &storage,
    };
    defer _ = c.mlx_array_free(non_expert_arr.arr);

    var expert_arr = Arr{
        .arr = c.mlx_array_new(),
        .owned = false,
        .name = "blk.1.ffn_gate_exps.2.weight",
        .lazy_entry = &lazy_expert,
        .quantized_storage = &storage,
    };
    defer _ = c.mlx_array_free(expert_arr.arr);

    try std.testing.expect(canFallbackQuantizedBudgetPressure(&non_expert_arr, &storage));
    try std.testing.expect(!canFallbackQuantizedBudgetPressure(&expert_arr, &storage));

    storage.packed_expert = .{
        .expert_index = 0,
        .expert_count = 8,
        .expert_axis = 0,
        .row_offset = 0,
    };
    try std.testing.expect(!canFallbackQuantizedBudgetPressure(&non_expert_arr, &storage));
}

test "packed MoE staging compacts selected experts and remaps ids" {
    var raw = [_]u8{
        0, 1,  2,
        3, 4,  5,
        6, 7,  8,
        9, 10, 11,
    };
    const shape = [_]i64{ 4, 3, 1 };
    var storage: QuantizedStorage = .{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = raw[0..],
        .shape = &shape,
        .packed_expert = .{
            .expert_index = 0,
            .expert_count = 4,
            .expert_axis = 0,
            .row_offset = 0,
        },
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    const expert_ids = [_]u32{ 3, 1, 3, 2 };

    try std.testing.expectEqual(@as(usize, 0), quantizedStorageBudgetBytes(&storage));
    var staged = (try stageSelectedPackedMoeStorage(std.testing.allocator, &storage, &expert_ids, null)).?;
    defer staged.deinit();

    try std.testing.expectEqual(@as(u32, 3), staged.storage.packed_expert.?.expert_count);
    try std.testing.expectEqual(@as(i64, 3), staged.storage.shape[0]);
    try std.testing.expectEqualSlices(u32, &.{ 0, 1, 0, 2 }, staged.expert_ids);
    try std.testing.expectEqualSlices(u8, &.{
        9, 10, 11,
        3, 4,  5,
        6, 7,  8,
    }, staged.storage.raw_bytes);
}

test "dense budget pressure fallback only targets linear non-expert weights" {
    var lazy_non_expert: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "model.layers.0.self_attn.q_proj.weight",
            .byte_len = 4096,
        },
    };
    var lazy_expert: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "blk.1.ffn_gate_exps.2.weight",
            .byte_len = 4096,
        },
        .expert_coord = .{
            .layer_index = 1,
            .expert_index = 2,
        },
    };
    try std.testing.expect(canFallbackDenseBudgetPressure("model.layers.0.self_attn.q_proj.weight", &lazy_non_expert));
    try std.testing.expect(!canFallbackDenseBudgetPressure("model.embed_tokens.weight", &lazy_non_expert));
    try std.testing.expect(!canFallbackDenseBudgetPressure("blk.1.ffn_gate_exps.2.weight", &lazy_expert));
}

test "mlx compute reserves eager resident weights against run budget" {
    if (metalShaderValidationEnabledForTest()) return error.SkipZigTest;

    var budget = run_memory.RunBudget.init(.{
        .host_limit_bytes = 1024,
        .backend_limit_bytes = 0,
        .combined_limit_bytes = 0,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });
    var data: WeightStore = .{
        .allocator = std.testing.allocator,
        .resident_weights = undefined,
        .resident_weight_estimate_bytes = 256,
        .stream = undefined,
        .prefix = "",
        .lazy_weights = .empty,
    };

    const compute = try MlxCompute.init(std.testing.allocator, &data, &budget);
    defer if (compute.resident_weight_reservation) |reservation| budget.release(reservation);

    try std.testing.expectEqual(@as(usize, 256), budget.host_weight_bytes);
}

test "mlx lazy weight reservations count backend usage" {
    if (metalShaderValidationEnabledForTest()) return error.SkipZigTest;

    var budget = run_memory.RunBudget.init(.{
        .host_limit_bytes = 0,
        .backend_limit_bytes = 1024,
        .combined_limit_bytes = 0,
        .kv_limit_bytes = 0,
        .scratch_limit_bytes = 0,
    });
    var data: WeightStore = .{
        .allocator = std.testing.allocator,
        .resident_weights = undefined,
        .stream = undefined,
        .prefix = "",
        .lazy_weights = .empty,
    };
    var compute = try MlxCompute.init(std.testing.allocator, &data, &budget);
    defer compute.weight_reservations.deinit(std.testing.allocator);

    var entry: LazyWeightEntry = .{
        .tensor_ref = .{
            .name = "weight",
            .byte_len = 0,
        },
        .loaded_quantized = c.mlx_array_new(),
        .backend_loaded_bytes = 320,
    };
    defer _ = c.mlx_array_free(entry.loaded_quantized.?);

    try reserveLazyWeightForUse(&compute, "weight", &entry);
    try std.testing.expectEqual(@as(usize, 320), budget.backend_weight_bytes);
    releaseWeightReservation(&compute, "weight");
    try std.testing.expectEqual(@as(usize, 0), budget.backend_weight_bytes);
}

test "findPackedExpertAxisLocal finds unique axis matching expert count" {
    // Shape [128, 4096, 8192] with 128 experts → axis 0
    const shape3 = [_]i64{ 128, 4096, 8192 };
    try std.testing.expectEqual(@as(?usize, 0), findPackedExpertAxisLocal(&shape3, 128));

    // Shape [4096, 128, 8192] → axis 1
    const shape_mid = [_]i64{ 4096, 128, 8192 };
    try std.testing.expectEqual(@as(?usize, 1), findPackedExpertAxisLocal(&shape_mid, 128));

    // Shape [8, 4096, 14336] with 8 experts → axis 0
    const shape_8 = [_]i64{ 8, 4096, 14336 };
    try std.testing.expectEqual(@as(?usize, 0), findPackedExpertAxisLocal(&shape_8, 8));

    // Ambiguous: two dims match expert count → null
    const shape_ambig = [_]i64{ 128, 128, 8192 };
    try std.testing.expectEqual(@as(?usize, null), findPackedExpertAxisLocal(&shape_ambig, 128));

    // No dim matches → null
    const shape_none = [_]i64{ 4096, 8192 };
    try std.testing.expectEqual(@as(?usize, null), findPackedExpertAxisLocal(&shape_none, 128));
}

test "parseExpertIndexFromLazyName extracts expert index" {
    try std.testing.expectEqual(@as(?u32, 0), parseExpertIndexFromLazyName("model.layers.5.block_sparse_moe.experts.0.gate_proj.weight"));
    try std.testing.expectEqual(@as(?u32, 127), parseExpertIndexFromLazyName("model.layers.5.block_sparse_moe.experts.127.gate_proj.weight"));
    try std.testing.expectEqual(@as(?u32, null), parseExpertIndexFromLazyName("model.layers.5.input_layernorm.weight"));
}

test "non-lazy quantized MLX weights still use direct quant path selection" {
    var data: WeightStore = .{
        .allocator = std.testing.allocator,
        .resident_weights = undefined,
        .stream = undefined,
        .prefix = "",
        .lazy_weights = .empty,
        .quant_execution_mode = .device_native,
        .native_quant = mlx_quant.nullProvider(),
    };
    var storage: QuantizedStorage = .{
        .tensor_type = .{ .known = .Q4_0 },
        .raw_bytes = &.{},
        .shape = &.{ 64, 64 },
        .raw_owned = false,
        .allocator = std.testing.allocator,
    };
    var weight = Arr{
        .arr = c.mlx_array_new(),
        .owned = false,
        .name = "weight",
        .lazy_entry = null,
        .quantized_storage = &storage,
    };

    const executor = selectQuantLinearExecutorForLinear(&data, &weight, &storage, 1, 64, 64);
    try std.testing.expectEqual(QuantLinearExecutor.wrapper_direct_quant, executor);
}
