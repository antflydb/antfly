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

const std = @import("std");
const build_options = @import("build_options");
const gemma4_runtime = @import("../architectures/gemma4_runtime.zig");
const gpt_arch = @import("../architectures/gpt.zig");
const contracts = @import("../graph/backend_contracts.zig");
const model_runtime = @import("../graph/model_runtime.zig");
const gpt_mod = @import("../models/gpt.zig");
const decoder_rms_runtime = @import("decoder_rms_runtime.zig");
const decoder_tail_runtime = @import("decoder_tail_runtime.zig");
const metal_compute_mod = @import("../ops/metal_compute.zig");
const ops = @import("../ops/ops.zig");
const native_blas = @import("native.zig");

const c_std = @cImport(@cInclude("stdlib.h"));

pub const TimingStats = struct {
    prepare_calls: u64 = 0,
    prepare_greedy_nanos: u128 = 0,
    prepare_greedy_failures: u64 = 0,
    lookup_nanos: u128 = 0,
    norm_prep_nanos: u128 = 0,
    linear_prep_nanos: u128 = 0,
    final_lookup_nanos: u128 = 0,
    final_prep_nanos: u128 = 0,
    prepare_attn_pre_norm_failures: u64 = 0,
    prepare_attn_post_norm_failures: u64 = 0,
    prepare_ffn_pre_norm_failures: u64 = 0,
    prepare_ffn_post_norm_failures: u64 = 0,
    prepare_attn_q_failures: u64 = 0,
    prepare_attn_k_failures: u64 = 0,
    prepare_attn_v_failures: u64 = 0,
    prepare_attn_out_failures: u64 = 0,
    prepare_mlp_gate_failures: u64 = 0,
    prepare_mlp_up_failures: u64 = 0,
    prepare_mlp_down_failures: u64 = 0,
    prepare_ple_gate_failures: u64 = 0,
    prepare_ple_proj_failures: u64 = 0,
    prepare_ple_model_proj_failures: u64 = 0,
    prepare_ple_proj_norm_failures: u64 = 0,
    prepare_ple_post_norm_failures: u64 = 0,
    prepare_final_norm_failures: u64 = 0,
    prepare_attn_out_first_rank: usize = 0,
    prepare_attn_out_first_dim0: i64 = 0,
    prepare_attn_out_first_dim1: i64 = 0,
    prefill_calls: u64 = 0,
    prefill_embed_nanos: u128 = 0,
    prefill_ple_prepare_nanos: u128 = 0,
    prefill_ple_lookup_nanos: u128 = 0,
    prefill_ple_embedding_nanos: u128 = 0,
    prefill_ple_model_proj_nanos: u128 = 0,
    prefill_ple_norm_nanos: u128 = 0,
    prefill_ple_combine_nanos: u128 = 0,
    prefill_ple_fallback_nanos: u128 = 0,
    prefill_block_nanos: u128 = 0,
    preplan_frame_layer_spec_nanos: u128 = 0,
    preplan_frame_plan_nanos: u128 = 0,
    prefill_frame_begin_nanos: u128 = 0,
    prefill_frame_layer_spec_nanos: u128 = 0,
    prefill_frame_plan_nanos: u128 = 0,
    prefill_frame_execute_nanos: u128 = 0,
    prefill_frame_finish_nanos: u128 = 0,
    prefill_block_attention_nanos: u128 = 0,
    prefill_block_attention_project_nanos: u128 = 0,
    prefill_block_attention_qkv_nanos: u128 = 0,
    prefill_block_attention_head_norm_nanos: u128 = 0,
    prefill_block_attention_apply_nanos: u128 = 0,
    prefill_block_attention_fused_residual_nanos: u128 = 0,
    prefill_block_ffn_nanos: u128 = 0,
    prefill_block_ffn_norm_nanos: u128 = 0,
    prefill_block_ffn_fused_nanos: u128 = 0,
    prefill_block_ple_nanos: u128 = 0,
    prefill_block_output_scale_nanos: u128 = 0,
    prefill_compare_nanos: u128 = 0,
    prefill_sync_nanos: u128 = 0,
    prefill_last_hidden_slice_nanos: u128 = 0,
    prefill_tail_nanos: u128 = 0,
    prefill_query_tokens: u64 = 0,
    prefill_layers: u64 = 0,
    prefill_attn_norm_ops: u64 = 0,
    prefill_q_linear_ops: u64 = 0,
    prefill_qkv_ops: u64 = 0,
    prefill_qkv_fused_ops: u64 = 0,
    prefill_qkv_split_ops: u64 = 0,
    prefill_kv_pair_ops: u64 = 0,
    prefill_head_norm_ops: u64 = 0,
    prefill_kv_seed_ops: u64 = 0,
    prefill_attention_apply_ops: u64 = 0,
    prefill_attn_out_linear_ops: u64 = 0,
    prefill_attn_post_norm_ops: u64 = 0,
    prefill_attn_residual_add_ops: u64 = 0,
    prefill_ffn_norm_ops: u64 = 0,
    prefill_ffn_fused_ops: u64 = 0,
    prefill_ffn_split_ops: u64 = 0,
    prefill_ffn_pair_ops: u64 = 0,
    prefill_ffn_activation_ops: u64 = 0,
    prefill_ffn_multiply_ops: u64 = 0,
    prefill_ffn_down_linear_ops: u64 = 0,
    prefill_ffn_post_norm_ops: u64 = 0,
    prefill_ffn_residual_add_ops: u64 = 0,
    prefill_ple_ops: u64 = 0,
    prefill_ple_direct_hits: u64 = 0,
    prefill_ple_fallbacks: u64 = 0,
    prefill_output_scale_ops: u64 = 0,
    prefill_tail_logits_ops: u64 = 0,
    greedy_calls: u64 = 0,
    greedy_embed_nanos: u128 = 0,
    greedy_block_nanos: u128 = 0,
    greedy_block_attention_nanos: u128 = 0,
    greedy_block_attention_project_nanos: u128 = 0,
    greedy_block_attention_qkv_nanos: u128 = 0,
    greedy_block_attention_head_norm_nanos: u128 = 0,
    greedy_block_attention_apply_nanos: u128 = 0,
    greedy_block_attention_fused_residual_nanos: u128 = 0,
    greedy_block_ffn_nanos: u128 = 0,
    greedy_block_ffn_norm_nanos: u128 = 0,
    greedy_block_ffn_fused_nanos: u128 = 0,
    greedy_block_ple_nanos: u128 = 0,
    greedy_block_output_scale_nanos: u128 = 0,
    greedy_tail_nanos: u128 = 0,
    sampled_calls: u64 = 0,
    sampled_embed_nanos: u128 = 0,
    sampled_block_nanos: u128 = 0,
    sampled_block_attention_nanos: u128 = 0,
    sampled_block_attention_project_nanos: u128 = 0,
    sampled_block_attention_qkv_nanos: u128 = 0,
    sampled_block_attention_head_norm_nanos: u128 = 0,
    sampled_block_attention_apply_nanos: u128 = 0,
    sampled_block_attention_fused_residual_nanos: u128 = 0,
    sampled_block_ffn_nanos: u128 = 0,
    sampled_block_ffn_norm_nanos: u128 = 0,
    sampled_block_ffn_fused_nanos: u128 = 0,
    sampled_block_ple_nanos: u128 = 0,
    sampled_block_output_scale_nanos: u128 = 0,
    sampled_tail_nanos: u128 = 0,
    gemma_fused_qkv_hits: u64 = 0,
    gemma_fused_qkv_fallbacks: u64 = 0,
    gemma_fused_attn_residual_hits: u64 = 0,
    gemma_fused_attn_residual_fallbacks: u64 = 0,
    gemma_fused_ffn_hits: u64 = 0,
    gemma_fused_ffn_fallbacks: u64 = 0,
    gemma_qkv_hits: u64 = 0,
    gemma_qkv_fallbacks: u64 = 0,
    gemma_o_proj_hits: u64 = 0,
    gemma_o_proj_fallbacks: u64 = 0,
    gemma_mlp_proj_hits: u64 = 0,
    gemma_mlp_proj_fallbacks: u64 = 0,
    gemma_attention_matmul_hits: u64 = 0,
    gemma_attention_matmul_fallbacks: u64 = 0,
    gemma_rms_norm_hits: u64 = 0,
    gemma_rms_norm_fallbacks: u64 = 0,
    gemma_softmax_hits: u64 = 0,
    gemma_softmax_fallbacks: u64 = 0,
    gemma_residual_add_hits: u64 = 0,
    gemma_residual_add_fallbacks: u64 = 0,
    gemma_elementwise_mul_hits: u64 = 0,
    gemma_elementwise_mul_fallbacks: u64 = 0,
};

var timing_stats = TimingStats{};

pub fn resetTimingStats() void {
    timing_stats = .{};
}

pub fn getTimingStats() TimingStats {
    return timing_stats;
}

fn recordGemmaDirectQkv(hit: bool) void {
    if (hit) {
        timing_stats.gemma_fused_qkv_hits += 1;
        timing_stats.gemma_qkv_hits += 3;
    } else {
        timing_stats.gemma_fused_qkv_fallbacks += 1;
        timing_stats.gemma_qkv_fallbacks += 3;
    }
}

fn recordGemmaDirectAttentionResidual(hit: bool, post_norm: bool) void {
    if (hit) {
        timing_stats.gemma_fused_attn_residual_hits += 1;
        timing_stats.gemma_o_proj_hits += 1;
        timing_stats.gemma_attention_matmul_hits += 2;
        timing_stats.gemma_softmax_hits += 1;
        timing_stats.gemma_residual_add_hits += 1;
        if (post_norm) timing_stats.gemma_rms_norm_hits += 1;
    } else {
        timing_stats.gemma_fused_attn_residual_fallbacks += 1;
        timing_stats.gemma_o_proj_fallbacks += 1;
        timing_stats.gemma_attention_matmul_fallbacks += 2;
        timing_stats.gemma_softmax_fallbacks += 1;
        timing_stats.gemma_residual_add_fallbacks += 1;
        if (post_norm) timing_stats.gemma_rms_norm_fallbacks += 1;
    }
}

fn recordGemmaDirectFfn(hit: bool, pre_norm: bool, post_gate_norm: bool, post_down_norm: bool) void {
    const norm_count: u64 =
        (if (pre_norm) @as(u64, 1) else 0) +
        (if (post_gate_norm) @as(u64, 1) else 0) +
        (if (post_down_norm) @as(u64, 1) else 0);
    if (hit) {
        timing_stats.gemma_fused_ffn_hits += 1;
        timing_stats.gemma_mlp_proj_hits += 3;
        timing_stats.gemma_elementwise_mul_hits += 1;
        timing_stats.gemma_residual_add_hits += 1;
        timing_stats.gemma_rms_norm_hits += norm_count;
    } else {
        timing_stats.gemma_fused_ffn_fallbacks += 1;
        timing_stats.gemma_mlp_proj_fallbacks += 3;
        timing_stats.gemma_elementwise_mul_fallbacks += 1;
        timing_stats.gemma_residual_add_fallbacks += 1;
        timing_stats.gemma_rms_norm_fallbacks += norm_count;
    }
}

fn recordGemmaDirectRmsNorm(hit: bool) void {
    if (hit) {
        timing_stats.gemma_rms_norm_hits += 1;
    } else {
        timing_stats.gemma_rms_norm_fallbacks += 1;
    }
}

test "gemma direct runtime residency helpers map fused paths to logical counters" {
    resetTimingStats();
    defer resetTimingStats();

    recordGemmaDirectQkv(true);
    recordGemmaDirectAttentionResidual(true, true);
    recordGemmaDirectFfn(true, true, false, true);
    recordGemmaDirectRmsNorm(true);
    recordGemmaDirectQkv(false);
    recordGemmaDirectAttentionResidual(false, true);
    recordGemmaDirectFfn(false, false, false, true);
    recordGemmaDirectRmsNorm(false);

    const stats = getTimingStats();
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_fused_qkv_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_fused_qkv_fallbacks);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_fused_attn_residual_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_fused_attn_residual_fallbacks);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_fused_ffn_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_fused_ffn_fallbacks);
    try std.testing.expectEqual(@as(u64, 3), stats.gemma_qkv_hits);
    try std.testing.expectEqual(@as(u64, 3), stats.gemma_qkv_fallbacks);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_o_proj_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_o_proj_fallbacks);
    try std.testing.expectEqual(@as(u64, 3), stats.gemma_mlp_proj_hits);
    try std.testing.expectEqual(@as(u64, 3), stats.gemma_mlp_proj_fallbacks);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_attention_matmul_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_attention_matmul_fallbacks);
    try std.testing.expectEqual(@as(u64, 4), stats.gemma_rms_norm_hits);
    try std.testing.expectEqual(@as(u64, 3), stats.gemma_rms_norm_fallbacks);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_softmax_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_softmax_fallbacks);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_residual_add_hits);
    try std.testing.expectEqual(@as(u64, 2), stats.gemma_residual_add_fallbacks);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_elementwise_mul_hits);
    try std.testing.expectEqual(@as(u64, 1), stats.gemma_elementwise_mul_fallbacks);
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn getenvBool(comptime name: [*:0]const u8) bool {
    const value = c_std.getenv(name) orelse return false;
    const slice = std.mem.span(value);
    return std.mem.eql(u8, slice, "1") or
        std.ascii.eqlIgnoreCase(slice, "true") or
        std.ascii.eqlIgnoreCase(slice, "yes") or
        std.ascii.eqlIgnoreCase(slice, "on");
}

fn gatedFamilyCompareRequested() bool {
    return getenvBool("TERMITE_METAL_COMPARE_GATED_FAMILY") or gemmaPrefillCompareRequested();
}

fn gemmaPrefillCompareRequested() bool {
    return getenvBool("TERMITE_METAL_COMPARE_GEMMA_PREFILL_BLOCK");
}

fn gatedFamilyCompareAllowFrameRequested() bool {
    return getenvBool("TERMITE_METAL_COMPARE_GATED_FAMILY_ALLOW_FRAME");
}

fn disableReservedHiddenCarrierRequested() bool {
    return getenvBool("TERMITE_METAL_DISABLE_RESERVED_HIDDEN_CARRIER");
}

fn disableDirectPrefillSeedRequested() bool {
    return getenvBool("TERMITE_METAL_DISABLE_DIRECT_PREFILL_SEED");
}

fn tracePrefillSeedRequested() bool {
    return c_std.getenv("TERMITE_METAL_DUMP_DECODE_KV_LAYER") != null;
}

fn disableDirectGatedFamilyRequested() bool {
    return getenvBool("TERMITE_METAL_DISABLE_GATED_FAMILY_DIRECT");
}

fn disableGemmaFusedQkvRequested() bool {
    return getenvBool("TERMITE_METAL_DISABLE_GEMMA_FUSED_QKV");
}

fn disableDirectPleRequested() bool {
    return getenvBool("TERMITE_METAL_DISABLE_DIRECT_PLE");
}

fn materializeDirectPleSliceRequested() bool {
    return getenvBool("TERMITE_METAL_MATERIALIZE_DIRECT_PLE_SLICE");
}

fn syncGatedFamilyFinalHiddenRequested() bool {
    return getenvBool("TERMITE_METAL_SYNC_GATED_FAMILY_FINAL_HIDDEN");
}

fn materializeGatedFamilyHiddenRequested() bool {
    return getenvBool("TERMITE_METAL_MATERIALIZE_GATED_FAMILY_HIDDEN");
}

fn syncGatedFamilyStagesRequested() bool {
    return getenvBool("TERMITE_METAL_SYNC_GATED_FAMILY_STAGES");
}

fn disableGatedFamilyPrefillFrameBarriersRequested() bool {
    return getenvBool("TERMITE_METAL_DISABLE_GATED_FAMILY_PREFILL_FRAME_BARRIERS");
}

fn prefillTraceRequested() bool {
    return getenvBool("TERMITE_METAL_PREFILL_TRACE");
}

fn disableGatedFamilyRuntimePrefillBlockRequested(gpt_config: gpt_mod.Config, configured_layer_count: usize) bool {
    if (getenvBool("TERMITE_METAL_DISABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK")) return true;
    if (gemmaPrefillCompareRequested()) return false;
    if (getenvBool("TERMITE_METAL_ENABLE_GATED_FAMILY_RUNTIME_PREFILL_BLOCK")) return false;
    return !gemma4_runtime.supportsWholeFramePrefill(gpt_config, configured_layer_count);
}

fn decoderActivationKind(gpt_config: gpt_mod.Config) contracts.DecoderRuntimeActivationKind {
    return gemma4_runtime.decoderActivationKind(gpt_config);
}

fn traceGatedFamilyDeviceRequested() bool {
    return getenvBool("TERMITE_METAL_TRACE_GATED_DEVICE");
}

fn traceGatedFamilyDevice(
    cb: *const ops.ComputeBackend,
    layer: usize,
    label: []const u8,
    tensor: ops.CT,
) void {
    if (!traceGatedFamilyDeviceRequested()) return;
    std.debug.print(
        "metal-gated-device layer={d} {s}={}\n",
        .{ layer, label, metal_compute_mod.MetalCompute.debugHasDeviceTensor(cb, tensor) },
    );
}

fn gatedFamilyCompareLayer() ?usize {
    const value = c_std.getenv("TERMITE_METAL_COMPARE_LAYER") orelse
        c_std.getenv("TERMITE_METAL_COMPARE_GATED_LAYER") orelse return null;
    return std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch null;
}

fn gatedFamilyCompareTolerance() f32 {
    const value = c_std.getenv("TERMITE_METAL_COMPARE_TOLERANCE") orelse return 0.0001;
    return std.fmt.parseFloat(f32, std.mem.span(value)) catch 0.0001;
}

fn compareStageRequested(label: []const u8) bool {
    const raw = c_std.getenv("TERMITE_METAL_COMPARE_STAGE") orelse return true;
    const stage = std.mem.span(raw);
    if (stage.len == 0) return true;
    if (std.mem.indexOf(u8, label, stage) != null) return true;
    if (std.mem.eql(u8, stage, "qkv")) {
        return std.mem.endsWith(u8, label, "-q") or
            std.mem.endsWith(u8, label, "-k") or
            std.mem.endsWith(u8, label, "-v");
    }
    if (std.mem.eql(u8, stage, "residual")) {
        return std.mem.indexOf(u8, label, "hidden") != null or
            std.mem.indexOf(u8, label, "residual") != null;
    }
    return false;
}

fn gatedFamilyDumpLayer() ?usize {
    const value = c_std.getenv("TERMITE_METAL_DUMP_GATED_LAYER") orelse return null;
    return std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch null;
}

fn shouldDumpGatedFamilyLayer(layer: usize) bool {
    return if (gatedFamilyDumpLayer()) |target| target == layer else false;
}

fn cloneTensorForCompare(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, tensor: ops.CT) !ops.CT {
    const shape_i64 = try cb.tensorShape(tensor, allocator);
    defer allocator.free(shape_i64);
    const shape = try allocator.alloc(i32, shape_i64.len);
    defer allocator.free(shape);
    for (shape_i64, 0..) |dim, idx| shape[idx] = @intCast(dim);
    const data = try cb.toFloat32(tensor, allocator);
    defer allocator.free(data);
    return cb.fromFloat32Shape(data, shape);
}

fn compareLastHiddenRow(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    label: []const u8,
    got: ops.CT,
    got_rows: usize,
    want: ops.CT,
    want_rows: usize,
    hidden_size: usize,
) !bool {
    if (!compareStageRequested(label)) return true;
    const got_host = try cb.toFloat32(got, allocator);
    defer allocator.free(got_host);
    const want_host = try cb.toFloat32(want, allocator);
    defer allocator.free(want_host);
    if (got_rows == 0 or want_rows == 0) return true;
    if (got_host.len < got_rows * hidden_size or want_host.len < want_rows * hidden_size) return true;

    const got_last = got_host[(got_rows - 1) * hidden_size ..][0..hidden_size];
    const want_last = want_host[(want_rows - 1) * hidden_size ..][0..hidden_size];

    var max_abs: f32 = 0;
    var max_idx: usize = 0;
    var sum_abs: f64 = 0;
    for (got_last, want_last, 0..) |got_value, want_value, idx| {
        const abs_value = @abs(got_value - want_value);
        sum_abs += abs_value;
        if (abs_value > max_abs) {
            max_abs = abs_value;
            max_idx = idx;
        }
    }
    const tolerance = gatedFamilyCompareTolerance();
    const ok = max_abs <= tolerance;

    std.debug.print(
        "gated-family-compare {s}: ok={} row_dim={d} max_abs={d:.6}@{d} mean_abs={d:.6} got={d:.6} want={d:.6} tol={d:.6}\n",
        .{
            label,
            ok,
            got_last.len,
            max_abs,
            max_idx,
            sum_abs / @as(f64, @floatFromInt(got_last.len)),
            got_last[max_idx],
            want_last[max_idx],
            tolerance,
        },
    );
    return ok;
}

fn dumpLastHiddenRowStats(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    label: []const u8,
    tensor: ops.CT,
    rows: usize,
    hidden_size: usize,
) !void {
    const host = try cb.toFloat32(tensor, allocator);
    defer allocator.free(host);
    if (rows == 0 or host.len < rows * hidden_size) return;
    const last = host[(rows - 1) * hidden_size ..][0..hidden_size];
    var min_value: f32 = last[0];
    var max_value: f32 = last[0];
    var sum: f64 = 0;
    var sum_abs: f64 = 0;
    for (last) |value| {
        min_value = @min(min_value, value);
        max_value = @max(max_value, value);
        sum += value;
        sum_abs += @abs(value);
    }
    const hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(last));
    std.debug.print(
        "gated-family-dump {s}: row_dim={d} hash=0x{x} min={d:.6} max={d:.6} mean={d:.6} mean_abs={d:.6}\n",
        .{
            label,
            last.len,
            hash,
            min_value,
            max_value,
            sum / @as(f64, @floatFromInt(last.len)),
            sum_abs / @as(f64, @floatFromInt(last.len)),
        },
    );
}

fn maybeDumpInitialGatedInputs(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    hidden: ops.CT,
    ple_vectors: ?ops.CT,
    rows: usize,
    hidden_size: usize,
    ple_dim: usize,
) !void {
    if (gatedFamilyDumpLayer() != 0) return;
    try dumpLastHiddenRowStats(cb, allocator, "input-hidden", hidden, rows, hidden_size);
    if (ple_vectors) |pv| {
        try dumpLastHiddenRowStats(cb, allocator, "layer-0-ple-input", pv, rows, ple_dim);
    }
}

fn prepareGemmaQueryForAttention(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    q_input: ops.CT,
    rows: usize,
    head_dim: usize,
    q_dim: usize,
) !ops.CT {
    if (gpt_config.global_head_dim == 0) return q_input;
    const scale = @sqrt(@as(f32, @floatFromInt(head_dim)));
    if (cb.vtable.reshape2d != null) {
        const scale_shape = [_]i32{1};
        const scale_ct = try cb.fromFloat32Shape(&[_]f32{scale}, &scale_shape);
        defer cb.free(scale_ct);
        return cb.multiply(q_input, scale_ct);
    }
    const q_data = try cb.toFloat32(q_input, allocator);
    defer allocator.free(q_data);
    const scaled = try allocator.alloc(f32, q_data.len);
    errdefer allocator.free(scaled);
    @memcpy(scaled, q_data);
    for (scaled) |*value| value.* *= scale;
    const shape = [_]i32{ @intCast(rows), @intCast(q_dim) };
    return cb.fromFloat32Shape(scaled, &shape);
}

fn prepareGemmaValueForAttention(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    v_input: ops.CT,
    rows: usize,
    num_kv_heads: usize,
    head_dim: usize,
    kv_dim: usize,
    shares_kv: bool,
) !ops.CT {
    if (gpt_config.global_head_dim == 0 or shares_kv) return v_input;
    const ones = try allocator.alloc(f32, head_dim);
    defer allocator.free(ones);
    @memset(ones, 1.0);
    const ones_shape = [_]i32{@intCast(head_dim)};
    const ones_ct = try cb.fromFloat32Shape(ones, &ones_shape);
    defer cb.free(ones_ct);

    if (try cb.reshape2d(v_input, rows * num_kv_heads, head_dim)) |reshaped| {
        defer cb.free(reshaped);
        const normed_flat = try cb.rmsNorm(reshaped, ones_ct, head_dim, gpt_config.norm_eps);
        defer cb.free(normed_flat);
        return (try cb.reshape2d(normed_flat, rows, kv_dim)) orelse error.ReshapeFailed;
    }
    return cb.rmsNorm(v_input, ones_ct, head_dim, gpt_config.norm_eps);
}

fn prepareKeyForPagedPrefillSeed(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    k_input: ops.CT,
    seq_len: usize,
    query_sequence_len: usize,
    head_dim: usize,
    layer: usize,
) !ops.CT {
    if (gpt_config.position_encoding != .rope) return k_input;
    const rope_dim: usize = gpt_config.layerRopeActiveDim(layer);
    const rope_consecutive_pairs = gpt_config.rope_layout == .consecutive_pairs;
    const rope_theta = blk: {
        const base_theta = gpt_config.layerRopeTheta(layer);
        const freq_dim: f32 = @floatFromInt(gpt_config.layerRopeFrequencyDim(layer));
        const active_dim: f32 = @floatFromInt(rope_dim);
        if (active_dim < freq_dim) {
            break :blk std.math.pow(f32, base_theta, active_dim / freq_dim);
        }
        break :blk base_theta;
    };
    const position_offset = seq_len - query_sequence_len;
    return cb.rope(
        k_input,
        query_sequence_len,
        head_dim,
        rope_dim,
        rope_theta,
        gpt_config.rope_freq_scale,
        position_offset,
        rope_consecutive_pairs,
    );
}

fn prepareRopeForDirectAttention(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    input: ops.CT,
    seq_len: usize,
    query_sequence_len: usize,
    head_dim: usize,
    layer: usize,
) !ops.CT {
    if (gpt_config.position_encoding != .rope) return input;
    const rope_dim: usize = gpt_config.layerRopeActiveDim(layer);
    const rope_consecutive_pairs = gpt_config.rope_layout == .consecutive_pairs;
    const rope_theta = blk: {
        const base_theta = gpt_config.layerRopeTheta(layer);
        const freq_dim: f32 = @floatFromInt(gpt_config.layerRopeFrequencyDim(layer));
        const active_dim: f32 = @floatFromInt(rope_dim);
        if (active_dim < freq_dim) {
            break :blk std.math.pow(f32, base_theta, active_dim / freq_dim);
        }
        break :blk base_theta;
    };
    const position_offset = seq_len - query_sequence_len;
    return cb.rope(
        input,
        query_sequence_len,
        head_dim,
        rope_dim,
        rope_theta,
        gpt_config.rope_freq_scale,
        position_offset,
        rope_consecutive_pairs,
    );
}

fn maybeSyncTensor(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    tensor: ops.CT,
) !void {
    if (!syncGatedFamilyStagesRequested()) return;
    const host = try cb.toFloat32(tensor, allocator);
    allocator.free(host);
}

fn compareGatedFamilyLayer(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    compare_hidden: *?ops.CT,
    next_hidden: ops.CT,
    seq_len: usize,
    layer: usize,
    decode_context: *const gpt_arch.DecodeContext,
    ple_vectors: ?ops.CT,
) !bool {
    if (compare_hidden.* == null) return true;
    const report_layer = shouldCompareGatedFamilyLayer(layer);
    const num_kv_heads = gpt_config.effectiveKVHeadsForLayer(layer);
    const head_dim = gpt_config.effectiveHeadDimForLayer(layer);
    const compare_next = try gpt_arch.debugDecoderBlockNoOverrides(
        cb,
        allocator,
        gpt_config,
        compare_hidden.*.?,
        1,
        seq_len,
        num_kv_heads,
        head_dim,
        layer,
        decode_context,
        ple_vectors,
    );
    cb.free(compare_hidden.*.?);
    compare_hidden.* = compare_next;
    if (!report_layer) return true;
    var label_buf: [64]u8 = undefined;
    const label = try std.fmt.bufPrint(&label_buf, "layer-{d}-hidden", .{layer});
    return try compareLastHiddenRow(
        cb,
        allocator,
        label,
        next_hidden,
        decode_context.query_sequence_len,
        compare_next,
        decode_context.query_sequence_len,
        gpt_config.hidden_size,
    );
}

fn shouldCompareGatedFamilyLayer(layer: usize) bool {
    if (!gatedFamilyCompareRequested()) return false;
    return if (gatedFamilyCompareLayer()) |target| target == layer else true;
}

fn createZeroTensor(cb: *const ops.ComputeBackend, allocator: std.mem.Allocator, len: usize) !ops.CT {
    if (try cb.zeroTensor(1, len)) |ct| return ct;
    const zeros = try allocator.alloc(f32, len);
    defer allocator.free(zeros);
    @memset(zeros, 0);
    return cb.fromFloat32(zeros);
}

const BlockTimingPhase = enum {
    prefill,
    greedy,
    sampled,
};

fn addBlockAttentionTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_attention_nanos += nanos,
        .greedy => timing_stats.greedy_block_attention_nanos += nanos,
        .sampled => timing_stats.sampled_block_attention_nanos += nanos,
    }
}

fn addBlockAttentionProjectTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_attention_project_nanos += nanos,
        .greedy => timing_stats.greedy_block_attention_project_nanos += nanos,
        .sampled => timing_stats.sampled_block_attention_project_nanos += nanos,
    }
}

fn addBlockAttentionQkvTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_attention_qkv_nanos += nanos,
        .greedy => timing_stats.greedy_block_attention_qkv_nanos += nanos,
        .sampled => timing_stats.sampled_block_attention_qkv_nanos += nanos,
    }
}

fn addBlockAttentionHeadNormTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_attention_head_norm_nanos += nanos,
        .greedy => timing_stats.greedy_block_attention_head_norm_nanos += nanos,
        .sampled => timing_stats.sampled_block_attention_head_norm_nanos += nanos,
    }
}

fn addBlockAttentionApplyTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_attention_apply_nanos += nanos,
        .greedy => timing_stats.greedy_block_attention_apply_nanos += nanos,
        .sampled => timing_stats.sampled_block_attention_apply_nanos += nanos,
    }
}

fn addBlockAttentionFusedResidualTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_attention_fused_residual_nanos += nanos,
        .greedy => timing_stats.greedy_block_attention_fused_residual_nanos += nanos,
        .sampled => timing_stats.sampled_block_attention_fused_residual_nanos += nanos,
    }
}

fn addBlockFfnTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_ffn_nanos += nanos,
        .greedy => timing_stats.greedy_block_ffn_nanos += nanos,
        .sampled => timing_stats.sampled_block_ffn_nanos += nanos,
    }
}

fn addBlockFfnNormTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_ffn_norm_nanos += nanos,
        .greedy => timing_stats.greedy_block_ffn_norm_nanos += nanos,
        .sampled => timing_stats.sampled_block_ffn_norm_nanos += nanos,
    }
}

fn addBlockFfnFusedTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_ffn_fused_nanos += nanos,
        .greedy => timing_stats.greedy_block_ffn_fused_nanos += nanos,
        .sampled => timing_stats.sampled_block_ffn_fused_nanos += nanos,
    }
}

fn addBlockPleTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_ple_nanos += nanos,
        .greedy => timing_stats.greedy_block_ple_nanos += nanos,
        .sampled => timing_stats.sampled_block_ple_nanos += nanos,
    }
}

fn addBlockOutputScaleTiming(phase: BlockTimingPhase, nanos: u64) void {
    if (nanos == 0) return;
    switch (phase) {
        .prefill => timing_stats.prefill_block_output_scale_nanos += nanos,
        .greedy => timing_stats.greedy_block_output_scale_nanos += nanos,
        .sampled => timing_stats.sampled_block_output_scale_nanos += nanos,
    }
}

pub fn normSlot(layer: usize, kind: enum { attn_pre, attn_post, ffn_pre, ffn_post }) usize {
    return switch (kind) {
        .attn_pre => gemma4_runtime.normSlot(layer, .attn_pre),
        .attn_post => gemma4_runtime.normSlot(layer, .attn_post),
        .ffn_pre => gemma4_runtime.normSlot(layer, .ffn_pre),
        .ffn_post => gemma4_runtime.normSlot(layer, .ffn_post),
    };
}

pub fn linearSlot(layer: usize, kind: enum { attn_q, attn_k, attn_v, attn_out_proj, mlp_gate, mlp_up, mlp_down }) usize {
    return switch (kind) {
        .attn_q => gemma4_runtime.linearSlot(layer, .attn_q),
        .attn_k => gemma4_runtime.linearSlot(layer, .attn_k),
        .attn_v => gemma4_runtime.linearSlot(layer, .attn_v),
        .attn_out_proj => gemma4_runtime.linearSlot(layer, .attn_out_proj),
        .mlp_gate => gemma4_runtime.linearSlot(layer, .mlp_gate),
        .mlp_up => gemma4_runtime.linearSlot(layer, .mlp_up),
        .mlp_down => gemma4_runtime.linearSlot(layer, .mlp_down),
    };
}

fn pleGateSlot(configured_layer_count: usize, layer: usize) usize {
    return gemma4_runtime.pleGateSlot(configured_layer_count, layer);
}

fn pleProjSlot(configured_layer_count: usize, layer: usize) usize {
    return gemma4_runtime.pleProjSlot(configured_layer_count, layer);
}

fn finalLmHeadSlot(configured_layer_count: usize) usize {
    return gemma4_runtime.finalLmHeadSlot(configured_layer_count);
}

fn pleModelProjSlot(configured_layer_count: usize) usize {
    return gemma4_runtime.pleModelProjSlot(configured_layer_count);
}

pub fn finalNormSlot(configured_layer_count: usize) usize {
    return gemma4_runtime.finalNormSlot(configured_layer_count);
}

fn pleProjNormSlot(configured_layer_count: usize) usize {
    return gemma4_runtime.pleProjNormSlot(configured_layer_count);
}

fn plePostNormSlot(configured_layer_count: usize, layer: usize) usize {
    return gemma4_runtime.plePostNormSlot(configured_layer_count, layer);
}

fn qHeadNormSlot(configured_layer_count: usize, layer: usize) usize {
    return gemma4_runtime.qHeadNormSlot(configured_layer_count, layer);
}

fn kHeadNormSlot(configured_layer_count: usize, layer: usize) usize {
    return gemma4_runtime.kHeadNormSlot(configured_layer_count, layer);
}

fn layerRopeThetaForActiveDim(gpt_config: gpt_mod.Config, layer: usize) f32 {
    const rope_dim: usize = gpt_config.layerRopeActiveDim(layer);
    const base_theta = gpt_config.layerRopeTheta(layer);
    const freq_dim: f32 = @floatFromInt(gpt_config.layerRopeFrequencyDim(layer));
    const active_dim: f32 = @floatFromInt(rope_dim);
    if (active_dim < freq_dim) {
        return std.math.pow(f32, base_theta, active_dim / freq_dim);
    }
    return base_theta;
}

pub fn fillDenseQwen3LayerSpecs(
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    out: []ops.DecoderRuntimeLayerSpec,
) ![]const ops.DecoderRuntimeLayerSpec {
    if (gpt_config.family != .qwen3) return error.UnsupportedModelFamily;
    const layer_count = @min(configured_layer_count, gpt_config.num_hidden_layers);
    if (layer_count > out.len) return error.OutOfMemory;
    for (0..layer_count) |layer| {
        const head_dim = gpt_config.effectiveHeadDimForLayer(layer);
        const kv_heads = gpt_config.effectiveKVHeadsForLayer(layer);
        out[layer] = .{
            .kv_heads = kv_heads,
            .head_dim = head_dim,
            .intermediate_size = gpt_config.intermediateSize(layer),
            .kv_layer_index = layer,
            .shares_kv = false,
            .sliding_window = 0,
            .rope_dim = @intCast(gpt_config.layerRopeDim(layer)),
            .rope_active_dim = @intCast(gpt_config.layerRopeActiveDim(layer)),
            .rope_theta = layerRopeThetaForActiveDim(gpt_config, layer),
            .attn_pre_norm_slot = normSlot(layer, .attn_pre),
            .attn_post_norm_slot = normSlot(layer, .ffn_pre),
            .ffn_pre_norm_slot = normSlot(layer, .ffn_pre),
            .ffn_post_norm_slot = normSlot(layer, .ffn_pre),
            .q_head_norm_slot = qHeadNormSlot(configured_layer_count, layer),
            .k_head_norm_slot = kHeadNormSlot(configured_layer_count, layer),
            .q_linear_slot = linearSlot(layer, .attn_q),
            .k_linear_slot = linearSlot(layer, .attn_k),
            .v_linear_slot = linearSlot(layer, .attn_v),
            .attention_linear_slot = linearSlot(layer, .attn_out_proj),
            .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
            .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
            .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
        };
    }
    return out[0..layer_count];
}

const DirectHiddenResult = struct {
    hidden: ops.CT,
    total_rows: usize,
    decoder_frame_active: bool = false,
};

const ReservedHiddenCarrier = struct {
    front: ops.CT,
    back: ops.CT,
    active_front: bool = true,

    fn init(
        cb: *const ops.ComputeBackend,
        hidden_input: ops.CT,
        rows: usize,
        hidden_size: usize,
    ) !?ReservedHiddenCarrier {
        if (comptime !build_options.enable_metal) return null;
        const pair = (try metal_compute_mod.MetalCompute.reserveHiddenStatePair(cb, rows, hidden_size)) orelse return null;
        if (!(try metal_compute_mod.MetalCompute.copyTensorInto(cb, hidden_input, pair.front))) {
            cb.free(pair.front);
            cb.free(pair.back);
            return null;
        }
        return .{
            .front = pair.front,
            .back = pair.back,
        };
    }

    fn active(self: *const ReservedHiddenCarrier) ops.CT {
        return if (self.active_front) self.front else self.back;
    }

    fn inactive(self: *const ReservedHiddenCarrier) ops.CT {
        return if (self.active_front) self.back else self.front;
    }

    fn ownsSlot(self: *const ReservedHiddenCarrier, tensor: ops.CT) bool {
        return tensor == self.front or tensor == self.back;
    }

    fn replaceActive(self: *ReservedHiddenCarrier, cb: *const ops.ComputeBackend, next_hidden: ops.CT) !void {
        if (!(try metal_compute_mod.MetalCompute.copyTensorInto(cb, next_hidden, self.inactive()))) {
            return error.UnsupportedTensorType;
        }
        if (!self.ownsSlot(next_hidden)) cb.free(next_hidden);
        self.active_front = !self.active_front;
    }

    fn deinit(self: *ReservedHiddenCarrier, cb: *const ops.ComputeBackend, keep_active: bool) void {
        if (keep_active) {
            cb.free(self.inactive());
        } else {
            cb.free(self.front);
            cb.free(self.back);
        }
        self.* = undefined;
    }
};

test "ReservedHiddenCarrier does not free carrier slots during replacement" {
    const front: ops.CT = @ptrFromInt(0x1000);
    const back: ops.CT = @ptrFromInt(0x2000);
    const external: ops.CT = @ptrFromInt(0x3000);
    const carrier = ReservedHiddenCarrier{ .front = front, .back = back };

    try std.testing.expect(carrier.ownsSlot(front));
    try std.testing.expect(carrier.ownsSlot(back));
    try std.testing.expect(!carrier.ownsSlot(external));
}

fn applyPleDirect(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    hidden: ops.CT,
    ple_vectors: ops.CT,
    layer: usize,
    rows: usize,
    allow_host_fallback: bool,
) !?ops.CT {
    const ple_dim: usize = gpt_config.ple_hidden_size;
    const hidden_size: usize = gpt_config.hidden_size;
    const ple_offset = layer * ple_dim;
    if (rows == 0) return null;
    const activation = decoderActivationKind(gpt_config);

    const ple_ct = try cb.sliceLastDim(ple_vectors, ple_offset, ple_offset + ple_dim);
    defer cb.free(ple_ct);
    const ple_input = if (materializeDirectPleSliceRequested())
        try cloneTensorForCompare(cb, std.heap.page_allocator, ple_ct)
    else
        ple_ct;
    defer if (ple_input != ple_ct) cb.free(ple_input);
    try gpt_arch.maybeDumpGatedLayerStageStats(
        cb,
        std.heap.page_allocator,
        layer,
        "ple-input",
        ple_input,
        ple_dim,
    );

    if (try metal_compute_mod.MetalCompute.applyPleResidual(
        cb,
        hidden,
        ple_input,
        pleGateSlot(configured_layer_count, layer),
        pleProjSlot(configured_layer_count, layer),
        plePostNormSlot(configured_layer_count, layer),
        hidden_size,
        ple_dim,
        gpt_config.norm_eps,
        activation,
    )) |direct| return direct;

    const gate_proj = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = pleGateSlot(configured_layer_count, layer),
        .input = hidden,
        .in_dim = hidden_size,
        .out_dim = ple_dim,
    })) orelse return null;
    defer cb.free(gate_proj);
    try gpt_arch.maybeDumpGatedLayerStageStats(
        cb,
        std.heap.page_allocator,
        layer,
        "ple-gate-proj",
        gate_proj,
        ple_dim,
    );

    const gate = (try cb.decoderRuntimeApplyActivation(&.{
        .input = gate_proj,
        .kind = activation,
        .dim = ple_dim,
    })) orelse blk: {
        if (!allow_host_fallback) return null;
        break :blk try gpt_arch.applyActivation(cb, gpt_config, gate_proj);
    };
    defer cb.free(gate);
    try gpt_arch.maybeDumpGatedLayerStageStats(
        cb,
        std.heap.page_allocator,
        layer,
        "ple-gate",
        gate,
        ple_dim,
    );

    const gated = try cb.multiply(gate, ple_input);
    defer cb.free(gated);
    try gpt_arch.maybeDumpGatedLayerStageStats(
        cb,
        std.heap.page_allocator,
        layer,
        "ple-gated",
        gated,
        ple_dim,
    );

    const projected = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = pleProjSlot(configured_layer_count, layer),
        .input = gated,
        .in_dim = ple_dim,
        .out_dim = hidden_size,
    })) orelse return null;
    defer cb.free(projected);
    try gpt_arch.maybeDumpGatedLayerStageStats(
        cb,
        std.heap.page_allocator,
        layer,
        "ple-proj",
        projected,
        hidden_size,
    );

    const normed = (try cb.decoderRuntimeApplyRmsNorm(&.{
        .slot = plePostNormSlot(configured_layer_count, layer),
        .input = projected,
        .hidden_size = hidden_size,
        .eps = gpt_config.norm_eps,
    })) orelse return null;
    defer cb.free(normed);
    try gpt_arch.maybeDumpGatedLayerStageStats(
        cb,
        std.heap.page_allocator,
        layer,
        "ple-post",
        normed,
        hidden_size,
    );

    return (try cb.decoderRuntimeApplyAdd(&.{
        .lhs = hidden,
        .rhs = normed,
        .dim = hidden_size,
    })) orelse cb.add(hidden, normed);
}

fn computePleVectorsDirect(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    input_ids: []const i64,
    hidden: ops.CT,
    total: usize,
) !?ops.CT {
    if (gpt_config.family != .gemma or !gpt_config.hasPle()) return null;
    if (total == 0) return null;

    const ple_dim: usize = gpt_config.ple_hidden_size;
    const num_layers: usize = gpt_config.num_hidden_layers;
    const ple_total_dim = ple_dim * num_layers;

    const planned_scope = try metal_compute_mod.MetalCompute.beginPlannedGraphScope(cb, .ple);
    defer metal_compute_mod.MetalCompute.endPlannedGraphScope(cb, planned_scope) catch {};

    var started_at = monotonicNowNs();
    const token_w = gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_input.per_layer_token_embd.weight") catch |err| switch (err) {
        error.MissingWeight => try gpt_arch.getModelWeight(cb, gpt_config, "model.embed_tokens_per_layer.weight"),
        else => return err,
    };
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_ple_lookup_nanos += finished_at - started_at;
    defer cb.free(token_w);

    started_at = monotonicNowNs();
    const token_embd_raw = try cb.embeddingLookup(token_w, input_ids, total, ple_total_dim);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_ple_embedding_nanos += finished_at - started_at;
    defer cb.free(token_embd_raw);

    started_at = monotonicNowNs();
    const model_proj_raw = (try cb.decoderRuntimeApplyLinear(&.{
        .slot = pleModelProjSlot(configured_layer_count),
        .input = hidden,
        .in_dim = gpt_config.hidden_size,
        .out_dim = ple_total_dim,
    })) orelse return null;
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_ple_model_proj_nanos += finished_at - started_at;
    defer cb.free(model_proj_raw);

    started_at = monotonicNowNs();
    const reshaped = (try cb.reshape2d(model_proj_raw, total * num_layers, ple_dim)) orelse return null;
    defer cb.free(reshaped);

    const normed_flat = (try cb.decoderRuntimeApplyRmsNorm(&.{
        .slot = pleProjNormSlot(configured_layer_count),
        .input = reshaped,
        .hidden_size = ple_dim,
        .eps = gpt_config.norm_eps,
    })) orelse return null;
    defer cb.free(normed_flat);

    const normed_proj = (try cb.reshape2d(normed_flat, total, ple_total_dim)) orelse return null;
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_ple_norm_nanos += finished_at - started_at;
    defer cb.free(normed_proj);

    started_at = monotonicNowNs();
    const embed_scale_val = @sqrt(@as(f32, @floatFromInt(ple_dim)));
    const combine_scale_val: f32 = 1.0 / @sqrt(2.0);
    if (try cb.decoderRuntimeApplyScaledAddScale(&.{
        .lhs = token_embd_raw,
        .rhs = normed_proj,
        .dim = total * ple_total_dim,
        .lhs_scale = embed_scale_val,
        .output_scale = combine_scale_val,
    })) |combined_scaled| {
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.prefill_ple_combine_nanos += finished_at - started_at;
        return combined_scaled;
    }

    const scale_shape = [_]i32{1};
    const embed_scale_ct = try cb.fromFloat32Shape(&[_]f32{embed_scale_val}, &scale_shape);
    defer cb.free(embed_scale_ct);

    const token_scaled = try cb.multiply(token_embd_raw, embed_scale_ct);
    defer cb.free(token_scaled);

    const combined = try cb.add(token_scaled, normed_proj);
    defer cb.free(combined);

    const combine_scale_ct = try cb.fromFloat32Shape(&[_]f32{combine_scale_val}, &scale_shape);
    defer cb.free(combine_scale_ct);

    const combined_scaled = try cb.multiply(combined, combine_scale_ct);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_ple_combine_nanos += finished_at - started_at;
    return combined_scaled;
}

pub fn preparedLayers(configured_layers: usize) usize {
    return gemma4_runtime.preparedLayers(configured_layers);
}

fn prepareTraceRequested() bool {
    return c_std.getenv("TERMITE_METAL_PREPARE_TRACE") != null;
}

fn tracePrepareLayerFailure(layer: usize, kind: []const u8, slot: usize, in_dim: usize, out_dim: usize) void {
    if (!prepareTraceRequested()) return;
    std.debug.print(
        "prepare-trace: prepare-fail layer={d} kind={s} slot={d} in={d} out={d}\n",
        .{ layer, kind, slot, in_dim, out_dim },
    );
}

pub fn overrideLevel() usize {
    const value = c_std.getenv("TERMITE_METAL_WHOLE_TOKEN_GATED_OVERRIDE_LEVEL") orelse return 4;
    return std.fmt.parseUnsigned(usize, std.mem.span(value), 10) catch 4;
}

pub fn supportsConfig(gpt_config: gpt_mod.Config) bool {
    return switch (gpt_config.family) {
        // Multimodal gated decoders can use the same qLen=1 whole-token decode
        // path after prefill because vision/PLE state is already reflected in
        // the retained KV. The remaining unsupported cases are the extra
        // decoder-side sublayers that still branch the block structure.
        .llama, .mistral, .qwen2, .qwen3 => !gpt_config.usesMoe() and !gpt_config.hasPle(),
        .gemma => gemma4_runtime.supportsRuntimeConfig(gpt_config),
        else => false,
    };
}

fn supportsDirectGatedRuntime(gpt_config: gpt_mod.Config, configured_layer_count: usize, decode_context: *const gpt_arch.DecodeContext) bool {
    if (disableDirectGatedFamilyRequested()) return false;
    if (!supportsConfig(gpt_config)) return false;
    if (gpt_config.family == .gemma) return false;
    if (gpt_config.isMultimodal()) return false;
    if (gpt_config.num_kv_shared_layers != 0) return false;
    if (gpt_config.global_head_dim != 0 or gpt_config.num_global_key_value_heads != 0) return false;
    if (gpt_config.sliding_window != 0) return false;
    if (preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers)) != gpt_config.num_hidden_layers) return false;
    return decode_context.attention_mode == .paged_decode or decode_context.attention_mode == .paged_prefill;
}

fn supportsDirectGemmaRuntime(gpt_config: gpt_mod.Config, configured_layer_count: usize, decode_context: *const gpt_arch.DecodeContext) bool {
    if (disableDirectGatedFamilyRequested()) return false;
    if (gpt_config.family != .gemma) return false;
    if (!supportsConfig(gpt_config)) return false;
    if (preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers)) != gpt_config.num_hidden_layers) return false;
    return decode_context.attention_mode == .paged_decode or decode_context.attention_mode == .paged_prefill;
}

fn tryBackendOwnedGreedyToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?i64 {
    if (gatedFamilyCompareRequested()) return null;
    if (!supportsDirectGemmaRuntime(gpt_config, configured_layer_count, decode_context)) return null;
    if (!gpt_config.hasPle()) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;
    if (gpt_config.num_hidden_layers > 256) return null;

    var layers_buf: [256]contracts.DecoderRuntimeLayerSpec = undefined;
    const layers = try gemma4_runtime.fillLayerSpecs(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        &layers_buf,
        true,
    );
    const layer_count = layers.len;

    var attention = gpt_arch.attentionContextFromDecode(decode_context);
    attention.layer_index = 0;
    attention.skip_kv_write = false;
    const items = [_]contracts.DecoderRuntimeDecodeItem{.{
        .token_id = token_id,
        .position = if (seq_len == 0) 0 else seq_len - 1,
        .seq_len = seq_len,
        .attention = attention,
    }};
    var output_token_ids = [_]i64{0};
    const token_embedding_weight = try gpt_arch.getEmbeddingWeight(cb, gpt_config);
    defer cb.free(token_embedding_weight);
    const ple_token_embedding_weight = gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_input.per_layer_token_embd.weight") catch |err| switch (err) {
        error.MissingWeight => try gpt_arch.getModelWeight(cb, gpt_config, "model.embed_tokens_per_layer.weight"),
        else => return err,
    };
    defer cb.free(ple_token_embedding_weight);
    const request = contracts.DecoderRuntimeDecodeRequest{
        .contract = .gemma4_gated_ple_shared_kv,
        .mode = .greedy_argmax,
        .configured_layer_count = configured_layer_count,
        .layer_count = layer_count,
        .hidden_size = gpt_config.hidden_size,
        .vocab_size = gpt_config.vocab_size,
        .num_attention_heads = gpt_config.num_attention_heads,
        .norm_eps = gpt_config.norm_eps,
        .ple_hidden_size = gpt_config.ple_hidden_size,
        .token_embedding_scale = gpt_config.tokenEmbeddingScale(),
        .global_head_dim = gpt_config.global_head_dim,
        .rope_freq_scale = gpt_config.rope_freq_scale,
        .rope_consecutive_pairs = gpt_config.rope_layout == .consecutive_pairs,
        .activation = decoderActivationKind(gpt_config),
        .final_norm_slot = finalNormSlot(configured_layer_count),
        .final_lm_head_slot = finalLmHeadSlot(configured_layer_count),
        .ple_model_proj_slot = if (gpt_config.hasPle()) pleModelProjSlot(configured_layer_count) else null,
        .ple_proj_norm_slot = if (gpt_config.hasPle()) pleProjNormSlot(configured_layer_count) else null,
        .layers = layers,
        .items = items[0..],
        .token_embedding_weight = token_embedding_weight,
        .ple_token_embedding_weight = ple_token_embedding_weight,
        .output_token_ids = output_token_ids[0..],
    };
    if (try cb.decoderRuntimeDecodeBatch(&request)) return output_token_ids[0];
    return null;
}

fn shouldUseDecoderRuntimeFrame(
    phase: BlockTimingPhase,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    decode_context: *const gpt_arch.DecodeContext,
) bool {
    switch (phase) {
        .prefill => {
            if (decode_context.attention_mode != .paged_prefill) return false;
            if (decode_context.query_sequence_len == 0) return false;
        },
        .greedy, .sampled => {
            if (decode_context.attention_mode != .paged_decode) return false;
            if (decode_context.query_sequence_len != 1) return false;
        },
    }
    // Debug/compare paths may materialize tensors on the host between layer
    // submissions. Keep those on the conservative per-call path.
    if (gatedFamilyCompareRequested() and !gatedFamilyCompareAllowFrameRequested()) return false;
    if (c_std.getenv("TERMITE_DEBUG_GPT_STATS") != null) return false;
    if (disableGatedFamilyRuntimePrefillBlockRequested(gpt_config, configured_layer_count)) return false;
    return true;
}

pub fn preplanPrefillFrame(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    rows: usize,
) !bool {
    if (rows <= 1) return false;
    if (disableDirectGatedFamilyRequested()) return false;
    if (gpt_config.family != .gemma) return false;
    if (!supportsConfig(gpt_config)) return false;
    if (preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers)) != gpt_config.num_hidden_layers) return false;
    if (!gpt_config.hasPle()) return false;
    if (gpt_config.ple_hidden_size == 0) return false;
    if (gpt_config.num_hidden_layers > 256) return false;

    var frame_layers_buf: [256]contracts.DecoderRuntimeLayerSpec = undefined;
    const layer_spec_started_at = monotonicNowNs();
    const layers = try gemma4_runtime.fillLayerSpecs(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        &frame_layers_buf,
        true,
    );
    const layer_spec_finished_at = monotonicNowNs();
    if (layer_spec_finished_at > layer_spec_started_at) {
        timing_stats.preplan_frame_layer_spec_nanos += layer_spec_finished_at - layer_spec_started_at;
    }

    const frame_plan_started_at = monotonicNowNs();
    const planned = try cb.decoderRuntimePlanPrefillFrame(&.{
        .contract = .gemma4_gated_ple_shared_kv,
        .layer_count = layers.len,
        .rows = rows,
        .hidden_size = gpt_config.hidden_size,
        .vocab_size = gpt_config.vocab_size,
        .num_attention_heads = gpt_config.num_attention_heads,
        .global_head_dim = gpt_config.global_head_dim,
        .ple_hidden_size = gpt_config.ple_hidden_size,
        .final_norm_slot = finalNormSlot(configured_layer_count),
        .final_lm_head_slot = finalLmHeadSlot(configured_layer_count),
        .layers = layers,
    });
    const frame_plan_finished_at = monotonicNowNs();
    if (frame_plan_finished_at > frame_plan_started_at) {
        timing_stats.preplan_frame_plan_nanos += frame_plan_finished_at - frame_plan_started_at;
    }
    return planned;
}

pub fn prewarmPleTokenEmbedding(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
) !bool {
    if (gpt_config.family != .gemma or !gpt_config.hasPle()) return false;
    if (gpt_config.ple_hidden_size == 0 or gpt_config.num_hidden_layers == 0) return false;
    const ple_total_dim = try std.math.mul(usize, gpt_config.ple_hidden_size, gpt_config.num_hidden_layers);
    const token_w = gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_input.per_layer_token_embd.weight") catch |err| switch (err) {
        error.MissingWeight => try gpt_arch.getModelWeight(cb, gpt_config, "model.embed_tokens_per_layer.weight"),
        else => return err,
    };
    defer cb.free(token_w);
    const ids = [_]i64{0};
    const embedding = try cb.embeddingLookup(token_w, ids[0..], ids.len, ple_total_dim);
    defer cb.free(embedding);
    return true;
}

fn finishDecoderRuntimeFrame(cb: *const ops.ComputeBackend, active: *bool) void {
    if (!active.*) return;
    cb.decoderRuntimeSubmitAndWaitFrame() catch |err| {
        std.log.warn("decoder runtime frame submit failed: {s}", .{@errorName(err)});
    };
    active.* = false;
}

fn cancelDecoderRuntimeFrame(cb: *const ops.ComputeBackend, active: *bool) void {
    if (!active.*) return;
    cb.decoderRuntimeCancelFrame() catch |err| {
        std.log.warn("decoder runtime frame cancel failed: {s}", .{@errorName(err)});
    };
    active.* = false;
}

fn maybeFlushDecoderRuntimePrefillFrame(
    cb: *const ops.ComputeBackend,
    phase: BlockTimingPhase,
    active: *bool,
) !void {
    if (phase != .prefill) return;
    if (!active.*) return;
    if (disableGatedFamilyPrefillFrameBarriersRequested()) return;
    try cb.decoderRuntimeFlushActiveFrame();
}

fn getLayerOutputScaleWeight(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    layer: usize,
) ?ops.CT {
    var output_scale_name_buf: [256]u8 = undefined;
    const output_scale_name = std.fmt.bufPrint(
        &output_scale_name_buf,
        "model.layers.{d}.per_layer_input.layer_output_scale.weight",
        .{layer},
    ) catch return null;
    return gpt_arch.getModelWeight(cb, gpt_config, output_scale_name) catch blk: {
        var fallback_buf: [256]u8 = undefined;
        const fallback_name = std.fmt.bufPrint(
            &fallback_buf,
            "model.layers.{d}.layer_scalar",
            .{layer},
        ) catch return null;
        break :blk gpt_arch.getModelWeight(cb, gpt_config, fallback_name) catch null;
    };
}

fn forwardFinalHiddenTensorGemmaDirect(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    hidden_input: ops.CT,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    phase: BlockTimingPhase,
    ple_vectors: ?ops.CT,
) !?DirectHiddenResult {
    if (!supportsDirectGemmaRuntime(gpt_config, configured_layer_count, decode_context)) {
        if (prefillTraceRequested()) std.debug.print(
            "prefill-trace: gemma-direct unsupported family={s} qlen={d} mode={s} configured_layers={d}\n",
            .{
                @tagName(gpt_config.family),
                decode_context.query_sequence_len,
                @tagName(decode_context.attention_mode),
                configured_layer_count,
            },
        );
        return null;
    }

    const should_use_decoder_frame = shouldUseDecoderRuntimeFrame(phase, gpt_config, configured_layer_count, decode_context);
    const frame_begin_started_at = monotonicNowNs();
    var decoder_frame_active = if (should_use_decoder_frame)
        cb.decoderRuntimeHasActiveFrame() or try cb.decoderRuntimeBeginFrame()
    else
        false;
    const frame_begin_finished_at = monotonicNowNs();
    if (phase == .prefill and frame_begin_finished_at > frame_begin_started_at) {
        timing_stats.prefill_frame_begin_nanos += frame_begin_finished_at - frame_begin_started_at;
    }
    if (prefillTraceRequested()) std.debug.print(
        "prefill-trace: gemma-direct frame_active={} phase={s} qlen={d} seq={d} ple={}\n",
        .{
            decoder_frame_active,
            @tagName(phase),
            decode_context.query_sequence_len,
            seq_len,
            ple_vectors != null,
        },
    );
    var return_decoder_frame = false;
    defer if (!return_decoder_frame) finishDecoderRuntimeFrame(cb, &decoder_frame_active);
    errdefer cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
    var frame_layers_buf: [256]contracts.DecoderRuntimeLayerSpec = undefined;
    var prefill_frame_planned = false;
    var planned_frame_layers: []const contracts.DecoderRuntimeLayerSpec = &.{};
    if (decoder_frame_active and phase == .prefill and decode_context.query_sequence_len > 1 and gpt_config.num_hidden_layers <= frame_layers_buf.len) {
        const layer_spec_started_at = monotonicNowNs();
        planned_frame_layers = try gemma4_runtime.fillLayerSpecs(
            cb,
            allocator,
            gpt_config,
            configured_layer_count,
            &frame_layers_buf,
            true,
        );
        const layer_spec_finished_at = monotonicNowNs();
        if (layer_spec_finished_at > layer_spec_started_at) {
            timing_stats.prefill_frame_layer_spec_nanos += layer_spec_finished_at - layer_spec_started_at;
        }
        const frame_plan_started_at = monotonicNowNs();
        prefill_frame_planned = try cb.decoderRuntimePlanPrefillFrame(&.{
            .contract = .gemma4_gated_ple_shared_kv,
            .layer_count = planned_frame_layers.len,
            .rows = decode_context.query_sequence_len,
            .hidden_size = gpt_config.hidden_size,
            .vocab_size = gpt_config.vocab_size,
            .num_attention_heads = gpt_config.num_attention_heads,
            .global_head_dim = gpt_config.global_head_dim,
            .ple_hidden_size = gpt_config.ple_hidden_size,
            .final_norm_slot = finalNormSlot(configured_layer_count),
            .final_lm_head_slot = finalLmHeadSlot(configured_layer_count),
            .layers = planned_frame_layers,
        });
        const frame_plan_finished_at = monotonicNowNs();
        if (frame_plan_finished_at > frame_plan_started_at) {
            timing_stats.prefill_frame_plan_nanos += frame_plan_finished_at - frame_plan_started_at;
        }
        if (prefillTraceRequested()) std.debug.print(
            "prefill-trace: gemma-direct frame_plan planned={} layers={d} rows={d}\n",
            .{ prefill_frame_planned, planned_frame_layers.len, decode_context.query_sequence_len },
        );
    } else if (prefillTraceRequested()) {
        std.debug.print(
            "prefill-trace: gemma-direct frame_plan skipped active={} phase={s} qlen={d} layers={d}\n",
            .{ decoder_frame_active, @tagName(phase), decode_context.query_sequence_len, gpt_config.num_hidden_layers },
        );
    }

    if (prefill_frame_planned and
        ple_vectors != null and
        !gatedFamilyCompareRequested() and
        gatedFamilyDumpLayer() == null)
    {
        var frame_hidden: ?ops.CT = null;
        const frame_attention = gpt_arch.attentionContextFromDecode(decode_context);
        const frame_execute_started_at = monotonicNowNs();
        if (try cb.decoderRuntimeExecuteGraphCommandPlanFrame(&.{
            .contract = .gemma4_gated_ple_shared_kv,
            .layer_count = planned_frame_layers.len,
            .rows = decode_context.query_sequence_len,
            .hidden_size = gpt_config.hidden_size,
            .vocab_size = gpt_config.vocab_size,
            .num_attention_heads = gpt_config.num_attention_heads,
            .global_head_dim = gpt_config.global_head_dim,
            .ple_hidden_size = gpt_config.ple_hidden_size,
            .final_norm_slot = finalNormSlot(configured_layer_count),
            .norm_eps = gpt_config.norm_eps,
            .rope_freq_scale = gpt_config.rope_freq_scale,
            .rope_consecutive_pairs = gpt_config.rope_layout == .consecutive_pairs,
            .activation = decoderActivationKind(gpt_config),
            .attention = frame_attention,
            .hidden = hidden_input,
            .ple_vectors = ple_vectors,
            .layers = planned_frame_layers,
            .output_hidden = &frame_hidden,
        })) {
            const frame_execute_finished_at = monotonicNowNs();
            if (frame_execute_finished_at > frame_execute_started_at) {
                timing_stats.prefill_frame_execute_nanos += frame_execute_finished_at - frame_execute_started_at;
            }
            return_decoder_frame = decoder_frame_active;
            return .{
                .hidden = frame_hidden orelse return error.UnexpectedNull,
                .total_rows = decode_context.query_sequence_len,
                .decoder_frame_active = return_decoder_frame,
            };
        } else if (prefillTraceRequested()) {
            std.debug.print(
                "prefill-trace: gemma-direct frame_execute=false layers={d} rows={d}\n",
                .{ planned_frame_layers.len, decode_context.query_sequence_len },
            );
        }
        const frame_execute_finished_at = monotonicNowNs();
        if (frame_execute_finished_at > frame_execute_started_at) {
            timing_stats.prefill_frame_execute_nanos += frame_execute_finished_at - frame_execute_started_at;
        }
    } else if (prefillTraceRequested()) {
        std.debug.print(
            "prefill-trace: gemma-direct frame_execute skipped planned={} ple={} compare={} dump={}\n",
            .{
                prefill_frame_planned,
                ple_vectors != null,
                gatedFamilyCompareRequested(),
                gatedFamilyDumpLayer() != null,
            },
        );
    }

    const use_copy_based_reserved_hidden = !decoder_frame_active or phase != .prefill or decode_context.query_sequence_len <= 1;
    var reserved_hidden: ?ReservedHiddenCarrier = if (disableReservedHiddenCarrierRequested() or !use_copy_based_reserved_hidden)
        null
    else
        try ReservedHiddenCarrier.init(
            cb,
            hidden_input,
            decode_context.query_sequence_len,
            gpt_config.hidden_size,
        );
    errdefer if (reserved_hidden) |*carrier| carrier.deinit(cb, false);

    var hidden = if (reserved_hidden) |*carrier| carrier.active() else hidden_input;
    var owns_hidden = false;
    errdefer if (owns_hidden) cb.free(hidden);
    var compare_hidden: ?ops.CT = null;
    defer if (compare_hidden) |ct| cb.free(ct);
    if (gatedFamilyCompareRequested()) {
        compare_hidden = try cloneTensorForCompare(cb, allocator, hidden_input);
    }

    const layer_count: usize = gpt_config.num_hidden_layers;
    for (0..layer_count) |layer| {
        if (phase == .prefill) timing_stats.prefill_layers += 1;
        var layer_input_snapshot: ?ops.CT = null;
        defer if (layer_input_snapshot) |ct| cb.free(ct);
        if (shouldCompareGatedFamilyLayer(layer)) {
            layer_input_snapshot = try cloneTensorForCompare(cb, allocator, hidden);
            if (compare_hidden != null) {
                var input_label_buf: [80]u8 = undefined;
                const input_label = try std.fmt.bufPrint(&input_label_buf, "layer-{d}-hidden-in", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    input_label,
                    hidden,
                    decode_context.query_sequence_len,
                    compare_hidden.?,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
            }
        }
        const attention_started_at = monotonicNowNs();
        const shares_kv = gpt_config.layerSharesKv(layer);
        const kv_layer_index = if (shares_kv) gpt_config.kvDonorLayerIndex(layer).? else layer;
        const head_dim = gpt_config.effectiveHeadDimForLayer(layer);
        const num_kv_heads = gpt_config.effectiveKVHeadsForLayer(layer);
        const attention_input_size = gpt_config.num_attention_heads * head_dim;
        const kv_dim = num_kv_heads * head_dim;

        const attention_qkv_started_at = monotonicNowNs();
        const attn_normed = (try cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = normSlot(layer, .attn_pre),
            .input = hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
        })) orelse {
            recordGemmaDirectRmsNorm(false);
            if (owns_hidden) cb.free(hidden);
            return null;
        };
        recordGemmaDirectRmsNorm(true);
        if (phase == .prefill) timing_stats.prefill_attn_norm_ops += 1;
        defer cb.free(attn_normed);
        traceGatedFamilyDevice(cb, layer, "hidden", hidden);
        traceGatedFamilyDevice(cb, layer, "attn_normed", attn_normed);
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "attn-norm",
            attn_normed,
            gpt_config.hidden_size,
        );

        if (phase == .prefill and
            decode_context.attention_mode == .paged_prefill and
            gpt_config.position_encoding == .rope and
            !disableDirectPrefillSeedRequested() and
            !disableGatedFamilyRuntimePrefillBlockRequested(gpt_config, configured_layer_count) and
            !(gpt_config.family == .gemma and gpt_config.global_head_dim != 0) and
            !shouldCompareGatedFamilyLayer(layer) and
            (ple_vectors == null or !disableDirectPleRequested()))
        {
            var block_ple_input: ?ops.CT = null;
            defer if (block_ple_input) |ple_ct| cb.free(ple_ct);
            if (ple_vectors) |ple| {
                const ple_dim: usize = gpt_config.ple_hidden_size;
                const ple_offset = layer * ple_dim;
                block_ple_input = try cb.sliceLastDim(ple, ple_offset, ple_offset + ple_dim);
            }

            var block_output_scale: ?ops.CT = null;
            defer if (block_output_scale) |scale| cb.free(scale);
            if (block_ple_input != null or ple_vectors == null) {
                var output_scale_name_buf: [256]u8 = undefined;
                const output_scale_name = std.fmt.bufPrint(
                    &output_scale_name_buf,
                    "model.layers.{d}.per_layer_input.layer_output_scale.weight",
                    .{layer},
                ) catch "";
                if (output_scale_name.len != 0) {
                    block_output_scale = gpt_arch.getModelWeight(cb, gpt_config, output_scale_name) catch null;
                }
            }

            var attention = gpt_arch.attentionContextFromDecode(decode_context);
            attention.layer_index = kv_layer_index;
            attention.skip_kv_write = shares_kv;
            const rope_dim: usize = gpt_config.layerRopeActiveDim(layer);
            const rope_theta = blk: {
                const base_theta = gpt_config.layerRopeTheta(layer);
                const freq_dim: f32 = @floatFromInt(gpt_config.layerRopeFrequencyDim(layer));
                const active_dim: f32 = @floatFromInt(rope_dim);
                if (active_dim < freq_dim) {
                    break :blk std.math.pow(f32, base_theta, active_dim / freq_dim);
                }
                break :blk base_theta;
            };

            const block_started_at = monotonicNowNs();
            if (try cb.runGatedDecoderBlock(&.{
                .attention_input = attn_normed,
                .residual = hidden,
                .attention = attention,
                .num_heads = gpt_config.num_attention_heads,
                .num_kv_heads = num_kv_heads,
                .head_dim = head_dim,
                .q_linear_slot = linearSlot(layer, .attn_q),
                .k_linear_slot = if (!shares_kv) linearSlot(layer, .attn_k) else null,
                .v_linear_slot = if (!shares_kv) linearSlot(layer, .attn_v) else null,
                .q_head_norm_slot = qHeadNormSlot(configured_layer_count, layer),
                .k_head_norm_slot = if (!shares_kv) kHeadNormSlot(configured_layer_count, layer) else null,
                .rope_active_dim = rope_dim,
                .rope_theta = rope_theta,
                .rope_freq_scale = gpt_config.rope_freq_scale,
                .rope_consecutive_pairs = gpt_config.rope_layout == .consecutive_pairs,
                .global_head_dim = gpt_config.global_head_dim,
                .attention_linear_slot = linearSlot(layer, .attn_out_proj),
                .attention_post_linear_rms_norm_slot = if (gpt_config.family == .qwen3) null else normSlot(layer, .attn_post),
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
                .ffn_rms_norm_slot = normSlot(layer, .ffn_pre),
                .ffn_post_gate_rms_norm_slot = null,
                .ffn_post_down_rms_norm_slot = if (gpt_config.family == .qwen3) null else normSlot(layer, .ffn_post),
                .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
                .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
                .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
                .intermediate_size = gpt_config.intermediateSize(layer),
                .activation = decoderActivationKind(gpt_config),
                .ple = block_ple_input,
                .ple_gate_linear_slot = if (block_ple_input != null) pleGateSlot(configured_layer_count, layer) else null,
                .ple_proj_linear_slot = if (block_ple_input != null) pleProjSlot(configured_layer_count, layer) else null,
                .ple_post_norm_slot = if (block_ple_input != null) plePostNormSlot(configured_layer_count, layer) else null,
                .ple_hidden_size = if (block_ple_input != null) gpt_config.ple_hidden_size else 0,
                .output_scale = block_output_scale,
                .graph_plan_tail_vocab_size = gpt_config.vocab_size,
            })) |block_hidden| {
                const block_finished_at = monotonicNowNs();
                if (shares_kv) {
                    timing_stats.prefill_q_linear_ops += 1;
                    timing_stats.prefill_head_norm_ops += 1;
                } else {
                    recordGemmaDirectQkv(true);
                    timing_stats.prefill_qkv_ops += 1;
                    timing_stats.prefill_qkv_fused_ops += 1;
                    timing_stats.prefill_head_norm_ops += 2;
                    timing_stats.prefill_kv_seed_ops += 1;
                }
                if (block_ple_input != null) {
                    timing_stats.prefill_ple_ops += 1;
                    timing_stats.prefill_ple_direct_hits += 1;
                }
                if (block_output_scale != null) {
                    timing_stats.prefill_output_scale_ops += 1;
                }
                if (block_finished_at > block_started_at) {
                    const block_elapsed = block_finished_at - block_started_at;
                    recordGemmaDirectAttentionResidual(true, true);
                    recordGemmaDirectFfn(true, true, false, true);
                    addBlockAttentionFusedResidualTiming(phase, block_elapsed);
                    addBlockFfnFusedTiming(phase, block_elapsed);
                }
                const attention_finished_at = monotonicNowNs();
                if (attention_finished_at > attention_started_at) {
                    addBlockAttentionTiming(phase, attention_finished_at - attention_started_at);
                }
                try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);

                const prev_hidden = hidden;
                const next_hidden = if (block_output_scale != null) blk: {
                    break :blk block_hidden;
                } else blk: {
                    const output_scale_started_at = monotonicNowNs();
                    const scaled = try gpt_arch.applyLayerOutputScale(cb, allocator, gpt_config, block_hidden, decode_context.query_sequence_len, gpt_config.hidden_size, layer);
                    const output_scale_finished_at = monotonicNowNs();
                    if (output_scale_finished_at > output_scale_started_at) {
                        addBlockOutputScaleTiming(phase, output_scale_finished_at - output_scale_started_at);
                    }
                    break :blk scaled;
                };
                const parity_ok = try compareGatedFamilyLayer(
                    cb,
                    allocator,
                    gpt_config,
                    &compare_hidden,
                    next_hidden,
                    seq_len,
                    layer,
                    decode_context,
                    ple_vectors,
                );
                const committed_hidden = if (gemmaPrefillCompareRequested() and !parity_ok and compare_hidden != null) blk: {
                    std.debug.print(
                        "gemma-prefill-parity-mismatch layer={d}: using staged reference hidden\n",
                        .{layer},
                    );
                    const fallback_hidden = compare_hidden.?;
                    compare_hidden = null;
                    cb.free(next_hidden);
                    compare_hidden = try cloneTensorForCompare(cb, allocator, fallback_hidden);
                    break :blk fallback_hidden;
                } else next_hidden;
                if (reserved_hidden) |*carrier| {
                    try carrier.replaceActive(cb, committed_hidden);
                    hidden = carrier.active();
                    owns_hidden = false;
                } else {
                    if (owns_hidden) cb.free(prev_hidden);
                    hidden = committed_hidden;
                    owns_hidden = true;
                }
                continue;
            }
        }

        const q: ops.CT, const k_value: ops.CT, const v_value: ops.CT = if (shares_kv) blk: {
            const q_local = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = linearSlot(layer, .attn_q),
                .input = attn_normed,
                .in_dim = gpt_config.hidden_size,
                .out_dim = gpt_config.num_attention_heads * head_dim,
            })) orelse {
                if (owns_hidden) cb.free(hidden);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_q_linear_ops += 1;
            errdefer cb.free(q_local);
            const zero_k = try createZeroTensor(cb, allocator, decode_context.query_sequence_len * kv_dim);
            errdefer cb.free(zero_k);
            const zero_v = try createZeroTensor(cb, allocator, decode_context.query_sequence_len * kv_dim);
            break :blk .{ q_local, zero_k, zero_v };
        } else blk: {
            if (!disableGemmaFusedQkvRequested()) {
                if (try cb.decoderRuntimeApplyLinearQkv(&.{
                    .q_slot = linearSlot(layer, .attn_q),
                    .k_slot = linearSlot(layer, .attn_k),
                    .v_slot = linearSlot(layer, .attn_v),
                    .input = attn_normed,
                    .in_dim = gpt_config.hidden_size,
                    .q_out_dim = gpt_config.num_attention_heads * head_dim,
                    .kv_out_dim = kv_dim,
                })) |qkv| {
                    recordGemmaDirectQkv(true);
                    if (phase == .prefill) {
                        timing_stats.prefill_qkv_ops += 1;
                        timing_stats.prefill_qkv_fused_ops += 1;
                    }
                    break :blk .{ qkv.first, qkv.second, qkv.third };
                }
            }
            recordGemmaDirectQkv(false);
            if (phase == .prefill) {
                timing_stats.prefill_qkv_ops += 1;
                timing_stats.prefill_qkv_split_ops += 1;
            }

            const q_local = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = linearSlot(layer, .attn_q),
                .input = attn_normed,
                .in_dim = gpt_config.hidden_size,
                .out_dim = gpt_config.num_attention_heads * head_dim,
            })) orelse {
                if (owns_hidden) cb.free(hidden);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_q_linear_ops += 1;
            errdefer cb.free(q_local);
            if (gpt_config.family == .gemma) {
                const k_local = (try cb.decoderRuntimeApplyLinear(&.{
                    .slot = linearSlot(layer, .attn_k),
                    .input = attn_normed,
                    .in_dim = gpt_config.hidden_size,
                    .out_dim = kv_dim,
                })) orelse {
                    cb.free(q_local);
                    if (owns_hidden) cb.free(hidden);
                    return null;
                };
                errdefer cb.free(k_local);
                const v_local = (try cb.decoderRuntimeApplyLinear(&.{
                    .slot = linearSlot(layer, .attn_v),
                    .input = attn_normed,
                    .in_dim = gpt_config.hidden_size,
                    .out_dim = kv_dim,
                })) orelse {
                    cb.free(q_local);
                    cb.free(k_local);
                    if (owns_hidden) cb.free(hidden);
                    return null;
                };
                if (phase == .prefill) timing_stats.prefill_q_linear_ops += 2;
                break :blk .{ q_local, k_local, v_local };
            } else {
                const kv_local = (try cb.decoderRuntimeApplyLinearPair(&.{
                    .slot_a = linearSlot(layer, .attn_k),
                    .slot_b = linearSlot(layer, .attn_v),
                    .input = attn_normed,
                    .in_dim = gpt_config.hidden_size,
                    .out_dim = kv_dim,
                })) orelse {
                    cb.free(q_local);
                    if (owns_hidden) cb.free(hidden);
                    return null;
                };
                if (phase == .prefill) timing_stats.prefill_kv_pair_ops += 1;
                break :blk .{ q_local, kv_local.first, kv_local.second };
            }
        };
        defer cb.free(q);
        defer cb.free(k_value);
        defer cb.free(v_value);
        traceGatedFamilyDevice(cb, layer, "q_projected", q);
        traceGatedFamilyDevice(cb, layer, "k_projected", k_value);
        traceGatedFamilyDevice(cb, layer, "v_projected", v_value);
        try maybeSyncTensor(cb, allocator, q);
        try maybeSyncTensor(cb, allocator, k_value);
        try maybeSyncTensor(cb, allocator, v_value);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        const attention_qkv_finished_at = monotonicNowNs();
        if (attention_qkv_finished_at > attention_qkv_started_at) {
            addBlockAttentionQkvTiming(phase, attention_qkv_finished_at - attention_qkv_started_at);
        }

        const attention_head_norm_started_at = monotonicNowNs();
        var name_buf: [256]u8 = undefined;
        const q_attn = if (try gpt_arch.maybeApplyQKHeadNorm(
            cb,
            allocator,
            gpt_config,
            q,
            decode_context.query_sequence_len,
            @intCast(gpt_config.num_attention_heads * head_dim),
            layer,
            "q",
            @intCast(head_dim),
            &name_buf,
        )) |normed_q|
            normed_q
        else
            q;
        if (phase == .prefill and q_attn != q) timing_stats.prefill_head_norm_ops += 1;
        defer if (q_attn != q) cb.free(q_attn);
        traceGatedFamilyDevice(cb, layer, "q_attn", q_attn);

        const k_attn = if (!shares_kv) blk: {
            if (try gpt_arch.maybeApplyQKHeadNorm(
                cb,
                allocator,
                gpt_config,
                k_value,
                decode_context.query_sequence_len,
                @intCast(num_kv_heads * head_dim),
                layer,
                "k",
                @intCast(head_dim),
                &name_buf,
            )) |normed_k| {
                if (phase == .prefill) timing_stats.prefill_head_norm_ops += 1;
                break :blk normed_k;
            }
            break :blk k_value;
        } else k_value;
        defer if (k_attn != k_value) cb.free(k_attn);
        traceGatedFamilyDevice(cb, layer, "k_attn", k_attn);
        const q_for_attn = try prepareGemmaQueryForAttention(
            cb,
            allocator,
            gpt_config,
            q_attn,
            decode_context.query_sequence_len,
            head_dim,
            gpt_config.num_attention_heads * head_dim,
        );
        defer if (q_for_attn != q_attn) cb.free(q_for_attn);
        traceGatedFamilyDevice(cb, layer, "q_for_attn", q_for_attn);
        const v_for_attn = try prepareGemmaValueForAttention(
            cb,
            allocator,
            gpt_config,
            v_value,
            decode_context.query_sequence_len,
            num_kv_heads,
            head_dim,
            kv_dim,
            shares_kv,
        );
        defer if (v_for_attn != v_value) cb.free(v_for_attn);
        traceGatedFamilyDevice(cb, layer, "v_for_attn", v_for_attn);
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "q",
            q_for_attn,
            gpt_config.num_attention_heads * head_dim,
        );
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "k",
            k_attn,
            num_kv_heads * head_dim,
        );
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "v",
            v_for_attn,
            num_kv_heads * head_dim,
        );
        try maybeSyncTensor(cb, allocator, q_attn);
        try maybeSyncTensor(cb, allocator, k_attn);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        const attention_head_norm_finished_at = monotonicNowNs();
        if (attention_head_norm_finished_at > attention_head_norm_started_at) {
            addBlockAttentionHeadNormTiming(phase, attention_head_norm_finished_at - attention_head_norm_started_at);
        }
        if (attention_head_norm_finished_at > attention_qkv_started_at) {
            addBlockAttentionProjectTiming(phase, attention_head_norm_finished_at - attention_qkv_started_at);
        }
        if (shouldCompareGatedFamilyLayer(layer) and !shares_kv) {
            const compare_q = try gpt_arch.debugAttentionProject(
                cb,
                allocator,
                gpt_config,
                attn_normed,
                decode_context.query_sequence_len,
                @intCast(gpt_config.hidden_size),
                @intCast(gpt_config.num_attention_heads * head_dim),
                layer,
                "q",
                &name_buf,
            );
            defer cb.free(compare_q);
            const compare_k = try gpt_arch.debugAttentionProject(
                cb,
                allocator,
                gpt_config,
                attn_normed,
                decode_context.query_sequence_len,
                @intCast(gpt_config.hidden_size),
                @intCast(kv_dim),
                layer,
                "k",
                &name_buf,
            );
            defer cb.free(compare_k);
            const compare_v = try gpt_arch.debugAttentionProject(
                cb,
                allocator,
                gpt_config,
                attn_normed,
                decode_context.query_sequence_len,
                @intCast(gpt_config.hidden_size),
                @intCast(kv_dim),
                layer,
                "v",
                &name_buf,
            );
            defer cb.free(compare_v);

            const compare_q_attn = if (try gpt_arch.maybeApplyQKHeadNorm(
                cb,
                allocator,
                gpt_config,
                compare_q,
                decode_context.query_sequence_len,
                @intCast(gpt_config.num_attention_heads * head_dim),
                layer,
                "q",
                @intCast(head_dim),
                &name_buf,
            )) |normed_q|
                normed_q
            else
                compare_q;
            defer if (compare_q_attn != compare_q) cb.free(compare_q_attn);
            const compare_q_for_attn = try prepareGemmaQueryForAttention(
                cb,
                allocator,
                gpt_config,
                compare_q_attn,
                decode_context.query_sequence_len,
                head_dim,
                gpt_config.num_attention_heads * head_dim,
            );
            defer if (compare_q_for_attn != compare_q_attn) cb.free(compare_q_for_attn);

            const compare_k_attn = if (try gpt_arch.maybeApplyQKHeadNorm(
                cb,
                allocator,
                gpt_config,
                compare_k,
                decode_context.query_sequence_len,
                @intCast(num_kv_heads * head_dim),
                layer,
                "k",
                @intCast(head_dim),
                &name_buf,
            )) |normed_k|
                normed_k
            else
                compare_k;
            defer if (compare_k_attn != compare_k) cb.free(compare_k_attn);
            const compare_v_for_attn = try prepareGemmaValueForAttention(
                cb,
                allocator,
                gpt_config,
                compare_v,
                decode_context.query_sequence_len,
                num_kv_heads,
                head_dim,
                kv_dim,
                shares_kv,
            );
            defer if (compare_v_for_attn != compare_v) cb.free(compare_v_for_attn);

            var q_label_buf: [64]u8 = undefined;
            const q_label = try std.fmt.bufPrint(&q_label_buf, "layer-{d}-q", .{layer});
            _ = try compareLastHiddenRow(cb, allocator, q_label, q_for_attn, decode_context.query_sequence_len, compare_q_for_attn, decode_context.query_sequence_len, gpt_config.num_attention_heads * head_dim);

            var k_label_buf: [64]u8 = undefined;
            const k_label = try std.fmt.bufPrint(&k_label_buf, "layer-{d}-k", .{layer});
            _ = try compareLastHiddenRow(cb, allocator, k_label, k_attn, decode_context.query_sequence_len, compare_k_attn, decode_context.query_sequence_len, kv_dim);

            var v_label_buf: [64]u8 = undefined;
            const v_label = try std.fmt.bufPrint(&v_label_buf, "layer-{d}-v", .{layer});
            _ = try compareLastHiddenRow(cb, allocator, v_label, v_for_attn, decode_context.query_sequence_len, compare_v_for_attn, decode_context.query_sequence_len, kv_dim);
        }

        var prefill_span_seeded = false;
        if (tracePrefillSeedRequested()) {
            std.debug.print(
                "metal-prefill-seed-check layer={d} kv_layer={d} mode={s} shares_kv={} disabled={} q={d} kv={d} pos={d}\n",
                .{
                    layer,
                    kv_layer_index,
                    @tagName(decode_context.attention_mode),
                    shares_kv,
                    disableDirectPrefillSeedRequested(),
                    decode_context.query_sequence_len,
                    decode_context.kv_sequence_len,
                    decode_context.kv_position_offset,
                },
            );
        }
        if (decode_context.attention_mode == .paged_prefill and !shares_kv and !disableDirectPrefillSeedRequested()) {
            var attention_seed = gpt_arch.attentionContextFromDecode(decode_context);
            attention_seed.layer_index = kv_layer_index;
            attention_seed.skip_kv_write = shares_kv;
            const seed_k = try prepareKeyForPagedPrefillSeed(
                cb,
                gpt_config,
                k_attn,
                seq_len,
                decode_context.query_sequence_len,
                head_dim,
                layer,
            );
            defer if (seed_k != k_attn) cb.free(seed_k);
            const seed_ok = try cb.seedPagedAttentionSpan(&.{
                .k = seed_k,
                .v = v_for_attn,
                .attention = attention_seed,
                .num_kv_heads = num_kv_heads,
                .head_dim = head_dim,
            });
            if (tracePrefillSeedRequested()) {
                std.debug.print(
                    "metal-prefill-seed-result layer={d} kv_layer={d} ok={} q={d} kv={d} pos={d}\n",
                    .{
                        layer,
                        kv_layer_index,
                        seed_ok,
                        attention_seed.query_sequence_len,
                        attention_seed.kv_sequence_len,
                        attention_seed.kv_position_offset,
                    },
                );
            }
            prefill_span_seeded = true;
            if (phase == .prefill) timing_stats.prefill_kv_seed_ops += 1;
            try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
            if (shouldCompareGatedFamilyLayer(layer) and attention_seed.kv_cache != null) {
                if (try metal_compute_mod.MetalCompute.debugGatherPagedKvLayer(
                    cb,
                    allocator,
                    attention_seed.kv_cache.?,
                    attention_seed.kv_sequence_len,
                    kv_layer_index,
                )) |cached_rows| {
                    defer {
                        allocator.free(cached_rows.k);
                        allocator.free(cached_rows.v);
                    }
                    const cached_shape = [_]i32{
                        @intCast(attention_seed.kv_sequence_len),
                        @intCast(kv_dim),
                    };
                    const cached_k = try cb.fromFloat32Shape(cached_rows.k, &cached_shape);
                    defer cb.free(cached_k);
                    const cached_v = try cb.fromFloat32Shape(cached_rows.v, &cached_shape);
                    defer cb.free(cached_v);

                    var cached_k_label_buf: [80]u8 = undefined;
                    const cached_k_label = try std.fmt.bufPrint(&cached_k_label_buf, "layer-{d}-cached-k", .{layer});
                    _ = try compareLastHiddenRow(
                        cb,
                        allocator,
                        cached_k_label,
                        cached_k,
                        attention_seed.kv_sequence_len,
                        seed_k,
                        decode_context.query_sequence_len,
                        kv_dim,
                    );

                    var cached_v_label_buf: [80]u8 = undefined;
                    const cached_v_label = try std.fmt.bufPrint(&cached_v_label_buf, "layer-{d}-cached-v", .{layer});
                    _ = try compareLastHiddenRow(
                        cb,
                        allocator,
                        cached_v_label,
                        cached_v,
                        attention_seed.kv_sequence_len,
                        v_for_attn,
                        decode_context.query_sequence_len,
                        kv_dim,
                    );
                }
            }
        }

        if (decode_context.attention_mode == .paged_prefill and !disableGatedFamilyRuntimePrefillBlockRequested(gpt_config, configured_layer_count) and (shares_kv or prefill_span_seeded)) {
            var attention = gpt_arch.attentionContextFromDecode(decode_context);
            attention.layer_index = kv_layer_index;
            attention.skip_kv_write = true;

            const q_block = try prepareRopeForDirectAttention(
                cb,
                gpt_config,
                q_for_attn,
                seq_len,
                decode_context.query_sequence_len,
                head_dim,
                layer,
            );
            defer if (q_block != q_for_attn) cb.free(q_block);

            var block_ple_input: ?ops.CT = null;
            defer if (block_ple_input) |ple_ct| cb.free(ple_ct);
            if (ple_vectors) |ple| {
                if (!disableDirectPleRequested()) {
                    const ple_dim: usize = gpt_config.ple_hidden_size;
                    const ple_offset = layer * ple_dim;
                    block_ple_input = try cb.sliceLastDim(ple, ple_offset, ple_offset + ple_dim);
                }
            }
            var block_output_scale: ?ops.CT = null;
            defer if (block_output_scale) |scale| cb.free(scale);
            if (block_ple_input != null or ple_vectors == null) {
                var output_scale_name_buf: [256]u8 = undefined;
                const output_scale_name = std.fmt.bufPrint(
                    &output_scale_name_buf,
                    "model.layers.{d}.per_layer_input.layer_output_scale.weight",
                    .{layer},
                ) catch "";
                if (output_scale_name.len != 0) {
                    block_output_scale = gpt_arch.getModelWeight(cb, gpt_config, output_scale_name) catch null;
                }
            }

            const block_started_at = monotonicNowNs();
            if (try cb.runGatedDecoderBlock(&.{
                .q = q_block,
                .k = k_attn,
                .v = v_for_attn,
                .residual = hidden,
                .attention = attention,
                .num_heads = gpt_config.num_attention_heads,
                .num_kv_heads = num_kv_heads,
                .head_dim = head_dim,
                .attention_linear_slot = linearSlot(layer, .attn_out_proj),
                .attention_post_linear_rms_norm_slot = normSlot(layer, .attn_post),
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
                .ffn_rms_norm_slot = normSlot(layer, .ffn_pre),
                .ffn_post_gate_rms_norm_slot = null,
                .ffn_post_down_rms_norm_slot = normSlot(layer, .ffn_post),
                .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
                .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
                .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
                .intermediate_size = gpt_config.intermediateSize(layer),
                .activation = decoderActivationKind(gpt_config),
                .ple = block_ple_input,
                .ple_gate_linear_slot = if (block_ple_input != null) pleGateSlot(configured_layer_count, layer) else null,
                .ple_proj_linear_slot = if (block_ple_input != null) pleProjSlot(configured_layer_count, layer) else null,
                .ple_post_norm_slot = if (block_ple_input != null) plePostNormSlot(configured_layer_count, layer) else null,
                .ple_hidden_size = if (block_ple_input != null) gpt_config.ple_hidden_size else 0,
                .output_scale = block_output_scale,
                .graph_plan_tail_vocab_size = gpt_config.vocab_size,
            })) |block_hidden| {
                const block_finished_at = monotonicNowNs();
                if (block_finished_at > block_started_at) {
                    const block_elapsed = block_finished_at - block_started_at;
                    recordGemmaDirectAttentionResidual(true, true);
                    recordGemmaDirectFfn(true, true, false, true);
                    addBlockAttentionFusedResidualTiming(phase, block_elapsed);
                    addBlockFfnFusedTiming(phase, block_elapsed);
                }
                const attention_finished_at = monotonicNowNs();
                if (attention_finished_at > attention_started_at) {
                    addBlockAttentionTiming(phase, attention_finished_at - attention_started_at);
                }
                try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
                var layer_result = block_hidden;
                if (block_ple_input != null) {
                    if (phase == .prefill) {
                        timing_stats.prefill_ple_ops += 1;
                        timing_stats.prefill_ple_direct_hits += 1;
                    }
                } else if (ple_vectors) |ple| {
                    const ple_started_at = monotonicNowNs();
                    const ple_result = if (!disableDirectPleRequested()) if (try applyPleDirect(
                        cb,
                        gpt_config,
                        configured_layer_count,
                        block_hidden,
                        ple,
                        layer,
                        decode_context.query_sequence_len,
                        !decoder_frame_active,
                    )) |direct_ple|
                        direct_ple
                    else if (!decoder_frame_active)
                        try gpt_arch.applyPle(
                            cb,
                            allocator,
                            gpt_config,
                            block_hidden,
                            ple,
                            decode_context.query_sequence_len,
                            layer,
                            &name_buf,
                        )
                    else {
                        cb.free(block_hidden);
                        if (owns_hidden) cb.free(hidden);
                        cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                        return null;
                    } else try gpt_arch.applyPle(
                        cb,
                        allocator,
                        gpt_config,
                        block_hidden,
                        ple,
                        decode_context.query_sequence_len,
                        layer,
                        &name_buf,
                    );
                    const ple_finished_at = monotonicNowNs();
                    if (ple_finished_at > ple_started_at) {
                        addBlockPleTiming(phase, ple_finished_at - ple_started_at);
                    }
                    cb.free(block_hidden);
                    layer_result = ple_result;
                }
                const prev_hidden = hidden;
                const next_hidden = if (block_output_scale != null) blk: {
                    if (phase == .prefill) timing_stats.prefill_output_scale_ops += 1;
                    break :blk layer_result;
                } else blk: {
                    const output_scale_started_at = monotonicNowNs();
                    const scaled = try gpt_arch.applyLayerOutputScale(cb, allocator, gpt_config, layer_result, decode_context.query_sequence_len, gpt_config.hidden_size, layer);
                    const output_scale_finished_at = monotonicNowNs();
                    if (output_scale_finished_at > output_scale_started_at) {
                        addBlockOutputScaleTiming(phase, output_scale_finished_at - output_scale_started_at);
                    }
                    break :blk scaled;
                };
                _ = try compareGatedFamilyLayer(
                    cb,
                    allocator,
                    gpt_config,
                    &compare_hidden,
                    next_hidden,
                    seq_len,
                    layer,
                    decode_context,
                    ple_vectors,
                );
                if (reserved_hidden) |*carrier| {
                    try carrier.replaceActive(cb, next_hidden);
                    hidden = carrier.active();
                    owns_hidden = false;
                } else {
                    if (owns_hidden) cb.free(prev_hidden);
                    hidden = next_hidden;
                    owns_hidden = true;
                }
                continue;
            }
        }

        if (decode_context.attention_mode == .paged_decode and decode_context.query_sequence_len == 1) {
            var attention = gpt_arch.attentionContextFromDecode(decode_context);
            attention.layer_index = kv_layer_index;
            attention.skip_kv_write = shares_kv;
            const block_started_at = monotonicNowNs();
            if (try cb.runGatedDecoderBlock(&.{
                .q = q_for_attn,
                .k = k_attn,
                .v = v_for_attn,
                .residual = hidden,
                .attention = attention,
                .num_heads = gpt_config.num_attention_heads,
                .num_kv_heads = num_kv_heads,
                .head_dim = head_dim,
                .attention_linear_slot = linearSlot(layer, .attn_out_proj),
                .attention_post_linear_rms_norm_slot = normSlot(layer, .attn_post),
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
                .ffn_rms_norm_slot = normSlot(layer, .ffn_pre),
                .ffn_post_gate_rms_norm_slot = null,
                .ffn_post_down_rms_norm_slot = normSlot(layer, .ffn_post),
                .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
                .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
                .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
                .intermediate_size = gpt_config.intermediateSize(layer),
                .activation = decoderActivationKind(gpt_config),
                .graph_plan_tail_vocab_size = gpt_config.vocab_size,
            })) |block_hidden| {
                const block_finished_at = monotonicNowNs();
                if (block_finished_at > block_started_at) {
                    const block_elapsed = block_finished_at - block_started_at;
                    recordGemmaDirectAttentionResidual(true, true);
                    recordGemmaDirectFfn(true, true, false, true);
                    addBlockAttentionFusedResidualTiming(phase, block_elapsed);
                    addBlockFfnFusedTiming(phase, block_elapsed);
                }
                const attention_finished_at = monotonicNowNs();
                if (attention_finished_at > attention_started_at) {
                    addBlockAttentionTiming(phase, attention_finished_at - attention_started_at);
                }
                var layer_result = block_hidden;
                if (ple_vectors) |ple| {
                    const ple_started_at = monotonicNowNs();
                    const ple_result = if (!disableDirectPleRequested()) if (try applyPleDirect(
                        cb,
                        gpt_config,
                        configured_layer_count,
                        block_hidden,
                        ple,
                        layer,
                        decode_context.query_sequence_len,
                        !decoder_frame_active,
                    )) |direct_ple|
                        direct_ple
                    else if (!decoder_frame_active)
                        try gpt_arch.applyPle(
                            cb,
                            allocator,
                            gpt_config,
                            block_hidden,
                            ple,
                            decode_context.query_sequence_len,
                            layer,
                            &name_buf,
                        )
                    else {
                        cb.free(block_hidden);
                        if (owns_hidden) cb.free(hidden);
                        cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                        return null;
                    } else try gpt_arch.applyPle(
                        cb,
                        allocator,
                        gpt_config,
                        block_hidden,
                        ple,
                        decode_context.query_sequence_len,
                        layer,
                        &name_buf,
                    );
                    const ple_finished_at = monotonicNowNs();
                    if (ple_finished_at > ple_started_at) {
                        addBlockPleTiming(phase, ple_finished_at - ple_started_at);
                    }
                    cb.free(block_hidden);
                    layer_result = ple_result;
                }
                const prev_hidden = hidden;
                const output_scale_started_at = monotonicNowNs();
                const next_hidden = try gpt_arch.applyLayerOutputScale(cb, allocator, gpt_config, layer_result, decode_context.query_sequence_len, gpt_config.hidden_size, layer);
                const output_scale_finished_at = monotonicNowNs();
                if (output_scale_finished_at > output_scale_started_at) {
                    addBlockOutputScaleTiming(phase, output_scale_finished_at - output_scale_started_at);
                }
                _ = try compareGatedFamilyLayer(
                    cb,
                    allocator,
                    gpt_config,
                    &compare_hidden,
                    next_hidden,
                    seq_len,
                    layer,
                    decode_context,
                    ple_vectors,
                );
                if (reserved_hidden) |*carrier| {
                    try carrier.replaceActive(cb, next_hidden);
                    hidden = carrier.active();
                    owns_hidden = false;
                } else {
                    if (owns_hidden) cb.free(prev_hidden);
                    hidden = next_hidden;
                    owns_hidden = true;
                }
                continue;
            }
        }

        if (decode_context.attention_mode == .paged_decode and decode_context.query_sequence_len == 1) {
            var attention = gpt_arch.attentionContextFromDecode(decode_context);
            attention.layer_index = kv_layer_index;
            attention.skip_kv_write = shares_kv;
            const q_block = try prepareRopeForDirectAttention(
                cb,
                gpt_config,
                q_for_attn,
                seq_len,
                decode_context.query_sequence_len,
                head_dim,
                layer,
            );
            defer if (q_block != q_for_attn) cb.free(q_block);
            const k_block = if (!shares_kv)
                try prepareRopeForDirectAttention(
                    cb,
                    gpt_config,
                    k_attn,
                    seq_len,
                    decode_context.query_sequence_len,
                    head_dim,
                    layer,
                )
            else
                k_attn;
            defer if (k_block != k_attn) cb.free(k_block);
            const block_started_at = monotonicNowNs();
            if (try cb.runGatedDecoderBlock(&.{
                .q = q_block,
                .k = k_block,
                .v = v_for_attn,
                .residual = hidden,
                .attention = attention,
                .num_heads = gpt_config.num_attention_heads,
                .num_kv_heads = num_kv_heads,
                .head_dim = head_dim,
                .attention_linear_slot = linearSlot(layer, .attn_out_proj),
                .attention_post_linear_rms_norm_slot = normSlot(layer, .attn_post),
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
                .ffn_rms_norm_slot = normSlot(layer, .ffn_pre),
                .ffn_post_down_rms_norm_slot = normSlot(layer, .ffn_post),
                .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
                .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
                .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
                .intermediate_size = gpt_config.intermediateSize(layer),
                .activation = decoderActivationKind(gpt_config),
                .graph_plan_tail_vocab_size = gpt_config.vocab_size,
            })) |block_hidden| {
                const block_finished_at = monotonicNowNs();
                if (block_finished_at > block_started_at) {
                    const block_elapsed = block_finished_at - block_started_at;
                    recordGemmaDirectAttentionResidual(true, true);
                    recordGemmaDirectFfn(true, true, false, true);
                    addBlockAttentionFusedResidualTiming(phase, block_elapsed);
                    addBlockFfnFusedTiming(phase, block_elapsed);
                }
                const attention_finished_at = monotonicNowNs();
                if (attention_finished_at > attention_started_at) {
                    addBlockAttentionTiming(phase, attention_finished_at - attention_started_at);
                }
                if (owns_hidden) cb.free(hidden);

                var layer_result = block_hidden;
                if (ple_vectors) |ple| {
                    const ple_started_at = monotonicNowNs();
                    const ple_result = if (try applyPleDirect(
                        cb,
                        gpt_config,
                        configured_layer_count,
                        block_hidden,
                        ple,
                        layer,
                        decode_context.query_sequence_len,
                        !decoder_frame_active,
                    )) |direct_ple|
                        direct_ple
                    else if (!decoder_frame_active)
                        try gpt_arch.applyPle(
                            cb,
                            allocator,
                            gpt_config,
                            block_hidden,
                            ple,
                            decode_context.query_sequence_len,
                            layer,
                            &name_buf,
                        )
                    else {
                        cb.free(block_hidden);
                        cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                        return null;
                    };
                    const ple_finished_at = monotonicNowNs();
                    if (ple_finished_at > ple_started_at) {
                        addBlockPleTiming(phase, ple_finished_at - ple_started_at);
                    }
                    cb.free(block_hidden);
                    layer_result = ple_result;
                }
                const output_scale_started_at = monotonicNowNs();
                hidden = try gpt_arch.applyLayerOutputScale(cb, allocator, gpt_config, layer_result, decode_context.query_sequence_len, gpt_config.hidden_size, layer);
                const output_scale_finished_at = monotonicNowNs();
                if (output_scale_finished_at > output_scale_started_at) {
                    addBlockOutputScaleTiming(phase, output_scale_finished_at - output_scale_started_at);
                }
                owns_hidden = true;
                continue;
            }
        }

        const attention_apply_started_at = monotonicNowNs();
        const attn_residual = if (decode_context.attention_mode == .paged_decode and decode_context.query_sequence_len == 1) blk: {
            const fused_residual_started_at = monotonicNowNs();
            if (try gpt_arch.applyAttentionResidual(
                cb,
                gpt_config,
                q_for_attn,
                k_attn,
                v_for_attn,
                hidden,
                1,
                seq_len,
                gpt_config.num_attention_heads,
                num_kv_heads,
                head_dim,
                gpt_config.hidden_size,
                layer,
                kv_layer_index,
                shares_kv,
                linearSlot(layer, .attn_out_proj),
                null,
                normSlot(layer, .attn_post),
                decode_context,
            )) |fused_attn_residual| {
                recordGemmaDirectAttentionResidual(true, true);
                const fused_residual_finished_at = monotonicNowNs();
                if (fused_residual_finished_at > fused_residual_started_at) {
                    addBlockAttentionFusedResidualTiming(phase, fused_residual_finished_at - fused_residual_started_at);
                }
                break :blk fused_attn_residual;
            }

            const attn_out = try gpt_arch.applyAttention(
                cb,
                gpt_config,
                q_for_attn,
                k_attn,
                v_for_attn,
                1,
                seq_len,
                gpt_config.num_attention_heads,
                num_kv_heads,
                head_dim,
                layer,
                kv_layer_index,
                shares_kv,
                decode_context,
            );
            if (phase == .prefill) timing_stats.prefill_attention_apply_ops += 1;
            defer cb.free(attn_out);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-out",
                attn_out,
                attention_input_size,
            );

            if (try cb.runAttentionOutputResidual(&.{
                .attention_output = attn_out,
                .residual = hidden,
                .rows = decode_context.query_sequence_len,
                .attention_input_size = attention_input_size,
                .hidden_size = gpt_config.hidden_size,
                .linear_slot = linearSlot(layer, .attn_out_proj),
                .post_linear_rms_norm_slot = normSlot(layer, .attn_post),
                .eps = gpt_config.norm_eps,
            })) |fused_attn_residual| {
                recordGemmaDirectAttentionResidual(true, true);
                const fused_residual_finished_at = monotonicNowNs();
                if (fused_residual_finished_at > fused_residual_started_at) {
                    addBlockAttentionFusedResidualTiming(phase, fused_residual_finished_at - fused_residual_started_at);
                }
                try gpt_arch.maybeDumpGatedLayerStageStats(
                    cb,
                    allocator,
                    layer,
                    "attn-residual",
                    fused_attn_residual,
                    gpt_config.hidden_size,
                );
                break :blk fused_attn_residual;
            }
            recordGemmaDirectAttentionResidual(false, true);
            const fused_residual_finished_at = monotonicNowNs();
            if (fused_residual_finished_at > fused_residual_started_at) {
                addBlockAttentionFusedResidualTiming(phase, fused_residual_finished_at - fused_residual_started_at);
            }

            const projected = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = linearSlot(layer, .attn_out_proj),
                .input = attn_out,
                .in_dim = attention_input_size,
                .out_dim = gpt_config.hidden_size,
            })) orelse {
                if (owns_hidden) cb.free(hidden);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_attn_out_linear_ops += 1;
            defer cb.free(projected);
            try maybeSyncTensor(cb, allocator, projected);
            try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-proj",
                projected,
                gpt_config.hidden_size,
            );

            const attn_post = (try cb.decoderRuntimeApplyRmsNorm(&.{
                .slot = normSlot(layer, .attn_post),
                .input = projected,
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
            })) orelse attn_post_blk: {
                if (decoder_frame_active) {
                    if (owns_hidden) cb.free(hidden);
                    cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                    return null;
                }
                const projected_norm_input = try cloneTensorForCompare(cb, allocator, projected);
                defer cb.free(projected_norm_input);
                break :attn_post_blk try gpt_arch.debugGemmaPostAttentionNorm(
                    cb,
                    allocator,
                    gpt_config,
                    projected_norm_input,
                    layer,
                    &name_buf,
                );
            };
            if (phase == .prefill) timing_stats.prefill_attn_post_norm_ops += 1;
            defer cb.free(attn_post);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-post",
                attn_post,
                gpt_config.hidden_size,
            );

            const residual = try cb.add(attn_post, hidden);
            if (phase == .prefill) timing_stats.prefill_attn_residual_add_ops += 1;
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-residual",
                residual,
                gpt_config.hidden_size,
            );
            break :blk residual;
        } else blk: {
            const attn_out = try gpt_arch.applyAttention(
                cb,
                gpt_config,
                q_for_attn,
                k_attn,
                v_for_attn,
                1,
                seq_len,
                gpt_config.num_attention_heads,
                num_kv_heads,
                head_dim,
                layer,
                kv_layer_index,
                shares_kv,
                decode_context,
            );
            if (phase == .prefill) timing_stats.prefill_attention_apply_ops += 1;
            defer cb.free(attn_out);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-out",
                attn_out,
                attention_input_size,
            );

            const fused_residual_started_at = monotonicNowNs();
            if (try cb.runAttentionOutputResidual(&.{
                .attention_output = attn_out,
                .residual = hidden,
                .rows = decode_context.query_sequence_len,
                .attention_input_size = attention_input_size,
                .hidden_size = gpt_config.hidden_size,
                .linear_slot = linearSlot(layer, .attn_out_proj),
                .post_linear_rms_norm_slot = normSlot(layer, .attn_post),
                .eps = gpt_config.norm_eps,
            })) |fused_attn_residual| {
                recordGemmaDirectAttentionResidual(true, true);
                const fused_residual_finished_at = monotonicNowNs();
                if (fused_residual_finished_at > fused_residual_started_at) {
                    addBlockAttentionFusedResidualTiming(phase, fused_residual_finished_at - fused_residual_started_at);
                }
                try gpt_arch.maybeDumpGatedLayerStageStats(
                    cb,
                    allocator,
                    layer,
                    "attn-residual",
                    fused_attn_residual,
                    gpt_config.hidden_size,
                );
                break :blk fused_attn_residual;
            }
            recordGemmaDirectAttentionResidual(false, true);
            const fused_residual_finished_at = monotonicNowNs();
            if (fused_residual_finished_at > fused_residual_started_at) {
                addBlockAttentionFusedResidualTiming(phase, fused_residual_finished_at - fused_residual_started_at);
            }

            const projected = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = linearSlot(layer, .attn_out_proj),
                .input = attn_out,
                .in_dim = attention_input_size,
                .out_dim = gpt_config.hidden_size,
            })) orelse {
                if (owns_hidden) cb.free(hidden);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_attn_out_linear_ops += 1;
            defer cb.free(projected);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-proj",
                projected,
                gpt_config.hidden_size,
            );

            const attn_post = (try cb.decoderRuntimeApplyRmsNorm(&.{
                .slot = normSlot(layer, .attn_post),
                .input = projected,
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
            })) orelse attn_post_blk: {
                if (decoder_frame_active) {
                    if (owns_hidden) cb.free(hidden);
                    cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                    return null;
                }
                break :attn_post_blk try gpt_arch.debugGemmaPostAttentionNorm(
                    cb,
                    allocator,
                    gpt_config,
                    projected,
                    layer,
                    &name_buf,
                );
            };
            if (phase == .prefill) timing_stats.prefill_attn_post_norm_ops += 1;
            defer cb.free(attn_post);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-post",
                attn_post,
                gpt_config.hidden_size,
            );

            const residual = try cb.add(attn_post, hidden);
            if (phase == .prefill) timing_stats.prefill_attn_residual_add_ops += 1;
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "attn-residual",
                residual,
                gpt_config.hidden_size,
            );
            break :blk residual;
        };
        const attention_apply_finished_at = monotonicNowNs();
        if (attention_apply_finished_at > attention_apply_started_at) {
            addBlockAttentionApplyTiming(phase, attention_apply_finished_at - attention_apply_started_at);
        }
        try maybeSyncTensor(cb, allocator, attn_residual);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        const attention_finished_at = monotonicNowNs();
        if (attention_finished_at > attention_started_at) {
            addBlockAttentionTiming(phase, attention_finished_at - attention_started_at);
        }
        var compare_attn_residual: ?ops.CT = null;
        defer if (compare_attn_residual) |ct| cb.free(ct);
        if (shouldCompareGatedFamilyLayer(layer)) {
            if (shares_kv) {
                const compare_q = try gpt_arch.debugAttentionProject(
                    cb,
                    allocator,
                    gpt_config,
                    attn_normed,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.hidden_size),
                    @intCast(gpt_config.num_attention_heads * head_dim),
                    layer,
                    "q",
                    &name_buf,
                );
                defer cb.free(compare_q);
                const compare_q_attn = if (try gpt_arch.maybeApplyQKHeadNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_q,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.num_attention_heads * head_dim),
                    layer,
                    "q",
                    @intCast(head_dim),
                    &name_buf,
                )) |normed_q|
                    normed_q
                else
                    compare_q;
                defer if (compare_q_attn != compare_q) cb.free(compare_q_attn);
                const compare_q_for_attn = try prepareGemmaQueryForAttention(
                    cb,
                    allocator,
                    gpt_config,
                    compare_q_attn,
                    decode_context.query_sequence_len,
                    head_dim,
                    gpt_config.num_attention_heads * head_dim,
                );
                defer if (compare_q_for_attn != compare_q_attn) cb.free(compare_q_for_attn);
                const compare_v_for_attn = try prepareGemmaValueForAttention(
                    cb,
                    allocator,
                    gpt_config,
                    v_value,
                    decode_context.query_sequence_len,
                    num_kv_heads,
                    head_dim,
                    kv_dim,
                    shares_kv,
                );
                defer if (compare_v_for_attn != v_value) cb.free(compare_v_for_attn);

                var q_label_buf: [64]u8 = undefined;
                const q_label = try std.fmt.bufPrint(&q_label_buf, "layer-{d}-q", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    q_label,
                    q_for_attn,
                    decode_context.query_sequence_len,
                    compare_q_for_attn,
                    decode_context.query_sequence_len,
                    gpt_config.num_attention_heads * head_dim,
                );

                const compare_attn_out = try gpt_arch.applyAttention(
                    cb,
                    gpt_config,
                    compare_q_for_attn,
                    k_attn,
                    compare_v_for_attn,
                    1,
                    seq_len,
                    gpt_config.num_attention_heads,
                    num_kv_heads,
                    head_dim,
                    layer,
                    kv_layer_index,
                    shares_kv,
                    decode_context,
                );
                defer cb.free(compare_attn_out);
                const direct_attn_out = try gpt_arch.applyAttention(
                    cb,
                    gpt_config,
                    q_for_attn,
                    k_attn,
                    v_for_attn,
                    1,
                    seq_len,
                    gpt_config.num_attention_heads,
                    num_kv_heads,
                    head_dim,
                    layer,
                    kv_layer_index,
                    shares_kv,
                    decode_context,
                );
                defer cb.free(direct_attn_out);
                var attn_out_label_buf: [80]u8 = undefined;
                const attn_out_label = try std.fmt.bufPrint(&attn_out_label_buf, "layer-{d}-attn-out", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_out_label,
                    direct_attn_out,
                    decode_context.query_sequence_len,
                    compare_attn_out,
                    decode_context.query_sequence_len,
                    attention_input_size,
                );
                const compare_projected = try gpt_arch.debugAttentionOutputProject(
                    cb,
                    allocator,
                    gpt_config,
                    compare_attn_out,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.num_attention_heads * head_dim),
                    @intCast(gpt_config.hidden_size),
                    layer,
                    &name_buf,
                );
                defer cb.free(compare_projected);
                const direct_projected = try gpt_arch.debugAttentionOutputProject(
                    cb,
                    allocator,
                    gpt_config,
                    direct_attn_out,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.num_attention_heads * head_dim),
                    @intCast(gpt_config.hidden_size),
                    layer,
                    &name_buf,
                );
                defer cb.free(direct_projected);
                var attn_proj_label_buf: [80]u8 = undefined;
                const attn_proj_label = try std.fmt.bufPrint(&attn_proj_label_buf, "layer-{d}-attn-proj", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_proj_label,
                    direct_projected,
                    decode_context.query_sequence_len,
                    compare_projected,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
                const compare_attn_post = try gpt_arch.debugGemmaPostAttentionNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_projected,
                    layer,
                    &name_buf,
                );
                defer if (compare_attn_post != compare_projected) cb.free(compare_attn_post);
                const direct_attn_post = try gpt_arch.debugGemmaPostAttentionNorm(
                    cb,
                    allocator,
                    gpt_config,
                    direct_projected,
                    layer,
                    &name_buf,
                );
                defer if (direct_attn_post != direct_projected) cb.free(direct_attn_post);
                var attn_post_label_buf: [80]u8 = undefined;
                const attn_post_label = try std.fmt.bufPrint(&attn_post_label_buf, "layer-{d}-attn-post", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_post_label,
                    direct_attn_post,
                    decode_context.query_sequence_len,
                    compare_attn_post,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
                compare_attn_residual = try cb.add(compare_attn_post, compare_hidden.?);

                var attn_label_buf: [80]u8 = undefined;
                const attn_label = try std.fmt.bufPrint(&attn_label_buf, "layer-{d}-attn-residual", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_label,
                    attn_residual,
                    decode_context.query_sequence_len,
                    compare_attn_residual.?,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
            } else {
                const compare_q = try gpt_arch.debugAttentionProject(
                    cb,
                    allocator,
                    gpt_config,
                    attn_normed,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.hidden_size),
                    @intCast(gpt_config.num_attention_heads * head_dim),
                    layer,
                    "q",
                    &name_buf,
                );
                defer cb.free(compare_q);
                const compare_k = try gpt_arch.debugAttentionProject(
                    cb,
                    allocator,
                    gpt_config,
                    attn_normed,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.hidden_size),
                    @intCast(kv_dim),
                    layer,
                    "k",
                    &name_buf,
                );
                defer cb.free(compare_k);
                const compare_v = try gpt_arch.debugAttentionProject(
                    cb,
                    allocator,
                    gpt_config,
                    attn_normed,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.hidden_size),
                    @intCast(kv_dim),
                    layer,
                    "v",
                    &name_buf,
                );
                defer cb.free(compare_v);

                const compare_q_attn = if (try gpt_arch.maybeApplyQKHeadNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_q,
                    decode_context.query_sequence_len,
                    @intCast(gpt_config.num_attention_heads * head_dim),
                    layer,
                    "q",
                    @intCast(head_dim),
                    &name_buf,
                )) |normed_q|
                    normed_q
                else
                    compare_q;
                defer if (compare_q_attn != compare_q) cb.free(compare_q_attn);
                const compare_q_for_attn = try prepareGemmaQueryForAttention(
                    cb,
                    allocator,
                    gpt_config,
                    compare_q_attn,
                    decode_context.query_sequence_len,
                    head_dim,
                    gpt_config.num_attention_heads * head_dim,
                );
                defer if (compare_q_for_attn != compare_q_attn) cb.free(compare_q_for_attn);

                const compare_k_attn = if (try gpt_arch.maybeApplyQKHeadNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_k,
                    decode_context.query_sequence_len,
                    @intCast(num_kv_heads * head_dim),
                    layer,
                    "k",
                    @intCast(head_dim),
                    &name_buf,
                )) |normed_k|
                    normed_k
                else
                    compare_k;
                defer if (compare_k_attn != compare_k) cb.free(compare_k_attn);
                const compare_v_for_attn = try prepareGemmaValueForAttention(
                    cb,
                    allocator,
                    gpt_config,
                    compare_v,
                    decode_context.query_sequence_len,
                    num_kv_heads,
                    head_dim,
                    kv_dim,
                    shares_kv,
                );
                defer if (compare_v_for_attn != compare_v) cb.free(compare_v_for_attn);

                const compare_attn_out = try gpt_arch.applyAttention(
                    cb,
                    gpt_config,
                    compare_q_for_attn,
                    compare_k_attn,
                    compare_v_for_attn,
                    1,
                    seq_len,
                    gpt_config.num_attention_heads,
                    num_kv_heads,
                    head_dim,
                    layer,
                    kv_layer_index,
                    shares_kv,
                    decode_context,
                );
                defer cb.free(compare_attn_out);
                const direct_attn_out = try gpt_arch.applyAttention(
                    cb,
                    gpt_config,
                    q_for_attn,
                    k_attn,
                    v_for_attn,
                    1,
                    seq_len,
                    gpt_config.num_attention_heads,
                    num_kv_heads,
                    head_dim,
                    layer,
                    kv_layer_index,
                    shares_kv,
                    decode_context,
                );
                defer cb.free(direct_attn_out);
                var attn_out_label_buf: [80]u8 = undefined;
                const attn_out_label = try std.fmt.bufPrint(&attn_out_label_buf, "layer-{d}-attn-out", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_out_label,
                    direct_attn_out,
                    decode_context.query_sequence_len,
                    compare_attn_out,
                    decode_context.query_sequence_len,
                    attention_input_size,
                );
                const compare_projected = try gpt_arch.debugAttentionOutputProject(
                    cb,
                    allocator,
                    gpt_config,
                    compare_attn_out,
                    decode_context.query_sequence_len,
                    @intCast(attention_input_size),
                    @intCast(gpt_config.hidden_size),
                    layer,
                    &name_buf,
                );
                defer cb.free(compare_projected);
                var attn_proj_label_buf: [80]u8 = undefined;
                const attn_proj_label = try std.fmt.bufPrint(&attn_proj_label_buf, "layer-{d}-attn-proj", .{layer});
                const direct_projected = try gpt_arch.debugAttentionOutputProject(
                    cb,
                    allocator,
                    gpt_config,
                    direct_attn_out,
                    decode_context.query_sequence_len,
                    @intCast(attention_input_size),
                    @intCast(gpt_config.hidden_size),
                    layer,
                    &name_buf,
                );
                defer cb.free(direct_projected);
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_proj_label,
                    direct_projected,
                    decode_context.query_sequence_len,
                    compare_projected,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
                const compare_attn_post = try gpt_arch.debugGemmaPostAttentionNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_projected,
                    layer,
                    &name_buf,
                );
                defer cb.free(compare_attn_post);
                var attn_post_label_buf: [80]u8 = undefined;
                const attn_post_label = try std.fmt.bufPrint(&attn_post_label_buf, "layer-{d}-attn-post", .{layer});
                const direct_attn_post = try gpt_arch.debugGemmaPostAttentionNorm(
                    cb,
                    allocator,
                    gpt_config,
                    direct_projected,
                    layer,
                    &name_buf,
                );
                defer cb.free(direct_attn_post);
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_post_label,
                    direct_attn_post,
                    decode_context.query_sequence_len,
                    compare_attn_post,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
                compare_attn_residual = try cb.add(compare_attn_post, compare_hidden.?);

                var attn_label_buf: [80]u8 = undefined;
                const attn_label = try std.fmt.bufPrint(&attn_label_buf, "layer-{d}-attn-residual", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    attn_label,
                    attn_residual,
                    decode_context.query_sequence_len,
                    compare_attn_residual.?,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
            }
        }
        const ffn_started_at = monotonicNowNs();
        const ffn_norm_started_at = ffn_started_at;
        const ffn_normed = (try cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = normSlot(layer, .ffn_pre),
            .input = attn_residual,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
        })) orelse {
            recordGemmaDirectRmsNorm(false);
            cb.free(attn_residual);
            return null;
        };
        recordGemmaDirectRmsNorm(true);
        if (phase == .prefill) timing_stats.prefill_ffn_norm_ops += 1;
        defer cb.free(ffn_normed);
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "ffn-norm",
            ffn_normed,
            gpt_config.hidden_size,
        );
        try maybeSyncTensor(cb, allocator, ffn_normed);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        const ffn_norm_finished_at = monotonicNowNs();
        if (ffn_norm_finished_at > ffn_norm_started_at) {
            addBlockFfnNormTiming(phase, ffn_norm_finished_at - ffn_norm_started_at);
        }
        if (shouldCompareGatedFamilyLayer(layer) and compare_attn_residual != null) {
            const compare_ffn_normed = try gpt_arch.debugGemmaFfnPreNorm(
                cb,
                allocator,
                gpt_config,
                compare_attn_residual.?,
                layer,
                &name_buf,
            );
            defer cb.free(compare_ffn_normed);

            var ffn_norm_label_buf: [80]u8 = undefined;
            const ffn_norm_label = try std.fmt.bufPrint(&ffn_norm_label_buf, "layer-{d}-ffn-norm", .{layer});
            _ = try compareLastHiddenRow(
                cb,
                allocator,
                ffn_norm_label,
                ffn_normed,
                decode_context.query_sequence_len,
                compare_ffn_normed,
                decode_context.query_sequence_len,
                gpt_config.hidden_size,
            );
        }

        var compare_layer_hidden_pre_ple: ?ops.CT = null;
        defer if (compare_layer_hidden_pre_ple) |ct| cb.free(ct);
        const layer_hidden = blk: {
            const ffn_fused_started_at = monotonicNowNs();
            if (try cb.runGatedFfnResidual(&.{
                .gate_linear_slot = linearSlot(layer, .mlp_gate),
                .up_linear_slot = linearSlot(layer, .mlp_up),
                .down_linear_slot = linearSlot(layer, .mlp_down),
                .post_gate_rms_norm_slot = null,
                .post_down_rms_norm_slot = normSlot(layer, .ffn_post),
                .input = ffn_normed,
                .residual = attn_residual,
                .hidden_size = gpt_config.hidden_size,
                .intermediate_size = gpt_config.intermediateSize(layer),
                .activation = decoderActivationKind(gpt_config),
                .eps = gpt_config.norm_eps,
            })) |fused_ffn| {
                recordGemmaDirectFfn(true, false, false, true);
                if (phase == .prefill) timing_stats.prefill_ffn_fused_ops += 1;
                const ffn_fused_finished_at = monotonicNowNs();
                if (ffn_fused_finished_at > ffn_fused_started_at) {
                    addBlockFfnFusedTiming(phase, ffn_fused_finished_at - ffn_fused_started_at);
                }
                if (shouldCompareGatedFamilyLayer(layer) and compare_attn_residual != null) {
                    const compare_ffn_normed = try gpt_arch.debugGemmaFfnPreNorm(
                        cb,
                        allocator,
                        gpt_config,
                        compare_attn_residual.?,
                        layer,
                        &name_buf,
                    );
                    defer cb.free(compare_ffn_normed);
                    const compare_ffn_raw = try gpt_arch.debugFeedForward(
                        cb,
                        allocator,
                        gpt_config,
                        compare_ffn_normed,
                        decode_context.query_sequence_len,
                        layer,
                        &name_buf,
                        decode_context,
                    );
                    defer cb.free(compare_ffn_raw);
                    const compare_down_post = try gpt_arch.debugGemmaFfnPostNorm(
                        cb,
                        allocator,
                        gpt_config,
                        compare_ffn_raw,
                        layer,
                        &name_buf,
                    );
                    defer if (compare_down_post != compare_ffn_raw) cb.free(compare_down_post);
                    compare_layer_hidden_pre_ple = try cb.add(compare_down_post, compare_attn_residual.?);

                    var ffn_residual_label_buf: [80]u8 = undefined;
                    const ffn_residual_label = try std.fmt.bufPrint(&ffn_residual_label_buf, "layer-{d}-ffn-residual", .{layer});
                    _ = try compareLastHiddenRow(
                        cb,
                        allocator,
                        ffn_residual_label,
                        fused_ffn,
                        decode_context.query_sequence_len,
                        compare_layer_hidden_pre_ple.?,
                        decode_context.query_sequence_len,
                        gpt_config.hidden_size,
                    );
                }
                break :blk fused_ffn;
            }
            recordGemmaDirectFfn(false, false, false, true);
            if (phase == .prefill) timing_stats.prefill_ffn_split_ops += 1;
            const ffn_fused_finished_at = monotonicNowNs();
            if (ffn_fused_finished_at > ffn_fused_started_at) {
                addBlockFfnFusedTiming(phase, ffn_fused_finished_at - ffn_fused_started_at);
            }

            const gate_up = (try cb.decoderRuntimeApplyLinearPair(&.{
                .slot_a = linearSlot(layer, .mlp_gate),
                .slot_b = linearSlot(layer, .mlp_up),
                .input = ffn_normed,
                .in_dim = gpt_config.hidden_size,
                .out_dim = gpt_config.intermediateSize(layer),
            })) orelse {
                cb.free(attn_residual);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_ffn_pair_ops += 1;
            defer cb.free(gate_up.first);
            defer cb.free(gate_up.second);

            const activated = (try cb.decoderRuntimeApplyActivation(&.{
                .input = gate_up.first,
                .kind = switch (gpt_config.activation) {
                    .gelu => .gelu,
                    .gelu_new => .gelu_new,
                    .silu => .silu,
                    .relu => .relu,
                    .relu_squared => .relu_squared,
                },
                .dim = gpt_config.intermediateSize(layer),
            })) orelse activated_blk: {
                if (decoder_frame_active) {
                    cb.free(attn_residual);
                    cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                    return null;
                }
                break :activated_blk try gpt_arch.applyActivation(cb, gpt_config, gate_up.first);
            };
            if (phase == .prefill) timing_stats.prefill_ffn_activation_ops += 1;
            defer cb.free(activated);

            const gated = try cb.multiply(activated, gate_up.second);
            if (phase == .prefill) timing_stats.prefill_ffn_multiply_ops += 1;
            defer cb.free(gated);

            const down = (try cb.decoderRuntimeApplyLinear(&.{
                .slot = linearSlot(layer, .mlp_down),
                .input = gated,
                .in_dim = gpt_config.intermediateSize(layer),
                .out_dim = gpt_config.hidden_size,
            })) orelse {
                cb.free(attn_residual);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_ffn_down_linear_ops += 1;
            defer cb.free(down);
            try maybeSyncTensor(cb, allocator, down);
            try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "ffn-raw",
                down,
                gpt_config.hidden_size,
            );

            const down_post = (try cb.decoderRuntimeApplyRmsNorm(&.{
                .slot = normSlot(layer, .ffn_post),
                .input = down,
                .hidden_size = gpt_config.hidden_size,
                .eps = gpt_config.norm_eps,
            })) orelse {
                cb.free(attn_residual);
                return null;
            };
            if (phase == .prefill) timing_stats.prefill_ffn_post_norm_ops += 1;
            defer cb.free(down_post);
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "ffn-post",
                down_post,
                gpt_config.hidden_size,
            );

            if (shouldCompareGatedFamilyLayer(layer) and compare_attn_residual != null) {
                const compare_ffn_normed = try gpt_arch.debugGemmaFfnPreNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_attn_residual.?,
                    layer,
                    &name_buf,
                );
                defer cb.free(compare_ffn_normed);
                const compare_ffn_raw = try gpt_arch.debugFeedForward(
                    cb,
                    allocator,
                    gpt_config,
                    compare_ffn_normed,
                    decode_context.query_sequence_len,
                    layer,
                    &name_buf,
                    decode_context,
                );
                defer cb.free(compare_ffn_raw);
                const compare_down_post = try gpt_arch.debugGemmaFfnPostNorm(
                    cb,
                    allocator,
                    gpt_config,
                    compare_ffn_raw,
                    layer,
                    &name_buf,
                );
                defer if (compare_down_post != compare_ffn_raw) cb.free(compare_down_post);

                var ffn_raw_label_buf: [80]u8 = undefined;
                const ffn_raw_label = try std.fmt.bufPrint(&ffn_raw_label_buf, "layer-{d}-ffn-raw", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    ffn_raw_label,
                    down,
                    decode_context.query_sequence_len,
                    compare_ffn_raw,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );

                var ffn_post_label_buf: [80]u8 = undefined;
                const ffn_post_label = try std.fmt.bufPrint(&ffn_post_label_buf, "layer-{d}-ffn-post", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    ffn_post_label,
                    down_post,
                    decode_context.query_sequence_len,
                    compare_down_post,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );

                compare_layer_hidden_pre_ple = try cb.add(compare_down_post, compare_attn_residual.?);
            }

            const residual = try cb.add(down_post, attn_residual);
            if (phase == .prefill) timing_stats.prefill_ffn_residual_add_ops += 1;
            try gpt_arch.maybeDumpGatedLayerStageStats(
                cb,
                allocator,
                layer,
                "ffn-residual",
                residual,
                gpt_config.hidden_size,
            );
            break :blk residual;
        };
        cb.free(attn_residual);
        try maybeSyncTensor(cb, allocator, layer_hidden);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        var layer_result = layer_hidden;
        if (shouldCompareGatedFamilyLayer(layer) and compare_layer_hidden_pre_ple != null) {
            var ffn_residual_label_buf: [80]u8 = undefined;
            const ffn_residual_label = try std.fmt.bufPrint(&ffn_residual_label_buf, "layer-{d}-ffn-residual", .{layer});
            _ = try compareLastHiddenRow(
                cb,
                allocator,
                ffn_residual_label,
                layer_hidden,
                decode_context.query_sequence_len,
                compare_layer_hidden_pre_ple.?,
                decode_context.query_sequence_len,
                gpt_config.hidden_size,
            );
        }
        if (ple_vectors) |ple| {
            const ple_started_at = monotonicNowNs();
            if (phase == .prefill) timing_stats.prefill_ple_ops += 1;
            var used_direct_ple = false;
            const ple_result = if (!disableDirectPleRequested()) if (try applyPleDirect(
                cb,
                gpt_config,
                configured_layer_count,
                layer_hidden,
                ple,
                layer,
                decode_context.query_sequence_len,
                !decoder_frame_active,
            )) |direct_ple| blk: {
                used_direct_ple = true;
                break :blk direct_ple;
            } else if (!decoder_frame_active) blk: {
                if (phase == .prefill) timing_stats.prefill_ple_fallbacks += 1;
                break :blk try gpt_arch.applyPle(
                    cb,
                    allocator,
                    gpt_config,
                    layer_hidden,
                    ple,
                    decode_context.query_sequence_len,
                    layer,
                    &name_buf,
                );
            } else {
                cb.free(layer_hidden);
                if (owns_hidden) cb.free(hidden);
                cancelDecoderRuntimeFrame(cb, &decoder_frame_active);
                return null;
            } else blk: {
                if (phase == .prefill) timing_stats.prefill_ple_fallbacks += 1;
                break :blk try gpt_arch.applyPle(
                    cb,
                    allocator,
                    gpt_config,
                    layer_hidden,
                    ple,
                    decode_context.query_sequence_len,
                    layer,
                    &name_buf,
                );
            };
            if (phase == .prefill and used_direct_ple) timing_stats.prefill_ple_direct_hits += 1;
            const ple_finished_at = monotonicNowNs();
            if (ple_finished_at > ple_started_at) {
                addBlockPleTiming(phase, ple_finished_at - ple_started_at);
            }
            if (shouldCompareGatedFamilyLayer(layer) and compare_layer_hidden_pre_ple != null) {
                const compare_ple_result = try gpt_arch.applyPle(
                    cb,
                    allocator,
                    gpt_config,
                    compare_layer_hidden_pre_ple.?,
                    ple,
                    decode_context.query_sequence_len,
                    layer,
                    &name_buf,
                );
                defer cb.free(compare_ple_result);
                var ple_label_buf: [80]u8 = undefined;
                const ple_label = try std.fmt.bufPrint(&ple_label_buf, "layer-{d}-ple", .{layer});
                _ = try compareLastHiddenRow(
                    cb,
                    allocator,
                    ple_label,
                    ple_result,
                    decode_context.query_sequence_len,
                    compare_ple_result,
                    decode_context.query_sequence_len,
                    gpt_config.hidden_size,
                );
            }
            cb.free(layer_hidden);
            layer_result = ple_result;
        }
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "ple",
            layer_result,
            gpt_config.hidden_size,
        );
        try maybeSyncTensor(cb, allocator, layer_result);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        const prev_hidden = hidden;
        const output_scale_started_at = monotonicNowNs();
        var next_hidden = try gpt_arch.applyLayerOutputScale(cb, allocator, gpt_config, layer_result, decode_context.query_sequence_len, gpt_config.hidden_size, layer);
        if (phase == .prefill) timing_stats.prefill_output_scale_ops += 1;
        const output_scale_finished_at = monotonicNowNs();
        if (output_scale_finished_at > output_scale_started_at) {
            addBlockOutputScaleTiming(phase, output_scale_finished_at - output_scale_started_at);
        }
        try gpt_arch.maybeDumpGatedLayerStageStats(
            cb,
            allocator,
            layer,
            "output-scale",
            next_hidden,
            gpt_config.hidden_size,
        );
        try maybeSyncTensor(cb, allocator, next_hidden);
        try maybeFlushDecoderRuntimePrefillFrame(cb, phase, &decoder_frame_active);
        if (shouldCompareGatedFamilyLayer(layer) and layer_input_snapshot != null) {
            var input_mut_label_buf: [80]u8 = undefined;
            const input_mut_label = try std.fmt.bufPrint(&input_mut_label_buf, "layer-{d}-input-after", .{layer});
            _ = try compareLastHiddenRow(
                cb,
                allocator,
                input_mut_label,
                hidden,
                decode_context.query_sequence_len,
                layer_input_snapshot.?,
                decode_context.query_sequence_len,
                gpt_config.hidden_size,
            );
        }
        if (materializeGatedFamilyHiddenRequested()) {
            const materialized_hidden = try cloneTensorForCompare(cb, allocator, next_hidden);
            cb.free(next_hidden);
            next_hidden = materialized_hidden;
        }
        if (shouldCompareGatedFamilyLayer(layer) and compare_layer_hidden_pre_ple != null) {
            const compare_scale_input = if (ple_vectors) |ple|
                try gpt_arch.applyPle(
                    cb,
                    allocator,
                    gpt_config,
                    compare_layer_hidden_pre_ple.?,
                    ple,
                    decode_context.query_sequence_len,
                    layer,
                    &name_buf,
                )
            else
                try cloneTensorForCompare(cb, allocator, compare_layer_hidden_pre_ple.?);
            const compare_scaled = try gpt_arch.applyLayerOutputScale(
                cb,
                allocator,
                gpt_config,
                compare_scale_input,
                decode_context.query_sequence_len,
                gpt_config.hidden_size,
                layer,
            );
            defer cb.free(compare_scaled);
            var scale_label_buf: [80]u8 = undefined;
            const scale_label = try std.fmt.bufPrint(&scale_label_buf, "layer-{d}-output-scale", .{layer});
            _ = try compareLastHiddenRow(
                cb,
                allocator,
                scale_label,
                next_hidden,
                decode_context.query_sequence_len,
                compare_scaled,
                decode_context.query_sequence_len,
                gpt_config.hidden_size,
            );
        }
        const ffn_finished_at = monotonicNowNs();
        if (ffn_finished_at > ffn_started_at) {
            addBlockFfnTiming(phase, ffn_finished_at - ffn_started_at);
        }
        if (shouldDumpGatedFamilyLayer(layer)) {
            var dump_label_buf: [64]u8 = undefined;
            const dump_label = try std.fmt.bufPrint(&dump_label_buf, "layer-{d}-hidden", .{layer});
            try dumpLastHiddenRowStats(
                cb,
                allocator,
                dump_label,
                next_hidden,
                decode_context.query_sequence_len,
                gpt_config.hidden_size,
            );
        }
        _ = try compareGatedFamilyLayer(
            cb,
            allocator,
            gpt_config,
            &compare_hidden,
            next_hidden,
            seq_len,
            layer,
            decode_context,
            ple_vectors,
        );
        if (reserved_hidden) |*carrier| {
            try carrier.replaceActive(cb, next_hidden);
            hidden = carrier.active();
            owns_hidden = false;
        } else {
            if (owns_hidden) cb.free(prev_hidden);
            hidden = next_hidden;
            owns_hidden = true;
        }
    }

    if (reserved_hidden) |*carrier| {
        carrier.deinit(cb, true);
        reserved_hidden = null;
    }
    return_decoder_frame = decoder_frame_active and phase == .prefill and decode_context.query_sequence_len > 1;
    return .{
        .hidden = hidden,
        .total_rows = decode_context.query_sequence_len,
        .decoder_frame_active = return_decoder_frame,
    };
}

fn forwardFinalHiddenTensorDirect(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    hidden_input: ops.CT,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    phase: BlockTimingPhase,
    ple_vectors: ?ops.CT,
) !?DirectHiddenResult {
    if (try forwardFinalHiddenTensorGemmaDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden_input,
        seq_len,
        decode_context,
        phase,
        ple_vectors,
    )) |gemma_hidden| return gemma_hidden;
    if (!supportsDirectGatedRuntime(gpt_config, configured_layer_count, decode_context)) return null;

    var decoder_frame_active = if (shouldUseDecoderRuntimeFrame(phase, gpt_config, configured_layer_count, decode_context))
        try cb.decoderRuntimeBeginFrame()
    else
        false;
    defer finishDecoderRuntimeFrame(cb, &decoder_frame_active);

    const use_copy_based_reserved_hidden = !decoder_frame_active or phase != .prefill or decode_context.query_sequence_len <= 1;
    var reserved_hidden = if (use_copy_based_reserved_hidden)
        try ReservedHiddenCarrier.init(
            cb,
            hidden_input,
            decode_context.query_sequence_len,
            gpt_config.hidden_size,
        )
    else
        null;
    errdefer if (reserved_hidden) |*carrier| carrier.deinit(cb, false);

    var hidden = if (reserved_hidden) |*carrier| carrier.active() else hidden_input;
    var owns_hidden = false;
    errdefer if (owns_hidden) cb.free(hidden);

    const layer_count: usize = gpt_config.num_hidden_layers;
    for (0..layer_count) |layer| {
        const normed = (try cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = normSlot(layer, .attn_pre),
            .input = hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
        })) orelse {
            if (owns_hidden) cb.free(hidden);
            return null;
        };
        errdefer cb.free(normed);

        var attention = gpt_arch.attentionContextFromDecode(decode_context);
        attention.layer_index = layer;
        attention.skip_kv_write = false;
        attention.sliding_window = 0;

        const block_out = (try cb.runGatedDecoderBlock(&.{
            .attention_input = normed,
            .residual = hidden,
            .attention = attention,
            .num_heads = gpt_config.num_attention_heads,
            .num_kv_heads = gpt_config.effectiveKVHeads(),
            .head_dim = gpt_config.headDim(),
            .q_linear_slot = linearSlot(layer, .attn_q),
            .k_linear_slot = linearSlot(layer, .attn_k),
            .v_linear_slot = linearSlot(layer, .attn_v),
            .attention_linear_slot = linearSlot(layer, .attn_out_proj),
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .ffn_rms_norm_slot = normSlot(layer, .ffn_pre),
            .gate_ffn_linear_slot = linearSlot(layer, .mlp_gate),
            .up_ffn_linear_slot = linearSlot(layer, .mlp_up),
            .down_ffn_linear_slot = linearSlot(layer, .mlp_down),
            .intermediate_size = gpt_config.intermediateSize(layer),
            .activation = .silu,
            .graph_plan_tail_vocab_size = gpt_config.vocab_size,
        })) orelse {
            cb.free(normed);
            if (owns_hidden) cb.free(hidden);
            return null;
        };

        cb.free(normed);
        if (reserved_hidden) |*carrier| {
            try carrier.replaceActive(cb, block_out);
            hidden = carrier.active();
            owns_hidden = false;
        } else {
            if (owns_hidden) cb.free(hidden);
            hidden = block_out;
            owns_hidden = true;
        }
    }

    if (reserved_hidden) |*carrier| {
        carrier.deinit(cb, true);
        reserved_hidden = null;
    }
    return .{
        .hidden = hidden,
        .total_rows = decode_context.query_sequence_len,
    };
}

fn forwardFinalHiddenLastRowDirect(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    hidden_input: ops.CT,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    phase: BlockTimingPhase,
    ple_vectors: ?ops.CT,
) !?ops.CT {
    const hidden_result = (try forwardFinalHiddenTensorDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden_input,
        seq_len,
        decode_context,
        phase,
        ple_vectors,
    )) orelse return null;
    errdefer cb.free(hidden_result.hidden);
    if (hidden_result.total_rows == 1) return hidden_result.hidden;
    const last_hidden = try cb.sliceRows2D(allocator, hidden_result.hidden, hidden_result.total_rows - 1, 1, gpt_config.hidden_size);
    cb.free(hidden_result.hidden);
    return last_hidden;
}

pub fn buildOverridesWithLevel(
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    configured_override_level: usize,
) gpt_arch.Layer0DecoderOverrides {
    const prepared_layer_count = preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers));
    var overrides = gpt_arch.Layer0DecoderOverrides{};
    for (0..prepared_layer_count) |layer| {
        if (configured_override_level >= 1) {
            overrides.attn_norm_slots[layer] = normSlot(layer, .attn_pre);
        }
        if (configured_override_level >= 2) {
            overrides.attn_q_slots[layer] = linearSlot(layer, .attn_q);
            overrides.attn_k_slots[layer] = linearSlot(layer, .attn_k);
            overrides.attn_v_slots[layer] = linearSlot(layer, .attn_v);
            overrides.attn_out_proj_linear_slots[layer] = linearSlot(layer, .attn_out_proj);
        }
        if (configured_override_level >= 3) {
            overrides.ffn_norm_slots[layer] = if (gpt_config.family == .gemma)
                normSlot(layer, .attn_post)
            else
                normSlot(layer, .ffn_pre);
        }
        if (configured_override_level >= 4) {
            overrides.mlp_gate_slots[layer] = linearSlot(layer, .mlp_gate);
            overrides.mlp_up_slots[layer] = linearSlot(layer, .mlp_up);
            overrides.mlp_down_slots[layer] = linearSlot(layer, .mlp_down);
        }
    }
    return overrides;
}

pub fn buildOverrides(gpt_config: gpt_mod.Config, configured_layer_count: usize) gpt_arch.Layer0DecoderOverrides {
    return buildOverridesWithLevel(gpt_config, configured_layer_count, overrideLevel());
}

fn prepareLinearNoBiasSlotForConfig(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    slot: usize,
    weight: ops.CT,
    in_dim: usize,
    out_dim: usize,
) !bool {
    if (gpt_config.family != .qwen3) {
        return decoder_rms_runtime.prepareLinearNoBiasSlot(cb, allocator, slot, weight, in_dim, out_dim);
    }

    // Jina v5 applies a LoRA retrieval adapter into the loaded f32 tensor.
    // Preparing from raw bf16 bytes would bypass that merge for resident slots.
    const shape = try cb.tensorShape(weight, allocator);
    defer allocator.free(shape);
    const shape_i32 = try allocator.alloc(i32, shape.len);
    defer allocator.free(shape_i32);
    for (shape, 0..) |dim, i| shape_i32[i] = @intCast(dim);
    const values = try cb.toFloat32(weight, allocator);
    defer allocator.free(values);
    const dense = try cb.fromFloat32Shape(values, shape_i32);
    defer cb.free(dense);
    return decoder_rms_runtime.prepareLinearNoBiasDenseSlot(cb, allocator, slot, dense, in_dim, out_dim, true);
}

pub fn prepareDecodeRuntime(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    kv_tokens: usize,
    configured_layer_count: usize,
) !bool {
    if (!supportsConfig(gpt_config)) return false;
    timing_stats.prepare_calls += 1;
    var started_at = monotonicNowNs();
    if (!(try cb.decoderRuntimePrepareGreedy(&.{
        .hidden_size = gpt_config.hidden_size,
        .intermediate_size = gpt_config.intermediate_size,
        .num_layers = gpt_config.num_hidden_layers,
        .num_heads = gpt_config.num_attention_heads,
        .num_kv_heads = gpt_config.effectiveKVHeads(),
        .head_dim = gpt_config.headDim(),
        .vocab_size = gpt_config.vocab_size,
        .kv_tokens = kv_tokens,
    }))) {
        timing_stats.prepare_greedy_failures += 1;
        return false;
    }
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prepare_greedy_nanos += finished_at - started_at;

    const prepared_layer_count = preparedLayers(@min(configured_layer_count, gpt_config.num_hidden_layers));
    for (0..prepared_layer_count) |layer| {
        var name_buf: [256]u8 = undefined;
        const layer_head_dim = gpt_config.effectiveHeadDimForLayer(layer);
        const layer_kv_heads = gpt_config.effectiveKVHeadsForLayer(layer);
        const attention_input_size = gpt_config.num_attention_heads * layer_head_dim;

        started_at = monotonicNowNs();
        const attn_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.input_layernorm.weight", .{layer});
        const attn_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, attn_norm_name);
        defer cb.free(attn_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .attn_pre), attn_norm_w, gpt_config.hidden_size))) {
            timing_stats.prepare_attn_pre_norm_failures += 1;
            tracePrepareLayerFailure(layer, "attn_pre_norm", normSlot(layer, .attn_pre), gpt_config.hidden_size, gpt_config.hidden_size);
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const attn_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_attention_layernorm.weight", .{layer});
        const attn_post_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, attn_post_norm_name);
        defer cb.free(attn_post_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(
            cb,
            allocator,
            gpt_config,
            if (gpt_config.family == .gemma) normSlot(layer, .attn_post) else normSlot(layer, .ffn_pre),
            attn_post_norm_w,
            gpt_config.hidden_size,
        ))) {
            tracePrepareLayerFailure(
                layer,
                if (gpt_config.family == .gemma) "attn_post_norm" else "ffn_pre_norm",
                if (gpt_config.family == .gemma) normSlot(layer, .attn_post) else normSlot(layer, .ffn_pre),
                gpt_config.hidden_size,
                gpt_config.hidden_size,
            );
            if (gpt_config.family == .gemma)
                timing_stats.prepare_attn_post_norm_failures += 1
            else
                timing_stats.prepare_ffn_pre_norm_failures += 1;
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

        if (gpt_config.family == .gemma or gpt_config.family == .qwen3) {
            var primary_buf: [256]u8 = undefined;

            if (gpt_config.family == .gemma) {
                started_at = monotonicNowNs();
                const ffn_pre_norm_name = std.fmt.bufPrint(&primary_buf, "model.layers.{d}.pre_feedforward_layernorm.weight", .{layer}) catch return error.NameTooLong;
                const ffn_pre_norm_w = gpt_arch.getModelWeight(cb, gpt_config, ffn_pre_norm_name) catch attn_post_norm_w;
                const owns_ffn_pre = ffn_pre_norm_w != attn_post_norm_w;
                defer if (owns_ffn_pre) cb.free(ffn_pre_norm_w);
                finished_at = monotonicNowNs();
                if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
                started_at = monotonicNowNs();
                if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .ffn_pre), ffn_pre_norm_w, gpt_config.hidden_size))) {
                    timing_stats.prepare_ffn_pre_norm_failures += 1;
                    tracePrepareLayerFailure(layer, "ffn_pre_norm", normSlot(layer, .ffn_pre), gpt_config.hidden_size, gpt_config.hidden_size);
                    return false;
                }
                finished_at = monotonicNowNs();
                if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

                started_at = monotonicNowNs();
                const ffn_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_feedforward_layernorm.weight", .{layer});
                const ffn_post_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, ffn_post_norm_name);
                defer cb.free(ffn_post_norm_w);
                finished_at = monotonicNowNs();
                if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
                started_at = monotonicNowNs();
                if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, normSlot(layer, .ffn_post), ffn_post_norm_w, gpt_config.hidden_size))) {
                    timing_stats.prepare_ffn_post_norm_failures += 1;
                    tracePrepareLayerFailure(layer, "ffn_post_norm", normSlot(layer, .ffn_post), gpt_config.hidden_size, gpt_config.hidden_size);
                    return false;
                }
                finished_at = monotonicNowNs();
                if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;
            }

            started_at = monotonicNowNs();
            const q_head_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.q_norm.weight", .{layer});
            const q_head_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, q_head_norm_name);
            defer cb.free(q_head_norm_w);
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
            started_at = monotonicNowNs();
            if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, qHeadNormSlot(configured_layer_count, layer), q_head_norm_w, layer_head_dim))) {
                timing_stats.prepare_attn_pre_norm_failures += 1;
                tracePrepareLayerFailure(layer, "q_head_norm", qHeadNormSlot(configured_layer_count, layer), layer_head_dim, layer_head_dim);
                return false;
            }
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;

            started_at = monotonicNowNs();
            const k_head_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.k_norm.weight", .{layer});
            const k_head_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, k_head_norm_name);
            defer cb.free(k_head_norm_w);
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
            started_at = monotonicNowNs();
            if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, kHeadNormSlot(configured_layer_count, layer), k_head_norm_w, layer_head_dim))) {
                timing_stats.prepare_attn_pre_norm_failures += 1;
                tracePrepareLayerFailure(layer, "k_head_norm", kHeadNormSlot(configured_layer_count, layer), layer_head_dim, layer_head_dim);
                return false;
            }
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;
        }

        started_at = monotonicNowNs();
        const q_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.q_proj.weight", .{layer});
        const q_w = try gpt_arch.getModelWeight(cb, gpt_config, q_name);
        defer cb.free(q_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try prepareLinearNoBiasSlotForConfig(cb, allocator, gpt_config, linearSlot(layer, .attn_q), q_w, gpt_config.hidden_size, attention_input_size))) {
            timing_stats.prepare_attn_q_failures += 1;
            tracePrepareLayerFailure(layer, "attn_q", linearSlot(layer, .attn_q), gpt_config.hidden_size, attention_input_size);
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const k_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.k_proj.weight", .{layer});
        const k_w = try gpt_arch.getModelWeight(cb, gpt_config, k_name);
        defer cb.free(k_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try prepareLinearNoBiasSlotForConfig(cb, allocator, gpt_config, linearSlot(layer, .attn_k), k_w, gpt_config.hidden_size, layer_kv_heads * layer_head_dim))) {
            timing_stats.prepare_attn_k_failures += 1;
            tracePrepareLayerFailure(layer, "attn_k", linearSlot(layer, .attn_k), gpt_config.hidden_size, layer_kv_heads * layer_head_dim);
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const v_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.v_proj.weight", .{layer});
        const v_w = try gpt_arch.getModelWeight(cb, gpt_config, v_name);
        defer cb.free(v_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try prepareLinearNoBiasSlotForConfig(cb, allocator, gpt_config, linearSlot(layer, .attn_v), v_w, gpt_config.hidden_size, layer_kv_heads * layer_head_dim))) {
            timing_stats.prepare_attn_v_failures += 1;
            tracePrepareLayerFailure(layer, "attn_v", linearSlot(layer, .attn_v), gpt_config.hidden_size, layer_kv_heads * layer_head_dim);
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const attn_out_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.self_attn.o_proj.weight", .{layer});
        const attn_out_w = try gpt_arch.getModelWeight(cb, gpt_config, attn_out_name);
        defer cb.free(attn_out_w);
        if (timing_stats.prepare_attn_out_first_rank == 0) {
            const attn_out_shape = try cb.tensorShape(attn_out_w, allocator);
            defer allocator.free(attn_out_shape);
            timing_stats.prepare_attn_out_first_rank = attn_out_shape.len;
            if (attn_out_shape.len > 0) timing_stats.prepare_attn_out_first_dim0 = attn_out_shape[0];
            if (attn_out_shape.len > 1) timing_stats.prepare_attn_out_first_dim1 = attn_out_shape[1];
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        const prepared_attn_out = try prepareLinearNoBiasSlotForConfig(
            cb,
            allocator,
            gpt_config,
            linearSlot(layer, .attn_out_proj),
            attn_out_w,
            attention_input_size,
            gpt_config.hidden_size,
        );
        if (!prepared_attn_out) {
            timing_stats.prepare_attn_out_failures += 1;
            tracePrepareLayerFailure(layer, "attn_out", linearSlot(layer, .attn_out_proj), attention_input_size, gpt_config.hidden_size);
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const gate_w = try gpt_arch.getFFNWeight(cb, gpt_config, layer, "gate", &name_buf);
        defer cb.free(gate_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try prepareLinearNoBiasSlotForConfig(cb, allocator, gpt_config, linearSlot(layer, .mlp_gate), gate_w, gpt_config.hidden_size, gpt_config.intermediateSize(layer)))) {
            timing_stats.prepare_mlp_gate_failures += 1;
            tracePrepareLayerFailure(layer, "mlp_gate", linearSlot(layer, .mlp_gate), gpt_config.hidden_size, gpt_config.intermediateSize(layer));
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const up_w = try gpt_arch.getFFNWeight(cb, gpt_config, layer, "up", &name_buf);
        defer cb.free(up_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try prepareLinearNoBiasSlotForConfig(cb, allocator, gpt_config, linearSlot(layer, .mlp_up), up_w, gpt_config.hidden_size, gpt_config.intermediateSize(layer)))) {
            timing_stats.prepare_mlp_up_failures += 1;
            tracePrepareLayerFailure(layer, "mlp_up", linearSlot(layer, .mlp_up), gpt_config.hidden_size, gpt_config.intermediateSize(layer));
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const down_w = try gpt_arch.getFFNWeight(cb, gpt_config, layer, "down", &name_buf);
        defer cb.free(down_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try prepareLinearNoBiasSlotForConfig(cb, allocator, gpt_config, linearSlot(layer, .mlp_down), down_w, gpt_config.intermediateSize(layer), gpt_config.hidden_size))) {
            timing_stats.prepare_mlp_down_failures += 1;
            tracePrepareLayerFailure(layer, "mlp_down", linearSlot(layer, .mlp_down), gpt_config.intermediateSize(layer), gpt_config.hidden_size);
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        if (gpt_config.family == .gemma and gpt_config.hasPle()) {
            started_at = monotonicNowNs();
            const ple_gate_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.inp_gate.weight", .{layer});
            const ple_gate_w = gpt_arch.getModelWeight(cb, gpt_config, ple_gate_name) catch |err| switch (err) {
                error.MissingWeight => blk: {
                    const fallback = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input_gate.weight", .{layer});
                    break :blk try gpt_arch.getModelWeight(cb, gpt_config, fallback);
                },
                else => return err,
            };
            defer cb.free(ple_gate_w);
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
            started_at = monotonicNowNs();
            if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(
                cb,
                allocator,
                pleGateSlot(configured_layer_count, layer),
                ple_gate_w,
                gpt_config.hidden_size,
                gpt_config.ple_hidden_size,
            ))) {
                timing_stats.prepare_ple_gate_failures += 1;
                tracePrepareLayerFailure(layer, "ple_gate", pleGateSlot(configured_layer_count, layer), gpt_config.hidden_size, gpt_config.ple_hidden_size);
                return false;
            }
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

            started_at = monotonicNowNs();
            const ple_proj_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.proj.weight", .{layer});
            const ple_proj_w = gpt_arch.getModelWeight(cb, gpt_config, ple_proj_name) catch |err| switch (err) {
                error.MissingWeight => blk: {
                    const fallback = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_projection.weight", .{layer});
                    break :blk try gpt_arch.getModelWeight(cb, gpt_config, fallback);
                },
                else => return err,
            };
            defer cb.free(ple_proj_w);
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
            started_at = monotonicNowNs();
            if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(
                cb,
                allocator,
                pleProjSlot(configured_layer_count, layer),
                ple_proj_w,
                gpt_config.ple_hidden_size,
                gpt_config.hidden_size,
            ))) {
                timing_stats.prepare_ple_proj_failures += 1;
                tracePrepareLayerFailure(layer, "ple_proj", pleProjSlot(configured_layer_count, layer), gpt_config.ple_hidden_size, gpt_config.hidden_size);
                return false;
            }
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

            started_at = monotonicNowNs();
            const ple_post_norm_name = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.per_layer_input.post_norm.weight", .{layer});
            const ple_post_norm_w = gpt_arch.getModelWeight(cb, gpt_config, ple_post_norm_name) catch |err| switch (err) {
                error.MissingWeight => blk: {
                    const fallback = try std.fmt.bufPrint(&name_buf, "model.layers.{d}.post_per_layer_input_norm.weight", .{layer});
                    break :blk try gpt_arch.getModelWeight(cb, gpt_config, fallback);
                },
                else => return err,
            };
            defer cb.free(ple_post_norm_w);
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
            started_at = monotonicNowNs();
            if (!(try decoder_rms_runtime.prepareRmsNormSlot(
                cb,
                allocator,
                gpt_config,
                plePostNormSlot(configured_layer_count, layer),
                ple_post_norm_w,
                gpt_config.hidden_size,
            ))) {
                timing_stats.prepare_ple_post_norm_failures += 1;
                tracePrepareLayerFailure(layer, "ple_post_norm", plePostNormSlot(configured_layer_count, layer), gpt_config.hidden_size, gpt_config.hidden_size);
                return false;
            }
            finished_at = monotonicNowNs();
            if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;
        }
    }

    if (gpt_config.family == .gemma and gpt_config.hasPle()) {
        const ple_total_dim = @as(usize, gpt_config.ple_hidden_size) * gpt_config.num_hidden_layers;

        started_at = monotonicNowNs();
        const ple_model_proj_w = gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_input.per_layer_model_proj.weight") catch |err| switch (err) {
            error.MissingWeight => try gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_model_projection.weight"),
            else => return err,
        };
        defer cb.free(ple_model_proj_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(
            cb,
            allocator,
            pleModelProjSlot(configured_layer_count),
            ple_model_proj_w,
            gpt_config.hidden_size,
            ple_total_dim,
        ))) {
            timing_stats.prepare_ple_model_proj_failures += 1;
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.linear_prep_nanos += finished_at - started_at;

        started_at = monotonicNowNs();
        const ple_proj_norm_w = gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_input.per_layer_proj_norm.weight") catch |err| switch (err) {
            error.MissingWeight => try gpt_arch.getModelWeight(cb, gpt_config, "model.per_layer_projection_norm.weight"),
            else => return err,
        };
        defer cb.free(ple_proj_norm_w);
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.lookup_nanos += finished_at - started_at;
        started_at = monotonicNowNs();
        if (!(try decoder_rms_runtime.prepareRmsNormSlot(
            cb,
            allocator,
            gpt_config,
            pleProjNormSlot(configured_layer_count),
            ple_proj_norm_w,
            gpt_config.ple_hidden_size,
        ))) {
            timing_stats.prepare_ple_proj_norm_failures += 1;
            return false;
        }
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.norm_prep_nanos += finished_at - started_at;
    }

    started_at = monotonicNowNs();
    const final_norm_w = try gpt_arch.getModelWeight(cb, gpt_config, "model.norm.weight");
    defer cb.free(final_norm_w);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_lookup_nanos += finished_at - started_at;
    started_at = monotonicNowNs();
    if (!(try decoder_rms_runtime.prepareRmsNormSlot(cb, allocator, gpt_config, finalNormSlot(configured_layer_count), final_norm_w, gpt_config.hidden_size))) {
        timing_stats.prepare_final_norm_failures += 1;
        return false;
    }
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_prep_nanos += finished_at - started_at;

    started_at = monotonicNowNs();
    const lm_head_w = try decoder_tail_runtime.getLmHeadWeight(cb, gpt_config);
    defer cb.free(lm_head_w);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_lookup_nanos += finished_at - started_at;
    started_at = monotonicNowNs();
    if (!(try decoder_rms_runtime.prepareLinearNoBiasSlot(
        cb,
        allocator,
        finalLmHeadSlot(configured_layer_count),
        lm_head_w,
        gpt_config.hidden_size,
        gpt_config.vocab_size,
    ))) {
        timing_stats.prepare_final_norm_failures += 1;
        return false;
    }
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.final_prep_nanos += finished_at - started_at;

    return true;
}

pub fn forwardGreedyToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?i64 {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;
    timing_stats.greedy_calls += 1;

    if (try tryBackendOwnedGreedyToken(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        token_id,
        seq_len,
        decode_context,
    )) |token| return token;

    var started_at = monotonicNowNs();
    const hidden = try decoder_rms_runtime.embedToken(cb, allocator, gpt_config, token_id);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_embed_nanos += finished_at - started_at;
    const ple_input_ids = [_]i64{token_id};
    started_at = monotonicNowNs();
    const ple_vectors = if (try computePleVectorsDirect(
        cb,
        gpt_config,
        configured_layer_count,
        &ple_input_ids,
        hidden,
        1,
    )) |direct_ple|
        direct_ple
    else
        try gpt_arch.computePleVectors(cb, allocator, gpt_config, &ple_input_ids, hidden, 1);
    defer if (ple_vectors) |pv| cb.free(pv);
    var compare_hidden_input: ?ops.CT = null;
    defer if (compare_hidden_input) |ct| cb.free(ct);
    if (gatedFamilyCompareRequested()) {
        compare_hidden_input = try cloneTensorForCompare(cb, allocator, hidden);
    }

    started_at = monotonicNowNs();
    const direct_hidden = try forwardFinalHiddenLastRowDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
        .greedy,
        ple_vectors,
    );
    const final_hidden = if (direct_hidden) |direct_hidden_row|
        direct_hidden_row
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        break :blk try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_block_nanos += finished_at - started_at;
    errdefer cb.free(final_hidden);

    var compare_fallback_hidden: ?ops.CT = null;
    defer if (compare_fallback_hidden) |ct| cb.free(ct);
    if (direct_hidden != null and compare_hidden_input != null) {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        compare_fallback_hidden = try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            compare_hidden_input.?,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
        compare_hidden_input = null;
        _ = try compareLastHiddenRow(
            cb,
            allocator,
            "decode-final-hidden",
            final_hidden,
            1,
            compare_fallback_hidden.?,
            1,
            gpt_config.hidden_size,
        );
    }

    started_at = monotonicNowNs();
    const token = if (try decoder_tail_runtime.forwardPreparedGreedyFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
    )) |prepared_token|
        prepared_token
    else
        try decoder_tail_runtime.forwardGreedyFromFinalHidden(
            cb,
            allocator,
            gpt_config,
            final_hidden,
            .rms,
            finalNormSlot(configured_layer_count),
        );
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_tail_nanos += finished_at - started_at;
    if (compare_fallback_hidden) |fallback_hidden| {
        const fallback_token = try decoder_tail_runtime.forwardGreedyFromFinalHidden(
            cb,
            allocator,
            gpt_config,
            fallback_hidden,
            .rms,
            finalNormSlot(configured_layer_count),
        );
        std.debug.print(
            "gated-family-compare decode-token: direct={any} fallback={any}\n",
            .{ token, fallback_token },
        );
    }
    return token;
}

pub fn forwardLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    const logits = (try forwardLastLogitsTensor(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        token_id,
        seq_len,
        decode_context,
    )) orelse return null;
    const logits_host = try cb.toFloat32(logits, allocator);
    cb.free(logits);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    return logits_host;
}

pub const PrefillPreparedTail = struct {
    final_hidden: ops.CT,
    final_norm_slot: usize,
    final_lm_head_slot: usize,
    hidden_size: usize,
    vocab_size: usize,
    norm_eps: f32,
};

pub fn forwardPrefillLastPreparedTail(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?PrefillPreparedTail {
    if (!supportsConfig(gpt_config)) return null;
    if (input_ids.len == 0 or decode_context.query_sequence_len != input_ids.len) return null;
    if (decode_context.attention_mode != .paged_prefill) return null;
    timing_stats.prefill_calls += 1;
    timing_stats.prefill_query_tokens += input_ids.len;

    var started_at = monotonicNowNs();
    const hidden = try decoder_rms_runtime.embedTokens(cb, allocator, gpt_config, input_ids);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_embed_nanos += finished_at - started_at;

    var ple_frame_active = false;
    errdefer cancelDecoderRuntimeFrame(cb, &ple_frame_active);
    if (shouldUseDecoderRuntimeFrame(.prefill, gpt_config, configured_layer_count, decode_context) and !cb.decoderRuntimeHasActiveFrame()) {
        const frame_begin_started_at = monotonicNowNs();
        ple_frame_active = try cb.decoderRuntimeBeginFrame();
        const frame_begin_finished_at = monotonicNowNs();
        if (frame_begin_finished_at > frame_begin_started_at) {
            timing_stats.prefill_frame_begin_nanos += frame_begin_finished_at - frame_begin_started_at;
        }
    }

    started_at = monotonicNowNs();
    const direct_ple_vectors = try computePleVectorsDirect(
        cb,
        gpt_config,
        configured_layer_count,
        input_ids,
        hidden,
        input_ids.len,
    );
    const ple_vectors = if (direct_ple_vectors) |direct_ple|
        direct_ple
    else blk: {
        cancelDecoderRuntimeFrame(cb, &ple_frame_active);
        const fallback_started_at = monotonicNowNs();
        const fallback_ple = try gpt_arch.computePleVectors(cb, allocator, gpt_config, input_ids, hidden, input_ids.len);
        const fallback_finished_at = monotonicNowNs();
        if (fallback_finished_at > fallback_started_at) {
            timing_stats.prefill_ple_fallback_nanos += fallback_finished_at - fallback_started_at;
        }
        break :blk fallback_ple;
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_ple_prepare_nanos += finished_at - started_at;
    defer if (ple_vectors) |pv| cb.free(pv);
    try maybeDumpInitialGatedInputs(
        cb,
        allocator,
        hidden,
        ple_vectors,
        input_ids.len,
        gpt_config.hidden_size,
        gpt_config.ple_hidden_size,
    );
    var compare_hidden_input: ?ops.CT = null;
    defer if (compare_hidden_input) |ct| cb.free(ct);
    if (gatedFamilyCompareRequested()) {
        compare_hidden_input = try cloneTensorForCompare(cb, allocator, hidden);
    }

    started_at = monotonicNowNs();
    const direct_hidden_result = try forwardFinalHiddenTensorDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
        .prefill,
        ple_vectors,
    );
    if (direct_hidden_result == null) {
        cancelDecoderRuntimeFrame(cb, &ple_frame_active);
        cb.free(hidden);
        return null;
    }
    var decoder_frame_active = if (direct_hidden_result) |direct_hidden|
        direct_hidden.decoder_frame_active
    else
        false;
    if (decoder_frame_active) ple_frame_active = false;
    defer finishDecoderRuntimeFrame(cb, &decoder_frame_active);
    var final_hidden, var total_rows = if (direct_hidden_result) |direct_hidden|
        .{ direct_hidden.hidden, direct_hidden.total_rows }
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        const fallback = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
        break :blk .{ fallback.hidden, fallback.total_rows };
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_block_nanos += finished_at - started_at;
    var owns_final_hidden = true;
    errdefer if (owns_final_hidden) cb.free(final_hidden);
    if (direct_hidden_result != null and compare_hidden_input != null and !gemmaPrefillCompareRequested()) {
        const compare_started_at = monotonicNowNs();
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        const compare_fallback = try gpt_arch.forwardFinalHiddenTensorFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            compare_hidden_input.?,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
        compare_hidden_input = null;
        var keep_compare_fallback = false;
        defer if (!keep_compare_fallback) cb.free(compare_fallback.hidden);
        const final_parity_ok = try compareLastHiddenRow(
            cb,
            allocator,
            "prefill-final-hidden",
            final_hidden,
            total_rows,
            compare_fallback.hidden,
            compare_fallback.total_rows,
            gpt_config.hidden_size,
        );
        if (gemmaPrefillCompareRequested() and !final_parity_ok) {
            std.debug.print(
                "gemma-prefill-final-parity-mismatch: using staged reference final hidden\n",
                .{},
            );
            cb.free(final_hidden);
            final_hidden = compare_fallback.hidden;
            total_rows = compare_fallback.total_rows;
            keep_compare_fallback = true;
        }
        const compare_finished_at = monotonicNowNs();
        if (compare_finished_at > compare_started_at) timing_stats.prefill_compare_nanos += compare_finished_at - compare_started_at;
    }
    if (syncGatedFamilyFinalHiddenRequested()) {
        const sync_started_at = monotonicNowNs();
        finishDecoderRuntimeFrame(cb, &decoder_frame_active);
        const synced_hidden = try cb.toFloat32(final_hidden, allocator);
        allocator.free(synced_hidden);
        const sync_finished_at = monotonicNowNs();
        if (sync_finished_at > sync_started_at) timing_stats.prefill_sync_nanos += sync_finished_at - sync_started_at;
    }
    const slice_started_at = monotonicNowNs();
    const last_hidden = if (total_rows == 1)
        final_hidden
    else
        try cb.sliceRows2D(allocator, final_hidden, total_rows - 1, 1, gpt_config.hidden_size);
    const slice_finished_at = monotonicNowNs();
    if (slice_finished_at > slice_started_at) timing_stats.prefill_last_hidden_slice_nanos += slice_finished_at - slice_started_at;
    errdefer if (last_hidden != final_hidden) cb.free(last_hidden);
    if (last_hidden != final_hidden) {
        cb.free(final_hidden);
        owns_final_hidden = false;
    } else {
        owns_final_hidden = false;
    }
    const frame_finish_started_at = monotonicNowNs();
    finishDecoderRuntimeFrame(cb, &decoder_frame_active);
    const frame_finish_finished_at = monotonicNowNs();
    if (frame_finish_finished_at > frame_finish_started_at) {
        timing_stats.prefill_frame_finish_nanos += frame_finish_finished_at - frame_finish_started_at;
    }
    return .{
        .final_hidden = last_hidden,
        .final_norm_slot = finalNormSlot(configured_layer_count),
        .final_lm_head_slot = finalLmHeadSlot(configured_layer_count),
        .hidden_size = gpt_config.hidden_size,
        .vocab_size = gpt_config.vocab_size,
        .norm_eps = gpt_config.norm_eps,
    };
}

pub fn forwardPrefillLastLogits(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    input_ids: []const i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?[]f32 {
    const tail = (try forwardPrefillLastPreparedTail(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        input_ids,
        seq_len,
        decode_context,
    )) orelse return null;
    defer cb.free(tail.final_hidden);

    const started_at = monotonicNowNs();
    const logits = if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        tail.final_hidden,
        .rms,
        tail.final_norm_slot,
        tail.final_lm_head_slot,
    )) |prepared_logits|
        prepared_logits
    else blk: {
        break :blk (try decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
            cb,
            gpt_config,
            tail.final_hidden,
            .rms,
            tail.final_norm_slot,
        )) orelse return null;
    };
    timing_stats.prefill_tail_logits_ops += 1;
    defer cb.free(logits);
    const result = try cb.toFloat32(logits, allocator);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, result);
    const finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.prefill_tail_nanos += finished_at - started_at;
    return result;
}

pub fn forwardLastLogitsTensor(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
) !?ops.CT {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode and decode_context.attention_mode != .paged_prefill) return null;

    const hidden = try decoder_rms_runtime.embedToken(cb, allocator, gpt_config, token_id);
    const ple_input_ids = [_]i64{token_id};
    const ple_vectors = try gpt_arch.computePleVectors(cb, allocator, gpt_config, &ple_input_ids, hidden, 1);
    defer if (ple_vectors) |pv| cb.free(pv);
    try maybeDumpInitialGatedInputs(
        cb,
        allocator,
        hidden,
        ple_vectors,
        1,
        gpt_config.hidden_size,
        gpt_config.ple_hidden_size,
    );
    var compare_hidden_input: ?ops.CT = null;
    defer if (compare_hidden_input) |ct| cb.free(ct);
    if (gatedFamilyCompareRequested()) {
        compare_hidden_input = try cloneTensorForCompare(cb, allocator, hidden);
    }
    const direct_hidden = try forwardFinalHiddenLastRowDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
        .greedy,
        ple_vectors,
    );
    const final_hidden = if (direct_hidden) |direct_hidden_row|
        direct_hidden_row
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        break :blk try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
    };
    defer cb.free(final_hidden);
    if (direct_hidden != null and compare_hidden_input != null) {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        const compare_fallback = try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            compare_hidden_input.?,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
        compare_hidden_input = null;
        defer cb.free(compare_fallback);
        _ = try compareLastHiddenRow(
            cb,
            allocator,
            "decode-final-hidden",
            final_hidden,
            1,
            compare_fallback,
            1,
            gpt_config.hidden_size,
        );
    }
    if (syncGatedFamilyFinalHiddenRequested()) {
        const synced_hidden = try cb.toFloat32(final_hidden, allocator);
        allocator.free(synced_hidden);
    }

    if (try decoder_tail_runtime.forwardPreparedLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
    )) |logits| {
        return logits;
    }
    return decoder_tail_runtime.forwardLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
    );
}

pub fn forwardSampledToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    configured_layer_count: usize,
    token_id: i64,
    seq_len: usize,
    decode_context: *const gpt_arch.DecodeContext,
    sampling: model_runtime.SamplingConfig,
    token_history: []const i64,
) !?i64 {
    if (!supportsConfig(gpt_config)) return null;
    if (decode_context.query_sequence_len != 1) return null;
    if (decode_context.attention_mode != .paged_decode) return null;
    timing_stats.sampled_calls += 1;

    var started_at = monotonicNowNs();
    const hidden = try decoder_rms_runtime.embedToken(cb, allocator, gpt_config, token_id);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_embed_nanos += finished_at - started_at;
    const ple_input_ids = [_]i64{token_id};
    const ple_vectors = try gpt_arch.computePleVectors(cb, allocator, gpt_config, &ple_input_ids, hidden, 1);
    defer if (ple_vectors) |pv| cb.free(pv);

    started_at = monotonicNowNs();
    const final_hidden = if (try forwardFinalHiddenLastRowDirect(
        cb,
        allocator,
        gpt_config,
        configured_layer_count,
        hidden,
        seq_len,
        decode_context,
        .sampled,
        ple_vectors,
    )) |direct_hidden|
        direct_hidden
    else blk: {
        const overrides = buildOverrides(gpt_config, configured_layer_count);
        break :blk try gpt_arch.forwardFinalHiddenLastRowFromEmbeddingsWithLayer0Overrides(
            cb,
            allocator,
            gpt_config,
            hidden,
            overrides,
            1,
            seq_len,
            decode_context,
            ple_vectors,
        );
    };
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_block_nanos += finished_at - started_at;
    defer cb.free(final_hidden);

    started_at = monotonicNowNs();
    if (try decoder_tail_runtime.forwardPreparedSampledFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        finalLmHeadSlot(configured_layer_count),
        sampling,
        token_history,
    )) |prepared_token| {
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.sampled_tail_nanos += finished_at - started_at;
        return prepared_token;
    }

    started_at = monotonicNowNs();
    const token = try decoder_tail_runtime.forwardSampledFromFinalHidden(
        cb,
        allocator,
        gpt_config,
        final_hidden,
        .rms,
        finalNormSlot(configured_layer_count),
        sampling,
        token_history,
    );
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_tail_nanos += finished_at - started_at;
    return token;
}

test "raw whole-token gated supports multimodal decode-only gemma config" {
    const base: gpt_mod.Config = .{
        .family = .gemma,
        .num_hidden_layers = 2,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .hidden_size = 256,
        .intermediate_size = 512,
        .vocab_size = 1024,
    };

    var multimodal = base;
    multimodal.image_token_index = 262_144;
    multimodal.mm_tokens_per_image = 256;
    try std.testing.expect(supportsConfig(multimodal));

    var ple = multimodal;
    ple.ple_hidden_size = 64;
    try std.testing.expect(supportsConfig(ple));

    var moe = multimodal;
    moe.num_local_experts = 8;
    moe.num_experts_per_tok = 2;
    try std.testing.expect(!supportsConfig(moe));
}

test "direct gemma runtime allows gemma4-style global head configs without shared kv" {
    const config: gpt_mod.Config = .{
        .family = .gemma,
        .num_hidden_layers = 6,
        .num_attention_heads = 8,
        .num_key_value_heads = 1,
        .num_global_key_value_heads = 1,
        .hidden_size = 1536,
        .attention_head_dim = 256,
        .global_head_dim = 512,
        .intermediate_size = 6144,
        .sliding_window = 512,
        .sliding_window_pattern = 5,
        .vocab_size = 1024,
    };
    const decode_context: gpt_arch.DecodeContext = .{
        .attention_mode = .paged_decode,
        .query_sequence_len = 1,
        .kv_sequence_len = 8,
        .total_sequence_len = 8,
        .kv_position_offset = 0,
    };
    const prefill_context: gpt_arch.DecodeContext = .{
        .attention_mode = .paged_prefill,
        .query_sequence_len = 4,
        .kv_sequence_len = 4,
        .total_sequence_len = 4,
        .kv_position_offset = 0,
    };

    try std.testing.expect(supportsDirectGemmaRuntime(config, config.num_hidden_layers, &decode_context));
    try std.testing.expect(supportsDirectGemmaRuntime(config, config.num_hidden_layers, &prefill_context));

    var shared_kv = config;
    shared_kv.num_kv_shared_layers = 2;
    try std.testing.expect(supportsDirectGemmaRuntime(shared_kv, shared_kv.num_hidden_layers, &decode_context));
    try std.testing.expect(supportsDirectGemmaRuntime(shared_kv, shared_kv.num_hidden_layers, &prefill_context));
}
