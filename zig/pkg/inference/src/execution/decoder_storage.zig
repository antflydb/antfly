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

//! Physical KV storage policy, shared by task admission and decoder runtimes.
const std = @import("std");
const Config = @import("../models/gpt.zig").Config;
const pool = @import("../runtime/kv/pool.zig");
const CapacityPolicy = @import("../runtime/tier/memory.zig").GenerationKvCapacityPolicy;

pub const Options = struct {
    /// Existing whole-model runtimes address every architectural layer. The
    /// compact recurrent route instead assigns slots only to stored KV layers.
    layer_layout: enum { architectural, stored } = .stored,
    force_sliding_trim: bool = false,
    host_mirrors: bool = true,
};

pub fn poolPlan(backend: pool.BackendKind, dtype: pool.KvDType, config: *const Config, options: Options) pool.KvPoolConfig {
    return .{
        .backend = backend,
        .dtype = dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(switch (options.layer_layout) {
            .architectural => config.num_hidden_layers,
            .stored => config.kvStorageLayerCount(),
        }),
        .num_kv_heads = config.maxKvHeads(),
        .head_dim = config.maxHeadDim(),
        .sliding_window_size = config.kvPoolSlidingWindowSize(options.force_sliding_trim),
        .store_cpu_bytes = options.host_mirrors,
    };
}

/// A CUDA request manager owns logical positions; the numerical owner supplies
/// device KV hooks. Shared prompt-cache pools retain their existing storage.
pub fn requestMetadataPool(storage_config: pool.KvPoolConfig) pool.KvPoolConfig {
    var config = storage_config;
    if (config.backend == .cuda) config.store_cpu_bytes = false;
    return config;
}

/// Checkpoint preparation/restore preserves every original logical position.
/// Numerical KV is snapshotted through its device reader, without host mirrors.
pub fn checkpointPool(dtype: pool.KvDType, config: Config) pool.KvPoolConfig {
    var result = poolPlan(.cuda, dtype, &config, .{ .host_mirrors = false });
    result.sliding_window_size = null;
    return result;
}

pub const Route = enum { standard, metal_whole_model };

/// Task policy supplies request eligibility; execution owns the physical choice.
pub fn capacityPolicy(route: Route, split_ring_eligible: bool) CapacityPolicy {
    return if (route == .metal_whole_model and split_ring_eligible) .split_swa_ring else .full_history;
}

pub fn lazyDeviceReservation(config: Config) bool {
    if (config.family != .gemma or !config.hasPle()) return false;
    const mixed_width = config.hasGlobalAttentionLayers() and
        (config.maxKvHeads() != config.effectiveKVHeads() or config.maxHeadDim() != config.headDim());
    return mixed_width or config.num_kv_shared_layers > 0;
}

test "decoder storage preserves physical geometry and metadata-only CUDA owners" {
    const model = Config{
        .num_hidden_layers = 8,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 32,
        .sliding_window = 64,
        .position_encoding = .rope,
    };
    for ([_]pool.BackendKind{ .native, .metal, .cuda }) |backend| {
        const plan = poolPlan(backend, .f16, &model, .{});
        try std.testing.expectEqual(@as(u16, 8), plan.num_layers_packed);
        try std.testing.expectEqual(@as(u32, 2), plan.num_kv_heads);
        try std.testing.expectEqual(@as(u32, 32), plan.head_dim);
        try std.testing.expectEqual(@as(?u32, 64), plan.sliding_window_size);
        const logical = requestMetadataPool(plan);
        try std.testing.expectEqual(backend != .cuda, logical.store_cpu_bytes);
        try std.testing.expect(plan.store_cpu_bytes);
    }
    const checkpoint = checkpointPool(.f16, model);
    try std.testing.expect(!checkpoint.store_cpu_bytes);
    try std.testing.expect(checkpoint.sliding_window_size == null);
    try std.testing.expectEqual(CapacityPolicy.full_history, capacityPolicy(.standard, true));
    try std.testing.expectEqual(CapacityPolicy.full_history, capacityPolicy(.metal_whole_model, false));
    try std.testing.expectEqual(CapacityPolicy.split_swa_ring, capacityPolicy(.metal_whole_model, true));
}

/// Task semantics supplied before choosing a physical cache layout. This
/// record contains no tokenizer, pipeline or task configuration dependency.
pub const Request = struct {
    pure_greedy: bool,
    grammar: bool,
    prompt_cache: bool,
    compaction: bool,
    speculation: bool,
    draft: bool,
};

pub fn slidingTrimForced() bool {
    return @import("antfly_platform").env.getenvBoolDefault("ANTFLY_INFERENCE_KV_SLIDING_TRIM", false);
}

pub fn ringRequestEligible(request: Request, sampled_qualified: bool) bool {
    // Select full-history storage before prefill for unsupported consumers;
    // never switch layouts or weaken the read guard after ring eviction.
    return ((request.pure_greedy and !request.grammar) or sampled_qualified) and
        !request.prompt_cache and !request.compaction and !request.speculation and !request.draft;
}

pub fn splitRingRequestEligible(config: Config, request: Request, sampled_qualified: bool) bool {
    if (comptime !@import("build_options").enable_metal) return false;
    const sampled_or_grammar = !request.pure_greedy or request.grammar;
    return config.supportsSplitSwaGlobalKvRing() and
        (!sampled_or_grammar or config.num_local_experts == 0) and
        !@import("../architectures/gemma4_runtime.zig").wholeFramePrefillExplicitlyDisabled() and
        ringRequestEligible(request, sampled_qualified) and
        !slidingTrimForced() and
        @import("../backends/metal_kv_storage.zig").MetalKvStorage.splitSwaKvRingEnabled();
}

pub fn splitRingEligible(config: Config, request: Request, dtype: pool.KvDType, sampled_qualified: bool) bool {
    if (comptime !@import("build_options").enable_metal) return false;
    return splitRingRequestEligible(config, request, sampled_qualified) and
        @import("../backends/metal_kv_storage.zig").KeyFormat.fromKvDType(dtype) != null;
}

pub fn routeCapacityPolicy(route: Route, config: Config, request: Request, dtype: pool.KvDType, sampled_qualified: bool) CapacityPolicy {
    if (route == .standard) return .full_history;
    return capacityPolicy(route, splitRingEligible(config, request, dtype, sampled_qualified));
}

pub fn selectedPeakBytes(config: Config, dtype: pool.KvDType, gpu_oracle: bool) !usize {
    const policy = @import("../runtime/kv/selection.zig").defaults;
    const transient = try policy.transientRows();
    var resident_device: usize = 0;
    var largest_row_bytes: usize = 0;
    var largest_key_width: usize = 0;
    var largest_query_width: usize = 0;
    var query_capture: usize = 0;
    for (0..@as(usize, @intCast(config.num_hidden_layers))) |layer| {
        const geometry = try config.kvDonorGeometry(layer);
        if (!config.layerUsesSlidingAttention(layer)) {
            const query_width = try std.math.mul(usize, @intCast(config.num_attention_heads), @intCast(geometry.head_dim));
            largest_query_width = @max(largest_query_width, query_width);
            query_capture = try std.math.add(usize, query_capture, try std.math.mul(usize, policy.query_horizon, try std.math.mul(usize, query_width, @sizeOf(f32))));
        }
        if (!config.isKvDonorLayer(layer) or geometry.sliding) continue;
        const row_bytes = try dtype.bytesForTokenPairChecked(geometry.num_kv_heads, geometry.head_dim);
        resident_device = try std.math.add(usize, resident_device, try std.math.mul(usize, transient, row_bytes));
        largest_row_bytes = @max(largest_row_bytes, row_bytes);
        largest_key_width = @max(largest_key_width, try std.math.mul(usize, @intCast(geometry.num_kv_heads), @intCast(geometry.head_dim)));
    }
    const staged_device = try std.math.mul(usize, transient, largest_row_bytes);
    // Positions, accumulator, GPU result, ranking records, keep bitmap, and
    // retained source/position arrays, rounded upward for alignment overhead.
    const host_selection = try std.math.mul(usize, transient, 80);
    // Metal owns copies of Q and positions, per-query/head normalization, and
    // scores. Consumer dispatches are serial, so only the largest is live.
    const gpu_q = try std.math.mul(usize, policy.query_horizon, try std.math.mul(usize, largest_query_width, @sizeOf(f32)));
    const gpu_query_positions = try std.math.mul(usize, policy.query_horizon, @sizeOf(u32));
    const gpu_key_positions = try std.math.mul(usize, transient, @sizeOf(u32));
    const gpu_norm = try std.math.mul(usize, policy.query_horizon, try std.math.mul(usize, @intCast(config.num_attention_heads), 2 * @sizeOf(f32)));
    const gpu_scores = try std.math.mul(usize, transient, @sizeOf(f32));
    var scorer = try std.math.add(usize, gpu_q, gpu_query_positions);
    scorer = try std.math.add(usize, scorer, gpu_key_positions);
    scorer = try std.math.add(usize, scorer, gpu_norm);
    scorer = try std.math.add(usize, scorer, gpu_scores);
    var oracle: usize = 0;
    if (gpu_oracle) {
        oracle = try std.math.mul(usize, transient, try std.math.mul(usize, largest_key_width, @sizeOf(f32)));
        oracle = try std.math.add(usize, oracle, try std.math.mul(usize, transient, @sizeOf(f64)));
    }
    var total = try std.math.add(usize, resident_device, staged_device);
    total = try std.math.add(usize, total, query_capture);
    total = try std.math.add(usize, total, host_selection);
    total = try std.math.add(usize, total, scorer);
    return std.math.add(usize, total, oracle);
}
