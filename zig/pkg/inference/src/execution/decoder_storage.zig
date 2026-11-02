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
