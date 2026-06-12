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
const gpt_arch = @import("../architectures/gpt.zig");
const gpt_mod = @import("../models/gpt.zig");
const metal_compute = @import("../ops/metal_compute.zig");
const ops = @import("../ops/ops.zig");

pub const TimingStats = struct {
    embed_calls: u64 = 0,
    embed_lookup_nanos: u128 = 0,
    embed_gather_nanos: u128 = 0,
    embed_scale_nanos: u128 = 0,
    rms_norm_calls: u64 = 0,
    rms_norm_nanos: u128 = 0,
    linear_calls: u64 = 0,
    linear_nanos: u128 = 0,
    linear_quantized_calls: u64 = 0,
    linear_quantized_nanos: u128 = 0,
    linear_dense_calls: u64 = 0,
    linear_dense_nanos: u128 = 0,
};

var timing_stats = TimingStats{};

pub fn resetTimingStats() void {
    timing_stats = .{};
}

pub fn getTimingStats() TimingStats {
    return timing_stats;
}

fn monotonicNowNs() u64 {
    var ts: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &ts))) {
        .SUCCESS => return @intCast(@as(i128, ts.sec) * std.time.ns_per_s + ts.nsec),
        else => return 0,
    }
}

fn dequantizeTensorToFloat32Generic(
    cb: *const ops.ComputeBackend,
    tensor: ops.CT,
    allocator: std.mem.Allocator,
) ![]f32 {
    return switch (cb.kind()) {
        .metal => metal_compute.MetalCompute.dequantizeTensorToFloat32(cb, tensor, allocator),
        else => cb.toFloat32(tensor, allocator),
    };
}

fn getQuantizedStorageGeneric(
    cb: *const ops.ComputeBackend,
    tensor: ops.CT,
) ?*const @import("../models/weight_source.zig").QuantizedStorage {
    return switch (cb.kind()) {
        .metal => metal_compute.MetalCompute.getQuantizedStorage(cb, tensor),
        else => null,
    };
}

fn zeroBiasTensorGeneric(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    out_dim: usize,
) !ops.CT {
    return switch (cb.kind()) {
        .metal => metal_compute.MetalCompute.zeroBiasTensor(cb, out_dim),
        else => blk: {
            const bias_host = try allocator.alloc(f32, out_dim);
            defer allocator.free(bias_host);
            @memset(bias_host, 0.0);
            const bias_shape = [_]i32{@intCast(out_dim)};
            break :blk try cb.fromFloat32Shape(bias_host, &bias_shape);
        },
    };
}

pub fn prepareRmsNormSlot(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    slot: usize,
    weight: ops.CT,
    hidden_size: usize,
) !bool {
    const started_at = monotonicNowNs();
    timing_stats.rms_norm_calls += 1;
    if (getQuantizedStorageGeneric(cb, weight) == null and
        std.math.approxEqAbs(f32, gpt_config.norm_weight_offset, 0.0, 1e-6))
    {
        const prepared = try cb.decoderRuntimePrepareRmsNorm(&.{
            .slot = slot,
            .weight = weight,
            .hidden_size = hidden_size,
        });
        const finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.rms_norm_nanos += finished_at - started_at;
        return prepared;
    }

    const weight_host = try dequantizeTensorToFloat32Generic(cb, weight, allocator);
    defer allocator.free(weight_host);
    if (!std.math.approxEqAbs(f32, gpt_config.norm_weight_offset, 0.0, 1e-6)) {
        for (weight_host) |*value| value.* += gpt_config.norm_weight_offset;
    }
    const weight_shape = [_]i32{@intCast(hidden_size)};
    const weight_dense = try cb.fromFloat32Shape(weight_host, &weight_shape);
    defer cb.free(weight_dense);

    const prepared = try cb.decoderRuntimePrepareRmsNorm(&.{
        .slot = slot,
        .weight = weight_dense,
        .hidden_size = hidden_size,
    });
    const finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.rms_norm_nanos += finished_at - started_at;
    return prepared;
}

pub fn prepareLinearNoBiasSlot(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    slot: usize,
    weight: ops.CT,
    in_dim: usize,
    out_dim: usize,
) !bool {
    return prepareLinearNoBiasSlotWithFallback(cb, allocator, slot, weight, in_dim, out_dim, true);
}

pub fn prepareLinearNoBiasDenseSlot(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    slot: usize,
    weight: ops.CT,
    in_dim: usize,
    out_dim: usize,
    retain_dense_fallback: bool,
) !bool {
    const started_at = monotonicNowNs();
    timing_stats.linear_calls += 1;
    const bias_dense = try zeroBiasTensorGeneric(cb, allocator, out_dim);
    defer cb.free(bias_dense);

    const prepared = try cb.decoderRuntimePrepareLinear(&.{
        .slot = slot,
        .weight = weight,
        .bias = bias_dense,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = retain_dense_fallback,
    });
    const finished_at = monotonicNowNs();
    timing_stats.linear_dense_calls += 1;
    if (finished_at > started_at) {
        const elapsed = finished_at - started_at;
        timing_stats.linear_nanos += elapsed;
        timing_stats.linear_dense_nanos += elapsed;
    }
    return prepared;
}

pub fn prepareLinearNoBiasSlotWithFallback(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    slot: usize,
    weight: ops.CT,
    in_dim: usize,
    out_dim: usize,
    retain_dense_fallback: bool,
) !bool {
    const started_at = monotonicNowNs();
    timing_stats.linear_calls += 1;
    const bias_dense = try zeroBiasTensorGeneric(cb, allocator, out_dim);
    defer cb.free(bias_dense);

    if (getQuantizedStorageGeneric(cb, weight) != null) {
        const prepared = try cb.decoderRuntimePrepareLinear(&.{
            .slot = slot,
            .weight = weight,
            .bias = bias_dense,
            .in_dim = in_dim,
            .out_dim = out_dim,
            .retain_dense_fallback = retain_dense_fallback,
        });
        const finished_at = monotonicNowNs();
        timing_stats.linear_quantized_calls += 1;
        if (finished_at > started_at) {
            const elapsed = finished_at - started_at;
            timing_stats.linear_nanos += elapsed;
            timing_stats.linear_quantized_nanos += elapsed;
        }
        return prepared;
    }

    const prepared = try cb.decoderRuntimePrepareLinear(&.{
        .slot = slot,
        .weight = weight,
        .bias = bias_dense,
        .in_dim = in_dim,
        .out_dim = out_dim,
        .retain_dense_fallback = retain_dense_fallback,
    });
    const finished_at = monotonicNowNs();
    timing_stats.linear_dense_calls += 1;
    if (finished_at > started_at) {
        const elapsed = finished_at - started_at;
        timing_stats.linear_nanos += elapsed;
        timing_stats.linear_dense_nanos += elapsed;
    }
    return prepared;
}

pub fn embedToken(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    token_id: i64,
) !ops.CT {
    timing_stats.embed_calls += 1;
    const input_ids = [_]i64{token_id};
    var started_at = monotonicNowNs();
    const embed_w = try gpt_arch.getEmbeddingWeight(cb, gpt_config);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.embed_lookup_nanos += finished_at - started_at;
    defer cb.free(embed_w);
    started_at = monotonicNowNs();
    const embedded = try cb.embeddingLookup(embed_w, input_ids[0..], 1, gpt_config.hidden_size);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.embed_gather_nanos += finished_at - started_at;
    started_at = monotonicNowNs();
    const scaled = try gpt_arch.maybeScaleTokenEmbeddings(cb, allocator, gpt_config, embedded, 1, gpt_config.hidden_size);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.embed_scale_nanos += finished_at - started_at;
    return scaled;
}

pub fn embedTokens(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    input_ids: []const i64,
) !ops.CT {
    timing_stats.embed_calls += 1;
    var started_at = monotonicNowNs();
    const embed_w = try gpt_arch.getEmbeddingWeight(cb, gpt_config);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.embed_lookup_nanos += finished_at - started_at;
    defer cb.free(embed_w);
    started_at = monotonicNowNs();
    const embedded = try cb.embeddingLookup(embed_w, input_ids, input_ids.len, gpt_config.hidden_size);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.embed_gather_nanos += finished_at - started_at;
    started_at = monotonicNowNs();
    const scaled = try gpt_arch.maybeScaleTokenEmbeddings(cb, allocator, gpt_config, embedded, input_ids.len, gpt_config.hidden_size);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.embed_scale_nanos += finished_at - started_at;
    return scaled;
}
