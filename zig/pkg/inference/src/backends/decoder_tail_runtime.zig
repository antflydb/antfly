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
const gpt_arch = @import("../architectures/gpt.zig");
const model_runtime = @import("../graph/model_runtime.zig");
const gpt_mod = @import("../models/gpt.zig");
const ops = @import("../ops/ops.zig");
const c_std = @import("../util/c_env.zig");

pub const TimingStats = struct {
    greedy_calls: u64 = 0,
    greedy_lm_head_lookup_nanos: u128 = 0,
    greedy_fast_argmax_nanos: u128 = 0,
    greedy_fallback_norm_nanos: u128 = 0,
    greedy_fallback_linear_nanos: u128 = 0,
    greedy_fallback_host_nanos: u128 = 0,
    logits_calls: u64 = 0,
    logits_norm_nanos: u128 = 0,
    logits_lm_head_lookup_nanos: u128 = 0,
    logits_linear_nanos: u128 = 0,
    sampled_calls: u64 = 0,
    sampled_logits_nanos: u128 = 0,
    sampled_device_sample_nanos: u128 = 0,
    sampled_host_sample_nanos: u128 = 0,
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

fn preparedTailDisabled() bool {
    const value = c_std.getenv("TERMITE_METAL_DISABLE_PREPARED_TAIL") orelse return false;
    const slice = std.mem.span(value);
    return slice.len > 0 and !std.mem.eql(u8, slice, "0");
}

pub const FinalNormKind = enum {
    layer,
    rms,
};

fn applyFinalNorm(
    cb: *const ops.ComputeBackend,
    norm_kind: FinalNormKind,
    norm_slot: usize,
    input: ops.CT,
    hidden_size: usize,
    eps: f32,
) !?ops.CT {
    return switch (norm_kind) {
        .layer => cb.decoderRuntimeApplyLayerNorm(&.{
            .slot = norm_slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        }),
        .rms => cb.decoderRuntimeApplyRmsNorm(&.{
            .slot = norm_slot,
            .input = input,
            .hidden_size = hidden_size,
            .eps = eps,
        }),
    };
}

fn applyFinalNormLinearArgmax(
    cb: *const ops.ComputeBackend,
    norm_kind: FinalNormKind,
    norm_slot: usize,
    lm_head_weight: ops.CT,
    input: ops.CT,
    hidden_size: usize,
    eps: f32,
    out_dim: usize,
) !?usize {
    const final_norm = (try applyFinalNorm(
        cb,
        norm_kind,
        norm_slot,
        input,
        hidden_size,
        eps,
    )) orelse return null;
    defer cb.free(final_norm);

    const logits = try cb.linearNoBias(final_norm, lm_head_weight, 1, hidden_size, out_dim);
    defer cb.free(logits);

    if (try cb.argmaxLastRow(logits, 1, out_dim)) |argmax_id| return argmax_id;
    return null;
}

pub fn forwardPreparedGreedyFromFinalHidden(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
    norm_kind: FinalNormKind,
    norm_slot: usize,
    lm_head_linear_slot: usize,
) !?i64 {
    if (preparedTailDisabled()) return null;
    // Final logit softcap is monotonic, so it preserves greedy argmax.
    timing_stats.greedy_calls += 1;
    const started_at = monotonicNowNs();
    const token = switch (norm_kind) {
        .layer => try cb.decoderRuntimeApplyLayerNormLinearArgmax(&.{
            .norm_slot = norm_slot,
            .linear_slot = lm_head_linear_slot,
            .input = final_hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .out_dim = gpt_config.vocab_size,
        }),
        .rms => try cb.decoderRuntimeApplyRmsNormLinearArgmax(&.{
            .norm_slot = norm_slot,
            .linear_slot = lm_head_linear_slot,
            .input = final_hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .out_dim = gpt_config.vocab_size,
        }),
    };
    const finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_fast_argmax_nanos += finished_at - started_at;
    return if (token) |token_id| @intCast(token_id) else null;
}

pub fn getLmHeadWeight(cb: *const ops.ComputeBackend, gpt_config: gpt_mod.Config) !ops.CT {
    return if (gpt_config.weight_tying)
        try gpt_arch.getEmbeddingWeight(cb, gpt_config)
    else
        cb.getWeight("lm_head.weight") catch try gpt_arch.getEmbeddingWeight(cb, gpt_config);
}

pub fn forwardGreedyFromFinalHidden(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
    norm_kind: FinalNormKind,
    norm_slot: usize,
) !?i64 {
    timing_stats.greedy_calls += 1;
    var started_at = monotonicNowNs();
    const lm_head_weight = try getLmHeadWeight(cb, gpt_config);
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_lm_head_lookup_nanos += finished_at - started_at;
    defer cb.free(lm_head_weight);

    started_at = monotonicNowNs();
    if (try applyFinalNormLinearArgmax(
        cb,
        norm_kind,
        norm_slot,
        lm_head_weight,
        final_hidden,
        gpt_config.hidden_size,
        gpt_config.norm_eps,
        gpt_config.vocab_size,
    )) |argmax_id| {
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.greedy_fast_argmax_nanos += finished_at - started_at;
        return @intCast(argmax_id);
    }
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_fast_argmax_nanos += finished_at - started_at;

    started_at = monotonicNowNs();
    const final_norm = (try applyFinalNorm(
        cb,
        norm_kind,
        norm_slot,
        final_hidden,
        gpt_config.hidden_size,
        gpt_config.norm_eps,
    )) orelse return null;
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_fallback_norm_nanos += finished_at - started_at;
    defer cb.free(final_norm);

    started_at = monotonicNowNs();
    const logits = try cb.linearNoBias(final_norm, lm_head_weight, 1, gpt_config.hidden_size, gpt_config.vocab_size);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_fallback_linear_nanos += finished_at - started_at;
    defer cb.free(logits);

    started_at = monotonicNowNs();
    if (try cb.argmaxLastRow(logits, 1, gpt_config.vocab_size)) |argmax_id| {
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.greedy_fallback_host_nanos += finished_at - started_at;
        return @intCast(argmax_id);
    }

    const logits_host = try cb.toFloat32(logits, allocator);
    defer allocator.free(logits_host);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    var best_idx: usize = 0;
    for (logits_host[1..], 1..) |value, idx| {
        if (value > logits_host[best_idx]) best_idx = idx;
    }
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.greedy_fallback_host_nanos += finished_at - started_at;
    return @intCast(best_idx);
}

pub fn forwardLogitsTensorFromFinalHidden(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
    norm_kind: FinalNormKind,
    norm_slot: usize,
) !?ops.CT {
    timing_stats.logits_calls += 1;
    var started_at = monotonicNowNs();
    const final_norm = (try applyFinalNorm(
        cb,
        norm_kind,
        norm_slot,
        final_hidden,
        gpt_config.hidden_size,
        gpt_config.norm_eps,
    )) orelse return null;
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.logits_norm_nanos += finished_at - started_at;
    defer cb.free(final_norm);

    started_at = monotonicNowNs();
    const lm_head_weight = try getLmHeadWeight(cb, gpt_config);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.logits_lm_head_lookup_nanos += finished_at - started_at;
    defer cb.free(lm_head_weight);

    started_at = monotonicNowNs();
    const logits = try cb.linearNoBias(final_norm, lm_head_weight, 1, gpt_config.hidden_size, gpt_config.vocab_size);
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.logits_linear_nanos += finished_at - started_at;
    return logits;
}

pub fn forwardPreparedLogitsTensorFromFinalHidden(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
    norm_kind: FinalNormKind,
    norm_slot: usize,
    lm_head_linear_slot: usize,
) !?ops.CT {
    if (preparedTailDisabled()) return null;
    timing_stats.logits_calls += 1;
    const started_at = monotonicNowNs();
    const logits = switch (norm_kind) {
        .layer => try cb.decoderRuntimeApplyLayerNormLinear(&.{
            .norm_slot = norm_slot,
            .linear_slot = lm_head_linear_slot,
            .input = final_hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .out_dim = gpt_config.vocab_size,
        }),
        .rms => try cb.decoderRuntimeApplyRmsNormLinear(&.{
            .norm_slot = norm_slot,
            .linear_slot = lm_head_linear_slot,
            .input = final_hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .out_dim = gpt_config.vocab_size,
        }),
    };
    const finished_at = monotonicNowNs();
    if (finished_at > started_at) {
        timing_stats.logits_linear_nanos += finished_at - started_at;
    }
    return logits;
}

pub fn forwardPreparedSampledFromFinalHidden(
    cb: *const ops.ComputeBackend,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
    norm_kind: FinalNormKind,
    norm_slot: usize,
    lm_head_linear_slot: usize,
    sampling: model_runtime.SamplingConfig,
    token_history: []const i64,
) !?i64 {
    if (preparedTailDisabled()) return null;
    if (gpt_config.final_logit_softcapping > 0.0) return null;
    timing_stats.sampled_calls += 1;
    const started_at = monotonicNowNs();
    const token = switch (norm_kind) {
        .layer => try cb.decoderRuntimeApplyLayerNormLinearSample(&.{
            .norm_slot = norm_slot,
            .linear_slot = lm_head_linear_slot,
            .input = final_hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .out_dim = gpt_config.vocab_size,
            .temperature = sampling.temperature,
            .top_k = if (sampling.top_k > 0) @intCast(sampling.top_k) else 0,
            .top_p = sampling.top_p,
            .min_p = sampling.min_p,
            .repetition_penalty = sampling.repetition_penalty,
            .frequency_penalty = sampling.frequency_penalty,
            .presence_penalty = sampling.presence_penalty,
            .token_history = token_history,
        }),
        .rms => try cb.decoderRuntimeApplyRmsNormLinearSample(&.{
            .norm_slot = norm_slot,
            .linear_slot = lm_head_linear_slot,
            .input = final_hidden,
            .hidden_size = gpt_config.hidden_size,
            .eps = gpt_config.norm_eps,
            .out_dim = gpt_config.vocab_size,
            .temperature = sampling.temperature,
            .top_k = if (sampling.top_k > 0) @intCast(sampling.top_k) else 0,
            .top_p = sampling.top_p,
            .min_p = sampling.min_p,
            .repetition_penalty = sampling.repetition_penalty,
            .frequency_penalty = sampling.frequency_penalty,
            .presence_penalty = sampling.presence_penalty,
            .token_history = token_history,
        }),
    };
    const finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_device_sample_nanos += finished_at - started_at;
    return if (token) |token_id| @intCast(token_id) else null;
}

pub fn forwardSampledFromFinalHidden(
    cb: *const ops.ComputeBackend,
    allocator: std.mem.Allocator,
    gpt_config: gpt_mod.Config,
    final_hidden: ops.CT,
    norm_kind: FinalNormKind,
    norm_slot: usize,
    sampling: model_runtime.SamplingConfig,
    token_history: []const i64,
) !?i64 {
    timing_stats.sampled_calls += 1;
    var started_at = monotonicNowNs();
    const logits = (try forwardLogitsTensorFromFinalHidden(
        cb,
        gpt_config,
        final_hidden,
        norm_kind,
        norm_slot,
    )) orelse return null;
    var finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_logits_nanos += finished_at - started_at;
    defer cb.free(logits);
    if (gpt_config.final_logit_softcapping <= 0.0) {
        started_at = monotonicNowNs();
        const logits_host = try cb.toFloat32(logits, allocator);
        defer allocator.free(logits_host);
        const sampled = @as(i64, @intCast(model_runtime.sampleTokenFromLogits(allocator, logits_host, sampling, token_history)));
        finished_at = monotonicNowNs();
        if (finished_at > started_at) timing_stats.sampled_host_sample_nanos += finished_at - started_at;
        return sampled;
    }
    started_at = monotonicNowNs();
    const logits_host = try cb.toFloat32(logits, allocator);
    defer allocator.free(logits_host);
    gpt_arch.applyFinalLogitSoftcapInPlace(gpt_config, logits_host);
    const sampled = @as(i64, @intCast(model_runtime.sampleTokenFromLogits(allocator, logits_host, sampling, token_history)));
    finished_at = monotonicNowNs();
    if (finished_at > started_at) timing_stats.sampled_host_sample_nanos += finished_at - started_at;
    return sampled;
}
