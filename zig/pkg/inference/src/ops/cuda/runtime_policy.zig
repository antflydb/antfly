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

//! Immutable operator settings shared by CUDA's hot allocation and GEMM paths.
const std = @import("std");
const env = @import("antfly_platform").env;

pub const Policy = struct {
    temp_cache_mb: ?usize = null,
    max_temp_buffers: ?usize = null,
    stable_temp_reuse: bool = false,
    trace: bool = false,
    trace_skip: usize = 0,
    trace_limit: usize = 4096,
    workspace_bytes: usize = 0,
    prefill_profile: bool = false,
    split_attention: ?bool = null,
    split_attention_chunk: ?usize = null,

    pub fn resolve() Policy {
        const workspace_mb = env.getenvUsize("ANTFLY_INFERENCE_CUDA_CUBLASLT_WORKSPACE_MB") orelse
            env.getenvUsize("ANTFLY_CUDA_CUBLASLT_WORKSPACE_MB") orelse
            env.getenvUsize("TERMITE_CUDA_CUBLASLT_WORKSPACE_MB") orelse 0;
        return .{
            .temp_cache_mb = env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MB"),
            .max_temp_buffers = env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_CACHE_MAX_BUFFERS"),
            .stable_temp_reuse = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_STABLE_REUSE", false),
            .trace = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_TEMP_TRACE", false),
            .trace_skip = env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_TRACE_SKIP") orelse 0,
            .trace_limit = env.getenvUsize("ANTFLY_INFERENCE_CUDA_TEMP_TRACE_LIMIT") orelse 4096,
            .workspace_bytes = std.math.mul(usize, workspace_mb, 1024 * 1024) catch 0,
            .prefill_profile = env.getenvBoolDefault("ANTFLY_INFERENCE_CUDA_PROFILE_PREFILL_OPS", false),
            .split_attention = if (env.getenv("ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION") != null)
                env.getenvBool("ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION")
            else
                null,
            .split_attention_chunk = env.getenvUsize("ANTFLY_INFERENCE_CUDA_TURBOQUANT_SPLIT_ATTENTION_CHUNK"),
        };
    }

    // Full document contexts make serial per-key decode disproportionately
    // expensive. Keep the paged F16/F32 route on the GPU, with bounded split
    // scratch and the existing kernel's storage/shape eligibility checks.
    // Other model families retain their own independently qualified defaults.
    pub fn splitAttentionEnabled(self: Policy, lighton: bool, compute_major: i32) bool {
        return self.split_attention orelse (lighton and compute_major >= 8);
    }

    pub fn splitAttentionChunk(self: Policy, lighton: bool) usize {
        return self.split_attention_chunk orelse if (lighton) 32 else 128;
    }

    pub fn cacheBytes(self: Policy, training: bool) usize {
        const mb = self.temp_cache_mb orelse if (training) @as(usize, 2048) else 1024;
        return std.math.mul(usize, mb, 1024 * 1024) catch 0;
    }

    pub fn bufferLimit(self: Policy, training: bool) usize {
        return self.max_temp_buffers orelse if (training) @as(usize, 4096) else 256;
    }
};

test "LightOnOCR3 split attention defaults preserve explicit disable and other model policies" {
    const defaults = Policy{};
    try std.testing.expect(defaults.splitAttentionEnabled(true, 8));
    try std.testing.expect(defaults.splitAttentionEnabled(true, 12));
    try std.testing.expect(!defaults.splitAttentionEnabled(true, 7));
    try std.testing.expect(!defaults.splitAttentionEnabled(false, 12));
    try std.testing.expectEqual(@as(usize, 32), defaults.splitAttentionChunk(true));
    try std.testing.expectEqual(@as(usize, 128), defaults.splitAttentionChunk(false));
    const disabled = Policy{ .split_attention = false, .split_attention_chunk = 0 };
    try std.testing.expect(!disabled.splitAttentionEnabled(true, 12));
    try std.testing.expectEqual(@as(usize, 0), disabled.splitAttentionChunk(true));
    const explicit = Policy{ .split_attention = true, .split_attention_chunk = 64 };
    try std.testing.expect(explicit.splitAttentionEnabled(false, 8));
    try std.testing.expectEqual(@as(usize, 64), explicit.splitAttentionChunk(true));
}

test "LightOnOCR3 CUDA resolved buffer policy retains zero limits overflow and training defaults" {
    const defaults = Policy{};
    try std.testing.expectEqual(@as(usize, 1024 * 1024 * 1024), defaults.cacheBytes(false));
    try std.testing.expectEqual(@as(usize, 2048 * 1024 * 1024), defaults.cacheBytes(true));
    try std.testing.expectEqual(@as(usize, 256), defaults.bufferLimit(false));
    try std.testing.expectEqual(@as(usize, 4096), defaults.bufferLimit(true));
    const zero = Policy{ .temp_cache_mb = 0, .max_temp_buffers = 0 };
    for ([_]bool{ false, true }) |training| {
        try std.testing.expectEqual(@as(usize, 0), zero.cacheBytes(training));
        try std.testing.expectEqual(@as(usize, 0), zero.bufferLimit(training));
        const bounded = Policy{ .temp_cache_mb = 7, .max_temp_buffers = 9 };
        try std.testing.expectEqual(@as(usize, 7 * 1024 * 1024), bounded.cacheBytes(training));
        try std.testing.expectEqual(@as(usize, 9), bounded.bufferLimit(training));
        try std.testing.expectEqual(@as(usize, 0), (Policy{ .temp_cache_mb = std.math.maxInt(usize) }).cacheBytes(training));
    }
}
