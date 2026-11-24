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

//! Text and media cohort geometry shared by prepared EmbeddingGemma2 stages.
const std = @import("std");
const sessions = @import("../backends/session.zig");

pub fn textEncoderWorkspace(rows: usize) error{ResourceLimitExceeded}!usize {
    const activations = std.math.mul(usize, rows, 192 * 1024) catch return error.ResourceLimitExceeded;
    return std.math.add(usize, 256 * 1024 * 1024, activations) catch error.ResourceLimitExceeded;
}

pub fn nativeTextEncoderRequest(input_bytes: usize, rows: usize, dimensions: usize) error{ResourceLimitExceeded}!sessions.RunRequest {
    return .{
        .batch = 1,
        .sequence = rows,
        .input_bytes = input_bytes,
        .workspace_bytes = try textEncoderWorkspace(rows),
        .output_bytes = std.math.mul(usize, dimensions, @sizeOf(f32)) catch return error.ResourceLimitExceeded,
    };
}

pub fn cohortCount(lengths: []const usize, maximum: usize, compact: bool) usize {
    var count: usize = 0;
    var tokens: usize = 0;
    var longest: usize = 0;
    for (lengths[0..@min(lengths.len, @min(maximum, 4))]) |length| {
        if (length == 0 or length > 8192) break;
        const next = @max(longest, length);
        if (if (compact) tokens + length > 8192 else next * (count + 1) > 8192 or (count > 0 and next * (count + 1) * 4 > (tokens + length) * 5)) break;
        longest = next;
        tokens += length;
        count += 1;
    }
    return count;
}

pub const TextCohort = struct {
    padded_sequence: usize,
    token_slots: usize,
    request: sessions.RunRequest,
};

/// Plan the physical encoder before the caller packs its semantic input rows.
/// Compact kernels reserve active rows while both routes charge the same
/// caller-side padded packet and retain its original output dimensions.
pub fn textCohort(lengths: []const usize, dimensions: usize, compact: bool) error{ InvalidInputShape, ResourceLimitExceeded }!TextCohort {
    if (lengths.len == 0) return error.InvalidInputShape;
    var sequence: usize = 0;
    var active_rows: usize = 0;
    for (lengths) |length| {
        if (length == 0) return error.InvalidInputShape;
        sequence = @max(sequence, length);
        active_rows = std.math.add(usize, active_rows, length) catch return error.ResourceLimitExceeded;
    }
    const tokens = std.math.mul(usize, lengths.len, sequence) catch return error.ResourceLimitExceeded;
    const output_elements = std.math.mul(usize, lengths.len, dimensions) catch return error.ResourceLimitExceeded;
    return .{
        .padded_sequence = sequence,
        .token_slots = tokens,
        .request = .{
            .batch = if (compact) 1 else lengths.len,
            .sequence = if (compact) active_rows else sequence,
            .input_bytes = std.math.mul(usize, tokens, 24) catch return error.ResourceLimitExceeded,
            .host_preprocess_bytes = std.math.mul(usize, tokens, 32) catch return error.ResourceLimitExceeded,
            .workspace_bytes = try textEncoderWorkspace(if (compact) active_rows else tokens),
            .output_bytes = std.math.mul(usize, output_elements, @sizeOf(f32)) catch return error.ResourceLimitExceeded,
        },
    };
}

test "prepared text cohorts share packet geometry and reserve their actual compact or padded rows" {
    const compact = try textCohort(&.{ 32, 48 }, 128, true);
    const padded = try textCohort(&.{ 32, 48 }, 128, false);
    try std.testing.expectEqual(@as(usize, 48), compact.padded_sequence);
    try std.testing.expectEqual(@as(usize, 96), compact.token_slots);
    try std.testing.expectEqual(compact.token_slots, padded.token_slots);
    try std.testing.expectEqual(@as(usize, 80), compact.request.sequence);
    try std.testing.expectEqual(@as(usize, 1), compact.request.batch);
    try std.testing.expectEqual(@as(usize, 48), padded.request.sequence);
    try std.testing.expectEqual(@as(usize, 2), padded.request.batch);
    try std.testing.expectEqual(try textEncoderWorkspace(80), compact.request.workspace_bytes);
    try std.testing.expectEqual(try textEncoderWorkspace(96), padded.request.workspace_bytes);
    try std.testing.expectEqual(compact.request.input_bytes, padded.request.input_bytes);
    try std.testing.expectEqual(compact.request.output_bytes, padded.request.output_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, textCohort(&.{ 32, 48 }, std.math.maxInt(usize), true));
}

test "admission native text groups use token rows within the default CPU scratch envelope" {
    const memory = @import("../runtime/tier/memory.zig");
    const MiB = 1024 * 1024;
    var controller: memory.AdmissionController = .{};
    const limits = memory.Limits{ .host_limit_bytes = 2048 * MiB, .scratch_limit_bytes = 341 * MiB };
    const admission = sessions.RunAdmission{
        .controller = &controller,
        .backend_class = .cpu,
        .limits = limits,
        .static_workspace_bytes = 128 * MiB,
        .check_live_memory = false,
        .model_profile = .{ .hidden_size = 512, .intermediate_size = 1536, .attention_heads = 4 },
    };
    var temporaries = try controller.tryAcquire(.cpu, limits, .{ .host_scratch_bytes = MiB }, false);
    defer temporaries.release();
    // Text bytes can greatly exceed token rows. Preparation has its own lease.
    var execution = try controller.tryAcquire(.cpu, limits, try admission.estimateRequest(try nativeTextEncoderRequest(4096, 6, 768), &.{}), false);
    defer execution.release();
    try execution.retain(try admission.estimateRequest(try nativeTextEncoderRequest(4096, 4, 768), &.{}));
    try std.testing.expectError(error.ResourceLimitExceeded, controller.tryAcquire(.cpu, limits, try admission.estimateRequest(try nativeTextEncoderRequest(4096, 8192, 768), &.{}), false));
    try std.testing.expectError(error.ResourceLimitExceeded, textEncoderWorkspace(std.math.maxInt(usize)));
}
