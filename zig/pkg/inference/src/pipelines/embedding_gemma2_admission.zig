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

//! Scalar text admission shared by flat inputs and ordered native text groups.
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
