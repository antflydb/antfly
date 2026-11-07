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

//! Focused ownership and arithmetic-routing tests. The default target needs no
//! device or models; test-gemma-paired-head explicitly exercises CUDA hardware.
test {
    _ = @import("pipelines/generation.zig");
    _ = @import("runtime/scheduler/native_generate.zig");
    _ = @import("ops/execution_batch.zig");
    _ = @import("ops/execution_scope.zig");
    _ = @import("server/server.zig");
    _ = @import("runtime/kv/prompt_cache.zig");
    _ = @import("runtime/scheduler/generation_admission.zig");
    _ = @import("ops/cuda/cuda_compute.zig");
}

test "CUDA E4B paired head hardware arithmetic" {
    const std = @import("std");
    if (!@import("antfly_platform").env.getenvBool("ANTFLY_TEST_CUDA_GEMMA_PAIRED_HEAD")) return error.SkipZigTest;
    const Compute = @import("ops/cuda/cuda_compute.zig").CudaCompute;
    const Buffer = @import("ops/cuda/buffer.zig").DeviceBuffer;
    var compute = try Compute.init(std.testing.allocator);
    defer compute.deinit();
    const width = 262144;
    if (!@import("ops/cuda/gemma_decode_head.zig").supported(compute.ctx.info.compute_major, compute.ctx.info.driver_version, 2, 2560, width)) return error.SkipZigTest;
    const row_bytes = 2880;
    const a = std.testing.allocator;
    const weights = try a.alloc(u8, width * 2100);
    defer a.free(weights);
    for (weights, 0..) |*byte, i| byte.* = @truncate(i * 37 + i / 11);
    for (0..width * 10) |block| {
        const scale: u16 = @intCast(0x3000 + (block * 37) % 4095);
        weights[block * 210 + 208] = @truncate(scale);
        weights[block * 210 + 209] = @truncate(scale >> 8);
    }
    var weight = try Buffer.alloc(&compute.ctx, weights.len);
    defer weight.free(&compute.ctx);
    try weight.copyFromHost(&compute.ctx, weights);
    var input = try Buffer.alloc(&compute.ctx, 2 * row_bytes);
    defer input.free(&compute.ctx);
    var expected = try Buffer.alloc(&compute.ctx, 2 * width * 4);
    defer expected.free(&compute.ctx);
    var actual = try Buffer.alloc(&compute.ctx, 2 * width * 4);
    defer actual.free(&compute.ctx);
    const expected_host = try a.alloc(u8, expected.len);
    defer a.free(expected_host);
    const actual_host = try a.alloc(u8, actual.len);
    defer a.free(actual_host);
    for (0..3) |seed| {
        var activations: [2 * row_bytes]u8 = undefined;
        for (&activations, 0..) |*byte, i| byte.* = @truncate(i * 13 + seed * 29 + i / row_bytes);
        for (0..160) |block| {
            const scale: u16 = @intCast(0x2000 + ((block + seed) * 53) % 6000);
            activations[block * 36] = @truncate(scale);
            activations[block * 36 + 1] = @truncate(scale >> 8);
        }
        try input.copyFromHost(&compute.ctx, &activations);
        for (0..2) |row| try compute.kernels.launchLinearQ6KQ8_1F32Tile8E4B(&compute.ctx, .{ .ptr = expected.ptr + row * width * 4, .len = width * 4 }, .{ .ptr = input.ptr + row * row_bytes, .len = row_bytes }, weight, 2560, width);
        try std.testing.expect(try compute.gemma_decode_head.run(&compute.ctx, actual, input, weight, 2, 2560, width));
        try expected.copyToHost(&compute.ctx, expected_host);
        try actual.copyToHost(&compute.ctx, actual_host);
        try compute.ctx.synchronize();
        try std.testing.expectEqualSlices(u8, expected_host, actual_host);
    }
    const started = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..20) |_| for (0..2) |row| try compute.kernels.launchLinearQ6KQ8_1F32Tile8E4B(&compute.ctx, .{ .ptr = expected.ptr + row * width * 4, .len = width * 4 }, .{ .ptr = input.ptr + row * row_bytes, .len = row_bytes }, weight, 2560, width);
    try compute.ctx.synchronize();
    const middle = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    for (0..20) |_| _ = try compute.gemma_decode_head.run(&compute.ctx, actual, input, weight, 2, 2560, width);
    try compute.ctx.synchronize();
    const ended = std.Io.Clock.awake.now(std.testing.io).nanoseconds;
    std.debug.print("GEMMA_PAIRED_HEAD baseline_ns={d} paired_ns={d}\n", .{ middle - started, ended - middle });
}
