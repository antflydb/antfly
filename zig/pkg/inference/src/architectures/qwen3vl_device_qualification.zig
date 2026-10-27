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

//! Differential checks for the CUDA encoder-to-prompt handoff. Called only by
//! the existing real-checkpoint vision qualification; not by serving requests.
const std = @import("std");
const ops = @import("../ops/ops.zig");
const projector = @import("qwen3vl_projector.zig");
const gpt_config = @import("../models/gpt.zig");
const cuda = @import("../ops/cuda/cuda_compute.zig");
const platform = @import("antfly_platform");

pub fn check(cb: *const ops.ComputeBackend, a: std.mem.Allocator, cfg: gpt_config.Config, prepared: []const *const projector.PreparedImage, host: []const projector.ProjectedImages, limits: projector.Limits) !void {
    if (cb.kind() != .cuda or prepared.len != host.len) return error.InvalidTensorShape;
    const compute: *cuda.CudaCompute = @ptrCast(@alignCast(cb.ptr));
    const before = compute.stats.d2h_bytes;
    const device = try projector.encodePreparedProjectedImagesDeviceBatched(cb, a, cfg, prepared, limits);
    defer {
        for (device) |*value| value.deinit();
        a.free(device);
    }
    try std.testing.expectEqual(before, compute.stats.d2h_bytes);
    var worst: f32 = 0;
    for (device, host, 0..) |value, reference, index| {
        try std.testing.expect(value.embeddings.len == 0 and value.deepstack_layer_count == 0);
        try std.testing.expectEqual(cb.ptr, value.device_backend.?.ptr);
        try std.testing.expectEqualSlices(usize, reference.tokens_per_image, value.tokens_per_image);
        try std.testing.expectEqualDeep(reference.grids[0], value.grids[0]);
        const tensor: *const cuda.CudaTensor = @ptrCast(@alignCast(value.device_embeddings.?));
        try std.testing.expect(tensor.owns_buffer and tensor.resident_parent == null and !tensor.buffer.borrowed);
        try std.testing.expectEqual(value.tokens_per_image[0] * value.hidden_size, tensor.elem_count);
        for (device[0..index]) |peer| {
            const prior: *const cuda.CudaTensor = @ptrCast(@alignCast(peer.device_embeddings.?));
            try std.testing.expect(tensor.buffer.ptr >= prior.buffer.ptr + prior.buffer.len or prior.buffer.ptr >= tensor.buffer.ptr + tensor.buffer.len);
        }
        const features = try cb.toFloat32(value.device_embeddings.?, a);
        defer a.free(features);
        try std.testing.expectEqual(reference.embeddings.len, features.len);
        for (reference.embeddings, features) |expected, actual| {
            const error_abs = @abs(expected - actual);
            worst = @max(worst, error_abs);
            // Retain the original encoder/merger qualification threshold.
            try std.testing.expect(std.math.isFinite(actual) and error_abs <= 2e-4 + @abs(expected) * 2e-5);
        }
        // An independent allocation must not reuse a still-owned image. This
        // also exercises prompt assembly after another queued GPU producer.
        const poison = try a.alloc(f32, features.len);
        defer a.free(poison);
        @memset(poison, -991);
        const unrelated = try cb.fromFloat32Shape(poison, &.{ @intCast(value.tokens_per_image[0]), @intCast(value.hidden_size) });
        defer cb.free(unrelated);
        const placeholder_ids = [_]i32{ 0, cfg.image_token_index, 1 };
        var mirror = reference;
        mirror.embeddings = features;
        var expected_prompt = try projector.prepareExpandedPromptEmbeddings(cb, a, cfg, &placeholder_ids, mirror, 4096);
        defer expected_prompt.deinit(cb);
        const download_before = compute.stats.d2h_bytes;
        var actual_prompt = try projector.prepareExpandedPromptEmbeddings(cb, a, cfg, &placeholder_ids, value, 4096);
        defer actual_prompt.deinit(cb);
        try std.testing.expectEqual(download_before, compute.stats.d2h_bytes);
        try std.testing.expectEqualSlices(i64, expected_prompt.token_ids, actual_prompt.token_ids);
        try std.testing.expectEqualSlices(u32, expected_prompt.plan.mrope_positions, actual_prompt.plan.mrope_positions);
        try std.testing.expectEqualSlices(bool, expected_prompt.plan.visual_token_mask, actual_prompt.plan.visual_token_mask);
        const expected_values = try cb.toFloat32(expected_prompt.input_embeddings.?, a);
        defer a.free(expected_values);
        const actual_values = try cb.toFloat32(actual_prompt.input_embeddings.?, a);
        defer a.free(actual_values);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(expected_values), std.mem.sliceAsBytes(actual_values));
    }
    if (platform.env.getenvBool("ANTFLY_BENCH_LIGHTONOCR3"))
        std.debug.print("LIGHTONOCR3 device_vision_qualification batch={d} feature_download_bytes=0 independent_page_storage=true prompt_bitwise=true max_abs={d}\n", .{ device.len, worst });
}
