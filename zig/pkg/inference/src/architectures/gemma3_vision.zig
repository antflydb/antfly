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

const std = @import("std");
const ops = @import("../ops/ops.zig");
const gpt_mod = @import("../models/gpt.zig");
const encoder = @import("patch_encoder_projector.zig");
const ComputeBackend = ops.ComputeBackend;
const CT = ops.CT;

pub const DebugOutputs = encoder.DebugOutputs;

const Weights = struct {
    cb: *const ComputeBackend,
    allocator: std.mem.Allocator,

    pub fn get(self: @This(), name: []const u8) !CT {
        return self.cb.getWeight(name);
    }
    const Position = struct {
        cb: *const ComputeBackend,
        allocator: std.mem.Allocator,
        tensor: CT,
        data: []f32,
        pub fn deinit(self: *@This()) void {
            self.allocator.free(self.data);
            self.cb.free(self.tensor);
        }
    };
    pub fn positions(self: @This()) !Position {
        const tensor = try self.get("vision_tower.vision_model.embeddings.position_embedding.weight");
        errdefer self.cb.free(tensor);
        return .{ .cb = self.cb, .allocator = self.allocator, .tensor = tensor, .data = try self.cb.toFloat32(tensor, self.allocator) };
    }
    pub fn projection(self: @This(), in_dim: usize, out_dim: usize) !CT {
        const weight = try self.get("multi_modal_projector.mm_input_projection_weight");
        defer self.cb.free(weight);
        const data = try self.cb.toFloat32(weight, self.allocator);
        defer self.allocator.free(data);
        const transposed = try encoder.transposeMatrix(self.allocator, data, in_dim, out_dim);
        defer self.allocator.free(transposed);
        return self.cb.fromFloat32Shape(transposed, &.{ @intCast(out_dim), @intCast(in_dim) });
    }
};

fn configuration(cfg: gpt_mod.Config) !encoder.Config {
    if (!cfg.isMultimodal()) return error.InvalidMultimodalConfig;
    return .{ .text_hidden = cfg.hidden_size, .vision_hidden = cfg.vision_hidden_size, .intermediate_size = cfg.vision_intermediate_size, .block_count = cfg.vision_num_hidden_layers, .head_count = cfg.vision_num_attention_heads, .image_size = cfg.vision_image_size, .patch_size = cfg.vision_patch_size, .mm_tokens_per_image = cfg.mm_tokens_per_image, .norm_weight_offset = cfg.norm_weight_offset };
}

/// Original host token output, without allocating unused debug captures or a
/// duplicate final vector. Pixel input remains [batch,3,image,image] CHW.
pub fn encodeProjectedImageTokens(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: gpt_mod.Config, pixel_values: []const f32, batch: usize) ![]f32 {
    return encoder.encode(cb, allocator, Weights{ .cb = cb, .allocator = allocator }, try configuration(cfg), pixel_values, batch, false);
}

pub fn encodeProjectedImageTokensDebug(cb: *const ComputeBackend, allocator: std.mem.Allocator, cfg: gpt_mod.Config, pixel_values: []const f32, batch: usize) !DebugOutputs {
    return encoder.encode(cb, allocator, Weights{ .cb = cb, .allocator = allocator }, try configuration(cfg), pixel_values, batch, true);
}

const Fixture = @import("testing/patch_encoder_fixture.zig");
fn checkActualEncoder(cb_value: ComputeBackend, _: ComputeBackend) !void {
    const a = std.testing.allocator;
    const cb = &cb_value;
    var debug = try encodeProjectedImageTokensDebug(cb, a, Fixture.config(), &Fixture.pixels, 1);
    defer debug.deinit();
    try std.testing.expectEqualSlices(f32, &Fixture.patch_tokens, debug.patch_tokens);
    try std.testing.expectEqualSlices(f32, &Fixture.patch_tokens, debug.positioned_tokens);
    const pooled = Fixture.pooled();
    const normed = Fixture.normed();
    for (debug.pooled_tokens, pooled) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
    for (debug.soft_normed_tokens, normed) |actual, expected| try std.testing.expectApproxEqAbs(expected, actual, 1e-5);
    try Fixture.expectProjected(debug.projected_tokens);
    const held = try a.dupe(f32, debug.projected_tokens);
    defer a.free(held);
    const plain = try encodeProjectedImageTokens(cb, a, Fixture.config(), &Fixture.pixels, 1);
    defer a.free(plain);
    try std.testing.expectEqualSlices(f32, debug.projected_tokens, plain);
    try std.testing.expectEqualSlices(f32, held, debug.projected_tokens);
}
fn checkMissingProjection(cb: ComputeBackend, _: ComputeBackend) !void {
    const missing = if (cb.kind() == .cuda) error.WeightNotFound else error.MissingWeight;
    try std.testing.expectError(missing, encodeProjectedImageTokens(&cb, std.testing.allocator, Fixture.config(), &Fixture.pixels, 1));
}
fn checkMissingSecondNorm(cb: ComputeBackend, _: ComputeBackend) !void {
    const missing = if (cb.kind() == .cuda) error.WeightNotFound else error.MissingWeight;
    try std.testing.expectError(missing, encodeProjectedImageTokens(&cb, std.testing.allocator, Fixture.config(), &Fixture.pixels, 1));
}
fn checkActualEncoderOom(a: std.mem.Allocator, debug: bool) !void {
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    try Fixture.populate(&store);
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    if (debug) {
        var actual = try encodeProjectedImageTokensDebug(&cb, a, Fixture.config(), &Fixture.pixels, 1);
        defer actual.deinit();
        try Fixture.expectProjected(actual.projected_tokens);
    } else {
        const actual = try encodeProjectedImageTokens(&cb, a, Fixture.config(), &Fixture.pixels, 1);
        defer a.free(actual);
        try Fixture.expectProjected(actual);
    }
}
test "Native patch encoder projector shares one numerical definition and preserves independent one block oracle" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(Fixture.populate, checkActualEncoder);
}
test "Metal patch encoder projector preserves original host positions pooling and retained debug outputs" {
    try @import("../ops/media_residency_test.zig").onMetal(Fixture.populate, checkActualEncoder);
}
test "Native patch encoder projector owns late weight failure after hidden export and residual creation" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onNative(Fixture.noProjection, checkMissingProjection);
    try helper.onNative(Fixture.noSecondNorm, checkMissingSecondNorm);
}
test "Metal patch encoder projector owns late weight failure after hidden export and residual creation" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onMetal(Fixture.noProjection, checkMissingProjection);
    try helper.onMetal(Fixture.noSecondNorm, checkMissingSecondNorm);
}
test "Native patch encoder projector owns all real model and execution allocation failures" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    for ([_]bool{ false, true }) |debug| {
        try checkActualEncoderOom(std.testing.allocator, debug);
        try std.testing.checkAllAllocationFailures(std.testing.allocator, checkActualEncoderOom, .{debug});
    }
}

fn checkNonzeroEncoder(cb_value: ComputeBackend, _: ComputeBackend) !void {
    var actual = try encodeProjectedImageTokensDebug(&cb_value, std.testing.allocator, Fixture.config(), &Fixture.pixels, 1);
    defer actual.deinit();
    const pooled = [_]f32{ 1.2499998242532255, -0.9999998242532254 };
    for (actual.pooled_tokens, pooled) |value, expected| try std.testing.expectApproxEqAbs(expected, value, 1e-5);
    const normed = [_]f32{ 2.2086296898879585, -1.766903689804831 };
    for (actual.soft_normed_tokens, normed) |value, expected| try std.testing.expectApproxEqAbs(expected, value, 1e-5);
    const projected = [_]f32{ -4.858985069331365, -6.184162759053068, -9.276244138579603 };
    for (actual.projected_tokens, projected) |value, expected| try std.testing.expectApproxEqAbs(expected, value, 1e-5);
}
test "Native patch encoder projector nonzero attention and MLP agree with independent scalar golden" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    try @import("../ops/media_residency_test.zig").onNative(Fixture.populateNonzero, checkNonzeroEncoder);
}
test "Metal patch encoder projector nonzero attention and MLP agree with independent scalar golden" {
    try @import("../ops/media_residency_test.zig").onMetal(Fixture.populateNonzero, checkNonzeroEncoder);
}

test "CUDA patch encoder projector preserves independent oracle host pooling and retained debug outputs" {
    try @import("../ops/media_residency_test.zig").onCuda(Fixture.populate, checkActualEncoder);
}
test "CUDA patch encoder projector nonzero attention and MLP agree with independent scalar golden" {
    try @import("../ops/media_residency_test.zig").onCuda(Fixture.populateNonzero, checkNonzeroEncoder);
}
test "CUDA patch encoder projector owns late weight failure after hidden export and residual creation" {
    const helper = @import("../ops/media_residency_test.zig");
    try helper.onCuda(Fixture.noProjection, checkMissingProjection);
    try helper.onCuda(Fixture.noSecondNorm, checkMissingSecondNorm);
}
