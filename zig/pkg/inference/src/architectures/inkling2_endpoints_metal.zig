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

//! Device-resident token embeddings and output projection for Inkling.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const metal = @import("../backends/metal_runtime.zig");
const tensors = @import("../backends/metal_tensor.zig");
const Tensor = tensors.MetalTensor;

pub const Endpoints = struct {
    weights: [5]Tensor,
    width: u32,
    vocab: u32,
    epsilon: f32,
    softcap: f32,

    pub fn load(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: *const checkpoint.Checkpoint) !Endpoints {
        const text = source.config.value.text_config;
        return fromBytes(a, runtime, text.hidden_size, text.unpadded_vocab_size, text.rms_norm_eps, text.final_logit_softcapping, .{
            try source.reader.rawTensorBytes("model.llm.embed.weight"),
            try source.reader.rawTensorBytes("model.llm.embed_norm.weight"),
            try source.reader.rawTensorBytes("model.llm.attn_res.output_query"),
            try source.reader.rawTensorBytes("model.llm.norm.weight"),
            try source.reader.rawTensorBytes("model.llm.unembed.weight"),
        });
    }

    pub fn fromBytes(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, width: u32, vocab: u32, epsilon: f32, softcap: f32, payloads: [5][]const u8) !Endpoints {
        if (width == 0 or vocab == 0) return error.InvalidShape;
        const matrix = try std.math.mul(usize, width, vocab);
        const elements = [5]usize{ matrix, width, width, width, matrix };
        const dtypes: [5]tensors.DType = .{ .i16, .i16, .f32, .i16, .i16 };
        var result: Endpoints = .{ .weights = undefined, .width = width, .vocab = vocab, .epsilon = epsilon, .softcap = softcap };
        var count: usize = 0;
        errdefer for (result.weights[0..count]) |*weight| weight.deinit();
        for (&result.weights, dtypes, elements, payloads) |*weight, dtype, len, bytes| {
            const extent = std.math.cast(i32, len) orelse return error.InvalidShape;
            const size = try std.math.mul(usize, len, dtype.byteSize());
            if (bytes.len < size) return error.InvalidShape;
            weight.* = try Tensor.deviceAllocateTyped(a, runtime, dtype, .shared, &.{extent});
            count += 1;
            try weight.uploadBytes(bytes[0..size]);
        }
        return result;
    }

    pub fn deinit(self: *Endpoints) void {
        for (&self.weights) |*weight| weight.deinit();
    }

    pub fn embed(self: *const Endpoints, runtime: *metal.RawMetalDecodeRuntime, token: u32, output: *Tensor) !void {
        if (token >= self.vocab) return error.InvalidInkling2Token;
        const bytes = @as(usize, self.width) * 2;
        var source = try self.weights[0].retainedView(@as(usize, token) * bytes, bytes, &.{@intCast(self.width)});
        defer source.deinit();
        try metal.rowRmsBf16(runtime, 1, self.width, self.epsilon, &source, &self.weights[1], output);
    }

    pub fn logits(self: *const Endpoints, runtime: *metal.RawMetalDecodeRuntime, blocks: ?*const Tensor, last: *const Tensor, sources: u32, normalized: *Tensor, output: *Tensor) !void {
        if (output.dtype != .f32) return error.UnsupportedTensorType;
        const mix: metal.ResidualMixBf16 = .{
            .rows = 1,
            .width = self.width,
            .sources = sources,
            .epsilon = self.epsilon,
            .norm_epsilon = self.epsilon,
        };
        try mix.encode(runtime, .{ .blocks = blocks, .last = last, .query = &self.weights[2], .gamma = &self.weights[3] }, normalized);
        const head: metal.DenseBf16Projection = .{
            .rows = 1,
            .input_width = self.width,
            .output_width = self.vocab,
            .softcap = self.softcap,
        };
        try head.encode(runtime, normalized, &self.weights[4], output);
    }
};

test "Inkling Metal decoder endpoints embed and project vocabulary logits in one frame" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const embeddings: [6]u16 = .{ 0xbf80, 0xbf80, 0x4000, 0x4000, 0, 0 };
    const norm: [2]u16 = @splat(0x3f80);
    const query: [2]f32 = @splat(0);
    const head: [6]u16 = .{ 0x3f80, 0, 0xbf80, 0, 0x4100, 0x4100 };
    var endpoints = try Endpoints.fromBytes(a, runtime, 2, 2, 0.000001, 1, .{
        std.mem.asBytes(&embeddings), std.mem.asBytes(&norm),
        std.mem.asBytes(&query),      std.mem.asBytes(&norm),
        std.mem.asBytes(&head),
    });
    defer endpoints.deinit();
    var embedded = try Tensor.deviceAllocateTyped(a, runtime, .i16, .private, &.{2});
    defer embedded.deinit();
    var normalized = try Tensor.deviceAllocateTyped(a, runtime, .i16, .private, &.{2});
    defer normalized.deinit();
    var logits = try Tensor.deviceAllocateTyped(a, runtime, .f32, .shared, &.{2});
    defer logits.deinit();

    try metal.beginFrame(runtime);
    errdefer metal.cancelFrame(runtime) catch {};
    try std.testing.expectError(error.InvalidInkling2Token, endpoints.embed(runtime, 2, &embedded));
    try endpoints.embed(runtime, 1, &embedded);
    try endpoints.logits(runtime, null, &embedded, 1, &normalized, &logits);
    try metal.submitFrame(runtime);
    try metal.waitFrame(runtime);

    var bits: [2]u16 = undefined;
    try embedded.downloadBytesInto(std.mem.asBytes(&bits));
    try std.testing.expectEqual(norm, bits);
    var scores: [2]f32 = undefined;
    try logits.downloadF32Into(&scores);
    try std.testing.expectApproxEqAbs(@as(f32, 0.76159416), scores[0], 0.00001);
    try std.testing.expectApproxEqAbs(@as(f32, -0.76159416), scores[1], 0.00001);
}
