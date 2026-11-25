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

//! Checkpoint adapter for the Metal packed expert stage.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const metal = @import("../backends/metal_runtime.zig");
const tensor = @import("../backends/metal_tensor.zig");
const Tensor = tensor.MetalTensor;
const Ffn = metal.RoutedBlockScaledFfn;

fn upload(allocator: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, dtype: tensor.DType, bytes: []const u8) !Tensor {
    if (bytes.len == 0 or bytes.len % dtype.byteSize() != 0) return error.InvalidShape;
    const elements = std.math.cast(i32, bytes.len / dtype.byteSize()) orelse return error.InvalidShape;
    var result = try Tensor.deviceAllocateTyped(allocator, runtime, dtype, .shared, &.{elements});
    errdefer result.deinit();
    try result.uploadBytes(bytes);
    return result;
}

const PackedWeights = struct {
    values: Tensor,
    scales: Tensor,
    amax: ?Tensor,
    denominator: ?Tensor,

    fn load(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: checkpoint.ExpertWeights) !PackedWeights {
        if (source.format == .nvfp4 and (source.outer_amax == null or source.outer_denominator == null))
            return error.InvalidShape;
        var values = try upload(a, runtime, .u8, source.weights);
        errdefer values.deinit();
        var scales = try upload(a, runtime, .u8, source.block_scales);
        errdefer scales.deinit();
        var amax = if (source.outer_amax) |bytes| try upload(a, runtime, .f32, bytes) else null;
        errdefer if (amax) |*value| value.deinit();
        const denominator = if (source.outer_denominator) |bytes| try upload(a, runtime, .f32, bytes) else null;
        return .{ .values = values, .scales = scales, .amax = amax, .denominator = denominator };
    }

    fn deinit(self: *PackedWeights) void {
        if (self.denominator) |*value| value.deinit();
        if (self.amax) |*value| value.deinit();
        self.scales.deinit();
        self.values.deinit();
    }

    fn residentBytes(self: *const PackedWeights) usize {
        var bytes: usize = 0;
        for ([_]?*const Tensor{ &self.values, &self.scales, if (self.amax) |*value| value else null, if (self.denominator) |*value| value else null }) |entry| {
            if (entry) |value| bytes += value.allocatedDeviceBytes() orelse value.deviceByteLen();
        }
        return bytes;
    }

    fn bind(self: *const PackedWeights) Ffn.Weights {
        return .{
            .values = &self.values,
            .scales = &self.scales,
            .amax = if (self.amax) |*value| value else null,
            .denominator = if (self.denominator) |*value| value else null,
        };
    }
};

pub const Layer = struct {
    router_weight: Tensor,
    router_bias: Tensor,
    gate_up: PackedWeights,
    down: PackedWeights,
    format: checkpoint.ExpertFormat,

    pub fn load(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: *const checkpoint.Checkpoint, layer: usize) !Layer {
        return fromWeights(a, runtime, try source.router(layer), try source.experts(layer, .w13), try source.experts(layer, .w2));
    }

    pub fn fromWeights(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, router: checkpoint.RouterWeights, gate: checkpoint.ExpertWeights, down: checkpoint.ExpertWeights) !Layer {
        if (gate.format != down.format) return error.InvalidShape;
        var router_weight = try upload(a, runtime, .i16, router.weight);
        errdefer router_weight.deinit();
        var router_bias = try upload(a, runtime, .f32, router.bias);
        errdefer router_bias.deinit();
        var gate_up = try PackedWeights.load(a, runtime, gate);
        errdefer gate_up.deinit();
        return .{
            .router_weight = router_weight,
            .router_bias = router_bias,
            .gate_up = gate_up,
            .down = try PackedWeights.load(a, runtime, down),
            .format = gate.format,
        };
    }

    pub fn deinit(self: *Layer) void {
        self.down.deinit();
        self.gate_up.deinit();
        self.router_bias.deinit();
        self.router_weight.deinit();
    }

    pub fn residentBytes(self: *const Layer) usize {
        return self.gate_up.residentBytes() + self.down.residentBytes() +
            (self.router_weight.allocatedDeviceBytes() orelse self.router_weight.deviceByteLen()) +
            (self.router_bias.allocatedDeviceBytes() orelse self.router_bias.deviceByteLen());
    }

    pub fn geometry(self: *const Layer, text: checkpoint.TextConfig, rows: u32) Ffn {
        return .{
            .rows = rows,
            .hidden = text.hidden_size,
            .intermediate = text.intermediate_size,
            .experts = text.n_routed_experts,
            .top_k = text.num_experts_per_tok,
            .format = self.format,
            .route_scale = text.route_scale,
            .gate_cap = text.mlp_gate_softcap,
            .up_cap = text.mlp_up_softcap,
        };
    }

    pub fn encode(self: *const Layer, runtime: *metal.RawMetalDecodeRuntime, text: checkpoint.TextConfig, rows: u32, input: *const Tensor, logits: *Tensor, scratch: Ffn.Workspace, output: *Tensor) !void {
        try self.geometry(text, rows).encodeWithRouter(runtime, input, .{
            .weight = &self.router_weight,
            .bias = &self.router_bias,
            .logits = logits,
        }, self.gate_up.bind(), self.down.bind(), scratch, output);
    }
};

test "Inkling Metal checkpoint adapter keeps packed weights and scales resident" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const router_weight = [2]u16{ 0x3f80, 0xbf80 };
    const router_bias = [2]f32{ 0.25, -0.5 };
    const router: checkpoint.RouterWeights = .{
        .weight = std.mem.asBytes(&router_weight),
        .bias = std.mem.asBytes(&router_bias),
    };
    const values = [_]u8{ 0x22, 0x38 };
    const scales = [_]u8{ 0x38, 0x40 };
    const maxima = [2]f32{ 1, 2 };
    const denominator = [1]f32{4};
    for ([_]checkpoint.ExpertFormat{ .nvfp4, .mxfp8 }) |format| {
        const source: checkpoint.ExpertWeights = .{
            .format = format,
            .weights = &values,
            .block_scales = &scales,
            .outer_amax = if (format == .nvfp4) std.mem.asBytes(&maxima) else null,
            .outer_denominator = if (format == .nvfp4) std.mem.asBytes(&denominator) else null,
        };
        var layer = try Layer.fromWeights(std.testing.allocator, runtime, router, source, source);
        defer layer.deinit();
        const binding = layer.gate_up.bind();
        var copied: [2]u8 = undefined;
        try binding.values.downloadBytesInto(&copied);
        try std.testing.expectEqual(values, copied);
        try binding.scales.downloadBytesInto(&copied);
        try std.testing.expectEqual(scales, copied);
        try std.testing.expectEqual(format == .nvfp4, binding.amax != null);
        try std.testing.expectEqual(format == .nvfp4, binding.denominator != null);
        if (binding.amax) |amax| {
            var actual: [2]f32 = undefined;
            try amax.downloadF32Into(&actual);
            try std.testing.expectEqual(maxima, actual);
        }
    }
}
