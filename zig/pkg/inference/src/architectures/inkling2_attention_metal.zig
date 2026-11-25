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

//! Resident Metal attention weights in the shared Inkling checkpoint layout.
const std = @import("std");
const checkpoint = @import("inkling2_checkpoint.zig");
const layout = @import("inkling2_attention_weights.zig");
const metal = @import("../backends/metal_runtime.zig");
const Tensor = @import("../backends/metal_tensor.zig").MetalTensor;
const Weight = layout.Weight;

pub const Layer = struct {
    local: bool,
    projections: Tensor,
    weights: [@typeInfo(Weight).@"enum".field_names.len]?Tensor = @splat(null),

    pub fn load(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: *const checkpoint.Checkpoint, layer: usize) !Layer {
        return loadTensors(a, runtime, source, source.config.value.text_config, layer);
    }

    fn loadTensors(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, source: anytype, text: checkpoint.TextConfig, layer: usize) !Layer {
        const local = try layout.isLocal(text, layer);
        const hidden: usize = text.hidden_size;
        const elements = try std.math.mul(usize, hidden, layout.projectionSize(text, &layout.projection_weights));
        var result: Layer = .{ .local = local, .projections = try allocate(a, runtime, elements) };
        errdefer result.deinit();
        var offset: usize = 0;
        for (layout.projection_weights) |key| {
            const count = hidden * layout.projectionWidth(text, key);
            const bytes = count * 2;
            var destination = try result.projections.retainedView(offset, bytes, &.{@intCast(count)});
            errdefer destination.deinit();
            try destination.uploadBytes(try source.layerTensor(layer, layout.tensorName(key, local)));
            result.weights[@backingInt(key)] = destination;
            offset += bytes;
        }
        for ([_]Weight{ .out, .q_scale, .r_scale, .k_taps, .v_taps, .k_dynamic, .v_dynamic }) |key| {
            if (local and (key == .k_dynamic or key == .v_dynamic)) continue;
            result.weights[@backingInt(key)] = try upload(a, runtime, try source.layerTensor(layer, layout.tensorName(key, local)));
        }
        const basis = try source.layerTensor(layer, layout.tensorName(.relative_basis, local));
        const transposed = try layout.transposeRelative(a, basis, text.d_rel, text.rel_extent);
        defer a.free(transposed);
        result.weights[@backingInt(Weight.relative_basis)] = try upload(a, runtime, transposed);
        return result;
    }

    pub fn deinit(self: *Layer) void {
        for (&self.weights) |*entry| {
            if (entry.*) |*weight| weight.deinit();
        }
        self.projections.deinit();
    }

    pub fn get(self: *const Layer, key: Weight) ?*const Tensor {
        return if (self.weights[@backingInt(key)]) |*weight| weight else null;
    }
};

fn allocate(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, elements: usize) !Tensor {
    if (elements == 0) return error.InvalidShape;
    const extent = std.math.cast(i32, elements) orelse return error.InvalidShape;
    return Tensor.deviceAllocateTyped(a, runtime, .i16, .shared, &.{extent});
}

fn upload(a: std.mem.Allocator, runtime: *metal.RawMetalDecodeRuntime, bytes: []const u8) !Tensor {
    if (bytes.len % 2 != 0) return error.InvalidShape;
    var result = try allocate(a, runtime, bytes.len / 2);
    errdefer result.deinit();
    try result.uploadBytes(bytes);
    return result;
}

test "Inkling Metal attention weights preserve packed offsets and local/global layout" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    const Fixture = struct {
        const data_a = [_]u8{ 0x80, 0x3f };
        const data_b = [_]u8{ 0x00, 0x40 };

        fn layerTensor(_: @This(), layer: usize, name: []const u8) ![]const u8 {
            if (std.mem.eql(u8, name, "attn.k_sconv.weight_proj")) {
                if (layer == 0) return error.UnexpectedDynamicWeight;
                return &data_b;
            }
            return if (std.mem.eql(u8, name, "attn.wq_du.weight")) &data_b else &data_a;
        }
    };
    var text: checkpoint.TextConfig = undefined;
    text.hidden_size = 1;
    text.num_attention_heads = 1;
    text.num_key_value_heads = 1;
    text.head_dim = 1;
    text.d_rel = 1;
    text.rel_extent = 1;
    @memset(&text.local_layer_ids, 0);
    for ([_]usize{ 0, 1 }) |index| {
        var layer = try Layer.loadTensors(std.testing.allocator, runtime, Fixture{}, text, index);
        defer layer.deinit();
        const q = layer.get(.q) orelse return error.MissingWeight;
        const k = layer.get(.k) orelse return error.MissingWeight;
        try std.testing.expectEqual(q.deviceHandle(), k.deviceHandle());
        try std.testing.expectEqual(@as(usize, 8), k.deviceByteOffset() - q.deviceByteOffset());
        var bytes: [2]u8 = undefined;
        try q.downloadBytesInto(&bytes);
        try std.testing.expectEqual(Fixture.data_b, bytes);
        try k.downloadBytesInto(&bytes);
        try std.testing.expectEqual(Fixture.data_a, bytes);
        try std.testing.expectEqual(index == 0, layer.local);
        try std.testing.expectEqual(index == 1, layer.get(.k_dynamic) != null);
    }
}
