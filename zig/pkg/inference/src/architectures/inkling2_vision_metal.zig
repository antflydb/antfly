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

//! Resident BF16 vision encoder and spatial adapter over Metal tile operations.
const std = @import("std");
const schema = @import("inkling2_vision.zig");
const checkpoint = @import("inkling2_checkpoint.zig");
const metal = @import("../backends/metal_runtime.zig");
const tensors = @import("../backends/metal_tensor.zig");
const Tensor = tensors.MetalTensor;
const Runtime = metal.RawMetalDecodeRuntime;
const Program = metal.PatchTransformerBf16;
const Geometry = schema.Geometry;
const Weight = schema.Weight;
const LayerWeight = enum { ln1, bias1, ln2, bias2, scale1, scale2, out, out_bias, up, up_bias, down, down_bias };
const width = schema.width;
const inner = schema.inner;
const features = schema.patch_features;

const Temp = enum { patches, embedded, hidden, normalized, q, k, v, attention, update, wide, merged };

fn Buffers(comptime E: type) type {
    return std.EnumArray(E, Tensor);
}

fn workspaceCounts(geometry: Geometry) std.EnumArray(Temp, usize) {
    const patches: usize = geometry.patchRows();
    const rows = patches + 1;
    const narrow = rows * width;
    return .init(.{
        .patches = patches * features,
        .embedded = patches * width,
        .hidden = narrow,
        .normalized = narrow,
        .q = narrow,
        .k = narrow,
        .v = narrow,
        .attention = narrow,
        .update = narrow,
        .wide = rows * inner,
        .merged = geometry.mergedRows() * (width * 4),
    });
}

const WorkspacePlan = struct {
    elements: std.EnumArray(Temp, usize),
    offsets: std.EnumArray(Temp, usize) = .initFill(0),
    bytes: usize = 0,

    fn reserve(self: *WorkspacePlan, key: Temp, cursor: *usize) void {
        const offset = std.mem.alignForward(usize, cursor.*, 256);
        self.offsets.set(key, offset);
        cursor.* = offset + self.elements.get(key) * @sizeOf(u16);
        self.bytes = @max(self.bytes, cursor.*);
    }

    fn init(geometry: Geometry) WorkspacePlan {
        var plan: WorkspacePlan = .{ .elements = workspaceCounts(geometry) };
        var fixed: usize = 0;
        plan.reserve(.hidden, &fixed);
        plan.reserve(.normalized, &fixed);
        plan.offsets.set(.update, plan.offsets.get(.normalized));

        // These phases run in order in each frame. Keep their live operands
        // disjoint within a phase and reuse the same space between phases.
        var cursor = fixed;
        plan.reserve(.patches, &cursor);
        plan.reserve(.embedded, &cursor);
        cursor = fixed;
        for ([_]Temp{ .q, .k, .v, .attention }) |key| plan.reserve(key, &cursor);
        cursor = fixed;
        plan.reserve(.wide, &cursor);
        plan.reserve(.merged, &cursor);
        return plan;
    }
};

pub const AdmissionBytes = struct { host: usize = 0, device: usize = 0 };

pub fn admissionBytes(x: u32, y: u32) AdmissionBytes {
    const geometry = Geometry.fromDimensions(x, y);
    const rgb = @as(usize, x) * y * 3;
    const device = rgb + WorkspacePlan.init(geometry).bytes +
        @as(usize, geometry.mergedRows()) * schema.text_width * @sizeOf(u16);
    // RGBA decoding and the composited RGB image can be live together.
    return .{ .host = @as(usize, x) * y * 4 + rgb, .device = device };
}

fn allocate(a: std.mem.Allocator, runtime: *Runtime, count: usize, mode: tensors.StorageMode) !Tensor {
    return Tensor.deviceAllocateTyped(a, runtime, .i16, mode, &.{std.math.cast(i32, count) orelse return error.InvalidShape});
}

fn checkpointBytes(source: *const checkpoint.Checkpoint, name: []const u8, shape: []const i64) ![]const u8 {
    const entry = source.reader.header.tensors.get(name) orelse return error.MissingInkling2Weight;
    if (entry.dtype != .bf16 or entry.encoding != .dense or !std.mem.eql(i64, entry.shape, shape))
        return error.InvalidInkling2Weight;
    return source.reader.rawTensorBytes(name);
}

fn upload(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint, name: []const u8, shape: []const i64) !Tensor {
    const bytes = try checkpointBytes(source, name, shape);
    var result = try allocate(a, runtime, bytes.len / @sizeOf(u16), .shared);
    errdefer result.deinit();
    try result.uploadBytes(bytes);
    return result;
}

const EncoderLayer = struct {
    weights: Buffers(LayerWeight),
    qkv: Tensor,
    qkv_bias: Tensor,

    fn load(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint, index: usize) !EncoderLayer {
        var result: EncoderLayer = undefined;
        var loaded: usize = 0;
        errdefer for (result.weights.values[0..loaded]) |*weight| weight.deinit();
        var name_buf: [128]u8 = undefined;
        for (std.enums.values(LayerWeight), &result.weights.values) |key, *weight| {
            const definition = schema.layer_tensors.get(std.meta.stringToEnum(schema.Layer, @tagName(key)).?);
            const name = try std.fmt.bufPrint(&name_buf, "model.visual.backbone.layers.{d}.{s}", .{ index, definition.name });
            weight.* = try upload(a, runtime, source, name, definition.shape);
            loaded += 1;
        }
        result.qkv = try pack(a, runtime, source, index, .{ .q, .k, .v }, width * width);
        errdefer result.qkv.deinit();
        result.qkv_bias = try pack(a, runtime, source, index, .{ .q_bias, .k_bias, .v_bias }, width);
        return result;
    }

    fn pack(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint, index: usize, keys: [3]schema.Layer, elements: usize) !Tensor {
        var slab = try allocate(a, runtime, 3 * elements, .shared);
        errdefer slab.deinit();
        const bytes_per_plane = elements * @sizeOf(u16);
        var name_buf: [128]u8 = undefined;
        for (keys, 0..) |key, plane| {
            const definition = schema.layer_tensors.get(key);
            const name = try std.fmt.bufPrint(&name_buf, "model.visual.backbone.layers.{d}.{s}", .{ index, definition.name });
            const bytes = try checkpointBytes(source, name, definition.shape);
            var view = try slab.retainedView(plane * bytes_per_plane, bytes_per_plane, &.{@intCast(elements)});
            defer view.deinit();
            try view.uploadBytes(bytes);
        }
        return slab;
    }

    fn deinit(self: *EncoderLayer) void {
        self.qkv_bias.deinit();
        self.qkv.deinit();
        for (&self.weights.values) |*weight| weight.deinit();
    }
};

pub const Vision = struct {
    weights: Buffers(Weight),
    layers: [schema.layers]EncoderLayer,

    pub fn load(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint) !Vision {
        var vision: Vision = undefined;
        var name_buf: [128]u8 = undefined;
        var weights_loaded: usize = 0;
        errdefer for (vision.weights.values[0..weights_loaded]) |*weight| weight.deinit();
        for (&vision.weights.values, schema.model_tensors.values) |*weight, tensor| {
            const name = try std.fmt.bufPrint(&name_buf, "model.visual.{s}", .{tensor.name});
            weight.* = try upload(a, runtime, source, name, tensor.shape);
            weights_loaded += 1;
        }
        var layers_loaded: usize = 0;
        errdefer for (vision.layers[0..layers_loaded]) |*layer| layer.deinit();
        for (&vision.layers, 0..) |*layer, i| {
            layer.* = try EncoderLayer.load(a, runtime, source, i);
            layers_loaded += 1;
        }
        return vision;
    }

    pub fn deinit(self: *Vision) void {
        for (&self.layers) |*layer| layer.deinit();
        for (&self.weights.values) |*tensor| tensor.deinit();
    }

    pub fn residentBytes(self: *const Vision) usize {
        var bytes = tensorBytes(&self.weights.values);
        for (&self.layers) |*layer| {
            bytes += tensorBytes(&layer.weights.values);
            bytes += layer.qkv.allocatedDeviceBytes() orelse layer.qkv.deviceByteLen();
            bytes += layer.qkv_bias.allocatedDeviceBytes() orelse layer.qkv_bias.deviceByteLen();
        }
        return bytes;
    }

    fn tensorBytes(values: []const Tensor) usize {
        var bytes: usize = 0;
        for (values) |*tensor| bytes += tensor.allocatedDeviceBytes() orelse tensor.deviceByteLen();
        return bytes;
    }

    pub const Embeddings = struct {
        tensor: Tensor,
        rows: u32,
    };

    pub fn encodeRgb(self: *const Vision, a: std.mem.Allocator, runtime: *Runtime, rgb: []const u8, x: u32, y: u32) !Embeddings {
        return self.encodeRgbWithProjection(a, runtime, rgb, x, y, true);
    }

    fn encodeRgbWithProjection(self: *const Vision, a: std.mem.Allocator, runtime: *Runtime, rgb: []const u8, x: u32, y: u32, comptime grouped: bool) !Embeddings {
        const geometry = try Geometry.fromRgb(rgb, x, y);
        const patch_rows = geometry.patchRows();
        const rows = patch_rows + 1;
        const merged_rows = geometry.mergedRows();
        var rgb_buffer = try Tensor.deviceAllocateTyped(a, runtime, .u8, .shared, &.{@intCast(rgb.len)});
        defer rgb_buffer.deinit();
        try rgb_buffer.uploadBytes(rgb);
        const plan = WorkspacePlan.init(geometry);
        var arena = try allocate(a, runtime, plan.bytes / @sizeOf(u16), .private);
        defer arena.deinit();
        var scratch: Buffers(Temp) = undefined;
        var initialized: usize = 0;
        defer for (scratch.values[0..initialized]) |*tensor| tensor.deinit();
        for (&scratch.values, plan.elements.values, plan.offsets.values) |*tensor, elements, offset| {
            tensor.* = try arena.retainedTypedView(offset, &.{@intCast(elements)}, .i16);
            initialized += 1;
        }
        var output = try allocate(a, runtime, @as(usize, merged_rows) * schema.text_width, .private);
        errdefer output.deinit();
        const weights = &self.weights;
        const hidden = scratch.getPtr(.hidden);
        const normalized = scratch.getPtr(.normalized);
        const patches = scratch.getPtr(.patches);
        const embedded = scratch.getPtr(.embedded);
        const qkv_elements = 3 * plan.elements.get(.q);
        var qkv = try arena.retainedTypedView(plan.offsets.get(.q), &.{@intCast(qkv_elements)}, .i16);
        defer qkv.deinit();
        const projection: metal.DenseBf16Projection = .{ .rows = rows, .input_width = width, .output_width = 3 * width, .plane_width = width };
        const q = scratch.getPtr(.q);
        const k = scratch.getPtr(.k);
        const v = scratch.getPtr(.v);
        const attention = scratch.getPtr(.attention);
        const update = scratch.getPtr(.update);
        const wide = scratch.getPtr(.wide);
        const merged = scratch.getPtr(.merged);
        const params: Program.Params = .{ .rows = rows, .width = width, .gx = geometry.grid_x, .gy = geometry.grid_y, .epsilon = schema.eps };
        try metal.beginFrame(runtime);
        errdefer metal.cancelFrame(runtime) catch {};
        try Program.encode(.patches, runtime, .{
            .rows = patch_rows,
            .width = width,
            .gx = geometry.grid_x,
            .gy = geometry.grid_y,
            .image_x = x,
            .image_y = y,
        }, .{ patches, &rgb_buffer });
        try project(runtime, patch_rows, features, width, patches, weights.getPtrConst(.patch), null, embedded);
        try Program.encode(.positions, runtime, params, .{ normalized, embedded, weights.getPtrConst(.pos), weights.getPtrConst(.cls), weights.getPtrConst(.cls_pos) });
        try Program.encode(.norm, runtime, params, .{ hidden, normalized, weights.getPtrConst(.pre), weights.getPtrConst(.pre_bias) });
        for (&self.layers) |*layer| {
            try Program.encode(.norm, runtime, params, .{ normalized, hidden, layer.weights.getPtrConst(.ln1), layer.weights.getPtrConst(.bias1) });
            if (grouped) {
                try projection.encodeWithBias(runtime, normalized, &layer.qkv, &layer.qkv_bias, &qkv);
            } else {
                // The separate projections are the hardware fixture oracle.
                const weight_elements = width * width;
                const weight_bytes = weight_elements * @sizeOf(u16);
                const bias_bytes = width * @sizeOf(u16);
                for ([_]*Tensor{ q, k, v }, 0..) |destination, i| {
                    var weight = try layer.qkv.retainedView(i * weight_bytes, weight_bytes, &.{weight_elements});
                    defer weight.deinit();
                    var bias = try layer.qkv_bias.retainedView(i * bias_bytes, bias_bytes, &.{width});
                    defer bias.deinit();
                    try project(runtime, rows, width, width, normalized, &weight, &bias, destination);
                }
            }
            try Program.encode(.rotary, runtime, params, .{ q, k });
            try Program.encode(.attention, runtime, params, .{ attention, q, k, v });
            try project(runtime, rows, width, width, attention, layer.weights.getPtrConst(.out), layer.weights.getPtrConst(.out_bias), update);
            try Program.encode(.residual, runtime, params, .{ hidden, update, layer.weights.getPtrConst(.scale1) });
            try Program.encode(.norm, runtime, params, .{ normalized, hidden, layer.weights.getPtrConst(.ln2), layer.weights.getPtrConst(.bias2) });
            try project(runtime, rows, width, inner, normalized, layer.weights.getPtrConst(.up), layer.weights.getPtrConst(.up_bias), wide);
            try Program.encode(.gelu, runtime, .{ .rows = rows, .width = inner }, .{wide});
            try project(runtime, rows, inner, width, wide, layer.weights.getPtrConst(.down), layer.weights.getPtrConst(.down_bias), update);
            try Program.encode(.residual, runtime, params, .{ hidden, update, layer.weights.getPtrConst(.scale2) });
        }
        try Program.encode(.merge, runtime, .{ .rows = merged_rows, .width = width, .gx = geometry.grid_x, .gy = geometry.grid_y }, .{ merged, hidden });
        try project(runtime, merged_rows, width * 4, inner, merged, weights.getPtrConst(.in), null, wide);
        try Program.encode(.gelu, runtime, .{ .rows = merged_rows, .width = inner }, .{wide});
        try project(runtime, merged_rows, inner, schema.text_width, wide, weights.getPtrConst(.out), null, &output);
        try metal.submitFrame(runtime);
        try metal.waitFrame(runtime);
        return .{ .tensor = output, .rows = merged_rows };
    }
};

fn project(runtime: *Runtime, rows: u32, in: u32, out: u32, input: *const Tensor, weights: *const Tensor, bias: ?*const Tensor, output: *Tensor) !void {
    const projection: metal.DenseBf16Projection = .{ .rows = rows, .input_width = in, .output_width = out };
    try projection.encodeWithBias(runtime, input, weights, bias, output);
}

test "Inkling Metal vision publishes projected rows from its admitted arena" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;

    const Fixture = struct {
        fn view(backing: *const Tensor, shape: []const i64) !Tensor {
            var elements: i32 = 1;
            for (shape) |extent| elements *= @intCast(extent);
            return backing.retainedTypedView(0, &.{elements}, .i16);
        }
    };
    const elements = inner * width * 4;
    const data = try a.alloc(u16, elements);
    defer a.free(data);
    @memset(data, 0);
    var zeros = try allocate(a, runtime, elements, .shared);
    defer zeros.deinit();
    try zeros.uploadBytes(std.mem.sliceAsBytes(data));
    var marker = try allocate(a, runtime, elements, .shared);
    defer marker.deinit();
    data[0] = 0x3f80;
    try marker.uploadBytes(std.mem.sliceAsBytes(data));

    var vision: Vision = undefined;
    var loaded: usize = 0;
    defer for (vision.weights.values[0..loaded]) |*value| value.deinit();
    for (std.enums.values(Weight), &vision.weights.values) |key, *value| {
        const source = switch (key) {
            .pre_bias, .in, .out => &marker,
            else => &zeros,
        };
        value.* = try Fixture.view(source, schema.model_tensors.get(key).shape);
        loaded += 1;
    }
    var layers_loaded: usize = 0;
    defer for (vision.layers[0..layers_loaded]) |*layer| layer.deinit();
    for (&vision.layers) |*layer| {
        var count: usize = 0;
        errdefer for (layer.weights.values[0..count]) |*weight| weight.deinit();
        for (std.enums.values(LayerWeight), &layer.weights.values) |key, *value| {
            const definition = schema.layer_tensors.get(std.meta.stringToEnum(schema.Layer, @tagName(key)).?);
            value.* = try Fixture.view(&zeros, definition.shape);
            count += 1;
        }
        layer.qkv = try Fixture.view(&zeros, &.{ 3 * width, width });
        errdefer layer.qkv.deinit();
        layer.qkv_bias = try Fixture.view(&zeros, &.{3 * width});
        layers_loaded += 1;
    }

    const rgb: [40 * 40 * 3]u8 = @splat(64);
    const before = tensors.memoryStatsSnapshot();
    var counters = @import("../execution/runtime_observation.zig").Scope.init();
    defer counters.deinit();
    var output = try vision.encodeRgb(a, runtime, &rgb, 40, 40);
    defer output.tensor.deinit();
    // Four patch stages, eleven dispatches per layer with packed QKV,
    // and four stages to merge and project the language-model rows.
    try std.testing.expectEqual(@as(?u64, 8 + 11 * schema.layers), counters.snapshot().metal_encoded_dispatches);
    const after = tensors.memoryStatsSnapshot();
    // RGB ingress and transient arena retire after the frame. The returned
    // embedding has independent storage and does not retain either one.
    try std.testing.expectEqual(@as(u64, 3), after.device_owned_buffers_created - before.device_owned_buffers_created);
    try std.testing.expectEqual(@as(u32, 1), output.rows);
    try std.testing.expectEqual(@as(u64, schema.text_width * @sizeOf(u16)), after.device_owned_live_bytes - before.device_owned_live_bytes);
    var values: [schema.text_width]u16 = undefined;
    try output.tensor.downloadBytesInto(std.mem.asBytes(&values));
    try std.testing.expectEqual(@as(u16, 0x3f57), values[0]); // GELU(1), rounded to BF16.
    try std.testing.expectEqual(@as([schema.text_width - 1]u16, @splat(0)), values[1..].*);
}

test "Inkling Metal grouped vision matches separate projections on the released checkpoint" {
    if (!@import("build_options").enable_metal or !metal.metalDeviceAvailable()) return error.SkipZigTest;
    const dir = @import("antfly_platform").env.getenv("ANTFLY_INKLING2_METAL_MODEL_DIR") orelse return error.SkipZigTest;
    const a = std.testing.allocator;
    var source = try checkpoint.Checkpoint.open(a, dir);
    defer source.deinit();
    var provider = try @import("../backends/metal_native_provider.zig").MetalNativeProvider.create();
    defer provider.deinitOwned();
    const runtime = provider.raw_decode_runtime orelse return error.SkipZigTest;
    var vision = try Vision.load(a, runtime, &source);
    defer vision.deinit();

    // Check the released K slab directly against the checkpoint, independent
    // of the two projection paths' agreement on their shared packed storage.
    var k = try vision.layers[0].qkv.retainedView(width * width * 2, width * width * 2, &.{width * width});
    defer k.deinit();
    const reference = try checkpointBytes(&source, "model.visual.backbone.layers.0.self_attn.k_proj.weight", &.{ width, width });
    const unpacked = try a.alloc(u8, reference.len);
    defer a.free(unpacked);
    try k.downloadBytesInto(unpacked);
    try std.testing.expectEqualSlices(u8, reference, unpacked);

    const side = 80;
    var rgb: [side * side * 3]u8 = undefined;
    for (&rgb, 0..) |*value, i| value.* = @truncate(i * 179 + i / 17);
    var separate = try vision.encodeRgbWithProjection(a, runtime, &rgb, side, side, false);
    defer separate.tensor.deinit();
    var grouped = try vision.encodeRgb(a, runtime, &rgb, side, side);
    defer grouped.tensor.deinit();
    try std.testing.expectEqual(@as(u32, 4), grouped.rows);
    try std.testing.expectEqual(separate.rows, grouped.rows);
    var expected: [4 * schema.text_width]u16 = undefined;
    var actual: [expected.len]u16 = undefined;
    try separate.tensor.downloadBytesInto(std.mem.asBytes(&expected));
    try grouped.tensor.downloadBytesInto(std.mem.asBytes(&actual));
    try std.testing.expectEqualSlices(u16, &expected, &actual);
}
