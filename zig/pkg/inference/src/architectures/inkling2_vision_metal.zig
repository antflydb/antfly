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
const Layer = schema.Layer;
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

fn upload(a: std.mem.Allocator, runtime: *Runtime, source: *const checkpoint.Checkpoint, name: []const u8, shape: []const i64) !Tensor {
    const entry = source.reader.header.tensors.get(name) orelse return error.MissingInkling2Weight;
    if (entry.dtype != .bf16 or entry.encoding != .dense or !std.mem.eql(i64, entry.shape, shape))
        return error.InvalidInkling2Weight;
    const bytes = try source.reader.rawTensorBytes(name);
    var result = try allocate(a, runtime, bytes.len / @sizeOf(u16), .shared);
    errdefer result.deinit();
    try result.uploadBytes(bytes);
    return result;
}

pub const Vision = struct {
    weights: Buffers(Weight),
    layers: [schema.layers]Buffers(Layer),

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
        errdefer for (vision.layers[0..layers_loaded]) |*layer| {
            for (&layer.values) |*tensor| tensor.deinit();
        };
        for (&vision.layers, 0..) |*layer, i| {
            var loaded: usize = 0;
            errdefer for (layer.values[0..loaded]) |*weight| weight.deinit();
            for (&layer.values, schema.layer_tensors.values) |*weight, tensor| {
                const name = try std.fmt.bufPrint(&name_buf, "model.visual.backbone.layers.{d}.{s}", .{ i, tensor.name });
                weight.* = try upload(a, runtime, source, name, tensor.shape);
                loaded += 1;
            }
            layers_loaded += 1;
        }
        return vision;
    }

    pub fn deinit(self: *Vision) void {
        for (&self.layers) |*layer| for (&layer.values) |*tensor| tensor.deinit();
        for (&self.weights.values) |*tensor| tensor.deinit();
    }

    pub fn residentBytes(self: *const Vision) usize {
        var bytes = tensorBytes(&self.weights.values);
        for (&self.layers) |*layer| bytes += tensorBytes(&layer.values);
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
            try Program.encode(.norm, runtime, params, .{ normalized, hidden, layer.getPtrConst(.ln1), layer.getPtrConst(.bias1) });
            for ([_]*Tensor{ q, k, v }, [_]Layer{ .q, .k, .v }, [_]Layer{ .q_bias, .k_bias, .v_bias }) |destination, weight, bias|
                try project(runtime, rows, width, width, normalized, layer.getPtrConst(weight), layer.getPtrConst(bias), destination);
            try Program.encode(.rotary, runtime, params, .{ q, k });
            try Program.encode(.attention, runtime, params, .{ attention, q, k, v });
            try project(runtime, rows, width, width, attention, layer.getPtrConst(.out), layer.getPtrConst(.out_bias), update);
            try Program.encode(.residual, runtime, params, .{ hidden, update, layer.getPtrConst(.scale1) });
            try Program.encode(.norm, runtime, params, .{ normalized, hidden, layer.getPtrConst(.ln2), layer.getPtrConst(.bias2) });
            try project(runtime, rows, width, inner, normalized, layer.getPtrConst(.up), layer.getPtrConst(.up_bias), wide);
            try Program.encode(.gelu, runtime, .{ .rows = rows, .width = inner }, .{wide});
            try project(runtime, rows, inner, width, wide, layer.getPtrConst(.down), layer.getPtrConst(.down_bias), update);
            try Program.encode(.residual, runtime, params, .{ hidden, update, layer.getPtrConst(.scale2) });
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
    defer for (vision.layers[0..layers_loaded]) |*layer| for (&layer.values) |*weight| weight.deinit();
    for (&vision.layers) |*layer| {
        var count: usize = 0;
        errdefer for (layer.values[0..count]) |*weight| weight.deinit();
        for (&layer.values, schema.layer_tensors.values) |*value, definition| {
            value.* = try Fixture.view(&zeros, definition.shape);
            count += 1;
        }
        layers_loaded += 1;
    }

    const rgb: [40 * 40 * 3]u8 = @splat(64);
    const before = tensors.memoryStatsSnapshot();
    var output = try vision.encodeRgb(a, runtime, &rgb, 40, 40);
    defer output.tensor.deinit();
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
