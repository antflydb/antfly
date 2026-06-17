// Copyright 2026 Antfly, Inc.
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
const ops = @import("../ops.zig");
const tensor_mod = @import("../../backends/tensor.zig");
const buffer_mod = @import("buffer.zig");
const context_mod = @import("context.zig");
const dense_lt_mod = @import("dense_lt.zig");
const kernels_mod = @import("kernels.zig");
const libraries_mod = @import("libraries.zig");
const scratch_mod = @import("scratch.zig");
const weight_source_mod = @import("../../models/weight_source.zig");
const gguf_tensor_types = @import("../../gguf/tensor_types.zig");
const quant_codec = @import("../../gguf/quant_codec.zig");
const platform = @import("antfly_platform");

const CT = ops.CT;

pub const CudaTensor = struct {
    buffer: buffer_mod.DeviceBuffer,
    dtype: tensor_mod.DType,
    shape: []i64,
    elem_count: usize,
    quant_type: ?gguf_tensor_types.TensorType = null,
    owns_buffer: bool = true,
    owns_shape: bool = true,
    owned_by_tensor: bool = true,
};

pub const CapabilityProfile = enum {
    clipclap,
    deberta_reranker,
    gliner2,
};

pub const CudaCompute = struct {
    allocator: std.mem.Allocator,
    ctx: context_mod.CudaContext,
    kernels: kernels_mod.KernelModule,
    libraries: libraries_mod.CudaLibraries = .{},
    dense_lt: dense_lt_mod.DenseLt = .{},
    resident_weights: std.StringHashMapUnmanaged(CudaTensor) = .{},
    temp_buffers: std.ArrayListUnmanaged(buffer_mod.DeviceBuffer) = .empty,
    temp_ids_masks: scratch_mod.DeviceScratch = .{},
    owned_by_backend: bool = false,

    pub fn init(allocator: std.mem.Allocator) !CudaCompute {
        var ctx = try context_mod.CudaContext.initDefault();
        errdefer ctx.deinit();
        var kernels = try kernels_mod.KernelModule.load(&ctx);
        errdefer kernels.unload(&ctx);
        var libraries = try libraries_mod.CudaLibraries.init();
        errdefer libraries.deinit();
        var dense_lt = try dense_lt_mod.DenseLt.init(allocator, &ctx, &libraries);
        errdefer dense_lt.deinit(&ctx);
        return .{
            .allocator = allocator,
            .ctx = ctx,
            .kernels = kernels,
            .libraries = libraries,
            .dense_lt = dense_lt,
        };
    }

    pub fn create(allocator: std.mem.Allocator) !*CudaCompute {
        const self = try allocator.create(CudaCompute);
        errdefer allocator.destroy(self);
        self.* = try CudaCompute.init(allocator);
        self.owned_by_backend = true;
        return self;
    }

    pub fn deinit(self: *CudaCompute) void {
        var it = self.resident_weights.iterator();
        while (it.next()) |entry| {
            var tensor = entry.value_ptr.*;
            tensor.owns_buffer = true;
            tensor.owns_shape = true;
            freeCudaTensorStorage(self, &tensor);
            self.allocator.free(entry.key_ptr.*);
        }
        self.resident_weights.deinit(self.allocator);
        for (self.temp_buffers.items) |*buffer| buffer.free(&self.ctx);
        self.temp_buffers.deinit(self.allocator);
        self.temp_ids_masks.deinit(&self.ctx);
        self.dense_lt.deinit(&self.ctx);
        self.libraries.deinit();
        self.kernels.unload(&self.ctx);
        self.ctx.deinit();
    }

    pub fn computeBackend(self: *CudaCompute) ops.ComputeBackend {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn supportsProfile(self: *const CudaCompute, profile: CapabilityProfile) bool {
        return switch (profile) {
            .clipclap => self.kernels.hasClipClapPrimitives(),
            .deberta_reranker => self.kernels.hasDebertaRerankerPrimitives(),
            .gliner2 => self.kernels.hasGliner2Primitives(),
        };
    }

    pub fn requireProfile(self: *const CudaCompute, profile: CapabilityProfile) !void {
        if (!self.supportsProfile(profile)) return error.CudaKernelUnavailable;
    }

    pub fn hasCublas(self: *const CudaCompute) bool {
        return self.libraries.hasCublas();
    }

    pub fn hasCublasLt(self: *const CudaCompute) bool {
        return self.libraries.hasCublasLt();
    }

    pub fn hasDenseLibraryAcceleration(self: *const CudaCompute) bool {
        return self.dense_lt.enabled();
    }

    pub fn denseLtStats(self: *const CudaCompute) dense_lt_mod.Stats {
        return self.dense_lt.stats();
    }

    pub fn smokeDenseLt(self: *CudaCompute, allocator: std.mem.Allocator) !void {
        if (!self.dense_lt.enabled()) return error.CudaOpUnsupported;
        try smokeDenseLtDType(self, allocator, .f16);
        try smokeDenseLtDType(self, allocator, .bf16);
    }

    pub fn insertWeightFromLoaded(self: *CudaCompute, owned_key: []const u8, loaded: *const weight_source_mod.LoadedWeight) !void {
        if (loaded.quantized_storage) |storage| {
            if (cudaDequantizeQuantWeightOnUpload(owned_key)) {
                const elem_count = try elementCountFromShape(storage.shape);
                const data = try self.allocator.alloc(f32, elem_count);
                defer self.allocator.free(data);
                try quant_codec.dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, data);

                const shape = try self.allocator.dupe(i64, storage.shape);
                errdefer self.allocator.free(shape);
                var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
                errdefer device.free(&self.ctx);
                try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
                try self.ctx.synchronize();
                errdefer self.allocator.free(owned_key);
                try self.resident_weights.put(self.allocator, owned_key, .{
                    .buffer = device,
                    .dtype = .f32,
                    .shape = shape,
                    .elem_count = data.len,
                    .quant_type = null,
                    .owns_buffer = false,
                    .owns_shape = false,
                    .owned_by_tensor = false,
                });
                return;
            }
            const elem_count = try elementCountFromShape(storage.shape);
            const shape = try self.allocator.dupe(i64, storage.shape);
            errdefer self.allocator.free(shape);
            var device = try allocDeviceBuffer(self, storage.raw_bytes.len);
            errdefer device.free(&self.ctx);
            try device.copyFromHost(&self.ctx, storage.raw_bytes);
            try self.ctx.synchronize();
            errdefer self.allocator.free(owned_key);
            try self.resident_weights.put(self.allocator, owned_key, .{
                .buffer = device,
                .dtype = .u8,
                .shape = shape,
                .elem_count = elem_count,
                .quant_type = storage.tensor_type,
                .owns_buffer = false,
                .owns_shape = false,
                .owned_by_tensor = false,
            });
            return;
        }
        if (loaded.quantized) return error.UnsupportedTensorType;
        if (loaded.tensor.dtype != .f32) {
            if (shouldPreserveDense16Weight(owned_key, &loaded.tensor)) {
                return self.insertWeightFromTensor(owned_key, &loaded.tensor);
            }
            var converted = try weight_source_mod.convertToF32(self.allocator, &loaded.tensor);
            defer converted.deinit();
            return self.insertWeightFromTensor(owned_key, &converted);
        }
        try self.insertWeightFromTensor(owned_key, &loaded.tensor);
    }

    pub fn insertWeightFromTensor(self: *CudaCompute, owned_key: []const u8, tensor: *const tensor_mod.Tensor) !void {
        if (tensor.dtype == .f32) {
            const data = tensor.asFloat32();
            const shape = try self.allocator.dupe(i64, tensor.shape);
            errdefer self.allocator.free(shape);
            var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
            errdefer device.free(&self.ctx);
            try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
            try self.ctx.synchronize();
            errdefer self.allocator.free(owned_key);
            try self.resident_weights.put(self.allocator, owned_key, .{
                .buffer = device,
                .dtype = .f32,
                .shape = shape,
                .elem_count = data.len,
                .quant_type = null,
                .owns_buffer = false,
                .owns_shape = false,
                .owned_by_tensor = false,
            });
            return;
        }

        if (!shouldPreserveDense16Weight(owned_key, tensor)) return error.UnsupportedTensorType;
        const elem_count = try elementCountFromShape(tensor.shape);
        const shape = try self.allocator.dupe(i64, tensor.shape);
        errdefer self.allocator.free(shape);
        var device = try allocDeviceBuffer(self, tensor.data.len);
        errdefer device.free(&self.ctx);
        try device.copyFromHost(&self.ctx, tensor.data);
        try self.ctx.synchronize();
        errdefer self.allocator.free(owned_key);
        try self.resident_weights.put(self.allocator, owned_key, .{
            .buffer = device,
            .dtype = tensor.dtype,
            .shape = shape,
            .elem_count = elem_count,
            .quant_type = null,
            .owns_buffer = false,
            .owns_shape = false,
            .owned_by_tensor = false,
        });
    }
};

fn cudaDequantizeQuantWeightsOnUpload() bool {
    return platform.env.getenvBoolDefault("TERMITE_CUDA_DEQUANTIZE_QUANT_WEIGHTS", false);
}

fn isGlinerSpanWeightName(name: []const u8) bool {
    return std.mem.startsWith(u8, name, "span_rep.span_rep_layer.") and std.mem.endsWith(u8, name, ".weight");
}

fn cudaDequantizeQuantWeightOnUpload(name: []const u8) bool {
    if (cudaDequantizeQuantWeightsOnUpload()) return true;
    if (!isGlinerSpanWeightName(name)) return false;
    if (platform.env.getenvBool("TERMITE_CUDA_DISABLE_GLINER_SPAN_F32_UPLOAD")) return false;
    return !cudaGlinerSpanQ4KernelsEnabled();
}

fn cudaDense16WeightsEnabled() bool {
    return !platform.env.getenvBool("TERMITE_CUDA_DISABLE_DENSE16_WEIGHTS");
}

fn isDense16DType(dtype: tensor_mod.DType) bool {
    return dtype == .f16 or dtype == .bf16;
}

fn shouldPreserveDense16Weight(name: []const u8, tensor: *const tensor_mod.Tensor) bool {
    if (!cudaDense16WeightsEnabled()) return false;
    if (!isDense16DType(tensor.dtype)) return false;
    if (!std.mem.endsWith(u8, name, ".weight")) return false;
    if (tensor.shape.len != 2) return false;
    if (tensor.shape[0] <= 1 or tensor.shape[1] < 64) return false;
    const elem_count = elementCountFromShape(tensor.shape) catch return false;
    const expected_bytes = checkedMul(elem_count, tensor.dtype.byteSize()) catch return false;
    return tensor.data.len == expected_bytes;
}

fn cudaGlinerSpanQ4KernelsEnabled() bool {
    if (platform.env.getenvBool("TERMITE_CUDA_DISABLE_GLINER_SPAN_Q4_KERNELS")) return false;
    return platform.env.getenvBoolDefault("TERMITE_CUDA_ENABLE_GLINER_SPAN_Q4_KERNELS", true);
}

fn isGlinerSpanQ4Shape(rows: usize, in_dim: usize, out_dim: usize) bool {
    if (rows < 2) return false;
    if (in_dim == 768 and out_dim == 3072) return true;
    if (in_dim == 3072 and out_dim == 768) return true;
    if (in_dim == 1536 and out_dim == 3072) return true;
    return false;
}

fn useGlinerSpanQ4Kernel(self: *const CudaCompute, rows: usize, in_dim: usize, out_dim: usize) bool {
    return cudaGlinerSpanQ4KernelsEnabled() and
        self.kernels.hasGlinerSpanQ4KPrimitives() and
        isGlinerSpanQ4Shape(rows, in_dim, out_dim);
}

fn smokeDenseLtDType(self: *CudaCompute, allocator: std.mem.Allocator, dtype: tensor_mod.DType) !void {
    const rows: usize = 64;
    const in_dim: usize = 128;
    const out_dim: usize = 128;
    const input_count = try checkedMul(rows, in_dim);
    const weight_count = try checkedMul(out_dim, in_dim);
    const out_count = try checkedMul(rows, out_dim);

    const input_data = try allocator.alloc(f32, input_count);
    defer allocator.free(input_data);
    const weight_bits = try allocator.alloc(u16, weight_count);
    defer allocator.free(weight_bits);
    const bias_data = try allocator.alloc(f32, out_dim);
    defer allocator.free(bias_data);
    const expected = try allocator.alloc(f32, out_count);
    defer allocator.free(expected);

    for (input_data, 0..) |*value, i| value.* = smokeDenseLtValue(i, 23, 11, 16.0);
    for (weight_bits, 0..) |*value, i| value.* = dense16BitsRounded(dtype, smokeDenseLtValue(i, 17, 8, 32.0));
    for (bias_data, 0..) |*value, i| value.* = smokeDenseLtValue(i, 11, 5, 10.0);

    for (0..rows) |row| {
        for (0..out_dim) |col| {
            var acc = bias_data[col];
            for (0..in_dim) |k| {
                const x = dense16ValueRounded(dtype, input_data[row * in_dim + k]);
                const w = dense16BitsToF32(dtype, weight_bits[col * in_dim + k]);
                acc += x * w;
            }
            expected[row * out_dim + col] = acc;
        }
    }

    var input = try allocDeviceBuffer(self, input_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &input);
    var weight = try allocDeviceBuffer(self, weight_count * @sizeOf(u16));
    defer releaseDeviceBuffer(self, &weight);
    var bias = try allocDeviceBuffer(self, out_dim * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &bias);
    var output = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    defer releaseDeviceBuffer(self, &output);

    try input.copyFromHost(&self.ctx, std.mem.sliceAsBytes(input_data));
    try weight.copyFromHost(&self.ctx, std.mem.sliceAsBytes(weight_bits));
    try bias.copyFromHost(&self.ctx, std.mem.sliceAsBytes(bias_data));
    const ran = try tryDenseLtLinear16(self, output, input, weight, bias, rows, in_dim, out_dim, dtype);
    if (!ran) return error.CublasLtSmokeFallback;
    try self.ctx.synchronize();

    const out = try allocator.alloc(f32, out_count);
    defer allocator.free(out);
    try output.copyToHost(&self.ctx, std.mem.sliceAsBytes(out));
    try self.ctx.synchronize();

    const tolerance: f32 = if (dtype == .bf16) 0.12 else 0.025;
    for (out, expected) |actual, want| {
        if (@abs(actual - want) > tolerance) return error.CudaSmokeMismatch;
    }
}

fn smokeDenseLtValue(index: usize, modulo: usize, offset: i32, denom: f32) f32 {
    const centered = @as(i32, @intCast(index % modulo)) - offset;
    return @as(f32, @floatFromInt(centered)) / denom;
}

fn dense16BitsRounded(dtype: tensor_mod.DType, value: f32) u16 {
    return switch (dtype) {
        .f16 => blk: {
            const half: f16 = @floatCast(value);
            break :blk @bitCast(half);
        },
        .bf16 => blk: {
            const bits: u32 = @bitCast(value);
            const lsb = (bits >> 16) & 1;
            break :blk @intCast((bits + 0x7fff + lsb) >> 16);
        },
        else => 0,
    };
}

fn dense16ValueRounded(dtype: tensor_mod.DType, value: f32) f32 {
    return dense16BitsToF32(dtype, dense16BitsRounded(dtype, value));
}

fn dense16BitsToF32(dtype: tensor_mod.DType, bits: u16) f32 {
    return switch (dtype) {
        .f16 => blk: {
            const half: f16 = @bitCast(bits);
            break :blk @floatCast(half);
        },
        .bf16 => @bitCast(@as(u32, bits) << 16),
        else => 0.0,
    };
}

fn tensorFromCt(tensor: CT) *CudaTensor {
    return @ptrCast(@alignCast(tensor));
}

fn unsupportedCt() anyerror!CT {
    return error.CudaOpUnsupported;
}

fn backendKind(_: *anyopaque) ops.BackendKind {
    return .cuda;
}

fn deinitBackend(ctx: *anyopaque) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const owned = self.owned_by_backend;
    if (owned) {
        self.deinit();
        self.allocator.destroy(self);
    }
}

fn freeCudaTensorStorage(self: *CudaCompute, cuda_tensor: *CudaTensor) void {
    if (cuda_tensor.owns_buffer) releaseDeviceBuffer(self, &cuda_tensor.buffer);
    if (cuda_tensor.owns_shape) self.allocator.free(cuda_tensor.shape);
}

const max_temp_buffers = 256;

fn allocDeviceBuffer(self: *CudaCompute, len: usize) !buffer_mod.DeviceBuffer {
    if (len == 0) return .{};
    var best_index: ?usize = null;
    var best_len: usize = std.math.maxInt(usize);
    for (self.temp_buffers.items, 0..) |buffer, i| {
        if (buffer.len >= len and buffer.len < best_len) {
            best_index = i;
            best_len = buffer.len;
        }
    }
    if (best_index) |i| {
        const buffer = self.temp_buffers.swapRemove(i);
        return buffer;
    }
    return buffer_mod.DeviceBuffer.alloc(&self.ctx, len);
}

fn releaseDeviceBuffer(self: *CudaCompute, buffer: *buffer_mod.DeviceBuffer) void {
    if (buffer.ptr == 0) return;
    if (self.temp_buffers.items.len < max_temp_buffers) {
        self.temp_buffers.append(self.allocator, buffer.*) catch {
            buffer.free(&self.ctx);
            return;
        };
        buffer.* = .{};
        return;
    }
    buffer.free(&self.ctx);
}

fn freeTensor(ctx: *anyopaque, tensor: CT) void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (!cuda_tensor.owned_by_tensor) return;
    freeCudaTensorStorage(self, cuda_tensor);
    self.allocator.destroy(cuda_tensor);
}

fn getWeight(ctx: *anyopaque, name: []const u8) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    return self.resident_weights.getPtr(name) orelse error.WeightNotFound;
}

fn prefetchWeightHint(_: *anyopaque, _: []const u8, _: u32) void {}
fn drainPrefetchBudget(_: *anyopaque, _: usize) void {}

fn fromFloat32Op(ctx: *anyopaque, data: []const f32) anyerror!CT {
    var shape = [_]i32{@intCast(data.len)};
    return fromFloat32ShapeOp(ctx, data, &shape);
}

fn fromFloat32ShapeOp(ctx: *anyopaque, data: []const f32, shape: []const i32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    var elem_count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        elem_count = try std.math.mul(usize, elem_count, @intCast(dim));
    }
    if (elem_count != data.len) return error.InvalidShape;

    const shape_i64 = try self.allocator.alloc(i64, shape.len);
    errdefer self.allocator.free(shape_i64);
    for (shape, 0..) |dim, i| shape_i64[i] = dim;

    var device = try allocDeviceBuffer(self, data.len * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    try self.ctx.synchronize();

    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .f32,
        .shape = shape_i64,
        .elem_count = elem_count,
    };
    return tensor;
}

fn toFloat32Op(ctx: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]f32 {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const cuda_tensor = tensorFromCt(tensor);
    if (cuda_tensor.quant_type) |quant_type| {
        const dims = try allocator.alloc(u64, cuda_tensor.shape.len);
        defer allocator.free(dims);
        for (cuda_tensor.shape, 0..) |dim, i| {
            if (dim < 0) return error.InvalidShape;
            dims[i] = @intCast(dim);
        }
        const raw_len_u64 = gguf_tensor_types.byteLen(quant_type, dims) orelse return error.UnsupportedTensorType;
        const raw_len: usize = @intCast(raw_len_u64);
        const raw = try allocator.alloc(u8, raw_len);
        defer allocator.free(raw);
        try cuda_tensor.buffer.copyToHost(&self.ctx, raw);
        try self.ctx.synchronize();

        const out = try allocator.alloc(f32, cuda_tensor.elem_count);
        errdefer allocator.free(out);
        try quant_codec.dequantizeToFloat32(quant_type, raw, out);
        return out;
    }
    if (isDense16DType(cuda_tensor.dtype)) {
        const raw_len = try checkedMul(cuda_tensor.elem_count, @sizeOf(u16));
        const raw = try allocator.alloc(u8, raw_len);
        defer allocator.free(raw);
        try cuda_tensor.buffer.copyToHost(&self.ctx, raw);
        try self.ctx.synchronize();

        const out = try allocator.alloc(f32, cuda_tensor.elem_count);
        errdefer allocator.free(out);
        convertDense16RawToF32(cuda_tensor.dtype, raw, out) catch return error.UnsupportedTensorType;
        return out;
    }
    if (cuda_tensor.dtype != .f32) return error.UnsupportedTensorType;
    const out = try allocator.alloc(f32, cuda_tensor.elem_count);
    errdefer allocator.free(out);
    try cuda_tensor.buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(out));
    try self.ctx.synchronize();
    return out;
}

fn tensorDTypeOp(_: *anyopaque, tensor: CT) anyerror!tensor_mod.DType {
    return tensorFromCt(tensor).dtype;
}

fn tensorShapeOp(_: *anyopaque, tensor: CT, allocator: std.mem.Allocator) anyerror![]i64 {
    return allocator.dupe(i64, tensorFromCt(tensor).shape);
}

fn evalTensorOp(ctx: *anyopaque, _: CT) anyerror!void {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    try self.ctx.synchronize();
}

fn createTensor(
    self: *CudaCompute,
    device: buffer_mod.DeviceBuffer,
    shape: []i64,
    elem_count: usize,
) !CT {
    const tensor = try self.allocator.create(CudaTensor);
    tensor.* = .{
        .buffer = device,
        .dtype = .f32,
        .shape = shape,
        .elem_count = elem_count,
    };
    return tensor;
}

fn uploadOwnedHost(self: *CudaCompute, data: []f32, shape_src: []const i64) !CT {
    errdefer self.allocator.free(data);
    const elem_count = data.len;
    const shape = try self.allocator.dupe(i64, shape_src);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    try self.ctx.synchronize();
    self.allocator.free(data);
    return createTensor(self, device, shape, elem_count);
}

fn downloadAlloc(self: *CudaCompute, tensor: *const CudaTensor) ![]f32 {
    try ensureF32(tensor);
    const out = try self.allocator.alloc(f32, tensor.elem_count);
    errdefer self.allocator.free(out);
    try tensor.buffer.copyToHost(&self.ctx, std.mem.sliceAsBytes(out));
    try self.ctx.synchronize();
    return out;
}

fn allocShape2(allocator: std.mem.Allocator, rows: usize, cols: usize) ![]i64 {
    const shape = try allocator.alloc(i64, 2);
    shape[0] = @intCast(rows);
    shape[1] = @intCast(cols);
    return shape;
}

fn dupeShape(allocator: std.mem.Allocator, shape: []const i64) ![]i64 {
    return allocator.dupe(i64, shape);
}

fn ensureF32(tensor: *const CudaTensor) !void {
    if (tensor.dtype != .f32 or tensor.quant_type != null) return error.UnsupportedTensorType;
}

fn ensureF32OrQuantized(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    try ensureF32(tensor);
}

fn ensureDenseWeightSupported(tensor: *const CudaTensor) !void {
    if (tensor.quant_type != null) return;
    if (tensor.dtype == .f32 or isDense16DType(tensor.dtype)) return;
    return error.UnsupportedTensorType;
}

fn convertDense16RawToF32(dtype: tensor_mod.DType, raw: []const u8, out: []f32) !void {
    if (raw.len < try checkedMul(out.len, @sizeOf(u16))) return error.InvalidShape;
    switch (dtype) {
        .f16 => {
            for (out, 0..) |*dst, i| {
                const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
                const half: f16 = @bitCast(bits);
                dst.* = @floatCast(half);
            }
        },
        .bf16 => {
            for (out, 0..) |*dst, i| {
                const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
                dst.* = @bitCast(@as(u32, bits) << 16);
            }
        },
        else => return error.UnsupportedTensorType,
    }
}

fn isKnownQuant(tensor: *const CudaTensor, known: gguf_tensor_types.KnownTensorType) bool {
    const quant_type = tensor.quant_type orelse return false;
    return switch (quant_type) {
        .known => |actual| actual == known,
        else => false,
    };
}

fn ensureCount(tensor: *const CudaTensor, expected: usize) !void {
    if (tensor.elem_count != expected) return error.InvalidShape;
}

fn checkedMul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.InvalidShape;
}

fn checkedAdd(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.InvalidShape;
}

fn checkedSub(a: usize, b: usize) !usize {
    return std.math.sub(usize, a, b) catch error.InvalidShape;
}

fn elementCountFromShape(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return error.InvalidShape;
        count = try checkedMul(count, @intCast(dim));
    }
    return count;
}

fn sameShape(a: []const i64, b: []const i64) bool {
    return std.mem.eql(i64, a, b);
}

fn uploadTempI64(self: *CudaCompute, data: []const i64) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(i64));
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    return device;
}

fn uploadTempU32(self: *CudaCompute, data: []const u32) !buffer_mod.DeviceBuffer {
    const device = try self.temp_ids_masks.acquire(&self.ctx, data.len * @sizeOf(u32));
    try device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(data));
    return device;
}

const TempBufferPair = struct {
    first: buffer_mod.DeviceBuffer,
    second: buffer_mod.DeviceBuffer,
};

fn uploadTempU32Pair(self: *CudaCompute, first: []const u32, second: []const u32) !TempBufferPair {
    const first_bytes = try checkedMul(first.len, @sizeOf(u32));
    const second_bytes = try checkedMul(second.len, @sizeOf(u32));
    const total_bytes = try checkedAdd(first_bytes, second_bytes);
    const device = try self.temp_ids_masks.acquire(&self.ctx, total_bytes);
    const first_device: buffer_mod.DeviceBuffer = .{ .ptr = device.ptr, .len = first_bytes };
    const second_device: buffer_mod.DeviceBuffer = .{ .ptr = device.ptr + first_bytes, .len = second_bytes };
    try first_device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(first));
    try second_device.copyFromHost(&self.ctx, std.mem.sliceAsBytes(second));
    return .{ .first = first_device, .second = second_device };
}

fn embeddingLookup(ctx: *anyopaque, weight: CT, ids: []const i64, total: usize, dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const weight_tensor = tensorFromCt(weight);
    try ensureDenseWeightSupported(weight_tensor);
    if (ids.len != total) return error.InvalidShape;
    if (dim == 0 or weight_tensor.elem_count % dim != 0) return error.InvalidShape;
    const vocab = weight_tensor.elem_count / dim;
    for (ids) |raw_id| {
        if (raw_id < 0) return error.InvalidTokenId;
        const id: usize = @intCast(raw_id);
        if (id >= vocab) return error.InvalidTokenId;
    }
    const ids_device = try uploadTempI64(self, ids);
    const out_count = try checkedMul(total, dim);
    const shape = try allocShape2(self.allocator, total, dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q4_K => try self.kernels.launchEmbeddingLookupQ4KF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else switch (weight_tensor.dtype) {
        .f32 => try self.kernels.launchEmbeddingLookupF32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
        .f16 => try self.kernels.launchEmbeddingLookupWeightF16F32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
        .bf16 => try self.kernels.launchEmbeddingLookupWeightBf16F32(&self.ctx, device, weight_tensor.buffer, ids_device, total, dim),
        else => return error.UnsupportedTensorType,
    }
    return createTensor(self, device, shape, out_count);
}

fn takeRows(ctx: *anyopaque, request: *const ops.TakeRowsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(request.input);
    try ensureF32(input_tensor);
    if (request.dim == 0 or request.rows != request.row_ids.len) return error.InvalidShape;
    if (input_tensor.elem_count % request.dim != 0) return error.InvalidShape;
    const source_rows = input_tensor.elem_count / request.dim;
    for (request.row_ids) |row_id| {
        if (row_id >= source_rows) return error.InvalidShape;
    }
    const row_ids_device = try uploadTempU32(self, request.row_ids);

    const out_count = try checkedMul(request.rows, request.dim);
    const shape = try allocShape2(self.allocator, request.rows, request.dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchTakeRowsF32(&self.ctx, device, input_tensor.buffer, row_ids_device, source_rows, request.rows, request.dim);
    return try createTensor(self, device, shape, out_count);
}

fn glinerWordEmbeddings(ctx: *anyopaque, request: *const ops.GlinerWordEmbeddingsRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const hidden_tensor = tensorFromCt(request.hidden);
    try ensureF32(hidden_tensor);
    if (request.batch == 0 or request.seq_len == 0 or request.hidden_size == 0) return error.InvalidShape;
    const token_count = try checkedMul(request.batch, request.seq_len);
    if (request.words_mask.len < token_count) return error.InvalidShape;
    try ensureCount(hidden_tensor, try checkedMul(token_count, request.hidden_size));

    const out_rows = try checkedMul(request.batch, request.num_words);
    const out_count = try checkedMul(out_rows, request.hidden_size);
    const shape = try allocShape2(self.allocator, out_rows, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (out_count == 0) return try createTensor(self, device, shape, out_count);

    const mask_device = try uploadTempI64(self, request.words_mask[0..token_count]);
    try self.kernels.launchGlinerWordEmbeddingsF32(
        &self.ctx,
        device,
        hidden_tensor.buffer,
        mask_device,
        request.batch,
        request.seq_len,
        request.hidden_size,
        request.num_words,
    );
    return try createTensor(self, device, shape, out_count);
}

fn glinerLabelGruCombined(ctx: *anyopaque, request: *const ops.GlinerLabelGruCombinedRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const label_tensor = tensorFromCt(request.label_embeddings);
    try ensureF32(label_tensor);
    if (request.num_labels == 0 or request.hidden_size == 0) return error.InvalidShape;
    try ensureCount(label_tensor, try checkedMul(request.num_labels, request.hidden_size));

    const pos_w = try getWeight(ctx, "count_embed.pos_embedding.weight");
    const pos_tensor = tensorFromCt(pos_w);
    if (pos_tensor.elem_count < request.hidden_size) return error.InvalidShape;

    const label_count = try checkedMul(request.num_labels, request.hidden_size);
    const gate_dim = try checkedMul(request.hidden_size, 3);

    const pos_ct = if (pos_tensor.quant_type == null) blk: {
        if (pos_tensor.dtype != .f32) return null;
        const pos_shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
        var pos_shape_owned = false;
        errdefer if (!pos_shape_owned) self.allocator.free(pos_shape);
        var pos_device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
        var pos_device_owned = false;
        errdefer if (!pos_device_owned) pos_device.free(&self.ctx);
        try self.kernels.launchRepeatFirstRowF32(&self.ctx, pos_device, pos_tensor.buffer, request.num_labels, request.hidden_size);
        const repeated = try createTensor(self, pos_device, pos_shape, label_count);
        pos_shape_owned = true;
        pos_device_owned = true;
        break :blk repeated;
    } else blk: {
        if (!isKnownQuant(pos_tensor, .Q4_K)) return null;
        if (request.hidden_size == 0 or request.hidden_size % 256 != 0) return error.InvalidShape;
        const zero_ids = try self.allocator.alloc(i64, request.num_labels);
        defer self.allocator.free(zero_ids);
        @memset(zero_ids, 0);
        break :blk try embeddingLookup(ctx, pos_w, zero_ids, request.num_labels, request.hidden_size);
    };
    defer freeTensor(ctx, pos_ct);

    const w_ih = try getWeight(ctx, "count_embed.gru.weight_ih_l0");
    const b_ih = try getWeight(ctx, "count_embed.gru.bias_ih_l0");
    const gi = try linear(ctx, pos_ct, w_ih, b_ih, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gi);

    const w_hh = try getWeight(ctx, "count_embed.gru.weight_hh_l0");
    const b_hh = try getWeight(ctx, "count_embed.gru.bias_hh_l0");
    const gh = try linear(ctx, request.label_embeddings, w_hh, b_hh, request.num_labels, request.hidden_size, gate_dim);
    defer freeTensor(ctx, gh);

    const gi_tensor = tensorFromCt(gi);
    const gh_tensor = tensorFromCt(gh);
    const shape = try allocShape2(self.allocator, request.num_labels, request.hidden_size);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, label_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGruCombineF32(
        &self.ctx,
        device,
        label_tensor.buffer,
        gi_tensor.buffer,
        gh_tensor.buffer,
        request.num_labels,
        request.hidden_size,
    );
    return try createTensor(self, device, shape, label_count);
}

fn glinerGatherConcatRelu(ctx: *anyopaque, request: *const ops.GlinerGatherConcatReluRequest) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    if (self.kernels.gliner_gather_concat_relu_f32 == null) return null;
    if (request.start_rows.len != request.rows or request.end_rows.len != request.rows) return error.InvalidShape;

    const start_tensor = tensorFromCt(request.start);
    const end_tensor = tensorFromCt(request.end);
    try ensureF32(start_tensor);
    try ensureF32(end_tensor);
    try ensureCount(start_tensor, try checkedMul(request.source_rows, request.dim));
    try ensureCount(end_tensor, try checkedMul(request.source_rows, request.dim));

    const row_devices = try uploadTempU32Pair(self, request.start_rows, request.end_rows);
    const out_dim = try checkedMul(request.dim, 2);
    const out_count = try checkedMul(request.rows, out_dim);
    const shape = try allocShape2(self.allocator, request.rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchGlinerGatherConcatReluF32(
        &self.ctx,
        device,
        start_tensor.buffer,
        end_tensor.buffer,
        row_devices.first,
        row_devices.second,
        request.source_rows,
        request.rows,
        request.dim,
    );
    return try createTensor(self, device, shape, out_count);
}

fn tryDenseLtLinear16(
    self: *CudaCompute,
    dst: buffer_mod.DeviceBuffer,
    input: buffer_mod.DeviceBuffer,
    weight: buffer_mod.DeviceBuffer,
    bias: ?buffer_mod.DeviceBuffer,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    dtype: tensor_mod.DType,
) !bool {
    if (!self.dense_lt.enabled()) return false;
    const activation_count = try checkedMul(rows, in_dim);
    var activation16 = try allocDeviceBuffer(self, try checkedMul(activation_count, @sizeOf(u16)));
    defer releaseDeviceBuffer(self, &activation16);
    switch (dtype) {
        .f16 => try self.kernels.launchCastF32ToF16(&self.ctx, activation16, input, activation_count),
        .bf16 => try self.kernels.launchCastF32ToBf16(&self.ctx, activation16, input, activation_count),
        else => return false,
    }
    return try self.dense_lt.linear(&self.ctx, &self.libraries, dst, activation16, weight, bias, rows, in_dim, out_dim, dtype);
}

fn linear(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureDenseWeightSupported(weight_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q4_K => if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim))
                    try self.kernels.launchLinearQ4KSpanBiasTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim)
                else if (rows >= 2)
                    try self.kernels.launchLinearQ4KBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim)
                else
                    try self.kernels.launchLinearQ4KBiasTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else switch (weight_tensor.dtype) {
        .f32 => if (rows >= 2 and in_dim >= 256 and out_dim >= 4) {
            try self.kernels.launchLinearBiasTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        } else {
            try self.kernels.launchLinearBiasF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        },
        .f16 => {
            if (!try tryDenseLtLinear16(self, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim, .f16)) {
                try self.kernels.launchLinearBiasWeightF16F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            }
        },
        .bf16 => {
            if (!try tryDenseLtLinear16(self, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim, .bf16)) {
                try self.kernels.launchLinearBiasWeightBf16F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
            }
        },
        else => return error.UnsupportedTensorType,
    }
    return createTensor(self, device, shape, out_count);
}

fn linearQuickGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    if (!isKnownQuant(weight_tensor, .Q4_K)) return null;
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLinearQ4KBiasQuickGeluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
    return try createTensor(self, device, shape, out_count);
}

fn linearRelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_dense = weight_tensor.quant_type == null and weight_tensor.dtype == .f32 and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (!use_q4 and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (use_q4) {
        if (useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim)) {
            try self.kernels.launchLinearQ4KSpanBiasReluTile4Rows8F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        } else if (rows >= 2) {
            try self.kernels.launchLinearQ4KBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        } else {
            try self.kernels.launchLinearQ4KBiasReluTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
        }
    } else {
        try self.kernels.launchLinearBiasReluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
    }
    return try createTensor(self, device, shape, out_count);
}

fn linearGelu(ctx: *anyopaque, input: CT, weight: CT, bias: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    if (weight_tensor.quant_type != null or weight_tensor.dtype != .f32) return null;
    if (rows < 2 or in_dim < 256 or out_dim < 4) return null;
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(bias_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLinearBiasGeluTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, rows, in_dim, out_dim);
    return try createTensor(self, device, shape, out_count);
}

fn linearAdd(ctx: *anyopaque, input: CT, weight: CT, bias: CT, residual: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    const residual_tensor = tensorFromCt(residual);
    try ensureF32(input_tensor);
    try ensureF32(bias_tensor);
    try ensureF32(residual_tensor);
    const use_q4 = isKnownQuant(weight_tensor, .Q4_K);
    const use_dense = weight_tensor.quant_type == null and weight_tensor.dtype == .f32 and rows >= 2 and in_dim >= 256 and out_dim >= 4;
    if (!use_q4 and !use_dense) return null;
    if (use_dense) try ensureF32(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_tensor, out_dim);
    const out_count = try checkedMul(rows, out_dim);
    try ensureCount(residual_tensor, out_count);

    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (use_q4) {
        try self.kernels.launchLinearQ4KBiasAddTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
    } else {
        try self.kernels.launchLinearBiasAddTile4Rows2F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, residual_tensor.buffer, rows, in_dim, out_dim);
    }
    return try createTensor(self, device, shape, out_count);
}

fn linearNoBias(ctx: *anyopaque, input: CT, weight: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureDenseWeightSupported(weight_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_tensor, try checkedMul(out_dim, in_dim));

    const out_count = try checkedMul(rows, out_dim);
    const shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    if (weight_tensor.quant_type) |quant_type| {
        switch (quant_type) {
            .known => |known| switch (known) {
                .Q8_0 => try self.kernels.launchLinearQ8_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                .Q4_0 => try self.kernels.launchLinearQ4_0F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                .Q4_K => try self.kernels.launchLinearQ4KTile4F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
                else => return error.UnsupportedTensorType,
            },
            else => return error.UnsupportedTensorType,
        }
    } else switch (weight_tensor.dtype) {
        .f32 => try self.kernels.launchLinearF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim),
        .f16 => {
            if (!try tryDenseLtLinear16(self, device, input_tensor.buffer, weight_tensor.buffer, null, rows, in_dim, out_dim, .f16)) {
                try self.kernels.launchLinearWeightF16F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
            }
        },
        .bf16 => {
            if (!try tryDenseLtLinear16(self, device, input_tensor.buffer, weight_tensor.buffer, null, rows, in_dim, out_dim, .bf16)) {
                try self.kernels.launchLinearWeightBf16F32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, rows, in_dim, out_dim);
            }
        },
        else => return error.UnsupportedTensorType,
    }
    return createTensor(self, device, shape, out_count);
}

fn linearTriple(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, weight_c: CT, bias_c: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearTripleResult {
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const weight_c_tensor = tensorFromCt(weight_c);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);
    const bias_c_tensor = tensorFromCt(bias_c);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureF32(bias_c_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_c_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);
    try ensureCount(bias_c_tensor, out_dim);

    // The fused Q4_K triple kernel launches one CUDA block per output element.
    // For GLiNER/DeBERTa-sized rows this creates millions of tiny blocks; the
    // individual Q4_K path uses the row/column-tiled kernel and is faster.
    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, second);
    const third = try linear(ctx, input, weight_c, bias_c, rows, in_dim, out_dim);
    return .{ .first = first, .second = second, .third = third };
}

const SpanQ4PairMode = enum {
    shared,
    shared_relu,
    separate,
};

fn linearPairSpanQ4(
    self: *CudaCompute,
    input_a_tensor: *const CudaTensor,
    input_b_tensor: ?*const CudaTensor,
    weight_a_tensor: *const CudaTensor,
    bias_a_tensor: *const CudaTensor,
    weight_b_tensor: *const CudaTensor,
    bias_b_tensor: *const CudaTensor,
    rows: usize,
    in_dim: usize,
    out_dim: usize,
    mode: SpanQ4PairMode,
) anyerror!?ops.LinearPairResult {
    if (!isKnownQuant(weight_a_tensor, .Q4_K) or
        !isKnownQuant(weight_b_tensor, .Q4_K) or
        !useGlinerSpanQ4Kernel(self, rows, in_dim, out_dim))
    {
        return null;
    }

    const out_count = try checkedMul(rows, out_dim);
    const first_tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(first_tensor);
    const second_tensor = try self.allocator.create(CudaTensor);
    errdefer self.allocator.destroy(second_tensor);
    const first_shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(first_shape);
    const second_shape = try allocShape2(self.allocator, rows, out_dim);
    errdefer self.allocator.free(second_shape);
    var first_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer second_device.free(&self.ctx);

    switch (mode) {
        .shared => try self.kernels.launchLinearQ4KSpanPairBiasTile8Rows2F32(
            &self.ctx,
            first_device,
            second_device,
            input_a_tensor.buffer,
            weight_a_tensor.buffer,
            bias_a_tensor.buffer,
            weight_b_tensor.buffer,
            bias_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        ),
        .shared_relu => try self.kernels.launchLinearQ4KSpanPairBiasReluTile8Rows2F32(
            &self.ctx,
            first_device,
            second_device,
            input_a_tensor.buffer,
            weight_a_tensor.buffer,
            bias_a_tensor.buffer,
            weight_b_tensor.buffer,
            bias_b_tensor.buffer,
            rows,
            in_dim,
            out_dim,
        ),
        .separate => {
            const second_input = input_b_tensor orelse return error.InvalidShape;
            try self.kernels.launchLinearQ4KSpanPair2BiasTile8Rows2F32(
                &self.ctx,
                first_device,
                second_device,
                input_a_tensor.buffer,
                second_input.buffer,
                weight_a_tensor.buffer,
                bias_a_tensor.buffer,
                weight_b_tensor.buffer,
                bias_b_tensor.buffer,
                rows,
                in_dim,
                out_dim,
            );
        },
    }

    first_tensor.* = .{
        .buffer = first_device,
        .dtype = .f32,
        .shape = first_shape,
        .elem_count = out_count,
    };
    second_tensor.* = .{
        .buffer = second_device,
        .dtype = .f32,
        .shape = second_shape,
        .elem_count = out_count,
    };
    return .{
        .first = first_tensor,
        .second = second_tensor,
    };
}

fn linearPair(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    if (try linearPairSpanQ4(self, input_tensor, null, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .shared)) |span_pair| {
        return span_pair;
    }

    // The fused Q4_K pair kernel is block-per-output. Dispatch the two linears
    // separately so each uses the row/column-tiled Q4_K path.
    const first = try linear(ctx, input, weight_a, bias_a, rows, in_dim, out_dim);
    errdefer freeTensor(ctx, first);
    const second = try linear(ctx, input, weight_b, bias_b, rows, in_dim, out_dim);
    return .{ .first = first, .second = second };
}

fn linearPairRelu(ctx: *anyopaque, input: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    return try linearPairSpanQ4(self, input_tensor, null, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .shared_relu);
}

fn linearPairInputs(ctx: *anyopaque, input_a: CT, input_b: CT, weight_a: CT, bias_a: CT, weight_b: CT, bias_b: CT, rows: usize, in_dim: usize, out_dim: usize) anyerror!?ops.LinearPairResult {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_a_tensor = tensorFromCt(input_a);
    const input_b_tensor = tensorFromCt(input_b);
    const weight_a_tensor = tensorFromCt(weight_a);
    const weight_b_tensor = tensorFromCt(weight_b);
    const bias_a_tensor = tensorFromCt(bias_a);
    const bias_b_tensor = tensorFromCt(bias_b);

    try ensureF32(input_a_tensor);
    try ensureF32(input_b_tensor);
    try ensureF32(bias_a_tensor);
    try ensureF32(bias_b_tensor);
    try ensureCount(input_a_tensor, try checkedMul(rows, in_dim));
    try ensureCount(input_b_tensor, try checkedMul(rows, in_dim));
    try ensureCount(weight_a_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(weight_b_tensor, try checkedMul(out_dim, in_dim));
    try ensureCount(bias_a_tensor, out_dim);
    try ensureCount(bias_b_tensor, out_dim);

    return try linearPairSpanQ4(self, input_a_tensor, input_b_tensor, weight_a_tensor, bias_a_tensor, weight_b_tensor, bias_b_tensor, rows, in_dim, out_dim, .separate);
}

fn layerNorm(ctx: *anyopaque, input: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(input_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchLayerNormF32(&self.ctx, device, input_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn addLayerNorm(ctx: *anyopaque, a: CT, b: CT, gamma: CT, beta: CT, dim: usize, eps: f32) anyerror!?CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    const gamma_tensor = tensorFromCt(gamma);
    const beta_tensor = tensorFromCt(beta);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureF32(gamma_tensor);
    try ensureF32(beta_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return null;
    if (dim == 0 or a_tensor.elem_count % dim != 0) return null;
    try ensureCount(gamma_tensor, dim);
    try ensureCount(beta_tensor, dim);

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAddLayerNormF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, gamma_tensor.buffer, beta_tensor.buffer, a_tensor.elem_count / dim, dim, eps);
    return try createTensor(self, device, shape, a_tensor.elem_count);
}

fn rmsNorm(ctx: *anyopaque, input: CT, weight: CT, dim: usize, eps: f32) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    if (dim == 0 or input_tensor.elem_count % dim != 0) return error.InvalidShape;
    try ensureCount(weight_tensor, dim);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchRmsNormF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, input_tensor.elem_count / dim, dim, eps);
    return createTensor(self, device, shape, input_tensor.elem_count);
}
const UnaryOp = enum { gelu, relu, quick_gelu, sigmoid, tanh };

fn unaryHost(ctx: *anyopaque, input: CT, op: UnaryOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    const kernel_op: kernels_mod.ElementwiseOp = switch (op) {
        .gelu => .gelu,
        .relu => .relu,
        .quick_gelu => .quick_gelu,
        .sigmoid => .sigmoid,
        .tanh => .tanh,
    };
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, kernel_op);
    return createTensor(self, device, shape, input_tensor.elem_count);
}

fn gelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .gelu);
}

fn relu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .relu);
}

fn quickGelu(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .quick_gelu);
}

fn sigmoid(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .sigmoid);
}

fn tanhAct(ctx: *anyopaque, input: CT) anyerror!CT {
    return unaryHost(ctx, input, .tanh);
}
fn concat(ctx: *anyopaque, a: CT, b: CT, total: usize, dim_a: usize, dim_b: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    try ensureCount(a_tensor, try checkedMul(total, dim_a));
    try ensureCount(b_tensor, try checkedMul(total, dim_b));
    const out_dim = try checkedAdd(dim_a, dim_b);
    const out_count = try checkedMul(total, out_dim);
    const shape = try allocShape2(self.allocator, total, out_dim);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConcatLastDimF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, total, dim_a, dim_b);
    return createTensor(self, device, shape, out_count);
}

fn splitLastDim3(ctx: *anyopaque, input: CT, rows: usize, dim: usize) anyerror!ops.SplitLastDim3Result {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);
    const part_count = try checkedMul(rows, dim);
    try ensureCount(input_tensor, try checkedMul(part_count, 3));
    const shape_first = try allocShape2(self.allocator, rows, dim);
    var shape_first_owned = false;
    errdefer if (!shape_first_owned) self.allocator.free(shape_first);
    const shape_second = try allocShape2(self.allocator, rows, dim);
    var shape_second_owned = false;
    errdefer if (!shape_second_owned) self.allocator.free(shape_second);
    const shape_third = try allocShape2(self.allocator, rows, dim);
    var shape_third_owned = false;
    errdefer if (!shape_third_owned) self.allocator.free(shape_third);
    var first_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var first_device_owned = false;
    errdefer if (!first_device_owned) first_device.free(&self.ctx);
    var second_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var second_device_owned = false;
    errdefer if (!second_device_owned) second_device.free(&self.ctx);
    var third_device = try allocDeviceBuffer(self, part_count * @sizeOf(f32));
    var third_device_owned = false;
    errdefer if (!third_device_owned) third_device.free(&self.ctx);
    try self.kernels.launchSplitLastDim3F32(&self.ctx, first_device, second_device, third_device, input_tensor.buffer, rows, dim);
    const first = try createTensor(self, first_device, shape_first, part_count);
    first_device_owned = true;
    shape_first_owned = true;
    errdefer freeTensor(ctx, first);
    const second = try createTensor(self, second_device, shape_second, part_count);
    second_device_owned = true;
    shape_second_owned = true;
    errdefer freeTensor(ctx, second);
    const third = try createTensor(self, third_device, shape_third, part_count);
    third_device_owned = true;
    shape_third_owned = true;
    return .{ .first = first, .second = second, .third = third };
}

fn binaryElementwise(ctx: *anyopaque, a: CT, b: CT, op: kernels_mod.ElementwiseOp) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const a_tensor = tensorFromCt(a);
    const b_tensor = tensorFromCt(b);
    try ensureF32(a_tensor);
    try ensureF32(b_tensor);
    if (a_tensor.elem_count != b_tensor.elem_count or !sameShape(a_tensor.shape, b_tensor.shape)) return error.InvalidShape;

    const shape = try dupeShape(self.allocator, a_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, a_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, a_tensor.buffer, b_tensor.buffer, a_tensor.elem_count, op);
    return createTensor(self, device, shape, a_tensor.elem_count);
}

fn add(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .add);
}

fn multiply(ctx: *anyopaque, a: CT, b: CT) anyerror!CT {
    return binaryElementwise(ctx, a, b, .multiply);
}

fn silu(ctx: *anyopaque, input: CT) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    try ensureF32(input_tensor);

    const shape = try dupeShape(self.allocator, input_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, input_tensor.elem_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchElementwiseF32(&self.ctx, device, input_tensor.buffer, .{}, input_tensor.elem_count, .silu);
    return createTensor(self, device, shape, input_tensor.elem_count);
}
fn sdpaLaunch(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: ?[]const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const token_count = try checkedMul(batch, seq_len);
    const has_mask = mask != null;
    if (mask) |mask_values| {
        if (mask_values.len < token_count) return error.InvalidShape;
    }

    const mask_device = if (mask) |mask_values| try uploadTempI64(self, mask_values) else buffer_mod.DeviceBuffer{};
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, mask_device, bias_buffer, batch, seq_len, num_heads, head_dim, false, has_mask, bias_mode, true);
    return createTensor(self, device, shape, count);
}

fn sdpa(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, mask: []const i64, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    return sdpaLaunch(ctx, q_ct, k_ct, v_ct, mask, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn sdpaFull(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!?CT {
    return try sdpaLaunch(ctx, q_ct, k_ct, v_ct, null, attn_bias_ct, batch, seq_len, num_heads, head_dim);
}

fn causalSelfAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, attn_bias_ct: ?CT, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    const bias_tensor: ?*CudaTensor = if (attn_bias_ct) |bct| tensorFromCt(bct) else null;
    const bias_buffer = if (bias_tensor) |bt| bt.buffer else buffer_mod.DeviceBuffer{};
    const bias_mode: u32 = if (bias_tensor) |bt| blk: {
        const shared = try checkedMul(num_heads, try checkedMul(seq_len, seq_len));
        const batched = try checkedMul(batch, shared);
        break :blk if (bt.elem_count == batched) 2 else if (bt.elem_count == shared) 1 else return error.InvalidShape;
    } else 0;

    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, .{}, bias_buffer, batch, seq_len, num_heads, head_dim, true, false, bias_mode, false);
    return createTensor(self, device, shape, count);
}
fn crossAttention(_: *anyopaque, _: CT, _: CT, _: CT, _: []const i64, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}
fn relativePositionBias(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: bool) anyerror!CT {
    return unsupportedCt();
}
fn debertaDisentangledAttention(ctx: *anyopaque, q_ct: CT, k_ct: CT, v_ct: CT, q_r_ct: CT, k_r_ct: CT, mask: []const i64, batch: usize, seq_len: usize, num_heads: usize, head_dim: usize) anyerror!CT {
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const q_tensor = tensorFromCt(q_ct);
    const k_tensor = tensorFromCt(k_ct);
    const v_tensor = tensorFromCt(v_ct);
    const q_r_tensor = tensorFromCt(q_r_ct);
    const k_r_tensor = tensorFromCt(k_r_ct);
    try ensureF32(q_tensor);
    try ensureF32(k_tensor);
    try ensureF32(v_tensor);
    try ensureF32(q_r_tensor);
    try ensureF32(k_r_tensor);
    if (seq_len == 0) return error.InvalidShape;
    const hidden = try checkedMul(num_heads, head_dim);
    const count = try checkedMul(try checkedMul(batch, seq_len), hidden);
    const rel_positions = try checkedSub(try checkedMul(2, seq_len), 1);
    const rel_count = try checkedMul(rel_positions, hidden);
    try ensureCount(q_tensor, count);
    try ensureCount(k_tensor, count);
    try ensureCount(v_tensor, count);
    try ensureCount(q_r_tensor, rel_count);
    try ensureCount(k_r_tensor, rel_count);
    if (mask.len < try checkedMul(batch, seq_len)) return error.InvalidShape;

    const mask_device = try uploadTempI64(self, mask);
    const shape = try dupeShape(self.allocator, q_tensor.shape);
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchDebertaAttentionF32(&self.ctx, device, q_tensor.buffer, k_tensor.buffer, v_tensor.buffer, q_r_tensor.buffer, k_r_tensor.buffer, mask_device, batch, seq_len, num_heads, head_dim);
    return createTensor(self, device, shape, count);
}
fn windowedSelfAttention(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedCt();
}
fn channelSelfAttention(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedCt();
}
fn tokenGridConv2d(
    _: *anyopaque,
    _: CT,
    _: CT,
    _: CT,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
    _: usize,
) anyerror!CT {
    return unsupportedCt();
}
fn conv1d(_: *anyopaque, _: CT, _: CT, _: CT, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}
fn conv2d(
    ctx: *anyopaque,
    input: CT,
    weight: CT,
    bias: CT,
    batch: usize,
    in_channels: usize,
    out_channels: usize,
    height: usize,
    width: usize,
    kernel_h: usize,
    kernel_w: usize,
    stride_h: usize,
    stride_w: usize,
    padding_h: usize,
    padding_w: usize,
    groups: usize,
) anyerror!CT {
    if (groups == 0 or in_channels % groups != 0 or out_channels % groups != 0) return error.InvalidShape;
    const self: *CudaCompute = @ptrCast(@alignCast(ctx));
    const input_tensor = tensorFromCt(input);
    const weight_tensor = tensorFromCt(weight);
    const bias_tensor = tensorFromCt(bias);
    try ensureF32(input_tensor);
    try ensureF32(weight_tensor);
    try ensureF32(bias_tensor);
    const out_h = (height + 2 * padding_h - kernel_h) / stride_h + 1;
    const out_w = (width + 2 * padding_w - kernel_w) / stride_w + 1;
    const out_count = try checkedMul(try checkedMul(batch, out_channels), try checkedMul(out_h, out_w));
    try ensureCount(input_tensor, try checkedMul(try checkedMul(batch, in_channels), try checkedMul(height, width)));
    try ensureCount(weight_tensor, try checkedMul(try checkedMul(out_channels, in_channels / groups), try checkedMul(kernel_h, kernel_w)));
    try ensureCount(bias_tensor, out_channels);
    const shape = try self.allocator.dupe(i64, &.{ @as(i64, @intCast(batch)), @as(i64, @intCast(out_channels)), @as(i64, @intCast(out_h)), @as(i64, @intCast(out_w)) });
    errdefer self.allocator.free(shape);
    var device = try allocDeviceBuffer(self, out_count * @sizeOf(f32));
    errdefer device.free(&self.ctx);
    try self.kernels.launchConv2dF32(&self.ctx, device, input_tensor.buffer, weight_tensor.buffer, bias_tensor.buffer, batch, in_channels, out_channels, height, width, kernel_h, kernel_w, stride_h, stride_w, padding_h, padding_w, groups, out_h, out_w);
    return createTensor(self, device, shape, out_count);
}
fn rope(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: f32, _: f32, _: usize, _: bool) anyerror!CT {
    return unsupportedCt();
}
fn ropePerItem(_: *anyopaque, _: CT, _: usize, _: usize, _: usize, _: usize, _: f32, _: f32, _: []const usize, _: []const usize, _: bool) anyerror!CT {
    return unsupportedCt();
}
fn gqaCausalAttention(_: *anyopaque, _: CT, _: CT, _: CT, _: ?CT, _: usize, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}
fn gqaPagedAttention(_: *anyopaque, _: CT, _: CT, _: CT, _: ?CT, _: ops.AttentionContext, _: usize, _: usize, _: usize, _: usize) anyerror!CT {
    return unsupportedCt();
}

const vtable = ops.ComputeBackend.VTable{
    .backendKind = &backendKind,
    .deinitBackend = &deinitBackend,
    .freeTensor = &freeTensor,
    .getWeight = &getWeight,
    .prefetchWeightHint = &prefetchWeightHint,
    .drainPrefetchBudget = &drainPrefetchBudget,
    .embeddingLookup = &embeddingLookup,
    .takeRows = &takeRows,
    .glinerWordEmbeddings = &glinerWordEmbeddings,
    .glinerLabelGruCombined = &glinerLabelGruCombined,
    .glinerGatherConcatRelu = &glinerGatherConcatRelu,
    .linear = &linear,
    .linearQuickGelu = &linearQuickGelu,
    .linearRelu = &linearRelu,
    .linearGelu = &linearGelu,
    .linearAdd = &linearAdd,
    .linearNoBias = &linearNoBias,
    .linearPair = &linearPair,
    .linearPairRelu = &linearPairRelu,
    .linearPairInputs = &linearPairInputs,
    .linearTriple = &linearTriple,
    .layerNorm = &layerNorm,
    .addLayerNorm = &addLayerNorm,
    .rmsNorm = &rmsNorm,
    .gelu = &gelu,
    .relu = &relu,
    .silu = &silu,
    .quickGelu = &quickGelu,
    .sigmoid = &sigmoid,
    .tanh_act = &tanhAct,
    .splitLastDim3 = &splitLastDim3,
    .concat = &concat,
    .add = &add,
    .scaledDotProductAttention = &sdpa,
    .scaledDotProductAttentionFull = &sdpaFull,
    .causalSelfAttention = &causalSelfAttention,
    .crossAttention = &crossAttention,
    .relativePositionBias = &relativePositionBias,
    .disentangledRelativeAttention = &debertaDisentangledAttention,
    .windowedSelfAttention = &windowedSelfAttention,
    .channelSelfAttention = &channelSelfAttention,
    .tokenGridConv2d = &tokenGridConv2d,
    .multiply = &multiply,
    .conv1d = &conv1d,
    .conv2d = &conv2d,
    .rope = &rope,
    .ropePerItem = &ropePerItem,
    .gqaCausalAttention = &gqaCausalAttention,
    .gqaPagedAttention = &gqaPagedAttention,
    .fromFloat32 = &fromFloat32Op,
    .fromFloat32Shape = &fromFloat32ShapeOp,
    .toFloat32 = &toFloat32Op,
    .tensorDType = &tensorDTypeOp,
    .tensorShape = &tensorShapeOp,
    .evalTensor = &evalTensorOp,
};

test "cuda compute vtable is type checked" {
    const backend_kind_fn: *const fn (*anyopaque) ops.BackendKind = &backendKind;
    const linear_fn: *const fn (*anyopaque, CT, CT, CT, usize, usize, usize) anyerror!CT = &linear;
    const linear_no_bias_fn: *const fn (*anyopaque, CT, CT, usize, usize, usize) anyerror!CT = &linearNoBias;
    const rms_norm_fn: *const fn (*anyopaque, CT, CT, usize, f32) anyerror!CT = &rmsNorm;
    const rope_per_item_fn: *const fn (*anyopaque, CT, usize, usize, usize, usize, f32, f32, []const usize, []const usize, bool) anyerror!CT = &ropePerItem;
    _ = backend_kind_fn;
    _ = linear_fn;
    _ = linear_no_bias_fn;
    _ = rms_norm_fn;
    _ = rope_per_item_fn;
    _ = vtable;
}

test "cuda shape helpers reject incompatible shapes" {
    try std.testing.expect(try checkedMul(2, 3) == 6);
    try std.testing.expect(sameShape(&.{ 2, 3 }, &.{ 2, 3 }));
    try std.testing.expect(!sameShape(&.{ 2, 3 }, &.{ 3, 2 }));
}
