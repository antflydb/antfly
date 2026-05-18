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
const tensor_mod = @import("../backends/tensor.zig");
const native_compute = @import("../ops/native_compute.zig");
const tensor_store_mod = @import("tensor_store.zig");
const weight_source_mod = @import("weight_source.zig");
const quant_codec = @import("../gguf/quant_codec.zig");
const gguf_tensor_types = @import("../gguf/tensor_types.zig");

const Allocator = std.mem.Allocator;

pub const StorageKind = union(enum) {
    dense_native,
    quantized_dequantized_f32: gguf_tensor_types.TensorType,
};

pub const ByteSink = struct {
    context: ?*anyopaque = null,
    write: *const fn (context: ?*anyopaque, bytes: []const u8) anyerror!void,
};

pub const Stream = struct {
    shape: []i64,
    dtype: tensor_mod.DType,
    storage_kind: StorageKind,
    source_byte_len: usize,
    byte_len: usize,
    context: ?*anyopaque = null,
    write_all: *const fn (context: ?*anyopaque, allocator: Allocator, sink: ByteSink) anyerror!void,
    deinit: *const fn (context: ?*anyopaque, allocator: Allocator) void,
};

pub const Source = struct {
    context: ?*anyopaque = null,
    open: *const fn (context: ?*anyopaque, allocator: Allocator, name: []const u8, target_dtype: tensor_mod.DType) anyerror!?Stream,
    open_q8_0_block: ?*const fn (context: ?*anyopaque, allocator: Allocator, name: []const u8) anyerror!?Q8_0BlockTensor = null,

    pub fn openTensor(self: Source, allocator: Allocator, name: []const u8, target_dtype: tensor_mod.DType) !?Stream {
        return self.open(self.context, allocator, name, target_dtype);
    }

    pub fn openQ8_0BlockTensor(self: Source, allocator: Allocator, name: []const u8) !?Q8_0BlockTensor {
        const open = self.open_q8_0_block orelse return null;
        return open(self.context, allocator, name);
    }
};

pub const Q8_0BlockTensor = struct {
    shape: []i64,
    scale_shape: []i64,
    values_u8: []u8,
    scales_f32: []f32,
    axis: i64,
    block_size: i64,
    zero_point_u8: u8 = 128,
    source_byte_len: usize,

    pub fn deinit(self: *Q8_0BlockTensor, allocator: Allocator) void {
        allocator.free(self.shape);
        allocator.free(self.scale_shape);
        allocator.free(self.values_u8);
        allocator.free(self.scales_f32);
    }
};

const DenseStreamContext = struct {
    shape: []i64,
    target_dtype: tensor_mod.DType,
    tensor_ptr: ?*const tensor_mod.Tensor = null,
    owned_weight: ?weight_source_mod.LoadedWeight = null,
};

const QuantizedStreamContext = struct {
    shape: []i64,
    target_dtype: tensor_mod.DType,
    storage_ptr: ?*const weight_source_mod.QuantizedStorage = null,
    owned_storage: ?weight_source_mod.QuantizedStorage = null,
};

pub fn fromNativeWeightStore(store: *native_compute.WeightStore) Source {
    return .{
        .context = store,
        .open = &openNativeWeightStoreTensor,
        .open_q8_0_block = &openNativeWeightStoreQ8_0BlockTensor,
    };
}

fn openNativeWeightStoreTensor(
    raw_context: ?*anyopaque,
    allocator: Allocator,
    name: []const u8,
    target_dtype: tensor_mod.DType,
) !?Stream {
    const context = raw_context orelse return null;
    const store: *native_compute.WeightStore = @ptrCast(@alignCast(context));

    if (store.resident_weights.getPtr(name)) |weight| {
        if (weight.quantized_storage) |*storage| {
            return try makeQuantizedStreamBorrowed(allocator, storage, target_dtype);
        }
        return try makeDenseStreamBorrowed(allocator, &weight.tensor, target_dtype);
    }

    if (store.lazy_weights.getPtr(name)) |entry| {
        if (entry.loaded) |*loaded| {
            if (loaded.quantized_storage) |*storage| {
                return try makeQuantizedStreamBorrowed(allocator, storage, target_dtype);
            }
            return try makeDenseStreamBorrowed(allocator, &loaded.tensor, target_dtype);
        }

        const tensor_store = store.tensor_store orelse return null;
        if (entry.tensor_ref.quantized) {
            if (try tensor_store.loadQuantizedStorageRef(&entry.tensor_ref)) |storage| {
                return try makeQuantizedStreamOwned(allocator, storage, target_dtype);
            }
        }

        const loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
        return try makeDenseStreamOwned(allocator, loaded, target_dtype);
    }

    return null;
}

fn openNativeWeightStoreQ8_0BlockTensor(
    raw_context: ?*anyopaque,
    allocator: Allocator,
    name: []const u8,
) !?Q8_0BlockTensor {
    const context = raw_context orelse return null;
    const store: *native_compute.WeightStore = @ptrCast(@alignCast(context));

    if (store.resident_weights.getPtr(name)) |weight| {
        if (weight.quantized_storage) |*storage| {
            return try maybeMaterializeQ8_0BlockTensor(allocator, storage);
        }
        return null;
    }

    if (store.lazy_weights.getPtr(name)) |entry| {
        if (entry.loaded) |*loaded| {
            if (loaded.quantized_storage) |*storage| {
                return try maybeMaterializeQ8_0BlockTensor(allocator, storage);
            }
            return null;
        }

        const tensor_store = store.tensor_store orelse return null;
        if (entry.tensor_ref.quantized) {
            if (try tensor_store.loadQuantizedStorageRef(&entry.tensor_ref)) |storage| {
                defer {
                    var owned = storage;
                    owned.deinit();
                }
                return try maybeMaterializeQ8_0BlockTensor(allocator, &storage);
            }
        }
    }

    return null;
}

fn dtypeByteSize(dtype: tensor_mod.DType) usize {
    return dtype.byteSize();
}

fn shapeElementCount(shape: []const i64) !usize {
    var count: usize = 1;
    for (shape) |dim| {
        if (dim <= 0) return error.UnsupportedShape;
        count *= @intCast(dim);
    }
    return count;
}

fn decodeFp16Le(lo: u8, hi: u8) f32 {
    const bits: u16 = @bitCast([2]u8{ lo, hi });
    const half: f16 = @bitCast(bits);
    return @floatCast(half);
}

fn maybeMaterializeQ8_0BlockTensor(
    allocator: Allocator,
    storage: *const weight_source_mod.QuantizedStorage,
) !?Q8_0BlockTensor {
    switch (storage.tensor_type) {
        .known => |known| if (known != .Q8_0) return null,
        else => return null,
    }
    if (storage.shape.len == 0) return null;

    const block_values: usize = gguf_tensor_types.valuesPerBlock(storage.tensor_type) orelse return null;
    const block_bytes: usize = gguf_tensor_types.bytesPerBlock(storage.tensor_type) orelse return null;
    const axis: usize = storage.shape.len - 1;
    const last_dim: usize = @intCast(storage.shape[axis]);
    if (last_dim == 0 or last_dim % block_values != 0) return null;

    const element_count = try shapeElementCount(storage.shape);
    const block_count = element_count / block_values;
    if (storage.raw_bytes.len != block_count * block_bytes) return error.InvalidQuantizedDataSize;

    const shape = try allocator.dupe(i64, storage.shape);
    errdefer allocator.free(shape);
    const scale_shape = try allocator.alloc(i64, storage.shape.len);
    errdefer allocator.free(scale_shape);
    @memcpy(scale_shape, storage.shape);
    scale_shape[axis] = @intCast(last_dim / block_values);

    const values_u8 = try allocator.alloc(u8, element_count);
    errdefer allocator.free(values_u8);
    const scales_f32 = try allocator.alloc(f32, block_count);
    errdefer allocator.free(scales_f32);

    for (0..block_count) |block_idx| {
        const block_off = block_idx * block_bytes;
        const block = storage.raw_bytes[block_off .. block_off + block_bytes];
        scales_f32[block_idx] = decodeFp16Le(block[0], block[1]);
        for (0..block_values) |i| {
            const q: i8 = @bitCast(block[2 + i]);
            values_u8[block_idx * block_values + i] = @intCast(@as(i16, q) + 128);
        }
    }

    return .{
        .shape = shape,
        .scale_shape = scale_shape,
        .values_u8 = values_u8,
        .scales_f32 = scales_f32,
        .axis = @intCast(axis),
        .block_size = @intCast(block_values),
        .source_byte_len = storage.raw_bytes.len,
    };
}

fn makeDenseStreamBorrowed(
    allocator: Allocator,
    tensor: *const tensor_mod.Tensor,
    target_dtype: tensor_mod.DType,
) !Stream {
    const ctx = try allocator.create(DenseStreamContext);
    errdefer allocator.destroy(ctx);
    ctx.* = .{
        .shape = try allocator.dupe(i64, tensor.shape),
        .target_dtype = target_dtype,
        .tensor_ptr = tensor,
    };
    errdefer allocator.free(ctx.shape);
    return .{
        .shape = ctx.shape,
        .dtype = target_dtype,
        .storage_kind = .dense_native,
        .source_byte_len = tensor.data.len,
        .byte_len = try shapeElementCount(ctx.shape) * dtypeByteSize(target_dtype),
        .context = ctx,
        .write_all = &writeDenseStream,
        .deinit = &deinitDenseStream,
    };
}

fn makeDenseStreamOwned(
    allocator: Allocator,
    loaded: weight_source_mod.LoadedWeight,
    target_dtype: tensor_mod.DType,
) !Stream {
    const ctx = try allocator.create(DenseStreamContext);
    errdefer allocator.destroy(ctx);
    ctx.* = .{
        .shape = try allocator.dupe(i64, loaded.tensor.shape),
        .target_dtype = target_dtype,
        .owned_weight = loaded,
    };
    errdefer allocator.free(ctx.shape);
    return .{
        .shape = ctx.shape,
        .dtype = target_dtype,
        .storage_kind = .dense_native,
        .source_byte_len = loaded.tensor.data.len,
        .byte_len = try shapeElementCount(ctx.shape) * dtypeByteSize(target_dtype),
        .context = ctx,
        .write_all = &writeDenseStream,
        .deinit = &deinitDenseStream,
    };
}

fn denseTensor(ctx: *const DenseStreamContext) *const tensor_mod.Tensor {
    return if (ctx.tensor_ptr) |tensor|
        tensor
    else
        &ctx.owned_weight.?.tensor;
}

fn writeDenseStream(raw_context: ?*anyopaque, allocator: Allocator, sink: ByteSink) !void {
    const context = raw_context orelse return error.InvalidState;
    const ctx: *DenseStreamContext = @ptrCast(@alignCast(context));
    const tensor = denseTensor(ctx);
    if (tensor.dtype == ctx.target_dtype) {
        return sink.write(sink.context, tensor.data);
    }
    if (ctx.target_dtype != .f32) return error.UnsupportedTensorType;

    const chunk_elems: usize = 4096;
    var scratch = try allocator.alloc(f32, chunk_elems);
    defer allocator.free(scratch);

    switch (tensor.dtype) {
        .f16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            const total = tensor.elementCount();
            var start: usize = 0;
            while (start < total) : (start += chunk_elems) {
                const count = @min(chunk_elems, total - start);
                for (0..count) |i| {
                    const offset = (start + i) * 2;
                    const half: f16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                    scratch[i] = @floatCast(half);
                }
                try sink.write(sink.context, std.mem.sliceAsBytes(scratch[0..count]));
            }
        },
        .bf16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            const total = tensor.elementCount();
            var start: usize = 0;
            while (start < total) : (start += chunk_elems) {
                const count = @min(chunk_elems, total - start);
                for (0..count) |i| {
                    const offset = (start + i) * 2;
                    const bits: u16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                    scratch[i] = @bitCast(@as(u32, bits) << 16);
                }
                try sink.write(sink.context, std.mem.sliceAsBytes(scratch[0..count]));
            }
        },
        else => return error.UnsupportedTensorType,
    }
}

fn deinitDenseStream(raw_context: ?*anyopaque, allocator: Allocator) void {
    const context = raw_context orelse return;
    const ctx: *DenseStreamContext = @ptrCast(@alignCast(context));
    allocator.free(ctx.shape);
    if (ctx.owned_weight) |*loaded| loaded.deinit();
    allocator.destroy(ctx);
}

fn makeQuantizedStreamBorrowed(
    allocator: Allocator,
    storage: *const weight_source_mod.QuantizedStorage,
    target_dtype: tensor_mod.DType,
) !Stream {
    if (target_dtype != .f32) return error.UnsupportedTensorType;
    const ctx = try allocator.create(QuantizedStreamContext);
    errdefer allocator.destroy(ctx);
    ctx.* = .{
        .shape = try allocator.dupe(i64, storage.shape),
        .target_dtype = target_dtype,
        .storage_ptr = storage,
    };
    errdefer allocator.free(ctx.shape);
    return .{
        .shape = ctx.shape,
        .dtype = .f32,
        .storage_kind = .{ .quantized_dequantized_f32 = storage.tensor_type },
        .source_byte_len = storage.raw_bytes.len,
        .byte_len = try shapeElementCount(ctx.shape) * @sizeOf(f32),
        .context = ctx,
        .write_all = &writeQuantizedF32Stream,
        .deinit = &deinitQuantizedStream,
    };
}

fn makeQuantizedStreamOwned(
    allocator: Allocator,
    storage: weight_source_mod.QuantizedStorage,
    target_dtype: tensor_mod.DType,
) !Stream {
    if (target_dtype != .f32) return error.UnsupportedTensorType;
    const ctx = try allocator.create(QuantizedStreamContext);
    errdefer allocator.destroy(ctx);
    ctx.* = .{
        .shape = try allocator.dupe(i64, storage.shape),
        .target_dtype = target_dtype,
        .owned_storage = storage,
    };
    errdefer allocator.free(ctx.shape);
    return .{
        .shape = ctx.shape,
        .dtype = .f32,
        .storage_kind = .{ .quantized_dequantized_f32 = storage.tensor_type },
        .source_byte_len = storage.raw_bytes.len,
        .byte_len = try shapeElementCount(ctx.shape) * @sizeOf(f32),
        .context = ctx,
        .write_all = &writeQuantizedF32Stream,
        .deinit = &deinitQuantizedStream,
    };
}

fn quantizedStorage(ctx: *const QuantizedStreamContext) *const weight_source_mod.QuantizedStorage {
    return if (ctx.storage_ptr) |storage|
        storage
    else
        &ctx.owned_storage.?;
}

fn writeQuantizedF32Stream(raw_context: ?*anyopaque, allocator: Allocator, sink: ByteSink) !void {
    const context = raw_context orelse return error.InvalidState;
    const ctx: *QuantizedStreamContext = @ptrCast(@alignCast(context));
    const storage = quantizedStorage(ctx);
    if (ctx.shape.len == 0) return error.UnsupportedShape;

    const row_width: usize = @intCast(ctx.shape[ctx.shape.len - 1]);
    var row_count: usize = 1;
    for (ctx.shape[0 .. ctx.shape.len - 1]) |dim| row_count *= @intCast(dim);

    const row = try allocator.alloc(f32, row_width);
    defer allocator.free(row);

    for (0..row_count) |row_index| {
        try quant_codec.dequantizeRow(storage.tensor_type, storage.raw_bytes, row_width, row_index, row);
        try sink.write(sink.context, std.mem.sliceAsBytes(row));
    }
}

fn deinitQuantizedStream(raw_context: ?*anyopaque, allocator: Allocator) void {
    const context = raw_context orelse return;
    const ctx: *QuantizedStreamContext = @ptrCast(@alignCast(context));
    allocator.free(ctx.shape);
    if (ctx.owned_storage) |*storage| storage.deinit();
    allocator.destroy(ctx);
}

test "maybeMaterializeQ8_0BlockTensor matches GGUF Q8_0 dequantization" {
    const allocator = std.testing.allocator;

    const block_bytes = gguf_tensor_types.bytesPerBlock(.{ .known = .Q8_0 }).?;
    var raw = try allocator.alloc(u8, block_bytes);
    defer allocator.free(raw);

    const scale: f16 = 0.5;
    const scale_bits: u16 = @bitCast(scale);
    const scale_bytes: [2]u8 = @bitCast(scale_bits);
    raw[0] = scale_bytes[0];
    raw[1] = scale_bytes[1];
    for (0..32) |i| {
        const q: i8 = @intCast(@as(i32, @intCast(i)) - 16);
        raw[2 + i] = @bitCast(q);
    }

    const storage = weight_source_mod.QuantizedStorage{
        .tensor_type = .{ .known = .Q8_0 },
        .raw_bytes = raw,
        .shape = &.{ 1, 32 },
        .raw_owned = false,
        .allocator = allocator,
    };

    var block_tensor = (try maybeMaterializeQ8_0BlockTensor(allocator, &storage)).?;
    defer block_tensor.deinit(allocator);

    try std.testing.expectEqual(@as(i64, 1), block_tensor.shape[0]);
    try std.testing.expectEqual(@as(i64, 32), block_tensor.shape[1]);
    try std.testing.expectEqual(@as(i64, 1), block_tensor.scale_shape[0]);
    try std.testing.expectEqual(@as(i64, 1), block_tensor.scale_shape[1]);
    try std.testing.expectEqual(@as(i64, 1), block_tensor.axis);
    try std.testing.expectEqual(@as(i64, 32), block_tensor.block_size);

    var expected: [32]f32 = undefined;
    try quant_codec.dequantizeToFloat32(.{ .known = .Q8_0 }, raw, &expected);
    for (expected, 0..) |want, i| {
        const got = (block_tensor.scales_f32[0] * @as(f32, @floatFromInt(@as(i32, block_tensor.values_u8[i]) - 128)));
        try std.testing.expectApproxEqAbs(want, got, 1e-6);
    }
}
