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

// TensorProto data extraction and dtype mapping.
//
// Converts ONNX TensorProto data fields (raw_data, float_data, etc.)
// into usable f32 slices for the termite constant pool, and maps
// ONNX data types to termite DType.

const std = @import("std");
const proto = @import("proto.zig");
const pb = @import("protobuf").wire;
const ml = @import("ml");

const TensorProto = proto.TensorProto;
const DataType = proto.DataType;
const Shape = ml.graph.Shape;
const DType = ml.graph.DType;

pub const TensorData = struct {
    /// f32 data suitable for the constant pool.
    data: []f32,
    /// Original shape.
    shape: Shape,

    pub fn deinit(self: *const TensorData, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

/// Map ONNX DataType to termite DType.
pub fn onnxDTypeToTermite(dt: DataType) !DType {
    return switch (dt) {
        .float32 => .f32,
        .float16 => .f16,
        .bfloat16 => .bf16,
        .int32 => .i32,
        .int64 => .i64,
        .uint8 => .u8,
        .bool_ => .bool_,
        else => error.UnsupportedDType,
    };
}

/// Build a termite Shape from ONNX tensor dims and data type.
pub fn tensorShape(tensor: *const TensorProto) !Shape {
    const dtype = try onnxDTypeToTermite(tensor.data_type);
    if (tensor.dims.len > 8) return error.TooManyDimensions;
    var dims: [8]i64 = @splat(0);
    for (tensor.dims, 0..) |d, i| dims[i] = d;
    return Shape{
        .dtype = dtype,
        .dims = dims,
        .rank_ = @intCast(tensor.dims.len),
    };
}

/// Count total elements from dims.
pub fn numElements(dims: []const i64) usize {
    if (dims.len == 0) return 1; // scalar
    var n: usize = 1;
    for (dims) |d| {
        if (d <= 0) return 0; // dynamic or empty
        n *= @intCast(d);
    }
    return n;
}

/// Extract tensor data as f32 slice. Handles raw_data, float_data, int32_data, int64_data.
/// Returns error.ExternalData if the tensor uses external storage (caller must load separately).
/// Caller owns returned slice.
pub fn extractFloat32(allocator: std.mem.Allocator, tensor: *const TensorProto) ![]f32 {
    // External data must be loaded by the caller via tensor.externalDataInfo()
    if (tensor.isExternal()) return error.ExternalData;

    return extractFloat32Inner(allocator, tensor);
}

/// Extract tensor data as f32, loading external data if needed.
/// `base_dir` is the directory containing the .onnx file; external `location`
/// entries are resolved relative to this path. Caller owns returned slice.
pub fn extractFloat32WithExternal(
    allocator: std.mem.Allocator,
    tensor: *const TensorProto,
    base_dir: ?[]const u8,
) ![]f32 {
    if (!tensor.isExternal()) return extractFloat32Inner(allocator, tensor);

    const bd = base_dir orelse return error.ExternalData;
    const info = tensor.externalDataInfo();
    if (info.location.len == 0) return error.MissingExternalLocation;

    // Reject path traversal — the ONNX spec says `location` must be a
    // relative path that stays within the model directory.
    if (std.fs.path.isAbsolute(info.location)) return error.InvalidExternalPath;
    if (std.mem.indexOf(u8, info.location, "..") != null) return error.InvalidExternalPath;

    const full_path = try std.fs.path.join(allocator, &.{ bd, info.location });
    defer allocator.free(full_path);

    const raw = try readExternalRegion(allocator, full_path, info.offset, info.length);
    defer allocator.free(raw);

    const count = numElements(tensor.dims);
    return extractRawDataAsF32(allocator, raw, tensor.data_type, count);
}

/// Extract tensor data as native little-endian bytes for the ONNX dtype.
/// This is used for integer constants where converting through f32 loses
/// sentinel values such as i64 max/min used by shape ops.
pub fn extractNativeBytesWithExternal(
    allocator: std.mem.Allocator,
    tensor: *const TensorProto,
    base_dir: ?[]const u8,
) ![]u8 {
    const dtype = try onnxDTypeToTermite(tensor.data_type);
    const count = numElements(tensor.dims);
    const byte_count = try std.math.mul(usize, count, dtype.byteSize());

    if (tensor.isExternal()) {
        const bd = base_dir orelse return error.ExternalData;
        const info = tensor.externalDataInfo();
        if (info.location.len == 0) return error.MissingExternalLocation;
        if (std.fs.path.isAbsolute(info.location)) return error.InvalidExternalPath;
        if (std.mem.indexOf(u8, info.location, "..") != null) return error.InvalidExternalPath;

        const full_path = try std.fs.path.join(allocator, &.{ bd, info.location });
        defer allocator.free(full_path);

        const raw = try readExternalRegion(allocator, full_path, info.offset, info.length);
        errdefer allocator.free(raw);
        if (raw.len < byte_count) return error.InsufficientData;
        if (raw.len == byte_count) return raw;

        const out = try allocator.dupe(u8, raw[0..byte_count]);
        allocator.free(raw);
        return out;
    }

    if (tensor.raw_data.len > 0) {
        if (tensor.raw_data.len < byte_count) return error.InsufficientData;
        return allocator.dupe(u8, tensor.raw_data[0..byte_count]);
    }

    if (byte_count == 0) return allocator.alloc(u8, 0);

    return switch (tensor.data_type) {
        .float32 => extractPackedBytes(u32, allocator, tensor.float_data, count),
        .float64 => extractPackedBytes(u64, allocator, tensor.double_data, count),
        .int32 => extractPackedVarintBytes(i32, allocator, tensor.int32_data, count),
        .int64 => extractPackedVarintBytes(i64, allocator, tensor.int64_data, count),
        else => blk: {
            const out = try allocator.alloc(u8, byte_count);
            @memset(out, 0);
            break :blk out;
        },
    };
}

fn extractFloat32Inner(allocator: std.mem.Allocator, tensor: *const TensorProto) ![]f32 {
    const count = numElements(tensor.dims);

    // Prefer raw_data (most common in optimized models)
    if (tensor.raw_data.len > 0) {
        return extractRawDataAsF32(allocator, tensor.raw_data, tensor.data_type, count);
    }

    // Try typed data fields
    if (tensor.float_data.len > 0) {
        return extractPackedF32(allocator, tensor.float_data, count);
    }
    if (tensor.int64_data.len > 0) {
        return extractInt64AsF32(allocator, tensor.int64_data, count);
    }
    if (tensor.int32_data.len > 0) {
        return extractInt32AsF32(allocator, tensor.int32_data, count);
    }
    if (tensor.double_data.len > 0) {
        return extractF64AsF32(allocator, tensor.double_data, count);
    }

    // Empty tensor or scalar zero
    if (count == 0) return allocator.alloc(f32, 0);

    // Scalar constant with no data fields — treat as zero
    const result = try allocator.alloc(f32, count);
    @memset(result, 0);
    return result;
}

/// Read a byte region from an external file. `length == -1` means "to end of file".
/// Caller owns the returned slice.
fn readExternalRegion(
    allocator: std.mem.Allocator,
    path: []const u8,
    offset: i64,
    length: i64,
) ![]u8 {
    if (offset < 0) return error.InvalidExternalOffset;

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |e| switch (e) {
        error.FileNotFound => return error.ExternalFileNotFound,
        else => return e,
    };
    defer file.close(io);

    const stat = try file.stat(io);
    const file_size: u64 = stat.size;
    const off_u: u64 = @intCast(offset);
    if (off_u > file_size) return error.ExternalRegionOutOfBounds;

    const len_u: u64 = if (length < 0)
        file_size - off_u
    else blk: {
        const l: u64 = @intCast(length);
        if (off_u + l > file_size) return error.ExternalRegionOutOfBounds;
        break :blk l;
    };

    const buf = try allocator.alloc(u8, @intCast(len_u));
    errdefer allocator.free(buf);

    const n = try file.readPositionalAll(io, buf, off_u);
    if (n != buf.len) return error.ExternalRegionShortRead;
    return buf;
}

fn extractRawDataAsF32(allocator: std.mem.Allocator, raw: []const u8, dt: DataType, count: usize) ![]f32 {
    const result = try allocator.alloc(f32, count);
    errdefer allocator.free(result);

    // Raw data from protobuf is byte-aligned; avoid @alignCast panics by
    // reading via std.mem.readInt / bytesToValue which handle unaligned data.
    switch (dt) {
        .float32 => {
            if (raw.len < count * 4) return error.InsufficientData;
            for (0..count) |i| {
                result[i] = @bitCast(std.mem.readInt(u32, raw[i * 4 ..][0..4], .little));
            }
        },
        .float64 => {
            if (raw.len < count * 8) return error.InsufficientData;
            for (0..count) |i| {
                const bits = std.mem.readInt(u64, raw[i * 8 ..][0..8], .little);
                result[i] = @floatCast(@as(f64, @bitCast(bits)));
            }
        },
        .float16 => {
            if (raw.len < count * 2) return error.InsufficientData;
            for (0..count) |i| {
                const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
                result[i] = @floatCast(@as(f16, @bitCast(bits)));
            }
        },
        .bfloat16 => {
            if (raw.len < count * 2) return error.InsufficientData;
            for (0..count) |i| {
                const bits = std.mem.readInt(u16, raw[i * 2 ..][0..2], .little);
                const f32_bits: u32 = @as(u32, bits) << 16;
                result[i] = @bitCast(f32_bits);
            }
        },
        .int32 => {
            if (raw.len < count * 4) return error.InsufficientData;
            for (0..count) |i| {
                const v = std.mem.readInt(i32, raw[i * 4 ..][0..4], .little);
                result[i] = @floatFromInt(v);
            }
        },
        .int64 => {
            if (raw.len < count * 8) return error.InsufficientData;
            for (0..count) |i| {
                const v = std.mem.readInt(i64, raw[i * 8 ..][0..8], .little);
                result[i] = @floatFromInt(v);
            }
        },
        .uint8 => {
            if (raw.len < count) return error.InsufficientData;
            for (0..count) |i| result[i] = @floatFromInt(raw[i]);
        },
        .bool_ => {
            if (raw.len < count) return error.InsufficientData;
            for (0..count) |i| result[i] = if (raw[i] != 0) 1.0 else 0.0;
        },
        else => return error.UnsupportedDType,
    }
    return result;
}

fn extractPackedF32(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]f32 {
    const actual_count = data.len / 4;
    const n = @min(actual_count, count);
    const result = try allocator.alloc(f32, if (count > 0) count else n);
    for (0..n) |i| {
        result[i] = @bitCast(std.mem.readInt(u32, data[i * 4 ..][0..4], .little));
    }
    if (n < result.len) @memset(result[n..], 0);
    return result;
}

fn extractPackedBytes(comptime T: type, allocator: std.mem.Allocator, data: []const u8, count: usize) ![]u8 {
    const byte_count = try std.math.mul(usize, count, @sizeOf(T));
    const out = try allocator.alloc(u8, byte_count);
    errdefer allocator.free(out);
    const n = @min(data.len / @sizeOf(T), count);
    if (n > 0) @memcpy(out[0 .. n * @sizeOf(T)], data[0 .. n * @sizeOf(T)]);
    if (n < count) @memset(out[n * @sizeOf(T) ..], 0);
    return out;
}

fn extractPackedVarintBytes(comptime T: type, allocator: std.mem.Allocator, data: []const u8, count: usize) ![]u8 {
    const out = try allocator.alloc(u8, count * @sizeOf(T));
    errdefer allocator.free(out);
    var pos: usize = 0;
    var i: usize = 0;
    while (pos < data.len and i < count) : (i += 1) {
        const raw = pb.readVarint(data, &pos) catch break;
        const value: T = switch (T) {
            i32 => @truncate(@as(i64, @bitCast(raw))),
            i64 => @bitCast(raw),
            else => @compileError("unsupported varint element type"),
        };
        const bytes = std.mem.toBytes(value);
        @memcpy(out[i * @sizeOf(T) ..][0..@sizeOf(T)], &bytes);
    }
    if (i < count) @memset(out[i * @sizeOf(T) ..], 0);
    return out;
}

/// Decode packed varint-encoded int64 values to f32.
/// Protobuf `repeated int64` uses varint encoding, NOT raw 8-byte LE.
fn extractInt64AsF32(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]f32 {
    const result = try allocator.alloc(f32, if (count > 0) count else 0);
    errdefer allocator.free(result);
    var pos: usize = 0;
    var i: usize = 0;
    while (pos < data.len and i < result.len) {
        const raw = pb.readVarint(data, &pos) catch break;
        const v: i64 = @bitCast(raw);
        result[i] = @floatFromInt(v);
        i += 1;
    }
    if (i < result.len) @memset(result[i..], 0);
    return result;
}

/// Decode packed varint-encoded int32 values to f32.
/// Protobuf `repeated int32` uses varint encoding, NOT raw 4-byte LE.
fn extractInt32AsF32(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]f32 {
    const result = try allocator.alloc(f32, if (count > 0) count else 0);
    errdefer allocator.free(result);
    var pos: usize = 0;
    var i: usize = 0;
    while (pos < data.len and i < result.len) {
        const raw = pb.readVarint(data, &pos) catch break;
        const v: i32 = @truncate(@as(i64, @bitCast(raw)));
        result[i] = @floatFromInt(v);
        i += 1;
    }
    if (i < result.len) @memset(result[i..], 0);
    return result;
}

fn extractF64AsF32(allocator: std.mem.Allocator, data: []const u8, count: usize) ![]f32 {
    const actual_count = data.len / 8;
    const n = @min(actual_count, count);
    const result = try allocator.alloc(f32, if (count > 0) count else n);
    for (0..n) |i| {
        const bits = std.mem.readInt(u64, data[i * 8 ..][0..8], .little);
        result[i] = @floatCast(@as(f64, @bitCast(bits)));
    }
    if (n < result.len) @memset(result[n..], 0);
    return result;
}

// ── Tests ────────────────────────────────────────────────────────────

test "onnxDTypeToTermite" {
    try std.testing.expectEqual(DType.f32, try onnxDTypeToTermite(.float32));
    try std.testing.expectEqual(DType.f16, try onnxDTypeToTermite(.float16));
    try std.testing.expectEqual(DType.bf16, try onnxDTypeToTermite(.bfloat16));
    try std.testing.expectEqual(DType.i32, try onnxDTypeToTermite(.int32));
    try std.testing.expectEqual(DType.i64, try onnxDTypeToTermite(.int64));
}

test "numElements" {
    try std.testing.expectEqual(@as(usize, 6), numElements(&.{ 2, 3 }));
    try std.testing.expectEqual(@as(usize, 24), numElements(&.{ 2, 3, 4 }));
    try std.testing.expectEqual(@as(usize, 1), numElements(&.{})); // scalar
}

test "extractFloat32 from raw_data f32" {
    const allocator = std.testing.allocator;
    const values = [_]f32{ 1.0, 2.0, 3.0 };
    const raw = std.mem.sliceAsBytes(&values);
    var dims = [_]i64{3};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .raw_data = raw,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(f32, 1.0), result[0]);
    try std.testing.expectEqual(@as(f32, 2.0), result[1]);
    try std.testing.expectEqual(@as(f32, 3.0), result[2]);
}

test "tensorShape builds correct shape" {
    var dims = [_]i64{ 2, 3, 4 };
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
    };
    const shape = try tensorShape(&tensor);
    try std.testing.expectEqual(DType.f32, shape.dtype);
    try std.testing.expectEqual(@as(u8, 3), shape.rank());
    try std.testing.expectEqual(@as(i64, 2), shape.dim(0));
    try std.testing.expectEqual(@as(i64, 3), shape.dim(1));
    try std.testing.expectEqual(@as(i64, 4), shape.dim(2));
}

test "tensorShape scalar (no dims)" {
    var dims = [_]i64{};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
    };
    const shape = try tensorShape(&tensor);
    try std.testing.expectEqual(@as(u8, 0), shape.rank());
}

test "tensorShape rejects too many dims" {
    var dims = [_]i64{ 1, 2, 3, 4, 5, 6, 7, 8, 9 }; // 9 > max 8
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
    };
    try std.testing.expectError(error.TooManyDimensions, tensorShape(&tensor));
}

test "tensorShape with non-f32 dtype" {
    var dims = [_]i64{ 10, 20 };
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .int64,
    };
    const shape = try tensorShape(&tensor);
    try std.testing.expectEqual(DType.i64, shape.dtype);
    try std.testing.expectEqual(@as(u8, 2), shape.rank());
}

test "onnxDTypeToTermite rejects unsupported types" {
    try std.testing.expectError(error.UnsupportedDType, onnxDTypeToTermite(.undefined));
}

test "onnxDTypeToTermite u8 and bool" {
    try std.testing.expectEqual(DType.u8, try onnxDTypeToTermite(.uint8));
    try std.testing.expectEqual(DType.bool_, try onnxDTypeToTermite(.bool_));
}

test "numElements with dynamic dim" {
    // Dynamic dims (<=0) yield 0 elements
    try std.testing.expectEqual(@as(usize, 0), numElements(&.{ 2, -1, 4 }));
    try std.testing.expectEqual(@as(usize, 0), numElements(&.{ 0, 3 }));
}

test "extractFloat32 from raw_data int32" {
    const allocator = std.testing.allocator;
    const values = [_]i32{ 10, 20, 30 };
    const raw = std.mem.sliceAsBytes(&values);
    var dims = [_]i64{3};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .int32,
        .raw_data = raw,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(f32, 10.0), result[0]);
    try std.testing.expectEqual(@as(f32, 20.0), result[1]);
    try std.testing.expectEqual(@as(f32, 30.0), result[2]);
}

test "extractFloat32 from raw_data int64" {
    const allocator = std.testing.allocator;
    const values = [_]i64{ 100, 200 };
    const raw = std.mem.sliceAsBytes(&values);
    var dims = [_]i64{2};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .int64,
        .raw_data = raw,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 2), result.len);
    try std.testing.expectEqual(@as(f32, 100.0), result[0]);
    try std.testing.expectEqual(@as(f32, 200.0), result[1]);
}

test "extractFloat32 from raw_data uint8" {
    const allocator = std.testing.allocator;
    const raw = [_]u8{ 1, 127, 255 };
    var dims = [_]i64{3};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .uint8,
        .raw_data = &raw,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(f32, 1.0), result[0]);
    try std.testing.expectEqual(@as(f32, 127.0), result[1]);
    try std.testing.expectEqual(@as(f32, 255.0), result[2]);
}

test "extractFloat32 empty tensor" {
    const allocator = std.testing.allocator;
    var dims = [_]i64{0};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "extractFloat32 scalar with no data" {
    const allocator = std.testing.allocator;
    var dims = [_]i64{};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
    };
    // Scalar (count=1) with no data → zero-filled
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqual(@as(f32, 0.0), result[0]);
}

test "extractFloat32 returns error for external data" {
    const allocator = std.testing.allocator;
    var dims = [_]i64{ 2, 3 };
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
    };
    try std.testing.expectError(error.ExternalData, extractFloat32(allocator, &tensor));
}

test "TensorProto.isExternal" {
    const default_tensor = TensorProto{};
    try std.testing.expect(!default_tensor.isExternal());

    const ext_tensor = TensorProto{ .data_location = .external };
    try std.testing.expect(ext_tensor.isExternal());
}

test "TensorProto.externalDataInfo" {
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "weights.bin" },
        .{ .key = "offset", .value = "1024" },
        .{ .key = "length", .value = "4096" },
    };
    const tensor = TensorProto{
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;
    const info = tensor.externalDataInfo();
    try std.testing.expectEqualStrings("weights.bin", info.location);
    try std.testing.expectEqual(@as(i64, 1024), info.offset);
    try std.testing.expectEqual(@as(i64, 4096), info.length);
}

test "extractFloat32 from varint-encoded int64_data" {
    const allocator = std.testing.allocator;
    // Encode [100, 200, -1] as packed varints using the wire module
    var enc: pb.Buf = .empty;
    defer enc.deinit(allocator);
    try pb.writeVarint(allocator, &enc, @bitCast(@as(i64, 100)));
    try pb.writeVarint(allocator, &enc, @bitCast(@as(i64, 200)));
    try pb.writeVarint(allocator, &enc, @bitCast(@as(i64, -1)));

    var dims = [_]i64{3};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .int64,
        .int64_data = enc.items,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(f32, 100.0), result[0]);
    try std.testing.expectEqual(@as(f32, 200.0), result[1]);
    try std.testing.expectEqual(@as(f32, -1.0), result[2]);
}

test "extractFloat32 from varint-encoded int32_data" {
    const allocator = std.testing.allocator;
    var enc: pb.Buf = .empty;
    defer enc.deinit(allocator);
    try pb.writeVarint(allocator, &enc, @bitCast(@as(i64, 42)));
    try pb.writeVarint(allocator, &enc, @bitCast(@as(i64, 0)));
    try pb.writeVarint(allocator, &enc, @bitCast(@as(i64, 7)));

    var dims = [_]i64{3};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .int32,
        .int32_data = enc.items,
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(f32, 42.0), result[0]);
    try std.testing.expectEqual(@as(f32, 0.0), result[1]);
    try std.testing.expectEqual(@as(f32, 7.0), result[2]);
}

// Small helper: write bytes to an absolute path using std.Io.
fn writeTestFile(path: []const u8, bytes: []const u8) !void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = path, .data = bytes });
}

fn removeTestFile(path: []const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    std.Io.Dir.cwd().deleteFile(io, path) catch {};
}

test "extractFloat32WithExternal reads whole file" {
    const allocator = std.testing.allocator;

    // Write a tiny f32 payload to a temp file.
    const values = [_]f32{ 1.5, 2.5, 3.5, 4.5 };
    const raw = std.mem.sliceAsBytes(&values);
    const base_dir = "/tmp";
    const file_name = "termite_onnx_ext_test.bin";
    const full_path = "/tmp/termite_onnx_ext_test.bin";
    try writeTestFile(full_path, raw);
    defer removeTestFile(full_path);

    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = file_name },
    };
    var dims = [_]i64{4};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;

    const result = try extractFloat32WithExternal(allocator, &tensor, base_dir);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 4), result.len);
    try std.testing.expectEqual(@as(f32, 1.5), result[0]);
    try std.testing.expectEqual(@as(f32, 2.5), result[1]);
    try std.testing.expectEqual(@as(f32, 3.5), result[2]);
    try std.testing.expectEqual(@as(f32, 4.5), result[3]);
}

test "extractFloat32WithExternal honors offset and length" {
    const allocator = std.testing.allocator;

    // Write [header: 16 bytes, f32 payload: 3 values, trailer: 4 bytes] to a file.
    const header = @as([16]u8, @splat(0xAA));
    const values = [_]f32{ 10.0, 20.0, 30.0 };
    const trailer = @as([4]u8, @splat(0xBB));
    const payload = std.mem.sliceAsBytes(&values);

    var composed: [16 + 3 * 4 + 4]u8 = undefined;
    @memcpy(composed[0..16], &header);
    @memcpy(composed[16 .. 16 + payload.len], payload);
    @memcpy(composed[16 + payload.len ..], &trailer);

    const base_dir = "/tmp";
    const file_name = "termite_onnx_ext_offset.bin";
    const full_path = "/tmp/termite_onnx_ext_offset.bin";
    try writeTestFile(full_path, &composed);
    defer removeTestFile(full_path);

    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = file_name },
        .{ .key = "offset", .value = "16" },
        .{ .key = "length", .value = "12" }, // 3 f32 values
    };
    var dims = [_]i64{3};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;

    const result = try extractFloat32WithExternal(allocator, &tensor, base_dir);
    defer allocator.free(result);

    try std.testing.expectEqual(@as(usize, 3), result.len);
    try std.testing.expectEqual(@as(f32, 10.0), result[0]);
    try std.testing.expectEqual(@as(f32, 20.0), result[1]);
    try std.testing.expectEqual(@as(f32, 30.0), result[2]);
}

test "extractFloat32WithExternal falls through for non-external tensors" {
    const allocator = std.testing.allocator;
    const values = [_]f32{ 7.0, 8.0 };
    const raw = std.mem.sliceAsBytes(&values);
    var dims = [_]i64{2};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .raw_data = raw,
    };
    const result = try extractFloat32WithExternal(allocator, &tensor, "/nonexistent");
    defer allocator.free(result);
    try std.testing.expectEqual(@as(f32, 7.0), result[0]);
    try std.testing.expectEqual(@as(f32, 8.0), result[1]);
}

test "extractFloat32WithExternal errors on missing base_dir" {
    const allocator = std.testing.allocator;
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "weights.bin" },
    };
    var dims = [_]i64{1};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;
    try std.testing.expectError(
        error.ExternalData,
        extractFloat32WithExternal(allocator, &tensor, null),
    );
}

test "extractFloat32WithExternal rejects path traversal" {
    const allocator = std.testing.allocator;
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "../secret.bin" },
    };
    var dims = [_]i64{1};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;
    try std.testing.expectError(
        error.InvalidExternalPath,
        extractFloat32WithExternal(allocator, &tensor, "/tmp"),
    );
}

test "extractFloat32WithExternal rejects absolute location" {
    const allocator = std.testing.allocator;
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "/etc/passwd" },
    };
    var dims = [_]i64{1};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;
    try std.testing.expectError(
        error.InvalidExternalPath,
        extractFloat32WithExternal(allocator, &tensor, "/tmp"),
    );
}

test "extractFloat32WithExternal missing file errors" {
    const allocator = std.testing.allocator;
    var entries = [_]proto.ExternalDataEntry{
        .{ .key = "location", .value = "does_not_exist_termite_onnx_test.bin" },
    };
    var dims = [_]i64{1};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .float32,
        .data_location = .external,
        .external_data = &entries,
    };
    _ = &entries;
    try std.testing.expectError(
        error.ExternalFileNotFound,
        extractFloat32WithExternal(allocator, &tensor, "/tmp"),
    );
}
