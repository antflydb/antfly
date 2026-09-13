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
const data = @import("onnx_data").tensor;
const proto = @import("onnx_data").proto;
const pb = @import("protobuf").wire;
const ml = @import("ml");

const TensorProto = data.TensorProto;
const DataType = data.DataType;
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
        .float64 => .f64,
        .int8 => .i8,
        .int16 => .i16,
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
    var dims: [8]i64 = .{0} ** 8;
    for (tensor.dims, 0..) |d, i| dims[i] = d;
    return Shape{
        .dtype = dtype,
        .dims = dims,
        .rank_ = @intCast(tensor.dims.len),
    };
}

pub const numElements = data.numElements;

pub const extractFloat32 = data.extractFloat32;

pub const extractFloat32WithExternal = data.extractFloat32WithExternal;

pub const extractNativeBytesWithExternal = data.extractNativeBytesWithExternal;

// ── Tests ────────────────────────────────────────────────────────────

test "onnxDTypeToTermite" {
    try std.testing.expectEqual(DType.f32, try onnxDTypeToTermite(.float32));
    try std.testing.expectEqual(DType.f16, try onnxDTypeToTermite(.float16));
    try std.testing.expectEqual(DType.bf16, try onnxDTypeToTermite(.bfloat16));
    try std.testing.expectEqual(DType.f64, try onnxDTypeToTermite(.float64));
    try std.testing.expectEqual(DType.i8, try onnxDTypeToTermite(.int8));
    try std.testing.expectEqual(DType.i16, try onnxDTypeToTermite(.int16));
    try std.testing.expectEqual(DType.i32, try onnxDTypeToTermite(.int32));
    try std.testing.expectEqual(DType.i64, try onnxDTypeToTermite(.int64));
}

test "extractFloat32 from raw_data int8" {
    const allocator = std.testing.allocator;
    const values = [_]i8{ -128, -3, 0, 7, 127 };
    var dims = [_]i64{values.len};
    const tensor = TensorProto{
        .dims = &dims,
        .data_type = .int8,
        .raw_data = std.mem.sliceAsBytes(&values),
    };
    const result = try extractFloat32(allocator, &tensor);
    defer allocator.free(result);
    try std.testing.expectEqualSlices(f32, &.{ -128.0, -3.0, 0.0, 7.0, 127.0 }, result);
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
    const header = [_]u8{0xAA} ** 16;
    const values = [_]f32{ 10.0, 20.0, 30.0 };
    const trailer = [_]u8{0xBB} ** 4;
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
