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

//! Tensor file decoding without graph or optimizer dependencies.

const std = @import("std");

const proto = @import("proto.zig");

const pb = @import("protobuf").wire;

pub const TensorProto = proto.TensorProto;

pub const DataType = proto.DataType;

/// Borrowed acquisition capability. Each opened handle is a scoped borrow:
/// callers must deinit it, and validate it after consuming its bytes. Providers
/// may then reuse or evict idle handles without invalidating active readers.
pub const ExternalDataReader = struct {
    context: *anyopaque,
    open_fn: *const fn (*anyopaque, []const u8) anyerror!OpenedFile,
    pub const OpenedFile = struct {
        file: std.Io.File,
        io: std.Io,
        size: u64,
        context: ?*anyopaque = null,
        release_fn: ?*const fn (*anyopaque, std.Io.File) void = null,
        validate_fn: ?*const fn (*anyopaque, std.Io.File) anyerror!void = null,

        pub fn deinit(self: OpenedFile) void {
            if (self.release_fn) |release| release(self.context.?, self.file);
        }

        pub fn validate(self: OpenedFile) !void {
            if (self.validate_fn) |validate_file| try validate_file(self.context.?, self.file);
        }
    };

    pub fn open(self: ExternalDataReader, path: []const u8) !OpenedFile {
        return self.open_fn(self.context, path);
    }
    pub fn size(self: ExternalDataReader, path: []const u8) !u64 {
        const opened = try self.open(path);
        defer opened.deinit();
        try opened.validate();
        return opened.size;
    }
    pub fn read(self: ExternalDataReader, allocator: std.mem.Allocator, path: []const u8, offset: i64, length: i64, max_bytes: usize) ![]u8 {
        if (offset < 0) return error.InvalidExternalOffset;
        const opened = try self.open(path);
        defer opened.deinit();
        const off: u64 = @intCast(offset);
        if (off > opened.size) return error.ExternalRegionOutOfBounds;
        const len: u64 = if (length < 0) opened.size - off else @intCast(length);
        if (len > opened.size - off) return error.ExternalRegionOutOfBounds;
        if (len > max_bytes) return error.FileTooLarge;
        const bytes = try allocator.alloc(u8, @intCast(len));
        errdefer allocator.free(bytes);
        if (try opened.file.readPositionalAll(opened.io, bytes, off) != bytes.len) return error.ExternalRegionShortRead;
        try opened.validate();
        return bytes;
    }
};

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
    return extractFloat32WithReader(allocator, tensor, base_dir, null);
}

pub fn extractFloat32WithReader(allocator: std.mem.Allocator, tensor: *const TensorProto, base_dir: ?[]const u8, reader: ?ExternalDataReader) ![]f32 {
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

    const raw = try readExternalRegionWithReader(allocator, full_path, info.offset, info.length, reader);
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
    return extractNativeBytesWithReader(allocator, tensor, base_dir, null);
}

pub fn extractNativeBytesWithReader(allocator: std.mem.Allocator, tensor: *const TensorProto, base_dir: ?[]const u8, reader: ?ExternalDataReader) ![]u8 {
    const element_size = try nativeByteSize(tensor.data_type);
    const count = numElements(tensor.dims);
    const byte_count = try std.math.mul(usize, count, element_size);

    if (tensor.isExternal()) {
        const bd = base_dir orelse return error.ExternalData;
        const info = tensor.externalDataInfo();
        if (info.location.len == 0) return error.MissingExternalLocation;
        if (std.fs.path.isAbsolute(info.location)) return error.InvalidExternalPath;
        if (std.mem.indexOf(u8, info.location, "..") != null) return error.InvalidExternalPath;

        const full_path = try std.fs.path.join(allocator, &.{ bd, info.location });
        defer allocator.free(full_path);

        const raw = try readExternalRegionWithReader(allocator, full_path, info.offset, info.length, reader);
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

fn readExternalRegionWithReader(allocator: std.mem.Allocator, path: []const u8, offset: i64, length: i64, reader: ?ExternalDataReader) ![]u8 {
    if (reader) |source| return source.read(allocator, path, offset, length, std.math.maxInt(usize));
    return readExternalRegion(allocator, path, offset, length);
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
        .int8 => {
            if (raw.len < count) return error.InsufficientData;
            for (0..count) |i| result[i] = @floatFromInt(@as(i8, @bitCast(raw[i])));
        },
        .int16 => {
            if (raw.len < count * 2) return error.InsufficientData;
            for (0..count) |i| {
                const v = std.mem.readInt(i16, raw[i * 2 ..][0..2], .little);
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

fn nativeByteSize(dtype: DataType) !usize {
    return switch (dtype) {
        .float32, .int32 => 4,
        .float16, .bfloat16 => 2,
        .float64, .int64 => 8,
        .int8, .uint8, .bool_ => 1,
        else => error.UnsupportedDType,
    };
}

test "retained ONNX reader preserves float and native integer data without pathname IO" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const values = [_]f32{ 1, 2 };
    const integer: i64 = std.math.maxInt(i64);
    const bytes = std.mem.asBytes(&values) ++ std.mem.asBytes(&integer);
    try tmp.dir.writeFile(io, .{ .sub_path = "captured", .data = bytes });
    const file = try tmp.dir.openFile(io, "captured", .{});
    defer file.close(io);
    const Context = struct {
        file: std.Io.File,
        active: usize = 0,
        fail_validation: bool = false,
        fn release(raw: *anyopaque, _: std.Io.File) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active -= 1;
        }
        fn validate(raw: *anyopaque, _: std.Io.File) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.fail_validation) return error.ModelArtifactsChanging;
        }
        fn open(raw: *anyopaque, _: []const u8) !ExternalDataReader.OpenedFile {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.active += 1;
            return .{ .file = self.file, .io = std.testing.io, .size = 16, .context = self, .release_fn = release, .validate_fn = validate };
        }
    };
    var context = Context{ .file = file };
    const reader = ExternalDataReader{ .context = &context, .open_fn = Context.open };
    var dims = [_]i64{2};
    var fields = [_]proto.ExternalDataEntry{ .{ .key = "location", .value = "not-on-disk" }, .{ .key = "offset", .value = "0" }, .{ .key = "length", .value = "8" } };
    var tensor = TensorProto{ .data_type = .float32, .dims = &dims, .data_location = .external, .external_data = &fields };
    const floats = try extractFloat32WithReader(a, &tensor, "/not-a-real-model", reader);
    defer a.free(floats);
    try std.testing.expectEqualSlices(f32, &values, floats);
    tensor.data_type = .int64;
    dims[0] = 1;
    fields[1].value = "8";
    const native = try extractNativeBytesWithReader(a, &tensor, "/not-a-real-model", reader);
    defer a.free(native);
    try std.testing.expectEqualSlices(u8, std.mem.asBytes(&integer), native);
    try std.testing.expectEqual(@as(usize, 0), context.active);
    try std.testing.expectEqual(@as(u64, 16), try reader.size("unused"));
    try std.testing.expectError(error.ExternalRegionOutOfBounds, reader.read(a, "unused", 17, 1, 1024));
    try std.testing.expectError(error.FileTooLarge, reader.read(a, "unused", 0, -1, 1));
    context.fail_validation = true;
    try std.testing.expectError(error.ModelArtifactsChanging, reader.read(a, "unused", 0, -1, 1024));
    try std.testing.expectError(error.ModelArtifactsChanging, reader.size("unused"));
    try std.testing.expectEqual(@as(usize, 0), context.active);
}
