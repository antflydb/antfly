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

// SafeTensors file parser.
//
// Format:
//   - 8 bytes: header size as little-endian u64
//   - N bytes: JSON header with tensor metadata
//   - Remaining: raw tensor data
//
// The JSON header maps tensor names to metadata:
//   { "tensor_name": { "dtype": "F32", "shape": [768, 512], "data_offsets": [0, 1572864] } }

const std = @import("std");
const Tensor = @import("../backends/tensor.zig").Tensor;
const DType = @import("../backends/tensor.zig").DType;

pub const max_header_size = 100 * 1024 * 1024; // 100MB sanity limit

pub const TensorMeta = struct {
    dtype: DType,
    shape: []const i64,
    data_start: u64,
    data_end: u64,
};

pub const Header = struct {
    allocator: std.mem.Allocator,
    tensors: std.StringHashMapUnmanaged(TensorMeta),
    metadata: ?std.json.Value,

    pub fn deinit(self: *Header) void {
        var it = self.tensors.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.shape);
        }
        self.tensors.deinit(self.allocator);
    }

    pub fn tensorNames(self: *const Header, allocator: std.mem.Allocator) ![][]const u8 {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        var it = self.tensors.iterator();
        while (it.next()) |entry| {
            try names.append(allocator, entry.key_ptr.*);
        }
        return try names.toOwnedSlice(allocator);
    }
};

/// Parse a SafeTensors file header from raw bytes.
/// Returns the header and the byte offset where tensor data begins.
pub fn parseHeader(allocator: std.mem.Allocator, file_bytes: []const u8) !struct { header: Header, data_offset: u64 } {
    if (file_bytes.len < 8) return error.FileTooSmall;

    const header_size = std.mem.readInt(u64, file_bytes[0..8], .little);
    if (header_size > max_header_size) return error.HeaderTooLarge;
    if (header_size == 0) return error.EmptyHeader;

    const total_header = 8 + header_size;
    if (file_bytes.len < total_header) return error.FileTruncated;

    const json_bytes = file_bytes[8..@intCast(total_header)];
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
    defer parsed.deinit();

    const root = parsed.value;
    if (root != .object) return error.InvalidHeader;

    var tensors = std.StringHashMapUnmanaged(TensorMeta){};
    var meta_value: ?std.json.Value = null;

    var obj_it = root.object.iterator();
    while (obj_it.next()) |entry| {
        const key = entry.key_ptr.*;
        const val = entry.value_ptr.*;

        if (std.mem.eql(u8, key, "__metadata__")) {
            meta_value = val;
            continue;
        }

        if (val != .object) continue;

        const dtype = parseDType(jsonString(val.object.get("dtype"))) orelse return error.UnsupportedDType;
        const shape = try parseShape(allocator, val.object.get("shape") orelse return error.MissingShape);
        const offsets = val.object.get("data_offsets") orelse return error.MissingOffsets;
        const data_start = jsonU64(offsets.array.items[0]) orelse return error.InvalidOffset;
        const data_end = jsonU64(offsets.array.items[1]) orelse return error.InvalidOffset;

        const owned_key = try allocator.dupe(u8, key);
        try tensors.put(allocator, owned_key, .{
            .dtype = dtype,
            .shape = shape,
            .data_start = data_start,
            .data_end = data_end,
        });
    }

    return .{
        .header = .{
            .allocator = allocator,
            .tensors = tensors,
            .metadata = meta_value,
        },
        .data_offset = total_header,
    };
}

fn jsonString(val: ?std.json.Value) ?[]const u8 {
    const v = val orelse return null;
    return switch (v) {
        .string => |s| s,
        else => null,
    };
}

fn jsonU64(val: std.json.Value) ?u64 {
    return switch (val) {
        .integer => |i| @intCast(i),
        else => null,
    };
}

fn parseShape(allocator: std.mem.Allocator, val: std.json.Value) ![]const i64 {
    if (val != .array) return error.InvalidShape;
    const items = val.array.items;
    const shape = try allocator.alloc(i64, items.len);
    for (items, 0..) |item, i| {
        shape[i] = switch (item) {
            .integer => |v| v,
            else => return error.InvalidShape,
        };
    }
    return shape;
}

fn parseDType(name: ?[]const u8) ?DType {
    const s = name orelse return null;
    if (std.mem.eql(u8, s, "F32")) return .f32;
    if (std.mem.eql(u8, s, "F16")) return .f16;
    if (std.mem.eql(u8, s, "BF16")) return .bf16;
    if (std.mem.eql(u8, s, "F64")) return .f64;
    if (std.mem.eql(u8, s, "I8")) return .i8;
    if (std.mem.eql(u8, s, "I16")) return .i16;
    if (std.mem.eql(u8, s, "I32")) return .i32;
    if (std.mem.eql(u8, s, "I64")) return .i64;
    if (std.mem.eql(u8, s, "U8") or std.mem.eql(u8, s, "BOOL")) return .u8;
    return null;
}

const c_file = @import("../util/c_file.zig");

/// Memory-mapped SafeTensors file reader.
pub const MMapReader = struct {
    allocator: std.mem.Allocator,
    header: Header,
    data_offset: u64,
    file_bytes: []const u8,
    mmap_region: ?c_file.MmapRegion = null,

    /// Create a reader from bytes already in memory (e.g., from mmap or file read).
    pub fn fromBytes(allocator: std.mem.Allocator, file_bytes: []const u8) !MMapReader {
        const result = try parseHeader(allocator, file_bytes);
        return .{
            .allocator = allocator,
            .header = result.header,
            .data_offset = result.data_offset,
            .file_bytes = file_bytes,
        };
    }

    /// Create a reader by reading a file from a directory handle.
    pub fn openFile(allocator: std.mem.Allocator, dir: std.fs.Dir, sub_path: []const u8) !MMapReader {
        const file = try dir.openFile(sub_path, .{});
        defer file.close();

        const stat = try file.stat();
        const file_bytes = try allocator.alloc(u8, stat.size);
        errdefer allocator.free(file_bytes);

        const bytes_read = try file.readAll(file_bytes);
        if (bytes_read != stat.size) return error.IncompleteRead;

        const result = try parseHeader(allocator, file_bytes);
        return .{
            .allocator = allocator,
            .header = result.header,
            .data_offset = result.data_offset,
            .file_bytes = file_bytes,
        };
    }

    pub fn readTensor(self: *const MMapReader, name: []const u8) !Tensor {
        const meta = self.header.tensors.get(name) orelse return error.TensorNotFound;

        const abs_start = self.data_offset + meta.data_start;
        const abs_end = self.data_offset + meta.data_end;
        if (abs_end > self.file_bytes.len) return error.DataOutOfBounds;

        const raw = self.file_bytes[@intCast(abs_start)..@intCast(abs_end)];

        if (self.mmap_region != null) {
            // Data is mmap'd — return borrowed view (no copy).
            return .{
                .data = @constCast(raw),
                .dtype = meta.dtype,
                .shape = meta.shape,
                .name = name,
                .allocator = self.allocator,
                .owns_data = false,
                .owns_shape = false,
            };
        }

        const owned_shape = try self.allocator.dupe(i64, meta.shape);
        const owned = try self.allocator.dupe(u8, raw);
        return .{
            .data = owned,
            .dtype = meta.dtype,
            .shape = owned_shape,
            .name = name,
            .allocator = self.allocator,
            .owns_data = true,
            .owns_shape = true,
        };
    }

    /// Open a SafeTensors file by absolute path using mmap.
    pub fn openFileAbsolute(allocator: std.mem.Allocator, path: []const u8) !MMapReader {
        var mmap_region = try c_file.MmapRegion.init(allocator, path);
        errdefer mmap_region.deinit();

        const result = try parseHeader(allocator, mmap_region.data);
        // Header parse done — switch to random-access advice for inference.
        mmap_region.adviseRandom();
        return .{
            .allocator = allocator,
            .header = result.header,
            .data_offset = result.data_offset,
            .file_bytes = mmap_region.data,
            .mmap_region = mmap_region,
        };
    }

    pub fn deinit(self: *MMapReader) void {
        self.header.deinit();
        if (self.mmap_region) |*region| {
            region.deinit();
        } else {
            self.allocator.free(self.file_bytes);
        }
    }
};

/// Load model index for sharded SafeTensors models.
/// The index maps tensor names to shard filenames.
pub const ShardedIndex = struct {
    weight_map: std.StringHashMapUnmanaged([]const u8), // tensor name -> shard filename
    allocator: std.mem.Allocator,

    pub fn load(allocator: std.mem.Allocator, json_bytes: []const u8) !ShardedIndex {
        const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_bytes, .{});
        defer parsed.deinit();

        const root = parsed.value;
        if (root != .object) return error.InvalidIndex;

        const wm = root.object.get("weight_map") orelse return error.MissingWeightMap;
        if (wm != .object) return error.InvalidWeightMap;

        var weight_map = std.StringHashMapUnmanaged([]const u8){};
        var it = wm.object.iterator();
        while (it.next()) |entry| {
            const tensor_name = try allocator.dupe(u8, entry.key_ptr.*);
            const filename = switch (entry.value_ptr.*) {
                .string => |s| try allocator.dupe(u8, s),
                else => return error.InvalidWeightMap,
            };
            try weight_map.put(allocator, tensor_name, filename);
        }

        return .{ .weight_map = weight_map, .allocator = allocator };
    }

    pub fn deinit(self: *ShardedIndex) void {
        var it = self.weight_map.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.weight_map.deinit(self.allocator);
    }
};

// -- Tests --

test "parse header" {
    const allocator = std.testing.allocator;

    // Build a minimal safetensors file in memory.
    const json_str =
        \\{"test_tensor": {"dtype": "F32", "shape": [2, 3], "data_offsets": [0, 24]}}
    ;
    var buf: [8 + json_str.len + 24]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], json_str.len, .little);
    @memcpy(buf[8..][0..json_str.len], json_str);
    // Fill tensor data with zeros.
    @memset(buf[8 + json_str.len ..], 0);

    var result = try parseHeader(allocator, &buf);
    defer result.header.deinit();

    const meta = result.header.tensors.get("test_tensor") orelse return error.TestFailed;
    try std.testing.expectEqual(DType.f32, meta.dtype);
    try std.testing.expectEqual(@as(usize, 2), meta.shape.len);
    try std.testing.expectEqual(@as(i64, 2), meta.shape[0]);
    try std.testing.expectEqual(@as(i64, 3), meta.shape[1]);
    try std.testing.expectEqual(@as(u64, 0), meta.data_start);
    try std.testing.expectEqual(@as(u64, 24), meta.data_end);
    try std.testing.expectEqual(@as(u64, 8 + json_str.len), result.data_offset);
}

test "read tensor from bytes" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{"weights": {"dtype": "F32", "shape": [4], "data_offsets": [0, 16]}}
    ;
    const data_size = 16;
    var buf: [8 + json_str.len + data_size]u8 = undefined;
    std.mem.writeInt(u64, buf[0..8], json_str.len, .little);
    @memcpy(buf[8..][0..json_str.len], json_str);

    // Write f32 values: 1.0, 2.0, 3.0, 4.0
    const data_start = 8 + json_str.len;
    const floats = [_]f32{ 1.0, 2.0, 3.0, 4.0 };
    @memcpy(buf[data_start..][0..data_size], std.mem.asBytes(&floats));

    const file_bytes = try allocator.dupe(u8, &buf);
    var reader = try MMapReader.fromBytes(allocator, file_bytes);
    defer reader.deinit();

    var tensor = try reader.readTensor("weights");
    defer tensor.deinit();

    try std.testing.expectEqual(DType.f32, tensor.dtype);
    try std.testing.expectEqual(@as(usize, 4), tensor.elementCount());
    const values = tensor.asFloat32();
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), values[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), values[3], 1e-6);
}

test "sharded index" {
    const allocator = std.testing.allocator;

    const json_str =
        \\{"weight_map": {"bert.embeddings.weight": "model-00001-of-00002.safetensors", "bert.encoder.weight": "model-00002-of-00002.safetensors"}}
    ;

    var index = try ShardedIndex.load(allocator, json_str);
    defer index.deinit();

    const shard1 = index.weight_map.get("bert.embeddings.weight") orelse return error.TestFailed;
    try std.testing.expectEqualStrings("model-00001-of-00002.safetensors", shard1);

    const shard2 = index.weight_map.get("bert.encoder.weight") orelse return error.TestFailed;
    try std.testing.expectEqualStrings("model-00002-of-00002.safetensors", shard2);
}
