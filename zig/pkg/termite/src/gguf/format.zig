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
const tensor_types = @import("tensor_types.zig");

pub const magic = "GGUF";
pub const default_alignment: u64 = 32;

pub const Header = struct {
    version: u32,
    tensor_count: u64,
    metadata_count: u64,
};

pub const MetadataValueType = enum(u32) {
    u8 = 0,
    i8 = 1,
    u16 = 2,
    i16 = 3,
    u32 = 4,
    i32 = 5,
    f32 = 6,
    bool_ = 7,
    string = 8,
    array = 9,
    u64 = 10,
    i64 = 11,
    f64 = 12,
};

pub const MetadataArray = struct {
    element_type: MetadataValueType,
    values: []MetadataValue,
};

pub const MetadataValue = union(enum) {
    u8: u8,
    i8: i8,
    u16: u16,
    i16: i16,
    u32: u32,
    i32: i32,
    f32: f32,
    bool_: bool,
    string: []const u8,
    array: MetadataArray,
    u64: u64,
    i64: i64,
    f64: f64,

    pub fn deinit(self: *MetadataValue, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .string => |value| allocator.free(value),
            .array => |*arr| {
                for (arr.values) |*value| value.deinit(allocator);
                allocator.free(arr.values);
            },
            else => {},
        }
    }
};

pub const MetadataEntry = struct {
    key: []const u8,
    value: MetadataValue,

    pub fn deinit(self: *MetadataEntry, allocator: std.mem.Allocator) void {
        self.value.deinit(allocator);
        allocator.free(self.key);
    }
};

pub const TensorInfo = struct {
    name: []const u8,
    dimensions: []u64,
    tensor_type: tensor_types.TensorType,
    offset: u64,
    data_offset: u64,

    pub fn deinit(self: *TensorInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        allocator.free(self.dimensions);
    }
};

pub const File = struct {
    header: Header,
    metadata: []MetadataEntry,
    tensors: []TensorInfo,
    alignment: u64,
    data_region_offset: u64,

    pub fn deinit(self: *File, allocator: std.mem.Allocator) void {
        for (self.metadata) |*entry| entry.deinit(allocator);
        allocator.free(self.metadata);
        for (self.tensors) |*tensor| tensor.deinit(allocator);
        allocator.free(self.tensors);
    }
};

pub fn parse(allocator: std.mem.Allocator, bytes: []const u8) !File {
    var cursor = Cursor{ .bytes = bytes };
    const header = try parseHeader(&cursor);

    const metadata = try allocator.alloc(MetadataEntry, @intCast(header.metadata_count));
    errdefer allocator.free(metadata);
    var metadata_len: usize = 0;
    errdefer {
        for (metadata[0..metadata_len]) |*entry| entry.deinit(allocator);
    }

    for (0..@as(usize, @intCast(header.metadata_count))) |_| {
        metadata[metadata_len] = try parseMetadataEntry(allocator, &cursor);
        metadata_len += 1;
    }

    const parsed_metadata = metadata[0..metadata_len];
    const alignment = readAlignment(parsed_metadata) orelse default_alignment;
    const tensor_dialect = tensorDialectFromMetadata(parsed_metadata);

    const tensors = try allocator.alloc(TensorInfo, @intCast(header.tensor_count));
    errdefer allocator.free(tensors);
    var tensor_len: usize = 0;
    errdefer {
        for (tensors[0..tensor_len]) |*tensor| tensor.deinit(allocator);
    }

    for (0..@as(usize, @intCast(header.tensor_count))) |_| {
        tensors[tensor_len] = try parseTensorInfo(allocator, &cursor, alignment, tensor_dialect);
        tensor_len += 1;
    }

    const data_region_offset = alignForward(cursor.pos, alignment);
    for (tensors[0..tensor_len]) |*tensor| {
        tensor.data_offset = data_region_offset + tensor.offset;
    }

    return .{
        .header = header,
        .metadata = parsed_metadata,
        .tensors = tensors[0..tensor_len],
        .alignment = alignment,
        .data_region_offset = data_region_offset,
    };
}

fn parseHeader(cursor: *Cursor) !Header {
    const got_magic = try cursor.readBytes(magic.len);
    if (!std.mem.eql(u8, got_magic, magic)) return error.InvalidGgufMagic;

    return .{
        .version = try cursor.readInt(u32),
        .tensor_count = try cursor.readInt(u64),
        .metadata_count = try cursor.readInt(u64),
    };
}

fn parseMetadataEntry(allocator: std.mem.Allocator, cursor: *Cursor) !MetadataEntry {
    const key = try cursor.readOwnedString(allocator);
    errdefer allocator.free(key);

    const raw_type = try cursor.readInt(u32);
    const value_type = metadataValueTypeFromRaw(raw_type) orelse return error.UnsupportedMetadataType;
    const value = try parseMetadataValue(allocator, cursor, value_type);

    return .{ .key = key, .value = value };
}

fn parseMetadataValue(allocator: std.mem.Allocator, cursor: *Cursor, value_type: MetadataValueType) !MetadataValue {
    return switch (value_type) {
        .u8 => .{ .u8 = try cursor.readInt(u8) },
        .i8 => .{ .i8 = try cursor.readInt(i8) },
        .u16 => .{ .u16 = try cursor.readInt(u16) },
        .i16 => .{ .i16 = try cursor.readInt(i16) },
        .u32 => .{ .u32 = try cursor.readInt(u32) },
        .i32 => .{ .i32 = try cursor.readInt(i32) },
        .f32 => .{ .f32 = @bitCast(try cursor.readInt(u32)) },
        .bool_ => .{ .bool_ = (try cursor.readInt(u8)) != 0 },
        .string => .{ .string = try cursor.readOwnedString(allocator) },
        .array => blk: {
            const raw_elem_type = try cursor.readInt(u32);
            const elem_type = metadataValueTypeFromRaw(raw_elem_type) orelse return error.UnsupportedMetadataType;
            if (elem_type == .array) return error.UnsupportedNestedMetadataArray;

            const count = try cursor.readInt(u64);
            const values = try allocator.alloc(MetadataValue, @intCast(count));
            errdefer allocator.free(values);
            var len: usize = 0;
            errdefer {
                for (values[0..len]) |*value| value.deinit(allocator);
            }
            for (0..@as(usize, @intCast(count))) |_| {
                values[len] = try parseMetadataValue(allocator, cursor, elem_type);
                len += 1;
            }
            break :blk .{ .array = .{ .element_type = elem_type, .values = values[0..len] } };
        },
        .u64 => .{ .u64 = try cursor.readInt(u64) },
        .i64 => .{ .i64 = try cursor.readInt(i64) },
        .f64 => .{ .f64 = @bitCast(try cursor.readInt(u64)) },
    };
}

fn parseTensorInfo(allocator: std.mem.Allocator, cursor: *Cursor, alignment: u64, dialect: tensor_types.TensorType.Dialect) !TensorInfo {
    const name = try cursor.readOwnedString(allocator);
    errdefer allocator.free(name);

    const n_dimensions = try cursor.readInt(u32);
    const dimensions = try allocator.alloc(u64, n_dimensions);
    errdefer allocator.free(dimensions);
    for (dimensions) |*dim| dim.* = try cursor.readInt(u64);

    const tensor_type = tensor_types.TensorType.fromRawForDialect(try cursor.readInt(u32), dialect);
    const offset = try cursor.readInt(u64);

    return .{
        .name = name,
        .dimensions = dimensions,
        .tensor_type = tensor_type,
        .offset = offset,
        .data_offset = alignForward(cursor.pos, alignment) + offset,
    };
}

fn tensorDialectFromMetadata(metadata: []const MetadataEntry) tensor_types.TensorType.Dialect {
    for (metadata) |entry| {
        if (!std.mem.eql(u8, entry.key, "general.architecture")) continue;
        if (entry.value != .string) return .ggml_org;
        if (std.mem.eql(u8, entry.value.string, "bitnet-b1.58")) return .bitnet;
        if (std.mem.eql(u8, entry.value.string, "bitnet")) return .bitnet;
        return .ggml_org;
    }
    return .ggml_org;
}

fn readAlignment(metadata: []const MetadataEntry) ?u64 {
    for (metadata) |entry| {
        if (!std.mem.eql(u8, entry.key, "general.alignment")) continue;
        return switch (entry.value) {
            .u32 => |value| value,
            .u64 => |value| value,
            else => null,
        };
    }
    return null;
}

fn metadataValueTypeFromRaw(raw: u32) ?MetadataValueType {
    return switch (raw) {
        @intFromEnum(MetadataValueType.u8) => .u8,
        @intFromEnum(MetadataValueType.i8) => .i8,
        @intFromEnum(MetadataValueType.u16) => .u16,
        @intFromEnum(MetadataValueType.i16) => .i16,
        @intFromEnum(MetadataValueType.u32) => .u32,
        @intFromEnum(MetadataValueType.i32) => .i32,
        @intFromEnum(MetadataValueType.f32) => .f32,
        @intFromEnum(MetadataValueType.bool_) => .bool_,
        @intFromEnum(MetadataValueType.string) => .string,
        @intFromEnum(MetadataValueType.array) => .array,
        @intFromEnum(MetadataValueType.u64) => .u64,
        @intFromEnum(MetadataValueType.i64) => .i64,
        @intFromEnum(MetadataValueType.f64) => .f64,
        else => null,
    };
}

fn alignForward(value: u64, alignment: u64) u64 {
    if (alignment == 0 or alignment == 1) return value;
    const rem = value % alignment;
    if (rem == 0) return value;
    return value + (alignment - rem);
}

const Cursor = struct {
    bytes: []const u8,
    pos: u64 = 0,

    fn readBytes(self: *Cursor, count: usize) ![]const u8 {
        const start: usize = @intCast(self.pos);
        const end = start + count;
        if (end > self.bytes.len) return error.UnexpectedEndOfFile;
        self.pos += count;
        return self.bytes[start..end];
    }

    fn readInt(self: *Cursor, comptime T: type) !T {
        const bytes = try self.readBytes(@sizeOf(T));
        return std.mem.readInt(T, bytes[0..@sizeOf(T)], .little);
    }

    fn readOwnedString(self: *Cursor, allocator: std.mem.Allocator) ![]u8 {
        const len = try self.readInt(u64);
        const bytes = try self.readBytes(@intCast(len));
        return allocator.dupe(u8, bytes);
    }
};

test "parse minimal gguf file" {
    const allocator = std.testing.allocator;
    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 2);

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, "llama");

    try appendString(allocator, &data, "general.alignment");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.u32));
    try appendLe(u32, allocator, &data, 64);

    try appendString(allocator, &data, "tok_embeddings.weight");
    try appendLe(u32, allocator, &data, 2);
    try appendLe(u64, allocator, &data, 8);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 0);

    while (data.items.len % 64 != 0) try data.append(allocator, 0);
    try data.appendNTimes(allocator, 0, 64);

    var parsed = try parse(allocator, data.items);
    defer parsed.deinit(allocator);

    try std.testing.expectEqual(@as(u32, 3), parsed.header.version);
    try std.testing.expectEqual(@as(u64, 1), parsed.header.tensor_count);
    try std.testing.expectEqual(@as(u64, 64), parsed.alignment);
    try std.testing.expectEqual(@as(usize, 2), parsed.metadata.len);
    try std.testing.expectEqualStrings("tok_embeddings.weight", parsed.tensors[0].name);
    try std.testing.expectEqual(@as(u64, 192), parsed.data_region_offset);
    try std.testing.expectEqual(@as(u64, 192), parsed.tensors[0].data_offset);
}

test "parse tensor type 39 by gguf architecture dialect" {
    const allocator = std.testing.allocator;

    var llama = try buildSingleTensorGguf(allocator, "llama", 39);
    defer llama.deinit(allocator);
    var parsed_llama = try parse(allocator, llama.items);
    defer parsed_llama.deinit(allocator);
    try std.testing.expectEqualStrings("MXFP4", parsed_llama.tensors[0].tensor_type.name());

    var bitnet = try buildSingleTensorGguf(allocator, "bitnet-b1.58", 39);
    defer bitnet.deinit(allocator);
    var parsed_bitnet = try parse(allocator, bitnet.items);
    defer parsed_bitnet.deinit(allocator);
    try std.testing.expectEqualStrings("TL2", parsed_bitnet.tensors[0].tensor_type.name());
    try std.testing.expectEqual(@as(u32, 39), parsed_bitnet.tensors[0].tensor_type.raw());
}

fn buildSingleTensorGguf(allocator: std.mem.Allocator, architecture: []const u8, raw_tensor_type: u32) !std.ArrayListUnmanaged(u8) {
    var data = std.ArrayListUnmanaged(u8).empty;
    errdefer data.deinit(allocator);

    try data.appendSlice(allocator, magic);
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 1);

    try appendString(allocator, &data, "general.architecture");
    try appendLe(u32, allocator, &data, @intFromEnum(MetadataValueType.string));
    try appendString(allocator, &data, architecture);

    try appendString(allocator, &data, "test.weight");
    try appendLe(u32, allocator, &data, 2);
    try appendLe(u64, allocator, &data, 256);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, raw_tensor_type);
    try appendLe(u64, allocator, &data, 0);

    while (data.items.len % default_alignment != 0) try data.append(allocator, 0);
    try data.appendNTimes(allocator, 0, 128);
    return data;
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}
