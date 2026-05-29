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
const format = @import("format.zig");
const tensor_types = @import("tensor_types.zig");

pub const TensorSpec = struct {
    name: []const u8,
    dimensions: []const u64,
    tensor_type: tensor_types.TensorType,
};

pub const Layout = struct {
    header_bytes: []u8,
    offsets: []u64,
    alignment: u64,

    pub fn deinit(self: *Layout, allocator: std.mem.Allocator) void {
        allocator.free(self.header_bytes);
        allocator.free(self.offsets);
    }
};

pub fn buildLayout(
    allocator: std.mem.Allocator,
    metadata: []const format.MetadataEntry,
    tensors: []const TensorSpec,
) !Layout {
    var header = std.ArrayListUnmanaged(u8).empty;
    errdefer header.deinit(allocator);

    const alignment = readAlignment(metadata) orelse format.default_alignment;
    const offsets = try computeTensorOffsets(allocator, tensors, alignment);
    errdefer allocator.free(offsets);

    try header.appendSlice(allocator, format.magic);
    try appendLe(u32, allocator, &header, 3);
    try appendLe(u64, allocator, &header, tensors.len);
    try appendLe(u64, allocator, &header, metadata.len);

    for (metadata) |entry| {
        try appendString(allocator, &header, entry.key);
        try appendLe(u32, allocator, &header, metadataValueType(entry.value));
        try appendMetadataValue(allocator, &header, entry.value);
    }

    for (tensors, offsets) |tensor, offset| {
        try appendString(allocator, &header, tensor.name);
        try appendLe(u32, allocator, &header, @intCast(tensor.dimensions.len));
        for (tensor.dimensions) |dim| try appendLe(u64, allocator, &header, dim);
        try appendLe(u32, allocator, &header, tensor.tensor_type.raw());
        try appendLe(u64, allocator, &header, offset);
    }

    return .{
        .header_bytes = try header.toOwnedSlice(allocator),
        .offsets = offsets,
        .alignment = alignment,
    };
}

fn computeTensorOffsets(
    allocator: std.mem.Allocator,
    tensors: []const TensorSpec,
    alignment: u64,
) ![]u64 {
    const offsets = try allocator.alloc(u64, tensors.len);
    var current_offset: u64 = 0;
    for (tensors, 0..) |tensor, i| {
        current_offset = alignForward(current_offset, alignment);
        offsets[i] = current_offset;
        current_offset += tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
    }
    return offsets;
}

fn metadataValueType(value: format.MetadataValue) u32 {
    return switch (value) {
        .u8 => @intFromEnum(format.MetadataValueType.u8),
        .i8 => @intFromEnum(format.MetadataValueType.i8),
        .u16 => @intFromEnum(format.MetadataValueType.u16),
        .i16 => @intFromEnum(format.MetadataValueType.i16),
        .u32 => @intFromEnum(format.MetadataValueType.u32),
        .i32 => @intFromEnum(format.MetadataValueType.i32),
        .f32 => @intFromEnum(format.MetadataValueType.f32),
        .bool_ => @intFromEnum(format.MetadataValueType.bool_),
        .string => @intFromEnum(format.MetadataValueType.string),
        .array => @intFromEnum(format.MetadataValueType.array),
        .u64 => @intFromEnum(format.MetadataValueType.u64),
        .i64 => @intFromEnum(format.MetadataValueType.i64),
        .f64 => @intFromEnum(format.MetadataValueType.f64),
    };
}

fn appendMetadataValue(
    allocator: std.mem.Allocator,
    data: *std.ArrayListUnmanaged(u8),
    value: format.MetadataValue,
) !void {
    switch (value) {
        .u8 => |item| try appendLe(u8, allocator, data, item),
        .i8 => |item| try appendLe(i8, allocator, data, item),
        .u16 => |item| try appendLe(u16, allocator, data, item),
        .i16 => |item| try appendLe(i16, allocator, data, item),
        .u32 => |item| try appendLe(u32, allocator, data, item),
        .i32 => |item| try appendLe(i32, allocator, data, item),
        .f32 => |item| try appendLe(u32, allocator, data, @bitCast(item)),
        .bool_ => |item| try appendLe(u8, allocator, data, if (item) 1 else 0),
        .string => |item| try appendString(allocator, data, item),
        .array => |arr| {
            try appendLe(u32, allocator, data, @intFromEnum(arr.element_type));
            try appendLe(u64, allocator, data, arr.values.len);
            for (arr.values) |item| try appendMetadataValue(allocator, data, item);
        },
        .u64 => |item| try appendLe(u64, allocator, data, item),
        .i64 => |item| try appendLe(i64, allocator, data, item),
        .f64 => |item| try appendLe(u64, allocator, data, @bitCast(item)),
    }
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

fn alignForward(value: u64, alignment: u64) u64 {
    if (alignment <= 1) return value;
    return std.mem.alignForward(u64, value, alignment);
}

fn readAlignment(metadata: []const format.MetadataEntry) ?u64 {
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

test "writer emits parseable gguf file" {
    const allocator = std.testing.allocator;
    const metadata = [_]format.MetadataEntry{
        .{ .key = "general.architecture", .value = .{ .string = "llama" } },
        .{ .key = "general.alignment", .value = .{ .u32 = 32 } },
    };
    const dims = [_]u64{ 4, 3 };
    const tensors = [_]TensorSpec{.{
        .name = "token_embd.weight",
        .dimensions = &dims,
        .tensor_type = .{ .known = .F16 },
    }};

    var layout = try buildLayout(allocator, &metadata, &tensors);
    defer layout.deinit(allocator);

    const tensor_bytes = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23 };
    var raw = std.ArrayListUnmanaged(u8).empty;
    defer raw.deinit(allocator);
    try raw.appendSlice(allocator, layout.header_bytes);
    const data_region_offset = alignForward(layout.header_bytes.len, layout.alignment);
    try raw.appendNTimes(allocator, 0, data_region_offset - layout.header_bytes.len);
    try raw.appendSlice(allocator, &tensor_bytes);

    var parsed = try format.parse(allocator, raw.items);
    defer parsed.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), parsed.metadata.len);
    try std.testing.expectEqual(@as(usize, 1), parsed.tensors.len);
    try std.testing.expectEqualStrings("llama", parsed.metadata[0].value.string);
    try std.testing.expectEqualStrings("token_embd.weight", parsed.tensors[0].name);
    try std.testing.expectEqual(@as(u64, 32), parsed.alignment);
}
