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

//! Arena-owned typed JSON copies. Preserve exact numeric tokens and native
//! integer values without a serialize/parse round trip at page boundaries.
const std = @import("std");
const Json = std.json.Value;

pub fn clone(alloc: std.mem.Allocator, value: Json) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    return cloneDepth(alloc, value, 0);
}

fn cloneDepth(alloc: std.mem.Allocator, value: Json, depth: usize) error{ OutOfMemory, SqlProgramLimitExceeded }!Json {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    return switch (value) {
        .string => |text| .{ .string = try alloc.dupe(u8, text) },
        .number_string => |text| .{ .number_string = try alloc.dupe(u8, text) },
        .array => |items| blk: {
            var out = std.array_list.Managed(Json).init(alloc);
            try out.ensureTotalCapacity(items.items.len);
            for (items.items) |item| out.appendAssumeCapacity(try cloneDepth(alloc, item, depth + 1));
            break :blk .{ .array = out };
        },
        .object => |object| blk: {
            var out: std.json.ObjectMap = .empty;
            try out.ensureTotalCapacity(alloc, object.count());
            for (object.keys(), object.values()) |key, item| try out.put(alloc, try alloc.dupe(u8, key), try cloneDepth(alloc, item, depth + 1));
            break :blk .{ .object = out };
        },
        else => value,
    };
}

test "typed JSON clone preserves exact numeric representation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const integer = try clone(arena.allocator(), .{ .integer = 9007199254740993 });
    try std.testing.expectEqual(@as(i64, 9007199254740993), integer.integer);
    const token = try clone(arena.allocator(), .{ .number_string = "18446744073709551615" });
    try std.testing.expectEqualStrings("18446744073709551615", token.number_string);
}

/// Versioned native values for immutable row payloads. Counts precede arrays
/// and maps, so decoding allocates each container once. Primitive arrays share
/// one tag; integer lanes use lossless zigzag varints. No schema is inferred.
pub fn encodeBinary(a: std.mem.Allocator, value: Json) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    try out.appendSlice(a, "AJV1");
    try encodeValue(a, &out, value, 0);
    return out.toOwnedSlice(a);
}
const WireTag = enum(u8) { nil, no, yes, integer, float, string, number, array, object, integers, floats, bools, nulls };
fn appendInt(a: std.mem.Allocator, out: *std.ArrayList(u8), value: anytype) !void {
    var bytes: [@sizeOf(@TypeOf(value))]u8 = undefined;
    std.mem.writeInt(@TypeOf(value), &bytes, value, .little);
    try out.appendSlice(a, &bytes);
}
fn appendText(a: std.mem.Allocator, out: *std.ArrayList(u8), text: []const u8) !void {
    try appendInt(a, out, std.math.cast(u32, text.len) orelse return error.SqlProgramLimitExceeded);
    try out.appendSlice(a, text);
}
fn appendInteger(a: std.mem.Allocator, out: *std.ArrayList(u8), value: i64) !void {
    var bits: u64 = @as(u64, @bitCast(value << 1)) ^ @as(u64, @bitCast(value >> 63));
    while (bits >= 128) : (bits >>= 7) try out.append(a, @as(u8, @truncate(bits)) | 128);
    try out.append(a, @intCast(bits));
}
fn encodeValue(a: std.mem.Allocator, out: *std.ArrayList(u8), value: Json, depth: usize) anyerror!void {
    if (depth > 64) return error.SqlProgramLimitExceeded;
    const tag: WireTag = switch (value) {
        .null => .nil,
        .bool => |v| if (v) .yes else .no,
        .integer => .integer,
        .float => .float,
        .string => .string,
        .number_string => .number,
        .object => .object,
        .array => |items| blk: {
            if (items.items.len == 0) break :blk .array;
            const first = std.meta.activeTag(items.items[0]);
            for (items.items) |item| if (std.meta.activeTag(item) != first) break :blk .array;
            break :blk switch (first) {
                .integer => .integers,
                .float => .floats,
                .bool => .bools,
                .null => .nulls,
                else => .array,
            };
        },
    };
    try out.append(a, @backingInt(tag));
    switch (value) {
        .null, .bool => {},
        .integer => |v| try appendInteger(a, out, v),
        .float => |v| try appendInt(a, out, @as(u64, @bitCast(v))),
        .string, .number_string => |v| try appendText(a, out, v),
        .object => |v| {
            try appendInt(a, out, std.math.cast(u32, v.count()) orelse return error.SqlProgramLimitExceeded);
            for (v.keys(), v.values()) |key, item| {
                try appendText(a, out, key);
                try encodeValue(a, out, item, depth + 1);
            }
        },
        .array => |v| {
            try appendInt(a, out, std.math.cast(u32, v.items.len) orelse return error.SqlProgramLimitExceeded);
            for (v.items) |item| switch (tag) {
                .integers => try appendInteger(a, out, item.integer),
                .floats => try appendInt(a, out, @as(u64, @bitCast(item.float))),
                .bools => try out.append(a, @intFromBool(item.bool)),
                .nulls => {},
                else => try encodeValue(a, out, item, depth + 1),
            };
        },
    }
}
/// The caller owns an arena/budget. Invalid counts are rejected before allocating;
/// an independent value bound also covers zero-byte homogeneous null arrays.
pub fn decodeBinary(a: std.mem.Allocator, bytes: []const u8) !Json {
    if (!std.mem.startsWith(u8, bytes, "AJV1")) return error.InvalidTypedJson;
    var reader: BinaryReader = .{ .a = a, .bytes = bytes, .at = 4 };
    const value = try reader.value(0);
    if (reader.at != bytes.len) return error.InvalidTypedJson;
    return value;
}
const BinaryReader = struct {
    a: std.mem.Allocator,
    bytes: []const u8,
    at: usize,
    remaining_values: usize = 4 * 1024 * 1024,
    fn take(self: *@This(), n: usize) ![]const u8 {
        if (n > self.bytes.len - self.at) return error.InvalidTypedJson;
        defer self.at += n;
        return self.bytes[self.at..][0..n];
    }
    fn int(self: *@This(), comptime T: type) !T {
        return std.mem.readInt(T, (try self.take(@sizeOf(T)))[0..@sizeOf(T)], .little);
    }
    fn text(self: *@This()) ![]const u8 {
        const length = try self.int(u32);
        return self.a.dupe(u8, try self.take(length));
    }
    fn integer(self: *@This()) !i64 {
        var bits: u64 = 0;
        var shift: u7 = 0;
        while (shift < 70) : (shift += 7) {
            const byte = (try self.take(1))[0];
            if (shift == 63 and byte > 1) return error.InvalidTypedJson;
            bits |= @as(u64, byte & 127) << @as(u6, @intCast(shift));
            if (byte < 128) {
                if (shift != 0 and byte == 0) return error.InvalidTypedJson;
                return @bitCast((bits >> 1) ^ (0 -% (bits & 1)));
            }
        }
        return error.InvalidTypedJson;
    }
    fn value(self: *@This(), depth: usize) anyerror!Json {
        if (depth > 64 or self.remaining_values == 0) return error.InvalidTypedJson;
        self.remaining_values -= 1;
        const tag = std.enums.fromInt(WireTag, (try self.take(1))[0]) orelse return error.InvalidTypedJson;
        return switch (tag) {
            .nil => .null,
            .no => .{ .bool = false },
            .yes => .{ .bool = true },
            .integer => .{ .integer = try self.integer() },
            .float => .{ .float = @bitCast(try self.int(u64)) },
            .string => .{ .string = try self.text() },
            .number => .{ .number_string = try self.text() },
            .object => blk: {
                const count = try self.int(u32);
                if (count > self.remaining_values or count > (self.bytes.len - self.at) / 5) return error.InvalidTypedJson;
                var object: std.json.ObjectMap = .empty;
                try object.ensureTotalCapacity(self.a, count);
                for (0..count) |_| {
                    const key = try self.text();
                    if (object.contains(key)) return error.InvalidTypedJson;
                    try object.put(self.a, key, try self.value(depth + 1));
                }
                break :blk .{ .object = object };
            },
            .array, .integers, .floats, .bools, .nulls => blk: {
                const count = try self.int(u32);
                const minimum: usize = if (tag == .floats) 8 else if (tag == .nulls) 0 else 1;
                if (count > self.remaining_values or (minimum != 0 and count > (self.bytes.len - self.at) / minimum)) return error.InvalidTypedJson;
                const items = try self.a.alloc(Json, count);
                if (tag != .array) self.remaining_values -= count;
                for (items) |*item| item.* = switch (tag) {
                    .integers => .{ .integer = try self.integer() },
                    .floats => .{ .float = @bitCast(try self.int(u64)) },
                    .bools => bool_value: {
                        const byte = (try self.take(1))[0];
                        if (byte > 1) return error.InvalidTypedJson;
                        break :bool_value .{ .bool = byte == 1 };
                    },
                    .nulls => .null,
                    else => try self.value(depth + 1),
                };
                break :blk .{ .array = .fromOwnedSlice(self.a, items) };
            },
        };
    }
};

test "external lake bounded WAL typed JSON binary preserves numeric identity and bounds malformed containers" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const input = try std.json.parseFromSliceLeaky(Json, a, "{\"empty\":[],\"ints\":[-9223372036854775808,0,9223372036854775807],\"floats\":[-0.0,1.25],\"bools\":[true,false],\"nulls\":[null,null],\"mixed\":[1,\"x\",{}]}", .{});
    const bytes = try encodeBinary(a, input);
    const decoded = try decodeBinary(a, bytes);
    try std.testing.expectEqualStrings(try std.json.Stringify.valueAlloc(a, input, .{}), try std.json.Stringify.valueAlloc(a, decoded, .{}));
    try std.testing.expectEqual(@as(u64, @bitCast(input.object.get("floats").?.array.items[0].float)), @as(u64, @bitCast(decoded.object.get("floats").?.array.items[0].float)));
    const exact = try decodeBinary(a, try encodeBinary(a, .{ .number_string = "18446744073709551615" }));
    try std.testing.expectEqualStrings("18446744073709551615", exact.number_string);
    for (0..bytes.len) |end| try std.testing.expectError(error.InvalidTypedJson, decodeBinary(a, bytes[0..end]));
    try std.testing.expectError(error.InvalidTypedJson, decodeBinary(a, "AJV1\x07\xff\xff\xff\xff"));
    try std.testing.expectError(error.InvalidTypedJson, decodeBinary(a, "AJV1\x0c\xff\xff\xff\xff"));
}
