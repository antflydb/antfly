// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const Allocator = std.mem.Allocator;

pub const Hash128 = struct {
    hi: u64,
    lo: u64,

    pub fn format(self: @This(), comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("{x:0>16}{x:0>16}", .{ self.hi, self.lo });
    }
};

pub fn hash128(bytes: []const u8) Hash128 {
    return .{
        .hi = std.hash.Wyhash.hash(0x9e3779b97f4a7c15, bytes),
        .lo = std.hash.Wyhash.hash(0xc2b2ae3d27d4eb4f, bytes),
    };
}

pub fn appendComponent(out: *std.ArrayListUnmanaged(u8), alloc: Allocator, value: []const u8) !void {
    if (out.items.len > 0) try out.append(alloc, '|');
    const prefix = try std.fmt.allocPrint(alloc, "{}:", .{value.len});
    defer alloc.free(prefix);
    try out.appendSlice(alloc, prefix);
    try out.appendSlice(alloc, value);
}

pub fn canonicalTupleAlloc(alloc: Allocator, values: []const []const u8) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(alloc);
    for (values) |value| try appendComponent(&out, alloc, value);
    return try out.toOwnedSlice(alloc);
}

pub const Component = struct {
    payload: []const u8,
    next: usize,
};

pub fn componentAt(bytes: []const u8, start: usize) !Component {
    var pos = start;
    if (pos < bytes.len and bytes[pos] == '|') pos += 1;
    const len_start = pos;
    while (pos < bytes.len and bytes[pos] >= '0' and bytes[pos] <= '9') : (pos += 1) {}
    if (pos == len_start or pos >= bytes.len or bytes[pos] != ':') return error.InvalidTokenComponent;
    const payload_len = try std.fmt.parseInt(usize, bytes[len_start..pos], 10);
    pos += 1;
    const payload_start = pos;
    const payload_end = payload_start + payload_len;
    if (payload_end > bytes.len) return error.InvalidTokenComponent;
    return .{ .payload = bytes[payload_start..payload_end], .next = payload_end };
}

pub fn decodeTupleAlloc(alloc: Allocator, bytes: []const u8) ![][]u8 {
    var out = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (out.items) |item| alloc.free(item);
        out.deinit(alloc);
    }
    var pos: usize = 0;
    while (pos < bytes.len) {
        const component = try componentAt(bytes, pos);
        try out.append(alloc, try alloc.dupe(u8, component.payload));
        pos = component.next;
        if (pos < bytes.len) {
            if (bytes[pos] != '|') return error.InvalidTokenComponent;
        }
    }
    return try out.toOwnedSlice(alloc);
}

pub fn canonicalFieldTokenAlloc(
    alloc: Allocator,
    table: []const u8,
    field: []const u8,
    doc_key: []const u8,
    type_name: []const u8,
    value: []const u8,
) ![]u8 {
    var values = [_][]const u8{ "field", table, field, doc_key, type_name, value };
    return try canonicalTupleAlloc(alloc, values[0..]);
}

pub fn scalarTokenFromFieldValueAlloc(alloc: Allocator, field_type: []const u8, value: std.json.Value) !?[]u8 {
    if (isIntegerType(field_type)) {
        return switch (value) {
            .integer => |v| blk: {
                const encoded = try std.fmt.allocPrint(alloc, "{d}", .{v});
                defer alloc.free(encoded);
                break :blk try scalarTokenAlloc(alloc, "i", encoded);
            },
            else => null,
        };
    }
    if (isNumericType(field_type)) {
        return switch (value) {
            .integer => |v| blk: {
                const encoded = try std.fmt.allocPrint(alloc, "{d}", .{@as(f64, @floatFromInt(v))});
                defer alloc.free(encoded);
                break :blk try scalarTokenAlloc(alloc, "n", encoded);
            },
            .float => |v| blk: {
                const encoded = try std.fmt.allocPrint(alloc, "{d}", .{v});
                defer alloc.free(encoded);
                break :blk try scalarTokenAlloc(alloc, "n", encoded);
            },
            else => null,
        };
    }
    if (isBoolType(field_type)) {
        return switch (value) {
            .bool => |v| try scalarTokenAlloc(alloc, "b", if (v) "true" else "false"),
            else => null,
        };
    }
    if (isDatetimeType(field_type)) {
        return switch (value) {
            .string => |v| try scalarTokenAlloc(alloc, "t", v),
            else => null,
        };
    }
    return switch (value) {
        .string => |v| try scalarTokenAlloc(alloc, "s", v),
        .integer => |v| blk: {
            const encoded = try std.fmt.allocPrint(alloc, "{d}", .{v});
            defer alloc.free(encoded);
            break :blk try scalarTokenAlloc(alloc, "i", encoded);
        },
        .float => |v| blk: {
            const encoded = try std.fmt.allocPrint(alloc, "{d}", .{v});
            defer alloc.free(encoded);
            break :blk try scalarTokenAlloc(alloc, "n", encoded);
        },
        .bool => |v| try scalarTokenAlloc(alloc, "b", if (v) "true" else "false"),
        else => null,
    };
}

pub fn scalarTokenFromFieldTextAlloc(alloc: Allocator, field_type: []const u8, text: []const u8) ![]u8 {
    if (isIntegerType(field_type)) {
        const value = try std.fmt.parseInt(i64, text, 10);
        const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
        defer alloc.free(encoded);
        return try scalarTokenAlloc(alloc, "i", encoded);
    }
    if (isNumericType(field_type)) {
        const normalized = try normalizeNumberTextAlloc(alloc, text);
        defer alloc.free(normalized);
        return try scalarTokenAlloc(alloc, "n", normalized);
    }
    if (isBoolType(field_type)) {
        if (std.mem.eql(u8, text, "true") or std.mem.eql(u8, text, "false")) {
            return try scalarTokenAlloc(alloc, "b", text);
        }
        return error.InvalidScalarToken;
    }
    if (isDatetimeType(field_type)) return try scalarTokenAlloc(alloc, "t", text);
    return try scalarTokenAlloc(alloc, "s", text);
}

pub fn scalarTokenJsonAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const parts = try decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |item| alloc.free(item);
        alloc.free(parts);
    }
    if (parts.len != 2) return error.InvalidScalarToken;
    const tag = parts[0];
    const value = parts[1];
    if (std.mem.eql(u8, tag, "s") or std.mem.eql(u8, tag, "t")) {
        return try std.json.Stringify.valueAlloc(alloc, value, .{});
    }
    if (std.mem.eql(u8, tag, "b")) {
        if (std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "false")) return try alloc.dupe(u8, value);
        return error.InvalidScalarToken;
    }
    if (std.mem.eql(u8, tag, "i") or std.mem.eql(u8, tag, "n")) {
        _ = std.fmt.parseFloat(f64, value) catch return error.InvalidScalarToken;
        return try alloc.dupe(u8, value);
    }
    return error.InvalidScalarToken;
}

pub fn scalarTokenTextAlloc(alloc: Allocator, encoded: []const u8) ![]u8 {
    const parts = try decodeTupleAlloc(alloc, encoded);
    defer {
        for (parts) |item| alloc.free(item);
        alloc.free(parts);
    }
    if (parts.len != 2) return error.InvalidScalarToken;
    return try alloc.dupe(u8, parts[1]);
}

fn scalarTokenAlloc(alloc: Allocator, tag: []const u8, value: []const u8) ![]u8 {
    return try canonicalTupleAlloc(alloc, &.{ tag, value });
}

fn normalizeNumberTextAlloc(alloc: Allocator, text: []const u8) ![]u8 {
    const value = try std.fmt.parseFloat(f64, text);
    return try std.fmt.allocPrint(alloc, "{d}", .{value});
}

fn isNumericType(field_type: []const u8) bool {
    return std.mem.eql(u8, field_type, "number") or
        std.mem.eql(u8, field_type, "numeric") or
        std.mem.eql(u8, field_type, "float") or
        std.mem.eql(u8, field_type, "double");
}

fn isIntegerType(field_type: []const u8) bool {
    return std.mem.eql(u8, field_type, "integer") or std.mem.eql(u8, field_type, "int");
}

fn isBoolType(field_type: []const u8) bool {
    return std.mem.eql(u8, field_type, "bool") or std.mem.eql(u8, field_type, "boolean");
}

fn isDatetimeType(field_type: []const u8) bool {
    return std.mem.eql(u8, field_type, "datetime") or
        std.mem.eql(u8, field_type, "timestamp") or
        std.mem.eql(u8, field_type, "date");
}

test "token canonical tuple is length delimited" {
    const alloc = std.testing.allocator;
    const values = [_][]const u8{ "a|b", "12:34" };
    const encoded = try canonicalTupleAlloc(alloc, values[0..]);
    defer alloc.free(encoded);
    try std.testing.expectEqualStrings("3:a|b|5:12:34", encoded);
}

test "token canonical tuple decodes length delimited values" {
    const alloc = std.testing.allocator;
    const values = [_][]const u8{ "a|b", "12:34", "" };
    const encoded = try canonicalTupleAlloc(alloc, values[0..]);
    defer alloc.free(encoded);
    const decoded = try decodeTupleAlloc(alloc, encoded);
    defer {
        for (decoded) |item| alloc.free(item);
        alloc.free(decoded);
    }
    try std.testing.expectEqual(@as(usize, 3), decoded.len);
    try std.testing.expectEqualStrings("a|b", decoded[0]);
    try std.testing.expectEqualStrings("12:34", decoded[1]);
    try std.testing.expectEqualStrings("", decoded[2]);
}

test "typed scalar tokens keep display collisions distinct" {
    const alloc = std.testing.allocator;
    const string_one = (try scalarTokenFromFieldValueAlloc(alloc, "string", .{ .string = "1" })).?;
    defer alloc.free(string_one);
    const int_one = (try scalarTokenFromFieldValueAlloc(alloc, "number", .{ .integer = 1 })).?;
    defer alloc.free(int_one);
    const bool_false = (try scalarTokenFromFieldValueAlloc(alloc, "boolean", .{ .bool = false })).?;
    defer alloc.free(bool_false);
    const string_false = (try scalarTokenFromFieldValueAlloc(alloc, "string", .{ .string = "false" })).?;
    defer alloc.free(string_false);

    try std.testing.expect(!std.mem.eql(u8, string_one, int_one));
    try std.testing.expect(!std.mem.eql(u8, bool_false, string_false));
}
