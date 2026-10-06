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
const token = @import("token.zig");

pub const Kind = enum {
    string,
    integer,
    number,
    boolean,
    datetime,
    bytes,

    pub fn tag(self: Kind) []const u8 {
        return switch (self) {
            .string => "s",
            .integer => "i",
            .number => "n",
            .boolean => "b",
            .datetime => "t",
            .bytes => "x",
        };
    }
};

pub const Scalar = struct {
    kind: Kind,
    canonical: []const u8,
};

pub fn kindFromFieldType(field_type: []const u8) Kind {
    if (std.mem.eql(u8, field_type, "integer") or std.mem.eql(u8, field_type, "int")) return .integer;
    if (std.mem.eql(u8, field_type, "number") or std.mem.eql(u8, field_type, "numeric") or std.mem.eql(u8, field_type, "float") or std.mem.eql(u8, field_type, "double")) return .number;
    if (std.mem.eql(u8, field_type, "bool") or std.mem.eql(u8, field_type, "boolean")) return .boolean;
    if (std.mem.eql(u8, field_type, "datetime") or std.mem.eql(u8, field_type, "timestamp") or std.mem.eql(u8, field_type, "date")) return .datetime;
    if (std.mem.eql(u8, field_type, "bytes") or std.mem.eql(u8, field_type, "binary")) return .bytes;
    return .string;
}

pub fn scalarFromJsonAlloc(alloc: Allocator, field_type: []const u8, json_value: std.json.Value) !?[]u8 {
    return switch (kindFromFieldType(field_type)) {
        .integer => switch (json_value) {
            .integer => |v| try canonicalIntegerAlloc(alloc, v),
            .string => |v| blk: {
                const parsed = std.fmt.parseInt(i64, v, 10) catch break :blk null;
                break :blk try canonicalIntegerAlloc(alloc, parsed);
            },
            else => null,
        },
        .number => switch (json_value) {
            .integer => |v| try canonicalNumberAlloc(alloc, @as(f64, @floatFromInt(v))),
            .float => |v| try canonicalNumberAlloc(alloc, v),
            .string => |v| blk: {
                const parsed = std.fmt.parseFloat(f64, v) catch break :blk null;
                break :blk canonicalNumberAlloc(alloc, parsed) catch |err| switch (err) {
                    error.InvalidScalarToken => null,
                    else => return err,
                };
            },
            else => null,
        },
        .boolean => switch (json_value) {
            .bool => |v| try canonicalBooleanAlloc(alloc, v),
            else => null,
        },
        .datetime => switch (json_value) {
            .string => |v| try canonicalAlloc(alloc, .datetime, v),
            else => null,
        },
        .bytes, .string => switch (json_value) {
            .string => |v| try canonicalAlloc(alloc, kindFromFieldType(field_type), v),
            .integer => |v| try canonicalIntegerAlloc(alloc, v),
            .float => |v| try canonicalNumberAlloc(alloc, v),
            .bool => |v| try canonicalBooleanAlloc(alloc, v),
            else => null,
        },
    };
}

pub fn scalarFromTextAlloc(alloc: Allocator, field_type: []const u8, text: []const u8) ![]u8 {
    return switch (kindFromFieldType(field_type)) {
        .integer => canonicalIntegerAlloc(alloc, try std.fmt.parseInt(i64, text, 10)),
        .number => canonicalNumberAlloc(alloc, try std.fmt.parseFloat(f64, text)),
        .boolean => {
            if (std.mem.eql(u8, text, "true")) return try canonicalBooleanAlloc(alloc, true);
            if (std.mem.eql(u8, text, "false")) return try canonicalBooleanAlloc(alloc, false);
            return error.InvalidScalarToken;
        },
        .datetime => canonicalAlloc(alloc, .datetime, text),
        .bytes => canonicalAlloc(alloc, .bytes, text),
        .string => canonicalAlloc(alloc, .string, text),
    };
}

pub fn canonicalAlloc(alloc: Allocator, kind: Kind, canonical_value: []const u8) ![]u8 {
    return try token.canonicalTupleAlloc(alloc, &.{ kind.tag(), canonical_value });
}

pub fn canonicalIntegerAlloc(alloc: Allocator, value: i64) ![]u8 {
    const encoded = try std.fmt.allocPrint(alloc, "{d}", .{value});
    defer alloc.free(encoded);
    return try canonicalAlloc(alloc, .integer, encoded);
}

pub fn canonicalNumberAlloc(alloc: Allocator, value: f64) ![]u8 {
    if (std.math.isNan(value)) return error.InvalidScalarToken;
    const normalized = if (value == 0) 0 else value;
    const encoded = try std.fmt.allocPrint(alloc, "{d}", .{normalized});
    defer alloc.free(encoded);
    return try canonicalAlloc(alloc, .number, encoded);
}

pub fn canonicalBooleanAlloc(alloc: Allocator, value: bool) ![]u8 {
    return try canonicalAlloc(alloc, .boolean, if (value) "true" else "false");
}

pub fn parseScalarAlloc(alloc: Allocator, encoded: []const u8) !Scalar {
    const parts = try token.decodeTupleAlloc(alloc, encoded);
    errdefer {
        for (parts) |part| alloc.free(part);
        if (parts.len > 0) alloc.free(parts);
    }
    if (parts.len != 2) return error.InvalidScalarToken;
    const kind: Kind = if (std.mem.eql(u8, parts[0], "s"))
        .string
    else if (std.mem.eql(u8, parts[0], "i"))
        .integer
    else if (std.mem.eql(u8, parts[0], "n"))
        .number
    else if (std.mem.eql(u8, parts[0], "b"))
        .boolean
    else if (std.mem.eql(u8, parts[0], "t"))
        .datetime
    else if (std.mem.eql(u8, parts[0], "x"))
        .bytes
    else
        return error.InvalidScalarToken;
    const canonical = parts[1];
    alloc.free(parts[0]);
    alloc.free(parts);
    return .{ .kind = kind, .canonical = canonical };
}

pub fn deinitScalar(alloc: Allocator, scalar: Scalar) void {
    alloc.free(scalar.canonical);
}

test "value canonical scalars keep typed collisions distinct" {
    const alloc = std.testing.allocator;
    const string_one = try scalarFromTextAlloc(alloc, "string", "1");
    defer alloc.free(string_one);
    const int_one = try scalarFromTextAlloc(alloc, "integer", "1");
    defer alloc.free(int_one);
    const number_one = try scalarFromTextAlloc(alloc, "number", "1.0");
    defer alloc.free(number_one);

    try std.testing.expect(!std.mem.eql(u8, string_one, int_one));
    try std.testing.expect(!std.mem.eql(u8, int_one, number_one));
}

test "value parses numeric strings through declared numeric field types" {
    const alloc = std.testing.allocator;
    const int_scalar = (try scalarFromJsonAlloc(alloc, "integer", .{ .string = "42" })).?;
    defer alloc.free(int_scalar);
    const number_scalar = (try scalarFromJsonAlloc(alloc, "number", .{ .string = "42.5" })).?;
    defer alloc.free(number_scalar);
    const invalid_number = try scalarFromJsonAlloc(alloc, "number", .{ .string = "nope" });

    const int_decoded = try parseScalarAlloc(alloc, int_scalar);
    defer deinitScalar(alloc, int_decoded);
    const number_decoded = try parseScalarAlloc(alloc, number_scalar);
    defer deinitScalar(alloc, number_decoded);

    try std.testing.expectEqual(Kind.integer, int_decoded.kind);
    try std.testing.expectEqualStrings("42", int_decoded.canonical);
    try std.testing.expectEqual(Kind.number, number_decoded.kind);
    try std.testing.expectEqualStrings("42.5", number_decoded.canonical);
    try std.testing.expect(invalid_number == null);
}

test "value parses canonical scalar tag and payload" {
    const alloc = std.testing.allocator;
    const encoded = try scalarFromTextAlloc(alloc, "boolean", "true");
    defer alloc.free(encoded);
    const scalar = try parseScalarAlloc(alloc, encoded);
    defer deinitScalar(alloc, scalar);
    try std.testing.expectEqual(Kind.boolean, scalar.kind);
    try std.testing.expectEqualStrings("true", scalar.canonical);
}
