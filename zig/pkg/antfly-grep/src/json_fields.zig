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

//! Pre-quoted schema keys. String escaping, dynamic JSON, numeric formatting
//! and custom serializers stay owned by std.json. Only statically known struct
//! keys avoid the runtime escape scan, recursively through typed envelopes.
const std = @import("std");

pub fn field(json: *std.json.Stringify, comptime name: []const u8) !void {
    const safe = comptime for (name) |ch| {
        if (ch < 0x20 or ch >= 0x7f or ch == '"' or ch == '\\') break false;
    } else true;
    if (safe) try json.objectFieldRaw("\"" ++ name ++ "\"") else try json.objectField(name);
}

pub fn write(json: *std.json.Stringify, value: anytype) std.json.Stringify.Error!void {
    const T = @TypeOf(value);
    switch (@typeInfo(T)) {
        .@"struct" => |s| {
            if (comptime s.is_tuple or std.meta.hasFn(T, "jsonStringify")) return json.write(value);
            try json.beginObject();
            inline for (s.field_names, s.field_types) |name, ft| {
                if (ft == void) continue;
                const emit = if (@typeInfo(ft) == .optional) json.options.emit_null_optional_fields or @field(value, name) != null else true;
                if (emit) {
                    try field(json, name);
                    try write(json, @field(value, name));
                }
            }
            try json.endObject();
        },
        .optional => if (value) |child| try write(json, child) else try json.write(null),
        else => try json.write(value),
    }
}

test "static field encoding is byte identical to std.json including escapes and options" {
    const Custom = struct {
        pub fn jsonStringify(_: @This(), json: *std.json.Stringify) !void {
            try json.write("custom");
        }
    };
    const value = .{ .plain = .{ .number = @as(i64, -12), .unicode = "é\n\"\\" }, .@"quote\"\nkey" = true, .absent = @as(?u32, null), .present = @as(?u32, 42), .tuple = .{ 1, "two" }, .custom = Custom{}, .bytes = @as([]const u8, &.{0xff}), .dynamic = std.json.Value{ .integer = 7 } };
    for ([_]std.json.Stringify.Options{ .{}, .{ .emit_null_optional_fields = false, .escape_unicode = true }, .{ .whitespace = .indent_2 } }) |options| {
        const expected = try std.json.Stringify.valueAlloc(std.testing.allocator, value, options);
        defer std.testing.allocator.free(expected);
        var out = std.Io.Writer.Allocating.init(std.testing.allocator);
        defer out.deinit();
        var json: std.json.Stringify = .{ .writer = &out.writer, .options = options };
        try write(&json, value);
        try std.testing.expectEqualStrings(expected, out.written());
    }
}
