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

/// A structured log field (key-value pair).
pub const Field = struct {
    key: []const u8,
    value: Value,

    pub const Value = union(enum) {
        string: []const u8,
        int: i64,
        uint: u64,
        float: f64,
        boolean: bool,
    };

    pub fn str(key: []const u8, val: []const u8) Field {
        return .{ .key = key, .value = .{ .string = val } };
    }

    pub fn int(key: []const u8, val: i64) Field {
        return .{ .key = key, .value = .{ .int = val } };
    }

    pub fn uint(key: []const u8, val: u64) Field {
        return .{ .key = key, .value = .{ .uint = val } };
    }

    pub fn float(key: []const u8, val: f64) Field {
        return .{ .key = key, .value = .{ .float = val } };
    }

    pub fn boolean(key: []const u8, val: bool) Field {
        return .{ .key = key, .value = .{ .boolean = val } };
    }

    /// Write this field's value as JSON to the writer.
    pub fn writeJsonValue(self: Field, writer: *std.Io.Writer) !void {
        switch (self.value) {
            .string => |s| {
                try writer.writeByte('"');
                try writeJsonEscaped(writer, s);
                try writer.writeByte('"');
            },
            .int => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d}", .{v}),
            .boolean => |v| try writer.writeAll(if (v) "true" else "false"),
        }
    }

    /// Write this field's value as text (key=value) to the writer.
    pub fn writeTextValue(self: Field, writer: *std.Io.Writer) !void {
        switch (self.value) {
            .string => |s| try writer.writeAll(s),
            .int => |v| try writer.print("{d}", .{v}),
            .uint => |v| try writer.print("{d}", .{v}),
            .float => |v| try writer.print("{d}", .{v}),
            .boolean => |v| try writer.writeAll(if (v) "true" else "false"),
        }
    }
};

fn writeJsonEscaped(writer: *std.Io.Writer, s: []const u8) !void {
    for (s) |c| {
        switch (c) {
            '"' => try writer.writeAll("\\\""),
            '\\' => try writer.writeAll("\\\\"),
            '\n' => try writer.writeAll("\\n"),
            '\r' => try writer.writeAll("\\r"),
            '\t' => try writer.writeAll("\\t"),
            else => {
                if (c < 0x20) {
                    try writer.print("\\u{x:0>4}", .{c});
                } else {
                    try writer.writeByte(c);
                }
            },
        }
    }
}

test "fields: json value output" {
    var w: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer w.deinit();

    {
        const f = Field.str("key", "hello world");
        try f.writeJsonValue(&w.writer);
        try std.testing.expectEqualStrings("\"hello world\"", w.writer.buffered());
        w.clearRetainingCapacity();
    }

    {
        const f = Field.int("key", -42);
        try f.writeJsonValue(&w.writer);
        try std.testing.expectEqualStrings("-42", w.writer.buffered());
        w.clearRetainingCapacity();
    }

    {
        const f = Field.boolean("key", true);
        try f.writeJsonValue(&w.writer);
        try std.testing.expectEqualStrings("true", w.writer.buffered());
        w.clearRetainingCapacity();
    }
}

test "fields: json escaping" {
    var w: std.Io.Writer.Allocating = .init(std.testing.allocator);
    defer w.deinit();

    const f = Field.str("key", "hello\n\"world\"\\end");
    try f.writeJsonValue(&w.writer);
    try std.testing.expectEqualStrings("\"hello\\n\\\"world\\\"\\\\end\"", w.writer.buffered());
}
