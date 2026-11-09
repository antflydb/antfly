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

//! Binary-safe native command JSON. Only integrity metadata uses byte arrays;
//! ordinary primary JSON documents keep their existing compact representation.
const std = @import("std");

pub fn write(value: anytype, stream: anytype) @TypeOf(stream.*).Error!void {
    return writeMode(value, stream, false);
}

/// Compact process-local transport. Valid UTF-8 byte slices use JSON strings;
/// arbitrary binary identities retain the existing byte-array representation.
/// The canonical command writer above remains unchanged.
pub fn writeCompact(value: anytype, stream: anytype) @TypeOf(stream.*).Error!void {
    return writeMode(value, stream, true);
}

fn writeMode(value: anytype, stream: anytype, comptime compact: bool) @TypeOf(stream.*).Error!void {
    const T = @TypeOf(value);
    // std.json.Value is a semantic JSON tree. Its object representation owns
    // hash-map pointers, which are not part of the wire format; delegate that
    // union to std.json's value encoder instead of recursively visiting its
    // implementation fields as if they were integrity command identities.
    if (comptime T == std.json.Value) return stream.write(value);
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            if (@hasDecl(T, "nativeJsonProjection")) return writeMode(value.nativeJsonProjection(), stream, compact);
            try stream.beginObject();
            inline for (info.field_names) |reflected_name| {
                if (!@hasDecl(T, "nativeJsonSkipField") or !value.nativeJsonSkipField(reflected_name)) {
                    try stream.objectField(reflected_name);
                    try writeMode(@field(value, reflected_name), stream, compact);
                }
            }
            try stream.endObject();
        },
        .@"union" => |info| {
            if (info.tag_type == null) @compileError("integrity commands require tagged unions");
            try stream.beginObject();
            switch (value) {
                inline else => |payload, tag| {
                    try stream.objectField(@tagName(tag));
                    try writeMode(payload, stream, compact);
                },
            }
            try stream.endObject();
        },
        .pointer => |info| {
            if (info.size != .slice) @compileError("integrity commands cannot serialize pointer identities");
            if (comptime compact and info.child == u8) {
                if (std.unicode.utf8ValidateSlice(value)) return stream.write(value);
            }
            try stream.beginArray();
            for (value) |element| try writeMode(element, stream, compact);
            try stream.endArray();
        },
        .array => {
            try stream.beginArray();
            for (value) |element| try writeMode(element, stream, compact);
            try stream.endArray();
        },
        .optional => if (value) |payload| try writeMode(payload, stream, compact) else try stream.write(null),
        .void => {
            // std.json's canonical tagged-union representation for void.
            try stream.beginObject();
            try stream.endObject();
        },
        .@"enum" => try stream.write(@tagName(value)),
        else => try stream.write(value),
    }
}

test "privacy compact metadata transport preserves binary bytes without expanding text" {
    const a = std.testing.allocator;
    const Input = struct { text: []const u8, binary: []const u8 };
    const input: Input = .{ .text = "scan cursor", .binary = &.{ 0xff, 0, 0xfe } };
    var output: std.Io.Writer.Allocating = .init(a);
    defer output.deinit();
    var stream: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try writeCompact(input, &stream);
    try std.testing.expectEqualStrings("{\"text\":\"scan cursor\",\"binary\":[255,0,254]}", output.written());
    var restored = try std.json.parseFromSlice(Input, a, output.written(), .{});
    defer restored.deinit();
    try std.testing.expectEqualSlices(u8, input.text, restored.value.text);
    try std.testing.expectEqualSlices(u8, input.binary, restored.value.binary);
    output.clearRetainingCapacity();
    var canonical: std.json.Stringify = .{ .writer = &output.writer, .options = .{} };
    try write(input, &canonical);
    try std.testing.expect(std.mem.startsWith(u8, output.written(), "{\"text\":[115,99,97,110"));
}
