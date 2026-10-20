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

//! Strict schemas use the same native types as ingestion and generated output.
const std = @import("std");
pub fn value(comptime T: type, a: std.mem.Allocator) !std.json.Value {
    return build(T, a, true);
}
pub fn wire(comptime T: type, a: std.mem.Allocator) !std.json.Value {
    return build(T, a, false);
}
fn build(comptime T: type, a: std.mem.Allocator, comptime strict: bool) anyerror!std.json.Value {
    var object: std.json.ObjectMap = .empty;
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            var properties: std.json.ObjectMap = .empty;
            var required: std.ArrayList(std.json.Value) = .empty;
            inline for (info.field_names, info.field_types, info.field_attrs) |name, Field, attrs| {
                try properties.put(a, name, try build(Field, a, strict));
                if (strict or attrs.defaultValue(Field) == null) try required.append(a, .{ .string = name });
            }
            try object.put(a, "type", .{ .string = "object" });
            try object.put(a, "properties", .{ .object = properties });
            if (required.items.len > 0) try object.put(a, "required", .{ .array = std.array_list.Managed(std.json.Value).fromOwnedSlice(a, try required.toOwnedSlice(a)) });
            try object.put(a, "additionalProperties", .{ .bool = false });
        },
        .@"enum" => |info| {
            var choices: std.ArrayList(std.json.Value) = .empty;
            inline for (info.field_names) |name| try choices.append(a, .{ .string = name });
            try object.put(a, "type", .{ .string = "string" });
            try object.put(a, "enum", .{ .array = std.array_list.Managed(std.json.Value).fromOwnedSlice(a, try choices.toOwnedSlice(a)) });
        },
        .optional => |info| {
            var any: std.ArrayList(std.json.Value) = .empty;
            try any.append(a, try build(info.child, a, strict));
            try any.append(a, try std.json.parseFromSliceLeaky(std.json.Value, a, "{\"type\":\"null\"}", .{}));
            try object.put(a, "anyOf", .{ .array = std.array_list.Managed(std.json.Value).fromOwnedSlice(a, try any.toOwnedSlice(a)) });
        },
        .pointer => |info| {
            if (info.size != .slice) @compileError("memory schema supports slices only");
            try object.put(a, "type", .{ .string = if (info.child == u8) "string" else "array" });
            if (info.child != u8) try object.put(a, "items", try build(info.child, a, strict));
        },
        .int => try object.put(a, "type", .{ .string = "integer" }),
        .float => try object.put(a, "type", .{ .string = "number" }),
        .bool => try object.put(a, "type", .{ .string = "boolean" }),
        else => @compileError("unsupported memory schema type: " ++ @typeName(T)),
    }
    return .{ .object = object };
}
test "memory schemas cover native generated contracts" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const schema = try value(@import("types.zig").Generated, arena.allocator());
    try std.testing.expect(schema.object.contains("additionalProperties"));
}
