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
    if (T == std.json.Value) return .{ .object = .empty };
    var object: std.json.ObjectMap = .empty;
    switch (@typeInfo(T)) {
        .@"struct" => |info| {
            var properties: std.json.ObjectMap = .empty;
            var required: std.ArrayList(std.json.Value) = .empty;
            inline for (info.field_names, info.field_types, info.field_attrs) |name, Field, attrs| {
                var property = try build(Field, a, strict);
                // Wire defaults are part of the client contract. Strict model
                // schemas still require every field and do not supply defaults.
                if (!strict) if (attrs.defaultValue(Field)) |default| switch (@typeInfo(Field)) {
                    .bool, .int, .float, .@"enum", .optional => {
                        const json = try std.json.Stringify.valueAlloc(a, default, .{});
                        try property.object.put(a, "default", try std.json.parseFromSliceLeaky(std.json.Value, a, json, .{ .parse_numbers = false }));
                    },
                    else => {},
                };
                try properties.put(a, name, property);
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
        .int => |info| {
            try object.put(a, "type", .{ .string = "integer" });
            try object.put(a, "minimum", .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{std.math.minInt(T)}) });
            try object.put(a, "maximum", .{ .number_string = try std.fmt.allocPrint(a, "{d}", .{std.math.maxInt(T)}) });
            if (!strict) try object.put(a, "format", .{ .string = try std.fmt.allocPrint(a, "{s}int{d}", .{ if (info.signedness == .unsigned) "u" else "", if (info.bits <= 32) @as(u8, 32) else @as(u8, 64) }) });
        },
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

test "memory wire schemas preserve scalar defaults and native integer domains" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Contract = struct { revision: u64 = std.math.maxInt(u64), known: ?i64 = null, include_provisional: bool = true, limit: u16 = 20 };
    const props = (try wire(Contract, a)).object.get("properties").?.object;
    const revision = props.get("revision").?.object;
    try std.testing.expectEqualStrings("uint64", revision.get("format").?.string);
    try std.testing.expectEqualStrings("18446744073709551615", revision.get("maximum").?.number_string);
    try std.testing.expectEqualStrings("18446744073709551615", revision.get("default").?.number_string);
    try std.testing.expect(props.get("include_provisional").?.object.get("default").?.bool);
    const known = props.get("known").?.object;
    try std.testing.expect(known.get("default").? == .null);
    const integer = known.get("anyOf").?.array.items[0].object;
    try std.testing.expectEqualStrings("int64", integer.get("format").?.string);
    try std.testing.expectEqualStrings("-9223372036854775808", integer.get("minimum").?.number_string);
    const strict = try value(Contract, a);
    try std.testing.expectEqual(@as(usize, 4), strict.object.get("required").?.array.items.len);
    try std.testing.expect(strict.object.get("properties").?.object.get("include_provisional").?.object.get("default") == null);
}
