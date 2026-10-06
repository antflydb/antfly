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
const json = @import("antfly-json");

pub const Allocator = std.mem.Allocator;
pub const ParsedJsonPathValue = struct {
    parsed: json.Parsed(json.Value),
    value: json.Value,

    pub fn deinit(self: *@This()) void {
        self.parsed.deinit();
    }
};

pub fn parseJsonValueAlloc(alloc: Allocator, body: []const u8) !json.Parsed(json.Value) {
    return try json.parseFromSlice(json.Value, alloc, body, .{});
}

pub fn parseJsonObjectAlloc(alloc: Allocator, body: []const u8) !json.Parsed(json.Value) {
    var parsed = try parseJsonValueAlloc(alloc, body);
    errdefer parsed.deinit();
    if (parsed.value != .object) return error.InvalidQueryRequest;
    return parsed;
}

pub fn parseJsonPathValueAlloc(
    alloc: Allocator,
    body: []const u8,
    path: []const u8,
) !?ParsedJsonPathValue {
    var parsed = parseJsonValueAlloc(alloc, body) catch return null;
    errdefer parsed.deinit();
    const value = extractJsonPathValue(parsed.value, path) orelse {
        parsed.deinit();
        return null;
    };
    return .{
        .parsed = parsed,
        .value = value,
    };
}

/// Parses into an owned JSON value; the caller must eventually free it via
/// `deinitJsonValue` or an owning container's `deinit`.
pub fn parseOwnedJsonValueAlloc(alloc: Allocator, body: []const u8) !json.Value {
    var parsed = try parseJsonValueAlloc(alloc, body);
    defer parsed.deinit();
    return try cloneJsonValue(alloc, parsed.value);
}

pub fn parseOwnedJsonValueAllocAlways(alloc: Allocator, body: []const u8) !json.Value {
    return try parseOwnedJsonValueAlloc(alloc, body);
}

/// Parses into an owned JSON object map; the caller must eventually free nested
/// values via `deinitJsonValue` or an owning container's `deinit`.
pub fn parseOwnedJsonObjectMapAlloc(
    alloc: Allocator,
    body: []const u8,
) !json.ArrayHashMap(json.Value) {
    var owned = try parseOwnedJsonValueAlloc(alloc, body);
    errdefer deinitJsonValue(alloc, &owned);
    if (owned != .object) return error.InvalidQueryRequest;
    var object = owned.object;
    owned = undefined;
    var out: json.ArrayHashMap(json.Value) = .{};
    errdefer out.deinit(alloc);
    var it = object.iterator();
    while (it.next()) |entry| {
        try out.map.put(alloc, entry.key_ptr.*, entry.value_ptr.*);
    }
    object.deinit(alloc);
    return out;
}

pub fn stringifyJsonValueAlloc(alloc: Allocator, value: json.Value) ![]u8 {
    return try std.fmt.allocPrint(alloc, "{f}", .{json.fmt(value, .{})});
}

pub fn scalarJsonValueStringAlloc(alloc: Allocator, value: json.Value) !?[]u8 {
    return switch (value) {
        .string => |text| try alloc.dupe(u8, text),
        .integer => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .float => |n| try std.fmt.allocPrint(alloc, "{d}", .{n}),
        .bool => |flag| try alloc.dupe(u8, if (flag) "true" else "false"),
        .null => null,
        else => null,
    };
}

pub fn extractJsonPathValue(value: json.Value, path: []const u8) ?json.Value {
    if (value == .object) {
        if (value.object.get(path)) |direct| return direct;
    }
    var current = value;
    var parts = std.mem.splitScalar(u8, path, '.');
    while (parts.next()) |part| {
        if (current != .object) return null;
        current = current.object.get(part) orelse return null;
    }
    return current;
}

pub fn cloneJsonValue(alloc: Allocator, value: json.Value) !json.Value {
    return switch (value) {
        .null => .null,
        .bool => |v| .{ .bool = v },
        .integer => |v| .{ .integer = v },
        .float => |v| .{ .float = v },
        .number_string => |v| .{ .number_string = try alloc.dupe(u8, v) },
        .string => |v| .{ .string = try alloc.dupe(u8, v) },
        .array => |arr| blk: {
            var out = json.Array.init(alloc);
            errdefer {
                for (out.items) |*item| deinitJsonValue(alloc, item);
                out.deinit();
            }
            for (arr.items) |item| {
                var cloned = try cloneJsonValue(alloc, item);
                errdefer deinitJsonValue(alloc, &cloned);
                try out.append(cloned);
                cloned = undefined;
            }
            break :blk .{ .array = out };
        },
        .object => |obj| blk: {
            var out = json.ObjectMap.empty;
            errdefer {
                var it = out.iterator();
                while (it.next()) |entry| {
                    alloc.free(@constCast(entry.key_ptr.*));
                    deinitJsonValue(alloc, entry.value_ptr);
                }
                out.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                const key = try alloc.dupe(u8, entry.key_ptr.*);
                errdefer alloc.free(key);
                var cloned = try cloneJsonValue(alloc, entry.value_ptr.*);
                errdefer deinitJsonValue(alloc, &cloned);
                try out.put(alloc, key, cloned);
                cloned = undefined;
            }
            break :blk .{ .object = out };
        },
    };
}

pub fn deinitJsonValue(alloc: Allocator, value: *json.Value) void {
    switch (value.*) {
        .string => |text| alloc.free(text),
        .number_string => |text| alloc.free(text),
        .array => |*arr| {
            for (arr.items) |*item| deinitJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(@constCast(entry.key_ptr.*));
                deinitJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
        else => {},
    }
    value.* = undefined;
}

pub fn jsonValuesEqual(lhs: json.Value, rhs: json.Value) bool {
    if (std.meta.activeTag(lhs) != std.meta.activeTag(rhs)) return false;

    return switch (lhs) {
        .null => true,
        .bool => |lhs_bool| lhs_bool == rhs.bool,
        .integer => |lhs_int| lhs_int == rhs.integer,
        .float => |lhs_float| lhs_float == rhs.float,
        .number_string => |lhs_number| std.mem.eql(u8, lhs_number, rhs.number_string),
        .string => |lhs_string| std.mem.eql(u8, lhs_string, rhs.string),
        .array => |lhs_array| blk: {
            if (lhs_array.items.len != rhs.array.items.len) break :blk false;
            for (lhs_array.items, rhs.array.items) |lhs_item, rhs_item| {
                if (!jsonValuesEqual(lhs_item, rhs_item)) break :blk false;
            }
            break :blk true;
        },
        .object => |lhs_object| blk: {
            if (lhs_object.count() != rhs.object.count()) break :blk false;
            var it = lhs_object.iterator();
            while (it.next()) |entry| {
                const rhs_value = rhs.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!jsonValuesEqual(entry.value_ptr.*, rhs_value)) break :blk false;
            }
            break :blk true;
        },
    };
}

test "json helpers parse object and extract nested path" {
    const alloc = std.testing.allocator;

    var parsed_obj = try parseJsonObjectAlloc(alloc, "{\"outer\":{\"inner\":7},\"flag\":true}");
    defer parsed_obj.deinit();
    try std.testing.expect(parsed_obj.value == .object);
    try std.testing.expectEqual(@as(i64, 7), extractJsonPathValue(parsed_obj.value, "outer.inner").?.integer);
    try std.testing.expectEqual(@as(?json.Value, null), extractJsonPathValue(parsed_obj.value, "outer.missing"));

    var parsed_path = (try parseJsonPathValueAlloc(alloc, "{\"outer\":{\"inner\":\"x\"}}", "outer.inner")).?;
    defer parsed_path.deinit();
    try std.testing.expectEqualStrings("x", parsed_path.value.string);

    try std.testing.expectError(error.InvalidQueryRequest, parseJsonObjectAlloc(alloc, "[1,2,3]"));
    try std.testing.expectEqual(@as(?ParsedJsonPathValue, null), try parseJsonPathValueAlloc(alloc, "{\"outer\":1}", "outer.inner"));
}

test "json helpers clone compare stringify and scalar conversion" {
    const alloc = std.testing.allocator;

    var original = try parseOwnedJsonValueAllocAlways(
        alloc,
        "{\"name\":\"ada\",\"meta\":{\"ok\":true},\"tags\":[\"x\",\"y\"]}",
    );
    defer deinitJsonValue(alloc, &original);

    var cloned = try cloneJsonValue(alloc, original);
    defer deinitJsonValue(alloc, &cloned);

    try std.testing.expect(jsonValuesEqual(original, cloned));

    cloned.object.getPtr("meta").?.object.getPtr("ok").?.bool = false;
    try std.testing.expect(!jsonValuesEqual(original, cloned));
    try std.testing.expect(original.object.get("meta").?.object.get("ok").?.bool);

    const stringified = try stringifyJsonValueAlloc(alloc, original);
    defer alloc.free(stringified);
    try std.testing.expect(std.mem.indexOf(u8, stringified, "\"name\":\"ada\"") != null);

    const scalar_string = (try scalarJsonValueStringAlloc(alloc, .{ .string = "hello" })).?;
    defer alloc.free(scalar_string);
    try std.testing.expectEqualStrings("hello", scalar_string);

    const scalar_int = (try scalarJsonValueStringAlloc(alloc, .{ .integer = 42 })).?;
    defer alloc.free(scalar_int);
    try std.testing.expectEqualStrings("42", scalar_int);

    const scalar_bool = (try scalarJsonValueStringAlloc(alloc, .{ .bool = true })).?;
    defer alloc.free(scalar_bool);
    try std.testing.expectEqualStrings("true", scalar_bool);

    try std.testing.expectEqual(@as(?[]u8, null), try scalarJsonValueStringAlloc(alloc, .null));
}

test "json helpers parse owned object map" {
    const alloc = std.testing.allocator;

    var parsed = try parseOwnedJsonObjectMapAlloc(alloc, "{\"name\":\"ada\",\"kind\":\"user\"}");
    defer {
        var it = parsed.map.iterator();
        while (it.next()) |entry| {
            alloc.free(@constCast(entry.key_ptr.*));
            deinitJsonValue(alloc, entry.value_ptr);
        }
        parsed.deinit(alloc);
    }

    try std.testing.expectEqualStrings("ada", parsed.map.get("name").?.string);
    try std.testing.expectEqualStrings("user", parsed.map.get("kind").?.string);
}
