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

//! Structural JSON assertions for tests.
//!
//! Object member order is ignored. Array order and JSON value types remain
//! significant. Failures report the first mismatched JSON Pointer.

const std = @import("std");

const Value = std.json.Value;

const MatchMode = enum {
    equal,
    subset,
};

pub fn valuesEqual(expected: Value, actual: Value) bool {
    return matches(expected, actual, .equal);
}

pub fn valueIsSubset(expected: Value, actual: Value) bool {
    return matches(expected, actual, .subset);
}

pub fn expectEqual(expected: Value, actual: Value) !void {
    try expectMatch(expected, actual, .equal);
}

pub fn expectSubset(expected: Value, actual: Value) !void {
    try expectMatch(expected, actual, .subset);
}

pub fn expectEqualJsonText(alloc: std.mem.Allocator, expected_json: []const u8, actual_json: []const u8) !void {
    var expected = try std.json.parseFromSlice(Value, alloc, expected_json, .{});
    defer expected.deinit();
    var actual = try std.json.parseFromSlice(Value, alloc, actual_json, .{});
    defer actual.deinit();
    try expectEqual(expected.value, actual.value);
}

pub fn expectEqualJsonValue(alloc: std.mem.Allocator, expected_json: []const u8, actual: Value) !void {
    var expected = try std.json.parseFromSlice(Value, alloc, expected_json, .{});
    defer expected.deinit();
    try expectEqual(expected.value, actual);
}

pub fn expectSubsetJsonText(alloc: std.mem.Allocator, expected_json: []const u8, actual_json: []const u8) !void {
    var expected = try std.json.parseFromSlice(Value, alloc, expected_json, .{});
    defer expected.deinit();
    var actual = try std.json.parseFromSlice(Value, alloc, actual_json, .{});
    defer actual.deinit();
    try expectSubset(expected.value, actual.value);
}

pub fn expectSubsetJsonValue(alloc: std.mem.Allocator, expected_json: []const u8, actual: Value) !void {
    var expected = try std.json.parseFromSlice(Value, alloc, expected_json, .{});
    defer expected.deinit();
    try expectSubset(expected.value, actual);
}

fn expectMatch(expected: Value, actual: Value, mode: MatchMode) !void {
    if (matches(expected, actual, mode)) return;

    var path = std.ArrayListUnmanaged(u8).empty;
    defer path.deinit(std.testing.allocator);
    _ = try findMismatch(std.testing.allocator, &path, expected, actual, mode);
    std.debug.print(
        "JSON mismatch at {s}\nexpected: {f}\nactual:   {f}\n",
        .{
            if (path.items.len == 0) "/" else path.items,
            std.json.fmt(expected, .{}),
            std.json.fmt(actual, .{}),
        },
    );
    return error.TestExpectedEqual;
}

fn matches(expected: Value, actual: Value, mode: MatchMode) bool {
    if (std.meta.activeTag(expected) != std.meta.activeTag(actual)) return false;
    return switch (expected) {
        .null => true,
        .bool => |value| value == actual.bool,
        .integer => |value| value == actual.integer,
        .float => |value| value == actual.float,
        .number_string => |value| std.mem.eql(u8, value, actual.number_string),
        .string => |value| std.mem.eql(u8, value, actual.string),
        .array => |array| blk: {
            if (array.items.len != actual.array.items.len) break :blk false;
            for (array.items, actual.array.items) |expected_item, actual_item| {
                if (!matches(expected_item, actual_item, mode)) break :blk false;
            }
            break :blk true;
        },
        .object => |object| blk: {
            if (mode == .equal and object.count() != actual.object.count()) break :blk false;
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                const actual_value = actual.object.get(entry.key_ptr.*) orelse break :blk false;
                if (!matches(entry.value_ptr.*, actual_value, mode)) break :blk false;
            }
            break :blk true;
        },
    };
}

fn findMismatch(
    alloc: std.mem.Allocator,
    path: *std.ArrayListUnmanaged(u8),
    expected: Value,
    actual: Value,
    mode: MatchMode,
) !bool {
    if (std.meta.activeTag(expected) != std.meta.activeTag(actual)) return true;
    switch (expected) {
        .null => return false,
        .bool => |value| return value != actual.bool,
        .integer => |value| return value != actual.integer,
        .float => |value| return value != actual.float,
        .number_string => |value| return !std.mem.eql(u8, value, actual.number_string),
        .string => |value| return !std.mem.eql(u8, value, actual.string),
        .array => |array| {
            if (array.items.len != actual.array.items.len) return true;
            for (array.items, actual.array.items, 0..) |expected_item, actual_item, index| {
                const previous_len = path.items.len;
                var index_buffer: [32]u8 = undefined;
                const index_token = try std.fmt.bufPrint(&index_buffer, "/{d}", .{index});
                try path.appendSlice(alloc, index_token);
                if (try findMismatch(alloc, path, expected_item, actual_item, mode)) return true;
                path.shrinkRetainingCapacity(previous_len);
            }
            return false;
        },
        .object => |object| {
            if (mode == .equal and object.count() != actual.object.count()) return true;
            var iterator = object.iterator();
            while (iterator.next()) |entry| {
                const previous_len = path.items.len;
                try appendJsonPointerToken(alloc, path, entry.key_ptr.*);
                const actual_value = actual.object.get(entry.key_ptr.*) orelse return true;
                if (try findMismatch(alloc, path, entry.value_ptr.*, actual_value, mode)) return true;
                path.shrinkRetainingCapacity(previous_len);
            }
            return false;
        },
    }
}

fn appendJsonPointerToken(alloc: std.mem.Allocator, path: *std.ArrayListUnmanaged(u8), token: []const u8) !void {
    try path.append(alloc, '/');
    for (token) |byte| switch (byte) {
        '~' => try path.appendSlice(alloc, "~0"),
        '/' => try path.appendSlice(alloc, "~1"),
        else => try path.append(alloc, byte),
    };
}

test "JSON structural assertions ignore object order and preserve array order" {
    const alloc = std.testing.allocator;
    var expected = try std.json.parseFromSlice(Value, alloc, "{\"name\":\"antfly\",\"values\":[1,2]}", .{});
    defer expected.deinit();
    var reordered = try std.json.parseFromSlice(Value, alloc, "{\"values\":[1,2],\"name\":\"antfly\"}", .{});
    defer reordered.deinit();
    var reversed = try std.json.parseFromSlice(Value, alloc, "{\"name\":\"antfly\",\"values\":[2,1]}", .{});
    defer reversed.deinit();

    try expectEqual(expected.value, reordered.value);
    try std.testing.expect(!valuesEqual(expected.value, reversed.value));
}

test "JSON subset assertions recursively allow additional object members" {
    const alloc = std.testing.allocator;
    try expectSubsetJsonText(
        alloc,
        "{\"result\":{\"enabled\":true}}",
        "{\"jsonrpc\":\"2.0\",\"result\":{\"enabled\":true,\"count\":3}}",
    );
    var expected = try std.json.parseFromSlice(Value, alloc, "{\"result\":{\"enabled\":false}}", .{});
    defer expected.deinit();
    var actual = try std.json.parseFromSlice(Value, alloc, "{\"result\":{\"enabled\":true}}", .{});
    defer actual.deinit();
    try std.testing.expect(!valueIsSubset(expected.value, actual.value));
}
