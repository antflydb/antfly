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
const types = @import("types.zig");

pub fn lookupJson(
    alloc: Allocator,
    raw: []const u8,
    opts: types.LookupOptions,
) !types.LookupResult {
    if (opts.fields.len == 0 and opts.include_all_fields) {
        return .{ .json = try alloc.dupe(u8, raw) };
    }

    var projected = try projectLookupJsonValue(alloc, raw, opts);
    defer freeJsonValue(alloc, &projected);

    return .{
        .json = try std.json.Stringify.valueAlloc(alloc, projected, .{}),
    };
}

pub fn projectLookupJsonValue(
    alloc: Allocator,
    raw: []const u8,
    opts: types.LookupOptions,
) !std.json.Value {
    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, raw, .{});
    defer parsed.deinit();

    if (parsed.value != .object) {
        if (opts.fields.len == 0 and opts.include_all_fields) {
            return try cloneJsonValue(alloc, parsed.value);
        }
        return std.json.Value{ .object = std.json.ObjectMap.empty };
    }

    return try projectValue(alloc, parsed.value, opts);
}

fn projectValue(alloc: Allocator, root: std.json.Value, opts: types.LookupOptions) !std.json.Value {
    if (opts.fields.len == 0) return try cloneJsonValue(alloc, root);

    var includes = std.ArrayListUnmanaged([]const u8).empty;
    defer includes.deinit(alloc);
    var excludes = std.ArrayListUnmanaged([]const u8).empty;
    defer excludes.deinit(alloc);

    for (opts.fields) |field| {
        if (field.len > 0 and field[0] == '-') {
            try excludes.append(alloc, field[1..]);
        } else {
            try includes.append(alloc, field);
        }
    }

    var result = if (includes.items.len > 0)
        std.json.Value{ .object = std.json.ObjectMap.empty }
    else
        try cloneJsonValue(alloc, root);
    errdefer freeJsonValue(alloc, &result);

    if (includes.items.len > 0) {
        for (includes.items) |pattern| {
            try applyIncludePattern(alloc, root.object, &result.object, pattern);
        }
    }

    for (excludes.items) |pattern| {
        applyExcludePattern(alloc, &result.object, pattern);
    }

    return result;
}

fn applyIncludePattern(
    alloc: Allocator,
    src: std.json.ObjectMap,
    dst: *std.json.ObjectMap,
    pattern: []const u8,
) Allocator.Error!void {
    var parts = std.mem.tokenizeScalar(u8, pattern, '.');
    var path = std.ArrayListUnmanaged([]const u8).empty;
    defer path.deinit(alloc);
    while (parts.next()) |part| try path.append(alloc, part);
    if (path.items.len == 0) return;
    try applyIncludeRecursive(alloc, src, dst, path.items, 0);
}

fn applyIncludeRecursive(
    alloc: Allocator,
    src: std.json.ObjectMap,
    dst: *std.json.ObjectMap,
    parts: []const []const u8,
    depth: usize,
) Allocator.Error!void {
    if (depth >= parts.len) return;

    const part = parts[depth];
    if (std.mem.eql(u8, part, "*")) {
        var it = src.iterator();
        while (it.next()) |entry| {
            try includeField(alloc, entry.key_ptr.*, entry.value_ptr.*, dst, parts, depth);
        }
        return;
    }

    if (src.get(part)) |value| {
        try includeField(alloc, part, value, dst, parts, depth);
    }
}

fn includeField(
    alloc: Allocator,
    key: []const u8,
    value: std.json.Value,
    dst: *std.json.ObjectMap,
    parts: []const []const u8,
    depth: usize,
) Allocator.Error!void {
    if (depth == parts.len - 1) {
        try putOwnedValue(alloc, dst, key, try cloneJsonValue(alloc, value));
        return;
    }

    switch (value) {
        .object => |obj| {
            const nested = try ensureObjectValue(alloc, dst, key);
            try applyIncludeRecursive(alloc, obj, nested, parts, depth + 1);
        },
        .array => |arr| {
            if (depth + 1 < parts.len) {
                if (parseArrayIndex(parts[depth + 1])) |idx| {
                    if (idx >= arr.items.len) return;

                    var projected_items = std.json.Array.init(alloc);
                    errdefer {
                        for (projected_items.items) |*item| freeJsonValue(alloc, item);
                        projected_items.deinit();
                    }

                    const item = arr.items[idx];
                    if (depth + 1 == parts.len - 1) {
                        try projected_items.append(try cloneJsonValue(alloc, item));
                    } else switch (item) {
                        .object => |item_obj| {
                            var projected_item = std.json.Value{ .object = std.json.ObjectMap.empty };
                            errdefer freeJsonValue(alloc, &projected_item);
                            try applyIncludeRecursive(alloc, item_obj, &projected_item.object, parts, depth + 2);
                            if (projected_item.object.count() > 0) {
                                try projected_items.append(projected_item);
                            } else {
                                freeJsonValue(alloc, &projected_item);
                            }
                        },
                        else => {},
                    }

                    if (projected_items.items.len == 0) return;
                    try putOwnedValue(alloc, dst, key, .{ .array = projected_items });
                    return;
                }
            }

            var projected_items = std.json.Array.init(alloc);
            errdefer {
                for (projected_items.items) |*item| freeJsonValue(alloc, item);
                projected_items.deinit();
            }

            for (arr.items) |item| {
                switch (item) {
                    .object => |item_obj| {
                        var projected_item = std.json.Value{ .object = std.json.ObjectMap.empty };
                        errdefer freeJsonValue(alloc, &projected_item);
                        try applyIncludeRecursive(alloc, item_obj, &projected_item.object, parts, depth + 1);
                        if (projected_item.object.count() > 0) {
                            try projected_items.append(projected_item);
                        } else {
                            freeJsonValue(alloc, &projected_item);
                        }
                    },
                    else => {},
                }
            }

            if (projected_items.items.len == 0) return;
            try putOwnedValue(alloc, dst, key, .{ .array = projected_items });
        },
        else => {},
    }
}

fn applyExcludePattern(
    alloc: Allocator,
    doc: *std.json.ObjectMap,
    pattern: []const u8,
) void {
    var parts_iter = std.mem.tokenizeScalar(u8, pattern, '.');
    var parts = std.ArrayListUnmanaged([]const u8).empty;
    defer parts.deinit(alloc);
    while (parts_iter.next()) |part| parts.append(alloc, part) catch return;
    if (parts.items.len == 0) return;

    applyExcludeRecursive(alloc, doc, parts.items, 0);
}

fn applyExcludeRecursive(
    alloc: Allocator,
    doc: *std.json.ObjectMap,
    parts: []const []const u8,
    depth: usize,
) void {
    if (depth >= parts.len) return;

    const part = parts[depth];
    const is_last = depth == parts.len - 1;

    if (std.mem.eql(u8, part, "*")) {
        var keys = std.ArrayListUnmanaged([]const u8).empty;
        defer keys.deinit(alloc);
        var it = doc.iterator();
        while (it.next()) |entry| keys.append(alloc, entry.key_ptr.*) catch return;

        for (keys.items) |key| {
            if (is_last) {
                if (doc.fetchSwapRemove(key)) |entry| {
                    alloc.free(entry.key);
                    var removed = entry.value;
                    freeJsonValue(alloc, &removed);
                }
            } else if (doc.getPtr(key)) |value| {
                descendExclude(alloc, value, parts, depth + 1);
            }
        }
        return;
    }

    if (is_last) {
        if (doc.fetchSwapRemove(part)) |entry| {
            alloc.free(entry.key);
            var removed = entry.value;
            freeJsonValue(alloc, &removed);
        }
        return;
    }

    if (doc.getPtr(part)) |value| {
        descendExclude(alloc, value, parts, depth + 1);
    }
}

fn descendExclude(alloc: Allocator, value: *std.json.Value, parts: []const []const u8, depth: usize) void {
    switch (value.*) {
        .object => |*nested| applyExcludeRecursive(alloc, nested, parts, depth),
        .array => |*arr| {
            const part = parts[depth];
            if (parseArrayIndex(part)) |idx| {
                if (idx >= arr.items.len) return;
                const is_last = depth == parts.len - 1;
                if (is_last) {
                    var removed = arr.orderedRemove(idx);
                    freeJsonValue(alloc, &removed);
                    return;
                }
                if (arr.items[idx] == .object) applyExcludeRecursive(alloc, &arr.items[idx].object, parts, depth + 1);
                return;
            }
            for (arr.items) |*item| {
                if (item.* == .object) applyExcludeRecursive(alloc, &item.object, parts, depth);
            }
        },
        else => {},
    }
}

fn parseArrayIndex(part: []const u8) ?usize {
    if (part.len == 0) return null;
    for (part) |ch| {
        if (ch < '0' or ch > '9') return null;
    }
    return std.fmt.parseInt(usize, part, 10) catch null;
}

fn ensureObjectValue(
    alloc: Allocator,
    dst: *std.json.ObjectMap,
    key: []const u8,
) !*std.json.ObjectMap {
    if (dst.getPtr(key)) |existing| {
        if (existing.* == .object) return &existing.object;
        freeJsonValue(alloc, existing);
        existing.* = .{ .object = std.json.ObjectMap.empty };
        return &existing.object;
    }

    try dst.put(alloc, try alloc.dupe(u8, key), .{ .object = std.json.ObjectMap.empty });
    return &dst.getPtr(key).?.object;
}

fn putOwnedValue(
    alloc: Allocator,
    obj: *std.json.ObjectMap,
    key: []const u8,
    value: std.json.Value,
) !void {
    if (obj.getPtr(key)) |existing| {
        freeJsonValue(alloc, existing);
        existing.* = value;
        return;
    }
    try obj.put(alloc, try alloc.dupe(u8, key), value);
}

fn cloneJsonValue(alloc: Allocator, value: std.json.Value) !std.json.Value {
    return switch (value) {
        .null => .null,
        .bool => |b| .{ .bool = b },
        .integer => |i| .{ .integer = i },
        .float => |f| .{ .float = f },
        .number_string => |s| .{ .number_string = try alloc.dupe(u8, s) },
        .string => |s| .{ .string = try alloc.dupe(u8, s) },
        .array => |arr| blk: {
            var cloned = std.json.Array.init(alloc);
            errdefer {
                for (cloned.items) |*item| freeJsonValue(alloc, item);
                cloned.deinit();
            }
            for (arr.items) |item| try cloned.append(try cloneJsonValue(alloc, item));
            break :blk .{ .array = cloned };
        },
        .object => |obj| blk: {
            var cloned = std.json.ObjectMap.empty;
            errdefer {
                var it = cloned.iterator();
                while (it.next()) |entry| {
                    alloc.free(entry.key_ptr.*);
                    freeJsonValue(alloc, entry.value_ptr);
                }
                cloned.deinit(alloc);
            }
            var it = obj.iterator();
            while (it.next()) |entry| {
                try cloned.put(alloc, try alloc.dupe(u8, entry.key_ptr.*), try cloneJsonValue(alloc, entry.value_ptr.*));
            }
            break :blk .{ .object = cloned };
        },
    };
}

fn freeJsonValue(alloc: Allocator, value: *std.json.Value) void {
    switch (value.*) {
        .null, .bool, .integer, .float => {},
        .number_string => |s| alloc.free(s),
        .string => |s| alloc.free(s),
        .array => |*arr| {
            for (arr.items) |*item| freeJsonValue(alloc, item);
            arr.deinit();
        },
        .object => |*obj| {
            var it = obj.iterator();
            while (it.next()) |entry| {
                alloc.free(entry.key_ptr.*);
                freeJsonValue(alloc, entry.value_ptr);
            }
            obj.deinit(alloc);
        },
    }
}

test "document query lookupJson projects nested fields and exclusions" {
    const alloc = std.testing.allocator;

    const raw =
        \\{"title":"alpha","author":{"name":"ann","age":42},"tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
    ;

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{ "title", "author.name", "tags.name", "-body" },
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    try std.testing.expect(parsed.value.object.get("title") != null);
    try std.testing.expect(parsed.value.object.get("body") == null);
    try std.testing.expectEqualStrings("ann", parsed.value.object.get("author").?.object.get("name").?.string);
    const tags = parsed.value.object.get("tags").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), tags.len);
    try std.testing.expectEqualStrings("db", tags[0].object.get("name").?.string);
}

test "document query lookupJson returns full document when include_all_fields" {
    const alloc = std.testing.allocator;
    const raw = "{\"title\":\"alpha\",\"body\":\"hello\"}";

    var result = try lookupJson(alloc, raw, .{});
    defer result.deinit(alloc);

    try std.testing.expectEqualStrings(raw, result.json);
}

test "document query lookupJson supports indexed array paths" {
    const alloc = std.testing.allocator;

    const raw =
        \\{"title":"alpha","tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
    ;

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{"tags.1.name"},
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    const tags = parsed.value.object.get("tags").?.array.items;
    try std.testing.expectEqual(@as(usize, 1), tags.len);
    try std.testing.expectEqualStrings("zig", tags[0].object.get("name").?.string);
}

test "document query lookupJson supports indexed array exclusions" {
    const alloc = std.testing.allocator;

    const raw =
        \\{"title":"alpha","tags":[{"name":"db","score":1},{"name":"zig","score":2}],"body":"hello"}
    ;

    var result = try lookupJson(alloc, raw, .{
        .fields = &.{ "tags", "-tags.1.score" },
        .include_all_fields = false,
    });
    defer result.deinit(alloc);

    const parsed = try std.json.parseFromSlice(std.json.Value, alloc, result.json, .{});
    defer parsed.deinit();

    const tags = parsed.value.object.get("tags").?.array.items;
    try std.testing.expectEqual(@as(usize, 2), tags.len);
    try std.testing.expectEqual(@as(i64, 1), tags[0].object.get("score").?.integer);
    try std.testing.expect(tags[1].object.get("score") == null);
}
